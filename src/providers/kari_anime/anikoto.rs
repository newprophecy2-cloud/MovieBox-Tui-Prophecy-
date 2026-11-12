use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId,
    Release, Season, SourceMirror, SubtitleOption,
};
use reqwest::{
    Client,
    header::{ACCEPT, REFERER, USER_AGENT},
};
use scraper::{Html, Selector};
use serde_json::Value;
use std::collections::HashSet;

const SITE: &str = "https://anikototv.to";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

fn req_error(e: reqwest::Error) -> ProviderError {
    ProviderError::Network(e.to_string())
}
fn season(s: &str) -> usize {
    let l = s.to_lowercase();
    for marker in ["season ", "season-", "season_", "s"] {
        if let Some(i) = l.find(marker) {
            let tail = &l[i + marker.len()..];
            let n: String = tail.chars().take_while(char::is_ascii_digit).collect();
            if let Ok(v) = n.parse() {
                return v;
            }
        }
    }
    1
}
fn headers(b: reqwest::RequestBuilder, referer: &str) -> reqwest::RequestBuilder {
    b.header(USER_AGENT, UA)
        .header(ACCEPT, "application/json, text/html, */*")
        .header(REFERER, referer)
}
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Candidate {
    slug: String,
    name: String,
    id: Option<String>,
}
fn candidate_html(html: &str, ajax: bool) -> Vec<Candidate> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a[href*='/watch/']").unwrap();
    let mut out = Vec::new();
    for a in doc.select(&sel) {
        let href = a.value().attr("href").unwrap_or("");
        let Some(rest) = href.split("/watch/").nth(1) else {
            continue;
        };
        let slug = rest.split(['?', '#', '/']).next().unwrap_or("");
        if slug.is_empty() {
            continue;
        }
        let name = if ajax {
            a.select(&Selector::parse(".name.d-title").unwrap())
                .next()
                .map(|x| x.text().collect::<String>())
                .unwrap_or_else(|| a.text().collect())
        } else {
            a.value()
                .attr("title")
                .or_else(|| a.value().attr("data-title"))
                .or_else(|| {
                    a.select(&Selector::parse("img").unwrap())
                        .next()
                        .and_then(|x| x.value().attr("alt"))
                })
                .unwrap_or("")
                .to_owned()
        };
        if name.trim().is_empty() {
            continue;
        }
        let id = a
            .ancestors()
            .filter_map(|n| n.value().as_element())
            .find_map(|e| e.attr("data-tip").or_else(|| e.attr("data-id")))
            .map(str::to_owned);
        out.push(Candidate {
            slug: slug.into(),
            name: name.trim().into(),
            id,
        });
    }
    out
}
async fn get_text(http: &Client, url: &str, referer: &str) -> Result<String, ProviderError> {
    let _permit = crate::rate_limit::acquire_url(url)
        .await
        .map_err(ProviderError::Unavailable)?;
    let r = headers(http.get(url), referer)
        .send()
        .await
        .map_err(req_error)?;
    let status = r.status();
    crate::rate_limit::observe_url(url, status, r.headers()).await;
    if status.as_u16() == 429 {
        let retry = crate::rate_limit::retry_after(r.headers()).map(|delay| delay.as_secs());
        return Err(ProviderError::RateLimited(retry));
    }
    if status.as_u16() == 404 {
        return Err(ProviderError::NotFound);
    }
    if !status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "HTTP status {}",
            status.as_u16()
        )));
    }
    r.text()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))
}
fn decode_native_id(raw: &str) -> Result<Candidate, ProviderError> {
    let candidate: Candidate = serde_json::from_str(raw)
        .map_err(|_| ProviderError::Parsing("Anikoto native identity is invalid".into()))?;
    if candidate.slug.trim().is_empty() || candidate.name.trim().is_empty() {
        return Err(ProviderError::Parsing(
            "Anikoto native identity is incomplete".into(),
        ));
    }
    Ok(candidate)
}
async fn native_id(http: &Client, candidate: &Candidate) -> Result<String, ProviderError> {
    if let Some(id) = candidate.id.as_deref().filter(|s| !s.trim().is_empty()) {
        return Ok(id.to_owned());
    }
    let page = get_text(
        http,
        &format!("{SITE}/watch/{}", candidate.slug),
        &format!("{SITE}/"),
    )
    .await?;
    let doc = Html::parse_document(&page);
    doc.select(&Selector::parse("#watch-main[data-id], [data-id]").unwrap())
        .find_map(|e| e.value().attr("data-id"))
        .map(str::to_owned)
        .ok_or_else(|| {
            ProviderError::Parsing("Anikoto native watch page lacks episode-list ID".into())
        })
}
async fn native_episode_html(
    http: &Client,
    candidate: &Candidate,
) -> Result<String, ProviderError> {
    let aid = native_id(http, candidate).await?;
    let url = format!("{SITE}/ajax/episode/list/{aid}");
    let observation_url = url.clone();
    let _permit = crate::rate_limit::acquire_url(&url)
        .await
        .map_err(ProviderError::Unavailable)?;
    let response = headers(http.get(url), &format!("{SITE}/watch/{}", candidate.slug))
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
        .map_err(req_error)?;
    let status = response.status();
    crate::rate_limit::observe_url(&observation_url, status, response.headers()).await;
    if status.as_u16() == 429 {
        let retry = crate::rate_limit::retry_after(response.headers()).map(|delay| delay.as_secs());
        return Err(ProviderError::RateLimited(retry));
    }
    if status.as_u16() == 404 {
        return Err(ProviderError::NotFound);
    }
    if !status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "Anikoto episode-list HTTP {}",
            status.as_u16()
        )));
    }
    let raw: Value = response
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))?;
    Ok(raw
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned())
}
fn episodes_from_html(
    html: &str,
    requested_season: usize,
) -> Vec<(usize, String, String, bool, bool, String)> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a[data-ids][data-num]").unwrap();
    let mut out = Vec::new();
    for e in doc.select(&sel) {
        let a = e.value();
        let n = a.attr("data-num").unwrap_or("").parse::<usize>().ok();
        let Some(n) = n else { continue };
        let ids = a.attr("data-ids").unwrap_or("");
        if ids.is_empty() {
            continue;
        }
        let text = e.text().collect::<String>().trim().to_owned();
        let sub = a.attr("data-sub").unwrap_or("1") == "1";
        let dub = a.attr("data-dub").unwrap_or("0") == "1";
        out.push((
            n,
            if text.is_empty() {
                format!("Episode {n}")
            } else {
                text
            },
            ids.into(),
            sub,
            dub,
            requested_season.to_string(),
        ));
    }
    out
}
/// Search Anikoto's own title index and retain its provider-native watch identity.
pub(crate) async fn search(http: &Client, query: &str) -> Result<Vec<CatalogItem>, ProviderError> {
    if query.trim().is_empty() {
        return Err(ProviderError::Parsing(
            "Anikoto search query is empty".into(),
        ));
    }
    let url = format!(
        "{SITE}/search?keyword={}",
        url::form_urlencoded::byte_serialize(query.trim().as_bytes()).collect::<String>()
    );
    let html = get_text(http, &url, &format!("{SITE}/")).await?;
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for candidate in candidate_html(&html, false) {
        if !seen.insert(candidate.slug.clone()) {
            continue;
        }
        let value =
            serde_json::to_string(&candidate).map_err(|e| ProviderError::Parsing(e.to_string()))?;
        out.push(CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::Anikoto,
                value,
            },
            title: candidate.name,
            media_type: MediaType::Series,
            year: None,
            poster_url: None,
            season_count: Some(1),
        });
    }
    if out.is_empty() {
        Err(ProviderError::NotFound)
    } else {
        Ok(out)
    }
}
/// Load episode rows from Anikoto's native watch/episode-list route.
pub(crate) async fn details(
    http: &Client,
    native_ref: &str,
) -> Result<MediaDetails, ProviderError> {
    let candidate = decode_native_id(native_ref)?;
    let html = native_episode_html(http, &candidate).await?;
    let s_no = season(&candidate.name);
    let rows = episodes_from_html(&html, s_no);
    let has_sub = rows.iter().any(|row| row.3);
    let has_dub = rows.iter().any(|row| row.4);
    let episodes = rows
        .into_iter()
        .filter_map(|(n, t, _, sub, dub, _)| {
            (sub || dub).then_some(Episode {
                season: s_no,
                number: n,
                title: Some(t),
                overview: None,
            })
        })
        .collect();
    Ok(MediaDetails {
        id: ProviderMediaId {
            provider: ProviderKind::Anikoto,
            value: native_ref.to_owned(),
        },
        title: candidate.name,
        media_type: MediaType::Series,
        year: None,
        description: None,
        tagline: None,
        imdb_rating: None,
        director: None,
        stars: None,
        prints: None,
        audios: match (has_sub, has_dub) {
            (true, true) => Some("sub; dub".into()),
            (true, false) => Some("sub".into()),
            (false, true) => Some("dub".into()),
            (false, false) => None,
        },
        poster_url: None,
        duration: None,
        genres: vec![],
        seasons: vec![Season {
            number: s_no,
            episodes,
        }],
        dubs: vec![],
    })
}
/// Resolve precisely the requested episode/audio via Anikoto AJAX and shared MegaPlay crypto/signing.
pub(crate) async fn resolve(
    http: &Client,
    native_ref: &str,
    requested_season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    let candidate = decode_native_id(native_ref)?;
    if requested_season != season(&candidate.name) {
        return Err(ProviderError::Unavailable(format!(
            "Anikoto cannot verify season {requested_season} from the native episode list"
        )));
    }
    let aid = native_id(http, &candidate).await?;
    let html = native_episode_html(http, &candidate).await?;
    let watch = format!("{SITE}/watch/{}", candidate.slug);
    let wanted = episodes_from_html(&html, requested_season)
        .into_iter()
        .find(|(n, _, _, _, _, s)| {
            *n == episode && s.parse::<usize>().ok() == Some(requested_season)
        })
        .ok_or_else(|| {
            ProviderError::Unavailable(format!(
                "Anikoto episode {requested_season}x{episode} unavailable"
            ))
        })?;
    // Prefer sub when present; preserve dub-only episodes rather than dropping them.
    let category = if wanted.3 {
        "sub"
    } else if wanted.4 {
        "dub"
    } else {
        return Err(ProviderError::Unavailable(
            "requested subtitle audio is unavailable".into(),
        ));
    };
    let sl_url = format!(
        "{SITE}/ajax/server/list?servers={}",
        url::form_urlencoded::byte_serialize(wanted.2.as_bytes()).collect::<String>()
    );
    let _permit = crate::rate_limit::acquire_url(&sl_url)
        .await
        .map_err(ProviderError::Unavailable)?;
    let list = headers(http.get(&sl_url), &watch)
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
        .map_err(req_error)?;
    let list_status = list.status();
    crate::rate_limit::observe_url(&sl_url, list_status, list.headers()).await;
    if list_status.as_u16() == 429 {
        let retry = crate::rate_limit::retry_after(list.headers()).map(|delay| delay.as_secs());
        return Err(ProviderError::RateLimited(retry));
    }
    if list_status.as_u16() == 404 {
        return Err(ProviderError::NotFound);
    }
    if !list_status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "Anikoto server-list HTTP {}",
            list_status.as_u16()
        )));
    }
    let list = list
        .json::<Value>()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))?;
    let server_html = list
        .pointer("/result")
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut servers = {
        let doc = Html::parse_document(server_html);
        let blocks = Selector::parse(".type[data-type]").unwrap();
        let rows = Selector::parse("li[data-sv-id][data-link-id]").unwrap();
        let mut servers = Vec::new();
        for b in doc.select(&blocks) {
            if b.value().attr("data-type").unwrap_or("").to_lowercase() != category {
                continue;
            }
            for li in b.select(&rows) {
                servers.push((
                    li.value().attr("data-link-id").unwrap_or("").to_string(),
                    li.text().collect::<String>().trim().to_string(),
                ));
            }
        }
        servers
    };
    servers.sort_by_key(|(_, name)| {
        if name.to_lowercase().contains("vidstream") {
            0
        } else {
            1
        }
    });
    if servers.is_empty() {
        return Err(ProviderError::Unavailable(format!(
            "Anikoto has no {category} servers for requested episode"
        )));
    }
    let mut last = None;
    for (link, name) in servers {
        let ep_url = format!(
            "{SITE}/ajax/server?get={}",
            url::form_urlencoded::byte_serialize(link.as_bytes()).collect::<String>()
        );
        let _permit = match crate::rate_limit::acquire_url(&ep_url).await {
            Ok(permit) => permit,
            Err(error) => {
                last = Some(ProviderError::Unavailable(error));
                continue;
            }
        };
        let response = match headers(http.get(&ep_url), &watch)
            .header("X-Requested-With", "XMLHttpRequest")
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                last = Some(req_error(e));
                continue;
            }
        };
        let status = response.status();
        crate::rate_limit::observe_url(&ep_url, status, response.headers()).await;
        if status.as_u16() == 429 {
            let retry =
                crate::rate_limit::retry_after(response.headers()).map(|delay| delay.as_secs());
            last = Some(ProviderError::RateLimited(retry));
            continue;
        }
        if status.as_u16() == 404 {
            last = Some(ProviderError::NotFound);
            continue;
        }
        if !status.is_success() {
            last = Some(ProviderError::Unavailable(format!(
                "Anikoto server HTTP {}",
                status.as_u16()
            )));
            continue;
        }
        let val = match response.json::<Value>().await {
            Ok(value) => value,
            Err(error) => {
                last = Some(ProviderError::Parsing(error.to_string()));
                continue;
            }
        };
        let embed = val
            .pointer("/result/url")
            .and_then(Value::as_str)
            .unwrap_or("");
        if embed.is_empty() {
            continue;
        }
        let resolved = match super::megaplay::resolve_embed(http, embed, &watch, UA).await {
            Ok(x) => x,
            Err(e) => {
                last = Some(e);
                continue;
            }
        };
        let mut subs = Vec::new();
        for t in &resolved.tracks {
            if t.kind.eq_ignore_ascii_case("thumbnails") {
                continue;
            }
            let u = match crate::net::validate_url_syntax(&t.file, false) {
                Ok(u) => u.to_string(),
                Err(_) => continue,
            };
            subs.push(SubtitleOption {
                name: if t.label.is_empty() {
                    "Subtitle".into()
                } else {
                    t.label.clone()
                },
                url: u,
            });
        }
        let stream = crate::net::validate_url_syntax(&resolved.stream_url, false)
            .map_err(|e| ProviderError::Parsing(format!("stream URL: {e}")))?
            .to_string();
        return Ok(vec![Release {
            provider: ProviderKind::Anikoto,
            filename: format!(
                "{} S{:02}E{:02} [{name}]",
                candidate.slug, requested_season, episode
            ),
            quality: Some("1080p".into()),
            codec: None,
            language: Some(category.into()),
            size_bytes: None,
            season: Some(requested_season),
            episode: Some(episode),
            mirrors: vec![SourceMirror {
                label: name,
                resolver_url: stream,
                headers: vec![("Referer".into(), resolved.embed_origin)],
                direct_file: false,
            }],
            subtitles: subs,
            resource_id: Some(format!("{}|{aid}|{category}|{}", candidate.slug, wanted.2)),
        }]);
    }
    Err(last.unwrap_or_else(|| ProviderError::Unavailable("all Anikoto servers failed".into())))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_identity_round_trips_slug_and_episode_list_id() {
        let candidate = Candidate {
            slug: "example-native-slug".into(),
            name: "Example".into(),
            id: Some("native-42".into()),
        };
        let encoded = serde_json::to_string(&candidate).unwrap();
        let decoded = decode_native_id(&encoded).unwrap();
        assert_eq!(decoded.slug, "example-native-slug");
        assert_eq!(decoded.id.as_deref(), Some("native-42"));
        assert!(decode_native_id("42").is_err());
    }
    #[test]
    fn parses_episode_audio_and_data_ids() {
        let x = episodes_from_html(
            r#"<a data-num="2" data-ids="77" data-sub="1" data-dub="0">2 Test</a><a data-num="3" data-ids="78" data-sub="0" data-dub="1">3 Dub only</a>"#,
            1,
        );
        assert_eq!(x.len(), 2);
        assert_eq!(x[0].0, 2);
        assert_eq!(x[0].2, "77");
        assert!(x[0].3);
        assert!(!x[0].4);
        assert_eq!(x[1].2, "78");
        assert!(!x[1].3);
        assert!(x[1].4);
    }
    #[test]
    fn parses_watch_search_fixture() {
        let x = candidate_html(
            r#"<div class="ani poster tip" data-tip="42"><a href="/watch/example"><img alt="Example"></a></div>"#,
            false,
        );
        assert_eq!(x[0].slug, "example");
        assert_eq!(x[0].id.as_deref(), Some("42"));
    }

    #[tokio::test]
    async fn resolve_rejects_unverified_requested_season_before_network_access() {
        let candidate = Candidate {
            slug: "native-show".into(),
            name: "Native Show".into(),
            id: Some("42".into()),
        };
        let identity = serde_json::to_string(&candidate).unwrap();
        let error = resolve(&Client::new(), &identity, 2, 1).await.unwrap_err();
        assert!(matches!(error, ProviderError::Unavailable(_)));
    }
}
