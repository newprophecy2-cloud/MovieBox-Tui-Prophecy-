use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId,
    Release, Season, SourceMirror, SubtitleOption,
};
use reqwest::{
    Client,
    header::{ACCEPT, REFERER, USER_AGENT},
};
use scraper::{Html, Selector};
use serde_json::{Value, json};
use std::collections::HashSet;

const SITE: &str = "https://anikototv.to";
const ANILIST: &str = "https://graphql.anilist.co";
const GQL: &str = r#"query ($search: String, $id: Int) { Page(page:1,perPage:20) { media(search:$search,type:ANIME) { id idMal format episodes seasonYear title { english romaji native } synonyms description(asHtml:false) coverImage { large } genres } } Media(id:$id,type:ANIME) { id idMal format episodes seasonYear title { english romaji native } synonyms description(asHtml:false) coverImage { large } genres } }"#;
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

fn req_error(e: reqwest::Error) -> ProviderError {
    ProviderError::Network(e.to_string())
}
fn titles(m: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for s in ["english", "romaji", "native"]
        .into_iter()
        .filter_map(|k| m.get("title")?.get(k)?.as_str())
        .chain(
            m.get("synonyms")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str),
        )
    {
        let s = s.trim();
        if !s.is_empty() && seen.insert(s.to_lowercase()) {
            out.push(s.to_owned());
        }
    }
    out
}
fn title(m: &Value) -> String {
    titles(m).into_iter().next().unwrap_or_default()
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
fn canonical(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .filter(|c| c.is_alphanumeric())
        .collect()
}
fn score(name: &str, candidates: &[String]) -> usize {
    let n = canonical(name);
    let ns = season(name);
    let mut best = 0;
    for t in candidates {
        let c = canonical(t);
        let ts = season(t);
        if (ns > 1 && ts != ns) || (ts > 1 && ns != ts) {
            continue;
        }
        if n == c {
            return 1000;
        }
        if n.starts_with(&c) {
            best = best.max(600usize.saturating_sub(n.len() - c.len()));
        } else if c.starts_with(&n) {
            best = best.max(500usize.saturating_sub(c.len() - n.len()));
        } else if n.contains(&c) {
            best = best.max(300);
        }
    }
    best
}
fn media_from(v: &Value) -> Result<MediaDetails, ProviderError> {
    let id = v
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ProviderError::Parsing("AniList media has no ID".into()))?
        .to_string();
    let name = title(v);
    if name.is_empty() {
        return Err(ProviderError::Parsing("AniList media has no title".into()));
    }
    let count = v
        .get("episodes")
        .and_then(Value::as_u64)
        .unwrap_or(1)
        .max(1) as usize;
    let eps = (1..=count)
        .flat_map(|n| {
            [
                Episode {
                    season: 1,
                    number: n,
                    title: Some(format!("Episode {n}")),
                    overview: None,
                },
                Episode {
                    season: 1,
                    number: n,
                    title: Some(format!("Episode {n} (Dub)")),
                    overview: None,
                },
            ]
        })
        .collect();
    Ok(MediaDetails {
        id: ProviderMediaId {
            provider: ProviderKind::Anikoto,
            value: id,
        },
        title: name,
        media_type: if v.pointer("/format").and_then(Value::as_str) == Some("MOVIE") {
            MediaType::Movie
        } else {
            MediaType::Series
        },
        year: v
            .get("seasonYear")
            .and_then(Value::as_i64)
            .map(|x| x.to_string()),
        description: v
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        tagline: None,
        imdb_rating: None,
        director: None,
        stars: None,
        prints: None,
        audios: Some("sub,dub".into()),
        poster_url: v
            .pointer("/coverImage/large")
            .and_then(Value::as_str)
            .map(str::to_owned),
        duration: None,
        genres: v
            .get("genres")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        seasons: vec![Season {
            number: 1,
            episodes: eps,
        }],
        dubs: vec![],
    })
}
async fn gql(http: &Client, search: Option<&str>, id: Option<i64>) -> Result<Value, ProviderError> {
    let r = http
        .post(ANILIST)
        .header(ACCEPT, "application/json")
        .header(USER_AGENT, UA)
        .json(&json!({"query":GQL,"variables":{"search":search,"id":id}}))
        .send()
        .await
        .map_err(req_error)?;
    if !r.status().is_success() {
        return Err(ProviderError::Unavailable(format!(
            "AniList HTTP status {}",
            r.status().as_u16()
        )));
    }
    let v: Value = r
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))?;
    if v.get("errors").is_some() {
        return Err(ProviderError::Unavailable(
            "AniList returned an error".into(),
        ));
    }
    Ok(v)
}
fn headers(b: reqwest::RequestBuilder, referer: &str) -> reqwest::RequestBuilder {
    b.header(USER_AGENT, UA)
        .header(ACCEPT, "application/json, text/html, */*")
        .header(REFERER, referer)
}
fn slug_candidates(ts: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for t in ts {
        let lower = t.to_lowercase();
        let clean: String = lower
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let parts: Vec<_> = clean.split('-').filter(|s| !s.is_empty()).collect();
        for end in [parts.len(), parts.len().saturating_sub(1)] {
            if end == 0 {
                continue;
            }
            let s = parts[..end].join("-");
            if seen.insert(s.clone()) {
                out.push(s);
            }
        }
    }
    out
}
#[derive(Clone, Debug)]
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
    let r = headers(http.get(url), referer)
        .send()
        .await
        .map_err(req_error)?;
    if !r.status().is_success() {
        return Err(ProviderError::Unavailable(format!(
            "HTTP status {}",
            r.status().as_u16()
        )));
    }
    r.text()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))
}
async fn candidate_for(
    http: &Client,
    id: &str,
) -> Result<(String, String, Vec<String>), ProviderError> {
    let n = id
        .parse::<i64>()
        .map_err(|_| ProviderError::Parsing("invalid AniList ID".into()))?;
    let v = gql(http, None, Some(n)).await?;
    let m = v.pointer("data/Media").ok_or(ProviderError::NotFound)?;
    let ts = titles(m);
    if ts.is_empty() {
        return Err(ProviderError::NotFound);
    }
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    for slug in slug_candidates(&ts) {
        if !seen.insert(slug.clone()) {
            continue;
        }
        let url = format!("{SITE}/watch/{slug}");
        if let Ok(page) = get_text(http, &url, &format!("{SITE}/")).await {
            let doc = Html::parse_document(&page);
            let id_sel = Selector::parse("#watch-main[data-id], [data-id]").unwrap();
            let aid = doc
                .select(&id_sel)
                .find_map(|e| e.value().attr("data-id"))
                .map(str::to_owned);
            if aid.is_some() {
                let name = doc
                    .select(&Selector::parse("h1").unwrap())
                    .next()
                    .map(|e| e.text().collect::<String>())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| slug.replace('-', " "));
                candidates.push(Candidate {
                    slug,
                    name,
                    id: aid,
                });
            }
        }
    }
    let qs: Vec<String> = ts
        .iter()
        .take(3)
        .map(|t| {
            t.chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == ' ' {
                        c
                    } else {
                        ' '
                    }
                })
                .collect::<String>()
                .trim()
                .to_owned()
        })
        .filter(|s| !s.is_empty())
        .collect();
    for q in &qs {
        let url = format!(
            "{SITE}/search?keyword={}",
            url::form_urlencoded::byte_serialize(q.as_bytes()).collect::<String>()
        );
        if let Ok(html) = get_text(http, &url, &format!("{SITE}/")).await {
            candidates.extend(candidate_html(&html, false));
        }
    }
    let mut best = 0;
    for c in &candidates {
        best = best.max(score(&c.name, &ts));
    }
    if best == 0 {
        for q in &qs {
            let url = format!(
                "{SITE}/ajax/anime/search?keyword={}",
                url::form_urlencoded::byte_serialize(q.as_bytes()).collect::<String>()
            );
            if let Ok(t) = headers(http.get(url), &format!("{SITE}/"))
                .header("X-Requested-With", "XMLHttpRequest")
                .send()
                .await
                .map_err(req_error)?
                .text()
                .await
            {
                let parsed: Value = serde_json::from_str(&t).unwrap_or(Value::Null);
                if let Some(h) = parsed.pointer("/result/html").and_then(Value::as_str) {
                    candidates.extend(candidate_html(h, true));
                }
            }
        }
    }
    let candidate = candidates
        .into_iter()
        .map(|c| (score(&c.name, &ts), c))
        .filter(|(s, _)| *s > 0)
        .max_by_key(|(s, _)| *s)
        .map(|(_, c)| c)
        .ok_or_else(|| ProviderError::NotFound)?;
    let aid = if let Some(i) = candidate.id {
        i
    } else {
        let page = get_text(
            http,
            &format!("{SITE}/watch/{}", candidate.slug),
            &format!("{SITE}/"),
        )
        .await?;
        let doc = Html::parse_document(&page);
        doc.select(&Selector::parse("#watch-main[data-id], [data-id]").unwrap())
            .find_map(|e| e.value().attr("data-id"))
            .ok_or_else(|| ProviderError::Parsing("Anikoto watch page lacks anime ID".into()))?
            .to_string()
    };
    Ok((aid, candidate.slug, ts))
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
/// Search AniList, retaining AniList IDs as the provider's stable playback identity.
pub(crate) async fn search(http: &Client, query: &str) -> Result<Vec<CatalogItem>, ProviderError> {
    let v = gql(http, Some(query), None).await?;
    let arr = v
        .pointer("data/Page/media")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderError::Parsing("AniList search response missing media".into()))?;
    let mut out = Vec::new();
    for m in arr {
        let d = media_from(m)?;
        out.push(CatalogItem {
            id: d.id,
            title: d.title,
            media_type: d.media_type,
            year: d.year,
            poster_url: d.poster_url,
            season_count: Some(1),
        });
    }
    if out.is_empty() {
        Err(ProviderError::NotFound)
    } else {
        Ok(out)
    }
}
/// Load AniList details and replace synthetic episodes with Anikoto's actual episode/audio rows when available.
pub(crate) async fn details(
    http: &Client,
    anilist_id: &str,
) -> Result<MediaDetails, ProviderError> {
    let id = anilist_id
        .parse::<i64>()
        .map_err(|_| ProviderError::Parsing("invalid AniList ID".into()))?;
    let v = gql(http, None, Some(id)).await?;
    let m = v.pointer("data/Media").ok_or(ProviderError::NotFound)?;
    let mut d = media_from(m)?;
    let (aid, slug, _) = candidate_for(http, anilist_id).await?;
    let url = format!("{SITE}/ajax/episode/list/{aid}");
    let raw = headers(http.get(url), &format!("{SITE}/watch/{slug}"))
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
        .map_err(req_error)?
        .json::<Value>()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))?;
    let html = raw.get("result").and_then(Value::as_str).unwrap_or("");
    let eps: Vec<Episode> = episodes_from_html(html, season(&d.title))
        .into_iter()
        .flat_map(|(n, t, _, sub, dub, _)| {
            let mut e = Vec::new();
            if sub {
                e.push(Episode {
                    season: season(&d.title),
                    number: n,
                    title: Some(t.clone()),
                    overview: None,
                });
            }
            if dub {
                e.push(Episode {
                    season: season(&d.title),
                    number: n,
                    title: Some(format!("{t} (Dub)")),
                    overview: None,
                });
            }
            e
        })
        .collect();
    if !eps.is_empty() {
        d.seasons = vec![Season {
            number: season(&d.title),
            episodes: eps,
        }];
    }
    Ok(d)
}
/// Resolve precisely the requested episode/audio via Anikoto AJAX and shared MegaPlay crypto/signing.
pub(crate) async fn resolve(
    http: &Client,
    anilist_id: &str,
    requested_season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    let (aid, slug, _) = candidate_for(http, anilist_id).await?;
    let watch = format!("{SITE}/watch/{slug}");
    let url = format!("{SITE}/ajax/episode/list/{aid}");
    let raw = headers(http.get(url), &watch)
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
        .map_err(req_error)?
        .json::<Value>()
        .await
        .map_err(|e| ProviderError::Parsing(e.to_string()))?;
    let html = raw.get("result").and_then(Value::as_str).unwrap_or("");
    let wanted = episodes_from_html(html, requested_season)
        .into_iter()
        .find(|(n, _, _, _, _, s)| {
            *n == episode && s.parse::<usize>().ok() == Some(requested_season)
        })
        .ok_or_else(|| {
            ProviderError::Unavailable(format!(
                "Anikoto episode {requested_season}x{episode} unavailable"
            ))
        })?;
    // Use only the requested audio. Existing Release API does not carry audio as a separate selector; provider uses sub by default.
    let category = if wanted.3 {
        "sub"
    } else if wanted.4 {
        return Err(ProviderError::Unavailable("requested episode is dub-only; audio selection is not represented in resolve arguments".into()));
    } else {
        return Err(ProviderError::Unavailable(
            "requested subtitle audio is unavailable".into(),
        ));
    };
    let sl_url = format!(
        "{SITE}/ajax/server/list?servers={}",
        url::form_urlencoded::byte_serialize(wanted.2.as_bytes()).collect::<String>()
    );
    let list = headers(http.get(sl_url), &watch)
        .header("X-Requested-With", "XMLHttpRequest")
        .send()
        .await
        .map_err(req_error)?
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
        let val = match headers(http.get(ep_url), &watch)
            .header("X-Requested-With", "XMLHttpRequest")
            .send()
            .await
        {
            Ok(r) => match r.json::<Value>().await {
                Ok(v) => v,
                Err(e) => {
                    last = Some(ProviderError::Parsing(e.to_string()));
                    continue;
                }
            },
            Err(e) => {
                last = Some(req_error(e));
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
            filename: format!("{} S{:02}E{:02} [{name}]", slug, requested_season, episode),
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
            resource_id: Some(format!("{anilist_id}|{slug}|{aid}|{category}|{}", wanted.2)),
        }]);
    }
    Err(last.unwrap_or_else(|| ProviderError::Unavailable("all Anikoto servers failed".into())))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn title_match_rejects_wrong_season_and_similar_unrelated() {
        assert_eq!(score("Example Season 2", &["Example".into()]), 0);
        assert_eq!(score("Example", &["Example".into()]), 1000);
        assert_eq!(score("Unrelated", &["Example".into()]), 0);
    }
    #[test]
    fn parses_episode_audio_and_data_ids() {
        let x = episodes_from_html(
            r#"<a data-num="2" data-ids="77" data-sub="1" data-dub="0">2 Test</a>"#,
            1,
        );
        assert_eq!(x[0].0, 2);
        assert_eq!(x[0].2, "77");
        assert!(x[0].3);
        assert!(!x[0].4);
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
}
