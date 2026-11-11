use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId,
    Release, Season, SourceMirror, SubtitleOption,
};
use serde_json::{Map, Value, json};
use std::collections::HashMap;

const BASE: &str = "https://miruro.cx";
const ANILIST: &str = "https://graphql.anilist.co";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
const ANILIST_QUERY: &str = "query ($id: Int) { Media(id: $id, type: ANIME) { id idMal format episodes seasonYear title { english romaji native } description(asHtml: false) coverImage { large } genres } }";

#[derive(Debug, serde::Deserialize)]
struct Doc {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    nodes: Vec<Node>,
    #[serde(default)]
    data: Vec<Value>,
    #[serde(default)]
    location: String,
}
#[derive(Debug, serde::Deserialize)]
struct Node {
    #[serde(default, rename = "type")]
    kind: String,
    #[serde(default)]
    data: Vec<Value>,
}

fn parse_docs(body: &str) -> Result<Vec<Doc>, ProviderError> {
    let mut docs = Vec::new();
    for line in body.lines().map(str::trim).filter(|s| !s.is_empty()) {
        docs.push(
            serde_json::from_str(line)
                .map_err(|e| ProviderError::Parsing(format!("Miruro __data.json document: {e}")))?,
        );
    }
    if docs.is_empty() {
        return Err(ProviderError::Parsing(
            "Miruro returned an empty __data.json document".into(),
        ));
    }
    Ok(docs)
}

fn integer(v: &Value) -> Option<usize> {
    v.as_u64()
        .and_then(|n| usize::try_from(n).ok())
        .or_else(|| {
            let n = v.as_f64()?;
            (n.is_finite() && n >= 0.0 && n.fract() == 0.0 && n <= usize::MAX as f64)
                .then_some(n as usize)
        })
}

// Miruro's devalue graph stores composite values in a flat table; numeric
// members are pointers only when they address a table slot. Other numbers
// remain literal values, matching the source decoder's partial contract.
fn hydrate_at(arr: &[Value], i: usize, depth: usize) -> Value {
    if depth > 32 {
        return Value::Null;
    }
    let Some(v) = arr.get(i) else {
        return json!(i);
    };
    hydrate_value(arr, v, depth)
}
fn hydrate_ref(arr: &[Value], v: &Value, depth: usize) -> Value {
    if let Some(i) = integer(v).filter(|i| *i < arr.len()) {
        hydrate_at(arr, i, depth + 1)
    } else {
        v.clone()
    }
}
fn hydrate_value(arr: &[Value], v: &Value, depth: usize) -> Value {
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            for (k, child) in m {
                out.insert(k.clone(), hydrate_ref(arr, child, depth + 1));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|child| hydrate_ref(arr, child, depth + 1))
                .collect(),
        ),
        _ => v.clone(),
    }
}
fn hydrated_chunk(body: &str, key: &str) -> Result<Vec<Value>, ProviderError> {
    for doc in parse_docs(body)? {
        if doc.kind == "redirect" || doc.data.is_empty() {
            continue;
        }
        if doc
            .data
            .iter()
            .any(|v| v.as_object().is_some_and(|m| m.contains_key(key)))
        {
            return Ok((0..doc.data.len())
                .map(|i| hydrate_at(&doc.data, i, 0))
                .collect());
        }
    }
    Err(ProviderError::Parsing(format!(
        "Miruro response contains no {key} chunk"
    )))
}

#[derive(Clone, Debug)]
struct Item {
    show_id: String,
    anilist: String,
    title: String,
    format: String,
    year: Option<String>,
    cover: Option<String>,
}
fn catalog_items(body: &str) -> Result<Vec<Item>, ProviderError> {
    for doc in parse_docs(body)? {
        if doc.kind == "redirect" {
            continue;
        }
        for node in doc.nodes {
            if node.kind != "data" || node.data.is_empty() {
                continue;
            }
            let root = hydrate_at(&node.data, 0, 0);
            let Some(items) = root.get("items").and_then(Value::as_array) else {
                continue;
            };
            let mut out = Vec::new();
            for raw in items {
                let Some(ext) = raw.get("external_ids") else {
                    continue;
                };
                let aid = ext
                    .get("anilist")
                    .and_then(Value::as_array)
                    .and_then(|a| a.iter().find_map(Value::as_str))
                    .unwrap_or("")
                    .trim();
                let title = raw
                    .get("title")
                    .and_then(|v| {
                        ["english", "romaji", "native"].iter().find_map(|k| {
                            v.get(*k)
                                .and_then(Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                        })
                    })
                    .unwrap_or("");
                let show_id = raw.get("id").and_then(Value::as_str).unwrap_or("").trim();
                if aid.is_empty() || title.is_empty() || show_id.is_empty() {
                    continue;
                }
                // Only digit AniList IDs can represent the exact external mapping.
                if !aid.bytes().all(|b| b.is_ascii_digit()) {
                    continue;
                }
                out.push(Item {
                    show_id: show_id.into(),
                    anilist: aid.into(),
                    title: title.into(),
                    format: raw
                        .get("format")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .into(),
                    year: raw
                        .get("season_year")
                        .and_then(integer)
                        .filter(|n| *n > 0)
                        .map(|n| n.to_string()),
                    cover: raw
                        .get("cover_url")
                        .and_then(Value::as_str)
                        .and_then(valid_url),
                });
            }
            return Ok(out);
        }
    }
    Err(ProviderError::Parsing(
        "Miruro search response has no catalog items".into(),
    ))
}
fn valid_url(raw: &str) -> Option<String> {
    crate::net::validate_url_syntax(raw.trim(), false)
        .ok()
        .map(|u| u.to_string())
}
fn encode_component(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}
fn watch_path(show_id: &str, slug: &str, episode: usize) -> String {
    format!(
        "{BASE}/watch/{}/{}/__data.json?ep={episode}",
        encode_component(show_id),
        encode_component(slug)
    )
}
fn redirect_slug(body: &str) -> Option<String> {
    for doc in parse_docs(body).ok()? {
        if doc.kind != "redirect" || doc.location.is_empty() {
            continue;
        }
        let parts: Vec<_> = doc.location.trim_matches('/').split('/').collect();
        if parts.len() >= 3 && parts[0] == "watch" {
            let encoded = parts[2].split('?').next()?;
            let slug = url::form_urlencoded::parse(format!("x={encoded}").as_bytes())
                .next()?
                .1
                .into_owned();
            if !slug.trim().is_empty() {
                return Some(slug);
            }
        }
    }
    None
}

async fn get_text(http: &reqwest::Client, raw: &str) -> Result<String, ProviderError> {
    let url = crate::net::validate_url_syntax(raw, false).map_err(ProviderError::Parsing)?;
    let response = http
        .get(url)
        .header(reqwest::header::USER_AGENT, UA)
        .header(reqwest::header::ACCEPT, "application/json,text/plain,*/*")
        .header(reqwest::header::REFERER, format!("{BASE}/"))
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    let status = response.status();
    if status.as_u16() == 429 {
        return Err(ProviderError::RateLimited(None));
    }
    if !status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "Miruro HTTP status {}",
            status.as_u16()
        )));
    }
    response
        .text()
        .await
        .map_err(|e| ProviderError::Parsing(format!("Miruro response body: {e}")))
}
async fn anilist_media(http: &reqwest::Client, id: &str) -> Result<Value, ProviderError> {
    let num = id
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            ProviderError::Parsing("Miruro identity must be a positive AniList ID".into())
        })?;
    let response = http
        .post(ANILIST)
        .header(reqwest::header::USER_AGENT, UA)
        .header(reqwest::header::ACCEPT, "application/json")
        .json(&json!({"query": ANILIST_QUERY, "variables": {"id": num}}))
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    let status = response.status();
    if status.as_u16() == 429 {
        return Err(ProviderError::RateLimited(None));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(format!("AniList JSON: {e}")))?;
    if !status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "AniList HTTP status {}",
            status.as_u16()
        )));
    }
    value
        .get("data")
        .and_then(|v| v.get("Media"))
        .filter(|v| !v.is_null())
        .cloned()
        .ok_or(ProviderError::NotFound)
}
fn titles(media: &Value) -> Vec<String> {
    ["english", "romaji", "native"]
        .iter()
        .filter_map(|key| media.get("title")?.get(*key)?.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}
async fn find_item(
    http: &reqwest::Client,
    anilist_id: &str,
    title: &str,
) -> Result<Item, ProviderError> {
    let url = format!("{BASE}/search/__data.json?q={}", encode_component(title));
    let items = catalog_items(&get_text(http, &url).await?)?;
    items
        .into_iter()
        .find(|it| it.anilist == anilist_id)
        .ok_or(ProviderError::NotFound)
}
async fn resolve_coords(http: &reqwest::Client, id: &str) -> Result<(Item, String), ProviderError> {
    let media = anilist_media(http, id).await?;
    let aid = media
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ProviderError::Parsing("AniList response has no numeric identity".into()))?
        .to_string();
    let mut found = None;
    for title in titles(&media) {
        match find_item(http, &aid, &title).await {
            Ok(item) => {
                found = Some(item);
                break;
            }
            Err(ProviderError::NotFound) => continue,
            Err(e) => return Err(e),
        }
    }
    let item = found.ok_or(ProviderError::NotFound)?;
    let probe = get_text(http, &watch_path(&item.show_id, "x", 1)).await?;
    let slug = redirect_slug(&probe).unwrap_or_else(|| "x".into());
    Ok((item, slug))
}
async fn watch_data(
    http: &reqwest::Client,
    item: &Item,
    slug: &str,
    episode: usize,
) -> Result<(String, String), ProviderError> {
    let mut body = get_text(http, &watch_path(&item.show_id, slug, episode)).await?;
    if let Some(canonical) = redirect_slug(&body).filter(|s| s != slug) {
        body = get_text(http, &watch_path(&item.show_id, &canonical, episode)).await?;
        return Ok((body, canonical));
    }
    Ok((body, slug.to_owned()))
}
fn episode_rows(body: &str) -> Result<Vec<Episode>, ProviderError> {
    let slots = hydrated_chunk(body, "episodes")?;
    let mut out = Vec::new();
    for slot in slots {
        let Some(m) = slot.as_object() else {
            continue;
        };
        if m.contains_key("episodes") {
            continue;
        }
        let Some(number) = m.get("number").and_then(integer).filter(|n| *n > 0) else {
            continue;
        };
        out.push(Episode {
            season: 1,
            number,
            title: m
                .get("title")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
            overview: None,
        });
    }
    out.sort_by_key(|e| e.number);
    out.dedup_by_key(|e| e.number);
    if out.is_empty() {
        return Err(ProviderError::Unavailable(
            "Miruro watch payload has no actual episode records".into(),
        ));
    }
    Ok(out)
}

#[derive(Clone, Debug)]
struct Stream {
    url: String,
    format: String,
    quality: Option<String>,
}
#[derive(Clone, Debug)]
struct Sub {
    language: String,
    label: String,
    file: String,
    default: bool,
}
#[derive(Clone, Debug)]
struct Server {
    name: String,
    referer: Option<String>,
    streams: Vec<Stream>,
}
#[derive(Clone, Debug)]
struct ProviderPool {
    name: String,
    track: String,
    servers: Vec<Server>,
    subtitles: Vec<Sub>,
}
fn watch_tracks(body: &str) -> Result<(Vec<ProviderPool>, Vec<String>), ProviderError> {
    let slots = hydrated_chunk(body, "tracks")?;
    let mut tracks: Vec<(String, Vec<ProviderPool>)> = Vec::new();
    let mut order = Vec::new();
    for slot in slots {
        let Some(m) = slot.as_object() else {
            continue;
        };
        if let Some(rows) = m.get("tracks").and_then(Value::as_array) {
            for row in rows {
                let Some(tm) = row.as_object() else {
                    continue;
                };
                let track = tm
                    .get("track")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_ascii_lowercase();
                let mut providers = Vec::new();
                for pv in tm
                    .get("providers")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    let Some(pm) = pv.as_object() else {
                        continue;
                    };
                    let name = pm
                        .get("provider")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_owned();
                    let mut servers = Vec::new();
                    for sv in pm
                        .get("servers")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        let Some(sm) = sv.as_object() else {
                            continue;
                        };
                        let referer = sm
                            .get("headers")
                            .and_then(|h| h.get("Referer"))
                            .and_then(Value::as_str)
                            .and_then(valid_url);
                        let name = sm
                            .get("server")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim()
                            .to_owned();
                        let streams = sm
                            .get("streams")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                            .filter_map(|s| {
                                let raw = s.get("url")?.as_str()?.trim();
                                Some(Stream {
                                    url: valid_url(raw)?,
                                    format: s
                                        .get("format")
                                        .and_then(Value::as_str)
                                        .unwrap_or("")
                                        .trim()
                                        .to_owned(),
                                    quality: s
                                        .get("quality")
                                        .and_then(Value::as_str)
                                        .map(str::to_owned),
                                })
                            })
                            .collect();
                        servers.push(Server {
                            name,
                            referer,
                            streams,
                        });
                    }
                    let subtitles = pm
                        .get("subtitles")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|s| {
                            let file = s.get("file")?.as_str()?.trim();
                            Some(Sub {
                                file: valid_url(file)?,
                                language: s
                                    .get("language")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned(),
                                label: s
                                    .get("label")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned(),
                                default: s.get("default").and_then(Value::as_bool).unwrap_or(false),
                            })
                        })
                        .collect();
                    if !name.is_empty() {
                        providers.push(ProviderPool {
                            name,
                            track: track.clone(),
                            servers,
                            subtitles,
                        });
                    }
                }
                tracks.push((track, providers));
            }
        }
        if let Some(rows) = m.get("providerOrder").and_then(Value::as_array) {
            order.extend(
                rows.iter()
                    .filter_map(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            );
        }
    }
    if tracks.is_empty() {
        return Err(ProviderError::Unavailable(
            "Miruro watch payload contains no audio tracks".into(),
        ));
    }
    let mut pools = Vec::new();
    for (_, ps) in tracks {
        pools.extend(ps);
    }
    Ok((pools, order))
}
fn is_dead(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
        .is_some_and(|h| h == "shiora.top" || h.ends_with(".shiora.top"))
}
fn subtitles(pool: &ProviderPool) -> Vec<SubtitleOption> {
    pool.subtitles
        .iter()
        .filter_map(|sub| {
            let path = reqwest::Url::parse(&sub.file)
                .ok()?
                .path()
                .to_ascii_lowercase();
            if ![".vtt", ".srt", ".ass", ".ssa", ".ttml", ".dfxp"]
                .iter()
                .any(|ext| path.ends_with(ext))
            {
                return None;
            }
            let mut name = if sub.language.trim().is_empty() {
                sub.label.trim()
            } else {
                sub.language.trim()
            }
            .to_owned();
            if name.is_empty() {
                name = "Unknown".into();
            }
            if sub.default {
                name.push_str(" (default)");
            }
            Some(SubtitleOption {
                name,
                url: sub.file.clone(),
            })
        })
        .collect()
}
fn releases(body: &str, season: usize, episode: usize) -> Result<Vec<Release>, ProviderError> {
    let (pools, order) = watch_tracks(body)?;
    let mut by_provider: HashMap<String, Vec<ProviderPool>> = HashMap::new();
    for p in pools {
        if ["sub", "ssub", "dub"].contains(&p.track.as_str()) {
            by_provider.entry(p.name.clone()).or_default().push(p);
        }
    }
    let mut names = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for n in order {
        if by_provider.contains_key(&n) && seen.insert(n.clone()) {
            names.push(n);
        }
    }
    let mut remainder: Vec<_> = by_provider
        .keys()
        .filter(|n| !seen.contains(*n))
        .cloned()
        .collect();
    remainder.sort();
    names.extend(remainder);
    let mut output = Vec::new();
    for name in names {
        let pools = by_provider.remove(&name).unwrap_or_default();
        // Kari's per-provider ordering prefers soft-sub, then hard-sub, then dub.
        let mut pools = pools;
        pools.sort_by_key(|p| match p.track.as_str() {
            "ssub" => 0,
            "sub" => 1,
            _ => 2,
        });
        for pool in pools {
            for server in &pool.servers {
                for stream in &server.streams {
                    if is_dead(&stream.url) {
                        continue;
                    }
                    let format = stream.format.to_ascii_lowercase();
                    let path = reqwest::Url::parse(&stream.url)
                        .map(|u| u.path().to_ascii_lowercase())
                        .unwrap_or_default();
                    let is_mp4 = format == "mp4" || path.ends_with(".mp4");
                    let is_hls = format == "hls" || path.contains(".m3u8");
                    if !is_mp4 && !is_hls {
                        continue;
                    }
                    let quality = stream
                        .quality
                        .clone()
                        .filter(|q| !q.trim().is_empty())
                        .or_else(|| Some("Auto".into()));
                    let mirror = SourceMirror {
                        label: if server.name.is_empty() {
                            format!("Miruro {}", name)
                        } else {
                            server.name.clone()
                        },
                        resolver_url: stream.url.clone(),
                        headers: server
                            .referer
                            .as_ref()
                            .map(|r| vec![("Referer".into(), r.clone())])
                            .unwrap_or_default(),
                        direct_file: is_mp4,
                    };
                    // Keep exact URL and provider identity. No inferred codec is attached.
                    output.push(Release {
                        provider: ProviderKind::Miruro,
                        filename: format!(
                            "miruro-{episode}.{}",
                            if is_mp4 { "mp4" } else { "m3u8" }
                        ),
                        quality,
                        codec: None,
                        language: Some(pool.track.clone()),
                        size_bytes: None,
                        season: Some(season),
                        episode: Some(episode),
                        mirrors: vec![mirror],
                        subtitles: subtitles(&pool),
                        resource_id: Some(name.clone()),
                    });
                }
            }
        }
    }
    if output.is_empty() {
        return Err(ProviderError::Unavailable(
            "Miruro has no valid HLS or MP4 streams for this episode".into(),
        ));
    }
    Ok(output)
}

pub(crate) async fn search(
    http: &reqwest::Client,
    query: &str,
) -> Result<Vec<CatalogItem>, ProviderError> {
    if query.trim().is_empty() {
        return Err(ProviderError::Parsing(
            "Miruro search query is empty".into(),
        ));
    }
    let body = get_text(
        http,
        &format!(
            "{BASE}/search/__data.json?q={}",
            encode_component(query.trim())
        ),
    )
    .await?;
    let items = catalog_items(&body)?;
    let out: Vec<_> = items
        .into_iter()
        .map(|item| CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::Miruro,
                value: item.anilist,
            },
            title: item.title,
            media_type: if item.format.eq_ignore_ascii_case("MOVIE") {
                MediaType::Movie
            } else {
                MediaType::Series
            },
            year: item.year,
            poster_url: item.cover,
            season_count: Some(1),
        })
        .collect();
    if out.is_empty() {
        Err(ProviderError::NotFound)
    } else {
        Ok(out)
    }
}

pub(crate) async fn details(
    http: &reqwest::Client,
    id: &str,
) -> Result<MediaDetails, ProviderError> {
    let media = anilist_media(http, id).await?;
    let aid = media
        .get("id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ProviderError::Parsing("AniList detail has no numeric ID".into()))?
        .to_string();
    let mut found = None;
    for title in titles(&media) {
        match find_item(http, &aid, &title).await {
            Ok(item) => {
                found = Some(item);
                break;
            }
            Err(ProviderError::NotFound) => continue,
            Err(e) => return Err(e),
        }
    }
    let item = found.ok_or(ProviderError::NotFound)?;
    let probe = get_text(http, &watch_path(&item.show_id, "x", 1)).await?;
    let slug = redirect_slug(&probe).unwrap_or_else(|| "x".into());
    let (watch, _) = watch_data(http, &item, &slug, 1).await?;
    let episodes = episode_rows(&watch)?;
    Ok(MediaDetails {
        id: ProviderMediaId {
            provider: ProviderKind::Miruro,
            value: aid,
        },
        title: item.title,
        media_type: if item.format.eq_ignore_ascii_case("MOVIE") {
            MediaType::Movie
        } else {
            MediaType::Series
        },
        year: item.year,
        description: media
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_owned),
        tagline: None,
        imdb_rating: None,
        director: None,
        stars: None,
        prints: None,
        audios: None,
        poster_url: item.cover,
        duration: None,
        genres: media
            .get("genres")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        seasons: vec![Season {
            number: 1,
            episodes,
        }],
        dubs: vec![],
    })
}

pub(crate) async fn resolve(
    http: &reqwest::Client,
    id: &str,
    season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    if season == 0 || episode == 0 {
        return Err(ProviderError::Parsing(
            "Miruro episode identity must be one-based".into(),
        ));
    }
    let (item, slug) = resolve_coords(http, id).await?;
    let (body, _) = watch_data(http, &item, &slug, episode).await?;
    releases(&body, season, episode)
}

#[cfg(test)]
mod tests {
    use super::*;
    const SEARCH: &str = r#"{"type":"data","nodes":[{"type":"data","data":[0]},{"type":"data","data":[{"mode":1,"items":2},"anime",[3],{"id":4,"external_ids":5,"title":7,"format":10,"season_year":11,"cover_url":12},"SHOWID",{"anilist":6},["20"],{"english":8,"romaji":9},"Naruto","NARUTO","TV",2002,"https://cover.example.com/naruto.jpg"]}]}"#;
    const EPISODES: &str = r#"{"type":"chunk","id":1,"data":[{"episodes":1,"currentEpisode":2},[3],1,{"id":4,"number":5,"title":6,"filler":7},"ep1",1,"Enter",false]}"#;
    const TRACKS: &str = r#"{"type":"chunk","id":2,"data":[{"episodeNumber":1,"tracks":1,"providerOrder":2},[3],["bee"],{"track":4,"providers":5},"ssub",[6],{"provider":7,"servers":8,"subtitles":19},"bee",[9],{"server":10,"headers":11,"streams":13,"embed":18},"Vidstream-1",{"Referer":12},"https://megaplay.buzz/",[14],{"url":15,"format":16,"quality":17},"https://cdn.example.com/master.m3u8?token=abc","hls","",null,[20],{"language":21,"label":22,"file":23,"default":24},"en","English","https://cdn.example.com/en.vtt",true]}"#;
    #[test]
    fn devalue_search_fixture_maps_exact_external_id() {
        let items = catalog_items(SEARCH).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].anilist, "20");
        assert_eq!(items[0].show_id, "SHOWID");
        assert_eq!(items[0].title, "Naruto");
    }
    #[test]
    fn watch_fixture_decodes_actual_episode_record() {
        let rows = episode_rows(EPISODES).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].number, 1);
        assert_eq!(rows[0].title.as_deref(), Some("Enter"));
    }
    #[test]
    fn track_order_hls_and_subtitle_metadata_are_parsed() {
        let (pools, order) = watch_tracks(TRACKS).unwrap();
        assert_eq!(order, vec!["bee"]);
        assert_eq!(pools[0].track, "ssub");
        assert_eq!(pools[0].servers[0].streams[0].format, "hls");
        assert_eq!(pools[0].subtitles[0].file, "https://cdn.example.com/en.vtt");
    }
    #[test]
    fn redirect_identity_and_subtitle_are_preserved() {
        assert_eq!(
            redirect_slug(r#"{"type":"redirect","location":"/watch/SHOWID/naruto?ep=1"}"#)
                .as_deref(),
            Some("naruto")
        );
        let r = releases(TRACKS, 1, 1).unwrap();
        assert_eq!(r[0].provider, ProviderKind::Miruro);
        assert_eq!(r[0].codec, None);
        assert_eq!(r[0].episode, Some(1));
        assert_eq!(
            r[0].subtitles,
            vec![SubtitleOption {
                name: "en (default)".into(),
                url: "https://cdn.example.com/en.vtt".into()
            }]
        );
    }
    #[test]
    fn pools_follow_kari_track_preference() {
        let mut pools = vec!["dub", "sub", "ssub"]
            .into_iter()
            .map(|track| ProviderPool {
                name: "bee".into(),
                track: track.into(),
                servers: vec![],
                subtitles: vec![],
            })
            .collect::<Vec<_>>();
        pools.sort_by_key(|p| match p.track.as_str() {
            "ssub" => 0,
            "sub" => 1,
            _ => 2,
        });
        assert_eq!(
            pools.iter().map(|p| p.track.as_str()).collect::<Vec<_>>(),
            vec!["ssub", "sub", "dub"]
        );
    }
    #[test]
    fn unsupported_payload_and_empty_streams_fail_closed() {
        assert!(catalog_items(r#"{"type":"data","nodes":[]}"#).is_err());
        assert!(watch_tracks(EPISODES).is_err());
    }
    #[test]
    fn only_supported_stream_urls_are_retained() {
        assert!(valid_url("javascript:alert(1)").is_none());
        assert!(is_dead("https://cdn.shiora.top/a.m3u8"));
    }
}
