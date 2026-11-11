use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId,
    Release, Season, SourceMirror, SubtitleOption,
};
use reqwest::header::{ACCEPT, ORIGIN, REFERER, USER_AGENT};
use serde_json::{Map, Value, json};

const BASE: &str = "https://anidream.to";
const ANILIST: &str = "https://graphql.anilist.co";
const ANILIST_DETAIL_QUERY: &str = "query ($id: Int) { Media(id: $id, type: ANIME) { id format seasonYear title { english romaji native } description(asHtml: false) coverImage { large } genres } }";
const BROWSE: &str = "106b82f7a2b5783e3a3aed32e330e18e28a5006cded7804c38ebc41464230772";
const EPISODES: &str = "33c4f1e3d3bb451ddccfbb87aeb0d7e52f86a1c8954c61f38d31ea4527593190";
const PLAYBACK: &str = "5ebed94ee2be52c6280fe2b832298a7f8a78c1dc67d7f28b42231743b2ebde1a";
const UA: &str = crate::net::DEFAULT_BROWSER_USER_AGENT;

fn node(tag: u8, fields: Value) -> Value {
    let mut m = fields.as_object().cloned().unwrap_or_default();
    m.insert("t".into(), json!(tag));
    Value::Object(m)
}
fn encode(value: &Value, next: &mut usize) -> Result<Value, ProviderError> {
    Ok(match value {
        Value::Null => node(2, json!({"s":0})),
        Value::Bool(b) => node(2, json!({"s": if *b {2} else {3}})),
        Value::Number(n) => node(0, json!({"s":n})),
        Value::String(s) => node(1, json!({"s":s})),
        Value::Array(a) => {
            let id = *next;
            *next += 1;
            let items = a
                .iter()
                .map(|v| encode(v, next))
                .collect::<Result<Vec<_>, _>>()?;
            node(9, json!({"i":id,"a":items,"o":0}))
        }
        Value::Object(o) => {
            let id = *next;
            *next += 1;
            let mut keys: Vec<_> = o.keys().collect();
            keys.sort();
            let vals = keys
                .iter()
                .map(|k| encode(&o[*k], next))
                .collect::<Result<Vec<_>, _>>()?;
            node(10, json!({"i":id,"p":{"k":keys,"v":vals},"o":0}))
        }
    })
}
fn payload(input: Value) -> Result<String, ProviderError> {
    let mut refs = 0;
    let t = encode(&json!({"data":input}), &mut refs)?;
    serde_json::to_string(&json!({"t":t,"f":0,"m":[]}))
        .map_err(|e| ProviderError::Parsing(format!("AniDream Seroval encode: {e}")))
}
fn special(value: &Value) -> Value {
    match value.as_i64().unwrap_or(-1) {
        0 | 1 => Value::Null,
        2 => json!(true),
        3 => json!(false),
        _ => Value::Null,
    }
}
fn decode(
    value: &Value,
    refs: &mut std::collections::HashMap<usize, Value>,
) -> Result<Value, ProviderError> {
    let Some(tag) = value.get("t").and_then(Value::as_u64) else {
        return Ok(value.clone());
    };
    match tag {
        0 | 1 | 3 => Ok(value.get("s").cloned().unwrap_or(Value::Null)),
        2 => Ok(special(value.get("s").unwrap_or(&Value::Null))),
        9 => {
            let id = value.get("i").and_then(Value::as_u64).unwrap_or(0) as usize;
            if let Some(v) = refs.get(&id) {
                return Ok(v.clone());
            }
            let mut out = Vec::new();
            refs.insert(id, json!([]));
            for item in value
                .get("a")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                out.push(decode(item, refs)?);
            }
            let result = Value::Array(out);
            refs.insert(id, result.clone());
            Ok(result)
        }
        10 | 11 => {
            let id = value.get("i").and_then(Value::as_u64).unwrap_or(0) as usize;
            if let Some(v) = refs.get(&id) {
                return Ok(v.clone());
            }
            let mut out = Map::new();
            refs.insert(id, json!({}));
            let p = value.get("p").unwrap_or(&Value::Null);
            let keys = p
                .get("k")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let vals = p
                .get("v")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for (k, v) in keys.iter().zip(vals.iter()) {
                if let Some(key) = k.as_str() {
                    out.insert(key.to_owned(), decode(v, refs)?);
                }
            }
            let result = Value::Object(out);
            refs.insert(id, result.clone());
            Ok(result)
        }
        25 => {
            let Some(fields) = value.get("s").and_then(Value::as_object) else {
                return Ok(Value::Null);
            };
            let mut out = Map::new();
            for (k, v) in fields {
                out.insert(k.clone(), decode(v, refs)?);
            }
            Ok(Value::Object(out))
        }
        _ => Ok(value.clone()),
    }
}
fn decode_response(raw: &str) -> Result<Value, ProviderError> {
    let root: Value = serde_json::from_str(raw)
        .map_err(|e| ProviderError::Parsing(format!("AniDream response JSON: {e}")))?;
    let v = decode(&root, &mut Default::default())?;
    let env = v.as_object().ok_or_else(|| {
        ProviderError::Parsing("AniDream Seroval envelope is not an object".into())
    })?;
    if !env.get("error").unwrap_or(&Value::Null).is_null() {
        return Err(ProviderError::Unavailable(seroval_error(
            env.get("error").unwrap(),
        )));
    }
    env.get("result")
        .cloned()
        .ok_or_else(|| ProviderError::Parsing("AniDream envelope has no result".into()))
}
fn seroval_error(v: &Value) -> String {
    v.get("message")
        .and_then(Value::as_str)
        .or_else(|| v.as_str())
        .unwrap_or("unknown upstream error")
        .to_owned()
}
fn text(v: &Value) -> String {
    v.as_str().unwrap_or_default().trim().to_owned()
}
fn title(v: &Value) -> String {
    ["english", "romaji", "native"]
        .iter()
        .map(|k| text(&v[*k]))
        .find(|s| !s.is_empty())
        .unwrap_or_default()
}
fn safe_url(raw: &str) -> Result<String, ProviderError> {
    let parsed =
        crate::net::validate_url_syntax(raw.trim(), false).map_err(ProviderError::Parsing)?;
    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        return Err(ProviderError::Parsing(
            "AniDream URL has unsupported scheme".into(),
        ));
    }
    Ok(parsed.to_string())
}
async fn call(http: &reqwest::Client, hash: &str, input: Value) -> Result<Value, ProviderError> {
    let p = payload(input)?;
    let url = format!(
        "{BASE}/_serverFn/{hash}?payload={}",
        url::form_urlencoded::byte_serialize(p.as_bytes()).collect::<String>()
    );
    let r = http
        .get(url)
        .header(ACCEPT, "application/json")
        .header("X-TSR-ServerFn", "true")
        .header(REFERER, format!("{BASE}/"))
        .header(ORIGIN, BASE)
        .header(USER_AGENT, UA)
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    let status = r.status();
    if status.as_u16() == 429 {
        return Err(ProviderError::RateLimited(None));
    }
    if status.as_u16() == 404 {
        return Err(ProviderError::NotFound);
    }
    if status.as_u16() == 403 {
        return Err(ProviderError::Unavailable(
            "AniDream request blocked (HTTP 403)".into(),
        ));
    }
    if !status.is_success() {
        return Err(ProviderError::Unavailable(format!(
            "AniDream HTTP status {}",
            status.as_u16()
        )));
    }
    let body = r
        .text()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
    decode_response(&body)
}
fn item_from(v: &Value) -> Option<CatalogItem> {
    let id = v["id"].as_i64()?;
    if id <= 0 {
        return None;
    }
    let name = title(&v["title"]);
    if name.is_empty() {
        return None;
    }
    let movie = text(&v["format"]).eq_ignore_ascii_case("MOVIE");
    Some(CatalogItem {
        id: ProviderMediaId {
            provider: ProviderKind::Anidream,
            value: id.to_string(),
        },
        title: name,
        media_type: if movie {
            MediaType::Movie
        } else {
            MediaType::Series
        },
        year: v["start_date"]["year"]
            .as_i64()
            .filter(|y| *y > 0)
            .map(|y| y.to_string()),
        poster_url: {
            let s = text(&v["cover_image"]["large"]);
            if s.is_empty() {
                None
            } else {
                safe_url(&s).ok()
            }
        },
        season_count: None,
    })
}
fn item_details(
    item: CatalogItem,
    episodes: Vec<Episode>,
    description: Option<String>,
    genres: Vec<String>,
) -> MediaDetails {
    let season = episodes.iter().map(|e| e.season).max().unwrap_or(1);
    let seasons = if episodes.is_empty() {
        vec![]
    } else {
        (1..=season)
            .map(|n| Season {
                number: n,
                episodes: episodes.iter().filter(|e| e.season == n).cloned().collect(),
            })
            .collect()
    };
    MediaDetails {
        id: item.id,
        title: item.title,
        media_type: item.media_type,
        year: item.year,
        description,
        tagline: None,
        imdb_rating: None,
        director: None,
        stars: None,
        prints: None,
        audios: Some("sub".into()),
        poster_url: item.poster_url,
        duration: None,
        genres,
        seasons,
        dubs: vec![],
    }
}
async fn episode_rows(http: &reqwest::Client, id: i64) -> Result<Vec<Episode>, ProviderError> {
    let result = call(http, EPISODES, json!({"id":id})).await?;
    let rows = result
        .as_array()
        .ok_or_else(|| ProviderError::Parsing("AniDream episodes result is not an array".into()))?;
    let mut out = Vec::new();
    for row in rows {
        let f = row["number"].as_f64().unwrap_or(0.0);
        if f <= 0.0 || f.fract() != 0.0 || f > usize::MAX as f64 {
            continue;
        }
        let n = f as usize;
        let mut name = title(&row["title"]);
        if name.is_empty() {
            name = format!("Episode {n}")
        }
        // The existing ReleaseProvider trait has no audio selector. Only
        // advertise sub episodes, which resolve.rs can request unambiguously.
        if row["has_sub"].as_bool() == Some(true) || row["has_sub"].is_null() {
            out.push(Episode {
                season: 1,
                number: n,
                title: Some(name),
                overview: None,
            });
        }
    }
    Ok(out)
}
pub async fn search(
    http: &reqwest::Client,
    query: &str,
) -> Result<Vec<CatalogItem>, ProviderError> {
    let q = query.trim();
    if q.is_empty() {
        return Err(ProviderError::Parsing(
            "AniDream search query is empty".into(),
        ));
    }
    let value = call(
        http,
        BROWSE,
        json!({"type":"ANIME","q":q,"page":1,"limit":25,"isAdult":false}),
    )
    .await?;
    let rows = value["items"].as_array().ok_or_else(|| {
        ProviderError::Parsing("AniDream browse response has no items array".into())
    })?;
    Ok(rows.iter().filter_map(item_from).collect())
}
pub async fn details(http: &reqwest::Client, id: &str) -> Result<MediaDetails, ProviderError> {
    let id_num = id
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or(ProviderError::NotFound)?;
    let response = http
        .post(ANILIST)
        .header(ACCEPT, "application/json")
        .header(USER_AGENT, UA)
        .json(&json!({"query": ANILIST_DETAIL_QUERY, "variables": {"id": id_num}}))
        .send()
        .await
        .map_err(|e| ProviderError::Network(format!("AniDream AniList detail: {e}")))?;
    if response.status().as_u16() == 429 {
        return Err(ProviderError::RateLimited(None));
    }
    if !response.status().is_success() {
        return Err(ProviderError::Unavailable(format!(
            "AniList HTTP status {}",
            response.status().as_u16()
        )));
    }
    let response: Value = response
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(format!("AniDream AniList detail JSON: {e}")))?;
    let media = response
        .pointer("/data/Media")
        .filter(|media| !media.is_null())
        .ok_or(ProviderError::NotFound)?;
    let name = title(&media["title"]);
    if name.is_empty() {
        return Err(ProviderError::Parsing(
            "AniDream AniList detail has no title".into(),
        ));
    }
    let episodes = episode_rows(http, id_num).await?;
    let item = CatalogItem {
        id: ProviderMediaId {
            provider: ProviderKind::Anidream,
            value: id_num.to_string(),
        },
        title: name,
        media_type: if text(&media["format"]).eq_ignore_ascii_case("MOVIE") {
            MediaType::Movie
        } else {
            MediaType::Series
        },
        year: media["seasonYear"].as_i64().map(|year| year.to_string()),
        poster_url: safe_url(&text(&media["coverImage"]["large"])).ok(),
        season_count: if episodes.is_empty() { None } else { Some(1) },
    };
    let description = media["description"].as_str().map(str::to_owned);
    let genres = media["genres"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    Ok(item_details(item, episodes, description, genres))
}
pub async fn resolve(
    http: &reqwest::Client,
    id: &str,
    season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    let anilist_id = id
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or(ProviderError::NotFound)?;
    if season == 0 || episode == 0 {
        return Err(ProviderError::Parsing(
            "AniDream requires positive season and episode".into(),
        ));
    }
    let mut last = None;
    for server in [2, 1] {
        let result = call(
            http,
            PLAYBACK,
            json!({"anilistId":anilist_id,"episode":episode,"dub":false,"server":server}),
        )
        .await;
        let value = match result {
            Ok(v) => v,
            Err(e) => {
                last = Some(e);
                continue;
            }
        };
        if value["ok"].as_bool() != Some(true) || value["data"].is_null() {
            last = Some(if text(&value["code"]).eq_ignore_ascii_case("NOT_FOUND") {
                ProviderError::NotFound
            } else {
                ProviderError::Unavailable(text(&value["message"]))
            });
            continue;
        }
        let data = &value["data"];
        let stream = safe_url(&text(&data["streamUrl"]))?;
        let subtitles = data["subtitles"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| {
                let u = safe_url(&text(&s["url"])).ok()?;
                let label = if !text(&s["label"]).is_empty() {
                    text(&s["label"])
                } else {
                    text(&s["language"])
                };
                Some(SubtitleOption {
                    name: if label.is_empty() {
                        "Subtitle".into()
                    } else {
                        label
                    },
                    url: u,
                })
            })
            .collect();
        return Ok(vec![Release {
            provider: ProviderKind::Anidream,
            filename: format!("AniDream {anilist_id} S{season:02}E{episode:02}"),
            quality: Some("Auto".into()),
            codec: None,
            language: Some("sub".into()),
            size_bytes: None,
            season: Some(season),
            episode: Some(episode),
            mirrors: vec![SourceMirror {
                label: "AniDream HLS".into(),
                resolver_url: stream,
                headers: vec![
                    ("Referer".into(), format!("{BASE}/")),
                    ("User-Agent".into(), UA.into()),
                ],
                direct_file: false,
            }],
            subtitles,
            resource_id: Some(format!("{anilist_id}:{season}:{episode}")),
        }]);
    }
    Err(last.unwrap_or_else(|| ProviderError::Unavailable("AniDream has no stream".into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seroval_request_golden_shape() {
        let s = payload(json!({"q":"x","page":1,"flag":false})).unwrap();
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["f"], 0);
        assert_eq!(v["m"], json!([]));
        assert_eq!(v["t"]["t"], 10);
        assert_eq!(v["t"]["p"]["k"], json!(["data"]));
        let data = &v["t"]["p"]["v"][0];
        assert_eq!(data["t"], 10);
        assert_eq!(data["p"]["k"], json!(["flag", "page", "q"]));
    }
    #[test]
    fn seroval_response_fixture_maps_result_and_error() {
        let fixture = r#"{"t":10,"i":0,"p":{"k":["result","error","context"],"v":[{"t":9,"i":1,"a":[{"t":10,"i":2,"p":{"k":["number","has_sub"],"v":[{"t":0,"s":1},{"t":2,"s":2}]},"o":0}]},{"t":2,"s":1},{"t":11,"i":3,"p":{"k":[],"v":[]},"o":0}]},"o":0}"#;
        let v = decode_response(fixture).unwrap();
        assert_eq!(v[0]["number"], 1);
        assert_eq!(v[0]["has_sub"], true);
        let err = r#"{"t":10,"i":0,"p":{"k":["result","error","context"],"v":[{"t":2,"s":1},{"t":25,"i":1,"s":{"message":{"t":1,"s":"upstream exploded"}},"c":"$TSR/Error"},{"t":11,"i":2,"p":{"k":[],"v":[]},"o":0}]},"o":0}"#;
        assert!(
            decode_response(err)
                .unwrap_err()
                .to_string()
                .contains("upstream exploded")
        );
    }
    #[test]
    fn catalog_fixture_mapping() {
        let v = json!({"id":21,"mal_id":21,"format":"TV","title":{"english":"One Piece","romaji":"ONE PIECE"},"start_date":{"year":1999},"cover_image":{"large":"https://img.example/op.jpg"},"description":"A pirate \\x3Cbr> story","genres":["Action"]});
        let c = item_from(&v).unwrap();
        assert_eq!(c.id.value, "21");
        assert_eq!(c.id.provider, ProviderKind::Anidream);
        assert_eq!(c.title, "One Piece");
        assert_eq!(c.year.as_deref(), Some("1999"));
        let ep =
            json!({"number":1,"title":{"english":"Enter Luffy"},"has_sub":true,"has_dub":true});
        assert_eq!(ep["number"].as_u64(), Some(1));
    }
}
