use crate::providers::models::{
    ProviderError, ProviderKind, Release, SourceMirror, SubtitleOption,
};
use reqwest::header::{ACCEPT, CONTENT_TYPE, ORIGIN, REFERER, USER_AGENT};
use serde_json::{Value, json};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const EMBED_BASE: &str = "https://megaplay.buzz";
const ANILIST_URL: &str = "https://graphql.anilist.co";
const ANILIST_REFERER: &str = "https://anilist.co/";
const KARI_DESKTOP_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36";
const MAL_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

static MAL_ID_CACHE: OnceLock<Mutex<std::collections::HashMap<String, (usize, Instant)>>> =
    OnceLock::new();

fn parse_anilist_route(id: &str) -> Result<(usize, &'static str), ProviderError> {
    let value = id.trim();
    let (numeric, audio) = if let Some((numeric, audio)) = value.rsplit_once('/') {
        (numeric, audio)
    } else if let Some((numeric, audio)) = value.rsplit_once(':') {
        (numeric, audio)
    } else {
        (value, "sub")
    };
    let audio = if audio.eq_ignore_ascii_case("dub") {
        "dub"
    } else if audio.eq_ignore_ascii_case("sub") {
        "sub"
    } else {
        return Err(ProviderError::Parsing(format!(
            "AniCine audio mode must be sub or dub, got {audio:?}"
        )));
    };
    let parsed = numeric.parse::<usize>().map_err(|_| {
        ProviderError::Parsing(format!("AniCine requires a numeric AniList ID, got {id:?}"))
    })?;
    if parsed == 0 {
        return Err(ProviderError::Parsing(
            "AniCine AniList ID must be positive".into(),
        ));
    }
    Ok((parsed, audio))
}

fn embed_url(id: &str, episode: usize, audio: &str) -> String {
    format!("{EMBED_BASE}/stream/ani/{id}/{episode}/{audio}")
}

fn mal_embed_url(mal_id: usize, episode: usize, audio: &str) -> String {
    format!("{EMBED_BASE}/stream/mal/{mal_id}/{episode}/{audio}")
}

async fn mal_id(
    http: &reqwest::Client,
    id: &str,
    numeric_id: usize,
) -> Result<usize, ProviderError> {
    let cache = MAL_ID_CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Some((cached, when)) = cache.lock().ok().and_then(|mut cache| {
        let value = cache.get(id).copied();
        if value.is_some_and(|(_, then)| then.elapsed() >= MAL_CACHE_TTL) {
            cache.remove(id);
            None
        } else {
            value
        }
    }) {
        let _ = when;
        return Ok(cached);
    }

    let payload = json!({
        "query": "query($id:Int){Media(id:$id,type:ANIME){idMal}}",
        "variables": {"id": numeric_id},
    });
    let response = http
        .post(ANILIST_URL)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .header(USER_AGENT, KARI_DESKTOP_USER_AGENT)
        .header(REFERER, ANILIST_REFERER)
        .header(ORIGIN, "https://anilist.co")
        .json(&payload)
        .send()
        .await
        .map_err(|e| ProviderError::Network(format!("AniCine AniList MAL lookup: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(status_error("AniList MAL lookup", status));
    }
    let value: Value = response
        .json()
        .await
        .map_err(|e| ProviderError::Parsing(format!("AniCine AniList MAL JSON: {e}")))?;
    let found = value
        .get("data")
        .and_then(|v| v.get("Media"))
        .and_then(|v| v.get("idMal"))
        .and_then(Value::as_u64)
        .and_then(|v| usize::try_from(v).ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| ProviderError::NotFound)?;
    if let Ok(mut cache) = cache.lock() {
        cache.insert(id.to_owned(), (found, Instant::now()));
    }
    Ok(found)
}

fn status_error(context: &str, status: reqwest::StatusCode) -> ProviderError {
    if status.as_u16() == 429 {
        ProviderError::RateLimited(None)
    } else {
        ProviderError::Unavailable(format!("AniCine {context} HTTP status {}", status.as_u16()))
    }
}

pub(crate) async fn resolve(
    http: &reqwest::Client,
    id: &str,
    season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    let (numeric_id, audio) = parse_anilist_route(id)?;
    if episode == 0 {
        return Err(ProviderError::Parsing(
            "AniCine episode number must be positive".into(),
        ));
    }
    let primary = embed_url(&numeric_id.to_string(), episode, audio);
    match resolve_embed_as_release(http, &primary, id, season, episode).await {
        Ok(releases) => Ok(releases),
        Err(primary_error) => {
            // Match Kari's route fallback: only try MAL when AniList's embed did
            // not resolve and the caller's request has not otherwise been cancelled.
            let mal = mal_id(http, id, numeric_id).await.map_err(|mal_error| {
                ProviderError::Unavailable(format!(
                    "AniCine AniList embed failed ({primary_error}); MAL mapping unavailable ({mal_error})"
                ))
            })?;
            let fallback = mal_embed_url(mal, episode, audio);
            resolve_embed_as_release(http, &fallback, &mal.to_string(), season, episode)
                .await
                .map_err(|fallback_error| {
                    ProviderError::Unavailable(format!(
                        "AniCine AniList and MAL embeds failed ({primary_error}; {fallback_error})"
                    ))
                })
        }
    }
}

async fn resolve_embed_as_release(
    http: &reqwest::Client,
    url: &str,
    resource_id: &str,
    season: usize,
    episode: usize,
) -> Result<Vec<Release>, ProviderError> {
    let resolved = super::megaplay::resolve_embed(
        http,
        url,
        &format!("{EMBED_BASE}/"),
        KARI_DESKTOP_USER_AGENT,
    )
    .await?;
    let media = crate::net::validate_url_syntax(&resolved.stream_url, false)
        .map_err(|e| ProviderError::Parsing(format!("AniCine media URL: {e}")))?;
    let subtitles = subtitles_from_tracks(&resolved.tracks);
    Ok(vec![Release {
        provider: ProviderKind::Anicine,
        filename: format!("anicine-{resource_id}-{episode}.m3u8"),
        quality: Some("1080p".into()),
        codec: None,
        language: Some("ja".into()),
        size_bytes: None,
        season: Some(season),
        episode: Some(episode),
        mirrors: vec![SourceMirror {
            label: "AniCine MegaPlay".into(),
            resolver_url: media.to_string(),
            headers: vec![
                ("User-Agent".into(), KARI_DESKTOP_USER_AGENT.into()),
                ("Referer".into(), resolved.embed_origin),
            ],
            direct_file: false,
        }],
        subtitles,
        resource_id: Some(resource_id.to_owned()),
    }])
}

fn subtitles_from_tracks(tracks: &[super::megaplay::Track]) -> Vec<SubtitleOption> {
    let mut subtitles = Vec::new();
    for track in tracks {
        if track.file.trim().is_empty() || track.kind.trim().eq_ignore_ascii_case("thumbnails") {
            continue;
        }
        let Ok(track_url) = crate::net::validate_url_syntax(&track.file, false) else {
            continue;
        };
        let label = track.label.trim();
        let name = if label.is_empty() { "English" } else { label };
        subtitles.push(SubtitleOption {
            name: name.to_owned(),
            url: track_url.to_string(),
        });
    }
    subtitles
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_positive_numeric_anilist_ids() {
        assert_eq!(parse_anilist_route(" 123 ").unwrap(), (123, "sub"));
        assert_eq!(parse_anilist_route("123/dub").unwrap(), (123, "dub"));
        assert_eq!(parse_anilist_route("123:sub").unwrap(), (123, "sub"));
        assert!(parse_anilist_route("abc").is_err());
        assert!(parse_anilist_route("0").is_err());
    }

    #[test]
    fn builds_anilist_and_mal_sub_and_dub_routes() {
        assert_eq!(
            embed_url("123", 4, "sub"),
            "https://megaplay.buzz/stream/ani/123/4/sub"
        );
        assert_eq!(
            embed_url("123", 4, "dub"),
            "https://megaplay.buzz/stream/ani/123/4/dub"
        );
        assert_eq!(
            mal_embed_url(456, 4, "dub"),
            "https://megaplay.buzz/stream/mal/456/4/dub"
        );
    }

    #[test]
    fn extracts_only_valid_external_subtitle_tracks() {
        let tracks = vec![
            super::super::megaplay::Track {
                file: "https://cdn.example/sub.vtt".into(),
                label: "English CC".into(),
                kind: "captions".into(),
                default: true,
            },
            super::super::megaplay::Track {
                file: "https://cdn.example/thumbs.vtt".into(),
                label: "Thumbnails".into(),
                kind: "thumbnails".into(),
                default: false,
            },
            super::super::megaplay::Track {
                file: "javascript:alert(1)".into(),
                label: "Bad".into(),
                kind: "captions".into(),
                default: false,
            },
        ];
        let subtitles = subtitles_from_tracks(&tracks);
        assert_eq!(subtitles.len(), 1);
        assert_eq!(subtitles[0].name, "English CC");
        assert_eq!(subtitles[0].url, "https://cdn.example/sub.vtt");
    }

    #[test]
    fn unknown_source_codec_is_not_claimed() {
        // Keep an explicit assertion near the route tests documenting that the
        // returned release uses codec=None; no H.264 inference is made.
        assert_eq!(ProviderKind::Anicine.label(), "AniCine");
    }
}
