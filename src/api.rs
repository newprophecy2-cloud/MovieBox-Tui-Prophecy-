use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::Mutex,
    task::JoinSet,
};

use crate::{
    providers::models::{
        CatalogItem, MediaDetails, ProviderError, ProviderKind, ProviderMediaId, Release, Season,
    },
    service::MovieBoxService,
};

const MAX_QUERY: usize = 200;
const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 100;
const SOURCE_TIMEOUT: Duration = Duration::from_secs(8);
const OVERALL_TIMEOUT: Duration = Duration::from_secs(30);
const TRACK_PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const PROVIDERS: [ProviderKind; 6] = [
    ProviderKind::Dramachi,
    ProviderKind::FourKHdHub,
    ProviderKind::MovieBox,
    ProviderKind::BdixCircleFtp,
    ProviderKind::BdixDhakaFlix,
    ProviderKind::Addons,
];

fn provider_priority(provider: ProviderKind) -> usize {
    PROVIDERS
        .iter()
        .position(|candidate| *candidate == provider)
        .unwrap_or(PROVIDERS.len())
}

#[derive(Clone)]
pub struct ApiState {
    pub service: Arc<MovieBoxService>,
    pub rate_limiter: Arc<Mutex<HashMap<String, u32>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OpaqueRef {
    p: ProviderKind,
    v: String,
    s: usize,
    e: usize,
}
#[derive(Debug, Serialize, Deserialize)]
struct OpaqueSource {
    p: ProviderKind,
    v: String,
    s: usize,
    e: usize,
    i: usize,
}
#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}
#[derive(Debug, Serialize)]
struct ErrorBody {
    error: ApiError,
}
#[derive(Debug, Serialize)]
struct ApiError {
    code: &'static str,
    message: String,
    provider: Option<String>,
    retryable: bool,
    request_id: String,
}
#[derive(Debug, Serialize)]
struct SearchResponse {
    results: Vec<ApiCatalogItem>,
    next_cursor: Option<String>,
    partial: bool,
}
#[derive(Debug, Serialize)]
struct ApiCatalogItem {
    id: String,
    title: String,
    media_type: crate::providers::models::MediaType,
    year: Option<String>,
    poster_url: Option<String>,
    season_count: Option<usize>,
}
#[derive(Debug, Serialize)]
struct ApiDetails {
    id: String,
    title: String,
    media_type: crate::providers::models::MediaType,
    year: Option<String>,
    description: Option<String>,
    tagline: Option<String>,
    imdb_rating: Option<String>,
    director: Option<String>,
    stars: Option<String>,
    prints: Option<String>,
    audios: Option<String>,
    poster_url: Option<String>,
    duration: Option<String>,
    genres: Vec<String>,
    seasons: Vec<ApiSeason>,
    audio: Vec<crate::providers::models::AudioTrackOption>,
}
#[derive(Debug, Serialize)]
struct ApiSeason {
    number: usize,
    episodes: Vec<ApiEpisode>,
}
#[derive(Debug, Serialize)]
struct ApiEpisode {
    id: String,
    season: usize,
    number: usize,
    title: Option<String>,
    overview: Option<String>,
}
#[derive(Debug, Serialize)]
struct ApiSource {
    id: String,
    label: String,
    quality: Option<String>,
    codec: Option<String>,
    language: Option<String>,
    size_bytes: Option<u64>,
    downloadable: bool,
    expires_at: Option<String>,
}
#[derive(Debug, Serialize)]
struct SourcesResponse {
    sources: Vec<ApiSource>,
    partial: bool,
}
#[derive(Debug, Serialize)]
struct ApiAudioTrack {
    track_index: u32,
    language_code: String,
    language_name: String,
    original_language: Option<String>,
    codec: Option<String>,
    channels: Option<u16>,
    is_default: bool,
}
#[derive(Debug, Serialize)]
struct AudioTracksResponse {
    source_id: String,
    media_type: &'static str,
    status: &'static str,
    available: bool,
    tracks: Vec<ApiAudioTrack>,
}
#[derive(Debug, Serialize)]
struct ApiSubtitleTrack {
    subtitle_index: u32,
    language_code: String,
    language_name: String,
    original_language: Option<String>,
    format: Option<String>,
    kind: &'static str,
    is_default: bool,
}
#[derive(Debug, Serialize)]
struct SubtitleTracksResponse {
    source_id: String,
    media_type: &'static str,
    status: &'static str,
    available: bool,
    subtitles: Vec<ApiSubtitleTrack>,
}
#[derive(Debug, Deserialize)]
struct ProbeOutput {
    #[serde(default)]
    streams: Vec<ProbeStream>,
}
#[derive(Debug, Deserialize)]
struct ProbeStream {
    index: Option<u32>,
    codec_name: Option<String>,
    codec_type: Option<String>,
    channels: Option<u16>,
    disposition: Option<ProbeDisposition>,
    tags: Option<HashMap<String, String>>,
}
#[derive(Debug, Deserialize)]
struct ProbeDisposition {
    #[serde(default)]
    default: i32,
}

fn request_id() -> String {
    format!("req-{}", uuid_like())
}
fn uuid_like() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}
fn encode<T: Serialize>(value: &T) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap_or_default())
}
fn decode<T: for<'a> Deserialize<'a>>(value: &str) -> Result<T, ()> {
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(value).map_err(|_| ())?).map_err(|_| ())
}
fn provider_ref(id: &ProviderMediaId, s: usize, e: usize) -> String {
    encode(&OpaqueRef {
        p: id.provider,
        v: id.value.clone(),
        s,
        e,
    })
}
fn source_ref(r: &Release, episode: &OpaqueRef, i: usize) -> String {
    encode(&OpaqueSource {
        p: r.provider,
        v: episode.v.clone(),
        s: episode.s,
        e: episode.e,
        i,
    })
}
fn episode_ref(id: &ProviderMediaId, episode: &crate::providers::models::Episode) -> String {
    encode(&OpaqueRef {
        p: id.provider,
        v: id.value.clone(),
        s: episode.season,
        e: episode.number,
    })
}
fn catalog(item: CatalogItem) -> ApiCatalogItem {
    ApiCatalogItem {
        id: provider_ref(&item.id, 0, 0),
        title: item.title,
        media_type: item.media_type,
        year: item.year,
        poster_url: item.poster_url,
        season_count: item.season_count,
    }
}
fn details(d: MediaDetails) -> ApiDetails {
    let provider_id = d.id.clone();
    ApiDetails {
        id: provider_ref(&d.id, 0, 0),
        title: d.title,
        media_type: d.media_type,
        year: d.year,
        description: d.description,
        tagline: d.tagline,
        imdb_rating: d.imdb_rating,
        director: d.director,
        stars: d.stars,
        prints: d.prints,
        audios: d.audios,
        poster_url: d.poster_url,
        duration: d.duration,
        genres: d.genres,
        seasons: d
            .seasons
            .into_iter()
            .map(|s| season(s, &provider_id))
            .collect(),
        audio: d.dubs,
    }
}
fn season(s: Season, provider_id: &ProviderMediaId) -> ApiSeason {
    ApiSeason {
        number: s.number,
        episodes: s
            .episodes
            .into_iter()
            .map(|e| ApiEpisode {
                id: episode_ref(provider_id, &e),
                season: e.season,
                number: e.number,
                title: e.title,
                overview: e.overview,
            })
            .collect(),
    }
}
fn err(
    code: &'static str,
    msg: impl Into<String>,
    provider: Option<ProviderKind>,
    status: u16,
) -> (u16, String) {
    let body = ErrorBody {
        error: ApiError {
            code,
            message: msg.into(),
            provider: provider.map(|p| p.label().to_string()),
            retryable: matches!(
                code,
                "PROVIDER_TIMEOUT" | "SOURCE_TIMEOUT" | "UPSTREAM_UNAVAILABLE"
            ),
            request_id: request_id(),
        },
    };
    (status, serde_json::to_string(&body).unwrap())
}
fn provider_error(e: ProviderError, p: ProviderKind) -> (u16, String) {
    match e {
        ProviderError::NotFound => err("ANIME_NOT_FOUND", "The item was not found.", Some(p), 404),
        ProviderError::RateLimited(_) => err(
            "RATE_LIMITED",
            "The upstream provider rate limited the request.",
            Some(p),
            429,
        ),
        ProviderError::Network(_) => err(
            "UPSTREAM_UNAVAILABLE",
            "The provider could not be reached.",
            Some(p),
            503,
        ),
        ProviderError::Parsing(_) => err(
            "UPSTREAM_UNAVAILABLE",
            "The provider response could not be parsed.",
            Some(p),
            502,
        ),
        ProviderError::Unavailable(message)
            if p == ProviderKind::Addons && message == "No enabled stream addon is configured." =>
        {
            err(
                "NO_STREAM_ADDON",
                "No enabled stream addon is configured.",
                Some(p),
                503,
            )
        }
        ProviderError::Unavailable(message)
            if p == ProviderKind::Addons && message == "No addon stream sources available" =>
        {
            err(
                "NO_SOURCE_CANDIDATES",
                "No source candidates were returned by the enabled stream addons.",
                Some(p),
                404,
            )
        }
        ProviderError::Unavailable(_) => err(
            "UPSTREAM_UNAVAILABLE",
            "The provider is unavailable.",
            Some(p),
            503,
        ),
    }
}
fn parse_ref(value: &str) -> Result<OpaqueRef, (u16, String)> {
    decode(value).map_err(|_| err("INVALID_ID", "The identifier is invalid.", None, 400))
}
fn parse_source(value: &str) -> Result<OpaqueSource, (u16, String)> {
    decode(value).map_err(|_| err("INVALID_ID", "The source identifier is invalid.", None, 400))
}

pub async fn run() -> std::io::Result<()> {
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse::<u16>()
        .unwrap_or(8080);
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let state = ApiState {
        service: Arc::new(MovieBoxService::new()),
        rate_limiter: Arc::new(Mutex::new(HashMap::new())),
    };
    log::info!("MovieBox API listening on 0.0.0.0:{port}");
    loop {
        let (stream, addr) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, addr, state).await {
                log::debug!("request ended: {e}");
            }
        });
    }
}

async fn handle(mut stream: TcpStream, addr: SocketAddr, state: ApiState) -> std::io::Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let raw = String::from_utf8_lossy(&buf[..n]);
    let mut lines = raw.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    if method != "GET" {
        return write_json(&mut stream, 405, &serde_json::json!({"error":{"code":"METHOD_NOT_ALLOWED","message":"Only GET is supported."}}), None).await;
    }
    let key = addr.ip().to_string();
    {
        let mut limits = state.rate_limiter.lock().await;
        let count = limits.entry(key).or_default();
        *count += 1;
        if *count > 120 {
            return write_json(&mut stream, 429, &serde_json::json!({"error":{"code":"RATE_LIMITED","message":"Too many requests."}}), Some("Retry-After: 60\r\n")).await;
        }
    }
    route(&mut stream, target, state).await
}

async fn route(stream: &mut TcpStream, target: &str, state: ApiState) -> std::io::Result<()> {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    match path {
        "/" => write_json(stream, 200, &serde_json::json!({"name":"MovieBox API","version":"v1","docs":"/docs","health":"/health"}), None).await,
        "/health" => write_json(stream, 200, &serde_json::json!({"status":"ok"}), None).await,
        "/ready" => write_json(stream, 200, &serde_json::json!({"status":"ready","meaning":"The process and local service layer are initialized; upstream availability is request-specific."}), None).await,
        "/api/v1" => write_json(stream, 200, &serde_json::json!({"version":"v1","resources":["anime","episodes","sources"]}), None).await,
        "/openapi.json" => write_json(stream, 200, &openapi(), None).await,
        "/docs" => write_text(stream, 200, "text/html; charset=utf-8", "<!doctype html><html><head><title>MovieBox API</title></head><body><h1>MovieBox API</h1><p>OpenAPI: <a href=\"/openapi.json\">/openapi.json</a></p><p>Swagger UI can load this document from a local or trusted client.</p></body></html>", None).await,
        "/api/v1/anime/search" => search_json(stream, query, state).await,
        "/api/v1/anime/search/stream" => search_sse(stream, query, state).await,
        _ if path.starts_with("/api/v1/anime/") => anime_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/episodes/") => episode_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/sources/") => source_route(stream, path, state).await,
        _ => { let (s,b) = err("INTERNAL_ERROR", "Route not found.", None, 404); write_raw_json(stream, s, &b, None).await }
    }
}

fn parse_query(query: &str) -> SearchQuery {
    let mut out = SearchQuery {
        q: None,
        limit: None,
        cursor: None,
    };
    for item in query.split('&') {
        let Some((k, v)) = item.split_once('=') else {
            continue;
        };
        let v = percent_decode(v);
        match k {
            "q" => out.q = Some(v),
            "limit" => out.limit = v.parse().ok(),
            "cursor" => out.cursor = Some(v),
            _ => {}
        }
    }
    out
}
fn percent_decode(v: &str) -> String {
    let mut out = String::new();
    let bytes = v.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(n) = u8::from_str_radix(&v[i + 1..i + 3], 16) {
                out.push(n as char);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' {
            ' '
        } else {
            bytes[i] as char
        });
        i += 1;
    }
    out
}

async fn search_json(stream: &mut TcpStream, query: &str, state: ApiState) -> std::io::Result<()> {
    let q = parse_query(query);
    let Some(qv) = q.q.filter(|v| !v.trim().is_empty() && v.len() <= MAX_QUERY) else {
        let (s, b) = err(
            "INVALID_QUERY",
            "q is required and must be 1-200 characters.",
            None,
            400,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    if q.cursor
        .as_deref()
        .is_some_and(|c| decode::<usize>(c).is_err())
    {
        let (s, b) = err("CURSOR_INVALID", "The cursor is invalid.", None, 400);
        return write_raw_json(stream, s, &b, None).await;
    }
    let (items, failed) = search_all(&state.service, &qv).await;
    let results = items
        .into_iter()
        .take(limit)
        .map(catalog)
        .collect::<Vec<_>>();
    let body = SearchResponse {
        results,
        next_cursor: None,
        partial: failed,
    };
    write_json(stream, 200, &body, None).await
}

async fn search_sse(stream: &mut TcpStream, query: &str, state: ApiState) -> std::io::Result<()> {
    let q = parse_query(query);
    let Some(qv) = q.q.filter(|v| !v.trim().is_empty() && v.len() <= MAX_QUERY) else {
        let (s, b) = err(
            "INVALID_QUERY",
            "q is required and must be 1-200 characters.",
            None,
            400,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    write_headers(stream, 200, "text/event-stream", None).await?;
    let mut tasks = JoinSet::new();
    for p in PROVIDERS {
        let svc = state.service.clone();
        let q = qv.clone();
        tasks.spawn(async move {
            let result = tokio::time::timeout(OVERALL_TIMEOUT, svc.search_typed(p, &q, 1)).await;
            (p, result)
        });
    }
    while let Some(Ok((p, result))) = tasks.join_next().await {
        match result {
            Ok(Ok(items)) => {
                for item in items {
                    let payload =
                        serde_json::to_string(&serde_json::json!({"result":catalog(item)}))
                            .unwrap();
                    send_event(stream, "search_result", &payload).await?;
                }
            }
            Ok(Err(e)) => {
                let payload=serde_json::to_string(&serde_json::json!({"provider":p.label(),"code":"UPSTREAM_UNAVAILABLE","message":e.user_message(p)})).unwrap();
                send_event(stream, "search_provider_error", &payload).await?;
            }
            Err(_) => {
                let payload=serde_json::json!({"provider":p.label(),"code":"PROVIDER_TIMEOUT","message":"Provider request timed out."}).to_string();
                send_event(stream, "search_provider_error", &payload).await?;
            }
        }
    }
    send_event(
        stream,
        "search_complete",
        "{\"partial\":true,\"replay\":false}",
    )
    .await
}

async fn search_all(service: &Arc<MovieBoxService>, query: &str) -> (Vec<CatalogItem>, bool) {
    let mut tasks = JoinSet::new();
    for p in PROVIDERS {
        let svc = service.clone();
        let q = query.to_string();
        tasks.spawn(async move {
            (
                p,
                tokio::time::timeout(OVERALL_TIMEOUT, svc.search_typed(p, &q, 1)).await,
            )
        });
    }
    let mut items = Vec::new();
    let mut failed = false;
    while let Some(Ok((p, r))) = tasks.join_next().await {
        match r {
            Ok(Ok(mut v)) => items.append(&mut v),
            _ => {
                let _ = p;
                failed = true;
            }
        }
    }
    let mut seen = std::collections::HashSet::new();
    items.retain(|i| seen.insert((i.id.provider, i.id.value.clone())));
    items.sort_by_key(|item| provider_priority(item.id.provider));
    (items, failed)
}

async fn anime_route(stream: &mut TcpStream, path: &str, state: ApiState) -> std::io::Result<()> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() < 4 {
        let (s, b) = err("INVALID_ID", "An anime identifier is required.", None, 400);
        return write_raw_json(stream, s, &b, None).await;
    }
    let r = parse_ref(parts[3]);
    let Ok(r) = r else {
        let (s, b) = r.unwrap_err();
        return write_raw_json(stream, s, &b, None).await;
    };
    let d = state.service.details_typed(r.p, &r.v).await;
    let Ok(d) = d else {
        let (s, b) = provider_error(d.unwrap_err(), r.p);
        return write_raw_json(stream, s, &b, None).await;
    };
    if parts.len() == 4 {
        return write_json(stream, 200, &details(d), None).await;
    }
    if parts[4] == "seasons" {
        return write_json(stream, 200, &details(d).seasons, None).await;
    }
    if parts[4] == "episodes" {
        if parts.len() == 5 {
            let eps: Vec<_> = details(d)
                .seasons
                .into_iter()
                .flat_map(|s| s.episodes)
                .collect();
            return write_json(stream, 200, &eps, None).await;
        }
        let (se, e) = if let Ok(episode) = decode::<OpaqueRef>(parts[5]) {
            if episode.p != r.p || episode.v != r.v {
                let (s, b) = err(
                    "INVALID_ID",
                    "The episode does not belong to the requested anime.",
                    None,
                    400,
                );
                return write_raw_json(stream, s, &b, None).await;
            }
            (episode.s, episode.e)
        } else if let Ok(number) = parts[5].parse::<usize>() {
            (r.s, number)
        } else {
            let (s, b) = err(
                "INVALID_ID",
                "The episode identifier is invalid.",
                None,
                400,
            );
            return write_raw_json(stream, s, &b, None).await;
        };
        let found = details(d)
            .seasons
            .into_iter()
            .flat_map(|s| s.episodes)
            .find(|x| x.season == se && x.number == e);
        return match found {
            Some(x) => write_json(stream, 200, &x, None).await,
            None => {
                let (s, b) = err(
                    "EPISODE_NOT_FOUND",
                    "The episode was not found.",
                    Some(r.p),
                    404,
                );
                write_raw_json(stream, s, &b, None).await
            }
        };
    }
    let (s, b) = err("INTERNAL_ERROR", "Unsupported anime route.", None, 404);
    write_raw_json(stream, s, &b, None).await
}

async fn releases(state: &ApiState, r: &OpaqueRef) -> Result<Vec<Release>, ProviderError> {
    let svc = &state.service;
    match r.p {
        ProviderKind::MovieBox => {
            crate::providers::ReleaseProvider::episode_streams(&svc.client, &r.v, r.s, r.e).await
        }
        ProviderKind::FourKHdHub => match svc.fourk_client.as_ref() {
            Some(client) => {
                crate::providers::ReleaseProvider::episode_streams(client, &r.v, r.s, r.e).await
            }
            None => Err(ProviderError::Unavailable("4KHDHub is unavailable".into())),
        },
        ProviderKind::Dramachi => {
            crate::providers::ReleaseProvider::episode_streams(&svc.dramachi_client, &r.v, r.s, r.e)
                .await
        }
        ProviderKind::BdixCircleFtp => {
            crate::providers::ReleaseProvider::episode_streams(
                &svc.circleftp_client,
                &r.v,
                r.s,
                r.e,
            )
            .await
        }
        ProviderKind::BdixDhakaFlix => {
            crate::providers::ReleaseProvider::episode_streams(
                &svc.dhakaflix_client,
                &r.v,
                r.s,
                r.e,
            )
            .await
        }
        ProviderKind::Addons => {
            let addons = tokio::task::spawn_blocking(crate::config::load_addons)
                .await
                .unwrap_or_default();
            if !addons
                .iter()
                .any(|addon| addon.enabled && addon.provides_stream)
            {
                return Err(ProviderError::Unavailable(
                    "No enabled stream addon is configured.".into(),
                ));
            }
            let (releases, _blocked) = crate::providers::addons::aggregate_streams(
                &svc.addon_client,
                &addons,
                &r.v,
                r.s,
                r.e,
                true,
            )
            .await;
            if releases.is_empty() {
                Err(ProviderError::Unavailable(
                    "No addon stream sources available".into(),
                ))
            } else {
                Ok(releases)
            }
        }
    }
}
fn api_sources(rs: &[Release], episode: &OpaqueRef) -> Vec<ApiSource> {
    rs.iter()
        .enumerate()
        .map(|(i, r)| ApiSource {
            id: source_ref(r, episode, i),
            label: r.source_label().to_string(),
            quality: r.quality.clone(),
            codec: r.codec.clone(),
            language: r.language.clone(),
            size_bytes: r.size_bytes,
            downloadable: r.mirrors.iter().any(|m| m.direct_file),
            expires_at: None,
        })
        .collect()
}
async fn episode_route(stream: &mut TcpStream, path: &str, state: ApiState) -> std::io::Result<()> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() < 4 {
        let (s, b) = err(
            "INVALID_ID",
            "An episode identifier is required.",
            None,
            400,
        );
        return write_raw_json(stream, s, &b, None).await;
    }
    let r = parse_ref(parts[3]);
    let Ok(r) = r else {
        let (s, b) = r.unwrap_err();
        return write_raw_json(stream, s, &b, None).await;
    };
    if parts.get(4) == Some(&"sources") {
        if parts.get(5) == Some(&"stream") {
            return source_sse(stream, r, state).await;
        }
        let result = tokio::time::timeout(OVERALL_TIMEOUT, releases(&state, &r)).await;
        return match result {
            Ok(Ok(rs)) => {
                write_json(
                    stream,
                    200,
                    &SourcesResponse {
                        sources: api_sources(&rs, &r),
                        partial: false,
                    },
                    None,
                )
                .await
            }
            Ok(Err(e)) => {
                let (s, b) = provider_error(e, r.p);
                write_raw_json(stream, s, &b, None).await
            }
            Err(_) => {
                let (s, b) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The request deadline was exceeded.",
                    Some(r.p),
                    504,
                );
                write_raw_json(stream, s, &b, None).await
            }
        };
    }
    let (s, b) = err("INTERNAL_ERROR", "Unsupported episode route.", None, 404);
    write_raw_json(stream, s, &b, None).await
}
async fn source_sse(stream: &mut TcpStream, r: OpaqueRef, state: ApiState) -> std::io::Result<()> {
    write_headers(stream, 200, "text/event-stream", None).await?;
    match tokio::time::timeout(OVERALL_TIMEOUT, releases(&state, &r)).await {
        Ok(Ok(rs)) => {
            for x in rs {
                send_event(
                    stream,
                    "source_result",
                    &serde_json::json!({"source":api_sources(&[x],&r)}).to_string(),
                )
                .await?;
            }
            send_event(stream, "source_complete", "{\"partial\":false}").await
        }
        Ok(Err(e)) => {
            let code = match &e {
                ProviderError::Unavailable(message)
                    if r.p == ProviderKind::Addons
                        && message == "No enabled stream addon is configured." =>
                {
                    "NO_STREAM_ADDON"
                }
                ProviderError::Unavailable(message)
                    if r.p == ProviderKind::Addons
                        && message == "No addon stream sources available" =>
                {
                    "NO_SOURCE_CANDIDATES"
                }
                _ => "UPSTREAM_UNAVAILABLE",
            };
            send_event(
                stream,
                "source_error",
                &serde_json::json!({"code":code,"provider":r.p.label(),"message":e.user_message(r.p)}).to_string(),
            )
            .await?;
            send_event(stream, "source_complete", "{\"partial\":true}").await
        }
        Err(_) => {
            send_event(
                stream,
                "source_error",
                "{\"code\":\"REQUEST_DEADLINE_EXCEEDED\"}",
            )
            .await
        }
    }
}
async fn probe_media(url: &str, headers: &[(String, String)]) -> Result<ProbeOutput, String> {
    let mut command = Command::new("ffprobe");
    command
        .arg("-v")
        .arg("error")
        .arg("-show_entries")
        .arg("stream=index,codec_name,codec_type,channels,disposition:stream_tags=language,title,handler_name")
        .arg("-of")
        .arg("json")
        .arg("-timeout")
        .arg("15000000");
    if !headers.is_empty() {
        let header_text = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}"))
            .collect::<Vec<_>>()
            .join("\r\n");
        command.arg("-headers").arg(header_text);
    }
    command.arg(url);
    let output = tokio::time::timeout(TRACK_PROBE_TIMEOUT, command.output())
        .await
        .map_err(|_| "Media track probing timed out.".to_string())?
        .map_err(|_| "The ffprobe media tool is unavailable.".to_string())?;
    if !output.status.success() {
        return Err("The media source could not be probed.".to_string());
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|_| "The media probe returned malformed metadata.".to_string())
}

fn tag_value<'a>(tags: Option<&'a HashMap<String, String>>, key: &str) -> Option<&'a str> {
    tags?
        .iter()
        .find_map(|(name, value)| name.eq_ignore_ascii_case(key).then_some(value.as_str()))
}

fn normalized_language(raw: Option<&str>) -> (String, String, Option<String>) {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return ("und".into(), "Unknown".into(), None);
    };
    let original = Some(raw.to_string());
    match raw.to_ascii_lowercase().as_str() {
        "hi" | "hin" => ("hi".into(), "Hindi".into(), original),
        "en" | "eng" => ("en".into(), "English".into(), original),
        "ja" | "jpn" => ("ja".into(), "Japanese".into(), original),
        "ko" | "kor" => ("ko".into(), "Korean".into(), original),
        "zh" | "zho" | "chi" => ("zh".into(), "Chinese".into(), original),
        "es" | "spa" => ("es".into(), "Spanish".into(), original),
        "fr" | "fra" | "fre" => ("fr".into(), "French".into(), original),
        "de" | "deu" | "ger" => ("de".into(), "German".into(), original),
        "und" => ("und".into(), "Unknown".into(), original),
        _ => (raw.to_ascii_lowercase(), raw.to_string(), original),
    }
}

async fn write_track_response(
    stream: &mut TcpStream,
    source_id: &str,
    provider: ProviderKind,
    release: &Release,
    subtitles: bool,
) -> std::io::Result<()> {
    let Some(url) = release.direct_url() else {
        let (s, b) = err(
            "TRACK_DISCOVERY_UNAVAILABLE",
            "This source has no directly probeable media URL.",
            Some(provider),
            503,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    let headers = release
        .mirrors
        .first()
        .map(|mirror| mirror.headers.as_slice())
        .unwrap_or(&[]);
    let probe = match probe_media(url, headers).await {
        Ok(probe) => probe,
        Err(message) => {
            let (s, b) = err("TRACK_DISCOVERY_UNAVAILABLE", message, Some(provider), 503);
            return write_raw_json(stream, s, &b, None).await;
        }
    };
    if subtitles {
        let tracks = probe
            .streams
            .into_iter()
            .filter(|stream| stream.codec_type.as_deref() == Some("subtitle"))
            .map(|stream| {
                let (language_code, language_name, original_language) =
                    normalized_language(tag_value(stream.tags.as_ref(), "language"));
                ApiSubtitleTrack {
                    subtitle_index: stream.index.unwrap_or_default(),
                    language_code,
                    language_name,
                    original_language,
                    format: stream.codec_name,
                    kind: "embedded",
                    is_default: stream
                        .disposition
                        .is_some_and(|disposition| disposition.default > 0),
                }
            })
            .collect::<Vec<_>>();
        return write_json(
            stream,
            200,
            &SubtitleTracksResponse {
                source_id: source_id.to_string(),
                media_type: "video",
                status: "confirmed",
                available: !tracks.is_empty(),
                subtitles: tracks,
            },
            None,
        )
        .await;
    }
    let tracks = probe
        .streams
        .into_iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
        .map(|stream| {
            let (language_code, language_name, original_language) =
                normalized_language(tag_value(stream.tags.as_ref(), "language"));
            ApiAudioTrack {
                track_index: stream.index.unwrap_or_default(),
                language_code,
                language_name,
                original_language,
                codec: stream.codec_name,
                channels: stream.channels,
                is_default: stream
                    .disposition
                    .is_some_and(|disposition| disposition.default > 0),
            }
        })
        .collect::<Vec<_>>();
    write_json(
        stream,
        200,
        &AudioTracksResponse {
            source_id: source_id.to_string(),
            media_type: "video",
            status: "confirmed",
            available: !tracks.is_empty(),
            tracks,
        },
        None,
    )
    .await
}

async fn source_route(stream: &mut TcpStream, path: &str, state: ApiState) -> std::io::Result<()> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    let Some(id) = parts.get(3) else {
        let (s, b) = err("INVALID_ID", "A source identifier is required.", None, 400);
        return write_raw_json(stream, s, &b, None).await;
    };
    let sr = parse_source(id);
    let Ok(sr) = sr else {
        let (s, b) = sr.unwrap_err();
        return write_raw_json(stream, s, &b, None).await;
    };
    let r = OpaqueRef {
        p: sr.p,
        v: sr.v.clone(),
        s: sr.s,
        e: sr.e,
    };
    let Ok(rs) = tokio::time::timeout(SOURCE_TIMEOUT, releases(&state, &r))
        .await
        .unwrap_or(Err(ProviderError::Unavailable(
            "Source resolution timed out".into(),
        )))
    else {
        let (s, b) = err(
            "SOURCE_TIMEOUT",
            "Source resolution exceeded the allowed time.",
            Some(sr.p),
            504,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    let Some(rel) = rs.get(sr.i) else {
        let (s, b) = err(
            "SOURCE_NOT_FOUND",
            "The source was not found.",
            Some(sr.p),
            404,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    if matches!(parts.get(4), Some(&"audio") | Some(&"audio-tracks")) {
        return write_track_response(stream, id, sr.p, rel, false).await;
    }
    if matches!(parts.get(4), Some(&"subtitles") | Some(&"subtitle-tracks")) {
        return write_track_response(stream, id, sr.p, rel, true).await;
    }
    if parts.get(4) == Some(&"download") {
        return if rel.mirrors.iter().any(|m| m.direct_file) {
            write_json(
                stream,
                200,
                &serde_json::json!({"supported":true,"mirrors":rel.mirrors}),
                None,
            )
            .await
        } else {
            let (s, b) = err(
                "UNSUPPORTED_OPERATION",
                "This source does not advertise an authorized direct download.",
                Some(sr.p),
                501,
            );
            write_raw_json(stream, s, &b, None).await
        };
    }
    write_json(stream,200,&serde_json::json!({"source":api_sources(std::slice::from_ref(rel),&r),"url":rel.direct_url(),"headers":rel.mirrors.first().map(|m|&m.headers)}),None).await
}

async fn write_headers(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    extra: Option<&str>,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        429 => "Too Many Requests",
        _ => "Error",
    };
    let mut h = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nCache-Control: no-cache\r\nConnection: close\r\n"
    );
    if let Some(x) = extra {
        h.push_str(x)
    };
    h.push_str("\r\n");
    stream.write_all(h.as_bytes()).await
}
async fn write_json<T: Serialize>(
    stream: &mut TcpStream,
    status: u16,
    value: &T,
    extra: Option<&str>,
) -> std::io::Result<()> {
    let body = serde_json::to_string(value).unwrap();
    write_headers(stream, status, "application/json", extra).await?;
    stream.write_all(body.as_bytes()).await
}
async fn write_raw_json(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    extra: Option<&str>,
) -> std::io::Result<()> {
    write_headers(stream, status, "application/json", extra).await?;
    stream.write_all(body.as_bytes()).await
}
async fn write_text(
    stream: &mut TcpStream,
    status: u16,
    ct: &str,
    body: &str,
    extra: Option<&str>,
) -> std::io::Result<()> {
    write_headers(stream, status, ct, extra).await?;
    stream.write_all(body.as_bytes()).await
}
async fn send_event(stream: &mut TcpStream, event: &str, data: &str) -> std::io::Result<()> {
    stream
        .write_all(format!("event: {event}\ndata: {data}\n\n").as_bytes())
        .await
}
fn openapi() -> serde_json::Value {
    serde_json::json!({
        "openapi": "3.0.3",
        "info": {"title": "MovieBox API", "version": "1.0.0"},
        "paths": {
            "/": {"get": {"responses": {"200": {"description": "API information"}}}},
            "/health": {"get": {"responses": {"200": {"description": "Liveness"}}}},
            "/ready": {"get": {"responses": {"200": {"description": "Readiness"}}}},
            "/openapi.json": {"get": {"responses": {"200": {"description": "OpenAPI document"}}}},
            "/docs": {"get": {"responses": {"200": {"description": "API documentation"}}}},
            "/api/v1/anime/search": {"get": {"parameters": [{"name": "q", "in": "query", "required": true, "schema": {"type": "string", "maxLength": 200}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100}}, {"name": "cursor", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Search results"}, "400": {"description": "Invalid query"}}}},
            "/api/v1/anime/search/stream": {"get": {"responses": {"200": {"description": "SSE search_result, search_provider_error, search_complete events"}}}},
            "/api/v1/anime/{anime_id}": {"get": {"responses": {"200": {"description": "Anime details"}}}},
            "/api/v1/anime/{anime_id}/seasons": {"get": {"responses": {"200": {"description": "Seasons"}}}},
            "/api/v1/anime/{anime_id}/episodes": {"get": {"responses": {"200": {"description": "Episodes"}}}},
            "/api/v1/episodes/{episode_id}/sources": {"get": {"responses": {"200": {"description": "Fresh source candidates"}}}},
            "/api/v1/episodes/{episode_id}/sources/stream": {"get": {"responses": {"200": {"description": "SSE source_result, source_error, source_complete events"}}}},
            "/api/v1/sources/{source_id}": {"get": {"responses": {"200": {"description": "Resolved source metadata"}}}},
            "/api/v1/sources/{source_id}/audio": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/audio-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitles": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitle-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/download": {"get": {"responses": {"200": {"description": "Download metadata"}, "501": {"description": "Unsupported"}}}}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_reference_round_trip_preserves_internal_identity() {
        let original = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "56914".into(),
            s: 1,
            e: 2,
        };
        let public = encode(&original);
        assert!(!public.contains("dramachi"));
        assert_eq!(decode::<OpaqueRef>(&public).unwrap().v, "56914");
    }

    #[test]
    fn addons_episode_id_round_trip_preserves_provider_and_subject() {
        let media = MediaDetails {
            id: ProviderMediaId {
                provider: ProviderKind::Addons,
                value: "series:tt15716676".into(),
            },
            title: "Devil May Cry".into(),
            media_type: crate::providers::models::MediaType::Series,
            year: Some("2025".into()),
            description: None,
            tagline: None,
            imdb_rating: None,
            director: None,
            stars: None,
            prints: None,
            audios: None,
            poster_url: None,
            duration: None,
            genres: vec![],
            seasons: vec![Season {
                number: 1,
                episodes: vec![crate::providers::models::Episode {
                    season: 1,
                    number: 5,
                    title: Some("Descent".into()),
                    overview: None,
                }],
            }],
            dubs: vec![],
        };
        let episode = details(media).seasons.remove(0).episodes.remove(0);
        let decoded = decode::<OpaqueRef>(&episode.id).unwrap();
        assert_eq!(decoded.p, ProviderKind::Addons);
        assert_eq!(decoded.v, "series:tt15716676");
        assert_eq!((decoded.s, decoded.e), (1, 5));
    }

    #[test]
    fn moviebox_episode_id_round_trip_preserves_subject_and_provider() {
        let id = ProviderMediaId {
            provider: ProviderKind::MovieBox,
            value: "29706226128880560".into(),
        };
        let episode = crate::providers::models::Episode {
            season: 1,
            number: 5,
            title: None,
            overview: None,
        };
        let decoded = decode::<OpaqueRef>(&episode_ref(&id, &episode)).unwrap();
        assert_eq!(decoded.p, ProviderKind::MovieBox);
        assert_eq!(decoded.v, "29706226128880560");
        assert_eq!((decoded.s, decoded.e), (1, 5));
    }

    #[test]
    fn source_lookup_identity_round_trip_keeps_provider_dispatch() {
        let episode = OpaqueRef {
            p: ProviderKind::Addons,
            v: "series:tt15716676".into(),
            s: 1,
            e: 5,
        };
        let release = Release {
            provider: ProviderKind::Addons,
            filename: "Descent.1080p.mkv".into(),
            quality: Some("1080p".into()),
            codec: None,
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(5),
            mirrors: vec![],
            resource_id: None,
        };
        let source = source_ref(&release, &episode, 0);
        let decoded = decode::<OpaqueSource>(&source).unwrap();
        assert_eq!(decoded.p, ProviderKind::Addons);
        assert_eq!(decoded.v, episode.v);
        assert_eq!((decoded.s, decoded.e, decoded.i), (1, 5, 0));
    }

    #[test]
    fn malformed_and_unresolvable_provider_ids_are_rejected_or_preserved() {
        assert!(parse_ref("not-an-id").is_err());
        assert!(parse_source("not-a-source-id").is_err());
        let unresolved = OpaqueRef {
            p: ProviderKind::Addons,
            v: "series:provider-specific-id-that-does-not-exist".into(),
            s: 1,
            e: 5,
        };
        let round_trip = decode::<OpaqueRef>(&encode(&unresolved)).unwrap();
        assert_eq!(round_trip.p, ProviderKind::Addons);
        assert_eq!(round_trip.v, unresolved.v);
    }

    #[test]
    fn provider_priority_matches_configured_order() {
        assert!(
            provider_priority(ProviderKind::Dramachi) < provider_priority(ProviderKind::Addons)
        );
        assert!(
            provider_priority(ProviderKind::FourKHdHub) < provider_priority(ProviderKind::MovieBox)
        );
        assert!(
            provider_priority(ProviderKind::MovieBox)
                < provider_priority(ProviderKind::BdixCircleFtp)
        );
    }

    #[test]
    fn addons_configuration_errors_are_specific_and_safe() {
        let (status, body) = provider_error(
            ProviderError::Unavailable("No enabled stream addon is configured.".into()),
            ProviderKind::Addons,
        );
        assert_eq!(status, 503);
        assert!(body.contains("NO_STREAM_ADDON"));
        assert!(!body.contains("manifest") && !body.contains("cookie"));

        let (status, body) = provider_error(
            ProviderError::Unavailable("No addon stream sources available".into()),
            ProviderKind::Addons,
        );
        assert_eq!(status, 404);
        assert!(body.contains("NO_SOURCE_CANDIDATES"));
    }

    #[test]
    fn track_language_normalization_preserves_original_tags() {
        assert_eq!(
            normalized_language(Some("hin")),
            ("hi".into(), "Hindi".into(), Some("hin".into()))
        );
        assert_eq!(
            normalized_language(Some("jpn")),
            ("ja".into(), "Japanese".into(), Some("jpn".into()))
        );
        assert_eq!(
            normalized_language(None),
            ("und".into(), "Unknown".into(), None)
        );
    }

    #[test]
    fn probe_metadata_separates_audio_and_subtitle_streams() {
        let probe: ProbeOutput = serde_json::from_value(serde_json::json!({
            "streams": [
                {"index": 0, "codec_type": "video", "codec_name": "hevc"},
                {"index": 1, "codec_type": "audio", "codec_name": "aac", "channels": 2,
                 "tags": {"language": "hin"}, "disposition": {"default": 1}},
                {"index": 2, "codec_type": "audio", "codec_name": "aac", "channels": 2,
                 "tags": {"language": "jpn"}},
                {"index": 3, "codec_type": "subtitle", "codec_name": "subrip",
                 "tags": {"language": "eng"}}
            ]
        }))
        .unwrap();
        let audio = probe
            .streams
            .iter()
            .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
            .collect::<Vec<_>>();
        let subtitles = probe
            .streams
            .iter()
            .filter(|stream| stream.codec_type.as_deref() == Some("subtitle"))
            .collect::<Vec<_>>();
        assert_eq!(audio.len(), 2);
        assert_eq!(subtitles.len(), 1);
        assert_eq!(
            normalized_language(tag_value(audio[0].tags.as_ref(), "language")).0,
            "hi"
        );
        assert_eq!(audio[0].channels, Some(2));
        assert_eq!(subtitles[0].codec_name.as_deref(), Some("subrip"));
    }

    #[test]
    fn search_query_limits_are_bounded() {
        let query = parse_query("q=Naruto&limit=999&cursor=bad");
        assert_eq!(query.q.as_deref(), Some("Naruto"));
        assert_eq!(query.limit, Some(999));
        assert!(decode::<usize>(query.cursor.as_deref().unwrap()).is_err());
    }

    #[test]
    fn openapi_contains_registered_system_and_sse_routes() {
        let document = openapi();
        let paths = document.get("paths").unwrap();
        for route in [
            "/health",
            "/ready",
            "/openapi.json",
            "/api/v1/anime/search/stream",
            "/api/v1/episodes/{episode_id}/sources/stream",
            "/api/v1/sources/{source_id}/audio-tracks",
            "/api/v1/sources/{source_id}/subtitle-tracks",
        ] {
            assert!(paths.get(route).is_some(), "missing {route}");
        }
    }
}
