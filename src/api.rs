#![allow(
    dead_code,
    reason = "MovieBox catalog handlers remain compiled for a controlled future re-enable while the provider is intentionally disabled"
)]

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{Mutex, Semaphore},
    task::JoinSet,
};

use crate::{
    providers::models::{
        CatalogItem, MediaDetails, MediaType, PlaybackSource, ProviderError, ProviderKind,
        ProviderMediaId, Release, Season,
    },
    service::MovieBoxService,
};

const MAX_QUERY: usize = 200;
const DEFAULT_LIMIT: usize = 20;
const MAX_LIMIT: usize = 100;
const SOURCE_TIMEOUT: Duration = Duration::from_secs(15);
const OVERALL_TIMEOUT: Duration = Duration::from_secs(45);
const SEARCH_SOURCE_CHECK_TIMEOUT: Duration = Duration::from_secs(45);
const SEARCH_SOURCE_CHECK_CONCURRENCY: usize = 8;
const TRACK_PROBE_TIMEOUT: Duration = Duration::from_secs(45);
const RELEASE_CACHE_TTL: Duration = Duration::from_secs(600);
const PLAYBACK_CACHE_TTL: Duration = Duration::from_secs(120);
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_API_CONNECTIONS: usize = 256;
const MOVIEBOX_PROVIDER_ENABLED: bool = true;
const MAX_TRANSCODE_SESSIONS: usize = 2;
const TRANSCODE_STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const PROVIDERS: [ProviderKind; 6] = [
    ProviderKind::MovieBox,
    ProviderKind::FourKHdHub,
    ProviderKind::Dramachi,
    ProviderKind::BdixCircleFtp,
    ProviderKind::BdixDhakaFlix,
    ProviderKind::Addons,
];

fn moviebox_provider_enabled() -> bool {
    MOVIEBOX_PROVIDER_ENABLED && crate::config::load().moviebox_enabled
}

fn configured_search_providers() -> Vec<ProviderKind> {
    let config = crate::config::load();
    let mut providers = Vec::with_capacity(PROVIDERS.len());
    if moviebox_provider_enabled() && config.moviebox_enabled {
        providers.push(ProviderKind::MovieBox);
    }
    if config.fourkhdhub_enabled {
        providers.push(ProviderKind::FourKHdHub);
    }
    if config.dramachi_enabled {
        providers.push(ProviderKind::Dramachi);
    }
    if config.bdix_circleftp_enabled {
        providers.push(ProviderKind::BdixCircleFtp);
    }
    if config.bdix_dhakaflix_enabled {
        providers.push(ProviderKind::BdixDhakaFlix);
    }
    // Catalog/metadata addons remain useful without stream-capable addons.
    providers.push(ProviderKind::Addons);
    providers
}

fn provider_priority(provider: ProviderKind) -> usize {
    PROVIDERS
        .iter()
        .position(|candidate| *candidate == provider)
        .unwrap_or(PROVIDERS.len())
}

#[derive(Clone)]
pub struct ApiState {
    pub service: Arc<MovieBoxService>,
    release_cache: Arc<Mutex<HashMap<String, ReleaseCacheEntry>>>,
    playback_cache: Arc<Mutex<HashMap<String, PlaybackCacheEntry>>>,
    search_source_check_slots: Arc<Semaphore>,
    transcode_slots: Arc<Semaphore>,
    connection_slots: Arc<Semaphore>,
}

#[derive(Clone)]
struct ReleaseCacheEntry {
    stored_at: Instant,
    releases: Vec<Release>,
}

#[derive(Clone)]
struct PlaybackCacheEntry {
    stored_at: Instant,
    playback: PlaybackSource,
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
    #[serde(default)]
    f: Option<String>,
    #[serde(default)]
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
    provider_status: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language_intent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verified_language_matches: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceEligibility {
    Playable,
    NoSourceConfirmed,
    CheckFailed,
}

#[derive(Debug)]
struct EligibleSearch {
    items: Vec<CatalogItem>,
    failed: bool,
    provider_status: HashMap<String, String>,
}

fn release_has_usable_source(release: &Release) -> bool {
    is_verified_h264_codec(release.codec.as_deref())
        && release.mirrors.iter().any(|mirror| {
            !mirror.resolver_url.trim().is_empty()
                && crate::net::validate_url_syntax(&mirror.resolver_url, true).is_ok()
        })
}

fn is_verified_h264_codec(codec: Option<&str>) -> bool {
    let Some(codec) = codec.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let normalized = codec.to_ascii_lowercase().replace(['.', '-', '_', ' '], "");
    matches!(
        normalized.as_str(),
        "h264" | "avc" | "avc1" | "avc3" | "x264"
    )
}

fn releases_have_usable_source(releases: &[Release]) -> bool {
    releases.iter().any(release_has_usable_source)
}

fn provider_error_means_no_source(error: &ProviderError) -> bool {
    matches!(error, ProviderError::NotFound)
        || matches!(
            error,
            ProviderError::Unavailable(message)
                if message == "No addon stream sources available"
        )
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
struct CatalogItemResponse {
    id: String,
    title: String,
    provider: ProviderKind,
    media_type: MediaType,
    year: Option<String>,
    poster_url: Option<String>,
    season_count: Option<usize>,
}

#[derive(Debug, Serialize)]
struct CatalogSectionResponse {
    id: String,
    title: String,
    provider: ProviderKind,
    page: usize,
    next_page: Option<usize>,
    ranking_basis: &'static str,
    items: Vec<CatalogItemResponse>,
}

#[derive(Debug, Serialize)]
struct HomeResponse {
    sections: Vec<CatalogSectionResponse>,
    page: usize,
    provider_status: HashMap<String, String>,
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
#[derive(Debug, Clone, Serialize)]
struct ApiEpisode {
    id: String,
    season: usize,
    number: usize,
    title: Option<String>,
    overview: Option<String>,
}
#[derive(Debug, Serialize)]
struct AnimeEpisodeFullResponse {
    anime: ApiDetails,
    selected_episode: ApiEpisode,
    sources: Vec<ApiSource>,
    sources_status: &'static str,
    sources_partial: bool,
    source_error: Option<serde_json::Value>,
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
    // Public IDs may come from older clients/providers that retain Base64 padding.
    // Accept both canonical unpadded IDs and padded URL-safe Base64 IDs.
    let bytes = URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| URL_SAFE.decode(value))
        .map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}
fn provider_ref(id: &ProviderMediaId, s: usize, e: usize) -> String {
    encode(&OpaqueRef {
        p: id.provider,
        v: id.value.clone(),
        s,
        e,
    })
}
fn release_fingerprint(r: &Release) -> String {
    let mut hasher = Sha256::new();
    let size = r
        .size_bytes
        .map(|value| value.to_string())
        .unwrap_or_default();
    for value in [
        r.provider.label(),
        &r.filename,
        r.quality.as_deref().unwrap_or_default(),
        r.codec.as_deref().unwrap_or_default(),
        r.language.as_deref().unwrap_or_default(),
        &size,
        r.resource_id.as_deref().unwrap_or_default(),
    ] {
        hasher.update(value.as_bytes());
        hasher.update([0]);
    }
    for mirror in &r.mirrors {
        hasher.update(mirror.label.as_bytes());
        hasher.update([0]);
        hasher.update(mirror.resolver_url.as_bytes());
        hasher.update([u8::from(mirror.direct_file)]);
        hasher.update([0]);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}
fn source_ref(r: &Release, episode: &OpaqueRef, i: usize) -> String {
    encode(&OpaqueSource {
        p: r.provider,
        v: episode.v.clone(),
        s: episode.s,
        e: episode.e,
        f: Some(release_fingerprint(r)),
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
        release_cache: Arc::new(Mutex::new(HashMap::new())),
        playback_cache: Arc::new(Mutex::new(HashMap::new())),
        search_source_check_slots: Arc::new(Semaphore::new(SEARCH_SOURCE_CHECK_CONCURRENCY)),
        transcode_slots: Arc::new(Semaphore::new(MAX_TRANSCODE_SESSIONS)),
        connection_slots: Arc::new(Semaphore::new(MAX_API_CONNECTIONS)),
    };
    log::info!("MovieBox API listening on 0.0.0.0:{port}");
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(connection_permit) = state.connection_slots.clone().try_acquire_owned() else {
            let mut stream = stream;
            let _ = write_json(
                &mut stream,
                503,
                &serde_json::json!({
                    "error": {
                        "code": "SERVER_BUSY",
                        "message": "The server is temporarily at capacity."
                    }
                }),
                Some("Connection: close\r\n"),
            )
            .await;
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let _connection_permit = connection_permit;
            if let Err(e) = handle(stream, state).await {
                log::debug!("request ended: {e}");
            }
        });
    }
}

async fn handle(mut stream: TcpStream, state: ApiState) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(REQUEST_READ_TIMEOUT, stream.read(&mut chunk))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "request read timed out")
            })??;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_REQUEST_BYTES {
            return write_json(
                &mut stream,
                431,
                &serde_json::json!({
                    "error": {
                        "code": "REQUEST_HEADERS_TOO_LARGE",
                        "message": "The request headers exceed the permitted size."
                    }
                }),
                Some("Connection: close\r\n"),
            )
            .await;
        }
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let raw = String::from_utf8_lossy(&buf);
    let mut lines = raw.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("");
    let target = parts.next().unwrap_or("/");
    let version = parts.next().unwrap_or("");
    if method.is_empty() || target.is_empty() || version != "HTTP/1.1" || parts.next().is_some() {
        return write_json(
            &mut stream,
            400,
            &serde_json::json!({"error":{"code":"MALFORMED_REQUEST","message":"The HTTP request line is invalid."}}),
            Some("Connection: close\r\n"),
        )
        .await;
    }
    if method != "GET" {
        return write_json(&mut stream, 405, &serde_json::json!({"error":{"code":"METHOD_NOT_ALLOWED","message":"Only GET is supported."}}), None).await;
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
        "/api/v1/diagnostics" => diagnostics_route(stream, state).await,
        "/api/v1/watch" => direct_watch_route(stream, query, state).await,
        "/api/v1/anime/search" => search_json(stream, query, state).await,
        "/api/v1/anime/search/stream" => search_sse(stream, query, state).await,
        "/api/v1/home" | "/api/v1/home/featured" | "/api/v1/trending"
        | "/api/v1/trending/anime" | "/api/v1/trending/movies"
        | "/api/v1/trending/k-dramas" | "/api/v1/popular"
        | "/api/v1/popular/anime" | "/api/v1/popular/movies"
        | "/api/v1/popular/k-dramas" if !moviebox_provider_enabled() => {
            moviebox_disabled(stream).await
        }
        "/api/v1/latest" => unsupported_catalog(stream, "Reliable release-date metadata is not exposed consistently by the available providers.").await,
        "/api/v1/movies" | "/api/v1/k-dramas" | "/api/v1/categories" if !moviebox_provider_enabled() => {
            moviebox_disabled(stream).await
        }
        _ if path.starts_with("/api/v1/movies/") && !moviebox_provider_enabled() => moviebox_disabled(stream).await,
        _ if path.starts_with("/api/v1/k-dramas/") => unsupported_catalog(stream, "K-Drama details require a verified item classification before lookup.").await,
        _ if path.starts_with("/api/v1/anime/") => anime_route(stream, path, query, state).await,
        _ if path.starts_with("/api/v1/episodes/") => episode_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/sources/") => source_route(stream, path, state).await,
        _ => { let (s,b) = err("INTERNAL_ERROR", "Route not found.", None, 404); write_raw_json(stream, s, &b, None).await }
    }
}

async fn diagnostics_route(stream: &mut TcpStream, state: ApiState) -> std::io::Result<()> {
    // Capture whether the config existed before load_addons() applies its local
    // defaults. Never expose manifest URLs or other configuration values.
    let addons_config_file_present =
        crate::config::addons_path().is_some_and(|path| path.is_file());
    let addons = tokio::task::spawn_blocking(crate::config::load_addons)
        .await
        .unwrap_or_default();
    let enabled_stream_addons = addons
        .iter()
        .filter(|addon| addon.enabled && addon.provides_stream)
        .count();
    let enabled_catalog_addons = addons
        .iter()
        .filter(|addon| addon.enabled && (addon.provides_catalog || addon.provides_meta))
        .count();
    let config = tokio::task::spawn_blocking(crate::config::load)
        .await
        .unwrap_or_default();
    let built_in = serde_json::json!({
        "fourkhdhub": { "configured": config.fourkhdhub_enabled, "initialized": state.service.fourk_client.is_some(), "upstream": "not_checked" },
        "dramachi": { "configured": config.dramachi_enabled, "initialized": true, "upstream": "not_checked" },
        "bdix_circleftp": { "configured": config.bdix_circleftp_enabled, "initialized": true, "upstream": "not_checked" },
        "bdix_dhakaflix": { "configured": config.bdix_dhakaflix_enabled, "initialized": true, "upstream": "not_checked" }
    });
    let built_in_streaming = config.streaming_enabled
        && ((config.fourkhdhub_enabled && state.service.fourk_client.is_some())
            || config.dramachi_enabled
            || config.bdix_circleftp_enabled
            || config.bdix_dhakaflix_enabled);

    write_json(
        stream,
        200,
        &serde_json::json!({
            "status": if built_in_streaming || enabled_stream_addons > 0 { "ok" } else { "degraded" },
            "process": {"status": "ok"},
            "configuration": {
                "addons_config_file_present_at_start_of_check": addons_config_file_present,
                "enabled_stream_addons": enabled_stream_addons,
                "enabled_catalog_or_metadata_addons": enabled_catalog_addons,
                "streaming_configured": built_in_streaming || enabled_stream_addons > 0,
                "streaming_enabled": config.streaming_enabled,
                "moviebox_configured": config.moviebox_enabled,
                "moviebox_effective": moviebox_provider_enabled()
            },
            "built_in_providers": built_in,
            "external_addons": {
                "catalog_or_metadata": if enabled_catalog_addons > 0 { "configured" } else { "unavailable" },
                "streaming": if enabled_stream_addons > 0 { "configured" } else { "unconfigured" }
            },
            "streaming": {
                "status": if built_in_streaming || enabled_stream_addons > 0 { "available" } else { "unavailable" },
                "basis": if built_in_streaming { "built_in_provider_initialized" } else if enabled_stream_addons > 0 { "stream_addon_configured" } else { "no_initialized_provider_or_stream_addon" },
                "upstream_checks": "request_specific"
            },
            "hint": if enabled_stream_addons == 0 {
                Some("No stream-capable addon is enabled; built-in provider streaming is independent and is checked per request.")
            } else {
                None
            }
        }),
        None,
    )
    .await
}

fn catalog_item_response(item: &CatalogItem) -> CatalogItemResponse {
    CatalogItemResponse {
        id: encode(&OpaqueRef {
            p: item.id.provider,
            v: item.id.value.clone(),
            s: 0,
            e: 0,
        }),
        title: item.title.clone(),
        provider: item.id.provider,
        media_type: item.media_type,
        year: item.year.clone(),
        poster_url: item.poster_url.clone(),
        season_count: item.season_count,
    }
}

fn parse_page_limit(query: &str) -> (usize, usize) {
    let mut page = 1usize;
    let mut limit = DEFAULT_LIMIT;
    for part in query.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key {
            "page" => page = value.parse().unwrap_or(1).clamp(1, 1000),
            "limit" => limit = value.parse().unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
            _ => {}
        }
    }
    (page, limit)
}

async fn homepage_items(
    state: &ApiState,
    page: usize,
) -> Result<
    (
        Vec<CatalogItem>,
        HashMap<String, crate::models::BrowseMetrics>,
    ),
    String,
> {
    tokio::time::timeout(OVERALL_TIMEOUT, homepage_items_uncapped(state, page))
        .await
        .map_err(|_| "MovieBox homepage request timed out.".to_string())?
}

async fn homepage_items_uncapped(
    state: &ApiState,
    page: usize,
) -> Result<
    (
        Vec<CatalogItem>,
        HashMap<String, crate::models::BrowseMetrics>,
    ),
    String,
> {
    let (movies, dramas, anime) = tokio::join!(
        state.service.homepage("2", page),
        state.service.homepage("5", page),
        state.service.homepage("8", page),
    );
    let mut items = Vec::new();
    let mut metrics: HashMap<String, crate::models::BrowseMetrics> = HashMap::new();
    let mut seen = std::collections::HashSet::new();
    for result in [movies, dramas, anime] {
        let (batch, batch_metrics) = result.map_err(|error| error.to_string())?;
        for item in batch {
            let id = item.id.value.clone();
            if seen.insert(id.clone()) {
                items.push(item);
            }
            let incoming = batch_metrics.get(&id).cloned().unwrap_or_default();
            let stored = metrics.entry(id).or_default();
            stored.trending = stored.trending.or(incoming.trending);
            stored.rating = stored.rating.or(incoming.rating);
            stored.recent_rating = stored.recent_rating.or(incoming.recent_rating);
            stored.popularity = stored.popularity.or(incoming.popularity);
            if stored.genres.is_empty() {
                stored.genres = incoming.genres;
            }
            stored.category = stored.category.clone().or(incoming.category);
            stored.country = stored.country.clone().or(incoming.country);
            stored.original_language = stored
                .original_language
                .clone()
                .or(incoming.original_language);
        }
    }

    let semaphore = Arc::new(Semaphore::new(8));
    let mut tasks = JoinSet::new();
    for item in items
        .iter()
        .filter(|item| item.media_type == MediaType::Series)
    {
        let service = state.service.clone();
        let semaphore = semaphore.clone();
        let id = item.id.value.clone();
        tasks.spawn(async move {
            let permit = semaphore.acquire_owned().await.ok()?;
            let metadata = service.moviebox_classification_metadata(&id).await;
            drop(permit);
            Some((id, metadata))
        });
    }
    while let Some(result) = tasks.join_next().await {
        if let Ok(Some((id, Some((category, country, language))))) = result {
            if let Some(stored) = metrics.get_mut(&id) {
                stored.category = stored.category.clone().or(category);
                stored.country = stored.country.clone().or(country);
                stored.original_language = stored.original_language.clone().or(language);
            }
        }
    }
    Ok((items, metrics))
}

#[derive(Debug, Clone, Copy)]
enum Classification {
    Anime,
    KDrama,
}

fn normalized_contains(value: &str, needles: &[&str]) -> bool {
    let value = value.to_ascii_lowercase();
    needles.iter().any(|needle| value.contains(needle))
}

fn normalized_matches(value: &str, aliases: &[&str]) -> bool {
    let value = value.trim().to_ascii_lowercase();
    aliases.iter().any(|alias| value == *alias)
}

fn is_classified(
    item: &CatalogItem,
    metrics: &HashMap<String, crate::models::BrowseMetrics>,
    classification: Classification,
) -> bool {
    let Some(metadata) = metrics.get(&item.id.value) else {
        return false;
    };
    let category = metadata.category.as_deref().unwrap_or_default();
    let country = metadata.country.as_deref().unwrap_or_default();
    let language = metadata.original_language.as_deref().unwrap_or_default();
    let anime_genre = metadata
        .genres
        .iter()
        .any(|genre| normalized_contains(genre, &["anime", "animation"]));
    match classification {
        Classification::Anime => {
            item.media_type == MediaType::Series
                && (normalized_contains(category, &["anime"])
                    || anime_genre
                    || normalized_matches(language, &["japanese", "ja", "jpn"]))
        }
        Classification::KDrama => {
            item.media_type == MediaType::Series
                && (normalized_contains(category, &["k-drama", "korean drama", "kdrama"])
                    || normalized_matches(
                        country,
                        &["south korea", "korea", "korean", "kr", "kor"],
                    )
                    || normalized_matches(language, &["korean", "ko", "kor", "한국어"]))
        }
    }
}

fn classified_items(
    items: Vec<CatalogItem>,
    metrics: &HashMap<String, crate::models::BrowseMetrics>,
    classification: Classification,
) -> Vec<CatalogItem> {
    items
        .into_iter()
        .filter(|item| is_classified(item, metrics, classification))
        .collect()
}

fn section_items(
    items: &[CatalogItem],
    metrics: &HashMap<String, crate::models::BrowseMetrics>,
    section: &str,
    limit: usize,
) -> (Vec<CatalogItem>, &'static str) {
    let metric = match section {
        "trending" => crate::models::BrowseMetric::Trending,
        "popular" => crate::models::BrowseMetric::Popularity,
        _ => {
            return (
                items.iter().take(limit).cloned().collect(),
                "MovieBox homepage ordering",
            );
        }
    };
    let mut ranked = items
        .iter()
        .filter_map(|item| {
            metrics
                .get(&item.id.value)
                .and_then(|m| m.value(metric))
                .map(|score| (score, item.clone()))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let basis = match metric {
        crate::models::BrowseMetric::Trending => "MovieBox trending metric",
        crate::models::BrowseMetric::Popularity => "MovieBox popularity metric",
        _ => "MovieBox homepage ordering",
    };
    (
        ranked
            .into_iter()
            .take(limit)
            .map(|(_, item)| item)
            .collect(),
        basis,
    )
}

fn section_response(
    section: &str,
    page: usize,
    limit: usize,
    items: Vec<CatalogItem>,
    metrics: &HashMap<String, crate::models::BrowseMetrics>,
) -> CatalogSectionResponse {
    let (items, basis) = match section {
        "movies" => (
            items
                .into_iter()
                .filter(|i| i.media_type == MediaType::Movie)
                .take(limit)
                .collect(),
            "MovieBox homepage metadata",
        ),
        "anime" => (
            classified_items(items, metrics, Classification::Anime)
                .into_iter()
                .take(limit)
                .collect(),
            "MovieBox homepage metadata",
        ),
        "kdramas" => (
            classified_items(items, metrics, Classification::KDrama)
                .into_iter()
                .take(limit)
                .collect(),
            "MovieBox verified country/language/category metadata",
        ),
        "trending_movies" => section_items(
            &items
                .into_iter()
                .filter(|i| i.media_type == MediaType::Movie)
                .collect::<Vec<_>>(),
            metrics,
            "trending",
            limit,
        ),
        "trending_anime" => section_items(
            &classified_items(items, metrics, Classification::Anime),
            metrics,
            "trending",
            limit,
        ),
        "trending_kdramas" => section_items(
            &classified_items(items, metrics, Classification::KDrama),
            metrics,
            "trending",
            limit,
        ),
        "popular_movies" => section_items(
            &items
                .into_iter()
                .filter(|i| i.media_type == MediaType::Movie)
                .collect::<Vec<_>>(),
            metrics,
            "popular",
            limit,
        ),
        "popular_anime" => section_items(
            &classified_items(items, metrics, Classification::Anime),
            metrics,
            "popular",
            limit,
        ),
        "popular_kdramas" => section_items(
            &classified_items(items, metrics, Classification::KDrama),
            metrics,
            "popular",
            limit,
        ),
        _ => section_items(&items, metrics, section, limit),
    };
    CatalogSectionResponse {
        id: section.to_string(),
        title: section.replace('_', " "),
        provider: ProviderKind::MovieBox,
        page,
        next_page: (!items.is_empty()).then_some(page + 1),
        ranking_basis: basis,
        items: items.iter().map(catalog_item_response).collect(),
    }
}

async fn home_route(stream: &mut TcpStream, query: &str, state: ApiState) -> std::io::Result<()> {
    let (page, limit) = parse_page_limit(query);
    match homepage_items(&state, page).await {
        Ok((items, metrics)) => {
            let sections = [
                "featured", "trending", "popular", "movies", "anime", "kdramas",
            ]
            .into_iter()
            .map(|section| section_response(section, page, limit, items.clone(), &metrics))
            .collect::<Vec<_>>();
            let status = HashMap::from([(
                ProviderKind::MovieBox.cache_key().to_string(),
                "ok".to_string(),
            )]);
            write_json(
                stream,
                200,
                &HomeResponse {
                    sections,
                    page,
                    provider_status: status,
                },
                None,
            )
            .await
        }
        Err(message) => {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                &message,
                Some(ProviderKind::MovieBox),
                503,
            );
            write_raw_json(stream, status, &body, None).await
        }
    }
}

async fn home_section_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    section: &str,
) -> std::io::Result<()> {
    let (page, limit) = parse_page_limit(query);
    match homepage_items(&state, page).await {
        Ok((items, metrics)) => {
            write_json(
                stream,
                200,
                &section_response(section, page, limit, items, &metrics),
                None,
            )
            .await
        }
        Err(message) => {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                &message,
                Some(ProviderKind::MovieBox),
                503,
            );
            write_raw_json(stream, status, &body, None).await
        }
    }
}

async fn catalog_list_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    media_type: MediaType,
) -> std::io::Result<()> {
    let (page, limit) = parse_page_limit(query);
    match homepage_items(&state, page).await {
        Ok((items, _)) => {
            let items = items
                .into_iter()
                .filter(|item| item.media_type == media_type)
                .take(limit)
                .map(|item| catalog_item_response(&item))
                .collect::<Vec<_>>();
            write_json(stream, 200, &serde_json::json!({"page": page, "next_page": (!items.is_empty()).then_some(page + 1), "provider": "moviebox", "items": items}), None).await
        }
        Err(message) => {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                &message,
                Some(ProviderKind::MovieBox),
                503,
            );
            write_raw_json(stream, status, &body, None).await
        }
    }
}

async fn classified_list_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    classification: Classification,
) -> std::io::Result<()> {
    let (page, limit) = parse_page_limit(query);
    match homepage_items(&state, page).await {
        Ok((items, metrics)) => {
            let items = classified_items(items, &metrics, classification)
                .into_iter()
                .take(limit)
                .map(|item| catalog_item_response(&item))
                .collect::<Vec<_>>();
            write_json(
                stream,
                200,
                &serde_json::json!({
                    "page": page,
                    "next_page": (!items.is_empty()).then_some(page + 1),
                    "provider": "moviebox",
                    "classification": match classification {
                        Classification::Anime => "anime",
                        Classification::KDrama => "k-drama",
                    },
                    "items": items
                }),
                None,
            )
            .await
        }
        Err(message) => {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                &message,
                Some(ProviderKind::MovieBox),
                503,
            );
            write_raw_json(stream, status, &body, None).await
        }
    }
}

async fn catalog_details_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
    expected: MediaType,
) -> std::io::Result<()> {
    let Some(id) = path.trim_end_matches('/').rsplit('/').next() else {
        return unsupported_catalog(stream, "A catalog identifier is required.").await;
    };
    let Ok(reference) = decode::<OpaqueRef>(id) else {
        return unsupported_catalog(stream, "The catalog identifier is invalid.").await;
    };
    if reference.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
    match state.service.details_typed(reference.p, &reference.v).await {
        Ok(media) if media.media_type == expected => {
            write_json(stream, 200, &details(media), None).await
        }
        Ok(_) => unsupported_catalog(stream, "The item is not in the requested catalogue.").await,
        Err(error) => {
            let (status, body) = provider_error(error, reference.p);
            write_raw_json(stream, status, &body, None).await
        }
    }
}

async fn categories_route(stream: &mut TcpStream, state: ApiState) -> std::io::Result<()> {
    let moviebox = state.service.capabilities(ProviderKind::MovieBox);
    write_json(
        stream,
        200,
        &serde_json::json!({
            "categories": [
                {"id":"anime", "supported":moviebox.supports_series},
                {"id":"movies", "supported":moviebox.supports_search},
                {"id":"series", "supported":moviebox.supports_series},
                {"id":"k-dramas", "supported":true, "classification":"country/language/category metadata"}
            ],
            "provider_capabilities": {"moviebox": moviebox}
        }),
        None,
    )
    .await
}

async fn unsupported_catalog(stream: &mut TcpStream, message: &str) -> std::io::Result<()> {
    let (status, body) = err("UNSUPPORTED_CATEGORY", message, None, 501);
    write_raw_json(stream, status, &body, None).await
}

async fn moviebox_disabled(stream: &mut TcpStream) -> std::io::Result<()> {
    let (status, body) = err(
        "PROVIDER_DISABLED",
        "MovieBox provider is temporarily disabled because its upstream episode mapping is unverified.",
        Some(ProviderKind::MovieBox),
        503,
    );
    write_raw_json(stream, status, &body, None).await
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
fn normalize_title(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn percent_decode(v: &str) -> String {
    let bytes = v.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (
                (bytes[i + 1] as char).to_digit(16),
                (bytes[i + 2] as char).to_digit(16),
            ) {
                decoded.push(((high << 4) | low) as u8);
                i += 3;
                continue;
            }
        }
        decoded.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

async fn direct_watch_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let mut title = None;
    let mut season = None;
    let mut episode = None;
    let mut audio = "hindi".to_string();
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        let value = percent_decode(value);
        match key {
            "title" => title = Some(value),
            "season" => season = value.parse::<usize>().ok(),
            "episode" => episode = value.parse::<usize>().ok(),
            "audio" => audio = value.trim().to_ascii_lowercase(),
            _ => {}
        }
    }
    let Some(title) = title.filter(|v| !v.trim().is_empty() && v.chars().count() <= MAX_QUERY)
    else {
        let (status, body) = err(
            "INVALID_QUERY",
            "title is required (example: /api/v1/watch?title=Lookism&season=1&episode=13&audio=hindi).",
            None,
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    let (Some(season), Some(episode)) = (season, episode) else {
        let (status, body) = err(
            "INVALID_QUERY",
            "season and episode must be positive integers.",
            None,
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    if season == 0 || episode == 0 {
        let (status, body) = err(
            "INVALID_QUERY",
            "season and episode must be positive integers.",
            None,
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    if !matches!(audio.as_str(), "hindi" | "hi" | "hin") {
        let (status, body) = err(
            "UNSUPPORTED_AUDIO_LANGUAGE",
            "This direct watch route currently supports audio=hindi only.",
            None,
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let (items, _, _) = search_all(&state.service, &title).await;
    let title_normalized = normalize_title(&title);
    let candidates: Vec<_> = items
        .into_iter()
        .filter(|item| item.media_type == MediaType::Series)
        .filter(|item| normalize_title(&item.title) == title_normalized)
        .collect();
    if candidates.is_empty() {
        let (status, body) = err(
            "ANIME_NOT_FOUND",
            "No matching series title was found. Try a title returned by /api/v1/anime/search.",
            None,
            404,
        );
        return write_raw_json(stream, status, &body, None).await;
    }

    // Search all matching provider entries before declaring the episode missing.
    // A provider may have incomplete season/episode metadata while another has it.
    let mut last_provider = None;
    let mut available = Vec::new();
    let mut episode_found = false;
    let mut stream_failures: Vec<String> = Vec::new();
    for item in candidates {
        let anime_ref = OpaqueRef {
            p: item.id.provider,
            v: item.id.value.clone(),
            s: 0,
            e: 0,
        };
        if anime_ref.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
            continue;
        }
        let anime_details = match state.service.details_typed(anime_ref.p, &anime_ref.v).await {
            Ok(details) => details,
            Err(error) => {
                log::warn!(
                    "direct watch details lookup failed for provider {}: {}",
                    anime_ref.p.label(),
                    error.user_message(anime_ref.p)
                );
                last_provider = Some(anime_ref.p);
                continue;
            }
        };
        last_provider = Some(anime_ref.p);
        let episode_exists = anime_details
            .seasons
            .iter()
            .any(|s| s.number == season && s.episodes.iter().any(|e| e.number == episode));
        if episode_exists {
            episode_found = true;

            // Preflight this provider's streams. If it has no Hindi-labelled source,
            // keep searching the remaining providers instead of failing on the first match.
            let episode_reference = OpaqueRef {
                p: anime_ref.p,
                v: anime_ref.v.clone(),
                s: season,
                e: episode,
            };
            match tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &episode_reference))
                .await
            {
                Ok(Ok(releases)) => {
                    let has_hindi = releases.iter().any(|release| {
                        let is_hindi = |value: &str| {
                            let value = value.to_ascii_lowercase();
                            value == "hi"
                                || value == "hin"
                                || value.contains("hindi")
                                || value.contains("हिन्दी")
                                || value.contains("हिंदी")
                        };
                        release.language.as_deref().is_some_and(is_hindi)
                            || is_hindi(&release.filename)
                    });
                    if has_hindi || !releases.is_empty() {
                        // Provider language labels are hints, not a hard gate. Official
                        // MovieBox-Tui can expose dual/multi-audio streams without a Hindi tag.
                        let anime_id = encode(&anime_ref);
                        let episode_id = provider_ref(&anime_details.id, season, episode);
                        return anime_episode_stream_route(
                            stream,
                            &anime_id,
                            &episode_id,
                            anime_ref,
                            anime_details,
                            state,
                            &audio,
                        )
                        .await;
                    }
                    stream_failures.push(format!(
                        "{}: no stream sources returned",
                        anime_ref.p.label()
                    ));
                }
                Ok(Err(error)) => {
                    stream_failures.push(format!(
                        "{}: {}",
                        anime_ref.p.label(),
                        error.user_message(anime_ref.p)
                    ));
                }
                Err(_) => {
                    stream_failures
                        .push(format!("{}: stream lookup timed out", anime_ref.p.label()));
                }
            }
            continue;
        }
        for available_season in &anime_details.seasons {
            if !available_season.episodes.is_empty() {
                let numbers = available_season
                    .episodes
                    .iter()
                    .map(|e| e.number.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                available.push(format!(
                    "{} S{} episodes [{}] ({})",
                    item.title,
                    available_season.number,
                    numbers,
                    item.id.provider.label(),
                ));
            }
        }
    }

    if episode_found {
        let detail = if stream_failures.is_empty() {
            "The episode exists, but no matching provider returned a playable source.".to_string()
        } else {
            format!(
                "The episode exists, but no provider returned a playable source. Provider results: {}",
                stream_failures
                    .into_iter()
                    .take(6)
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        };
        let (status, body) = err("NO_PLAYABLE_SOURCE", &detail, last_provider, 404);
        return write_raw_json(stream, status, &body, None).await;
    }

    let detail = if available.is_empty() {
        "No matching provider returned a parseable episode list. Check provider availability and series details.".to_string()
    } else {
        format!(
            "Season {} episode {} was not found in matching providers. Available episodes: {}",
            season,
            episode,
            available.into_iter().take(6).collect::<Vec<_>>().join("; ")
        )
    };
    let (status, body) = err("EPISODE_NOT_FOUND", &detail, last_provider, 404);
    write_raw_json(stream, status, &body, None).await
}

async fn search_json(stream: &mut TcpStream, query: &str, state: ApiState) -> std::io::Result<()> {
    let q = parse_query(query);
    let Some(qv) =
        q.q.filter(|v| !v.trim().is_empty() && v.chars().count() <= MAX_QUERY)
    else {
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
    let (items, failed, provider_status) = search_all(&state.service, &qv).await;
    let eligible = eligible_search(&state, items, failed, provider_status).await;
    let items = eligible.items;
    let failed = eligible.failed;
    let provider_status = eligible.provider_status;
    let offset = q
        .cursor
        .as_deref()
        .and_then(|cursor| decode::<usize>(cursor).ok())
        .unwrap_or_default();
    let language_intent = crate::language::parse_query(&qv).language;
    let verified_language_matches = language_intent.map(|language| {
        let label = format!("({})", language.result_label());
        items
            .iter()
            .filter(|item| item.title.ends_with(&label))
            .count()
    });
    let message = language_intent
        .filter(|_| verified_language_matches == Some(0))
        .map(|language| {
            format!(
                "No verified {} version found. Try another provider or language.",
                language.name()
            )
        });
    let total = items.len();
    let next_cursor =
        (offset.saturating_add(limit) < total).then(|| encode(&offset.saturating_add(limit)));
    let results = items
        .into_iter()
        .skip(offset.min(total))
        .take(limit)
        .map(catalog)
        .collect::<Vec<_>>();
    let body = SearchResponse {
        results,
        next_cursor,
        partial: failed,
        provider_status,
        language_intent: language_intent.map(|language| language.code().to_string()),
        verified_language_matches,
        message,
    };
    write_json(stream, 200, &body, None).await
}

async fn search_sse(stream: &mut TcpStream, query: &str, state: ApiState) -> std::io::Result<()> {
    let q = parse_query(query);
    let Some(qv) =
        q.q.filter(|v| !v.trim().is_empty() && v.chars().count() <= MAX_QUERY)
    else {
        let (s, b) = err(
            "INVALID_QUERY",
            "q is required and must be 1-200 characters.",
            None,
            400,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    let (items, failed, provider_status) = search_all(&state.service, &qv).await;
    let eligible = eligible_search(&state, items, failed, provider_status).await;
    write_headers(stream, 200, "text/event-stream", None).await?;
    for item in eligible.items {
        let payload = serde_json::to_string(&serde_json::json!({"result":catalog(item)})).unwrap();
        send_event(stream, "search_result", &payload).await?;
    }
    for (provider, status) in eligible.provider_status {
        if status == "ok" {
            continue;
        }
        let (code, event) = match status.as_str() {
            "no_source_confirmed" => ("NO_SOURCE_CONFIRMED", "search_provider_status"),
            "source_check_failed" => ("SOURCE_CHECK_FAILED", "search_provider_error"),
            "timeout" => ("PROVIDER_TIMEOUT", "search_provider_error"),
            _ => ("UPSTREAM_UNAVAILABLE", "search_provider_error"),
        };
        let payload = serde_json::json!({
            "provider": provider,
            "code": code,
            "message": if code == "NO_SOURCE_CONFIRMED" {
                "No usable source was confirmed for the provider search results."
            } else {
                "The provider search or source eligibility check did not complete."
            }
        })
        .to_string();
        send_event(stream, event, &payload).await?;
    }
    send_event(
        stream,
        "search_complete",
        &serde_json::json!({"partial":eligible.failed,"replay":false}).to_string(),
    )
    .await
}

async fn search_all(
    service: &Arc<MovieBoxService>,
    query: &str,
) -> (Vec<CatalogItem>, bool, HashMap<String, String>) {
    let intent = crate::language::parse_query(query);
    let requested_language = intent.language;
    let normalized_query = intent.title;
    let mut tasks = JoinSet::new();
    for p in configured_search_providers() {
        let svc = service.clone();
        let q = normalized_query.clone();
        tasks.spawn(async move {
            (
                p,
                tokio::time::timeout(OVERALL_TIMEOUT, svc.search_typed(p, &q, 1)).await,
            )
        });
    }
    let mut items = Vec::new();
    let mut failed = false;
    let mut provider_status = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((p, r)) => match r {
                Ok(Ok(mut v)) => {
                    provider_status.insert(p.cache_key().to_string(), "ok".to_string());
                    items.append(&mut v);
                }
                Ok(Err(e)) => {
                    provider_status.insert(p.cache_key().to_string(), e.user_message(p));
                    failed = true;
                }
                Err(_) => {
                    provider_status.insert(p.cache_key().to_string(), "timeout".to_string());
                    failed = true;
                }
            },
            Err(error) => {
                log::error!("search provider task failed: {error}");
                provider_status.insert(
                    "provider_task".to_string(),
                    "task failed unexpectedly".to_string(),
                );
                failed = true;
            }
        }
    }
    let mut seen = std::collections::HashSet::new();
    items.retain(|i| seen.insert((i.id.provider, i.id.value.clone())));
    // Keep results from every provider so watch resolution can fail over across providers.
    if let Some(language) = requested_language {
        let mut matching = Vec::new();
        let mut other = Vec::new();
        for mut item in items {
            if crate::language::decorate_catalog_item(&mut item, Some(language)) {
                matching.push(item);
            } else {
                other.push(item);
            }
        }
        matching.extend(other);
        items = matching;
    }
    items.sort_by(|a, b| {
        let language_order = requested_language.map_or(std::cmp::Ordering::Equal, |language| {
            let label = format!("({})", language.result_label());
            b.title.ends_with(&label).cmp(&a.title.ends_with(&label))
        });
        language_order
            .then_with(|| provider_priority(a.id.provider).cmp(&provider_priority(b.id.provider)))
    });
    (items, failed, provider_status)
}

async fn catalog_item_source_eligibility(
    state: &ApiState,
    item: CatalogItem,
) -> (CatalogItem, SourceEligibility) {
    let details = match tokio::time::timeout(
        SEARCH_SOURCE_CHECK_TIMEOUT,
        state
            .service
            .details_typed(item.id.provider, &item.id.value),
    )
    .await
    {
        Ok(Ok(details)) => details,
        Ok(Err(_)) | Err(_) => return (item, SourceEligibility::CheckFailed),
    };

    let episode_refs = details
        .seasons
        .iter()
        .flat_map(|season| season.episodes.iter())
        .map(|episode| OpaqueRef {
            p: details.id.provider,
            v: details.id.value.clone(),
            s: episode.season,
            e: episode.number,
        })
        .collect::<Vec<_>>();
    let refs = if episode_refs.is_empty() {
        vec![OpaqueRef {
            p: details.id.provider,
            v: details.id.value.clone(),
            s: 0,
            e: 0,
        }]
    } else {
        episode_refs
    };

    let mut check_failed = false;
    for episode_ref in refs {
        match tokio::time::timeout(SOURCE_TIMEOUT, cached_releases(state, &episode_ref)).await {
            Ok(Ok(releases)) if releases_have_usable_source(&releases) => {
                return (item, SourceEligibility::Playable);
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) if provider_error_means_no_source(&error) => {}
            Ok(Err(_)) | Err(_) => check_failed = true,
        }
    }

    (
        item,
        if check_failed {
            SourceEligibility::CheckFailed
        } else {
            SourceEligibility::NoSourceConfirmed
        },
    )
}

async fn eligible_search(
    state: &ApiState,
    items: Vec<CatalogItem>,
    mut failed: bool,
    mut provider_status: HashMap<String, String>,
) -> EligibleSearch {
    let mut tasks = JoinSet::new();
    for item in items {
        let state = state.clone();
        let semaphore = Arc::clone(&state.search_source_check_slots);
        tasks.spawn(async move {
            let permit = semaphore.acquire_owned().await;
            if permit.is_err() {
                return (item, SourceEligibility::CheckFailed);
            }
            let item_fallback = item.clone();
            match tokio::time::timeout(
                SEARCH_SOURCE_CHECK_TIMEOUT,
                catalog_item_source_eligibility(&state, item),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => (item_fallback, SourceEligibility::CheckFailed),
            }
        });
    }

    let mut eligible = Vec::new();
    let mut provider_playable = std::collections::HashSet::new();
    let mut provider_failed = std::collections::HashSet::new();
    let mut provider_no_source = std::collections::HashSet::new();
    while let Some(result) = tasks.join_next().await {
        let Ok((item, status)) = result else {
            failed = true;
            continue;
        };
        match status {
            SourceEligibility::Playable => {
                provider_playable.insert(item.id.provider);
                eligible.push(item);
            }
            SourceEligibility::NoSourceConfirmed => {
                provider_no_source.insert(item.id.provider);
            }
            SourceEligibility::CheckFailed => {
                provider_failed.insert(item.id.provider);
                failed = true;
            }
        }
    }
    for provider in configured_search_providers() {
        let key = provider.cache_key().to_string();
        if provider_failed.contains(&provider) {
            provider_status.insert(key, "source_check_failed".to_string());
        } else if provider_playable.contains(&provider) {
            provider_status.insert(key, "ok".to_string());
        } else if provider_no_source.contains(&provider) {
            provider_status.insert(key, "no_source_confirmed".to_string());
        }
    }
    eligible.sort_by(|a, b| {
        provider_priority(a.id.provider)
            .cmp(&provider_priority(b.id.provider))
            .then_with(|| a.title.cmp(&b.title))
    });
    EligibleSearch {
        items: eligible,
        failed,
        provider_status,
    }
}

async fn anime_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
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
    if r.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
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
    if parts.len() == 7 && parts[4] == "episodes" && parts[6] == "full" {
        return anime_episode_full_route(stream, parts[3], parts[5], r, d, state).await;
    }
    if parts.len() == 7 && parts[4] == "episodes" && parts[6] == "stream" {
        let audio = query
            .split('&')
            .find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "audio").then(|| percent_decode(value).trim().to_ascii_lowercase())
            })
            .unwrap_or_else(|| "hindi".to_string());
        if !matches!(audio.as_str(), "hindi" | "hi" | "hin") {
            let (status, body) = err(
                "UNSUPPORTED_AUDIO_LANGUAGE",
                "This stream route currently supports audio=hindi only.",
                Some(r.p),
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
        return anime_episode_stream_route(stream, parts[3], parts[5], r, d, state, &audio).await;
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

async fn anime_episode_stream_route(
    stream: &mut TcpStream,
    anime_id: &str,
    episode_id: &str,
    anime_ref: OpaqueRef,
    anime_details: MediaDetails,
    state: ApiState,
    requested_audio: &str,
) -> std::io::Result<()> {
    let episode_ref = match decode::<OpaqueRef>(episode_id) {
        Ok(reference) => reference,
        Err(_) => {
            let (status, body) = err(
                "INVALID_ID",
                "The episode identifier is invalid.",
                None,
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    if episode_ref.p != anime_ref.p
        || episode_ref.v != anime_ref.v
        || anime_details.id.value != anime_ref.v
    {
        let (status, body) = err(
            "INVALID_ID",
            "The episode does not belong to the requested anime.",
            Some(anime_ref.p),
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    if anime_details.id.provider != anime_ref.p || encode(&anime_ref) != anime_id {
        let (status, body) = err(
            "INVALID_ID",
            "The anime identifier does not match the requested item.",
            Some(anime_ref.p),
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let episode_exists = anime_details.seasons.iter().any(|season| {
        season.number == episode_ref.s
            && season
                .episodes
                .iter()
                .any(|episode| episode.number == episode_ref.e)
    });
    if !episode_exists {
        let (status, body) = err(
            "EPISODE_NOT_FOUND",
            "The episode was not found in this anime.",
            Some(anime_ref.p),
            404,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let releases =
        match tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &episode_ref)).await {
            Ok(Ok(releases)) => releases,
            Ok(Err(error)) => {
                let (status, body) = provider_error(error, episode_ref.p);
                return write_raw_json(stream, status, &body, None).await;
            }
            Err(_) => {
                let (status, body) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The stream source request timed out.",
                    Some(episode_ref.p),
                    504,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
        };
    let is_hindi = |value: &str| {
        let normalized = value.to_ascii_lowercase();
        normalized == "hi"
            || normalized == "hin"
            || normalized.contains("hindi")
            || normalized.contains("हिन्दी")
            || normalized.contains("हिंदी")
    };
    let mut candidates: Vec<&Release> = releases
        .iter()
        .filter(|release| {
            release.language.as_deref().is_some_and(is_hindi) || is_hindi(&release.filename)
        })
        .collect();
    let language_metadata_match = !candidates.is_empty();
    // Dual/multi-audio sources may have no language tag until the actual media is probed.
    if candidates.is_empty() {
        candidates = releases.iter().collect();
    }
    candidates.sort_by_key(|release| std::cmp::Reverse(release.resolution_u64()));
    let Some(release) = candidates.first().copied() else {
        let (status, body) = err(
            "NO_STREAM_SOURCE",
            "The provider returned no stream sources for this episode.",
            Some(episode_ref.p),
            404,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    let playback = if release.provider == ProviderKind::FourKHdHub {
        let Some(client) = state.service.fourk_client.as_ref() else {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                "The playback provider is unavailable.",
                Some(release.provider),
                503,
            );
            return write_raw_json(stream, status, &body, None).await;
        };
        match tokio::time::timeout(
            OVERALL_TIMEOUT,
            client.resolve_release(
                release,
                crate::providers::models::ResolutionIntent::Playback,
            ),
        )
        .await
        {
            Ok(Ok(playback)) => playback,
            Ok(Err(error)) => {
                let (status, body) = err(
                    "PLAYBACK_RESOLUTION_FAILED",
                    error.user_message(),
                    Some(release.provider),
                    502,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
            Err(_) => {
                let (status, body) = err(
                    "PLAYBACK_RESOLUTION_TIMEOUT",
                    "Hindi stream resolution timed out.",
                    Some(release.provider),
                    504,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
        }
    } else if let Some(url) = release.direct_url() {
        crate::providers::models::PlaybackSource {
            provider: release.provider,
            url: url.to_string(),
            headers: release
                .mirrors
                .first()
                .map(|mirror| mirror.headers.clone())
                .unwrap_or_default(),
            subtitle: None,
            source_label: release.source_label().to_string(),
            max_height: release
                .quality
                .as_deref()
                .and_then(|quality| quality.trim_end_matches('p').parse().ok()),
        }
    } else {
        let (status, body) = err(
            "PLAYBACK_RESOLUTION_FAILED",
            "The Hindi source does not expose a playable URL.",
            Some(release.provider),
            502,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    // Probe the resolved source for real audio tracks when possible. A failed probe
    // must not block playback (some HLS/DASH providers prevent ffprobe access).
    let audio_tracks = match probe_media(&playback.url, &playback.headers).await {
        Ok(probe) => probe
            .streams
            .into_iter()
            .filter(|track| {
                track
                    .codec_type
                    .as_deref()
                    .is_some_and(|kind| kind.eq_ignore_ascii_case("audio"))
            })
            .map(|track| {
                let raw_language = tag_value(track.tags.as_ref(), "language");
                let (language, language_label, raw_tag) = normalized_language(raw_language);
                let label = tag_value(track.tags.as_ref(), "title")
                    .or_else(|| tag_value(track.tags.as_ref(), "handler_name"))
                    .map(str::to_string)
                    .unwrap_or(language_label);
                serde_json::json!({
                    "index": track.index,
                    "language": language,
                    "label": label,
                    "codec": track.codec_name,
                    "channels": track.channels,
                    "default": track.disposition.as_ref().is_some_and(|d| d.default != 0),
                    "raw_language": raw_tag
                })
            })
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    let mut body = playback_json(&playback);
    if let Some(object) = body.as_object_mut() {
        object.insert("anime_id".into(), serde_json::json!(anime_id));
        object.insert("episode_id".into(), serde_json::json!(episode_id));
        object.insert("audio_requested".into(), serde_json::json!(requested_audio));
        object.insert("audio_tracks".into(), serde_json::json!(audio_tracks));
        object.insert(
            "language_match".into(),
            serde_json::json!(if language_metadata_match {
                "provider_metadata"
            } else {
                "unverified_media_tracks"
            }),
        );
        object.insert("resolved".into(), serde_json::json!(true));
    }
    write_json(stream, 200, &body, None).await
}

async fn anime_episode_full_route(
    stream: &mut TcpStream,
    anime_id: &str,
    episode_id: &str,
    anime_ref: OpaqueRef,
    details_result: MediaDetails,
    state: ApiState,
) -> std::io::Result<()> {
    let episode_ref = match decode::<OpaqueRef>(episode_id) {
        Ok(episode) => episode,
        Err(_) => {
            let (status, body) = err(
                "INVALID_ID",
                "The episode identifier is invalid.",
                None,
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    if episode_ref.p != anime_ref.p || episode_ref.v != anime_ref.v {
        let (status, body) = err(
            "INVALID_ID",
            "The episode does not belong to the requested anime.",
            None,
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }

    let anime = details(details_result);
    if anime.id != anime_id {
        let (status, body) = err(
            "INVALID_ID",
            "The anime identifier does not match the requested item.",
            Some(anime_ref.p),
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let selected_episode = anime
        .seasons
        .iter()
        .flat_map(|season| season.episodes.iter())
        .find(|episode| episode.season == episode_ref.s && episode.number == episode_ref.e)
        .cloned();
    let Some(selected_episode) = selected_episode else {
        let (status, body) = err(
            "EPISODE_NOT_FOUND",
            "The episode was not found in this anime.",
            Some(anime_ref.p),
            404,
        );
        return write_raw_json(stream, status, &body, None).await;
    };

    let mut source_error = None;
    let (sources, sources_status, sources_partial) =
        match tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &episode_ref)).await {
            Ok(Ok(releases)) => (api_sources(&releases, &episode_ref), "ok", false),
            Ok(Err(error)) => {
                let (_, body) = provider_error(error, episode_ref.p);
                source_error = serde_json::from_str(&body).ok();
                (Vec::new(), "unavailable", true)
            }
            Err(_) => {
                let (_, body) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The source request deadline was exceeded.",
                    Some(episode_ref.p),
                    504,
                );
                source_error = serde_json::from_str(&body).ok();
                (Vec::new(), "timeout", true)
            }
        };

    write_json(
        stream,
        200,
        &AnimeEpisodeFullResponse {
            anime,
            selected_episode,
            sources,
            sources_status,
            sources_partial,
            source_error,
        },
        None,
    )
    .await
}

fn release_cache_key(r: &OpaqueRef) -> String {
    format!("{}:{}:{}:{}", r.p.cache_key(), r.v, r.s, r.e)
}

async fn cached_releases(state: &ApiState, r: &OpaqueRef) -> Result<Vec<Release>, ProviderError> {
    let key = release_cache_key(r);
    if let Some(entry) = state.release_cache.lock().await.get(&key).cloned()
        && entry.stored_at.elapsed() < RELEASE_CACHE_TTL
    {
        return Ok(entry.releases);
    }
    let fetched = releases_uncached(state, r).await?;
    let mut cache = state.release_cache.lock().await;
    if cache.len() >= 2048 {
        cache.retain(|_, entry| entry.stored_at.elapsed() < RELEASE_CACHE_TTL);
    }
    cache.insert(
        key,
        ReleaseCacheEntry {
            stored_at: Instant::now(),
            releases: fetched.clone(),
        },
    );
    Ok(fetched)
}

async fn releases_uncached(state: &ApiState, r: &OpaqueRef) -> Result<Vec<Release>, ProviderError> {
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

fn api_episode_from_details(details: &MediaDetails, episode: &OpaqueRef) -> Option<ApiEpisode> {
    details
        .seasons
        .iter()
        .flat_map(|season| season.episodes.iter())
        .find(|candidate| candidate.season == episode.s && candidate.number == episode.e)
        .map(|candidate| ApiEpisode {
            id: encode(&OpaqueRef {
                p: episode.p,
                v: episode.v.clone(),
                s: candidate.season,
                e: candidate.number,
            }),
            season: candidate.season,
            number: candidate.number,
            title: candidate.title.clone(),
            overview: candidate.overview.clone(),
        })
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
    if r.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
    if parts.len() == 4 {
        return match tokio::time::timeout(OVERALL_TIMEOUT, state.service.details_typed(r.p, &r.v))
            .await
        {
            Ok(Ok(details)) => match api_episode_from_details(&details, &r) {
                Some(episode) => write_json(stream, 200, &episode, None).await,
                None => {
                    let (s, b) = err(
                        "EPISODE_NOT_FOUND",
                        "The episode was not found in this anime.",
                        Some(r.p),
                        404,
                    );
                    write_raw_json(stream, s, &b, None).await
                }
            },
            Ok(Err(error)) => {
                let (s, b) = provider_error(error, r.p);
                write_raw_json(stream, s, &b, None).await
            }
            Err(_) => {
                let (s, b) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The episode details request timed out.",
                    Some(r.p),
                    504,
                );
                write_raw_json(stream, s, &b, None).await
            }
        };
    }
    if parts.get(4) == Some(&"sources") {
        if parts.get(5) == Some(&"stream") {
            return source_sse(stream, r, state).await;
        }
        let result = tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &r)).await;
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
    match tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &r)).await {
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
            .await?;
            send_event(stream, "source_complete", "{\"partial\":true}").await
        }
    }
}
async fn probe_media(url: &str, headers: &[(String, String)]) -> Result<ProbeOutput, String> {
    crate::net::validate_public_url(url, true)
        .await
        .map_err(|_| "The media source URL is not an allowed public destination.".to_string())?;
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
    // ffprobe normally reports ISO-639 codes, but some files use full language
    // names or common aliases. Normalize known values while preserving the source tag.
    match raw.to_ascii_lowercase().replace('_', "-").as_str() {
        "hi" | "hin" | "hindi" => ("hi".into(), "Hindi".into(), original),
        "en" | "eng" | "english" => ("en".into(), "English".into(), original),
        "ja" | "jpn" | "japanese" | "日本語" => ("ja".into(), "Japanese".into(), original),
        "ko" | "kor" | "korean" | "한국어" => ("ko".into(), "Korean".into(), original),
        "zh" | "zho" | "chi" | "chinese" | "mandarin" => ("zh".into(), "Chinese".into(), original),
        "es" | "spa" | "spanish" => ("es".into(), "Spanish".into(), original),
        "fr" | "fra" | "fre" | "french" => ("fr".into(), "French".into(), original),
        "de" | "deu" | "ger" | "german" => ("de".into(), "German".into(), original),
        "pt" | "por" | "portuguese" => ("pt".into(), "Portuguese".into(), original),
        "ru" | "rus" | "russian" => ("ru".into(), "Russian".into(), original),
        "it" | "ita" | "italian" => ("it".into(), "Italian".into(), original),
        "ar" | "ara" | "arabic" => ("ar".into(), "Arabic".into(), original),
        "bn" | "ben" | "bengali" | "bangla" => ("bn".into(), "Bengali".into(), original),
        "ta" | "tam" | "tamil" => ("ta".into(), "Tamil".into(), original),
        "te" | "tel" | "telugu" => ("te".into(), "Telugu".into(), original),
        "ur" | "urd" | "urdu" => ("ur".into(), "Urdu".into(), original),
        "pa" | "pan" | "punjabi" => ("pa".into(), "Punjabi".into(), original),
        "id" | "ind" | "indonesian" | "bahasa indonesia" => {
            ("id".into(), "Indonesian".into(), original)
        }
        "ms" | "msa" | "may" | "malay" => ("ms".into(), "Malay".into(), original),
        "th" | "tha" | "thai" => ("th".into(), "Thai".into(), original),
        "tr" | "tur" | "turkish" => ("tr".into(), "Turkish".into(), original),
        "vi" | "vie" | "vietnamese" => ("vi".into(), "Vietnamese".into(), original),
        "fil" | "tgl" | "filipino" | "tagalog" => ("fil".into(), "Filipino".into(), original),
        "fa" | "fas" | "per" | "persian" | "farsi" => ("fa".into(), "Persian".into(), original),
        "und" | "unknown" => ("und".into(), "Unknown".into(), original),
        _ => (raw.to_ascii_lowercase(), raw.to_string(), original),
    }
}

async fn write_track_response(
    stream: &mut TcpStream,
    source_id: &str,
    provider: ProviderKind,
    release: &Release,
    state: &ApiState,
    subtitles: bool,
) -> std::io::Result<()> {
    let (url, headers) = if provider == ProviderKind::FourKHdHub {
        let Some(client) = state.service.fourk_client.as_ref() else {
            let (s, b) = err(
                "UPSTREAM_UNAVAILABLE",
                "4KHDHub is unavailable.",
                Some(provider),
                503,
            );
            return write_raw_json(stream, s, &b, None).await;
        };
        let resolved = tokio::time::timeout(
            OVERALL_TIMEOUT,
            client.resolve_release(
                release,
                crate::providers::models::ResolutionIntent::Playback,
            ),
        )
        .await;
        match resolved {
            Ok(Ok(playback)) => (watch_url(&playback.url), playback.headers),
            Ok(Err(e)) => {
                let (s, b) = err(
                    "TRACK_DISCOVERY_UNAVAILABLE",
                    e.user_message(),
                    Some(provider),
                    503,
                );
                return write_raw_json(stream, s, &b, None).await;
            }
            Err(_) => {
                let (s, b) = err(
                    "TRACK_DISCOVERY_UNAVAILABLE",
                    "Media resolution timed out.",
                    Some(provider),
                    503,
                );
                return write_raw_json(stream, s, &b, None).await;
            }
        }
    } else {
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
            .map(|mirror| mirror.headers.clone())
            .unwrap_or_default();
        (url.to_string(), headers)
    };
    let probe = match probe_media(&url, &headers).await {
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
    if sr.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
    let r = OpaqueRef {
        p: sr.p,
        v: sr.v.clone(),
        s: sr.s,
        e: sr.e,
    };
    let rs = match tokio::time::timeout(SOURCE_TIMEOUT, cached_releases(&state, &r)).await {
        Ok(Ok(releases)) => releases,
        Ok(Err(error)) => {
            let (status, body) = match error {
                ProviderError::NotFound => err(
                    "SOURCE_NOT_FOUND",
                    "No source was found for this episode.",
                    Some(sr.p),
                    404,
                ),
                ProviderError::RateLimited(_) => err(
                    "RATE_LIMITED",
                    "The upstream provider rate limited source resolution.",
                    Some(sr.p),
                    429,
                ),
                ProviderError::Network(_) | ProviderError::Unavailable(_) => err(
                    "UPSTREAM_UNAVAILABLE",
                    "The provider could not resolve this source.",
                    Some(sr.p),
                    502,
                ),
                ProviderError::Parsing(_) => err(
                    "UPSTREAM_INVALID_RESPONSE",
                    "The provider returned an invalid source response.",
                    Some(sr.p),
                    502,
                ),
            };
            return write_raw_json(stream, status, &body, None).await;
        }
        Err(_) => {
            let (status, body) = err(
                "SOURCE_TIMEOUT",
                "Source resolution exceeded the allowed time.",
                Some(sr.p),
                504,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let rel = match sr.f.as_deref() {
        Some(fingerprint) => rs
            .iter()
            .find(|release| release_fingerprint(release) == fingerprint),
        None => rs.get(sr.i),
    };
    let Some(rel) = rel else {
        let (s, b) = err(
            "SOURCE_NOT_FOUND",
            "The source was not found.",
            Some(sr.p),
            404,
        );
        return write_raw_json(stream, s, &b, None).await;
    };
    if matches!(parts.get(4), Some(&"audio") | Some(&"audio-tracks")) {
        return write_track_response(stream, id, sr.p, rel, &state, false).await;
    }
    if parts.get(4) == Some(&"transcode") {
        return transcode_source(stream, id, rel, &state).await;
    }
    if matches!(parts.get(4), Some(&"subtitles") | Some(&"subtitle-tracks")) {
        return write_track_response(stream, id, sr.p, rel, &state, true).await;
    }
    if parts.get(4) == Some(&"download") {
        return if rel.mirrors.iter().any(|m| m.direct_file) {
            write_json(
                stream,
                200,
                &serde_json::json!({"supported":true,"mirrors":public_mirrors(&rel.mirrors)}),
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
    if matches!(
        parts.get(4),
        Some(&"playback") | Some(&"play") | Some(&"watch")
    ) {
        if sr.p == ProviderKind::FourKHdHub
            && let Some(entry) = state.playback_cache.lock().await.get(*id).cloned()
            && entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL
        {
            return write_json(stream, 200, &playback_json(&entry.playback), None).await;
        }
        let resolved = if sr.p == ProviderKind::FourKHdHub {
            let Some(client) = state.service.fourk_client.as_ref() else {
                let (s, b) = err(
                    "UPSTREAM_UNAVAILABLE",
                    "4KHDHub is unavailable.",
                    Some(sr.p),
                    503,
                );
                return write_raw_json(stream, s, &b, None).await;
            };
            tokio::time::timeout(
                OVERALL_TIMEOUT,
                client.resolve_release(rel, crate::providers::models::ResolutionIntent::Playback),
            )
            .await
        } else if let Some(url) = rel.direct_url() {
            Ok(Ok(crate::providers::models::PlaybackSource {
                provider: sr.p,
                url: url.to_string(),
                headers: rel
                    .mirrors
                    .first()
                    .map(|m| m.headers.clone())
                    .unwrap_or_default(),
                subtitle: None,
                source_label: rel.source_label().to_string(),
                max_height: rel
                    .quality
                    .as_deref()
                    .and_then(|q| q.trim_end_matches('p').parse().ok()),
            }))
        } else {
            Ok(Err(
                crate::providers::fourkhdhub::FourKHdHubError::NoPlayableMirror(
                    "source has no playable URL".into(),
                ),
            ))
        };
        return match resolved {
            Ok(Ok(playback)) => {
                if sr.p == ProviderKind::FourKHdHub {
                    let mut cache = state.playback_cache.lock().await;
                    if cache.len() >= 2048 {
                        cache.retain(|_, entry| entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL);
                    }
                    cache.insert(
                        id.to_string(),
                        PlaybackCacheEntry {
                            stored_at: Instant::now(),
                            playback: playback.clone(),
                        },
                    );
                }
                write_json(stream, 200, &playback_json(&playback), None).await
            }
            Ok(Err(e)) => {
                let (s, b) = err(
                    "PLAYBACK_RESOLUTION_FAILED",
                    e.user_message(),
                    Some(sr.p),
                    502,
                );
                write_raw_json(stream, s, &b, None).await
            }
            Err(_) => {
                let (s, b) = err(
                    "PLAYBACK_RESOLUTION_TIMEOUT",
                    "4KHDHub playback resolution timed out.",
                    Some(sr.p),
                    504,
                );
                write_raw_json(stream, s, &b, None).await
            }
        };
    }
    if sr.p == ProviderKind::FourKHdHub {
        let Some(client) = state.service.fourk_client.as_ref() else {
            let (s, b) = err(
                "UPSTREAM_UNAVAILABLE",
                "4KHDHub is unavailable.",
                Some(sr.p),
                503,
            );
            return write_raw_json(stream, s, &b, None).await;
        };
        let resolved = tokio::time::timeout(
            OVERALL_TIMEOUT,
            client.resolve_release(rel, crate::providers::models::ResolutionIntent::Playback),
        )
        .await;
        return match resolved {
            Ok(Ok(playback)) => write_json(stream, 200, &playback_json(&playback), None).await,
            Ok(Err(error)) => {
                let (s, b) = err(
                    "PLAYBACK_RESOLUTION_FAILED",
                    error.user_message(),
                    Some(sr.p),
                    502,
                );
                write_raw_json(stream, s, &b, None).await
            }
            Err(_) => {
                let (s, b) = err(
                    "PLAYBACK_RESOLUTION_TIMEOUT",
                    "4KHDHub playback resolution timed out.",
                    Some(sr.p),
                    504,
                );
                write_raw_json(stream, s, &b, None).await
            }
        };
    }
    write_json(
        stream,
        200,
        &serde_json::json!({
            "source": api_sources(std::slice::from_ref(rel), &r),
            "url": rel.direct_url(),
            "headers": rel.mirrors.first().map(|m| public_headers(&m.headers)),
            "sensitive_headers_redacted": true
        }),
        None,
    )
    .await
}
fn watch_url(raw: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(raw) else {
        return raw.to_string();
    };
    if url
        .query_pairs()
        .any(|(name, value)| name == "download" && value.is_empty())
    {
        url.set_query(None);
    }
    url.to_string()
}
fn playback_json(playback: &PlaybackSource) -> serde_json::Value {
    serde_json::json!({
        "provider": playback.provider,
        "resolved": true,
        "mode": "watch",
        "url": watch_url(&playback.url),
        "headers": public_headers(&playback.headers),
        "subtitle": playback.subtitle,
        "source_label": playback.source_label,
        "max_height": playback.max_height,
    })
}
fn public_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.to_ascii_lowercase().as_str(),
                "cookie" | "authorization" | "proxy-authorization" | "x-api-key" | "x-auth-token"
            )
        })
        .cloned()
        .collect()
}
fn public_mirrors(
    mirrors: &[crate::providers::models::SourceMirror],
) -> Vec<crate::providers::models::SourceMirror> {
    mirrors
        .iter()
        .map(|mirror| crate::providers::models::SourceMirror {
            label: mirror.label.clone(),
            resolver_url: mirror.resolver_url.clone(),
            headers: public_headers(&mirror.headers),
            direct_file: mirror.direct_file,
        })
        .collect()
}

fn validate_transcode_url(raw: &str) -> Option<String> {
    let url = reqwest::Url::parse(raw).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let path = url.path().to_ascii_lowercase();
    let local_or_intermediate = host == "localhost"
        || host.ends_with(".local")
        || host.contains("hubcloud.")
        || host.contains("hubdrive.")
        || host.contains("greenmotors.")
        || host.contains("greenmountmotors.")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| !is_public_transcode_ip(address));
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || local_or_intermediate
        || path.ends_with(".zip")
        || path.contains("login.php")
        || path.contains("logout")
    {
        return None;
    }
    Some(url.to_string())
}

async fn validate_transcode_destination(raw: &str) -> Option<String> {
    let normalized = validate_transcode_url(raw)?;
    let url = reqwest::Url::parse(&normalized).ok()?;
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let addresses = tokio::net::lookup_host((host, port)).await.ok()?;
    let mut found = false;
    for address in addresses {
        found = true;
        if !is_public_transcode_ip(address.ip()) {
            return None;
        }
    }
    found.then_some(normalized)
}

fn is_public_transcode_ip(address: std::net::IpAddr) -> bool {
    crate::net::is_public_ip(address)
}

fn direct_transcode_playback_source(source: &Release) -> Option<PlaybackSource> {
    if source.provider == ProviderKind::FourKHdHub {
        return None;
    }
    let mirror = source.mirrors.iter().find(|mirror| mirror.direct_file)?;
    Some(PlaybackSource {
        provider: source.provider,
        url: validate_transcode_url(&mirror.resolver_url)?,
        headers: mirror.headers.clone(),
        subtitle: None,
        source_label: mirror.label.clone(),
        max_height: source
            .quality
            .as_deref()
            .and_then(|quality| quality.trim_end_matches('p').parse().ok()),
    })
}

async fn resolve_transcode_playback_source(
    source_id: &str,
    source: &Release,
    state: &ApiState,
) -> Result<PlaybackSource, (&'static str, &'static str, u16)> {
    if source.provider != ProviderKind::FourKHdHub {
        return direct_transcode_playback_source(source).ok_or((
            "TRANSCODE_UNAVAILABLE",
            "This source has no validated direct media URL for transcoding.",
            501,
        ));
    }

    if let Some(entry) = state.playback_cache.lock().await.get(source_id).cloned()
        && entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL
    {
        return Ok(entry.playback);
    }
    let Some(client) = state.service.fourk_client.as_ref() else {
        return Err(("UPSTREAM_UNAVAILABLE", "4KHDHub is unavailable.", 503));
    };
    let resolved = tokio::time::timeout(
        OVERALL_TIMEOUT,
        client.resolve_release(source, crate::providers::models::ResolutionIntent::Playback),
    )
    .await;
    let playback = match resolved {
        Ok(Ok(playback)) => playback,
        Ok(Err(_)) => {
            return Err((
                "TRANSCODE_RESOLUTION_FAILED",
                "4KHDHub could not resolve a playable media source for transcoding.",
                502,
            ));
        }
        Err(_) => {
            return Err((
                "TRANSCODE_RESOLUTION_TIMEOUT",
                "4KHDHub media resolution timed out.",
                504,
            ));
        }
    };
    let mut cache = state.playback_cache.lock().await;
    if cache.len() >= 2048 {
        cache.retain(|_, entry| entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL);
    }
    cache.insert(
        source_id.to_string(),
        PlaybackCacheEntry {
            stored_at: Instant::now(),
            playback: playback.clone(),
        },
    );
    Ok(playback)
}

async fn transcode_source(
    stream: &mut TcpStream,
    source_id: &str,
    source: &crate::providers::models::Release,
    state: &ApiState,
) -> std::io::Result<()> {
    let Some(permit) = state.transcode_slots.clone().try_acquire_owned().ok() else {
        let (status, body) = err(
            "TRANSCODE_BUSY",
            "All H.264 transcoding slots are currently busy. Try again later.",
            Some(source.provider),
            429,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    let playback = match resolve_transcode_playback_source(source_id, source, state).await {
        Ok(playback) => playback,
        Err((code, message, status)) => {
            let (status, body) = err(code, message, Some(source.provider), status);
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let Some(validated_url) = validate_transcode_destination(&playback.url).await else {
        let (status, body) = err(
            "TRANSCODE_UNAVAILABLE",
            "The media destination is not a permitted public HTTPS source.",
            Some(source.provider),
            502,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    let mut command = Command::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-nostdin"]);
    let headers = playback
        .headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    if !headers.is_empty() {
        command.arg("-headers").arg(headers);
    }
    command.args([
        "-rw_timeout",
        "15000000",
        "-i",
        validated_url.as_str(),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-profile:v",
        "main",
        "-pix_fmt",
        "yuv420p",
        "-crf",
        "23",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-ac",
        "2",
        "-f",
        "mpegts",
        "pipe:1",
    ]);
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let (status, body) = err(
                "TRANSCODE_UNAVAILABLE",
                format!("FFmpeg is not available: {error}"),
                Some(source.provider),
                503,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let Some(mut stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        let (status, body) = err(
            "TRANSCODE_FAILED",
            "FFmpeg did not open an output stream.",
            Some(source.provider),
            502,
        );
        return write_raw_json(stream, status, &body, None).await;
    };
    let mut stderr = child.stderr.take();
    let stderr_task = tokio::spawn(async move {
        if let Some(mut stderr) = stderr.take() {
            let mut buffer = [0u8; 1024];
            while stderr.read(&mut buffer).await.unwrap_or(0) > 0 {}
        }
    });
    let mut first_chunk = vec![0u8; 32 * 1024];
    let first_len = match tokio::time::timeout(
        TRANSCODE_STARTUP_TIMEOUT,
        stdout.read(&mut first_chunk),
    )
    .await
    {
        Ok(Ok(length)) if length > 0 => length,
        Ok(_) => {
            let _ = child.kill().await;
            let _ = stderr_task.await;
            drop(permit);
            let (status, body) = err(
                "TRANSCODE_FAILED",
                "FFmpeg could not start a playable output stream.",
                Some(source.provider),
                502,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = stderr_task.await;
            drop(permit);
            let (status, body) = err(
                "TRANSCODE_STARTUP_TIMEOUT",
                "FFmpeg did not produce output before the startup timeout.",
                Some(source.provider),
                504,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    if let Err(error) = write_headers(
        stream,
        200,
        "video/mp2t",
        Some(&format!(
            "X-Transcode: h264\r\nX-Source-Id: {source_id}\r\n"
        )),
    )
    .await
    {
        let _ = child.kill().await;
        let _ = stderr_task.await;
        drop(permit);
        return Err(error);
    }
    if let Err(error) = stream.write_all(&first_chunk[..first_len]).await {
        let _ = child.kill().await;
        let _ = stderr_task.await;
        drop(permit);
        return Err(error);
    }
    let result = tokio::io::copy(&mut stdout, stream).await;
    let exit_status = if result.is_ok() {
        match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
            Ok(Ok(status)) => Some(status),
            _ => {
                let _ = child.kill().await;
                None
            }
        }
    } else {
        let _ = child.kill().await;
        None
    };
    let _ = stderr_task.await;
    drop(permit);
    result?;
    match exit_status {
        Some(status) if status.success() => Ok(()),
        _ => Err(std::io::Error::other(
            "FFmpeg transcoding did not exit successfully",
        )),
    }
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
            "/api/v1": {"get": {"responses": {"200": {"description": "API version and resources"}}}},
            "/openapi.json": {"get": {"responses": {"200": {"description": "OpenAPI document"}}}},
            "/docs": {"get": {"responses": {"200": {"description": "API documentation"}}}},
            "/api/v1/diagnostics": {"get": {"responses": {"200": {"description": "Sanitized provider diagnostics"}}}},
            "/api/v1/watch": {"get": {"responses": {"200": {"description": "Direct watch resolution"}, "400": {"description": "Invalid watch request"}}}},
            "/api/v1/anime/search": {"get": {"parameters": [{"name": "q", "in": "query", "required": true, "schema": {"type": "string", "maxLength": 200}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100}}, {"name": "cursor", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Search results"}, "400": {"description": "Invalid query"}}}},
            "/api/v1/home": {"get": {"parameters": [{"name": "page", "in": "query", "schema": {"type": "integer", "minimum": 1}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100}}], "responses": {"200": {"description": "MovieBox homepage sections"}, "503": {"description": "Provider unavailable"}}}},
            "/api/v1/home/featured": {"get": {"responses": {"200": {"description": "MovieBox homepage featured section"}}}},
            "/api/v1/latest": {"get": {"responses": {"501": {"description": "Reliable release-date metadata unavailable"}}}},
            "/api/v1/trending": {"get": {"responses": {"200": {"description": "Provider-backed trending section"}}}},
            "/api/v1/trending/anime": {"get": {"responses": {"200": {"description": "Provider-backed trending anime"}}}},
            "/api/v1/trending/movies": {"get": {"responses": {"200": {"description": "Provider-backed trending movies"}}}},
            "/api/v1/trending/k-dramas": {"get": {"responses": {"200": {"description": "Metadata-backed trending K-Dramas"}}}},
            "/api/v1/popular": {"get": {"responses": {"200": {"description": "Provider-backed popular section"}}}},
            "/api/v1/popular/anime": {"get": {"responses": {"200": {"description": "Provider-backed popular anime"}}}},
            "/api/v1/popular/movies": {"get": {"responses": {"200": {"description": "Provider-backed popular movies"}}}},
            "/api/v1/popular/k-dramas": {"get": {"responses": {"200": {"description": "Metadata-backed popular K-Dramas"}}}},
            "/api/v1/movies": {"get": {"responses": {"200": {"description": "MovieBox movie catalogue"}}}},
            "/api/v1/movies/{movie_id}": {"get": {"responses": {"200": {"description": "Movie details"}}}},
            "/api/v1/k-dramas": {"get": {"responses": {"200": {"description": "Metadata-backed K-Drama catalogue"}}}},
            "/api/v1/k-dramas/{id}": {"get": {"responses": {"501": {"description": "Unsupported category"}}}},
            "/api/v1/categories": {"get": {"responses": {"200": {"description": "Supported content categories"}}}},
            "/api/v1/anime/search/stream": {"get": {"responses": {"200": {"description": "SSE search_result, search_provider_error, search_complete events"}}}},
            "/api/v1/anime/{anime_id}": {"get": {"responses": {"200": {"description": "Anime details"}}}},
            "/api/v1/anime/{anime_id}/seasons": {"get": {"responses": {"200": {"description": "Seasons"}}}},
            "/api/v1/anime/{anime_id}/episodes": {"get": {"responses": {"200": {"description": "Episodes"}}}},
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/full": {"get": {"responses": {"200": {"description": "Anime details, selected episode, and source candidates"}, "400": {"description": "Invalid anime or episode ID"}, "404": {"description": "Episode not found"}}}},
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/stream": {"get": {"responses": {"200": {"description": "Resolved episode playback source"}, "502": {"description": "Playback resolution failed"}, "504": {"description": "Playback resolution timed out"}}}},
            "/api/v1/episodes/{episode_id}": {"get": {"responses": {"200": {"description": "Episode details"}, "400": {"description": "Invalid episode ID"}}}},
            "/api/v1/episodes/{episode_id}/sources": {"get": {"responses": {"200": {"description": "Fresh source candidates"}}}},
            "/api/v1/episodes/{episode_id}/sources/stream": {"get": {"responses": {"200": {"description": "SSE source_result, source_error, source_complete events"}}}},
            "/api/v1/sources/{source_id}": {"get": {"responses": {"200": {"description": "Resolved source metadata"}}}},
            "/api/v1/sources/{source_id}/audio": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/audio-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitles": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitle-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/transcode": {"get": {"responses": {"200": {"description": "Transcoded media stream"}, "502": {"description": "Transcoding failed"}}}},
            "/api/v1/sources/{source_id}/playback": {"get": {"responses": {"200": {"description": "Server-resolved playable 4KHDHub URL"}, "502": {"description": "Playback resolution failed"}, "504": {"description": "Playback resolution timed out"}}}},
            "/api/v1/sources/{source_id}/watch": {"get": {"responses": {"200": {"description": "Server-resolved watch URL with inline media disposition"}, "502": {"description": "Watch resolution failed"}, "504": {"description": "Watch resolution timed out"}}}},
            "/api/v1/sources/{source_id}/download": {"get": {"responses": {"200": {"description": "Download metadata"}, "501": {"description": "Unsupported"}}}}
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_preserves_utf8_and_plus_spaces() {
        assert_eq!(
            percent_decode("%E0%A6%AC%E0%A6%BE%E0%A6%82%E0%A6%B2%E0%A6%BE"),
            "বাংলা"
        );
        assert_eq!(percent_decode("hello+world"), "hello world");
        assert_eq!(
            percent_decode("%E0%A4%B9%E0%A4%BF%E0%A4%A8%E0%A5%8D%E0%A4%A6%E0%A5%80"),
            "हिन्दी"
        );
    }

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
        assert!(decoded.f.is_some());
    }
    #[test]
    fn source_fingerprint_survives_release_reordering() {
        let episode = OpaqueRef {
            p: ProviderKind::MovieBox,
            v: "subject-1".into(),
            s: 2,
            e: 5,
        };
        let first = Release {
            provider: ProviderKind::MovieBox,
            filename: "show.s02e05.1080p.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("h264".into()),
            language: Some("Hindi".into()),
            size_bytes: Some(123),
            season: Some(2),
            episode: Some(5),
            mirrors: vec![],
            resource_id: Some("resource-a".into()),
        };
        let second = Release {
            filename: "show.s02e05.720p.mkv".into(),
            quality: Some("720p".into()),
            ..first.clone()
        };
        let decoded = decode::<OpaqueSource>(&source_ref(&first, &episode, 0)).unwrap();
        let reordered = [first, second];
        let matched = decoded.f.as_deref().and_then(|fingerprint| {
            reordered
                .iter()
                .find(|release| release_fingerprint(release) == fingerprint)
        });
        assert_eq!(
            matched.map(|release| release.filename.as_str()),
            Some("show.s02e05.1080p.mkv")
        );
    }

    #[test]
    fn source_fingerprint_distinguishes_same_label_different_mirrors() {
        let mut first = Release {
            provider: ProviderKind::Addons,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("h264".into()),
            language: None,
            size_bytes: Some(100),
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Direct".into(),
                resolver_url: "https://cdn-a.example/episode.mkv".into(),
                headers: vec![],
                direct_file: true,
            }],
            resource_id: None,
        };
        let first_fingerprint = release_fingerprint(&first);
        first.mirrors[0].resolver_url = "https://cdn-b.example/episode.mkv".into();
        assert_ne!(first_fingerprint, release_fingerprint(&first));
    }
    #[test]
    fn public_headers_remove_secret_bearing_fields() {
        let headers = vec![
            ("User-Agent".into(), "player".into()),
            ("Referer".into(), "https://example.test".into()),
            ("Cookie".into(), "signed-cookie".into()),
            ("Authorization".into(), "Bearer secret".into()),
            ("X-Api-Key".into(), "secret-key".into()),
        ];
        let public = public_headers(&headers);
        assert_eq!(public.len(), 2);
        assert!(public.iter().any(|(name, _)| name == "User-Agent"));
        assert!(public.iter().any(|(name, _)| name == "Referer"));
    }

    #[test]
    fn transcode_url_rejects_credentials_private_destinations_and_non_https() {
        assert!(validate_transcode_url("https://user:pass@example.com/video.mp4").is_none());
        assert!(validate_transcode_url("https://127.0.0.1/video.mp4").is_none());
        assert!(validate_transcode_url("https://10.0.0.8/video.mp4").is_none());
        assert!(validate_transcode_url("http://example.com/video.mp4").is_none());
        assert!(validate_transcode_url("https://example.com/video.mp4").is_some());
    }

    #[test]
    fn public_transcode_ip_filter_covers_ipv4_and_ipv6_internal_ranges() {
        assert!(!is_public_transcode_ip("127.0.0.1".parse().unwrap()));
        assert!(!is_public_transcode_ip("169.254.1.1".parse().unwrap()));
        assert!(!is_public_transcode_ip("::1".parse().unwrap()));
        assert!(!is_public_transcode_ip("fc00::1".parse().unwrap()));
        assert!(!is_public_transcode_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_public_transcode_ip("::ffff:10.0.0.8".parse().unwrap()));
        assert!(is_public_transcode_ip("::ffff:8.8.8.8".parse().unwrap()));
        assert!(is_public_transcode_ip("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn malformed_and_unresolvable_provider_ids_are_rejected_or_preserved() {
        assert!(parse_ref("not-an-id").is_err());
        assert!(parse_source("not-a-source-id").is_err());

        // Regression case for the reported Lookism identifier.
        let lookism_id =
            "eyJwIjoiZm91cmtoZGh1YiIsInYiOiIvbG9va2lzbS1zZXJpZXMtMTQzMi8iLCJzIjowLCJlIjowfQ";
        let lookism = parse_ref(lookism_id).unwrap();
        assert_eq!(lookism.p, ProviderKind::FourKHdHub);
        assert_eq!(lookism.v, "/lookism-series-1432/");
        assert_eq!(lookism.s, 0);
        assert_eq!(lookism.e, 0);

        // IDs generated by legacy callers may include standard Base64 padding.
        let legacy_id = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "/lookism-series-1432/".into(),
            s: 0,
            e: 0,
        };
        let padded = URL_SAFE.encode(serde_json::to_vec(&legacy_id).unwrap());
        let parsed = parse_ref(&padded).unwrap();
        assert_eq!(parsed.p, ProviderKind::FourKHdHub);
        assert_eq!(parsed.v, "/lookism-series-1432/");
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
    fn configured_search_providers_keep_builtins_and_exclude_disabled_bdix() {
        let providers = configured_search_providers();
        assert!(providers.contains(&ProviderKind::FourKHdHub));
        assert!(providers.contains(&ProviderKind::Dramachi));
        assert!(providers.contains(&ProviderKind::Addons));
        assert!(!providers.contains(&ProviderKind::BdixCircleFtp));
        assert!(!providers.contains(&ProviderKind::BdixDhakaFlix));
        assert!(providers.contains(&ProviderKind::MovieBox));
    }

    #[test]
    fn provider_priority_matches_configured_order() {
        assert!(
            provider_priority(ProviderKind::MovieBox) < provider_priority(ProviderKind::FourKHdHub)
        );
        assert!(
            provider_priority(ProviderKind::FourKHdHub) < provider_priority(ProviderKind::Dramachi)
        );
        assert!(
            provider_priority(ProviderKind::Dramachi)
                < provider_priority(ProviderKind::BdixCircleFtp)
        );
        assert_eq!(provider_priority(ProviderKind::MovieBox), 0);
    }

    #[test]
    fn title_normalization_ignores_spacing_punctuation_and_case() {
        assert_eq!(normalize_title("Dan Da Dan"), normalize_title("Dandadan"));
        assert_eq!(
            normalize_title("My Hero Academia"),
            normalize_title("my-hero_academia!")
        );
        assert_eq!(
            normalize_title("Naruto Shippuden"),
            normalize_title("NARUTO-SHIPPUDEN")
        );
        assert_ne!(
            normalize_title("Naruto"),
            normalize_title("Naruto Shippuden")
        );
    }

    #[test]
    fn fourk_series_keeps_dramachi_for_fallback() {
        let item = |provider, media_type, id: &str| CatalogItem {
            id: ProviderMediaId {
                provider,
                value: id.to_string(),
            },
            title: id.to_string(),
            media_type,
            year: None,
            poster_url: None,
            season_count: None,
        };
        let mut items = [
            item(ProviderKind::FourKHdHub, MediaType::Series, "fourk-series"),
            item(ProviderKind::Dramachi, MediaType::Series, "drama-series"),
            item(ProviderKind::Addons, MediaType::Series, "addon-series"),
        ];
        items[0].season_count = Some(1);
        assert!(
            items
                .iter()
                .any(|item| item.id.provider == ProviderKind::FourKHdHub)
        );
        assert!(
            items
                .iter()
                .any(|item| item.id.provider == ProviderKind::Addons)
        );
        assert!(
            items
                .iter()
                .any(|item| item.id.provider == ProviderKind::Dramachi)
        );
    }

    #[test]
    fn dramachi_remains_available_when_primary_has_no_usable_series() {
        let item = |provider, media_type, id: &str| CatalogItem {
            id: ProviderMediaId {
                provider,
                value: id.to_string(),
            },
            title: id.to_string(),
            media_type,
            year: None,
            poster_url: None,
            season_count: None,
        };
        let items = [
            item(ProviderKind::FourKHdHub, MediaType::Movie, "fourk-movie"),
            item(ProviderKind::Dramachi, MediaType::Series, "drama-series"),
        ];
        assert!(
            items
                .iter()
                .any(|item| item.id.provider == ProviderKind::Dramachi)
        );

        let unknown_primary = [
            item(ProviderKind::FourKHdHub, MediaType::Series, "fourk-unknown"),
            item(ProviderKind::Dramachi, MediaType::Series, "drama-series"),
        ];
        assert!(
            unknown_primary
                .iter()
                .any(|item| item.id.provider == ProviderKind::Dramachi)
        );

        let empty_primary = [item(
            ProviderKind::Dramachi,
            MediaType::Series,
            "drama-only-series",
        )];
        assert_eq!(empty_primary.len(), 1);
        assert_eq!(empty_primary[0].id.provider, ProviderKind::Dramachi);
    }

    #[test]
    fn malformed_provider_response_remains_an_upstream_error() {
        let (status, body) = provider_error(
            ProviderError::Parsing("missing episode section".into()),
            ProviderKind::FourKHdHub,
        );
        assert_eq!(status, 502);
        assert!(body.contains("UPSTREAM_UNAVAILABLE"));
    }

    #[test]
    fn provider_network_error_remains_retryable() {
        let (status, body) = provider_error(
            ProviderError::Network("timeout".into()),
            ProviderKind::FourKHdHub,
        );
        assert_eq!(status, 503);
        assert!(body.contains("UPSTREAM_UNAVAILABLE"));
        assert!(body.contains("retryable\":true"));
    }

    #[test]
    fn transcode_uses_only_validated_direct_media_mirrors() {
        let source = Release {
            provider: ProviderKind::Dramachi,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("H.265".into()),
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Dramachi CDN".into(),
                resolver_url: "https://cdn.example/video.mkv".into(),
                headers: vec![("Referer".into(), "https://provider.example/".into())],
                direct_file: true,
            }],
            resource_id: None,
        };
        let playback = direct_transcode_playback_source(&source).unwrap();
        assert_eq!(playback.url, "https://cdn.example/video.mkv");
        assert_eq!(playback.headers, source.mirrors[0].headers);

        let mut intermediate = source.clone();
        intermediate.mirrors[0].direct_file = false;
        assert!(direct_transcode_playback_source(&intermediate).is_none());

        let mut private = source.clone();
        private.mirrors[0].resolver_url = "https://127.0.0.1/private.mkv".into();
        assert!(direct_transcode_playback_source(&private).is_none());
    }

    #[test]
    fn fourk_transcode_must_use_the_resolver_not_the_intermediate_mirror() {
        let source = Release {
            provider: ProviderKind::FourKHdHub,
            filename: "episode.mkv".into(),
            quality: Some("2160p".into()),
            codec: Some("H.265".into()),
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Download HubCloud".into(),
                resolver_url: "https://hubcloud.example/drive/abc".into(),
                headers: vec![],
                direct_file: false,
            }],
            resource_id: None,
        };
        assert!(direct_transcode_playback_source(&source).is_none());
        assert_eq!(
            source.direct_url(),
            Some("https://hubcloud.example/drive/abc")
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
            normalized_language(Some("Japanese")),
            ("ja".into(), "Japanese".into(), Some("Japanese".into()))
        );
        assert_eq!(
            normalized_language(Some("English")),
            ("en".into(), "English".into(), Some("English".into()))
        );
        assert_eq!(
            normalized_language(Some("Bengali")),
            ("bn".into(), "Bengali".into(), Some("Bengali".into()))
        );
        assert_eq!(
            normalized_language(Some("und")),
            ("und".into(), "Unknown".into(), Some("und".into()))
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
    fn search_source_eligibility_requires_a_valid_mirror_url() {
        let mut release = Release {
            provider: ProviderKind::Addons,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: None,
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![],
            resource_id: None,
        };
        assert!(!release_has_usable_source(&release));
        release
            .mirrors
            .push(crate::providers::models::SourceMirror {
                label: "invalid".into(),
                resolver_url: "not-a-url".into(),
                headers: vec![],
                direct_file: true,
            });
        assert!(!release_has_usable_source(&release));
        release.mirrors[0].resolver_url = "https://cdn.example/episode.mkv".into();
        assert!(!release_has_usable_source(&release));
        release.codec = Some("H.264".into());
        assert!(release_has_usable_source(&release));
        assert!(releases_have_usable_source(&[release]));
    }

    #[test]
    fn search_source_eligibility_rejects_incompatible_and_unknown_codecs() {
        for codec in [Some("HEVC"), Some("H.265"), Some("x265"), Some("AV1"), None] {
            assert!(
                !is_verified_h264_codec(codec),
                "codec should be rejected: {codec:?}"
            );
        }
        for codec in [Some("H.264"), Some("AVC"), Some("avc1"), Some("x264")] {
            assert!(
                is_verified_h264_codec(codec),
                "codec should be accepted: {codec:?}"
            );
        }
    }

    #[test]
    fn search_source_failures_are_not_treated_as_confirmed_absence() {
        assert!(provider_error_means_no_source(&ProviderError::NotFound));
        assert!(provider_error_means_no_source(&ProviderError::Unavailable(
            "No addon stream sources available".into(),
        )));
        assert!(!provider_error_means_no_source(&ProviderError::Network(
            "connection reset".into(),
        )));
        assert!(!provider_error_means_no_source(
            &ProviderError::RateLimited(None)
        ));
        assert!(!provider_error_means_no_source(&ProviderError::Parsing(
            "invalid response".into(),
        )));
    }

    #[test]
    fn search_source_eligibility_has_a_bounded_per_item_budget() {
        assert_eq!(SEARCH_SOURCE_CHECK_TIMEOUT, Duration::from_secs(45));
        assert!(SEARCH_SOURCE_CHECK_TIMEOUT >= SOURCE_TIMEOUT);
    }

    #[test]
    fn episode_reference_resolves_to_the_documented_episode_shape() {
        let details = MediaDetails {
            id: ProviderMediaId {
                provider: ProviderKind::Dramachi,
                value: "56914".into(),
            },
            title: "Naruto Shippuden".into(),
            media_type: MediaType::Series,
            year: Some("2007".into()),
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
                    number: 1,
                    title: Some("Enter: Naruto Uzumaki!".into()),
                    overview: None,
                }],
            }],
            dubs: vec![],
        };
        let reference = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "56914".into(),
            s: 1,
            e: 1,
        };
        let episode = api_episode_from_details(&details, &reference).unwrap();
        assert_eq!(episode.season, 1);
        assert_eq!(episode.number, 1);
        assert_eq!(episode.title.as_deref(), Some("Enter: Naruto Uzumaki!"));
        assert_eq!(decode::<OpaqueRef>(&episode.id).unwrap().v, "56914");
    }

    #[test]
    fn openapi_contains_registered_system_and_sse_routes() {
        let document = openapi();
        let paths = document.get("paths").unwrap();
        for route in [
            "/health",
            "/ready",
            "/api/v1",
            "/openapi.json",
            "/api/v1/diagnostics",
            "/api/v1/watch",
            "/api/v1/anime/search/stream",
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/full",
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/stream",
            "/api/v1/episodes/{episode_id}",
            "/api/v1/episodes/{episode_id}/sources/stream",
            "/api/v1/sources/{source_id}/audio-tracks",
            "/api/v1/sources/{source_id}/subtitle-tracks",
            "/api/v1/sources/{source_id}/transcode",
            "/api/v1/home",
            "/api/v1/home/featured",
            "/api/v1/trending",
            "/api/v1/popular",
            "/api/v1/movies",
            "/api/v1/movies/{movie_id}",
            "/api/v1/k-dramas",
            "/api/v1/categories",
        ] {
            assert!(paths.get(route).is_some(), "missing {route}");
        }
    }

    #[test]
    fn catalog_paging_is_bounded_and_defaults_safely() {
        assert_eq!(parse_page_limit("page=0&limit=999"), (1, 100));
        assert_eq!(parse_page_limit("page=bad&limit=bad"), (1, 20));
        assert_eq!(parse_page_limit(""), (1, 20));
    }

    #[test]
    fn provider_catalog_id_round_trip_preserves_movie_identity() {
        let item = CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::MovieBox,
                value: "movie-123".into(),
            },
            title: "Example Movie".into(),
            media_type: MediaType::Movie,
            year: Some("2024".into()),
            poster_url: None,
            season_count: None,
        };
        let public = catalog_item_response(&item);
        let decoded = decode::<OpaqueRef>(&public.id).unwrap();
        assert_eq!(decoded.p, ProviderKind::MovieBox);
        assert_eq!(decoded.v, "movie-123");
        assert_eq!(public.media_type, MediaType::Movie);
    }

    #[test]
    fn provider_catalog_id_round_trip_preserves_fourk_series_identity() {
        let item = CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::FourKHdHub,
                value: "/naruto-series-38/".into(),
            },
            title: "Naruto".into(),
            media_type: MediaType::Series,
            year: Some("2002".into()),
            poster_url: None,
            season_count: Some(8),
        };
        let public = catalog_item_response(&item);
        let decoded = decode::<OpaqueRef>(&public.id).unwrap();
        assert_eq!(decoded.p, ProviderKind::FourKHdHub);
        assert_eq!(decoded.v, "/naruto-series-38/");
        assert_eq!(public.media_type, MediaType::Series);
    }

    #[test]
    fn trending_and_popular_sections_use_only_real_metrics() {
        let items = vec![CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::MovieBox,
                value: "one".into(),
            },
            title: "One".into(),
            media_type: MediaType::Movie,
            year: None,
            poster_url: None,
            season_count: None,
        }];
        let metrics = HashMap::new();
        assert!(section_items(&items, &metrics, "trending", 20).0.is_empty());
        assert!(section_items(&items, &metrics, "popular", 20).0.is_empty());
    }

    #[test]
    fn classification_requires_verified_metadata_and_series_type() {
        let anime = CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::MovieBox,
                value: "anime-1".into(),
            },
            title: "Example".into(),
            media_type: MediaType::Series,
            year: None,
            poster_url: None,
            season_count: Some(1),
        };
        let movie = CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::MovieBox,
                value: "movie-1".into(),
            },
            title: "Korean Movie".into(),
            media_type: MediaType::Movie,
            year: None,
            poster_url: None,
            season_count: None,
        };
        let metrics = HashMap::from([
            (
                "anime-1".into(),
                crate::models::BrowseMetrics {
                    genres: vec!["Anime".into()],
                    category: Some("anime".into()),
                    ..Default::default()
                },
            ),
            (
                "movie-1".into(),
                crate::models::BrowseMetrics {
                    country: Some("South Korea".into()),
                    ..Default::default()
                },
            ),
        ]);
        assert!(is_classified(&anime, &metrics, Classification::Anime));
        assert!(!is_classified(&anime, &metrics, Classification::KDrama));
        assert!(!is_classified(&movie, &metrics, Classification::KDrama));
        assert!(!is_classified(
            &anime,
            &HashMap::new(),
            Classification::Anime
        ));
    }
}
