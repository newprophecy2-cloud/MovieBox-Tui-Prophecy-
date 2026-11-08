#![allow(
    dead_code,
    reason = "MovieBox catalog handlers remain compiled for a controlled future re-enable while the provider is intentionally disabled"
)]

use futures::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared},
};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{Mutex, Semaphore, mpsc},
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
#[cfg(not(test))]
const SOURCE_TIMEOUT: Duration = Duration::from_secs(15);
#[cfg(test)]
const SOURCE_TIMEOUT: Duration = Duration::from_millis(25);
#[cfg(not(test))]
const OVERALL_TIMEOUT: Duration = Duration::from_secs(45);
#[cfg(test)]
const OVERALL_TIMEOUT: Duration = Duration::from_secs(10);
const BROWSE_TIMEOUT: Duration = Duration::from_secs(6);
const CLASSIFICATION_TIMEOUT: Duration = Duration::from_secs(1);
const PLAYBACK_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(20);
const DIRECT_FALLBACK_TIMEOUT: Duration = Duration::from_secs(45);
const SEARCH_PROVIDER_CONCURRENCY: usize = 3;
const SEARCH_MAX_PAGES: usize = 3;
const TRACK_PROBE_TIMEOUT: Duration = Duration::from_secs(45);
const CODEC_VERIFICATION_WORKERS: usize = 2;
const CODEC_VERIFICATION_LEASE_SECONDS: i32 = 120;
const CODEC_VERIFICATION_IDLE: Duration = Duration::from_secs(3);
const RELEASE_CACHE_TTL: Duration = Duration::from_secs(600);
const PLAYBACK_CACHE_TTL: Duration = Duration::from_secs(120);
const SEARCH_CACHE_TTL: Duration = Duration::from_secs(60);
const SEARCH_CACHE_MAX_ENTRIES: usize = 256;
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 256 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_API_CONNECTIONS: usize = 256;
const MOVIEBOX_PROVIDER_ENABLED: bool = false;
const REMUX_STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const REMUX_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(20);
static REMUX_LIMITER: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(2)));
static REMUX_DEBUG_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const PROVIDERS: [ProviderKind; 7] = [
    ProviderKind::MovieBox,
    ProviderKind::AniZone,
    ProviderKind::FourKHdHub,
    ProviderKind::Dramachi,
    ProviderKind::BdixCircleFtp,
    ProviderKind::BdixDhakaFlix,
    ProviderKind::Addons,
];

fn moviebox_provider_enabled() -> bool {
    MOVIEBOX_PROVIDER_ENABLED && crate::config::load().moviebox_enabled
}

fn database_required() -> bool {
    std::env::var("REQUIRE_DATABASE")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

fn configured_search_providers() -> Vec<ProviderKind> {
    let config = crate::config::load();
    let mut providers = Vec::with_capacity(PROVIDERS.len());

    // Search scope follows configuration. An enabled provider that is
    // unavailable still participates and reports its failure; omitting it
    // would turn an incomplete search into a false success.
    if MOVIEBOX_PROVIDER_ENABLED && config.moviebox_enabled {
        providers.push(ProviderKind::MovieBox);
    }
    if config.fourkhdhub_enabled {
        providers.push(ProviderKind::FourKHdHub);
    }
    if config.dramachi_enabled {
        providers.push(ProviderKind::Dramachi);
    }
    if config.anizone_enabled {
        providers.push(ProviderKind::AniZone);
    }
    if config.bdix_circleftp_enabled {
        providers.push(ProviderKind::BdixCircleFtp);
    }
    if config.bdix_dhakaflix_enabled {
        providers.push(ProviderKind::BdixDhakaFlix);
    }
    if config.addons_enabled {
        providers.push(ProviderKind::Addons);
    }
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
    pub catalog: Option<Arc<crate::catalog::CatalogStore>>,
    release_cache: Arc<Mutex<HashMap<String, ReleaseCacheEntry>>>,
    release_inflight: Arc<Mutex<HashMap<String, SharedReleaseFuture>>>,
    playback_cache: Arc<Mutex<HashMap<String, PlaybackCacheEntry>>>,
    playback_inflight: Arc<Mutex<HashMap<String, SharedPlaybackFuture>>>,
    search_cache: Arc<Mutex<HashMap<String, SearchCacheEntry>>>,
    redis_search: Option<Arc<dyn SearchRedisCache>>,
    remux_cache: crate::remux_cache::RemuxCache,
    native_stream: crate::api_native_stream::NativeStreamProxy,
    connection_slots: Arc<Semaphore>,
    #[cfg(test)]
    test_release_overrides: TestReleaseOverrides,
    #[cfg(test)]
    test_release_delays: Arc<Mutex<HashMap<String, Duration>>>,
    #[cfg(test)]
    test_fallback_refs: Arc<Mutex<HashMap<String, Vec<OpaqueRef>>>>,
}

#[cfg(test)]
type TestReleaseOverrides = Arc<Mutex<HashMap<String, Result<Vec<Release>, ProviderError>>>>;

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
type SharedReleaseFuture = Shared<BoxFuture<'static, Result<Vec<Release>, ProviderError>>>;
type SharedPlaybackFuture = Shared<BoxFuture<'static, Result<PlaybackSource, String>>>;

#[derive(Clone)]
struct SearchCacheEntry {
    stored_at: Instant,
    result: SearchResultSet,
}

type SearchCacheFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ()>> + Send + 'a>>;

trait SearchRedisCache: Send + Sync {
    fn get<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, Option<Vec<u8>>>;
    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        ttl_seconds: u64,
        payload: Vec<u8>,
    ) -> SearchCacheFuture<'a, ()>;
    fn delete<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, ()>;
}

struct RedisSearchCacheConnection(redis::aio::MultiplexedConnection);

impl SearchRedisCache for RedisSearchCacheConnection {
    fn get<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut connection = self.0.clone();
            let result: Result<Option<Vec<u8>>, _> = redis::cmd("GET")
                .arg(key)
                .query_async(&mut connection)
                .await;
            result.map_err(|_| ())
        })
    }

    fn set_ex<'a>(
        &'a self,
        key: &'a str,
        ttl_seconds: u64,
        payload: Vec<u8>,
    ) -> SearchCacheFuture<'a, ()> {
        Box::pin(async move {
            let mut connection = self.0.clone();
            let result: Result<(), _> = redis::cmd("SETEX")
                .arg(key)
                .arg(ttl_seconds)
                .arg(payload)
                .query_async(&mut connection)
                .await;
            result.map_err(|_| ())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, ()> {
        Box::pin(async move {
            let mut connection = self.0.clone();
            let result: Result<(), _> = redis::cmd("DEL")
                .arg(key)
                .query_async(&mut connection)
                .await;
            result.map_err(|_| ())
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpaqueRef {
    p: ProviderKind,
    v: String,
    s: usize,
    e: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UnifiedOpaqueRef {
    p: ProviderKind,
    v: String,
    s: usize,
    e: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    canonical_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    provider_mappings: Vec<ProviderMediaId>,
    #[serde(default)]
    unified: bool,
}
#[derive(Debug, Serialize, Deserialize)]
struct OpaqueSource {
    p: ProviderKind,
    v: String,
    s: usize,
    e: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
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
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DirectPlayRequest {
    source_id: String,
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
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    provider_status_details: HashMap<String, SearchProviderStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    language_intent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verified_language_matches: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
enum SearchProviderState {
    Success,
    Empty,
    Timeout,
    Error,
    Partial,
    Disabled,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SearchProviderStatus {
    provider: String,
    status: SearchProviderState,
    result_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pages_attempted: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pages_successful: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration_ms: Option<u64>,
    /// Unix epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct SearchProviderExecutionMetadata {
    pages_attempted: Option<usize>,
    pages_successful: Option<usize>,
    duration_ms: u64,
    timestamp_unix_ms: u64,
}

#[derive(Default)]
struct SearchPageProgress {
    pages_attempted: AtomicUsize,
    pages_successful: AtomicUsize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SearchResultSet {
    items: Vec<CatalogItem>,
    failed: bool,
    provider_status: HashMap<String, String>,
    #[serde(default)]
    provider_status_details: HashMap<String, SearchProviderStatus>,
    #[serde(default)]
    canonical_ids: HashMap<String, String>,
    #[serde(default)]
    verified_aliases: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceCodecEligibility {
    Verified,
    Unverified,
    BlockedH265,
}

fn source_codec_eligibility(codec: Option<&str>) -> SourceCodecEligibility {
    let Some(codec) = codec.map(str::trim).filter(|value| !value.is_empty()) else {
        return SourceCodecEligibility::Unverified;
    };
    if crate::codec_verification::is_hevc_codec(codec) {
        return SourceCodecEligibility::BlockedH265;
    }
    if crate::codec_verification::is_allowed_codec(codec) {
        SourceCodecEligibility::Verified
    } else {
        SourceCodecEligibility::Unverified
    }
}

fn release_has_usable_source(release: &Release) -> bool {
    let has_valid_url = release.mirrors.iter().any(|mirror| {
        !mirror.resolver_url.trim().is_empty()
            && crate::net::validate_url_syntax(&mirror.resolver_url, true).is_ok()
    });
    if release.provider == ProviderKind::AniZone {
        return has_valid_url
            && release.mirrors.iter().any(|mirror| {
                mirror.resolver_url.to_ascii_lowercase().contains(".m3u8")
                    || mirror.resolver_url.to_ascii_lowercase().contains("m3u8")
            });
    }
    has_valid_url
        && source_codec_eligibility(release.codec.as_deref()) == SourceCodecEligibility::Verified
}

fn release_has_verification_candidate(release: &Release) -> bool {
    matches!(
        release.provider,
        ProviderKind::FourKHdHub | ProviderKind::Dramachi
    ) && release.mirrors.iter().any(|mirror| {
        !mirror.resolver_url.trim().is_empty()
            && crate::net::validate_url_syntax(&mirror.resolver_url, false).is_ok()
            && (release.provider == ProviderKind::FourKHdHub || mirror.direct_file)
    })
}

fn release_requires_codec_probe(release: &Release) -> bool {
    source_codec_eligibility(release.codec.as_deref()) == SourceCodecEligibility::Unverified
        && release_has_verification_candidate(release)
}

fn release_requires_playback_codec_verification(release: &Release) -> bool {
    release.provider == ProviderKind::AniZone
        || (matches!(
            release.provider,
            ProviderKind::FourKHdHub | ProviderKind::Dramachi
        ) && source_codec_eligibility(release.codec.as_deref())
            == SourceCodecEligibility::Unverified)
}

fn release_requires_direct_play_codec_verification(release: &Release) -> bool {
    release.provider != ProviderKind::AniZone
        && release_requires_playback_codec_verification(release)
}

fn direct_play_compatibility(provider: ProviderKind) -> &'static str {
    if provider == ProviderKind::AniZone {
        "unverified"
    } else {
        "verified"
    }
}

async fn verify_direct_playback_codec(
    release: &Release,
    playback: &PlaybackSource,
) -> Result<Release, String> {
    let mut verified_release = release.clone();
    if release_requires_direct_play_codec_verification(release) {
        let verification_started = Instant::now();
        match crate::codec_verification::verify_playback_source(playback)
            .await
            .map_err(|_| format!("{} media codec could not be verified", release.provider))?
        {
            crate::codec_verification::CodecProbeResult::Allowed(codec) => {
                verified_release.codec = Some(codec);
            }
            crate::codec_verification::CodecProbeResult::BlockedHevc(_) => {
                return Err(format!(
                    "{} source blocked by H.265/HEVC playback policy",
                    release.provider
                ));
            }
            crate::codec_verification::CodecProbeResult::Unsupported(_)
            | crate::codec_verification::CodecProbeResult::NoVideoStream => {
                return Err(format!(
                    "confirmed_codec_incompatibility:{}",
                    release.provider
                ));
            }
            crate::codec_verification::CodecProbeResult::Unverified(_) => {
                return Err("codec_unverified".into());
            }
        }
        log::info!(
            "direct_play_timing provider={} stage=codec_compatibility_verification elapsed_ms={} outcome=verified",
            release.provider,
            verification_started.elapsed().as_millis()
        );
    }
    Ok(verified_release)
}

fn is_blocked_h265_codec(codec: Option<&str>) -> bool {
    source_codec_eligibility(codec) == SourceCodecEligibility::BlockedH265
}

fn codec_ineligibility_error(releases: &[Release]) -> Option<(&'static str, &'static str)> {
    if releases.is_empty()
        || releases.iter().any(|release| {
            source_codec_eligibility(release.codec.as_deref()) == SourceCodecEligibility::Verified
                || (release.provider == ProviderKind::AniZone && release_has_usable_source(release))
                || release_requires_codec_probe(release)
        })
    {
        return None;
    }
    if releases
        .iter()
        .any(|release| is_blocked_h265_codec(release.codec.as_deref()))
    {
        Some((
            "SOURCE_CODEC_BLOCKED",
            "H.265/HEVC sources are blocked by playback policy.",
        ))
    } else {
        Some((
            "CODEC_UNVERIFIED",
            "The source video codec is missing or unrecognized and cannot be played.",
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MediaMode {
    Remux,
}

fn media_mode_after_remux_preflight(preflight_validated: bool) -> Option<MediaMode> {
    preflight_validated.then_some(MediaMode::Remux)
}

fn releases_have_usable_source(releases: &[Release]) -> bool {
    releases.iter().any(release_has_usable_source)
}

#[derive(Debug, Serialize)]
struct ApiCatalogItem {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    canonical_id: Option<String>,
    title: String,
    media_type: crate::providers::models::MediaType,
    year: Option<String>,
    poster_url: Option<String>,
    season_count: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    provider_mappings: Vec<ApiProviderMapping>,
}

#[derive(Debug, Clone, Serialize)]
struct ApiProviderMapping {
    id: String,
    provider: ProviderKind,
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
struct UnifiedApiDetails {
    id: String,
    canonical_id: String,
    provider_mappings: Vec<ApiProviderMapping>,
    partial: bool,
    provider_status: HashMap<String, String>,
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
    seasons: Vec<UnifiedApiSeason>,
    audio: Vec<crate::providers::models::AudioTrackOption>,
}

#[derive(Debug, Serialize)]
struct UnifiedApiSeason {
    canonical_id: String,
    number: usize,
    episode_count: usize,
    provider_mappings: Vec<ApiSeasonProviderMapping>,
    episodes: Vec<UnifiedApiEpisode>,
}

#[derive(Debug, Serialize)]
struct ApiSeasonProviderMapping {
    provider: ProviderKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider_season_id: Option<String>,
    provider_season_number: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct UnifiedApiEpisode {
    id: String,
    canonical_id: String,
    provider_mappings: Vec<ApiProviderMapping>,
    #[serde(skip)]
    provider_lookups: Vec<UnifiedEpisodeProviderLookup>,
    #[serde(skip)]
    mapping_status: String,
    season: usize,
    number: usize,
    title: Option<String>,
    overview: Option<String>,
}

#[derive(Debug, Clone)]
struct UnifiedEpisodeProviderLookup {
    provider: ProviderKind,
    provider_title_id: String,
    provider_season_id: Option<String>,
    provider_episode_id: Option<String>,
    provider_season_number: usize,
    provider_episode_number: usize,
    lookup_reference: String,
    provider_title: Option<String>,
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
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    provider_status: HashMap<String, String>,
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
        for (name, value) in &mirror.headers {
            hasher.update(name.to_ascii_lowercase().as_bytes());
            hasher.update([0]);
            hasher.update(value.as_bytes());
            hasher.update([0]);
        }
        hasher.update([0xff]);
    }
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}
fn source_ref(r: &Release, episode: &OpaqueRef, i: usize) -> String {
    source_ref_for_content(r, episode, i, None)
}
fn source_ref_for_content(
    r: &Release,
    episode: &OpaqueRef,
    i: usize,
    content_type: Option<&str>,
) -> String {
    encode(&OpaqueSource {
        p: r.provider,
        v: episode.v.clone(),
        s: episode.s,
        e: episode.e,
        content_type: content_type.map(str::to_string),
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
fn fallback_canonical_id(item: &CatalogItem) -> String {
    let normalized = crate::unified::normalize_anime_title(&item.title);
    let year = item
        .year
        .as_deref()
        .and_then(|value| value.parse::<i32>().ok())
        .or(normalized.year_marker)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let identity = format!("{:?}|{}|{}", item.media_type, normalized.key, year);
    let mut hasher = Sha256::new();
    hasher.update(identity.as_bytes());
    format!("local-{}", URL_SAFE_NO_PAD.encode(hasher.finalize()))
}

fn fallback_singleton_canonical_id(item: &CatalogItem) -> String {
    let identity = format!(
        "{}|{}|{}",
        fallback_canonical_id(item),
        item.id.provider.cache_key(),
        item.id.value
    );
    let mut hasher = Sha256::new();
    hasher.update(identity.as_bytes());
    format!("local-{}", URL_SAFE_NO_PAD.encode(hasher.finalize()))
}

fn catalog(
    item: crate::unified::UnifiedCatalogItem,
    canonical_id: Option<String>,
) -> ApiCatalogItem {
    let poster_url = item
        .primary
        .poster_url
        .clone()
        .filter(|value| crate::net::validate_url_syntax(value, true).is_ok())
        .or_else(|| {
            item.provider_items.iter().find_map(|candidate| {
                candidate
                    .poster_url
                    .clone()
                    .filter(|value| crate::net::validate_url_syntax(value, true).is_ok())
            })
        });
    let year = item.primary.year.clone().or_else(|| {
        item.provider_items.iter().find_map(|candidate| {
            candidate
                .year
                .clone()
                .filter(|value| !value.trim().is_empty())
        })
    });
    let season_count = item.primary.season_count.or_else(|| {
        item.provider_items
            .iter()
            .find_map(|candidate| candidate.season_count)
    });
    let fallback_item = CatalogItem {
        year: year.clone(),
        ..item.primary.clone()
    };
    let canonical_id = canonical_id.unwrap_or_else(|| {
        if item.provider_mappings.is_empty() {
            fallback_singleton_canonical_id(&fallback_item)
        } else {
            fallback_canonical_id(&fallback_item)
        }
    });
    let provider_id_mappings = item.provider_mappings.clone();
    let provider_mappings = item
        .provider_mappings
        .into_iter()
        .map(|id| ApiProviderMapping {
            id: provider_ref(&id, 0, 0),
            provider: id.provider,
        })
        .collect::<Vec<_>>();
    ApiCatalogItem {
        id: encode(&UnifiedOpaqueRef {
            p: item.primary.id.provider,
            v: item.primary.id.value.clone(),
            s: 0,
            e: 0,
            canonical_id: Some(canonical_id.clone()),
            provider_mappings: provider_id_mappings,
            unified: true,
        }),
        canonical_id: Some(canonical_id),
        title: item.primary.title,
        media_type: item.primary.media_type,
        year,
        poster_url,
        season_count,
        provider_mappings,
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
                "PROVIDER_TIMEOUT"
                    | "SOURCE_TIMEOUT"
                    | "UPSTREAM_UNAVAILABLE"
                    | "REMUX_BUSY"
                    | "MEDIA_CACHE_FULL"
                    | "MEDIA_CACHE_IO_ERROR"
                    | "REMUX_TIMEOUT"
            ),
            request_id: request_id(),
        },
    };
    (status, serde_json::to_string(&body).unwrap())
}
fn provider_error(e: ProviderError, p: ProviderKind) -> (u16, String) {
    log::warn!(
        "provider_failure provider={} category={}",
        p,
        provider_error_category(&e)
    );
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
fn provider_error_category(error: &ProviderError) -> &'static str {
    match error {
        ProviderError::Network(message) => {
            let message = message.to_ascii_lowercase();
            if message.contains("timeout") || message.contains("timed out") {
                "timeout"
            } else if message.contains("502") {
                "upstream_http_error"
            } else {
                "connection_failure"
            }
        }
        ProviderError::RateLimited(_) => "rate_limited",
        ProviderError::NotFound => "not_found",
        ProviderError::Parsing(_) => "parsing_failure",
        ProviderError::Unavailable(message) => {
            let message = message.to_ascii_lowercase();
            if message.contains("no direct provider url") || message.contains("missing media url") {
                "missing_media_url"
            } else if message.contains("redirect") {
                "redirect_failure"
            } else if message.contains("codec") || message.contains("media") {
                "media_validation_failure"
            } else if message.contains("url") {
                "invalid_media_url"
            } else {
                "provider_unavailable"
            }
        }
    }
}
fn direct_resolution_category(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("confirmed_codec_incompatibility") {
        "confirmed_codec_incompatibility"
    } else if error.contains("codec_unverified") {
        "codec_unverified"
    } else if error.contains("ffprobe_failure") {
        "ffprobe_failure"
    } else if error.contains("invalid_media_sample") {
        "invalid_media_sample"
    } else if error.contains("hls_manifest_failure") {
        "hls_manifest_failure"
    } else if error.contains("hls_variant_failure") {
        "hls_variant_failure"
    } else if error.contains("hls_segment_failure") {
        "hls_segment_failure"
    } else if error.contains("hls_encryption_unsupported") {
        "hls_encryption_unsupported"
    } else if error.contains("provider_timeout") || error.contains("timeout") {
        "provider_timeout"
    } else if error.contains("provider_network_failure") {
        "provider_network_failure"
    } else if error.contains("no direct provider url") || error.contains("missing media url") {
        "missing_media_url"
    } else if error.contains("url rejected") || error.contains("invalid url") {
        "invalid_media_url"
    } else if error.contains("redirect") {
        "redirect_failure"
    } else {
        "direct_resolution_failure"
    }
}
fn parse_ref(value: &str) -> Result<OpaqueRef, (u16, String)> {
    decode(value).map_err(|_| err("INVALID_ID", "The identifier is invalid.", None, 400))
}
fn parse_source(value: &str) -> Result<OpaqueSource, (u16, String)> {
    decode(value).map_err(|_| err("INVALID_ID", "The source identifier is invalid.", None, 400))
}

async fn init_redis_search_cache() -> Option<redis::aio::MultiplexedConnection> {
    let url = std::env::var("REDIS_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())?;
    let client = match redis::Client::open(url) {
        Ok(client) => client,
        Err(error) => {
            log::warn!("REDIS_URL is invalid; using in-memory search cache: {error}");
            return None;
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(2),
        client.get_multiplexed_async_connection(),
    )
    .await
    {
        Ok(Ok(connection)) => {
            log::info!("Redis search cache enabled");
            Some(connection)
        }
        Ok(Err(error)) => {
            log::warn!("Redis search cache unavailable; using in-memory cache: {error}");
            None
        }
        Err(_) => {
            log::warn!("Redis search cache connection timed out; using in-memory cache");
            None
        }
    }
}

pub async fn run() -> std::io::Result<()> {
    let config = crate::config::load();
    config
        .validate_hls_chunk_settings()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let media_settings = config
        .validate_media_settings()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let cache_ttl_seconds = media_settings.cache_ttl.as_secs();
    let cache_max_bytes = media_settings.cache_max_bytes;
    let max_media_jobs = media_settings.max_concurrent_jobs;
    let media_job_timeout_seconds = media_settings.job_timeout.as_secs();
    let remux_cache = crate::remux_cache::RemuxCache::open(media_settings)
        .await
        .map_err(|error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("could not initialize temporary media cache: {error}"),
            )
        })?;
    let native_stream =
        crate::api_native_stream::NativeStreamProxy::new(config.hls_max_segment_bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let janitor_cache = remux_cache.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if let Err(error) = janitor_cache.cleanup_expired().await {
                log::warn!("temporary media cache cleanup failed: {error}");
            }
        }
    });
    log::info!(
        "temporary media cache initialized ttl_seconds={cache_ttl_seconds} max_bytes={cache_max_bytes} max_concurrent_jobs={max_media_jobs} job_timeout_seconds={media_job_timeout_seconds}; capacity is not production-validated"
    );
    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse::<u16>()
        .unwrap_or(8080);
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let redis_search = init_redis_search_cache().await.map(|connection| {
        Arc::new(RedisSearchCacheConnection(connection)) as Arc<dyn SearchRedisCache>
    });
    let catalog = match crate::catalog::CatalogStore::connect_from_env().await {
        Ok(catalog) => catalog.map(Arc::new),
        Err(error) => {
            log::error!("PostgreSQL catalog initialization failed: {error}");
            None
        }
    };
    log::info!(
        "PostgreSQL catalog initialized: {}; database required for readiness: {}",
        catalog.is_some(),
        database_required()
    );
    let state = ApiState {
        service: Arc::new(MovieBoxService::new()),
        catalog,
        release_cache: Arc::new(Mutex::new(HashMap::new())),
        release_inflight: Arc::new(Mutex::new(HashMap::new())),
        playback_cache: Arc::new(Mutex::new(HashMap::new())),
        playback_inflight: Arc::new(Mutex::new(HashMap::new())),
        search_cache: Arc::new(Mutex::new(HashMap::new())),
        redis_search,
        remux_cache,
        native_stream,
        connection_slots: Arc::new(Semaphore::new(MAX_API_CONNECTIONS)),
        #[cfg(test)]
        test_release_overrides: Arc::new(Mutex::new(HashMap::new())),
        #[cfg(test)]
        test_release_delays: Arc::new(Mutex::new(HashMap::new())),
        #[cfg(test)]
        test_fallback_refs: Arc::new(Mutex::new(HashMap::new())),
    };
    start_codec_verification_workers(state.clone());
    start_anime_import_workers(state.clone());
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
    let header_end = buf
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .unwrap()
        + 4;
    let raw = String::from_utf8_lossy(&buf[..header_end]).into_owned();
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
    let request_headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect::<HashMap<_, _>>();
    let body_length = request_headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    if body_length > MAX_REQUEST_BODY_BYTES {
        return write_json(
            &mut stream,
            413,
            &serde_json::json!({"error":{"code":"REQUEST_BODY_TOO_LARGE","message":"The request body exceeds the permitted size."}}),
            Some("Connection: close\r\n"),
        ).await;
    }
    while buf.len() < header_end + body_length {
        let n = tokio::time::timeout(REQUEST_READ_TIMEOUT, stream.read(&mut chunk))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "request body read timed out")
            })??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    if buf.len() < header_end + body_length {
        return write_json(
            &mut stream,
            400,
            &serde_json::json!({"error":{"code":"INCOMPLETE_REQUEST_BODY","message":"The request body is incomplete."}}),
            Some("Connection: close\r\n"),
        ).await;
    }
    let body = buf[header_end..header_end + body_length].to_vec();
    if method == "OPTIONS" {
        return write_headers(
            &mut stream,
            204,
            "text/plain; charset=utf-8",
            Some("Content-Length: 0\r\n"),
        )
        .await;
    }
    let allowed_method = matches!(method, "GET" | "HEAD")
        || (method == "POST"
            && (target == "/api/v1/play/direct"
                || target == "/api/pretranscode/start"
                || target.starts_with("/api/v1/admin/")
                || target.starts_with("/api/v1/web/")
                || target.starts_with("/api/v1/android/")));
    if !allowed_method {
        return write_json(&mut stream, 405, &serde_json::json!({"error":{"code":"METHOD_NOT_ALLOWED","message":"Only GET is supported."}}), None).await;
    }
    let started = Instant::now();
    let metric_route = normalized_metric_route(target);
    let result = route(
        &mut stream,
        target,
        state,
        &request_headers,
        &body,
        method,
        method == "HEAD",
    )
    .await;
    crate::observability::record_request(
        &metric_route,
        0,
        started.elapsed().as_millis() as u64,
        result
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::TimedOut),
    );
    result
}

fn normalized_metric_route(target: &str) -> String {
    let path = target.split_once('?').map_or(target, |(path, _)| path);
    let known = [
        "/",
        "/health",
        "/ready",
        "/openapi.json",
        "/docs",
        "/api/v1",
        "/api/v1/search",
        "/api/v1/diagnostics",
        "/api/v1/metrics",
        "/api/v1/home",
        "/api/v1/genres",
        "/api/v1/trending",
        "/api/v1/popular",
        "/api/v1/latest",
        "/api/v1/movies",
        "/api/v1/k-dramas",
        "/api/v1/anime/search",
        "/api/v1/anime/search/stream",
        "/api/v1/watch",
    ];
    if known.contains(&path) {
        return path.to_string();
    }
    if path.starts_with("/api/v1/admin/catalog/jobs/") {
        return "/api/v1/admin/catalog/jobs/{job_id}".into();
    }
    if path.starts_with("/api/v1/catalog/titles/") {
        if path.ends_with("/seasons") {
            return "/api/v1/catalog/titles/{title_id}/seasons".into();
        }
        if path.contains("/seasons/") && path.ends_with("/episodes") {
            return "/api/v1/catalog/titles/{title_id}/seasons/{season}/episodes".into();
        }
        return "/api/v1/catalog/titles/{title_id}".into();
    }
    if path.starts_with("/api/v1/catalog/episodes/") {
        return if path.ends_with("/sources") {
            "/api/v1/catalog/episodes/{episode_id}/sources".into()
        } else {
            "/api/v1/catalog/episodes/{episode_id}".into()
        };
    }
    if path.starts_with("/api/v1/catalog/sources/") {
        return "/api/v1/catalog/sources/{source_id}".into();
    }
    if path.starts_with("/api/v1/anime/") {
        return "/api/v1/anime/{anime_id}".into();
    }
    if path.starts_with("/api/v1/episodes/") {
        return "/api/v1/episodes/{episode_id}".into();
    }
    if path.starts_with("/api/v1/sources/") {
        return "/api/v1/sources/{source_id}".into();
    }
    "/__unknown__".into()
}

async fn route(
    stream: &mut TcpStream,
    target: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    body: &[u8],
    method: &str,
    head_only: bool,
) -> std::io::Result<()> {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let normalized_path = if path == "/diagnostics" {
        Some("/api/v1/diagnostics".to_string())
    } else if path == "/watch" {
        Some("/api/v1/watch".to_string())
    } else if path == "/home"
        || path == "/trending"
        || path == "/popular"
        || path == "/latest"
        || path == "/categories"
        || path == "/movies"
        || path == "/k-dramas"
        || path.starts_with("/home/")
        || path.starts_with("/trending/")
        || path.starts_with("/popular/")
        || path.starts_with("/anime/")
        || path.starts_with("/episodes/")
        || path.starts_with("/sources/")
        || path.starts_with("/movies/")
        || path.starts_with("/k-dramas/")
    {
        Some(format!("/api/v1{path}"))
    } else {
        None
    };
    let path = normalized_path.as_deref().unwrap_or(path);
    match path {
        "/" => write_json(stream, 200, &serde_json::json!({"name":"MovieBox API","version":"v1","docs":"/docs","health":"/health"}), None).await,
        "/health" => write_json(stream, 200, &serde_json::json!({"status":"ok"}), None).await,
        "/ready" => readiness_route(stream, state).await,
        "/api/v1" => write_json(stream, 200, &serde_json::json!({"version":"v1","resources":["anime","movies","episodes","sources"]}), None).await,
        "/openapi.json" => write_json(stream, 200, &openapi(), None).await,
        "/docs" => write_text(stream, 200, "text/html; charset=utf-8", "<!doctype html><html><head><title>MovieBox API</title></head><body><h1>MovieBox API</h1><p>OpenAPI: <a href=\"/openapi.json\">/openapi.json</a></p><p>Swagger UI can load this document from a local or trusted client.</p></body></html>", None).await,
        "/api/v1/search" if method == "GET" => catalog_search_route(stream, query, state).await,
        "/api/v1/play/direct" if method == "POST" => {
            direct_play_route(stream, state, body).await
        }
        _ if path.starts_with("/api/v1/web/") => web_playback_contract_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/android/") => android_playback_contract_route(stream, path, state).await,
        "/api/v1/admin/catalog/import" if method == "POST" => admin_catalog_import_route(stream, state, request_headers, body).await,
        "/api/v1/admin/catalog/import-by-name" if method == "POST" => admin_catalog_import_by_name_route(stream, state, request_headers, body).await,
        "/api/v1/admin/catalog/update-all" if method == "POST" => admin_catalog_update_route(stream, state, request_headers).await,
        "/api/v1/admin/catalog/titles/upsert" if method == "POST" => admin_catalog_upsert_route(stream, state, request_headers, body).await,
        "/api/v1/admin/catalog/inspect" if method == "GET" => admin_catalog_inspect_route(stream, query, state, request_headers).await,
        _ if path.starts_with("/api/v1/admin/catalog/jobs/") && method == "GET" => admin_catalog_job_route(stream, path, state, request_headers).await,
        "/api/v1/home" if state.catalog.is_some() => catalog_home_route(stream, query, state).await,
        "/api/v1/genres" => catalog_genres_route(stream, query, state).await,
        "/api/v1/trending" if state.catalog.is_some() => catalog_listing_route(stream, query, state, None, "trending").await,
        "/api/v1/popular" if state.catalog.is_some() => catalog_listing_route(stream, query, state, None, "popular").await,
        "/api/v1/latest" if state.catalog.is_some() => catalog_listing_route(stream, query, state, None, "latest").await,
        "/api/v1/movies" if state.catalog.is_some() => catalog_listing_route(stream, query, state, Some("movie"), "movies").await,
        "/api/v1/k-dramas" if state.catalog.is_some() => catalog_listing_route(stream, query, state, Some("k_drama"), "k-dramas").await,
        _ if path.starts_with("/api/v1/catalog/titles/") && path.ends_with("/seasons") => catalog_seasons_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/titles/") && path.contains("/seasons/") && path.ends_with("/episodes") => catalog_episodes_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/episodes/") && path.ends_with("/sources") => catalog_episode_sources_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/episodes/") => catalog_episode_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/sources/") && path.ends_with("/tracks") => catalog_source_tracks_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/sources/") && path.ends_with("/remux") => catalog_source_remux_route(stream, path, query, state, request_headers, head_only).await,
        _ if path.starts_with("/api/v1/catalog/sources/") && path.ends_with("/play") => catalog_source_play_route(stream, path, query, state, request_headers, head_only).await,
        _ if path.starts_with("/api/v1/catalog/sources/") => catalog_source_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/catalog/titles/") => catalog_title_detail_route(stream, path, state).await,
        "/api/v1/diagnostics" => diagnostics_route(stream, state).await,
        "/api/v1/metrics" | "/metrics" => {
            write_json(stream, 200, &crate::observability::snapshot(), None).await
        }
        "/api/stream-proxy" => {
            stream_proxy_route(stream, query, state, request_headers, head_only).await
        }
        _ if path.starts_with("/api/v1/sources/")
            && path.trim_matches('/').split('/').nth(4) == Some("native") =>
        {
            native_stream_route(stream, path, query, state, request_headers, method, head_only)
                .await
        }
        _ if path.starts_with("/api/v1/sources/")
            && path.trim_matches('/').split('/').nth(4) == Some("direct") =>
        {
            direct_stream_route(stream, path, state, request_headers, method, head_only).await
        }
        "/api/media-info" => media_info_route(stream, query, request_headers).await,
        "/api/subtitles/extract" => subtitle_extract_route(stream, query, request_headers).await,
        "/api/pretranscode/start" => pretranscode_unavailable(stream, 501).await,
        "/api/pretranscode/status" => pretranscode_unavailable(stream, 501).await,
        _ if path.starts_with("/api/subtitles/assemblyai/") => assemblyai_unavailable(stream).await,
        _ if path.starts_with("/api/subtitles/") => pretranscode_unavailable(stream, 404).await,
        "/api/v1/watch" => direct_watch_route(stream, query, state).await,
        "/api/v1/anime/search" => search_json(stream, query, state).await,
        "/api/v1/anime/search/stream" => search_sse(stream, query, state).await,
        "/api/v1/home" | "/api/v1/home/featured" | "/api/v1/trending"
        | "/api/v1/trending/anime" | "/api/v1/trending/movies"
        | "/api/v1/trending/k-dramas" | "/api/v1/popular"
        | "/api/v1/popular/anime" | "/api/v1/popular/movies"
        | "/api/v1/popular/k-dramas" => home_compat_route(stream, path, query, state).await,
        "/api/v1/latest" => unsupported_catalog(stream, "Reliable release-date metadata is not exposed consistently by the available providers.").await,
        "/api/v1/movies" if moviebox_provider_enabled() => {
            catalog_list_route(stream, query, state, MediaType::Movie).await
        }
        "/api/v1/k-dramas" if moviebox_provider_enabled() => {
            classified_list_route(stream, query, state, Classification::KDrama).await
        }
        "/api/v1/categories" if moviebox_provider_enabled() => {
            categories_route(stream, state).await
        }
        "/api/v1/movies" | "/api/v1/k-dramas" | "/api/v1/categories" if !moviebox_provider_enabled() => {
            moviebox_disabled(stream).await
        }
        _ if path.starts_with("/api/v1/movies/") && !moviebox_provider_enabled() => moviebox_disabled(stream).await,
        _ if path.starts_with("/api/v1/movies/") => movie_route(stream, path, query, state).await,
        _ if path.starts_with("/api/v1/k-dramas/") => unsupported_catalog(stream, "K-Drama details require a verified item classification before lookup.").await,
        _ if path.starts_with("/api/v1/anime/") => anime_route(stream, path, query, state).await,
        _ if path.starts_with("/api/v1/episodes/") => episode_route(stream, path, state).await,
        _ if path.starts_with("/api/v1/sources/") => {
            source_route(stream, path, query, state, request_headers, head_only).await
        }
        _ => { let (s,b) = err("INTERNAL_ERROR", "Route not found.", None, 404); write_raw_json(stream, s, &b, None).await }
    }
}

fn admin_authorized(headers: &HashMap<String, String>) -> bool {
    let Some(expected) = std::env::var("ADMIN_TOKEN").ok().filter(|v| !v.is_empty()) else {
        return false;
    };
    let supplied = headers.get("x-admin-token").cloned().or_else(|| {
        headers
            .get("authorization")
            .and_then(|v| v.strip_prefix("Bearer ").map(str::to_owned))
    });
    supplied.is_some_and(|value| value == expected)
}

fn query_number(query: &str, key: &str, default: i64) -> i64 {
    query_value(query, key)
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

async fn readiness_route(stream: &mut TcpStream, state: ApiState) -> std::io::Result<()> {
    let catalog_ready = state.catalog.is_some();
    if database_required() && !catalog_ready {
        return write_json(
            stream,
            503,
            &serde_json::json!({
                "status": "not_ready",
                "dependencies": {"postgresql": "unavailable"},
                "meaning": "The PostgreSQL catalog is required for this deployment but is not initialized."
            }),
            None,
        )
        .await;
    }
    write_json(
        stream,
        200,
        &serde_json::json!({
            "status": "ready",
            "dependencies": {"postgresql": if catalog_ready { "ready" } else { "optional_not_configured" }},
            "meaning": "The process and mandatory dependencies are initialized; upstream availability is request-specific."
        }),
        None,
    )
    .await
}

async fn web_playback_contract_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let database_missing = state.catalog.is_none();
    let (status, code, message) = if database_missing {
        (
            503,
            "DATABASE_UNAVAILABLE",
            "The catalog database is not configured.",
        )
    } else {
        (
            404,
            "NO_COMPATIBLE_SOURCE",
            "No verified non-HEVC stored source is available for this Web session.",
        )
    };
    write_json(stream, status, &serde_json::json!({"success":false,"error":{"code":code,"message":message,"retryable":false},"meta":{"request_id":request_id(),"route":path,"policy":"web_live_remux_requires_verified_stored_source"}}), None).await
}

async fn android_playback_contract_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let database_missing = state.catalog.is_none();
    let (status, code, message) = if database_missing {
        (
            503,
            "DATABASE_UNAVAILABLE",
            "The catalog database is not configured.",
        )
    } else {
        (
            404,
            "NO_COMPATIBLE_SOURCE",
            "No verified Android-compatible non-HEVC direct-play source is available.",
        )
    };
    write_json(stream, status, &serde_json::json!({"success":false,"error":{"code":code,"message":message,"retryable":false},"meta":{"request_id":request_id(),"route":path,"policy":"android_direct_play_only"}}), None).await
}

async fn catalog_search_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    let Some(q) = query_value(query, "q").filter(|v| !v.trim().is_empty()) else {
        return write_json(stream, 400, &serde_json::json!({"success":false,"error":{"code":"INVALID_REQUEST","message":"q is required.","retryable":false}}), None).await;
    };
    match catalog.search(&q, query_number(query, "limit", 20), query_number(query, "offset", 0)).await {
        Ok(items) if items.is_empty() => {
            if catalog.enqueue_pending_import(&q).await.is_err() {
                return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"QUEUE_UNAVAILABLE","message":"The missing-title request could not be queued.","retryable":true},"meta":{"request_id":request_id()}}), None).await;
            }
            write_json(stream, 200, &serde_json::json!({"success":true,"data":{"items":[],"pending_import":true},"meta":{"request_id":request_id()}}), None).await
        }
        Ok(items) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"items":items,"pending_import":false},"meta":{"request_id":request_id()}}), None).await,
        Err(crate::catalog::CatalogError::Validation(message)) => write_json(stream, 400, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":message,"retryable":false},"meta":{"request_id":request_id()}}), None).await,
        Err(error) => {
            log::error!(
                "catalog operation failed operation=search category={} error={error:?}",
                error.category()
            );
            write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true},"meta":{"request_id":request_id()}}), None).await
        },
    }
}

async fn catalog_home_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog else {
        return catalog_search_route(stream, query, state).await;
    };
    let limit = query_number(query, "limit", 20);
    let mut sections = Vec::new();
    for (key, content_type) in [
        ("featured", None),
        ("anime", Some("anime")),
        ("movies", Some("movie")),
        ("k_dramas", Some("k_drama")),
    ] {
        match catalog
            .list_titles(content_type, limit, query_number(query, "offset", 0))
            .await
        {
            Ok(items) => sections.push(serde_json::json!({"key":key,"items":items})),
            Err(error) => {
                log::error!(
                    "catalog operation failed operation=home_list_titles category={} error={error:?}",
                    error.category()
                );
                return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await;
            }
        }
    }
    write_json(stream, 200, &serde_json::json!({"success":true,"data":{"sections":sections,"source":"postgresql_catalog"},"meta":{"request_id":request_id()}}), None).await
}

async fn catalog_listing_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    content_type: Option<&str>,
    section: &str,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    match catalog.list_titles(content_type, query_number(query, "limit", 20), query_number(query, "offset", 0)).await {
        Ok(items) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"section":section,"items":items,"source":"postgresql_catalog"},"meta":{"request_id":request_id()}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
    }
}

async fn catalog_genres_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    match catalog.list_genres(query_number(query, "limit", 100), query_number(query, "offset", 0)).await {
        Ok(items) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"items":items,"source":"postgresql_catalog"},"meta":{"request_id":request_id()}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
    }
}

async fn catalog_title_detail_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    let Some(id) = path
        .strip_prefix("/api/v1/catalog/titles/")
        .filter(|value| !value.is_empty())
    else {
        return write_json(stream, 400, &serde_json::json!({"success":false,"error":{"code":"INVALID_REQUEST","message":"A catalog title ID is required.","retryable":false}}), None).await;
    };
    match catalog.get_title(id).await {
        Ok(Some(_title)) => match catalog.get_title_detail(id).await {
            Ok(Some(detail)) => write_json(stream, 200, &serde_json::json!({"success":true,"data":detail,"meta":{"request_id":request_id()}}), None).await,
            Ok(None) => write_json(stream, 404, &serde_json::json!({"success":false,"error":{"code":"CATALOG_TITLE_NOT_FOUND","message":"The title was not found in the catalog.","retryable":false}}), None).await,
            Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
        },
        Ok(None) => write_json(stream, 404, &serde_json::json!({"success":false,"error":{"code":"TITLE_NOT_FOUND","message":"The title was not found in the catalog.","retryable":false}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
    }
}

async fn catalog_id_route(
    stream: &mut TcpStream,
    value: Result<Vec<serde_json::Value>, crate::catalog::CatalogError>,
) -> std::io::Result<()> {
    match value { Ok(items) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"items":items,"resource_type":"catalog_collection"},"meta":{"request_id":request_id()}}), None).await, Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await }
}
async fn catalog_seasons_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path
        .strip_prefix("/api/v1/catalog/titles/")
        .unwrap_or("")
        .trim_end_matches("/seasons");
    catalog_id_route(stream, c.list_seasons(id).await).await
}
async fn catalog_episodes_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let rest = path.strip_prefix("/api/v1/catalog/titles/").unwrap_or("");
    let mut parts = rest.split("/seasons/");
    let title = parts.next().unwrap_or("");
    let season = parts
        .next()
        .unwrap_or("")
        .trim_end_matches("/episodes")
        .parse()
        .unwrap_or(0);
    catalog_id_route(stream, c.list_episodes(title, season).await).await
}
async fn catalog_episode_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path.strip_prefix("/api/v1/catalog/episodes/").unwrap_or("");
    match c.get_episode(id).await { Ok(Some(episode))=>write_json(stream,200,&serde_json::json!({"success":true,"data":{"episode":episode},"meta":{"request_id":request_id()}}),None).await, Ok(None)=>write_json(stream,404,&serde_json::json!({"success":false,"error":{"code":"CATALOG_EPISODE_NOT_FOUND","message":"The episode was not found.","retryable":false}}),None).await, Err(_)=>write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await }
}
async fn catalog_episode_sources_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path
        .strip_prefix("/api/v1/catalog/episodes/")
        .unwrap_or("")
        .trim_end_matches("/sources");
    catalog_id_route(stream, c.list_sources(id).await).await
}
async fn catalog_source_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path.strip_prefix("/api/v1/catalog/sources/").unwrap_or("");
    match c.get_source(id).await { Ok(Some(source))=>write_json(stream,200,&serde_json::json!({"success":true,"data":{"source":source},"meta":{"request_id":request_id()}}),None).await, Ok(None)=>write_json(stream,404,&serde_json::json!({"success":false,"error":{"code":"CATALOG_SOURCE_NOT_FOUND","retryable":false}}),None).await, Err(_)=>write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await }
}
async fn catalog_source_tracks_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let Some(c) = state.catalog else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path
        .strip_prefix("/api/v1/catalog/sources/")
        .unwrap_or("")
        .trim_end_matches("/tracks");
    catalog_id_route(stream, c.list_tracks(id).await).await
}

fn catalog_live_source_path(
    source: &serde_json::Value,
    action: &str,
) -> Result<Option<String>, (u16, String)> {
    if source
        .get("provider_key")
        .and_then(serde_json::Value::as_str)
        != Some("fourkhdhub")
    {
        return Ok(None);
    }
    let codec = source
        .get("video_codec")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match source_codec_eligibility(Some(codec)) {
        SourceCodecEligibility::Verified => {}
        SourceCodecEligibility::BlockedH265 => {
            return Err(err(
                "SOURCE_CODEC_BLOCKED",
                "H.265/HEVC sources are blocked by playback policy.",
                Some(ProviderKind::FourKHdHub),
                415,
            ));
        }
        SourceCodecEligibility::Unverified => {
            return Err(err(
                "CODEC_UNVERIFIED",
                "The provider source has no recognized non-HEVC codec.",
                Some(ProviderKind::FourKHdHub),
                415,
            ));
        }
    }
    let Some(id) = source
        .get("provider_source_id")
        .and_then(serde_json::Value::as_str)
    else {
        return Err(err(
            "INVALID_SOURCE_MAPPING",
            "The stored provider source mapping is missing.",
            Some(ProviderKind::FourKHdHub),
            400,
        ));
    };
    let title_owned = source
        .get("title_id")
        .and_then(serde_json::Value::as_str)
        .is_some();
    match parse_source(id) {
        Ok(reference)
            if reference.p == ProviderKind::FourKHdHub
                && if title_owned {
                    reference.content_type.as_deref() == Some("movie")
                        && reference.s == 0
                        && reference.e == 0
                } else {
                    reference.content_type.is_none()
                } =>
        {
            Ok(Some(format!("/api/v1/sources/{id}/{action}")))
        }
        _ => Err(err(
            "INVALID_SOURCE_MAPPING",
            "The stored provider source mapping is invalid.",
            Some(ProviderKind::FourKHdHub),
            400,
        )),
    }
}

fn catalog_source_ineligible_error(
    source: &serde_json::Value,
) -> (u16, &'static str, &'static str, bool) {
    let source_status = source
        .get("source_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if source_status == "blocked" {
        return (
            404,
            "SOURCE_BLOCKED",
            "The source is explicitly blocked.",
            false,
        );
    }
    if source_status == "invalid" {
        return (
            404,
            "SOURCE_INVALID",
            "The source mapping is invalid.",
            false,
        );
    }
    if source
        .get("source_not_expired")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        return (
            410,
            "SOURCE_EXPIRED",
            "The provider source has expired.",
            false,
        );
    }
    let codec = source
        .get("video_codec")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let verification_status = source
        .get("codec_verification_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if verification_status == "blocked" || is_blocked_h265_codec(Some(codec)) {
        return (
            415,
            "SOURCE_CODEC_BLOCKED",
            "H.265/HEVC sources are blocked by playback policy.",
            false,
        );
    }
    let last_error = source
        .get("last_error_code")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match last_error {
        "PROVIDER_RESOLUTION_TIMEOUT" => {
            return (
                504,
                "PROVIDER_RESOLUTION_TIMEOUT",
                "The provider source could not be resolved before timeout.",
                true,
            );
        }
        "PROVIDER_RESOLUTION_FAILED" => {
            return (
                502,
                "PROVIDER_RESOLUTION_FAILED",
                "The provider could not resolve this source.",
                true,
            );
        }
        "MEDIA_PROBE_TIMEOUT" => {
            return (
                504,
                "MEDIA_PROBE_TIMEOUT",
                "Media codec inspection timed out.",
                true,
            );
        }
        "FFPROBE_UNAVAILABLE" => {
            return (
                503,
                "FFPROBE_UNAVAILABLE",
                "The FFprobe media tool is unavailable.",
                true,
            );
        }
        "UNSUPPORTED_VIDEO_CODEC" => {
            return (
                415,
                "CODEC_UNVERIFIED",
                "The inspected video codec is not permitted by playback policy.",
                false,
            );
        }
        "VIDEO_STREAM_MISSING" => {
            return (
                415,
                "CODEC_UNVERIFIED",
                "The media contains no video stream.",
                false,
            );
        }
        "SOURCE_MAPPING_CHANGED" | "SOURCE_MAPPING_INVALID" => {
            return (
                409,
                "CODEC_UNVERIFIED",
                "The provider source mapping could not be verified.",
                false,
            );
        }
        value if value.starts_with("MEDIA_PROBE") => {
            return (
                502,
                "MEDIA_PROBE_FAILED",
                "The media could not be inspected.",
                true,
            );
        }
        _ => {}
    }
    if !matches!(source_status, "active" | "playable") {
        return (
            404,
            "SOURCE_UNAVAILABLE",
            "The source is not active.",
            false,
        );
    }
    (
        409,
        "CODEC_UNVERIFIED",
        "The media codec has not been successfully verified.",
        true,
    )
}

async fn write_catalog_source_ineligible(
    stream: &mut TcpStream,
    source: &serde_json::Value,
) -> std::io::Result<()> {
    let (status, code, message, retryable) = catalog_source_ineligible_error(source);
    write_json(
        stream,
        status,
        &serde_json::json!({"success":false,"error":{"code":code,"message":message,"retryable":retryable}}),
        None,
    )
    .await
}

async fn catalog_source_remux_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    let Some(catalog) = state.catalog.clone() else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}), None).await;
    };
    let id = path
        .strip_prefix("/api/v1/catalog/sources/")
        .unwrap_or("")
        .trim_end_matches("/remux");
    match catalog.get_source(id).await {
        Ok(Some(source)) if source.get("playback_eligible").and_then(serde_json::Value::as_bool).unwrap_or(false) => {
            match catalog_live_source_path(&source, "remux") {
                Ok(Some(live_path)) => source_route(stream, &live_path, query, state, request_headers, head_only).await,
                Ok(None) => write_json(stream, 501, &serde_json::json!({"success":false,"error":{"code":"REMUX_UNAVAILABLE","message":"This source has no live provider remux mapping.","retryable":false}}), None).await,
                Err((status, body)) => write_raw_json(stream, status, &body, None).await,
            }
        }
        Ok(Some(source)) => write_catalog_source_ineligible(stream, &source).await,
        Ok(None) => write_json(stream, 404, &serde_json::json!({"success":false,"error":{"code":"CATALOG_SOURCE_NOT_FOUND","retryable":false}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}), None).await,
    }
}

async fn catalog_source_play_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    let Some(c) = state.catalog.clone() else {
        return write_json(stream,503,&serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),None).await;
    };
    let id = path
        .strip_prefix("/api/v1/catalog/sources/")
        .unwrap_or("")
        .trim_end_matches("/play");
    match c.get_source(id).await {
        Ok(Some(source)) => {
            let playback_eligible = source
                .get("playback_eligible")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if !playback_eligible {
                return write_catalog_source_ineligible(stream, &source).await;
            }
            match catalog_live_source_path(&source, "playback") {
                Ok(Some(live_path)) => {
                    return source_route(stream, &live_path, query, state, request_headers, head_only).await;
                }
                Ok(None) => {}
                Err((status, body)) => return write_raw_json(stream, status, &body, None).await,
            }
            let codec = source
                .get("video_codec")
                .and_then(|x| x.as_str())
                .unwrap_or("");
            let verified = source
                .get("codec_verification_status")
                .and_then(|x| x.as_str())
                == Some("verified");
            match (verified, source_codec_eligibility(Some(codec))) {
                (true, SourceCodecEligibility::Verified) => write_json(
                    stream,
                    200,
                    &serde_json::json!({"success":true,"data":{"source":source,"resolution":"stored_source"},"meta":{"request_id":request_id()}}),
                    None,
                )
                .await,
                (_, SourceCodecEligibility::BlockedH265) => write_json(
                    stream,
                    415,
                    &serde_json::json!({"success":false,"error":{"code":"SOURCE_CODEC_BLOCKED","message":"H.265/HEVC sources are blocked by playback policy.","retryable":false}}),
                    None,
                )
                .await,
                _ => write_json(
                    stream,
                    415,
                    &serde_json::json!({"success":false,"error":{"code":"CODEC_UNVERIFIED","message":"The source video codec is missing or unrecognized and cannot be played.","retryable":false}}),
                    None,
                )
                .await,
            }
        }
        Ok(None) => write_json(
            stream,
            404,
            &serde_json::json!({"success":false,"error":{"code":"CATALOG_SOURCE_NOT_FOUND","retryable":false}}),
            None,
        )
        .await,
        Err(_) => write_json(
            stream,
            503,
            &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","retryable":true}}),
            None,
        )
        .await,
    }
}

async fn admin_catalog_upsert_route(
    stream: &mut TcpStream,
    state: ApiState,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    let input: crate::catalog::TitleUpsert = match serde_json::from_slice(body) { Ok(input) => input, Err(_) => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":"The JSON title payload is invalid.","retryable":false}}), None).await };
    match catalog.upsert(input).await {
        Ok((title, inserted)) => write_json(stream, if inserted { 201 } else { 200 }, &serde_json::json!({"success":true,"data":{"title":title,"inserted":inserted},"meta":{"request_id":request_id()}}), None).await,
        Err(crate::catalog::CatalogError::Validation(message)) => write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":message,"retryable":false}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
    }
}

async fn admin_catalog_import_route(
    stream: &mut TcpStream,
    state: ApiState,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    #[derive(Deserialize)]
    struct ImportRequest {
        #[serde(default)]
        titles: Vec<crate::catalog::ImportTitle>,
    }
    let request: ImportRequest = match serde_json::from_slice(body) { Ok(value) => value, Err(_) => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":"Expected {titles:[...] }.","retryable":false}}), None).await };
    if request.titles.len() > 500 {
        return write_json(stream, 413, &serde_json::json!({"success":false,"error":{"code":"BATCH_TOO_LARGE","message":"Import batches are limited to 500 titles.","retryable":false}}), None).await;
    }
    let mut job = match catalog.create_job("import").await { Ok(job) => job, Err(_) => return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not create import job.","retryable":true}}), None).await };
    for title in request.titles {
        match catalog.import_title(title).await {
            Ok(summary) => {
                job.inserted_count += summary.inserted;
                job.updated_count += summary.updated;
                job.skipped_count += summary.skipped;
                job.titles_imported += 1;
                job.seasons_imported += summary.seasons;
                job.episodes_imported += summary.episodes;
                job.sources_imported += summary.sources;
                job.tracks_imported += summary.tracks;
            }
            Err(error) => {
                log::error!("catalog import title failed: {error:?}");
                job.failed_count += 1
            }
        }
    }
    job.status = if job.failed_count == 0 {
        "completed".into()
    } else {
        "completed_with_errors".into()
    };
    let _ = catalog.finish_job(&job).await;
    write_json(
        stream,
        if job.failed_count == 0 { 201 } else { 422 },
        &serde_json::json!({"success":job.failed_count == 0,"data":{"job":job},"meta":{"request_id":request_id()}}),
        None,
    )
    .await
}

#[derive(Debug, Deserialize)]
struct ImportByNameRequest {
    #[serde(alias = "name")]
    anime_name: String,
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    provider_title_id: Option<String>,
}

fn normalize_import_match(value: &str) -> String {
    value
        .chars()
        .flat_map(char::to_lowercase)
        .filter(|character| character.is_alphanumeric())
        .collect()
}

fn import_resolution_height(quality: Option<&str>) -> Option<i32> {
    let quality = quality?.to_ascii_lowercase();
    [2160, 1440, 1080, 720, 480, 360]
        .into_iter()
        .find(|height| quality.contains(&height.to_string()))
}

fn import_container(filename: &str) -> Option<String> {
    let extension = filename.rsplit_once('.')?.1.trim();
    (!extension.is_empty()
        && extension.len() <= 10
        && extension.chars().all(|c| c.is_ascii_alphanumeric()))
    .then(|| extension.to_ascii_lowercase())
}

async fn import_episode_sources(
    state: &ApiState,
    provider: ProviderKind,
    provider_title_id: &str,
    season: usize,
    episode: usize,
) -> Result<Vec<crate::catalog::ProviderSourceInput>, &'static str> {
    let episode_ref = OpaqueRef {
        p: provider,
        v: provider_title_id.to_string(),
        s: season,
        e: episode,
    };
    let releases =
        match tokio::time::timeout(SOURCE_TIMEOUT, cached_releases(state, &episode_ref)).await {
            Ok(Ok(releases)) => releases,
            Ok(Err(_)) => return Err("SOURCE_LOOKUP_FAILED"),
            Err(_) => return Err("SOURCE_LOOKUP_TIMEOUT"),
        };
    let sources = releases
        .iter()
        .enumerate()
        .filter(|(_, release)| {
            release.provider == provider && release_has_verification_candidate(release)
        })
        .take(3)
        .map(|(index, release)| crate::catalog::ProviderSourceInput {
            provider_source_id: source_ref(release, &episode_ref, index),
            source_reference: None,
            quality: release.quality.clone(),
            codec: release.codec.clone(),
            audio_codec: None,
            container: import_container(&release.filename),
            resolution_width: None,
            resolution_height: import_resolution_height(release.quality.as_deref()),
            source_status: Some("active".into()),
            codec_verification_status: Some("unverified".into()),
            expires_at: None,
            audio_tracks: Vec::new(),
            subtitle_tracks: Vec::new(),
        })
        .collect::<Vec<_>>();
    if sources.is_empty() {
        Err("NO_ELIGIBLE_SOURCE")
    } else {
        Ok(sources)
    }
}

const ANIME_IMPORT_WORKERS: usize = 1;
const ANIME_IMPORT_IDLE: Duration = Duration::from_secs(3);
const ANIME_IMPORT_SOURCE_ATTEMPTS: usize = 3;

fn start_anime_import_workers(state: ApiState) {
    if state.catalog.is_none() {
        return;
    }
    for worker_id in 0..ANIME_IMPORT_WORKERS {
        let state = state.clone();
        tokio::spawn(async move { anime_import_worker(state, worker_id).await });
    }
}

async fn anime_import_worker(state: ApiState, worker_id: usize) {
    loop {
        let Some(catalog) = state.catalog.as_ref() else {
            return;
        };
        if let Err(error) = catalog.recover_staged_anime_import_jobs().await {
            log::error!("anime import worker could not recover staged jobs: {error:?}");
        }
        match catalog.claim_anime_import_job().await {
            Ok(Some(job)) => {
                if let Err(error) = process_anime_import_job(&state, &job).await {
                    log::error!(
                        "anime import worker {worker_id} could not process job {}: {error:?}",
                        job.id
                    );
                }
            }
            Ok(None) => tokio::time::sleep(ANIME_IMPORT_IDLE).await,
            Err(error) => {
                log::error!("anime import worker {worker_id} could not claim a job: {error:?}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn process_anime_import_job(
    state: &ApiState,
    job: &crate::catalog::ClaimedAnimeImportJob,
) -> Result<(), crate::catalog::CatalogError> {
    let catalog = state
        .catalog
        .as_ref()
        .expect("worker starts only with catalog");
    if !job.metadata_imported {
        let base: crate::catalog::ImportTitle =
            match serde_json::from_value(job.base_import.clone()) {
                Ok(base) => base,
                Err(error) => {
                    log::error!(
                        "anime import {} has invalid metadata snapshot: {error}",
                        job.id
                    );
                    catalog
                        .fail_anime_import_metadata(&job.id, "INVALID_METADATA_SNAPSHOT")
                        .await?;
                    return Ok(());
                }
            };
        if let Err(error) = catalog.import_title(base).await {
            log::error!(
                "anime import {} metadata transaction failed: {error:?}",
                job.id
            );
            catalog
                .fail_anime_import_metadata(&job.id, "METADATA_IMPORT_FAILED")
                .await?;
            return Ok(());
        }
        catalog.mark_anime_import_metadata(&job.id).await?;
    }
    let Some(provider) = ProviderKind::parse(&job.provider_key) else {
        catalog
            .fail_anime_import_metadata(&job.id, "UNSUPPORTED_PROVIDER_SNAPSHOT")
            .await?;
        return Ok(());
    };
    let rows = catalog
        .pending_anime_import_episodes(&job.id, i64::from(job.batch_size))
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    for (
        ordinal,
        _season_index,
        _episode_index,
        season_number,
        provider_season,
        episode_number,
        record,
    ) in rows
    {
        let episode: crate::catalog::EpisodeInput = serde_json::from_value(
            record.get("episode").cloned().unwrap_or_default(),
        )
        .map_err(|error| {
            crate::catalog::CatalogError::Validation(format!("invalid episode checkpoint: {error}"))
        })?;
        let mut source_result = Err("SOURCE_LOOKUP_FAILED");
        for attempt in 0..ANIME_IMPORT_SOURCE_ATTEMPTS {
            source_result = import_episode_sources(
                state,
                provider,
                &job.provider_title_id,
                usize::try_from(provider_season).unwrap_or_default(),
                usize::try_from(episode_number).unwrap_or_default(),
            )
            .await;
            if source_result.is_ok() {
                break;
            }
            if attempt + 1 < ANIME_IMPORT_SOURCE_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(250 * (1 << attempt))).await;
            }
        }
        let (sources, warning) = match source_result {
            Ok(sources) => (sources, None),
            Err(code) => (Vec::new(), Some(code)),
        };
        let mut input: crate::catalog::ImportTitle =
            serde_json::from_value(job.base_import.clone()).map_err(|error| {
                crate::catalog::CatalogError::Validation(format!(
                    "invalid durable metadata snapshot: {error}"
                ))
            })?;
        let Some(season) = input
            .seasons
            .iter_mut()
            .find(|season| season.season_number == season_number)
        else {
            catalog
                .fail_anime_import_episode(&job.id, ordinal, "SEASON_SNAPSHOT_MISSING")
                .await?;
            continue;
        };
        season.episodes = vec![episode];
        let provider_episode_id = record
            .get("provider_episode_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string();
        let Some(provider_title) = input.providers.first_mut() else {
            catalog
                .fail_anime_import_episode(&job.id, ordinal, "PROVIDER_SNAPSHOT_MISSING")
                .await?;
            continue;
        };
        provider_title.episodes = vec![crate::catalog::ProviderEpisodeInput {
            provider_key: None,
            display_name: None,
            season_number: Some(season_number),
            provider_episode_id,
            provider_episode_number: Some(episode_number),
            sources,
        }];
        if let Err(error) = catalog.import_title(input).await {
            log::error!(
                "anime import {} episode checkpoint {ordinal} transaction failed: {error:?}",
                job.id
            );
            catalog
                .fail_anime_import_episode(&job.id, ordinal, "EPISODE_IMPORT_FAILED")
                .await?;
            continue;
        }
        catalog
            .complete_anime_import_episode(&job.id, ordinal, warning)
            .await?;
    }
    catalog.release_anime_import_job_lease(&job.id).await?;
    Ok(())
}

fn start_codec_verification_workers(state: ApiState) {
    if state.catalog.is_none()
        || state.service.fourk_client.is_none()
        || !crate::config::load().fourkhdhub_enabled
    {
        return;
    }
    for worker_id in 0..CODEC_VERIFICATION_WORKERS {
        let state = state.clone();
        tokio::spawn(async move { codec_verification_worker(state, worker_id).await });
    }
}

async fn codec_verification_worker(state: ApiState, worker_id: usize) {
    loop {
        let Some(catalog) = state.catalog.as_ref() else {
            return;
        };
        match catalog
            .claim_codec_verification_job(CODEC_VERIFICATION_LEASE_SECONDS)
            .await
        {
            Ok(Some(job)) => match verify_catalog_source(&state, &job).await {
                Ok(crate::codec_verification::CodecProbeResult::Allowed(codec)) => {
                    if let Err(error) = catalog
                        .complete_codec_verification(&job, &codec, false)
                        .await
                    {
                        log::error!(
                            "codec verifier could not persist a successful result: {error}"
                        );
                    }
                }
                Ok(crate::codec_verification::CodecProbeResult::BlockedHevc(codec)) => {
                    if let Err(error) = catalog
                        .complete_codec_verification(&job, &codec, true)
                        .await
                    {
                        log::error!(
                            "codec verifier could not persist a blocked HEVC result: {error}"
                        );
                    }
                }
                Ok(crate::codec_verification::CodecProbeResult::Unsupported(_)) => {
                    if let Err(error) = catalog
                        .fail_codec_verification(&job, "UNSUPPORTED_VIDEO_CODEC", false)
                        .await
                    {
                        log::error!(
                            "codec verifier could not persist an unsupported-codec result: {error}"
                        );
                    }
                }
                Ok(crate::codec_verification::CodecProbeResult::Unverified(_)) => {
                    if let Err(error) = catalog
                        .fail_codec_verification(&job, "CODEC_UNVERIFIED", true)
                        .await
                    {
                        log::error!(
                            "codec verifier could not persist an unverified result: {error}"
                        );
                    }
                }
                Ok(crate::codec_verification::CodecProbeResult::NoVideoStream) => {
                    if let Err(error) = catalog
                        .fail_codec_verification(&job, "VIDEO_STREAM_MISSING", false)
                        .await
                    {
                        log::error!(
                            "codec verifier could not persist a missing-video result: {error}"
                        );
                    }
                }
                Err((code, retryable)) => {
                    log::warn!("codec verification worker {worker_id} failed: code={code}");
                    if let Err(error) = catalog.fail_codec_verification(&job, code, retryable).await
                    {
                        log::error!("codec verifier could not persist failure state: {error}");
                    }
                }
            },
            Ok(None) => tokio::time::sleep(CODEC_VERIFICATION_IDLE).await,
            Err(error) => {
                log::error!("codec verification worker {worker_id} could not claim a job: {error}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn verify_catalog_source(
    state: &ApiState,
    job: &crate::catalog::CodecVerificationJob,
) -> Result<crate::codec_verification::CodecProbeResult, (&'static str, bool)> {
    let source_ref = decode::<OpaqueSource>(&job.provider_source_id)
        .map_err(|_| ("SOURCE_MAPPING_INVALID", false))?;
    if source_ref.f.is_none() {
        return Err(("SOURCE_MAPPING_INVALID", false));
    }
    if source_ref.p != ProviderKind::FourKHdHub {
        return Err(("SOURCE_MAPPING_INVALID", false));
    }
    let provider_ref = match job.content_type.as_str() {
        "movie"
            if source_ref.content_type.as_deref() == Some("movie")
                && source_ref.s == 0
                && source_ref.e == 0
                && source_ref.v == job.provider_content_id =>
        {
            OpaqueRef {
                p: source_ref.p,
                v: job.provider_content_id.clone(),
                s: 0,
                e: 0,
            }
        }
        "movie" => return Err(("SOURCE_MAPPING_INVALID", false)),
        _ => {
            let episode_ref = decode::<OpaqueRef>(&job.provider_content_id)
                .map_err(|_| ("SOURCE_MAPPING_INVALID", false))?;
            if source_ref.content_type.is_some()
                || episode_ref.p != ProviderKind::FourKHdHub
                || source_ref.p != episode_ref.p
                || source_ref.v != episode_ref.v
                || source_ref.s != episode_ref.s
                || source_ref.e != episode_ref.e
            {
                return Err(("SOURCE_MAPPING_INVALID", false));
            }
            episode_ref
        }
    };
    let releases =
        match tokio::time::timeout(SOURCE_TIMEOUT, cached_releases(state, &provider_ref)).await {
            Ok(Ok(releases)) => releases,
            Ok(Err(_)) => return Err(("PROVIDER_RESOLUTION_FAILED", true)),
            Err(_) => return Err(("PROVIDER_RESOLUTION_TIMEOUT", true)),
        };
    let Some(release) = exact_fourk_release(&releases, &source_ref) else {
        return Err(("SOURCE_MAPPING_CHANGED", false));
    };
    let Some(client) = state.service.fourk_client.as_ref() else {
        return Err(("PROVIDER_RESOLUTION_FAILED", true));
    };
    let playback = match tokio::time::timeout(
        PLAYBACK_RESOLUTION_TIMEOUT,
        client.resolve_release(
            release,
            crate::providers::models::ResolutionIntent::Playback,
        ),
    )
    .await
    {
        Ok(Ok(playback)) => playback,
        Ok(Err(_)) => return Err(("PROVIDER_RESOLUTION_FAILED", true)),
        Err(_) => return Err(("PROVIDER_RESOLUTION_TIMEOUT", true)),
    };
    let _media_job = state
        .remux_cache
        .try_acquire_job()
        .map_err(|_| ("REMUX_BUSY", true))?;
    crate::codec_verification::verify_playback_source(&playback)
        .await
        .map_err(|error| (error.code(), error.retryable()))
}

fn exact_fourk_release<'a>(releases: &'a [Release], source: &OpaqueSource) -> Option<&'a Release> {
    let fingerprint = source.f.as_deref()?;
    if source.p != ProviderKind::FourKHdHub {
        return None;
    }
    releases.iter().find(|release| {
        release.provider == ProviderKind::FourKHdHub && release_fingerprint(release) == fingerprint
    })
}

async fn admin_catalog_import_by_name_route(
    stream: &mut TcpStream,
    state: ApiState,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog.clone() else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    let request: ImportByNameRequest = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(_) => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":"Expected {\"anime_name\":\"title\"}.","retryable":false}}), None).await,
    };
    let anime_name = request.anime_name.trim();
    if anime_name.chars().count() < 2 || anime_name.chars().count() > MAX_QUERY {
        return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"VALIDATION_FAILED","message":"anime_name must contain 2 to 200 characters.","retryable":false}}), None).await;
    }
    let provider = match request.provider.as_deref() {
        None => ProviderKind::FourKHdHub,
        Some(value) => match ProviderKind::parse(value) {
            Some(provider) => provider,
            None => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"UNSUPPORTED_PROVIDER","message":"Import-by-name currently supports the FourKHDHub anime provider.","retryable":false}}), None).await,
        },
    };
    if provider != ProviderKind::FourKHdHub {
        return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"UNSUPPORTED_PROVIDER","message":"Import-by-name currently supports the FourKHDHub anime provider.","retryable":false}}), None).await;
    }
    let items = match tokio::time::timeout(
        OVERALL_TIMEOUT,
        state.service.search_typed(provider, anime_name, 0),
    )
    .await
    {
        Ok(Ok(items)) => items,
        Ok(Err(error)) => {
            let (status, body) = provider_error(error, provider);
            return write_raw_json(stream, status, &body, None).await;
        }
        Err(_) => {
            let (status, body) = err(
                "PROVIDER_TIMEOUT",
                "The anime provider search timed out.",
                Some(provider),
                504,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let target = normalize_import_match(anime_name);
    let mut matches = items
        .iter()
        .filter(|item| {
            item.media_type == MediaType::Series && normalize_import_match(&item.title) == target
        })
        .filter(|item| {
            request
                .provider_title_id
                .as_deref()
                .is_none_or(|id| id == item.id.value)
        })
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| left.id.value.cmp(&right.id.value));
    matches.dedup_by(|left, right| left.id == right.id);
    if matches.len() != 1 {
        let candidates = items.iter().take(8).map(|item| serde_json::json!({"title":item.title,"year":item.year,"media_type":item.media_type,"provider_title_id":item.id.value})).collect::<Vec<_>>();
        let (status, code, message) = if matches.is_empty() {
            (
                404,
                "ANIME_NOT_FOUND",
                "No exact anime title match was found; retry with the exact provider title.",
            )
        } else {
            (
                409,
                "AMBIGUOUS_TITLE_MATCH",
                "More than one exact title matched; retry with provider_title_id from candidates.",
            )
        };
        return write_json(stream, status, &serde_json::json!({"success":false,"error":{"code":code,"message":message,"retryable":false},"data":{"candidates":candidates}}), None).await;
    }
    let item = matches[0];
    let details = match tokio::time::timeout(
        OVERALL_TIMEOUT,
        state.service.details_typed(provider, &item.id.value),
    )
    .await
    {
        Ok(Ok(details)) => details,
        Ok(Err(error)) => {
            let (status, body) = provider_error(error, provider);
            return write_raw_json(stream, status, &body, None).await;
        }
        Err(_) => {
            let (status, body) = err(
                "PROVIDER_TIMEOUT",
                "The anime detail lookup timed out.",
                Some(provider),
                504,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    if !details.is_series() || details.seasons.is_empty() {
        return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"ANIME_DETAILS_INCOMPLETE","message":"The provider did not return a series with episode metadata.","retryable":false}}), None).await;
    }
    let Some(episode_total) = details.seasons.iter().try_fold(0usize, |total, season| {
        total.checked_add(season.episodes.len())
    }) else {
        return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"ANIME_DETAILS_INCOMPLETE","message":"The provider episode metadata is invalid.","retryable":false}}), None).await;
    };
    if episode_total == 0 {
        return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"ANIME_DETAILS_INCOMPLETE","message":"The provider did not return any episode metadata.","retryable":false}}), None).await;
    }
    if episode_total > 200 {
        let batch_size = std::env::var("ANIME_IMPORT_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| (1..=50).contains(value))
            .unwrap_or(10);
        let provider_title_id = details.id.value.clone();
        let base = crate::catalog::ImportTitle {
            mal_id: None,
            canonical_title: details.title.clone(),
            content_type: "anime".into(),
            synopsis: details.description.clone(),
            release_year: details
                .year
                .as_deref()
                .and_then(|year| year.get(..4))
                .and_then(|year| year.parse().ok()),
            poster_url: details.poster_url.clone(),
            catalog_status: Some("active".into()),
            alternative_titles: Vec::new(),
            genres: details.genres.clone(),
            providers: vec![crate::catalog::ProviderTitleInput {
                provider_key: provider.cache_key().into(),
                display_name: Some(provider.label().into()),
                provider_title_id: provider_title_id.clone(),
                provider_title: Some(details.title.clone()),
                provider_url: None,
                metadata: serde_json::json!({"import_source":"anime_import_by_name","season_count":details.seasons.len()}),
                episodes: Vec::new(),
                sources: Vec::new(),
            }],
            seasons: details
                .seasons
                .iter()
                .map(|season| crate::catalog::SeasonInput {
                    season_number: season.number as i32,
                    canonical_label: Some(format!("Season {}", season.number)),
                    episode_count_reported: i32::try_from(season.episodes.len()).ok(),
                    episodes: Vec::new(),
                })
                .collect(),
        };
        let (job_id, created) = match catalog.create_anime_import_job(anime_name, provider.cache_key(), &provider_title_id, &base, batch_size, episode_total as i64).await {
            Ok(result) => result,
            Err(crate::catalog::CatalogError::Validation(_)) => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"ANIME_DETAILS_INCOMPLETE","message":"The provider returned invalid or empty episode records.","retryable":false}}), None).await,
            Err(error) => {
                log::error!("could not enqueue large anime import: {error:?}");
                return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not create a durable anime import job.","retryable":true}}), None).await;
            }
        };
        if !created {
            let job = catalog.get_job(&job_id).await.ok().flatten();
            return write_json(stream, 202, &serde_json::json!({"success":true,"data":{"job_id":job_id,"status":job.as_ref().map(|job| job.status.as_str()).unwrap_or("queued"),"anime_title":details.title,"total_episodes":job.as_ref().and_then(|job| job.total_episodes).unwrap_or(episode_total as i64),"batch_size":job.as_ref().and_then(|job| job.batch_size).unwrap_or(batch_size)},"meta":{"request_id":request_id()}}), None).await;
        }
        let mut ordinal = 0_i64;
        let mut batch = Vec::with_capacity(batch_size as usize);
        for (season_index, season) in details.seasons.iter().enumerate() {
            for (episode_index, episode) in season.episodes.iter().enumerate() {
                let record = (|| -> Result<_, crate::catalog::CatalogError> {
                    let season_number = i32::try_from(season.number).map_err(|_| {
                        crate::catalog::CatalogError::Validation(
                            "invalid provider season number".into(),
                        )
                    })?;
                    let season_index = i32::try_from(season_index).map_err(|_| {
                        crate::catalog::CatalogError::Validation("invalid season index".into())
                    })?;
                    let episode_index = i32::try_from(episode_index).map_err(|_| {
                        crate::catalog::CatalogError::Validation("invalid episode index".into())
                    })?;
                    let provider_season = i32::try_from(episode.season).map_err(|_| {
                        crate::catalog::CatalogError::Validation(
                            "invalid provider season reference".into(),
                        )
                    })?;
                    let episode_number = i32::try_from(episode.number).map_err(|_| {
                        crate::catalog::CatalogError::Validation(
                            "invalid provider episode number".into(),
                        )
                    })?;
                    let episode_metadata = serde_json::to_value(crate::catalog::EpisodeInput {
                        episode_number,
                        episode_type: None,
                        episode_coordinate: None,
                        absolute_episode_number: None,
                        special_identifier: None,
                        episode_title: episode.title.clone(),
                        overview: episode.overview.clone(),
                        release_date: None,
                        thumbnail_url: None,
                        provider_episode_ids: Vec::new(),
                    })
                    .map_err(|error| crate::catalog::CatalogError::Validation(error.to_string()))?;
                    Ok(crate::catalog::AnimeImportEpisodeRef {
                        ordinal,
                        season_index,
                        episode_index,
                        season_number,
                        provider_season,
                        episode_number,
                        provider_episode_id: encode(&OpaqueRef {
                            p: provider,
                            v: provider_title_id.clone(),
                            s: episode.season,
                            e: episode.number,
                        }),
                        episode_metadata,
                    })
                })();
                ordinal += 1;
                match record {
                    Ok(record) => batch.push(record),
                    Err(_) => return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"ANIME_DETAILS_INCOMPLETE","message":"The provider returned an invalid episode record.","retryable":false}}), None).await,
                }
                if batch.len() == batch_size as usize {
                    if let Err(error) = catalog
                        .append_anime_import_episode_batch(&job_id, &batch)
                        .await
                    {
                        log::error!("could not stage anime import episode batch: {error:?}",);
                        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not stage the complete anime import job.","retryable":true}}), None).await;
                    }
                    batch.clear();
                }
            }
        }
        if !batch.is_empty() {
            if let Err(error) = catalog
                .append_anime_import_episode_batch(&job_id, &batch)
                .await
            {
                log::error!("could not stage final anime import episode batch: {error:?}",);
                return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not stage the complete anime import job.","retryable":true}}), None).await;
            }
        }
        let queued_episodes = match catalog.activate_anime_import_job(&job_id).await {
            Ok(count) => count,
            Err(error) => {
                log::error!("could not activate anime import after staging: {error:?}");
                return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The anime import job could not be activated.","retryable":true}}), None).await;
            }
        };
        return write_json(stream, 202, &serde_json::json!({"success":true,"data":{"job_id":job_id,"status":"queued","anime_title":details.title,"total_episodes":queued_episodes,"batch_size":batch_size},"meta":{"request_id":request_id()}}), None).await;
    }
    let mut source_warnings = Vec::new();
    let mut provider_episodes = Vec::with_capacity(episode_total);
    let mut seasons = Vec::with_capacity(details.seasons.len());
    for season in &details.seasons {
        let season_number = season.number as i32;
        let mut canonical_episodes = Vec::with_capacity(season.episodes.len());
        for episode in &season.episodes {
            let mut sources = match import_episode_sources(
                &state,
                provider,
                &details.id.value,
                episode.season,
                episode.number,
            )
            .await
            {
                Ok(sources) => sources,
                Err(code) => {
                    source_warnings.push(serde_json::json!({"season":episode.season,"episode":episode.number,"code":code}));
                    Vec::new()
                }
            };
            let provider_episode_id = encode(&OpaqueRef {
                p: provider,
                v: details.id.value.clone(),
                s: episode.season,
                e: episode.number,
            });
            provider_episodes.push(crate::catalog::ProviderEpisodeInput {
                provider_key: None,
                display_name: None,
                season_number: Some(season_number),
                provider_episode_id,
                provider_episode_number: Some(episode.number as i32),
                sources: std::mem::take(&mut sources),
            });
            canonical_episodes.push(crate::catalog::EpisodeInput {
                episode_number: episode.number as i32,
                episode_type: None,
                episode_coordinate: None,
                absolute_episode_number: None,
                special_identifier: None,
                episode_title: episode.title.clone(),
                overview: episode.overview.clone(),
                release_date: None,
                thumbnail_url: None,
                provider_episode_ids: Vec::new(),
            });
        }
        seasons.push(crate::catalog::SeasonInput {
            season_number,
            canonical_label: Some(format!("Season {season_number}")),
            episode_count_reported: Some(season.episodes.len() as i32),
            episodes: canonical_episodes,
        });
    }
    let provider_title_id = details.id.value.clone();
    let metadata = serde_json::to_value(&details).unwrap_or_else(|_| serde_json::json!({}));
    let import = crate::catalog::ImportTitle {
        mal_id: None,
        canonical_title: details.title.clone(),
        content_type: "anime".into(),
        synopsis: details.description.clone(),
        release_year: details
            .year
            .as_deref()
            .and_then(|year| year.get(..4))
            .and_then(|year| year.parse().ok()),
        poster_url: details.poster_url.clone(),
        catalog_status: Some("active".into()),
        alternative_titles: Vec::new(),
        genres: details.genres.clone(),
        providers: vec![crate::catalog::ProviderTitleInput {
            provider_key: provider.cache_key().into(),
            display_name: Some(provider.label().into()),
            provider_title_id: provider_title_id.clone(),
            provider_title: Some(details.title.clone()),
            provider_url: None,
            metadata,
            episodes: provider_episodes,
            sources: Vec::new(),
        }],
        seasons,
    };
    let mut job = match catalog.create_job("import_by_name").await {
        Ok(job) => job,
        Err(_) => return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not create import job.","retryable":true}}), None).await,
    };
    let summary = match catalog.import_title(import).await {
        Ok(summary) => summary,
        Err(error) => {
            log::error!("import-by-name transaction failed: {error:?}");
            job.failed_count = 1;
            job.status = "failed".into();
            let _ = catalog.finish_job(&job).await;
            return write_json(stream, 422, &serde_json::json!({"success":false,"error":{"code":"CATALOG_IMPORT_FAILED","message":"The provider anime metadata could not be committed to the catalog.","retryable":false}}), None).await;
        }
    };
    job.inserted_count = summary.inserted;
    job.updated_count = summary.updated;
    job.titles_imported = 1;
    job.seasons_imported = summary.seasons;
    job.episodes_imported = summary.episodes;
    job.sources_imported = summary.sources;
    job.tracks_imported = summary.tracks;
    job.status = "completed".into();
    let _ = catalog.finish_job(&job).await;
    let title = catalog
        .find_title_by_provider(provider.cache_key(), &provider_title_id)
        .await
        .ok()
        .flatten();
    write_json(
        stream,
        if summary.inserted > 0 { 201 } else { 200 },
        &serde_json::json!({"success":true,"data":{"title":title,"job":job,"source_candidates":{"stored":summary.sources,"warnings":source_warnings,"signed_stream_urls_persisted":false,"playback_route":"/api/v1/catalog/sources/{source_id}/play","remux_route":"/api/v1/catalog/sources/{source_id}/remux"}},"meta":{"request_id":request_id()}}),
        None,
    ).await
}

async fn admin_catalog_update_route(
    stream: &mut TcpStream,
    state: ApiState,
    headers: &HashMap<String, String>,
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    match catalog.create_job("update_all").await { Ok(mut job) => { job.status = "completed".into(); let _ = catalog.finish_job(&job).await; write_json(stream, 202, &serde_json::json!({"success":true,"data":{"job":job},"meta":{"request_id":request_id()}}), None).await }, Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"Could not create update job.","retryable":true}}), None).await }
}

async fn admin_catalog_inspect_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    headers: &HashMap<String, String>,
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    match catalog.inspect(query_value(query, "title").as_deref(), query_number(query, "limit", 50), query_number(query, "offset", 0)).await { Ok(items) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"items":items},"meta":{"request_id":request_id()}}), None).await, Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await }
}

async fn admin_catalog_job_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
    headers: &HashMap<String, String>,
) -> std::io::Result<()> {
    if !admin_authorized(headers) {
        return write_json(stream, 401, &serde_json::json!({"success":false,"error":{"code":"UNAUTHORIZED","message":"Administrator authentication is required.","retryable":false}}), None).await;
    }
    let Some(catalog) = state.catalog else {
        return write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is not configured.","retryable":true}}), None).await;
    };
    let Some(id) = path.strip_prefix("/api/v1/admin/catalog/jobs/") else {
        return write_json(stream, 400, &serde_json::json!({"success":false,"error":{"code":"INVALID_REQUEST","message":"The job identifier is invalid.","retryable":false}}), None).await;
    };
    match catalog.get_job(id).await {
        Ok(Some(job)) => write_json(stream, 200, &serde_json::json!({"success":true,"data":{"job":job},"meta":{"request_id":request_id()}}), None).await,
        Ok(None) => write_json(stream, 404, &serde_json::json!({"success":false,"error":{"code":"JOB_NOT_FOUND","message":"The catalog job was not found.","retryable":false}}), None).await,
        Err(_) => write_json(stream, 503, &serde_json::json!({"success":false,"error":{"code":"DATABASE_UNAVAILABLE","message":"The catalog database is unavailable.","retryable":true}}), None).await,
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
        "anizone": { "configured": config.anizone_enabled, "initialized": true, "upstream": "not_checked" },
        "fourkhdhub": { "configured": config.fourkhdhub_enabled, "initialized": state.service.fourk_client.is_some(), "upstream": "not_checked" },
        "dramachi": { "configured": config.dramachi_enabled, "initialized": true, "upstream": "not_checked" },
        "bdix_circleftp": { "configured": config.bdix_circleftp_enabled, "initialized": true, "upstream": "not_checked" },
        "bdix_dhakaflix": { "configured": config.bdix_dhakaflix_enabled, "initialized": true, "upstream": "not_checked" }
    });
    let built_in_streaming = config.streaming_enabled
        && ((config.fourkhdhub_enabled && state.service.fourk_client.is_some())
            || config.dramachi_enabled
            || config.anizone_enabled
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
    tokio::time::timeout(BROWSE_TIMEOUT, homepage_items_uncapped(state, page))
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
            let metadata = tokio::time::timeout(
                CLASSIFICATION_TIMEOUT,
                service.moviebox_classification_metadata(&id),
            )
            .await
            .ok()
            .flatten();
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

async fn home_compat_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    if !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
    if path == "/api/v1/home" {
        return home_route(stream, query, state).await;
    }
    let section = match path {
        "/api/v1/home/featured" => "featured",
        "/api/v1/trending" => "trending",
        "/api/v1/trending/anime" => "anime",
        "/api/v1/trending/movies" => "movies",
        "/api/v1/trending/k-dramas" => "kdramas",
        "/api/v1/popular" => "popular",
        "/api/v1/popular/anime" => "anime",
        "/api/v1/popular/movies" => "movies",
        "/api/v1/popular/k-dramas" => "kdramas",
        _ => "featured",
    };
    home_section_route(stream, query, state, section).await
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
    let mut codec_failure: Option<(&'static str, &'static str, ProviderKind)> = None;
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

            // Preflight this provider's streams, but keep searching when its candidates
            // are blocked or unusable so another provider can still satisfy the request.
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
                    if let Some((code, message)) = codec_ineligibility_error(&releases) {
                        codec_failure = Some((code, message, anime_ref.p));
                    }
                    if releases_have_usable_source(&releases) {
                        // Provider language labels are hints, not a hard gate. Official
                        // MovieBox-Tui can expose dual/multi-audio streams without a Hindi tag.
                        let anime_id = encode(&anime_ref);
                        let episode_id = provider_ref(&anime_details.id, season, episode);
                        return playback_route(
                            stream,
                            PlaybackIdentity::Episode {
                                anime_id,
                                episode_id,
                                anime_ref,
                            },
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
        if let Some((code, message, provider)) = codec_failure {
            let (status, body) = err(code, message, Some(provider), 415);
            return write_raw_json(stream, status, &body, None).await;
        }
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
    let eligible = cached_search(&state, &qv).await;
    let provider_status_details = if eligible.provider_status_details.is_empty() {
        search_provider_status_details(
            &eligible.items,
            &eligible.provider_status,
            &configured_search_providers(),
        )
    } else {
        eligible.provider_status_details.clone()
    };
    let items = crate::unified::group_catalog_items_with_aliases(
        eligible.items,
        &eligible.verified_aliases,
    );
    let failed = eligible.failed;
    let provider_status = eligible.provider_status;
    let canonical_ids = eligible.canonical_ids;
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
            .filter(|item| item.primary.title.ends_with(&label))
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
        .map(|item| {
            let canonical_id = canonical_ids
                .get(&provider_mapping_key(&item.primary.id))
                .cloned();
            catalog(item, canonical_id)
        })
        .collect::<Vec<_>>();
    let body = SearchResponse {
        results,
        next_cursor,
        partial: failed,
        provider_status,
        provider_status_details,
        language_intent: language_intent.map(|language| language.code().to_string()),
        verified_language_matches,
        message,
    };
    write_json(stream, 200, &body, None).await
}

async fn cached_search(state: &ApiState, query: &str) -> SearchResultSet {
    let providers = configured_search_providers();
    let key = search_cache_identity(query, &providers);
    {
        let cache = state.search_cache.lock().await;
        if let Some(entry) = cache.get(&key) {
            if entry.stored_at.elapsed() < SEARCH_CACHE_TTL && !entry.result.failed {
                crate::observability::record_cache("search_memory", "hit");
                return filter_current_search_providers(entry.result.clone());
            }
        }
    }
    crate::observability::record_cache("search_memory", "miss");

    if let Some(result) = read_redis_search_cache(state.redis_search.as_deref(), &key).await {
        crate::observability::record_cache("search_redis", "redis_hit");
        insert_search_cache(state, &key, result.clone()).await;
        return filter_current_search_providers(result);
    }
    crate::observability::record_cache("search_redis", "redis_miss");

    let (items, failed, provider_status, execution_metadata) =
        search_all_for_providers_with_metadata(&state.service, query, providers).await;
    let mappings = items
        .iter()
        .map(|item| {
            (
                item.id.provider.cache_key().to_string(),
                item.id.value.clone(),
            )
        })
        .collect::<Vec<_>>();
    let verified_aliases = match state.catalog.as_ref() {
        Some(store) => store
            .verified_provider_aliases(&mappings)
            .await
            .unwrap_or_default(),
        None => HashMap::new(),
    };
    let mut result =
        persist_search_results(state, items, failed, provider_status, verified_aliases).await;
    apply_execution_metadata(&mut result.provider_status_details, &execution_metadata);
    // Failed/partial searches are never persisted as cache entries. In particular,
    // a transient provider outage must not replace a complete result.
    if is_complete_search_cacheable(&result) {
        insert_search_cache(state, &key, result.clone()).await;
        write_redis_search_cache(
            state.redis_search.as_deref(),
            &key,
            &result,
            SEARCH_CACHE_TTL.as_secs(),
        )
        .await;
    }
    filter_current_search_providers(result)
}

async fn insert_search_cache(state: &ApiState, key: &str, result: SearchResultSet) {
    let mut cache = state.search_cache.lock().await;
    if cache.len() >= SEARCH_CACHE_MAX_ENTRIES {
        let before = cache.len();
        cache.retain(|_, entry| entry.stored_at.elapsed() < SEARCH_CACHE_TTL);
        for _ in 0..before.saturating_sub(cache.len()) {
            crate::observability::record_cache("search_memory", "expiration");
        }
        if cache.len() >= SEARCH_CACHE_MAX_ENTRIES {
            cache.clear();
            crate::observability::record_cache("search_memory", "eviction");
        }
    }
    cache.insert(
        key.to_string(),
        SearchCacheEntry {
            stored_at: Instant::now(),
            result,
        },
    );
    crate::observability::record_cache("search_memory", "insert");
}

fn filter_current_search_providers(mut result: SearchResultSet) -> SearchResultSet {
    let enabled = configured_search_providers();
    result
        .items
        .retain(|item| enabled.contains(&item.id.provider));
    result
        .provider_status
        .retain(|provider, _| enabled.iter().any(|kind| kind.cache_key() == provider));
    result
}

fn search_cache_identity(query: &str, providers: &[ProviderKind]) -> String {
    let normalized_query = query.trim().to_ascii_lowercase();
    let mut provider_names = providers
        .iter()
        .map(|provider| provider.cache_key())
        .collect::<Vec<_>>();
    provider_names.sort_unstable();
    provider_names.dedup();
    let mut hasher = Sha256::new();
    hasher.update(b"moviebox-provider-search-v4\0");
    hasher.update((normalized_query.len() as u64).to_be_bytes());
    hasher.update(normalized_query.as_bytes());
    for provider in provider_names {
        hasher.update((provider.len() as u64).to_be_bytes());
        hasher.update(provider.as_bytes());
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("moviebox:search:v4:{hex}")
}

fn decode_complete_search_cache_record(payload: &[u8]) -> Option<SearchResultSet> {
    let result = serde_json::from_slice::<SearchResultSet>(payload).ok()?;
    is_complete_search_cacheable(&result).then_some(result)
}

async fn read_redis_search_cache(
    cache: Option<&dyn SearchRedisCache>,
    key: &str,
) -> Option<SearchResultSet> {
    let cache = cache?;
    match cache.get(key).await {
        Ok(Some(payload)) => match decode_complete_search_cache_record(&payload) {
            Some(result) => Some(result),
            None => {
                let _ = cache.delete(key).await;
                None
            }
        },
        Ok(None) | Err(()) => None,
    }
}

async fn write_redis_search_cache(
    cache: Option<&dyn SearchRedisCache>,
    key: &str,
    result: &SearchResultSet,
    ttl_seconds: u64,
) {
    let (Some(cache), true) = (cache, is_complete_search_cacheable(result)) else {
        return;
    };
    if let Ok(payload) = serde_json::to_vec(result) {
        let _ = cache.set_ex(key, ttl_seconds, payload).await;
    }
}

fn is_complete_search_cacheable(result: &SearchResultSet) -> bool {
    !result.failed
}

fn search_provider_status_details(
    items: &[CatalogItem],
    legacy_status: &HashMap<String, String>,
    enabled: &[ProviderKind],
) -> HashMap<String, SearchProviderStatus> {
    let mut counts = HashMap::new();
    for provider in PROVIDERS {
        counts.insert(
            provider.cache_key().to_string(),
            items
                .iter()
                .filter(|item| item.id.provider == provider)
                .count(),
        );
    }
    search_provider_status_details_from_counts(&counts, legacy_status, enabled)
}

fn search_provider_status_details_from_counts(
    counts: &HashMap<String, usize>,
    legacy_status: &HashMap<String, String>,
    enabled: &[ProviderKind],
) -> HashMap<String, SearchProviderStatus> {
    PROVIDERS
        .iter()
        .map(|provider| {
            let name = provider.cache_key().to_string();
            let result_count = counts.get(&name).copied().unwrap_or_default();
            let is_enabled = enabled.contains(provider);
            let legacy = legacy_status.get(&name);
            let (status, error_code, message) = if !is_enabled {
                (SearchProviderState::Disabled, None, None)
            } else if let Some(value) = legacy {
                let normalized = value.to_ascii_lowercase();
                if normalized == "ok" {
                    if result_count == 0 {
                        (SearchProviderState::Empty, None, None)
                    } else {
                        (SearchProviderState::Success, None, None)
                    }
                } else if normalized == "timeout" {
                    (
                        SearchProviderState::Timeout,
                        Some("PROVIDER_TIMEOUT".into()),
                        Some(value.clone()),
                    )
                } else if normalized.starts_with("partial:") {
                    (
                        SearchProviderState::Partial,
                        Some("PARTIAL_PROVIDER_ERROR".into()),
                        Some(value.clone()),
                    )
                } else {
                    (
                        SearchProviderState::Error,
                        Some("UPSTREAM_UNAVAILABLE".into()),
                        Some(value.clone()),
                    )
                }
            } else {
                (
                    SearchProviderState::Error,
                    Some("PROVIDER_STATUS_MISSING".into()),
                    Some("Provider status was not reported.".into()),
                )
            };
            (
                name.clone(),
                SearchProviderStatus {
                    provider: name,
                    status,
                    result_count,
                    pages_attempted: None,
                    pages_successful: None,
                    duration_ms: None,
                    timestamp: None,
                    error_code,
                    message,
                },
            )
        })
        .collect()
}

fn execution_metadata(
    supports_pagination: bool,
    progress: &SearchPageProgress,
    duration: Duration,
) -> SearchProviderExecutionMetadata {
    SearchProviderExecutionMetadata {
        pages_attempted: supports_pagination
            .then(|| progress.pages_attempted.load(Ordering::Relaxed)),
        pages_successful: supports_pagination
            .then(|| progress.pages_successful.load(Ordering::Relaxed)),
        duration_ms: duration.as_millis().min(u64::MAX as u128) as u64,
        timestamp_unix_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64,
    }
}

fn apply_execution_metadata(
    statuses: &mut HashMap<String, SearchProviderStatus>,
    metadata: &HashMap<String, SearchProviderExecutionMetadata>,
) {
    for (provider, metrics) in metadata {
        let Some(status) = statuses.get_mut(provider) else {
            continue;
        };
        status.pages_attempted = metrics.pages_attempted;
        status.pages_successful = metrics.pages_successful;
        status.duration_ms = Some(metrics.duration_ms);
        status.timestamp = Some(metrics.timestamp_unix_ms);
    }
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
    write_headers(stream, 200, "text/event-stream", None).await?;
    let providers = configured_search_providers();
    send_event(
        stream,
        "search_started",
        &serde_json::json!({
            "query": qv,
            "providers": providers.iter().map(|provider| provider.cache_key()).collect::<Vec<_>>(),
            "replay": false
        })
        .to_string(),
    )
    .await?;

    // The channel is deliberately bounded. Provider tasks can never enqueue an
    // unbounded number of events while a client is slow or disconnected.
    const EVENT_BUFFER: usize = 16;
    let (sender, mut receiver) = mpsc::channel(EVENT_BUFFER);
    let slots = Arc::new(Semaphore::new(SEARCH_PROVIDER_CONCURRENCY));
    let mut tasks = JoinSet::new();
    let intent = crate::language::parse_query(&qv);
    let normalized_query = intent.title;
    for provider in providers.iter().copied() {
        let sender = sender.clone();
        let service = Arc::clone(&state.service);
        let query = normalized_query.clone();
        let slots = Arc::clone(&slots);
        tasks.spawn(async move {
            let supports_pagination = service.capabilities(provider).supports_pagination;
            let progress = Arc::new(SearchPageProgress::default());
            let Some(_permit) = slots.acquire_owned().await.ok() else {
                let _ = sender
                    .send(SearchStreamEvent::Failed {
                        provider,
                        status: "concurrency_limit".into(),
                        metadata: execution_metadata(
                            supports_pagination,
                            &progress,
                            Duration::ZERO,
                        ),
                    })
                    .await;
                return;
            };
            let started = Instant::now();
            let result = tokio::time::timeout(
                OVERALL_TIMEOUT,
                search_provider_pages_with_progress(
                    supports_pagination,
                    Arc::clone(&progress),
                    |page| {
                        let service = Arc::clone(&service);
                        let query = query.clone();
                        async move { service.search_typed(provider, &query, page).await }
                    },
                ),
            )
            .await;
            let metadata = execution_metadata(supports_pagination, &progress, started.elapsed());
            let event = match result {
                Ok(Ok(result)) => SearchStreamEvent::Succeeded {
                    provider,
                    items: result.items,
                    partial_error: result.partial_error,
                    metadata,
                },
                Ok(Err(error)) => SearchStreamEvent::Failed {
                    provider,
                    status: safe_provider_search_error(provider, &error),
                    metadata,
                },
                Err(_) => SearchStreamEvent::TimedOut { provider, metadata },
            };
            let _ = sender.send(event).await;
        });
    }
    drop(sender);

    let mut provider_status = HashMap::new();
    let mut provider_result_counts = HashMap::new();
    let mut execution_metadata_by_provider = HashMap::new();
    let mut failed = false;
    let mut seen = std::collections::HashSet::new();
    let mut sequence = 0usize;
    while let Some(event) = receiver.recv().await {
        match event {
            SearchStreamEvent::Succeeded {
                provider,
                mut items,
                partial_error,
                metadata,
            } => {
                execution_metadata_by_provider.insert(provider.cache_key().to_string(), metadata);
                let partial_status = partial_error
                    .as_ref()
                    .map(|error| safe_provider_search_error(provider, error));
                provider_status.insert(
                    provider.cache_key().to_string(),
                    partial_status
                        .as_ref()
                        .map(|status| format!("partial: {status}"))
                        .unwrap_or_else(|| "ok".to_string()),
                );
                if partial_error.is_some() {
                    failed = true;
                }
                if let Some(language) = intent.language {
                    for item in &mut items {
                        crate::language::decorate_catalog_item(item, Some(language));
                    }
                }
                let raw_provider_result_count = items.len();
                let mut provider_count = 0usize;
                for item in items {
                    if !seen.insert(item.id.clone()) {
                        continue;
                    }
                    sequence += 1;
                    provider_count += 1;
                    let provider_id = item.id.clone();
                    let card = crate::unified::UnifiedCatalogItem {
                        primary: item,
                        provider_mappings: Vec::new(),
                        provider_items: Vec::new(),
                    };
                    let payload = serde_json::json!({
                        "result": catalog(card, None),
                        "provider": provider_id.provider,
                        "provider_id": provider_id.value,
                        "sequence": sequence
                    });
                    if let Err(error) =
                        send_event(stream, "search_result", &payload.to_string()).await
                    {
                        abort_search_tasks(&mut tasks).await;
                        return Err(error);
                    }
                }
                provider_result_counts
                    .insert(provider.cache_key().to_string(), raw_provider_result_count);
                send_event(
                    stream,
                    "search_provider_complete",
                    &serde_json::json!({
                        "provider": provider,
                        "count": provider_count,
                        "partial": partial_error.is_some()
                    })
                    .to_string(),
                )
                .await?;
                if let Some(error) = partial_error {
                    send_event(
                        stream,
                        "search_provider_error",
                        &serde_json::json!({
                            "provider": provider,
                            "code": "PARTIAL_PROVIDER_ERROR",
                            "status": safe_provider_search_error(provider, &error)
                        })
                        .to_string(),
                    )
                    .await?;
                }
            }
            SearchStreamEvent::Failed {
                provider,
                status,
                metadata,
            } => {
                execution_metadata_by_provider.insert(provider.cache_key().to_string(), metadata);
                failed = true;
                provider_status.insert(provider.cache_key().to_string(), status.clone());
                send_event(
                    stream,
                    "search_provider_error",
                    &serde_json::json!({
                        "provider": provider,
                        "code": "UPSTREAM_UNAVAILABLE",
                        "status": status
                    })
                    .to_string(),
                )
                .await?;
            }
            SearchStreamEvent::TimedOut { provider, metadata } => {
                execution_metadata_by_provider.insert(provider.cache_key().to_string(), metadata);
                failed = true;
                provider_status.insert(provider.cache_key().to_string(), "timeout".to_string());
                send_event(
                    stream,
                    "search_provider_error",
                    &serde_json::json!({
                        "provider": provider,
                        "code": "PROVIDER_TIMEOUT",
                        "status": "timeout"
                    })
                    .to_string(),
                )
                .await?;
            }
        }
    }
    abort_search_tasks(&mut tasks).await;

    // The live stream intentionally does not write to the search cache. This
    // prevents partial disconnects from being persisted as complete searches.
    let mut provider_status_details = search_provider_status_details_from_counts(
        &provider_result_counts,
        &provider_status,
        &providers,
    );
    apply_execution_metadata(
        &mut provider_status_details,
        &execution_metadata_by_provider,
    );
    send_event(
        stream,
        "search_complete",
        &serde_json::json!({
            "partial": failed,
            "providers": provider_status,
            "provider_status_details": provider_status_details,
            "replay": false
        })
        .to_string(),
    )
    .await
}

enum SearchStreamEvent {
    Succeeded {
        provider: ProviderKind,
        items: Vec<CatalogItem>,
        partial_error: Option<ProviderError>,
        metadata: SearchProviderExecutionMetadata,
    },
    Failed {
        provider: ProviderKind,
        status: String,
        metadata: SearchProviderExecutionMetadata,
    },
    TimedOut {
        provider: ProviderKind,
        metadata: SearchProviderExecutionMetadata,
    },
}

async fn abort_search_tasks(tasks: &mut JoinSet<()>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

#[derive(Debug, Clone, PartialEq)]
struct ProviderSearchPages {
    items: Vec<CatalogItem>,
    partial_error: Option<ProviderError>,
}

fn partial_provider_status(provider: ProviderKind, error: &ProviderError) -> String {
    format!("partial: {}", safe_provider_search_error(provider, error))
}

fn safe_provider_search_error(provider: ProviderKind, error: &ProviderError) -> String {
    let label = provider.label();
    match error {
        ProviderError::Network(message)
            if message.to_ascii_lowercase().contains("timeout")
                || message.to_ascii_lowercase().contains("timed out") =>
        {
            format!("{label} timed out.")
        }
        ProviderError::Network(_) => format!("Cannot reach {label}."),
        ProviderError::RateLimited(Some(seconds)) => {
            format!("Rate limited. Wait {seconds}s.")
        }
        ProviderError::RateLimited(None) => "Rate limited. Try later.".to_string(),
        ProviderError::NotFound => "No results found.".to_string(),
        ProviderError::Parsing(_) => format!("{label} parse error."),
        ProviderError::Unavailable(message) => {
            let status = message
                .strip_prefix("HTTP status ")
                .and_then(|value| value.split_whitespace().next())
                .filter(|value| {
                    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                });
            match status {
                Some(code) => format!("{label} error ({code})."),
                None => format!("{label} unavailable."),
            }
        }
    }
}

async fn search_all(
    service: &Arc<MovieBoxService>,
    query: &str,
) -> (Vec<CatalogItem>, bool, HashMap<String, String>) {
    let (items, failed, statuses, _) =
        search_all_for_providers_with_metadata(service, query, configured_search_providers()).await;
    (items, failed, statuses)
}

async fn search_all_for_providers_with_metadata(
    service: &Arc<MovieBoxService>,
    query: &str,
    providers: Vec<ProviderKind>,
) -> (
    Vec<CatalogItem>,
    bool,
    HashMap<String, String>,
    HashMap<String, SearchProviderExecutionMetadata>,
) {
    let intent = crate::language::parse_query(query);
    let requested_language = intent.language;
    let normalized_query = intent.title;
    let mut tasks = JoinSet::new();
    let provider_slots = Arc::new(Semaphore::new(SEARCH_PROVIDER_CONCURRENCY));
    for p in providers {
        let svc = service.clone();
        let q = normalized_query.clone();
        let slots = Arc::clone(&provider_slots);
        tasks.spawn(async move {
            let supports_pagination = svc.capabilities(p).supports_pagination;
            let progress = Arc::new(SearchPageProgress::default());
            let permit = slots.acquire_owned().await;
            if permit.is_err() {
                let metadata = execution_metadata(supports_pagination, &progress, Duration::ZERO);
                return (
                    p,
                    Ok(Err(ProviderError::Unavailable(
                        "provider search concurrency limit reached".to_string(),
                    ))),
                    metadata,
                );
            }
            let started = Instant::now();
            let result = tokio::time::timeout(OVERALL_TIMEOUT, async {
                search_provider_pages_with_progress(
                    supports_pagination,
                    Arc::clone(&progress),
                    |page| {
                        let svc = Arc::clone(&svc);
                        let q = q.clone();
                        async move { svc.search_typed(p, &q, page).await }
                    },
                )
                .await
            })
            .await;
            let metadata = execution_metadata(supports_pagination, &progress, started.elapsed());
            (p, result, metadata)
        });
    }
    let mut items = Vec::new();
    let mut failed = false;
    let mut provider_status = HashMap::new();
    let mut execution_metadata_by_provider = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((p, r, metadata)) => {
                let duration_ms = metadata.duration_ms;
                execution_metadata_by_provider.insert(p.cache_key().to_string(), metadata);
                match r {
                    Ok(Ok(result)) => {
                        let ProviderSearchPages {
                            items: result_items,
                            partial_error,
                        } = result;
                        if let Some(error) = partial_error {
                            crate::observability::record_provider(
                                p.cache_key(),
                                "failure",
                                duration_ms,
                            );
                            provider_status.insert(
                                p.cache_key().to_string(),
                                partial_provider_status(p, &error),
                            );
                            failed = true;
                        } else {
                            crate::observability::record_provider(
                                p.cache_key(),
                                "success",
                                duration_ms,
                            );
                            provider_status.insert(p.cache_key().to_string(), "ok".to_string());
                        }
                        items.extend(result_items);
                    }
                    Ok(Err(e)) => {
                        crate::observability::record_provider(
                            p.cache_key(),
                            if matches!(&e, ProviderError::Unavailable(_)) {
                                "unavailable"
                            } else {
                                "failure"
                            },
                            duration_ms,
                        );
                        provider_status
                            .insert(p.cache_key().to_string(), safe_provider_search_error(p, &e));
                        failed = true;
                    }
                    Err(_) => {
                        crate::observability::record_provider(
                            p.cache_key(),
                            "timeout",
                            duration_ms,
                        );
                        provider_status.insert(p.cache_key().to_string(), "timeout".to_string());
                        failed = true;
                    }
                }
            }
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
    (
        items,
        failed,
        provider_status,
        execution_metadata_by_provider,
    )
}

async fn search_provider_pages<F, Fut>(
    supports_pagination: bool,
    fetch: F,
) -> Result<ProviderSearchPages, ProviderError>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<Vec<CatalogItem>, ProviderError>>,
{
    search_provider_pages_with_progress(
        supports_pagination,
        Arc::new(SearchPageProgress::default()),
        fetch,
    )
    .await
}

async fn search_provider_pages_with_progress<F, Fut>(
    supports_pagination: bool,
    progress: Arc<SearchPageProgress>,
    mut fetch: F,
) -> Result<ProviderSearchPages, ProviderError>
where
    F: FnMut(usize) -> Fut,
    Fut: Future<Output = Result<Vec<CatalogItem>, ProviderError>>,
{
    let pages = if supports_pagination {
        1..=SEARCH_MAX_PAGES
    } else {
        1..=1
    };
    let mut all = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for page in pages {
        progress.pages_attempted.fetch_add(1, Ordering::Relaxed);
        let page_items = match fetch(page).await {
            Ok(items) => {
                progress.pages_successful.fetch_add(1, Ordering::Relaxed);
                items
            }
            Err(error) if all.is_empty() => return Err(error),
            Err(error) => {
                return Ok(ProviderSearchPages {
                    items: all,
                    partial_error: Some(error),
                });
            }
        };
        if page_items.is_empty() {
            break;
        }
        for item in page_items {
            if seen.insert(item.id.clone()) {
                all.push(item);
            }
        }
    }
    Ok(ProviderSearchPages {
        items: all,
        partial_error: None,
    })
}

fn provider_mapping_key(id: &ProviderMediaId) -> String {
    format!("{}:{}", id.provider.cache_key(), id.value)
}

async fn persist_search_group(
    state: &ApiState,
    group: &crate::unified::UnifiedCatalogItem,
) -> Option<String> {
    let store = state.catalog.as_ref()?;
    let primary = &group.primary;
    let mut records = vec![primary];
    records.extend(group.provider_items.iter());
    let provider_titles = records
        .iter()
        .map(|item| crate::catalog::UnifiedAnimeProviderTitleInput {
            provider_key: item.id.provider.cache_key().to_string(),
            display_name: Some(item.id.provider.label().to_string()),
            provider_title_id: item.id.value.clone(),
            provider_title: Some(item.title.clone()),
            provider_url: None,
            // Search success confirms discovery only, not detail/playback resolution.
            last_successful_resolution: false,
            metadata: serde_json::json!({
                "year": item.year,
                "media_type": item.media_type,
                "season_count": item.season_count,
            }),
        })
        .collect();
    let year = records.iter().find_map(|item| {
        item.year
            .as_deref()
            .and_then(|value| value.parse::<i32>().ok())
            .or_else(|| crate::unified::normalize_anime_title(&item.title).year_marker)
    });
    let poster_url = records.iter().find_map(|item| {
        item.poster_url
            .as_deref()
            .filter(|value| crate::net::validate_url_syntax(value, true).is_ok())
            .map(str::to_string)
    });
    let input = crate::catalog::UnifiedAnimeInput {
        canonical_title: primary.title.clone(),
        content_type: match primary.media_type {
            MediaType::Movie => "movie",
            MediaType::Series => "anime",
        }
        .to_string(),
        synopsis: None,
        release_year: year,
        poster_url,
        catalog_status: None,
        provider_titles,
        seasons: Vec::new(),
    };
    match store.upsert_unified_anime(input).await {
        Ok(id) => Some(id),
        Err(error) => {
            log::warn!("could not persist unified search mappings: {error}");
            None
        }
    }
}

async fn persist_search_results(
    state: &ApiState,
    mut items: Vec<CatalogItem>,
    failed: bool,
    provider_status: HashMap<String, String>,
    verified_aliases: HashMap<String, Vec<String>>,
) -> SearchResultSet {
    items.sort_by(|a, b| {
        provider_priority(a.id.provider)
            .cmp(&provider_priority(b.id.provider))
            .then_with(|| a.title.cmp(&b.title))
    });
    let mut canonical_ids = HashMap::new();
    if state.catalog.is_some() {
        for group in
            crate::unified::group_catalog_items_with_aliases(items.clone(), &verified_aliases)
        {
            if let Some(id) = persist_search_group(state, &group).await {
                canonical_ids.insert(provider_mapping_key(&group.primary.id), id);
            }
        }
    }
    SearchResultSet {
        provider_status_details: search_provider_status_details(
            &items,
            &provider_status,
            &configured_search_providers(),
        ),
        items,
        failed,
        provider_status,
        canonical_ids,
        verified_aliases,
    }
}

async fn movie_route(
    stream: &mut TcpStream,
    path: &str,
    _query: &str,
    state: ApiState,
) -> std::io::Result<()> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    if parts.len() < 4 {
        let (s, b) = err("INVALID_ID", "A movie identifier is required.", None, 400);
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
    let details_result =
        match tokio::time::timeout(OVERALL_TIMEOUT, state.service.details_typed(r.p, &r.v)).await {
            Ok(Ok(details)) => details,
            Ok(Err(error)) => {
                let (s, b) = provider_error(error, r.p);
                return write_raw_json(stream, s, &b, None).await;
            }
            Err(_) => {
                let (s, b) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The movie details request timed out.",
                    Some(r.p),
                    504,
                );
                return write_raw_json(stream, s, &b, None).await;
            }
        };
    if details_result.media_type != MediaType::Movie {
        let (s, b) = err(
            "NOT_A_MOVIE",
            "The requested item is not a movie.",
            Some(r.p),
            400,
        );
        return write_raw_json(stream, s, &b, None).await;
    }
    if parts.len() == 4 {
        return write_json(stream, 200, &details(details_result), None).await;
    }
    let movie_ref = MovieIdentity {
        provider: r.p,
        provider_id: r.v.clone(),
    };
    let provider_lookup_ref = movie_ref.provider_lookup_ref();
    match parts.get(4).copied() {
        Some("sources") => {
            match tokio::time::timeout(
                OVERALL_TIMEOUT,
                cached_releases(&state, &provider_lookup_ref),
            )
            .await
            {
                Ok(Ok(releases)) => {
                    write_json(
                        stream,
                        200,
                        &SourcesResponse {
                            sources: api_sources_for_content(
                                &releases,
                                &provider_lookup_ref,
                                Some("movie"),
                            ),
                            partial: false,
                        },
                        None,
                    )
                    .await
                }
                Ok(Err(error)) => {
                    let (s, b) = provider_error(error, r.p);
                    write_raw_json(stream, s, &b, None).await
                }
                Err(_) => {
                    let (s, b) = err(
                        "REQUEST_DEADLINE_EXCEEDED",
                        "The movie source request timed out.",
                        Some(r.p),
                        504,
                    );
                    write_raw_json(stream, s, &b, None).await
                }
            }
        }
        Some("stream") | Some("playback") => {
            playback_route(
                stream,
                PlaybackIdentity::Movie {
                    movie_id: parts[3].to_string(),
                    movie_ref,
                },
                details_result,
                state,
                "auto",
            )
            .await
        }
        _ => {
            let (s, b) = err("INTERNAL_ERROR", "Unsupported movie route.", None, 404);
            write_raw_json(stream, s, &b, None).await
        }
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
    if let Ok(unified) = decode::<UnifiedOpaqueRef>(parts[3]) {
        if unified.unified {
            let primary = OpaqueRef {
                p: unified.p,
                v: unified.v.clone(),
                s: unified.s,
                e: unified.e,
            };
            return unified_anime_route(stream, path, query, state, unified, primary).await;
        }
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
        return anime_episode_full_route(stream, parts[3], parts[5], r, d, Vec::new(), state).await;
    }
    if parts.len() == 7 && parts[4] == "episodes" && parts[6] == "stream" {
        let audio = query
            .split('&')
            .find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "audio").then(|| percent_decode(value).trim().to_ascii_lowercase())
            })
            .unwrap_or_else(|| "hindi".to_string());
        if crate::unified::normalize_language_code(&audio).is_none() {
            let (status, body) = err(
                "UNSUPPORTED_AUDIO_LANGUAGE",
                "The requested audio language is not recognized.",
                Some(r.p),
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
        return playback_route(
            stream,
            PlaybackIdentity::Episode {
                anime_id: parts[3].to_string(),
                episode_id: parts[5].to_string(),
                anime_ref: r,
            },
            d,
            state,
            &audio,
        )
        .await;
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

#[derive(Clone)]
struct UnifiedEpisodeCandidate {
    anime_id: ProviderMediaId,
    episode: crate::providers::models::Episode,
}

fn normalized_episode_title(title: Option<&str>) -> Option<String> {
    title
        .map(crate::unified::normalize_anime_title)
        .map(|title| title.key)
        .filter(|title| !title.is_empty())
}

fn is_generic_episode_title(title: Option<&str>) -> bool {
    let Some(title) = title else {
        return true;
    };
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let stripped = lower
        .trim_start_matches("episode")
        .trim_start_matches("ep.")
        .trim_start_matches("ep")
        .trim_start_matches('e')
        .trim_start_matches("chapter")
        .trim_start_matches("ch.")
        .trim_start_matches("ch")
        .trim();
    stripped.is_empty() || stripped.chars().all(|c| c.is_ascii_digit())
}

fn episode_special_marker(title: Option<&str>) -> bool {
    title
        .map(crate::unified::normalize_anime_title)
        .is_some_and(|title| {
            title.key.split_whitespace().any(|token| {
                matches!(
                    token,
                    "special" | "ova" | "ona" | "movie" | "film" | "recap"
                )
            })
        })
}

fn episode_candidates_can_merge(
    left: &UnifiedEpisodeCandidate,
    right: &UnifiedEpisodeCandidate,
) -> bool {
    left.episode.season == right.episode.season
        && left.episode.number == right.episode.number
        && left.anime_id.provider != right.anime_id.provider
        && episode_special_marker(left.episode.title.as_deref())
            == episode_special_marker(right.episode.title.as_deref())
        && crate::unified::episode_titles_compatible(
            left.episode.title.as_deref(),
            right.episode.title.as_deref(),
        )
}

const DYNAMIC_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(8);
const DYNAMIC_DISCOVERY_MAX_RESULTS: usize = 5;

fn same_anime_identity(left: &MediaDetails, right: &MediaDetails) -> bool {
    if left.media_type != MediaType::Series || right.media_type != MediaType::Series {
        return false;
    }
    let left_title = crate::unified::normalize_anime_title(&left.title);
    let right_title = crate::unified::normalize_anime_title(&right.title);
    if left_title.key.is_empty()
        || left_title.key != right_title.key
        || left_title.franchise_discriminator != right_title.franchise_discriminator
    {
        return false;
    }
    match (
        left.year
            .as_deref()
            .and_then(|year| year.parse::<i32>().ok()),
        right
            .year
            .as_deref()
            .and_then(|year| year.parse::<i32>().ok()),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn matching_episode(
    source: &crate::providers::models::Episode,
    candidate: &MediaDetails,
) -> Option<crate::providers::models::Episode> {
    let episodes = candidate
        .seasons
        .iter()
        .flat_map(|season| season.episodes.iter());
    if let Some(exact) = episodes.clone().find(|episode| {
        episode.season == source.season
            && episode.number == source.number
            && episode_special_marker(episode.title.as_deref())
                == episode_special_marker(source.title.as_deref())
            && crate::unified::episode_titles_compatible(
                source.title.as_deref(),
                episode.title.as_deref(),
            )
    }) {
        return Some(exact.clone());
    }

    // Different season layouts are accepted only when a meaningful episode title
    // uniquely identifies the record. Number-only matching is deliberately unsafe.
    if is_generic_episode_title(source.title.as_deref()) {
        return None;
    }
    let source_title = normalized_episode_title(source.title.as_deref())?;
    if episode_special_marker(source.title.as_deref()) {
        return None;
    }
    let mut titled = episodes.filter(|episode| {
        !episode_special_marker(episode.title.as_deref())
            && !is_generic_episode_title(episode.title.as_deref())
            && normalized_episode_title(episode.title.as_deref()).as_deref()
                == Some(source_title.as_str())
    });
    let found = titled.next()?.clone();
    titled.next().is_none().then_some(found)
}

fn append_unique_episode_references(target: &mut Vec<OpaqueRef>, discovered: Vec<OpaqueRef>) {
    for reference in discovered {
        if !target.iter().any(|existing| {
            existing.p == reference.p
                && existing.v == reference.v
                && existing.s == reference.s
                && existing.e == reference.e
        }) {
            target.push(reference);
        }
    }
}

async fn discover_episode_mappings(
    state: &ApiState,
    existing_mappings: &[ProviderMediaId],
    known_details: &[(ProviderMediaId, MediaDetails)],
    selected_anime: &MediaDetails,
    selected_episode: &crate::providers::models::Episode,
    canonical_id: Option<&str>,
) -> Vec<OpaqueRef> {
    let targets = configured_search_providers()
        .into_iter()
        .filter(|provider| {
            *provider != selected_anime.id.provider
                && !known_details
                    .iter()
                    .any(|(mapping, _)| mapping.provider == *provider)
        });
    let mut tasks = JoinSet::new();
    for provider in targets {
        let service = Arc::clone(&state.service);
        let query = selected_anime.title.clone();
        let source_identity = selected_anime.clone();
        tasks.spawn(async move {
            let supports_pagination = service.capabilities(provider).supports_pagination;
            let search = tokio::time::timeout(
                DYNAMIC_DISCOVERY_TIMEOUT,
                search_provider_pages(supports_pagination, |page| {
                    let service = Arc::clone(&service);
                    let query = query.clone();
                    async move { service.search_typed(provider, &query, page).await }
                }),
            )
            .await;
            let Ok(Ok(result)) = search else {
                return Vec::new();
            };
            let items = result.items;
            let source_title = crate::unified::normalize_anime_title(&source_identity.title);
            let candidates = items
                .into_iter()
                .filter(|item| {
                    let candidate_title = crate::unified::normalize_anime_title(&item.title);
                    item.media_type == MediaType::Series
                        && candidate_title.key == source_title.key
                        && candidate_title.franchise_discriminator
                            == source_title.franchise_discriminator
                        && years_compatible(source_identity.year.as_deref(), item.year.as_deref())
                })
                .take(DYNAMIC_DISCOVERY_MAX_RESULTS)
                .collect::<Vec<_>>();
            let mut results = Vec::new();
            for item in candidates {
                let fetched = tokio::time::timeout(
                    DYNAMIC_DISCOVERY_TIMEOUT,
                    service.details_typed(provider, &item.id.value),
                )
                .await;
                if let Ok(Ok(details)) = fetched
                    && details.id.provider == provider
                    && details.id.value == item.id.value
                    && same_anime_identity(&source_identity, &details)
                {
                    results.push((item.id, details));
                }
            }
            results
        });
    }

    let mut discovered = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Ok(items) = result {
            discovered.extend(items);
        }
    }

    let mut by_provider: HashMap<ProviderKind, Vec<(ProviderMediaId, MediaDetails)>> =
        HashMap::new();
    for candidate in discovered {
        if known_details
            .iter()
            .any(|(known, _)| known.provider == candidate.0.provider)
            || matching_episode(selected_episode, &candidate.1).is_none()
        {
            continue;
        }
        by_provider
            .entry(candidate.0.provider)
            .or_default()
            .push(candidate);
    }
    // Ambiguous duplicate provider search results are not treated as a mapping.
    let verified = by_provider
        .into_values()
        .filter(|candidates| candidates.len() == 1)
        .flatten()
        .collect::<Vec<_>>();
    let references = verified
        .iter()
        .filter_map(|(_, details)| {
            let episode = matching_episode(selected_episode, details)?;
            Some(OpaqueRef {
                p: details.id.provider,
                v: details.id.value.clone(),
                s: episode.season,
                e: episode.number,
            })
        })
        .collect::<Vec<_>>();

    if !verified.is_empty() {
        let mut all_details = known_details.to_vec();
        let mut all_mappings = existing_mappings.to_vec();
        for (search_id, details) in verified {
            if !all_mappings
                .iter()
                .any(|existing| existing.provider == search_id.provider)
            {
                all_mappings.push(search_id);
            }
            if !all_details
                .iter()
                .any(|(existing, _)| existing.provider == details.id.provider)
            {
                all_details.push((details.id.clone(), details));
            }
        }
        all_details.sort_by_key(|(mapping, _)| provider_priority(mapping.provider));
        if !all_details.is_empty() {
            let canonical = canonical_id.unwrap_or("dynamic-anime").to_string();
            let response =
                unified_api_details(&all_details, &all_mappings, canonical, HashMap::new());
            let _ = persist_unified_details(state, &all_details, &all_mappings, &response).await;
        }
    }
    references
}

fn years_compatible(left: Option<&str>, right: Option<&str>) -> bool {
    match (
        left.and_then(|year| year.parse::<i32>().ok()),
        right.and_then(|year| year.parse::<i32>().ok()),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

fn canonical_episode_response_id(
    canonical_anime_id: &str,
    candidate: &UnifiedEpisodeCandidate,
) -> String {
    let identity =
        normalized_episode_title(candidate.episode.title.as_deref()).unwrap_or_else(|| {
            format!(
                "{}:{}",
                candidate.anime_id.provider.cache_key(),
                candidate.anime_id.value
            )
        });
    let mut hasher = Sha256::new();
    hasher.update(canonical_anime_id.as_bytes());
    hasher.update([0]);
    hasher.update(candidate.episode.season.to_be_bytes());
    hasher.update(candidate.episode.number.to_be_bytes());
    hasher.update([0]);
    hasher.update(identity.as_bytes());
    format!(
        "{}:episode:{}",
        canonical_anime_id,
        URL_SAFE_NO_PAD.encode(hasher.finalize())
    )
}

async fn persist_unified_details(
    state: &ApiState,
    details: &[(ProviderMediaId, MediaDetails)],
    mappings: &[ProviderMediaId],
    response: &UnifiedApiDetails,
) -> Option<String> {
    let store = state.catalog.as_ref()?;
    let primary = &details[0].1;
    let provider_titles = mappings
        .iter()
        .map(|mapping| {
            let provider_details = details
                .iter()
                .find(|(candidate, _)| candidate == mapping)
                .map(|(_, details)| details);
            crate::catalog::UnifiedAnimeProviderTitleInput {
                provider_key: mapping.provider.cache_key().to_string(),
                display_name: Some(mapping.provider.label().to_string()),
                provider_title_id: mapping.value.clone(),
                provider_title: provider_details.map(|details| details.title.clone()),
                provider_url: None,
                last_successful_resolution: provider_details.is_some(),
                metadata: provider_details.map_or_else(
                    || serde_json::json!({}),
                    |details| {
                        serde_json::json!({
                            "year": details.year,
                            "media_type": details.media_type,
                        })
                    },
                ),
            }
        })
        .collect();

    let mut seasons = Vec::new();
    for unified_season in &response.seasons {
        let provider_seasons = details
            .iter()
            .filter_map(|(mapping, details)| {
                details
                    .seasons
                    .iter()
                    .find(|season| season.number == unified_season.number)
                    .map(|season| crate::catalog::UnifiedAnimeProviderSeasonInput {
                        provider_key: mapping.provider.cache_key().to_string(),
                        provider_season_id: season.provider_season_id.clone(),
                        provider_season_number: Some(season.number as i32),
                        metadata: serde_json::json!({
                            "episode_count": season.episodes.len(),
                        }),
                    })
            })
            .collect();

        let episodes = unified_season
            .episodes
            .iter()
            .filter(|episode| episode.number > 0)
            .map(|episode| crate::catalog::UnifiedAnimeEpisodeInput {
                episode_number: episode.number as i32,
                episode_type: None,
                episode_coordinate: None,
                absolute_episode_number: None,
                special_identifier: None,
                episode_title: episode.title.clone(),
                overview: episode.overview.clone(),
                release_date: None,
                thumbnail_url: None,
                provider_episodes: episode
                    .provider_lookups
                    .iter()
                    .filter_map(|lookup| {
                        lookup
                            .provider_episode_id
                            .as_ref()
                            .map(|provider_episode_id| {
                                crate::catalog::UnifiedAnimeProviderEpisodeInput {
                                    provider_key: lookup.provider.cache_key().to_string(),
                                    provider_episode_id: provider_episode_id.clone(),
                                    provider_episode_number: Some(
                                        lookup.provider_episode_number as i32,
                                    ),
                                    metadata: serde_json::json!({
                                        "provider_season_id": lookup.provider_season_id,
                                    }),
                                }
                            })
                    })
                    .collect(),
                provider_lookups: episode
                    .provider_lookups
                    .iter()
                    .map(|lookup| crate::catalog::UnifiedAnimeProviderLookupInput {
                        provider_key: lookup.provider.cache_key().to_string(),
                        provider_title_id: lookup.provider_title_id.clone(),
                        provider_season_number: lookup.provider_season_number as i32,
                        provider_episode_number: lookup.provider_episode_number as i32,
                        lookup_reference: lookup.lookup_reference.clone(),
                        mapping_status: episode.mapping_status.clone(),
                        metadata: serde_json::json!({
                            "provider_episode_title": lookup.provider_title,
                        }),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        seasons.push(crate::catalog::UnifiedAnimeSeasonInput {
            season_number: unified_season.number as i32,
            canonical_label: None,
            episode_count_reported: Some(episodes.len() as i32),
            provider_seasons,
            episodes,
        });
    }

    let input = crate::catalog::UnifiedAnimeInput {
        canonical_title: primary.title.clone(),
        content_type: match primary.media_type {
            MediaType::Movie => "movie",
            MediaType::Series => "anime",
        }
        .to_string(),
        synopsis: response.description.clone(),
        release_year: details
            .iter()
            .find_map(|(_, details)| details.year.as_deref().and_then(|year| year.parse().ok())),
        poster_url: response.poster_url.clone(),
        catalog_status: None,
        provider_titles,
        seasons,
    };
    match store.upsert_unified_anime(input).await {
        Ok(id) => Some(id),
        Err(error) => {
            log::warn!("could not persist unified anime details: {error}");
            None
        }
    }
}

async fn resolve_unified_provider_mappings(
    state: &ApiState,
    reference: &UnifiedOpaqueRef,
) -> Vec<ProviderMediaId> {
    let mut mappings = vec![ProviderMediaId {
        provider: reference.p,
        value: reference.v.clone(),
    }];
    mappings.extend(reference.provider_mappings.iter().cloned());

    if let (Some(store), Some(canonical_id)) =
        (state.catalog.as_ref(), reference.canonical_id.as_deref())
    {
        match store.get_title_detail(canonical_id).await {
            Ok(Some(detail)) => {
                for value in detail.provider_mappings {
                    let Some(object) = value.as_object() else {
                        continue;
                    };
                    let provider = object
                        .get("provider_key")
                        .and_then(serde_json::Value::as_str)
                        .and_then(ProviderKind::parse);
                    let provider_id = object
                        .get("provider_title_id")
                        .and_then(serde_json::Value::as_str);
                    if let (Some(provider), Some(provider_id)) = (provider, provider_id) {
                        if !provider_id.trim().is_empty() {
                            mappings.push(ProviderMediaId {
                                provider,
                                value: provider_id.to_string(),
                            });
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(error) => log::warn!("could not load persisted unified provider mappings: {error}"),
        }
    }

    let eligible = configured_search_providers();
    mappings
        .retain(|mapping| eligible.contains(&mapping.provider) && !mapping.value.trim().is_empty());
    let mut seen = std::collections::HashSet::new();
    mappings.retain(|mapping| seen.insert((mapping.provider, mapping.value.clone())));
    mappings.sort_by_key(|mapping| provider_priority(mapping.provider));
    mappings
}

async fn fetch_unified_provider_details(
    state: &ApiState,
    mappings: &[ProviderMediaId],
) -> (
    Vec<(ProviderMediaId, MediaDetails)>,
    HashMap<String, String>,
) {
    let mut tasks = JoinSet::new();
    let slots = Arc::new(Semaphore::new(SEARCH_PROVIDER_CONCURRENCY));
    for mapping in mappings.iter().cloned() {
        let service = state.service.clone();
        let slots = Arc::clone(&slots);
        tasks.spawn(async move {
            let Ok(_permit) = slots.acquire_owned().await else {
                return (mapping, None, Some("concurrency_limit".to_string()));
            };
            let result = tokio::time::timeout(
                OVERALL_TIMEOUT,
                service.details_typed(mapping.provider, &mapping.value),
            )
            .await;
            match result {
                Ok(Ok(details))
                    if details.id.provider == mapping.provider
                        && details.id.value == mapping.value =>
                {
                    (mapping, Some(details), None)
                }
                Ok(Ok(_)) => (
                    mapping,
                    None,
                    Some("provider_identity_mismatch".to_string()),
                ),
                Ok(Err(error)) => {
                    let provider = mapping.provider;
                    (mapping, None, Some(error.user_message(provider)))
                }
                Err(_) => (mapping, None, Some("timeout".to_string())),
            }
        });
    }

    let mut results = Vec::new();
    let mut status = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((mapping, Some(details), _)) => {
                status.insert(mapping.provider.cache_key().to_string(), "ok".to_string());
                results.push((mapping, details));
            }
            Ok((mapping, None, Some(error))) => {
                status.insert(mapping.provider.cache_key().to_string(), error);
            }
            Ok((mapping, None, None)) => {
                status.insert(
                    mapping.provider.cache_key().to_string(),
                    "unavailable".to_string(),
                );
            }
            Err(error) => {
                log::warn!("unified details provider task failed: {error}");
                status.insert("provider_task".to_string(), "task_failed".to_string());
            }
        }
    }
    results.sort_by_key(|(mapping, _)| provider_priority(mapping.provider));
    (results, status)
}

async fn discover_unmapped_provider_details(
    state: &ApiState,
    known_details: &[(ProviderMediaId, MediaDetails)],
    canonical_id: Option<&str>,
) -> (
    Vec<(ProviderMediaId, MediaDetails)>,
    HashMap<String, String>,
) {
    let mut status = HashMap::new();
    let trusted_identity = match known_details.first().map(|(_, details)| details.clone()) {
        Some(identity) => Some(identity),
        None => {
            if let (Some(store), Some(cid)) = (state.catalog.as_ref(), canonical_id) {
                match store.get_title_detail(cid).await {
                    Ok(Some(detail)) => {
                        let media_type = if detail.title.content_type == "movie" {
                            MediaType::Movie
                        } else {
                            MediaType::Series
                        };
                        Some(MediaDetails {
                            id: ProviderMediaId {
                                provider: ProviderKind::FourKHdHub,
                                value: String::new(),
                            },
                            title: detail.title.canonical_title,
                            media_type,
                            year: detail.title.release_year.map(|y| y.to_string()),
                            description: detail.title.synopsis,
                            tagline: None,
                            imdb_rating: None,
                            director: None,
                            stars: None,
                            prints: None,
                            audios: None,
                            poster_url: detail.title.poster_url,
                            duration: None,
                            genres: Vec::new(),
                            seasons: Vec::new(),
                            dubs: Vec::new(),
                        })
                    }
                    _ => None,
                }
            } else {
                None
            }
        }
    };
    let Some(primary_identity) = trusted_identity else {
        return (Vec::new(), status);
    };
    let targets = configured_search_providers()
        .into_iter()
        .filter(|provider| {
            !known_details
                .iter()
                .any(|(mapping, _)| mapping.provider == *provider)
        });
    let query = primary_identity.title.clone();
    let slots = Arc::new(Semaphore::new(SEARCH_PROVIDER_CONCURRENCY));
    let mut tasks = JoinSet::new();

    for provider in targets {
        let service = Arc::clone(&state.service);
        let query = query.clone();
        let primary_identity = primary_identity.clone();
        let slots = Arc::clone(&slots);
        tasks.spawn(async move {
            let Ok(_permit) = slots.acquire_owned().await else {
                return (provider, None, "concurrency_limit".to_string());
            };
            let supports_pagination = service.capabilities(provider).supports_pagination;
            let search = tokio::time::timeout(
                DYNAMIC_DISCOVERY_TIMEOUT,
                search_provider_pages(supports_pagination, |page| {
                    let service = Arc::clone(&service);
                    let query = query.clone();
                    async move { service.search_typed(provider, &query, page).await }
                }),
            )
            .await;
            let (items, pagination_status) = match search {
                Ok(Ok(result)) => (
                    result.items,
                    result
                        .partial_error
                        .as_ref()
                        .map(|error| partial_provider_status(provider, error)),
                ),
                Ok(Err(error)) => {
                    return (provider, None, error.user_message(provider));
                }
                Err(_) => return (provider, None, "timeout".to_string()),
            };
            if items.is_empty() {
                return (provider, None, "empty".to_string());
            }
            let primary_title = crate::unified::normalize_anime_title(&primary_identity.title);
            let candidates = items
                .into_iter()
                .filter(|item| {
                    let candidate_title = crate::unified::normalize_anime_title(&item.title);
                    item.media_type == primary_identity.media_type
                        && candidate_title.key == primary_title.key
                        && candidate_title.franchise_discriminator
                            == primary_title.franchise_discriminator
                        && years_compatible(primary_identity.year.as_deref(), item.year.as_deref())
                })
                .take(DYNAMIC_DISCOVERY_MAX_RESULTS)
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                return (provider, None, "no_verified_match".to_string());
            }

            let mut verified = Vec::new();
            for item in candidates {
                let details = tokio::time::timeout(
                    DYNAMIC_DISCOVERY_TIMEOUT,
                    service.details_typed(provider, &item.id.value),
                )
                .await;
                if let Ok(Ok(details)) = details
                    && details.id.provider == provider
                    && details.id.value == item.id.value
                    && same_anime_identity(&primary_identity, &details)
                {
                    verified.push((item.id, details));
                }
            }
            match verified.len() {
                1 => (
                    provider,
                    verified.pop(),
                    pagination_status.unwrap_or_else(|| "ok".to_string()),
                ),
                0 => (provider, None, "no_verified_match".to_string()),
                _ => (provider, None, "ambiguous_match".to_string()),
            }
        });
    }

    let mut discovered = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok((provider, Some((mapping, details)), provider_status)) => {
                status.insert(provider.cache_key().to_string(), provider_status);
                discovered.push((mapping, details));
            }
            Ok((provider, None, provider_status)) => {
                status.insert(provider.cache_key().to_string(), provider_status);
            }
            Err(error) => log::warn!("unified provider discovery task failed: {error}"),
        }
    }
    discovered.sort_by_key(|(mapping, _)| provider_priority(mapping.provider));
    (discovered, status)
}

fn unified_api_details(
    details: &[(ProviderMediaId, MediaDetails)],
    all_mappings: &[ProviderMediaId],
    canonical_id: String,
    provider_status: HashMap<String, String>,
) -> UnifiedApiDetails {
    let primary = &details[0].1;
    let id = encode(&UnifiedOpaqueRef {
        p: details[0].0.provider,
        v: details[0].0.value.clone(),
        s: 0,
        e: 0,
        canonical_id: Some(canonical_id.clone()),
        provider_mappings: all_mappings
            .iter()
            .filter(|mapping| **mapping != details[0].0)
            .cloned()
            .collect(),
        unified: true,
    });

    let mut groups: Vec<Vec<UnifiedEpisodeCandidate>> = Vec::new();
    let mut candidates = Vec::new();
    let mut season_numbers = Vec::new();
    for (anime_id, provider_details) in details {
        for season in &provider_details.seasons {
            if !season_numbers.contains(&season.number) {
                season_numbers.push(season.number);
            }
            for episode in &season.episodes {
                candidates.push(UnifiedEpisodeCandidate {
                    anime_id: anime_id.clone(),
                    episode: episode.clone(),
                });
            }
        }
    }
    // Process providers in deterministic priority order. The existing gap helper
    // identifies coordinates absent from the canonical list; a gap is filled only
    // by this loop when a provider has returned a real episode record for it.
    candidates.sort_by_key(|candidate| {
        (
            provider_priority(candidate.anime_id.provider),
            candidate.episode.season,
            candidate.episode.number,
        )
    });
    let mut seen_provider_records = HashSet::new();
    candidates.retain(|candidate| {
        let key = (
            candidate.anime_id.provider,
            candidate.anime_id.value.clone(),
            candidate.episode.season,
            candidate.episode.number,
            candidate.episode.title.clone(),
            candidate.episode.overview.clone(),
            candidate.episode.provider_episode_id.clone(),
            candidate.episode.provider_season_id.clone(),
        );
        seen_provider_records.insert(key)
    });
    for candidate in candidates {
        let expected_through = details
            .iter()
            .flat_map(|(_, item)| item.seasons.iter())
            .filter(|season| season.number == candidate.episode.season)
            .flat_map(|season| season.episodes.iter())
            .map(|episode| episode.number)
            .max()
            .unwrap_or(candidate.episode.number);
        let canonical_coordinates = groups
            .iter()
            .map(|group| {
                let episode = &group[0].episode;
                crate::unified::CanonicalEpisode {
                    season: episode.season,
                    episode: episode.number,
                    title: episode.title.clone(),
                    overview: episode.overview.clone(),
                    mappings: Vec::new(),
                    unresolved_mappings: Vec::new(),
                }
            })
            .collect::<Vec<_>>();
        let missing = crate::unified::missing_episode_numbers(
            &canonical_coordinates,
            candidate.episode.season,
            expected_through,
        );
        if missing.contains(&candidate.episode.number) {
            // This is a true observed gap fill: only the provider's returned record
            // is added, and no title or provider ID is synthesized.
            groups.push(vec![candidate]);
        } else if let Some(group) = groups.iter_mut().find(|group| {
            group
                .iter()
                .all(|existing| episode_candidates_can_merge(existing, &candidate))
        }) {
            group.push(candidate);
        } else {
            // Same coordinate with conflicting metadata (or a same-provider
            // duplicate) remains a separate unresolved response record.
            groups.push(vec![candidate]);
        }
    }
    groups.sort_by_key(|group| {
        let candidate = &group[0];
        (
            candidate.episode.season,
            candidate.episode.number,
            provider_priority(candidate.anime_id.provider),
        )
    });
    let mut coordinate_counts = HashMap::<(usize, usize), usize>::new();
    for group in &groups {
        *coordinate_counts
            .entry((group[0].episode.season, group[0].episode.number))
            .or_default() += 1;
    }
    let unresolved_coordinates = coordinate_counts
        .into_iter()
        .filter_map(|(coordinate, count)| (count > 1).then_some(coordinate))
        .collect::<std::collections::HashSet<_>>();
    season_numbers.sort_unstable();
    for group in &groups {
        if !season_numbers.contains(&group[0].episode.season) {
            season_numbers.push(group[0].episode.season);
        }
    }
    season_numbers.sort_unstable();

    let mut unified_episodes = Vec::new();
    for mut group in groups {
        group.sort_by_key(|candidate| provider_priority(candidate.anime_id.provider));
        let first = &group[0];
        let mut mappings = group
            .iter()
            .skip(1)
            .map(|candidate| ApiProviderMapping {
                id: encode(&OpaqueRef {
                    p: candidate.anime_id.provider,
                    v: candidate.anime_id.value.clone(),
                    s: candidate.episode.season,
                    e: candidate.episode.number,
                }),
                provider: candidate.anime_id.provider,
            })
            .collect::<Vec<_>>();
        mappings.dedup_by_key(|mapping| mapping.provider);
        let title = group.iter().find_map(|candidate| {
            candidate
                .episode
                .title
                .clone()
                .filter(|value| !value.trim().is_empty())
        });
        let overview = group.iter().find_map(|candidate| {
            candidate
                .episode
                .overview
                .clone()
                .filter(|value| !value.trim().is_empty())
        });
        unified_episodes.push(UnifiedApiEpisode {
            id: encode(&OpaqueRef {
                p: first.anime_id.provider,
                v: first.anime_id.value.clone(),
                s: first.episode.season,
                e: first.episode.number,
            }),
            canonical_id: canonical_episode_response_id(&canonical_id, first),
            provider_mappings: std::mem::take(&mut mappings),
            provider_lookups: group
                .iter()
                .map(|candidate| UnifiedEpisodeProviderLookup {
                    provider: candidate.anime_id.provider,
                    provider_title_id: candidate.anime_id.value.clone(),
                    provider_season_id: candidate.episode.provider_season_id.clone(),
                    provider_episode_id: candidate.episode.provider_episode_id.clone(),
                    provider_season_number: candidate.episode.season,
                    provider_episode_number: candidate.episode.number,
                    lookup_reference: encode(&OpaqueRef {
                        p: candidate.anime_id.provider,
                        v: candidate.anime_id.value.clone(),
                        s: candidate.episode.season,
                        e: candidate.episode.number,
                    }),
                    provider_title: candidate.episode.title.clone(),
                })
                .collect(),
            mapping_status: if unresolved_coordinates
                .contains(&(first.episode.season, first.episode.number))
            {
                "unresolved".to_string()
            } else {
                "resolved".to_string()
            },
            season: first.episode.season,
            number: first.episode.number,
            title,
            overview,
        });
    }

    let seasons = season_numbers
        .into_iter()
        .map(|number| {
            let season_mappings = details
                .iter()
                .filter_map(|(anime_id, provider_details)| {
                    provider_details
                        .seasons
                        .iter()
                        .find(|season| season.number == number)
                        .map(|season| ApiSeasonProviderMapping {
                            provider: anime_id.provider,
                            provider_season_id: season.provider_season_id.clone(),
                            provider_season_number: Some(season.number),
                        })
                })
                .collect();
            UnifiedApiSeason {
                canonical_id: format!("{canonical_id}:season:{number}"),
                number,
                episode_count: unified_episodes
                    .iter()
                    .filter(|episode| episode.season == number)
                    .count(),
                provider_mappings: season_mappings,
                episodes: unified_episodes
                    .iter()
                    .filter(|episode| episode.season == number)
                    .cloned()
                    .collect(),
            }
        })
        .collect();

    let provider_mappings = all_mappings
        .iter()
        .filter(|mapping| **mapping != details[0].0)
        .map(|mapping| ApiProviderMapping {
            id: provider_ref(mapping, 0, 0),
            provider: mapping.provider,
        })
        .collect();
    let first_nonempty = |select: fn(&MediaDetails) -> &Option<String>| {
        details.iter().find_map(|(_, item)| {
            select(item)
                .as_ref()
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
    };
    let mut genres = Vec::new();
    let mut normalized_genres = std::collections::HashSet::new();
    for (_, item) in details {
        for genre in &item.genres {
            let key = genre.trim().to_lowercase();
            if !key.is_empty() && normalized_genres.insert(key) {
                genres.push(genre.clone());
            }
        }
    }
    UnifiedApiDetails {
        id,
        canonical_id,
        provider_mappings,
        partial: provider_status.values().any(|status| status != "ok"),
        provider_status,
        title: if primary.title.trim().is_empty() {
            details
                .iter()
                .find_map(|(_, item)| (!item.title.trim().is_empty()).then(|| item.title.clone()))
                .unwrap_or_default()
        } else {
            primary.title.clone()
        },
        media_type: primary.media_type,
        year: first_nonempty(|item| &item.year),
        description: first_nonempty(|item| &item.description),
        tagline: first_nonempty(|item| &item.tagline),
        imdb_rating: first_nonempty(|item| &item.imdb_rating),
        director: first_nonempty(|item| &item.director),
        stars: first_nonempty(|item| &item.stars),
        prints: first_nonempty(|item| &item.prints),
        audios: first_nonempty(|item| &item.audios),
        poster_url: details.iter().find_map(|(_, item)| {
            item.poster_url
                .as_ref()
                .filter(|value| crate::net::validate_url_syntax(value, true).is_ok())
                .cloned()
        }),
        duration: first_nonempty(|item| &item.duration),
        genres,
        seasons,
        // Audio track selection/aggregation is explicitly a later phase.
        audio: primary.dubs.clone(),
    }
}

async fn unified_anime_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
    unified: UnifiedOpaqueRef,
    primary_ref: OpaqueRef,
) -> std::io::Result<()> {
    let parts: Vec<_> = path.trim_matches('/').split('/').collect();
    let mut mappings = resolve_unified_provider_mappings(&state, &unified).await;
    let (mut details, mut provider_status) =
        fetch_unified_provider_details(&state, &mappings).await;
    let (discovered, discovery_status) =
        discover_unmapped_provider_details(&state, &details, unified.canonical_id.as_deref()).await;
    for (mapping, provider_details) in discovered {
        // Replace a stale same-provider mapping with the newly verified ID.
        mappings.retain(|existing| existing.provider != mapping.provider);
        details.retain(|(existing, _)| existing.provider != mapping.provider);
        mappings.push(mapping.clone());
        details.push((mapping, provider_details));
    }
    provider_status.extend(discovery_status);
    mappings.sort_by_key(|mapping| provider_priority(mapping.provider));
    details.sort_by_key(|(mapping, _)| provider_priority(mapping.provider));
    if details.is_empty() {
        let provider = primary_ref.p;
        let message = provider_status
            .get(provider.cache_key())
            .map(String::as_str)
            .unwrap_or("No mapped provider returned details.");
        let (status, body) = if message == "timeout" {
            err(
                "PROVIDER_TIMEOUT",
                "Mapped provider details timed out.",
                Some(provider),
                504,
            )
        } else {
            err(
                "UPSTREAM_UNAVAILABLE",
                "No mapped provider returned details.",
                Some(provider),
                502,
            )
        };
        log::debug!("unified details unavailable: {message}");
        return write_raw_json(stream, status, &body, None).await;
    }

    let primary_details = details
        .iter()
        .find(|(mapping, _)| mapping.provider == primary_ref.p && mapping.value == primary_ref.v)
        .map(|(_, details)| details.clone())
        .unwrap_or_else(|| details[0].1.clone());
    if parts.len() == 7 && parts[4] == "episodes" && parts[6] == "full" {
        let episode_ref = match decode::<OpaqueRef>(parts[5]) {
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
        if !mappings
            .iter()
            .any(|mapping| mapping.provider == episode_ref.p && mapping.value == episode_ref.v)
        {
            let (status, body) = err(
                "INVALID_ID",
                "The episode provider mapping is not part of this anime.",
                Some(primary_ref.p),
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
        let episode_details = details
            .iter()
            .find(|(mapping, _)| {
                mapping.provider == episode_ref.p && mapping.value == episode_ref.v
            })
            .map(|(_, details)| details.clone());
        let Some(episode_details) = episode_details else {
            let (status, body) = err(
                "UPSTREAM_UNAVAILABLE",
                "The mapped episode provider details are unavailable.",
                Some(episode_ref.p),
                502,
            );
            return write_raw_json(stream, status, &body, None).await;
        };
        let selected_episode = episode_details
            .seasons
            .iter()
            .flat_map(|season| season.episodes.iter())
            .find(|episode| episode.season == episode_ref.s && episode.number == episode_ref.e)
            .cloned();
        let Some(selected_episode) = selected_episode else {
            let (status, body) = err(
                "EPISODE_NOT_FOUND",
                "The episode was not found in this anime.",
                Some(episode_ref.p),
                404,
            );
            return write_raw_json(stream, status, &body, None).await;
        };
        let mut alternate_episode_refs = details
            .iter()
            .filter_map(|(mapping, provider_details)| {
                if mapping.provider == episode_ref.p && mapping.value == episode_ref.v {
                    return None;
                }
                let episode = matching_episode(&selected_episode, provider_details)?;
                if episode_special_marker(selected_episode.title.as_deref())
                    != episode_special_marker(episode.title.as_deref())
                    || !crate::unified::episode_titles_compatible(
                        selected_episode.title.as_deref(),
                        episode.title.as_deref(),
                    )
                {
                    return None;
                }
                Some(OpaqueRef {
                    p: mapping.provider,
                    v: mapping.value.clone(),
                    s: episode.season,
                    e: episode.number,
                })
            })
            .collect::<Vec<_>>();
        append_unique_episode_references(
            &mut alternate_episode_refs,
            discover_episode_mappings(
                &state,
                &mappings,
                &details,
                &episode_details,
                &selected_episode,
                unified.canonical_id.as_deref(),
            )
            .await,
        );
        return anime_episode_full_route(
            stream,
            parts[3],
            parts[5],
            primary_ref,
            episode_details,
            alternate_episode_refs,
            state,
        )
        .await;
    }
    if parts.len() == 7 && parts[4] == "episodes" && parts[6] == "stream" {
        let audio = query
            .split('&')
            .find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "audio").then(|| percent_decode(value).trim().to_ascii_lowercase())
            })
            .unwrap_or_else(|| "hindi".to_string());
        if crate::unified::normalize_language_code(&audio).is_none() {
            let (status, body) = err(
                "UNSUPPORTED_AUDIO_LANGUAGE",
                "The requested audio language is not recognized.",
                Some(primary_ref.p),
                400,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
        return playback_route(
            stream,
            PlaybackIdentity::Episode {
                anime_id: parts[3].to_string(),
                episode_id: parts[5].to_string(),
                anime_ref: primary_ref.clone(),
            },
            details
                .iter()
                .find(|(mapping, _)| {
                    mapping.provider
                        == decode::<OpaqueRef>(parts[5])
                            .ok()
                            .map(|reference| reference.p)
                            .unwrap_or(primary_ref.p)
                        && mapping.value
                            == decode::<OpaqueRef>(parts[5])
                                .ok()
                                .map(|reference| reference.v)
                                .unwrap_or_else(|| primary_ref.v.clone())
                })
                .map(|(_, details)| details.clone())
                .unwrap_or_else(|| primary_details.clone()),
            state,
            &audio,
        )
        .await;
    }

    let canonical_id = unified.canonical_id.clone().unwrap_or_else(|| {
        fallback_canonical_id(&CatalogItem {
            id: ProviderMediaId {
                provider: details[0].0.provider,
                value: details[0].0.value.clone(),
            },
            title: details[0].1.title.clone(),
            media_type: details[0].1.media_type,
            year: details[0].1.year.clone(),
            poster_url: details[0].1.poster_url.clone(),
            season_count: Some(details[0].1.seasons.len()),
        })
    });
    let mut response = unified_api_details(
        &details,
        &mappings,
        canonical_id.clone(),
        provider_status.clone(),
    );
    if let Some(persisted_id) =
        persist_unified_details(&state, &details, &mappings, &response).await
    {
        if persisted_id != canonical_id {
            response = unified_api_details(&details, &mappings, persisted_id, provider_status);
        }
    }
    if parts.len() == 4 {
        return write_json(stream, 200, &response, None).await;
    }
    if parts[4] == "seasons" {
        return write_json(stream, 200, &response.seasons, None).await;
    }
    if parts[4] == "episodes" {
        if parts.len() == 5 {
            let episodes = response
                .seasons
                .iter()
                .flat_map(|season| season.episodes.iter().cloned())
                .collect::<Vec<_>>();
            return write_json(stream, 200, &episodes, None).await;
        }
        let selected = decode::<OpaqueRef>(parts[5])
            .ok()
            .and_then(|episode_ref| {
                if !mappings.iter().any(|mapping| {
                    mapping.provider == episode_ref.p && mapping.value == episode_ref.v
                }) {
                    return None;
                }
                response
                    .seasons
                    .iter()
                    .flat_map(|season| season.episodes.iter())
                    .find(|episode| {
                        episode.season == episode_ref.s && episode.number == episode_ref.e
                    })
                    .cloned()
            })
            .or_else(|| {
                let number = parts[5].parse::<usize>().ok()?;
                response
                    .seasons
                    .iter()
                    .flat_map(|season| season.episodes.iter())
                    .find(|episode| episode.number == number)
                    .cloned()
            });
        return match selected {
            Some(episode) => write_json(stream, 200, &episode, None).await,
            None => {
                let (status, body) = err(
                    "EPISODE_NOT_FOUND",
                    "The episode was not found.",
                    Some(primary_ref.p),
                    404,
                );
                write_raw_json(stream, status, &body, None).await
            }
        };
    }
    let (status, body) = err("INTERNAL_ERROR", "Unsupported anime route.", None, 404);
    write_raw_json(stream, status, &body, None).await
}

#[derive(Clone)]
struct MovieIdentity {
    provider: ProviderKind,
    provider_id: String,
}

impl MovieIdentity {
    fn provider_lookup_ref(&self) -> OpaqueRef {
        // The current provider API exposes only `episode_streams`; the MovieBox
        // adapter's movie play-info protocol is selected by (0, 0). This is
        // provider transport data, not a catalog season or episode identity.
        OpaqueRef {
            p: self.provider,
            v: self.provider_id.clone(),
            s: 0,
            e: 0,
        }
    }
}

enum PlaybackIdentity {
    Movie {
        movie_id: String,
        movie_ref: MovieIdentity,
    },
    Episode {
        anime_id: String,
        episode_id: String,
        anime_ref: OpaqueRef,
    },
}

impl PlaybackIdentity {
    fn add_response_fields(&self, object: &mut serde_json::Map<String, serde_json::Value>) {
        match self {
            Self::Movie { movie_id, .. } => {
                object.insert("movie_id".into(), serde_json::json!(movie_id));
            }
            Self::Episode {
                anime_id,
                episode_id,
                ..
            } => {
                object.insert("anime_id".into(), serde_json::json!(anime_id));
                object.insert("episode_id".into(), serde_json::json!(episode_id));
            }
        }
    }

    fn content_label(&self) -> &'static str {
        match self {
            Self::Movie { .. } => "movie",
            Self::Episode { .. } => "episode",
        }
    }
}

fn source_remux_url(source_id: &str) -> String {
    format!("/api/v1/sources/{source_id}/remux")
}

fn source_playback_url(
    state: &ApiState,
    source_id: &str,
    upstream_url: &str,
    upstream_headers: &[(String, String)],
) -> String {
    state
        .native_stream
        .playback_url(source_id, upstream_url, upstream_headers)
        .unwrap_or_else(|| source_remux_url(source_id))
}

async fn resolve_playback_candidate(
    state: &ApiState,
    release: &Release,
) -> Result<crate::providers::models::PlaybackSource, String> {
    if release.provider == ProviderKind::AniZone {
        return state
            .service
            .anizone_client
            .resolve_release(release)
            .await
            .map_err(|error| error.to_string());
    }
    if release.provider == ProviderKind::FourKHdHub {
        let client = state
            .service
            .fourk_client
            .as_ref()
            .ok_or_else(|| "The playback provider is unavailable.".to_string())?;
        return client
            .resolve_release(
                release,
                crate::providers::models::ResolutionIntent::Playback,
            )
            .await
            .map_err(|error| error.user_message().to_string());
    }
    let url = release
        .direct_url()
        .filter(|url| !url.trim().is_empty())
        .ok_or_else(|| "The source does not expose a playable URL.".to_string())?;
    crate::net::validate_public_url(url, true)
        .await
        .map_err(|_| "The source URL is not an allowed public destination.".to_string())?;
    Ok(crate::providers::models::PlaybackSource {
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
    })
}

async fn playback_route(
    stream: &mut TcpStream,
    identity: PlaybackIdentity,
    media_details: MediaDetails,
    state: ApiState,
    requested_audio: &str,
) -> std::io::Result<()> {
    let source_ref = match &identity {
        PlaybackIdentity::Episode {
            anime_id,
            episode_id,
            anime_ref,
        } => {
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
            if episode_ref.p != media_details.id.provider || episode_ref.v != media_details.id.value
            {
                let (status, body) = err(
                    "INVALID_ID",
                    "The episode does not belong to the requested anime.",
                    Some(anime_ref.p),
                    400,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
            let unified_anime_id_matches =
                decode::<UnifiedOpaqueRef>(anime_id)
                    .ok()
                    .is_some_and(|reference| {
                        reference.unified
                            && reference.p == anime_ref.p
                            && reference.v == anime_ref.v
                    });
            if (media_details.id.provider != anime_ref.p || encode(anime_ref) != *anime_id)
                && !unified_anime_id_matches
            {
                let (status, body) = err(
                    "INVALID_ID",
                    "The anime identifier does not match the requested item.",
                    Some(anime_ref.p),
                    400,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
            let episode_exists = media_details.seasons.iter().any(|season| {
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
            episode_ref
        }
        PlaybackIdentity::Movie {
            movie_id,
            movie_ref,
        } => {
            let transport_ref = decode::<OpaqueRef>(movie_id).ok();
            if media_details.media_type != MediaType::Movie
                || media_details.id.provider != movie_ref.provider
                || media_details.id.value != movie_ref.provider_id
                || !transport_ref.as_ref().is_some_and(|reference| {
                    reference.p == movie_ref.provider
                        && reference.v == movie_ref.provider_id
                        && reference.s == 0
                        && reference.e == 0
                })
            {
                let (status, body) = err(
                    "INVALID_ID",
                    "The movie identifier does not match the requested movie.",
                    Some(movie_ref.provider),
                    400,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
            movie_ref.provider_lookup_ref()
        }
    };
    let content_label = identity.content_label();
    let deadline = Instant::now() + OVERALL_TIMEOUT;
    let mut references = vec![source_ref.clone()];
    if matches!(&identity, PlaybackIdentity::Episode { .. }) {
        let fallback_source = OpaqueSource {
            p: source_ref.p,
            v: source_ref.v.clone(),
            s: source_ref.s,
            e: source_ref.e,
            content_type: None,
            f: None,
            i: 0,
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            if let Ok(fallback_refs) = tokio::time::timeout(
                remaining,
                verified_fallback_episode_refs(&state, &fallback_source),
            )
            .await
            {
                append_unique_episode_references(&mut references, fallback_refs);
            }
        }
    }
    let requested_language = (requested_audio != "auto")
        .then(|| crate::unified::normalize_language_code(requested_audio))
        .flatten();
    let mut selected_ref = source_ref.clone();
    let mut selected_release = None;
    let mut playback = None;
    let mut language_metadata_match = false;
    let mut saw_playable_source = false;
    let mut saw_incompatible_source = false;
    let mut saw_provider_failure = false;
    for candidate_ref in references.into_iter().take(8) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            saw_provider_failure = true;
            break;
        }
        let releases =
            match tokio::time::timeout(remaining, cached_releases(&state, &candidate_ref)).await {
                Ok(Ok(releases)) => releases,
                Ok(Err(error)) => {
                    saw_provider_failure = true;
                    log::warn!(
                        "playback provider failed provider={}: {}",
                        candidate_ref.p,
                        error
                    );
                    continue;
                }
                Err(_) => {
                    saw_provider_failure = true;
                    log::warn!("playback provider timed out provider={}", candidate_ref.p);
                    continue;
                }
            };
        let usable = releases
            .iter()
            .filter(|release| {
                release_has_usable_source(release) || release_requires_codec_probe(release)
            })
            .collect::<Vec<_>>();
        saw_playable_source |= !usable.is_empty();
        let mut candidates = usable
            .iter()
            .copied()
            .filter(|release| {
                requested_language.as_deref().is_none_or(|requested| {
                    release
                        .language
                        .as_deref()
                        .and_then(crate::unified::normalize_language_code)
                        .as_deref()
                        == Some(requested)
                })
            })
            .collect::<Vec<_>>();
        language_metadata_match |= !candidates.is_empty();
        if candidates.is_empty() && requested_language.is_none() {
            candidates = usable;
        }
        candidates.sort_by_key(|release| std::cmp::Reverse(release.resolution_u64()));
        for candidate in candidates.into_iter().take(3) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                saw_provider_failure = true;
                break;
            }
            let resolved = match tokio::time::timeout(
                remaining,
                resolve_playback_candidate(&state, candidate),
            )
            .await
            {
                Ok(Ok(resolved)) => resolved,
                Ok(Err(error)) => {
                    saw_provider_failure = true;
                    log::warn!(
                        "playback candidate failed provider={} source={}: {}",
                        candidate.provider,
                        candidate.source_label(),
                        error
                    );
                    continue;
                }
                Err(_) => {
                    saw_provider_failure = true;
                    log::warn!(
                        "playback candidate timed out provider={} source={}",
                        candidate.provider,
                        candidate.source_label()
                    );
                    continue;
                }
            };
            if release_requires_playback_codec_verification(candidate) {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    saw_provider_failure = true;
                    break;
                }
                match tokio::time::timeout(
                    remaining,
                    crate::codec_verification::verify_playback_source(&resolved),
                )
                .await
                {
                    Ok(Ok(crate::codec_verification::CodecProbeResult::Allowed(_))) => {}
                    Ok(Ok(crate::codec_verification::CodecProbeResult::BlockedHevc(_)))
                    | Ok(Ok(crate::codec_verification::CodecProbeResult::Unsupported(_)))
                    | Ok(Ok(crate::codec_verification::CodecProbeResult::NoVideoStream)) => {
                        saw_incompatible_source = true;
                        continue;
                    }
                    Ok(Ok(crate::codec_verification::CodecProbeResult::Unverified(_))) => {
                        saw_provider_failure = true;
                        continue;
                    }
                    Ok(Err(error)) => {
                        saw_provider_failure = true;
                        log::warn!(
                            "playback codec verification failed provider={} source={} code={}",
                            candidate.provider,
                            candidate.source_label(),
                            error.code()
                        );
                        continue;
                    }
                    Err(_) => {
                        saw_provider_failure = true;
                        log::warn!(
                            "playback codec verification timed out provider={} source={}",
                            candidate.provider,
                            candidate.source_label()
                        );
                        continue;
                    }
                }
            }
            selected_ref = candidate_ref.clone();
            selected_release = Some(candidate.clone());
            playback = Some(resolved);
            break;
        }
        if playback.is_some() {
            break;
        }
    }
    let Some(release) = selected_release else {
        if let Some(language) = requested_language {
            if saw_playable_source && !language_metadata_match {
                let (status, body) = err(
                    "AUDIO_LANGUAGE_UNAVAILABLE",
                    format!(
                        "No verified playable source is available for audio language {language}."
                    ),
                    Some(source_ref.p),
                    404,
                );
                return write_raw_json(stream, status, &body, None).await;
            }
        }
        let (code, message, status) = if saw_incompatible_source {
            (
                "NO_COMPATIBLE_SOURCE",
                "All eligible provider sources were incompatible or could not be verified."
                    .to_string(),
                415,
            )
        } else if saw_provider_failure {
            (
                "PLAYBACK_RESOLUTION_FAILED",
                "No eligible provider source could be resolved for the requested audio."
                    .to_string(),
                502,
            )
        } else {
            (
                "NO_STREAM_SOURCE",
                format!("The providers returned no stream sources for this {content_label}."),
                404,
            )
        };
        let (status, body) = err(code, message, Some(source_ref.p), status);
        return write_raw_json(stream, status, &body, None).await;
    };
    let playback = playback.expect("selected playback candidate always has a source");
    let source_id = match api_sources_for_content(
        std::slice::from_ref(&release),
        &selected_ref,
        Some(content_label),
    )
    .first()
    {
        Some(source) => source.id.clone(),
        None => {
            let (status, body) = err(
                "NO_STREAM_SOURCE",
                "The selected source cannot be represented as a verified playback source.",
                Some(release.provider),
                404,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    // Probe the resolved source for real audio tracks when possible. A failed probe
    // must not block playback (some HLS/DASH providers prevent ffprobe access).
    let probe_result = probe_media_bounded(&state, &playback.url, &playback.headers).await;
    let audio_tracks = match probe_result {
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
    let mut playback = playback;
    playback.url = source_playback_url(&state, &source_id, &playback.url, &playback.headers);
    playback.headers.clear();
    let mut body = playback_json(&playback);
    if let Some(object) = body.as_object_mut() {
        identity.add_response_fields(object);
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
    alternate_episode_refs: Vec<OpaqueRef>,
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
    if details_result.id.provider != episode_ref.p || details_result.id.value != episode_ref.v {
        let (status, body) = err(
            "INVALID_ID",
            "The episode provider mapping does not match the resolved details.",
            Some(episode_ref.p),
            400,
        );
        return write_raw_json(stream, status, &body, None).await;
    }

    let anime = details(details_result);
    let unified_anime_id_matches =
        decode::<UnifiedOpaqueRef>(anime_id)
            .ok()
            .is_some_and(|reference| {
                reference.unified && reference.p == anime_ref.p && reference.v == anime_ref.v
            });
    if anime.id != anime_id && !unified_anime_id_matches {
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

    let mut references = Vec::with_capacity(alternate_episode_refs.len() + 1);
    references.push(episode_ref.clone());
    for reference in alternate_episode_refs {
        if reference.s == episode_ref.s
            && reference.e == episode_ref.e
            && !references.iter().any(|existing| {
                existing.p == reference.p
                    && existing.v == reference.v
                    && existing.s == reference.s
                    && existing.e == reference.e
            })
        {
            references.push(reference);
        }
    }
    let release_results = futures::stream::iter(references.into_iter().map(|reference| {
        let state = state.clone();
        async move {
            let provider = reference.p;
            let result =
                tokio::time::timeout(OVERALL_TIMEOUT, cached_releases(&state, &reference)).await;
            (reference, provider, result)
        }
    }))
    .buffer_unordered(3)
    .collect::<Vec<_>>()
    .await;
    let results = release_results
        .into_iter()
        .map(|(reference, provider, result)| {
            let result = match result {
                Ok(Ok(releases)) => Ok(releases),
                Ok(Err(error)) => Err(EpisodeReleaseFailure::Provider(error)),
                Err(_) => Err(EpisodeReleaseFailure::Timeout),
            };
            (reference, provider, result)
        });
    let (sources, sources_status, sources_partial, source_error, provider_status) =
        aggregate_episode_sources(results);

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
            provider_status,
        },
        None,
    )
    .await
}

fn release_cache_key(r: &OpaqueRef) -> String {
    format!("{}:{}:{}:{}", r.p.cache_key(), r.v, r.s, r.e)
}

enum EpisodeReleaseFailure {
    Provider(ProviderError),
    Timeout,
}

type EpisodeReleaseResult = (
    OpaqueRef,
    ProviderKind,
    Result<Vec<Release>, EpisodeReleaseFailure>,
);

fn aggregate_episode_sources(
    release_results: impl IntoIterator<Item = EpisodeReleaseResult>,
) -> (
    Vec<ApiSource>,
    &'static str,
    bool,
    Option<serde_json::Value>,
    HashMap<String, String>,
) {
    let mut sources = Vec::new();
    let mut errors = Vec::new();
    let mut provider_status = HashMap::new();
    for (reference, provider, result) in release_results {
        let provider_key = provider.cache_key().to_string();
        match result {
            Ok(releases) => {
                let provider_sources = api_sources(&releases, &reference);
                provider_status.insert(
                    provider_key,
                    if provider_sources.is_empty() {
                        "episode_list_ok_no_sources".to_string()
                    } else {
                        "sources_ok".to_string()
                    },
                );
                sources.extend(provider_sources);
            }
            Err(EpisodeReleaseFailure::Provider(error)) => {
                provider_status.insert(provider_key, "source_error".to_string());
                let (_, body) = provider_error(error, provider);
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) {
                    errors.push(value);
                }
            }
            Err(EpisodeReleaseFailure::Timeout) => {
                provider_status.insert(provider_key, "source_timeout".to_string());
                let (_, body) = err(
                    "REQUEST_DEADLINE_EXCEEDED",
                    "The source request deadline was exceeded.",
                    Some(provider),
                    504,
                );
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) {
                    errors.push(value);
                }
            }
        }
    }
    sources.sort_by(|left, right| left.id.cmp(&right.id));
    sources.dedup_by(|left, right| left.id == right.id);
    let source_error = errors.into_iter().next();
    let sources_partial = source_error.is_some();
    let sources_status = if sources.is_empty() {
        "unavailable"
    } else if sources_partial {
        "partial"
    } else {
        "ok"
    };
    (
        sources,
        sources_status,
        sources_partial,
        source_error,
        provider_status,
    )
}

async fn cached_releases(state: &ApiState, r: &OpaqueRef) -> Result<Vec<Release>, ProviderError> {
    let key = release_cache_key(r);
    #[cfg(test)]
    {
        if let Some(delay) = state.test_release_delays.lock().await.get(&key).copied() {
            tokio::time::sleep(delay).await;
        }
        if let Some(result) = state.test_release_overrides.lock().await.get(&key).cloned() {
            return result;
        }
    }
    if let Some(entry) = state.release_cache.lock().await.get(&key).cloned()
        && entry.stored_at.elapsed() < RELEASE_CACHE_TTL
    {
        crate::observability::record_cache("release", "hit");
        return Ok(entry.releases);
    }
    crate::observability::record_cache("release", "miss");

    // Keep one provider fetch in flight per episode. The task is spawned before
    // callers await it, so a request timeout cannot leave a permanently pending
    // future in the map or cancel the shared upstream operation.
    let shared = {
        let mut inflight = state.release_inflight.lock().await;
        if let Some(existing) = inflight.get(&key) {
            existing.clone()
        } else {
            let state = state.clone();
            let episode = r.clone();
            let task = tokio::spawn(async move { releases_uncached(&state, &episode).await });
            let future = async move {
                task.await.unwrap_or_else(|_| {
                    Err(ProviderError::Unavailable(
                        "source fetch task failed".into(),
                    ))
                })
            }
            .boxed()
            .shared();
            inflight.insert(key.clone(), future.clone());
            future
        }
    };

    let fetched = shared.await;
    if let Ok(releases) = &fetched {
        let mut cache = state.release_cache.lock().await;
        if cache.len() >= 2048 {
            cache.retain(|_, entry| entry.stored_at.elapsed() < RELEASE_CACHE_TTL);
        }
        cache.insert(
            key.clone(),
            ReleaseCacheEntry {
                stored_at: Instant::now(),
                releases: releases.clone(),
            },
        );
        crate::observability::record_cache("release", "insert");
    }
    // Keep the in-flight future visible until the successful cache entry is
    // published. A completion-boundary caller sees either representation.
    state.release_inflight.lock().await.remove(&key);
    fetched
}

async fn releases_uncached(state: &ApiState, r: &OpaqueRef) -> Result<Vec<Release>, ProviderError> {
    let svc = &state.service;
    match r.p {
        ProviderKind::MovieBox => {
            if !moviebox_provider_enabled() {
                return Err(ProviderError::Unavailable(
                    "MovieBox provider is disabled".into(),
                ));
            }
            crate::providers::ReleaseProvider::episode_streams(&svc.client, &r.v, r.s, r.e).await
        }
        ProviderKind::AniZone => {
            crate::providers::ReleaseProvider::episode_streams(&svc.anizone_client, &r.v, r.s, r.e)
                .await
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
    api_sources_for_content(rs, episode, None)
}
fn api_sources_for_content(
    rs: &[Release],
    episode: &OpaqueRef,
    content_type: Option<&str>,
) -> Vec<ApiSource> {
    let mut seen = std::collections::HashSet::new();
    rs.iter()
        .enumerate()
        .filter(|(_, release)| {
            if release.provider == ProviderKind::MovieBox && !moviebox_provider_enabled() {
                return false;
            }
            // Provider adapters may return a parseable release list containing
            // records for neighboring episodes. Unknown coordinates remain
            // usable for providers that do not expose them, but an explicit
            // mismatch must never become a source for this episode.
            release.provider == episode.p
                && release.season.is_none_or(|season| season == episode.s)
                && release.episode.is_none_or(|number| number == episode.e)
                && (source_codec_eligibility(release.codec.as_deref())
                    == SourceCodecEligibility::Verified
                    || (release.provider == ProviderKind::AniZone
                        && release_has_usable_source(release))
                    || release_requires_codec_probe(release))
        })
        .filter(|(_, release)| {
            // The public source ID includes an ordinal for compatibility, so
            // deduplicate before assigning it. Keep quality/language/codec and
            // headers in the identity because they are meaningful alternatives.
            let mut identity = String::new();
            identity.push_str(release.provider.cache_key());
            identity.push('\0');
            identity.push_str(release.quality.as_deref().unwrap_or_default());
            identity.push('\0');
            identity.push_str(release.codec.as_deref().unwrap_or_default());
            identity.push('\0');
            identity.push_str(release.language.as_deref().unwrap_or_default());
            identity.push('\0');
            for mirror in &release.mirrors {
                identity.push_str(&mirror.resolver_url);
                identity.push('\0');
                for (name, value) in &mirror.headers {
                    identity.push_str(&name.to_ascii_lowercase());
                    identity.push('=');
                    identity.push_str(value);
                    identity.push('\0');
                }
                identity.push('\u{ff}');
            }
            seen.insert(identity)
        })
        .map(|(i, r)| ApiSource {
            id: source_ref_for_content(r, episode, i, content_type),
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
    let validated_url = crate::net::validate_public_url(url, true)
        .await
        .map_err(|_| "The media source URL is not an allowed public destination.".to_string())?;
    probe_media_validated(&validated_url, headers).await
}

async fn probe_media_bounded(
    state: &ApiState,
    url: &str,
    headers: &[(String, String)],
) -> Result<ProbeOutput, String> {
    let _permit = state
        .remux_cache
        .try_acquire_job()
        .map_err(|_| "Media-processing capacity is busy.".to_string())?;
    probe_media(url, headers).await
}

async fn probe_media_validated(
    url: &reqwest::Url,
    headers: &[(String, String)],
) -> Result<ProbeOutput, String> {
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
    command.arg(url.as_str());
    command.kill_on_drop(true);
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
        match cached_playback_source(source_id, release, state).await {
            Ok(playback) => (watch_url(&playback.url), playback.headers),
            Err(message) => {
                let (s, b) = err("TRACK_DISCOVERY_UNAVAILABLE", message, Some(provider), 503);
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
    let probe = match probe_media_bounded(state, &url, &headers).await {
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

async fn source_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
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
    if let Some((code, message)) = codec_ineligibility_error(std::slice::from_ref(rel)) {
        let (status, body) = err(code, message, Some(sr.p), 415);
        return write_raw_json(stream, status, &body, None).await;
    }
    if matches!(parts.get(4), Some(&"audio") | Some(&"audio-tracks")) {
        return write_track_response(stream, id, sr.p, rel, &state, false).await;
    }
    if matches!(parts.get(4), Some(&"transcode") | Some(&"remux")) {
        return transcode_source(stream, id, rel, &state, query, request_headers, head_only).await;
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
        return write_verified_playback_response(stream, id, rel, &state, sr.p).await;
    }
    if sr.p == ProviderKind::FourKHdHub {
        return write_verified_playback_response(stream, id, rel, &state, sr.p).await;
    }
    let (status, body) = err(
        "CODEC_UNVERIFIED",
        "Direct media URLs are withheld until the source is positively verified as H.264.",
        Some(sr.p),
        415,
    );
    write_raw_json(stream, status, &body, None).await
}

fn direct_safe_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(name, _)| {
            name.eq_ignore_ascii_case("referer")
                || name.eq_ignore_ascii_case("user-agent")
                || name.eq_ignore_ascii_case("origin")
        })
        .cloned()
        .collect()
}

fn direct_url_type(
    url: &reqwest::Url,
    release: &Release,
    declared_content_type: Option<&str>,
) -> Option<&'static str> {
    let path = url.path().to_ascii_lowercase();
    let content_type = declared_content_type
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if path.ends_with(".m3u8")
        || content_type == "application/vnd.apple.mpegurl"
        || content_type == "application/x-mpegurl"
    {
        Some("hls")
    } else if path.ends_with(".mpd") || content_type == "application/dash+xml" {
        Some("dash")
    } else if matches!(
        content_type.as_str(),
        "video/mp4" | "video/webm" | "video/x-matroska"
    ) || (release
        .codec
        .as_deref()
        .is_some_and(is_supported_direct_codec)
        && (release.provider == ProviderKind::FourKHdHub
            || release
                .mirrors
                .first()
                .is_some_and(|mirror| mirror.direct_file)))
    {
        Some("direct")
    } else {
        None
    }
}

fn is_supported_direct_codec(codec: &str) -> bool {
    let normalized: String = codec
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    normalized.contains("h264")
        || normalized.contains("avc")
        || normalized.contains("vp8")
        || normalized.contains("vp9")
        || normalized.contains("av1")
}

async fn resolve_direct_provider_source(
    source: &Release,
    state: &ApiState,
) -> Result<PlaybackSource, String> {
    if source.provider == ProviderKind::AniZone {
        return match tokio::time::timeout(
            PLAYBACK_RESOLUTION_TIMEOUT,
            state.service.anizone_client.resolve_release(source),
        )
        .await
        {
            Ok(Ok(playback)) => Ok(playback),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("resolution timed out".to_string()),
        };
    }
    if source.provider == ProviderKind::FourKHdHub {
        let Some(client) = state.service.fourk_client.as_ref() else {
            return Err("provider unavailable".to_string());
        };
        return match tokio::time::timeout(
            PLAYBACK_RESOLUTION_TIMEOUT,
            client.resolve_release(
                source,
                crate::providers::models::ResolutionIntent::DirectPlayback,
            ),
        )
        .await
        {
            Ok(Ok(playback)) => Ok(playback),
            Ok(Err(error)) => Err(error.user_message().to_string()),
            Err(_) => Err("resolution timed out".to_string()),
        };
    }
    let Some(url) = source.direct_url() else {
        return Err("no direct provider URL".to_string());
    };
    Ok(PlaybackSource {
        provider: source.provider,
        url: url.to_string(),
        headers: source
            .mirrors
            .first()
            .map(|mirror| mirror.headers.clone())
            .unwrap_or_default(),
        subtitle: None,
        source_label: source.source_label().to_string(),
        max_height: None,
    })
}

async fn resolve_valid_direct_source(
    state: &ApiState,
    release: &Release,
    content_type: Option<&str>,
) -> Result<(PlaybackSource, String, reqwest::Url), String> {
    let resolution_started = Instant::now();
    let playback = resolve_direct_provider_source(release, state).await?;
    log::info!(
        "direct_play_timing provider={} stage=provider_url_resolution elapsed_ms={} outcome=ok",
        release.provider,
        resolution_started.elapsed().as_millis()
    );
    let validation_started = Instant::now();
    let validated_url = crate::net::validate_public_url(&playback.url, false)
        .await
        .map_err(|_| "resolved URL rejected".to_string())?;
    log::info!(
        "direct_play_timing provider={} stage=resolved_url_validation elapsed_ms={} outcome=ok",
        release.provider,
        validation_started.elapsed().as_millis()
    );
    let verified_release = verify_direct_playback_codec(release, &playback).await?;
    let url_type = direct_url_type(&validated_url, &verified_release, content_type)
        .ok_or_else(|| "direct playback compatibility unknown".to_string())?;
    Ok((playback, url_type.to_string(), validated_url))
}

async fn verified_fallback_episode_refs(state: &ApiState, source: &OpaqueSource) -> Vec<OpaqueRef> {
    #[cfg(test)]
    if let Some(refs) = state
        .test_fallback_refs
        .lock()
        .await
        .get(&release_cache_key(&OpaqueRef {
            p: source.p,
            v: source.v.clone(),
            s: source.s,
            e: source.e,
        }))
        .cloned()
    {
        return refs;
    }
    let allowed_providers = configured_search_providers();
    let mut mappings = if let Some(catalog) = state.catalog.as_ref() {
        match catalog
            .verified_episode_provider_mappings(source.p.cache_key(), &source.v, source.s, source.e)
            .await
        {
            Ok(mappings) => mappings
                .into_iter()
                .filter_map(|(provider, title, season, episode)| {
                    let provider = ProviderKind::parse(&provider)?;
                    (provider != source.p && allowed_providers.contains(&provider)).then_some(
                        OpaqueRef {
                            p: provider,
                            v: title,
                            s: season,
                            e: episode,
                        },
                    )
                })
                .collect::<Vec<_>>(),
            Err(error) => {
                log::debug!("verified fallback mappings unavailable: {error}");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    if matches!(
        source.p,
        ProviderKind::FourKHdHub | ProviderKind::Dramachi | ProviderKind::AniZone
    ) {
        let details = tokio::time::timeout(
            DYNAMIC_DISCOVERY_TIMEOUT,
            state.service.details_typed(source.p, &source.v),
        )
        .await;
        if let Ok(Ok(details)) = details
            && details.id.provider == source.p
            && details.id.value == source.v
            && details.media_type == MediaType::Series
        {
            let episode = details
                .seasons
                .iter()
                .flat_map(|season| season.episodes.iter())
                .find(|episode| episode.season == source.s && episode.number == source.e)
                .cloned();
            if let Some(episode) = episode {
                let provider_id = details.id.clone();
                let known_details = vec![(provider_id.clone(), details.clone())];
                let discovered = discover_episode_mappings(
                    state,
                    std::slice::from_ref(&provider_id),
                    &known_details,
                    &details,
                    &episode,
                    None,
                )
                .await;
                append_unique_episode_references(&mut mappings, discovered);
            }
        }
    }
    mappings.sort_by(|left, right| {
        (provider_priority(left.p), &left.v, left.s, left.e).cmp(&(
            provider_priority(right.p),
            &right.v,
            right.s,
            right.e,
        ))
    });
    let mut seen = std::collections::HashSet::new();
    mappings.retain(|reference| {
        seen.insert((reference.p, reference.v.clone(), reference.s, reference.e))
    });
    mappings.into_iter().take(8).collect()
}

fn release_matches_episode(release: &Release, source: &OpaqueSource) -> bool {
    release.provider == source.p
        && release.season.is_none_or(|season| season == source.s)
        && release.episode.is_none_or(|episode| episode == source.e)
}

async fn try_direct_fallback(
    state: &ApiState,
    source: &OpaqueSource,
    deadline: Instant,
) -> Option<((PlaybackSource, String, reqwest::Url), String, ProviderKind)> {
    // Retry the requested provider first so a failed high-quality mirror can
    // fall back to another validated source for the exact same episode. The
    // original source fingerprint is skipped below, preventing an identical
    // failed URL from being retried indefinitely.
    let original_ref = OpaqueRef {
        p: source.p,
        v: source.v.clone(),
        s: source.s,
        e: source.e,
    };
    let mut refs = vec![original_ref];
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return None;
    }
    let fallback_refs =
        tokio::time::timeout(remaining, verified_fallback_episode_refs(state, source))
            .await
            .ok()?;
    refs.extend(fallback_refs);
    refs.sort_by(|left, right| {
        (provider_priority(left.p), &left.v, left.s, left.e).cmp(&(
            provider_priority(right.p),
            &right.v,
            right.s,
            right.e,
        ))
    });
    let mut attempted_refs = std::collections::HashSet::new();
    let mut attempted_sources = std::collections::HashSet::new();
    for fallback_ref in refs.into_iter().take(3) {
        if !attempted_refs.insert((
            fallback_ref.p,
            fallback_ref.v.clone(),
            fallback_ref.s,
            fallback_ref.e,
        )) {
            continue;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(alternate_releases) =
            tokio::time::timeout(remaining, cached_releases(state, &fallback_ref))
                .await
                .unwrap_or(Err(ProviderError::Unavailable("fallback timeout".into())))
        else {
            continue;
        };
        let mut candidates = alternate_releases
            .into_iter()
            .filter(|candidate| {
                candidate.provider == fallback_ref.p
                    && candidate
                        .season
                        .is_none_or(|season| season == fallback_ref.s)
                    && candidate
                        .episode
                        .is_none_or(|episode| episode == fallback_ref.e)
                    && (release_has_usable_source(candidate)
                        || release_requires_codec_probe(candidate))
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|candidate| {
            (
                provider_priority(candidate.provider),
                std::cmp::Reverse(candidate.resolution_u64()),
            )
        });
        for candidate in candidates.into_iter().take(3) {
            if candidate.provider == source.p
                && source
                    .f
                    .as_deref()
                    .is_some_and(|fingerprint| release_fingerprint(&candidate) == fingerprint)
            {
                continue;
            }
            let candidate_source_id = api_sources_for_content(
                std::slice::from_ref(&candidate),
                &fallback_ref,
                source.content_type.as_deref(),
            )
            .first()
            .map(|source| source.id.clone());
            let Some(candidate_source_id) = candidate_source_id else {
                continue;
            };
            if !attempted_sources.insert(candidate_source_id.clone()) {
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            if let Ok(Ok(resolved)) = tokio::time::timeout(
                remaining,
                resolve_valid_direct_source(state, &candidate, source.content_type.as_deref()),
            )
            .await
            {
                return Some((resolved, candidate_source_id, candidate.provider));
            }
        }
    }
    None
}

async fn direct_play_route(
    stream: &mut TcpStream,
    state: ApiState,
    body: &[u8],
) -> std::io::Result<()> {
    let request: DirectPlayRequest = match serde_json::from_slice::<DirectPlayRequest>(body) {
        Ok(request) if !request.source_id.trim().is_empty() => request,
        _ => {
            return write_json(
                stream,
                400,
                &serde_json::json!({"success":false,"error":{"code":"INVALID_REQUEST","message":"Expected an object containing only a non-empty source_id.","retryable":false},"meta":{"request_id":request_id(),"route":"/api/v1/play/direct"}}),
                None,
            )
            .await;
        }
    };
    let source_id = request.source_id.trim();
    let source = match parse_source(source_id) {
        Ok(source) => source,
        Err((status, body)) => return write_raw_json(stream, status, &body, None).await,
    };
    if source.p == ProviderKind::MovieBox && !moviebox_provider_enabled() {
        return moviebox_disabled(stream).await;
    }
    let reference = OpaqueRef {
        p: source.p,
        v: source.v.clone(),
        s: source.s,
        e: source.e,
    };
    let started = Instant::now();
    let deadline = started + OVERALL_TIMEOUT;
    let mut selected_source_id = source_id.to_string();
    let mut selected_provider = source.p;
    let source_lookup_started = Instant::now();
    let remaining = deadline.saturating_duration_since(Instant::now());
    let original_fetch = if remaining.is_zero() {
        tokio::time::timeout(Duration::ZERO, cached_releases(&state, &reference)).await
    } else {
        tokio::time::timeout(remaining, cached_releases(&state, &reference)).await
    };
    log::info!(
        "direct_play_timing provider={} stage=source_lookup elapsed_ms={} outcome={}",
        source.p,
        source_lookup_started.elapsed().as_millis(),
        if matches!(&original_fetch, Ok(Ok(_))) {
            "ok"
        } else {
            "failed"
        }
    );
    let mut original_error = None;
    let mut original_resolution_error = None;
    let provider_resolution_started = Instant::now();
    let original_resolution = match original_fetch {
        Ok(Ok(releases)) => {
            let release = match source.f.as_deref() {
                Some(fingerprint) => releases.iter().find(|release| {
                    release_matches_episode(release, &source)
                        && release_fingerprint(release) == fingerprint
                }),
                None => releases
                    .iter()
                    .enumerate()
                    .find(|(index, release)| {
                        *index == source.i && release_matches_episode(release, &source)
                    })
                    .map(|(_, release)| release),
            };
            match release {
                Some(release) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    match tokio::time::timeout(
                        remaining,
                        resolve_valid_direct_source(
                            &state,
                            release,
                            source.content_type.as_deref(),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(resolved)) => Some(resolved),
                        Ok(Err(error)) => {
                            original_resolution_error = Some(error);
                            None
                        }
                        Err(_) => {
                            original_resolution_error = Some("resolution timed out".to_string());
                            None
                        }
                    }
                }
                None => {
                    original_resolution_error = Some("source not found".to_string());
                    None
                }
            }
        }
        Ok(Err(error)) => {
            original_error = Some(error);
            None
        }
        Err(_) => {
            original_resolution_error = Some("resolution timed out".to_string());
            None
        }
    };
    log::info!(
        "direct_play_timing provider={} stage=provider_resolution_and_verification elapsed_ms={} outcome={}",
        source.p,
        provider_resolution_started.elapsed().as_millis(),
        if original_resolution.is_some() {
            "ok"
        } else {
            "failed"
        }
    );
    let (playback, url_type, validated_url) = if let Some(resolved) = original_resolution {
        resolved
    } else {
        let fallback_started = Instant::now();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let fallback_attempt = tokio::time::timeout(
            remaining.min(DIRECT_FALLBACK_TIMEOUT),
            try_direct_fallback(&state, &source, deadline),
        )
        .await;
        let fallback_timed_out = fallback_attempt.is_err();
        let fallback = fallback_attempt.ok().flatten();
        log::info!(
            "direct_play_timing provider={} stage=fallback elapsed_ms={} outcome={}",
            source.p,
            fallback_started.elapsed().as_millis(),
            if fallback.is_some() {
                "ok"
            } else {
                "unavailable"
            }
        );
        let Some((resolved, fallback_source_id, fallback_provider)) = fallback else {
            if fallback_timed_out {
                let (status, response) = err(
                    "DIRECT_PLAY_RESOLUTION_TIMEOUT",
                    "Alternative provider playback resolution timed out.",
                    Some(source.p),
                    504,
                );
                return write_raw_json(stream, status, &response, None).await;
            }
            if let Some(error) = original_error {
                let (status, response) = provider_error(error, source.p);
                return write_raw_json(stream, status, &response, None).await;
            }
            let error = original_resolution_error.unwrap_or_else(|| "resolution failed".into());
            log::warn!(
                "direct_play_failure provider={} category={}",
                source.p,
                direct_resolution_category(&error)
            );
            let (code, message, status) = if error == "source not found" {
                ("SOURCE_NOT_FOUND", "The source was not found.", 404)
            } else if matches!(
                error.as_str(),
                "resolved URL rejected" | "direct playback compatibility unknown"
            ) {
                (
                    "DIRECT_URL_REJECTED",
                    "The resolved provider URL is not an allowed public HTTPS destination.",
                    403,
                )
            } else if error.contains("timed out") {
                (
                    "DIRECT_PLAY_RESOLUTION_TIMEOUT",
                    "Direct provider URL resolution timed out.",
                    504,
                )
            } else if error == "provider unavailable" {
                (
                    "DIRECT_PLAY_RESOLUTION_FAILED",
                    "The provider is unavailable.",
                    503,
                )
            } else if error == "no direct provider URL" {
                (
                    "DIRECT_PLAY_RESOLUTION_FAILED",
                    "The provider did not return a direct media URL.",
                    502,
                )
            } else {
                (
                    "DIRECT_PLAY_RESOLUTION_FAILED",
                    "The provider could not resolve a direct media URL.",
                    502,
                )
            };
            let (status, response) = err(code, message, Some(source.p), status);
            return write_raw_json(stream, status, &response, None).await;
        };
        selected_source_id = fallback_source_id;
        selected_provider = fallback_provider;
        resolved
    };
    let headers = direct_safe_headers(&playback.headers);
    let response_started = Instant::now();
    let response = serde_json::json!({
        "success": true,
        "data": {
            "mode": "direct",
            "source_id": selected_source_id,
            "playback_url": validated_url.to_string(),
            "url_type": url_type,
            "compatibility": direct_play_compatibility(selected_provider),
            "content_type": null,
            "expires_at": null,
            "requires_headers": !headers.is_empty(),
            "headers": headers,
            "provider": selected_provider.cache_key(),
            "message": if selected_provider == source.p { "Original provider playback URL resolved." } else { "Fallback provider playback URL resolved." }
        },
        "meta": {
            "request_id": request_id(),
            "resolution_latency_ms": started.elapsed().as_millis()
        }
    });
    let result = write_json(stream, 200, &response, None).await;
    log::info!(
        "direct_play_timing provider={} stage=response_assembly_and_write elapsed_ms={} total_elapsed_ms={}",
        selected_provider,
        response_started.elapsed().as_millis(),
        started.elapsed().as_millis()
    );
    result
}

async fn native_stream_route(
    stream: &mut TcpStream,
    path: &str,
    query: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    method: &str,
    head_only: bool,
) -> std::io::Result<()> {
    if !matches!(method, "GET" | "HEAD") {
        let (status, body) = err(
            "METHOD_NOT_ALLOWED",
            "Native media streaming supports GET and HEAD only.",
            None,
            405,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    let (Some(source_id), Some("native"), Some(token)) =
        (parts.get(3), parts.get(4).copied(), parts.get(5))
    else {
        return write_json(
            stream,
            404,
            &serde_json::json!({"error":"Native playback reference not found."}),
            None,
        )
        .await;
    };
    let source_id = match percent_encoding::percent_decode_str(source_id).decode_utf8() {
        Ok(source_id) if !source_id.is_empty() => source_id.into_owned(),
        _ => {
            return write_json(
                stream,
                400,
                &serde_json::json!({"error":"Invalid source ID."}),
                None,
            )
            .await;
        }
    };
    let mut suffix = Vec::new();
    for part in parts.iter().skip(6) {
        let decoded = match percent_encoding::percent_decode_str(part).decode_utf8() {
            Ok(decoded) if !decoded.is_empty() => decoded.into_owned(),
            _ => {
                return write_json(
                    stream,
                    400,
                    &serde_json::json!({"error":"Invalid media path."}),
                    None,
                )
                .await;
            }
        };
        suffix.push(decoded);
    }
    state
        .native_stream
        .serve(
            stream,
            &source_id,
            token,
            &suffix,
            query,
            request_headers,
            head_only,
        )
        .await
}

async fn direct_stream_route(
    stream: &mut TcpStream,
    path: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    method: &str,
    head_only: bool,
) -> std::io::Result<()> {
    if !matches!(method, "GET" | "HEAD") {
        let (status, body) = err(
            "METHOD_NOT_ALLOWED",
            "Direct media streaming supports GET and HEAD only.",
            None,
            405,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let parts = path.trim_matches('/').split('/').collect::<Vec<_>>();
    let (Some(source_id), Some("direct"), Some(token)) =
        (parts.get(3), parts.get(4).copied(), parts.get(5))
    else {
        return write_json(
            stream,
            404,
            &serde_json::json!({"error":"Direct playback reference not found."}),
            None,
        )
        .await;
    };
    if parts.len() != 6 {
        return write_json(
            stream,
            404,
            &serde_json::json!({"error":"Direct playback reference not found."}),
            None,
        )
        .await;
    }
    let source_id = match percent_encoding::percent_decode_str(source_id).decode_utf8() {
        Ok(source_id) if !source_id.is_empty() => source_id.into_owned(),
        _ => {
            return write_json(
                stream,
                400,
                &serde_json::json!({"error":"Invalid source ID."}),
                None,
            )
            .await;
        }
    };
    state
        .native_stream
        .serve_direct(stream, &source_id, token, request_headers, head_only)
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

async fn cached_playback_source(
    source_id: &str,
    release: &Release,
    state: &ApiState,
) -> Result<PlaybackSource, String> {
    if let Some(entry) = state.playback_cache.lock().await.get(source_id).cloned()
        && entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL
    {
        crate::observability::record_cache("playback", "hit");
        return Ok(entry.playback);
    }
    crate::observability::record_cache("playback", "miss");

    let key = source_id.to_string();
    let shared = {
        let mut inflight = state.playback_inflight.lock().await;
        if let Some(existing) = inflight.get(&key) {
            existing.clone()
        } else {
            let state = state.clone();
            let release = release.clone();
            let task = tokio::spawn(async move {
                if release.provider == ProviderKind::AniZone {
                    match tokio::time::timeout(
                        PLAYBACK_RESOLUTION_TIMEOUT,
                        state.service.anizone_client.resolve_release(&release),
                    )
                    .await
                    {
                        Ok(Ok(playback)) => Ok(playback),
                        Ok(Err(error)) => Err(error.to_string()),
                        Err(_) => Err("AniZone playback resolution timed out.".to_string()),
                    }
                } else if release.provider == ProviderKind::FourKHdHub {
                    let Some(client) = state.service.fourk_client.as_ref() else {
                        return Err("4KHDHub is unavailable.".to_string());
                    };
                    match tokio::time::timeout(
                        PLAYBACK_RESOLUTION_TIMEOUT,
                        client.resolve_release(
                            &release,
                            crate::providers::models::ResolutionIntent::Playback,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(playback)) => Ok(playback),
                        Ok(Err(error)) => Err(error.user_message().to_string()),
                        Err(_) => Err("4KHDHub playback resolution timed out.".to_string()),
                    }
                } else if let Some(url) = release.direct_url() {
                    Ok(PlaybackSource {
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
                    })
                } else {
                    Err("The source has no playable URL.".to_string())
                }
            });
            let future = async move {
                task.await
                    .unwrap_or_else(|_| Err("Playback resolver task failed.".to_string()))
            }
            .boxed()
            .shared();
            inflight.insert(key.clone(), future.clone());
            future
        }
    };

    let resolved = shared.await;
    if let Ok(playback) = &resolved {
        let mut cache = state.playback_cache.lock().await;
        if cache.len() >= 2048 {
            cache.retain(|_, entry| entry.stored_at.elapsed() < PLAYBACK_CACHE_TTL);
        }
        cache.insert(
            key.clone(),
            PlaybackCacheEntry {
                stored_at: Instant::now(),
                playback: playback.clone(),
            },
        );
        crate::observability::record_cache("playback", "insert");
    }
    // Make the completed value visible before the shared future is removed.
    state.playback_inflight.lock().await.remove(&key);
    resolved
}

async fn write_verified_playback_response(
    stream: &mut TcpStream,
    source_id: &str,
    release: &Release,
    state: &ApiState,
    provider: ProviderKind,
) -> std::io::Result<()> {
    let playback = match cached_playback_source(source_id, release, state).await {
        Ok(playback) => playback,
        Err(message) => {
            let timed_out = message.to_ascii_lowercase().contains("timed out");
            let (status, body) = err(
                if timed_out {
                    "PLAYBACK_RESOLUTION_TIMEOUT"
                } else {
                    "PLAYBACK_RESOLUTION_FAILED"
                },
                message,
                Some(provider),
                if timed_out { 504 } else { 502 },
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let _media_job = match state.remux_cache.try_acquire_job() {
        Ok(permit) => permit,
        Err(_) => {
            let (status, body) = err(
                "REMUX_BUSY",
                "Media-processing capacity is busy.",
                Some(provider),
                429,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    if state.native_stream.supports_url(&playback.url) {
        return match crate::codec_verification::verify_playback_source(&playback).await {
            Ok(crate::codec_verification::CodecProbeResult::Allowed(_)) => {
                let mut backend_playback = playback;
                backend_playback.url = source_playback_url(
                    state,
                    source_id,
                    &backend_playback.url,
                    &backend_playback.headers,
                );
                backend_playback.headers.clear();
                write_json(stream, 200, &playback_json(&backend_playback), None).await
            }
            Ok(crate::codec_verification::CodecProbeResult::BlockedHevc(_)) => {
                let (status, body) = err(
                    "SOURCE_CODEC_BLOCKED",
                    "H.265/HEVC sources are blocked by playback policy.",
                    Some(provider),
                    415,
                );
                write_raw_json(stream, status, &body, None).await
            }
            Ok(crate::codec_verification::CodecProbeResult::Unsupported(_))
            | Ok(crate::codec_verification::CodecProbeResult::Unverified(_))
            | Ok(crate::codec_verification::CodecProbeResult::NoVideoStream)
            | Err(_) => {
                let (status, body) = err(
                    "CODEC_UNVERIFIED",
                    "The media video codec could not be positively verified as H.264.",
                    Some(provider),
                    415,
                );
                write_raw_json(stream, status, &body, None).await
            }
        };
    }
    let evidence = match crate::codec_verification::inspect_direct_playback_source(&playback).await
    {
        Ok(evidence) => evidence,
        Err(_) => {
            let (status, body) = err(
                "CODEC_UNVERIFIED",
                "The media video codec could not be positively verified as H.264.",
                Some(provider),
                415,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let mut backend_playback = playback;
    match evidence.codec {
        crate::codec_verification::CodecProbeResult::Allowed(codec) => {
            let capabilities = crate::playback_planner::VerifiedMediaCapabilities {
                format_name: evidence.format_name,
                video_codec: Some(codec),
                audio_codecs: evidence.audio_codecs,
                range_supported: evidence.range_supported,
            };
            match crate::playback_planner::plan_playback(&backend_playback.url, Some(&capabilities))
            {
                crate::playback_planner::PlaybackStrategy::DirectRangePassthrough => {
                    backend_playback.url = state
                        .native_stream
                        .direct_playback_url(
                            source_id,
                            &backend_playback.url,
                            &backend_playback.headers,
                        )
                        .unwrap_or_else(|| {
                            source_playback_url(
                                state,
                                source_id,
                                &backend_playback.url,
                                &backend_playback.headers,
                            )
                        });
                }
                crate::playback_planner::PlaybackStrategy::RemuxRequired => {
                    backend_playback.url = source_playback_url(
                        state,
                        source_id,
                        &backend_playback.url,
                        &backend_playback.headers,
                    );
                }
                _ => {
                    let (status, body) = err(
                        "CODEC_UNVERIFIED",
                        "The media format or playback capabilities could not be positively verified.",
                        Some(provider),
                        415,
                    );
                    return write_raw_json(stream, status, &body, None).await;
                }
            }
            backend_playback.headers.clear();
            write_json(stream, 200, &playback_json(&backend_playback), None).await
        }
        crate::codec_verification::CodecProbeResult::BlockedHevc(_) => {
            let (status, body) = err(
                "SOURCE_CODEC_BLOCKED",
                "H.265/HEVC sources are blocked by playback policy.",
                Some(provider),
                415,
            );
            write_raw_json(stream, status, &body, None).await
        }
        crate::codec_verification::CodecProbeResult::Unsupported(_)
        | crate::codec_verification::CodecProbeResult::Unverified(_)
        | crate::codec_verification::CodecProbeResult::NoVideoStream => {
            let (status, body) = err(
                "CODEC_UNVERIFIED",
                "The media video codec could not be positively verified as H.264.",
                Some(provider),
                415,
            );
            write_raw_json(stream, status, &body, None).await
        }
    }
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

fn validate_transcode_url(raw: &str) -> Option<reqwest::Url> {
    let url = crate::net::validate_url_syntax(raw, false).ok()?;
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
    Some(url)
}

async fn validate_transcode_destination(raw: &str) -> Option<reqwest::Url> {
    let url = validate_transcode_url(raw)?;
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
    found.then_some(url)
}

fn is_public_transcode_ip(address: std::net::IpAddr) -> bool {
    crate::net::is_public_ip(address)
}

fn direct_transcode_playback_source(source: &Release) -> Option<PlaybackSource> {
    if source_codec_eligibility(source.codec.as_deref()) != SourceCodecEligibility::Verified {
        return None;
    }
    if source.provider == ProviderKind::FourKHdHub {
        return None;
    }
    let (mirror, url) = source.mirrors.iter().find_map(|mirror| {
        mirror
            .direct_file
            .then(|| validate_transcode_url(&mirror.resolver_url))
            .flatten()
            .map(|url| (mirror, url))
    })?;
    Some(PlaybackSource {
        provider: source.provider,
        url: url.to_string(),
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
    if let Some((code, message)) = codec_ineligibility_error(std::slice::from_ref(source)) {
        return Err((code, message, 415));
    }
    if source.provider != ProviderKind::FourKHdHub {
        return direct_transcode_playback_source(source).ok_or((
            "TRANSCODE_UNAVAILABLE",
            "This source has no validated direct media URL for transcoding.",
            501,
        ));
    }

    cached_playback_source(source_id, source, state)
        .await
        .map_err(|message| {
            if message.to_ascii_lowercase().contains("timed out") {
                (
                    "TRANSCODE_RESOLUTION_TIMEOUT",
                    "4KHDHub media resolution timed out.",
                    504,
                )
            } else {
                (
                    "TRANSCODE_RESOLUTION_FAILED",
                    "4KHDHub could not resolve a playable media source for transcoding.",
                    502,
                )
            }
        })
}

async fn validated_remux_preflight(url: &reqwest::Url, headers: &[(String, String)]) -> bool {
    // Provider codec labels are often incomplete or inferred. Try a short real stream-copy
    // into browser-compatible fragmented MP4, then verify the resulting video/audio codecs
    // before allowing stream-copy mode.
    let Ok(source_probe) =
        tokio::time::timeout(REMUX_PREFLIGHT_TIMEOUT, probe_media_validated(url, headers)).await
    else {
        // Metadata inspection is best-effort; unknown metadata does not itself rule out remux.
        return remux_preflight_output_valid(url.as_str(), headers).await;
    };
    if let Ok(probe) = source_probe
        && !probe
            .streams
            .iter()
            .any(|stream| stream.codec_type.as_deref() == Some("video"))
    {
        return false;
    }
    remux_preflight_output_valid(url.as_str(), headers).await
}

async fn remux_preflight_output_valid(url: &str, headers: &[(String, String)]) -> bool {
    const MAX_PREFLIGHT_BYTES: usize = 32 * 1024 * 1024;
    let mut command = Command::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-nostdin"]);
    let header_text = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    if !header_text.is_empty() {
        command.arg("-headers").arg(header_text);
    }
    command.args([
        "-rw_timeout",
        "15000000",
        "-i",
        url,
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-ac",
        "2",
        "-sn",
        "-dn",
        "-t",
        "2",
        "-movflags",
        "frag_keyframe+empty_moov+default_base_moof",
        "-f",
        "mp4",
        "pipe:1",
    ]);
    let mut child = match command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    let Some(stdout) = child.stdout.take() else {
        return false;
    };
    let mut remuxed_bytes = Vec::new();
    let remux_status = tokio::time::timeout(REMUX_PREFLIGHT_TIMEOUT, async {
        let bytes_read = stdout
            .take(MAX_PREFLIGHT_BYTES as u64 + 1)
            .read_to_end(&mut remuxed_bytes)
            .await?;
        if bytes_read > MAX_PREFLIGHT_BYTES {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Ok::<bool, std::io::Error>(false);
        }
        let status = child.wait().await?;
        Ok(status.success() && !remuxed_bytes.is_empty())
    })
    .await;
    if !matches!(remux_status, Ok(Ok(true))) {
        return false;
    }

    let mut probe = Command::new("ffprobe");
    let mut probe_child = match probe
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=index,codec_name,codec_type",
            "-of",
            "json",
            "-i",
            "pipe:0",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    let Some(mut probe_stdin) = probe_child.stdin.take() else {
        return false;
    };
    let probe_result = tokio::time::timeout(REMUX_PREFLIGHT_TIMEOUT, async move {
        let (write_result, output_result) = tokio::join!(
            async { probe_stdin.write_all(&remuxed_bytes).await },
            probe_child.wait_with_output()
        );
        (write_result, output_result)
    })
    .await;
    let Ok((Ok(()), Ok(output))) = probe_result else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    serde_json::from_slice::<ProbeOutput>(&output.stdout)
        .is_ok_and(|probe| browser_mp4_streams_compatible(&probe.streams))
}

fn browser_mp4_streams_compatible(streams: &[ProbeStream]) -> bool {
    let video = streams
        .iter()
        .find(|stream| stream.codec_type.as_deref() == Some("video"));
    let audio = streams
        .iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
        .collect::<Vec<_>>();
    video.is_some_and(|stream| stream.codec_name.as_deref() == Some("h264"))
        && audio
            .iter()
            .all(|stream| stream.codec_name.as_deref() == Some("aac"))
}

const REMUX_OUTPUT_PROFILE: &str = "mp4-faststart-h264-copy-aac-192k-stereo-v1";

async fn has_faststart_mp4_index(path: &std::path::Path) -> std::io::Result<bool> {
    use tokio::io::AsyncSeekExt;

    let mut file = tokio::fs::File::open(path).await?;
    let length = file.metadata().await?.len();
    let mut offset = 0u64;
    let mut saw_ftyp = false;
    let mut saw_moov = false;
    for _ in 0..256 {
        if offset.saturating_add(8) > length {
            return Ok(false);
        }
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut header = [0u8; 16];
        file.read_exact(&mut header[..8]).await?;
        let size32 = u32::from_be_bytes(header[..4].try_into().unwrap()) as u64;
        let atom_type: [u8; 4] = header[4..8].try_into().unwrap();
        let (atom_size, header_size) = match size32 {
            0 => (length - offset, 8u64),
            1 => {
                if offset.saturating_add(16) > length {
                    return Ok(false);
                }
                file.read_exact(&mut header[8..16]).await?;
                (u64::from_be_bytes(header[8..16].try_into().unwrap()), 16u64)
            }
            size => (size, 8u64),
        };
        if atom_size < header_size || offset.saturating_add(atom_size) > length {
            return Ok(false);
        }
        match &atom_type {
            b"ftyp" => saw_ftyp = true,
            b"moov" => saw_moov = true,
            b"mdat" => return Ok(saw_ftyp && saw_moov),
            _ => {}
        }
        offset += atom_size;
    }
    Ok(false)
}

async fn build_cached_mp4(
    cache: crate::remux_cache::RemuxCache,
    cache_key: String,
    url: reqwest::Url,
    headers: Vec<(String, String)>,
    partial_path: std::path::PathBuf,
    verify_input_codec: bool,
    run_preflight: bool,
) -> Result<f64, crate::remux_cache::CacheError> {
    use crate::remux_cache::CacheError;
    let deadline = Instant::now() + cache.job_timeout();
    let job_deadline = tokio::time::Instant::from_std(deadline);

    if verify_input_codec {
        let referer = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("referer"))
            .map(|(_, value)| value.as_str());
        match tokio::time::timeout_at(job_deadline, verify_generic_media_codec(&url, referer))
            .await
            .map_err(|_| CacheError::Build("media codec probe timed out".into()))?
        {
            Ok(SourceCodecEligibility::Verified) => {}
            Ok(SourceCodecEligibility::BlockedH265) => return Err(CacheError::BlockedCodec),
            Ok(SourceCodecEligibility::Unverified) | Err(_) => {
                return Err(CacheError::UnverifiedCodec);
            }
        }
    }
    if run_preflight {
        let preflight =
            tokio::time::timeout_at(job_deadline, validated_remux_preflight(&url, &headers))
                .await
                .map_err(|_| CacheError::Build("media remux preflight timed out".into()))?;
        if !preflight {
            log::warn!("remux preflight was inconclusive; continuing with full-output validation");
        }
    }
    let mut command = Command::new("ffmpeg");
    command.args(["-hide_banner", "-loglevel", "error", "-nostdin", "-y"]);
    if !headers.is_empty() {
        let header_text = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        command.arg("-headers").arg(header_text);
    }
    command
        .args([
            "-rw_timeout",
            "120000000",
            "-i",
            url.as_str(),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ac",
            "2",
            "-sn",
            "-dn",
            "-max_interleave_delta",
            "0",
            "-movflags",
            "+faststart",
            "-f",
            "mp4",
        ])
        .arg(&partial_path);
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| CacheError::Build("FFmpeg is unavailable or could not be started".into()))?;
    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut sink = tokio::io::sink();
            let _ = tokio::io::copy(&mut stderr, &mut sink).await;
        })
    });
    let exit_status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(error) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                if let Some(task) = stderr_task {
                    let _ = task.await;
                }
                return Err(CacheError::Io(error));
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            return Err(CacheError::Build("media remux job timed out".into()));
        }
        if let Ok(metadata) = tokio::fs::metadata(&partial_path).await
            && (metadata.len() > cache.max_bytes()
                || cache
                    .report_partial_bytes(&cache_key, metadata.len())
                    .await
                    .is_err())
        {
            let _ = child.kill().await;
            let _ = child.wait().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            return Err(CacheError::BudgetExceeded);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    if !exit_status.success() {
        return Err(CacheError::Build(
            "the upstream media could not be remuxed as a seekable MP4".into(),
        ));
    }
    let has_faststart =
        tokio::time::timeout_at(job_deadline, has_faststart_mp4_index(&partial_path))
            .await
            .map_err(|_| CacheError::Build("MP4 index validation timed out".into()))?
            .map_err(CacheError::Io)?;
    if !has_faststart {
        return Err(CacheError::Build(
            "FFmpeg output is not a complete fast-start MP4".into(),
        ));
    }
    let mut probe = Command::new("ffprobe");
    probe
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=format_name,duration:stream=codec_name,codec_type",
            "-of",
            "json",
        ])
        .arg(&partial_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout_at(job_deadline, probe.output())
        .await
        .map_err(|_| CacheError::Build("FFprobe timed out while validating remux output".into()))?
        .map_err(CacheError::Io)?;
    if !output.status.success() {
        return Err(CacheError::Build(
            "FFprobe could not validate the remux output".into(),
        ));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|_| CacheError::Build("FFprobe returned malformed remux metadata".into()))?;
    let formats = metadata
        .pointer("/format/format_name")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if !formats.split(',').any(|format| format == "mp4") {
        return Err(CacheError::Build(
            "FFprobe did not identify the remux output as MP4".into(),
        ));
    }
    let duration = metadata
        .pointer("/format/duration")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|duration| duration.is_finite() && *duration > 0.0)
        .ok_or_else(|| CacheError::Build("verified media duration is unavailable".into()))?;
    let streams = metadata
        .get("streams")
        .cloned()
        .and_then(|streams| serde_json::from_value::<Vec<ProbeStream>>(streams).ok())
        .ok_or_else(|| CacheError::Build("FFprobe stream metadata is unavailable".into()))?;
    if !browser_mp4_streams_compatible(&streams) {
        return Err(CacheError::Build(
            "remux output codecs do not satisfy the verified playback policy".into(),
        ));
    }
    Ok(duration)
}

fn cache_error_http(error: crate::remux_cache::CacheError) -> (u16, &'static str, String) {
    use crate::remux_cache::CacheError;
    match error {
        CacheError::Busy => (
            429,
            "REMUX_BUSY",
            "Media-processing capacity is busy.".into(),
        ),
        CacheError::TooLarge | CacheError::BudgetExceeded => (
            507,
            "MEDIA_CACHE_FULL",
            "Temporary media cache capacity is unavailable.".into(),
        ),
        CacheError::InvalidKey => (400, "INVALID_ID", "The source identity is invalid.".into()),
        CacheError::BlockedCodec => (
            415,
            "SOURCE_CODEC_BLOCKED",
            "H.265/HEVC sources are blocked by playback policy.".into(),
        ),
        CacheError::UnverifiedCodec => (
            415,
            "CODEC_UNVERIFIED",
            "The media video codec could not be positively verified as H.264.".into(),
        ),
        CacheError::Build(message) if message.contains("timed out") => (
            504,
            "REMUX_TIMEOUT",
            "Media preparation exceeded its configured time limit.".into(),
        ),
        CacheError::Build(_) => (
            502,
            "REMUX_FAILED",
            "The selected source could not be prepared as seekable MP4.".into(),
        ),
        CacheError::Io(_) => (
            500,
            "MEDIA_CACHE_IO_ERROR",
            "Temporary media storage failed.".into(),
        ),
    }
}

async fn write_cached_remux_response(
    stream: &mut TcpStream,
    lease: crate::remux_cache::CacheLease,
    source_id: Option<&str>,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    use crate::remux_cache::{ByteRange, copy_range_to_writer, parse_range_header};

    let size = lease.cached.metadata.size_bytes;
    let source_header = source_id
        .map(|id| format!("X-Source-Id: {id}\r\n"))
        .unwrap_or_default();
    let (status, start, length, content_range) = match parse_range_header(
        request_headers.get("range").map(String::as_str),
        size,
    ) {
        ByteRange::Full => (200, 0, size, None),
        ByteRange::Partial { start, end } => {
            let length = end - start + 1;
            (
                206,
                start,
                length,
                Some(format!("Content-Range: bytes {start}-{end}/{size}\r\n")),
            )
        }
        ByteRange::Unsatisfiable => {
            let extra = format!(
                "Accept-Ranges: bytes\r\nContent-Range: bytes */{size}\r\nContent-Length: 0\r\nX-Media-Mode: remux\r\n{source_header}X-Remux-Cache-State: {}\r\n",
                if lease.was_hit { "hit" } else { "built" }
            );
            return write_headers(stream, 416, "video/mp4", Some(&extra)).await;
        }
    };
    let content_range = content_range.unwrap_or_default();
    let extra = format!(
        "Accept-Ranges: bytes\r\nContent-Length: {length}\r\n{content_range}X-Media-Mode: remux\r\n{source_header}X-Media-Duration: {:.3}\r\nX-Remux-Cache-State: {}\r\n",
        lease.cached.metadata.duration_seconds,
        if lease.was_hit { "hit" } else { "built" }
    );
    write_headers(stream, status, "video/mp4", Some(&extra)).await?;
    if head_only {
        return Ok(());
    }
    copy_range_to_writer(&lease.cached.path, start, length, stream).await
}

async fn transcode_source(
    stream: &mut TcpStream,
    source_id: &str,
    source: &crate::providers::models::Release,
    state: &ApiState,
    query: &str,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
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
    if head_only {
        let (status, body) = err(
            "METHOD_NOT_ALLOWED",
            "Remuxing supports GET only.",
            Some(source.provider),
            405,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    let seek_seconds = match parse_remux_seek_seconds(query) {
        Ok(value) => value,
        Err((status, code, message)) => {
            let (status, body) = err(code, message, Some(source.provider), status);
            return write_raw_json(stream, status, &body, Some("Accept-Ranges: none\r\n")).await;
        }
    };
    if request_headers.contains_key("range") {
        let (status, body) = err(
            "SEEK_UNAVAILABLE",
            "Byte-range seeking is unavailable for progressive remuxed media.",
            None,
            501,
        );
        return write_raw_json(
            stream,
            status,
            &body,
            Some("Accept-Ranges: none\r\nX-Remux-Cache-State: disabled\r\n"),
        )
        .await;
    }
    let _media_job = match state.remux_cache.try_acquire_job() {
        Ok(permit) => permit,
        Err(_) => {
            let (status, body) = err(
                "REMUX_BUSY",
                "Media-processing capacity is busy.",
                Some(source.provider),
                429,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    let debug_id = REMUX_DEBUG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    if let Some(seek_seconds) = seek_seconds {
        match crate::codec_verification::inspect_direct_playback_source(&playback).await {
            Ok(evidence) if evidence.range_supported => {
                crate::progressive_remux::serve_from_timestamp(
                    stream,
                    &validated_url,
                    &playback.headers,
                    seek_seconds,
                    state.remux_cache.job_timeout(),
                    debug_id,
                )
                .await
            }
            Ok(_) => {
                let (status, body) = err(
                    "SEEK_UNAVAILABLE",
                    "Timestamp seeking requires a verified upstream byte-range source.",
                    Some(source.provider),
                    501,
                );
                write_raw_json(stream, status, &body, Some("Accept-Ranges: none\r\n")).await
            }
            Err(_) => {
                let (status, body) = err(
                    "SEEK_UNAVAILABLE",
                    "The upstream source could not be verified for timestamp seeking.",
                    Some(source.provider),
                    501,
                );
                write_raw_json(stream, status, &body, Some("Accept-Ranges: none\r\n")).await
            }
        }
    } else {
        crate::progressive_remux::serve(
            stream,
            &validated_url,
            &playback.headers,
            state.remux_cache.job_timeout(),
            debug_id,
        )
        .await
    }
}

fn parse_remux_seek_seconds(query: &str) -> Result<Option<f64>, (u16, &'static str, &'static str)> {
    let Some(raw) = query_value(query, "start_seconds").or_else(|| query_value(query, "start"))
    else {
        return Ok(None);
    };
    let value = raw.parse::<f64>().map_err(|_| {
        (
            400,
            "INVALID_SEEK",
            "The timestamp seek position must be a finite number of seconds.",
        )
    })?;
    if !value.is_finite() || !(0.0..=86_400.0).contains(&value) {
        return Err((
            400,
            "INVALID_SEEK",
            "The timestamp seek position must be between 0 and 86400 seconds.",
        ));
    }
    Ok(Some(value))
}

const MEDIA_BODY_LIMIT: usize = 8 * 1024 * 1024;

fn query_value(query: &str, key: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find_map(|(name, value)| (name == key).then(|| value.into_owned()))
}

async fn validated_media_url(query: &str) -> Result<(reqwest::Url, Option<String>), (u16, String)> {
    let raw =
        query_value(query, "url").ok_or((400, "The url query parameter is required.".into()))?;
    let url = crate::net::validate_public_url(&raw, true)
        .await
        .map_err(|_| {
            (
                403,
                "The requested media URL is not an allowed public destination.".into(),
            )
        })?;
    let referer = query_value(query, "referer").and_then(|value| {
        crate::net::validate_url_syntax(&value, true)
            .ok()
            .map(|url| url.to_string())
    });
    Ok((url, referer))
}

fn media_request_client() -> reqwest::Client {
    crate::net::http_client_builder()
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

async fn stream_proxy_route(
    stream: &mut TcpStream,
    query: &str,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    let (url, referer) = match validated_media_url(query).await {
        Ok(value) => value,
        Err((status, message)) => {
            let (status, body) = err(
                if status == 403 {
                    "SSRF_BLOCKED"
                } else {
                    "INVALID_MEDIA_URL"
                },
                message,
                None,
                status,
            );
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    remux_media_url(
        stream,
        &url,
        referer.as_deref(),
        state,
        request_headers,
        head_only,
    )
    .await
}

fn sanitize_error(error: &str) -> String {
    error.replace('"', "'").replace(['\r', '\n'], " ")
}

fn is_seeking_range(request_headers: &HashMap<String, String>) -> bool {
    let Some(range) = request_headers.get("range") else {
        return false;
    };
    let Some((unit, spec)) = range.trim().split_once('=') else {
        return true;
    };
    if !unit.eq_ignore_ascii_case("bytes") {
        return false;
    }
    let Some((start, end)) = spec.split_once('-') else {
        return true;
    };
    if start.is_empty() {
        return true;
    }
    match start.parse::<u64>() {
        Ok(0) => !end.is_empty(),
        Ok(_) | Err(_) => true,
    }
}

async fn stream_ffmpeg_remux(
    stream: &mut TcpStream,
    mut child: tokio::process::Child,
    response_extra: &str,
    debug_id: u64,
) -> std::io::Result<()> {
    let Some(mut stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        return Err(std::io::Error::other("FFmpeg did not open remux output"));
    };
    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];
            while stderr.read(&mut buffer).await.unwrap_or(0) > 0 {}
        })
    });

    let startup = tokio::time::timeout(REMUX_STARTUP_TIMEOUT, async {
        let mut buffered = Vec::with_capacity(128 * 1024);
        let mut chunk = [0u8; 32 * 1024];
        loop {
            let length = stdout.read(&mut chunk).await?;
            if length == 0 {
                return Err(std::io::Error::other(
                    "FFmpeg ended before the first fragmented MP4 media atom",
                ));
            }
            buffered.extend_from_slice(&chunk[..length]);
            if buffered.windows(4).any(|atom| atom == b"moof") {
                return Ok(buffered);
            }
            if buffered.len() > 4 * 1024 * 1024 {
                return Err(std::io::Error::other(
                    "FFmpeg did not produce a fragmented MP4 startup segment",
                ));
            }
        }
    })
    .await;
    let first = match startup {
        Ok(Ok(buffered)) => buffered,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            log::warn!("remux startup failed id={debug_id}: {error}");
            write_raw_json(
                stream,
                502,
                r#"{"error":{"code":"REMUX_STARTUP_FAILED","message":"The media source could not be prepared for playback."}}"#,
                Some("X-Media-Mode: remux\r\nX-Remux-Cache-State: disabled\r\n"),
            )
            .await?;
            return Ok(());
        }
        Err(_) => {
            let _ = child.kill().await;
            if let Some(task) = stderr_task {
                let _ = task.await;
            }
            log::warn!("remux startup timed out id={debug_id}");
            write_raw_json(
                stream,
                504,
                r#"{"error":{"code":"REMUX_STARTUP_TIMEOUT","message":"The media source took too long to start."}}"#,
                Some("X-Media-Mode: remux\r\nX-Remux-Cache-State: disabled\r\n"),
            )
            .await?;
            return Ok(());
        }
    };
    if let Err(error) = write_headers(stream, 200, "video/mp4", Some(response_extra)).await {
        let _ = child.kill().await;
        let _ = child.wait().await;
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        return Err(error);
    }

    if let Err(error) = stream.write_all(&first).await {
        let _ = child.kill().await;
        let _ = child.wait().await;
        if let Some(task) = stderr_task {
            let _ = task.await;
        }
        return Err(error);
    }

    let mut read_error = None;
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let length = match stdout.read(&mut chunk).await {
            Ok(0) => break,
            Ok(length) => length,
            Err(error) => {
                read_error = Some(error);
                break;
            }
        };
        if let Err(error) = stream.write_all(&chunk[..length]).await {
            read_error = Some(error);
            let _ = child.kill().await;
            break;
        }
    }
    if read_error.is_some() {
        let _ = child.kill().await;
    }
    let status = match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(Ok(status)) => Some(status),
        _ => {
            let _ = child.kill().await;
            None
        }
    };
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    if !matches!(status, Some(status) if status.success()) || read_error.is_some() {
        return Err(read_error.unwrap_or_else(|| std::io::Error::other("FFmpeg remux failed")));
    }
    Ok(())
}

async fn legacy_remux_media_url(
    stream: &mut TcpStream,
    url: &reqwest::Url,
    referer: Option<&str>,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    let debug_id = REMUX_DEBUG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let upstream_host = url.host_str().unwrap_or("unknown-host");
    let started_at = Instant::now();
    let mut upstream_headers = vec![("User-Agent".to_string(), "Mozilla/5.0".to_string())];
    if let Some(referer) = referer {
        upstream_headers.push(("Referer".to_string(), referer.to_string()));
    }
    let range_requested = is_seeking_range(request_headers);
    if head_only {
        let (status, body) = err(
            "METHOD_NOT_ALLOWED",
            "Remuxing supports GET only; HEAD is unavailable without disk-backed output.",
            None,
            405,
        );
        return write_raw_json(stream, status, &body, None).await;
    }
    if range_requested {
        let (status, body) = err(
            "SEEK_UNAVAILABLE",
            "Byte-range seeking is disabled because remuxed media is streamed without disk caching.",
            None,
            501,
        );
        return write_raw_json(
            stream,
            status,
            &body,
            Some("X-Remux-Cache-State: disabled\r\n"),
        )
        .await;
    }
    let permit = match REMUX_LIMITER.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return write_raw_json(
                stream,
                429,
                r#"{"error":{"code":"REMUX_BUSY","message":"Remux capacity is busy."}}"#,
                None,
            )
            .await;
        }
    };

    log::info!(
        "remux start id={debug_id} upstream_host={upstream_host} referer_present={} cache_enabled=false",
        referer.is_some()
    );
    let mut command = Command::new("ffmpeg");
    command.args([
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-probesize",
        "5M",
        "-analyzeduration",
        "5M",
        "-fflags",
        "+nobuffer",
    ]);
    let header_text = upstream_headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    command.arg("-headers").arg(header_text);
    command.args([
        "-rw_timeout",
        "120000000",
        "-i",
        url.as_str(),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-ac",
        "2",
        "-sn",
        "-dn",
        "-max_interleave_delta",
        "0",
        "-movflags",
        "+frag_keyframe+empty_moov+default_base_moof",
        "-flush_packets",
        "1",
        "-f",
        "mp4",
        "pipe:1",
    ]);
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    let child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            drop(permit);
            log::error!("remux ffmpeg spawn failed id={debug_id}: {error}");
            return write_raw_json(
                stream,
                503,
                r#"{"error":{"code":"REMUX_UNAVAILABLE","message":"FFmpeg is unavailable."}}"#,
                None,
            )
            .await;
        }
    };
    let extra = "Accept-Ranges: none\r\nX-Media-Mode: remux\r\nX-Remux-Cache-State: disabled\r\n";
    let result = stream_ffmpeg_remux(stream, child, extra, debug_id).await;
    drop(permit);
    log::info!(
        "remux response complete id={debug_id} elapsed_ms={}",
        started_at.elapsed().as_millis()
    );
    result
}

fn generic_media_identity(url: &reqwest::Url, referer: Option<&str>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"moviebox-generic-media-v1\0");
    digest.update(url.as_str().as_bytes());
    digest.update([0]);
    if let Some(referer) = referer {
        digest.update(referer.as_bytes());
    }
    URL_SAFE_NO_PAD.encode(digest.finalize())
}

async fn remux_media_url(
    stream: &mut TcpStream,
    url: &reqwest::Url,
    referer: Option<&str>,
    state: ApiState,
    request_headers: &HashMap<String, String>,
    head_only: bool,
) -> std::io::Result<()> {
    let identity = generic_media_identity(url, referer);
    let key = crate::remux_cache::cache_key(&identity, REMUX_OUTPUT_PROFILE);
    let mut upstream_headers = vec![("User-Agent".to_string(), "Mozilla/5.0".to_string())];
    if let Some(referer) = referer {
        upstream_headers.push(("Referer".to_string(), referer.to_string()));
    }
    let build_cache = state.remux_cache.clone();
    let build_key = key.clone();
    let build_url = url.clone();
    let build_headers = upstream_headers;
    let started = Instant::now();
    let lease = state
        .remux_cache
        .get_or_build(&key, move |partial_path| async move {
            build_cached_mp4(
                build_cache,
                build_key,
                build_url,
                build_headers,
                partial_path,
                true,
                false,
            )
            .await
        })
        .await;
    let lease = match lease {
        Ok(lease) => lease,
        Err(error) => {
            let (status, code, message) = cache_error_http(error);
            let (status, body) = err(code, message, None, status);
            return write_raw_json(stream, status, &body, None).await;
        }
    };
    log::info!(
        "generic media remux prepared source_fingerprint={} cache_state={} bytes={} elapsed_ms={}",
        &key[..12],
        if lease.was_hit { "hit" } else { "built" },
        lease.cached.metadata.size_bytes,
        started.elapsed().as_millis()
    );
    write_cached_remux_response(stream, lease, None, request_headers, head_only).await
}

async fn verify_generic_media_codec(
    url: &reqwest::Url,
    referer: Option<&str>,
) -> Result<SourceCodecEligibility, MediaInfoProbeError> {
    let mut command = Command::new("ffprobe");
    command.args([
        "-hide_banner",
        "-v",
        "error",
        "-rw_timeout",
        "15000000",
        "-show_entries",
        "stream=codec_type,codec_name",
        "-of",
        "json",
    ]);
    if let Some(referer) = referer {
        command
            .arg("-headers")
            .arg(format!("Referer: {referer}\r\n"));
    }
    command.arg(url.as_str());
    let output = run_media_info_probe(&mut command, Duration::from_secs(20)).await?;
    let probe: ProbeOutput =
        serde_json::from_slice(&output.stdout).map_err(|_| MediaInfoProbeError::Failed)?;
    let video_codecs = probe
        .streams
        .iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("video"))
        .filter_map(|stream| stream.codec_name.as_deref())
        .collect::<Vec<_>>();
    if video_codecs.is_empty() {
        return Ok(SourceCodecEligibility::Unverified);
    }
    if video_codecs
        .iter()
        .any(|codec| crate::codec_verification::is_hevc_codec(codec))
    {
        return Ok(SourceCodecEligibility::BlockedH265);
    }
    if video_codecs
        .iter()
        .all(|codec| crate::codec_verification::is_allowed_codec(codec))
    {
        Ok(SourceCodecEligibility::Verified)
    } else {
        Ok(SourceCodecEligibility::Unverified)
    }
}

async fn media_info_route(
    stream: &mut TcpStream,
    query: &str,
    _request_headers: &HashMap<String, String>,
) -> std::io::Result<()> {
    let (url, referer) = match validated_media_url(query).await {
        Ok(value) => value,
        Err((status, message)) => {
            let code = if status == 400 {
                "INVALID_REQUEST"
            } else {
                "SSRF_BLOCKED"
            };
            return write_raw_json(
                stream,
                status,
                &format!(
                    r#"{{"error":{{"code":"{}","message":"{}"}}}}"#,
                    code, message
                ),
                None,
            )
            .await;
        }
    };
    let mut command = Command::new("ffprobe");
    command.args(["-v", "error", "-rw_timeout", "15000000"]);
    if let Some(referer) = referer {
        command
            .arg("-headers")
            .arg(format!("Referer: {referer}\r\n"));
    }
    command.args(["-show_streams", "-show_format", "-of", "json", url.as_str()]);
    let output = match run_media_info_probe(&mut command, Duration::from_secs(45)).await {
        Ok(output) => output,
        Err(MediaInfoProbeError::Failed) => {
            return write_raw_json(stream, 502, r#"{"error":{"code":"MEDIA_INFO_FAILED","message":"FFprobe could not inspect the media."}}"#, None).await;
        }
        Err(MediaInfoProbeError::Spawn(_)) => {
            return write_raw_json(stream, 503, r#"{"error":{"code":"MEDIA_TOOL_UNAVAILABLE","message":"FFprobe is unavailable."}}"#, None).await;
        }
        Err(MediaInfoProbeError::Timeout) => {
            return write_raw_json(stream, 504, r#"{"error":{"code":"MEDIA_INFO_TIMEOUT","message":"Media inspection timed out."}}"#, None).await;
        }
    };
    write_headers(stream, 200, "application/json", None).await?;
    stream.write_all(&output.stdout).await
}

#[derive(Debug)]
enum MediaInfoProbeError {
    Failed,
    Spawn(std::io::Error),
    Timeout,
}

async fn run_media_info_probe(
    command: &mut Command,
    timeout_after: Duration,
) -> Result<std::process::Output, MediaInfoProbeError> {
    command.kill_on_drop(true);
    match tokio::time::timeout(timeout_after, command.output()).await {
        Ok(Ok(output)) if output.status.success() && output.stdout.len() <= MEDIA_BODY_LIMIT => {
            Ok(output)
        }
        Ok(Ok(_)) => Err(MediaInfoProbeError::Failed),
        Ok(Err(error)) => Err(MediaInfoProbeError::Spawn(error)),
        Err(_) => Err(MediaInfoProbeError::Timeout),
    }
}

async fn subtitle_extract_route(
    stream: &mut TcpStream,
    query: &str,
    _request_headers: &HashMap<String, String>,
) -> std::io::Result<()> {
    let (url, _) = match validated_media_url(query).await {
        Ok(value) => value,
        Err((status, message)) => {
            let code = if status == 400 {
                "INVALID_REQUEST"
            } else {
                "SSRF_BLOCKED"
            };
            return write_raw_json(
                stream,
                status,
                &format!(
                    r#"{{"error":{{"code":"{}","message":"{}"}}}}"#,
                    code, message
                ),
                None,
            )
            .await;
        }
    };
    let response =
        match tokio::time::timeout(
            Duration::from_secs(30),
            media_request_client().get(url).send(),
        )
        .await
        {
            Ok(Ok(response)) => response,
            _ => return write_raw_json(
                stream,
                502,
                r#"{"error":{"code":"SUBTITLE_FAILED","message":"Subtitle retrieval failed."}}"#,
                None,
            )
            .await,
        };
    if !response.status().is_success() {
        return write_raw_json(
            stream,
            404,
            r#"{"error":{"code":"SUBTITLE_NOT_FOUND","message":"Subtitle was not available."}}"#,
            None,
        )
        .await;
    }
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/vtt")
        .to_string();
    let bytes = match collect_limited(response, MEDIA_BODY_LIMIT).await {
        Ok(bytes) => bytes,
        Err(_) => return write_raw_json(stream, 502, r#"{"error":{"code":"SUBTITLE_TOO_LARGE","message":"Subtitle exceeds the size limit."}}"#, None).await,
    };
    write_headers(stream, 200, &content_type, None).await?;
    stream.write_all(&bytes).await
}

async fn collect_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, ()> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(());
    }
    let mut body = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| ())?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn assemblyai_unavailable(stream: &mut TcpStream) -> std::io::Result<()> {
    write_json(
        stream,
        501,
        &serde_json::json!({
            "error": {
                "code": "ASSEMBLYAI_UNAVAILABLE",
                "message": "AssemblyAI subtitle retrieval is not configured on this backend."
            }
        }),
        None,
    )
    .await
}

async fn pretranscode_unavailable(stream: &mut TcpStream, status: u16) -> std::io::Result<()> {
    let (code, message) = if status == 501 {
        (
            "PRETRANSCODE_UNAVAILABLE",
            "Background pretranscoding is not enabled; use live stream-proxy transcoding.",
        )
    } else {
        (
            "SUBTITLE_UNAVAILABLE",
            "This subtitle route is not supported by the backend.",
        )
    };
    write_json(
        stream,
        status,
        &serde_json::json!({"error":{"code":code,"message":message}}),
        None,
    )
    .await
}

async fn write_headers(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    extra: Option<&str>,
) -> std::io::Result<()> {
    crate::observability::record_response(status);
    let reason = match status {
        204 => "No Content",
        200 => "OK",
        206 => "Partial Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        507 => "Insufficient Storage",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Error",
    };
    let mut h = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nCache-Control: no-cache\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, HEAD, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Accept, Content-Type, Authorization, Range, X-Requested-With\r\nAccess-Control-Expose-Headers: Content-Length, Content-Range, Accept-Ranges, Content-Type, X-Media-Mode, X-Source-Id, X-Media-Duration, X-Remux-Cache-State\r\nAccess-Control-Max-Age: 86400\r\n"
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
    let mut document = serde_json::json!({
        "openapi": "3.0.3",
        "info": {"title": "MovieBox API", "version": "1.0.0"},
        "paths": {
            "/": {"get": {"responses": {"200": {"description": "API information"}}}},
            "/health": {"get": {"responses": {"200": {"description": "Liveness"}}}},
            "/ready": {"get": {"responses": {"200": {"description": "Readiness"}, "503": {"description": "Mandatory PostgreSQL catalog unavailable"}}}},
            "/api/v1": {"get": {"responses": {"200": {"description": "API version and resources"}}}},
            "/openapi.json": {"get": {"responses": {"200": {"description": "OpenAPI document"}}}},
            "/docs": {"get": {"responses": {"200": {"description": "API documentation"}}}},
            "/api/v1/diagnostics": {"get": {"responses": {"200": {"description": "Sanitized provider diagnostics"}}}},
            "/api/v1/watch": {"get": {"responses": {"200": {"description": "Episode playback descriptor; verified compatible MP4 sources use direct range passthrough, HLS/DASH remains native, and other verified sources use progressive fragmented-MP4 remux without arbitrary byte-range seeking"}, "400": {"description": "Invalid watch request"}, "415": {"description": "All discovered sources are blocked or have unverified codecs"}}}},
            "/api/v1/anime/search": {"get": {"description": "Search returns one conservative canonical card per matched Anime, with canonical_id and provider_mappings. The opaque top-level id retains the primary provider reference and mapped provider lookups. PostgreSQL persistence is optional.", "parameters": [{"name": "q", "in": "query", "required": true, "schema": {"type": "string", "maxLength": 200}}, {"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 100}}, {"name": "cursor", "in": "query", "schema": {"type": "string"}}], "responses": {"200": {"description": "Unified search results"}, "400": {"description": "Invalid query"}}}},
            "/api/v1/search": {"get": {"description": "PostgreSQL-only catalog search; never performs live provider discovery.", "responses": {"200": {"description": "Catalog search results"}, "400": {"description": "Invalid query"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/genres": {"get": {"description": "PostgreSQL-backed canonical genre list.", "responses": {"200": {"description": "Genres"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/titles/{title_id}": {"get": {"description": "Canonical PostgreSQL catalog title detail; movie titles may include eligible title-owned movie_sources.", "responses": {"200": {"description": "Title detail"}, "404": {"description": "Title not found"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/titles/{title_id}/seasons": {"get": {"description": "Stored catalog seasons for a title.", "responses": {"200": {"description": "Season list"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/titles/{title_id}/seasons/{season_number}/episodes": {"get": {"description": "Stored catalog episodes for a title season.", "responses": {"200": {"description": "Episode list"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/episodes/{episode_id}": {"get": {"description": "Stored catalog episode detail.", "responses": {"200": {"description": "Episode detail"}, "404": {"description": "Episode not found"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/episodes/{episode_id}/sources": {"get": {"description": "Eligible verified, unexpired catalog sources.", "responses": {"200": {"description": "Eligible source list"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/sources/{source_id}": {"get": {"description": "Stored source metadata, including expiry and playback eligibility.", "responses": {"200": {"description": "Source detail"}, "404": {"description": "Source not found"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/sources/{source_id}/tracks": {"get": {"description": "Stored audio and subtitle tracks for a source.", "responses": {"200": {"description": "Track list"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/sources/{source_id}/play": {"get": {"description": "Playback resolution for an eligible stored source; imported FourKHDHub source IDs are resolved live and signed URLs are not persisted.", "responses": {"200": {"description": "Playback resolved"}, "404": {"description": "Source unavailable, blocked, expired, or missing"}, "415": {"description": "Source codec is blocked or unverified"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/catalog/sources/{source_id}/remux": {"get": {"description": "Live FourKHDHub source resolution followed by bounded progressive fragmented-MP4 remux. Optional start_seconds (or start) is a non-negative timestamp in seconds; it is honored only after verified upstream byte-range capability, and does not change HTTP byte-range semantics or store complete source/output video files.", "responses": {"200": {"description": "Progressive fragmented-MP4 stream; no Content-Length or byte-range seek support"}, "400": {"description": "Malformed, non-finite, negative, or excessive timestamp"}, "404": {"description": "Source unavailable or missing"}, "415": {"description": "Source codec is blocked or unverified"}, "429": {"description": "Media-job capacity is busy"}, "501": {"description": "Timestamp or byte-range seeking is unavailable for the selected source"}, "502": {"description": "Upstream or remux startup failed"}, "503": {"description": "Catalog database or FFmpeg unavailable"}, "504": {"description": "Configured media-job or upstream timeout"}}}},
            "/api/v1/admin/catalog/import": {"post": {"description": "Protected manual catalog import; accepts a bounded titles batch.", "responses": {"201": {"description": "Import job completed"}, "401": {"description": "Unauthorized"}, "413": {"description": "Batch too large"}, "503": {"description": "Database unavailable"}}}},
            "/api/v1/admin/catalog/import-by-name": {"post": {"description": "Protected exact-match anime import from FourKHDHub, including provider details, episodes, and live source references.", "responses": {"201": {"description": "Anime inserted"}, "200": {"description": "Anime updated idempotently"}, "401": {"description": "Unauthorized"}, "404": {"description": "Exact title not found"}, "409": {"description": "Ambiguous exact title"}, "503": {"description": "Provider or catalog unavailable"}}}},
            "/api/v1/admin/catalog/update-all": {"post": {"description": "Protected manual catalog update job; never scheduled automatically.", "responses": {"202": {"description": "Update job accepted"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/admin/catalog/titles/upsert": {"post": {"description": "Protected idempotent catalog title upsert.", "responses": {"200": {"description": "Updated"}, "201": {"description": "Inserted"}, "401": {"description": "Unauthorized"}, "422": {"description": "Validation failed"}}}},
            "/api/v1/admin/catalog/inspect": {"get": {"description": "Protected catalog inspection.", "responses": {"200": {"description": "Catalog records"}, "401": {"description": "Unauthorized"}}}},
            "/api/v1/admin/catalog/jobs/{job_id}": {"get": {"description": "Protected import or update job status and persisted counters.", "responses": {"200": {"description": "Job status"}, "401": {"description": "Unauthorized"}, "404": {"description": "Job not found"}, "503": {"description": "Catalog database unavailable"}}}},
            "/api/v1/web/episodes/{episode_id}/play": {"post": {"description": "Web playback contract; requires a verified stored non-HEVC source and never performs provider discovery.", "responses": {"404": {"description": "No compatible stored source"}, "503": {"description": "Database unavailable"}}}},
            "/api/v1/android/episodes/{episode_id}/play": {"post": {"description": "Android direct-play contract; server-side HEVC transcoding is forbidden.", "responses": {"404": {"description": "No compatible stored source"}, "503": {"description": "Database unavailable"}}}},
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
            "/api/v1/movies/{movie_id}/sources": {"get": {"responses": {"200": {"description": "Movie source candidates"}, "502": {"description": "Movie source resolution failed"}}}},
            "/api/v1/movies/{movie_id}/stream": {"get": {"responses": {"200": {"description": "Movie playback descriptor; verified compatible MP4 sources use direct range passthrough, native HLS/DASH stays native, and other verified sources use progressive fragmented-MP4 remux"}, "415": {"description": "Sources are blocked or have unverified codecs"}, "429": {"description": "Media-job capacity is busy"}, "502": {"description": "Movie playback resolution failed"}, "504": {"description": "Movie playback resolution timed out"}}}},
            "/api/v1/movies/{movie_id}/playback": {"get": {"responses": {"200": {"description": "Alias for movie stream playback"}}}},
            "/api/v1/k-dramas": {"get": {"responses": {"200": {"description": "Metadata-backed K-Drama catalogue"}}}},
            "/api/v1/k-dramas/{id}": {"get": {"responses": {"501": {"description": "Unsupported category"}}}},
            "/api/v1/categories": {"get": {"responses": {"200": {"description": "Supported content categories"}}}},
            "/api/v1/anime/search/stream": {"get": {"description": "Incremental SSE search. Events are search_started, search_result (one independently completed provider result at a time), search_provider_complete, search_provider_error, and search_complete. search_provider_error includes provider, code, and status; search_complete includes partial, providers, and replay.", "responses": {"200": {"description": "SSE search_started, search_result, search_provider_complete, search_provider_error, and search_complete events"}}}},
            "/api/v1/anime/{anime_id}": {"get": {"description": "Legacy provider IDs remain accepted. Unified search IDs resolve eligible mapped providers concurrently and return successful details with per-provider status, canonical identity, and partial results.", "responses": {"200": {"description": "Aggregated anime details"}, "502": {"description": "No mapped provider returned details"}, "504": {"description": "Mapped provider details timed out"}}}},
            "/api/v1/anime/{anime_id}/seasons": {"get": {"description": "Unified season list with canonical season IDs, available episode counts, provider season numbers, and aggregated episodes.", "responses": {"200": {"description": "Unified season list"}}}},
            "/api/v1/anime/{anime_id}/episodes": {"get": {"description": "Observed episodes aggregated from successful mapped providers; missing numbers are included only when a provider returned an episode record.", "responses": {"200": {"description": "Unified episode list"}}}},
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/full": {"get": {"responses": {"200": {"description": "Anime details, selected episode, and source candidates"}, "400": {"description": "Invalid anime or episode ID"}, "404": {"description": "Episode not found"}}}},
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/stream": {"get": {"responses": {"200": {"description": "Episode playback descriptor; verified compatible MP4 sources use direct range passthrough, native HLS/DASH stays native, and other verified sources use progressive fragmented-MP4 remux"}, "415": {"description": "Sources are blocked or have unverified codecs"}, "429": {"description": "Media-job capacity is busy"}, "502": {"description": "Playback resolution failed"}, "504": {"description": "Playback resolution timed out"}}}},
            "/api/v1/episodes/{episode_id}": {"get": {"responses": {"200": {"description": "Episode details"}, "400": {"description": "Invalid episode ID"}}}},
            "/api/v1/episodes/{episode_id}/sources": {"get": {"responses": {"200": {"description": "Fresh source candidates"}}}},
            "/api/v1/episodes/{episode_id}/sources/stream": {"get": {"responses": {"200": {"description": "SSE source_result, source_error, source_complete events"}}}},
            "/api/v1/sources/{source_id}": {"get": {"responses": {"200": {"description": "Resolved source metadata"}, "415": {"description": "Source codec is blocked or unverified"}}}},
            "/api/v1/sources/{source_id}/audio": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "415": {"description": "Source codec is blocked or unverified"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/audio-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded audio tracks"}, "415": {"description": "Source codec is blocked or unverified"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitles": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "415": {"description": "Source codec is blocked or unverified"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/subtitle-tracks": {"get": {"responses": {"200": {"description": "Confirmed embedded subtitle tracks"}, "415": {"description": "Source codec is blocked or unverified"}, "503": {"description": "Track discovery unavailable"}}}},
            "/api/v1/sources/{source_id}/transcode": {"get": {"description": "Progressive fragmented-MP4 remux using bounded pipes. Optional start_seconds (or start) requests a non-negative timestamp in seconds when the verified upstream supports byte ranges; HTTP byte ranges remain unsupported and no complete video file is cached.", "responses": {"200": {"description": "Progressive fragmented-MP4 remux"}, "400": {"description": "Malformed, non-finite, negative, or excessive timestamp"}, "415": {"description": "Source codec is blocked or unverified"}, "429": {"description": "Media-job capacity is busy"}, "501": {"description": "Timestamp or byte-range seeking is unavailable"}, "502": {"description": "Remux startup or upstream failed"}, "503": {"description": "FFmpeg unavailable"}, "504": {"description": "Configured media-job or upstream timeout"}}}},
            "/api/v1/sources/{source_id}/remux": {"get": {"description": "Progressive fragmented-MP4 remux using bounded pipes. Optional start_seconds (or start) requests a non-negative timestamp in seconds when the verified upstream supports byte ranges; HTTP byte ranges remain unsupported and no complete video file is cached.", "responses": {"200": {"description": "Progressive fragmented-MP4 remux"}, "400": {"description": "Malformed, non-finite, negative, or excessive timestamp"}, "415": {"description": "Source codec is blocked or unverified"}, "429": {"description": "Media-job capacity is busy"}, "501": {"description": "Timestamp or byte-range seeking is unavailable"}, "502": {"description": "Remux startup or upstream failed"}, "503": {"description": "FFmpeg unavailable"}, "504": {"description": "Configured media-job or upstream timeout"}}}},
            "/api/v1/sources/{source_id}/playback": {"get": {"responses": {"200": {"description": "Playback descriptor; verified compatible MP4 sources use direct byte-range streaming, HLS/DASH uses native streaming, and other verified sources use progressive fragmented-MP4 remux without arbitrary byte-range seeking"}, "415": {"description": "Source codec or playback format is blocked or unverified"}, "429": {"description": "Media-job capacity is busy"}, "502": {"description": "Playback resolution failed"}, "504": {"description": "Playback resolution timed out"}}}},
            "/api/v1/sources/{source_id}/native/{token}": {"get": {"description": "Opaque source-scoped HLS/DASH manifest or media resource; child URIs are rewritten from the final upstream manifest URL and media bytes stream progressively with Range passthrough.", "responses": {"200": {"description": "Native manifest or media body"}, "206": {"description": "Exact upstream byte range"}, "400": {"description": "Invalid token path or unsupported manifest range"}, "403": {"description": "Private or unsafe upstream destination rejected"}, "404": {"description": "Expired or invalid source-scoped token"}, "413": {"description": "Configured segment size ceiling exceeded"}, "503": {"description": "Native stream capacity is busy"}, "504": {"description": "Upstream request timeout"}}}, "head": {"responses": {"200": {"description": "Native resource headers"}, "404": {"description": "Expired or invalid source-scoped token"}}}},
            "/api/v1/sources/{source_id}/watch": {"get": {"responses": {"200": {"description": "Watch descriptor; verified direct-compatible media uses range passthrough, native HLS/DASH stays native, and other verified media uses progressive fragmented-MP4 remux"}, "415": {"description": "Source codec is blocked or unverified"}, "502": {"description": "Watch resolution failed"}, "504": {"description": "Watch resolution timed out"}}}},
            "/api/v1/sources/{source_id}/download": {"get": {"responses": {"200": {"description": "Download metadata"}, "415": {"description": "Source codec is blocked or unverified"}, "501": {"description": "Unsupported"}}}},
            "/api/stream-proxy": {"get": {"responses": {"200": {"description": "Validated temporary seekable MP4"}, "206": {"description": "Requested MP4 byte range"}, "400": {"description": "Missing or invalid media URL"}, "403": {"description": "Unsafe destination rejected"}, "416": {"description": "Unsatisfiable byte range"}, "429": {"description": "Media-job capacity is busy"}, "504": {"description": "Configured media-job timeout"}, "507": {"description": "Temporary cache budget unavailable"}}}, "head": {"responses": {"405": {"description": "HEAD is not supported for remuxed media"}}}},
            "/api/media-info": {"get": {"responses": {"200": {"description": "FFprobe media metadata including duration"}, "400": {"description": "Missing or invalid media URL"}, "502": {"description": "Media inspection failed"}, "504": {"description": "Media inspection timed out"}}}},
            "/api/subtitles/extract": {"get": {"responses": {"200": {"description": "Retrieved external subtitle body"}, "400": {"description": "Missing or invalid subtitle URL"}, "404": {"description": "Subtitle not found"}}}},
            "/api/subtitles/assemblyai/{transcript_id}": {"get": {"responses": {"501": {"description": "AssemblyAI subtitle retrieval is not configured"}}}},
            "/api/pretranscode/start": {"post": {"responses": {"501": {"description": "Background pretranscoding is not enabled"}}}},
            "/api/pretranscode/status": {"get": {"responses": {"501": {"description": "Background pretranscoding is not enabled"}}}}
        }
    });
    document["paths"]["/api/v1/play/direct"] = serde_json::json!({
        "post": {
            "summary": "Resolve a provider URL for direct playback",
            "description": "Accepts only an existing opaque source_id. Returns the selected provider URL when compatibility is established from provider metadata and safe resolver evidence. If the selected source is missing or cannot be resolved, safe-URL-validated, or verified as compatible, it tries bounded alternative provider episode references already verified through the unified catalog or discovered by the anime mapping workflow. Extensionless and signed URLs are supported when the provider marks the source as a direct file with a supported codec. This route never relays media, creates proxy tokens, invokes FFmpeg or FFprobe, remuxes, or transcodes.",
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "required": ["source_id"],
                            "additionalProperties": false,
                            "properties": {"source_id": {"type": "string"}}
                        }
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Selected or verified alternate provider playback URL resolved",
                    "content": {"application/json": {"schema": {
                        "type": "object",
                        "required": ["success", "data", "meta"],
                        "properties": {
                            "success": {"const": true},
                            "data": {"type": "object", "required": ["mode", "source_id", "playback_url", "url_type", "compatibility", "requires_headers", "headers"], "properties": {
                                "mode": {"const": "direct"},
                                "source_id": {"type": "string"},
                                "playback_url": {"type": "string", "format": "uri"},
                                "url_type": {"type": "string", "enum": ["hls", "dash", "direct"]},
                                "compatibility": {"type": "string", "enum": ["verified", "unverified"]},
                                "content_type": {"type": ["string", "null"]},
                                "expires_at": {"type": ["string", "null"]},
                                "requires_headers": {"type": "boolean"},
                                "headers": {"type": "array", "items": {"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "string"}}},
                                "message": {"type": "string"}
                            }},
                            "meta": {"type": "object", "required": ["request_id", "resolution_latency_ms"], "properties": {
                                "request_id": {"type": "string"},
                                "resolution_latency_ms": {"type": "integer", "minimum": 0}
                            }}
                        }
                    }}}
                },
                "400": {"description": "Invalid request or source ID"},
                "403": {"description": "Resolved URL rejected by destination policy"},
                "404": {"description": "Source not found"},
                "415": {"description": "Direct playback compatibility is unverified or unsupported"},
                "502": {"description": "Provider URL resolution failed"},
                "503": {"description": "Provider unavailable"},
                "504": {"description": "Provider URL resolution timed out"}
            }
        }
    });
    document["paths"]["/api/v1/sources/{source_id}/direct/{token}"] = serde_json::json!({
        "get": {
            "description": "Opaque source-scoped progressive direct media stream for sources proven to have a compatible MP4-family format and upstream byte-range support; preserves upstream 200, 206, and 416 responses.",
            "responses": {
                "200": {"description": "Upstream full media body"},
                "206": {"description": "Exact upstream byte range"},
                "400": {"description": "Invalid byte range"},
                "403": {"description": "Private or unsafe upstream destination rejected"},
                "404": {"description": "Expired or invalid source-scoped token"},
                "502": {"description": "Upstream media request failed or returned invalid range metadata"},
                "503": {"description": "Direct stream capacity is busy"},
                "504": {"description": "Upstream request timeout"}
            }
        },
        "head": {
            "responses": {
                "200": {"description": "Upstream media headers"},
                "404": {"description": "Expired or invalid source-scoped token"}
            }
        }
    });
    document
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synthetic_anime(
        provider: ProviderKind,
        season: usize,
        number: usize,
        title: &str,
    ) -> MediaDetails {
        MediaDetails {
            id: ProviderMediaId {
                provider,
                value: "synthetic-title".into(),
            },
            title: "Synthetic Black Clover".into(),
            media_type: MediaType::Series,
            year: Some("2017".into()),
            description: None,
            tagline: None,
            imdb_rating: None,
            director: None,
            stars: None,
            prints: None,
            audios: None,
            poster_url: None,
            duration: None,
            genres: Vec::new(),
            seasons: vec![Season {
                number: season,
                episodes: vec![crate::providers::models::Episode {
                    season,
                    number,
                    title: Some(title.into()),
                    overview: None,

                    provider_episode_id: None,
                    provider_season_id: None,
                }],

                provider_season_id: None,
            }],
            dubs: Vec::new(),
        }
    }

    #[test]
    fn dynamic_episode_match_rejects_conflicting_titles_and_anime_identity() {
        let source = crate::providers::models::Episode {
            season: 1,
            number: 52,
            title: Some("A New Beginning".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let conflicting = synthetic_anime(ProviderKind::Dramachi, 1, 52, "The Wrong Beginning");
        assert!(matching_episode(&source, &conflicting).is_none());

        let mut wrong_anime = conflicting;
        wrong_anime.title = "Naruto".into();
        assert!(!same_anime_identity(
            &synthetic_anime(ProviderKind::FourKHdHub, 1, 52, "A New Beginning"),
            &wrong_anime,
        ));
    }

    #[test]
    fn dynamic_episode_match_supports_different_season_layout_only_with_unique_title() {
        let source = crate::providers::models::Episode {
            season: 2,
            number: 4,
            title: Some("The Battle Begins".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate = synthetic_anime(ProviderKind::AniZone, 1, 56, "The Battle Begins");
        let matched = matching_episode(&source, &candidate).unwrap();
        assert_eq!((matched.season, matched.number), (1, 56));
    }

    #[test]
    fn synthetic_black_clover_episode_coordinates_match_across_distinct_provider_ids() {
        for (number, title) in [
            (52, "The Battle Begins"),
            (100, "The Sleeping Lion"),
            (170, "The Faraway Future"),
        ] {
            let source = crate::providers::models::Episode {
                season: 1,
                number,
                title: Some(title.into()),
                overview: None,

                provider_episode_id: None,
                provider_season_id: None,
            };
            let mut candidate = synthetic_anime(ProviderKind::Dramachi, 1, number, title);
            candidate.id.value = format!("dramachi-black-clover-ep-{number}");
            assert_eq!(
                matching_episode(&source, &candidate).unwrap().number,
                number
            );
            assert_ne!(candidate.id.value, "fourkhdhub-black-clover");
        }
    }

    #[test]
    fn dynamic_episode_match_never_matches_special_to_regular_episode_or_number_only() {
        let special = crate::providers::models::Episode {
            season: 1,
            number: 52,
            title: Some("Black Clover OVA".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let regular = synthetic_anime(ProviderKind::Dramachi, 1, 53, "A New Beginning");
        assert!(matching_episode(&special, &regular).is_none());
        let untitled = crate::providers::models::Episode {
            season: 1,
            number: 52,
            title: None,
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        assert!(matching_episode(&untitled, &regular).is_none());
    }

    #[test]
    fn incomplete_episode_mapping_adds_discovered_provider_without_duplicates() {
        let mut references = vec![OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "black-clover-dramachi".into(),
            s: 1,
            e: 67,
        }];
        append_unique_episode_references(
            &mut references,
            vec![
                OpaqueRef {
                    p: ProviderKind::Dramachi,
                    v: "black-clover-dramachi".into(),
                    s: 1,
                    e: 67,
                },
                OpaqueRef {
                    p: ProviderKind::AniZone,
                    v: "black-clover-anizone".into(),
                    s: 1,
                    e: 67,
                },
            ],
        );

        assert_eq!(references.len(), 2);
        assert!(
            references
                .iter()
                .any(|reference| { reference.p == ProviderKind::Dramachi && reference.e == 67 })
        );
        assert!(
            references
                .iter()
                .any(|reference| { reference.p == ProviderKind::AniZone && reference.e == 67 })
        );
    }

    #[test]
    fn partial_episode_mapping_is_preserved_when_discovery_is_empty() {
        let mut references = vec![OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "existing-title".into(),
            s: 1,
            e: 67,
        }];
        append_unique_episode_references(&mut references, Vec::new());
        assert_eq!(references.len(), 1);
        assert_eq!(references[0].p, ProviderKind::Dramachi);
        assert_eq!(references[0].e, 67);
    }

    #[test]
    fn episode_reference_deduplication_keeps_season_and_episode_identity() {
        let mut references = Vec::new();
        append_unique_episode_references(
            &mut references,
            vec![
                OpaqueRef {
                    p: ProviderKind::AniZone,
                    v: "title".into(),
                    s: 1,
                    e: 67,
                },
                OpaqueRef {
                    p: ProviderKind::AniZone,
                    v: "title".into(),
                    s: 2,
                    e: 1,
                },
            ],
        );
        append_unique_episode_references(
            &mut references,
            vec![OpaqueRef {
                p: ProviderKind::AniZone,
                v: "title".into(),
                s: 1,
                e: 67,
            }],
        );
        assert_eq!(references.len(), 2);
        assert!(
            references
                .iter()
                .any(|reference| reference.s == 1 && reference.e == 67)
        );
        assert!(
            references
                .iter()
                .any(|reference| reference.s == 2 && reference.e == 1)
        );
    }

    #[test]
    fn import_title_match_normalizes_case_and_punctuation_without_fuzzy_matching() {
        assert_eq!(
            normalize_import_match("Lookism"),
            normalize_import_match("LOOK-ISM")
        );
        assert_ne!(
            normalize_import_match("Lookism"),
            normalize_import_match("Lookism 2")
        );
    }

    #[tokio::test]
    async fn media_info_probe_reports_missing_ffprobe_as_spawn_failure() {
        let mut command = Command::new("/moviebox-test-missing-ffprobe");
        assert!(matches!(
            run_media_info_probe(&mut command, Duration::from_millis(100)).await,
            Err(MediaInfoProbeError::Spawn(_))
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn media_info_probe_reports_timeout() {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 30"]);
        assert!(matches!(
            run_media_info_probe(&mut command, Duration::from_millis(20)).await,
            Err(MediaInfoProbeError::Timeout)
        ));
    }

    #[test]
    fn initial_zero_byte_range_keeps_live_first_policy() {
        let mut headers = HashMap::new();
        assert!(!is_seeking_range(&headers));
        headers.insert("range".to_string(), "bytes=0-".to_string());
        assert!(!is_seeking_range(&headers));
        for value in ["bytes=0-0", "bytes=0-1048575", "Bytes=0-1"] {
            headers.insert("range".to_string(), value.to_string());
            assert!(is_seeking_range(&headers), "{value}");
        }
        headers.insert("range".to_string(), "bytes=1-".to_string());
        assert!(is_seeking_range(&headers));
        headers.insert("range".to_string(), "bytes=-512".to_string());
        assert!(is_seeking_range(&headers));
    }

    #[test]
    fn remux_timestamp_seek_is_bounded_and_uses_seconds() {
        assert_eq!(
            parse_remux_seek_seconds("start_seconds=120.5").unwrap(),
            Some(120.5)
        );
        assert_eq!(parse_remux_seek_seconds("start=3").unwrap(), Some(3.0));
        assert_eq!(
            parse_remux_seek_seconds("start=3&start_seconds=120.5").unwrap(),
            Some(120.5)
        );
        assert_eq!(
            parse_remux_seek_seconds("start_seconds=120.5&start=3").unwrap(),
            Some(120.5)
        );
        for query in [
            "start_seconds=-1",
            "start_seconds=NaN",
            "start_seconds=inf",
            "start_seconds=86400.1",
            "start_seconds=not-a-number",
        ] {
            assert!(parse_remux_seek_seconds(query).is_err(), "{query}");
        }
        assert_eq!(parse_remux_seek_seconds("").unwrap(), None);
    }

    #[tokio::test]
    async fn cached_remux_http_range_returns_exact_requested_bytes() {
        let root = tempfile::tempdir().unwrap();
        let cache = crate::remux_cache::RemuxCache::open(crate::config::MediaSettings {
            cache_dir: root.path().to_path_buf(),
            cache_ttl: Duration::from_secs(60),
            cache_max_bytes: 1024,
            max_concurrent_jobs: 1,
            job_timeout: Duration::from_secs(30),
        })
        .await
        .unwrap();
        let key = crate::remux_cache::cache_key("range-fixture", "test-profile");
        let lease = cache
            .get_or_build(&key, |path| async move {
                tokio::fs::write(path, b"0123456789")
                    .await
                    .map_err(crate::remux_cache::CacheError::Io)?;
                Ok(12.0)
            })
            .await
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let mut headers = HashMap::new();
        headers.insert("range".into(), "bytes=3-6".into());
        write_cached_remux_response(&mut server, lease, None, &headers, false)
            .await
            .unwrap();
        server.shutdown().await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let boundary = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("HTTP header boundary");
        let response_headers = String::from_utf8_lossy(&response[..boundary]);
        assert!(response_headers.starts_with("HTTP/1.1 206"));
        assert!(response_headers.contains("Content-Range: bytes 3-6/10"));
        assert!(response_headers.contains("Content-Length: 4"));
        assert_eq!(&response[boundary + 4..], b"3456");
    }

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

                    provider_episode_id: None,
                    provider_season_id: None,
                }],

                provider_season_id: None,
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

            provider_episode_id: None,
            provider_season_id: None,
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

    fn verified_release(provider: ProviderKind, filename: &str, url: &str) -> Release {
        Release {
            provider,
            filename: filename.into(),
            quality: Some("1080p".into()),
            codec: Some("H.264".into()),
            language: Some("Japanese".into()),
            size_bytes: Some(100),
            season: Some(1),
            episode: Some(2),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Direct".into(),
                resolver_url: url.into(),
                headers: vec![],
                direct_file: true,
            }],
            resource_id: None,
        }
    }

    async fn test_state() -> (ApiState, tempfile::TempDir) {
        let root = tempfile::tempdir().unwrap();
        let remux_cache = crate::remux_cache::RemuxCache::open(crate::config::MediaSettings {
            cache_dir: root.path().join("remux"),
            cache_ttl: Duration::from_secs(60),
            cache_max_bytes: 1_048_576,
            max_concurrent_jobs: 1,
            job_timeout: Duration::from_secs(60),
        })
        .await
        .unwrap();
        let native_stream = crate::api_native_stream::NativeStreamProxy::new(1_048_576).unwrap();
        (
            ApiState {
                service: Arc::new(MovieBoxService::new()),
                catalog: None,
                release_cache: Arc::new(Mutex::new(HashMap::new())),
                release_inflight: Arc::new(Mutex::new(HashMap::new())),
                playback_cache: Arc::new(Mutex::new(HashMap::new())),
                playback_inflight: Arc::new(Mutex::new(HashMap::new())),
                search_cache: Arc::new(Mutex::new(HashMap::new())),
                redis_search: None,
                remux_cache,
                native_stream,
                connection_slots: Arc::new(Semaphore::new(MAX_API_CONNECTIONS)),
                test_release_overrides: Arc::new(Mutex::new(HashMap::new())),
                test_release_delays: Arc::new(Mutex::new(HashMap::new())),
                test_fallback_refs: Arc::new(Mutex::new(HashMap::new())),
            },
            root,
        )
    }

    async fn cache_releases(state: &ApiState, reference: &OpaqueRef, releases: Vec<Release>) {
        state.release_cache.lock().await.insert(
            release_cache_key(reference),
            ReleaseCacheEntry {
                stored_at: Instant::now(),
                releases,
            },
        );
    }

    async fn override_releases(
        state: &ApiState,
        reference: &OpaqueRef,
        result: Result<Vec<Release>, ProviderError>,
    ) {
        state
            .test_release_overrides
            .lock()
            .await
            .insert(release_cache_key(reference), result);
    }

    async fn set_fallback_refs(state: &ApiState, source: &OpaqueSource, refs: Vec<OpaqueRef>) {
        state.test_fallback_refs.lock().await.insert(
            release_cache_key(&OpaqueRef {
                p: source.p,
                v: source.v.clone(),
                s: source.s,
                e: source.e,
            }),
            refs,
        );
    }

    fn test_anime_details(reference: &OpaqueRef) -> (MediaDetails, String, String) {
        let id = ProviderMediaId {
            provider: reference.p,
            value: reference.v.clone(),
        };
        let episode = crate::providers::models::Episode {
            season: reference.s,
            number: reference.e,
            title: Some("Test episode".into()),
            overview: Some("Test overview".into()),

            provider_episode_id: None,
            provider_season_id: None,
        };
        let details_result = MediaDetails {
            id: id.clone(),
            title: "Test anime".into(),
            media_type: MediaType::Series,
            year: Some("2026".into()),
            description: Some("Test description".into()),
            tagline: None,
            imdb_rating: None,
            director: None,
            stars: None,
            prints: None,
            audios: None,
            poster_url: Some("https://cdn.example/poster.jpg".into()),
            duration: None,
            genres: vec!["Anime".into()],
            seasons: vec![Season {
                number: reference.s,
                episodes: vec![episode.clone()],

                provider_season_id: None,
            }],
            dubs: vec![],
        };
        (
            details_result.clone(),
            details(details_result).id,
            episode_ref(&id, &episode),
        )
    }

    async fn read_http_json(stream: &mut TcpStream) -> (u16, serde_json::Value) {
        let mut response = Vec::new();
        let (_header_end, body) = loop {
            let mut chunk = [0u8; 4096];
            let count = stream.read(&mut chunk).await.unwrap();
            assert!(count > 0, "route closed before returning an HTTP response");
            response.extend_from_slice(&chunk[..count]);
            let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let body_start = header_end + 4;
            if let Ok(body) = serde_json::from_slice(&response[body_start..]) {
                break (body_start, body);
            }
        };
        let status = String::from_utf8_lossy(&response)
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse::<u16>()
            .unwrap();
        (status, body)
    }

    async fn invoke_full_route(
        state: ApiState,
        anime_id: &str,
        episode_id: &str,
        anime_ref: OpaqueRef,
        details_result: MediaDetails,
        alternate_refs: Vec<OpaqueRef>,
    ) -> (u16, serde_json::Value) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        anime_episode_full_route(
            &mut server,
            anime_id,
            episode_id,
            anime_ref,
            details_result,
            alternate_refs,
            state,
        )
        .await
        .unwrap();
        read_http_json(&mut client).await
    }

    async fn invoke_direct_route(state: ApiState, source_id: &str) -> (u16, serde_json::Value) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let body = serde_json::to_vec(&serde_json::json!({"source_id": source_id})).unwrap();
        direct_play_route(&mut server, state, &body).await.unwrap();
        read_http_json(&mut client).await
    }

    #[tokio::test]
    async fn anime_episode_full_route_http_aggregates_sources_and_preserves_contract() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let alternate = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "alternate-title".into(),
            s: 1,
            e: 2,
        };
        let (state, _root) = test_state().await;
        cache_releases(
            &state,
            &primary,
            vec![verified_release(
                primary.p,
                "primary.mkv",
                "https://cdn.example/primary.mkv",
            )],
        )
        .await;
        cache_releases(
            &state,
            &alternate,
            vec![verified_release(
                alternate.p,
                "alternate.mkv",
                "https://cdn.example/alternate.mkv",
            )],
        )
        .await;
        let (details_result, anime_id, episode_id) = test_anime_details(&primary);
        let (status, body) = invoke_full_route(
            state,
            &anime_id,
            &episode_id,
            primary.clone(),
            details_result,
            vec![alternate],
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["anime"]["title"], "Test anime");
        assert_eq!(body["selected_episode"]["number"], 2);
        assert_eq!(body["sources_status"], "ok");
        assert_eq!(body["sources_partial"], false);
        assert_eq!(body["sources"].as_array().unwrap().len(), 2);
        let providers = body["sources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|source| {
                decode::<OpaqueSource>(source["id"].as_str().unwrap())
                    .unwrap()
                    .p
            })
            .collect::<Vec<_>>();
        assert!(providers.contains(&ProviderKind::FourKHdHub));
        assert!(providers.contains(&ProviderKind::Dramachi));
    }

    #[tokio::test]
    async fn anime_episode_full_route_http_reports_partial_failure_and_timeout() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let failed = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "failed-title".into(),
            s: 1,
            e: 2,
        };
        let timed_out = OpaqueRef {
            p: ProviderKind::Addons,
            v: "timeout-title".into(),
            s: 1,
            e: 2,
        };
        let (state, _root) = test_state().await;
        cache_releases(
            &state,
            &primary,
            vec![verified_release(
                primary.p,
                "primary.mkv",
                "https://cdn.example/primary.mkv",
            )],
        )
        .await;
        override_releases(
            &state,
            &failed,
            Err(ProviderError::Network("provider down".into())),
        )
        .await;
        state.test_release_delays.lock().await.insert(
            release_cache_key(&timed_out),
            SOURCE_TIMEOUT + Duration::from_millis(30),
        );
        let (details_result, anime_id, episode_id) = test_anime_details(&primary);
        let (status, body) = invoke_full_route(
            state,
            &anime_id,
            &episode_id,
            primary,
            details_result,
            vec![failed, timed_out],
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["sources_status"], "partial");
        assert_eq!(body["sources_partial"], true);
        assert_eq!(
            body["source_error"]["error"]["code"],
            "UPSTREAM_UNAVAILABLE"
        );
    }

    #[tokio::test]
    async fn anime_episode_full_route_http_rejects_wrong_anime_episode_and_provider_ids() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let (details_result, anime_id, episode_id) = test_anime_details(&primary);
        let (state, _root) = test_state().await;
        let (status, body) = invoke_full_route(
            state,
            &encode(&OpaqueRef {
                p: ProviderKind::FourKHdHub,
                v: "wrong-anime".into(),
                s: 0,
                e: 0,
            }),
            &episode_id,
            primary.clone(),
            details_result.clone(),
            vec![],
        )
        .await;
        assert_eq!(status, 400);
        assert_eq!(body["error"]["code"], "INVALID_ID");

        let (state, _root) = test_state().await;
        let wrong_provider_episode = encode(&OpaqueRef {
            p: ProviderKind::Dramachi,
            v: primary.v.clone(),
            s: 1,
            e: 2,
        });
        let (status, body) = invoke_full_route(
            state,
            &anime_id,
            &wrong_provider_episode,
            primary.clone(),
            details_result.clone(),
            vec![],
        )
        .await;
        assert_eq!(status, 400);
        assert_eq!(body["error"]["code"], "INVALID_ID");

        let (state, _root) = test_state().await;
        let wrong_episode = encode(&OpaqueRef {
            p: primary.p,
            v: primary.v.clone(),
            s: 1,
            e: 3,
        });
        let (status, body) = invoke_full_route(
            state,
            &anime_id,
            &wrong_episode,
            primary.clone(),
            details_result,
            vec![],
        )
        .await;
        assert_eq!(status, 404);
        assert_eq!(body["error"]["code"], "EPISODE_NOT_FOUND");
    }

    #[tokio::test]
    async fn direct_play_route_http_falls_back_after_original_fetch_failure_and_timeout() {
        for timeout in [false, true] {
            let primary = OpaqueRef {
                p: ProviderKind::FourKHdHub,
                v: "primary-title".into(),
                s: 1,
                e: 2,
            };
            let alternate = OpaqueRef {
                p: ProviderKind::Dramachi,
                v: "alternate-title".into(),
                s: 1,
                e: 2,
            };
            let original = verified_release(
                primary.p,
                "original.mkv",
                "https://cdn.example/original.mkv",
            );
            let fallback = verified_release(
                alternate.p,
                "fallback.mkv",
                "https://cdn.example/fallback.mkv",
            );
            let source_id = source_ref(&original, &primary, 0);
            let fallback_source_id = api_sources(std::slice::from_ref(&fallback), &alternate)[0]
                .id
                .clone();
            let (state, _root) = test_state().await;
            if timeout {
                state.test_release_delays.lock().await.insert(
                    release_cache_key(&primary),
                    SOURCE_TIMEOUT + Duration::from_millis(30),
                );
            } else {
                override_releases(
                    &state,
                    &primary,
                    Err(ProviderError::Unavailable("original failed".into())),
                )
                .await;
            }
            cache_releases(&state, &alternate, vec![fallback]).await;
            let source = decode::<OpaqueSource>(&source_id).unwrap();
            set_fallback_refs(&state, &source, vec![alternate]).await;
            let (status, body) = invoke_direct_route(state, &source_id).await;
            assert_eq!(status, 200);
            assert_eq!(body["data"]["source_id"], fallback_source_id);
            assert_eq!(body["data"]["provider"], "dramachi");
            assert_eq!(
                body["data"]["message"],
                "Fallback provider playback URL resolved."
            );
        }
    }

    #[tokio::test]
    async fn real_http_disconnect_during_direct_upstream_acquisition_cancels_upstream_and_releases_slot()
     {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if socket.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
            }
            let _ = started_tx.send(());
            let mut probe = [0u8; 1];
            let _ = socket.read(&mut probe).await;
        });

        let (mut state, _root) = test_state().await;
        state.native_stream.test_set_slot_capacity(1);
        let direct_path = state
            .native_stream
            .direct_playback_url(
                "source-1",
                &format!("http://{upstream_address}/video.mp4"),
                &[],
            )
            .unwrap();
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend.local_addr().unwrap();
        let client = TcpStream::connect(backend_address).await.unwrap();
        let (server, _) = backend.accept().await.unwrap();
        let handler = tokio::spawn(handle(server, state.clone()));
        let mut client = client;
        client
            .write_all(
                format!(
                    "GET {direct_path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        started_rx.await.unwrap();
        drop(client);

        tokio::time::timeout(Duration::from_secs(3), handler)
            .await
            .expect("route handler did not stop after client disconnect")
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), upstream_task)
            .await
            .expect("upstream request remained open after client disconnect")
            .unwrap();
        assert!(state.native_stream.test_try_acquire_slot());
    }

    #[tokio::test]
    async fn real_http_disconnect_during_direct_body_relay_cancels_upstream_and_releases_slot() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let (headers_tx, headers_rx) = tokio::sync::oneshot::channel();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                if socket.read_exact(&mut byte).await.is_err() {
                    return;
                }
                request.push(byte[0]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
            let _ = headers_tx.send(());
            let mut probe = [0u8; 1];
            let _ = socket.read(&mut probe).await;
        });

        let (mut state, _root) = test_state().await;
        state.native_stream.test_set_slot_capacity(1);
        let direct_path = state
            .native_stream
            .direct_playback_url(
                "source-1",
                &format!("http://{upstream_address}/video.mp4"),
                &[],
            )
            .unwrap();
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend.local_addr().unwrap();
        let mut client = TcpStream::connect(backend_address).await.unwrap();
        let (server, _) = backend.accept().await.unwrap();
        let handler = tokio::spawn(handle(server, state.clone()));
        client
            .write_all(
                format!(
                    "GET {direct_path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        headers_rx.await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            client.read_exact(&mut byte).await.unwrap();
            response.push(byte[0]);
        }
        assert!(response.starts_with(b"HTTP/1.1 200"));
        drop(client);

        tokio::time::timeout(Duration::from_secs(3), handler)
            .await
            .expect("route handler did not stop after body-relay disconnect")
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), upstream_task)
            .await
            .expect("upstream body remained open after client disconnect")
            .unwrap();
        assert!(state.native_stream.test_try_acquire_slot());
    }

    #[tokio::test]
    async fn direct_play_route_falls_back_to_another_source_from_same_provider() {
        let primary = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "same-provider-title".into(),
            s: 1,
            e: 2,
        };
        let failed = verified_release(
            primary.p,
            "failed-1080p.mkv",
            "http://127.0.0.1/expired.mkv",
        );
        let fallback = Release {
            quality: Some("720p".into()),
            ..verified_release(
                primary.p,
                "working-720p.mkv",
                "https://cdn.example/working-720p.mkv",
            )
        };
        let source_id = source_ref(&failed, &primary, 0);
        let fallback_source_id = api_sources(std::slice::from_ref(&fallback), &primary)[0]
            .id
            .clone();
        let (state, _root) = test_state().await;
        cache_releases(&state, &primary, vec![failed, fallback]).await;

        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["source_id"], fallback_source_id);
        assert_eq!(body["data"]["provider"], "dramachi");
        assert_eq!(
            body["data"]["playback_url"],
            "https://cdn.example/working-720p.mkv"
        );
    }

    #[tokio::test]
    async fn direct_play_route_preserves_primary_success_and_falls_back_for_missing_or_invalid_source()
     {
        let primary = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "black-clover-dramachi".into(),
            s: 1,
            e: 52,
        };
        let alternate = OpaqueRef {
            p: ProviderKind::BdixCircleFtp,
            v: "black-clover-bdix".into(),
            s: 1,
            e: 52,
        };
        let primary_release = Release {
            episode: Some(52),
            ..verified_release(
                primary.p,
                "black-clover-52.mkv",
                "https://cdn.example/black-clover-52.mkv",
            )
        };
        let alternate_release = Release {
            episode: Some(52),
            ..verified_release(
                alternate.p,
                "black-clover-52-alt.mkv",
                "https://cdn.example/black-clover-52-alt.mkv",
            )
        };
        let alternate_source_id = api_sources(std::slice::from_ref(&alternate_release), &alternate)
            [0]
        .id
        .clone();

        let (state, _root) = test_state().await;
        cache_releases(&state, &primary, vec![primary_release.clone()]).await;
        cache_releases(&state, &alternate, vec![alternate_release.clone()]).await;
        let primary_source_id = source_ref(&primary_release, &primary, 0);
        set_fallback_refs(
            &state,
            &decode::<OpaqueSource>(&primary_source_id).unwrap(),
            vec![alternate.clone()],
        )
        .await;
        let (status, body) = invoke_direct_route(state, &primary_source_id).await;
        assert_eq!(status, 200);
        assert_eq!(
            body["data"]["playback_url"],
            "https://cdn.example/black-clover-52.mkv"
        );
        assert_eq!(body["data"]["provider"], "dramachi");

        for missing_source in [true, false] {
            let original = if missing_source {
                primary_release.clone()
            } else {
                Release {
                    episode: Some(52),
                    ..verified_release(
                        primary.p,
                        "invalid-primary.mkv",
                        "http://127.0.0.1/invalid-primary.mkv",
                    )
                }
            };
            let source_id = source_ref(&original, &primary, 0);
            let (state, _root) = test_state().await;
            if missing_source {
                cache_releases(&state, &primary, Vec::new()).await;
            } else {
                cache_releases(&state, &primary, vec![original]).await;
            }
            cache_releases(&state, &alternate, vec![alternate_release.clone()]).await;
            set_fallback_refs(
                &state,
                &decode::<OpaqueSource>(&source_id).unwrap(),
                vec![alternate.clone()],
            )
            .await;
            let (status, body) = invoke_direct_route(state, &source_id).await;
            assert_eq!(status, 200);
            assert_eq!(body["data"]["source_id"], alternate_source_id);
            assert_eq!(
                body["data"]["playback_url"],
                "https://cdn.example/black-clover-52-alt.mkv"
            );
            assert_eq!(body["data"]["provider"], "bdix_circleftp");
        }
    }

    #[tokio::test]
    async fn direct_play_route_does_not_fallback_to_wrong_episode_reference() {
        let primary = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "primary-title".into(),
            s: 1,
            e: 100,
        };
        let alternate = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "alternate-title".into(),
            s: 1,
            e: 100,
        };
        let original =
            verified_release(primary.p, "primary.mkv", "https://cdn.example/primary.mkv");
        let wrong_episode = Release {
            episode: Some(170),
            ..verified_release(
                alternate.p,
                "wrong-episode.mkv",
                "https://cdn.example/wrong-episode.mkv",
            )
        };
        let source_id = source_ref(&original, &primary, 0);
        let (state, _root) = test_state().await;
        override_releases(
            &state,
            &primary,
            Err(ProviderError::Unavailable("primary source absent".into())),
        )
        .await;
        cache_releases(&state, &alternate, vec![wrong_episode]).await;
        set_fallback_refs(
            &state,
            &decode::<OpaqueSource>(&source_id).unwrap(),
            vec![alternate],
        )
        .await;
        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 503);
        assert_eq!(body["error"]["code"], "UPSTREAM_UNAVAILABLE");
    }

    #[tokio::test]
    async fn direct_play_route_http_skips_failed_alternates_and_returns_later_source() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let first = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "first-title".into(),
            s: 1,
            e: 2,
        };
        let second = OpaqueRef {
            p: ProviderKind::Addons,
            v: "second-title".into(),
            s: 1,
            e: 2,
        };
        let later = OpaqueRef {
            p: ProviderKind::BdixCircleFtp,
            v: "later-title".into(),
            s: 1,
            e: 2,
        };
        let original = verified_release(
            primary.p,
            "original.mkv",
            "https://cdn.example/original.mkv",
        );
        let success = verified_release(later.p, "later.mkv", "https://cdn.example/later.mkv");
        let source_id = source_ref(&original, &primary, 0);
        let expected_id = api_sources(std::slice::from_ref(&success), &later)[0]
            .id
            .clone();
        let (state, _root) = test_state().await;
        override_releases(
            &state,
            &primary,
            Err(ProviderError::Unavailable("original failed".into())),
        )
        .await;
        override_releases(
            &state,
            &first,
            Err(ProviderError::Network("first failed".into())),
        )
        .await;
        override_releases(
            &state,
            &second,
            Err(ProviderError::Unavailable("second failed".into())),
        )
        .await;
        cache_releases(&state, &later, vec![success]).await;
        let source = decode::<OpaqueSource>(&source_id).unwrap();
        set_fallback_refs(&state, &source, vec![first, second, later]).await;
        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["source_id"], expected_id);
        assert_eq!(body["data"]["provider"], "bdix_circleftp");
    }

    #[tokio::test]
    async fn direct_play_route_http_rejects_identity_unsafe_incompatible_and_all_failed_sources() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let original = verified_release(
            primary.p,
            "original.mkv",
            "https://cdn.example/original.mkv",
        );
        let source_id = source_ref(&original, &primary, 0);

        let (state, _root) = test_state().await;
        override_releases(
            &state,
            &primary,
            Ok(vec![Release {
                episode: Some(3),
                ..original.clone()
            }]),
        )
        .await;
        let source = decode::<OpaqueSource>(&source_id).unwrap();
        set_fallback_refs(&state, &source, Vec::new()).await;
        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 404);
        assert_eq!(body["error"]["code"], "SOURCE_NOT_FOUND");

        for candidate in [
            verified_release(
                ProviderKind::Dramachi,
                "unsafe.mkv",
                "http://127.0.0.1/unsafe.mkv",
            ),
            Release {
                codec: Some("HEVC".into()),
                ..verified_release(
                    ProviderKind::Dramachi,
                    "incompatible.mkv",
                    "https://cdn.example/incompatible.mkv",
                )
            },
        ] {
            let alternate = OpaqueRef {
                p: candidate.provider,
                v: candidate.filename.clone(),
                s: 1,
                e: 2,
            };
            let (state, _root) = test_state().await;
            override_releases(
                &state,
                &primary,
                Err(ProviderError::Unavailable("original failed".into())),
            )
            .await;
            cache_releases(&state, &alternate, vec![candidate]).await;
            let source = decode::<OpaqueSource>(&source_id).unwrap();
            set_fallback_refs(&state, &source, vec![alternate]).await;
            let (status, body) = invoke_direct_route(state, &source_id).await;
            assert_eq!(status, 503);
            assert_eq!(body["error"]["code"], "UPSTREAM_UNAVAILABLE");
        }

        let (state, _root) = test_state().await;
        override_releases(
            &state,
            &primary,
            Err(ProviderError::Unavailable("original failed".into())),
        )
        .await;
        let source = decode::<OpaqueSource>(&source_id).unwrap();
        let failed_alternate = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "failed-title".into(),
            s: 1,
            e: 2,
        };
        override_releases(
            &state,
            &failed_alternate,
            Err(ProviderError::Network("alternate failed".into())),
        )
        .await;
        set_fallback_refs(&state, &source, vec![failed_alternate]).await;
        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 503);
        assert_eq!(body["error"]["code"], "UPSTREAM_UNAVAILABLE");
    }

    #[test]
    fn anime_episode_full_route_aggregates_verified_provider_sources() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let alternate = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "alternate-title".into(),
            s: 1,
            e: 2,
        };
        let (sources, status, partial, error, provider_status) = aggregate_episode_sources([
            (
                primary.clone(),
                primary.p,
                Ok(vec![verified_release(
                    primary.p,
                    "primary.mkv",
                    "https://cdn.example/primary.mkv",
                )]),
            ),
            (
                alternate.clone(),
                alternate.p,
                Ok(vec![verified_release(
                    alternate.p,
                    "alternate.mkv",
                    "https://cdn.example/alternate.mkv",
                )]),
            ),
        ]);
        assert_eq!(status, "ok");
        assert!(!partial);
        assert!(error.is_none());
        assert_eq!(
            provider_status.get("fourkhdhub").map(String::as_str),
            Some("sources_ok")
        );
        assert_eq!(
            provider_status.get("dramachi").map(String::as_str),
            Some("sources_ok")
        );
        assert_eq!(sources.len(), 2);
        let providers = sources
            .iter()
            .map(|source| decode::<OpaqueSource>(&source.id).unwrap().p)
            .collect::<Vec<_>>();
        assert!(providers.contains(&ProviderKind::FourKHdHub));
        assert!(providers.contains(&ProviderKind::Dramachi));
    }

    #[test]
    fn anime_episode_full_route_reports_partial_provider_failures() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 2,
        };
        let failed = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "alternate-title".into(),
            s: 1,
            e: 2,
        };
        let (sources, status, partial, error, provider_status) = aggregate_episode_sources([
            (
                primary.clone(),
                primary.p,
                Ok(vec![verified_release(
                    primary.p,
                    "primary.mkv",
                    "https://cdn.example/primary.mkv",
                )]),
            ),
            (
                failed,
                ProviderKind::Dramachi,
                Err(EpisodeReleaseFailure::Provider(ProviderError::Network(
                    "provider down".into(),
                ))),
            ),
        ]);
        assert_eq!(sources.len(), 1);
        assert_eq!(status, "partial");
        assert!(partial);
        assert!(error.is_some());
        assert_eq!(
            provider_status.get("dramachi").map(String::as_str),
            Some("source_error")
        );
    }

    #[test]
    fn direct_play_route_rejects_wrong_episode_identity() {
        let source = OpaqueSource {
            p: ProviderKind::Dramachi,
            v: "title".into(),
            s: 1,
            e: 2,
            content_type: None,
            f: None,
            i: 0,
        };
        let wrong_episode = verified_release(
            ProviderKind::Dramachi,
            "wrong-episode.mkv",
            "https://cdn.example/wrong-episode.mkv",
        );
        let wrong_episode = Release {
            episode: Some(3),
            ..wrong_episode
        };
        assert!(!release_matches_episode(&wrong_episode, &source));
    }

    #[test]
    fn direct_play_route_rejects_wrong_provider_identity() {
        let source = OpaqueSource {
            p: ProviderKind::Dramachi,
            v: "title".into(),
            s: 1,
            e: 2,
            content_type: None,
            f: None,
            i: 0,
        };
        let wrong_provider = verified_release(
            ProviderKind::FourKHdHub,
            "wrong-provider.mkv",
            "https://cdn.example/wrong-provider.mkv",
        );
        assert!(!release_matches_episode(&wrong_provider, &source));
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
    fn direct_route_returns_only_safe_headers_and_manifest_types() {
        let headers = vec![
            ("User-Agent".into(), "player".into()),
            ("Referer".into(), "https://provider.example/".into()),
            ("Origin".into(), "https://provider.example".into()),
            ("Cookie".into(), "signed-cookie".into()),
            ("Authorization".into(), "Bearer secret".into()),
        ];
        let safe = direct_safe_headers(&headers);
        assert_eq!(safe.len(), 3);
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/master.m3u8").unwrap(),
                &Release {
                    provider: ProviderKind::Addons,
                    filename: "master.m3u8".into(),
                    quality: None,
                    codec: None,
                    language: None,
                    size_bytes: None,
                    season: None,
                    episode: None,
                    mirrors: vec![],
                    resource_id: None,
                },
                None,
            ),
            Some("hls")
        );
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/manifest.mpd").unwrap(),
                &Release {
                    provider: ProviderKind::Addons,
                    filename: "manifest.mpd".into(),
                    quality: None,
                    codec: None,
                    language: None,
                    size_bytes: None,
                    season: None,
                    episode: None,
                    mirrors: vec![],
                    resource_id: None,
                },
                None,
            ),
            Some("dash")
        );
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/video.mp4").unwrap(),
                &Release {
                    provider: ProviderKind::Addons,
                    filename: "video.mp4".into(),
                    quality: None,
                    codec: None,
                    language: None,
                    size_bytes: None,
                    season: None,
                    episode: None,
                    mirrors: vec![],
                    resource_id: None,
                },
                Some("video/mp4"),
            ),
            Some("direct")
        );
    }

    #[test]
    fn direct_route_uses_metadata_for_extensionless_and_signed_video_urls() {
        let release = Release {
            provider: ProviderKind::FourKHdHub,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("H.264".into()),
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Direct".into(),
                resolver_url: "https://cdn.example/file?id=1".into(),
                headers: vec![("Referer".into(), "https://provider.example/".into())],
                direct_file: false,
            }],
            resource_id: None,
        };
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/file?id=1").unwrap(),
                &release,
                None,
            ),
            Some("direct")
        );
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/file?sig=abc").unwrap(),
                &release,
                Some("video/mp4; charset=binary"),
            ),
            Some("direct")
        );
    }

    #[test]
    fn direct_route_keeps_ambiguous_urls_unknown() {
        let release = Release {
            provider: ProviderKind::Addons,
            filename: "episode.bin".into(),
            quality: None,
            codec: None,
            language: None,
            size_bytes: None,
            season: None,
            episode: None,
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Unknown".into(),
                resolver_url: "https://cdn.example/file".into(),
                headers: vec![],
                direct_file: false,
            }],
            resource_id: None,
        };
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/file").unwrap(),
                &release,
                None,
            ),
            None
        );
        assert_eq!(
            direct_url_type(
                &reqwest::Url::parse("https://cdn.example/playlist.m3u").unwrap(),
                &release,
                None,
            ),
            None
        );
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
    fn configured_search_providers_include_safe_enabled_anime_providers() {
        let providers = configured_search_providers();
        assert!(providers.contains(&ProviderKind::FourKHdHub));
        assert!(providers.contains(&ProviderKind::Dramachi));
        assert!(providers.contains(&ProviderKind::AniZone));
        assert!(!providers.contains(&ProviderKind::BdixCircleFtp));
        assert!(!providers.contains(&ProviderKind::BdixDhakaFlix));
        assert!(!providers.contains(&ProviderKind::Addons));
    }

    #[test]
    fn unified_search_card_keeps_provider_specific_mapping_ids() {
        let make_item = |provider, id: &str, title: &str| CatalogItem {
            id: ProviderMediaId {
                provider,
                value: id.to_string(),
            },
            title: title.to_string(),
            media_type: MediaType::Series,
            year: Some("2017".to_string()),
            poster_url: None,
            season_count: Some(1),
        };
        let unified = crate::unified::group_catalog_items(vec![
            make_item(ProviderKind::FourKHdHub, "black-clover", "Black Clover"),
            make_item(
                ProviderKind::Dramachi,
                "black-clover-2017",
                "Black Clover (2017)",
            ),
        ]);
        assert_eq!(unified.len(), 1);

        let response = catalog(unified.into_iter().next().unwrap(), None);
        let primary: OpaqueRef = decode(&response.id).unwrap();
        assert_eq!(primary.p, ProviderKind::FourKHdHub);
        assert_eq!(response.provider_mappings.len(), 1);
        let secondary: OpaqueRef = decode(&response.provider_mappings[0].id).unwrap();
        assert_eq!(secondary.p, ProviderKind::Dramachi);
        assert_eq!(secondary.v, "black-clover-2017");
    }

    #[test]
    fn unified_episode_coordinates_keep_conflicting_provider_titles_unresolved() {
        let primary_ref = OpaqueRef {
            p: ProviderKind::AniZone,
            v: "haibane-anizone".into(),
            s: 1,
            e: 1,
        };
        let alternate_ref = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "42999".into(),
            s: 1,
            e: 1,
        };
        let (mut primary, _, _) = test_anime_details(&primary_ref);
        let (mut alternate, _, _) = test_anime_details(&alternate_ref);
        primary.seasons[0].episodes[0].title = Some("Cocoon / Dream of Falling".into());
        alternate.seasons[0].episodes[0].title = Some("Haibane Renmei S01E01".into());

        let details = vec![
            (primary.id.clone(), primary),
            (alternate.id.clone(), alternate),
        ];
        let mappings = details.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>();
        let response =
            unified_api_details(&details, &mappings, "local-haibane".into(), HashMap::new());
        assert_eq!(response.seasons[0].episode_count, 2);
        assert!(response.seasons[0].episodes.iter().all(|episode| {
            episode.provider_lookups.len() == 1
                && episode.provider_mappings.is_empty()
                && episode.mapping_status == "unresolved"
        }));
    }

    #[test]
    fn unmerged_search_cards_receive_distinct_local_canonical_ids() {
        let make_item = |id: &str| CatalogItem {
            id: ProviderMediaId {
                provider: ProviderKind::Dramachi,
                value: id.to_string(),
            },
            title: "Example".to_string(),
            media_type: MediaType::Series,
            year: Some("2017".to_string()),
            poster_url: None,
            season_count: Some(1),
        };
        let groups = crate::unified::group_catalog_items(vec![
            make_item("example-a"),
            make_item("example-b"),
        ]);
        assert_eq!(groups.len(), 2);
        let cards = groups
            .into_iter()
            .map(|group| catalog(group, None))
            .collect::<Vec<_>>();
        assert_ne!(cards[0].canonical_id, cards[1].canonical_id);
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
    fn api_sources_rejects_explicitly_wrong_episode_and_deduplicates_same_url() {
        let episode = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "title".into(),
            s: 1,
            e: 2,
        };
        let first = verified_release(
            ProviderKind::Dramachi,
            "episode-2-a.mkv",
            "https://cdn.example/episode-2.mkv",
        );
        let duplicate = Release {
            filename: "episode-2-b.mkv".into(),
            ..first.clone()
        };
        let wrong_episode = Release {
            episode: Some(3),
            ..first.clone()
        };
        let sources = api_sources(&[first, duplicate, wrong_episode], &episode);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].quality.as_deref(), Some("1080p"));
    }

    #[test]
    fn source_identity_preserves_same_url_when_required_headers_differ() {
        let episode = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "title".into(),
            s: 1,
            e: 2,
        };
        let first = verified_release(
            ProviderKind::Dramachi,
            "episode-2.mkv",
            "https://cdn.example/episode-2.mkv",
        );
        let second = Release {
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Direct".into(),
                resolver_url: "https://cdn.example/episode-2.mkv".into(),
                headers: vec![("Referer".into(), "https://one.example/".into())],
                direct_file: true,
            }],
            ..first.clone()
        };
        assert_ne!(release_fingerprint(&first), release_fingerprint(&second));
        assert_eq!(api_sources(&[first, second], &episode).len(), 2);
    }

    #[test]
    fn catalog_import_candidate_does_not_require_or_trust_codec_hints() {
        let mut release = Release {
            provider: ProviderKind::FourKHdHub,
            filename: "episode.mkv".into(),
            quality: Some("2160p".into()),
            codec: None,
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Provider mirror".into(),
                resolver_url: "https://hubcloud.example/drive/opaque".into(),
                headers: vec![],
                direct_file: false,
            }],
            resource_id: None,
        };
        assert!(release_has_verification_candidate(&release));
        assert!(!release_has_usable_source(&release));
        let episode = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "series".into(),
            s: 1,
            e: 1,
        };
        assert_eq!(
            api_sources(std::slice::from_ref(&release), &episode).len(),
            1
        );

        release.codec = Some("HEVC".into());
        assert!(release_has_verification_candidate(&release));
        assert!(!release_has_usable_source(&release));
        assert!(api_sources(std::slice::from_ref(&release), &episode).is_empty());
        assert!(!release_requires_codec_probe(&release));

        release.codec = None;
        assert!(release_requires_codec_probe(&release));
        release.codec = Some("REMUX".into());
        assert!(release_requires_codec_probe(&release));

        release.provider = ProviderKind::Dramachi;
        assert!(!release_has_verification_candidate(&release));
        assert!(!release_requires_codec_probe(&release));
    }

    #[test]
    fn verifier_release_selection_requires_exact_fingerprint_and_provider() {
        let episode = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "provider-title".into(),
            s: 1,
            e: 2,
        };
        let release = Release {
            provider: ProviderKind::FourKHdHub,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: None,
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(2),
            mirrors: vec![],
            resource_id: None,
        };
        let mut source: OpaqueSource =
            decode(&source_ref(&release, &episode, 0)).expect("encoded source reference");
        let other_release = Release {
            filename: "different.mkv".into(),
            ..release.clone()
        };
        assert!(exact_fourk_release(&[other_release.clone(), release.clone()], &source).is_some());

        source.f = None;
        assert!(exact_fourk_release(std::slice::from_ref(&release), &source).is_none());
        source.f = Some("wrong-fingerprint".into());
        assert!(exact_fourk_release(std::slice::from_ref(&release), &source).is_none());
        source.f = Some(release_fingerprint(&release));
        source.p = ProviderKind::MovieBox;
        assert!(exact_fourk_release(&[release], &source).is_none());
    }

    #[test]
    fn catalog_source_errors_distinguish_blocked_expired_resolution_and_probe_states() {
        let blocked = serde_json::json!({
            "source_status":"active",
            "source_not_expired":true,
            "codec_verification_status":"blocked",
            "video_codec":"hevc"
        });
        assert_eq!(
            catalog_source_ineligible_error(&blocked).1,
            "SOURCE_CODEC_BLOCKED"
        );

        let expired = serde_json::json!({
            "source_status":"active",
            "source_not_expired":false,
            "codec_verification_status":"unverified",
            "video_codec":"h264"
        });
        assert_eq!(
            catalog_source_ineligible_error(&expired).1,
            "SOURCE_EXPIRED"
        );

        let resolver_failed = serde_json::json!({
            "source_status":"active",
            "source_not_expired":true,
            "codec_verification_status":"unverified",
            "last_error_code":"PROVIDER_RESOLUTION_FAILED"
        });
        assert_eq!(
            catalog_source_ineligible_error(&resolver_failed).1,
            "PROVIDER_RESOLUTION_FAILED"
        );

        let probe_timed_out = serde_json::json!({
            "source_status":"active",
            "source_not_expired":true,
            "codec_verification_status":"unverified",
            "last_error_code":"MEDIA_PROBE_TIMEOUT"
        });
        assert_eq!(
            catalog_source_ineligible_error(&probe_timed_out).1,
            "MEDIA_PROBE_TIMEOUT"
        );

        let unknown = serde_json::json!({
            "source_status":"active",
            "source_not_expired":true,
            "codec_verification_status":"unverified",
            "last_error_code":null
        });
        assert_eq!(
            catalog_source_ineligible_error(&unknown).1,
            "CODEC_UNVERIFIED"
        );
    }

    #[test]
    fn transcode_uses_only_validated_direct_media_mirrors() {
        let source = Release {
            provider: ProviderKind::Dramachi,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("H.264".into()),
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
        let mut h265 = source.clone();
        h265.codec = Some("H.265".into());
        assert!(direct_transcode_playback_source(&h265).is_none());
        let mut unverified = source.clone();
        unverified.codec = None;
        assert!(direct_transcode_playback_source(&unverified).is_none());

        let mut with_invalid_first_mirror = source.clone();
        with_invalid_first_mirror.mirrors.insert(
            0,
            crate::providers::models::SourceMirror {
                label: "invalid first candidate".into(),
                resolver_url: "http://cdn.example/untrusted.mkv".into(),
                headers: vec![],
                direct_file: true,
            },
        );
        assert_eq!(
            direct_transcode_playback_source(&with_invalid_first_mirror)
                .unwrap()
                .url,
            "https://cdn.example/video.mkv"
        );

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
            codec: Some("H.264".into()),
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
        release.codec = Some("H.264/AVC".into());
        assert!(release_has_usable_source(&release));
        assert!(releases_have_usable_source(&[release]));
    }

    #[test]
    fn dramachi_direct_mp4_without_codec_is_exposed_as_a_probe_candidate() {
        let episode = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "42999".into(),
            s: 1,
            e: 13,
        };
        let release = Release {
            provider: ProviderKind::Dramachi,
            filename: "Haibane_Renmei_S01E13_KimoiTV.com.mp4".into(),
            quality: Some("360p".into()),
            codec: None,
            language: Some("Japanese".into()),
            size_bytes: Some(64_660_000),
            season: Some(1),
            episode: Some(13),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "Dramachi direct file".into(),
                resolver_url: "https://media.example/cdn/episode.mp4".into(),
                headers: vec![],
                direct_file: true,
            }],
            resource_id: Some("145126".into()),
        };

        assert!(!release_has_usable_source(&release));
        assert!(release_requires_codec_probe(&release));
        assert_eq!(
            codec_ineligibility_error(std::slice::from_ref(&release)),
            None
        );
        let sources = api_sources(std::slice::from_ref(&release), &episode);
        assert_eq!(sources.len(), 1);
        assert_eq!(
            decode::<OpaqueSource>(&sources[0].id).unwrap().p,
            ProviderKind::Dramachi
        );
        assert_eq!(sources[0].codec, None);
    }

    #[test]
    fn source_codec_eligibility_blocks_hevc_and_fails_closed_for_unknown_values() {
        for codec in [
            Some("HEVC"),
            Some("H.265"),
            Some("x265"),
            Some("hvc1"),
            Some("HEVC Main 10"),
        ] {
            assert_eq!(
                source_codec_eligibility(codec),
                SourceCodecEligibility::BlockedH265,
                "codec should be blocked: {codec:?}"
            );
        }
        for codec in [Some("H.264"), Some("AVC"), Some("H.264 / AVC")] {
            assert_eq!(
                source_codec_eligibility(codec),
                SourceCodecEligibility::Verified,
                "verified H.264 codec should be accepted: {codec:?}"
            );
        }
        for codec in [Some("AV1"), Some("vp9"), Some("not-h264")] {
            assert_eq!(
                source_codec_eligibility(codec),
                SourceCodecEligibility::Unverified,
                "non-H.264 codec must fail closed: {codec:?}"
            );
        }
        for codec in [None, Some(""), Some("unknown"), Some("mystery-codec")] {
            assert_eq!(
                source_codec_eligibility(codec),
                SourceCodecEligibility::Unverified,
                "unrecognized codec must fail closed: {codec:?}"
            );
        }
    }

    #[test]
    fn direct_play_verification_bypasses_anizone_without_changing_shared_policy() {
        let release = |provider, codec: Option<&str>| Release {
            provider,
            filename: "episode.mkv".into(),
            quality: Some("1080p".into()),
            codec: codec.map(str::to_string),
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![],
            resource_id: None,
        };

        assert!(!release_requires_direct_play_codec_verification(&release(
            ProviderKind::FourKHdHub,
            Some("H.264"),
        )));
        assert!(release_requires_direct_play_codec_verification(&release(
            ProviderKind::FourKHdHub,
            None,
        )));
        assert!(release_requires_direct_play_codec_verification(&release(
            ProviderKind::Dramachi,
            Some("unknown"),
        )));
        assert!(!release_requires_direct_play_codec_verification(&release(
            ProviderKind::AniZone,
            Some("H.264"),
        )));
        assert!(release_requires_playback_codec_verification(&release(
            ProviderKind::AniZone,
            Some("H.264"),
        )));
        assert_eq!(
            direct_play_compatibility(ProviderKind::AniZone),
            "unverified"
        );
        assert_eq!(
            direct_play_compatibility(ProviderKind::FourKHdHub),
            "verified"
        );
        assert!(!release_requires_playback_codec_verification(&release(
            ProviderKind::BdixCircleFtp,
            Some("H.264"),
        )));
    }

    #[tokio::test]
    async fn anizone_direct_play_skips_sample_verification_and_keeps_codec_unverified() {
        let release = Release {
            provider: ProviderKind::AniZone,
            filename: "Test Anime Episode 1".into(),
            quality: None,
            codec: None,
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![crate::providers::models::SourceMirror {
                label: "AniZone HLS".into(),
                resolver_url: "https://127.0.0.1:9/master.m3u8".into(),
                headers: vec![],
                direct_file: false,
            }],
            resource_id: Some("1".into()),
        };
        let playback = PlaybackSource {
            provider: ProviderKind::AniZone,
            url: "https://127.0.0.1:9/master.m3u8".into(),
            headers: vec![],
            subtitle: None,
            source_label: "AniZone HLS".into(),
            max_height: None,
        };

        let result = verify_direct_playback_codec(&release, &playback)
            .await
            .expect("AniZone codec verification is intentionally bypassed");
        assert_eq!(result.codec, None);
        assert_eq!(direct_play_compatibility(result.provider), "unverified");
        assert_eq!(
            direct_url_type(&reqwest::Url::parse(&playback.url).unwrap(), &result, None),
            Some("hls")
        );
    }

    #[test]
    fn public_source_list_hides_hevc_and_unverified_releases_without_reindexing_ids() {
        let release = |codec: Option<&str>, filename: &str| Release {
            provider: ProviderKind::Addons,
            filename: filename.into(),
            quality: Some("1080p".into()),
            codec: codec.map(str::to_string),
            language: None,
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![],
            resource_id: None,
        };
        let releases = vec![
            release(Some("HEVC"), "hevc.mkv"),
            release(None, "unknown.mkv"),
            release(Some("H.264"), "h264.mkv"),
        ];
        assert_eq!(
            codec_ineligibility_error(std::slice::from_ref(&releases[0])).map(|error| error.0),
            Some("SOURCE_CODEC_BLOCKED")
        );
        assert_eq!(
            codec_ineligibility_error(std::slice::from_ref(&releases[1])).map(|error| error.0),
            Some("CODEC_UNVERIFIED")
        );
        assert!(codec_ineligibility_error(std::slice::from_ref(&releases[2])).is_none());
        let episode = OpaqueRef {
            p: ProviderKind::Addons,
            v: "series:example".into(),
            s: 1,
            e: 1,
        };
        let sources = api_sources(&releases, &episode);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].codec.as_deref(), Some("H.264"));
        let decoded = decode::<OpaqueSource>(&sources[0].id).unwrap();
        assert_eq!(decoded.i, 2);
        let expected_fingerprint = release_fingerprint(&releases[2]);
        assert_eq!(decoded.f.as_deref(), Some(expected_fingerprint.as_str()));
    }

    #[test]
    fn movie_and_episode_source_ids_remain_distinct_at_zero_zero_provider_transport() {
        let release = Release {
            provider: ProviderKind::MovieBox,
            filename: "example.mp4".into(),
            quality: Some("1080p".into()),
            codec: Some("h264".into()),
            language: None,
            size_bytes: None,
            season: Some(0),
            episode: Some(0),
            mirrors: vec![],
            resource_id: Some("resource-1".into()),
        };
        let transport_ref = OpaqueRef {
            p: ProviderKind::MovieBox,
            v: "same-provider-id".into(),
            s: 0,
            e: 0,
        };
        let episode_id = source_ref_for_content(&release, &transport_ref, 0, None);
        let movie_id = source_ref_for_content(&release, &transport_ref, 0, Some("movie"));

        assert_ne!(episode_id, movie_id);
        assert_eq!(
            decode::<OpaqueSource>(&episode_id).unwrap().content_type,
            None
        );
        assert_eq!(
            decode::<OpaqueSource>(&movie_id)
                .unwrap()
                .content_type
                .as_deref(),
            Some("movie")
        );
    }

    #[test]
    fn remux_mode_depends_on_validated_output_not_provider_codec_label() {
        // Codec metadata is a provider hint only. A successful real remux preflight is
        // authoritative even when the hint is absent or names a different codec family.
        for _codec in [None, Some("HEVC"), Some("AV1"), Some("unknown")] {
            assert_eq!(
                media_mode_after_remux_preflight(true),
                Some(MediaMode::Remux)
            );
        }
        assert_eq!(media_mode_after_remux_preflight(false), None);
    }

    #[test]
    fn fragmented_mp4_remux_requires_browser_compatible_codecs() {
        let stream = |codec_type: &str, codec_name: &str| ProbeStream {
            index: None,
            codec_name: Some(codec_name.to_string()),
            codec_type: Some(codec_type.to_string()),
            channels: None,
            disposition: None,
            tags: None,
        };
        assert!(browser_mp4_streams_compatible(&[
            stream("video", "h264"),
            stream("audio", "aac"),
        ]));
        assert!(browser_mp4_streams_compatible(&[stream("video", "h264")]));
        assert!(!browser_mp4_streams_compatible(&[
            stream("video", "hevc"),
            stream("audio", "aac"),
        ]));
        assert!(!browser_mp4_streams_compatible(&[
            stream("video", "h264"),
            stream("audio", "ac3"),
        ]));
        assert!(!browser_mp4_streams_compatible(&[
            stream("video", "h264"),
            stream("audio", "aac"),
            stream("audio", "ac3"),
        ]));
    }

    #[test]
    fn remux_converts_and_preserves_all_eac3_audio_languages() {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/codec-h264-multi-eac3.mkv");
        let output = tempfile::NamedTempFile::new().expect("temporary remux output");
        let ffmpeg = std::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-nostdin",
                "-i",
                source.to_str().unwrap(),
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-ac",
                "2",
                "-sn",
                "-dn",
                "-max_interleave_delta",
                "0",
                "-movflags",
                "+frag_keyframe+empty_moov+default_base_moof",
                "-flush_packets",
                "1",
                "-f",
                "mp4",
                "-y",
                output.path().to_str().unwrap(),
            ])
            .output()
            .expect("ffmpeg is installed for Remux tests");
        assert!(
            ffmpeg.status.success(),
            "ffmpeg failed: {}",
            String::from_utf8_lossy(&ffmpeg.stderr)
        );
        let bytes = std::fs::read(output.path()).expect("read remux output");
        for atom in [b"ftyp".as_slice(), b"moov", b"moof", b"mdat"] {
            assert!(bytes.windows(atom.len()).any(|window| window == atom));
        }
        let probe = std::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_type,codec_name:stream_tags=language,title",
                "-of",
                "json",
                output.path().to_str().unwrap(),
            ])
            .output()
            .expect("ffprobe is installed for Remux tests");
        assert!(
            probe.status.success(),
            "ffprobe failed: {}",
            String::from_utf8_lossy(&probe.stderr)
        );
        let parsed: ProbeOutput =
            serde_json::from_slice(&probe.stdout).expect("valid Remux probe JSON");
        let videos = parsed
            .streams
            .iter()
            .filter(|stream| stream.codec_type.as_deref() == Some("video"))
            .collect::<Vec<_>>();
        let audio = parsed
            .streams
            .iter()
            .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
            .collect::<Vec<_>>();
        assert_eq!(videos.len(), 1);
        assert_eq!(videos[0].codec_name.as_deref(), Some("h264"));
        assert_eq!(audio.len(), 3);
        assert!(
            audio
                .iter()
                .all(|stream| stream.codec_name.as_deref() == Some("aac"))
        );
        let languages = audio
            .iter()
            .filter_map(|stream| tag_value(stream.tags.as_ref(), "language"))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(languages, ["eng", "hin", "kor"].into_iter().collect());
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

                    provider_episode_id: None,
                    provider_season_id: None,
                }],

                provider_season_id: None,
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
            "/api/v1/play/direct",
            "/api/v1/anime/search/stream",
            "/api/v1/catalog/titles/{title_id}/seasons",
            "/api/v1/catalog/titles/{title_id}/seasons/{season_number}/episodes",
            "/api/v1/catalog/episodes/{episode_id}",
            "/api/v1/catalog/episodes/{episode_id}/sources",
            "/api/v1/catalog/sources/{source_id}",
            "/api/v1/catalog/sources/{source_id}/tracks",
            "/api/v1/catalog/sources/{source_id}/play",
            "/api/v1/admin/catalog/jobs/{job_id}",
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/full",
            "/api/v1/anime/{anime_id}/episodes/{episode_id}/stream",
            "/api/v1/episodes/{episode_id}",
            "/api/v1/episodes/{episode_id}/sources/stream",
            "/api/v1/sources/{source_id}/audio-tracks",
            "/api/v1/sources/{source_id}/subtitle-tracks",
            "/api/v1/sources/{source_id}/transcode",
            "/api/v1/sources/{source_id}/remux",
            "/api/v1/sources/{source_id}/direct/{token}",
            "/api/v1/home",
            "/api/v1/home/featured",
            "/api/v1/trending",
            "/api/v1/popular",
            "/api/v1/movies",
            "/api/v1/movies/{movie_id}",
            "/api/v1/movies/{movie_id}/sources",
            "/api/v1/movies/{movie_id}/stream",
            "/api/v1/movies/{movie_id}/playback",
            "/api/v1/k-dramas",
            "/api/v1/categories",
            "/api/stream-proxy",
            "/api/media-info",
            "/api/subtitles/extract",
            "/api/subtitles/assemblyai/{transcript_id}",
            "/api/pretranscode/start",
            "/api/pretranscode/status",
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
    fn movie_playback_identity_does_not_require_or_emit_episode_id() {
        let identity = PlaybackIdentity::Movie {
            movie_id: "opaque-movie-id".into(),
            movie_ref: MovieIdentity {
                provider: ProviderKind::MovieBox,
                provider_id: "movie-123".into(),
            },
        };
        let mut fields = serde_json::Map::new();
        identity.add_response_fields(&mut fields);

        assert_eq!(
            fields.get("movie_id"),
            Some(&serde_json::json!("opaque-movie-id"))
        );
        assert!(!fields.contains_key("anime_id"));
        assert!(!fields.contains_key("episode_id"));
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

    fn mock_search_item(provider: ProviderKind, value: &str) -> CatalogItem {
        CatalogItem {
            id: ProviderMediaId {
                provider,
                value: value.into(),
            },
            title: value.into(),
            media_type: MediaType::Series,
            year: Some("2024".into()),
            poster_url: None,
            season_count: Some(1),
        }
    }

    struct MockRedisSearchCache {
        records: Mutex<HashMap<String, Vec<u8>>>,
        fail_reads: bool,
        fail_writes: bool,
        fail_deletes: bool,
    }

    impl Default for MockRedisSearchCache {
        fn default() -> Self {
            Self {
                records: Mutex::new(HashMap::new()),
                fail_reads: false,
                fail_writes: false,
                fail_deletes: false,
            }
        }
    }

    impl SearchRedisCache for MockRedisSearchCache {
        fn get<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, Option<Vec<u8>>> {
            Box::pin(async move {
                if self.fail_reads {
                    return Err(());
                }
                Ok(self.records.lock().await.get(key).cloned())
            })
        }

        fn set_ex<'a>(
            &'a self,
            key: &'a str,
            _ttl_seconds: u64,
            payload: Vec<u8>,
        ) -> SearchCacheFuture<'a, ()> {
            Box::pin(async move {
                if self.fail_writes {
                    return Err(());
                }
                self.records.lock().await.insert(key.to_string(), payload);
                Ok(())
            })
        }

        fn delete<'a>(&'a self, key: &'a str) -> SearchCacheFuture<'a, ()> {
            Box::pin(async move {
                if self.fail_deletes {
                    return Err(());
                }
                self.records.lock().await.remove(key);
                Ok(())
            })
        }
    }

    fn complete_search_cache_fixture() -> SearchResultSet {
        SearchResultSet {
            items: vec![mock_search_item(ProviderKind::Dramachi, "cache-fixture")],
            failed: false,
            provider_status: HashMap::from([("dramachi".into(), "ok".into())]),
            provider_status_details: HashMap::new(),
            canonical_ids: HashMap::new(),
            verified_aliases: HashMap::new(),
        }
    }

    fn partial_search_cache_fixture() -> SearchResultSet {
        SearchResultSet {
            failed: true,
            provider_status: HashMap::from([("dramachi".into(), "timeout".into())]),
            ..complete_search_cache_fixture()
        }
    }

    #[tokio::test]
    async fn search_cache_insertion_helper_enforces_capacity() {
        let (state, _root) = test_state().await;
        for index in 0..(SEARCH_CACHE_MAX_ENTRIES + 1) {
            insert_search_cache(
                &state,
                &format!("capacity-{index}"),
                complete_search_cache_fixture(),
            )
            .await;
        }
        assert!(state.search_cache.lock().await.len() <= SEARCH_CACHE_MAX_ENTRIES);
    }

    #[tokio::test]
    async fn redis_mock_covers_read_write_success_failures_and_unavailability() {
        let key = "moviebox:search:v4:fixture";
        let result = complete_search_cache_fixture();
        let success = MockRedisSearchCache::default();
        write_redis_search_cache(Some(&success), key, &result, 60).await;
        let loaded = read_redis_search_cache(Some(&success), key)
            .await
            .expect("mocked Redis read should return its written complete result");
        assert_eq!(loaded.provider_status.get("dramachi").unwrap(), "ok");

        let read_failure = MockRedisSearchCache {
            fail_reads: true,
            ..Default::default()
        };
        assert!(
            read_redis_search_cache(Some(&read_failure), key)
                .await
                .is_none()
        );

        let write_failure = MockRedisSearchCache {
            fail_writes: true,
            ..Default::default()
        };
        write_redis_search_cache(Some(&write_failure), key, &result, 60).await;
        assert!(write_failure.records.lock().await.is_empty());

        assert!(read_redis_search_cache(None, key).await.is_none());
        write_redis_search_cache(None, key, &result, 60).await;
    }

    #[tokio::test]
    async fn redis_mock_rejects_corrupt_and_partial_records_and_failed_searches() {
        let key = "moviebox:search:v4:fixture";
        let corrupt = MockRedisSearchCache::default();
        corrupt
            .records
            .lock()
            .await
            .insert(key.to_string(), b"corrupt payload".to_vec());
        assert!(read_redis_search_cache(Some(&corrupt), key).await.is_none());
        assert!(!corrupt.records.lock().await.contains_key(key));

        let partial = MockRedisSearchCache::default();
        let partial_result = partial_search_cache_fixture();
        let partial_payload = serde_json::to_vec(&partial_result).unwrap();
        partial
            .records
            .lock()
            .await
            .insert(key.to_string(), partial_payload);
        assert!(read_redis_search_cache(Some(&partial), key).await.is_none());
        assert!(!partial.records.lock().await.contains_key(key));
        write_redis_search_cache(Some(&partial), key, &partial_result, 60).await;
        assert!(!partial.records.lock().await.contains_key(key));
    }

    #[tokio::test]
    async fn redis_and_memory_cache_use_the_same_provider_aware_key() {
        let providers = [ProviderKind::Dramachi, ProviderKind::AniZone];
        let key = search_cache_identity("  Anime Title  ", &providers);
        let redis = MockRedisSearchCache::default();
        let result = complete_search_cache_fixture();
        write_redis_search_cache(Some(&redis), &key, &result, 60).await;
        let memory: HashMap<String, SearchResultSet> = HashMap::from([(key.clone(), result)]);
        assert!(memory.contains_key(&key));
        assert!(read_redis_search_cache(Some(&redis), &key).await.is_some());
        assert_ne!(
            key,
            search_cache_identity("anime title", &[ProviderKind::Dramachi])
        );
    }

    #[test]
    fn search_cache_identity_is_normalized_order_independent_and_provider_aware() {
        let left = [ProviderKind::Dramachi, ProviderKind::AniZone];
        let right = [ProviderKind::AniZone, ProviderKind::Dramachi];
        let key = search_cache_identity("  Anime Title  ", &left);
        assert_eq!(key, search_cache_identity("anime title", &right));
        assert_ne!(key, search_cache_identity("different title", &right));
        assert_ne!(
            key,
            search_cache_identity("anime title", &[ProviderKind::Dramachi])
        );
        assert!(!key.contains("Anime Title"));
        assert!(!key.contains("token"));
    }

    #[test]
    fn structured_search_status_classifies_outcomes_and_disabled_providers() {
        let items = vec![mock_search_item(ProviderKind::Dramachi, "result")];
        let legacy = HashMap::from([
            ("dramachi".to_string(), "ok".to_string()),
            ("anizone".to_string(), "ok".to_string()),
            ("fourkhdhub".to_string(), "timeout".to_string()),
            ("moviebox".to_string(), "upstream error".to_string()),
            (
                "bdix_circleftp".to_string(),
                "partial: later page failed".to_string(),
            ),
        ]);
        let enabled = [
            ProviderKind::Dramachi,
            ProviderKind::AniZone,
            ProviderKind::FourKHdHub,
            ProviderKind::MovieBox,
            ProviderKind::BdixCircleFtp,
        ];
        let details = search_provider_status_details(&items, &legacy, &enabled);
        assert_eq!(details["dramachi"].status, SearchProviderState::Success);
        assert_eq!(details["dramachi"].result_count, 1);
        assert_eq!(details["anizone"].status, SearchProviderState::Empty);
        assert_eq!(details["fourkhdhub"].status, SearchProviderState::Timeout);
        assert_eq!(details["moviebox"].status, SearchProviderState::Error);
        assert_eq!(
            details["bdix_circleftp"].status,
            SearchProviderState::Partial
        );
        assert_eq!(
            details["bdix_dhakaflix"].status,
            SearchProviderState::Disabled
        );
    }

    #[test]
    fn provider_search_errors_never_expose_credentials_or_private_urls() {
        let message = safe_provider_search_error(
            ProviderKind::Dramachi,
            &ProviderError::Unavailable(
                "https://private.example/api?token=secret Bearer abc123".to_string(),
            ),
        );
        assert_eq!(message, "Dramachi unavailable.");
        assert!(!message.contains("private.example"));
        assert!(!message.contains("secret"));
        assert!(!message.contains("abc123"));
    }

    #[test]
    fn old_and_new_search_cache_records_deserialize() {
        let old = serde_json::json!({
            "items": [],
            "failed": false,
            "provider_status": {"dramachi": "ok"}
        });
        let old_record: SearchResultSet = serde_json::from_value(old).unwrap();
        assert!(old_record.provider_status_details.is_empty());
        let old_structured: SearchResultSet = serde_json::from_value(serde_json::json!({
            "items": [],
            "failed": false,
            "provider_status": {"dramachi": "ok"},
            "provider_status_details": {
                "dramachi": {
                    "provider": "dramachi",
                    "status": "EMPTY",
                    "result_count": 0
                }
            }
        }))
        .unwrap();
        let prior_details = &old_structured.provider_status_details["dramachi"];
        assert_eq!(prior_details.status, SearchProviderState::Empty);
        assert_eq!(prior_details.pages_attempted, None);
        assert_eq!(prior_details.duration_ms, None);
        let new = SearchResultSet {
            items: vec![],
            failed: false,
            provider_status: HashMap::from([("dramachi".into(), "ok".into())]),
            provider_status_details: HashMap::from([(
                "dramachi".into(),
                SearchProviderStatus {
                    provider: "dramachi".into(),
                    status: SearchProviderState::Empty,
                    result_count: 0,
                    pages_attempted: None,
                    pages_successful: None,
                    duration_ms: None,
                    timestamp: None,
                    error_code: None,
                    message: None,
                },
            )]),
            canonical_ids: HashMap::new(),
            verified_aliases: HashMap::new(),
        };
        let roundtrip: SearchResultSet =
            serde_json::from_slice(&serde_json::to_vec(&new).unwrap()).unwrap();
        assert_eq!(
            roundtrip.provider_status_details["dramachi"].status,
            SearchProviderState::Empty
        );
    }

    #[test]
    fn corrupt_and_partial_search_cache_entries_are_rejected() {
        assert!(decode_complete_search_cache_record(b"not json").is_none());
        let partial = SearchResultSet {
            items: vec![],
            failed: true,
            provider_status: HashMap::from([("dramachi".into(), "timeout".into())]),
            provider_status_details: HashMap::new(),
            canonical_ids: HashMap::new(),
            verified_aliases: HashMap::new(),
        };
        let encoded = serde_json::to_vec(&partial).unwrap();
        assert!(decode_complete_search_cache_record(&encoded).is_none());
        assert!(!is_complete_search_cacheable(&partial));
    }

    #[tokio::test]
    async fn deterministic_provider_timeout_does_not_cancel_successful_provider() {
        let timeout = tokio::time::timeout(
            Duration::from_millis(10),
            search_provider_pages(true, |_page| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<Vec<CatalogItem>, ProviderError>(Vec::new())
            }),
        );
        let success = search_provider_pages(false, |_page| async {
            Ok::<Vec<CatalogItem>, ProviderError>(vec![mock_search_item(
                ProviderKind::Dramachi,
                "success",
            )])
        });
        let (timed_out, successful) = tokio::join!(timeout, success);
        assert!(timed_out.is_err());
        assert_eq!(successful.unwrap().items.len(), 1);
    }

    #[tokio::test]
    async fn deterministic_provider_outage_isolated_from_success() {
        let failed = search_provider_pages(false, |_page| async {
            Err::<Vec<CatalogItem>, _>(ProviderError::Unavailable("fixture outage".into()))
        });
        let successful = search_provider_pages(false, |_page| async {
            Ok::<Vec<CatalogItem>, ProviderError>(vec![mock_search_item(
                ProviderKind::FourKHdHub,
                "available",
            )])
        });
        let (failed, successful) = tokio::join!(failed, successful);
        assert!(matches!(failed, Err(ProviderError::Unavailable(_))));
        assert_eq!(successful.unwrap().items.len(), 1);
    }

    #[tokio::test]
    async fn incremental_events_arrive_in_provider_completion_order() {
        let (sender, mut receiver) = mpsc::channel(4);
        let slow_sender = sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            slow_sender
                .send(SearchStreamEvent::Succeeded {
                    provider: ProviderKind::Dramachi,
                    items: vec![mock_search_item(ProviderKind::Dramachi, "slow")],
                    partial_error: None,
                    metadata: SearchProviderExecutionMetadata::default(),
                })
                .await
                .unwrap();
        });
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            sender
                .send(SearchStreamEvent::Succeeded {
                    provider: ProviderKind::AniZone,
                    items: vec![mock_search_item(ProviderKind::AniZone, "fast")],
                    partial_error: None,
                    metadata: SearchProviderExecutionMetadata::default(),
                })
                .await
                .unwrap();
        });
        let first = tokio::time::timeout(Duration::from_millis(25), receiver.recv())
            .await
            .expect("fast provider must arrive before slow provider")
            .expect("event channel should remain open");
        assert!(matches!(
            first,
            SearchStreamEvent::Succeeded {
                provider: ProviderKind::AniZone,
                ..
            }
        ));
        let second = receiver.recv().await.expect("slow provider event");
        assert!(matches!(
            second,
            SearchStreamEvent::Succeeded {
                provider: ProviderKind::Dramachi,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn incremental_provider_error_isolated_from_success_event() {
        let (sender, mut receiver) = mpsc::channel(2);
        sender
            .send(SearchStreamEvent::Failed {
                provider: ProviderKind::FourKHdHub,
                status: "upstream unavailable".into(),
                metadata: SearchProviderExecutionMetadata::default(),
            })
            .await
            .unwrap();
        sender
            .send(SearchStreamEvent::Succeeded {
                provider: ProviderKind::AniZone,
                items: vec![mock_search_item(ProviderKind::AniZone, "available")],
                partial_error: None,
                metadata: SearchProviderExecutionMetadata::default(),
            })
            .await
            .unwrap();
        assert!(matches!(
            receiver.recv().await,
            Some(SearchStreamEvent::Failed {
                provider: ProviderKind::FourKHdHub,
                ..
            })
        ));
        assert!(matches!(
            receiver.recv().await,
            Some(SearchStreamEvent::Succeeded {
                provider: ProviderKind::AniZone,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn disconnected_stream_cancels_outstanding_provider_tasks() {
        let mut tasks = JoinSet::new();
        tasks.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        tasks.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
        abort_search_tasks(&mut tasks).await;
        assert!(tasks.join_next().await.is_none());
    }

    #[tokio::test]
    async fn deterministic_empty_response_is_safe_and_pagination_stops() {
        let mut pages = Vec::new();
        let result = search_provider_pages(true, |page| {
            pages.push(page);
            async { Ok::<Vec<CatalogItem>, ProviderError>(Vec::new()) }
        })
        .await
        .unwrap();
        assert!(result.items.is_empty());
        assert_eq!(pages, vec![1]);
    }

    #[tokio::test]
    async fn deterministic_pagination_deduplicates_but_preserves_provider_identity() {
        let mut requested_pages = Vec::new();
        let result = search_provider_pages(true, |page| {
            requested_pages.push(page);
            async move {
                Ok::<Vec<CatalogItem>, ProviderError>(match page {
                    1 => vec![
                        mock_search_item(ProviderKind::Dramachi, "same"),
                        mock_search_item(ProviderKind::FourKHdHub, "same"),
                    ],
                    2 => vec![mock_search_item(ProviderKind::Dramachi, "same")],
                    _ => Vec::new(),
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(requested_pages, vec![1, 2, 3]);
        assert_eq!(result.items.len(), 2);
        assert!(
            result
                .items
                .iter()
                .any(|item| item.id.provider == ProviderKind::Dramachi)
        );
        assert!(
            result
                .items
                .iter()
                .any(|item| item.id.provider == ProviderKind::FourKHdHub)
        );
    }

    #[derive(Debug, Deserialize)]
    struct ApkSearchResponseContract {
        results: Vec<ApkSearchItemContract>,
        next_cursor: Option<String>,
        partial: Option<bool>,
    }

    #[derive(Debug, Deserialize)]
    struct ApkSearchItemContract {
        id: String,
        title: String,
        media_type: Option<String>,
        year: Option<String>,
        poster_url: Option<String>,
        season_count: Option<usize>,
    }

    #[test]
    fn apk_search_contract_accepts_success_empty_partial_and_source_independent_results() {
        let successful_item = ApiCatalogItem {
            id: "backend-opaque-id".into(),
            canonical_id: Some("canonical-123".into()),
            title: "Fixture Anime".into(),
            media_type: MediaType::Series,
            year: Some("2024".into()),
            poster_url: None,
            season_count: Some(1),
            provider_mappings: vec![ApiProviderMapping {
                id: "provider-opaque-id".into(),
                provider: ProviderKind::Dramachi,
            }],
        };
        let cases = [
            SearchResponse {
                results: vec![successful_item],
                next_cursor: Some("cursor-2".into()),
                partial: false,
                provider_status: HashMap::new(),
                provider_status_details: HashMap::new(),
                language_intent: None,
                verified_language_matches: None,
                message: None,
            },
            SearchResponse {
                results: Vec::new(),
                next_cursor: None,
                partial: false,
                provider_status: HashMap::new(),
                provider_status_details: HashMap::new(),
                language_intent: None,
                verified_language_matches: None,
                message: None,
            },
            SearchResponse {
                results: vec![ApiCatalogItem {
                    id: "partial-opaque-id".into(),
                    canonical_id: None,
                    title: "No Playable Source Fixture".into(),
                    media_type: MediaType::Series,
                    year: None,
                    poster_url: None,
                    season_count: None,
                    provider_mappings: Vec::new(),
                }],
                next_cursor: None,
                partial: true,
                provider_status: HashMap::from([("fourkhdhub".into(), "timeout".into())]),
                provider_status_details: HashMap::new(),
                language_intent: None,
                verified_language_matches: None,
                message: Some("Partial provider results".into()),
            },
        ];
        let parsed = cases
            .iter()
            .map(|response| {
                serde_json::from_value::<ApkSearchResponseContract>(
                    serde_json::to_value(response).unwrap(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(parsed[0].results[0].id, "backend-opaque-id");
        assert_eq!(parsed[0].results[0].title, "Fixture Anime");
        assert_eq!(parsed[0].results[0].media_type.as_deref(), Some("series"));
        assert_eq!(parsed[0].results[0].year.as_deref(), Some("2024"));
        assert_eq!(parsed[0].results[0].season_count, Some(1));
        assert_eq!(parsed[0].next_cursor.as_deref(), Some("cursor-2"));
        assert!(parsed[1].results.is_empty());
        assert!(parsed[2].partial == Some(true));
        assert_eq!(parsed[2].results[0].id, "partial-opaque-id");
        assert!(parsed[2].results[0].poster_url.is_none());
    }

    // --- Master Fix 8: Comprehensive Multi-Provider Discovery, Mapping & Fallback Tests ---

    #[tokio::test]
    async fn pagination_does_not_stop_on_duplicate_only_page_when_page_three_has_new_valid_anime() {
        let mut requested_pages = Vec::new();
        let result = search_provider_pages(true, |page| {
            requested_pages.push(page);
            async move {
                Ok::<Vec<CatalogItem>, ProviderError>(match page {
                    1 => (0..10)
                        .map(|i| mock_search_item(ProviderKind::Dramachi, &format!("anime-{i}")))
                        .collect(),
                    2 => (0..10)
                        .map(|i| mock_search_item(ProviderKind::Dramachi, &format!("anime-{i}")))
                        .collect(),
                    3 => vec![mock_search_item(
                        ProviderKind::Dramachi,
                        "anime-page-3-valid",
                    )],
                    _ => Vec::new(),
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(requested_pages, vec![1, 2, 3]);
        assert_eq!(result.items.len(), 11);
        assert!(
            result
                .items
                .iter()
                .any(|item| item.id.value == "anime-page-3-valid")
        );
    }

    #[tokio::test]
    async fn pagination_finds_valid_candidate_on_page_one_two_and_three() {
        for target_page in 1..=3 {
            let mut requested_pages = Vec::new();
            let result = search_provider_pages(true, |page| {
                requested_pages.push(page);
                async move {
                    Ok::<Vec<CatalogItem>, ProviderError>(if page == target_page {
                        vec![mock_search_item(ProviderKind::AniZone, "target-candidate")]
                    } else if page < target_page {
                        vec![mock_search_item(
                            ProviderKind::AniZone,
                            &format!("filler-{page}"),
                        )]
                    } else {
                        Vec::new()
                    })
                }
            })
            .await
            .unwrap();
            assert!(
                result
                    .items
                    .iter()
                    .any(|item| item.id.value == "target-candidate")
            );
            assert!(requested_pages.contains(&target_page));
        }
    }

    #[tokio::test]
    async fn pagination_unsupported_provider_queries_only_page_one() {
        let mut requested_pages = Vec::new();
        let result = search_provider_pages(false, |page| {
            requested_pages.push(page);
            async move {
                Ok::<Vec<CatalogItem>, ProviderError>(vec![mock_search_item(
                    ProviderKind::FourKHdHub,
                    "item-page-1",
                )])
            }
        })
        .await
        .unwrap();
        assert_eq!(requested_pages, vec![1]);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id.value, "item-page-1");
    }

    #[tokio::test]
    async fn pagination_deduplicates_identical_ids_across_pages() {
        let result = search_provider_pages(true, |page| async move {
            Ok::<Vec<CatalogItem>, ProviderError>(match page {
                1 => vec![mock_search_item(ProviderKind::Dramachi, "dup-id")],
                2 => vec![mock_search_item(ProviderKind::Dramachi, "dup-id")],
                3 => vec![mock_search_item(ProviderKind::Dramachi, "dup-id")],
                _ => Vec::new(),
            })
        })
        .await
        .unwrap();
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id.value, "dup-id");
    }

    #[tokio::test]
    async fn provider_execution_metadata_counts_attempted_and_successful_pages() {
        let progress = Arc::new(SearchPageProgress::default());
        let result =
            search_provider_pages_with_progress(true, Arc::clone(&progress), |page| async move {
                match page {
                    1 => Ok(vec![mock_search_item(ProviderKind::Dramachi, "page-one")]),
                    2 => Err(ProviderError::Unavailable("fixture failure".into())),
                    _ => Ok(Vec::new()),
                }
            })
            .await
            .unwrap();
        assert_eq!(result.items.len(), 1);
        let metadata = execution_metadata(true, &progress, Duration::from_millis(17));
        assert_eq!(metadata.pages_attempted, Some(2));
        assert_eq!(metadata.pages_successful, Some(1));
        assert_eq!(metadata.duration_ms, 17);
        assert!(metadata.timestamp_unix_ms > 0);

        let legacy = HashMap::from([("dramachi".to_string(), "partial: page failed".to_string())]);
        let enabled = [ProviderKind::Dramachi];
        let mut details = search_provider_status_details(&result.items, &legacy, &enabled);
        apply_execution_metadata(
            &mut details,
            &HashMap::from([("dramachi".to_string(), metadata.clone())]),
        );
        assert_eq!(details["dramachi"].status, SearchProviderState::Partial);
        assert_eq!(details["dramachi"].result_count, 1);
        assert_eq!(details["dramachi"].pages_attempted, Some(2));
        assert_eq!(details["dramachi"].pages_successful, Some(1));
        assert_eq!(details["dramachi"].duration_ms, Some(17));
        assert_eq!(
            details["dramachi"].timestamp,
            Some(metadata.timestamp_unix_ms)
        );

        let non_paged_progress = SearchPageProgress::default();
        let non_paged = execution_metadata(false, &non_paged_progress, Duration::from_millis(3));
        assert_eq!(non_paged.pages_attempted, None);
        assert_eq!(non_paged.pages_successful, None);
    }

    #[tokio::test]
    async fn pagination_preserves_page_one_when_page_two_fails_and_reports_partial() {
        let mut requested_pages = Vec::new();
        let result = search_provider_pages(true, |page| {
            requested_pages.push(page);
            async move {
                match page {
                    1 => Ok(vec![mock_search_item(ProviderKind::Dramachi, "page-one")]),
                    2 => Err(ProviderError::Parsing("page two fixture failure".into())),
                    _ => Ok(Vec::new()),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(requested_pages, vec![1, 2]);
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].id.value, "page-one");
        let error = result.partial_error.expect("later-page error is retained");
        assert_eq!(
            error,
            ProviderError::Parsing("page two fixture failure".into())
        );
        assert_eq!(
            partial_provider_status(ProviderKind::Dramachi, &error),
            "partial: Dramachi parse error."
        );
    }

    #[tokio::test]
    async fn pagination_preserves_pages_one_and_two_when_page_three_fails() {
        let mut requested_pages = Vec::new();
        let result = search_provider_pages(true, |page| {
            requested_pages.push(page);
            async move {
                match page {
                    1 => Ok(vec![mock_search_item(ProviderKind::AniZone, "page-one")]),
                    2 => Ok(vec![mock_search_item(ProviderKind::AniZone, "page-two")]),
                    3 => Err(ProviderError::Unavailable("page three unavailable".into())),
                    _ => Ok(Vec::new()),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(requested_pages, vec![1, 2, 3]);
        assert_eq!(result.items.len(), 2);
        assert!(result.partial_error.is_some());
        assert_eq!(
            partial_provider_status(
                ProviderKind::AniZone,
                result.partial_error.as_ref().unwrap()
            ),
            "partial: AniZone unavailable."
        );
    }

    #[tokio::test]
    async fn partial_provider_failure_does_not_discard_another_provider_success() {
        let partial = search_provider_pages(true, |page| async move {
            match page {
                1 => Ok(vec![mock_search_item(
                    ProviderKind::Dramachi,
                    "partial-result",
                )]),
                2 => Err(ProviderError::Network("page two disconnected".into())),
                _ => Ok(Vec::new()),
            }
        });
        let successful = search_provider_pages(true, |page| async move {
            if page == 1 {
                Ok(vec![mock_search_item(
                    ProviderKind::FourKHdHub,
                    "independent-result",
                )])
            } else {
                Ok(Vec::new())
            }
        });
        let (partial, successful) = tokio::join!(partial, successful);
        let partial = partial.unwrap();
        let successful = successful.unwrap();
        assert_eq!(partial.items[0].id.value, "partial-result");
        assert!(partial.partial_error.is_some());
        assert_eq!(successful.items[0].id.value, "independent-result");
        assert!(successful.partial_error.is_none());
    }

    #[tokio::test]
    async fn pagination_page_timeout_and_failure_isolation() {
        let timeout = tokio::time::timeout(
            Duration::from_millis(15),
            search_provider_pages(true, |_page| async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<Vec<CatalogItem>, ProviderError>(Vec::new())
            }),
        )
        .await;
        assert!(timeout.is_err());

        // Independent provider call succeeds despite another failing
        let success = search_provider_pages(true, |page| async move {
            if page == 1 {
                Ok(vec![mock_search_item(
                    ProviderKind::AniZone,
                    "isolated-success",
                )])
            } else {
                Ok(Vec::new())
            }
        })
        .await;
        assert!(success.is_ok());
        assert_eq!(success.unwrap().items.len(), 1);
    }

    #[test]
    fn identity_validation_matching_title_and_media_type_accepted() {
        let base = synthetic_anime(ProviderKind::FourKHdHub, 1, 1, "Episode 1");
        let candidate = synthetic_anime(ProviderKind::AniZone, 1, 1, "Episode 1");
        assert!(same_anime_identity(&base, &candidate));
    }

    #[test]
    fn identity_validation_wrong_media_type_rejected() {
        let mut base = synthetic_anime(ProviderKind::FourKHdHub, 1, 1, "Episode 1");
        let mut candidate = synthetic_anime(ProviderKind::AniZone, 1, 1, "Episode 1");
        candidate.media_type = MediaType::Movie;
        assert!(!same_anime_identity(&base, &candidate));
        base.media_type = MediaType::Movie;
        candidate.media_type = MediaType::Series;
        assert!(!same_anime_identity(&base, &candidate));
    }

    #[test]
    fn identity_validation_wrong_franchise_rejected() {
        let mut base = synthetic_anime(ProviderKind::FourKHdHub, 1, 1, "Episode 1");
        base.title = "Naruto (2002)".into();
        let mut candidate = synthetic_anime(ProviderKind::AniZone, 1, 1, "Episode 1");
        candidate.title = "Naruto Shippuden".into();
        assert!(!same_anime_identity(&base, &candidate));
    }

    #[test]
    fn identity_validation_incompatible_year_rejected() {
        let mut base = synthetic_anime(ProviderKind::FourKHdHub, 1, 1, "Episode 1");
        base.year = Some("2017".into());
        let mut candidate = synthetic_anime(ProviderKind::AniZone, 1, 1, "Episode 1");
        candidate.year = Some("2023".into());
        assert!(!same_anime_identity(&base, &candidate));
    }

    #[test]
    fn identity_validation_ambiguous_candidate_rejected_by_mapping() {
        let candidate1 = (
            ProviderMediaId {
                provider: ProviderKind::Dramachi,
                value: "dramachi-1".into(),
            },
            synthetic_anime(ProviderKind::Dramachi, 1, 1, "Episode 1"),
        );
        let candidate2 = (
            ProviderMediaId {
                provider: ProviderKind::Dramachi,
                value: "dramachi-2".into(),
            },
            synthetic_anime(ProviderKind::Dramachi, 1, 1, "Episode 1"),
        );
        let mut by_provider: HashMap<ProviderKind, Vec<(ProviderMediaId, MediaDetails)>> =
            HashMap::new();
        by_provider
            .entry(ProviderKind::Dramachi)
            .or_default()
            .push(candidate1);
        by_provider
            .entry(ProviderKind::Dramachi)
            .or_default()
            .push(candidate2);
        let verified = by_provider
            .into_values()
            .filter(|candidates| candidates.len() == 1)
            .flatten()
            .collect::<Vec<_>>();
        assert!(verified.is_empty(), "Ambiguous candidates must be rejected");
    }

    #[tokio::test]
    async fn identity_recovery_when_no_trusted_identity_exists_rejects_unsafe_discovery() {
        let (state, _root) = test_state().await;
        let (discovered, status) = discover_unmapped_provider_details(&state, &[], None).await;
        assert!(discovered.is_empty());
        assert!(status.is_empty());
    }

    #[test]
    fn episode_mapping_matching_season_and_episode() {
        let source = crate::providers::models::Episode {
            season: 1,
            number: 10,
            title: Some("Destiny".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate = synthetic_anime(ProviderKind::Dramachi, 1, 10, "Destiny");
        let matched = matching_episode(&source, &candidate);
        assert!(matched.is_some());
        let ep = matched.unwrap();
        assert_eq!(ep.season, 1);
        assert_eq!(ep.number, 10);
    }

    #[test]
    fn episode_mapping_same_episode_from_multiple_providers() {
        let source = crate::providers::models::Episode {
            season: 1,
            number: 10,
            title: Some("Destiny".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate1 = synthetic_anime(ProviderKind::Dramachi, 1, 10, "Destiny");
        let candidate2 = synthetic_anime(ProviderKind::AniZone, 1, 10, "Destiny");
        assert_eq!(matching_episode(&source, &candidate1).unwrap().number, 10);
        assert_eq!(matching_episode(&source, &candidate2).unwrap().number, 10);
    }

    #[test]
    fn episode_mapping_wrong_episode_rejected() {
        let source = crate::providers::models::Episode {
            season: 1,
            number: 10,
            title: Some("Destiny".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate = synthetic_anime(ProviderKind::Dramachi, 1, 11, "A Different Day");
        assert!(matching_episode(&source, &candidate).is_none());
    }

    #[test]
    fn episode_mapping_special_episode_handled_safely() {
        let special = crate::providers::models::Episode {
            season: 1,
            number: 10,
            title: Some("Black Clover OVA 1".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let regular = synthetic_anime(ProviderKind::Dramachi, 1, 10, "Destiny");
        assert!(matching_episode(&special, &regular).is_none());
    }

    #[test]
    fn episode_mapping_generic_title_does_not_cause_false_merging_across_seasons() {
        let source = crate::providers::models::Episode {
            season: 2,
            number: 1,
            title: Some("Episode 1".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate = synthetic_anime(ProviderKind::AniZone, 1, 1, "Episode 1");
        assert!(matching_episode(&source, &candidate).is_none());
    }

    #[test]
    fn episode_mapping_conflicting_meaningful_titles_handled_conservatively() {
        let source = crate::providers::models::Episode {
            season: 1,
            number: 10,
            title: Some("The Light That Pierces Darkness".into()),
            overview: None,

            provider_episode_id: None,
            provider_season_id: None,
        };
        let candidate = synthetic_anime(
            ProviderKind::Dramachi,
            1,
            10,
            "A Completely Different Episode Title",
        );
        assert!(matching_episode(&source, &candidate).is_none());
    }

    #[tokio::test]
    async fn playback_fallback_primary_fails_and_alternate_succeeds_with_timeout_isolation() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-title".into(),
            s: 1,
            e: 5,
        };
        let timed_out_alt = OpaqueRef {
            p: ProviderKind::Dramachi,
            v: "dramachi-title".into(),
            s: 1,
            e: 5,
        };
        let successful_alt = OpaqueRef {
            p: ProviderKind::BdixCircleFtp,
            v: "bdix-title".into(),
            s: 1,
            e: 5,
        };

        let original = Release {
            episode: Some(5),
            ..verified_release(primary.p, "orig.mkv", "https://cdn.example/orig.mkv")
        };
        let success_rel = Release {
            episode: Some(5),
            ..verified_release(successful_alt.p, "succ.mkv", "https://cdn.example/succ.mkv")
        };
        let source_id = source_ref(&original, &primary, 0);
        let expected_source_id = api_sources(std::slice::from_ref(&success_rel), &successful_alt)
            [0]
        .id
        .clone();

        let (state, _root) = test_state().await;
        override_releases(
            &state,
            &primary,
            Err(ProviderError::Unavailable("primary unavailable".into())),
        )
        .await;
        state.test_release_delays.lock().await.insert(
            release_cache_key(&timed_out_alt),
            SOURCE_TIMEOUT + Duration::from_millis(50),
        );
        cache_releases(&state, &successful_alt, vec![success_rel]).await;

        let source = decode::<OpaqueSource>(&source_id).unwrap();
        set_fallback_refs(&state, &source, vec![timed_out_alt, successful_alt]).await;

        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"]["source_id"], expected_source_id);
        assert_eq!(body["data"]["provider"], "bdix_circleftp");
    }

    #[tokio::test]
    async fn playback_fallback_rejects_wrong_anime_identity() {
        let primary = OpaqueRef {
            p: ProviderKind::FourKHdHub,
            v: "primary-anime".into(),
            s: 1,
            e: 2,
        };
        let original = verified_release(primary.p, "orig.mkv", "https://cdn.example/orig.mkv");
        let source_id = source_ref(&original, &primary, 0);

        let (state, _root) = test_state().await;
        cache_releases(&state, &primary, Vec::new()).await;

        let source = decode::<OpaqueSource>(&source_id).unwrap();
        set_fallback_refs(&state, &source, Vec::new()).await;

        let (status, body) = invoke_direct_route(state, &source_id).await;
        assert_eq!(status, 404);
        assert_eq!(body["error"]["code"], "SOURCE_NOT_FOUND");
    }
}
