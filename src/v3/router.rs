use crate::providers::models::{EpisodeProvenance, MediaType, ProviderKind};
use crate::v3::audio_sources::{
    ResolveFailure, SourceCollectionResult, collect_episode_sources, compute_audio_availability,
    group_sources_by_provider, resolve_episode_stream,
};
use crate::v3::home::{CachedHomeData, HomeResponse, build_home_catalog};
use crate::v3::matching::{build_canonical_structure, merge_search_results, query_matches_anime};
use crate::v3::models::*;
use crate::v3::providers::{ProviderRegistry, provider_display_name, provider_key};
use crate::v3::session::{SessionConfig, SessionLookupError, SessionStore, hash_session_id};
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, Semaphore};

static REQ_SEQ: AtomicU64 = AtomicU64::new(1);

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);
const SEARCH_CACHE_TTL: Duration = Duration::from_secs(600);
const ANIME_CACHE_TTL: Duration = Duration::from_secs(1200);
const SOURCE_CACHE_TTL: Duration = Duration::from_secs(1200);
const HOME_CACHE_TTL: Duration = Duration::from_secs(1200);

type CachedSearchEntry = (
    Instant,
    Vec<NormalizedAnime>,
    Vec<ProviderFailureInfo>,
    usize,
    BTreeMap<String, String>,
);

#[derive(Clone)]
pub struct V3AppState {
    pub sessions: SessionStore,
    pub registry: ProviderRegistry,
    pub rate_limiter: Arc<Mutex<HashMap<IpAddr, (Instant, u32)>>>,
    pub rate_limit_per_min: u32,
    pub global_search_cache: Arc<RwLock<HashMap<String, CachedSearchEntry>>>,
    pub global_anime_cache: Arc<RwLock<HashMap<String, (Instant, CanonicalAnime)>>>,
    pub global_source_cache:
        Arc<RwLock<HashMap<String, (Instant, SourceCollectionResult, AudioAvailability)>>>,
    pub home_cache: Arc<RwLock<Option<CachedHomeData>>>,
    pub home_refresh_lock: Arc<Mutex<()>>,
}

impl V3AppState {
    pub fn from_env() -> Self {
        let rate_limit_per_min = std::env::var("PV_RATE_LIMIT_PER_MIN")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(180);
        Self {
            sessions: SessionStore::new(SessionConfig::from_env()),
            registry: ProviderRegistry::from_env(),
            rate_limiter: Arc::new(Mutex::new(HashMap::new())),
            rate_limit_per_min,
            global_search_cache: Arc::new(RwLock::new(HashMap::new())),
            global_anime_cache: Arc::new(RwLock::new(HashMap::new())),
            global_source_cache: Arc::new(RwLock::new(HashMap::new())),
            home_cache: Arc::new(RwLock::new(None)),
            home_refresh_lock: Arc::new(Mutex::new(())),
        }
    }
}

pub const REGISTERED_ANIME_ROUTES: [&str; 7] = [
    "GET /api/v1/anime/search",
    "GET /api/v1/anime/{anime_id}",
    "GET /api/v1/anime/{anime_id}/seasons",
    "GET /api/v1/seasons/{season_id}/episodes",
    "GET /api/v1/episodes/{episode_id}",
    "GET /api/v1/episodes/{episode_id}/sources",
    "POST /api/v1/episodes/{episode_id}/resolve",
];

pub async fn run() -> std::io::Result<()> {
    let state = V3AppState::from_env();
    state.sessions.spawn_cleanup_task();

    // Pre-warm Home catalog on startup and refresh every 15 minutes in the background.
    {
        let bg_state = state.clone();
        tokio::spawn(async move {
            loop {
                let _lock = bg_state.home_refresh_lock.lock().await;
                if let Some(data) = build_home_catalog(&bg_state.registry).await {
                    let mut w = bg_state.home_cache.write().await;
                    *w = Some(data);
                }
                drop(_lock);
                tokio::time::sleep(Duration::from_secs(900)).await;
            }
        });
    }

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.trim().parse::<u16>().ok())
        .unwrap_or(18080);
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let listener = TcpListener::bind(addr).await?;
    log::info!(
        "prophecy_v3_server_listening addr={addr} routes={}",
        REGISTERED_ANIME_ROUTES.len()
    );

    let conn_sem = Arc::new(Semaphore::new(256));
    loop {
        let (stream, peer) = listener.accept().await?;
        let Ok(permit) = conn_sem.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let state_clone = state.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(err) = handle_connection(stream, peer, state_clone).await {
                log::debug!("v3_conn_err peer={peer} err={err}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    peer: SocketAddr,
    state: V3AppState,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(4096);
    let mut temp = [0u8; 4096];
    let header_end;
    loop {
        let n = tokio::time::timeout(
            Duration::from_secs(10),
            stream.read(&mut buf_chunk(&mut temp)),
        )
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timeout"))??;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&temp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            header_end = pos;
            break;
        }
        if buf.len() > MAX_REQUEST_BYTES {
            return write_error(
                &mut stream,
                413,
                "INVALID_QUERY",
                "Request headers too large",
                None,
            )
            .await
            .map(|_| ());
        }
    }

    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut req_parts = request_line.split_whitespace();
    let method = req_parts.next().unwrap_or("").to_ascii_uppercase();
    let target = req_parts.next().unwrap_or("/");

    let mut headers = HashMap::new();
    let mut content_length: usize = 0;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val.parse().unwrap_or(0);
            }
            headers.insert(key, val);
        }
    }

    if content_length > MAX_BODY_BYTES {
        return write_error(
            &mut stream,
            413,
            "INVALID_QUERY",
            "Request body too large",
            None,
        )
        .await
        .map(|_| ());
    }

    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut temp))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "body timeout"))??;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&temp[..n]);
    }
    body.truncate(content_length);

    if method == "OPTIONS" {
        return write_cors_preflight(&mut stream).await;
    }

    let req_id = format!("pv-{}", REQ_SEQ.fetch_add(1, Ordering::Relaxed));
    let started = Instant::now();

    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let session_hdr = headers.get("x-pv-session").map(String::as_str);
    let session_hash = session_hdr
        .map(hash_session_id)
        .unwrap_or_else(|| "-".to_string());

    if !check_rate_limit(&state, peer.ip()).await {
        log::warn!(
            "v3_req request_id={req_id} session_hash={session_hash} route={path} status=429 error_code=RATE_LIMITED"
        );
        return write_error(
            &mut stream,
            429,
            "RATE_LIMITED",
            "Rate limit exceeded.",
            None,
        )
        .await
        .map(|_| ());
    }

    let res = tokio::time::timeout(
        REQUEST_TIMEOUT,
        dispatch_route(
            &mut stream,
            &method,
            path,
            query,
            &headers,
            &body,
            &state,
            &req_id,
            &session_hash,
        ),
    )
    .await;

    let elapsed = started.elapsed().as_millis();
    match res {
        Ok(Ok(status)) => {
            log::info!(
                "v3_req request_id={req_id} session_hash={session_hash} method={method} route={path} duration_ms={elapsed} status={status}"
            );
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(_) => {
            log::warn!(
                "v3_req request_id={req_id} session_hash={session_hash} method={method} route={path} duration_ms={elapsed} status=504 error_code=INTERNAL_ERROR"
            );
            write_error(
                &mut stream,
                504,
                "INTERNAL_ERROR",
                "Request timed out.",
                None,
            )
            .await
            .map(|_| ())
        }
    }
}

fn buf_chunk(buf: &mut [u8; 4096]) -> &mut [u8] {
    buf
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn check_rate_limit(state: &V3AppState, ip: IpAddr) -> bool {
    let mut map = state.rate_limiter.lock().await;
    let now = Instant::now();
    let entry = map.entry(ip).or_insert((now, 0));
    if now.duration_since(entry.0) >= Duration::from_secs(60) {
        *entry = (now, 1);
        return true;
    }
    entry.1 += 1;
    entry.1 <= state.rate_limit_per_min
}

pub async fn dispatch_route(
    stream: &mut TcpStream,
    method: &str,
    path: &str,
    query: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &V3AppState,
    _req_id: &str,
    _session_hash: &str,
) -> std::io::Result<u16> {
    let trimmed = path.trim_end_matches('/');
    let normalized = if trimmed.is_empty() { "/" } else { trimmed };

    // Infrastructure routes
    if method == "GET" && normalized == "/health" {
        return write_json(stream, 200, &serde_json::json!({"status": "ok"})).await;
    }
    if method == "GET" && normalized == "/ready" {
        let enabled = state.registry.enabled_providers().len();
        return write_json(
            stream,
            200,
            &serde_json::json!({"status": "ready", "enabled_providers": enabled}),
        )
        .await;
    }

    // Route 0: GET /api/v1/home
    if method == "GET" && normalized == "/api/v1/home" {
        return handle_home(stream, state).await;
    }

    // Route 1: GET /api/v1/anime/search?q={query}
    if method == "GET" && normalized == "/api/v1/anime/search" {
        return handle_search(stream, query, state).await;
    }

    let parts: Vec<&str> = normalized.trim_start_matches('/').split('/').collect();
    if parts.len() >= 4 && parts[0] == "api" && parts[1] == "v1" {
        let session_hdr = headers.get("x-pv-session").map(String::as_str);
        let session = match state.sessions.get_session(session_hdr).await {
            Ok(s) => s,
            Err(SessionLookupError::Required) => {
                return write_error(
                    stream,
                    401,
                    "SESSION_REQUIRED",
                    "Missing X-PV-Session header.",
                    None,
                )
                .await;
            }
            Err(SessionLookupError::Invalid) => {
                return write_error(
                    stream,
                    400,
                    "SESSION_INVALID",
                    "Invalid X-PV-Session token format.",
                    None,
                )
                .await;
            }
            Err(SessionLookupError::Expired) => {
                return write_error(
                    stream,
                    401,
                    "SESSION_EXPIRED",
                    "The temporary session has expired.",
                    None,
                )
                .await;
            }
        };

        // Route 2: GET /api/v1/anime/{anime_id}
        if method == "GET" && parts.len() == 4 && parts[2] == "anime" {
            return handle_anime_details(stream, parts[3], session, state).await;
        }
        // Route 3: GET /api/v1/anime/{anime_id}/seasons
        if method == "GET" && parts.len() == 5 && parts[2] == "anime" && parts[4] == "seasons" {
            return handle_anime_seasons(stream, parts[3], session, state).await;
        }
        // Route 4: GET /api/v1/seasons/{season_id}/episodes
        if method == "GET" && parts.len() == 5 && parts[2] == "seasons" && parts[4] == "episodes" {
            return handle_season_episodes(stream, parts[3], session, state).await;
        }
        // Route 5: GET /api/v1/episodes/{episode_id}
        if method == "GET" && parts.len() == 4 && parts[2] == "episodes" {
            return handle_episode_details(stream, parts[3], session, state).await;
        }
        // Route 6: GET /api/v1/episodes/{episode_id}/sources
        if method == "GET" && parts.len() == 5 && parts[2] == "episodes" && parts[4] == "sources" {
            return handle_episode_sources(stream, parts[3], session, state).await;
        }
        // Route 7: POST /api/v1/episodes/{episode_id}/resolve
        if method == "POST" && parts.len() == 5 && parts[2] == "episodes" && parts[4] == "resolve" {
            return handle_episode_resolve(stream, parts[3], body, session, state).await;
        }
    }

    write_error(stream, 404, "NOT_FOUND", "Route not found.", None).await
}

fn parse_query_param(query: &str, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

fn build_provider_variants(anime: &CanonicalAnime) -> Vec<ProviderVariantInfo> {
    anime
        .provider_records
        .iter()
        .map(|r| ProviderVariantInfo {
            provider_id: provider_key(r.provider_id).to_string(),
            provider_name: provider_display_name(r.provider_id).to_string(),
            provider_anime_id: r.provider_anime_id.clone(),
            original_title: r.title.clone(),
            declared_language: r.declared_language.clone(),
            declared_language_code: r.declared_language_code.clone(),
            year: r.year,
            media_type: r.media_type,
        })
        .collect()
}

async fn handle_home(
    stream: &mut TcpStream,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let cached_now = {
        let r = state.home_cache.read().await;
        r.clone()
    };
    let (data, was_cached) = if let Some(d) = cached_now {
        if d.built_at.elapsed() >= HOME_CACHE_TTL {
            let bg = state.clone();
            tokio::spawn(async move {
                if let Ok(_guard) = bg.home_refresh_lock.try_lock()
                    && let Some(fresh) = build_home_catalog(&bg.registry).await
                {
                    let mut w = bg.home_cache.write().await;
                    *w = Some(fresh);
                }
            });
        }
        (d, true)
    } else {
        let _lock = state.home_refresh_lock.lock().await;
        let double_check = { state.home_cache.read().await.clone() };
        if let Some(d) = double_check {
            (d, true)
        } else {
            match build_home_catalog(&state.registry).await {
                Some(fresh) => {
                    let mut w = state.home_cache.write().await;
                    *w = Some(fresh.clone());
                    (fresh, false)
                }
                None => {
                    return write_error(
                        stream,
                        502,
                        "HOME_UNAVAILABLE",
                        "Could not build home catalog from upstream metadata.",
                        None,
                    )
                    .await;
                }
            }
        }
    };

    let session = state.sessions.create_session(data.seeded_anime).await;
    let session_id = { session.read().await.session_id.clone() };

    write_json(
        stream,
        200,
        &HomeResponse {
            status: "success",
            session_id,
            cached: was_cached,
            sections: data.sections,
        },
    )
    .await
}

async fn handle_search(
    stream: &mut TcpStream,
    query: &str,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let q = parse_query_param(query, "q").unwrap_or_default();
    let trimmed = q.trim();
    if trimmed.is_empty() || trimmed.len() > 160 || trimmed.chars().any(|c| c.is_control()) {
        return write_error(
            stream,
            400,
            "INVALID_QUERY",
            "Search query 'q' must be 1..160 printable characters.",
            None,
        )
        .await;
    }

    let cache_key = trimmed.to_lowercase();
    let cached_hit = {
        let guard = state.global_search_cache.read().await;
        guard
            .get(&cache_key)
            .filter(|(ts, ..)| ts.elapsed() < SEARCH_CACHE_TTL)
            .cloned()
    };

    let (normalized, failures, total_enabled, provider_coverage) = if let Some((
        _,
        norm,
        fail,
        tot,
        cov,
    )) = cached_hit
    {
        (norm, fail, tot, cov)
    } else {
        let res = state.registry.search_all(trimmed).await;
        if !res.0.is_empty() {
            let mut w = state.global_search_cache.write().await;
            if w.len() > 500 {
                w.retain(|_, (ts, ..)| ts.elapsed() < SEARCH_CACHE_TTL);
            }
            w.insert(
                cache_key,
                (
                    Instant::now(),
                    res.0.clone(),
                    res.1.clone(),
                    res.2,
                    res.3.clone(),
                ),
            );
        }
        res
    };
    if total_enabled == 0 || (normalized.is_empty() && failures.len() >= total_enabled) {
        return write_error(
            stream,
            502,
            "ALL_PROVIDERS_FAILED",
            "All enabled providers failed to respond.",
            None,
        )
        .await;
    }

    let mut canonical = merge_search_results(normalized, usize::MAX);
    canonical.retain(|a| query_matches_anime(trimmed, a));
    canonical.truncate(state.sessions.config().max_search_results);

    let results: Vec<SearchAnimeItem> = canonical
        .iter()
        .map(|a| {
            let pid = |k: ProviderKind| -> Option<String> {
                a.provider_records
                    .iter()
                    .find(|r| r.provider_id == k)
                    .map(|r| r.provider_anime_id.clone())
            };
            SearchAnimeItem {
                anime_id: a.id.clone(),
                title: a.title.clone(),
                media_type: a.media_type,
                year: a.year,
                poster_url: a.poster_url.clone(),
                description: a.description.clone(),
                genres: a.genres.clone(),
                total_seasons: a.total_seasons,
                total_episodes: a.total_episodes,
                verified_episodes: a.verified_episodes,
                unverified_episodes: a.unverified_episodes,
                synthetic_episodes: a.synthetic_episodes,
                counts_status: if a.details_hydrated {
                    "hydrated"
                } else {
                    "provisional"
                },
                provider_ids: ProviderIdsMap {
                    moviebox: pid(ProviderKind::MovieBox),
                    anizone: pid(ProviderKind::AniZone),
                    dramachi: pid(ProviderKind::Dramachi),
                    fourkhdhub: pid(ProviderKind::FourKHdHub),
                },
                provider_variants: build_provider_variants(a),
            }
        })
        .collect();
    let total = results.len();
    let partial = !failures.is_empty();

    let sess = state.sessions.create_session(canonical).await;
    let session_id = sess.read().await.session_id.clone();

    write_json(
        stream,
        200,
        &SearchResponse {
            status: "success",
            session_id,
            query: trimmed.to_string(),
            total,
            partial,
            provider_coverage,
            provider_failures: failures,
            results,
            all_provider_stream_sources: Vec::new(),
        },
    )
    .await
}

async fn ensure_anime_hydrated(
    anime_id: &str,
    session: &Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> Result<CanonicalAnime, (u16, &'static str, String)> {
    let existing = {
        let guard = session.read().await;
        guard.anime_records.get(anime_id).cloned()
    };
    let Some(mut anime) = existing else {
        return Err((
            404,
            "ANIME_NOT_FOUND",
            "Anime does not exist in this session.".into(),
        ));
    };
    if anime.details_hydrated {
        return Ok(anime);
    }

    let cached_anime = {
        let guard = state.global_anime_cache.read().await;
        guard
            .get(&anime.id)
            .filter(|(ts, _)| ts.elapsed() < ANIME_CACHE_TTL)
            .map(|(_, a)| a.clone())
    };
    if let Some(hit) = cached_anime {
        let mut guard = session.write().await;
        for season in &hit.seasons {
            guard
                .season_index
                .insert(season.id.clone(), hit.id.clone());
            for ep in &season.episodes {
                guard
                    .episode_index
                    .insert(ep.id.clone(), (hit.id.clone(), season.id.clone()));
            }
        }
        guard.anime_records.insert(hit.id.clone(), hit.clone());
        return Ok(hit);
    }

    let futs = anime.provider_records.iter().map(|rec| {
        let reg = state.registry.clone();
        let pid = rec.provider_id;
        let aid = rec.provider_anime_id.clone();
        async move {
            let res = reg.fetch_details(pid, &aid).await;
            (pid, aid, res)
        }
    });
    let outcomes = futures::future::join_all(futs).await;

    for (pid, aid, res) in outcomes {
        if let Some(rec) = anime
            .provider_records
            .iter_mut()
            .find(|r| r.provider_id == pid && r.provider_anime_id == aid)
        {
            match res {
                Ok(det) => {
                    rec.details = Some(det);
                    rec.details_failed = false;
                }
                Err(err) => {
                    log::warn!("v3_details_failed provider={} err={err}", pid.cache_key());
                    rec.details_failed = true;
                }
            }
        }
    }

    build_canonical_structure(&mut anime);

    // Retry failed providers once if Movie or Series yielded 0 verified episodes.
    if anime.verified_episodes == 0
        && anime.provider_records.iter().any(|r| r.details_failed)
    {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let retry_futs = anime
            .provider_records
            .iter()
            .filter(|r| r.details_failed)
            .map(|rec| {
                let reg = state.registry.clone();
                let pid = rec.provider_id;
                let aid = rec.provider_anime_id.clone();
                async move { (pid, aid.clone(), reg.fetch_details(pid, &aid).await) }
            });
        let retry_outcomes = futures::future::join_all(retry_futs).await;
        for (pid, aid, res) in retry_outcomes {
            if let Some(rec) = anime
                .provider_records
                .iter_mut()
                .find(|r| r.provider_id == pid && r.provider_anime_id == aid)
                && let Ok(det) = res
            {
                rec.details = Some(det);
                rec.details_failed = false;
            }
        }
        build_canonical_structure(&mut anime);
    }

    if anime.verified_episodes > 0 {
        let mut w = state.global_anime_cache.write().await;
        if w.len() > 500 {
            w.retain(|_, (ts, _)| ts.elapsed() < ANIME_CACHE_TTL);
        }
        w.insert(anime.id.clone(), (Instant::now(), anime.clone()));
    }

    let mut guard = session.write().await;
    for season in &anime.seasons {
        guard
            .season_index
            .insert(season.id.clone(), anime.id.clone());
        for ep in &season.episodes {
            guard
                .episode_index
                .insert(ep.id.clone(), (anime.id.clone(), season.id.clone()));
        }
    }
    guard.anime_records.insert(anime.id.clone(), anime.clone());
    Ok(anime)
}

fn build_anime_provider_coverage(
    anime: &CanonicalAnime,
    registry: &ProviderRegistry,
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for entry in &registry.entries {
        let key = provider_key(entry.provider_id).to_string();
        if !entry.enabled {
            map.insert(key, "disabled".to_string());
            continue;
        }
        let matching: Vec<_> = anime
            .provider_records
            .iter()
            .filter(|r| r.provider_id == entry.provider_id)
            .collect();
        if !matching.is_empty() {
            let any_ok = matching.iter().any(|r| !r.details_failed);
            let any_fail = matching.iter().any(|r| r.details_failed);
            let status = match (any_ok, any_fail) {
                (true, false) => "success",
                (true, true) => "partial",
                (false, _) => "failed",
            };
            map.insert(key, status.to_string());
        }
    }
    map
}

async fn handle_anime_details(
    stream: &mut TcpStream,
    anime_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let anime = match ensure_anime_hydrated(anime_id, &session, state).await {
        Ok(a) => a,
        Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
    };
    let provider_coverage = build_anime_provider_coverage(&anime, &state.registry);
    let provider_variants = build_provider_variants(&anime);

    write_json(
        stream,
        200,
        &AnimeDetailsResponse {
            status: "success",
            anime_id: anime.id,
            title: anime.title,
            alternative_titles: anime.alternative_titles,
            media_type: anime.media_type,
            year: anime.year,
            description: anime.description,
            poster_url: anime.poster_url,
            genres: anime.genres,
            total_seasons: anime.total_seasons,
            total_episodes: anime.total_episodes,
            verified_episodes: anime.verified_episodes,
            unverified_episodes: anime.unverified_episodes,
            synthetic_episodes: anime.synthetic_episodes,
            season_structure_status: anime.season_structure_status,
            provider_coverage,
            provider_variants,
        },
    )
    .await
}

async fn handle_anime_seasons(
    stream: &mut TcpStream,
    anime_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let anime = match ensure_anime_hydrated(anime_id, &session, state).await {
        Ok(a) => a,
        Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
    };

    // Section 6: Do not expose fake Season 1 to the user for movies.
    // Only expose seasons that contain at least one Verified, non-synthetic episode.
    let seasons: Vec<SeasonItemResponse> = if anime.media_type == MediaType::Movie {
        Vec::new()
    } else {
        anime
            .seasons
            .iter()
            .filter(|s| s.verified_episodes > 0)
            .map(|s| SeasonItemResponse {
                season_id: s.id.clone(),
                season_number: s.season_number,
                episode_count: s.verified_episodes,
                verified_episodes: s.verified_episodes,
                unverified_episodes: 0,
                synthetic_episodes: 0,
                advertised_episode_count: s.advertised_episode_count,
                structure_status: s.structure_status.clone(),
            })
            .collect()
    };

    write_json(
        stream,
        200,
        &AnimeSeasonsResponse {
            status: "success",
            anime_id: anime.id,
            total_seasons: seasons.len(),
            total_episodes: anime.verified_episodes,
            verified_episodes: anime.verified_episodes,
            unverified_episodes: 0,
            synthetic_episodes: 0,
            season_structure_status: anime.season_structure_status,
            seasons,
        },
    )
    .await
}

async fn handle_season_episodes(
    stream: &mut TcpStream,
    season_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    _state: &V3AppState,
) -> std::io::Result<u16> {
    let season_opt = {
        let guard = session.read().await;
        guard.season_index.get(season_id).and_then(|aid| {
            guard
                .anime_records
                .get(aid)
                .and_then(|a| a.seasons.iter().find(|s| s.id == season_id).cloned())
        })
    };
    let Some(season) = season_opt.filter(|s| s.verified_episodes > 0) else {
        return write_error(
            stream,
            404,
            "SEASON_NOT_FOUND",
            "Season does not exist in this session.",
            None,
        )
        .await;
    };

    // Only Verified, non-synthetic episodes are visible to users.
    // Unverified and SyntheticFallback episodes remain preserved internally.
    let episodes: Vec<EpisodeItemResponse> = season
        .episodes
        .iter()
        .filter(|e| {
            e.provenance != EpisodeProvenance::SyntheticFallback
                && e.verification == crate::providers::models::EpisodeVerification::Verified
        })
        .map(|e| EpisodeItemResponse {
            episode_id: e.id.clone(),
            episode_number: e.episode_number,
            title: e.title.clone(),
            metadata_status: e.metadata_status.clone(),
            provenance: e.provenance,
            verification: e.verification,
        })
        .collect();

    write_json(
        stream,
        200,
        &SeasonEpisodesResponse {
            status: "success",
            season_id: season.id,
            total_episodes: episodes.len(),
            verified_episodes: episodes.len(),
            unverified_episodes: 0,
            synthetic_episodes: 0,
            structure_status: season.structure_status,
            episodes,
        },
    )
    .await
}

async fn ensure_episode_sources_hydrated(
    episode_id: &str,
    session: &Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> Result<
    (
        CanonicalEpisode,
        Vec<ProviderSourceCandidate>,
        AudioAvailability,
        BTreeMap<String, String>,
    ),
    (u16, &'static str, String),
> {
    let loc = {
        let guard = session.read().await;
        guard
            .episode_index
            .get(episode_id)
            .cloned()
            .and_then(|(aid, sid)| {
                guard.anime_records.get(&aid).and_then(|a| {
                    a.seasons
                        .iter()
                        .find(|s| s.id == sid)
                        .and_then(|s| s.episodes.iter().find(|e| e.id == episode_id).cloned())
                })
            })
    };
    let Some(mut episode) = loc else {
        return Err((
            404,
            "EPISODE_NOT_FOUND",
            "Episode does not exist in this session.".into(),
        ));
    };

    if episode.provenance == EpisodeProvenance::SyntheticFallback
        || episode.verification != crate::providers::models::EpisodeVerification::Verified
    {
        return Err((
            404,
            "EPISODE_NOT_FOUND",
            "Episode does not exist in this session.".into(),
        ));
    }

    if let (Some(srcs), Some(audio)) = (episode.sources.clone(), episode.audio.clone()) {
        // Bug #12: Return exact persisted provider_coverage so cached & uncached responses match
        let cov = episode.cached_provider_coverage.clone().unwrap_or_else(|| {
            let mut fallback = BTreeMap::new();
            for s in &srcs {
                fallback.insert(
                    provider_key(s.provider_id).to_string(),
                    "success".to_string(),
                );
            }
            fallback
        });
        return Ok((episode, srcs, audio, cov));
    }

    let mut source_cache_parts: Vec<String> = episode
        .provider_mappings
        .iter()
        .map(|m| {
            format!(
                "{}:{}:{}:{}",
                provider_key(m.provider_id),
                m.provider_anime_id,
                m.provider_season_number,
                m.provider_episode_number
            )
        })
        .collect();
    source_cache_parts.sort();
    let source_cache_key = source_cache_parts.join("|");

    let global_hit = if !source_cache_key.is_empty() {
        let guard = state.global_source_cache.read().await;
        guard
            .get(&source_cache_key)
            .filter(|(ts, ..)| ts.elapsed() < SOURCE_CACHE_TTL)
            .map(|(_, c, a)| (c.clone(), a.clone()))
    } else {
        None
    };

    let (collected, audio) = if let Some((c, a)) = global_hit {
        (c, a)
    } else {
        let c = collect_episode_sources(&state.registry, &episode).await;
        let a = compute_audio_availability(&c.candidates, c.all_failed);
        if !source_cache_key.is_empty() && !c.candidates.is_empty() {
            let mut w = state.global_source_cache.write().await;
            if w.len() > 1000 {
                w.retain(|_, (ts, ..)| ts.elapsed() < SOURCE_CACHE_TTL);
            }
            w.insert(source_cache_key, (Instant::now(), c.clone(), a.clone()));
        }
        (c, a)
    };

    {
        let mut guard = session.write().await;
        if let Some((aid, sid)) = guard.episode_index.get(episode_id).cloned()
            && let Some(anime) = guard.anime_records.get_mut(&aid)
            && let Some(season) = anime.seasons.iter_mut().find(|s| s.id == sid)
            && let Some(ep) = season.episodes.iter_mut().find(|e| e.id == episode_id)
        {
            ep.sources = Some(collected.candidates.clone());
            ep.audio = Some(audio.clone());
            ep.cached_provider_coverage = Some(collected.provider_coverage.clone());
            ep.cached_provider_details = collected.provider_details.clone();
            ep.pipeline_stats = collected.pipeline_stats.clone();
            ep.excluded_sources_count = collected.excluded_sources;
            ep.excluded_reasons = collected.excluded_reasons.clone();
            ep.excluded_details = collected.excluded_details.clone();
            ep.matching_diagnostics = collected.matching_diagnostics.clone();
            episode = ep.clone();
        }
    }

    Ok((
        episode,
        collected.candidates,
        audio,
        collected.provider_coverage,
    ))
}

async fn handle_episode_details(
    stream: &mut TcpStream,
    episode_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let (episode, _srcs, audio, provider_coverage) =
        match ensure_episode_sources_hydrated(episode_id, &session, state).await {
            Ok(v) => v,
            Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
        };
    let available_languages = audio.available_languages.clone();

    write_json(
        stream,
        200,
        &EpisodeDetailsResponse {
            status: "success",
            episode_id: episode.id,
            anime_id: episode.anime_id,
            season_id: episode.season_id,
            season_number: episode.season_number,
            episode_number: episode.episode_number,
            title: episode.title,
            metadata_status: episode.metadata_status,
            provenance: episode.provenance,
            verification: episode.verification,
            overview: episode.overview,
            provider_coverage,
            audio,
            available_languages,
        },
    )
    .await
}

async fn handle_episode_sources(
    stream: &mut TcpStream,
    episode_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let (episode, srcs, audio, provider_coverage) =
        match ensure_episode_sources_hydrated(episode_id, &session, state).await {
            Ok(v) => v,
            Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
        };

    let providers = group_sources_by_provider(&srcs);
    let total_sources: usize = providers.iter().map(|g| g.sources.len()).sum();
    let verified_sources = srcs
        .iter()
        .filter(|s| s.verification_status == "verified")
        .count();

    write_json(
        stream,
        200,
        &EpisodeSourcesResponse {
            status: "success",
            episode_id: episode.id,
            total_sources,
            verified_sources,
            excluded_sources: episode.excluded_sources_count,
            merged_duplicates_count: episode.pipeline_stats.merged_duplicates_count,
            failed_validation_count: episode.pipeline_stats.failed_validation_count,
            excluded_reasons: episode.excluded_reasons,
            excluded_details: episode.excluded_details,
            matching_diagnostics: episode.matching_diagnostics,
            pipeline_stats: episode.pipeline_stats,
            available_languages: audio.available_languages,
            declared_languages: audio.declared_languages,
            manifest_languages: audio.manifest_languages,
            observed_track_languages: audio.observed_track_languages,
            verified_languages: audio.verified_languages,
            language_verification_status: audio.language_verification_status,
            provider_coverage,
            provider_details: episode.cached_provider_details,
            providers,
        },
    )
    .await
}

async fn handle_episode_resolve(
    stream: &mut TcpStream,
    episode_id: &str,
    body: &[u8],
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let req: ResolveRequest = if body.is_empty() {
        ResolveRequest {
            source_id: None,
            audio_language: None,
            preferred_quality: None,
        }
    } else {
        match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(_) => {
                return write_error(
                    stream,
                    400,
                    "INVALID_QUERY",
                    "Invalid JSON body for resolve request.",
                    Some(episode_id),
                )
                .await;
            }
        }
    };

    let (episode, srcs, _audio, _cov) =
        match ensure_episode_sources_hydrated(episode_id, &session, state).await {
            Ok(v) => v,
            Err((st, code, msg)) => {
                return write_error(stream, st, code, &msg, Some(episode_id)).await;
            }
        };

    match resolve_episode_stream(
        &state.registry,
        &episode,
        &srcs,
        req.source_id.as_deref(),
        req.audio_language.as_deref(),
        req.preferred_quality.as_deref(),
    )
    .await
    {
        Ok(resp) => write_json(stream, 200, &resp).await,
        Err(ResolveFailure::AudioUnavailable(msg)) => {
            write_error(stream, 404, "AUDIO_UNAVAILABLE", &msg, Some(&episode.id)).await
        }
        Err(ResolveFailure::SourceUnavailable(msg)) => {
            write_error(stream, 404, "SOURCE_UNAVAILABLE", &msg, Some(&episode.id)).await
        }
        Err(ResolveFailure::ResolutionFailed(msg)) => {
            write_error(stream, 502, "RESOLUTION_FAILED", &msg, Some(&episode.id)).await
        }
    }
}

async fn write_cors_preflight(stream: &mut TcpStream) -> std::io::Result<()> {
    let resp = "HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type, X-PV-Session\r\nConnection: close\r\n\r\n";
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await
}

async fn write_json<T: serde::Serialize>(
    stream: &mut TcpStream,
    status: u16,
    payload: &T,
) -> std::io::Result<u16> {
    let body = serde_json::to_vec(payload).unwrap_or_else(|_| b"{}".to_vec());
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        502 => "Bad Gateway",
        504 => "Gateway Timeout",
        _ => "Internal Server Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(status)
}

async fn write_error(
    stream: &mut TcpStream,
    status: u16,
    code: &'static str,
    message: &str,
    episode_id: Option<&str>,
) -> std::io::Result<u16> {
    write_json(
        stream,
        status,
        &ApiErrorResponse {
            status: "error",
            code,
            message: message.to_string(),
            episode_id: episode_id.map(str::to_string),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::models::ProviderKind;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn spawn_test_server(state: V3AppState) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                if let Ok((stream, peer)) = listener.accept().await {
                    let st = state.clone();
                    tokio::spawn(async move {
                        let _ = handle_connection(stream, peer, st).await;
                    });
                }
            }
        });
        addr
    }

    async fn send_http(
        addr: SocketAddr,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<&str>,
    ) -> (u16, serde_json::Value) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let body_bytes = body.unwrap_or("").as_bytes();
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
            body_bytes.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        if !body_bytes.is_empty() {
            stream.write_all(body_bytes).await.unwrap();
        }
        stream.flush().await.unwrap();

        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.unwrap();
        let resp_str = String::from_utf8_lossy(&resp);
        let (head, body_part) = resp_str.split_once("\r\n\r\n").unwrap();
        let status: u16 = head
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let json: serde_json::Value = serde_json::from_str(body_part).unwrap();
        (status, json)
    }

    #[test]
    fn test_exactly_seven_anime_routes_registered() {
        assert_eq!(REGISTERED_ANIME_ROUTES.len(), 7);
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/anime/search"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/anime/{anime_id}"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/anime/{anime_id}/seasons"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/seasons/{season_id}/episodes"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/episodes/{episode_id}"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"GET /api/v1/episodes/{episode_id}/sources"));
        assert!(REGISTERED_ANIME_ROUTES.contains(&"POST /api/v1/episodes/{episode_id}/resolve"));
    }

    #[tokio::test]
    async fn test_http_health_ready_and_legacy_routes_404() {
        let state = V3AppState::from_env();
        let addr = spawn_test_server(state).await;

        let (st, j) = send_http(addr, "GET", "/health", &[], None).await;
        assert_eq!(st, 200);
        assert_eq!(j["status"], "ok");

        let (st, j) = send_http(addr, "GET", "/ready", &[], None).await;
        assert_eq!(st, 200);
        assert_eq!(j["status"], "ready");

        for legacy in [
            "/api/v1/home",
            "/api/v1/stream",
            "/api/v1/catalog",
            "/api/v1/playback",
            "/api/v2/anime",
        ] {
            let (st, j) = send_http(addr, "GET", legacy, &[], None).await;
            assert_eq!(st, 404, "legacy route {legacy} should return 404");
            assert_eq!(j["code"], "NOT_FOUND");
        }
    }

    #[tokio::test]
    async fn test_http_search_validation_and_session_errors() {
        let state = V3AppState::from_env();
        let addr = spawn_test_server(state).await;

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/search", &[], None).await;
        assert_eq!(st, 400);
        assert_eq!(j["code"], "INVALID_QUERY");

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/search?q=%20%20", &[], None).await;
        assert_eq!(st, 400);
        assert_eq!(j["code"], "INVALID_QUERY");

        let long_q = "a".repeat(205);
        let (st, j) = send_http(
            addr,
            "GET",
            &format!("/api/v1/anime/search?q={long_q}"),
            &[],
            None,
        )
        .await;
        assert_eq!(st, 400);
        assert_eq!(j["code"], "INVALID_QUERY");

        // Missing session
        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001", &[], None).await;
        assert_eq!(st, 401);
        assert_eq!(j["code"], "SESSION_REQUIRED");

        // Invalid session format
        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/anime/NP-A-001",
            &[("X-PV-Session", "bad!")],
            None,
        )
        .await;
        assert_eq!(st, 400);
        assert_eq!(j["code"], "SESSION_INVALID");

        // Unknown / expired session
        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/anime/NP-A-001",
            &[(
                "X-PV-Session",
                "000000000000000000000000000000000000000000000000",
            )],
            None,
        )
        .await;
        assert_eq!(st, 401);
        assert_eq!(j["code"], "SESSION_EXPIRED");
    }

    #[tokio::test]
    async fn test_http_episode_visibility_and_resolve_contract() {
        use crate::providers::models::{MediaType, Release, SourceMirror};
        let state = V3AppState::from_env();

        let anime = CanonicalAnime {
            id: "NP-A-001".to_string(),
            slug_token: "test-anime".to_string(),
            title: "Test Anime".to_string(),
            alternative_titles: vec![],
            media_type: MediaType::Series,
            year: Some(2024),
            description: None,
            poster_url: None,
            genres: vec![],
            provider_records: vec![],
            seasons: vec![CanonicalSeason {
                id: "NP-S-001".to_string(),
                anime_id: "NP-A-001".to_string(),
                season_number: 1,
                verified_episodes: 1,
                unverified_episodes: 0,
                synthetic_episodes: 0,
                advertised_episode_count: None,
                structure_status: "verified".to_string(),
                episodes: vec![
                    CanonicalEpisode {
                        id: "NP-E-001".to_string(),
                        anime_id: "NP-A-001".to_string(),
                        season_id: "NP-S-001".to_string(),
                        season_number: 1,
                        episode_number: 1,
                        title: "Episode 1".to_string(),
                        metadata_status: "verified".to_string(),
                        provenance: EpisodeProvenance::ProviderMetadata,
                        verification: crate::providers::models::EpisodeVerification::Verified,
                        overview: None,
                        provider_mappings: vec![],
                        audio: Some(AudioAvailability {
                            hindi: AudioStatus::Missing,
                            english: AudioStatus::Available,
                            japanese: AudioStatus::Unknown,
                            available_languages: vec!["en".to_string()],
                            declared_languages: vec!["en".to_string()],
                            manifest_languages: Vec::new(),
                            observed_track_languages: Vec::new(),
                            verified_languages: vec!["en".to_string()],
                            language_verification_status: "verified".to_string(),
                            checked_sources: true,
                        }),
                        sources: Some(vec![ProviderSourceCandidate {
                            source_id: "src-1".to_string(),
                            provider_id: ProviderKind::MovieBox,
                            provider_name: "MovieBox".to_string(),
                            provider_episode_id: "mb-ep-1".to_string(),
                            provider_season: 1,
                            provider_episode: 1,
                            release: Release {
                                provider: ProviderKind::MovieBox,
                                filename: "ep1.mp4".into(),
                                quality: Some("1080p".to_string()),
                                codec: None,
                                language: Some("en".to_string()),
                                size_bytes: None,
                                season: Some(1),
                                episode: Some(1),
                                mirrors: vec![SourceMirror {
                                    label: "Direct".into(),
                                    resolver_url: "https://cdn.example.com/ep1.mp4".to_string(),
                                    headers: vec![],
                                    direct_file: true,
                                }],
                                resource_id: None,
                            },
                            audio_languages: vec!["en".to_string()],
                            track_index: None,
                            provider_track_id: None,
                            language: "English".to_string(),
                            language_code: Some("en".to_string()),
                            declared_language: Some("English".to_string()),
                            declared_language_code: Some("en".to_string()),
                            verified_language: Some("English".to_string()),
                            verified_language_code: Some("en".to_string()),
                            audio_track_id: None,
                            codec: None,
                            audio_codec: None,
                            channels: None,
                            sample_duration_seconds: Some(30.0),
                            detection_method: "speech_lang_id_model".to_string(),
                            confidence: Some(0.95),
                            verification_status: "verified".to_string(),
                            playability_status: "http_reachable".to_string(),
                            track_selection_metadata: None,
                            audio_tracks: Vec::new(),
                            declared_audio_tracks: Vec::new(),
                            subtitles: Vec::new(),
                            provider_metadata: ProviderSourceMetadata {
                                provider_id: "moviebox".to_string(),
                                original_url: Some("https://cdn.example.com/ep1.mp4".to_string()),
                                original_language: Some("en".to_string()),
                                original_quality: Some("1080p".to_string()),
                                ..Default::default()
                            },
                            normalized_metadata: NormalizedSourceMetadata {
                                quality: Some("1080p".to_string()),
                                resolution_u64: 1080,
                                declared_languages: vec!["en".to_string()],
                                primary_language: "English".to_string(),
                                primary_language_code: Some("en".to_string()),
                                ..Default::default()
                            },
                            media_metadata: MediaProbeMetadata {
                                http_reachable: true,
                                http_status_code: Some(200),
                                access_status: "ok".to_string(),
                                manifest_validated: false,
                                media_probe_validated: false,
                                playability_status: "http_reachable".to_string(),
                                device_playback_status: "untested".to_string(),
                                probed_track_count: 0,
                            },
                            detected_language_metadata: DetectedLanguageMetadata {
                                declared_language: Some("English".to_string()),
                                declared_language_code: Some("en".to_string()),
                                detected_language: Some("English".to_string()),
                                detected_language_code: Some("en".to_string()),
                                confidence: Some(0.95),
                                detection_method: "speech_lang_id_model".to_string(),
                                verification_status: "verified".to_string(),
                            },
                            quality: Some("1080p".to_string()),
                            resolution_u64: 1080,
                            stream_url: Some("https://cdn.example.com/ep1.mp4".to_string()),
                            headers: HashMap::new(),
                            available: true,
                        }]),
                        cached_provider_coverage: None,
                        cached_provider_details: Vec::new(),
                        pipeline_stats: PipelineStats::default(),
                        excluded_sources_count: 0,
                        excluded_reasons: Vec::new(),
                        excluded_details: Vec::new(),
                        matching_diagnostics: Vec::new(),
                    },
                    CanonicalEpisode {
                        id: "NP-E-002".to_string(),
                        anime_id: "NP-A-001".to_string(),
                        season_id: "NP-S-001".to_string(),
                        season_number: 1,
                        episode_number: 2,
                        title: "Episode 2".to_string(),
                        metadata_status: "unverified".to_string(),
                        provenance: EpisodeProvenance::ProviderMetadata,
                        verification: crate::providers::models::EpisodeVerification::Verified,
                        overview: None,
                        provider_mappings: vec![],
                        audio: Some(AudioAvailability {
                            hindi: AudioStatus::Missing,
                            english: AudioStatus::Missing,
                            japanese: AudioStatus::Missing,
                            available_languages: vec![],
                            declared_languages: Vec::new(),
                            manifest_languages: Vec::new(),
                            observed_track_languages: Vec::new(),
                            verified_languages: Vec::new(),
                            language_verification_status: "unverified".to_string(),
                            checked_sources: true,
                        }),
                        sources: Some(vec![]),
                        cached_provider_coverage: None,
                        cached_provider_details: Vec::new(),
                        pipeline_stats: PipelineStats::default(),
                        excluded_sources_count: 0,
                        excluded_reasons: Vec::new(),
                        excluded_details: Vec::new(),
                        matching_diagnostics: Vec::new(),
                    },
                ],
            }],
            total_seasons: 1,
            total_episodes: 2,
            verified_episodes: 1,
            unverified_episodes: 0,
            synthetic_episodes: 0,
            season_structure_status: "complete".to_string(),
            details_hydrated: true,
        };
        let session = state.sessions.create_session(vec![anime]).await;
        let sid = session.read().await.session_id.clone();

        let addr = spawn_test_server(state).await;
        let hdrs = [("X-PV-Session", sid.as_str())];

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["anime_id"], "NP-A-001");

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001/seasons", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["seasons"].as_array().unwrap().len(), 1);

        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/seasons/NP-S-001/episodes",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["episodes"].as_array().unwrap().len(), 2);

        let (st, j) = send_http(addr, "GET", "/api/v1/episodes/NP-E-001", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["audio"]["english"], "available");
        assert_eq!(j["audio"]["hindi"], "missing");
        assert_eq!(j["available_languages"][0], "en");

        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/episodes/NP-E-001/sources",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["total_sources"], 1);
        assert_eq!(j["providers"][0]["provider"], "MovieBox");

        // Missing requested audio -> 404 AUDIO_UNAVAILABLE
        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-001/resolve",
            &hdrs,
            Some(r#"{"audio_language":"hi","preferred_quality":"1080p"}"#),
        )
        .await;
        assert_eq!(st, 404);
        assert_eq!(j["code"], "AUDIO_UNAVAILABLE");

        // Available audio -> 200 resolved stream_url
        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-001/resolve",
            &hdrs,
            Some(r#"{"audio_language":"en","preferred_quality":"1080p"}"#),
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["status"], "success");
        assert_eq!(j["source_id"], "src-1");
        assert_eq!(j["provider"], "MovieBox");
        assert_eq!(j["language_code"], "en");
        assert_eq!(j["stream_url"], "https://cdn.example.com/ep1.mp4");

        // Episode with no sources -> 404 SOURCE_UNAVAILABLE
        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-002/resolve",
            &hdrs,
            Some(r#"{"audio_language":"en"}"#),
        )
        .await;
        assert_eq!(st, 404);
        assert_eq!(j["code"], "SOURCE_UNAVAILABLE");

        // Exact source_id selection -> 200 resolved stream_url
        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-001/resolve",
            &hdrs,
            Some(r#"{"source_id":"src-1"}"#),
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["stream_url"], "https://cdn.example.com/ep1.mp4");

        // Invalid source_id -> 404 SOURCE_UNAVAILABLE
        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-001/resolve",
            &hdrs,
            Some(r#"{"source_id":"src-does-not-exist"}"#),
        )
        .await;
        assert_eq!(st, 404);
        assert_eq!(j["code"], "SOURCE_UNAVAILABLE");

        // Non-existent anime -> 404 ANIME_NOT_FOUND
        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-999", &hdrs, None).await;
        assert_eq!(st, 404);
        assert_eq!(j["code"], "ANIME_NOT_FOUND");
    }

    #[tokio::test]
    async fn test_http_multi_track_same_url_selection() {
        use crate::providers::models::{MediaType, Release, SourceMirror};
        let state = V3AppState::from_env();
        let mpd_url = "https://cdn.example.com/multi.mpd".to_string();

        let make_track_cand =
            |sid: &str, idx: usize, code: &str, lang: &str, aset: &str| ProviderSourceCandidate {
                source_id: sid.to_string(),
                provider_id: ProviderKind::MovieBox,
                provider_name: "MovieBox".to_string(),
                provider_episode_id: "mb-1".to_string(),
                provider_season: 1,
                provider_episode: 1,
                release: Release {
                    provider: ProviderKind::MovieBox,
                    filename: "multi.mpd".into(),
                    quality: Some("1080p".to_string()),
                    codec: Some("hevc".to_string()),
                    language: Some(code.to_string()),
                    size_bytes: None,
                    season: Some(1),
                    episode: Some(1),
                    mirrors: vec![SourceMirror {
                        label: "DASH".into(),
                        resolver_url: mpd_url.clone(),
                        headers: vec![],
                        direct_file: true,
                    }],
                    resource_id: None,
                },
                audio_languages: vec![code.to_string()],
                track_index: Some(idx),
                provider_track_id: Some(aset.to_string()),
                language: lang.to_string(),
                language_code: Some(code.to_string()),
                declared_language: Some(lang.to_string()),
                declared_language_code: Some(code.to_string()),
                verified_language: Some(lang.to_string()),
                verified_language_code: Some(code.to_string()),
                audio_track_id: Some(aset.to_string()),
                codec: Some("hevc".to_string()),
                audio_codec: Some("mp4a.40.2".to_string()),
                channels: Some("2".to_string()),
                sample_duration_seconds: Some(30.0),
                detection_method: "speech_lang_id_model".to_string(),
                confidence: Some(0.95),
                verification_status: "verified".to_string(),
                track_selection_metadata: Some(TrackSelectionMetadata {
                    format: "dash".to_string(),
                    track_index: idx,
                    adaptation_set_id: Some(aset.to_string()),
                    representation_id: None,
                    group_id: None,
                    rendition_name: None,
                    language_code: Some(code.to_string()),
                }),
                playability_status: "manifest_validated".to_string(),
                audio_tracks: Vec::new(),
                declared_audio_tracks: Vec::new(),
                subtitles: Vec::new(),
                provider_metadata: ProviderSourceMetadata {
                    provider_id: "moviebox".to_string(),
                    original_url: Some(mpd_url.clone()),
                    original_language: Some(code.to_string()),
                    original_quality: Some("1080p".to_string()),
                    original_codec: Some("hevc".to_string()),
                    original_track_ids: vec![aset.to_string()],
                    ..Default::default()
                },
                normalized_metadata: NormalizedSourceMetadata {
                    quality: Some("1080p".to_string()),
                    resolution_u64: 1080,
                    video_codec: Some("hevc".to_string()),
                    declared_languages: vec![code.to_string()],
                    primary_language: lang.to_string(),
                    primary_language_code: Some(code.to_string()),
                },
                media_metadata: MediaProbeMetadata {
                    http_reachable: true,
                    http_status_code: Some(200),
                    access_status: "ok".to_string(),
                    manifest_validated: true,
                    media_probe_validated: false,
                    playability_status: "manifest_validated".to_string(),
                    device_playback_status: "untested".to_string(),
                    probed_track_count: 0,
                },
                detected_language_metadata: DetectedLanguageMetadata {
                    declared_language: Some(lang.to_string()),
                    declared_language_code: Some(code.to_string()),
                    detected_language: Some(lang.to_string()),
                    detected_language_code: Some(code.to_string()),
                    confidence: Some(0.95),
                    detection_method: "speech_lang_id_model".to_string(),
                    verification_status: "verified".to_string(),
                },
                quality: Some("1080p".to_string()),
                resolution_u64: 1080,
                stream_url: Some(mpd_url.clone()),
                headers: HashMap::new(),
                available: true,
            };

        let anime = CanonicalAnime {
            id: "NP-A-002".to_string(),
            slug_token: "multi".to_string(),
            title: "Multi Track Anime".to_string(),
            alternative_titles: vec![],
            media_type: MediaType::Series,
            year: Some(2024),
            description: None,
            poster_url: None,
            genres: vec![],
            provider_records: vec![],
            seasons: vec![CanonicalSeason {
                id: "NP-S-002".to_string(),
                anime_id: "NP-A-002".to_string(),
                season_number: 1,
                verified_episodes: 1,
                unverified_episodes: 0,
                synthetic_episodes: 0,
                advertised_episode_count: None,
                structure_status: "verified".to_string(),
                episodes: vec![CanonicalEpisode {
                    id: "NP-E-010".to_string(),
                    anime_id: "NP-A-002".to_string(),
                    season_id: "NP-S-002".to_string(),
                    season_number: 1,
                    episode_number: 1,
                    title: "Episode 1".to_string(),
                    metadata_status: "verified".to_string(),
                    provenance: EpisodeProvenance::ProviderMetadata,
                    verification: crate::providers::models::EpisodeVerification::Verified,
                    overview: None,
                    provider_mappings: vec![],
                    audio: Some(AudioAvailability {
                        hindi: AudioStatus::Available,
                        english: AudioStatus::Available,
                        japanese: AudioStatus::Missing,
                        available_languages: vec!["hi".to_string(), "en".to_string()],
                        declared_languages: vec!["hi".to_string(), "en".to_string()],
                        manifest_languages: vec!["hi".to_string(), "en".to_string()],
                        observed_track_languages: vec!["hi".to_string(), "en".to_string()],
                        verified_languages: vec!["hi".to_string(), "en".to_string()],
                        language_verification_status: "verified".to_string(),
                        checked_sources: true,
                    }),
                    sources: Some(vec![
                        make_track_cand("src-1", 0, "hi", "Hindi", "2"),
                        make_track_cand("src-2", 1, "en", "English", "3"),
                    ]),
                    cached_provider_coverage: None,
                    cached_provider_details: Vec::new(),
                    pipeline_stats: PipelineStats::default(),
                    excluded_sources_count: 0,
                    excluded_reasons: Vec::new(),
                    excluded_details: Vec::new(),
                    matching_diagnostics: Vec::new(),
                }],
            }],
            total_seasons: 1,
            total_episodes: 1,
            verified_episodes: 1,
            unverified_episodes: 0,
            synthetic_episodes: 0,
            season_structure_status: "verified".to_string(),
            details_hydrated: true,
        };
        let session = state.sessions.create_session(vec![anime]).await;
        let sid = session.read().await.session_id.clone();
        let addr = spawn_test_server(state).await;
        let hdrs = [("X-PV-Session", sid.as_str())];

        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/episodes/NP-E-010/sources",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["total_sources"], 2);
        assert_eq!(j["providers"][0]["sources"][0]["track_index"], 0);
        assert_eq!(j["providers"][0]["sources"][1]["track_index"], 1);
        assert_eq!(
            j["providers"][0]["sources"][0]["stream_url"],
            j["providers"][0]["sources"][1]["stream_url"]
        );

        let (st, j) = send_http(
            addr,
            "POST",
            "/api/v1/episodes/NP-E-010/resolve",
            &hdrs,
            Some(r#"{"source_id":"src-2","audio_language":"en"}"#),
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["source_id"], "src-2");
        assert_eq!(j["track_index"], 1);
        assert_eq!(j["language_code"], "en");
        assert_eq!(j["track_selection_metadata"]["adaptation_set_id"], "3");
    }

    #[tokio::test]
    async fn test_cached_and_uncached_episode_sources_metadata_match() {
        let state = V3AppState::from_env();
        let mut cov = BTreeMap::new();
        cov.insert("fourkhdhub".to_string(), "success".to_string());
        cov.insert("anizone".to_string(), "failed".to_string());

        let anime = CanonicalAnime {
            id: "NP-A-CACHE".to_string(),
            slug_token: "cache".to_string(),
            title: "Cache Test Anime".to_string(),
            alternative_titles: vec![],
            media_type: MediaType::Series,
            year: Some(2024),
            description: None,
            poster_url: None,
            genres: vec![],
            provider_records: vec![],
            seasons: vec![CanonicalSeason {
                id: "NP-S-CACHE".to_string(),
                anime_id: "NP-A-CACHE".to_string(),
                season_number: 1,
                verified_episodes: 1,
                unverified_episodes: 0,
                synthetic_episodes: 0,
                advertised_episode_count: None,
                structure_status: "verified".to_string(),
                episodes: vec![CanonicalEpisode {
                    id: "NP-E-CACHE".to_string(),
                    anime_id: "NP-A-CACHE".to_string(),
                    season_id: "NP-S-CACHE".to_string(),
                    season_number: 1,
                    episode_number: 1,
                    title: "Episode 1".to_string(),
                    metadata_status: "verified".to_string(),
                    provenance: EpisodeProvenance::ProviderMetadata,
                    verification: crate::providers::models::EpisodeVerification::Verified,
                    overview: None,
                    provider_mappings: vec![],
                    audio: Some(AudioAvailability {
                        hindi: AudioStatus::Unknown,
                        english: AudioStatus::Unknown,
                        japanese: AudioStatus::Unknown,
                        available_languages: vec![],
                        declared_languages: Vec::new(),
                        manifest_languages: Vec::new(),
                        observed_track_languages: Vec::new(),
                        verified_languages: Vec::new(),
                        language_verification_status: "unverified".to_string(),
                        checked_sources: true,
                    }),
                    sources: Some(vec![]),
                    cached_provider_coverage: Some(cov),
                    cached_provider_details: vec![ProviderCoverageDetail {
                        provider: "4KHDHub".to_string(),
                        request_status: "success".to_string(),
                        source_count: 2,
                        validated_source_count: 1,
                        merged_duplicates_count: 1,
                        failed_validation_count: 0,
                        excluded_source_count: 0,
                        fallback_attempted: false,
                        fallback_status: None,
                        errors: vec![],
                    }],
                    pipeline_stats: PipelineStats {
                        raw_sources: 2,
                        parsed_sources: 2,
                        matched_sources: 2,
                        resolved_sources: 2,
                        validated_sources: 2,
                        merged_duplicates_count: 1,
                        failed_validation_count: 0,
                        returned_sources: 1,
                    },
                    excluded_sources_count: 1,
                    excluded_reasons: vec!["fourkhdhub:merged_duplicate_url".to_string()],
                    excluded_details: vec![ExcludedSourceRecord {
                        provider: "fourkhdhub".to_string(),
                        source_id: Some("src-1".to_string()),
                        stage: "deduplication".to_string(),
                        reason: "merged_duplicate_url".to_string(),
                        http_status_code: Some(200),
                        playability_status: Some("http_reachable".to_string()),
                        declared_language: None,
                        declared_language_code: None,
                        declared_audio_tracks: Vec::new(),
                        provider_metadata: None,
                        media_metadata: None,
                    }],
                    matching_diagnostics: Vec::new(),
                }],
            }],
            total_seasons: 1,
            total_episodes: 1,
            verified_episodes: 1,
            unverified_episodes: 0,
            synthetic_episodes: 0,
            season_structure_status: "verified".to_string(),
            details_hydrated: true,
        };
        let session = state.sessions.create_session(vec![anime]).await;
        let sid = session.read().await.session_id.clone();
        let addr = spawn_test_server(state).await;
        let hdrs = [("X-PV-Session", sid.as_str())];

        let (st1, j1) = send_http(
            addr,
            "GET",
            "/api/v1/episodes/NP-E-CACHE/sources",
            &hdrs,
            None,
        )
        .await;
        let (st2, j2) = send_http(
            addr,
            "GET",
            "/api/v1/episodes/NP-E-CACHE/sources",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st1, 200);
        assert_eq!(st2, 200);
        assert_eq!(j1, j2);
        assert_eq!(j1["provider_coverage"]["fourkhdhub"], "success");
        assert_eq!(j1["provider_coverage"]["anizone"], "failed");
    }

    #[tokio::test]
    async fn test_unverified_and_synthetic_episodes_hidden_from_api_and_playback() {
        let state = V3AppState::from_env();
        let make_ep = |id: &str,
                       sid: &str,
                       s_num: usize,
                       ep_num: usize,
                       prov: EpisodeProvenance,
                       ver: crate::providers::models::EpisodeVerification| {
            CanonicalEpisode {
                id: id.to_string(),
                anime_id: "NP-A-VIS".to_string(),
                season_id: sid.to_string(),
                season_number: s_num,
                episode_number: ep_num,
                title: format!("Episode {ep_num}"),
                metadata_status: if ver == crate::providers::models::EpisodeVerification::Verified {
                    "verified".to_string()
                } else {
                    "unverified".to_string()
                },
                provenance: prov,
                verification: ver,
                overview: None,
                provider_mappings: vec![],
                audio: None,
                sources: Some(vec![]),
                cached_provider_coverage: None,
                cached_provider_details: Vec::new(),
                pipeline_stats: PipelineStats::default(),
                excluded_sources_count: 0,
                excluded_reasons: Vec::new(),
                excluded_details: Vec::new(),
                matching_diagnostics: Vec::new(),
            }
        };

        let anime = CanonicalAnime {
            id: "NP-A-VIS".to_string(),
            slug_token: "vis".to_string(),
            title: "Visibility Rule Anime".to_string(),
            alternative_titles: vec![],
            media_type: MediaType::Series,
            year: Some(2024),
            description: None,
            poster_url: None,
            genres: vec![],
            provider_records: vec![],
            seasons: vec![
                CanonicalSeason {
                    id: "NP-S-VIS-001".to_string(),
                    anime_id: "NP-A-VIS".to_string(),
                    season_number: 1,
                    verified_episodes: 1,
                    unverified_episodes: 1,
                    synthetic_episodes: 1,
                    advertised_episode_count: None,
                    structure_status: "partial".to_string(),
                    episodes: vec![
                        make_ep(
                            "NP-E-VIS-0001",
                            "NP-S-VIS-001",
                            1,
                            1,
                            EpisodeProvenance::ProviderMetadata,
                            crate::providers::models::EpisodeVerification::Verified,
                        ),
                        make_ep(
                            "NP-E-VIS-0002",
                            "NP-S-VIS-001",
                            1,
                            2,
                            EpisodeProvenance::ProviderListing,
                            crate::providers::models::EpisodeVerification::Unverified,
                        ),
                        make_ep(
                            "NP-E-VIS-0003",
                            "NP-S-VIS-001",
                            1,
                            3,
                            EpisodeProvenance::SyntheticFallback,
                            crate::providers::models::EpisodeVerification::Unverified,
                        ),
                    ],
                },
                CanonicalSeason {
                    id: "NP-S-VIS-002".to_string(),
                    anime_id: "NP-A-VIS".to_string(),
                    season_number: 2,
                    verified_episodes: 0,
                    unverified_episodes: 1,
                    synthetic_episodes: 0,
                    advertised_episode_count: None,
                    structure_status: "partial".to_string(),
                    episodes: vec![make_ep(
                        "NP-E-VIS-S02-0001",
                        "NP-S-VIS-002",
                        2,
                        1,
                        EpisodeProvenance::ProviderListing,
                        crate::providers::models::EpisodeVerification::Unverified,
                    )],
                },
            ],
            total_seasons: 1,
            total_episodes: 1,
            verified_episodes: 1,
            unverified_episodes: 2,
            synthetic_episodes: 1,
            season_structure_status: "partial".to_string(),
            details_hydrated: true,
        };
        let session = state.sessions.create_session(vec![anime]).await;
        let sid = session.read().await.session_id.clone();
        let addr = spawn_test_server(state).await;
        let hdrs = [("X-PV-Session", sid.as_str())];

        // 1. Seasons: only Season 1 (with 1 verified episode) is visible; Season 2 is hidden
        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-VIS/seasons", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["total_seasons"], 1);
        assert_eq!(j["total_episodes"], 1);
        assert_eq!(j["seasons"].as_array().unwrap().len(), 1);
        assert_eq!(j["seasons"][0]["episode_count"], 1);

        // 2. Season 1 episodes: only Verified NP-E-VIS-0001 appears
        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/seasons/NP-S-VIS-001/episodes",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st, 200);
        assert_eq!(j["total_episodes"], 1);
        assert_eq!(j["episodes"].as_array().unwrap().len(), 1);
        assert_eq!(j["episodes"][0]["episode_id"], "NP-E-VIS-0001");

        // 3. Season 2 (0 verified episodes) is hidden (404)
        let (st, _) = send_http(
            addr,
            "GET",
            "/api/v1/seasons/NP-S-VIS-002/episodes",
            &hdrs,
            None,
        )
        .await;
        assert_eq!(st, 404);

        // 4. Unverified and SyntheticFallback episodes are hidden from details, sources, and resolve
        for hidden_ep in ["NP-E-VIS-0002", "NP-E-VIS-0003"] {
            let (st, _) = send_http(
                addr,
                "GET",
                &format!("/api/v1/episodes/{hidden_ep}"),
                &hdrs,
                None,
            )
            .await;
            assert_eq!(st, 404);
            let (st, _) = send_http(
                addr,
                "GET",
                &format!("/api/v1/episodes/{hidden_ep}/sources"),
                &hdrs,
                None,
            )
            .await;
            assert_eq!(st, 404);
            let (st, _) = send_http(
                addr,
                "POST",
                &format!("/api/v1/episodes/{hidden_ep}/resolve"),
                &hdrs,
                Some("{}"),
            )
            .await;
            assert_eq!(st, 404);
        }

        // 5. Internal session still preserves all hidden episodes for auditing
        let guard = session.read().await;
        let stored = guard.anime_records.get("NP-A-VIS").unwrap();
        assert_eq!(stored.seasons.len(), 2);
        assert_eq!(stored.seasons[0].episodes.len(), 3);
        assert_eq!(stored.seasons[1].episodes.len(), 1);
    }
}
