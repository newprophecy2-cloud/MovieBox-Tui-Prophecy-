use crate::v3::audio_sources::{
    ResolveFailure, collect_episode_sources, compute_audio_availability, resolve_episode_stream,
};
use crate::v3::matching::{build_canonical_structure, merge_search_results};
use crate::v3::models::*;
use crate::v3::providers::ProviderRegistry;
use crate::v3::session::{SessionConfig, SessionLookupError, SessionStore, hash_session_id};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};

static REQ_SEQ: AtomicU64 = AtomicU64::new(1);

const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(45);

#[derive(Clone)]
pub struct V3AppState {
    pub sessions: SessionStore,
    pub registry: ProviderRegistry,
    pub rate_limiter: Arc<Mutex<HashMap<IpAddr, (Instant, u32)>>>,
    pub rate_limit_per_min: u32,
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

    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.trim().parse::<u16>().ok())
        .unwrap_or(18080);
    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let listener = TcpListener::bind(addr).await?;
    log::info!("prophecy_v3_server_listening addr={addr} routes={}", REGISTERED_ANIME_ROUTES.len());

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
        let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf_chunk(&mut temp)))
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
            return write_error(&mut stream, 413, "INVALID_QUERY", "Request headers too large", None).await.map(|_| ());
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
        return write_error(&mut stream, 413, "INVALID_QUERY", "Request body too large", None).await.map(|_| ());
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
    let session_hash = session_hdr.map(hash_session_id).unwrap_or_else(|| "-".to_string());

    if !check_rate_limit(&state, peer.ip()).await {
        log::warn!("v3_req request_id={req_id} session_hash={session_hash} route={path} status=429 error_code=RATE_LIMITED");
        return write_error(&mut stream, 429, "RATE_LIMITED", "Rate limit exceeded.", None).await.map(|_| ());
    }

    let res = tokio::time::timeout(
        REQUEST_TIMEOUT,
        dispatch_route(&mut stream, &method, path, query, &headers, &body, &state, &req_id, &session_hash),
    )
    .await;

    let elapsed = started.elapsed().as_millis();
    match res {
        Ok(Ok(status)) => {
            log::info!("v3_req request_id={req_id} session_hash={session_hash} method={method} route={path} duration_ms={elapsed} status={status}");
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(_) => {
            log::warn!("v3_req request_id={req_id} session_hash={session_hash} method={method} route={path} duration_ms={elapsed} status=504 error_code=INTERNAL_ERROR");
            write_error(&mut stream, 504, "INTERNAL_ERROR", "Request timed out.", None).await.map(|_| ())
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
                return write_error(stream, 401, "SESSION_REQUIRED", "Missing X-PV-Session header.", None).await;
            }
            Err(SessionLookupError::Invalid) => {
                return write_error(stream, 400, "SESSION_INVALID", "Invalid X-PV-Session token format.", None).await;
            }
            Err(SessionLookupError::Expired) => {
                return write_error(stream, 401, "SESSION_EXPIRED", "The temporary session has expired.", None).await;
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

async fn handle_search(
    stream: &mut TcpStream,
    query: &str,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let q = parse_query_param(query, "q").unwrap_or_default();
    let trimmed = q.trim();
    if trimmed.is_empty()
        || trimmed.len() > 160
        || trimmed.chars().any(|c| c.is_control())
    {
        return write_error(
            stream,
            400,
            "INVALID_QUERY",
            "Search query 'q' must be 1..160 printable characters.",
            None,
        )
        .await;
    }

    let (normalized, failures, total_enabled) = state.registry.search_all(trimmed).await;
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

    let canonical = merge_search_results(normalized, state.sessions.config().max_search_results);
    let results: Vec<SearchResultItem> = canonical
        .iter()
        .map(|a| SearchResultItem {
            id: a.id.clone(),
            title: a.title.clone(),
            media_type: a.media_type,
            year: a.year,
            poster_url: a.poster_url.clone(),
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
            results,
            total,
            partial,
            provider_failures: failures,
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
        return Err((404, "ANIME_NOT_FOUND", "Anime does not exist in this session.".into()));
    };
    if anime.details_hydrated {
        return Ok(anime);
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

    let mut guard = session.write().await;
    for season in &anime.seasons {
        guard.season_index.insert(season.id.clone(), anime.id.clone());
        for ep in &season.episodes {
            guard
                .episode_index
                .insert(ep.id.clone(), (anime.id.clone(), season.id.clone()));
        }
    }
    guard.anime_records.insert(anime.id.clone(), anime.clone());
    Ok(anime)
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

    write_json(
        stream,
        200,
        &AnimeDetailsResponse {
            status: "success",
            id: anime.id,
            title: anime.title,
            media_type: anime.media_type,
            year: anime.year,
            description: anime.description,
            poster_url: anime.poster_url,
            genres: anime.genres,
            total_seasons: anime.total_seasons,
            total_episodes: anime.total_episodes,
            season_structure_status: anime.season_structure_status,
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

    let seasons: Vec<SeasonItemResponse> = anime
        .seasons
        .iter()
        .map(|s| SeasonItemResponse {
            id: s.id.clone(),
            season_number: s.season_number,
            episode_count: s.episodes.len(),
        })
        .collect();

    write_json(
        stream,
        200,
        &AnimeSeasonsResponse {
            status: "success",
            anime_id: anime.id,
            total_seasons: anime.total_seasons,
            total_episodes: anime.total_episodes,
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
    let Some(season) = season_opt else {
        return write_error(stream, 404, "SEASON_NOT_FOUND", "Season does not exist in this session.", None).await;
    };

    // Audio availability NEVER hides an episode (Spec Section 10.3 & 12.8)
    let episodes: Vec<EpisodeItemResponse> = season
        .episodes
        .iter()
        .map(|e| EpisodeItemResponse {
            id: e.id.clone(),
            episode_number: e.episode_number,
            title: e.title.clone(),
        })
        .collect();

    write_json(
        stream,
        200,
        &SeasonEpisodesResponse {
            status: "success",
            season_id: season.id,
            total_episodes: episodes.len(),
            episodes,
        },
    )
    .await
}

async fn ensure_episode_sources_hydrated(
    episode_id: &str,
    session: &Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> Result<(CanonicalEpisode, Vec<ProviderSourceCandidate>, AudioAvailability), (u16, &'static str, String)> {
    let loc = {
        let guard = session.read().await;
        guard.episode_index.get(episode_id).cloned().and_then(|(aid, sid)| {
            guard.anime_records.get(&aid).and_then(|a| {
                a.seasons
                    .iter()
                    .find(|s| s.id == sid)
                    .and_then(|s| s.episodes.iter().find(|e| e.id == episode_id).cloned())
            })
        })
    };
    let Some(mut episode) = loc else {
        return Err((404, "EPISODE_NOT_FOUND", "Episode does not exist in this session.".into()));
    };

    if let (Some(srcs), Some(audio)) = (episode.sources.clone(), episode.audio.clone()) {
        return Ok((episode, srcs, audio));
    }

    let (srcs, all_failed) = collect_episode_sources(&state.registry, &episode).await;
    let audio = compute_audio_availability(&srcs, all_failed);

    {
        let mut guard = session.write().await;
        if let Some((aid, sid)) = guard.episode_index.get(episode_id).cloned()
            && let Some(anime) = guard.anime_records.get_mut(&aid)
            && let Some(season) = anime.seasons.iter_mut().find(|s| s.id == sid)
            && let Some(ep) = season.episodes.iter_mut().find(|e| e.id == episode_id)
        {
            ep.sources = Some(srcs.clone());
            ep.audio = Some(audio.clone());
            episode = ep.clone();
        }
    }

    Ok((episode, srcs, audio))
}

async fn handle_episode_details(
    stream: &mut TcpStream,
    episode_id: &str,
    session: Arc<tokio::sync::RwLock<TemporarySession>>,
    state: &V3AppState,
) -> std::io::Result<u16> {
    let (episode, _srcs, audio) = match ensure_episode_sources_hydrated(episode_id, &session, state).await {
        Ok(v) => v,
        Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
    };

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
            overview: episode.overview,
            audio,
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
    let (episode, srcs, _audio) = match ensure_episode_sources_hydrated(episode_id, &session, state).await {
        Ok(v) => v,
        Err((st, code, msg)) => return write_error(stream, st, code, &msg, None).await,
    };

    let public: Vec<PublicSourceItem> = srcs
        .iter()
        .map(|s| PublicSourceItem {
            id: s.source_id.clone(),
            quality: s.quality.clone(),
            audio_languages: s.audio_languages.clone(),
            available: s.available,
        })
        .collect();
    let total = public.len();

    write_json(
        stream,
        200,
        &EpisodeSourcesResponse {
            status: "success",
            episode_id: episode.id,
            sources: public,
            total,
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

    let (episode, srcs, _audio) = match ensure_episode_sources_hydrated(episode_id, &session, state).await {
        Ok(v) => v,
        Err((st, code, msg)) => return write_error(stream, st, code, &msg, Some(episode_id)).await,
    };

    match resolve_episode_stream(
        &state.registry,
        &episode,
        &srcs,
        req.audio_language.as_deref(),
        req.preferred_quality.as_deref(),
    )
    .await
    {
        Ok((stream_url, headers)) => {
            write_json(
                stream,
                200,
                &ResolveSuccessResponse {
                    status: "success",
                    episode_id: episode.id,
                    stream_url,
                    headers,
                },
            )
            .await
        }
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
            episode_id: episode_id.map(str::to_string),
            error: ApiErrorBody {
                code,
                message: message.to_string(),
            },
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
            assert_eq!(j["error"]["code"], "NOT_FOUND");
        }
    }

    #[tokio::test]
    async fn test_http_search_validation_and_session_errors() {
        let state = V3AppState::from_env();
        let addr = spawn_test_server(state).await;

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/search", &[], None).await;
        assert_eq!(st, 400);
        assert_eq!(j["error"]["code"], "INVALID_QUERY");

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/search?q=%20%20", &[], None).await;
        assert_eq!(st, 400);
        assert_eq!(j["error"]["code"], "INVALID_QUERY");

        let long_q = "a".repeat(205);
        let (st, j) = send_http(addr, "GET", &format!("/api/v1/anime/search?q={long_q}"), &[], None).await;
        assert_eq!(st, 400);
        assert_eq!(j["error"]["code"], "INVALID_QUERY");

        // Missing session
        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001", &[], None).await;
        assert_eq!(st, 401);
        assert_eq!(j["error"]["code"], "SESSION_REQUIRED");

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
        assert_eq!(j["error"]["code"], "SESSION_INVALID");

        // Unknown / expired session
        let (st, j) = send_http(
            addr,
            "GET",
            "/api/v1/anime/NP-A-001",
            &[("X-PV-Session", "000000000000000000000000000000000000000000000000")],
            None,
        )
        .await;
        assert_eq!(st, 401);
        assert_eq!(j["error"]["code"], "SESSION_EXPIRED");
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
                episodes: vec![
                    CanonicalEpisode {
                        id: "NP-E-001".to_string(),
                        anime_id: "NP-A-001".to_string(),
                        season_id: "NP-S-001".to_string(),
                        season_number: 1,
                        episode_number: 1,
                        title: "Episode 1".to_string(),
                        overview: None,
                        provider_mappings: vec![],
                        audio: Some(AudioAvailability {
                            hindi: AudioStatus::Missing,
                            english: AudioStatus::Available,
                            japanese: AudioStatus::Unknown,
                            available_languages: vec!["en".to_string()],
                            checked_sources: true,
                        }),
                        sources: Some(vec![ProviderSourceCandidate {
                            source_id: "src-1".to_string(),
                            provider_id: ProviderKind::MovieBox,
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
                            quality: Some("1080p".to_string()),
                            resolution_u64: 1080,
                            available: true,
                        }]),
                    },
                    CanonicalEpisode {
                        id: "NP-E-002".to_string(),
                        anime_id: "NP-A-001".to_string(),
                        season_id: "NP-S-001".to_string(),
                        season_number: 1,
                        episode_number: 2,
                        title: "Episode 2".to_string(),
                        overview: None,
                        provider_mappings: vec![],
                        audio: Some(AudioAvailability {
                            hindi: AudioStatus::Missing,
                            english: AudioStatus::Missing,
                            japanese: AudioStatus::Missing,
                            available_languages: vec![],
                            checked_sources: true,
                        }),
                        sources: Some(vec![]),
                    },
                ],
            }],
            total_seasons: 1,
            total_episodes: 2,
            season_structure_status: "complete".to_string(),
            details_hydrated: true,
        };
        let session = state.sessions.create_session(vec![anime]).await;
        let sid = session.read().await.session_id.clone();

        let addr = spawn_test_server(state).await;
        let hdrs = [("X-PV-Session", sid.as_str())];

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["id"], "NP-A-001");

        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-001/seasons", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["seasons"].as_array().unwrap().len(), 1);

        let (st, j) = send_http(addr, "GET", "/api/v1/seasons/NP-S-001/episodes", &hdrs, None).await;
        assert_eq!(st, 200);
        // Both episodes visible even when audio is missing on NP-E-002
        assert_eq!(j["episodes"].as_array().unwrap().len(), 2);

        let (st, j) = send_http(addr, "GET", "/api/v1/episodes/NP-E-001", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["audio"]["english"], "available");
        assert_eq!(j["audio"]["hindi"], "missing");

        let (st, j) = send_http(addr, "GET", "/api/v1/episodes/NP-E-001/sources", &hdrs, None).await;
        assert_eq!(st, 200);
        assert_eq!(j["sources"].as_array().unwrap().len(), 1);

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
        assert_eq!(j["error"]["code"], "AUDIO_UNAVAILABLE");

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
        assert_eq!(j["error"]["code"], "SOURCE_UNAVAILABLE");

        // Non-existent anime -> 404 ANIME_NOT_FOUND
        let (st, j) = send_http(addr, "GET", "/api/v1/anime/NP-A-999", &hdrs, None).await;
        assert_eq!(st, 404);
        assert_eq!(j["error"]["code"], "ANIME_NOT_FOUND");
    }
}
