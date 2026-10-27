use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const MAX_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADERS: usize = 64;
const MAX_MANIFEST_BYTES: usize = 10 * 1024 * 1024;
const CHUNK_IDLE_TIMEOUT_SECS: u64 = 60;
const WATCHDOG_IDLE_SECS: u64 = 600;
const PREFETCH_IDLE_CANCEL_SECS: u64 = 5;
const DASH_RANGE_CHUNK_BYTES: usize = 95 * 1024;
const MAX_CACHED_SEGMENTS: usize = 24;
const MAX_CACHED_SEGMENT_CACHE_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONCURRENT_SEGMENT_FETCHES: usize = 4;
const MAX_SEGMENT_BYTES: usize = 16 * 1024 * 1024;
const HLS_CHUNK_TTL: Duration = Duration::from_secs(120);
const MAX_HLS_PLAYLIST_SEGMENTS: usize = 10_000;
const MAX_TRACKED_HLS_PLAYLISTS: usize = 8;
type CachedSegment = (String, String, Arc<[u8]>, Instant);
type CachedManifest = (String, Arc<[u8]>);

#[derive(Clone, Debug)]
struct HlsMediaSegment {
    url: String,
    duration_seconds: f64,
    byte_range: bool,
}

#[derive(Clone, Debug, Default)]
struct HlsMediaPlaylist {
    segments: Vec<HlsMediaSegment>,
    target_duration_seconds: Option<u64>,
    total_duration_seconds: Option<f64>,
    is_vod: bool,
}

pub fn is_hls_url(url: &str) -> bool {
    url.split(['?', '#'])
        .next()
        .unwrap_or(url)
        .to_ascii_lowercase()
        .ends_with(".m3u8")
}

fn parse_hls_media_playlist(manifest: &str, manifest_url: &str) -> HlsMediaPlaylist {
    let Ok(base) = reqwest::Url::parse(manifest_url) else {
        return HlsMediaPlaylist::default();
    };
    let mut playlist = HlsMediaPlaylist::default();
    let mut pending_duration = None;
    let mut pending_byte_range = false;
    let mut total_duration = 0.0f64;
    let mut truncated = false;

    for line in manifest.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
            playlist.target_duration_seconds = value.trim().parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("#EXTINF:") {
            pending_duration = value
                .split(',')
                .next()
                .and_then(|duration| duration.trim().parse::<f64>().ok())
                .filter(|duration| duration.is_finite() && *duration > 0.0);
        } else if line.starts_with("#EXT-X-BYTERANGE:") {
            pending_byte_range = true;
        } else if line == "#EXT-X-ENDLIST" || line == "#EXT-X-PLAYLIST-TYPE:VOD" {
            playlist.is_vod = true;
        } else if !line.is_empty() && !line.starts_with('#') {
            if let Some(duration) = pending_duration.take() {
                if playlist.segments.len() >= MAX_HLS_PLAYLIST_SEGMENTS {
                    truncated = true;
                } else if let Ok(url) = base.join(line)
                    && matches!(url.scheme(), "http" | "https")
                {
                    total_duration += duration;
                    playlist.segments.push(HlsMediaSegment {
                        url: url.to_string(),
                        duration_seconds: duration,
                        byte_range: pending_byte_range,
                    });
                }
            }
            pending_byte_range = false;
        }
    }

    if playlist.is_vod && !truncated && !playlist.segments.is_empty() {
        playlist.total_duration_seconds = Some(total_duration);
    }
    playlist
}

fn register_hls_playlist(ctx: &ProxyContext, playlist_url: &str, manifest: &str) {
    let playlist = parse_hls_media_playlist(manifest, playlist_url);
    if playlist.segments.is_empty() {
        return;
    }
    log::info!(
        "HLS playlist relayed segments={} target_duration_seconds={:?} vod_duration_seconds={:?} vod={} two_second_target={}",
        playlist.segments.len(),
        playlist.target_duration_seconds,
        playlist.total_duration_seconds,
        playlist.is_vod,
        playlist.target_duration_seconds == Some(2)
    );
    if let Ok(mut playlists) = ctx.hls_playlists.lock() {
        if !playlists.contains_key(playlist_url) && playlists.len() >= MAX_TRACKED_HLS_PLAYLISTS {
            if let Some(oldest) = playlists.keys().next().cloned() {
                playlists.remove(&oldest);
            }
        }
        playlists.insert(playlist_url.to_string(), playlist);
    }
}

fn find_hls_segment(
    ctx: &ProxyContext,
    segment_url: &str,
) -> Option<(String, usize, HlsMediaSegment)> {
    let playlists = ctx.hls_playlists.lock().ok()?;
    playlists.iter().find_map(|(playlist_url, playlist)| {
        playlist
            .segments
            .iter()
            .enumerate()
            .find(|(_, segment)| segment.url == segment_url)
            .map(|(index, segment)| (playlist_url.clone(), index, segment.clone()))
    })
}

async fn prefetch_url_is_public(url: &str) -> bool {
    #[cfg(test)]
    if reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_string))
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback())
    {
        return true;
    }
    crate::net::validate_public_url(url, true).await.is_ok()
}

fn schedule_hls_prefetch(ctx: &ProxyContext, playlist_url: &str, current_index: usize) {
    let previous = ctx
        .hls_prefetch
        .lock()
        .ok()
        .and_then(|mut jobs| jobs.remove(playlist_url));
    if let Some(jobs) = previous {
        for job in jobs {
            job.abort();
        }
    }
    if ctx.hls_prefetch_depth == 0 {
        return;
    }
    let next_segments = ctx
        .hls_playlists
        .lock()
        .ok()
        .and_then(|playlists| {
            playlists.get(playlist_url).map(|playlist| {
                playlist
                    .segments
                    .iter()
                    .skip(current_index.saturating_add(1))
                    .filter(|segment| !segment.byte_range)
                    .take(ctx.hls_prefetch_depth)
                    .cloned()
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default();
    if next_segments.is_empty() {
        return;
    }

    let mut jobs = Vec::with_capacity(next_segments.len());
    for segment in next_segments {
        let Some(host) = extract_host_authority(&segment.url) else {
            continue;
        };
        let allowed = ctx.target_host.as_deref() == Some(host.as_str())
            || ctx
                .subtitle_url
                .as_deref()
                .and_then(extract_host_authority)
                .as_deref()
                == Some(host.as_str())
            || ctx
                .hls_allowed_hosts
                .lock()
                .is_ok_and(|hosts| hosts.contains(&host));
        if !allowed {
            continue;
        }
        let url = segment.url.clone();
        let duration = segment.duration_seconds;
        let cache = Arc::clone(&ctx.segment_cache);
        let client = ctx.client.clone();
        let headers = Arc::clone(&ctx.auth_headers);
        let forward_all_headers = ctx.target_host.as_deref() == Some(host.as_str());
        jobs.push(tokio::spawn(async move {
            if !prefetch_url_is_public(&url).await {
                log::debug!("HLS segment prefetch skipped category=unsafe_or_unresolvable_url");
                return;
            }
            let started = Instant::now();
            if fetch_or_get_segment(
                &cache,
                &client,
                &url,
                &headers,
                forward_all_headers,
                true,
            )
            .await
            .is_ok()
            {
                log::debug!(
                    "HLS segment prefetch completed elapsed_ms={} source_duration_seconds={duration:.3}",
                    started.elapsed().as_millis()
                );
            }
        }));
    }
    if !jobs.is_empty()
        && let Ok(mut active) = ctx.hls_prefetch.lock()
    {
        active.insert(playlist_url.to_string(), jobs);
    }
}

fn schedule_dash_prefetch(ctx: &ProxyContext, current_url: &str) {
    const DASH_KEY: &str = "__dash_prefetch__";
    if let Some(previous) = ctx
        .hls_prefetch
        .lock()
        .ok()
        .and_then(|mut jobs| jobs.remove(DASH_KEY))
    {
        for job in previous {
            job.abort();
        }
    }

    let mut jobs = Vec::new();
    let depth = ctx.hls_prefetch_depth.min(u32::MAX as usize) as u32;
    for url in next_dash_segment_urls(current_url, depth) {
        let Some(host) = extract_host_authority(&url) else {
            continue;
        };
        if ctx.target_host.as_deref() != Some(host.as_str())
            || ctx.segment_cache.contains_or_in_flight(&url)
        {
            continue;
        }
        let cache = Arc::clone(&ctx.segment_cache);
        let client = ctx.client.clone();
        let headers = Arc::clone(&ctx.auth_headers);
        jobs.push(tokio::spawn(async move {
            if !prefetch_url_is_public(&url).await {
                log::debug!("DASH prefetch skipped category=unsafe_or_unresolvable_url");
                return;
            }
            let started = Instant::now();
            if fetch_or_get_segment(&cache, &client, &url, &headers, true, true)
                .await
                .is_ok()
            {
                log::debug!(
                    "DASH segment prefetch completed elapsed_ms={}",
                    started.elapsed().as_millis()
                );
            }
        }));
    }
    if !jobs.is_empty()
        && let Ok(mut active) = ctx.hls_prefetch.lock()
    {
        active.insert(DASH_KEY.to_string(), jobs);
    }
}

fn cancel_all_prefetch(ctx: &ProxyContext) {
    let jobs = ctx
        .hls_prefetch
        .lock()
        .ok()
        .map(|mut jobs| std::mem::take(&mut *jobs))
        .unwrap_or_default();
    for playlist_jobs in jobs.into_values() {
        for job in playlist_jobs {
            job.abort();
        }
    }
}

struct SegmentCache {
    completed: Mutex<std::collections::VecDeque<CachedSegment>>,
    in_flight: Mutex<std::collections::HashMap<String, Arc<tokio::sync::Notify>>>,
    fetch_slots: Arc<tokio::sync::Semaphore>,
    prefetch_slots: Arc<tokio::sync::Semaphore>,
    disk_cache: Option<Arc<crate::hls_chunk_cache::HlsChunkCache>>,
    namespace: String,
    ttl: Duration,
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self {
            completed: Mutex::new(std::collections::VecDeque::new()),
            in_flight: Mutex::new(std::collections::HashMap::new()),
            fetch_slots: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_SEGMENT_FETCHES)),
            prefetch_slots: Arc::new(tokio::sync::Semaphore::new(1)),
            disk_cache: None,
            namespace: String::new(),
            ttl: HLS_CHUNK_TTL,
        }
    }
}

impl SegmentCache {
    fn get(&self, url: &str) -> Option<(String, Arc<[u8]>)> {
        let mut lock = self.completed.lock().ok()?;
        let index = lock.iter().position(|(u, _, _, _)| u == url)?;
        if lock[index].3.elapsed() >= self.ttl {
            lock.remove(index);
            return None;
        }
        let item = lock.remove(index)?;
        let result = (item.1.clone(), Arc::clone(&item.2));
        lock.push_back(item);
        Some(result)
    }

    fn contains_or_in_flight(&self, url: &str) -> bool {
        if let Ok(lock) = self.completed.lock() {
            if lock
                .iter()
                .any(|(u, _, _, created)| u == url && created.elapsed() < self.ttl)
            {
                return true;
            }
        }
        if let Ok(lock) = self.in_flight.lock() {
            if lock.contains_key(url) {
                return true;
            }
        }
        false
    }

    fn insert(&self, url: String, content_type: String, data: Arc<[u8]>) {
        if data.len() > MAX_CACHED_SEGMENT_CACHE_BYTES {
            return;
        }
        if let Ok(mut lock) = self.completed.lock() {
            lock.retain(|(_, _, _, created)| created.elapsed() < self.ttl);
            if lock.iter().any(|(u, _, _, _)| u == &url) {
                return;
            }
            let mut cached_bytes: usize = lock.iter().map(|(_, _, bytes, _)| bytes.len()).sum();
            while !lock.is_empty()
                && (lock.len() >= MAX_CACHED_SEGMENTS
                    || cached_bytes.saturating_add(data.len()) > MAX_CACHED_SEGMENT_CACHE_BYTES)
            {
                if let Some((_, _, evicted, _)) = lock.pop_front() {
                    cached_bytes = cached_bytes.saturating_sub(evicted.len());
                }
            }
            lock.push_back((url, content_type, data, Instant::now()));
        }
    }
}

struct InFlightGuard {
    cache: Arc<SegmentCache>,
    url: String,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut lock) = self.cache.in_flight.lock() {
            lock.remove(&self.url);
        }
        self.notify.notify_waiters();
    }
}

#[derive(Clone)]
struct ProxyContext {
    proxy_port: u16,
    client: reqwest::Client,
    auth_headers: Arc<Vec<(String, String)>>,
    target_host: Option<String>,
    subtitle_url: Option<String>,
    max_height: Option<u64>,
    segment_cache: Arc<SegmentCache>,
    manifest_cache: Arc<Mutex<Option<CachedManifest>>>,
    hls_playlists: Arc<Mutex<std::collections::HashMap<String, HlsMediaPlaylist>>>,
    hls_allowed_hosts: Arc<Mutex<HashSet<String>>>,
    hls_prefetch: Arc<Mutex<std::collections::HashMap<String, Vec<tokio::task::JoinHandle<()>>>>>,
    hls_prefetch_depth: usize,
}
struct ConnectionGuard {
    conns: Arc<AtomicUsize>,
    activity: Arc<Mutex<Instant>>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.conns.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut lock) = self.activity.lock() {
            *lock = Instant::now();
        }
    }
}

pub fn spawn_sidecar(
    target_url: &str,
    headers: &[(String, String)],
    subtitle_url: Option<&str>,
    max_height: Option<u64>,
) -> Result<(String, std::process::Child), String> {
    let exe = std::env::current_exe()
        .ok()
        .or_else(|| std::env::args().next().map(PathBuf::from))
        .ok_or_else(|| "unable to locate current executable".to_string())?;

    let headers_json = serde_json::to_string(headers).unwrap_or_else(|_| "[]".to_string());
    let sub_arg = subtitle_url.unwrap_or("");
    let height_arg = max_height.map(|h| h.to_string()).unwrap_or_default();

    let mut cmd = Command::new(exe);
    cmd.args([
        "--proxy-for-vlc",
        target_url,
        &headers_json,
        sub_arg,
        &height_arg,
    ]);
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::null());
    crate::player::configure_detached_process(&mut cmd);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn proxy sidecar: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture sidecar stdout".to_string())?;

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let mut line = String::new();
        let _ = std::io::BufReader::new(stdout).read_line(&mut line);
        let _ = tx.send(line);
    });

    let line = rx.recv_timeout(Duration::from_secs(8)).map_err(|_| {
        let _ = child.kill();
        let _ = child.wait();
        "proxy sidecar timed out waiting for PORT line".to_string()
    })?;

    let port: u16 = line
        .trim()
        .strip_prefix("PORT ")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            let _ = child.kill();
            let _ = child.wait();
            format!("proxy sidecar returned unexpected output: {line:?}")
        })?;

    let proxy_path = if let Some(rest) = target_url.strip_prefix("https://") {
        format!("/https/{rest}")
    } else if let Some(rest) = target_url.strip_prefix("http://") {
        format!("/http/{rest}")
    } else {
        format!("/https/{target_url}")
    };

    Ok((format!("http://127.0.0.1:{port}{proxy_path}"), child))
}

pub async fn run_sidecar(
    target_url: String,
    headers: Vec<(String, String)>,
    subtitle_url: Option<String>,
    max_height: Option<u64>,
) {
    let config = crate::config::load();
    if let Err(error) = config.validate_hls_chunk_settings() {
        println!("ERROR invalid HLS chunk-cache configuration: {error}");
        use std::io::Write;
        let _ = std::io::stdout().flush();
        return;
    }
    let disk_cache = match crate::hls_chunk_cache::HlsChunkCache::open(
        &crate::config::cache_dir().join("hls-chunks"),
        HLS_CHUNK_TTL,
        config.hls_chunk_cache_max_bytes,
    ) {
        Ok(cache) => Some(Arc::new(cache)),
        Err(error) => {
            log::warn!("HLS chunk disk cache unavailable; using bounded memory cache: {error}");
            None
        }
    };
    let namespace_material = serde_json::to_vec(&headers).unwrap_or_default();
    let namespace = crate::hls_chunk_cache::HlsChunkCache::key_for(
        &target_url,
        &String::from_utf8_lossy(&namespace_material),
    );
    let segment_cache = Arc::new(match disk_cache {
        Some(cache) => SegmentCache {
            disk_cache: Some(cache.clone()),
            namespace,
            ..SegmentCache::default()
        },
        None => SegmentCache::default(),
    });
    if let Some(cache) = segment_cache.disk_cache.clone() {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                if let Err(error) = cache.cleanup_expired().await {
                    log::warn!("HLS chunk TTL cleanup failed: {error}");
                }
            }
        });
    }
    log::info!(
        "HLS sidecar cache ready ttl_seconds=120 budget_bytes={} prefetch_depth={} disk_enabled={}",
        config.hls_chunk_cache_max_bytes,
        config.hls_prefetch_depth,
        segment_cache.disk_cache.is_some()
    );

    let client = crate::net::http_client_builder_base()
        .http1_only()
        .pool_max_idle_per_host(16)
        .connect_timeout(Duration::from_secs(15))
        .build()
        .unwrap_or_default();

    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(l) => l,
        Err(_) => return,
    };

    let port = match listener.local_addr() {
        Ok(addr) => addr.port(),
        Err(_) => return,
    };

    println!("PORT {port}");
    use std::io::Write;
    let _ = std::io::stdout().flush();

    let active_connections = Arc::new(AtomicUsize::new(0));
    let last_activity = Arc::new(Mutex::new(Instant::now()));

    let watchdog_conns = Arc::clone(&active_connections);
    let watchdog_activity = Arc::clone(&last_activity);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            let conns = watchdog_conns.load(Ordering::Relaxed);
            let elapsed = {
                let lock = watchdog_activity.lock().unwrap();
                lock.elapsed()
            };
            if conns == 0 && elapsed > Duration::from_secs(WATCHDOG_IDLE_SECS) {
                std::process::exit(0);
            }
        }
    });

    let ctx = ProxyContext {
        proxy_port: port,
        client,
        auth_headers: Arc::new(headers),
        target_host: extract_host_authority(&target_url),
        subtitle_url,
        max_height,
        segment_cache,
        manifest_cache: Arc::new(Mutex::new(None)),
        hls_playlists: Arc::new(Mutex::new(std::collections::HashMap::new())),
        hls_allowed_hosts: Arc::new(Mutex::new(HashSet::new())),
        hls_prefetch: Arc::new(Mutex::new(std::collections::HashMap::new())),
        hls_prefetch_depth: config.hls_prefetch_depth,
    };

    let idle_ctx = ctx.clone();
    let idle_conns = Arc::clone(&active_connections);
    let idle_activity = Arc::clone(&last_activity);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let idle_for = idle_activity
                .lock()
                .map(|activity| activity.elapsed())
                .unwrap_or_default();
            if idle_conns.load(Ordering::Relaxed) == 0
                && idle_for >= Duration::from_secs(PREFETCH_IDLE_CANCEL_SECS)
            {
                cancel_all_prefetch(&idle_ctx);
            }
        }
    });

    if crate::player::is_dash_url(&target_url) {
        let warm_ctx = ctx.clone();
        let warm_url = target_url.clone();
        tokio::spawn(async move {
            warmup_dash_sidecar(&warm_ctx, &warm_url).await;
        });
    }
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => {
                let _ = conn.0.set_nodelay(true);
                conn
            }
            Err(err) => {
                log::warn!("transient proxy accept error: {err}");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let conn_ctx = ctx.clone();
        let active_conns = Arc::clone(&active_connections);
        let activity = Arc::clone(&last_activity);

        active_conns.fetch_add(1, Ordering::Relaxed);
        {
            if let Ok(mut lock) = activity.lock() {
                *lock = Instant::now();
            }
        }

        tokio::spawn(async move {
            let _guard = ConnectionGuard {
                conns: active_conns,
                activity,
            };
            let _ = handle_connection(stream, &conn_ctx).await;
        });
    }
}

pub(crate) fn extract_host_authority(url: &str) -> Option<String> {
    let after_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let authority = after_scheme.split('/').next()?;
    if authority.is_empty() {
        None
    } else {
        Some(authority.to_string())
    }
}

pub(crate) fn should_forward_header(forward_all: bool, name: &str) -> bool {
    forward_all || name.eq_ignore_ascii_case("user-agent") || name.eq_ignore_ascii_case("referer")
}

async fn handle_connection(
    stream: TcpStream,
    ctx: &ProxyContext,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let proxy_port = ctx.proxy_port;
    let client = &ctx.client;
    let auth_headers = ctx.auth_headers.as_slice();
    let target_host = ctx.target_host.as_deref();
    let subtitle_url = ctx.subtitle_url.as_deref();
    let max_height = ctx.max_height;
    let (reader, writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut writer = tokio::io::BufWriter::with_capacity(128 * 1024, writer);

    loop {
        let mut request_line = String::new();
        let n = tokio::time::timeout(
            Duration::from_secs(10),
            read_bounded_line(&mut buf_reader, &mut request_line, MAX_LINE_BYTES),
        )
        .await
        .map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy request line timed out")
        })??;
        if n == 0 {
            return Ok(());
        }
        if request_line.len() > MAX_LINE_BYTES {
            writer
                .write_all(b"HTTP/1.1 431 Request Header Fields Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await?;
            writer.flush().await?;
            return Ok(());
        }

        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("GET");
        let path_and_query = parts.next().unwrap_or("/");

        let mut range_header = None;
        let mut client_close = false;
        let mut header_count = 0usize;
        loop {
            let mut header_line = String::new();
            let n = tokio::time::timeout(
                Duration::from_secs(10),
                read_bounded_line(&mut buf_reader, &mut header_line, MAX_LINE_BYTES),
            )
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy header timed out")
            })??;
            if n == 0 {
                break;
            }
            let trimmed = header_line.trim();
            if trimmed.is_empty() {
                break;
            }
            if header_line.len() > MAX_LINE_BYTES {
                break;
            }
            header_count += 1;
            if header_count > MAX_HEADERS {
                break;
            }
            if let Some((name, val)) = trimmed.split_once(':') {
                if name.trim().eq_ignore_ascii_case("range") {
                    range_header = Some(val.trim().to_string());
                } else if name.trim().eq_ignore_ascii_case("connection") {
                    if val.trim().eq_ignore_ascii_case("close") {
                        client_close = true;
                    }
                }
            }
        }

        let target_url = match extract_target_url(path_and_query) {
            Some(url) => url,
            None => {
                let response =
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                writer.write_all(response.as_bytes()).await?;
                writer.flush().await?;
                return Ok(());
            }
        };
        if crate::net::validate_public_url(&target_url, true)
            .await
            .is_err()
        {
            let response =
                "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            writer.write_all(response.as_bytes()).await?;
            writer.flush().await?;
            return Ok(());
        }
        let extracted_host = extract_host_authority(&target_url);
        let sub_host = subtitle_url.and_then(extract_host_authority);
        let manifest_host_allowed = extracted_host.as_ref().is_some_and(|host| {
            ctx.hls_allowed_hosts
                .lock()
                .is_ok_and(|hosts| hosts.contains(host))
        });
        let is_allowed = match (target_host, extracted_host.as_deref()) {
            (Some(allowed), Some(extracted)) => {
                extracted == allowed
                    || (sub_host.is_some() && extracted_host == sub_host)
                    || manifest_host_allowed
            }
            _ => manifest_host_allowed,
        };
        if !is_allowed {
            let response =
                "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            writer.write_all(response.as_bytes()).await?;
            writer.flush().await?;
            return Ok(());
        }

        let forward_all_headers = extracted_host.as_deref() == target_host;

        if method == "GET" && (crate::player::is_dash_url(&target_url) || is_hls_url(&target_url)) {
            let cached_mpd = ctx.manifest_cache.lock().ok().and_then(|g| {
                g.as_ref()
                    .filter(|(u, _)| u == &target_url)
                    .map(|(_, b)| Arc::clone(b))
            });
            if let Some(rewritten_bytes) = cached_mpd {
                let conn_header = if client_close {
                    "Connection: close\r\n"
                } else {
                    "Connection: keep-alive\r\n"
                };
                let content_type = if is_hls_url(&target_url) {
                    "application/vnd.apple.mpegurl"
                } else {
                    "application/dash+xml"
                };
                let headers_out = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{conn_header}\r\n",
                    rewritten_bytes.len()
                );
                writer.write_all(headers_out.as_bytes()).await?;
                writer.write_all(&rewritten_bytes).await?;
                writer.flush().await?;
                if client_close {
                    return Ok(());
                }
                continue;
            }
        }

        let hls_segment = if method == "GET" {
            find_hls_segment(ctx, &target_url)
        } else {
            None
        };
        if let Some((playlist_url, segment_index, segment)) = hls_segment.as_ref()
            && !segment.byte_range
        {
            schedule_hls_prefetch(ctx, playlist_url, *segment_index);
            match fetch_or_get_segment(
                &ctx.segment_cache,
                client,
                &target_url,
                auth_headers,
                forward_all_headers,
                false,
            )
            .await
            {
                Ok((seg_status, content_type, seg_bytes)) if seg_status.is_success() => {
                    if let Err(total) = cached_segment_range(&seg_bytes, range_header.as_deref()) {
                        let header = format!(
                            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nAccept-Ranges: bytes\r\nConnection: {}\r\n\r\n",
                            if client_close { "close" } else { "keep-alive" }
                        );
                        writer.write_all(header.as_bytes()).await?;
                        writer.flush().await?;
                    } else {
                        let (out_status, content_range, body) =
                            cached_segment_range(&seg_bytes, range_header.as_deref())
                                .expect("range selection checked above");
                        let header = format!(
                            "HTTP/1.1 {} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{content_range}Accept-Ranges: bytes\r\nAccess-Control-Allow-Origin: *\r\nConnection: {}\r\n\r\n",
                            out_status.as_u16(),
                            out_status.canonical_reason().unwrap_or("OK"),
                            body.len(),
                            if client_close { "close" } else { "keep-alive" }
                        );
                        writer.write_all(header.as_bytes()).await?;
                        writer.write_all(body).await?;
                        writer.flush().await?;
                    }
                    if client_close {
                        return Ok(());
                    }
                    continue;
                }
                Ok((status, _, _)) => {
                    let phrase = status.canonical_reason().unwrap_or("Upstream Error");
                    let header = format!(
                        "HTTP/1.1 {} {}\r\nContent-Length: 0\r\nConnection: {}\r\n\r\n",
                        status.as_u16(),
                        phrase,
                        if client_close { "close" } else { "keep-alive" }
                    );
                    writer.write_all(header.as_bytes()).await?;
                    writer.flush().await?;
                    if client_close {
                        return Ok(());
                    }
                    continue;
                }
                Err(_) => {
                    let response = "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    writer.write_all(response.as_bytes()).await?;
                    writer.flush().await?;
                    return Ok(());
                }
            }
        }

        if method == "GET"
            && is_m4s_segment(&target_url)
            && hls_segment
                .as_ref()
                .is_none_or(|(_, _, segment)| !segment.byte_range)
        {
            schedule_dash_prefetch(ctx, &target_url);

            match fetch_or_get_segment(
                &ctx.segment_cache,
                client,
                &target_url,
                auth_headers,
                forward_all_headers,
                false,
            )
            .await
            {
                Ok((seg_status, content_type, seg_bytes)) => {
                    let conn_hdr = if client_close { "close" } else { "keep-alive" };
                    let (out_status, content_range_hdr, body_slice) = if seg_status.is_success() {
                        match cached_segment_range(&seg_bytes, range_header.as_deref()) {
                            Ok(selection) => selection,
                            Err(total) => {
                                let response = format!(
                                    "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{total}\r\nContent-Length: 0\r\nAccept-Ranges: bytes\r\nConnection: {conn_hdr}\r\n\r\n"
                                );
                                writer.write_all(response.as_bytes()).await?;
                                writer.flush().await?;
                                if client_close {
                                    return Ok(());
                                }
                                continue;
                            }
                        }
                    } else {
                        (seg_status, String::new(), seg_bytes.as_ref())
                    };
                    let accept_ranges = if seg_status.is_success() {
                        "Accept-Ranges: bytes\r\n"
                    } else {
                        ""
                    };
                    let response_hdr = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{content_range_hdr}{accept_ranges}Access-Control-Allow-Origin: *\r\nConnection: {conn_hdr}\r\n\r\n",
                        out_status.as_u16(),
                        out_status.canonical_reason().unwrap_or("OK"),
                        body_slice.len()
                    );
                    writer.write_all(response_hdr.as_bytes()).await?;
                    writer.write_all(body_slice).await?;
                    writer.flush().await?;
                    if client_close {
                        return Ok(());
                    }
                    continue;
                }
                Err(e) => {
                    let body = format!("Gateway Error: {e}");
                    let response = format!(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    writer.write_all(response.as_bytes()).await?;
                    writer.flush().await?;
                    return Ok(());
                }
            }
        }

        let mut req = match method {
            "HEAD" => client.head(&target_url),
            _ => client.get(&target_url),
        };

        for (name, val) in auth_headers {
            if should_forward_header(forward_all_headers, name) {
                req = req.header(name.as_str(), val.as_str());
            }
        }
        if let Some(range) = range_header {
            req = req.header("Range", range);
        }
        let upstream_res = match req.send().await {
            Ok(res) => res,
            Err(e) => {
                let body = format!("Gateway Error: {e}");
                let response = format!(
                    "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                writer.write_all(response.as_bytes()).await?;
                writer.flush().await?;
                return Ok(());
            }
        };

        let status = upstream_res.status();
        let effective_url = upstream_res.url().to_string();

        let content_length = upstream_res
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        let is_dash_manifest = crate::player::is_dash_url(&target_url)
            || upstream_res
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.contains("dash+xml") || ct.contains("xml"))
                .unwrap_or(false);
        let is_hls_manifest = is_hls_url(&target_url)
            || upstream_res
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|ct| {
                    let lower = ct.to_ascii_lowercase();
                    lower.contains("mpegurl") || lower.contains("x-mpegurl")
                })
                .unwrap_or(false);

        let within_manifest_limit = content_length.is_none_or(|len| len <= MAX_MANIFEST_BYTES);

        if is_dash_manifest && status.is_success() && within_manifest_limit {
            let manifest_bytes = upstream_res.bytes().await?;
            if manifest_bytes.len() > MAX_MANIFEST_BYTES {
                let body = "Manifest too large";
                writer
                .write_all(
                    format!(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await?;
                writer.flush().await?;
                return Ok(());
            }
            let manifest_str = String::from_utf8_lossy(&manifest_bytes);
            let rewritten = rewrite_dash_manifest(
                &manifest_str,
                proxy_port,
                target_host,
                subtitle_url,
                max_height,
            );
            let rewritten_bytes = rewritten.as_bytes();
            if let Ok(mut lock) = ctx.manifest_cache.lock() {
                *lock = Some((target_url.clone(), Arc::from(rewritten_bytes)));
            }

            let conn_header = if client_close {
                "Connection: close\r\n"
            } else {
                "Connection: keep-alive\r\n"
            };
            let headers_out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/dash+xml\r\nContent-Length: {}\r\n{conn_header}\r\n",
                rewritten_bytes.len()
            );
            writer.write_all(headers_out.as_bytes()).await?;
            writer.write_all(rewritten_bytes).await?;
            writer.flush().await?;
            if client_close {
                return Ok(());
            }
            continue;
        }
        if is_hls_manifest && status.is_success() && within_manifest_limit {
            let manifest_bytes = upstream_res.bytes().await?;
            if manifest_bytes.len() > MAX_MANIFEST_BYTES {
                let body = "Manifest too large";
                writer
                    .write_all(
                        format!(
                            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await?;
                writer.flush().await?;
                return Ok(());
            }
            let manifest_str = String::from_utf8_lossy(&manifest_bytes);
            remember_hls_manifest_hosts(ctx, &manifest_str, &effective_url);
            register_hls_playlist(ctx, &effective_url, &manifest_str);
            let rewritten = rewrite_hls_manifest(&manifest_str, &effective_url, proxy_port);
            let rewritten_bytes = rewritten.as_bytes();
            let conn_header = if client_close {
                "Connection: close\r\n"
            } else {
                "Connection: keep-alive\r\n"
            };
            let headers_out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.apple.mpegurl\r\nContent-Length: {}\r\n{conn_header}\r\n",
                rewritten_bytes.len()
            );
            writer.write_all(headers_out.as_bytes()).await?;
            writer.write_all(rewritten_bytes).await?;
            writer.flush().await?;
            if client_close {
                return Ok(());
            }
            continue;
        }
        let status_line = format!(
            "HTTP/1.1 {} {}\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or("OK")
        );
        writer.write_all(status_line.as_bytes()).await?;
        let keep_alive = !client_close && content_length.is_some();
        let headers_bytes =
            format_proxy_response_headers(upstream_res.headers(), &target_url, keep_alive);
        writer.write_all(&headers_bytes).await?;
        writer.flush().await?;
        let mut stream = upstream_res.bytes_stream();
        loop {
            let chunk_result =
                tokio::time::timeout(Duration::from_secs(CHUNK_IDLE_TIMEOUT_SECS), stream.next())
                    .await;
            match chunk_result {
                Ok(Some(Ok(chunk))) => {
                    writer.write_all(&chunk).await?;
                    writer.flush().await?;
                }
                Ok(Some(Err(e))) => return Err(Box::new(e)),
                Ok(None) => break,
                Err(_elapsed) => break,
            }
        }
        writer.flush().await?;

        if !keep_alive {
            return Ok(());
        }
    }
}

fn is_m4s_segment(url: &str) -> bool {
    let clean = url
        .split('?')
        .next()
        .unwrap_or("")
        .split('#')
        .next()
        .unwrap_or("");
    clean.to_ascii_lowercase().ends_with(".m4s")
}

async fn warmup_dash_sidecar(ctx: &ProxyContext, mpd_url: &str) {
    let mut req = ctx.client.get(mpd_url);
    for (name, val) in ctx.auth_headers.iter() {
        req = req.header(name.as_str(), val.as_str());
    }
    let Ok(res) = req.send().await else {
        return;
    };
    if !res.status().is_success() {
        return;
    }
    let Ok(manifest_bytes) = res.bytes().await else {
        return;
    };
    if manifest_bytes.len() > MAX_MANIFEST_BYTES {
        return;
    }
    let manifest_str = String::from_utf8_lossy(&manifest_bytes);
    let rewritten = rewrite_dash_manifest(
        &manifest_str,
        ctx.proxy_port,
        ctx.target_host.as_deref(),
        ctx.subtitle_url.as_deref(),
        ctx.max_height,
    );
    if let Ok(mut lock) = ctx.manifest_cache.lock() {
        *lock = Some((mpd_url.to_string(), Arc::from(rewritten.as_bytes())));
    }
    if ctx.hls_prefetch_depth == 0 {
        return;
    }
    let Some((base_dir, _)) = mpd_url.rsplit_once('/') else {
        return;
    };
    let vid_id = if rewritten.contains("id=\"0\"") {
        "0"
    } else if rewritten.contains("id=\"1\"") {
        "1"
    } else {
        "2"
    };
    let mut jobs = Vec::new();
    for rel in [
        format!("init-stream{vid_id}.m4s"),
        "init-stream3.m4s".to_string(),
        format!("chunk-stream{vid_id}-00001.m4s"),
        "chunk-stream3-00001.m4s".to_string(),
    ] {
        let seg_url = format!("{base_dir}/{rel}");
        if !ctx.segment_cache.contains_or_in_flight(&seg_url) {
            let cache_clone = Arc::clone(&ctx.segment_cache);
            let client_clone = ctx.client.clone();
            let headers_clone = Arc::clone(&ctx.auth_headers);
            jobs.push(tokio::spawn(async move {
                if !prefetch_url_is_public(&seg_url).await {
                    return;
                }
                let _ = fetch_or_get_segment(
                    &cache_clone,
                    &client_clone,
                    &seg_url,
                    &headers_clone,
                    true,
                    true,
                )
                .await;
            }));
        }
    }
    if !jobs.is_empty()
        && let Ok(mut active) = ctx.hls_prefetch.lock()
    {
        active.insert("__dash_prefetch__".to_string(), jobs);
    }
}

fn next_dash_segment_urls(url: &str, count: u32) -> Vec<String> {
    let (base, query_suffix) = match url.find('?') {
        Some(idx) => (&url[..idx], &url[idx..]),
        None => (url, ""),
    };
    if !base.to_ascii_lowercase().ends_with(".m4s") {
        return Vec::new();
    }
    let without_ext = &base[..base.len() - 4];
    let file_stem = without_ext.rsplit('/').next().unwrap_or(without_ext);
    if let Some(init_id) = file_stem.strip_prefix("init-stream") {
        let dir_prefix = &without_ext[..without_ext.len() - file_stem.len()];
        return if init_id == "3" {
            vec![format!("{dir_prefix}init-stream0.m4s{query_suffix}")]
        } else {
            vec![format!("{dir_prefix}init-stream3.m4s{query_suffix}")]
        };
    }
    if file_stem.to_ascii_lowercase().starts_with("init") {
        return Vec::new();
    }
    let digit_count = without_ext
        .bytes()
        .rev()
        .take_while(|b| b.is_ascii_digit())
        .count();
    if digit_count == 0 || digit_count > 9 {
        return Vec::new();
    }
    let split_pos = without_ext.len() - digit_count;
    let prefix = &without_ext[..split_pos];
    if !prefix.ends_with(['-', '_', '/']) {
        return Vec::new();
    }
    let digits = &without_ext[split_pos..];
    let Ok(num) = digits.parse::<u64>() else {
        return Vec::new();
    };
    let mut out: Vec<String> = (1..=u64::from(count))
        .map(|step| {
            let next_num = num + step;
            format!("{prefix}{next_num:0digit_count$}.m4s{query_suffix}")
        })
        .collect();

    for vid_tag in ["chunk-stream0-", "chunk-stream1-", "chunk-stream2-"] {
        if let Some(dir_prefix) = prefix.strip_suffix(vid_tag) {
            out.push(format!(
                "{dir_prefix}chunk-stream3-{num:0digit_count$}.m4s{query_suffix}"
            ));
            let next_num = num + 1;
            out.push(format!(
                "{dir_prefix}chunk-stream3-{next_num:0digit_count$}.m4s{query_suffix}"
            ));
            break;
        }
    }
    out
}

async fn read_bounded_line<R>(
    reader: &mut R,
    line: &mut String,
    limit: usize,
) -> std::io::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let mut bytes = Vec::with_capacity(limit.min(1024));
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if bytes.is_empty() {
                return Ok(0);
            }
            break;
        }
        let take = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|position| position + 1)
            .unwrap_or(available.len());
        if bytes.len().saturating_add(take) > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "proxy request line exceeds size limit",
            ));
        }
        let has_newline = available[..take].last() == Some(&b'\n');
        bytes.extend_from_slice(&available[..take]);
        reader.consume(take);
        if has_newline {
            break;
        }
    }
    *line = String::from_utf8_lossy(&bytes).into_owned();
    Ok(bytes.len())
}

fn parse_byte_range_request(header_val: &str, total: usize) -> Result<Option<(usize, usize)>, ()> {
    if total == 0 {
        return Ok(None);
    }
    let Some(spec) = header_val.trim().strip_prefix("bytes=") else {
        return Err(());
    };
    if spec.contains(',') {
        return Err(());
    }
    let Some((start_str, end_str)) = spec.split_once('-') else {
        return Err(());
    };
    if start_str.trim().is_empty() {
        let Ok(suffix_len) = end_str.trim().parse::<usize>() else {
            return Err(());
        };
        if suffix_len == 0 {
            return Ok(None);
        }
        let length = suffix_len.min(total);
        return Ok(Some((total - length, total - 1)));
    }
    let Ok(start) = start_str.trim().parse::<usize>() else {
        return Err(());
    };
    if start >= total {
        return Ok(None);
    }
    let end = if end_str.trim().is_empty() {
        total - 1
    } else {
        let Ok(end) = end_str.trim().parse::<usize>() else {
            return Err(());
        };
        end.min(total - 1)
    };
    if start <= end {
        Ok(Some((start, end)))
    } else {
        Ok(None)
    }
}

fn cached_segment_range<'a>(
    bytes: &'a [u8],
    range_header: Option<&str>,
) -> Result<(reqwest::StatusCode, String, &'a [u8]), usize> {
    let Some(header) = range_header else {
        return Ok((reqwest::StatusCode::OK, String::new(), bytes));
    };
    let (start, end) = match parse_byte_range_request(header, bytes.len()) {
        Ok(Some(range)) => range,
        Ok(None) => return Err(bytes.len()),
        Err(()) => return Ok((reqwest::StatusCode::OK, String::new(), bytes)),
    };
    Ok((
        reqwest::StatusCode::PARTIAL_CONTENT,
        format!("Content-Range: bytes {start}-{end}/{}\r\n", bytes.len()),
        &bytes[start..=end],
    ))
}

async fn fetch_or_get_segment(
    cache: &Arc<SegmentCache>,
    client: &reqwest::Client,
    url: &str,
    auth_headers: &[(String, String)],
    forward_all_headers: bool,
    is_prefetch: bool,
) -> Result<(reqwest::StatusCode, String, Arc<[u8]>), String> {
    let wait_deadline =
        tokio::time::Instant::now() + Duration::from_secs(CHUNK_IDLE_TIMEOUT_SECS + 30);
    let _prefetch_permit = if is_prefetch {
        Some(
            cache
                .prefetch_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| "segment prefetch limiter is closed".to_string())?,
        )
    } else {
        None
    };
    loop {
        if let Some((ct, data)) = cache.get(url) {
            log::debug!("media chunk cache hit layer=memory");
            return Ok((reqwest::StatusCode::OK, ct, data));
        }
        if let Some(disk) = cache.disk_cache.as_ref() {
            match disk.get(&cache.namespace, url).await {
                Ok(Some(lease)) => {
                    let content_type = lease.content_type.clone();
                    let data = Arc::clone(&lease.bytes);
                    cache.insert(url.to_string(), content_type.clone(), Arc::clone(&data));
                    log::debug!("media chunk cache hit layer=disk");
                    return Ok((reqwest::StatusCode::OK, content_type, data));
                }
                Ok(None) => {}
                Err(_) => log::warn!("HLS chunk cache read failed; falling back to upstream"),
            }
        }

        let (is_owner, notify, notified_fut) = match cache.in_flight.lock() {
            Ok(mut lock) => {
                if let Some(existing) = lock.get(url) {
                    let n = Arc::clone(existing);
                    (false, Arc::clone(&n), Some(n.notified_owned()))
                } else {
                    let n = Arc::new(tokio::sync::Notify::new());
                    lock.insert(url.to_string(), Arc::clone(&n));
                    (true, n, None)
                }
            }
            Err(_) => (true, Arc::new(tokio::sync::Notify::new()), None),
        };

        if let Some(fut) = notified_fut {
            let remaining = wait_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err("timed out waiting for a coalesced segment fetch".to_string());
            }
            let _ = tokio::time::timeout(remaining.min(Duration::from_secs(1)), fut).await;
            continue;
        }

        let _guard = if is_owner {
            Some(InFlightGuard {
                cache: Arc::clone(cache),
                url: url.to_string(),
                notify,
            })
        } else {
            None
        };

        let _fetch_permit = if is_prefetch {
            let mut reservation = cache
                .fetch_slots
                .clone()
                .try_acquire_many_owned(2)
                .map_err(|_| {
                    "prefetch deferred to preserve interactive fetch capacity".to_string()
                })?;
            let active = reservation
                .split(1)
                .ok_or_else(|| "could not reserve prefetch fetch slot".to_string())?;
            drop(reservation);
            active
        } else {
            cache
                .fetch_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| "segment fetch limiter is closed".to_string())?
        };
        log::debug!("media chunk cache miss prefetch={is_prefetch}");
        let fetch = async {
            if is_m4s_segment(url) {
                fetch_m4s_chunked(client, url, auth_headers, forward_all_headers).await
            } else {
                fetch_full_hls_segment(client, url, auth_headers, forward_all_headers).await
            }
        };
        let (status, content_type, data) =
            tokio::time::timeout(Duration::from_secs(CHUNK_IDLE_TIMEOUT_SECS + 30), fetch)
                .await
                .map_err(|_| "media segment fetch timed out".to_string())??;
        if status.is_success() && !data.is_empty() && data.len() <= MAX_SEGMENT_BYTES {
            cache.insert(url.to_string(), content_type.clone(), Arc::clone(&data));
            if let Some(disk) = cache.disk_cache.as_ref() {
                match disk
                    .insert(&cache.namespace, url, &content_type, &data)
                    .await
                {
                    Ok(true) => log::debug!("media chunk persisted to bounded HLS cache"),
                    Ok(false) => {
                        log::debug!("media chunk not persisted category=cache_budget_or_size")
                    }
                    Err(_) => {
                        log::warn!(
                            "HLS chunk cache write failed; serving complete upstream segment"
                        )
                    }
                }
            }
        }
        return Ok((status, content_type, data));
    }
}

async fn fetch_full_hls_segment(
    client: &reqwest::Client,
    url: &str,
    auth_headers: &[(String, String)],
    forward_all_headers: bool,
) -> Result<(reqwest::StatusCode, String, Arc<[u8]>), String> {
    let mut request = client.get(url);
    for (name, value) in auth_headers {
        if should_forward_header(forward_all_headers, name) {
            request = request.header(name.as_str(), value.as_str());
        }
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    if !status.is_success() {
        return Ok((status, content_type, Arc::from([])));
    }
    let body = read_response_limited(response, MAX_SEGMENT_BYTES).await?;
    Ok((status, content_type, Arc::from(body)))
}

async fn read_response_limited(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, String> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(format!("upstream segment exceeds the {limit}-byte limit"));
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(format!("upstream segment exceeds the {limit}-byte limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn fetch_m4s_chunked(
    client: &reqwest::Client,
    url: &str,
    auth_headers: &[(String, String)],
    forward_all_headers: bool,
) -> Result<(reqwest::StatusCode, String, Arc<[u8]>), String> {
    let first_end = DASH_RANGE_CHUNK_BYTES - 1;
    let mut req = client
        .get(url)
        .header("Range", format!("bytes=0-{first_end}"));
    for (name, val) in auth_headers {
        if should_forward_header(forward_all_headers, name) {
            req = req.header(name.as_str(), val.as_str());
        }
    }

    let res = req.send().await.map_err(|error| error.to_string())?;
    let status = res.status();
    let content_type = res
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    if !status.is_success() {
        // Error bodies are intentionally not buffered; the caller only needs the status.
        return Ok((status, content_type, Arc::<[u8]>::from(Vec::<u8>::new())));
    }

    if status == reqwest::StatusCode::OK {
        // Some origins ignore Range. Bound this fallback instead of buffering an arbitrary file.
        let body = read_response_limited(res, MAX_SEGMENT_BYTES).await?;
        return Ok((status, content_type, Arc::from(body)));
    }

    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(format!("unexpected upstream range status: {status}"));
    }
    let range_header = res
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| "upstream omitted Content-Range".to_string())?;
    let (range_start, range_end, total) = parse_content_range(range_header)
        .ok_or_else(|| "upstream returned an invalid Content-Range".to_string())?;
    if range_start != 0
        || range_end < range_start
        || range_end - range_start + 1 > DASH_RANGE_CHUNK_BYTES
        || total <= range_end
        || total > MAX_SEGMENT_BYTES
    {
        return Err("upstream returned an out-of-bounds Content-Range".to_string());
    }
    let first_limit = range_end - range_start + 1;
    let first_chunk = read_response_limited(res, first_limit).await?;
    if first_chunk.len() != first_limit {
        return Err("upstream range length did not match Content-Range".to_string());
    }
    if total == first_chunk.len() {
        return Ok((
            reqwest::StatusCode::OK,
            content_type,
            Arc::from(first_chunk),
        ));
    }

    let mut assembled = Vec::with_capacity(total);
    assembled.extend_from_slice(&first_chunk);
    let mut pos = first_chunk.len();
    while pos < total {
        let end = (pos + DASH_RANGE_CHUNK_BYTES - 1).min(total - 1);
        let mut sub_req = client
            .get(url)
            .header("Range", format!("bytes={pos}-{end}"));
        for (name, val) in auth_headers {
            if should_forward_header(forward_all_headers, name) {
                sub_req = sub_req.header(name.as_str(), val.as_str());
            }
        }
        let sub_res = sub_req.send().await.map_err(|error| error.to_string())?;
        if sub_res.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err("upstream did not honor a DASH byte-range request".to_string());
        }
        let expected = end - pos + 1;
        let sub_range = sub_res
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(parse_content_range);
        if sub_range != Some((pos, end, total)) {
            return Err("upstream returned a mismatched Content-Range".to_string());
        }
        let chunk = read_response_limited(sub_res, expected).await?;
        if chunk.len() != expected || assembled.len().saturating_add(chunk.len()) > total {
            return Err("upstream range length exceeded its declared bounds".to_string());
        }
        assembled.extend_from_slice(&chunk);
        pos = end + 1;
    }

    if assembled.len() != total {
        return Err("assembled DASH segment length did not match Content-Range".to_string());
    }
    Ok((reqwest::StatusCode::OK, content_type, Arc::from(assembled)))
}

fn parse_content_range(header_val: &str) -> Option<(usize, usize, usize)> {
    let value = header_val.trim().strip_prefix("bytes ")?;
    let (range, total) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse::<usize>().ok()?;
    let end = end.parse::<usize>().ok()?;
    let total = total.parse::<usize>().ok()?;
    (start <= end && end < total).then_some((start, end, total))
}
fn format_proxy_response_headers(
    headers: &reqwest::header::HeaderMap,
    target_url: &str,
    keep_alive: bool,
) -> Vec<u8> {
    let mut out = Vec::new();
    let clean_path = target_url
        .split('?')
        .next()
        .unwrap_or("")
        .split('#')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let is_srt = clean_path.ends_with(".srt");
    let is_vtt = clean_path.ends_with(".vtt");

    for (header_name, header_val) in headers {
        let name_str = header_name.as_str();
        if (is_srt || is_vtt) && name_str.eq_ignore_ascii_case("content-type") {
            continue;
        }
        if name_str.eq_ignore_ascii_case("content-type")
            || name_str.eq_ignore_ascii_case("content-length")
            || name_str.eq_ignore_ascii_case("content-range")
            || name_str.eq_ignore_ascii_case("accept-ranges")
        {
            if let Ok(val_str) = header_val.to_str() {
                out.extend_from_slice(format!("{name_str}: {val_str}\r\n").as_bytes());
            }
        }
    }

    out.extend_from_slice(b"Access-Control-Allow-Origin: *\r\n");
    if is_srt {
        out.extend_from_slice(b"Content-Type: application/x-subrip\r\n");
    } else if is_vtt {
        out.extend_from_slice(b"Content-Type: text/vtt\r\n");
    }
    if keep_alive {
        out.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
    } else {
        out.extend_from_slice(b"Connection: close\r\n\r\n");
    }
    out
}

fn extract_target_url(path_and_query: &str) -> Option<String> {
    let raw = path_and_query.strip_prefix('/')?;
    if let Some(rest) = raw.strip_prefix("sub/") {
        if let Ok(decoded) = percent_encoding::percent_decode_str(rest).decode_utf8() {
            let s = decoded.into_owned();
            if s.starts_with("http://") || s.starts_with("https://") {
                return Some(s);
            }
        }
    }
    if let Some(rest) = raw.strip_prefix("https/") {
        Some(format!("https://{rest}"))
    } else if let Some(rest) = raw.strip_prefix("http/") {
        Some(format!("http://{rest}"))
    } else if raw.starts_with("proxy?") || raw.contains("&url=") || raw.starts_with("proxy?url=") {
        let query_start = raw.find('?')?;
        let query = &raw[query_start + 1..];
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "url" {
                    return percent_encoding::percent_decode_str(v)
                        .decode_utf8()
                        .ok()
                        .map(|s| s.into_owned())
                        .filter(|s| s.starts_with("http://") || s.starts_with("https://"));
                }
            }
        }
        None
    } else {
        None
    }
}

fn proxy_media_url(url: &str, proxy_port: u16) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme != "http" && scheme != "https" {
        return None;
    }
    Some(format!("http://127.0.0.1:{proxy_port}/{scheme}/{rest}"))
}

fn rewrite_hls_uri_attributes(line: &str, manifest_url: &reqwest::Url, proxy_port: u16) -> String {
    let mut out = String::with_capacity(line.len());
    let mut cursor = 0;
    while let Some(rel) = line[cursor..].find("URI=") {
        let start = cursor + rel;
        out.push_str(&line[cursor..start + 4]);
        let Some(&quote_byte) = line.as_bytes().get(start + 4) else {
            break;
        };
        let quote = quote_byte as char;
        if quote != '"' && quote != '\'' {
            cursor = start + 4;
            continue;
        }
        let value_start = start + 5;
        let Some(end_rel) = line[value_start..].find(quote) else {
            out.push_str(&line[start + 4..]);
            return out;
        };
        let value_end = value_start + end_rel;
        let raw = &line[value_start..value_end];
        let resolved = manifest_url
            .join(raw)
            .ok()
            .and_then(|url| proxy_media_url(url.as_str(), proxy_port));
        out.push(quote);
        out.push_str(resolved.as_deref().unwrap_or(raw));
        out.push(quote);
        cursor = value_end + 1;
    }
    out.push_str(&line[cursor..]);
    out
}

fn hls_manifest_host_references(manifest: &str, manifest_url: &str) -> HashMap<String, String> {
    let Ok(base) = reqwest::Url::parse(manifest_url) else {
        return HashMap::new();
    };
    let mut hosts = HashMap::new();
    for line in manifest
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let mut references = Vec::new();
        if line.starts_with('#') {
            let mut cursor = 0;
            while let Some(relative) = line[cursor..].find("URI=") {
                let start = cursor + relative + 4;
                let Some(quote) = line.as_bytes().get(start).copied() else {
                    break;
                };
                if quote != b'"' && quote != b'\'' {
                    cursor = start;
                    continue;
                }
                let value_start = start + 1;
                let Some(end_rel) = line[value_start..].find(quote as char) else {
                    break;
                };
                references.push(&line[value_start..value_start + end_rel]);
                cursor = value_start + end_rel + 1;
            }
        } else {
            references.push(line);
        }
        for reference in references {
            let Ok(url) = base.join(reference) else {
                continue;
            };
            if !matches!(url.scheme(), "http" | "https") {
                continue;
            }
            let url = url.to_string();
            if let Some(host) = extract_host_authority(&url) {
                hosts.entry(host).or_insert(url);
                if hosts.len() >= 32 {
                    return hosts;
                }
            }
        }
    }
    hosts
}

fn remember_hls_manifest_hosts(ctx: &ProxyContext, manifest: &str, effective_url: &str) {
    let mut references = hls_manifest_host_references(manifest, effective_url);
    if let Some(host) = extract_host_authority(effective_url) {
        references
            .entry(host)
            .or_insert_with(|| effective_url.to_string());
    }
    // This is only a playlist-derived host allowlist. Each actual request is still
    // independently checked by validate_public_url before passing the host gate.
    for host in references.into_keys().take(32) {
        if let Ok(mut allowed) = ctx.hls_allowed_hosts.lock() {
            allowed.insert(host);
        }
    }
}

fn rewrite_hls_manifest(manifest: &str, manifest_url: &str, proxy_port: u16) -> String {
    let Ok(base) = reqwest::Url::parse(manifest_url) else {
        return manifest.to_string();
    };
    manifest
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                line.to_string()
            } else if trimmed.starts_with('#') {
                if trimmed.contains("URI=") {
                    rewrite_hls_uri_attributes(line, &base, proxy_port)
                } else {
                    line.to_string()
                }
            } else {
                base.join(trimmed)
                    .ok()
                    .and_then(|url| proxy_media_url(url.as_str(), proxy_port))
                    .unwrap_or_else(|| line.to_string())
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn filter_dash_representations(manifest: &str, max_height: u64) -> String {
    let mut reps = Vec::new();
    let mut cursor = 0;
    while let Some(start_rel) = manifest[cursor..].find("<Representation") {
        let start = cursor + start_rel;
        let rest = &manifest[start..];
        if let Some(tag_end_rel) = rest.find('>') {
            let tag_content = &rest[..=tag_end_rel];
            if tag_content.ends_with("/>") {
                let end = start + tag_end_rel + 1;
                reps.push((start, end));
                cursor = end;
            } else if let Some(close_rel) = rest.find("</Representation>") {
                let end = start + close_rel + "</Representation>".len();
                reps.push((start, end));
                cursor = end;
            } else {
                cursor = start + tag_end_rel + 1;
            }
        } else {
            break;
        }
    }

    if reps.is_empty() {
        return manifest.to_string();
    }

    let parse_height = |block: &str| -> Option<u64> {
        let mut search = block;
        while let Some(pos) = search.find("height=") {
            let after = &search[pos + "height=".len()..];
            let quote = after.chars().next()?;
            if quote == '"' || quote == '\'' {
                let digits: String = after[1..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if let Ok(h) = digits.parse::<u64>() {
                    return Some(h);
                }
            }
            search = after;
        }
        None
    };

    let heights: Vec<Option<u64>> = reps
        .iter()
        .map(|&(s, e)| parse_height(&manifest[s..e]))
        .collect();
    let video_heights: Vec<u64> = heights.iter().filter_map(|&h| h).collect();
    if video_heights.is_empty() {
        return manifest.to_string();
    }

    let target_ceiling = if video_heights.iter().any(|&h| h <= max_height) {
        max_height
    } else {
        *video_heights.iter().min().unwrap_or(&max_height)
    };

    let mut out = String::with_capacity(manifest.len());
    let mut last = 0;
    for (&(s, e), h_opt) in reps.iter().zip(heights.iter()) {
        if let Some(h) = h_opt {
            if *h > target_ceiling {
                out.push_str(&manifest[last..s]);
                last = e;
            }
        }
    }
    out.push_str(&manifest[last..]);
    out
}

fn rewrite_dash_manifest(
    manifest: &str,
    proxy_port: u16,
    target_host: Option<&str>,
    subtitle_url: Option<&str>,
    max_height: Option<u64>,
) -> String {
    let Some(host) = target_host else {
        return manifest.to_string();
    };

    let filtered_manifest = if let Some(limit) = max_height.filter(|&h| h > 0) {
        filter_dash_representations(manifest, limit)
    } else {
        manifest.to_string()
    };

    let https_prefix = format!("https://{host}/");
    let http_prefix = format!("http://{host}/");

    let proxy_https = format!("http://127.0.0.1:{proxy_port}/https/{host}/");
    let proxy_http = format!("http://127.0.0.1:{proxy_port}/http/{host}/");

    let mut rewritten = filtered_manifest
        .replace(&https_prefix, &proxy_https)
        .replace(&http_prefix, &proxy_http);

    if let Some(sub) = subtitle_url {
        if !sub.is_empty() {
            let encoded_sub =
                percent_encoding::utf8_percent_encode(sub, percent_encoding::NON_ALPHANUMERIC);
            let sub_proxy_url = format!("http://127.0.0.1:{proxy_port}/sub/{encoded_sub}");
            let sub_mime = match crate::service::subtitle_extension_from_url(sub) {
                "vtt" => "text/vtt",
                "ass" | "ssa" => "text/x-ssa",
                _ => "application/x-subrip",
            };
            let sub_adaptation_set = format!(
                r#"<AdaptationSet contentType="text" mimeType="{sub_mime}" lang="en">
    <Role schemeIdUri="urn:mpeg:dash:role:2011" value="subtitle"/>
    <Representation id="sub_en" bandwidth="1000">
      <BaseURL>{sub_proxy_url}</BaseURL>
    </Representation>
  </AdaptationSet>
</Period>"#
            );
            if rewritten.contains("</Period>") {
                rewritten = rewritten.replacen("</Period>", &sub_adaptation_set, 1);
            }
        }
    }

    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_well_formed_content_ranges() {
        assert_eq!(parse_content_range("bytes 0-99/200"), Some((0, 99, 200)));
        assert_eq!(
            parse_content_range("bytes 100-199/200"),
            Some((100, 199, 200))
        );
        assert_eq!(parse_content_range("bytes */200"), None);
        assert_eq!(parse_content_range("bytes 0-200/200"), None);
        assert_eq!(parse_content_range("bytes 10-2/200"), None);
        assert_eq!(parse_content_range("0-99/200"), None);
    }

    #[test]
    fn segment_cache_respects_total_byte_budget() {
        let cache = SegmentCache::default();
        let payload = Arc::<[u8]>::from(vec![0u8; 40 * 1024 * 1024]);
        cache.insert("first".into(), "video/mp4".into(), Arc::clone(&payload));
        cache.insert("second".into(), "video/mp4".into(), Arc::clone(&payload));
        assert!(cache.get("first").is_none());
        assert!(cache.get("second").is_some());
        assert!(cache.get("second").unwrap().1.len() <= MAX_CACHED_SEGMENT_CACHE_BYTES);
    }

    #[tokio::test]
    async fn in_memory_segment_cache_expires_using_configured_test_ttl() {
        let cache = SegmentCache {
            ttl: Duration::from_millis(10),
            ..SegmentCache::default()
        };
        cache.insert(
            "segment".into(),
            "video/mp2t".into(),
            Arc::from(&b"chunk"[..]),
        );
        assert!(cache.get("segment").is_some());
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(cache.get("segment").is_none());
    }

    #[test]
    fn test_extract_target_url() {
        assert_eq!(
            extract_target_url("/https/sacdn.example.com/dash/index.mpd").as_deref(),
            Some("https://sacdn.example.com/dash/index.mpd")
        );
        assert_eq!(
            extract_target_url("/http/example.com/video.mp4").as_deref(),
            Some("http://example.com/video.mp4")
        );
        assert_eq!(
            extract_target_url("/https/example.com/seg.m4s?token=abc&exp=123").as_deref(),
            Some("https://example.com/seg.m4s?token=abc&exp=123")
        );
        assert_eq!(
            extract_target_url("/proxy?url=https%3A%2F%2Fexample.com%2Ffallback.mpd").as_deref(),
            Some("https://example.com/fallback.mpd")
        );
        assert_eq!(extract_target_url("/invalid/path"), None);
    }

    #[test]
    fn test_rewrite_hls_manifest_rewrites_relative_segments_variants_and_uri_attributes() {
        let manifest = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"Hindi\",LANGUAGE=\"hi\",DEFAULT=YES,URI=\"audio/hi.m3u8\"\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",LANGUAGE=\"en\",DEFAULT=NO,URI=\"audio/en.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"Hindi\",LANGUAGE=\"hi\",URI=\"subs/hi.m3u8\"\n#EXT-X-KEY:METHOD=AES-128,URI=\"keys/key.bin\"\n#EXT-X-STREAM-INF:BANDWIDTH=100000,AUDIO=\"audio\",SUBTITLES=\"subs\"\nvariants/low.m3u8\n#EXTINF:5,\nsegments/001.ts\n";
        let rewritten = rewrite_hls_manifest(
            manifest,
            "https://cdn.example.com/video/master.m3u8?token=abc",
            8765,
        );
        assert!(
            rewritten
                .contains("URI=\"http://127.0.0.1:8765/https/cdn.example.com/video/keys/key.bin\"")
        );
        assert!(
            rewritten
                .contains("http://127.0.0.1:8765/https/cdn.example.com/video/variants/low.m3u8")
        );
        assert!(
            rewritten
                .contains("URI=\"http://127.0.0.1:8765/https/cdn.example.com/video/keys/key.bin\"")
        );
        assert!(rewritten.contains("LANGUAGE=\"hi\",DEFAULT=YES,URI=\"http://127.0.0.1:8765/https/cdn.example.com/video/audio/hi.m3u8\""));
        assert!(rewritten.contains("LANGUAGE=\"en\",DEFAULT=NO,URI=\"http://127.0.0.1:8765/https/cdn.example.com/video/audio/en.m3u8\""));
        assert!(rewritten.contains("TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"Hindi\",LANGUAGE=\"hi\",URI=\"http://127.0.0.1:8765/https/cdn.example.com/video/subs/hi.m3u8\""));
    }

    #[test]
    fn redirected_hls_base_and_cross_host_track_references_are_resolved() {
        let manifest = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",URI=\"https://audio.cdn.example/en.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",URI=\"subs/en.vtt\"\nrelative/seg.ts\n";
        let final_url = "https://final.cdn.example/redirected/master.m3u8?sig=x";
        let rewritten = rewrite_hls_manifest(manifest, final_url, 8765);
        assert!(rewritten.contains("/https/audio.cdn.example/en.m3u8"));
        assert!(rewritten.contains("/https/final.cdn.example/redirected/subs/en.vtt"));
        assert!(rewritten.contains("/https/final.cdn.example/redirected/relative/seg.ts"));
        let references = hls_manifest_host_references(manifest, final_url);
        assert!(references.contains_key("audio.cdn.example"));
        assert!(references.contains_key("final.cdn.example"));
    }

    #[test]
    fn test_is_hls_url_ignores_query_and_fragment() {
        assert!(is_hls_url(
            "https://cdn.example.com/master.m3u8?token=x#part"
        ));
        assert!(!is_hls_url("https://cdn.example.com/video.mp4"));
    }

    #[test]
    fn hls_vod_duration_comes_from_complete_extinf_list() {
        let source = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:2.003,\nseg-0.ts\n#EXTINF:1.997,\nseg-1.ts\n#EXT-X-ENDLIST\n";
        let playlist = parse_hls_media_playlist(source, "https://cdn.example/movie/index.m3u8");
        assert_eq!(playlist.segments.len(), 2);
        assert_eq!(
            playlist.segments[0].url,
            "https://cdn.example/movie/seg-0.ts"
        );
        assert_eq!(playlist.segments[0].duration_seconds, 2.003);
        assert_eq!(playlist.target_duration_seconds, Some(2));
        assert_eq!(playlist.total_duration_seconds, Some(4.0));
        assert!(playlist.is_vod);
        let rewritten = rewrite_hls_manifest(source, "https://cdn.example/movie/index.m3u8", 8765);
        assert!(rewritten.contains("#EXTINF:2.003,"));
        assert!(rewritten.contains("#EXT-X-ENDLIST"));
    }

    #[test]
    fn live_hls_playlist_does_not_claim_a_full_duration_and_marks_byte_ranges() {
        let source = "#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\n#EXT-X-BYTERANGE:10@0\nshared.mp4\n#EXTINF:1.8,\nseg.ts\n";
        let playlist = parse_hls_media_playlist(source, "https://cdn.example/live/index.m3u8");
        assert_eq!(playlist.segments.len(), 2);
        assert!(playlist.segments[0].byte_range);
        assert!(!playlist.segments[1].byte_range);
        assert_eq!(playlist.total_duration_seconds, None);
    }

    #[test]
    fn cached_hls_range_returns_exact_requested_bytes_and_headers() {
        let source = b"0123456789";
        let (status, content_range, body) =
            cached_segment_range(source, Some("bytes=3-6")).unwrap();
        assert_eq!(status, reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(content_range, "Content-Range: bytes 3-6/10\r\n");
        assert_eq!(body, b"3456");
        let (status, content_range, body) = cached_segment_range(source, Some("bytes=-4")).unwrap();
        assert_eq!(status, reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(content_range, "Content-Range: bytes 6-9/10\r\n");
        assert_eq!(body, b"6789");
        assert_eq!(cached_segment_range(source, Some("bytes=12-15")), Err(10));
        assert_eq!(cached_segment_range(source, Some("bytes=-0")), Err(10));
        let (status, _, body) = cached_segment_range(source, Some("bytes=0-1,4-5")).unwrap();
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(body, source);
    }

    #[tokio::test]
    async fn seek_cancels_previous_hls_prefetch_jobs() {
        let ctx = ProxyContext {
            proxy_port: 0,
            client: crate::net::http_client_builder_base().build().unwrap(),
            auth_headers: Arc::new(Vec::new()),
            target_host: None,
            subtitle_url: None,
            max_height: None,
            segment_cache: Arc::new(SegmentCache::default()),
            manifest_cache: Arc::new(Mutex::new(None)),
            hls_playlists: Arc::new(Mutex::new(std::collections::HashMap::new())),
            hls_allowed_hosts: Arc::new(Mutex::new(HashSet::new())),
            hls_prefetch: Arc::new(Mutex::new(std::collections::HashMap::new())),
            hls_prefetch_depth: 2,
        };
        let job = tokio::spawn(std::future::pending::<()>());
        let abort = job.abort_handle();
        ctx.hls_prefetch
            .lock()
            .unwrap()
            .insert("https://cdn.example/index.m3u8".into(), vec![job]);
        schedule_hls_prefetch(&ctx, "https://cdn.example/index.m3u8", 7);
        tokio::task::yield_now().await;
        assert!(abort.is_finished());

        let first_task = tokio::spawn(std::future::pending::<()>());
        let second_task = tokio::spawn(std::future::pending::<()>());
        let first_abort = first_task.abort_handle();
        let second_abort = second_task.abort_handle();
        ctx.hls_prefetch
            .lock()
            .unwrap()
            .insert("https://cdn.example/audio.m3u8".into(), vec![first_task]);
        ctx.hls_prefetch
            .lock()
            .unwrap()
            .insert("__dash_prefetch__".into(), vec![second_task]);
        cancel_all_prefetch(&ctx);
        tokio::task::yield_now().await;
        assert!(first_abort.is_finished());
        assert!(second_abort.is_finished());
        assert!(ctx.hls_prefetch.lock().unwrap().is_empty());
    }

    #[test]
    fn test_extract_target_url_sub_rejects_non_http() {
        assert_eq!(
            extract_target_url("/sub/file%3A%2F%2F%2Fetc%2Fpasswd"),
            None
        );
        assert_eq!(extract_target_url("/sub/data%3Atext%2Fhtml%2Chello"), None);
        assert!(
            extract_target_url("/sub/https%3A%2F%2Fcdn.example.com%2Fsub.vtt")
                .as_deref()
                .unwrap_or("")
                .starts_with("https://")
        );
    }

    #[test]
    fn test_extract_host_authority_strips_path_preserves_port() {
        assert_eq!(
            extract_host_authority("https://cdn.example.com/dash/index.mpd").as_deref(),
            Some("cdn.example.com")
        );
        assert_eq!(
            extract_host_authority("http://cdn.example.com:8080/dash/index.mpd").as_deref(),
            Some("cdn.example.com:8080")
        );
        assert_eq!(extract_host_authority("not-a-url"), None);
    }
    #[test]
    fn test_host_whitelist_allows_target_and_subtitle_hosts() {
        let target_host = "video.example.com";
        let subtitle_url = "https://captions.example.com/sub.srt";
        let sub_host = extract_host_authority(subtitle_url);

        let is_allowed = |url: &str| -> bool {
            let host = extract_host_authority(url);
            host.as_deref() == Some(target_host) || (sub_host.is_some() && host == sub_host)
        };

        assert!(is_allowed("https://video.example.com/chunk.m4s"));
        assert!(is_allowed("https://captions.example.com/sub.srt"));
        assert!(!is_allowed("https://evil.example.com/steal"));
        assert!(!is_allowed("https://sub.evil.com/fake"));
    }
    #[test]
    fn test_host_whitelist_rejects_missing_target_host() {
        let target_host: Option<&str> = None;
        let subtitle_url: Option<&str> = None;
        let sub_host = subtitle_url.and_then(extract_host_authority);

        let is_allowed = |url: &str| -> bool {
            let extracted_host = extract_host_authority(url);
            match (target_host, extracted_host.as_deref()) {
                (Some(allowed), Some(extracted)) => {
                    extracted == allowed || (sub_host.is_some() && extracted_host == sub_host)
                }
                _ => false,
            }
        };

        assert!(!is_allowed("https://video.example.com/chunk.m4s"));
        assert!(!is_allowed("http://127.0.0.1:8080"));
    }

    #[test]
    fn test_rewrite_dash_manifest_scoped_to_target_host() {
        let manifest = r#"<MPD xmlns="urn:mpeg:dash:schema:mpd:2011" xsi:schemaLocation="urn:mpeg:dash:schema:mpd:2011 https://standards.iso.org/schema.xsd">
<Period>
  <AdaptationSet>
    <Representation id="1080p">
      <BaseURL>https://sacdn.example.com/dash/123/1080.mp4</BaseURL>
      <SegmentTemplate initialization="https://sacdn.example.com/dash/123/init.m4s" media="seg_$Number$.m4s" />
    </Representation>
  </AdaptationSet>
</Period>
</MPD>"#;

        let rewritten =
            rewrite_dash_manifest(manifest, 8888, Some("sacdn.example.com"), None, None);
        assert!(
            rewritten.contains("https://standards.iso.org/schema.xsd"),
            "XML namespace schema must not be corrupted"
        );

        assert!(
            rewritten.contains("http://127.0.0.1:8888/https/sacdn.example.com/dash/123/1080.mp4"),
            "Matching host BaseURL must be rewritten to proxy route"
        );

        assert!(
            rewritten.contains("http://127.0.0.1:8888/https/sacdn.example.com/dash/123/init.m4s"),
            "Matching host initialization URL must be rewritten to proxy route"
        );

        assert!(
            rewritten.contains("media=\"seg_$Number$.m4s\""),
            "Relative media template must remain relative"
        );
    }

    #[test]
    fn test_rewrite_dash_manifest_with_explicit_port() {
        let manifest = r#"<MPD><Period><BaseURL>https://cdn.example.com:8080/dash/seg.mp4</BaseURL></Period></MPD>"#;
        let rewritten =
            rewrite_dash_manifest(manifest, 9999, Some("cdn.example.com:8080"), None, None);
        assert!(
            rewritten.contains("http://127.0.0.1:9999/https/cdn.example.com:8080/dash/seg.mp4"),
            "Port must be preserved in proxy route"
        );
    }
    #[test]
    fn test_format_proxy_response_headers_for_srt_with_query_and_case() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("content-type", "text/plain".parse().unwrap());
        headers.insert("content-length", "1234".parse().unwrap());
        headers.insert("accept-ranges", "bytes".parse().unwrap());
        headers.insert("server", "cloudflare".parse().unwrap());

        let url = "https://cdn.example.com/subs.SRT?token=abc123&expires=999#top";
        let out = String::from_utf8(format_proxy_response_headers(&headers, url, false)).unwrap();
        assert_eq!(out.matches("Access-Control-Allow-Origin: *").count(), 1);
        assert_eq!(out.matches("Content-Type: application/x-subrip").count(), 1);
        assert_eq!(out.matches("Connection: close").count(), 1);
        assert!(!out.contains("text/plain"));
        assert!(out.contains("content-length: 1234"));
        assert!(out.contains("accept-ranges: bytes"));
    }

    #[test]
    fn test_format_proxy_response_headers_for_vtt() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        headers.insert("content-length", "500".parse().unwrap());

        let url = "https://cdn.example.com/subs.vtt";
        let out = String::from_utf8(format_proxy_response_headers(&headers, url, false)).unwrap();
        assert_eq!(out.matches("Access-Control-Allow-Origin: *").count(), 1);
        assert_eq!(out.matches("Content-Type: text/vtt").count(), 1);
        assert!(!out.contains("application/octet-stream"));
        assert!(out.contains("content-length: 500"));
    }

    #[test]
    fn test_format_proxy_response_headers_for_media_stream() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("content-type", "video/mp4".parse().unwrap());
        headers.insert("content-length", "10000000".parse().unwrap());

        let url = "https://cdn.example.com/video.mp4";
        let out = String::from_utf8(format_proxy_response_headers(&headers, url, true)).unwrap();
        assert_eq!(out.matches("Access-Control-Allow-Origin: *").count(), 1);
        assert!(out.contains("content-type: video/mp4"));
        assert!(!out.contains("application/x-subrip"));
        assert!(!out.contains("text/vtt"));
    }

    #[test]
    fn test_format_proxy_response_headers_keep_alive() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("content-type", "video/mp4".parse().unwrap());
        headers.insert("content-length", "1000".parse().unwrap());

        let url = "https://cdn.example.com/segment.m4s";
        let out = String::from_utf8(format_proxy_response_headers(&headers, url, true)).unwrap();
        assert!(out.contains("Connection: keep-alive"));
        assert!(!out.contains("Connection: close"));
    }

    #[test]
    fn test_filter_dash_representations_caps_height() {
        let manifest = r#"<MPD>
<Period>
  <AdaptationSet>
    <Representation id="1080" height="1080" bandwidth="800000"><BaseURL>1080.m4s</BaseURL></Representation>
    <Representation id="720" height="720" bandwidth="400000"><BaseURL>720.m4s</BaseURL></Representation>
    <Representation id="480" height="480" bandwidth="200000"><BaseURL>480.m4s</BaseURL></Representation>
  </AdaptationSet>
</Period>
</MPD>"#;
        let filtered = filter_dash_representations(manifest, 720);
        assert!(!filtered.contains("height=\"1080\""));
        assert!(filtered.contains("height=\"720\""));
        assert!(filtered.contains("height=\"480\""));

        let filtered_480 = filter_dash_representations(manifest, 480);
        assert!(!filtered_480.contains("height=\"1080\""));
        assert!(!filtered_480.contains("height=\"720\""));
        assert!(filtered_480.contains("height=\"480\""));
    }

    #[test]
    fn test_next_dash_segment_urls_preserves_zero_padding_and_prefetches_audio() {
        let next = next_dash_segment_urls(
            "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream0-00721.m4s?token=abc",
            3,
        );
        assert_eq!(
            next,
            vec![
                "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream0-00722.m4s?token=abc",
                "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream0-00723.m4s?token=abc",
                "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream0-00724.m4s?token=abc",
                "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream3-00721.m4s?token=abc",
                "https://sbcdn3.hakunaymatata.com/dash/123/chunk-stream3-00722.m4s?token=abc",
            ]
        );
        assert_eq!(
            next_dash_segment_urls(
                "https://sbcdn3.hakunaymatata.com/dash/123/init-stream0.m4s",
                3
            ),
            vec!["https://sbcdn3.hakunaymatata.com/dash/123/init-stream3.m4s"]
        );
        assert!(
            next_dash_segment_urls("https://sbcdn3.hakunaymatata.com/dash/123/init.m4s", 3)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_fetch_m4s_chunked_splits_ranges_and_caches() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let total_size = 240 * 1024;
        let payload: Arc<Vec<u8>> = Arc::new((0..total_size).map(|i| (i % 253) as u8).collect());
        let request_count = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/dash/chunk-stream0-00001.m4s");

        let srv_payload = Arc::clone(&payload);
        let srv_reqs = Arc::clone(&request_count);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let data = Arc::clone(&srv_payload);
                let reqs = Arc::clone(&srv_reqs);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    reqs.fetch_add(1, Ordering::Relaxed);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let mut range_val = None;
                    for line in req.lines() {
                        if let Some(v) = line
                            .strip_prefix("Range: bytes=")
                            .or_else(|| line.strip_prefix("range: bytes="))
                        {
                            range_val = Some(v.trim().to_string());
                        }
                    }
                    let total = data.len();
                    let (start, end) = range_val
                        .and_then(|r| {
                            let (s, e) = r.split_once('-')?;
                            Some((
                                s.parse::<usize>().ok()?,
                                e.parse::<usize>().ok()?.min(total - 1),
                            ))
                        })
                        .unwrap_or((0, total - 1));
                    let slice = &data[start..=end];
                    let hdr = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Type: video/iso.segment\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        slice.len()
                    );
                    let _ = socket.write_all(hdr.as_bytes()).await;
                    let _ = socket.write_all(slice).await;
                });
            }
        });

        let client = crate::net::http_client_builder_base().build().unwrap();
        let cache = Arc::new(SegmentCache::default());
        let (status, ct, assembled) = fetch_or_get_segment(&cache, &client, &url, &[], true, false)
            .await
            .unwrap();
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(ct, "video/iso.segment");
        assert_eq!(assembled.as_ref(), payload.as_slice());
        assert_eq!(request_count.load(Ordering::Relaxed), 3);

        let (_, _, cached) = fetch_or_get_segment(&cache, &client, &url, &[], true, false)
            .await
            .unwrap();
        assert_eq!(cached.as_ref(), payload.as_slice());
        assert_eq!(request_count.load(Ordering::Relaxed), 3);
        server.abort();
    }

    #[tokio::test]
    async fn hls_direct_segment_is_reused_from_disk_after_cache_reopen() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dir = tempfile::tempdir().unwrap();
        let request_count = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/movie/seg-000.ts");
        let server_count = Arc::clone(&request_count);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let count = Arc::clone(&server_count);
                tokio::spawn(async move {
                    let mut req = [0u8; 1024];
                    if socket.read(&mut req).await.unwrap_or(0) == 0 {
                        return;
                    }
                    count.fetch_add(1, Ordering::Relaxed);
                    let body = b"first-playable-segment";
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let client = crate::net::http_client_builder_base().build().unwrap();
        let disk1 = Arc::new(
            crate::hls_chunk_cache::HlsChunkCache::open(dir.path(), HLS_CHUNK_TTL, 1024 * 1024)
                .unwrap(),
        );
        let cache1 = Arc::new(SegmentCache {
            disk_cache: Some(disk1),
            namespace: "source-episode-audio-variant".into(),
            ..SegmentCache::default()
        });
        let cold_started = Instant::now();
        let (_, content_type, first) =
            fetch_or_get_segment(&cache1, &client, &url, &[], true, false)
                .await
                .unwrap();
        let cold_elapsed = cold_started.elapsed();
        assert_eq!(content_type, "video/mp2t");
        assert_eq!(first.as_ref(), b"first-playable-segment");
        let persisted_bytes = cache1.disk_cache.as_ref().unwrap().disk_bytes();
        drop(cache1);

        let disk2 = Arc::new(
            crate::hls_chunk_cache::HlsChunkCache::open(dir.path(), HLS_CHUNK_TTL, 1024 * 1024)
                .unwrap(),
        );
        let cache2 = Arc::new(SegmentCache {
            disk_cache: Some(disk2),
            namespace: "source-episode-audio-variant".into(),
            ..SegmentCache::default()
        });
        let warm_started = Instant::now();
        let (_, _, warm) = fetch_or_get_segment(&cache2, &client, &url, &[], true, false)
            .await
            .unwrap();
        let warm_elapsed = warm_started.elapsed();
        assert_eq!(warm.as_ref(), b"first-playable-segment");
        assert_eq!(request_count.load(Ordering::Relaxed), 1);
        eprintln!(
            "LOCAL_MOCK_HLS cold_first_segment_ms={} warm_disk_hit_ms={} persisted_bytes={} upstream_requests=1",
            cold_elapsed.as_millis(),
            warm_elapsed.as_millis(),
            persisted_bytes
        );
        server.abort();
    }

    #[tokio::test]
    async fn same_segment_requests_coalesce_and_prefetch_is_globally_bounded() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let request_count = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let server_requests = Arc::clone(&request_count);
        let server_active = Arc::clone(&active);
        let server_maximum = Arc::clone(&maximum);
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let requests = Arc::clone(&server_requests);
                let active = Arc::clone(&server_active);
                let maximum = Arc::clone(&server_maximum);
                tokio::spawn(async move {
                    let mut req = [0u8; 1024];
                    if socket.read(&mut req).await.unwrap_or(0) == 0 {
                        return;
                    }
                    requests.fetch_add(1, Ordering::Relaxed);
                    let now = active.fetch_add(1, Ordering::Relaxed) + 1;
                    maximum.fetch_max(now, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    let body = b"data";
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                    active.fetch_sub(1, Ordering::Relaxed);
                });
            }
        });

        let client = crate::net::http_client_builder_base().build().unwrap();
        let shared = Arc::new(SegmentCache::default());
        let same_url = format!("http://127.0.0.1:{port}/same.ts");
        let mut same_requests = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&shared);
            let client = client.clone();
            let url = same_url.clone();
            same_requests.push(tokio::spawn(async move {
                fetch_or_get_segment(&cache, &client, &url, &[], true, false).await
            }));
        }
        for request in same_requests {
            assert_eq!(request.await.unwrap().unwrap().2.as_ref(), b"data");
        }
        assert_eq!(request_count.load(Ordering::Relaxed), 1);

        let mut distinct_requests = Vec::new();
        for index in 0..12 {
            let cache = Arc::clone(&shared);
            let client = client.clone();
            let url = format!("http://127.0.0.1:{port}/seg-{index}.ts");
            let prefetch = index < 6;
            distinct_requests.push((
                prefetch,
                tokio::spawn(async move {
                    fetch_or_get_segment(&cache, &client, &url, &[], true, prefetch).await
                }),
            ));
        }
        let mut started_prefetches = 0;
        let mut deferred_prefetches = 0;
        for (prefetch, request) in distinct_requests {
            match request.await.unwrap() {
                Ok(result) => {
                    assert_eq!(result.2.as_ref(), b"data");
                    if prefetch {
                        started_prefetches += 1;
                    }
                }
                Err(error) if prefetch && error.contains("prefetch deferred") => {
                    deferred_prefetches += 1;
                }
                Err(error) => panic!("unexpected segment fetch failure: {error}"),
            }
        }
        eprintln!(
            "LOCAL_MOCK_HLS concurrent_peak_upstream_requests={} configured_max={} same_key_requests=8 same_key_upstream_count=1",
            maximum.load(Ordering::Relaxed),
            MAX_CONCURRENT_SEGMENT_FETCHES
        );
        assert!(maximum.load(Ordering::Relaxed) <= MAX_CONCURRENT_SEGMENT_FETCHES);
        assert!(maximum.load(Ordering::Relaxed) > 1);
        assert_eq!(
            request_count.load(Ordering::Relaxed),
            7 + started_prefetches
        );
        assert_eq!(started_prefetches + deferred_prefetches, 6);
        server.abort();
    }

    #[tokio::test]
    async fn test_warmup_dash_sidecar_populates_manifest_and_opening_segments() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    if req.contains("/dash/index.mpd") {
                        let mpd = r#"<MPD><Period><AdaptationSet contentType="video"><Representation id="0" height="720" bandwidth="1000"><SegmentTemplate initialization="init-stream$RepresentationID$.m4s" media="chunk-stream$RepresentationID$-$Number%05d$.m4s"/></Representation></AdaptationSet></Period></MPD>"#;
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/dash+xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            mpd.len(),
                            mpd
                        );
                        let _ = socket.write_all(hdr.as_bytes()).await;
                    } else {
                        let seg = b"SEGMENT_BYTES";
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: video/iso.segment\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            seg.len()
                        );
                        let _ = socket.write_all(hdr.as_bytes()).await;
                        let _ = socket.write_all(seg).await;
                    }
                });
            }
        });

        let target_url = format!("http://{addr}/dash/index.mpd");
        let ctx = ProxyContext {
            proxy_port: addr.port(),
            client: crate::net::http_client_builder_base().build().unwrap(),
            auth_headers: Arc::new(Vec::new()),
            target_host: Some(addr.to_string()),
            subtitle_url: None,
            max_height: Some(720),
            segment_cache: Arc::new(SegmentCache::default()),
            manifest_cache: Arc::new(Mutex::new(None)),
            hls_playlists: Arc::new(Mutex::new(std::collections::HashMap::new())),
            hls_allowed_hosts: Arc::new(Mutex::new(HashSet::new())),
            hls_prefetch: Arc::new(Mutex::new(std::collections::HashMap::new())),
            hls_prefetch_depth: 2,
        };

        warmup_dash_sidecar(&ctx, &target_url).await;
        for _ in 0..20 {
            if ctx
                .segment_cache
                .get(&format!("http://{addr}/dash/chunk-stream0-00001.m4s"))
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert!(ctx.manifest_cache.lock().unwrap().is_some());
        assert!(
            ctx.segment_cache
                .get(&format!("http://{addr}/dash/init-stream0.m4s"))
                .is_some()
        );
        assert!(
            ctx.segment_cache
                .get(&format!("http://{addr}/dash/init-stream3.m4s"))
                .is_some()
        );
        assert!(
            ctx.segment_cache
                .get(&format!("http://{addr}/dash/chunk-stream0-00001.m4s"))
                .is_some()
        );
        server.abort();
    }
}
