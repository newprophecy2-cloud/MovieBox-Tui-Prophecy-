use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::OpenOptions,
    io,
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    fs as async_fs,
    io::AsyncWriteExt,
    process::Command,
    sync::{OwnedSemaphorePermit, Semaphore},
    time,
};

const CACHE_TTL: Duration = Duration::from_secs(2 * 60 * 60);
const STALE_LOCK_TTL: Duration = Duration::from_secs(20 * 60);
const LOCK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(60);
const CACHE_WAIT_INTERVAL: Duration = Duration::from_millis(400);
const FINALIZE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MAX_CACHE_ENTRY_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_CACHE_TOTAL_BYTES: u64 = 3 * 1024 * 1024 * 1024;
static CACHE_BUILD_LIMITER: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(1)));

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheMetadata {
    pub size_bytes: u64,
    pub duration_seconds: f64,
    pub stored_at_unix: u64,
}

#[derive(Clone, Debug)]
pub struct CachedRemux {
    pub path: PathBuf,
    pub metadata: CacheMetadata,
}

pub enum CacheState {
    Hit(CachedRemux),
    Build(Box<CacheWriter>),
    Building,
    Disabled,
}

#[derive(Clone)]
struct CachePaths {
    root: PathBuf,
    final_file: PathBuf,
    metadata_file: PathBuf,
    lock_file: PathBuf,
}

pub struct CacheWriter {
    paths: CachePaths,
    partial_file: PathBuf,
    file: Option<async_fs::File>,
    written_bytes: u64,
    last_lock_touch: Instant,
    _build_permit: OwnedSemaphorePermit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    Full,
    Partial { start: u64, end: u64 },
    Unsatisfiable,
}

pub fn cache_key(url: &str, headers: &[(String, String)]) -> String {
    let mut canonical_headers = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect::<Vec<_>>();
    canonical_headers.sort();

    let mut digest = Sha256::new();
    digest.update(b"moviebox-remux-cache-v1\0");
    digest.update(url.as_bytes());
    digest.update(b"\0");
    for (name, value) in canonical_headers {
        digest.update(name.as_bytes());
        digest.update(b":");
        digest.update(value.as_bytes());
        digest.update(b"\0");
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut key = String::with_capacity(64);
    for byte in digest.finalize().iter() {
        key.push(char::from(HEX[(*byte >> 4) as usize]));
        key.push(char::from(HEX[(*byte & 0x0f) as usize]));
    }
    key
}

pub async fn prepare(key: &str) -> CacheState {
    let paths = cache_paths(key);
    if async_fs::create_dir_all(&paths.root).await.is_err() {
        return CacheState::Disabled;
    }
    prune_cache_dir(&paths.root, None, MAX_CACHE_TOTAL_BYTES).await;

    if let Some(cached) = read_cached(&paths).await {
        return CacheState::Hit(cached);
    }

    if lock_is_stale(&paths.lock_file).await {
        let _ = async_fs::remove_file(&paths.lock_file).await;
    }
    if path_exists(&paths.lock_file).await {
        return CacheState::Building;
    }
    let Ok(build_permit) = CACHE_BUILD_LIMITER.clone().try_acquire_owned() else {
        return CacheState::Disabled;
    };
    prune_cache_dir(
        &paths.root,
        None,
        MAX_CACHE_TOTAL_BYTES.saturating_sub(MAX_CACHE_ENTRY_BYTES),
    )
    .await;
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.lock_file)
    {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return CacheState::Building;
        }
        Err(_) => return CacheState::Disabled,
    }

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let partial_file = paths.root.join(format!(
        "{}.{}.{}.fragmented.part",
        key,
        std::process::id(),
        stamp
    ));
    let file = match async_fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&partial_file)
        .await
    {
        Ok(file) => file,
        Err(_) => {
            let _ = async_fs::remove_file(&paths.lock_file).await;
            return CacheState::Disabled;
        }
    };

    CacheState::Build(Box::new(CacheWriter {
        paths,
        partial_file,
        file: Some(file),
        written_bytes: 0,
        last_lock_touch: Instant::now(),
        _build_permit: build_permit,
    }))
}

pub async fn lookup(key: &str) -> Option<CachedRemux> {
    read_cached(&cache_paths(key)).await
}

pub async fn is_building(key: &str) -> bool {
    let paths = cache_paths(key);
    if lock_is_stale(&paths.lock_file).await {
        let _ = async_fs::remove_file(&paths.lock_file).await;
        return false;
    }
    path_exists(&paths.lock_file).await
}

pub async fn wait_for(key: &str, timeout: Duration) -> Option<CachedRemux> {
    let paths = cache_paths(key);
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(cached) = read_cached(&paths).await {
            return Some(cached);
        }
        if !path_exists(&paths.lock_file).await || Instant::now() >= deadline {
            return None;
        }
        time::sleep(CACHE_WAIT_INTERVAL).await;
    }
}

impl CacheWriter {
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        let next_size = self.written_bytes.saturating_add(bytes.len() as u64);
        if next_size > MAX_CACHE_ENTRY_BYTES {
            return Err(io::Error::other(
                "remux cache entry exceeded the configured limit",
            ));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("remux cache writer is closed"))?;
        file.write_all(bytes).await?;
        self.written_bytes = next_size;
        if self.last_lock_touch.elapsed() >= LOCK_HEARTBEAT_INTERVAL {
            let mut lock = async_fs::OpenOptions::new()
                .append(true)
                .open(&self.paths.lock_file)
                .await?;
            lock.write_all(b".").await?;
            self.last_lock_touch = Instant::now();
        }
        Ok(())
    }

    pub async fn abort(mut self) {
        self.file.take();
        let _ = async_fs::remove_file(&self.partial_file).await;
        let _ = async_fs::remove_file(&self.paths.lock_file).await;
    }

    pub async fn finish(mut self) -> io::Result<CachedRemux> {
        if let Some(mut file) = self.file.take() {
            if let Err(error) = file.flush().await {
                drop(file);
                let _ = async_fs::remove_file(&self.partial_file).await;
                let _ = async_fs::remove_file(&self.paths.lock_file).await;
                return Err(error);
            }
            drop(file);
        }
        let result = self.finish_inner().await;
        let _ = async_fs::remove_file(&self.partial_file).await;
        let _ = async_fs::remove_file(&self.paths.lock_file).await;
        if result.is_ok() {
            prune_cache_dir(&self.paths.root, None, MAX_CACHE_TOTAL_BYTES).await;
        }
        result
    }

    async fn finish_inner(&self) -> io::Result<CachedRemux> {
        if self.written_bytes == 0 || self.written_bytes > MAX_CACHE_ENTRY_BYTES {
            return Err(io::Error::other("remux cache output is empty or too large"));
        }

        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let finalized_temp = self.paths.root.join(format!(
            ".{}.{}.{}.finalizing.mp4",
            self.paths
                .final_file
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("remux"),
            std::process::id(),
            stamp
        ));
        let mut ffmpeg = Command::new("ffmpeg");
        ffmpeg
            .args(["-hide_banner", "-loglevel", "error", "-nostdin", "-i"])
            .arg(&self.partial_file)
            .args([
                "-map",
                "0",
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                "-f",
                "mp4",
                "-y",
            ])
            .arg(&finalized_temp)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let status = match time::timeout(FINALIZE_TIMEOUT, ffmpeg.status()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                let _ = async_fs::remove_file(&finalized_temp).await;
                return Err(io::Error::other(format!(
                    "could not finalize remux cache: {error}"
                )));
            }
            Err(_) => {
                let _ = async_fs::remove_file(&finalized_temp).await;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "remux cache finalization timed out",
                ));
            }
        };
        if !status.success() {
            let _ = async_fs::remove_file(&finalized_temp).await;
            return Err(io::Error::other(
                "FFmpeg could not finalize the remux cache",
            ));
        }

        let file_metadata = async_fs::metadata(&finalized_temp).await?;
        if file_metadata.len() == 0 || file_metadata.len() > MAX_CACHE_ENTRY_BYTES {
            let _ = async_fs::remove_file(&finalized_temp).await;
            return Err(io::Error::other(
                "finalized remux cache has an invalid size",
            ));
        }
        let mut ffprobe = Command::new("ffprobe");
        ffprobe
            .args([
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
            ])
            .arg(&finalized_temp)
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let output = match time::timeout(Duration::from_secs(20), ffprobe.output()).await {
            Ok(Ok(output)) if output.status.success() => output,
            Ok(Ok(_)) => {
                let _ = async_fs::remove_file(&finalized_temp).await;
                return Err(io::Error::other(
                    "ffprobe could not read finalized remux duration",
                ));
            }
            Ok(Err(error)) => {
                let _ = async_fs::remove_file(&finalized_temp).await;
                return Err(io::Error::other(format!(
                    "could not probe remux duration: {error}"
                )));
            }
            Err(_) => {
                let _ = async_fs::remove_file(&finalized_temp).await;
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "remux duration probe timed out",
                ));
            }
        };
        let Some(duration_seconds) = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|duration| duration.is_finite() && *duration > 0.0)
        else {
            let _ = async_fs::remove_file(&finalized_temp).await;
            return Err(io::Error::other("finalized remux has no finite duration"));
        };

        let metadata = CacheMetadata {
            size_bytes: file_metadata.len(),
            duration_seconds,
            stored_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        let metadata_bytes = match serde_json::to_vec(&metadata) {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = async_fs::remove_file(&self.paths.final_file).await;
                return Err(io::Error::other(format!(
                    "could not serialize remux metadata: {error}"
                )));
            }
        };
        let metadata_temp = self
            .paths
            .metadata_file
            .with_extension(format!("json.{stamp}.tmp"));
        if let Err(error) = async_fs::write(&metadata_temp, metadata_bytes).await {
            let _ = async_fs::remove_file(&metadata_temp).await;
            let _ = async_fs::remove_file(&self.paths.final_file).await;
            return Err(io::Error::other(format!(
                "could not write remux metadata: {error}"
            )));
        }
        if let Err(error) = async_fs::rename(&metadata_temp, &self.paths.metadata_file).await {
            let _ = async_fs::remove_file(&metadata_temp).await;
            return Err(io::Error::other(format!(
                "could not publish remux metadata: {error}"
            )));
        }
        if let Err(error) = async_fs::rename(&finalized_temp, &self.paths.final_file).await {
            let _ = async_fs::remove_file(&finalized_temp).await;
            let _ = async_fs::remove_file(&self.paths.metadata_file).await;
            return Err(io::Error::other(format!(
                "could not publish remux cache: {error}"
            )));
        }

        Ok(CachedRemux {
            path: self.paths.final_file.clone(),
            metadata,
        })
    }
}

pub fn parse_range_header(header: Option<&str>, total_size: u64) -> ByteRange {
    let Some(header) = header else {
        return ByteRange::Full;
    };
    let Some((unit, spec)) = header.trim().split_once('=') else {
        return ByteRange::Full;
    };
    if !unit.eq_ignore_ascii_case("bytes") {
        return ByteRange::Full;
    }
    if spec.contains(',') || total_size == 0 {
        return ByteRange::Full;
    }
    let Some((start, end)) = spec.split_once('-') else {
        return ByteRange::Full;
    };
    if start.is_empty() {
        let Ok(suffix_len) = end.parse::<u64>() else {
            return ByteRange::Full;
        };
        if suffix_len == 0 {
            return ByteRange::Unsatisfiable;
        }
        let length = suffix_len.min(total_size);
        return ByteRange::Partial {
            start: total_size - length,
            end: total_size - 1,
        };
    }
    let Ok(start) = start.parse::<u64>() else {
        return ByteRange::Full;
    };
    let end = if end.is_empty() {
        total_size - 1
    } else {
        match end.parse::<u64>() {
            Ok(end) => end,
            Err(_) => return ByteRange::Full,
        }
    };
    if start >= total_size || start > end {
        return ByteRange::Unsatisfiable;
    }
    ByteRange::Partial {
        start,
        end: end.min(total_size - 1),
    }
}

fn cache_paths(key: &str) -> CachePaths {
    let root = crate::config::cache_dir().join("remux");
    CachePaths {
        final_file: root.join(format!("{key}.mp4")),
        metadata_file: root.join(format!("{key}.json")),
        lock_file: root.join(format!("{key}.lock")),
        root,
    }
}

async fn read_cached(paths: &CachePaths) -> Option<CachedRemux> {
    let file_metadata = async_fs::metadata(&paths.final_file).await.ok()?;
    let elapsed = file_metadata.modified().ok()?.elapsed().ok()?;
    if elapsed > CACHE_TTL {
        let _ = async_fs::remove_file(&paths.final_file).await;
        let _ = async_fs::remove_file(&paths.metadata_file).await;
        return None;
    }
    let Some(bytes) = async_fs::read(&paths.metadata_file).await.ok() else {
        let _ = async_fs::remove_file(&paths.final_file).await;
        return None;
    };
    let Ok(metadata) = serde_json::from_slice::<CacheMetadata>(&bytes) else {
        let _ = async_fs::remove_file(&paths.final_file).await;
        let _ = async_fs::remove_file(&paths.metadata_file).await;
        return None;
    };
    if metadata.size_bytes != file_metadata.len()
        || !metadata.duration_seconds.is_finite()
        || metadata.duration_seconds <= 0.0
    {
        let _ = async_fs::remove_file(&paths.final_file).await;
        let _ = async_fs::remove_file(&paths.metadata_file).await;
        return None;
    }
    Some(CachedRemux {
        path: paths.final_file.clone(),
        metadata,
    })
}

async fn lock_is_stale(path: &PathBuf) -> bool {
    let Ok(metadata) = async_fs::metadata(path).await else {
        return false;
    };
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|elapsed| elapsed > STALE_LOCK_TTL)
}

async fn path_exists(path: &PathBuf) -> bool {
    async_fs::metadata(path).await.is_ok()
}

async fn prune_cache_dir(root: &PathBuf, protected_path: Option<&PathBuf>, max_bytes: u64) {
    let Ok(mut entries) = async_fs::read_dir(root).await else {
        return;
    };
    let mut cached_files = Vec::new();
    let mut stale_work_files = Vec::new();
    let mut total_size = 0u64;
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        let age = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .unwrap_or_default();
        if name.len() == 68 && name.ends_with(".mp4") {
            if age > CACHE_TTL {
                let _ = async_fs::remove_file(&path).await;
                let _ = async_fs::remove_file(path.with_extension("json")).await;
                continue;
            }
            total_size = total_size.saturating_add(metadata.len());
            cached_files.push((path, metadata.len(), metadata.modified().ok()));
        } else if (name.ends_with(".fragmented.part")
            || name.ends_with(".lock")
            || name.ends_with(".finalizing.mp4")
            || name.contains(".json.") && name.ends_with(".tmp"))
            && age > STALE_LOCK_TTL
        {
            stale_work_files.push(path);
        }
    }
    for path in stale_work_files {
        let _ = async_fs::remove_file(path).await;
    }
    cached_files.sort_by_key(|(_, _, modified)| *modified);
    for (path, size, _) in cached_files {
        if total_size <= max_bytes {
            break;
        }
        if protected_path.is_some_and(|protected| protected == &path) {
            continue;
        }
        let _ = async_fs::remove_file(&path).await;
        let _ = async_fs::remove_file(path.with_extension("json")).await;
        total_size = total_size.saturating_sub(size);
    }
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, cache_key, parse_range_header};

    #[test]
    fn parses_full_open_ended_and_bounded_byte_ranges() {
        assert_eq!(parse_range_header(None, 100), ByteRange::Full);
        assert_eq!(
            parse_range_header(Some("bytes=10-"), 100),
            ByteRange::Partial { start: 10, end: 99 }
        );
        assert_eq!(
            parse_range_header(Some("bytes=10-19"), 100),
            ByteRange::Partial { start: 10, end: 19 }
        );
        assert_eq!(
            parse_range_header(Some("bytes=0-999"), 100),
            ByteRange::Partial { start: 0, end: 99 }
        );
    }

    #[test]
    fn parses_suffix_ranges_and_rejects_unsatisfiable_ranges() {
        assert_eq!(
            parse_range_header(Some("bytes=-10"), 100),
            ByteRange::Partial { start: 90, end: 99 }
        );
        assert_eq!(
            parse_range_header(Some("bytes=-200"), 100),
            ByteRange::Partial { start: 0, end: 99 }
        );
        assert_eq!(
            parse_range_header(Some("bytes=100-"), 100),
            ByteRange::Unsatisfiable
        );
        assert_eq!(
            parse_range_header(Some("bytes=20-10"), 100),
            ByteRange::Unsatisfiable
        );
        assert_eq!(
            parse_range_header(Some("bytes=-0"), 100),
            ByteRange::Unsatisfiable
        );
    }

    #[test]
    fn ignores_malformed_and_multi_range_requests() {
        assert_eq!(parse_range_header(Some("items=0-9"), 100), ByteRange::Full);
        assert_eq!(
            parse_range_header(Some("bytes=abc-def"), 100),
            ByteRange::Full
        );
        assert_eq!(
            parse_range_header(Some("bytes=0-1,4-5"), 100),
            ByteRange::Full
        );
    }

    #[test]
    fn cache_keys_are_stable_and_include_request_headers() {
        let first = vec![("Referer".to_string(), "https://example.com".to_string())];
        let equivalent = vec![("referer".to_string(), "https://example.com".to_string())];
        let different = vec![("Referer".to_string(), "https://other.example".to_string())];
        assert_eq!(
            cache_key("https://cdn.example/video", &first),
            cache_key("https://cdn.example/video", &equivalent)
        );
        assert_ne!(
            cache_key("https://cdn.example/video", &first),
            cache_key("https://cdn.example/video", &different)
        );
    }
}
