use std::{
    collections::HashMap,
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, Semaphore, watch},
};

use crate::config::MediaSettings;

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

#[derive(Debug)]
pub enum CacheError {
    Busy,
    TooLarge,
    BudgetExceeded,
    InvalidKey,
    BlockedCodec,
    UnverifiedCodec,
    Build(String),
    Io(io::Error),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy => formatter.write_str("media processing capacity is busy"),
            Self::TooLarge => {
                formatter.write_str("remux output exceeds the configured cache budget")
            }
            Self::BudgetExceeded => {
                formatter.write_str("cache budget is occupied by active media readers or jobs")
            }
            Self::InvalidKey => formatter.write_str("invalid cache identity"),
            Self::BlockedCodec => {
                formatter.write_str("the source codec is blocked by playback policy")
            }
            Self::UnverifiedCodec => formatter.write_str("the source codec could not be verified"),
            Self::Build(message) => write!(formatter, "{message}"),
            Self::Io(error) => write!(formatter, "cache I/O failed: {error}"),
        }
    }
}

impl std::error::Error for CacheError {}

impl From<io::Error> for CacheError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

struct ReadyEntry {
    cached: CachedRemux,
    stored_at: Instant,
    last_access: Instant,
}

enum EntryStatus {
    Building,
    Ready(ReadyEntry),
    Failed,
}

struct Entry {
    status: StdMutex<EntryStatus>,
    readers: AtomicUsize,
    partial_bytes: AtomicU64,
    changed: watch::Sender<u64>,
}

impl Entry {
    fn new() -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            status: StdMutex::new(EntryStatus::Building),
            readers: AtomicUsize::new(0),
            partial_bytes: AtomicU64::new(0),
            changed,
        }
    }
}

struct BuildGuard {
    entry: Arc<Entry>,
    partial_path: PathBuf,
    armed: bool,
}

impl BuildGuard {
    fn new(entry: Arc<Entry>, partial_path: PathBuf) -> Self {
        Self {
            entry,
            partial_path,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.entry.partial_bytes.store(0, Ordering::Release);
        {
            let mut status = self.entry.status.lock().expect("cache entry lock poisoned");
            *status = EntryStatus::Failed;
        }
        self.entry.changed.send_replace(1);
        let path = self.partial_path.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = tokio::fs::remove_file(path).await;
            });
        }
    }
}

#[derive(Clone)]
pub struct RemuxCache {
    root: Arc<PathBuf>,
    ttl: Duration,
    max_bytes: u64,
    job_timeout: Duration,
    jobs: Arc<Semaphore>,
    entries: Arc<Mutex<HashMap<String, Arc<Entry>>>>,
}

pub struct CacheLease {
    entry: Arc<Entry>,
    pub cached: CachedRemux,
    pub was_hit: bool,
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        self.entry.readers.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RemuxCache {
    pub async fn open(settings: MediaSettings) -> io::Result<Self> {
        tokio::fs::create_dir_all(&settings.cache_dir).await?;
        let root = tokio::fs::canonicalize(&settings.cache_dir).await?;
        if !tokio::fs::metadata(&root).await?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remux cache path is not a directory",
            ));
        }
        Self::remove_orphans(&root).await?;
        Ok(Self {
            root: Arc::new(root),
            ttl: settings.cache_ttl,
            max_bytes: settings.cache_max_bytes,
            job_timeout: settings.job_timeout,
            jobs: Arc::new(Semaphore::new(settings.max_concurrent_jobs)),
            entries: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn remove_orphans(root: &Path) -> io::Result<()> {
        let mut directory = tokio::fs::read_dir(root).await?;
        while let Some(entry) = directory.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_part = name.ends_with(".part");
            let is_owned_mp4 =
                name.ends_with(".mp4") && name.strip_suffix(".mp4").is_some_and(is_cache_key);
            if is_part || is_owned_mp4 {
                match tokio::fs::remove_file(entry.path()).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn job_timeout(&self) -> Duration {
        self.job_timeout
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn try_acquire_job(&self) -> Result<tokio::sync::OwnedSemaphorePermit, CacheError> {
        self.jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| CacheError::Busy)
    }

    pub async fn get_or_build<F, Fut>(&self, key: &str, build: F) -> Result<CacheLease, CacheError>
    where
        F: FnOnce(PathBuf) -> Fut,
        Fut: Future<Output = Result<f64, CacheError>>,
    {
        if !is_cache_key(key) {
            return Err(CacheError::InvalidKey);
        }
        let mut build = Some(build);

        loop {
            self.cleanup_expired().await?;
            let partial_path = self
                .root
                .join(format!("{key}.{}.part", uuid::Uuid::new_v4()));
            let (entry, is_builder, mut changed, permit) = {
                let mut entries = self.entries.lock().await;
                if let Some(entry) = entries.get(key).cloned() {
                    let mut status = entry.status.lock().expect("cache entry lock poisoned");
                    match &*status {
                        EntryStatus::Ready(ready) => {
                            entry.readers.fetch_add(1, Ordering::AcqRel);
                            let cached = ready.cached.clone();
                            if let EntryStatus::Ready(ready) = &mut *status {
                                ready.last_access = Instant::now();
                            }
                            drop(status);
                            return Ok(CacheLease {
                                entry,
                                cached,
                                was_hit: true,
                            });
                        }
                        EntryStatus::Building => {
                            let changed = entry.changed.subscribe();
                            drop(status);
                            (entry, false, changed, None)
                        }
                        EntryStatus::Failed => {
                            drop(status);
                            entries.remove(key);
                            continue;
                        }
                    }
                } else {
                    let permit = self
                        .jobs
                        .clone()
                        .try_acquire_owned()
                        .map_err(|_| CacheError::Busy)?;
                    let entry = Arc::new(Entry::new());
                    entries.insert(key.to_string(), entry.clone());
                    (entry.clone(), true, entry.changed.subscribe(), Some(permit))
                }
            };

            if !is_builder {
                if *changed.borrow() == 0 {
                    let _ = changed.changed().await;
                }
                continue;
            }

            let permit = permit.expect("builder owns media-job permit");
            let mut build_guard = BuildGuard::new(entry.clone(), partial_path.clone());
            let Some(builder) = build.take() else {
                drop(permit);
                return Err(CacheError::Build(
                    "cache build was retried after use".into(),
                ));
            };
            let build_result = builder(partial_path.clone()).await;
            drop(permit);

            let duration_seconds = match build_result {
                Ok(duration) if duration.is_finite() && duration > 0.0 => duration,
                Ok(_) => {
                    self.fail_build(key, &entry, &partial_path).await;
                    return Err(CacheError::Build(
                        "FFprobe returned an invalid duration".into(),
                    ));
                }
                Err(error) => {
                    self.fail_build(key, &entry, &partial_path).await;
                    return Err(error);
                }
            };
            let metadata = match tokio::fs::metadata(&partial_path).await {
                Ok(metadata) if metadata.is_file() && metadata.len() > 0 => metadata,
                Ok(_) => {
                    self.fail_build(key, &entry, &partial_path).await;
                    return Err(CacheError::Build("FFmpeg produced an empty remux".into()));
                }
                Err(error) => {
                    self.fail_build(key, &entry, &partial_path).await;
                    return Err(CacheError::Io(error));
                }
            };
            let size_bytes = metadata.len();
            if size_bytes > self.max_bytes {
                self.fail_build(key, &entry, &partial_path).await;
                return Err(CacheError::TooLarge);
            }
            entry.partial_bytes.store(size_bytes, Ordering::Release);
            if let Err(error) = self.enforce_budget().await {
                self.fail_build(key, &entry, &partial_path).await;
                return Err(error);
            }

            let final_path = self.root.join(format!("{key}.mp4"));
            if let Err(error) = tokio::fs::rename(&partial_path, &final_path).await {
                self.fail_build(key, &entry, &partial_path).await;
                return Err(CacheError::Io(error));
            }
            let stored_at_unix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let cached = CachedRemux {
                path: final_path,
                metadata: CacheMetadata {
                    size_bytes,
                    duration_seconds,
                    stored_at_unix,
                },
            };
            {
                let mut status = entry.status.lock().expect("cache entry lock poisoned");
                *status = EntryStatus::Ready(ReadyEntry {
                    cached: cached.clone(),
                    stored_at: Instant::now(),
                    last_access: Instant::now(),
                });
            }
            entry.partial_bytes.store(0, Ordering::Release);
            entry.readers.fetch_add(1, Ordering::AcqRel);
            build_guard.disarm();
            entry.changed.send_replace(1);
            return Ok(CacheLease {
                entry,
                cached,
                was_hit: false,
            });
        }
    }

    pub async fn report_partial_bytes(&self, key: &str, size: u64) -> Result<(), CacheError> {
        let entry = self
            .entries
            .lock()
            .await
            .get(key)
            .cloned()
            .ok_or(CacheError::InvalidKey)?;
        entry.partial_bytes.store(size, Ordering::Release);
        self.enforce_budget().await
    }

    async fn fail_build(&self, key: &str, entry: &Arc<Entry>, partial_path: &Path) {
        let _ = tokio::fs::remove_file(partial_path).await;
        entry.partial_bytes.store(0, Ordering::Release);
        {
            let mut status = entry.status.lock().expect("cache entry lock poisoned");
            *status = EntryStatus::Failed;
        }
        entry.changed.send_replace(1);
        let mut entries = self.entries.lock().await;
        if entries
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            entries.remove(key);
        }
    }

    pub async fn cleanup_expired(&self) -> io::Result<usize> {
        let now = Instant::now();
        let mut entries = self.entries.lock().await;
        let expired = entries
            .iter()
            .filter_map(|(key, entry)| {
                if entry.readers.load(Ordering::Acquire) != 0 {
                    return None;
                }
                let status = entry.status.lock().expect("cache entry lock poisoned");
                match &*status {
                    EntryStatus::Ready(ready)
                        if now.duration_since(ready.stored_at) >= self.ttl =>
                    {
                        Some((key.clone(), ready.cached.path.clone()))
                    }
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        let mut removed = 0;
        for (key, path) in expired {
            if let Some(entry) = entries.remove(&key) {
                let _ = tokio::fs::remove_file(path).await;
                entry.changed.send_replace(1);
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn enforce_budget(&self) -> Result<(), CacheError> {
        let mut entries = self.entries.lock().await;
        let mut used = entries
            .values()
            .map(|entry| {
                let status = entry.status.lock().expect("cache entry lock poisoned");
                match &*status {
                    EntryStatus::Ready(ready) => ready.cached.metadata.size_bytes,
                    EntryStatus::Building => entry.partial_bytes.load(Ordering::Acquire),
                    EntryStatus::Failed => 0,
                }
            })
            .sum::<u64>();
        if used <= self.max_bytes {
            return Ok(());
        }

        let mut evictable = entries
            .iter()
            .filter_map(|(key, entry)| {
                if entry.readers.load(Ordering::Acquire) != 0 {
                    return None;
                }
                let status = entry.status.lock().expect("cache entry lock poisoned");
                match &*status {
                    EntryStatus::Ready(ready) => Some((
                        key.clone(),
                        ready.cached.path.clone(),
                        ready.cached.metadata.size_bytes,
                        ready.last_access,
                    )),
                    _ => None,
                }
            })
            .collect::<Vec<_>>();
        evictable.sort_by_key(|(_, _, _, accessed)| *accessed);
        for (key, path, size, _) in evictable {
            if used <= self.max_bytes {
                break;
            }
            if let Some(entry) = entries.remove(&key) {
                match tokio::fs::remove_file(path).await {
                    Ok(()) => {
                        used = used.saturating_sub(size);
                        entry.changed.send_replace(1);
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        used = used.saturating_sub(size);
                    }
                    Err(error) => {
                        entries.insert(key, entry);
                        return Err(CacheError::Io(error));
                    }
                }
            }
        }
        if used > self.max_bytes {
            Err(CacheError::BudgetExceeded)
        } else {
            Ok(())
        }
    }
}

fn is_cache_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Keys are based on stable source identity plus the complete output profile;
/// signed URLs and authentication headers never appear in filenames or logs.
pub fn cache_key(stable_source_id: &str, output_profile: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"moviebox-remux-cache-v3\0");
    digest.update(stable_source_id.as_bytes());
    digest.update(b"\0");
    digest.update(output_profile.as_bytes());
    let mut key = String::with_capacity(64);
    for byte in digest.finalize() {
        key.push_str(&format!("{byte:02x}"));
    }
    key
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ByteRange {
    Full,
    Partial { start: u64, end: u64 },
    Unsatisfiable,
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
    if spec.contains(',') {
        return ByteRange::Full;
    }
    if total_size == 0 {
        return ByteRange::Unsatisfiable;
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
        let Ok(end) = end.parse::<u64>() else {
            return ByteRange::Full;
        };
        end.min(total_size - 1)
    };
    if start >= total_size || start > end {
        ByteRange::Unsatisfiable
    } else {
        ByteRange::Partial { start, end }
    }
}

pub async fn copy_range_to_writer<W>(
    path: &Path,
    start: u64,
    length: u64,
    writer: &mut W,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(start)).await?;
    let mut limited = file.take(length);
    let copied = tokio::io::copy(&mut limited, writer).await?;
    if copied != length {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "cached media file ended before the requested byte range",
        ));
    }
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::{
        ByteRange, CacheError, RemuxCache, cache_key, copy_range_to_writer, parse_range_header,
    };
    use crate::config::MediaSettings;
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::io::AsyncReadExt;
    use tokio::sync::oneshot;

    fn settings(root: PathBuf, ttl: Duration, max_bytes: u64, jobs: usize) -> MediaSettings {
        MediaSettings {
            cache_dir: root,
            cache_ttl: ttl,
            cache_max_bytes: max_bytes,
            max_concurrent_jobs: jobs,
            job_timeout: Duration::from_secs(30),
        }
    }

    async fn fresh_cache(ttl: Duration, max_bytes: u64, jobs: usize) -> (RemuxCache, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("moviebox-remux-test-{}", uuid::Uuid::new_v4()));
        let cache = RemuxCache::open(settings(root.clone(), ttl, max_bytes, jobs))
            .await
            .expect("open isolated cache");
        (cache, root)
    }

    async fn build_bytes(
        cache: &RemuxCache,
        source: &str,
        bytes: &'static [u8],
    ) -> super::CacheLease {
        cache
            .get_or_build(&cache_key(source, "mp4-h264-aac-v1"), |path| async move {
                tokio::fs::write(path, bytes)
                    .await
                    .map_err(CacheError::Io)?;
                Ok(120.0)
            })
            .await
            .expect("build cache entry")
    }

    #[test]
    fn parses_full_open_ended_bounded_and_suffix_ranges() {
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
        assert_eq!(
            parse_range_header(Some("bytes=-10"), 100),
            ByteRange::Partial { start: 90, end: 99 }
        );
        assert_eq!(
            parse_range_header(Some("bytes=-200"), 100),
            ByteRange::Partial { start: 0, end: 99 }
        );
    }

    #[test]
    fn ignores_malformed_or_multi_range_and_rejects_unsatisfiable_ranges() {
        assert_eq!(parse_range_header(Some("items=0-9"), 100), ByteRange::Full);
        assert_eq!(
            parse_range_header(Some("bytes=abc-def"), 100),
            ByteRange::Full
        );
        assert_eq!(
            parse_range_header(Some("bytes=0-1,4-5"), 100),
            ByteRange::Full
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
        assert_eq!(
            parse_range_header(Some("bytes=0-"), 0),
            ByteRange::Unsatisfiable
        );
    }

    #[test]
    fn cache_keys_are_stable_and_output_profile_specific() {
        let first = cache_key("provider:source-123", "mp4-h264-aac-v1");
        assert_eq!(first, cache_key("provider:source-123", "mp4-h264-aac-v1"));
        assert_ne!(first, cache_key("provider:source-123", "mp4-h264-aac-v2"));
        assert_eq!(first.len(), 64);
        assert!(!first.contains("source-123"));
    }

    #[tokio::test]
    async fn cache_hits_expire_and_rebuild_after_ttl() {
        let (cache, root) = fresh_cache(Duration::from_millis(30), 1024, 1).await;
        let builds = Arc::new(AtomicUsize::new(0));
        let first_builds = builds.clone();
        let key = cache_key("ttl-source", "profile");
        let first = cache
            .get_or_build(&key, move |path| async move {
                first_builds.fetch_add(1, Ordering::SeqCst);
                tokio::fs::write(path, b"first")
                    .await
                    .map_err(CacheError::Io)?;
                Ok(1.0)
            })
            .await
            .unwrap();
        drop(first);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let second_builds = builds.clone();
        let second = cache
            .get_or_build(&key, move |path| async move {
                second_builds.fetch_add(1, Ordering::SeqCst);
                tokio::fs::write(path, b"second")
                    .await
                    .map_err(CacheError::Io)?;
                Ok(1.0)
            })
            .await
            .unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 2);
        assert_eq!(
            tokio::fs::read(&second.cached.path).await.unwrap(),
            b"second"
        );
        drop(second);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn job_limit_fails_fast_without_an_unbounded_queue() {
        let (cache, root) = fresh_cache(Duration::from_secs(60), 1024, 1).await;
        let (started_tx, started_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel();
        let first_cache = cache.clone();
        let first = tokio::spawn(async move {
            first_cache
                .get_or_build(&cache_key("first", "profile"), move |path| async move {
                    let _ = started_tx.send(());
                    let _ = finish_rx.await;
                    tokio::fs::write(path, b"one")
                        .await
                        .map_err(CacheError::Io)?;
                    Ok(1.0)
                })
                .await
        });
        started_rx.await.unwrap();
        let second = cache
            .get_or_build(&cache_key("second", "profile"), |path| async move {
                tokio::fs::write(path, b"two")
                    .await
                    .map_err(CacheError::Io)?;
                Ok(1.0)
            })
            .await;
        assert!(matches!(second, Err(CacheError::Busy)));
        let _ = finish_tx.send(());
        drop(first.await.unwrap().unwrap());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn failed_partial_build_is_removed_and_startup_cleans_orphans() {
        let root = std::env::temp_dir().join(format!(
            "moviebox-remux-orphan-test-{}",
            uuid::Uuid::new_v4()
        ));
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(
            root.join(format!("{}.orphan.part", "a".repeat(64))),
            b"partial",
        )
        .await
        .unwrap();
        let cache = RemuxCache::open(settings(root.clone(), Duration::from_secs(60), 1024, 1))
            .await
            .unwrap();
        let mut orphan_entries = tokio::fs::read_dir(cache.root()).await.unwrap();
        assert!(orphan_entries.next_entry().await.unwrap().is_none());

        let key = cache_key("failure", "profile");
        let result = cache
            .get_or_build(&key, |path| async move {
                tokio::fs::write(&path, b"partial")
                    .await
                    .map_err(CacheError::Io)?;
                Err(CacheError::Build("simulated FFmpeg failure".into()))
            })
            .await;
        assert!(matches!(result, Err(CacheError::Build(_))));
        assert!(!cache.root().join(format!("{key}.mp4")).exists());
        let mut entries = tokio::fs::read_dir(cache.root()).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn cancelled_build_cleans_partial_and_allows_retry() {
        let (cache, root) = fresh_cache(Duration::from_secs(60), 1024, 1).await;
        let key = cache_key("cancelled", "profile");
        let build_cache = cache.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            build_cache
                .get_or_build(&key, move |path| async move {
                    tokio::fs::write(&path, b"partial")
                        .await
                        .map_err(CacheError::Io)?;
                    let _ = started_tx.send(());
                    std::future::pending::<Result<f64, CacheError>>().await
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut entries = tokio::fs::read_dir(cache.root()).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());

        let retried = cache
            .get_or_build(&cache_key("cancelled", "profile"), |path| async move {
                tokio::fs::write(path, b"complete")
                    .await
                    .map_err(CacheError::Io)?;
                Ok(1.0)
            })
            .await
            .expect("cancelled flight is retryable");
        assert_eq!(
            tokio::fs::read(&retried.cached.path).await.unwrap(),
            b"complete"
        );
        drop(retried);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn disk_budget_evicts_lru_but_protects_active_readers() {
        let (cache, root) = fresh_cache(Duration::from_secs(60), 5, 1).await;
        let first = build_bytes(&cache, "first", b"1234").await;
        let first_path = first.cached.path.clone();
        drop(first);
        let second = build_bytes(&cache, "second", b"abcd").await;
        assert!(!first_path.exists(), "inactive LRU entry should be evicted");
        drop(second);

        let active = build_bytes(&cache, "active", b"1234").await;
        let active_path = active.cached.path.clone();
        let blocked = cache
            .get_or_build(
                &cache_key("blocked", "mp4-h264-aac-v1"),
                |path| async move {
                    tokio::fs::write(path, b"abcd")
                        .await
                        .map_err(CacheError::Io)?;
                    Ok(120.0)
                },
            )
            .await;
        assert!(matches!(blocked, Err(CacheError::BudgetExceeded)));
        assert!(
            active_path.exists(),
            "active reader file must not be removed"
        );
        drop(active);
        let replacement = build_bytes(&cache, "replacement", b"abcd").await;
        assert!(!active_path.exists(), "released entry may be evicted");
        drop(replacement);
        let _ = tokio::fs::remove_dir_all(root).await;
    }

    #[tokio::test]
    async fn copies_exact_bytes_for_full_and_partial_ranges() {
        let root =
            std::env::temp_dir().join(format!("moviebox-range-test-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let path = root.join("range.mp4");
        tokio::fs::write(&path, b"0123456789").await.unwrap();
        let (mut writer, mut reader) = tokio::io::duplex(32);
        let copy_path = path.clone();
        let copy = tokio::spawn(async move {
            copy_range_to_writer(&copy_path, 3, 4, &mut writer)
                .await
                .unwrap();
        });
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await.unwrap();
        copy.await.unwrap();
        assert_eq!(body, b"3456");
        let _ = tokio::fs::remove_dir_all(root).await;
    }
}
