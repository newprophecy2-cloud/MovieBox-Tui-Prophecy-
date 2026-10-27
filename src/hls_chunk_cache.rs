use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

const MAGIC: &[u8; 4] = b"HLS1";
const FIXED_HEADER_BYTES: usize = 14;
const MAX_CONTENT_TYPE_BYTES: usize = 1024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct HlsChunkCache {
    inner: Arc<Inner>,
}

struct Inner {
    root: PathBuf,
    ttl: Duration,
    max_bytes: u64,
    state: Mutex<State>,
    writer: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    total_bytes: u64,
}

struct Entry {
    size: u64,
    created_at: SystemTime,
    last_access: SystemTime,
    active_readers: usize,
}

pub struct ChunkLease {
    inner: Weak<Inner>,
    key: String,
    pub content_type: String,
    pub bytes: Arc<[u8]>,
}

struct PartialFileGuard {
    path: PathBuf,
    armed: bool,
}

impl Drop for PartialFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl Drop for ChunkLease {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade()
            && let Ok(mut state) = inner.state.lock()
            && let Some(entry) = state.entries.get_mut(&self.key)
        {
            entry.active_readers = entry.active_readers.saturating_sub(1);
        }
    }
}

impl HlsChunkCache {
    /// Open a cache whose directory contains only digest-named files owned by this cache.
    /// TTL is measured from the segment's atomic publication time, not from last access.
    pub fn open(root: &Path, ttl: Duration, max_bytes: u64) -> std::io::Result<Self> {
        if max_bytes == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "HLS chunk cache budget must be positive",
            ));
        }
        if let Ok(metadata) = fs::symlink_metadata(root)
            && metadata.file_type().is_symlink()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "HLS chunk cache root must not be a symlink",
            ));
        }
        fs::create_dir_all(root)?;
        let root = fs::canonicalize(root)?;
        if fs::symlink_metadata(&root)?.file_type().is_symlink() || !root.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "HLS chunk cache path is not a directory",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        }

        let now = SystemTime::now();
        let mut state = State::default();
        for item in fs::read_dir(&root)? {
            let item = item?;
            let path = item.path();
            let name = item.file_name().to_string_lossy().into_owned();
            let metadata = fs::symlink_metadata(&path)?;
            if name.ends_with(".part") {
                // A cache may be shared by multiple local proxy processes. Only reap old
                // partials; fresh ones may still belong to an active writer.
                let stale = metadata.modified().ok().is_some_and(|modified| {
                    now.duration_since(modified).unwrap_or_default() >= ttl
                });
                if metadata.file_type().is_symlink() || stale {
                    fs::remove_file(&path)?;
                }
                continue;
            }
            let Some(key) = name.strip_suffix(".seg") else {
                continue;
            };
            if !is_digest(key) {
                continue;
            }
            if metadata.file_type().is_symlink() {
                fs::remove_file(&path)?;
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let Some(created_at) = read_created_at(&path)? else {
                fs::remove_file(&path)?;
                continue;
            };
            if now.duration_since(created_at).unwrap_or_default() >= ttl {
                fs::remove_file(&path)?;
                continue;
            }
            let last_access = metadata.modified().unwrap_or(created_at);
            let size = metadata.len();
            state.entries.insert(
                key.to_string(),
                Entry {
                    size,
                    created_at,
                    last_access,
                    active_readers: 0,
                },
            );
            state.total_bytes = state.total_bytes.saturating_add(size);
        }

        let cache = Self {
            inner: Arc::new(Inner {
                root,
                ttl,
                max_bytes,
                state: Mutex::new(state),
                writer: tokio::sync::Mutex::new(()),
            }),
        };
        cache.evict_to_budget_sync()?;
        Ok(cache)
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub fn key_for(namespace: &str, resource_url: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(namespace.as_bytes());
        hasher.update([0]);
        hasher.update(resource_url.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub async fn get(
        &self,
        namespace: &str,
        resource_url: &str,
    ) -> std::io::Result<Option<ChunkLease>> {
        let key = Self::key_for(namespace, resource_url);
        let path = self.chunk_path(&key);
        let lease = {
            let mut state = self.lock_state()?;
            let Some(entry) = state.entries.get_mut(&key) else {
                return Ok(None);
            };
            if SystemTime::now()
                .duration_since(entry.created_at)
                .unwrap_or_default()
                >= self.inner.ttl
            {
                if entry.active_readers == 0 {
                    let size = entry.size;
                    match fs::remove_file(&path) {
                        Ok(()) => {
                            state.entries.remove(&key);
                            state.total_bytes = state.total_bytes.saturating_sub(size);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                            state.entries.remove(&key);
                            state.total_bytes = state.total_bytes.saturating_sub(size);
                        }
                        Err(_) => {}
                    }
                }
                return Ok(None);
            }
            entry.active_readers += 1;
            entry.last_access = SystemTime::now();
            ChunkLease {
                inner: Arc::downgrade(&self.inner),
                key: key.clone(),
                content_type: String::new(),
                bytes: Arc::from([]),
            }
        };

        let read_path = path.clone();
        let result = tokio::task::spawn_blocking(move || read_chunk_file(&read_path)).await;
        let (content_type, bytes) = match result {
            Ok(Ok(Some(value))) => value,
            Ok(Ok(None)) => {
                drop(lease);
                self.forget_corrupt(&key, &path).await?;
                return Ok(None);
            }
            Ok(Err(error)) => {
                drop(lease);
                if error.kind() == std::io::ErrorKind::NotFound {
                    self.forget_corrupt(&key, &path).await?;
                    return Ok(None);
                }
                return Err(error);
            }
            Err(error) => {
                drop(lease);
                return Err(std::io::Error::other(format!(
                    "chunk cache reader failed: {error}"
                )));
            }
        };
        let mut lease = lease;
        lease.content_type = content_type;
        lease.bytes = bytes;
        self.touch(&key, &path);
        Ok(Some(lease))
    }

    /// Atomically publish a complete segment. Returns false when it cannot be cached within
    /// budget; the caller may still serve the already-complete response without caching it.
    pub async fn insert(
        &self,
        namespace: &str,
        resource_url: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> std::io::Result<bool> {
        if bytes.is_empty()
            || bytes.len() > MAX_CHUNK_BYTES
            || content_type.len() > MAX_CONTENT_TYPE_BYTES
        {
            return Ok(false);
        }
        let key = Self::key_for(namespace, resource_url);
        let path = self.chunk_path(&key);
        let ctype = content_type.as_bytes();
        let file_size = (FIXED_HEADER_BYTES + ctype.len() + bytes.len()) as u64;
        if file_size > self.inner.max_bytes {
            return Ok(false);
        }

        let _writer = self.inner.writer.lock().await;
        let now = SystemTime::now();
        {
            let mut state = self.lock_state()?;
            if let Some(existing) = state.entries.get(&key) {
                if now.duration_since(existing.created_at).unwrap_or_default() < self.inner.ttl {
                    return Ok(true);
                }
                if existing.active_readers > 0 {
                    return Ok(false);
                }
                let existing_size = existing.size;
                match fs::remove_file(&path) {
                    Ok(()) => {
                        state.entries.remove(&key);
                        state.total_bytes = state.total_bytes.saturating_sub(existing_size);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        state.entries.remove(&key);
                        state.total_bytes = state.total_bytes.saturating_sub(existing_size);
                    }
                    Err(error) => return Err(error),
                }
            }
            self.evict_for(&mut state, file_size)?;
            if state.total_bytes.saturating_add(file_size) > self.inner.max_bytes {
                return Ok(false);
            }
        }

        let temp_path = self
            .inner
            .root
            .join(format!("{key}-{}.part", uuid::Uuid::new_v4()));
        let mut partial_guard = PartialFileGuard {
            path: temp_path.clone(),
            armed: true,
        };
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .await?;
        let write_result = async {
            file.write_all(MAGIC).await?;
            file.write_all(&0u64.to_be_bytes()).await?;
            file.write_all(&(ctype.len() as u16).to_be_bytes()).await?;
            file.write_all(ctype).await?;
            file.write_all(bytes).await?;
            file.flush().await?;
            file.sync_all().await
        }
        .await;
        if let Err(error) = write_result {
            drop(file);
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(error);
        }
        // Stamp the TTL after the segment body is fully written, immediately before publish.
        let created_at = SystemTime::now();
        let epoch_nanos = created_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let stamp_result = async {
            file.seek(SeekFrom::Start(4)).await?;
            file.write_all(&epoch_nanos.to_be_bytes()).await?;
            file.flush().await?;
            file.sync_all().await
        }
        .await;
        drop(file);
        if let Err(error) = stamp_result {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(error);
        }
        if let Err(error) = tokio::fs::rename(&temp_path, &path).await {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(error);
        }
        partial_guard.armed = false;

        let mut state = self.lock_state()?;
        let actual_size = fs::metadata(&path)?.len();
        state.total_bytes = state.total_bytes.saturating_add(actual_size);
        state.entries.insert(
            key,
            Entry {
                size: actual_size,
                created_at,
                last_access: created_at,
                active_readers: 0,
            },
        );
        Ok(true)
    }

    pub async fn cleanup_expired(&self) -> std::io::Result<usize> {
        let _writer = self.inner.writer.lock().await;
        let now = SystemTime::now();
        let mut state = self.lock_state()?;
        let expired: Vec<String> = state
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.active_readers == 0
                    && now.duration_since(entry.created_at).unwrap_or_default() >= self.inner.ttl
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut removed = 0;
        for key in expired {
            let path = self.chunk_path(&key);
            let Some(entry) = state.entries.get(&key) else {
                continue;
            };
            match fs::remove_file(path) {
                Ok(()) => {
                    let size = entry.size;
                    state.entries.remove(&key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                    removed += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let size = entry.size;
                    state.entries.remove(&key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                    removed += 1;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(removed)
    }

    pub fn disk_bytes(&self) -> u64 {
        self.inner
            .state
            .lock()
            .map(|state| state.total_bytes)
            .unwrap_or(0)
    }

    fn lock_state(&self) -> std::io::Result<std::sync::MutexGuard<'_, State>> {
        self.inner
            .state
            .lock()
            .map_err(|_| std::io::Error::other("HLS chunk cache state lock is poisoned"))
    }

    fn chunk_path(&self, key: &str) -> PathBuf {
        self.inner.root.join(format!("{key}.seg"))
    }

    fn evict_for(&self, state: &mut State, new_size: u64) -> std::io::Result<()> {
        while state.total_bytes.saturating_add(new_size) > self.inner.max_bytes {
            let candidate = state
                .entries
                .iter()
                .filter(|(_, entry)| entry.active_readers == 0)
                .min_by_key(|(_, entry)| entry.last_access)
                .map(|(key, _)| key.clone());
            let Some(key) = candidate else {
                return Ok(());
            };
            let path = self.chunk_path(&key);
            let Some(entry) = state.entries.get(&key) else {
                continue;
            };
            match fs::remove_file(path) {
                Ok(()) => {
                    let size = entry.size;
                    state.entries.remove(&key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let size = entry.size;
                    state.entries.remove(&key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn evict_to_budget_sync(&self) -> std::io::Result<()> {
        let mut state = self.lock_state()?;
        self.evict_for(&mut state, 0)
    }

    async fn forget_corrupt(&self, key: &str, path: &Path) -> std::io::Result<()> {
        let _writer = self.inner.writer.lock().await;
        let mut state = self.lock_state()?;
        if let Some(entry) = state.entries.get(key) {
            if entry.active_readers > 0 {
                return Ok(());
            }
            let size = entry.size;
            match fs::remove_file(path) {
                Ok(()) => {
                    state.entries.remove(key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    state.entries.remove(key);
                    state.total_bytes = state.total_bytes.saturating_sub(size);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn touch(&self, key: &str, path: &Path) {
        let now = SystemTime::now();
        if let Ok(mut state) = self.inner.state.lock()
            && let Some(entry) = state.entries.get_mut(key)
        {
            entry.last_access = now;
        }
        // Preserve LRU order across process restarts. TTL continues to use the creation time
        // embedded in the file header, not this modified timestamp.
        let path = path.to_path_buf();
        let _ = std::thread::Builder::new()
            .name("hls-cache-touch".into())
            .spawn(move || {
                if let Ok(file) = open_no_follow(&path) {
                    let times = fs::FileTimes::new().set_modified(now);
                    let _ = file.set_times(times);
                }
            });
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn read_created_at(path: &Path) -> std::io::Result<Option<SystemTime>> {
    let mut file = open_no_follow(path)?;
    let mut header = [0u8; FIXED_HEADER_BYTES];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    if &header[..4] != MAGIC {
        return Ok(None);
    }
    let nanos = u64::from_be_bytes(header[4..12].try_into().expect("fixed header"));
    Ok(UNIX_EPOCH.checked_add(Duration::from_nanos(nanos)))
}

fn read_chunk_file(path: &Path) -> std::io::Result<Option<(String, Arc<[u8]>)>> {
    let mut file = open_no_follow(path)?;
    if !file.metadata()?.is_file() {
        return Ok(None);
    }
    let mut header = [0u8; FIXED_HEADER_BYTES];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error);
    }
    if &header[..4] != MAGIC {
        return Ok(None);
    }
    let content_type_len = u16::from_be_bytes([header[12], header[13]]) as usize;
    if content_type_len == 0 || content_type_len > MAX_CONTENT_TYPE_BYTES {
        return Ok(None);
    }
    let file_len = file.metadata()?.len() as usize;
    if file_len < FIXED_HEADER_BYTES + content_type_len
        || file_len > FIXED_HEADER_BYTES + MAX_CONTENT_TYPE_BYTES + MAX_CHUNK_BYTES
    {
        return Ok(None);
    }
    let mut ctype = vec![0u8; content_type_len];
    file.read_exact(&mut ctype)?;
    let mut data = Vec::with_capacity(file_len - FIXED_HEADER_BYTES - content_type_len);
    file.take((MAX_CHUNK_BYTES + 1) as u64)
        .read_to_end(&mut data)?;
    if data.is_empty() || data.len() > MAX_CHUNK_BYTES {
        return Ok(None);
    }
    let content_type = String::from_utf8(ctype).ok().filter(|value| {
        value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
    });
    Ok(content_type.map(|ct| (ct, Arc::from(data))))
}

fn open_no_follow(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT: inspect the reparse point itself instead of following it.
        options.custom_flags(0x0020_0000);
    }
    let file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cache entry is a Windows reparse point",
            ));
        }
    }
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_cache(max_bytes: u64, ttl: Duration) -> (TempDir, HlsChunkCache) {
        let temp = TempDir::new().unwrap();
        let cache = HlsChunkCache::open(temp.path(), ttl, max_bytes).unwrap();
        (temp, cache)
    }

    #[tokio::test]
    async fn writes_atomically_and_reuses_complete_segments() {
        let (_temp, cache) = test_cache(1024, Duration::from_secs(120));
        assert!(
            cache
                .insert(
                    "episode-1:audio-hi",
                    "https://cdn.example/seg-0.ts?sig=secret",
                    "video/mp2t",
                    b"complete"
                )
                .await
                .unwrap()
        );
        let hit = cache
            .get(
                "episode-1:audio-hi",
                "https://cdn.example/seg-0.ts?sig=secret",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hit.content_type, "video/mp2t");
        assert_eq!(hit.bytes.as_ref(), b"complete");
        assert_eq!(cache.disk_bytes(), 14 + "video/mp2t".len() as u64 + 8);
    }

    #[tokio::test]
    async fn cache_key_is_namespaced_by_source_and_variant_without_persisting_urls() {
        let (_temp, cache) = test_cache(1024, Duration::from_secs(120));
        cache
            .insert(
                "episode-A:audio-hi",
                "https://cdn.example/seg.ts?token=one",
                "video/mp2t",
                b"A",
            )
            .await
            .unwrap();
        assert!(
            cache
                .get("episode-B:audio-hi", "https://cdn.example/seg.ts?token=one")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .get("episode-A:audio-en", "https://cdn.example/seg.ts?token=one")
                .await
                .unwrap()
                .is_none()
        );
        let filename = fs::read_dir(cache.root())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .file_name()
            .to_string_lossy()
            .to_string();
        assert!(!filename.contains("token"));
        assert!(!filename.contains("episode-A"));
    }

    #[tokio::test]
    async fn ttl_is_measured_from_publication_and_active_readers_are_protected() {
        let (_temp, cache) = test_cache(1024, Duration::from_millis(20));
        cache
            .insert("src", "https://cdn.example/seg.ts", "video/mp2t", b"data")
            .await
            .unwrap();
        let lease = cache
            .get("src", "https://cdn.example/seg.ts")
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(cache.cleanup_expired().await.unwrap(), 0);
        assert!(
            cache
                .root()
                .join(format!(
                    "{}.seg",
                    HlsChunkCache::key_for("src", "https://cdn.example/seg.ts")
                ))
                .exists()
        );
        drop(lease);
        assert_eq!(cache.cleanup_expired().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn lru_budget_evicts_oldest_and_never_exceeds_budget() {
        let entry_size = 14 + "video/mp2t".len() as u64 + 4;
        let (_temp, cache) = test_cache(entry_size * 2, Duration::from_secs(120));
        cache
            .insert("src", "https://cdn.example/one.ts", "video/mp2t", b"1111")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        cache
            .insert("src", "https://cdn.example/two.ts", "video/mp2t", b"2222")
            .await
            .unwrap();
        let active = cache
            .get("src", "https://cdn.example/two.ts")
            .await
            .unwrap()
            .unwrap();
        cache
            .insert("src", "https://cdn.example/three.ts", "video/mp2t", b"3333")
            .await
            .unwrap();
        assert!(
            cache
                .get("src", "https://cdn.example/one.ts")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            cache
                .get("src", "https://cdn.example/two.ts")
                .await
                .unwrap()
                .is_some()
        );
        assert!(cache.disk_bytes() <= entry_size * 2);
        drop(active);
    }

    #[tokio::test]
    async fn restart_cleanup_removes_partials_and_expired_files() {
        let temp = TempDir::new().unwrap();
        let cache = HlsChunkCache::open(temp.path(), Duration::from_millis(5), 1024).unwrap();
        cache
            .insert("src", "https://cdn.example/old.ts", "video/mp2t", b"old")
            .await
            .unwrap();
        let part = temp.path().join(format!("{}.part", uuid::Uuid::new_v4()));
        fs::write(&part, b"partial").unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(cache);
        let restarted = HlsChunkCache::open(temp.path(), Duration::from_millis(5), 1024).unwrap();
        assert_eq!(restarted.disk_bytes(), 0);
        assert!(!part.exists());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 0);
    }

    #[test]
    fn startup_preserves_fresh_partial_from_another_writer() {
        let temp = TempDir::new().unwrap();
        let part = temp.path().join(format!("{}.part", uuid::Uuid::new_v4()));
        fs::write(&part, b"active writer").unwrap();
        let _cache = HlsChunkCache::open(temp.path(), Duration::from_secs(120), 1024).unwrap();
        assert_eq!(fs::read(&part).unwrap(), b"active writer");
    }

    #[cfg(unix)]
    #[test]
    fn cache_root_symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let linked_root = temp.path().join("linked-cache");
        symlink(outside.path(), &linked_root).unwrap();
        let result = HlsChunkCache::open(&linked_root, Duration::from_secs(120), 1024);
        assert!(matches!(
            result,
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn startup_unlinks_cache_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;
        let temp = TempDir::new().unwrap();
        let outside = TempDir::new().unwrap();
        let target = outside.path().join("protected");
        fs::write(&target, b"do not remove").unwrap();
        let key = "a".repeat(64);
        symlink(&target, temp.path().join(format!("{key}.seg"))).unwrap();
        let _cache = HlsChunkCache::open(temp.path(), Duration::from_secs(120), 1024).unwrap();
        assert!(target.exists());
        assert_eq!(fs::read(&target).unwrap(), b"do not remove");
    }

    #[test]
    fn dropping_partial_write_guard_removes_unpublished_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("abandoned.part");
        fs::write(&path, b"partial").unwrap();
        let guard = PartialFileGuard {
            path: path.clone(),
            armed: true,
        };
        drop(guard);
        assert!(!path.exists());
    }
}
