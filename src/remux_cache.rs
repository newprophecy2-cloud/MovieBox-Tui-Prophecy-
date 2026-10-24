use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{io, path::PathBuf, time::Duration};

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

/// Kept as a compatibility enum for the current API call sites. Persistent media
/// caching is deliberately disabled; `prepare` always returns `Disabled`.
pub enum CacheState {
    Hit(CachedRemux),
    Build(Box<CacheWriter>),
    Building,
    Disabled,
}

/// Compatibility type for the current stream pipeline. No instance is returned
/// by this module while disk-backed remux caching is disabled.
pub struct CacheWriter {
    _private: (),
}

impl CacheWriter {
    pub async fn write_chunk(&mut self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::Error::other("persistent remux caching is disabled"))
    }

    pub async fn abort(self) {}

    pub async fn finish(self) -> io::Result<CachedRemux> {
        Err(io::Error::other("persistent remux caching is disabled"))
    }
}

pub fn cache_key(url: &str, headers: &[(String, String)]) -> String {
    let mut canonical_headers = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect::<Vec<_>>();
    canonical_headers.sort();

    let mut digest = Sha256::new();
    digest.update(b"moviebox-remux-cache-v2-disabled\0");
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

/// This API intentionally does not create cache directories, temporary media,
/// lock files, or persistent output files.
pub async fn prepare(_key: &str) -> CacheState {
    CacheState::Disabled
}

pub async fn clear_legacy_cache() -> io::Result<()> {
    let root = crate::config::cache_dir().join("remux");
    match tokio::fs::remove_dir_all(root).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub async fn lookup(_key: &str) -> Option<CachedRemux> {
    None
}

pub async fn is_building(_key: &str) -> bool {
    false
}

pub async fn wait_for(_key: &str, _timeout: Duration) -> Option<CachedRemux> {
    None
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
        total_size.saturating_sub(1)
    } else {
        let Ok(end) = end.parse::<u64>() else {
            return ByteRange::Full;
        };
        end.min(total_size.saturating_sub(1))
    };
    if start >= total_size || start > end {
        ByteRange::Unsatisfiable
    } else {
        ByteRange::Partial { start, end }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ByteRange, CacheState, CacheWriter, cache_key, is_building, lookup, parse_range_header,
        prepare, wait_for,
    };
    use std::time::Duration;

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

    #[tokio::test]
    async fn media_cache_is_disabled_and_never_returns_disk_artifacts() {
        assert!(matches!(prepare("test-key").await, CacheState::Disabled));
        assert!(lookup("test-key").await.is_none());
        assert!(!is_building("test-key").await);
        assert!(
            wait_for("test-key", Duration::from_millis(1))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn compatibility_writer_refuses_all_media_writes() {
        let mut writer = CacheWriter { _private: () };
        assert!(writer.write_chunk(b"media bytes").await.is_err());
        assert!(writer.finish().await.is_err());
    }
}
