use crate::providers::addons::models::InstalledAddon;
use crate::providers::models::ProviderKind;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

const HLS_CHUNK_CACHE_MIN_BYTES: u64 = 512 * 1024 * 1024;
const HLS_CHUNK_CACHE_MAX_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub auto_update: bool,
    pub last_update_check: u64,
    pub active_mode: String,
    pub active_provider: ProviderKind,
    pub active_theme: String,
    pub moviebox_enabled: bool,
    pub fourkhdhub_enabled: bool,
    pub dramachi_enabled: bool,
    pub bdix_circleftp_enabled: bool,
    pub bdix_dhakaflix_enabled: bool,
    pub bdix_probed: bool,
    pub streaming_enabled: bool,
    pub tv_enabled: bool,
    pub addons_enabled: bool,
    pub default_player: Option<String>,
    pub download_dir: Option<String>,
    pub vlc_path: Option<String>,
    pub mpv_path: Option<String>,
    pub iina_path: Option<String>,
    pub media_cache_directory: Option<String>,
    pub remux_cache_ttl_seconds: u64,
    pub remux_cache_max_bytes: u64,
    pub max_concurrent_media_jobs: usize,
    pub media_job_timeout_seconds: u64,
    pub hls_chunk_cache_max_bytes: u64,
    pub hls_prefetch_depth: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            auto_update: true,
            last_update_check: 0,
            active_mode: "streaming".to_string(),
            active_provider: ProviderKind::MovieBox,
            active_theme: String::new(),
            moviebox_enabled: true,
            fourkhdhub_enabled: true,
            dramachi_enabled: true,
            bdix_circleftp_enabled: false,
            bdix_dhakaflix_enabled: false,
            bdix_probed: false,
            streaming_enabled: true,
            tv_enabled: true,
            addons_enabled: false,
            default_player: None,
            download_dir: None,
            vlc_path: None,
            mpv_path: None,
            iina_path: None,
            media_cache_directory: None,
            remux_cache_ttl_seconds: 3_600,
            remux_cache_max_bytes: 512 * 1024 * 1024,
            max_concurrent_media_jobs: 1,
            media_job_timeout_seconds: 1_800,
            hls_chunk_cache_max_bytes: HLS_CHUNK_CACHE_MIN_BYTES,
            hls_prefetch_depth: 2,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MediaSettings {
    pub cache_dir: PathBuf,
    pub cache_ttl: std::time::Duration,
    pub cache_max_bytes: u64,
    pub max_concurrent_jobs: usize,
    pub job_timeout: std::time::Duration,
}

impl Config {
    pub fn validate_hls_chunk_settings(&self) -> Result<(), String> {
        if !(HLS_CHUNK_CACHE_MIN_BYTES..=HLS_CHUNK_CACHE_MAX_BYTES)
            .contains(&self.hls_chunk_cache_max_bytes)
        {
            return Err(
                "hls_chunk_cache_max_bytes must be between 536870912 and 10737418240".into(),
            );
        }
        if self.hls_prefetch_depth > 4 {
            return Err("hls_prefetch_depth must be between 0 and 4".into());
        }
        Ok(())
    }

    pub fn validate_media_settings(&self) -> Result<MediaSettings, String> {
        if !(60..=604_800).contains(&self.remux_cache_ttl_seconds) {
            return Err("remux_cache_ttl_seconds must be between 60 and 604800".into());
        }
        if !(1_048_576..=1_099_511_627_776).contains(&self.remux_cache_max_bytes) {
            return Err("remux_cache_max_bytes must be between 1048576 and 1099511627776".into());
        }
        if !(1..=4).contains(&self.max_concurrent_media_jobs) {
            return Err("max_concurrent_media_jobs must be between 1 and 4".into());
        }
        if !(60..=86_400).contains(&self.media_job_timeout_seconds) {
            return Err("media_job_timeout_seconds must be between 60 and 86400".into());
        }

        let configured_cache_dir = match self
            .media_cache_directory
            .as_deref()
            .map(str::trim)
            .filter(|path| !path.is_empty())
        {
            Some(path) => PathBuf::from(path),
            None => cache_dir().join("remux"),
        };
        if !configured_cache_dir.is_absolute() {
            return Err("the configured cache directory must be an absolute path".into());
        }
        std::fs::create_dir_all(&configured_cache_dir)
            .map_err(|_| "the configured cache directory cannot be created".to_string())?;
        let canonical_cache_dir = std::fs::canonicalize(&configured_cache_dir)
            .map_err(|_| "the configured cache directory cannot be resolved".to_string())?;
        if !canonical_cache_dir.is_dir() {
            return Err("the configured cache path is not a directory".into());
        }
        let probe_path = canonical_cache_dir.join(format!(
            ".moviebox-cache-check-{}-{}.tmp",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe_path)
            .map_err(|_| "the configured cache directory is not writable".to_string())?;
        std::fs::remove_file(&probe_path)
            .map_err(|_| "the cache directory write-check file could not be removed".to_string())?;

        Ok(MediaSettings {
            cache_dir: canonical_cache_dir,
            cache_ttl: std::time::Duration::from_secs(self.remux_cache_ttl_seconds),
            cache_max_bytes: self.remux_cache_max_bytes,
            max_concurrent_jobs: self.max_concurrent_media_jobs,
            job_timeout: std::time::Duration::from_secs(self.media_job_timeout_seconds),
        })
    }
}

pub const APP_NAME: &str = "moviebox-tui";

static TEST_SANDBOX_DIR: std::sync::LazyLock<Option<PathBuf>> = std::sync::LazyLock::new(|| {
    let is_test_binary = cfg!(test)
        || std::env::var_os("NEXTEST").is_some()
        || std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().and_then(|d| d.file_name()).map(|n| n == "deps"))
            .unwrap_or(false);
    if is_test_binary {
        let dir =
            std::env::temp_dir().join(format!("moviebox_test_sandbox_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        Some(dir)
    } else {
        None
    }
});

pub fn is_test_environment() -> bool {
    TEST_SANDBOX_DIR.is_some()
}
pub const TERMUX_PREFIX_USR: &str = "/data/data/com.termux/files/usr";

pub fn is_termux_environment() -> bool {
    cfg!(target_os = "android")
        || std::env::var("TERMUX_VERSION").is_ok()
        || std::env::var("PREFIX").is_ok_and(|p| p.contains("com.termux"))
        || std::path::Path::new(TERMUX_PREFIX_USR).exists()
}

pub fn config_dir() -> Option<PathBuf> {
    if let Some(sandbox) = TEST_SANDBOX_DIR.as_ref() {
        return Some(sandbox.join("config").join(APP_NAME));
    }
    if let Ok(dir) = std::env::var("MOVIEBOX_CONFIG_DIR") {
        return Some(PathBuf::from(dir));
    }
    if let Some(dir) = dirs::config_dir() {
        return Some(dir.join(APP_NAME));
    }
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        let p = PathBuf::from(xdg);
        if !p.as_os_str().is_empty() {
            return Some(p.join(APP_NAME));
        }
    }
    if let Ok(prefix) = std::env::var("PREFIX") {
        let p = PathBuf::from(prefix).join("etc").join(APP_NAME);
        if p.exists() {
            return Some(p);
        }
    }
    if let Some(dir) = dirs::home_dir().map(|h| h.join(".config").join(APP_NAME)) {
        return Some(dir);
    }
    let fallback = std::env::temp_dir().join(APP_NAME).join("config");
    log::warn!(
        "unable to locate user config directory, falling back to {}",
        fallback.display()
    );
    Some(fallback)
}

pub fn data_dir() -> Option<PathBuf> {
    if let Some(sandbox) = TEST_SANDBOX_DIR.as_ref() {
        return Some(sandbox.join("data").join(APP_NAME));
    }
    if let Ok(dir) = std::env::var("MOVIEBOX_DATA_DIR") {
        return Some(PathBuf::from(dir));
    }
    if let Some(dir) = dirs::data_dir() {
        return Some(dir.join(APP_NAME));
    }
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
        let p = PathBuf::from(xdg);
        if !p.as_os_str().is_empty() {
            return Some(p.join(APP_NAME));
        }
    }
    if let Ok(prefix) = std::env::var("PREFIX") {
        let p = PathBuf::from(prefix).join("var").join("lib").join(APP_NAME);
        if p.exists() {
            return Some(p);
        }
    }
    if let Some(dir) = dirs::home_dir().map(|h| h.join(".local").join("share").join(APP_NAME)) {
        return Some(dir);
    }
    let fallback = std::env::temp_dir().join(APP_NAME).join("data");
    log::warn!(
        "unable to locate user data directory, falling back to {}",
        fallback.display()
    );
    Some(fallback)
}

pub fn cache_dir() -> PathBuf {
    if let Some(sandbox) = TEST_SANDBOX_DIR.as_ref() {
        return sandbox.join("cache").join(APP_NAME);
    }
    if let Ok(dir) = std::env::var("MOVIEBOX_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    if let Some(dir) = dirs::cache_dir() {
        return dir.join(APP_NAME);
    }
    if let Ok(xdg) = std::env::var("XDG_CACHE_HOME") {
        let p = PathBuf::from(xdg);
        if !p.as_os_str().is_empty() {
            return p.join(APP_NAME);
        }
    }
    if let Ok(prefix) = std::env::var("PREFIX") {
        let p = PathBuf::from(prefix)
            .join("var")
            .join("cache")
            .join(APP_NAME);
        if p.exists() {
            return p;
        }
    }
    dirs::home_dir()
        .map(|h| h.join(".cache").join(APP_NAME))
        .unwrap_or_else(|| std::env::temp_dir().join(APP_NAME))
}

pub fn logs_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        if let Some(dir) = dirs::data_local_dir() {
            return dir.join(APP_NAME).join("logs");
        }
    }
    data_dir()
        .map(|dir| dir.join("logs"))
        .unwrap_or_else(|| std::env::temp_dir().join(APP_NAME).join("logs"))
}

pub fn scripts_dir() -> Option<PathBuf> {
    data_dir().map(|dir| dir.join("scripts"))
}

pub fn playback_state_dir() -> Option<PathBuf> {
    data_dir().map(|dir| dir.join("playback"))
}

pub fn config_path() -> Option<PathBuf> {
    config_dir().map(|dir| dir.join("config.json"))
}

pub fn addons_path() -> Option<PathBuf> {
    config_dir().map(|dir| dir.join("addons_config.json"))
}

pub fn tv_path() -> Option<PathBuf> {
    config_dir().map(|dir| dir.join("tv_config.json"))
}

pub fn history_path() -> Option<PathBuf> {
    data_dir().map(|dir| dir.join("history.json"))
}

pub fn favorites_path() -> Option<PathBuf> {
    data_dir().map(|dir| dir.join("favorites.json"))
}

pub fn rotate_corrupt_file(path: &std::path::Path, label: &str) {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let corrupt_path = path.with_extension(format!("corrupt.{stamp}"));
    log::error!(
        "failed to parse {label} from {}, rotating to {}",
        crate::logging::sanitize_path(path),
        crate::logging::sanitize_path(&corrupt_path)
    );
    let _ = std::fs::rename(path, corrupt_path);
}

pub fn load() -> Config {
    let Some(path) = config_path() else {
        return Config::default();
    };
    if path.exists() {
        if let Ok(content) = std::fs::read_to_string(&path) {
            let mut val: serde_json::Value = match serde_json::from_str(&content) {
                Ok(v) => v,
                Err(e) => {
                    log::warn!("config invalid JSON: {e}; resetting");
                    serde_json::Value::default()
                }
            };
            let old_bdix = val
                .get("bdix_enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if let Some(obj) = val.as_object_mut() {
                obj.remove("bdix_enabled");
            }
            if let Ok(mut config) = serde_json::from_value::<Config>(val) {
                if old_bdix {
                    config.bdix_circleftp_enabled = true;
                    config.bdix_dhakaflix_enabled = true;
                }
                return config;
            }
        }
        rotate_corrupt_file(&path, "config");
    }
    Config::default()
}

pub fn save(config: &Config) {
    let Some(path) = config_path() else {
        return;
    };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(json) = serde_json::to_string_pretty(config) {
        if let Err(error) = crate::cache::atomic_write_file(&path, json.as_bytes()) {
            log::warn!("failed to write config: {error}");
        }
    }
}

pub fn load_addons() -> Vec<InstalledAddon> {
    // Render/container deployments may not have a durable writable home directory.
    // Permit an explicit JSON array of InstalledAddon objects via environment.
    let env_configured = std::env::var("MOVIEBOX_ADDONS_JSON")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let mut list = if let Some(raw) = env_configured.as_deref() {
        match serde_json::from_str::<Vec<InstalledAddon>>(raw) {
            Ok(parsed) => parsed,
            Err(error) => {
                log::error!("MOVIEBOX_ADDONS_JSON is invalid JSON: {error}");
                Vec::new()
            }
        }
    } else if let Some(path) = addons_path() {
        if path.exists() {
            match std::fs::read_to_string(&path) {
                Ok(content) => match serde_json::from_str::<Vec<InstalledAddon>>(&content) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        rotate_corrupt_file(&path, &format!("addons config ({e})"));
                        Vec::new()
                    }
                },
                Err(e) => {
                    log::warn!(
                        "failed to read addons config from {}: {e}",
                        crate::logging::sanitize_path(&path)
                    );
                    Vec::new()
                }
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    if !list.iter().any(|a| a.is_core()) {
        list.insert(0, InstalledAddon::cinemeta_default());
        if env_configured.is_none() {
            save_addons(&list);
        }
    } else {
        for a in &mut list {
            if a.is_core() {
                a.enabled = true;
            }
        }
    }
    list
}

pub fn save_addons(addons: &[InstalledAddon]) {
    let Some(path) = addons_path() else {
        return;
    };
    if let Some(app_dir) = path.parent()
        && std::fs::create_dir_all(app_dir).is_err()
    {
        return;
    }
    let Ok(json) = serde_json::to_string_pretty(addons) else {
        return;
    };
    if let Err(error) = crate::cache::atomic_write_file(&path, json.as_bytes()) {
        log::warn!("failed to write addons config: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_and_serde() {
        let config = Config::default();
        assert!(config.auto_update);
        assert_eq!(config.active_provider, ProviderKind::MovieBox);
        assert_eq!(config.remux_cache_ttl_seconds, 3_600);
        assert_eq!(config.remux_cache_max_bytes, 512 * 1024 * 1024);
        assert_eq!(config.max_concurrent_media_jobs, 1);
        assert_eq!(config.media_job_timeout_seconds, 1_800);
        assert_eq!(config.hls_chunk_cache_max_bytes, 512 * 1024 * 1024);
        assert_eq!(config.hls_prefetch_depth, 2);

        let json = serde_json::to_string(&config).expect("serialize");
        let deserialized: Config = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.active_mode, config.active_mode);
        let old_config: Config = serde_json::from_str(r#"{"auto_update":false}"#)
            .expect("older config without media settings remains compatible");
        assert_eq!(old_config.remux_cache_ttl_seconds, 3_600);
        assert_eq!(old_config.hls_chunk_cache_max_bytes, 512 * 1024 * 1024);
        assert_eq!(old_config.hls_prefetch_depth, 2);
    }

    #[test]
    fn media_settings_validate_and_resolve_existing_cache_root() {
        let settings = Config::default()
            .validate_media_settings()
            .expect("local test cache path is writable");
        assert!(settings.cache_dir.is_absolute());
        assert!(settings.cache_dir.ends_with("remux"));
        assert_eq!(settings.max_concurrent_jobs, 1);
    }

    #[test]
    fn media_settings_reject_out_of_range_values() {
        let config = Config {
            remux_cache_ttl_seconds: 59,
            ..Config::default()
        };
        assert!(config.validate_media_settings().is_err());

        let config = Config {
            remux_cache_max_bytes: 1_048_575,
            ..Config::default()
        };
        assert!(config.validate_media_settings().is_err());

        let config = Config {
            max_concurrent_media_jobs: 5,
            ..Config::default()
        };
        assert!(config.validate_media_settings().is_err());

        let config = Config {
            media_job_timeout_seconds: 59,
            ..Config::default()
        };
        assert!(config.validate_media_settings().is_err());
    }

    #[test]
    fn hls_chunk_settings_reject_unbounded_values() {
        let config = Config {
            hls_chunk_cache_max_bytes: HLS_CHUNK_CACHE_MIN_BYTES - 1,
            ..Config::default()
        };
        assert!(config.validate_hls_chunk_settings().is_err());

        let config = Config {
            hls_chunk_cache_max_bytes: HLS_CHUNK_CACHE_MAX_BYTES + 1,
            ..Config::default()
        };
        assert!(config.validate_hls_chunk_settings().is_err());

        for bytes in [HLS_CHUNK_CACHE_MIN_BYTES, HLS_CHUNK_CACHE_MAX_BYTES] {
            let config = Config {
                hls_chunk_cache_max_bytes: bytes,
                ..Config::default()
            };
            assert!(config.validate_hls_chunk_settings().is_ok());
        }

        let config = Config {
            hls_prefetch_depth: 5,
            ..Config::default()
        };
        assert!(config.validate_hls_chunk_settings().is_err());
    }

    #[test]
    fn media_cache_directory_override_must_be_absolute_and_is_persisted() {
        let root =
            std::env::temp_dir().join(format!("moviebox-config-cache-{}", uuid::Uuid::new_v4()));
        let config = Config {
            media_cache_directory: Some(root.to_string_lossy().into_owned()),
            ..Config::default()
        };
        let settings = config
            .validate_media_settings()
            .expect("absolute override is valid");
        assert_eq!(settings.cache_dir, std::fs::canonicalize(&root).unwrap());

        let invalid = Config {
            media_cache_directory: Some("relative/cache".into()),
            ..Config::default()
        };
        assert!(
            invalid
                .validate_media_settings()
                .unwrap_err()
                .contains("absolute path")
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
