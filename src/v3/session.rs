use crate::v3::models::{CanonicalAnime, TemporarySession};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    pub ttl: Duration,
    pub idle_ttl: Duration,
    pub max_ttl: Duration,
    pub max_active_sessions: usize,
    pub max_search_results: usize,
}

impl SessionConfig {
    pub fn from_env() -> Self {
        let parse_u64 = |key: &str, default: u64, min: u64, max: u64| -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|v| v.clamp(min, max))
                .unwrap_or(default)
        };
        let ttl_secs = parse_u64("PV_SESSION_TTL_SECONDS", 900, 30, 86_400);
        let idle_secs = parse_u64("PV_SESSION_IDLE_TTL_SECONDS", 600, 30, 86_400);
        let max_secs = parse_u64("PV_SESSION_MAX_TTL_SECONDS", 1800, ttl_secs, 86_400);
        let max_sessions = parse_u64("PV_MAX_ACTIVE_SESSIONS", 500, 10, 50_000) as usize;
        let max_results = parse_u64("PV_MAX_SEARCH_RESULTS", 50, 1, 200) as usize;
        Self {
            ttl: Duration::from_secs(ttl_secs),
            idle_ttl: Duration::from_secs(idle_secs),
            max_ttl: Duration::from_secs(max_secs),
            max_active_sessions: max_sessions,
            max_search_results: max_results,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLookupError {
    Required,
    Invalid,
    Expired,
}

#[derive(Clone)]
pub struct SessionStore {
    sessions: Arc<RwLock<HashMap<String, Arc<RwLock<TemporarySession>>>>>,
    config: SessionConfig,
}

impl SessionStore {
    pub fn new(config: SessionConfig) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub fn config(&self) -> SessionConfig {
        self.config
    }

    pub fn spawn_cleanup_task(&self) {
        let store = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let removed = store.cleanup_expired().await;
                if removed > 0 {
                    log::info!("v3_session_cleanup removed_expired={removed}");
                }
            }
        });
    }

    pub async fn create_session(&self, anime_list: Vec<CanonicalAnime>) -> Arc<RwLock<TemporarySession>> {
        let token = generate_session_token();
        let now = Instant::now();
        let expires_at = now + self.config.ttl.min(self.config.idle_ttl);
        let max_expires_at = now + self.config.max_ttl;

        let mut anime_records = HashMap::with_capacity(anime_list.len());
        let mut season_index = HashMap::new();
        let mut episode_index = HashMap::new();
        for anime in anime_list {
            for season in &anime.seasons {
                season_index.insert(season.id.clone(), anime.id.clone());
                for ep in &season.episodes {
                    episode_index.insert(ep.id.clone(), (anime.id.clone(), season.id.clone()));
                }
            }
            anime_records.insert(anime.id.clone(), anime);
        }

        let session = Arc::new(RwLock::new(TemporarySession {
            session_id: token.clone(),
            created_at: now,
            last_accessed_at: now,
            expires_at,
            max_expires_at,
            anime_records,
            season_index,
            episode_index,
            next_anime_seq: 1,
        }));

        let mut map = self.sessions.write().await;
        if map.len() >= self.config.max_active_sessions {
            let now_check = Instant::now();
            let mut expired_keys = Vec::new();
            for (k, sess) in map.iter() {
                if let Ok(guard) = sess.try_read()
                    && is_session_expired(&guard, now_check)
                {
                    expired_keys.push(k.clone());
                }
            }
            for k in expired_keys {
                map.remove(&k);
            }
            if map.len() >= self.config.max_active_sessions
                && let Some(oldest_key) = map
                    .iter()
                    .filter_map(|(k, v)| v.try_read().ok().map(|g| (k.clone(), g.last_accessed_at)))
                    .min_by_key(|(_, ts)| *ts)
                    .map(|(k, _)| k)
            {
                map.remove(&oldest_key);
            }
        }
        map.insert(token, Arc::clone(&session));
        session
    }

    pub async fn get_session(
        &self,
        raw_token: Option<&str>,
    ) -> Result<Arc<RwLock<TemporarySession>>, SessionLookupError> {
        let Some(token) = raw_token.map(str::trim).filter(|t| !t.is_empty()) else {
            return Err(SessionLookupError::Required);
        };
        if !is_valid_token_format(token) {
            return Err(SessionLookupError::Invalid);
        }

        let session_arc = {
            let map = self.sessions.read().await;
            map.get(token).cloned()
        };
        let Some(session_arc) = session_arc else {
            return Err(SessionLookupError::Expired);
        };

        let now = Instant::now();
        {
            let mut guard = session_arc.write().await;
            if is_session_expired(&guard, now) {
                drop(guard);
                let mut map = self.sessions.write().await;
                map.remove(token);
                return Err(SessionLookupError::Expired);
            }
            guard.last_accessed_at = now;
            guard.expires_at = (now + self.config.idle_ttl).min(guard.max_expires_at);
        }
        Ok(session_arc)
    }

    pub async fn cleanup_expired(&self) -> usize {
        let now = Instant::now();
        let candidates: Vec<(String, Arc<RwLock<TemporarySession>>)> = {
            let map = self.sessions.read().await;
            map.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect()
        };

        let mut expired = Vec::new();
        for (k, sess) in candidates {
            let guard = sess.read().await;
            if is_session_expired(&guard, now) {
                expired.push(k);
            }
        }
        if expired.is_empty() {
            return 0;
        }
        let mut map = self.sessions.write().await;
        let mut removed = 0;
        for k in expired {
            if map.remove(&k).is_some() {
                removed += 1;
            }
        }
        removed
    }

    pub async fn active_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

fn is_session_expired(session: &TemporarySession, now: Instant) -> bool {
    now >= session.expires_at || now >= session.max_expires_at
}

fn is_valid_token_format(token: &str) -> bool {
    token.len() >= 32 && token.len() <= 128 && token.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

pub fn generate_session_token() -> String {
    let mut bytes = [0u8; 24];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn generate_slug_token() -> String {
    let mut bytes = [0u8; 3];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}

pub fn format_anime_id(slug_token: &str) -> String {
    format!("NP-A-{slug_token}")
}

pub fn format_season_id(slug_token: &str, season_number: usize) -> String {
    format!("NP-S-{slug_token}-{season_number:03}")
}

pub fn format_episode_id(slug_token: &str, season_number: usize, episode_number: usize) -> String {
    if season_number <= 1 {
        format!("NP-E-{slug_token}-{episode_number:04}")
    } else {
        format!("NP-E-{slug_token}-S{season_number:02}-{episode_number:04}")
    }
}

pub fn hash_session_id(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    digest[..6].iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::models::MediaType;

    fn dummy_anime(token: &str) -> CanonicalAnime {
        CanonicalAnime {
            id: format_anime_id(token),
            slug_token: token.to_string(),
            title: "Test Anime".to_string(),
            alternative_titles: Vec::new(),
            media_type: MediaType::Series,
            year: Some(2020),
            description: None,
            poster_url: None,
            genres: Vec::new(),
            provider_records: Vec::new(),
            seasons: Vec::new(),
            total_seasons: 0,
            total_episodes: 0,
            season_structure_status: "pending".to_string(),
            details_hydrated: false,
        }
    }

    #[tokio::test]
    async fn test_session_creation_lookup_and_expiration() {
        let store = SessionStore::new(SessionConfig {
            ttl: Duration::from_millis(40),
            idle_ttl: Duration::from_millis(40),
            max_ttl: Duration::from_millis(80),
            max_active_sessions: 10,
            max_search_results: 20,
        });

        assert_eq!(store.get_session(None).await.unwrap_err(), SessionLookupError::Required);
        assert_eq!(store.get_session(Some("short")).await.unwrap_err(), SessionLookupError::Invalid);

        let sess = store.create_session(vec![dummy_anime("8F21D9")]).await;
        let token = sess.read().await.session_id.clone();
        assert!(store.get_session(Some(&token)).await.is_ok());

        tokio::time::sleep(Duration::from_millis(95)).await;
        assert_eq!(store.get_session(Some(&token)).await.unwrap_err(), SessionLookupError::Expired);
        assert_eq!(store.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_session_cleanup_removes_expired_entries() {
        let store = SessionStore::new(SessionConfig {
            ttl: Duration::from_millis(25),
            idle_ttl: Duration::from_millis(25),
            max_ttl: Duration::from_millis(25),
            max_active_sessions: 10,
            max_search_results: 20,
        });
        store.create_session(vec![dummy_anime("AABBCC")]).await;
        assert_eq!(store.active_count().await, 1);
        tokio::time::sleep(Duration::from_millis(40)).await;
        let removed = store.cleanup_expired().await;
        assert_eq!(removed, 1);
        assert_eq!(store.active_count().await, 0);
    }
}
