use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{BoxFuture, FutureExt, Shared};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::sync::Mutex;

const ANILIST_URL: &str = "https://graphql.anilist.co";
const REQUESTS_PER_MINUTE: usize = 24;
const MIN_REQUEST_INTERVAL: Duration = Duration::from_millis(2500);
const MAX_CONCURRENT_REQUESTS: usize = 1;
const MAX_RETRIES: usize = 2;
const POSITIVE_TTL: Duration = Duration::from_secs(86_400);
const MAPPING_TTL: Duration = Duration::from_secs(604_800);
const NEGATIVE_TTL: Duration = Duration::from_secs(600);
const CIRCUIT_FAILURE_THRESHOLD: u32 = 3;
const CIRCUIT_OPEN: Duration = Duration::from_secs(30);

type SharedLookup = Shared<BoxFuture<'static, Result<Option<Value>, String>>>;

#[derive(Clone)]
pub struct AniListCoordinator {
    client: reqwest::Client,
    limiter: crate::rate_limit::HostLimiter,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    inflight: Arc<Mutex<HashMap<String, SharedLookup>>>,
    circuit: Arc<Mutex<CircuitState>>,
}

#[derive(Clone)]
struct CacheEntry {
    value: Option<Value>,
    expires_at: Instant,
}

#[derive(Default)]
struct CircuitState {
    failures: u32,
    open_until: Option<Instant>,
}

impl AniListCoordinator {
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            limiter: crate::rate_limit::HostLimiter::new(
                MAX_CONCURRENT_REQUESTS,
                MIN_REQUEST_INTERVAL,
            ),
            cache: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            circuit: Arc::new(Mutex::new(CircuitState::default())),
        }
    }

    pub async fn metadata_by_id(&self, id: i64) -> Result<Option<Value>, String> {
        self.lookup(format!("id:{id}"), json!({"id": id}), POSITIVE_TTL)
            .await
    }

    pub async fn mapping_by_title(&self, title: &str) -> Result<Option<Value>, String> {
        let normalized = title.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Ok(None);
        }
        self.lookup(
            format!("title:{normalized}"),
            json!({"search": title.trim()}),
            MAPPING_TTL,
        )
        .await
    }

    async fn lookup(
        &self,
        key: String,
        variables: Value,
        positive_ttl: Duration,
    ) -> Result<Option<Value>, String> {
        if let Some(entry) = self.cache.lock().await.get(&key).cloned() {
            if entry.expires_at > Instant::now() {
                return Ok(entry.value);
            }
        }

        if let Some(existing) = self.inflight.lock().await.get(&key).cloned() {
            return existing.await;
        }

        let coordinator = self.clone();
        let key_for_task = key.clone();
        let future: SharedLookup = async move {
            let result = coordinator.fetch(variables).await;
            match &result {
                Ok(Some(value)) => {
                    coordinator.cache.lock().await.insert(
                        key_for_task.clone(),
                        CacheEntry {
                            value: Some(value.clone()),
                            expires_at: Instant::now() + positive_ttl,
                        },
                    );
                }
                Ok(None) => {
                    coordinator.cache.lock().await.insert(
                        key_for_task.clone(),
                        CacheEntry {
                            value: None,
                            expires_at: Instant::now() + NEGATIVE_TTL,
                        },
                    );
                }
                Err(_) => {}
            }
            result
        }
        .boxed()
        .shared();

        let mut inflight = self.inflight.lock().await;
        if let Some(existing) = inflight.get(&key).cloned() {
            drop(inflight);
            return existing.await;
        }
        inflight.insert(key.clone(), future.clone());
        drop(inflight);
        let result = future.await;
        self.inflight.lock().await.remove(&key);
        result
    }

    async fn fetch(&self, variables: Value) -> Result<Option<Value>, String> {
        {
            let state = self.circuit.lock().await;
            if state.open_until.is_some_and(|until| until > Instant::now()) {
                return Err("AniList circuit breaker is open".into());
            }
        }

        let _permit = self.limiter.acquire().await.map_err(|_| {
            "AniList request timed out while waiting for the global budget".to_string()
        })?;
        let query = "query($id:Int,$search:String){Page(perPage:1){media(id:$id,search:$search){id title{romaji english native} format episodes season seasonYear genres}}}";
        let response = tokio::time::timeout(
            Duration::from_secs(20),
            self.client
                .post(ANILIST_URL)
                .json(&json!({"query": query, "variables": variables}))
                .send(),
        )
        .await
        .map_err(|_| "AniList request timed out".to_string())?
        .map_err(|error| error.to_string())?;
        self.limiter
            .record_response(response.status(), response.headers())
            .await;

        if response.status() == StatusCode::NOT_FOUND {
            self.record_failure(false).await;
            return Ok(None);
        }
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            self.record_failure(true).await;
            return Err("AniList rate limited the request".into());
        }
        if !response.status().is_success() {
            self.record_failure(true).await;
            return Err(format!("AniList HTTP {}", response.status()));
        }
        let payload: Value = response.json().await.map_err(|error| error.to_string())?;
        let media = payload
            .get("data")
            .and_then(|data| data.get("Page"))
            .and_then(|page| page.get("media"));
        self.record_failure(false).await;
        Ok(media.filter(|value| !value.is_null()).cloned())
    }

    async fn record_failure(&self, failure: bool) {
        let mut state = self.circuit.lock().await;
        if failure {
            state.failures = state.failures.saturating_add(1);
            if state.failures >= CIRCUIT_FAILURE_THRESHOLD {
                state.open_until = Some(Instant::now() + CIRCUIT_OPEN);
            }
        } else {
            state.failures = 0;
            state.open_until = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn positive_and_negative_cache_entries_expire_independently() {
        let coordinator = AniListCoordinator::new(reqwest::Client::new());
        let now = Instant::now();
        let mut cache = coordinator.cache.lock().await;
        cache.insert(
            "positive".into(),
            CacheEntry {
                value: Some(json!({"id": 1})),
                expires_at: now + Duration::from_secs(60),
            },
        );
        cache.insert(
            "negative".into(),
            CacheEntry {
                value: None,
                expires_at: now + NEGATIVE_TTL,
            },
        );
        assert!(cache.get("positive").unwrap().value.is_some());
        assert!(cache.get("negative").unwrap().value.is_none());
    }

    #[tokio::test]
    async fn circuit_opens_after_consecutive_failures_and_resets_on_success() {
        let coordinator = AniListCoordinator::new(reqwest::Client::new());
        for _ in 0..CIRCUIT_FAILURE_THRESHOLD {
            coordinator.record_failure(true).await;
        }
        assert!(coordinator.circuit.lock().await.open_until.is_some());
        coordinator.record_failure(false).await;
        assert!(coordinator.circuit.lock().await.open_until.is_none());
    }

    #[test]
    fn conservative_budget_constants_are_explicit() {
        assert_eq!(REQUESTS_PER_MINUTE, 24);
        assert_eq!(MIN_REQUEST_INTERVAL, Duration::from_millis(2500));
        assert_eq!(MAX_CONCURRENT_REQUESTS, 1);
        assert_eq!(MAX_RETRIES, 2);
    }
}
