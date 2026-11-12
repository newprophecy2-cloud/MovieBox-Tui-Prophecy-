use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

const DEFAULT_CONCURRENCY: usize = 2;
const DEFAULT_SPACING: Duration = Duration::from_millis(100);
const MAX_COOLDOWN: Duration = Duration::from_secs(300);
const MAX_5XX_RETRIES: usize = 2;

static GLOBAL_HOST_REGISTRY: std::sync::OnceLock<HostLimiterRegistry> = std::sync::OnceLock::new();

/// Process-wide host coordination shared by all provider clients.
pub fn global_host_registry() -> &'static HostLimiterRegistry {
    GLOBAL_HOST_REGISTRY.get_or_init(HostLimiterRegistry::default)
}

/// Acquire coordination for the actual destination host. Dropping the owned
/// permit is cancellation-safe and releases the concurrency slot.
pub async fn acquire_url(url: &str) -> Result<OwnedSemaphorePermit, String> {
    let parsed = reqwest::Url::parse(url).map_err(|_| "invalid request URL".to_string())?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "request URL has no host".to_string())?;
    global_host_registry()
        .for_host(host)
        .await
        .acquire()
        .await
        .map_err(|_| "request limiter wait timed out".to_string())
}

/// Record only sanitized host/status information and adaptive cooldown data.
pub async fn observe_url(
    url: &str,
    status: reqwest::StatusCode,
    headers: &reqwest::header::HeaderMap,
) {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return;
    };
    let Some(host) = parsed.host_str() else {
        return;
    };
    global_host_registry()
        .for_host(host)
        .await
        .record_response(status, headers)
        .await;
    log::debug!(
        "coordinated upstream response host={} status={}",
        host,
        status.as_u16()
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    SafeIdempotent,
    UnsafePost,
    Never,
}

pub fn retry_class(method: &reqwest::Method, status: reqwest::StatusCode) -> RetryClass {
    if *method == reqwest::Method::POST {
        return RetryClass::UnsafePost;
    }
    if status.is_server_error()
        && matches!(
            *method,
            reqwest::Method::GET
                | reqwest::Method::HEAD
                | reqwest::Method::OPTIONS
                | reqwest::Method::PUT
                | reqwest::Method::DELETE
        )
    {
        RetryClass::SafeIdempotent
    } else {
        RetryClass::Never
    }
}

pub fn bounded_5xx_retry_delay(attempt: usize) -> Option<Duration> {
    (attempt < MAX_5XX_RETRIES)
        .then(|| Duration::from_millis(250u64.saturating_mul(1u64 << attempt.min(4))))
}

#[derive(Clone)]
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    threshold: u32,
    open_for: Duration,
}

struct CircuitState {
    failures: u32,
    open_until: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, open_for: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState {
                failures: 0,
                open_until: None,
            })),
            threshold: threshold.max(1),
            open_for,
        }
    }

    pub async fn allow(&self) -> bool {
        let mut state = self.state.lock().await;
        if state.open_until.is_some_and(|until| until > Instant::now()) {
            return false;
        }
        if state.open_until.is_some() {
            state.open_until = None;
            state.failures = 0;
        }
        true
    }

    pub async fn record(&self, success: bool) {
        let mut state = self.state.lock().await;
        if success {
            state.failures = 0;
            state.open_until = None;
        } else {
            state.failures = state.failures.saturating_add(1);
            if state.failures >= self.threshold {
                state.open_until = Some(Instant::now() + self.open_for);
            }
        }
    }
}

#[derive(Clone)]
pub struct HostLimiter {
    state: Arc<Mutex<State>>,
    permits: Arc<Semaphore>,
    spacing: Duration,
    max_cooldown: Duration,
}

struct State {
    next_allowed: Instant,
    cooldown_until: Option<Instant>,
}

impl HostLimiter {
    pub fn new(max_concurrent: usize, min_spacing: Duration) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                next_allowed: Instant::now(),
                cooldown_until: None,
            })),
            permits: Arc::new(Semaphore::new(max_concurrent.max(1))),
            spacing: min_spacing,
            max_cooldown: MAX_COOLDOWN,
        }
    }

    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, tokio::time::error::Elapsed> {
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let cooldown = state.cooldown_until.unwrap_or(now);
                let ready = state.next_allowed.max(cooldown);
                state.next_allowed = ready + self.spacing;
                ready.saturating_duration_since(now)
            };
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match tokio::time::timeout(
                Duration::from_secs(60),
                self.permits.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(permit)) => return Ok(permit),
                Ok(Err(_)) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn cooldown(&self, duration: Duration) {
        let duration = duration.min(self.max_cooldown);
        let until = Instant::now() + duration;
        let mut state = self.state.lock().await;
        state.cooldown_until = Some(state.cooldown_until.unwrap_or(until).max(until));
        state.next_allowed = state.next_allowed.max(until);
    }

    pub async fn record_response(
        &self,
        status: reqwest::StatusCode,
        headers: &reqwest::header::HeaderMap,
    ) {
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            self.cooldown(retry_after(headers).unwrap_or(Duration::from_secs(5)))
                .await;
        } else if status.is_server_error() {
            self.cooldown(Duration::from_millis(250)).await;
        }
    }
}

pub fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    if let Some(value) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
    {
        if let Ok(seconds) = value.trim().parse::<u64>() {
            return Some(Duration::from_secs(seconds).min(MAX_COOLDOWN));
        }
    }
    let reset = headers
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some(Duration::from_secs(reset.saturating_sub(now)).min(MAX_COOLDOWN))
}

#[derive(Clone)]
pub struct HostLimiterRegistry {
    hosts: Arc<Mutex<HashMap<String, HostLimiter>>>,
    max_concurrent: usize,
    spacing: Duration,
}

impl Default for HostLimiterRegistry {
    fn default() -> Self {
        Self::new(DEFAULT_CONCURRENCY, DEFAULT_SPACING)
    }
}

impl HostLimiterRegistry {
    pub fn new(max_concurrent: usize, spacing: Duration) -> Self {
        Self {
            hosts: Arc::new(Mutex::new(HashMap::new())),
            max_concurrent,
            spacing,
        }
    }

    pub async fn for_host(&self, host: &str) -> HostLimiter {
        let mut hosts = self.hosts.lock().await;
        hosts
            .entry(host.to_ascii_lowercase())
            .or_insert_with(|| HostLimiter::new(self.max_concurrent, self.spacing))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

    #[tokio::test]
    async fn independent_hosts_do_not_share_cooldown() {
        let registry = HostLimiterRegistry::new(1, Duration::ZERO);
        let a = registry.for_host("a.example").await;
        let b = registry.for_host("b.example").await;
        a.cooldown(Duration::from_secs(1)).await;
        let started = Instant::now();
        let _permit = b.acquire().await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(200));
    }

    #[tokio::test]
    async fn semaphore_bounds_concurrency() {
        let limiter = HostLimiter::new(1, Duration::ZERO);
        let first = limiter.acquire().await.unwrap();
        let waiter = tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.acquire().await.unwrap() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .is_ok()
        );
    }

    #[test]
    fn parses_retry_after_and_reset() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("7"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(7)));
        headers.remove(RETRY_AFTER);
        headers.insert("x-ratelimit-reset", HeaderValue::from_static("1"));
        assert!(retry_after(&headers).is_some());
    }

    #[test]
    fn unsafe_posts_are_never_retried() {
        assert_eq!(
            retry_class(
                &reqwest::Method::POST,
                reqwest::StatusCode::INTERNAL_SERVER_ERROR
            ),
            RetryClass::UnsafePost
        );
        assert_eq!(
            retry_class(
                &reqwest::Method::GET,
                reqwest::StatusCode::INTERNAL_SERVER_ERROR
            ),
            RetryClass::SafeIdempotent
        );
        assert_eq!(bounded_5xx_retry_delay(2), None);
    }

    #[tokio::test]
    async fn circuit_breaker_opens_and_recovers() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(20));
        assert!(breaker.allow().await);
        breaker.record(false).await;
        breaker.record(false).await;
        assert!(!breaker.allow().await);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(breaker.allow().await);
    }

    #[tokio::test]
    async fn response_429_creates_shared_cooldown() {
        let limiter = HostLimiter::new(1, Duration::ZERO);
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
        limiter
            .record_response(reqwest::StatusCode::TOO_MANY_REQUESTS, &headers)
            .await;
        let started = Instant::now();
        let _ = limiter.acquire().await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(900));
    }
}
