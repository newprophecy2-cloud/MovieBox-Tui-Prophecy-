use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

const MAX_ROUTE_LABELS: usize = 128;
const MAX_PROVIDER_LABELS: usize = 32;
const MAX_HISTOGRAM_SAMPLES: usize = 4096;

#[derive(Debug, Default)]
struct Histogram {
    samples_ms: Vec<u64>,
}

impl Histogram {
    fn observe(&mut self, value: u64) {
        if self.samples_ms.len() == MAX_HISTOGRAM_SAMPLES {
            self.samples_ms.remove(0);
        }
        self.samples_ms.push(value);
    }

    fn percentile(&self, percentile: usize) -> Option<u64> {
        if self.samples_ms.is_empty() {
            return None;
        }
        let mut values = self.samples_ms.clone();
        values.sort_unstable();
        let index = ((values.len() - 1) * percentile / 100).min(values.len() - 1);
        values.get(index).copied()
    }
}

#[derive(Debug, Default)]
struct RouteMetrics {
    requests: u64,
    duration: Histogram,
}

#[derive(Debug, Default)]
struct ProviderMetrics {
    requests: u64,
    successes: u64,
    failures: u64,
    timeouts: u64,
    unavailable: u64,
    latency: Histogram,
    last_success_unix_ms: Option<u64>,
    last_failure_unix_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct CacheMetrics {
    hits: u64,
    misses: u64,
    insertions: u64,
    evictions: u64,
    expirations: u64,
    redis_hits: u64,
    redis_misses: u64,
    in_flight: u64,
    errors: u64,
}

#[derive(Debug, Default)]
struct State {
    requests: u64,
    errors: u64,
    timeouts: u64,
    status_codes: HashMap<u16, u64>,
    routes: HashMap<String, RouteMetrics>,
    caches: HashMap<String, CacheMetrics>,
    providers: HashMap<String, ProviderMetrics>,
}

static STATE: LazyLock<Mutex<State>> = LazyLock::new(|| Mutex::new(State::default()));

#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub requests: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub status_codes: HashMap<u16, u64>,
    pub routes: HashMap<String, RouteSnapshot>,
    pub caches: HashMap<String, CacheSnapshot>,
    pub providers: HashMap<String, ProviderSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteSnapshot {
    pub requests: u64,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub p99_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub redis_hits: u64,
    pub redis_misses: u64,
    pub in_flight: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderSnapshot {
    pub requests: u64,
    pub successes: u64,
    pub failures: u64,
    pub timeouts: u64,
    pub unavailable: u64,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub p99_ms: Option<u64>,
    pub last_success_unix_ms: Option<u64>,
    pub last_failure_unix_ms: Option<u64>,
}

pub fn record_request(route: &str, status: u16, duration_ms: u64, timed_out: bool) {
    let mut state = STATE.lock().expect("observability mutex poisoned");
    state.requests += 1;
    if timed_out {
        state.timeouts += 1;
    }
    if status != 0 {
        if status >= 400 {
            state.errors += 1;
        }
        *state.status_codes.entry(status).or_default() += 1;
    }
    if state.routes.len() < MAX_ROUTE_LABELS || state.routes.contains_key(route) {
        let route_metrics = state.routes.entry(route.to_string()).or_default();
        route_metrics.requests += 1;
        route_metrics.duration.observe(duration_ms);
    }
}

pub fn record_response(status: u16) {
    let mut state = STATE.lock().expect("observability mutex poisoned");
    if status >= 400 {
        state.errors += 1;
    }
    *state.status_codes.entry(status).or_default() += 1;
}

pub fn record_cache(cache: &str, event: &str) {
    let mut state = STATE.lock().expect("observability mutex poisoned");
    let metrics = state.caches.entry(cache.to_string()).or_default();
    match event {
        "hit" => metrics.hits += 1,
        "miss" => metrics.misses += 1,
        "insert" => metrics.insertions += 1,
        "eviction" => metrics.evictions += 1,
        "expiration" => metrics.expirations += 1,
        "redis_hit" => metrics.redis_hits += 1,
        "redis_miss" => metrics.redis_misses += 1,
        "in_flight" => metrics.in_flight += 1,
        "error" => metrics.errors += 1,
        _ => {}
    }
}

pub fn record_provider(provider: &str, outcome: &str, duration_ms: u64) {
    let mut state = STATE.lock().expect("observability mutex poisoned");
    if state.providers.len() >= MAX_PROVIDER_LABELS && !state.providers.contains_key(provider) {
        return;
    }
    let metrics = state.providers.entry(provider.to_string()).or_default();
    metrics.requests += 1;
    metrics.latency.observe(duration_ms);
    match outcome {
        "success" => {
            metrics.successes += 1;
            metrics.last_success_unix_ms = Some(now_ms());
        }
        "timeout" => metrics.timeouts += 1,
        "unavailable" => metrics.unavailable += 1,
        _ => metrics.failures += 1,
    }
    if outcome != "success" {
        metrics.last_failure_unix_ms = Some(now_ms());
    }
}

pub fn snapshot() -> Snapshot {
    let state = STATE.lock().expect("observability mutex poisoned");
    Snapshot {
        requests: state.requests,
        errors: state.errors,
        timeouts: state.timeouts,
        status_codes: state.status_codes.clone(),
        routes: state
            .routes
            .iter()
            .map(|(route, metrics)| {
                (
                    route.clone(),
                    RouteSnapshot {
                        requests: metrics.requests,
                        p50_ms: metrics.duration.percentile(50),
                        p95_ms: metrics.duration.percentile(95),
                        p99_ms: metrics.duration.percentile(99),
                    },
                )
            })
            .collect(),
        caches: state
            .caches
            .iter()
            .map(|(name, metrics)| {
                (
                    name.clone(),
                    CacheSnapshot {
                        hits: metrics.hits,
                        misses: metrics.misses,
                        insertions: metrics.insertions,
                        evictions: metrics.evictions,
                        expirations: metrics.expirations,
                        redis_hits: metrics.redis_hits,
                        redis_misses: metrics.redis_misses,
                        in_flight: metrics.in_flight,
                        errors: metrics.errors,
                    },
                )
            })
            .collect(),
        providers: state
            .providers
            .iter()
            .map(|(name, metrics)| {
                (
                    name.clone(),
                    ProviderSnapshot {
                        requests: metrics.requests,
                        successes: metrics.successes,
                        failures: metrics.failures,
                        timeouts: metrics.timeouts,
                        unavailable: metrics.unavailable,
                        p50_ms: metrics.latency.percentile(50),
                        p95_ms: metrics.latency.percentile(95),
                        p99_ms: metrics.latency.percentile(99),
                        last_success_unix_ms: metrics.last_success_unix_ms,
                        last_failure_unix_ms: metrics.last_failure_unix_ms,
                    },
                )
            })
            .collect(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_reports_percentiles_and_is_bounded() {
        let mut histogram = Histogram::default();
        for value in 0..(MAX_HISTOGRAM_SAMPLES as u64 + 100) {
            histogram.observe(value);
        }
        assert_eq!(histogram.samples_ms.len(), MAX_HISTOGRAM_SAMPLES);
        assert!(histogram.percentile(50).is_some());
        assert!(histogram.percentile(95).is_some());
        assert!(histogram.percentile(99).is_some());
    }
}
