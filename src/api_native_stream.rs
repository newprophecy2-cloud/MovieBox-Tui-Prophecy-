use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use base64::Engine as _;
use futures::StreamExt;
use quick_xml::{Reader, Writer, events::Event};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    net::TcpStream,
    sync::Semaphore,
};
use url::Url;

const TOKEN_TTL: Duration = Duration::from_secs(4 * 60 * 60);
const MAX_TOKENS: usize = 65_536;
const MAX_REDIRECTS: usize = 5;
const MAX_MANIFEST_BYTES: usize = 10 * 1024 * 1024;
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const RESPONSE_HEADER_TIMEOUT: Duration = Duration::from_secs(30);
// This is an operation bound, not a playback-duration policy. It is deliberately
// long enough for normal long-form playback while bounding abandoned work.
const DIRECT_STREAM_OPERATION_DEADLINE: Duration = Duration::from_secs(24 * 60 * 60);
const THROUGHPUT_WINDOW: Duration = Duration::from_secs(30);
const THROUGHPUT_BUCKET: Duration = Duration::from_secs(1);
const MIN_STREAM_THROUGHPUT_BYTES_PER_SEC: u64 = 1024;

#[derive(Clone)]
pub(crate) struct NativeStreamProxy {
    client: reqwest::Client,
    tokens: Arc<Mutex<TokenTable>>,
    slots: Arc<Semaphore>,
    max_segment_bytes: u64,
}

#[derive(Default)]
struct TokenTable {
    claims: HashMap<String, TokenClaims>,
    by_identity: HashMap<String, String>,
}

fn token_identity(
    source_id: &str,
    root_url: &Url,
    target_url: &Url,
    kind: TokenKind,
    headers: &[(String, String)],
    variables: &[TemplateVariable],
) -> String {
    let mut digest = Sha256::new();
    for value in [source_id, root_url.as_str(), target_url.as_str()] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    digest.update(format!("{kind:?}").as_bytes());
    for (name, value) in headers {
        digest.update((name.len() as u64).to_be_bytes());
        digest.update(name.as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    for variable in variables {
        digest.update(variable.name.as_bytes());
        digest.update([0]);
        digest.update(variable.expression.as_bytes());
        digest.update([0]);
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest.finalize())
}

fn local_token_path(source_id: &str, token: &str) -> String {
    let encoded_source =
        percent_encoding::utf8_percent_encode(source_id, percent_encoding::NON_ALPHANUMERIC);
    format!("/api/v1/sources/{encoded_source}/native/{token}")
}

fn local_direct_token_path(source_id: &str, token: &str) -> String {
    let encoded_source =
        percent_encoding::utf8_percent_encode(source_id, percent_encoding::NON_ALPHANUMERIC);
    format!("/api/v1/sources/{encoded_source}/direct/{token}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TokenKind {
    Resource,
    Base,
    Template,
    Direct,
}

#[derive(Clone)]
struct TokenClaims {
    source_id: String,
    root_url: Url,
    target_url: Url,
    kind: TokenKind,
    upstream_headers: Vec<(String, String)>,
    variables: Vec<TemplateVariable>,
    expires_at: Instant,
}

struct UpstreamRequestOptions<'a> {
    range: Option<&'a str>,
    head_only: bool,
    require_range: bool,
}

#[derive(Default)]
struct ThroughputTracker {
    buckets: VecDeque<(Instant, u64)>,
}

impl ThroughputTracker {
    fn record(&mut self, now: Instant, bytes: u64) -> bool {
        if let Some((bucket_started, bucket_bytes)) = self.buckets.back_mut()
            && now.saturating_duration_since(*bucket_started) < THROUGHPUT_BUCKET
        {
            *bucket_bytes = bucket_bytes.saturating_add(bytes);
        } else {
            self.buckets.push_back((now, bytes));
        }
        while self
            .buckets
            .front()
            .is_some_and(|(started, _)| now.saturating_duration_since(*started) > THROUGHPUT_WINDOW)
        {
            self.buckets.pop_front();
        }
        let Some((oldest, _)) = self.buckets.front() else {
            return false;
        };
        let elapsed = now.saturating_duration_since(*oldest);
        if elapsed < THROUGHPUT_WINDOW {
            return false;
        }
        let bytes = self.buckets.iter().map(|(_, bytes)| *bytes).sum::<u64>();
        bytes < MIN_STREAM_THROUGHPUT_BYTES_PER_SEC.saturating_mul(elapsed.as_secs())
    }
}

#[derive(Clone, Debug)]
struct TemplateVariable {
    name: String,
    expression: String,
}

impl NativeStreamProxy {
    pub(crate) fn new(max_segment_bytes: u64) -> Result<Self, String> {
        let client = crate::net::http_client_builder_base()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|error| format!("could not initialize native media client: {error}"))?;
        Ok(Self {
            client,
            tokens: Arc::new(Mutex::new(TokenTable::default())),
            slots: Arc::new(Semaphore::new(16)),
            max_segment_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_set_slot_capacity(&mut self, capacity: usize) {
        self.slots = Arc::new(Semaphore::new(capacity));
    }

    #[cfg(test)]
    pub(crate) fn test_try_acquire_slot(&self) -> bool {
        self.slots.clone().try_acquire_owned().is_ok()
    }

    pub(crate) fn playback_url(
        &self,
        source_id: &str,
        raw_url: &str,
        upstream_headers: &[(String, String)],
    ) -> Option<String> {
        let root_url = Url::parse(raw_url).ok()?;
        if !is_native_manifest_url(&root_url) {
            return None;
        }
        self.issue_url(
            source_id,
            &root_url,
            root_url.clone(),
            TokenKind::Resource,
            upstream_headers,
            Vec::new(),
        )
        .ok()
    }

    pub(crate) fn direct_playback_url(
        &self,
        source_id: &str,
        raw_url: &str,
        upstream_headers: &[(String, String)],
    ) -> Option<String> {
        let url = Url::parse(raw_url).ok()?;
        if !matches!(url.scheme(), "http" | "https") || is_native_manifest_url(&url) {
            return None;
        }
        let token_path = self
            .issue_url(
                source_id,
                &url,
                url.clone(),
                TokenKind::Direct,
                upstream_headers,
                Vec::new(),
            )
            .ok()?;
        let token = token_path.rsplit('/').next()?;
        Some(local_direct_token_path(source_id, token))
    }

    pub(crate) fn supports_url(&self, raw_url: &str) -> bool {
        Url::parse(raw_url).is_ok_and(|url| is_native_manifest_url(&url))
    }

    fn issue_url(
        &self,
        source_id: &str,
        root_url: &Url,
        target_url: Url,
        kind: TokenKind,
        upstream_headers: &[(String, String)],
        variables: Vec<TemplateVariable>,
    ) -> Result<String, String> {
        let mut tokens = self
            .tokens
            .lock()
            .map_err(|_| "native media token table is unavailable".to_string())?;
        let now = Instant::now();
        let expired = tokens
            .claims
            .iter()
            .filter_map(|(token, claims)| (claims.expires_at <= now).then_some(token.clone()))
            .collect::<Vec<_>>();
        for token in expired {
            tokens.claims.remove(&token);
        }
        let live = tokens.claims.keys().cloned().collect::<HashSet<_>>();
        tokens.by_identity.retain(|_, token| live.contains(token));
        let identity = token_identity(
            source_id,
            root_url,
            &target_url,
            kind,
            upstream_headers,
            &variables,
        );
        if let Some(token) = tokens.by_identity.get(&identity).cloned() {
            if let Some(claims) = tokens.claims.get_mut(&token) {
                claims.expires_at = now + TOKEN_TTL;
                return Ok(local_token_path(source_id, &token));
            }
        }
        if tokens.claims.len() >= MAX_TOKENS {
            return Err("native media token capacity is full".into());
        }
        let token = uuid::Uuid::new_v4().simple().to_string();
        tokens.claims.insert(
            token.clone(),
            TokenClaims {
                source_id: source_id.to_string(),
                root_url: root_url.clone(),
                target_url,
                kind,
                upstream_headers: upstream_headers.to_vec(),
                variables,
                expires_at: now + TOKEN_TTL,
            },
        );
        tokens.by_identity.insert(identity, token.clone());
        Ok(local_token_path(source_id, &token))
    }

    fn claims(&self, source_id: &str, token: &str) -> Option<TokenClaims> {
        if token.len() != 32 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let mut tokens = self.tokens.lock().ok()?;
        let now = Instant::now();
        let expired = tokens
            .claims
            .iter()
            .filter_map(|(token, claims)| (claims.expires_at <= now).then_some(token.clone()))
            .collect::<Vec<_>>();
        for expired_token in expired {
            tokens.claims.remove(&expired_token);
        }
        let live = tokens.claims.keys().cloned().collect::<HashSet<_>>();
        tokens.by_identity.retain(|_, token| live.contains(token));
        let claims = tokens.claims.get(token)?;
        (claims.source_id == source_id).then(|| claims.clone())
    }

    pub(crate) async fn serve(
        &self,
        stream: &mut TcpStream,
        source_id: &str,
        token: &str,
        suffix: &[String],
        query: &str,
        request_headers: &HashMap<String, String>,
        head_only: bool,
    ) -> io::Result<()> {
        let Some(claims) = self.claims(source_id, token) else {
            return write_error(
                stream,
                404,
                "The native playback reference is invalid or expired.",
            )
            .await;
        };
        let target = match resolve_claim_target(&claims, suffix, query) {
            Ok(target) => target,
            Err(message) => return write_error(stream, 400, message).await,
        };
        let _permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return write_error(stream, 503, "Native media capacity is busy.").await,
        };

        let range = match request_headers.get("range") {
            Some(value) if valid_single_byte_range(value) => Some(value.as_str()),
            Some(_) => {
                return write_error(stream, 400, "Only valid single byte ranges are supported.")
                    .await;
            }
            None => None,
        };
        let response = match self
            .send_upstream(
                &target,
                &claims.root_url,
                &claims.upstream_headers,
                request_headers,
                UpstreamRequestOptions {
                    range,
                    head_only,
                    require_range: true,
                },
                Instant::now() + RESPONSE_HEADER_TIMEOUT,
            )
            .await
        {
            Ok(response) => response,
            Err((status, message)) => return write_error(stream, status, message).await,
        };
        let final_url = response.url().clone();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string();
        let status = response.status();
        let expected_range_bytes = range.and_then(|range| {
            response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|content_range| {
                    expected_range_length(range, content_range, response.content_length())
                })
        });
        let is_manifest = is_manifest_response(&final_url, &content_type)
            || is_manifest_response(&claims.target_url, &content_type);
        let manifest_is_hls =
            is_hls(&final_url, &content_type) || is_hls(&claims.target_url, &content_type);
        if is_manifest && range.is_some() {
            return write_error(
                stream,
                400,
                "Byte ranges are not supported for rewritten manifests.",
            )
            .await;
        }
        if is_manifest && status.is_success() && !head_only {
            let body = match read_limited(response, MAX_MANIFEST_BYTES).await {
                Ok(body) => body,
                Err(message) => return write_error(stream, 502, message).await,
            };
            let manifest = match String::from_utf8(body) {
                Ok(manifest) => manifest,
                Err(_) => {
                    return write_error(stream, 502, "The upstream manifest is not UTF-8.").await;
                }
            };
            let rewritten = if manifest_is_hls {
                rewrite_hls(
                    &manifest,
                    &final_url,
                    source_id,
                    &claims.root_url,
                    &claims.upstream_headers,
                    self,
                )
            } else {
                rewrite_dash(
                    &manifest,
                    &final_url,
                    source_id,
                    &claims.root_url,
                    &claims.upstream_headers,
                    self,
                )
            };
            let rewritten = match rewritten {
                Ok(value) => value,
                Err(_) => {
                    return write_error(
                        stream,
                        502,
                        "The upstream manifest could not be safely rewritten.",
                    )
                    .await;
                }
            };
            let media_type = if manifest_is_hls {
                "application/vnd.apple.mpegurl"
            } else {
                "application/dash+xml"
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {media_type}\r\nContent-Length: {}\r\nCache-Control: no-cache\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
                rewritten.len()
            );
            stream.write_all(headers.as_bytes()).await?;
            stream.write_all(rewritten.as_bytes()).await?;
            stream.flush().await?;
            return Ok(());
        }
        self.relay_response(stream, response, head_only, expected_range_bytes)
            .await
    }

    pub(crate) async fn serve_direct(
        &self,
        stream: &mut TcpStream,
        source_id: &str,
        token: &str,
        request_headers: &HashMap<String, String>,
        head_only: bool,
    ) -> io::Result<()> {
        let Some(claims) = self
            .claims(source_id, token)
            .filter(|claims| claims.kind == TokenKind::Direct)
        else {
            return write_error(
                stream,
                404,
                "The direct playback reference is invalid or expired.",
            )
            .await;
        };
        let _permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return write_error(stream, 503, "Direct media capacity is busy.").await,
        };
        let deadline = Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE;
        let range = match request_headers.get("range") {
            Some(value) if valid_single_byte_range(value) => Some(value.as_str()),
            Some(_) => {
                return write_error(stream, 400, "Only valid single byte ranges are supported.")
                    .await;
            }
            None => None,
        };
        let upstream = self.send_upstream(
            &claims.target_url,
            &claims.root_url,
            &claims.upstream_headers,
            request_headers,
            UpstreamRequestOptions {
                range,
                head_only,
                require_range: false,
            },
            deadline,
        );
        let response = match tokio::select! {
            response = upstream => response,
            disconnect = wait_for_peer_disconnect(stream) => {
                match disconnect {
                    Ok(()) => log::debug!("direct_stream_cancelled reason=downstream_disconnect phase=upstream"),
                    Err(error) => log::debug!("direct_stream_cancelled reason=downstream_probe_error phase=upstream error={error}"),
                }
                return Ok(());
            }
        } {
            Ok(response) => response,
            Err((status, message)) => return write_error(stream, status, message).await,
        };
        self.relay_direct_response(stream, response, head_only, range, deadline)
            .await
    }

    async fn send_upstream(
        &self,
        initial_url: &Url,
        root_url: &Url,
        provider_headers: &[(String, String)],
        request_headers: &HashMap<String, String>,
        options: UpstreamRequestOptions<'_>,
        deadline: Instant,
    ) -> Result<reqwest::Response, (u16, &'static str)> {
        let range = options.range;
        let head_only = options.head_only;
        let require_range = options.require_range;
        let mut current = initial_url.clone();
        for redirect_count in 0..=MAX_REDIRECTS {
            let validated = crate::net::validate_public_url(current.as_str(), true)
                .await
                .map_err(|_| (403, "The media destination is not a permitted public URL."))?;
            let mut request = if head_only {
                self.client.head(validated.clone())
            } else {
                self.client.get(validated.clone())
            };
            let same_origin = validated.origin() == root_url.origin();
            for (name, value) in provider_headers {
                if !safe_provider_header(name) {
                    continue;
                }
                if same_origin {
                    request = request.header(name.as_str(), value.as_str());
                } else if let Some(value) = cross_origin_header_value(name, value) {
                    request = request.header(name.as_str(), value);
                }
            }
            if !provider_headers
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
                && let Some(user_agent) = request_headers.get("user-agent")
            {
                request = request.header(reqwest::header::USER_AGENT, user_agent);
            }
            if let Some(range) = range {
                request = request.header(reqwest::header::RANGE, range);
            }
            let header_deadline = (Instant::now() + RESPONSE_HEADER_TIMEOUT).min(deadline);
            let response = tokio::time::timeout_at(header_deadline.into(), request.send())
                .await
                .map_err(|_| (504, "The upstream media response timed out."))?
                .map_err(|_| (502, "The upstream media request failed."))?;
            if !response.status().is_redirection() {
                if let Some(range) = range {
                    if require_range
                        && response.status().is_success()
                        && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
                    {
                        return Err((
                            502,
                            "The upstream server did not honor the requested byte range.",
                        ));
                    }
                    if response.status() == reqwest::StatusCode::PARTIAL_CONTENT {
                        let content_range = response
                            .headers()
                            .get(reqwest::header::CONTENT_RANGE)
                            .and_then(|value| value.to_str().ok());
                        if content_range
                            .and_then(|content_range| {
                                expected_range_length(
                                    range,
                                    content_range,
                                    response.content_length(),
                                )
                            })
                            .is_none()
                        {
                            return Err((
                                502,
                                "The upstream server returned an invalid byte range.",
                            ));
                        }
                    }
                }
                return Ok(response);
            }
            if redirect_count == MAX_REDIRECTS {
                return Err((502, "The upstream media redirect limit was exceeded."));
            }
            let Some(location) = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
            else {
                return Err((502, "The upstream media redirect omitted its destination."));
            };
            current = validated
                .join(location)
                .map_err(|_| (502, "The upstream media redirect was invalid."))?;
        }
        Err((502, "The upstream media redirect limit was exceeded."))
    }

    async fn relay_response(
        &self,
        stream: &mut TcpStream,
        response: reqwest::Response,
        head_only: bool,
        expected_range_bytes: Option<u64>,
    ) -> io::Result<()> {
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream");
        let content_length = response.content_length();
        if status.is_success() && content_length.is_some_and(|size| size > self.max_segment_bytes) {
            return write_error(
                stream,
                413,
                "The media segment exceeds the configured stream limit.",
            )
            .await;
        }
        if !status.is_success()
            && content_length.is_some_and(|size| size > MAX_ERROR_BODY_BYTES as u64)
        {
            return write_error(
                stream,
                502,
                "The upstream media server returned an oversized error.",
            )
            .await;
        }
        let mut header = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Upstream Error"),
            content_type
        );
        for (name, value) in response.headers() {
            if matches!(
                name.as_str(),
                "content-length"
                    | "content-range"
                    | "accept-ranges"
                    | "etag"
                    | "last-modified"
                    | "cache-control"
            ) && let Ok(value) = value.to_str()
            {
                header.push_str(name.as_str());
                header.push_str(": ");
                header.push_str(value);
                header.push_str("\r\n");
            }
        }
        if let Some(length) = expected_range_bytes
            && !response
                .headers()
                .contains_key(reqwest::header::CONTENT_LENGTH)
        {
            header.push_str(&format!("Content-Length: {length}\r\n"));
        }
        header.push_str("\r\n");
        stream.write_all(header.as_bytes()).await?;
        stream.flush().await?;
        if head_only {
            return Ok(());
        }
        let expected_body_bytes = expected_range_bytes.or(content_length);
        let mut total = 0u64;
        let mut body = response.bytes_stream();
        loop {
            let next = tokio::time::timeout(STREAM_IDLE_TIMEOUT, body.next()).await;
            let chunk = match next {
                Err(_) => {
                    log::warn!(
                        "native_stream_failure reason=idle_timeout status={} received_bytes={} expected_bytes={:?}",
                        status.as_u16(),
                        total,
                        expected_body_bytes
                    );
                    return Ok(());
                }
                Ok(None) => {
                    if expected_body_bytes.is_some_and(|expected| expected != total) {
                        log::warn!(
                            "native_stream_failure reason=incomplete_body status={} received_bytes={} expected_bytes={:?}",
                            status.as_u16(),
                            total,
                            expected_body_bytes
                        );
                    }
                    break;
                }
                Ok(Some(Err(_))) => {
                    log::warn!(
                        "native_stream_failure reason=upstream_body_read status={} received_bytes={} expected_bytes={:?}",
                        status.as_u16(),
                        total,
                        expected_body_bytes
                    );
                    return Ok(());
                }
                Ok(Some(Ok(chunk))) => chunk,
            };
            total = total.saturating_add(chunk.len() as u64);
            if let Some(expected) = expected_range_bytes
                && total > expected
            {
                let allowed = chunk.len() as u64 - (total - expected);
                if allowed > 0 {
                    stream.write_all(&chunk[..allowed as usize]).await?;
                    stream.flush().await?;
                }
                return Ok(());
            }
            if status.is_success() && total > self.max_segment_bytes {
                return Ok(());
            }
            if !status.is_success() && total > MAX_ERROR_BODY_BYTES as u64 {
                return Ok(());
            }
            stream.write_all(&chunk).await?;
            stream.flush().await?;
        }
        Ok(())
    }

    async fn relay_direct_response(
        &self,
        stream: &mut TcpStream,
        response: reqwest::Response,
        head_only: bool,
        request_range: Option<&str>,
        deadline: Instant,
    ) -> io::Result<()> {
        let status = response.status();
        if !(status.is_success() || status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE) {
            let error_status = if status.is_client_error() {
                status.as_u16()
            } else {
                502
            };
            return write_error(
                stream,
                error_status,
                "The upstream media server rejected the request.",
            )
            .await;
        }
        if !status.is_success()
            && response
                .content_length()
                .is_some_and(|length| length > MAX_ERROR_BODY_BYTES as u64)
        {
            return write_error(
                stream,
                502,
                "The upstream media server returned an oversized error.",
            )
            .await;
        }
        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            let valid_range = request_range.is_some_and(|request_range| {
                response
                    .headers()
                    .get(reqwest::header::CONTENT_RANGE)
                    .and_then(|value| value.to_str().ok())
                    .and_then(unsatisfied_range_total)
                    .is_some_and(|total| request_range_is_unsatisfiable(request_range, total))
            });
            if !valid_range {
                return write_error(
                    stream,
                    502,
                    "The upstream server returned an invalid unsatisfied range.",
                )
                .await;
            }
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("application/octet-stream");
        let content_length = response.content_length();
        let expected_range_bytes = if status == reqwest::StatusCode::PARTIAL_CONTENT {
            let Some(request_range) = request_range else {
                return write_error(
                    stream,
                    502,
                    "The upstream server returned an unsolicited byte range.",
                )
                .await;
            };
            let expected = response
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| expected_range_length(request_range, value, content_length));
            let Some(expected) = expected else {
                return write_error(
                    stream,
                    502,
                    "The upstream server returned an invalid byte range.",
                )
                .await;
            };
            Some(expected)
        } else {
            None
        };
        let mut header = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or("Upstream Error"),
            content_type
        );
        for (name, value) in response.headers() {
            if matches!(
                name.as_str(),
                "content-length" | "accept-ranges" | "etag" | "last-modified" | "cache-control"
            ) && let Ok(value) = value.to_str()
            {
                header.push_str(name.as_str());
                header.push_str(": ");
                header.push_str(value);
                header.push_str("\r\n");
            }
            if name == reqwest::header::CONTENT_RANGE
                && matches!(
                    status,
                    reqwest::StatusCode::PARTIAL_CONTENT
                        | reqwest::StatusCode::RANGE_NOT_SATISFIABLE
                )
                && let Ok(value) = value.to_str()
            {
                header.push_str("Content-Range: ");
                header.push_str(value);
                header.push_str("\r\n");
            }
            if name == reqwest::header::CONTENT_DISPOSITION
                && let Ok(value) = value.to_str()
            {
                header.push_str("Content-Disposition: ");
                header.push_str(value);
                header.push_str("\r\n");
            }
        }
        if let Some(length) = expected_range_bytes
            && !response
                .headers()
                .contains_key(reqwest::header::CONTENT_LENGTH)
        {
            header.push_str(&format!("Content-Length: {length}\r\n"));
        }
        header.push_str("\r\n");
        stream.write_all(header.as_bytes()).await?;
        stream.flush().await?;
        if head_only {
            return Ok(());
        }
        let expected_body_bytes = expected_range_bytes.or(content_length);
        let mut total = 0u64;
        let mut throughput = ThroughputTracker::default();
        let mut body = response.bytes_stream();
        loop {
            let next = tokio::select! {
                next = tokio::time::timeout_at(
                    deadline.into(),
                    tokio::time::timeout(STREAM_IDLE_TIMEOUT, body.next()),
                ) => next,
                disconnect = wait_for_peer_disconnect(stream) => {
                    match disconnect {
                        Ok(()) => log::debug!("direct_stream_cancelled reason=downstream_disconnect phase=body"),
                        Err(error) => log::debug!("direct_stream_cancelled reason=downstream_probe_error phase=body error={error}"),
                    }
                    return Ok(());
                }
            };
            let chunk = match next {
                Err(_) => {
                    log::warn!(
                        "direct_stream_failure reason=overall_deadline status={} received_bytes={} expected_bytes={:?}",
                        status.as_u16(),
                        total,
                        expected_body_bytes
                    );
                    return Ok(());
                }
                Ok(Err(_)) => {
                    log::warn!(
                        "direct_stream_failure reason=idle_timeout status={} received_bytes={} expected_bytes={:?}",
                        status.as_u16(),
                        total,
                        expected_body_bytes
                    );
                    return Ok(());
                }
                Ok(Ok(None)) => {
                    if expected_body_bytes.is_some_and(|expected| expected != total) {
                        log::warn!(
                            "direct_stream_failure reason=incomplete_body status={} received_bytes={} expected_bytes={:?}",
                            status.as_u16(),
                            total,
                            expected_body_bytes
                        );
                    }
                    return Ok(());
                }
                Ok(Ok(Some(Err(_)))) => {
                    log::warn!(
                        "direct_stream_failure reason=upstream_body_read status={} received_bytes={} expected_bytes={:?}",
                        status.as_u16(),
                        total,
                        expected_body_bytes
                    );
                    return Ok(());
                }
                Ok(Ok(Some(Ok(chunk)))) => chunk,
            };
            total = total.saturating_add(chunk.len() as u64);
            if throughput.record(Instant::now(), chunk.len() as u64) {
                log::warn!(
                    "direct_stream_failure reason=sustained_low_throughput status={} received_bytes={} expected_bytes={:?}",
                    status.as_u16(),
                    total,
                    expected_body_bytes
                );
                return Ok(());
            }
            if !status.is_success() && total > MAX_ERROR_BODY_BYTES as u64 {
                return Ok(());
            }
            if let Some(expected) = expected_range_bytes
                && total > expected
            {
                let allowed = chunk.len() as u64 - (total - expected);
                if allowed > 0 {
                    stream.write_all(&chunk[..allowed as usize]).await?;
                    stream.flush().await?;
                }
                return Ok(());
            }
            stream.write_all(&chunk).await?;
            stream.flush().await?;
        }
    }
}

async fn wait_for_peer_disconnect(stream: &TcpStream) -> io::Result<()> {
    loop {
        stream.readable().await?;
        let mut probe = [0u8; 1];
        match stream.try_read(&mut probe) {
            Ok(0) => return Ok(()),
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
}

fn unsatisfied_range_total(value: &str) -> Option<u64> {
    let mut parts = value.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let range = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    range.strip_prefix("*/")?.parse::<u64>().ok()
}

fn request_range_is_unsatisfiable(request_range: &str, total: u64) -> bool {
    if !valid_single_byte_range(request_range) {
        return false;
    }
    let Some((_, requested)) = request_range.trim().split_once('=') else {
        return false;
    };
    let Some((start, _)) = requested.split_once('-') else {
        return false;
    };
    if start.is_empty() {
        total == 0
    } else {
        start.parse::<u64>().is_ok_and(|start| start >= total)
    }
}

fn safe_provider_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    !matches!(
        name.as_str(),
        "host"
            | "connection"
            | "content-length"
            | "transfer-encoding"
            | "range"
            | "accept-encoding"
            | "proxy-authorization"
    ) && reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_ok()
}

fn sanitized_referer_origin(value: &str) -> Option<String> {
    let referer = Url::parse(value).ok()?;
    if !matches!(referer.scheme(), "http" | "https")
        || referer.host_str().is_none()
        || !referer.username().is_empty()
        || referer.password().is_some()
    {
        return None;
    }
    Some(format!("{}/", referer.origin().ascii_serialization()))
}

fn cross_origin_header_value(name: &str, value: &str) -> Option<String> {
    if name.eq_ignore_ascii_case("user-agent")
        || matches!(
            name.to_ascii_lowercase().as_str(),
            "accept" | "accept-language"
        )
    {
        return Some(value.to_string());
    }
    if name.eq_ignore_ascii_case("referer") || name.eq_ignore_ascii_case("origin") {
        return sanitized_referer_origin(value);
    }
    None
}

fn resolve_claim_target(
    claims: &TokenClaims,
    suffix: &[String],
    query: &str,
) -> Result<Url, &'static str> {
    match claims.kind {
        TokenKind::Resource if suffix.is_empty() && query.is_empty() => {
            Ok(claims.target_url.clone())
        }
        TokenKind::Base if suffix.is_empty() && query.is_empty() => Ok(claims.target_url.clone()),
        TokenKind::Base if !suffix.is_empty() && query.is_empty() => {
            if suffix.iter().any(|part| {
                let decoded = percent_encoding::percent_decode_str(part).decode_utf8_lossy();
                decoded == "."
                    || decoded == ".."
                    || decoded.contains(['/', '\\'])
                    || decoded.contains('%')
            }) {
                return Err("The requested relative media path is invalid.");
            }
            let relative = suffix.join("/");
            claims
                .target_url
                .join(&relative)
                .map_err(|_| "The requested relative media path is invalid.")
        }
        TokenKind::Template if suffix.is_empty() => {
            let pairs = url::form_urlencoded::parse(query.as_bytes()).collect::<HashMap<_, _>>();
            if pairs.len() != claims.variables.len() {
                return Err("The media template parameters are invalid.");
            }
            let mut target = claims.target_url.as_str().to_string();
            for (index, variable) in claims.variables.iter().enumerate() {
                let key = format!("v{index}");
                let value = pairs
                    .get(key.as_str())
                    .ok_or("The media template parameters are incomplete.")?
                    .as_ref();
                if !valid_template_value(&variable.name, value) {
                    return Err("The media template parameter is invalid.");
                }
                target = target.replace(&variable.expression, value);
            }
            if target.contains('$') {
                return Err("The media template is incomplete.");
            }
            Url::parse(&target).map_err(|_| "The media template URL is invalid.")
        }
        _ => Err("The native media reference does not match its signed resource."),
    }
}

fn valid_template_value(name: &str, value: &str) -> bool {
    if value.is_empty() || value.len() > 256 || value.contains(['/', '\\', '?', '#', '$']) {
        return false;
    }
    match name {
        "Number" | "Time" | "Bandwidth" => value.bytes().all(|byte| byte.is_ascii_digit()),
        "RepresentationID" => value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._~-".contains(&byte)),
        _ => false,
    }
}

fn valid_single_byte_range(value: &str) -> bool {
    let Some((unit, range)) = value.trim().split_once('=') else {
        return false;
    };
    if !unit.eq_ignore_ascii_case("bytes") || range.contains(',') {
        return false;
    }
    let Some((start, end)) = range.split_once('-') else {
        return false;
    };
    if start.is_empty() {
        return end.parse::<u64>().is_ok_and(|size| size > 0);
    }
    let Ok(start) = start.parse::<u64>() else {
        return false;
    };
    end.is_empty() || end.parse::<u64>().is_ok_and(|end| end >= start)
}

fn expected_range_length(
    request_range: &str,
    content_range: &str,
    content_length: Option<u64>,
) -> Option<u64> {
    if !valid_single_byte_range(request_range) {
        return None;
    }
    let (_, requested) = request_range.trim().split_once('=')?;
    let (requested_start, requested_end) = requested.split_once('-')?;
    let mut content_range_parts = content_range.split_whitespace();
    if !content_range_parts.next()?.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let content_range = content_range_parts.next()?;
    if content_range_parts.next().is_some() {
        return None;
    }
    let (interval, total) = content_range.split_once('/')?;
    let (actual_start, actual_end) = interval.split_once('-')?;
    let total = total.parse::<u64>().ok()?;
    let actual_start = actual_start.parse::<u64>().ok()?;
    let actual_end = actual_end.parse::<u64>().ok()?;
    if total == 0 || actual_start > actual_end || actual_end >= total {
        return None;
    }

    let (expected_start, expected_end) = if requested_start.is_empty() {
        let suffix_length = requested_end.parse::<u64>().ok()?;
        (total.saturating_sub(suffix_length), total - 1)
    } else {
        let start = requested_start.parse::<u64>().ok()?;
        if start >= total {
            return None;
        }
        let requested_end = if requested_end.is_empty() {
            total - 1
        } else {
            requested_end.parse::<u64>().ok()?.min(total - 1)
        };
        (start, requested_end)
    };
    if actual_start != expected_start || actual_end != expected_end {
        return None;
    }
    let length = actual_end.checked_sub(actual_start)?.checked_add(1)?;
    if content_length.is_some_and(|actual| actual != length) {
        return None;
    }
    Some(length)
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, &'static str> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err("The upstream manifest exceeds the configured size limit.");
    }
    let mut bytes = Vec::new();
    let mut body = response.bytes_stream();
    loop {
        let next = tokio::time::timeout(STREAM_IDLE_TIMEOUT, body.next())
            .await
            .map_err(|_| "The upstream manifest timed out.")?;
        let Some(chunk) = next else { break };
        let chunk = chunk.map_err(|_| "The upstream manifest could not be read.")?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err("The upstream manifest exceeds the configured size limit.");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn is_native_manifest_url(url: &Url) -> bool {
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".m3u8") || path.ends_with(".m3u") || path.ends_with(".mpd")
}

fn is_hls(url: &Url, content_type: &str) -> bool {
    let content_type = content_type.to_ascii_lowercase();
    let path = url.path().to_ascii_lowercase();
    path.ends_with(".m3u8")
        || path.ends_with(".m3u")
        || content_type.contains("mpegurl")
        || content_type.contains("vnd.apple.mpegurl")
}

fn is_manifest_response(url: &Url, content_type: &str) -> bool {
    is_native_manifest_url(url)
        || content_type.to_ascii_lowercase().contains("dash+xml")
        || content_type.to_ascii_lowercase().contains("mpegurl")
}

fn resolve_reference(base: &Url, raw: &str) -> Result<Url, String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("empty media reference".into());
    }
    let joined = base
        .join(raw)
        .map_err(|_| "invalid media reference".to_string())?;
    if !matches!(joined.scheme(), "http" | "https") || joined.host_str().is_none() {
        return Err("media reference is not HTTP(S)".into());
    }
    Ok(joined)
}

fn proxy_resource(
    proxy: &NativeStreamProxy,
    source_id: &str,
    root_url: &Url,
    upstream_headers: &[(String, String)],
    target: Url,
) -> Result<String, String> {
    proxy.issue_url(
        source_id,
        root_url,
        target,
        TokenKind::Resource,
        upstream_headers,
        Vec::new(),
    )
}

fn rewrite_hls(
    manifest: &str,
    manifest_url: &Url,
    source_id: &str,
    root_url: &Url,
    upstream_headers: &[(String, String)],
    proxy: &NativeStreamProxy,
) -> Result<String, String> {
    if !manifest
        .lines()
        .next()
        .is_some_and(|line| line.trim_start_matches('\u{feff}').trim() == "#EXTM3U")
    {
        return Err("invalid HLS playlist header".into());
    }
    let mut output = String::with_capacity(manifest.len().saturating_add(256));
    for line in manifest.split_inclusive('\n') {
        let trimmed = line.trim_end_matches(['\r', '\n']);
        let ending = &line[trimmed.len()..];
        if trimmed.trim().is_empty() {
            output.push_str(line);
        } else if trimmed.trim_start().starts_with('#') {
            output.push_str(&rewrite_hls_uri_attributes(
                trimmed,
                manifest_url,
                source_id,
                root_url,
                upstream_headers,
                proxy,
            )?);
            output.push_str(ending);
        } else {
            let rewritten = resolve_reference(manifest_url, trimmed).and_then(|url| {
                proxy_resource(proxy, source_id, root_url, upstream_headers, url)
            })?;
            let leading = trimmed.len() - trimmed.trim_start().len();
            let trailing = trimmed.len() - trimmed.trim_end().len();
            output.push_str(&trimmed[..leading]);
            output.push_str(&rewritten);
            if trailing > 0 {
                output.push_str(&trimmed[trimmed.len() - trailing..]);
            }
            output.push_str(ending);
        }
    }
    Ok(output)
}

fn rewrite_hls_uri_attributes(
    line: &str,
    base: &Url,
    source_id: &str,
    root_url: &Url,
    upstream_headers: &[(String, String)],
    proxy: &NativeStreamProxy,
) -> Result<String, String> {
    let mut result = String::with_capacity(line.len() + 128);
    let mut cursor = 0;
    while let Some(relative) = line[cursor..].find("URI=") {
        let start = cursor + relative;
        result.push_str(&line[cursor..start + 4]);
        let value_start = start + 4;
        if line.as_bytes().get(value_start) != Some(&b'"') {
            cursor = value_start;
            continue;
        }
        let content_start = value_start + 1;
        let Some(end_rel) = line[content_start..].find('"') else {
            return Err("unterminated HLS URI attribute".into());
        };
        let content_end = content_start + end_rel;
        let target = resolve_reference(base, &line[content_start..content_end])?;
        let local = proxy_resource(proxy, source_id, root_url, upstream_headers, target)?;
        result.push('"');
        result.push_str(&local);
        result.push('"');
        cursor = content_end + 1;
    }
    result.push_str(&line[cursor..]);
    Ok(result)
}

fn rewrite_dash(
    manifest: &str,
    manifest_url: &Url,
    source_id: &str,
    root_url: &Url,
    upstream_headers: &[(String, String)],
    proxy: &NativeStreamProxy,
) -> Result<String, String> {
    let mut reader = Reader::from_str(manifest);
    reader.config_mut().trim_text(false);
    let mut writer = Writer::new(Vec::with_capacity(manifest.len() + 512));
    let mut bases = vec![manifest_url.clone()];
    let mut names: Vec<String> = Vec::new();
    let mut base_updates: Vec<Option<Url>> = Vec::new();

    loop {
        match reader
            .read_event()
            .map_err(|_| "invalid DASH XML".to_string())?
        {
            Event::Start(event) => {
                let name = String::from_utf8_lossy(event.local_name().as_ref()).into_owned();
                let mut base = bases.last().cloned().ok_or("missing MPD base URL")?;
                let attributes = event
                    .attributes()
                    .with_checks(true)
                    .map(|attribute| {
                        attribute.map_err(|_| "invalid DASH XML attribute".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for attribute in &attributes {
                    let key = String::from_utf8_lossy(attribute.key.as_ref());
                    if key == "xml:base" {
                        let value = attribute
                            .decode_and_unescape_value(reader.decoder())
                            .map_err(|_| "invalid DASH base URL".to_string())?;
                        base = resolve_reference(&base, &value)?;
                    }
                }
                let mut rewritten = quick_xml::events::BytesStart::new(
                    String::from_utf8_lossy(event.name().as_ref()).into_owned(),
                );
                for attribute in attributes {
                    let key = String::from_utf8_lossy(attribute.key.as_ref()).into_owned();
                    let local_name = key.rsplit(':').next().unwrap_or(&key);
                    let value = attribute
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|_| "invalid DASH XML attribute value".to_string())?;
                    let value = if key == "xml:base" {
                        let local = proxy.issue_url(
                            source_id,
                            root_url,
                            base.clone(),
                            TokenKind::Base,
                            upstream_headers,
                            Vec::new(),
                        )?;
                        format!("{local}/")
                    } else if is_dash_uri_attribute(local_name)
                        || (name == "UTCTiming" && local_name == "value")
                    {
                        let target = resolve_reference(&base, &value)?;
                        rewrite_dash_uri(&target, source_id, root_url, upstream_headers, proxy)?
                    } else {
                        value.into_owned()
                    };
                    rewritten.push_attribute((key.as_str(), value.as_str()));
                }
                writer
                    .write_event(Event::Start(rewritten))
                    .map_err(|_| "could not serialize DASH manifest".to_string())?;
                bases.push(base);
                names.push(name);
                base_updates.push(None);
            }
            Event::Empty(event) => {
                let name = String::from_utf8_lossy(event.local_name().as_ref()).into_owned();
                let mut base = bases.last().cloned().ok_or("missing MPD base URL")?;
                let attributes = event
                    .attributes()
                    .with_checks(true)
                    .map(|attribute| {
                        attribute.map_err(|_| "invalid DASH XML attribute".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                for attribute in &attributes {
                    if attribute.key.as_ref() == b"xml:base" {
                        let value = attribute
                            .decode_and_unescape_value(reader.decoder())
                            .map_err(|_| "invalid DASH base URL".to_string())?;
                        base = resolve_reference(&base, &value)?;
                    }
                }
                let mut rewritten = quick_xml::events::BytesStart::new(
                    String::from_utf8_lossy(event.name().as_ref()).into_owned(),
                );
                for attribute in attributes {
                    let key = String::from_utf8_lossy(attribute.key.as_ref()).into_owned();
                    let local_name = key.rsplit(':').next().unwrap_or(&key);
                    let value = attribute
                        .decode_and_unescape_value(reader.decoder())
                        .map_err(|_| "invalid DASH XML attribute value".to_string())?;
                    let value = if key == "xml:base" {
                        let local = proxy.issue_url(
                            source_id,
                            root_url,
                            base.clone(),
                            TokenKind::Base,
                            upstream_headers,
                            Vec::new(),
                        )?;
                        format!("{local}/")
                    } else if is_dash_uri_attribute(local_name)
                        || (name == "UTCTiming" && local_name == "value")
                    {
                        let target = resolve_reference(&base, &value)?;
                        rewrite_dash_uri(&target, source_id, root_url, upstream_headers, proxy)?
                    } else {
                        value.into_owned()
                    };
                    rewritten.push_attribute((key.as_str(), value.as_str()));
                }
                writer
                    .write_event(Event::Empty(rewritten))
                    .map_err(|_| "could not serialize DASH manifest".to_string())?;
                let _ = name;
            }
            Event::Text(text) if names.last().is_some_and(|name| name == "BaseURL") => {
                let decoded = text
                    .decode()
                    .map_err(|_| "invalid DASH BaseURL".to_string())?;
                let unescaped = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| "invalid DASH BaseURL".to_string())?;
                let trimmed = unescaped.trim();
                if trimmed.is_empty() {
                    writer
                        .write_event(Event::Text(text))
                        .map_err(|_| "could not serialize DASH manifest".to_string())?;
                    continue;
                }
                let parent_base = bases.last().cloned().ok_or("missing MPD base URL")?;
                let resolved = resolve_reference(&parent_base, trimmed)?;
                let local = proxy.issue_url(
                    source_id,
                    root_url,
                    resolved.clone(),
                    TokenKind::Base,
                    upstream_headers,
                    Vec::new(),
                )?;
                let leading = unescaped.len() - unescaped.trim_start().len();
                let trailing = unescaped.len() - unescaped.trim_end().len();
                let replacement = format!(
                    "{}{}/{}",
                    &unescaped[..leading],
                    local,
                    &unescaped[unescaped.len() - trailing..]
                );
                writer
                    .write_event(Event::Text(quick_xml::events::BytesText::new(&replacement)))
                    .map_err(|_| "could not serialize DASH manifest".to_string())?;
                if let Some(update) = base_updates.last_mut() {
                    *update = Some(resolved);
                }
            }
            Event::Text(text)
                if names
                    .last()
                    .is_some_and(|name| matches!(name.as_str(), "Location" | "PatchLocation")) =>
            {
                let decoded = text
                    .decode()
                    .map_err(|_| "invalid DASH URI element".to_string())?;
                let unescaped = quick_xml::escape::unescape(&decoded)
                    .map_err(|_| "invalid DASH URI element".to_string())?;
                let target = resolve_reference(
                    bases.last().ok_or("missing MPD base URL")?,
                    unescaped.trim(),
                )?;
                let local = proxy_resource(proxy, source_id, root_url, upstream_headers, target)?;
                writer
                    .write_event(Event::Text(quick_xml::events::BytesText::new(&local)))
                    .map_err(|_| "could not serialize DASH manifest".to_string())?;
            }
            Event::End(event) => {
                writer
                    .write_event(Event::End(event))
                    .map_err(|_| "could not serialize DASH manifest".to_string())?;
                let name = names.pop().ok_or("invalid DASH element nesting")?;
                bases.pop();
                let update = base_updates.pop().flatten();
                if name == "BaseURL"
                    && let (Some(parent), Some(update)) = (bases.last_mut(), update)
                {
                    *parent = update;
                }
            }
            Event::Eof => break,
            event => writer
                .write_event(event)
                .map_err(|_| "could not serialize DASH manifest".to_string())?,
        }
    }
    String::from_utf8(writer.into_inner()).map_err(|_| "DASH manifest output was not UTF-8".into())
}

fn is_dash_uri_attribute(name: &str) -> bool {
    matches!(
        name,
        "media" | "initialization" | "sourceURL" | "index" | "href" | "licenseUrl"
    )
}

fn rewrite_dash_uri(
    target: &Url,
    source_id: &str,
    root_url: &Url,
    upstream_headers: &[(String, String)],
    proxy: &NativeStreamProxy,
) -> Result<String, String> {
    let variables = dash_variables(target.as_str())?;
    let normalized_target = Url::parse(&target.as_str().replace("$$", "$"))
        .map_err(|_| "invalid escaped DASH URL template".to_string())?;
    if variables.is_empty() {
        return proxy_resource(
            proxy,
            source_id,
            root_url,
            upstream_headers,
            normalized_target,
        );
    }
    let local = proxy.issue_url(
        source_id,
        root_url,
        normalized_target,
        TokenKind::Template,
        upstream_headers,
        variables.clone(),
    )?;
    let mut query = String::new();
    for (index, variable) in variables.iter().enumerate() {
        if index > 0 {
            query.push('&');
        }
        query.push_str(&format!("v{index}={}", variable.expression));
    }
    Ok(format!("{local}?{query}"))
}

fn dash_variables(template: &str) -> Result<Vec<TemplateVariable>, String> {
    let mut variables = Vec::new();
    let mut cursor = 0;
    while let Some(start_rel) = template[cursor..].find('$') {
        let start = cursor + start_rel;
        if template.as_bytes().get(start + 1) == Some(&b'$') {
            cursor = start + 2;
            continue;
        }
        let end = template[start + 1..]
            .find('$')
            .map(|offset| start + 1 + offset)
            .ok_or_else(|| "unterminated DASH template variable".to_string())?;
        let expression = &template[start..=end];
        let inner = &template[start + 1..end];
        let name = inner.split('%').next().unwrap_or(inner);
        if !matches!(name, "Number" | "Time" | "Bandwidth" | "RepresentationID") {
            return Err("unsupported DASH template variable".into());
        }
        if inner.contains('%')
            && (!matches!(name, "Number" | "Time" | "Bandwidth")
                || !inner.split_once('%').is_some_and(|(_, format)| {
                    format.len() >= 2
                        && format.len() <= 4
                        && format.ends_with('d')
                        && format[..format.len() - 1]
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || byte == b'0')
                }))
        {
            return Err("unsupported DASH template format".into());
        }
        if !variables
            .iter()
            .any(|variable: &TemplateVariable| variable.name == name)
        {
            variables.push(TemplateVariable {
                name: name.to_string(),
                expression: expression.to_string(),
            });
        }
        cursor = end + 1;
    }
    Ok(variables)
}

async fn write_error(stream: &mut TcpStream, status: u16, message: &str) -> io::Result<()> {
    let reason = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        416 => "Range Not Satisfiable",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Bad Gateway",
    };
    let body = message.as_bytes();
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut writer = BufWriter::new(stream);
    writer.write_all(header.as_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt},
        net::TcpListener,
    };

    fn proxy() -> NativeStreamProxy {
        NativeStreamProxy::new(64 * 1024 * 1024).unwrap()
    }

    #[test]
    fn hls_rewrite_resolves_child_and_cross_host_uri_attribute() {
        let proxy = proxy();
        let root = Url::parse("https://cdn.example/master.m3u8?sig=1").unwrap();
        let headers = vec![("Cookie".to_string(), "secret=never-in-url".to_string())];
        let manifest = "#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,URI=\"https://audio.example/a.m3u8?sig=2\"\n#EXTINF:2,\nsegments/1.ts\n";
        let result = rewrite_hls(manifest, &root, "source-1", &root, &headers, &proxy).unwrap();
        assert!(result.contains("/api/v1/sources/source%2D1/native/"));
        assert!(!result.contains("https://audio.example"));
        assert!(!result.contains("segments/1.ts"));
        let count = proxy.tokens.lock().unwrap().claims.len();
        assert_eq!(count, 2);
        assert!(
            proxy
                .tokens
                .lock()
                .unwrap()
                .claims
                .values()
                .all(|claims| claims.upstream_headers == headers)
        );
        assert!(!result.contains("never-in-url"));
        let repeated = rewrite_hls(manifest, &root, "source-1", &root, &headers, &proxy).unwrap();
        assert_eq!(repeated, result);
        assert_eq!(proxy.tokens.lock().unwrap().claims.len(), 2);
    }

    #[test]
    fn redirected_hls_manifest_resolves_relative_uris_against_effective_url() {
        let proxy = proxy();
        let original = Url::parse("https://origin.example/redirect/playlist.m3u8").unwrap();
        let effective = Url::parse("https://cdn.example/final/hls/index.m3u8").unwrap();
        let result = rewrite_hls(
            "#EXTM3U\nchunk-001.ts\n",
            &effective,
            "source-1",
            &original,
            &[],
            &proxy,
        )
        .unwrap();
        assert!(result.contains("/native/"));
        let tokens = proxy.tokens.lock().unwrap();
        assert!(tokens.claims.values().any(|claims| {
            claims.target_url.as_str() == "https://cdn.example/final/hls/chunk-001.ts"
        }));
        assert!(
            tokens
                .claims
                .values()
                .all(|claims| claims.root_url == original)
        );
    }

    #[test]
    fn dash_rewrite_handles_base_url_templates_audio_and_subtitle_references() {
        let proxy = proxy();
        let root = Url::parse("https://cdn.example/title/manifest.mpd").unwrap();
        let mpd = r#"<?xml version="1.0"?><MPD><BaseURL>video/</BaseURL><Period><AdaptationSet><SegmentTemplate media="seg-$Number%05d$.m4s" initialization="init-$RepresentationID$.m4s"/><Representation id="v1"/></AdaptationSet><AdaptationSet contentType="text"><SegmentURL media="subtitles/en.vtt"/></AdaptationSet></Period></MPD>"#;
        let result = rewrite_dash(mpd, &root, "source-1", &root, &[], &proxy).unwrap();
        assert!(result.contains("/native/"));
        assert!(!result.contains("seg-$Number%05d$.m4s"));
        assert!(!result.contains("subtitles/en.vtt"));
        assert!(result.contains("$Number%05d$"));
        assert!(result.contains("$RepresentationID$"));
        let tokens = proxy.tokens.lock().unwrap();
        assert!(tokens.claims.values().any(|claims| {
            claims.target_url.as_str() == "https://cdn.example/title/video/seg-$Number%05d$.m4s"
        }));
        assert!(tokens.claims.values().any(|claims| {
            claims.target_url.as_str() == "https://cdn.example/title/video/subtitles/en.vtt"
        }));
    }

    #[test]
    fn dash_variables_reject_unknown_substitutions_and_bound_expansion() {
        assert!(dash_variables("https://cdn.example/$Unknown$.m4s").is_err());
        assert!(dash_variables("https://cdn.example/$Number%999999d$.m4s").is_err());
        assert!(dash_variables("https://cdn.example/$Number%05d$-$Time$.m4s").is_ok());
    }

    #[test]
    fn token_is_scoped_to_source_and_base_paths_reject_traversal() {
        let proxy = proxy();
        let root = Url::parse("https://cdn.example/a/manifest.m3u8").unwrap();
        let path = proxy
            .issue_url(
                "source-a",
                &root,
                root.clone(),
                TokenKind::Resource,
                &[],
                Vec::new(),
            )
            .unwrap();
        let token = path.rsplit('/').next().unwrap();
        assert!(proxy.claims("source-a", token).is_some());
        assert!(proxy.claims("source-b", token).is_none());
        let claims = TokenClaims {
            source_id: "source-a".into(),
            root_url: root.clone(),
            target_url: Url::parse("https://cdn.example/a/").unwrap(),
            kind: TokenKind::Base,
            upstream_headers: Vec::new(),
            variables: Vec::new(),
            expires_at: Instant::now() + TOKEN_TTL,
        };
        assert!(resolve_claim_target(&claims, &["..".into(), "secret".into()], "").is_err());
        assert!(resolve_claim_target(&claims, &["seg.ts".into()], "").is_ok());
        assert_eq!(
            resolve_claim_target(&claims, &[], "").unwrap(),
            claims.target_url
        );
        assert!(
            local_token_path("source/with space", "deadbeef").contains("source%2Fwith%20space")
        );
    }

    #[test]
    fn cross_host_referer_is_reduced_to_origin_and_rejects_credentials() {
        assert_eq!(
            sanitized_referer_origin("https://user:pass@example.com/path?secret=1"),
            None
        );
        assert_eq!(
            sanitized_referer_origin("https://example.com/path?secret=1#fragment"),
            Some("https://example.com/".into())
        );
        assert!(!safe_provider_header("Proxy-Authorization"));
        assert!(safe_provider_header("Cookie"));
    }

    #[test]
    fn byte_range_validator_accepts_single_suffix_and_rejects_multiple() {
        for range in ["bytes=0-", "bytes=2-9", "bytes=-500"] {
            assert!(valid_single_byte_range(range));
        }
        for range in ["bytes=0-2,4-8", "items=1-2", "bytes=-0", "bytes=9-2"] {
            assert!(!valid_single_byte_range(range));
        }
        assert_eq!(
            expected_range_length("bytes=-5", "bytes 15-19/20", Some(5)),
            Some(5)
        );
        assert_eq!(
            expected_range_length("bytes=5-", "bytes 5-19/20", Some(15)),
            Some(15)
        );
        assert_eq!(
            expected_range_length("bytes=5-8", "bytes 6-9/20", Some(4)),
            None
        );
        assert_eq!(
            expected_range_length("bytes=5-8", "bytes 5-8/20", Some(3)),
            None
        );
    }

    #[tokio::test]
    async fn relay_streams_first_chunk_before_upstream_finishes_and_preserves_range_bytes() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp2t\r\nContent-Length: 4\r\nContent-Range: bytes 5-8/20\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\nab",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            socket.write_all(b"cd").await.unwrap();
            socket.flush().await.unwrap();
        });

        let response = reqwest::Client::new()
            .get(format!("http://{upstream_address}/segment.ts"))
            .header(reqwest::header::RANGE, "bytes=5-8")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);

        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream_address = downstream.local_addr().unwrap();
        let proxy = proxy();
        let expected_range_bytes = expected_range_length("bytes=5-8", "bytes 5-8/20", Some(4));
        assert_eq!(expected_range_bytes, Some(4));
        let relay_task = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .relay_response(&mut socket, response, false, expected_range_bytes)
                .await
                .unwrap();
        });
        let started = Instant::now();
        let socket = TcpStream::connect(downstream_address).await.unwrap();
        let mut reader = tokio::io::BufReader::new(socket);
        let mut response_headers = Vec::new();
        loop {
            let mut line = Vec::new();
            reader.read_until(b'\n', &mut line).await.unwrap();
            let end = line == b"\r\n";
            response_headers.extend_from_slice(&line);
            if end {
                break;
            }
        }
        let response_headers = String::from_utf8(response_headers).unwrap();
        assert!(response_headers.starts_with("HTTP/1.1 206"));
        assert!(
            response_headers
                .to_ascii_lowercase()
                .contains("content-range: bytes 5-8/20")
        );
        let mut first = [0u8; 2];
        reader.read_exact(&mut first).await.unwrap();
        assert_eq!(&first, b"ab");
        assert!(started.elapsed() < Duration::from_millis(200));
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!([first.as_slice(), tail.as_slice()].concat(), b"abcd");
        relay_task.await.unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test]
    async fn direct_relay_streams_unknown_length_variable_speed_body_progressively() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nConnection: close\r\n\r\nab",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            socket.write_all(b"cd").await.unwrap();
            socket.flush().await.unwrap();
        });
        let response = reqwest::Client::new()
            .get(format!("http://{upstream_address}/video.mp4"))
            .send()
            .await
            .unwrap();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = downstream.local_addr().unwrap();
        let proxy = proxy();
        let relay_task = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .relay_direct_response(
                    &mut socket,
                    response,
                    false,
                    None,
                    Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
                )
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            client.read_exact(&mut byte).await.unwrap();
            headers.push(byte[0]);
        }
        let started = Instant::now();
        let mut body = Vec::new();
        client.read_to_end(&mut body).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(50));
        assert_eq!(body, b"abcd");
        relay_task.await.unwrap();
        upstream_task.await.unwrap();
    }

    async fn read_raw_request(socket: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut byte = [0u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            socket.read_exact(&mut byte).await.unwrap();
            request.push(byte[0]);
        }
        String::from_utf8(request).unwrap().to_ascii_lowercase()
    }

    #[tokio::test]
    async fn direct_redirects_preserve_same_origin_headers_and_strip_cross_origin_credentials() {
        let same_origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let same_address = same_origin.local_addr().unwrap();
        let same_task = tokio::spawn(async move {
            let (mut first, _) = same_origin.accept().await.unwrap();
            let first_request = read_raw_request(&mut first).await;
            first
                .write_all(b"HTTP/1.1 302 Found\r\nLocation: /final\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let (mut second, _) = same_origin.accept().await.unwrap();
            let second_request = read_raw_request(&mut second).await;
            second
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            (first_request, second_request)
        });
        let headers = vec![
            ("Cookie".into(), "session=secret".into()),
            ("Authorization".into(), "Bearer secret".into()),
            ("X-Api-Key".into(), "secret".into()),
            (
                "Referer".into(),
                "https://provider.example/path?secret=1".into(),
            ),
            ("Accept".into(), "video/*".into()),
            ("User-Agent".into(), "player".into()),
        ];
        let proxy = proxy();
        let root = Url::parse(&format!("http://{same_address}/video.mp4")).unwrap();
        let response = proxy
            .send_upstream(
                &root,
                &root,
                &headers,
                &HashMap::new(),
                UpstreamRequestOptions {
                    range: Some("bytes=0-3"),
                    head_only: false,
                    require_range: false,
                },
                Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
            )
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let (first_request, second_request) = same_task.await.unwrap();
        assert!(first_request.contains("cookie: session=secret"));
        assert!(second_request.contains("authorization: bearer secret"));
        assert!(second_request.contains("x-api-key: secret"));
        assert!(second_request.contains("range: bytes=0-3"));

        let cross_origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cross_address = cross_origin.local_addr().unwrap();
        let cross_task = tokio::spawn(async move {
            let (mut socket, _) = cross_origin.accept().await.unwrap();
            let request = read_raw_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            request
        });
        let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_address = redirect.local_addr().unwrap();
        let target = format!("http://{cross_address}/final");
        let redirect_task = tokio::spawn(async move {
            let (mut socket, _) = redirect.accept().await.unwrap();
            let request = read_raw_request(&mut socket).await;
            let location =
                format!("HTTP/1.1 302 Found\r\nLocation: {target}\r\nConnection: close\r\n\r\n");
            socket.write_all(location.as_bytes()).await.unwrap();
            request
        });
        let root = Url::parse(&format!("http://{redirect_address}/video.mp4")).unwrap();
        let response = proxy
            .send_upstream(
                &root,
                &root,
                &headers,
                &HashMap::new(),
                UpstreamRequestOptions {
                    range: Some("bytes=0-3"),
                    head_only: false,
                    require_range: false,
                },
                Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
            )
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        let redirect_request = redirect_task.await.unwrap();
        let cross_request = cross_task.await.unwrap();
        assert!(redirect_request.contains("cookie: session=secret"));
        assert!(!cross_request.contains("cookie:"));
        assert!(!cross_request.contains("authorization:"));
        assert!(!cross_request.contains("x-api-key:"));
        assert!(cross_request.contains("user-agent: player"));
        assert!(cross_request.contains("accept: video/*"));
        assert!(cross_request.contains("referer: https://provider.example/"));
        assert!(cross_request.contains("range: bytes=0-3"));
    }

    #[tokio::test]
    async fn direct_redirect_chain_respects_original_operation_deadline_and_redirect_bounds() {
        let delayed = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let delayed_address = delayed.local_addr().unwrap();
        let delayed_task = tokio::spawn(async move {
            let (mut socket, _) = delayed.accept().await.unwrap();
            let _ = read_raw_request(&mut socket).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        });
        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first.local_addr().unwrap();
        let delayed_target = format!("http://{delayed_address}/final");
        let first_task = tokio::spawn(async move {
            let (mut socket, _) = first.accept().await.unwrap();
            let _ = read_raw_request(&mut socket).await;
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: {delayed_target}\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let proxy = proxy();
        let root = Url::parse(&format!("http://{first_address}/start")).unwrap();
        let result = proxy
            .send_upstream(
                &root,
                &root,
                &[],
                &HashMap::new(),
                UpstreamRequestOptions {
                    range: None,
                    head_only: false,
                    require_range: false,
                },
                Instant::now() + Duration::from_millis(50),
            )
            .await;
        assert_eq!(result.unwrap_err().0, 504);
        first_task.await.unwrap();
        delayed_task.await.unwrap();

        let loop_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let loop_address = loop_listener.local_addr().unwrap();
        let loop_task = tokio::spawn(async move {
            for _ in 0..=MAX_REDIRECTS {
                let (mut socket, _) = loop_listener.accept().await.unwrap();
                let _ = read_raw_request(&mut socket).await;
                socket
                    .write_all(
                        b"HTTP/1.1 302 Found\r\nLocation: /loop\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
            }
        });
        let root = Url::parse(&format!("http://{loop_address}/loop")).unwrap();
        let result = proxy
            .send_upstream(
                &root,
                &root,
                &[],
                &HashMap::new(),
                UpstreamRequestOptions {
                    range: None,
                    head_only: false,
                    require_range: false,
                },
                Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
            )
            .await;
        assert_eq!(result.unwrap_err().0, 502);
        loop_task.await.unwrap();

        let missing = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let missing_address = missing.local_addr().unwrap();
        let missing_task = tokio::spawn(async move {
            let (mut socket, _) = missing.accept().await.unwrap();
            let _ = read_raw_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 302 Found\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let root = Url::parse(&format!("http://{missing_address}/missing")).unwrap();
        let result = proxy
            .send_upstream(
                &root,
                &root,
                &[],
                &HashMap::new(),
                UpstreamRequestOptions {
                    range: None,
                    head_only: false,
                    require_range: false,
                },
                Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
            )
            .await;
        assert_eq!(result.unwrap_err().0, 502);
        missing_task.await.unwrap();
    }

    async fn mock_http_response(response_bytes: &'static [u8]) -> reqwest::Response {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            socket.write_all(response_bytes).await.unwrap();
            socket.flush().await.unwrap();
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/video"))
            .send()
            .await
            .unwrap();
        task.await.unwrap();
        response
    }

    async fn relay_direct_to_bytes(
        response: reqwest::Response,
        request_range: Option<&str>,
    ) -> Vec<u8> {
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = downstream.local_addr().unwrap();
        let proxy = proxy();
        let range = request_range.map(str::to_string);
        let task = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .relay_direct_response(
                    &mut socket,
                    response,
                    false,
                    range.as_deref(),
                    Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
                )
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        task.await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn direct_relay_preserves_206_200_and_416_upstream_semantics() {
        let response = mock_http_response(
            b"HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\nContent-Length: 3\r\nContent-Range: bytes 2-4/5\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\ncde",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, Some("bytes=2-4")).await;
        let relayed_text = String::from_utf8(relayed).unwrap();
        assert!(relayed_text.starts_with("HTTP/1.1 206"));
        assert!(
            relayed_text
                .to_ascii_lowercase()
                .contains("content-range: bytes 2-4/5")
        );
        assert!(relayed_text.ends_with("\r\n\r\ncde"));

        let response = mock_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 5\r\nContent-Range: bytes 2-4/5\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\nabcde",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, Some("bytes=2-4")).await;
        let relayed_text = String::from_utf8(relayed).unwrap();
        assert!(relayed_text.starts_with("HTTP/1.1 200 OK"));
        assert!(!relayed_text.to_ascii_lowercase().contains("content-range:"));
        assert!(relayed_text.ends_with("\r\n\r\nabcde"));

        let response = mock_http_response(
            b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Type: video/mp4\r\nContent-Length: 0\r\nContent-Range: bytes */5\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, Some("bytes=8-9")).await;
        let relayed_text = String::from_utf8(relayed).unwrap();
        assert!(relayed_text.starts_with("HTTP/1.1 416"));
        assert!(
            relayed_text
                .to_ascii_lowercase()
                .contains("content-range: bytes */5")
        );
    }

    #[tokio::test]
    async fn direct_handler_forwards_client_range_to_upstream() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap().to_ascii_lowercase();
            assert!(request.contains("range: bytes=2-4"));
            socket
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\nContent-Length: 3\r\nContent-Range: bytes 2-4/5\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\ncde",
                )
                .await
                .unwrap();
            socket.flush().await.unwrap();
        });

        let proxy = proxy();
        let direct_url = proxy
            .direct_playback_url(
                "source-1",
                &format!("http://{upstream_address}/video.mp4"),
                &[],
            )
            .unwrap();
        let token = direct_url.rsplit('/').next().unwrap().to_string();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream_address = downstream.local_addr().unwrap();
        let request_headers = HashMap::from([("range".to_string(), "bytes=2-4".to_string())]);
        let handler = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .serve_direct(&mut socket, "source-1", &token, &request_headers, false)
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(downstream_address).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 206"));
        assert!(
            response
                .to_ascii_lowercase()
                .contains("content-range: bytes 2-4/5")
        );
        assert!(response.ends_with("\r\n\r\ncde"));
        handler.await.unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test]
    async fn saturated_native_stream_pool_returns_503_without_waiting() {
        let mut proxy = proxy();
        let root = Url::parse("https://cdn.example/master.m3u8").unwrap();
        let token_path = proxy
            .issue_url(
                "source-1",
                &root,
                root.clone(),
                TokenKind::Resource,
                &[],
                Vec::new(),
            )
            .unwrap();
        let token = token_path.rsplit('/').next().unwrap().to_string();
        proxy.slots = Arc::new(Semaphore::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request_proxy = proxy.clone();
        let handler = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            request_proxy
                .serve(
                    &mut socket,
                    "source-1",
                    &token,
                    &[],
                    "",
                    &HashMap::new(),
                    false,
                )
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 503"));
        handler.await.unwrap();
    }

    #[test]
    fn throughput_tracker_has_warmup_and_detects_sustained_low_rate() {
        let started = Instant::now();
        let mut tracker = ThroughputTracker::default();
        assert!(!tracker.record(started, 1));
        assert!(!tracker.record(started + Duration::from_secs(10), 1));
        assert!(tracker.record(started + THROUGHPUT_WINDOW, 1));
    }

    #[test]
    fn cross_origin_policy_preserves_only_explicit_safe_headers() {
        assert_eq!(
            cross_origin_header_value("User-Agent", "player"),
            Some("player".into())
        );
        assert_eq!(
            cross_origin_header_value("Accept", "video/*"),
            Some("video/*".into())
        );
        assert_eq!(
            cross_origin_header_value("Referer", "https://provider.example/path?secret=1"),
            Some("https://provider.example/".into())
        );
        assert_eq!(
            cross_origin_header_value("Origin", "https://provider.example/app"),
            Some("https://provider.example/".into())
        );
        assert!(cross_origin_header_value("Cookie", "session=secret").is_none());
        assert!(cross_origin_header_value("Authorization", "Bearer secret").is_none());
        assert!(cross_origin_header_value("X-Api-Key", "secret").is_none());
    }

    #[tokio::test]
    async fn direct_relay_closes_safely_on_expired_operation_deadline() {
        let response = mock_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 3\r\nConnection: close\r\n\r\nab",
        )
        .await;
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = downstream.local_addr().unwrap();
        let proxy = proxy();
        let task = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .relay_direct_response(
                    &mut socket,
                    response,
                    false,
                    None,
                    Instant::now() - Duration::from_secs(1),
                )
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        task.await.unwrap();
        assert!(
            String::from_utf8(bytes)
                .unwrap()
                .starts_with("HTTP/1.1 200")
        );
    }

    #[tokio::test]
    async fn direct_relay_does_not_claim_complete_body_after_truncation() {
        let response = mock_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabc",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, None).await;
        let relayed_text = String::from_utf8(relayed).unwrap();
        assert!(relayed_text.starts_with("HTTP/1.1 200"));
        assert!(relayed_text.ends_with("\r\n\r\nabc"));
        assert!(!relayed_text.ends_with("\r\n\r\nabcde"));
    }

    #[tokio::test]
    async fn direct_relay_preserves_safe_client_error_and_maps_upstream_server_error() {
        let response = mock_http_response(
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, None).await;
        assert!(
            String::from_utf8(relayed)
                .unwrap()
                .starts_with("HTTP/1.1 404")
        );

        let response = mock_http_response(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let relayed = relay_direct_to_bytes(response, None).await;
        assert!(
            String::from_utf8(relayed)
                .unwrap()
                .starts_with("HTTP/1.1 502")
        );
    }

    #[tokio::test]
    async fn direct_relay_head_does_not_read_or_forward_body() {
        let response = mock_http_response(
            b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabcde",
        )
        .await;
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = downstream.local_addr().unwrap();
        let proxy = proxy();
        let task = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            proxy
                .relay_direct_response(
                    &mut socket,
                    response,
                    true,
                    None,
                    Instant::now() + DIRECT_STREAM_OPERATION_DEADLINE,
                )
                .await
                .unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        task.await.unwrap();
        let output = String::from_utf8(bytes).unwrap();
        assert!(output.starts_with("HTTP/1.1 200"));
        assert!(output.ends_with("\r\n\r\n"));
    }

    #[tokio::test]
    async fn aborting_direct_request_future_releases_semaphore_capacity() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_address = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            let (mut socket, _) = upstream.accept().await.unwrap();
            let mut request = [0u8; 256];
            let _ = socket.read(&mut request).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let proxy = proxy();
        let direct_url = proxy
            .direct_playback_url(
                "source-1",
                &format!("http://{upstream_address}/video.mp4"),
                &[],
            )
            .unwrap();
        let token = direct_url.rsplit('/').next().unwrap().to_string();
        let downstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let downstream_address = downstream.local_addr().unwrap();
        let request_proxy = proxy.clone();
        let handler = tokio::spawn(async move {
            let (mut socket, _) = downstream.accept().await.unwrap();
            request_proxy
                .serve_direct(&mut socket, "source-1", &token, &HashMap::new(), false)
                .await
        });
        let client = TcpStream::connect(downstream_address).await.unwrap();
        drop(client);
        tokio::time::sleep(Duration::from_millis(50)).await;
        handler.abort();
        let _ = handler.await;
        assert!(proxy.slots.clone().try_acquire_owned().is_ok());
        upstream_task.abort();
        let _ = upstream_task.await;
    }
}
