use std::{process::Stdio, time::Duration};

use futures::StreamExt;
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

use crate::providers::models::{PlaybackSource, ProviderKind};

const SAMPLE_LIMIT: usize = 32 * 1024 * 1024;
const DRAMACHI_FAST_SAMPLE_LIMIT: usize = 1024 * 1024;
const DRAMACHI_FALLBACK_SAMPLE_LIMIT: usize = 4 * 1024 * 1024;
const DIRECT_SAMPLE_LIMIT: usize = 4 * 1024 * 1024;
const HLS_MANIFEST_LIMIT: usize = 1024 * 1024;
const HLS_MEDIA_LIMIT: usize = 8 * 1024 * 1024;
const PROBE_STDOUT_LIMIT: usize = 64 * 1024;
const PROBE_STDERR_LIMIT: usize = 64 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(25);
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_REDIRECTS: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecProbeResult {
    Allowed(String),
    BlockedHevc(String),
    Unsupported(String),
    NoVideoStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecProbeError {
    UnsupportedProvider,
    InvalidMediaUrl,
    UnsafeRedirect,
    DownloadTimeout,
    MediaUnavailable,
    EmptyMedia,
    ProbeUnavailable,
    ProbeTimeout,
    ProbeFailed,
    ProbeOutputLimit,
    MalformedProbeOutput,
}

impl CodecProbeError {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnsupportedProvider => "VERIFICATION_PROVIDER_UNSUPPORTED",
            Self::InvalidMediaUrl => "VERIFICATION_URL_INVALID",
            Self::UnsafeRedirect => "VERIFICATION_REDIRECT_BLOCKED",
            Self::DownloadTimeout => "MEDIA_PROBE_TIMEOUT",
            Self::MediaUnavailable => "MEDIA_PROBE_FAILED",
            Self::EmptyMedia => "MEDIA_PROBE_FAILED",
            Self::ProbeUnavailable => "FFPROBE_UNAVAILABLE",
            Self::ProbeTimeout => "MEDIA_PROBE_TIMEOUT",
            Self::ProbeFailed => "MEDIA_PROBE_FAILED",
            Self::ProbeOutputLimit => "MEDIA_PROBE_OUTPUT_LIMIT",
            Self::MalformedProbeOutput => "MEDIA_PROBE_FAILED",
        }
    }

    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::DownloadTimeout
                | Self::MediaUnavailable
                | Self::ProbeUnavailable
                | Self::ProbeTimeout
                | Self::ProbeFailed
                | Self::ProbeOutputLimit
                | Self::MalformedProbeOutput
        )
    }
}

#[derive(Debug, Deserialize)]
struct ProbeOutput {
    streams: Option<Vec<ProbeStream>>,
    format: Option<ProbeFormat>,
}

#[derive(Debug, Deserialize)]
struct ProbeFormat {
    format_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProbeStream {
    codec_type: Option<String>,
    codec_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectPlaybackEvidence {
    pub codec: CodecProbeResult,
    pub format_name: Option<String>,
    pub audio_codecs: Vec<String>,
    pub range_supported: bool,
}

pub async fn verify_playback_source(
    source: &PlaybackSource,
) -> Result<CodecProbeResult, CodecProbeError> {
    let verification_started = std::time::Instant::now();
    if source.provider == ProviderKind::AniZone {
        return verify_hls_source(source).await;
    }
    if !matches!(
        source.provider,
        ProviderKind::FourKHdHub | ProviderKind::Dramachi
    ) {
        return Err(CodecProbeError::UnsupportedProvider);
    }

    let validation_started = std::time::Instant::now();
    let url = validate_media_url(&source.url).await?;
    log::info!(
        "direct_play_verification provider={} stage=source_validation elapsed_ms={}",
        source.provider,
        validation_started.elapsed().as_millis()
    );
    let sample_limit = if source.provider == ProviderKind::Dramachi {
        DRAMACHI_FAST_SAMPLE_LIMIT
    } else {
        SAMPLE_LIMIT
    };
    let download_started = std::time::Instant::now();
    let sample = match tokio::time::timeout(
        DOWNLOAD_TIMEOUT,
        download_sample(&url, &source.headers, sample_limit),
    )
    .await
    {
        Ok(Ok(sample)) => sample,
        Ok(Err(error)) => {
            log::warn!(
                "direct_play_verification provider={} stage=range_download attempt=1 elapsed_ms={} outcome=failed category={}",
                source.provider,
                download_started.elapsed().as_millis(),
                error.code()
            );
            return Err(error);
        }
        Err(_) => {
            log::warn!(
                "direct_play_verification provider={} stage=range_download attempt=1 elapsed_ms={} outcome=failed category=MEDIA_PROBE_TIMEOUT",
                source.provider,
                download_started.elapsed().as_millis()
            );
            return Err(CodecProbeError::DownloadTimeout);
        }
    };
    log::info!(
        "direct_play_verification provider={} stage=range_download attempt=1 elapsed_ms={} requested_bytes={} downloaded_bytes={} http_status={} range_supported={}",
        source.provider,
        download_started.elapsed().as_millis(),
        sample_limit,
        sample.bytes.len(),
        sample.http_status,
        sample.range_supported
    );
    if sample.bytes.is_empty() {
        return Err(CodecProbeError::EmptyMedia);
    }

    let ffprobe = ffprobe_executable().ok_or(CodecProbeError::ProbeUnavailable)?;
    let probe_started = std::time::Instant::now();
    let first_probe = probe_sample_with_executable(&ffprobe, &sample.bytes, PROBE_TIMEOUT).await;
    log::info!(
        "direct_play_verification provider={} stage=ffprobe attempt=1 elapsed_ms={} sample_bytes={} result={}",
        source.provider,
        probe_started.elapsed().as_millis(),
        sample.bytes.len(),
        probe_result_category(&first_probe)
    );
    let result = match first_probe {
        Ok(result) => Ok(result),
        Err(error)
            if source.provider == ProviderKind::Dramachi && should_retry_dramachi_probe(error) =>
        {
            // Front-loaded MP4 metadata can be inspected from a small range. If
            // moov/codec configuration is later in the file, retry once with a
            // still-bounded sample rather than downloading the generic 32 MiB.
            // Timeouts, missing ffprobe, and output-limit failures are not cured
            // by transferring and probing more media, so those fail immediately.
            log::info!(
                "direct_play_verification provider={} stage=probe_retry reason={}",
                source.provider,
                error.code()
            );
            let download_started = std::time::Instant::now();
            let sample = match tokio::time::timeout(
                DOWNLOAD_TIMEOUT,
                download_sample(&url, &source.headers, DRAMACHI_FALLBACK_SAMPLE_LIMIT),
            )
            .await
            {
                Ok(Ok(sample)) => sample,
                Ok(Err(error)) => {
                    log::warn!(
                        "direct_play_verification provider={} stage=range_download attempt=2 elapsed_ms={} outcome=failed category={}",
                        source.provider,
                        download_started.elapsed().as_millis(),
                        error.code()
                    );
                    return Err(error);
                }
                Err(_) => {
                    log::warn!(
                        "direct_play_verification provider={} stage=range_download attempt=2 elapsed_ms={} outcome=failed category=MEDIA_PROBE_TIMEOUT",
                        source.provider,
                        download_started.elapsed().as_millis()
                    );
                    return Err(CodecProbeError::DownloadTimeout);
                }
            };
            log::info!(
                "direct_play_verification provider={} stage=range_download attempt=2 elapsed_ms={} requested_bytes={} downloaded_bytes={} http_status={} range_supported={}",
                source.provider,
                download_started.elapsed().as_millis(),
                DRAMACHI_FALLBACK_SAMPLE_LIMIT,
                sample.bytes.len(),
                sample.http_status,
                sample.range_supported
            );
            if sample.bytes.is_empty() {
                return Err(CodecProbeError::EmptyMedia);
            }
            let probe_started = std::time::Instant::now();
            let result = probe_sample_with_executable(&ffprobe, &sample.bytes, PROBE_TIMEOUT).await;
            log::info!(
                "direct_play_verification provider={} stage=ffprobe attempt=2 elapsed_ms={} sample_bytes={} result={}",
                source.provider,
                probe_started.elapsed().as_millis(),
                sample.bytes.len(),
                probe_result_category(&result)
            );
            result
        }
        Err(error) => Err(error),
    };
    log::info!(
        "direct_play_verification provider={} stage=complete elapsed_ms={} result={}",
        source.provider,
        verification_started.elapsed().as_millis(),
        probe_result_category(&result)
    );
    result
}

async fn verify_hls_source(source: &PlaybackSource) -> Result<CodecProbeResult, CodecProbeError> {
    let manifest_url = validate_media_url(&source.url).await?;
    let client = guarded_media_client()?;
    let manifest = download_hls_text(&client, manifest_url.clone(), &source.headers).await?;
    let (variant_url, mut init_url, mut segment_url) =
        select_hls_media_urls(&manifest_url, &manifest)?;
    if let Some(variant_url) = variant_url {
        let variant_manifest = download_hls_text(&client, variant_url.clone(), &source.headers).await?;
        let (_, variant_init, variant_segment) = select_hls_media_urls(&variant_url, &variant_manifest)?;
        init_url = variant_init.or(init_url);
        segment_url = variant_segment;
    }
    let segment_url = segment_url.ok_or(CodecProbeError::MalformedProbeOutput)?;
    let mut media = Vec::new();
    if let Some(init_url) = init_url {
        media.extend(download_hls_bytes(&client, init_url, &source.headers, HLS_MEDIA_LIMIT).await?);
    }
    media.extend(download_hls_bytes(&client, segment_url, &source.headers, HLS_MEDIA_LIMIT).await?);
    if media.is_empty() {
        return Err(CodecProbeError::EmptyMedia);
    }
    let ffprobe = ffprobe_executable().ok_or(CodecProbeError::ProbeUnavailable)?;
    probe_sample_with_executable(&ffprobe, &media, PROBE_TIMEOUT).await
}

async fn download_hls_text(
    client: &reqwest::Client,
    url: reqwest::Url,
    trusted_headers: &[(String, String)],
) -> Result<String, CodecProbeError> {
    let bytes = download_hls_bytes(client, url, trusted_headers, HLS_MANIFEST_LIMIT).await?;
    String::from_utf8(bytes).map_err(|_| CodecProbeError::MalformedProbeOutput)
}

async fn download_hls_bytes(
    client: &reqwest::Client,
    url: reqwest::Url,
    trusted_headers: &[(String, String)],
    limit: usize,
) -> Result<Vec<u8>, CodecProbeError> {
    if url.scheme() != "https" {
        return Err(CodecProbeError::InvalidMediaUrl);
    }
    crate::net::validate_public_url(url.as_str(), false)
        .await
        .map_err(|_| CodecProbeError::InvalidMediaUrl)?;
    let mut request = client.get(url);
    for (name, value) in trusted_headers.iter().take(16) {
        if name.eq_ignore_ascii_case("referer") {
            let referer = reqwest::Url::parse(value).map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            if referer.scheme() != "https" || !referer.username().is_empty() || referer.password().is_some() {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            crate::net::validate_public_url(referer.as_str(), false)
                .await
                .map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            request = request.header(reqwest::header::REFERER, referer.as_str());
        } else if name.eq_ignore_ascii_case("user-agent") {
            if value.len() > 512 || value.contains(['\r', '\n']) {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            request = request.header(reqwest::header::USER_AGENT, value);
        }
    }
    let response = request.send().await.map_err(|_| CodecProbeError::MediaUnavailable)?;
    crate::net::validate_public_url(response.url().as_str(), false)
        .await
        .map_err(|_| CodecProbeError::UnsafeRedirect)?;
    if !response.status().is_success() {
        return Err(CodecProbeError::MediaUnavailable);
    }
    let mut body = response.bytes_stream();
    let mut bytes = Vec::with_capacity(limit.min(64 * 1024));
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| CodecProbeError::MediaUnavailable)?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(CodecProbeError::MediaUnavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn select_hls_media_urls(
    manifest_url: &reqwest::Url,
    manifest: &str,
) -> Result<(Option<reqwest::Url>, Option<reqwest::Url>, Option<reqwest::Url>), CodecProbeError> {
    let lines = manifest.lines().map(str::trim).collect::<Vec<_>>();
    if !lines.first().is_some_and(|line| line.eq_ignore_ascii_case("#EXTM3U")) {
        return Err(CodecProbeError::MalformedProbeOutput);
    }
    let mut variant = None;
    let mut init = None;
    let mut segment = None;
    let mut expect_variant = false;
    for line in lines.iter().copied() {
        if line.starts_with("#EXT-X-STREAM-INF") {
            expect_variant = true;
            continue;
        }
        if line.starts_with("#EXT-X-MAP:") {
            if let Some(uri) = quoted_attribute(line, "URI") {
                init = Some(manifest_url.join(&uri).map_err(|_| CodecProbeError::MalformedProbeOutput)?);
            }
            continue;
        }
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let url = manifest_url.join(line).map_err(|_| CodecProbeError::MalformedProbeOutput)?;
        if expect_variant && variant.is_none() {
            variant = Some(url);
            expect_variant = false;
        } else if segment.is_none() {
            segment = Some(url);
        }
    }
    if let Some(variant_url) = variant {
        return Ok((Some(variant_url), init, segment));
    }
    Ok((None, init, segment))
}

fn quoted_attribute(line: &str, name: &str) -> Option<String> {
    let marker = format!("{name}=\"");
    let start = line.find(&marker)? + marker.len();
    let end = line[start..].find('"')? + start;
    Some(line[start..end].to_string())
}

struct DownloadedSample {
    bytes: Vec<u8>,
    http_status: u16,
    range_supported: bool,
}

fn should_retry_dramachi_probe(error: CodecProbeError) -> bool {
    matches!(
        error,
        CodecProbeError::ProbeFailed | CodecProbeError::MalformedProbeOutput
    )
}

fn probe_result_category(result: &Result<CodecProbeResult, CodecProbeError>) -> &'static str {
    match result {
        Ok(CodecProbeResult::Allowed(_)) => "h264-allowed",
        Ok(CodecProbeResult::BlockedHevc(_)) => "hevc-blocked",
        Ok(CodecProbeResult::Unsupported(_)) => "unsupported-codec",
        Ok(CodecProbeResult::NoVideoStream) => "no-video-stream",
        Err(error) => error.code(),
    }
}

pub(crate) async fn inspect_direct_playback_source(
    source: &PlaybackSource,
) -> Result<DirectPlaybackEvidence, CodecProbeError> {
    if source.provider != ProviderKind::FourKHdHub {
        return Err(CodecProbeError::UnsupportedProvider);
    }
    let url = validate_media_url(&source.url).await?;
    let (sample, range_supported) =
        download_direct_sample(&url, &source.headers, DIRECT_SAMPLE_LIMIT).await?;
    if sample.is_empty() {
        return Err(CodecProbeError::EmptyMedia);
    }
    let ffprobe = ffprobe_executable().ok_or(CodecProbeError::ProbeUnavailable)?;
    let (codec, format_name, audio_codecs) =
        probe_sample_details_with_executable(&ffprobe, &sample, PROBE_TIMEOUT).await?;
    Ok(DirectPlaybackEvidence {
        codec,
        format_name,
        audio_codecs,
        range_supported,
    })
}

async fn download_direct_sample(
    url: &reqwest::Url,
    trusted_headers: &[(String, String)],
    limit: usize,
) -> Result<(Vec<u8>, bool), CodecProbeError> {
    let client = guarded_media_client()?;
    let end = limit.saturating_sub(1);
    let mut request = client
        .get(url.clone())
        .header(reqwest::header::RANGE, format!("bytes=0-{end}"));
    for (name, value) in trusted_headers.iter().take(16) {
        if name.eq_ignore_ascii_case("referer") {
            let referer =
                reqwest::Url::parse(value).map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            if referer.scheme() != "https"
                || !referer.username().is_empty()
                || referer.password().is_some()
            {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            crate::net::validate_public_url(referer.as_str(), false)
                .await
                .map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            request = request.header(reqwest::header::REFERER, referer.as_str());
        } else if name.eq_ignore_ascii_case("user-agent") {
            if value.len() > 512 || value.contains(['\r', '\n']) {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            request = request.header(reqwest::header::USER_AGENT, value);
        }
    }
    let response = request
        .send()
        .await
        .map_err(|_| CodecProbeError::MediaUnavailable)?;
    crate::net::validate_public_url(response.url().as_str(), false)
        .await
        .map_err(|_| CodecProbeError::UnsafeRedirect)?;
    let range_supported = response.status() == reqwest::StatusCode::PARTIAL_CONTENT
        && response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| validated_initial_range(value, response.content_length(), limit))
            .is_some();
    if !response.status().is_success() {
        return Err(CodecProbeError::MediaUnavailable);
    }
    let mut body = response.bytes_stream();
    let mut sample = Vec::with_capacity(limit.min(1024 * 1024));
    while let Some(next) = body.next().await {
        let chunk = next.map_err(|_| CodecProbeError::MediaUnavailable)?;
        let remaining = limit.saturating_sub(sample.len());
        if chunk.len() > remaining {
            sample.extend_from_slice(&chunk[..remaining]);
            break;
        }
        sample.extend_from_slice(&chunk);
        if sample.len() == limit {
            break;
        }
    }
    Ok((sample, range_supported))
}

fn validated_initial_range(
    value: &str,
    content_length: Option<u64>,
    requested_limit: usize,
) -> Option<u64> {
    let mut parts = value.split_whitespace();
    if !parts.next()?.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (interval, total) = parts.next()?.split_once('/')?;
    if parts.next().is_some() {
        return None;
    }
    let (start, end) = interval.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    let total = total.parse::<u64>().ok()?;
    if total == 0 || start != 0 || end < start || end >= total {
        return None;
    }
    let expected_end = (requested_limit as u64).min(total).checked_sub(1)?;
    if end != expected_end {
        return None;
    }
    let length = end.checked_add(1)?;
    if content_length.is_some_and(|actual| actual != length) {
        return None;
    }
    Some(length)
}

async fn validate_media_url(raw: &str) -> Result<reqwest::Url, CodecProbeError> {
    let syntax = crate::net::validate_url_syntax(raw, false)
        .map_err(|_| CodecProbeError::InvalidMediaUrl)?;
    if syntax.scheme() != "https" {
        return Err(CodecProbeError::InvalidMediaUrl);
    }
    crate::net::validate_public_url(syntax.as_str(), false)
        .await
        .map_err(|_| CodecProbeError::InvalidMediaUrl)
}

fn guarded_media_client() -> Result<reqwest::Client, CodecProbeError> {
    crate::net::http_client_builder_base()
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() >= MAX_REDIRECTS {
                return attempt.error("media redirect limit exceeded");
            }
            if crate::net::validate_url_syntax(attempt.url().as_str(), false).is_ok() {
                attempt.follow()
            } else {
                attempt.error("media redirect target is not an allowed public HTTPS URL")
            }
        }))
        .timeout(DOWNLOAD_TIMEOUT)
        .build()
        .map_err(|_| CodecProbeError::MediaUnavailable)
}

async fn download_sample(
    url: &reqwest::Url,
    trusted_headers: &[(String, String)],
    sample_limit: usize,
) -> Result<DownloadedSample, CodecProbeError> {
    let client = guarded_media_client()?;
    let mut request = client.get(url.clone()).header(
        reqwest::header::RANGE,
        format!("bytes=0-{}", sample_limit - 1),
    );
    let mut referer_seen = false;
    for (name, value) in trusted_headers.iter().take(16) {
        if name.eq_ignore_ascii_case("referer") {
            let referer =
                reqwest::Url::parse(value).map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            if referer.scheme() != "https"
                || referer.username() != ""
                || referer.password().is_some()
            {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            crate::net::validate_public_url(referer.as_str(), false)
                .await
                .map_err(|_| CodecProbeError::InvalidMediaUrl)?;
            request = request.header(reqwest::header::REFERER, referer.as_str());
            referer_seen = true;
        } else if name.eq_ignore_ascii_case("user-agent") {
            if value.len() > 512 || value.contains(['\r', '\n']) {
                return Err(CodecProbeError::InvalidMediaUrl);
            }
            request = request.header(reqwest::header::USER_AGENT, value);
        }
        // Only resolver-supplied Referer and User-Agent are needed here. Cookies,
        // authorization, Host, and proxy headers are deliberately not forwarded.
    }
    if !referer_seen {
        // Absence is allowed: some supported mirrors do not require a Referer.
    }

    let response = request
        .send()
        .await
        .map_err(|_| CodecProbeError::MediaUnavailable)?;
    let http_status = response.status().as_u16();
    if !response.status().is_success() {
        return Err(CodecProbeError::MediaUnavailable);
    }
    crate::net::validate_public_url(response.url().as_str(), false)
        .await
        .map_err(|_| CodecProbeError::UnsafeRedirect)?;
    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let range_supported = response.status() == reqwest::StatusCode::PARTIAL_CONTENT
        && content_range.as_deref().is_some_and(|content_range| {
            validated_initial_range(content_range, response.content_length(), sample_limit)
                .is_some()
        });
    if response.status() == reqwest::StatusCode::PARTIAL_CONTENT && !range_supported {
        return Err(CodecProbeError::MediaUnavailable);
    }

    let mut stream = response.bytes_stream();
    let mut sample = Vec::with_capacity(sample_limit.min(1024 * 1024));
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|_| CodecProbeError::MediaUnavailable)?;
        let remaining = sample_limit.saturating_sub(sample.len());
        if chunk.len() > remaining {
            sample.extend_from_slice(&chunk[..remaining]);
            break;
        }
        sample.extend_from_slice(&chunk);
        if sample.len() == sample_limit {
            break;
        }
    }
    Ok(DownloadedSample {
        bytes: sample,
        http_status,
        range_supported,
    })
}

fn ffprobe_executable() -> Option<std::path::PathBuf> {
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join("ffprobe");
        if candidate.is_file() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if std::fs::metadata(&candidate).ok()?.permissions().mode() & 0o111 == 0 {
                    continue;
                }
            }
            return Some(candidate);
        }
    }
    None
}

async fn probe_sample_with_executable(
    executable: &std::path::Path,
    sample: &[u8],
    timeout: Duration,
) -> Result<CodecProbeResult, CodecProbeError> {
    probe_sample_details_with_executable(executable, sample, timeout)
        .await
        .map(|(codec, _, _)| codec)
}

async fn probe_sample_details_with_executable(
    executable: &std::path::Path,
    sample: &[u8],
    timeout: Duration,
) -> Result<(CodecProbeResult, Option<String>, Vec<String>), CodecProbeError> {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .args([
            "-hide_banner",
            "-v",
            "error",
            "-probesize",
            "5000000",
            "-analyzeduration",
            "5000000",
            "-protocol_whitelist",
            "pipe,crypto,data",
            "-show_entries",
            "stream=codec_type,codec_name:format=format_name",
            "-of",
            "json",
            "-i",
            "pipe:0",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| CodecProbeError::ProbeUnavailable)?;
    let mut stdin = child.stdin.take().ok_or(CodecProbeError::ProbeFailed)?;
    let mut stdout = child.stdout.take().ok_or(CodecProbeError::ProbeFailed)?;
    let mut stderr = child.stderr.take().ok_or(CodecProbeError::ProbeFailed)?;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    let mut stdout_buf = [0_u8; 8192];
    let mut stderr_buf = [0_u8; 4096];
    let mut stdout_closed = false;
    let mut stderr_closed = false;
    let mut input_written = false;
    let mut process_status = None;
    let sample = sample.to_vec();
    let mut write_input = Box::pin(async move {
        let result = stdin.write_all(&sample).await;
        let _ = stdin.shutdown().await;
        result
    });

    let run = async {
        loop {
            if process_status.is_some() && stdout_closed && stderr_closed && input_written {
                break;
            }
            tokio::select! {
                read = stdout.read(&mut stdout_buf), if !stdout_closed => {
                    let count = read.map_err(|_| CodecProbeError::ProbeFailed)?;
                    if count == 0 {
                        stdout_closed = true;
                    } else if stdout_bytes.len().saturating_add(count) > PROBE_STDOUT_LIMIT {
                        let _ = child.kill().await;
                        return Err(CodecProbeError::ProbeOutputLimit);
                    } else {
                        stdout_bytes.extend_from_slice(&stdout_buf[..count]);
                    }
                }
                read = stderr.read(&mut stderr_buf), if !stderr_closed => {
                    let count = read.map_err(|_| CodecProbeError::ProbeFailed)?;
                    if count == 0 {
                        stderr_closed = true;
                    } else if stderr_bytes.len().saturating_add(count) > PROBE_STDERR_LIMIT {
                        let _ = child.kill().await;
                        return Err(CodecProbeError::ProbeOutputLimit);
                    } else {
                        stderr_bytes.extend_from_slice(&stderr_buf[..count]);
                    }
                }
                result = &mut write_input, if !input_written => {
                    // FFprobe may stop after finding enough stream metadata. A
                    // broken pipe is acceptable only if FFprobe exits successfully.
                    let _ = result;
                    input_written = true;
                }
                _ = tokio::time::sleep(Duration::from_millis(10)), if process_status.is_none() => {
                    process_status = child.try_wait().map_err(|_| CodecProbeError::ProbeFailed)?;
                }
            }
        }
        let status = process_status.ok_or(CodecProbeError::ProbeFailed)?;
        if !status.success() {
            return Err(CodecProbeError::ProbeFailed);
        }
        classify_probe_details(&stdout_bytes)
    };

    tokio::time::timeout(timeout, run)
        .await
        .map_err(|_| CodecProbeError::ProbeTimeout)?
}

#[cfg(test)]
fn classify_probe_json(bytes: &[u8]) -> Result<CodecProbeResult, CodecProbeError> {
    classify_probe_details(bytes).map(|(codec, _, _)| codec)
}

fn classify_probe_details(
    bytes: &[u8],
) -> Result<(CodecProbeResult, Option<String>, Vec<String>), CodecProbeError> {
    let output: ProbeOutput =
        serde_json::from_slice(bytes).map_err(|_| CodecProbeError::MalformedProbeOutput)?;
    let streams = output.streams.unwrap_or_default();
    let audio_codecs = streams
        .iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("audio"))
        .filter_map(|stream| stream.codec_name.clone())
        .map(|codec| codec.trim().to_ascii_lowercase())
        .filter(|codec| !codec.is_empty())
        .collect::<Vec<_>>();
    let video_codecs = streams
        .into_iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("video"))
        .filter_map(|stream| stream.codec_name)
        .map(|codec| codec.trim().to_ascii_lowercase())
        .filter(|codec| !codec.is_empty())
        .collect::<Vec<_>>();
    let codec = if video_codecs.is_empty() {
        CodecProbeResult::NoVideoStream
    } else if let Some(codec) = video_codecs.iter().find(|codec| is_hevc_codec(codec)) {
        CodecProbeResult::BlockedHevc(codec.clone())
    } else if let Some(codec) = video_codecs.iter().find(|codec| !is_allowed_codec(codec)) {
        CodecProbeResult::Unsupported(codec.clone())
    } else {
        CodecProbeResult::Allowed(video_codecs[0].clone())
    };
    Ok((
        codec,
        output.format.and_then(|format| format.format_name),
        audio_codecs,
    ))
}

pub fn is_hevc_codec(codec: &str) -> bool {
    let normalized = normalize_codec_label(codec);
    ["hevc", "h265", "x265", "hvc1", "hev1"]
        .iter()
        .any(|identifier| normalized == *identifier || normalized.starts_with(identifier))
}

pub fn is_allowed_codec(codec: &str) -> bool {
    let normalized = normalize_codec_label(codec);
    matches!(
        normalized.as_str(),
        "h264" | "avc" | "avc1" | "h264avc" | "avch264"
    ) || normalized.strip_prefix("h264").is_some_and(|suffix| {
        matches!(
            suffix,
            "baseline" | "constrainedbaseline" | "main" | "high" | "high10" | "high422" | "high444"
        )
    }) || normalized.strip_prefix("avc1").is_some_and(|suffix| {
        suffix.len() == 6 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn normalize_codec_label(codec: &str) -> String {
    codec
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;

    #[test]
    fn dramachi_probe_uses_small_fast_range_and_bounded_fallback() {
        assert_eq!(DRAMACHI_FAST_SAMPLE_LIMIT, 1024 * 1024);
        assert_eq!(DRAMACHI_FALLBACK_SAMPLE_LIMIT, 4 * 1024 * 1024);
        assert!(DRAMACHI_FALLBACK_SAMPLE_LIMIT < SAMPLE_LIMIT);
    }

    #[test]
    fn dramachi_retries_only_when_more_sample_bytes_can_help() {
        assert!(should_retry_dramachi_probe(CodecProbeError::ProbeFailed));
        assert!(should_retry_dramachi_probe(
            CodecProbeError::MalformedProbeOutput
        ));
        assert!(!should_retry_dramachi_probe(CodecProbeError::ProbeTimeout));
        assert!(!should_retry_dramachi_probe(
            CodecProbeError::ProbeUnavailable
        ));
        assert!(!should_retry_dramachi_probe(
            CodecProbeError::ProbeOutputLimit
        ));
    }

    async fn probe_fixture(bytes: &[u8]) -> Result<CodecProbeResult, CodecProbeError> {
        let executable = ffprobe_executable().expect("ffprobe installed for this test run");
        probe_sample_with_executable(&executable, bytes, Duration::from_secs(5)).await
    }

    #[tokio::test]
    async fn actual_h264_and_hevc_fixtures_are_classified_from_ffprobe() {
        let h264 = include_bytes!("../tests/fixtures/codec-h264.mkv");
        let hevc = include_bytes!("../tests/fixtures/codec-hevc.mkv");
        assert_eq!(
            probe_fixture(h264).await.unwrap(),
            CodecProbeResult::Allowed("h264".into())
        );
        assert_eq!(
            probe_fixture(hevc).await.unwrap(),
            CodecProbeResult::BlockedHevc("hevc".into())
        );
    }

    #[test]
    fn hls_master_and_media_playlists_resolve_bounded_probe_targets() {
        let master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\n720/index.m3u8\n";
        let base = reqwest::Url::parse("https://cdn.example/master.m3u8").unwrap();
        let (variant, init, segment) = select_hls_media_urls(&base, master).unwrap();
        assert_eq!(variant.unwrap().as_str(), "https://cdn.example/720/index.m3u8");
        assert!(init.is_none());
        assert!(segment.is_none());

        let media = "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\nseg0.m4s\n";
        let variant_url = reqwest::Url::parse("https://cdn.example/720/index.m3u8").unwrap();
        let (variant, init, segment) = select_hls_media_urls(&variant_url, media).unwrap();
        assert!(variant.is_none());
        assert_eq!(init.unwrap().as_str(), "https://cdn.example/720/init.mp4");
        assert_eq!(segment.unwrap().as_str(), "https://cdn.example/720/seg0.m4s");
    }

    #[test]
    fn malformed_hls_playlist_fails_closed() {
        let base = reqwest::Url::parse("https://cdn.example/master.m3u8").unwrap();
        assert!(matches!(
            select_hls_media_urls(&base, "not a playlist"),
            Err(CodecProbeError::MalformedProbeOutput)
        ));
        assert!(matches!(
            select_hls_media_urls(&base, "#EXTM3U\n#EXTINF:4,\n"),
            Err(CodecProbeError::MalformedProbeOutput)
        ));
    }

    #[tokio::test]
    async fn truncated_and_invalid_media_never_yield_a_codec_verdict() {
        let fixture = include_bytes!("../tests/fixtures/codec-h264.mkv");
        assert!(probe_fixture(&fixture[..32]).await.is_err());
        assert!(probe_fixture(b"not a media container").await.is_err());
    }

    #[tokio::test]
    async fn guarded_client_does_not_follow_redirects_to_private_destinations() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_address = redirect_listener.local_addr().unwrap();
        let private_target_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let private_target_address = private_target_listener.local_addr().unwrap();

        let redirect_server = tokio::spawn(async move {
            let (mut socket, _) = redirect_listener.accept().await.unwrap();
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{private_target_address}/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let client = guarded_media_client().unwrap();
        let result = client
            .get(format!("http://{redirect_address}/redirect"))
            .send()
            .await;
        assert!(result.is_err(), "private redirect must be rejected");
        redirect_server.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), private_target_listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_https_policy_urls_fail_before_any_media_request() {
        assert_eq!(
            validate_media_url("http://example.com/video.mkv").await,
            Err(CodecProbeError::InvalidMediaUrl)
        );
        assert_eq!(
            validate_media_url("https://127.0.0.1/video.mkv").await,
            Err(CodecProbeError::InvalidMediaUrl)
        );
    }

    #[test]
    fn unknown_codec_and_missing_video_stream_are_not_verified() {
        assert_eq!(
            classify_probe_json(br#"{"streams":[{"codec_type":"video","codec_name":"mystery"}]}"#)
                .unwrap(),
            CodecProbeResult::Unsupported("mystery".into())
        );
        assert_eq!(
            classify_probe_json(br#"{"streams":[{"codec_type":"audio","codec_name":"aac"}]}"#)
                .unwrap(),
            CodecProbeResult::NoVideoStream
        );
        assert!(classify_probe_json(b"{").is_err());
    }

    #[test]
    fn mixed_video_streams_are_blocked_if_any_stream_is_hevc() {
        assert_eq!(
            classify_probe_json(
                br#"{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"video","codec_name":"hevc"}]}"#,
            )
            .unwrap(),
            CodecProbeResult::BlockedHevc("hevc".into())
        );
    }

    #[test]
    fn direct_probe_extracts_container_and_audio_codecs_from_ffprobe() {
        let (codec, format_name, audio_codecs) = classify_probe_details(
            br#"{"streams":[{"codec_type":"video","codec_name":"h264"},{"codec_type":"audio","codec_name":"aac"}],"format":{"format_name":"mov,mp4,m4a,3gp,3g2,mj2"}}"#,
        )
        .unwrap();
        assert_eq!(codec, CodecProbeResult::Allowed("h264".into()));
        assert_eq!(format_name.as_deref(), Some("mov,mp4,m4a,3gp,3g2,mj2"));
        assert_eq!(audio_codecs, vec!["aac"]);
    }

    #[test]
    fn direct_probe_requires_an_exact_initial_byte_range_response() {
        assert_eq!(validated_initial_range("bytes 0-3/8", Some(4), 4), Some(4));
        assert_eq!(validated_initial_range("bytes 0-2/8", Some(3), 4), None);
        assert_eq!(validated_initial_range("bytes 0-3/8", Some(2), 4), None);
        assert_eq!(validated_initial_range("bytes 0-3/*", Some(4), 4), None);
    }

    #[test]
    fn url_validation_rejects_local_or_non_https_media_targets() {
        assert!(crate::net::validate_url_syntax("http://example.com/video", false).is_err());
        assert!(crate::net::validate_url_syntax("https://127.0.0.1/video", false).is_err());
        assert!(crate::net::validate_url_syntax("https://[::1]/video", false).is_err());
    }

    #[cfg(unix)]
    fn write_test_executable(script: &str) -> tempfile::TempPath {
        use std::io::Write;
        let mut file = tempfile::Builder::new()
            .prefix("moviebox-test-ffprobe-")
            .tempfile()
            .unwrap();
        file.write_all(script.as_bytes()).unwrap();
        let mut permissions = file.as_file().metadata().unwrap().permissions();
        permissions.set_mode(0o700);
        file.as_file().set_permissions(permissions).unwrap();
        file.into_temp_path()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reports_missing_probe_nonzero_exit_timeout_output_limit_and_bad_json() {
        let missing = Path::new("/moviebox-test-no-such-ffprobe");
        assert_eq!(
            probe_sample_with_executable(missing, b"x", Duration::from_millis(100)).await,
            Err(CodecProbeError::ProbeUnavailable)
        );

        let nonzero = write_test_executable("#!/usr/bin/python3\nraise SystemExit(7)\n");
        assert_eq!(
            probe_sample_with_executable(&nonzero, b"x", Duration::from_secs(1)).await,
            Err(CodecProbeError::ProbeFailed)
        );

        let timeout = write_test_executable("#!/usr/bin/python3\nimport time\ntime.sleep(30)\n");
        assert_eq!(
            probe_sample_with_executable(&timeout, b"x", Duration::from_millis(30)).await,
            Err(CodecProbeError::ProbeTimeout)
        );

        let large = write_test_executable(
            "#!/usr/bin/python3\nimport sys\nsys.stdout.write('x' * 70000)\n",
        );
        assert_eq!(
            probe_sample_with_executable(&large, b"x", Duration::from_secs(1)).await,
            Err(CodecProbeError::ProbeOutputLimit)
        );

        let large_stderr = write_test_executable(
            "#!/usr/bin/python3\nimport sys\nsys.stderr.write('x' * 70000)\n",
        );
        assert_eq!(
            probe_sample_with_executable(&large_stderr, b"x", Duration::from_secs(1)).await,
            Err(CodecProbeError::ProbeOutputLimit)
        );

        let malformed = write_test_executable("#!/usr/bin/python3\nprint('{bad json')\n");
        assert_eq!(
            probe_sample_with_executable(&malformed, b"x", Duration::from_secs(1)).await,
            Err(CodecProbeError::MalformedProbeOutput)
        );
    }

    #[test]
    fn codec_allowlist_is_exact_and_excludes_hevc() {
        assert!(is_allowed_codec("h264"));
        assert!(is_allowed_codec("H.264 / AVC"));
        assert!(is_allowed_codec("H.264 High"));
        assert!(is_allowed_codec("avc1.640028"));
        assert!(!is_allowed_codec("av1"));
        assert!(!is_allowed_codec("vp9"));
        assert!(!is_allowed_codec("not-h264"));
        assert!(is_hevc_codec("H.265 / HEVC"));
        assert!(is_hevc_codec("HEVC Main 10"));
        assert!(!is_hevc_codec("not-hevc"));
        assert!(!is_allowed_codec("hevc"));
    }
}
