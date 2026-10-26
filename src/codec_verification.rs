use std::{process::Stdio, time::Duration};

use futures::StreamExt;
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
};

use crate::providers::models::{PlaybackSource, ProviderKind};

const SAMPLE_LIMIT: usize = 32 * 1024 * 1024;
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
}

#[derive(Debug, Deserialize)]
struct ProbeStream {
    codec_type: Option<String>,
    codec_name: Option<String>,
}

pub async fn verify_playback_source(
    source: &PlaybackSource,
) -> Result<CodecProbeResult, CodecProbeError> {
    if source.provider != ProviderKind::FourKHdHub {
        return Err(CodecProbeError::UnsupportedProvider);
    }

    let url = validate_media_url(&source.url).await?;
    let sample = tokio::time::timeout(DOWNLOAD_TIMEOUT, download_sample(&url, &source.headers))
        .await
        .map_err(|_| CodecProbeError::DownloadTimeout)??;
    if sample.is_empty() {
        return Err(CodecProbeError::EmptyMedia);
    }

    let ffprobe = ffprobe_executable().ok_or(CodecProbeError::ProbeUnavailable)?;
    probe_sample_with_executable(&ffprobe, &sample, PROBE_TIMEOUT).await
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
) -> Result<Vec<u8>, CodecProbeError> {
    let client = guarded_media_client()?;
    let mut request = client.get(url.clone()).header(
        reqwest::header::RANGE,
        format!("bytes=0-{}", SAMPLE_LIMIT - 1),
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
    if !response.status().is_success() {
        return Err(CodecProbeError::MediaUnavailable);
    }
    crate::net::validate_public_url(response.url().as_str(), false)
        .await
        .map_err(|_| CodecProbeError::UnsafeRedirect)?;
    if let Some(content_range) = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        && !content_range.to_ascii_lowercase().starts_with("bytes 0-")
    {
        return Err(CodecProbeError::MediaUnavailable);
    }

    let mut stream = response.bytes_stream();
    let mut sample = Vec::with_capacity(SAMPLE_LIMIT.min(1024 * 1024));
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|_| CodecProbeError::MediaUnavailable)?;
        let remaining = SAMPLE_LIMIT.saturating_sub(sample.len());
        if chunk.len() > remaining {
            sample.extend_from_slice(&chunk[..remaining]);
            break;
        }
        sample.extend_from_slice(&chunk);
        if sample.len() == SAMPLE_LIMIT {
            break;
        }
    }
    Ok(sample)
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
            "stream=codec_type,codec_name",
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
        classify_probe_json(&stdout_bytes)
    };

    tokio::time::timeout(timeout, run)
        .await
        .map_err(|_| CodecProbeError::ProbeTimeout)?
}

fn classify_probe_json(bytes: &[u8]) -> Result<CodecProbeResult, CodecProbeError> {
    let output: ProbeOutput =
        serde_json::from_slice(bytes).map_err(|_| CodecProbeError::MalformedProbeOutput)?;
    let video_codecs = output
        .streams
        .unwrap_or_default()
        .into_iter()
        .filter(|stream| stream.codec_type.as_deref() == Some("video"))
        .filter_map(|stream| stream.codec_name)
        .map(|codec| codec.trim().to_ascii_lowercase())
        .filter(|codec| !codec.is_empty())
        .collect::<Vec<_>>();
    if video_codecs.is_empty() {
        return Ok(CodecProbeResult::NoVideoStream);
    }
    if let Some(codec) = video_codecs.iter().find(|codec| is_hevc_codec(codec)) {
        return Ok(CodecProbeResult::BlockedHevc(codec.clone()));
    }
    if let Some(codec) = video_codecs.iter().find(|codec| !is_allowed_codec(codec)) {
        return Ok(CodecProbeResult::Unsupported(codec.clone()));
    }
    Ok(CodecProbeResult::Allowed(video_codecs[0].clone()))
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
