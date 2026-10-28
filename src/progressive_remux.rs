use std::{ffi::OsStr, io, process::Stdio, time::Duration};

use futures::StreamExt;
use reqwest::{Response, Url, header};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
    task::JoinHandle,
    time::{Instant, timeout, timeout_at},
};

const MAX_REDIRECTS: usize = 5;
const MAX_PROVIDER_HEADERS: usize = 16;
const MAX_PROVIDER_HEADER_BYTES: usize = 16 * 1024;
const MAX_PROVIDER_HEADER_VALUE_BYTES: usize = 8 * 1024;
const UPSTREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_STARTUP_BUFFER_BYTES: usize = 4 * 1024 * 1024;
const OUTPUT_CHUNK_BYTES: usize = 64 * 1024;
const INPUT_CHUNK_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpstreamFailure {
    UnsafeDestination,
    Timeout,
    RequestFailed,
    RedirectLimit,
    RejectedStatus,
    BodyRead,
    ProcessInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemuxFailure {
    StartupTimeout,
    NoPlayableFragment,
    StartupBufferLimit,
    OutputRead,
    ClientWrite,
    OverallTimeout,
    Upstream(UpstreamFailure),
    ChildWait,
    ChildFailed,
}

struct InputTask(Option<JoinHandle<Result<(), UpstreamFailure>>>);

impl InputTask {
    fn new(task: JoinHandle<Result<(), UpstreamFailure>>) -> Self {
        Self(Some(task))
    }

    async fn wait(&mut self, deadline: Instant) -> Result<(), UpstreamFailure> {
        let Some(mut task) = self.0.take() else {
            return Ok(());
        };
        match timeout_at(deadline, &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(UpstreamFailure::ProcessInput),
            Err(_) => {
                task.abort();
                Err(UpstreamFailure::Timeout)
            }
        }
    }

    async fn abort(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for InputTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Remux a validated HTTP(S) source to fragmented MP4 using OS pipes only.
///
/// Upstream bytes are requested incrementally and written directly to FFmpeg's
/// stdin. FFmpeg stdout is read in bounded chunks and copied directly to the
/// HTTP client; socket backpressure therefore propagates to FFmpeg and the
/// upstream pump. No source or output media file and no full-length in-memory
/// buffer is created by this path.
pub(crate) async fn serve(
    stream: &mut TcpStream,
    url: &Url,
    provider_headers: &[(String, String)],
    timeout_budget: Duration,
    debug_id: u64,
) -> io::Result<()> {
    serve_with_program(
        stream,
        url,
        provider_headers,
        timeout_budget,
        debug_id,
        OsStr::new("ffmpeg"),
    )
    .await
}

/// Stream a remux beginning near a validated media timestamp. The source and
/// output remain pipes; FFmpeg performs input-side seeking against the
/// validated URL and may issue bounded upstream range requests.
pub(crate) async fn serve_from_timestamp(
    stream: &mut TcpStream,
    url: &Url,
    provider_headers: &[(String, String)],
    seek_seconds: f64,
    timeout_budget: Duration,
    debug_id: u64,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout_budget;
    let response = match open_upstream(url, provider_headers, deadline).await {
        Ok(response) => response,
        Err(UpstreamFailure::UnsafeDestination) => {
            return write_error(
                stream,
                403,
                "SSRF_BLOCKED",
                "The media destination is not a permitted public URL.",
            )
            .await;
        }
        Err(UpstreamFailure::Timeout) => {
            return write_error(
                stream,
                504,
                "UPSTREAM_TIMEOUT",
                "The upstream media request timed out.",
            )
            .await;
        }
        Err(_) => {
            return write_error(
                stream,
                502,
                "UPSTREAM_UNAVAILABLE",
                "The upstream media request failed.",
            )
            .await;
        }
    };
    let mut child = match timestamp_ffmpeg_command(OsStr::new("ffmpeg"), seek_seconds).spawn() {
        Ok(child) => child,
        Err(error) => {
            log::warn!("timestamp remux process start failed id={debug_id}: {error}");
            return write_error(stream, 503, "REMUX_UNAVAILABLE", "FFmpeg is unavailable.").await;
        }
    };
    let Some(stdin) = child.stdin.take() else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return write_error(
            stream,
            503,
            "REMUX_UNAVAILABLE",
            "FFmpeg could not open the remux input stream.",
        )
        .await;
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return write_error(
            stream,
            503,
            "REMUX_UNAVAILABLE",
            "FFmpeg could not open the remux output stream.",
        )
        .await;
    };
    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut sink = tokio::io::sink();
            let _ = tokio::io::copy(&mut stderr, &mut sink).await;
        })
    });
    let input_task = tokio::spawn(pipe_upstream(response, stdin, deadline));
    let mut input_task = InputTask::new(input_task);
    let mut response_started = false;
    let result = stream_child_output(
        stream,
        &mut child,
        stdout,
        &mut input_task,
        deadline,
        &mut response_started,
    )
    .await;
    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        input_task.abort().await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }
    match result {
        Ok(()) => Ok(()),
        Err(error) if !response_started => {
            log::warn!("timestamp remux startup failed id={debug_id} category={error:?}");
            let (status, code, message) = public_startup_error(error);
            write_error(stream, status, code, message).await
        }
        Err(_) => Err(io::Error::other("progressive media stream ended early")),
    }
}

async fn serve_with_program(
    stream: &mut TcpStream,
    url: &Url,
    provider_headers: &[(String, String)],
    timeout_budget: Duration,
    debug_id: u64,
    ffmpeg: &OsStr,
) -> io::Result<()> {
    let deadline = Instant::now() + timeout_budget;
    let response = match open_upstream(url, provider_headers, deadline).await {
        Ok(response) => response,
        Err(UpstreamFailure::UnsafeDestination) => {
            return write_error(
                stream,
                403,
                "SSRF_BLOCKED",
                "The media destination is not a permitted public URL.",
            )
            .await;
        }
        Err(UpstreamFailure::Timeout) => {
            return write_error(
                stream,
                504,
                "UPSTREAM_TIMEOUT",
                "The upstream media request timed out.",
            )
            .await;
        }
        Err(_) => {
            return write_error(
                stream,
                502,
                "UPSTREAM_UNAVAILABLE",
                "The upstream media request failed.",
            )
            .await;
        }
    };

    let mut child = match progressive_ffmpeg_command(ffmpeg).spawn() {
        Ok(child) => child,
        Err(error) => {
            log::warn!("progressive remux process start failed id={debug_id}: {error}");
            return write_error(stream, 503, "REMUX_UNAVAILABLE", "FFmpeg is unavailable.").await;
        }
    };
    let Some(stdin) = child.stdin.take() else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return write_error(
            stream,
            503,
            "REMUX_UNAVAILABLE",
            "FFmpeg could not open the remux input stream.",
        )
        .await;
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return write_error(
            stream,
            503,
            "REMUX_UNAVAILABLE",
            "FFmpeg could not open the remux output stream.",
        )
        .await;
    };
    let stderr_task = child.stderr.take().map(|mut stderr| {
        tokio::spawn(async move {
            let mut sink = tokio::io::sink();
            let _ = tokio::io::copy(&mut stderr, &mut sink).await;
        })
    });
    let input_task = tokio::spawn(pipe_upstream(response, stdin, deadline));
    let mut input_task = InputTask::new(input_task);
    let mut response_started = false;

    let result = stream_child_output(
        stream,
        &mut child,
        stdout,
        &mut input_task,
        deadline,
        &mut response_started,
    )
    .await;

    if result.is_err() {
        let _ = child.kill().await;
        let _ = child.wait().await;
        input_task.abort().await;
    }
    if let Some(task) = stderr_task {
        let _ = task.await;
    }

    match result {
        Ok(()) => Ok(()),
        Err(error) if !response_started => {
            log::warn!("progressive remux startup failed id={debug_id} category={error:?}");
            let (status, code, message) = public_startup_error(error);
            write_error(stream, status, code, message).await
        }
        Err(error) => {
            // Headers have already been sent. A second status line would be
            // invalid; returning closes the connection and exposes no details.
            log::warn!("progressive remux stream ended id={debug_id} category={error:?}");
            Err(io::Error::other("progressive media stream ended early"))
        }
    }
}

fn progressive_ffmpeg_command(executable: &OsStr) -> Command {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-probesize",
            "5M",
            "-analyzeduration",
            "5M",
            "-fflags",
            "+nobuffer",
            "-protocol_whitelist",
            "pipe,crypto,data",
            "-i",
            "pipe:0",
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ac",
            "2",
            "-sn",
            "-dn",
            "-max_interleave_delta",
            "0",
            "-movflags",
            "+frag_keyframe+empty_moov+default_base_moof",
            "-flush_packets",
            "1",
            "-f",
            "mp4",
            "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

fn timestamp_ffmpeg_command(executable: &OsStr, seek_seconds: f64) -> Command {
    let mut command = Command::new(executable);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-i",
            "pipe:0",
        ])
        .args([
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-filter:v",
            &format!("trim=start={seek_seconds:.3},setpts=PTS-STARTPTS"),
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-g",
            "30",
            "-keyint_min",
            "30",
            "-force_key_frames",
            "expr:gte(t,n_forced*1)",
            "-c:a",
            "aac",
            "-filter:a",
            &format!("atrim=start={seek_seconds:.3},asetpts=PTS-STARTPTS"),
            "-b:a",
            "192k",
            "-ac",
            "2",
            "-sn",
            "-dn",
            "-avoid_negative_ts",
            "make_zero",
            "-max_interleave_delta",
            "0",
            "-movflags",
            "+frag_keyframe+empty_moov+default_base_moof",
            "-flush_packets",
            "1",
            "-f",
            "mp4",
            "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn open_upstream(
    initial_url: &Url,
    provider_headers: &[(String, String)],
    deadline: Instant,
) -> Result<Response, UpstreamFailure> {
    let allow_http = crate::config::is_test_environment();
    let initial_url = crate::net::validate_public_url(initial_url.as_str(), allow_http)
        .await
        .map_err(|_| UpstreamFailure::UnsafeDestination)?;
    if initial_url.scheme() != "https" && !allow_http {
        return Err(UpstreamFailure::UnsafeDestination);
    }
    let client = crate::net::http_client_builder_base()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| UpstreamFailure::RequestFailed)?;
    let initial_origin = initial_url.origin();
    let mut current = initial_url;

    for redirect_count in 0..=MAX_REDIRECTS {
        let validated = crate::net::validate_public_url(current.as_str(), allow_http)
            .await
            .map_err(|_| UpstreamFailure::UnsafeDestination)?;
        if validated.scheme() != "https" && !allow_http {
            return Err(UpstreamFailure::UnsafeDestination);
        }
        let same_origin = validated.origin() == initial_origin;
        let mut request = client
            .get(validated.clone())
            .header(header::ACCEPT_ENCODING, "identity");
        for (name, value) in forwarded_headers(provider_headers, same_origin) {
            request = request.header(name, value);
        }
        let response = timeout_at(deadline, request.send())
            .await
            .map_err(|_| UpstreamFailure::Timeout)?
            .map_err(|_| UpstreamFailure::RequestFailed)?;
        if !response.status().is_redirection() {
            if response.status().is_success() {
                return Ok(response);
            }
            return Err(UpstreamFailure::RejectedStatus);
        }
        if redirect_count == MAX_REDIRECTS {
            return Err(UpstreamFailure::RedirectLimit);
        }
        let location = response
            .headers()
            .get(header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(UpstreamFailure::RequestFailed)?;
        current = validated
            .join(location)
            .map_err(|_| UpstreamFailure::UnsafeDestination)?;
        if current.scheme() != "https" && !allow_http {
            return Err(UpstreamFailure::UnsafeDestination);
        }
    }
    Err(UpstreamFailure::RedirectLimit)
}

fn forwarded_headers(
    provider_headers: &[(String, String)],
    same_origin: bool,
) -> Vec<(header::HeaderName, header::HeaderValue)> {
    let mut result = Vec::new();
    let mut total_bytes = 0usize;
    for (name, value) in provider_headers.iter().take(MAX_PROVIDER_HEADERS) {
        if value.len() > MAX_PROVIDER_HEADER_VALUE_BYTES
            || name.contains(['\r', '\n'])
            || value.contains(['\r', '\n'])
        {
            continue;
        }
        let Ok(name) = header::HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        if matches!(
            name.as_str(),
            "host"
                | "connection"
                | "content-length"
                | "transfer-encoding"
                | "range"
                | "accept-encoding"
                | "proxy-authorization"
        ) {
            continue;
        }
        let safe_value = if same_origin || name == header::USER_AGENT {
            Some(value.clone())
        } else if name == header::REFERER {
            sanitize_referer_origin(value)
        } else {
            None
        };
        let Some(value) = safe_value else {
            continue;
        };
        let Some(next_total) = total_bytes.checked_add(name.as_str().len() + value.len()) else {
            break;
        };
        if next_total > MAX_PROVIDER_HEADER_BYTES {
            break;
        }
        let Ok(value) = header::HeaderValue::from_str(&value) else {
            continue;
        };
        total_bytes = next_total;
        result.push((name, value));
    }
    result
}

fn sanitize_referer_origin(value: &str) -> Option<String> {
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

async fn pipe_upstream(
    response: Response,
    mut stdin: tokio::process::ChildStdin,
    deadline: Instant,
) -> Result<(), UpstreamFailure> {
    let mut body = response.bytes_stream();
    loop {
        let next = timeout(UPSTREAM_IDLE_TIMEOUT, timeout_at(deadline, body.next()))
            .await
            .map_err(|_| UpstreamFailure::Timeout)?
            .map_err(|_| UpstreamFailure::Timeout)?;
        let Some(next) = next else { break };
        let chunk = next.map_err(|_| UpstreamFailure::BodyRead)?;
        for piece in chunk.chunks(INPUT_CHUNK_BYTES) {
            timeout_at(deadline, stdin.write_all(piece))
                .await
                .map_err(|_| UpstreamFailure::Timeout)?
                .map_err(|_| UpstreamFailure::ProcessInput)?;
        }
    }
    timeout_at(deadline, stdin.shutdown())
        .await
        .map_err(|_| UpstreamFailure::Timeout)?
        .map_err(|_| UpstreamFailure::ProcessInput)
}

async fn stream_child_output(
    stream: &mut TcpStream,
    child: &mut Child,
    mut stdout: tokio::process::ChildStdout,
    input_task: &mut InputTask,
    deadline: Instant,
    response_started: &mut bool,
) -> Result<(), RemuxFailure> {
    let startup_deadline = std::cmp::min(deadline, Instant::now() + STARTUP_TIMEOUT);
    let mut startup = Vec::with_capacity(128 * 1024);
    let mut chunk = [0u8; OUTPUT_CHUNK_BYTES];
    let first_fragment = loop {
        let length = match timeout_at(
            startup_deadline,
            read_output_or_peer_close(stream, &mut stdout, &mut chunk),
        )
        .await
        {
            Ok(Ok(0)) => return Err(RemuxFailure::NoPlayableFragment),
            Ok(Ok(length)) => length,
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => {
                return Err(RemuxFailure::ClientWrite);
            }
            Ok(Err(_)) => return Err(RemuxFailure::OutputRead),
            Err(_) if startup_deadline >= deadline => return Err(RemuxFailure::OverallTimeout),
            Err(_) => return Err(RemuxFailure::StartupTimeout),
        };
        startup.extend_from_slice(&chunk[..length]);
        if has_playable_fragment(&startup) {
            break startup;
        }
        if startup.len() > MAX_STARTUP_BUFFER_BYTES {
            return Err(RemuxFailure::StartupBufferLimit);
        }
    };

    *response_started = true;
    let header_bytes = b"HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nCache-Control: no-cache\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, HEAD, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Accept, Content-Type, Authorization, Range, X-Requested-With\r\nAccess-Control-Expose-Headers: Accept-Ranges, Content-Type, X-Media-Mode, X-Remux-Cache-State\r\nAccess-Control-Max-Age: 86400\r\nAccept-Ranges: none\r\nX-Media-Mode: remux\r\nX-Remux-Cache-State: disabled\r\n\r\n";
    timeout_at(deadline, stream.write_all(header_bytes))
        .await
        .map_err(|_| RemuxFailure::OverallTimeout)?
        .map_err(|_| RemuxFailure::ClientWrite)?;
    timeout_at(deadline, stream.write_all(&first_fragment))
        .await
        .map_err(|_| RemuxFailure::OverallTimeout)?
        .map_err(|_| RemuxFailure::ClientWrite)?;

    loop {
        let length = match timeout_at(
            deadline,
            read_output_or_peer_close(stream, &mut stdout, &mut chunk),
        )
        .await
        {
            Ok(Ok(0)) => break,
            Ok(Ok(length)) => length,
            Ok(Err(error)) if error.kind() == io::ErrorKind::BrokenPipe => {
                return Err(RemuxFailure::ClientWrite);
            }
            Ok(Err(_)) => return Err(RemuxFailure::OutputRead),
            Err(_) => return Err(RemuxFailure::OverallTimeout),
        };
        timeout_at(deadline, stream.write_all(&chunk[..length]))
            .await
            .map_err(|_| RemuxFailure::OverallTimeout)?
            .map_err(|_| RemuxFailure::ClientWrite)?;
    }

    let status = match timeout_at(deadline, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => return Err(RemuxFailure::ChildWait),
        Err(_) => return Err(RemuxFailure::OverallTimeout),
    };
    if !status.success() {
        return Err(RemuxFailure::ChildFailed);
    }
    input_task
        .wait(deadline)
        .await
        .map_err(RemuxFailure::Upstream)
}

fn has_playable_fragment(bytes: &[u8]) -> bool {
    let mut offset = 0usize;
    let mut saw_complete_moof = false;
    loop {
        let remaining = bytes.len().saturating_sub(offset);
        if remaining < 8 {
            return false;
        }
        let size32 = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let kind = &bytes[offset + 4..offset + 8];
        let (header_size, box_size) = match size32 {
            0 => {
                return kind == b"mdat" && saw_complete_moof && remaining > 8;
            }
            1 => {
                if remaining < 16 {
                    return false;
                }
                let size64 = u64::from_be_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
                (16usize, size64)
            }
            size => (8usize, u64::from(size)),
        };
        if box_size < header_size as u64 {
            return false;
        }
        let Some(box_size) = usize::try_from(box_size).ok() else {
            return false;
        };
        if kind == b"mdat" && saw_complete_moof {
            return remaining > header_size;
        }
        let Some(box_end) = offset.checked_add(box_size) else {
            return false;
        };
        if box_end > bytes.len() {
            return false;
        }
        if kind == b"moof" {
            saw_complete_moof = true;
        }
        offset = box_end;
    }
}

async fn read_output_or_peer_close(
    stream: &TcpStream,
    stdout: &mut tokio::process::ChildStdout,
    buffer: &mut [u8],
) -> io::Result<usize> {
    tokio::select! {
        result = stdout.read(buffer) => result,
        _ = wait_for_peer_close(stream) => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
    }
}

async fn wait_for_peer_close(stream: &TcpStream) -> io::Result<()> {
    let mut discard = [0u8; 1024];
    loop {
        stream.readable().await?;
        match stream.try_read(&mut discard) {
            Ok(0) => return Ok(()),
            Ok(_) => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
}

fn public_startup_error(error: RemuxFailure) -> (u16, &'static str, &'static str) {
    match error {
        RemuxFailure::StartupTimeout | RemuxFailure::OverallTimeout => (
            504,
            "REMUX_STARTUP_TIMEOUT",
            "The media source took too long to start.",
        ),
        RemuxFailure::Upstream(UpstreamFailure::Timeout) => (
            504,
            "UPSTREAM_TIMEOUT",
            "The upstream media request timed out.",
        ),
        _ => (
            502,
            "REMUX_STARTUP_FAILED",
            "The media source could not be prepared for playback.",
        ),
    }
}

async fn write_error(
    stream: &mut TcpStream,
    status: u16,
    code: &str,
    message: &str,
) -> io::Result<()> {
    let reason = match status {
        403 => "Forbidden",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Error",
    };
    let body = format!("{{\"error\":{{\"code\":\"{code}\",\"message\":\"{message}\"}}}}\n");
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\nAccess-Control-Allow-Origin: *\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt},
        net::TcpListener,
    };

    async fn generated_matroska() -> Vec<u8> {
        let output = tokio::process::Command::new("ffmpeg")
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=640x360:rate=12",
                "-t",
                "12",
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
                "-g",
                "12",
                "-pix_fmt",
                "yuv420p",
                "-an",
                "-f",
                "matroska",
                "pipe:1",
            ])
            .output()
            .await
            .expect("FFmpeg is installed for progressive-remux tests");
        assert!(
            output.status.success(),
            "FFmpeg fixture generation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output.stdout
    }

    async fn mock_source(
        body: Vec<u8>,
        chunk_bytes: usize,
        delay: Duration,
    ) -> (Url, Arc<AtomicBool>, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let completed = Arc::new(AtomicBool::new(false));
        let completed_server = Arc::clone(&completed);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut reader = tokio::io::BufReader::new(&mut socket);
            let mut request_line = String::new();
            let _ = reader.read_line(&mut request_line).await;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).await.unwrap_or(0) == 0 || line == "\r\n" {
                    break;
                }
            }
            drop(reader);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: video/x-matroska\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            if socket.write_all(response.as_bytes()).await.is_err() {
                return;
            }
            for chunk in body.chunks(chunk_bytes.max(1)) {
                if socket.write_all(chunk).await.is_err() {
                    return;
                }
                let _ = socket.flush().await;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            completed_server.store(true, Ordering::Release);
        });
        (
            Url::parse(&format!("http://{address}/media.mkv")).unwrap(),
            completed,
            task,
        )
    }

    async fn downstream_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    async fn response_header_and_initial_body(client: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let length = timeout(Duration::from_secs(15), client.read(&mut buffer))
                .await
                .expect("response headers timed out")
                .expect("read response");
            assert_ne!(length, 0, "server closed before response headers");
            bytes.extend_from_slice(&buffer[..length]);
            if let Some(boundary) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let body = bytes.split_off(boundary + 4);
                let headers = String::from_utf8(bytes).unwrap();
                return (headers, body);
            }
            assert!(bytes.len() < 64 * 1024, "oversized response headers");
        }
    }

    #[test]
    fn ffmpeg_reads_and_writes_pipes_with_fragmented_mp4_flags() {
        let command = progressive_ffmpeg_command(OsStr::new("ffmpeg"));
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.windows(2).any(|pair| pair == ["-i", "pipe:0"]));
        assert!(args.windows(2).any(|pair| pair == ["-f", "mp4"]));
        assert!(args.last().is_some_and(|arg| arg == "pipe:1"));
        assert!(
            args.iter()
                .any(|arg| arg == "+frag_keyframe+empty_moov+default_base_moof")
        );
        assert!(
            !args
                .iter()
                .any(|arg| arg.contains(".mp4") || arg.contains(".mkv"))
        );
        assert_eq!(MAX_STARTUP_BUFFER_BYTES, 4 * 1024 * 1024);
        assert_eq!(OUTPUT_CHUNK_BYTES, 64 * 1024);
    }

    #[test]
    fn timestamp_command_trims_pipe_input_and_uses_pipe_output() {
        let command = timestamp_ffmpeg_command(OsStr::new("ffmpeg"), 120.5);
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let input = args.iter().position(|arg| arg == "-i").unwrap();
        let video_filter = args.iter().position(|arg| arg == "-filter:v").unwrap();
        let audio_filter = args.iter().position(|arg| arg == "-filter:a").unwrap();
        assert!(input < video_filter);
        assert_eq!(
            args[video_filter + 1],
            "trim=start=120.500,setpts=PTS-STARTPTS"
        );
        assert_eq!(
            args[audio_filter + 1],
            "atrim=start=120.500,asetpts=PTS-STARTPTS"
        );
        assert!(args.windows(2).any(|pair| pair == ["-g", "30"]));
        assert!(args.windows(2).any(|pair| pair == ["-keyint_min", "30"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["-force_key_frames", "expr:gte(t,n_forced*1)"])
        );
        assert_eq!(args[input + 1], "pipe:0");
        assert_eq!(args.last().map(String::as_str), Some("pipe:1"));
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "pipe:0").count(),
            1
        );
    }

    #[test]
    fn provider_headers_are_bounded_and_sensitive_headers_are_not_forwarded_across_origins() {
        let headers = vec![
            ("Authorization".into(), "secret-token".into()),
            ("User-Agent".into(), "player".into()),
            (
                "Referer".into(),
                "https://origin.example/path?secret=x".into(),
            ),
            ("Host".into(), "attacker.example".into()),
            ("X-Bad\r\nHeader".into(), "x".into()),
        ];
        let forwarded = forwarded_headers(&headers, false);
        assert_eq!(forwarded.len(), 2);
        assert!(forwarded.iter().any(|(name, _)| name == header::USER_AGENT));
        let referer = forwarded
            .iter()
            .find(|(name, _)| name == header::REFERER)
            .unwrap()
            .1
            .to_str()
            .unwrap();
        assert_eq!(referer, "https://origin.example/");
        assert!(
            forwarded
                .iter()
                .all(|(name, _)| name != header::AUTHORIZATION)
        );
    }

    #[test]
    fn startup_gate_requires_a_complete_moof_and_mdat_payload_byte() {
        fn atom(kind: &[u8; 4], payload: &[u8]) -> Vec<u8> {
            let size = u32::try_from(payload.len() + 8).unwrap();
            [size.to_be_bytes().as_slice(), kind, payload].concat()
        }

        let mut prefix = atom(b"ftyp", b"");
        prefix.extend_from_slice(&atom(b"moov", b""));
        prefix.extend_from_slice(&atom(b"moof", b""));
        assert!(!has_playable_fragment(&prefix));

        prefix.extend_from_slice(&atom(b"mdat", b""));
        assert!(!has_playable_fragment(&prefix));
        prefix.push(0x01);
        assert!(has_playable_fragment(&prefix));
    }

    #[tokio::test]
    async fn first_fragment_is_delivered_before_upstream_completes_and_body_is_incremental() {
        let media = generated_matroska().await;
        let (url, upstream_complete, upstream_task) =
            mock_source(media, 4096, Duration::from_millis(35)).await;
        let (mut client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve(&mut server, &url, &[], Duration::from_secs(30), 7001).await
        });
        let (headers, initial_body) = response_header_and_initial_body(&mut client).await;
        assert!(headers.starts_with("HTTP/1.1 200 OK"));
        assert!(headers.contains("Content-Type: video/mp4"));
        assert!(headers.contains("Accept-Ranges: none"));
        assert!(!headers.to_ascii_lowercase().contains("content-length:"));
        assert!(!headers.to_ascii_lowercase().contains("transfer-encoding:"));
        assert!(initial_body.windows(4).any(|atom| atom == b"moof"));
        assert!(!upstream_complete.load(Ordering::Acquire));

        let mut response_body = initial_body;
        let mut read_count = 0usize;
        let mut buffer = [0u8; 8192];
        loop {
            let length = timeout(Duration::from_secs(30), client.read(&mut buffer))
                .await
                .expect("progressive body stalled")
                .expect("read progressive body");
            if length == 0 {
                break;
            }
            read_count += 1;
            response_body.extend_from_slice(&buffer[..length]);
        }
        assert!(read_count >= 2, "media did not arrive in multiple reads");
        assert!(response_body.windows(4).any(|atom| atom == b"ftyp"));
        assert!(response_body.windows(4).any(|atom| atom == b"moov"));
        assert!(response_body.windows(4).any(|atom| atom == b"mdat"));
        serve_task.await.unwrap().unwrap();
        upstream_task.await.unwrap();
        assert!(upstream_complete.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn client_disconnect_during_startup_cancels_the_upstream_request() {
        let media = generated_matroska().await;
        let (url, upstream_complete, upstream_task) =
            mock_source(media, 4096, Duration::from_millis(35)).await;
        let (client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve(&mut server, &url, &[], Duration::from_secs(20), 7007).await
        });
        drop(client);

        let _handler_result = timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("disconnected playback handler did not stop")
            .unwrap();
        timeout(Duration::from_secs(5), upstream_task)
            .await
            .expect("upstream request remained open after downstream disconnect")
            .unwrap();
        assert!(
            !upstream_complete.load(Ordering::Acquire),
            "upstream should be cancelled before the full source is sent"
        );
    }

    #[tokio::test]
    async fn upstream_http_failure_is_safe_and_does_not_leak_response_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 12\r\nConnection: close\r\n\r\nsecret-token")
                .await
                .unwrap();
        });
        let url = Url::parse(&format!("http://{address}/private")).unwrap();
        let (mut client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve(&mut server, &url, &[], Duration::from_secs(5), 7002).await
        });
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 502 Bad Gateway"));
        assert!(response.contains("UPSTREAM_UNAVAILABLE"));
        assert!(!response.contains("secret-token"));
        serve_task.await.unwrap().unwrap();
        upstream.await.unwrap();
    }

    #[tokio::test]
    async fn missing_ffmpeg_fails_before_media_headers_and_cleans_up() {
        let (url, _, upstream_task) =
            mock_source(b"small-source".to_vec(), 64, Duration::ZERO).await;
        let (mut client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve_with_program(
                &mut server,
                &url,
                &[],
                Duration::from_secs(5),
                7003,
                OsStr::new("/nonexistent/test-ffmpeg"),
            )
            .await
        });
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 503 Service Unavailable"));
        serve_task.await.unwrap().unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test]
    async fn invalid_or_early_exit_media_fails_before_playable_headers() {
        let (url, _, upstream_task) =
            mock_source(b"not a video".to_vec(), 64, Duration::ZERO).await;
        let (mut client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve(&mut server, &url, &[], Duration::from_secs(10), 7004).await
        });
        let mut response = Vec::new();
        timeout(Duration::from_secs(10), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 502 Bad Gateway"));
        assert!(!response.contains("Content-Type: video/mp4"));
        serve_task.await.unwrap().unwrap();
        upstream_task.await.unwrap();
    }

    #[tokio::test]
    async fn stalled_source_obeys_overall_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2048];
            let _ = socket.read(&mut request).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\nConnection: close\r\n\r\npartial")
                .await;
            tokio::time::sleep(Duration::from_secs(2)).await;
        });
        let url = Url::parse(&format!("http://{address}/stalled.mkv")).unwrap();
        let (mut client, mut server) = downstream_pair().await;
        let serve_task = tokio::spawn(async move {
            serve(&mut server, &url, &[], Duration::from_millis(150), 7005).await
        });
        let mut response = Vec::new();
        timeout(Duration::from_secs(3), client.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 504 Gateway Timeout"));
        serve_task.await.unwrap().unwrap();
        upstream.abort();
    }

    #[test]
    fn pipe_limits_and_no_file_arguments_are_explicit() {
        assert_eq!(INPUT_CHUNK_BYTES, 32 * 1024);
        assert_eq!(OUTPUT_CHUNK_BYTES, 64 * 1024);
        let command = progressive_ffmpeg_command(OsStr::new("ffmpeg"));
        let args = command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "pipe:0").count(),
            1
        );
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "pipe:1").count(),
            1
        );
    }
}
