use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    process::{Child, Command},
    sync::Mutex,
};

const SESSION_TTL: Duration = Duration::from_secs(600);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(45);
const MAX_SESSIONS: usize = 32;

struct Session {
    dir: PathBuf,
    child: Arc<Mutex<Child>>,
    last_access: Instant,
}

#[derive(Clone)]
pub struct HlsManager {
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    root: Arc<PathBuf>,
}

impl Default for HlsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl HlsManager {
    pub fn new() -> Self {
        let root = std::env::temp_dir().join("moviebox-tui").join("hls");
        let _ = std::fs::create_dir_all(&root);
        let manager = Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            root: Arc::new(root),
        };
        let cleaner = manager.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                cleaner.cleanup_expired().await;
            }
        });
        manager
    }

    pub async fn start(&self, url: &str, headers: &[(String, String)]) -> Result<String, String> {
        {
            let sessions = self.sessions.lock().await;
            if sessions.len() >= MAX_SESSIONS {
                return Err("HLS session capacity is full.".into());
            }
        }
        let id = format!("hls-{}-{}", std::process::id(), uuid_like());
        let dir = self.root.join(&id);
        tokio::fs::create_dir_all(&dir)
            .await
            .map_err(|e| format!("HLS temp directory: {e}"))?;
        let header_block = headers
            .iter()
            .map(|(n, v)| format!("{n}: {v}\r\n"))
            .collect::<String>();
        let playlist = dir.join("index.m3u8");
        let segment_pattern = dir.join("segment_%06d.ts");
        let mut command = Command::new("ffmpeg");
        command.args(["-hide_banner", "-loglevel", "error", "-nostdin"]);
        if !header_block.is_empty() {
            command.args(["-headers", &header_block]);
        }
        command.args([
            "-rw_timeout",
            "15000000",
            "-i",
            url,
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-c",
            "copy",
            "-f",
            "hls",
            "-hls_time",
            "4",
            "-hls_list_size",
            "6",
            "-hls_flags",
            "delete_segments+append_list+independent_segments",
            "-hls_segment_filename",
        ]);
        command.arg(&segment_pattern).arg(&playlist);
        command.kill_on_drop(true);
        let child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(&dir).await;
                return Err(format!("FFmpeg is unavailable: {e}"));
            }
        };
        let child = Arc::new(Mutex::new(child));
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while Instant::now() < deadline {
            if tokio::fs::metadata(&playlist).await.is_ok() {
                break;
            }
            if child.lock().await.try_wait().ok().flatten().is_some() {
                let _ = tokio::fs::remove_dir_all(&dir).await;
                return Err("FFmpeg could not create an HLS manifest.".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if tokio::fs::metadata(&playlist).await.is_err() {
            let _ = child.lock().await.kill().await;
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return Err("HLS manifest startup timed out.".into());
        }
        self.sessions.lock().await.insert(
            id.clone(),
            Session {
                dir,
                child,
                last_access: Instant::now(),
            },
        );
        Ok(id)
    }

    pub async fn manifest(&self, id: &str) -> Result<String, String> {
        let (dir, _) = self.touch(id).await?;
        let text = tokio::fs::read_to_string(dir.join("index.m3u8"))
            .await
            .map_err(|e| format!("HLS manifest unavailable: {e}"))?;
        Ok(text
            .lines()
            .map(|line| {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    line.to_string()
                } else {
                    format!("/api/v1/hls/{id}/{trimmed}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n")
    }

    pub async fn segment(&self, id: &str, name: &str) -> Result<(String, Vec<u8>), String> {
        if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
            return Err("invalid HLS segment name".into());
        }
        let (dir, _) = self.touch(id).await?;
        let path = dir.join(name);
        let data = tokio::fs::read(&path)
            .await
            .map_err(|_| "HLS segment is not available yet.".to_string())?;
        let ct = if name.ends_with(".ts") {
            "video/mp2t"
        } else if name.ends_with(".m4s") {
            "video/iso.segment"
        } else {
            "application/octet-stream"
        };
        Ok((ct.into(), data))
    }

    async fn touch(&self, id: &str) -> Result<(PathBuf, Arc<Mutex<Child>>), String> {
        let mut sessions = self.sessions.lock().await;
        let session = sessions
            .get_mut(id)
            .ok_or_else(|| "HLS session not found or expired.".to_string())?;
        session.last_access = Instant::now();
        Ok((session.dir.clone(), Arc::clone(&session.child)))
    }

    async fn cleanup_expired(&self) {
        let expired = {
            let mut sessions = self.sessions.lock().await;
            let ids = sessions
                .iter()
                .filter(|(_, s)| s.last_access.elapsed() > SESSION_TTL)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| sessions.remove(&id))
                .collect::<Vec<_>>()
        };
        for session in expired {
            let _ = session.child.lock().await.kill().await;
            let _ = tokio::fs::remove_dir_all(session.dir).await;
        }
    }
}

fn uuid_like() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}

pub fn is_allowed_codec(codec: Option<&str>) -> bool {
    let Some(codec) = codec else {
        return false;
    };
    let codec = codec.to_ascii_lowercase();
    codec.contains("h264") || codec.contains("avc") || codec.contains("x264")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;

    #[test]
    fn codec_policy_rejects_unknown_hevc_and_av1() {
        assert!(is_allowed_codec(Some("h264")));
        assert!(is_allowed_codec(Some("avc1")));
        assert!(!is_allowed_codec(Some("hevc")));
        assert!(!is_allowed_codec(Some("av1")));
        assert!(!is_allowed_codec(None));
    }

    #[tokio::test]
    async fn generated_hls_manifest_and_segment_are_real_files() {
        if Command::new("ffmpeg")
            .arg("-version")
            .output()
            .await
            .is_err()
        {
            return;
        }
        let fixture =
            std::env::temp_dir().join(format!("moviebox-hls-fixture-{}.mp4", uuid_like()));
        let output = Command::new("ffmpeg")
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "testsrc=size=320x180:rate=25",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000",
                "-t",
                "2",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "aac",
                "-shortest",
            ])
            .arg(&fixture)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .await
            .unwrap();
        if !output.status.success() {
            return;
        }
        let manager = HlsManager::new();
        let id = manager.start(fixture.to_str().unwrap(), &[]).await.unwrap();
        let manifest = manager.manifest(&id).await.unwrap();
        let segment = manifest
            .lines()
            .find(|line| !line.starts_with('#') && !line.is_empty())
            .unwrap();
        let name = segment.rsplit('/').next().unwrap();
        let (mime, bytes) = manager.segment(&id, name).await.unwrap();
        assert_eq!(mime, "video/mp2t");
        assert!(!bytes.is_empty());
        assert!(manifest.contains("/api/v1/hls/"));
        let _ = tokio::fs::remove_file(fixture).await;
    }
}
