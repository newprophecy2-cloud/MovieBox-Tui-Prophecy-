use crate::unified::normalize_language_code;
use base64::Engine as _;
use serde::Deserialize;
use std::collections::HashMap;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const MAX_SAMPLE_SECONDS: u64 = 30;
const MAX_WAV_BYTES: usize = 2 * 1024 * 1024; // 30s * 16kHz * 2 bytes ~= 960 KB

pub const DEFAULT_SUPPORTED_LANGUAGES: &[&str] = &[
    "hi", "en", "ja", "bn", "ta", "te", "ml", "kn", "mr", "ko", "es", "fr", "de",
];

#[derive(Debug, Clone)]
pub struct AudioIdConfig {
    pub sample_seconds: u64,
    pub model_url: Option<String>,
    pub model_api_key: Option<String>,
    pub gemini_api_key: Option<String>,
    pub gemini_model: String,
    pub min_confidence: f32,
    pub supported_languages: Vec<String>,
}

impl AudioIdConfig {
    pub fn from_env() -> Self {
        let sample_seconds = std::env::var("PV_AUDIO_SAMPLE_SECONDS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(|v| v.clamp(5, MAX_SAMPLE_SECONDS))
            .unwrap_or(MAX_SAMPLE_SECONDS);
        let model_url = std::env::var("PV_AUDIO_LANG_ID_URL")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let model_api_key = std::env::var("PV_AUDIO_LANG_ID_API_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let gemini_api_key = std::env::var("GEMINI_API_KEY")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        let gemini_model = std::env::var("PV_GEMINI_AUDIO_MODEL")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| "gemini-2.5-flash".to_string());
        let min_confidence = std::env::var("PV_AUDIO_MIN_CONFIDENCE")
            .ok()
            .and_then(|v| v.trim().parse::<f32>().ok())
            .map(|v| v.clamp(0.5, 0.99))
            .unwrap_or(0.75);
        let supported_languages = std::env::var("PV_AUDIO_SUPPORTED_LANGUAGES")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(|s| normalize_language_code(s.trim()))
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                DEFAULT_SUPPORTED_LANGUAGES
                    .iter()
                    .map(|s| (*s).to_string())
                    .collect()
            });

        Self {
            sample_seconds,
            model_url,
            model_api_key,
            gemini_api_key,
            gemini_model,
            min_confidence,
            supported_languages,
        }
    }

    pub fn is_configured(&self) -> bool {
        self.model_url.is_some() || self.gemini_api_key.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FfprobeAudioStream {
    pub track_index: usize,
    pub provider_track_id: Option<String>,
    pub audio_codec: Option<String>,
    pub channels: Option<String>,
    pub sample_rate: Option<String>,
    pub is_default: Option<bool>,
    pub declared_language_code: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FfprobeOutput {
    #[serde(default)]
    streams: Vec<FfprobeStreamEntry>,
}

#[derive(Debug, Deserialize)]
struct FfprobeDisposition {
    #[serde(default)]
    default: i32,
}

#[derive(Debug, Deserialize)]
struct FfprobeStreamEntry {
    index: Option<usize>,
    codec_name: Option<String>,
    channels: Option<u32>,
    sample_rate: Option<String>,
    disposition: Option<FfprobeDisposition>,
    #[serde(default)]
    tags: HashMap<String, String>,
}

pub fn parse_ffprobe_audio_json(json_str: &str) -> Vec<FfprobeAudioStream> {
    let Ok(parsed) = serde_json::from_str::<FfprobeOutput>(json_str) else {
        return Vec::new();
    };
    parsed
        .streams
        .into_iter()
        .enumerate()
        .map(|(idx, s)| {
            let lang_tag = s
                .tags
                .get("language")
                .or_else(|| s.tags.get("LANGUAGE"))
                .map(String::as_str)
                .filter(|v| !v.eq_ignore_ascii_case("und"))
                .and_then(normalize_language_code);
            FfprobeAudioStream {
                track_index: idx,
                provider_track_id: s.index.map(|i| i.to_string()),
                audio_codec: s.codec_name,
                channels: s.channels.map(|c| c.to_string()),
                sample_rate: s.sample_rate,
                is_default: s.disposition.map(|d| d.default != 0),
                declared_language_code: lang_tag,
            }
        })
        .collect()
}

pub async fn extract_ffprobe_audio_streams(
    stream_url: &str,
    headers: &HashMap<String, String>,
) -> Vec<FfprobeAudioStream> {
    if crate::net::validate_public_url(stream_url, true)
        .await
        .is_err()
    {
        return Vec::new();
    }
    extract_ffprobe_audio_streams_raw(stream_url, headers).await
}

async fn extract_ffprobe_audio_streams_raw(
    input: &str,
    headers: &HashMap<String, String>,
) -> Vec<FfprobeAudioStream> {
    let mut cmd = Command::new("ffprobe");
    cmd.arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg("-select_streams")
        .arg("a");
    if !headers.is_empty() {
        let mut hdr_str = String::new();
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("referer")
                || k.eq_ignore_ascii_case("user-agent")
                || k.eq_ignore_ascii_case("origin")
            {
                hdr_str.push_str(&format!("{k}: {v}\r\n"));
            }
        }
        if !hdr_str.is_empty() {
            cmd.arg("-headers").arg(hdr_str);
        }
    }
    cmd.arg(input).stdout(Stdio::piped()).stderr(Stdio::null());

    let Ok(out) = tokio::time::timeout(Duration::from_secs(6), cmd.output()).await else {
        return Vec::new();
    };
    let Ok(output) = out else {
        return Vec::new();
    };
    if !output.status.success() {
        return Vec::new();
    }
    parse_ffprobe_audio_json(&String::from_utf8_lossy(&output.stdout))
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpeechIdResult {
    pub language_code: Option<String>,
    pub confidence: Option<f32>,
    pub sample_duration_seconds: Option<f32>,
    pub detection_method: String,
    pub verification_status: String,
}

#[derive(Debug, Deserialize)]
pub struct ExternalLangIdResponse {
    pub language_code: Option<String>,
    pub confidence: Option<f32>,
    #[serde(default)]
    pub speech_detected: Option<bool>,
    #[serde(default)]
    pub mixed_languages: Option<bool>,
    #[serde(default)]
    pub duration_seconds: Option<f32>,
}

/// Evaluates raw 16-bit signed PCM WAV bytes to reject silence or near-silent samples.
pub fn measure_wav_rms_and_duration(wav_bytes: &[u8]) -> Option<(f32, f32)> {
    if wav_bytes.len() <= 44 {
        return None;
    }
    let data_offset = wav_bytes
        .windows(4)
        .position(|w| w == b"data")
        .map(|pos| pos + 8)
        .filter(|&off| off < wav_bytes.len())
        .unwrap_or(44);
    let pcm = &wav_bytes[data_offset..];
    let sample_count = pcm.len() / 2;
    if sample_count == 0 {
        return None;
    }
    let mut sum_sq: f64 = 0.0;
    for chunk in pcm.chunks_exact(2) {
        let s = i16::from_le_bytes([chunk[0], chunk[1]]) as f64 / 32768.0;
        sum_sq += s * s;
    }
    let rms = (sum_sq / sample_count as f64).sqrt() as f32;
    let duration_secs = sample_count as f32 / 16000.0;
    Some((rms, duration_secs))
}

/// Pure classification evaluator for model output, enforcing all verification rules:
/// - Rejects silence / no speech / music-only
/// - Rejects mixed-language samples
/// - Rejects confidence below threshold
/// - Accepts only verified Hindi ("hi"), English ("en"), or Japanese ("ja")
pub fn evaluate_model_prediction(
    pred: ExternalLangIdResponse,
    measured_duration: Option<f32>,
    min_confidence: f32,
) -> SpeechIdResult {
    let default_langs: Vec<String> = DEFAULT_SUPPORTED_LANGUAGES
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    evaluate_model_prediction_with_languages(
        pred,
        measured_duration,
        min_confidence,
        &default_langs,
    )
}

pub fn evaluate_model_prediction_with_languages(
    pred: ExternalLangIdResponse,
    measured_duration: Option<f32>,
    min_confidence: f32,
    supported_languages: &[String],
) -> SpeechIdResult {
    let duration = pred.duration_seconds.or(measured_duration);

    if pred.speech_detected == Some(false) || pred.mixed_languages == Some(true) {
        return SpeechIdResult {
            language_code: None,
            confidence: pred.confidence,
            sample_duration_seconds: duration,
            detection_method: "speech_lang_id_model".to_string(),
            verification_status: "inconclusive".to_string(),
        };
    }

    let conf = pred.confidence.unwrap_or(0.0);
    let norm_code = pred
        .language_code
        .as_deref()
        .and_then(normalize_language_code)
        .filter(|c| supported_languages.iter().any(|s| s == c));

    match (norm_code, conf >= min_confidence) {
        (Some(code), true) => SpeechIdResult {
            language_code: Some(code),
            confidence: Some(conf),
            sample_duration_seconds: duration,
            detection_method: "speech_lang_id_model".to_string(),
            verification_status: "verified".to_string(),
        },
        _ => SpeechIdResult {
            language_code: None,
            confidence: pred.confidence,
            sample_duration_seconds: duration,
            detection_method: "speech_lang_id_model".to_string(),
            verification_status: "inconclusive".to_string(),
        },
    }
}

/// Fetches up to 30 seconds of the specific audio track into temporary RAM via ffmpeg.
/// Never downloads the full video and never bypasses 403/DRM/auth controls.
pub async fn extract_30s_audio_sample(
    stream_url: &str,
    headers: &HashMap<String, String>,
    track_index: Option<usize>,
    sample_seconds: u64,
) -> Result<Vec<u8>, String> {
    if crate::net::validate_public_url(stream_url, true)
        .await
        .is_err()
    {
        return Err("Stream URL failed SSRF/public URL validation".to_string());
    }
    extract_30s_audio_sample_raw(stream_url, headers, track_index, sample_seconds).await
}

async fn extract_30s_audio_sample_raw(
    input: &str,
    headers: &HashMap<String, String>,
    track_index: Option<usize>,
    sample_seconds: u64,
) -> Result<Vec<u8>, String> {
    let capped_secs = sample_seconds.clamp(1, MAX_SAMPLE_SECONDS);
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-nostdin");

    if !headers.is_empty() {
        let mut hdr_str = String::new();
        for (k, v) in headers {
            if k.eq_ignore_ascii_case("referer")
                || k.eq_ignore_ascii_case("user-agent")
                || k.eq_ignore_ascii_case("origin")
            {
                hdr_str.push_str(&format!("{k}: {v}\r\n"));
            }
        }
        if !hdr_str.is_empty() {
            cmd.arg("-headers").arg(hdr_str);
        }
    }

    let map_spec = format!("0:a:{}", track_index.unwrap_or(0));
    cmd.arg("-t")
        .arg(capped_secs.to_string())
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg(&map_spec)
        .arg("-vn")
        .arg("-sn")
        .arg("-dn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg("-f")
        .arg("wav")
        .arg("pipe:1")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to spawn ffmpeg sampler: {e}"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Missing ffmpeg stdout".to_string())?;

    let mut buf = Vec::with_capacity(256 * 1024);
    let mut chunk = [0u8; 8192];
    let read_res = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let n = stdout.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            let remaining = MAX_WAV_BYTES.saturating_sub(buf.len());
            if remaining == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n.min(remaining)]);
        }
        Ok::<(), std::io::Error>(())
    })
    .await;

    let _ = child.kill().await;
    let _ = child.wait().await;

    match read_res {
        Ok(Ok(())) if buf.len() > 44 => Ok(buf),
        Ok(Ok(())) => Err("No audio sample bytes decoded".to_string()),
        Ok(Err(e)) => Err(format!("Audio sample read error: {e}")),
        Err(_) => Err("Audio sample extraction timed out".to_string()),
    }
}

pub async fn identify_track_audio_language(
    stream_url: &str,
    headers: &HashMap<String, String>,
    track_index: Option<usize>,
    config: &AudioIdConfig,
) -> SpeechIdResult {
    if !config.is_configured() {
        return SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: None,
            detection_method: "none_configured".to_string(),
            verification_status: "unverified".to_string(),
        };
    }

    let wav =
        match extract_30s_audio_sample(stream_url, headers, track_index, config.sample_seconds)
            .await
        {
            Ok(b) => b,
            Err(_) => {
                return SpeechIdResult {
                    language_code: None,
                    confidence: None,
                    sample_duration_seconds: None,
                    detection_method: "sample_unavailable".to_string(),
                    verification_status: "unavailable".to_string(),
                };
            }
        };

    classify_wav_bytes(wav, config).await
}

pub async fn classify_wav_bytes(wav: Vec<u8>, config: &AudioIdConfig) -> SpeechIdResult {
    let Some((rms, duration)) = measure_wav_rms_and_duration(&wav) else {
        return SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: None,
            detection_method: "wav_energy_check".to_string(),
            verification_status: "inconclusive".to_string(),
        };
    };

    if rms < 0.002 || duration < 1.0 {
        return SpeechIdResult {
            language_code: None,
            confidence: Some(0.0),
            sample_duration_seconds: Some(duration),
            detection_method: "wav_energy_check".to_string(),
            verification_status: "inconclusive".to_string(),
        };
    }

    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
    else {
        return SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: Some(duration),
            detection_method: "speech_lang_id_model".to_string(),
            verification_status: "unavailable".to_string(),
        };
    };

    if let Some(ref model_url) = config.model_url {
        let mut req = client
            .post(model_url)
            .header("Content-Type", "audio/wav")
            .body(wav);
        if let Some(ref key) = config.model_api_key {
            req = req.bearer_auth(key);
        }

        return match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<ExternalLangIdResponse>().await {
                    Ok(pred) => {
                        evaluate_model_prediction(pred, Some(duration), config.min_confidence)
                    }
                    Err(_) => SpeechIdResult {
                        language_code: None,
                        confidence: None,
                        sample_duration_seconds: Some(duration),
                        detection_method: "speech_lang_id_model".to_string(),
                        verification_status: "inconclusive".to_string(),
                    },
                }
            }
            _ => SpeechIdResult {
                language_code: None,
                confidence: None,
                sample_duration_seconds: Some(duration),
                detection_method: "speech_lang_id_model".to_string(),
                verification_status: "unavailable".to_string(),
            },
        };
    }

    if let Some(ref api_key) = config.gemini_api_key {
        let b64 = base64::engine::general_purpose::STANDARD.encode(&wav);
        let endpoint = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent?key={}",
            config.gemini_model, api_key
        );
        let body = serde_json::json!({
            "contents": [{
                "parts": [
                    {
                        "text": "Identify the primary spoken language in this audio clip. Return ONLY JSON with fields: language_code (one of \"hi\", \"en\", \"ja\", or null if unknown/music/mixed), confidence (0.0 to 1.0), speech_detected (boolean), mixed_languages (boolean)."
                    },
                    {
                        "inline_data": {
                            "mime_type": "audio/wav",
                            "data": b64
                        }
                    }
                ]
            }],
            "generationConfig": {
                "responseMimeType": "application/json"
            }
        });
        if let Ok(resp) = client.post(&endpoint).json(&body).send().await
            && resp.status().is_success()
            && let Ok(val) = resp.json::<serde_json::Value>().await
            && let Some(text) = val["candidates"][0]["content"]["parts"][0]["text"].as_str()
            && let Ok(pred) = serde_json::from_str::<ExternalLangIdResponse>(text)
        {
            let mut res = evaluate_model_prediction(pred, Some(duration), config.min_confidence);
            res.detection_method = "gemini_audio_lang_id".to_string();
            return res;
        }
        return SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: Some(duration),
            detection_method: "gemini_audio_lang_id".to_string(),
            verification_status: "unavailable".to_string(),
        };
    }

    SpeechIdResult {
        language_code: None,
        confidence: None,
        sample_duration_seconds: Some(duration),
        detection_method: "none_configured".to_string(),
        verification_status: "unverified".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_wav(amplitude: i16, seconds: usize) -> Vec<u8> {
        let samples = 16000 * seconds;
        let data_len = (samples * 2) as u32;
        let mut buf = Vec::with_capacity(44 + data_len as usize);
        buf.extend_from_slice(b"RIFF");
        buf.extend_from_slice(&(36 + data_len).to_le_bytes());
        buf.extend_from_slice(b"WAVEfmt ");
        buf.extend_from_slice(&16u32.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&16000u32.to_le_bytes());
        buf.extend_from_slice(&32000u32.to_le_bytes());
        buf.extend_from_slice(&2u16.to_le_bytes());
        buf.extend_from_slice(&16u16.to_le_bytes());
        buf.extend_from_slice(b"data");
        buf.extend_from_slice(&data_len.to_le_bytes());
        for i in 0..samples {
            let val = if i % 2 == 0 { amplitude } else { -amplitude };
            buf.extend_from_slice(&val.to_le_bytes());
        }
        buf
    }

    #[test]
    fn test_silence_and_speech_energy_detection() {
        let silent = make_wav(0, 5);
        let (rms, dur) = measure_wav_rms_and_duration(&silent).unwrap();
        assert!(rms < 0.002);
        assert!((dur - 5.0).abs() < 0.1);

        let speech = make_wav(4000, 10);
        let (rms2, dur2) = measure_wav_rms_and_duration(&speech).unwrap();
        assert!(rms2 > 0.05);
        assert!((dur2 - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_model_prediction_hindi_english_japanese_and_rejections() {
        let hi = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("hi".into()),
                confidence: Some(0.94),
                speech_detected: Some(true),
                mixed_languages: Some(false),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(hi.language_code.as_deref(), Some("hi"));
        assert_eq!(hi.verification_status, "verified");

        let en = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("en".into()),
                confidence: Some(0.91),
                speech_detected: Some(true),
                mixed_languages: Some(false),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(en.language_code.as_deref(), Some("en"));
        assert_eq!(en.verification_status, "verified");

        let ja = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("ja".into()),
                confidence: Some(0.89),
                speech_detected: Some(true),
                mixed_languages: Some(false),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(ja.language_code.as_deref(), Some("ja"));
        assert_eq!(ja.verification_status, "verified");

        // Music-only / no-speech sample
        let no_speech = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("ja".into()),
                confidence: Some(0.85),
                speech_detected: Some(false),
                mixed_languages: Some(false),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(no_speech.language_code, None);
        assert_eq!(no_speech.verification_status, "inconclusive");

        // Mixed-language audio
        let mixed = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("en".into()),
                confidence: Some(0.82),
                speech_detected: Some(true),
                mixed_languages: Some(true),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(mixed.language_code, None);
        assert_eq!(mixed.verification_status, "inconclusive");

        // Low confidence
        let low = evaluate_model_prediction(
            ExternalLangIdResponse {
                language_code: Some("hi".into()),
                confidence: Some(0.45),
                speech_detected: Some(true),
                mixed_languages: Some(false),
                duration_seconds: Some(30.0),
            },
            None,
            0.75,
        );
        assert_eq!(low.language_code, None);
        assert_eq!(low.verification_status, "inconclusive");
    }

    #[tokio::test]
    async fn test_ffprobe_and_ffmpeg_30s_sample_and_mock_lang_id_pipeline() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let tmp_mkv =
            std::env::temp_dir().join(format!("pv_v3_audio_id_test_{}.mkv", std::process::id()));
        let status = Command::new("ffmpeg")
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=440:duration=2",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=880:duration=2",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=16000:cl=mono:d=2",
                "-map",
                "0:a",
                "-map",
                "1:a",
                "-map",
                "2:a",
                "-metadata:s:a:0",
                "language=hin",
                "-metadata:s:a:1",
                "language=eng",
                "-c:a",
                "aac",
                tmp_mkv.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .expect("ffmpeg should be installed");
        assert!(status.success());

        let streams =
            extract_ffprobe_audio_streams_raw(tmp_mkv.to_str().unwrap(), &HashMap::new()).await;
        assert_eq!(streams.len(), 3);
        assert_eq!(streams[0].audio_codec.as_deref(), Some("aac"));
        assert_eq!(streams[0].declared_language_code.as_deref(), Some("hi"));
        assert_eq!(streams[1].declared_language_code.as_deref(), Some("en"));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let responses = [
                r#"{"language_code":"hi","confidence":0.93,"speech_detected":true,"mixed_languages":false}"#,
                r#"{"language_code":"en","confidence":0.91,"speech_detected":true,"mixed_languages":false}"#,
            ];
            for body in responses {
                if let Ok((mut sock, _)) = listener.accept().await {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                }
            }
        });

        let cfg = AudioIdConfig {
            sample_seconds: 30,
            model_url: Some(format!("http://{addr}/identify")),
            model_api_key: None,
            gemini_api_key: None,
            gemini_model: "gemini-2.5-flash".to_string(),
            min_confidence: 0.75,
            supported_languages: DEFAULT_SUPPORTED_LANGUAGES
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        };

        let wav0 =
            extract_30s_audio_sample_raw(tmp_mkv.to_str().unwrap(), &HashMap::new(), Some(0), 30)
                .await
                .unwrap();
        let res0 = classify_wav_bytes(wav0, &cfg).await;
        assert_eq!(res0.language_code.as_deref(), Some("hi"));
        assert_eq!(res0.verification_status, "verified");

        let wav1 =
            extract_30s_audio_sample_raw(tmp_mkv.to_str().unwrap(), &HashMap::new(), Some(1), 30)
                .await
                .unwrap();
        let res1 = classify_wav_bytes(wav1, &cfg).await;
        assert_eq!(res1.language_code.as_deref(), Some("en"));
        assert_eq!(res1.verification_status, "verified");

        let wav2 =
            extract_30s_audio_sample_raw(tmp_mkv.to_str().unwrap(), &HashMap::new(), Some(2), 30)
                .await
                .unwrap();
        let res2 = classify_wav_bytes(wav2, &cfg).await;
        assert_eq!(res2.language_code, None);
        assert_eq!(res2.verification_status, "inconclusive");

        let _ = std::fs::remove_file(&tmp_mkv);
    }
}
