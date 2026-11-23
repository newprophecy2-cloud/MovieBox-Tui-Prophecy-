use crate::providers::models::{ProviderKind, Release};
use crate::unified::normalize_language_code;
use crate::v3::audio_id::{
    AudioIdConfig, FfprobeAudioStream, SpeechIdResult, extract_ffprobe_audio_streams,
    identify_track_audio_language,
};
use crate::v3::models::{
    AudioAvailability, AudioStatus, AudioTrackItem, CanonicalEpisode, DetectedLanguageMetadata,
    EpisodeMappingOutcome, EpisodeMatchingDiagnostic, ExcludedSourceRecord, MediaProbeMetadata,
    NormalizedSourceMetadata, PipelineStats, ProviderCoverageDetail, ProviderEpisodeMapping,
    ProviderSourceCandidate, ProviderSourceMetadata, ProviderSourcesGroup, PublicSourceItem,
    ResolveSuccessResponse, SubtitleItem, TrackSelectionMetadata,
};
use crate::v3::providers::{ProviderRegistry, provider_display_name, provider_key};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

#[derive(Debug)]
pub enum ResolveFailure {
    AudioUnavailable(String),
    SourceUnavailable(String),
    ResolutionFailed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestAudioTrack {
    pub track_index: usize,
    pub audio_track_id: String,
    pub original_language_code: Option<String>,
    pub language_code: Option<String>,
    pub language: String,
    pub audio_codec: Option<String>,
    pub channels: Option<String>,
    pub is_default: Option<bool>,
    pub track_selection_metadata: TrackSelectionMetadata,
}

#[derive(Debug)]
pub struct SourceCollectionResult {
    pub candidates: Vec<ProviderSourceCandidate>,
    pub all_failed: bool,
    pub provider_coverage: BTreeMap<String, String>,
    pub provider_details: Vec<ProviderCoverageDetail>,
    pub pipeline_stats: PipelineStats,
    pub excluded_sources: usize,
    pub excluded_reasons: Vec<String>,
    pub excluded_details: Vec<ExcludedSourceRecord>,
    pub matching_diagnostics: Vec<EpisodeMatchingDiagnostic>,
}

pub fn language_display_name(code: Option<&str>) -> String {
    match code {
        Some("hi") => "Hindi".to_string(),
        Some("en") => "English".to_string(),
        Some("ja") => "Japanese".to_string(),
        Some("bn") => "Bengali".to_string(),
        Some("ta") => "Tamil".to_string(),
        Some("te") => "Telugu".to_string(),
        Some("ml") => "Malayalam".to_string(),
        Some("kn") => "Kannada".to_string(),
        Some("mr") => "Marathi".to_string(),
        Some("ko") => "Korean".to_string(),
        Some("es") => "Spanish".to_string(),
        Some("fr") => "French".to_string(),
        Some("de") => "German".to_string(),
        Some(other) if !other.is_empty() => other.to_ascii_uppercase(),
        _ => "Unknown".to_string(),
    }
}

fn normalize_manifest_lang(raw: Option<&str>) -> Option<String> {
    let trimmed = raw?.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("und")
        || trimmed.eq_ignore_ascii_case("unknown")
    {
        return None;
    }
    let base = trimmed.split(['-', '_']).next().unwrap_or(trimmed);
    normalize_language_code(base)
}

/// Parses multi-language provider strings such as `"Hindi, English, Japanese"` or `"Hindi + English"`
/// into independent normalized language codes without collapsing them into `Unknown`.
pub fn extract_declared_languages(rel: &Release) -> Vec<String> {
    let Some(raw) = rel.language.as_deref() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for part in raw.split([',', '+', '/', '|', '&']) {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(code) = normalize_manifest_lang(Some(trimmed)) {
            if seen.insert(code.clone()) {
                out.push(code);
            }
            continue;
        }
        // Fallback: check individual words in strings like "Hindi AMZN DDP 2.0"
        for word in trimmed.split_whitespace() {
            if let Some(code) = normalize_manifest_lang(Some(word))
                && seen.insert(code.clone())
            {
                out.push(code);
            }
        }
    }
    out
}

pub fn extract_verified_language(rel: &Release) -> Option<String> {
    extract_declared_languages(rel).into_iter().next()
}

fn extract_xml_attr(tag: &str, attr: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let needle = format!("{attr}={quote}");
        if let Some(pos) = tag.find(&needle) {
            let rest = &tag[pos + needle.len()..];
            if let Some(end) = rest.find(quote) {
                let val = rest[..end].trim();
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }
    None
}

pub fn parse_dash_mpd_audio_tracks(xml: &str) -> Vec<ManifestAudioTrack> {
    let mut tracks = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<AdaptationSet") {
        let after_start = &rest[start..];
        let Some(tag_end) = after_start.find('>') else {
            break;
        };
        let open_tag = &after_start[..=tag_end];
        let block_end = after_start
            .find("</AdaptationSet>")
            .map(|p| p + "</AdaptationSet>".len())
            .unwrap_or(tag_end + 1);
        let block = &after_start[..block_end];

        let content_type = extract_xml_attr(open_tag, "contentType").unwrap_or_default();
        let mime_type = extract_xml_attr(open_tag, "mimeType").unwrap_or_default();
        let is_audio = content_type.eq_ignore_ascii_case("audio")
            || mime_type.to_ascii_lowercase().starts_with("audio/");

        if is_audio {
            let track_index = tracks.len();
            let adaptation_set_id = extract_xml_attr(open_tag, "id");
            let lang_raw = extract_xml_attr(open_tag, "lang");
            let language_code = normalize_manifest_lang(lang_raw.as_deref());
            let language = language_display_name(language_code.as_deref());

            let rep_tag = block
                .find("<Representation")
                .and_then(|p| block[p..].find('>').map(|e| &block[p..=p + e]));
            let representation_id = rep_tag.and_then(|t| extract_xml_attr(t, "id"));
            let audio_codec = extract_xml_attr(open_tag, "codecs")
                .or_else(|| rep_tag.and_then(|t| extract_xml_attr(t, "codecs")));
            let channels = block
                .find("<AudioChannelConfiguration")
                .and_then(|p| block[p..].find('>').map(|e| &block[p..=p + e]))
                .and_then(|t| extract_xml_attr(t, "value"));

            let audio_track_id = adaptation_set_id
                .clone()
                .or_else(|| representation_id.clone())
                .unwrap_or_else(|| format!("audio-{track_index}"));

            tracks.push(ManifestAudioTrack {
                track_index,
                audio_track_id,
                original_language_code: lang_raw,
                language_code: language_code.clone(),
                language,
                audio_codec,
                channels,
                is_default: None,
                track_selection_metadata: TrackSelectionMetadata {
                    format: "dash".to_string(),
                    track_index,
                    adaptation_set_id,
                    representation_id,
                    group_id: None,
                    rendition_name: None,
                    language_code,
                },
            });
        }

        rest = &after_start[block_end..];
    }
    tracks
}

fn parse_m3u8_attrs(line: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let payload = line.strip_prefix("#EXT-X-MEDIA:").unwrap_or(line);
    let bytes = payload.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && (bytes[i] == b',' || bytes[i] == b' ') {
            i += 1;
        }
        let key_start = i;
        while i < bytes.len() && bytes[i] != b'=' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let key = payload[key_start..i].trim().to_ascii_uppercase();
        i += 1;
        if i >= bytes.len() {
            break;
        }
        let val = if bytes[i] == b'"' {
            i += 1;
            let v_start = i;
            while i < bytes.len() && bytes[i] != b'"' {
                i += 1;
            }
            let v = payload[v_start..i].to_string();
            if i < bytes.len() {
                i += 1;
            }
            v
        } else {
            let v_start = i;
            while i < bytes.len() && bytes[i] != b',' {
                i += 1;
            }
            payload[v_start..i].trim().to_string()
        };
        map.insert(key, val);
    }
    map
}

pub fn parse_hls_m3u8_audio_tracks(m3u8: &str) -> Vec<ManifestAudioTrack> {
    let mut tracks = Vec::new();
    for raw_line in m3u8.lines() {
        let line = raw_line.trim();
        if !line.starts_with("#EXT-X-MEDIA:") {
            continue;
        }
        let attrs = parse_m3u8_attrs(line);
        if attrs.get("TYPE").map(String::as_str) != Some("AUDIO") {
            continue;
        }
        let track_index = tracks.len();
        let group_id = attrs.get("GROUP-ID").cloned();
        let rendition_name = attrs.get("NAME").cloned();
        let original_language_code = attrs.get("LANGUAGE").cloned();
        let language_code = normalize_manifest_lang(original_language_code.as_deref());
        let language = language_display_name(language_code.as_deref());
        let audio_codec = attrs.get("CODECS").cloned();
        let channels = attrs.get("CHANNELS").cloned();
        let is_default = attrs.get("DEFAULT").map(|v| v.eq_ignore_ascii_case("YES"));
        let audio_track_id = match (&group_id, &rendition_name) {
            (Some(g), Some(n)) => format!("{g}:{n}"),
            (_, Some(n)) => n.clone(),
            _ => format!("audio-{track_index}"),
        };

        tracks.push(ManifestAudioTrack {
            track_index,
            audio_track_id,
            original_language_code,
            language_code: language_code.clone(),
            language,
            audio_codec,
            channels,
            is_default,
            track_selection_metadata: TrackSelectionMetadata {
                format: "hls".to_string(),
                track_index,
                adaptation_set_id: None,
                representation_id: None,
                group_id,
                rendition_name,
                language_code,
            },
        });
    }
    tracks
}

pub fn compute_audio_availability(
    sources: &[ProviderSourceCandidate],
    all_providers_failed: bool,
) -> AudioAvailability {
    if all_providers_failed || sources.is_empty() {
        return AudioAvailability {
            hindi: AudioStatus::Unknown,
            english: AudioStatus::Unknown,
            japanese: AudioStatus::Unknown,
            available_languages: Vec::new(),
            declared_languages: Vec::new(),
            manifest_languages: Vec::new(),
            observed_track_languages: Vec::new(),
            verified_languages: Vec::new(),
            language_verification_status: "unverified".to_string(),
            checked_sources: false,
        };
    }

    let sort_lang_vec = |v: &mut Vec<String>| {
        v.sort_by_key(|code| match code.as_str() {
            "hi" => 0,
            "en" => 1,
            "ja" => 2,
            _ => 3,
        });
    };

    let mut langs = Vec::new();
    let mut seen = HashSet::new();
    let mut declared_languages = Vec::new();
    let mut seen_declared = HashSet::new();
    let mut manifest_languages = Vec::new();
    let mut seen_manifest = HashSet::new();
    let mut observed_track_languages = Vec::new();
    let mut seen_observed = HashSet::new();
    let mut any_explicit_lang = false;
    let mut any_unknown_lang = false;

    for src in sources {
        for l in &src.normalized_metadata.declared_languages {
            if seen_declared.insert(l.clone()) {
                declared_languages.push(l.clone());
            }
        }
        for t in &src.declared_audio_tracks {
            if let Some(ref l) = t.declared_language_code
                && seen_declared.insert(l.clone())
            {
                declared_languages.push(l.clone());
            }
        }
        for t in &src.audio_tracks {
            if let Some(ref l) = t.declared_language_code {
                if t.source.as_deref() == Some("manifest") && seen_manifest.insert(l.clone()) {
                    manifest_languages.push(l.clone());
                }
                if seen_observed.insert(l.clone()) {
                    observed_track_languages.push(l.clone());
                }
            }
        }
        if src.audio_languages.is_empty() {
            any_unknown_lang = true;
            continue;
        }
        any_explicit_lang = true;
        for l in &src.audio_languages {
            if seen.insert(l.clone()) {
                langs.push(l.clone());
            }
        }
    }

    sort_lang_vec(&mut declared_languages);
    sort_lang_vec(&mut manifest_languages);
    sort_lang_vec(&mut observed_track_languages);

    if !any_explicit_lang {
        return AudioAvailability {
            hindi: AudioStatus::Unknown,
            english: AudioStatus::Unknown,
            japanese: AudioStatus::Unknown,
            available_languages: Vec::new(),
            declared_languages,
            manifest_languages,
            observed_track_languages,
            verified_languages: Vec::new(),
            language_verification_status: "unverified".to_string(),
            checked_sources: true,
        };
    }

    sort_lang_vec(&mut langs);

    let status_for = |target: &str| -> AudioStatus {
        if seen.contains(target) {
            AudioStatus::Available
        } else if any_unknown_lang {
            AudioStatus::Unknown
        } else {
            AudioStatus::Missing
        }
    };

    let verified_languages = langs.clone();
    AudioAvailability {
        hindi: status_for("hi"),
        english: status_for("en"),
        japanese: status_for("ja"),
        available_languages: langs,
        declared_languages,
        manifest_languages,
        observed_track_languages,
        verified_languages,
        language_verification_status: "verified".to_string(),
        checked_sources: true,
    }
}

#[derive(Debug, Clone)]
pub struct ValidationFailure {
    pub reason: String,
    pub http_status_code: Option<u16>,
    pub playability_status: String,
    pub access_status: String,
}

pub fn redact_url_for_diagnostics(raw: &str) -> String {
    match url::Url::parse(raw.trim()) {
        Ok(u) => {
            let mut base = format!(
                "{}://{}{}",
                u.scheme(),
                u.host_str().unwrap_or(""),
                u.path()
            );
            if u.query().is_some() {
                base.push_str("?<redacted>");
            }
            base
        }
        Err(_) => "<invalid_url>".to_string(),
    }
}

fn is_same_site_url(target_url: &str, origin_url: &str) -> bool {
    let (Ok(target), Ok(origin)) = (
        reqwest::Url::parse(target_url),
        reqwest::Url::parse(origin_url),
    ) else {
        return false;
    };
    if target.scheme() != "https" {
        return false;
    }
    let (Some(th), Some(oh)) = (target.host_str(), origin.host_str()) else {
        return false;
    };
    let th = th.to_ascii_lowercase();
    let oh = oh.to_ascii_lowercase();
    th == oh || th.ends_with(&format!(".{oh}")) || oh.ends_with(&format!(".{th}"))
}

async fn validate_stream_url_and_inspect(
    url: &str,
    headers: &HashMap<String, String>,
) -> Result<(String, Option<u16>, Vec<ManifestAudioTrack>), ValidationFailure> {
    if crate::net::validate_public_url(url, true).await.is_err() {
        return Err(ValidationFailure {
            reason: "invalid_or_private_url".to_string(),
            http_status_code: None,
            playability_status: "unavailable".to_string(),
            access_status: "blocked".to_string(),
        });
    }

    let lower = url.to_ascii_lowercase();
    let is_mpd = lower.contains(".mpd");
    let is_m3u8 = lower.contains(".m3u8");

    static VALIDATION_CLIENT: std::sync::LazyLock<reqwest::Client> =
        std::sync::LazyLock::new(|| {
            crate::net::http_client_builder_base()
                .timeout(Duration::from_secs(6))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        });

    let mut req = VALIDATION_CLIENT.get(url);
    if !is_mpd && !is_m3u8 {
        req = req.header("Range", "bytes=0-1023");
    }
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let resp = req.send().await.map_err(|_| ValidationFailure {
        reason: "request_failed".to_string(),
        http_status_code: None,
        playability_status: "unavailable".to_string(),
        access_status: "unreachable".to_string(),
    })?;
    let status = resp.status();
    let code = status.as_u16();
    if !status.is_success() {
        let (playability_status, access_status) = if code == 403 || code == 401 {
            ("access_forbidden".to_string(), "forbidden".to_string())
        } else {
            ("unavailable".to_string(), "error".to_string())
        };
        return Err(ValidationFailure {
            reason: format!("http_{code}"),
            http_status_code: Some(code),
            playability_status,
            access_status,
        });
    }

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if is_mpd || is_m3u8 {
        let bytes = resp.bytes().await.map_err(|_| ValidationFailure {
            reason: "manifest_read_failed".to_string(),
            http_status_code: Some(code),
            playability_status: "unavailable".to_string(),
            access_status: "error".to_string(),
        })?;
        if bytes.len() > 512 * 1024 {
            return Err(ValidationFailure {
                reason: "manifest_too_large".to_string(),
                http_status_code: Some(code),
                playability_status: "unavailable".to_string(),
                access_status: "error".to_string(),
            });
        }
        let text = String::from_utf8_lossy(&bytes);
        if is_mpd {
            if !text.contains("<MPD") {
                return Err(ValidationFailure {
                    reason: "invalid_mpd_manifest".to_string(),
                    http_status_code: Some(code),
                    playability_status: "unavailable".to_string(),
                    access_status: "invalid_manifest".to_string(),
                });
            }
            return Ok((
                "manifest_validated".to_string(),
                Some(code),
                parse_dash_mpd_audio_tracks(&text),
            ));
        }
        if !text.contains("#EXTM3U") {
            return Err(ValidationFailure {
                reason: "invalid_m3u8_manifest".to_string(),
                http_status_code: Some(code),
                playability_status: "unavailable".to_string(),
                access_status: "invalid_manifest".to_string(),
            });
        }
        return Ok((
            "manifest_validated".to_string(),
            Some(code),
            parse_hls_m3u8_audio_tracks(&text),
        ));
    }

    if content_type.starts_with("text/html") {
        return Err(ValidationFailure {
            reason: "non_media_html_response".to_string(),
            http_status_code: Some(code),
            playability_status: "unavailable".to_string(),
            access_status: "non_media".to_string(),
        });
    }

    Ok(("http_reachable".to_string(), Some(code), Vec::new()))
}

pub fn build_media_probe_metadata(
    playability_status: &str,
    probed_track_count: usize,
) -> MediaProbeMetadata {
    build_media_probe_metadata_detailed(playability_status, None, None, probed_track_count)
}

pub fn build_media_probe_metadata_detailed(
    playability_status: &str,
    http_status_code: Option<u16>,
    access_status_override: Option<&str>,
    probed_track_count: usize,
) -> MediaProbeMetadata {
    let manifest_validated = playability_status == "manifest_validated";
    let media_probe_validated = playability_status == "media_probe_validated";
    let http_reachable = matches!(
        playability_status,
        "http_reachable" | "manifest_validated" | "media_probe_validated"
    );
    let access_status = access_status_override
        .map(str::to_string)
        .unwrap_or_else(|| {
            if http_reachable {
                "ok".to_string()
            } else if playability_status == "access_forbidden" || http_status_code == Some(403) {
                "forbidden".to_string()
            } else {
                "unavailable".to_string()
            }
        });
    MediaProbeMetadata {
        http_reachable,
        http_status_code,
        access_status,
        manifest_validated,
        media_probe_validated,
        playability_status: playability_status.to_string(),
        device_playback_status: "untested".to_string(),
        probed_track_count,
    }
}

/// Builds a non-playable diagnostic record for an excluded candidate (e.g., HTTP 403 on AniZone),
/// preserving provider-declared language metadata without exposing signed URL credentials.
pub fn build_validation_excluded_record(
    provider_key_str: &str,
    rel: &Release,
    mirror_label: &str,
    original_url: &str,
    failure: &ValidationFailure,
) -> ExcludedSourceRecord {
    let declared_langs = extract_declared_languages(rel);
    let declared_audio_tracks: Vec<AudioTrackItem> = declared_langs
        .iter()
        .map(|code| AudioTrackItem {
            track_id: None,
            track_index: None,
            language: language_display_name(Some(code)),
            language_code: Some(code.clone()),
            original_language_code: rel.language.clone(),
            declared_language: Some(language_display_name(Some(code))),
            declared_language_code: Some(code.clone()),
            verified_language: None,
            verified_language_code: None,
            codec: None,
            channels: None,
            sample_rate: None,
            is_default: None,
            source: Some("provider_declared".to_string()),
            verification_status: "unverified".to_string(),
            track_selection_metadata: None,
        })
        .collect();
    let primary_declared_code = declared_langs.first().cloned();
    let media_meta = build_media_probe_metadata_detailed(
        &failure.playability_status,
        failure.http_status_code,
        Some(&failure.access_status),
        0,
    );
    ExcludedSourceRecord {
        provider: provider_key_str.to_string(),
        source_id: None,
        stage: "validation".to_string(),
        reason: failure.reason.clone(),
        http_status_code: failure.http_status_code,
        playability_status: Some(failure.playability_status.clone()),
        declared_language: primary_declared_code
            .as_deref()
            .map(|c| language_display_name(Some(c))),
        declared_language_code: primary_declared_code,
        declared_audio_tracks,
        provider_metadata: Some(ProviderSourceMetadata {
            provider_id: provider_key_str.to_string(),
            provider_source_id: rel.resource_id.clone(),
            original_url: Some(redact_url_for_diagnostics(original_url)),
            mirror_label: Some(mirror_label.to_string()),
            original_filename: Some(rel.filename.clone()),
            original_language: rel.language.clone(),
            original_quality: rel.quality.clone(),
            original_codec: rel.codec.clone(),
            size_bytes: rel.size_bytes,
            original_track_ids: Vec::new(),
            contributing_variants: Vec::new(),
        }),
        media_metadata: Some(media_meta),
    }
}

fn audio_track_identity(t: &AudioTrackItem) -> String {
    if let Some(ref id) = t.track_id {
        format!(
            "id:{id}:idx:{}:lang:{}",
            t.track_index.map(|i| i.to_string()).unwrap_or_default(),
            t.declared_language_code.as_deref().unwrap_or("")
        )
    } else if let Some(idx) = t.track_index {
        format!(
            "idx:{idx}:lang:{}:codec:{}",
            t.declared_language_code.as_deref().unwrap_or(""),
            t.codec.as_deref().unwrap_or("")
        )
    } else {
        format!(
            "decl:{}:orig:{}:codec:{}:ch:{}",
            t.declared_language_code.as_deref().unwrap_or(""),
            t.original_language_code.as_deref().unwrap_or(""),
            t.codec.as_deref().unwrap_or(""),
            t.channels.as_deref().unwrap_or("")
        )
    }
}

/// Merges distinct audio tracks into `existing` without duplicating identical track entries.
pub fn merge_audio_tracks(existing: &mut Vec<AudioTrackItem>, incoming: Vec<AudioTrackItem>) {
    let mut seen: HashSet<String> = existing.iter().map(audio_track_identity).collect();
    for track in incoming {
        let key = audio_track_identity(&track);
        if seen.insert(key) {
            existing.push(track);
        }
    }
}

/// Merges an incoming candidate sharing the same `(provider_id, resolved_url)` into `existing`,
/// preserving all distinct audio tracks, declared languages, and original track IDs.
pub fn merge_source_candidate(
    existing: &mut ProviderSourceCandidate,
    incoming: ProviderSourceCandidate,
) {
    merge_audio_tracks(&mut existing.audio_tracks, incoming.audio_tracks);
    merge_audio_tracks(
        &mut existing.declared_audio_tracks,
        incoming.declared_audio_tracks,
    );
    for sub in incoming.subtitles {
        if !existing.subtitles.iter().any(|s| s.url == sub.url) {
            existing.subtitles.push(sub);
        }
    }
    for lang in incoming.audio_languages {
        if !existing.audio_languages.contains(&lang) {
            existing.audio_languages.push(lang);
        }
    }
    for lang in incoming.normalized_metadata.declared_languages {
        if !existing
            .normalized_metadata
            .declared_languages
            .contains(&lang)
        {
            existing.normalized_metadata.declared_languages.push(lang);
        }
    }
    for tid in incoming.provider_metadata.original_track_ids {
        if !existing.provider_metadata.original_track_ids.contains(&tid) {
            existing.provider_metadata.original_track_ids.push(tid);
        }
    }
    for variant in incoming.provider_metadata.contributing_variants {
        if !existing
            .provider_metadata
            .contributing_variants
            .contains(&variant)
        {
            existing
                .provider_metadata
                .contributing_variants
                .push(variant);
        }
    }
    if existing.provider_metadata.original_language.is_none() {
        existing.provider_metadata.original_language = incoming.provider_metadata.original_language;
    }
    if existing.declared_language.is_none() && incoming.declared_language.is_some() {
        existing.declared_language = incoming.declared_language;
        existing.declared_language_code = incoming.declared_language_code;
    }
    if existing.verified_language.is_none() && incoming.verified_language.is_some() {
        existing.verified_language = incoming.verified_language;
        existing.verified_language_code = incoming.verified_language_code;
        existing.verification_status = incoming.verification_status;
    }
    if existing.quality.is_none() && incoming.quality.is_some() {
        existing.quality = incoming.quality;
        existing.resolution_u64 = incoming.resolution_u64;
        existing.normalized_metadata.quality = existing.quality.clone();
        existing.normalized_metadata.resolution_u64 = existing.resolution_u64;
    }
    if existing.codec.is_none() && incoming.codec.is_some() {
        existing.codec = incoming.codec;
        existing.normalized_metadata.video_codec = existing.codec.clone();
    }
    existing.media_metadata.probed_track_count = existing.audio_tracks.len();
    if existing.media_metadata.playability_status == "http_reachable"
        && matches!(
            incoming.media_metadata.playability_status.as_str(),
            "manifest_validated" | "media_probe_validated"
        )
    {
        existing.playability_status = incoming.playability_status;
        existing.media_metadata = incoming.media_metadata;
        existing.media_metadata.probed_track_count = existing.audio_tracks.len();
    }
}

pub async fn collect_episode_sources(
    registry: &ProviderRegistry,
    episode: &CanonicalEpisode,
) -> SourceCollectionResult {
    let mut coverage = BTreeMap::new();
    let mut stats = PipelineStats::default();
    let mut excluded_details: Vec<ExcludedSourceRecord> = Vec::new();
    let mut excluded_reasons: Vec<String> = Vec::new();
    let mut matching_diagnostics: Vec<EpisodeMatchingDiagnostic> = Vec::new();
    let mut provider_detail_map: BTreeMap<String, ProviderCoverageDetail> = BTreeMap::new();

    if episode.provider_mappings.is_empty() {
        return SourceCollectionResult {
            candidates: Vec::new(),
            all_failed: true,
            provider_coverage: coverage,
            provider_details: Vec::new(),
            pipeline_stats: stats,
            excluded_sources: 0,
            excluded_reasons,
            excluded_details,
            matching_diagnostics,
        };
    }

    // Build complete episode matching diagnostics and accept ONLY Matched mappings
    let mut matched_mappings = Vec::new();
    for m in &episode.provider_mappings {
        let pkey = provider_key(m.provider_id).to_string();
        matching_diagnostics.push(EpisodeMatchingDiagnostic {
            requested_anime_id: episode.anime_id.clone(),
            requested_season_number: episode.season_number,
            requested_episode_number: episode.episode_number,
            requested_episode_title: episode.title.clone(),
            provider: pkey.clone(),
            provider_anime_id: m.provider_anime_id.clone(),
            provider_episode_id: m.provider_episode_id.clone(),
            provider_season_number: m.provider_season_number,
            provider_episode_number: m.provider_episode_number,
            provider_episode_title: m.title.clone(),
            matching_score: m.confidence,
            matching_evidence: m.matching_evidence.clone(),
            matching_status: m.outcome,
            exclusion_reason: m.exclusion_reason.clone(),
        });
        if episode.verification == crate::providers::models::EpisodeVerification::Verified
            && m.outcome == EpisodeMappingOutcome::Matched
        {
            matched_mappings.push(m.clone());
        } else {
            let reason = if episode.verification
                != crate::providers::models::EpisodeVerification::Verified
            {
                "canonical_episode_unverified".to_string()
            } else {
                m.exclusion_reason
                    .clone()
                    .unwrap_or_else(|| "episode_mismatch".to_string())
            };
            excluded_details.push(ExcludedSourceRecord {
                provider: pkey.clone(),
                source_id: None,
                stage: "matching".to_string(),
                reason: reason.clone(),
                http_status_code: None,
                playability_status: None,
                declared_language: m.variant_language.clone(),
                declared_language_code: m.variant_language_code.clone(),
                declared_audio_tracks: Vec::new(),
                provider_metadata: None,
                media_metadata: None,
            });
            if excluded_reasons.len() < 25 {
                excluded_reasons.push(format!("{pkey}:{reason}"));
            }
        }
    }

    let futs = matched_mappings.iter().map(|mapping| {
        let reg = registry.clone();
        let m = mapping.clone();
        async move {
            let res = reg
                .fetch_episode_sources(
                    m.provider_id,
                    &m.provider_anime_id,
                    m.provider_season_number,
                    m.provider_episode_number,
                )
                .await;
            (m, res)
        }
    });

    let outcomes = futures::future::join_all(futs).await;
    let mut mirror_tasks = Vec::new();
    let mut any_succeeded = false;

    for (mapping, outcome) in outcomes {
        let pkey = provider_key(mapping.provider_id).to_string();
        let pdisp = provider_display_name(mapping.provider_id).to_string();
        let pdet =
            provider_detail_map
                .entry(pkey.clone())
                .or_insert_with(|| ProviderCoverageDetail {
                    provider: pdisp,
                    request_status: "failed".to_string(),
                    source_count: 0,
                    validated_source_count: 0,
                    merged_duplicates_count: 0,
                    failed_validation_count: 0,
                    excluded_source_count: 0,
                    fallback_attempted: false,
                    fallback_status: None,
                    errors: Vec::new(),
                });

        match outcome {
            Ok(releases) => {
                any_succeeded = true;
                coverage.insert(pkey.clone(), "success".to_string());
                pdet.request_status = "success".to_string();
                stats.raw_sources += releases.len();
                for mut rel in releases {
                    // Preserve language-tagged variant metadata (e.g. MovieBox "Black Clover (Hindi)")
                    // when the release itself omits a language field.
                    if rel.language.is_none() && mapping.variant_language.is_some() {
                        rel.language = mapping.variant_language.clone();
                    }
                    if rel.mirrors.is_empty() {
                        pdet.excluded_source_count += 1;
                        pdet.errors.push("empty_mirrors".to_string());
                        excluded_details.push(ExcludedSourceRecord {
                            provider: pkey.clone(),
                            source_id: None,
                            stage: "parsing".to_string(),
                            reason: "empty_mirrors".to_string(),
                            http_status_code: None,
                            playability_status: None,
                            declared_language: None,
                            declared_language_code: None,
                            declared_audio_tracks: Vec::new(),
                            provider_metadata: None,
                            media_metadata: None,
                        });
                        if excluded_reasons.len() < 25 {
                            excluded_reasons.push(format!("{pkey}:empty_mirrors"));
                        }
                        continue;
                    }
                    stats.parsed_sources += rel.mirrors.len();
                    for mirror in &rel.mirrors {
                        pdet.source_count += 1;
                        let u = mirror.resolver_url.trim();
                        if u.is_empty() || crate::net::validate_url_syntax(u, true).is_err() {
                            pdet.excluded_source_count += 1;
                            pdet.errors.push("invalid_mirror_url".to_string());
                            excluded_details.push(ExcludedSourceRecord {
                                provider: pkey.clone(),
                                source_id: None,
                                stage: "parsing".to_string(),
                                reason: "invalid_mirror_url".to_string(),
                                http_status_code: None,
                                playability_status: None,
                                declared_language: None,
                                declared_language_code: None,
                                declared_audio_tracks: Vec::new(),
                                provider_metadata: None,
                                media_metadata: None,
                            });
                            if excluded_reasons.len() < 25 {
                                excluded_reasons.push(format!("{pkey}:invalid_mirror_url"));
                            }
                            continue;
                        }
                        stats.matched_sources += 1;
                        let mut single_rel = rel.clone();
                        single_rel.mirrors = vec![mirror.clone()];
                        mirror_tasks.push((mapping.clone(), single_rel, mirror.clone()));
                    }
                }
            }
            Err(err) => {
                coverage.insert(pkey.clone(), "failed".to_string());
                pdet.request_status = "failed".to_string();
                pdet.errors.push(err.to_string());
                log::warn!(
                    "v3_episode_sources_failed provider={} episode_id={} err={err}",
                    mapping.provider_id.cache_key(),
                    episode.id
                );
            }
        }
    }

    async fn resolve_and_validate_mirror(
        reg: ProviderRegistry,
        mapping: ProviderEpisodeMapping,
        single_rel: Release,
        mirror: crate::providers::models::SourceMirror,
    ) -> Result<
        (
            ProviderEpisodeMapping,
            Release,
            crate::providers::models::SourceMirror,
            String,
            HashMap<String, String>,
            Vec<SubtitleItem>,
            String,
            Option<u16>,
            Vec<ManifestAudioTrack>,
            Vec<FfprobeAudioStream>,
        ),
        (
            String,
            String,
            ExcludedSourceRecord,
            Option<ProviderEpisodeMapping>,
        ),
    > {
        let pkey = provider_key(mapping.provider_id).to_string();
        let (resolved_url, raw_headers) = if mirror.direct_file {
            (mirror.resolver_url.clone(), mirror.headers.clone())
        } else {
            match reg.resolve_release(mapping.provider_id, &single_rel).await {
                Ok((u, h)) => (u, h),
                Err(_) => {
                    return Err((
                        pkey,
                        "resolution".to_string(),
                        ExcludedSourceRecord {
                            provider: provider_key(mapping.provider_id).to_string(),
                            source_id: None,
                            stage: "resolution".to_string(),
                            reason: "resolve_failed".to_string(),
                            http_status_code: None,
                            playability_status: Some("unavailable".to_string()),
                            declared_language: single_rel.language.clone(),
                            declared_language_code: extract_declared_languages(&single_rel)
                                .into_iter()
                                .next(),
                            declared_audio_tracks: Vec::new(),
                            provider_metadata: None,
                            media_metadata: None,
                        },
                        None,
                    ));
                }
            }
        };

        let mut headers = HashMap::new();
        let mut validation_headers = HashMap::new();
        let mut subtitles: Vec<SubtitleItem> = Vec::new();
        let same_site_cookie = is_same_site_url(&resolved_url, &mirror.resolver_url);
        for (k, v) in raw_headers {
            if k.eq_ignore_ascii_case("x-anizone-subtitle-url") {
                let trimmed = v.trim();
                if !trimmed.is_empty()
                    && crate::net::validate_url_syntax(trimmed, false).is_ok()
                    && !subtitles.iter().any(|s| s.url == trimmed)
                {
                    subtitles.push(SubtitleItem {
                        language: None,
                        language_code: None,
                        label: Some("AniZone Subtitle".to_string()),
                        url: trimmed.to_string(),
                        format: Some("vtt".to_string()),
                        is_default: Some(true),
                        is_forced: None,
                        source: "provider".to_string(),
                    });
                }
                continue;
            }
            if k.eq_ignore_ascii_case("cookie") {
                // Keep cookies strictly internal to same-site validation/probe requests;
                // never expose cookies in public API headers or logs.
                if same_site_cookie && !v.trim().is_empty() {
                    validation_headers.insert(k, v);
                }
                continue;
            }
            if k.eq_ignore_ascii_case("referer")
                || k.eq_ignore_ascii_case("user-agent")
                || k.eq_ignore_ascii_case("origin")
            {
                validation_headers.insert(k.clone(), v.clone());
                headers.insert(k, v);
            }
        }

        match validate_stream_url_and_inspect(&resolved_url, &validation_headers).await {
            Ok((mut playability_status, http_status_code, manifest_tracks)) => {
                let sync_ffprobe = std::env::var("PV_SYNC_FFPROBE")
                    .ok()
                    .as_deref()
                    == Some("1");
                let ffprobe_tracks = if sync_ffprobe && manifest_tracks.is_empty() {
                    let probed =
                        extract_ffprobe_audio_streams(&resolved_url, &validation_headers).await;
                    if !probed.is_empty() {
                        playability_status = "media_probe_validated".to_string();
                    }
                    probed
                } else {
                    Vec::new()
                };
                Ok((
                    mapping,
                    single_rel,
                    mirror,
                    resolved_url,
                    headers,
                    subtitles,
                    playability_status,
                    http_status_code,
                    manifest_tracks,
                    ffprobe_tracks,
                ))
            }
            Err(failure) => {
                let rec = build_validation_excluded_record(
                    &pkey,
                    &single_rel,
                    &mirror.label,
                    &mirror.resolver_url,
                    &failure,
                );
                let retry_mapping = if mapping.provider_id == ProviderKind::AniZone
                    && failure.http_status_code == Some(403)
                {
                    Some(mapping)
                } else {
                    None
                };
                Err((pkey, "validation".to_string(), rec, retry_mapping))
            }
        }
    }

    let validate_futs = mirror_tasks
        .into_iter()
        .map(|(mapping, single_rel, mirror)| {
            resolve_and_validate_mirror(registry.clone(), mapping, single_rel, mirror)
        });

    let mut validated = futures::future::join_all(validate_futs).await;

    // Validation-stage AniZone HTTP 403 sub-server fallback (bounded to 1 attempt per mapping).
    let mut anizone_403_mappings: Vec<ProviderEpisodeMapping> = Vec::new();
    let mut seen_anizone_fallback_ids: HashSet<String> = HashSet::new();
    for item in &validated {
        if let Err((_, _, _, Some(retry_m))) = item
            && seen_anizone_fallback_ids.insert(retry_m.provider_anime_id.clone())
        {
            anizone_403_mappings.push(retry_m.clone());
        }
    }
    if !anizone_403_mappings.is_empty() {
        if !registry.anizone_has_sub_server() {
            if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                pdet.fallback_attempted = false;
                pdet.fallback_status = Some("not_configured".to_string());
            }
        } else {
            for m in anizone_403_mappings {
                if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                    pdet.fallback_attempted = true;
                }
                match registry
                    .fetch_anizone_sub_server_sources(
                        &m.provider_anime_id,
                        m.provider_season_number,
                        m.provider_episode_number,
                    )
                    .await
                {
                    Ok(fb_releases) if !fb_releases.is_empty() => {
                        let mut fb_tasks = Vec::new();
                        for mut rel in fb_releases {
                            if rel.language.is_none() && m.variant_language.is_some() {
                                rel.language = m.variant_language.clone();
                            }
                            for mirror in rel.mirrors.clone() {
                                fb_tasks.push(resolve_and_validate_mirror(
                                    registry.clone(),
                                    m.clone(),
                                    rel.clone(),
                                    mirror,
                                ));
                            }
                        }
                        if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                            pdet.source_count += fb_tasks.len();
                        }
                        stats.parsed_sources += fb_tasks.len();
                        stats.matched_sources += fb_tasks.len();
                        let fb_results = futures::future::join_all(fb_tasks).await;
                        let any_ok = fb_results.iter().any(|r| r.is_ok());
                        if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                            pdet.fallback_status = Some(if any_ok {
                                "success".to_string()
                            } else {
                                "failed".to_string()
                            });
                        }
                        validated.extend(fb_results);
                    }
                    Ok(_) => {
                        if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                            pdet.fallback_status = Some("failed".to_string());
                            pdet.errors.push("anizone_sub_server_empty".to_string());
                        }
                    }
                    Err(err) => {
                        if let Some(pdet) = provider_detail_map.get_mut("anizone") {
                            pdet.fallback_status = Some("failed".to_string());
                            pdet.errors
                                .push(format!("anizone_sub_server_failed: {err}"));
                        }
                    }
                }
            }
        }
    }

    let audio_id_cfg = AudioIdConfig::from_env();
    let mut candidates: Vec<ProviderSourceCandidate> = Vec::new();
    let mut idx = 0usize;
    let mut seen_url_index: HashMap<(ProviderKind, String), usize> = HashMap::new();

    for item in validated {
        let (
            mapping,
            rel,
            mirror,
            resolved_url,
            headers,
            subtitles,
            playability_status,
            http_status_code,
            manifest_tracks,
            ffprobe_tracks,
        ) = match item {
            Ok(v) => {
                stats.resolved_sources += 1;
                stats.validated_sources += 1;
                v
            }
            Err((pkey, stage, rec, _)) => {
                if stage == "validation" {
                    stats.resolved_sources += 1;
                    stats.failed_validation_count += 1;
                }
                if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
                    pdet.excluded_source_count += 1;
                    if stage == "validation" {
                        pdet.failed_validation_count += 1;
                    }
                    pdet.errors.push(rec.reason.clone());
                }
                if excluded_reasons.len() < 25 {
                    excluded_reasons.push(format!("{pkey}:{}", rec.reason));
                }
                excluded_details.push(rec);
                continue;
            }
        };

        let pkey = provider_key(mapping.provider_id).to_string();
        let existing_idx = seen_url_index
            .get(&(mapping.provider_id, resolved_url.clone()))
            .copied();

        let provider_ep_id = mapping.provider_episode_id.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                mapping.provider_season_number, mapping.provider_episode_number
            )
        });

        // Fix A: Preserve original provider declarations independently of FFprobe/normalization
        let declared_langs = extract_declared_languages(&rel);
        let declared_audio_tracks: Vec<AudioTrackItem> = declared_langs
            .iter()
            .map(|code| AudioTrackItem {
                track_id: None,
                track_index: None,
                language: language_display_name(Some(code)),
                language_code: Some(code.clone()),
                original_language_code: rel.language.clone(),
                declared_language: Some(language_display_name(Some(code))),
                declared_language_code: Some(code.clone()),
                verified_language: None,
                verified_language_code: None,
                codec: None,
                channels: None,
                sample_rate: None,
                is_default: None,
                source: Some("provider_declared".to_string()),
                verification_status: "unverified".to_string(),
                track_selection_metadata: None,
            })
            .collect();

        // ONE source URL = ONE ProviderSourceCandidate with Vec<AudioTrackItem>
        let mut audio_tracks: Vec<AudioTrackItem> = Vec::new();
        let mut verified_langs_for_source: Vec<String> = Vec::new();
        let mut original_track_ids: Vec<String> = Vec::new();
        let mut first_speech: Option<SpeechIdResult> = None;

        if !manifest_tracks.is_empty() {
            for track in manifest_tracks {
                let speech = identify_track_audio_language(
                    &resolved_url,
                    &headers,
                    Some(track.track_index),
                    &audio_id_cfg,
                )
                .await;
                let verified_code = if speech.verification_status == "verified" {
                    speech.language_code.clone()
                } else {
                    None
                };
                if let Some(ref vc) = verified_code
                    && !verified_langs_for_source.contains(vc)
                {
                    verified_langs_for_source.push(vc.clone());
                }
                if !original_track_ids.contains(&track.audio_track_id) {
                    original_track_ids.push(track.audio_track_id.clone());
                }
                let declared_code = track.language_code.clone();
                let display_code = verified_code.clone().or_else(|| declared_code.clone());
                audio_tracks.push(AudioTrackItem {
                    track_id: Some(track.audio_track_id),
                    track_index: Some(track.track_index),
                    language: language_display_name(display_code.as_deref()),
                    language_code: display_code,
                    original_language_code: track.original_language_code,
                    declared_language: declared_code
                        .as_deref()
                        .map(|c| language_display_name(Some(c))),
                    declared_language_code: declared_code,
                    verified_language: verified_code
                        .as_deref()
                        .map(|c| language_display_name(Some(c))),
                    verified_language_code: verified_code,
                    codec: track.audio_codec,
                    channels: track.channels,
                    sample_rate: None,
                    is_default: track.is_default,
                    source: Some("manifest".to_string()),
                    verification_status: speech.verification_status.clone(),
                    track_selection_metadata: Some(track.track_selection_metadata),
                });
                if first_speech.is_none() {
                    first_speech = Some(speech);
                }
            }
        } else if !ffprobe_tracks.is_empty() {
            for ftrack in ffprobe_tracks {
                let speech = identify_track_audio_language(
                    &resolved_url,
                    &headers,
                    Some(ftrack.track_index),
                    &audio_id_cfg,
                )
                .await;
                let verified_code = if speech.verification_status == "verified" {
                    speech.language_code.clone()
                } else {
                    None
                };
                if let Some(ref vc) = verified_code
                    && !verified_langs_for_source.contains(vc)
                {
                    verified_langs_for_source.push(vc.clone());
                }
                if let Some(ref tid) = ftrack.provider_track_id
                    && !original_track_ids.contains(tid)
                {
                    original_track_ids.push(tid.clone());
                }
                let declared_code = ftrack.declared_language_code.clone();
                let display_code = verified_code.clone().or_else(|| declared_code.clone());
                audio_tracks.push(AudioTrackItem {
                    track_id: ftrack.provider_track_id.clone(),
                    track_index: Some(ftrack.track_index),
                    language: language_display_name(display_code.as_deref()),
                    language_code: display_code,
                    original_language_code: ftrack.original_language_code,
                    declared_language: declared_code
                        .as_deref()
                        .map(|c| language_display_name(Some(c))),
                    declared_language_code: declared_code.clone(),
                    verified_language: verified_code
                        .as_deref()
                        .map(|c| language_display_name(Some(c))),
                    verified_language_code: verified_code,
                    codec: ftrack.audio_codec,
                    channels: ftrack.channels,
                    sample_rate: ftrack.sample_rate,
                    is_default: ftrack.is_default,
                    source: Some("ffprobe".to_string()),
                    verification_status: speech.verification_status.clone(),
                    track_selection_metadata: Some(TrackSelectionMetadata {
                        format: "container".to_string(),
                        track_index: ftrack.track_index,
                        adaptation_set_id: None,
                        representation_id: ftrack.provider_track_id,
                        group_id: None,
                        rendition_name: None,
                        language_code: declared_code,
                    }),
                });
                if first_speech.is_none() {
                    first_speech = Some(speech);
                }
            }
        } else {
            let speech =
                identify_track_audio_language(&resolved_url, &headers, None, &audio_id_cfg).await;
            let verified_code = if speech.verification_status == "verified" {
                speech.language_code.clone()
            } else {
                None
            };
            if let Some(ref vc) = verified_code {
                verified_langs_for_source.push(vc.clone());
            }
            first_speech = Some(speech);
        }

        let primary_speech = first_speech.unwrap_or(SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: None,
            detection_method: "none_configured".to_string(),
            verification_status: "unverified".to_string(),
        });
        let primary_declared_code = declared_langs.first().cloned().or_else(|| {
            audio_tracks
                .first()
                .and_then(|t| t.declared_language_code.clone())
        });
        let primary_verified_code = verified_langs_for_source.first().cloned();
        let primary_display_code = primary_verified_code
            .clone()
            .or_else(|| primary_declared_code.clone());
        let primary_track = audio_tracks.first().cloned();

        let variant_ref = match &mapping.variant_title {
            Some(vt) if !vt.is_empty() => format!("{}:{vt}", mapping.provider_anime_id),
            _ => mapping.provider_anime_id.clone(),
        };
        let provider_metadata = ProviderSourceMetadata {
            provider_id: pkey.clone(),
            provider_source_id: rel.resource_id.clone(),
            original_url: Some(mirror.resolver_url.clone()),
            mirror_label: Some(mirror.label.clone()),
            original_filename: Some(rel.filename.clone()),
            original_language: rel.language.clone(),
            original_quality: rel.quality.clone(),
            original_codec: rel.codec.clone(),
            size_bytes: rel.size_bytes,
            original_track_ids,
            contributing_variants: vec![variant_ref],
        };
        let normalized_metadata = NormalizedSourceMetadata {
            quality: rel.quality.clone(),
            resolution_u64: rel.resolution_u64(),
            video_codec: rel.codec.clone(),
            declared_languages: declared_langs,
            primary_language: language_display_name(primary_display_code.as_deref()),
            primary_language_code: primary_display_code.clone(),
        };
        let media_metadata = build_media_probe_metadata_detailed(
            &playability_status,
            http_status_code,
            Some("ok"),
            audio_tracks.len(),
        );
        let detected_language_metadata = DetectedLanguageMetadata {
            declared_language: primary_declared_code
                .as_deref()
                .map(|c| language_display_name(Some(c))),
            declared_language_code: primary_declared_code.clone(),
            detected_language: primary_verified_code
                .as_deref()
                .map(|c| language_display_name(Some(c))),
            detected_language_code: primary_verified_code.clone(),
            confidence: primary_speech.confidence,
            detection_method: primary_speech.detection_method.clone(),
            verification_status: primary_speech.verification_status.clone(),
        };

        let candidate = ProviderSourceCandidate {
            source_id: String::new(),
            provider_id: mapping.provider_id,
            provider_name: provider_display_name(mapping.provider_id).to_string(),
            provider_episode_id: provider_ep_id,
            provider_season: mapping.provider_season_number,
            provider_episode: mapping.provider_episode_number,
            resolution_u64: rel.resolution_u64(),
            quality: rel.quality.clone(),
            codec: rel.codec.clone(),
            release: rel,
            audio_languages: verified_langs_for_source,
            track_index: primary_track.as_ref().and_then(|t| t.track_index),
            provider_track_id: primary_track.as_ref().and_then(|t| t.track_id.clone()),
            language: language_display_name(primary_display_code.as_deref()),
            language_code: primary_display_code,
            declared_language: primary_declared_code
                .as_deref()
                .map(|c| language_display_name(Some(c))),
            declared_language_code: primary_declared_code,
            verified_language: primary_verified_code
                .as_deref()
                .map(|c| language_display_name(Some(c))),
            verified_language_code: primary_verified_code,
            audio_track_id: primary_track.as_ref().and_then(|t| t.track_id.clone()),
            audio_codec: primary_track.as_ref().and_then(|t| t.codec.clone()),
            channels: primary_track.as_ref().and_then(|t| t.channels.clone()),
            sample_duration_seconds: primary_speech.sample_duration_seconds,
            detection_method: primary_speech.detection_method,
            confidence: primary_speech.confidence,
            verification_status: primary_speech.verification_status,
            playability_status,
            track_selection_metadata: primary_track
                .as_ref()
                .and_then(|t| t.track_selection_metadata.clone()),
            audio_tracks,
            declared_audio_tracks,
            subtitles,
            provider_metadata,
            normalized_metadata,
            media_metadata,
            detected_language_metadata,
            stream_url: Some(resolved_url.clone()),
            headers,
            available: true,
        };

        // Fix B: Merge duplicate URL entries instead of discarding subsequent audio tracks
        if let Some(pos) = existing_idx {
            merge_source_candidate(&mut candidates[pos], candidate);
            stats.merged_duplicates_count += 1;
            if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
                pdet.merged_duplicates_count += 1;
            }
            excluded_details.push(ExcludedSourceRecord {
                provider: pkey,
                source_id: Some(candidates[pos].source_id.clone()),
                stage: "deduplication".to_string(),
                reason: "merged_duplicate_url".to_string(),
                http_status_code,
                playability_status: Some(candidates[pos].playability_status.clone()),
                declared_language: None,
                declared_language_code: None,
                declared_audio_tracks: Vec::new(),
                provider_metadata: None,
                media_metadata: None,
            });
            continue;
        }

        if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
            pdet.validated_source_count += 1;
        }
        idx += 1;
        let mut candidate = candidate;
        candidate.source_id = format!("src-{idx}");
        seen_url_index.insert((mapping.provider_id, resolved_url), candidates.len());
        candidates.push(candidate);
    }

    rank_candidates(&mut candidates, None, None);
    stats.returned_sources = candidates.len();
    let excluded_sources = excluded_details
        .iter()
        .filter(|d| d.reason != "merged_duplicate_url")
        .count();
    let provider_details: Vec<ProviderCoverageDetail> = provider_detail_map.into_values().collect();

    SourceCollectionResult {
        candidates,
        all_failed: !any_succeeded,
        provider_coverage: coverage,
        provider_details,
        pipeline_stats: stats,
        excluded_sources,
        excluded_reasons,
        excluded_details,
        matching_diagnostics,
    }
}

pub fn to_public_source_item(c: &ProviderSourceCandidate) -> PublicSourceItem {
    PublicSourceItem {
        source_id: c.source_id.clone(),
        provider: c.provider_name.clone(),
        stream_url: c.stream_url.clone(),
        quality: c.quality.clone(),
        track_index: c.track_index,
        provider_track_id: c.provider_track_id.clone(),
        language: c.language.clone(),
        language_code: c.language_code.clone(),
        declared_language: c.declared_language.clone(),
        declared_language_code: c.declared_language_code.clone(),
        verified_language: c.verified_language.clone(),
        verified_language_code: c.verified_language_code.clone(),
        audio_track_id: c.audio_track_id.clone(),
        codec: c.codec.clone(),
        audio_codec: c.audio_codec.clone(),
        channels: c.channels.clone(),
        sample_duration_seconds: c.sample_duration_seconds,
        detection_method: c.detection_method.clone(),
        confidence: c.confidence,
        verification_status: c.verification_status.clone(),
        playability_status: c.playability_status.clone(),
        track_selection_metadata: c.track_selection_metadata.clone(),
        audio_tracks: c.audio_tracks.clone(),
        declared_audio_tracks: c.declared_audio_tracks.clone(),
        subtitles: c.subtitles.clone(),
        provider_metadata: c.provider_metadata.clone(),
        normalized_metadata: c.normalized_metadata.clone(),
        media_metadata: c.media_metadata.clone(),
        detected_language_metadata: c.detected_language_metadata.clone(),
        headers: c.headers.clone(),
        availability_status: if c.available {
            "available"
        } else {
            "unavailable"
        },
        available: c.available,
    }
}

pub fn group_sources_by_provider(sources: &[ProviderSourceCandidate]) -> Vec<ProviderSourcesGroup> {
    let mut order: Vec<String> = Vec::new();
    let mut map: HashMap<String, Vec<PublicSourceItem>> = HashMap::new();
    for s in sources {
        if !map.contains_key(&s.provider_name) {
            order.push(s.provider_name.clone());
        }
        map.entry(s.provider_name.clone())
            .or_default()
            .push(to_public_source_item(s));
    }
    order
        .into_iter()
        .filter_map(|p| {
            map.remove(&p).map(|srcs| ProviderSourcesGroup {
                provider: p,
                sources: srcs,
            })
        })
        .collect()
}

fn provider_reliability_rank(p: ProviderKind) -> usize {
    match p {
        ProviderKind::AniZone => 0,
        ProviderKind::MovieBox => 1,
        ProviderKind::Dramachi => 2,
        ProviderKind::FourKHdHub => 3,
        _ => 4,
    }
}

fn parse_quality_target(q: &str) -> Option<u64> {
    let digits: String = q.chars().filter(|c| c.is_ascii_digit()).collect();
    digits.parse::<u64>().ok().filter(|v| *v > 0)
}

fn default_language_priority(langs: &[String]) -> usize {
    if langs.iter().any(|l| l == "hi") {
        0
    } else if langs.iter().any(|l| l == "en") {
        1
    } else if langs.iter().any(|l| l == "ja") {
        2
    } else if !langs.is_empty() {
        3
    } else {
        4
    }
}

pub fn rank_candidates(
    candidates: &mut [ProviderSourceCandidate],
    requested_audio: Option<&str>,
    preferred_quality: Option<&str>,
) {
    let target_lang = requested_audio.and_then(normalize_language_code);
    let target_q = preferred_quality.and_then(parse_quality_target);

    candidates.sort_by(|a, b| {
        let a_avail = !a.available;
        let b_avail = !b.available;
        let a_lang = target_lang
            .as_ref()
            .map(|req| {
                if a.audio_languages.contains(req) {
                    0usize
                } else {
                    1
                }
            })
            .unwrap_or_else(|| default_language_priority(&a.audio_languages));
        let b_lang = target_lang
            .as_ref()
            .map(|req| {
                if b.audio_languages.contains(req) {
                    0usize
                } else {
                    1
                }
            })
            .unwrap_or_else(|| default_language_priority(&b.audio_languages));
        let a_q = target_q.map(|q| a.resolution_u64.abs_diff(q)).unwrap_or(0);
        let b_q = target_q.map(|q| b.resolution_u64.abs_diff(q)).unwrap_or(0);

        a_avail
            .cmp(&b_avail)
            .then_with(|| a_lang.cmp(&b_lang))
            .then_with(|| a_q.cmp(&b_q))
            .then_with(|| b.resolution_u64.cmp(&a.resolution_u64))
            .then_with(|| {
                provider_reliability_rank(a.provider_id)
                    .cmp(&provider_reliability_rank(b.provider_id))
            })
    });
}

fn find_matching_track(
    candidate: &ProviderSourceCandidate,
    req_lang: &str,
) -> Option<AudioTrackItem> {
    // 1. Prefer speech-verified track
    if let Some(t) = candidate.audio_tracks.iter().find(|t| {
        t.verification_status == "verified" && t.verified_language_code.as_deref() == Some(req_lang)
    }) {
        return Some(t.clone());
    }
    // 2. Bug #8: Allow selecting a declared track ONLY when it has a real track identifier / selection metadata
    candidate
        .audio_tracks
        .iter()
        .find(|t| {
            t.declared_language_code.as_deref() == Some(req_lang)
                && (t.track_id.is_some()
                    || t.track_index.is_some()
                    || t.track_selection_metadata.is_some())
        })
        .cloned()
}

fn candidate_supports_requested_lang(candidate: &ProviderSourceCandidate, req_lang: &str) -> bool {
    (candidate.verification_status == "verified"
        && candidate.audio_languages.contains(&req_lang.to_string()))
        || find_matching_track(candidate, req_lang).is_some()
}

pub async fn resolve_episode_stream(
    registry: &ProviderRegistry,
    episode: &CanonicalEpisode,
    sources: &[ProviderSourceCandidate],
    source_id: Option<&str>,
    requested_audio: Option<&str>,
    preferred_quality: Option<&str>,
) -> Result<ResolveSuccessResponse, ResolveFailure> {
    if sources.is_empty() {
        return Err(ResolveFailure::SourceUnavailable(
            "The selected source is unavailable.".to_string(),
        ));
    }

    let normalized_req = requested_audio
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(normalize_language_code);

    if let Some(raw_audio) = requested_audio.map(str::trim).filter(|s| !s.is_empty())
        && normalized_req.is_none()
    {
        return Err(ResolveFailure::AudioUnavailable(format!(
            "The requested audio language '{raw_audio}' is unavailable."
        )));
    }

    let mut ranked = if let Some(raw_sid) = source_id {
        let sid = raw_sid.trim();
        if sid.is_empty() {
            return Err(ResolveFailure::SourceUnavailable(
                "The selected source is unavailable.".to_string(),
            ));
        }
        let Some(exact) = sources.iter().find(|c| c.source_id == sid).cloned() else {
            return Err(ResolveFailure::SourceUnavailable(format!(
                "Source '{sid}' was not found for this episode."
            )));
        };
        if let Some(ref req_lang) = normalized_req
            && !candidate_supports_requested_lang(&exact, req_lang)
        {
            return Err(ResolveFailure::AudioUnavailable(
                "The requested audio language is unavailable.".to_string(),
            ));
        }
        vec![exact]
    } else {
        sources.to_vec()
    };

    if source_id.is_none()
        && let Some(ref req_lang) = normalized_req
    {
        let matching: Vec<_> = ranked
            .iter()
            .filter(|c| candidate_supports_requested_lang(c, req_lang))
            .cloned()
            .collect();
        if matching.is_empty() {
            return Err(ResolveFailure::AudioUnavailable(
                "The requested audio language is unavailable.".to_string(),
            ));
        }
        ranked = matching;
    }

    if source_id.is_none() {
        rank_candidates(&mut ranked, normalized_req.as_deref(), preferred_quality);
    }

    let mut last_err = None;
    for candidate in ranked {
        if !candidate.available {
            continue;
        }
        let selected_track = normalized_req
            .as_deref()
            .and_then(|req| find_matching_track(&candidate, req));

        let resolved = if let Some(ref pre_url) = candidate.stream_url
            && matches!(
                candidate.playability_status.as_str(),
                "manifest_validated"
                    | "media_probe_validated"
                    | "http_reachable"
                    | "server_validated"
            ) {
            let hdrs: Vec<(String, String)> = candidate
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Ok((pre_url.clone(), hdrs))
        } else {
            registry
                .resolve_release(candidate.provider_id, &candidate.release)
                .await
        };

        match resolved {
            Ok((url, headers)) => {
                if let Err(err) = crate::net::validate_public_url(&url, true).await {
                    log::warn!(
                        "v3_resolved_url_rejected provider={} episode_id={} reason={err}",
                        candidate.provider_id.cache_key(),
                        episode.id
                    );
                    last_err = Some("Resolved URL failed security validation".to_string());
                    continue;
                }
                let mut safe_headers = HashMap::new();
                for (k, v) in headers {
                    if k.eq_ignore_ascii_case("referer")
                        || k.eq_ignore_ascii_case("user-agent")
                        || k.eq_ignore_ascii_case("origin")
                    {
                        safe_headers.insert(k, v);
                    }
                }
                return Ok(ResolveSuccessResponse {
                    status: "success",
                    episode_id: episode.id.clone(),
                    source_id: candidate.source_id,
                    provider: candidate.provider_name,
                    track_index: selected_track
                        .as_ref()
                        .and_then(|t| t.track_index)
                        .or(candidate.track_index),
                    provider_track_id: selected_track
                        .as_ref()
                        .and_then(|t| t.track_id.clone())
                        .or(candidate.provider_track_id),
                    language: selected_track
                        .as_ref()
                        .map(|t| t.language.clone())
                        .unwrap_or(candidate.language),
                    language_code: selected_track
                        .as_ref()
                        .and_then(|t| t.language_code.clone())
                        .or(candidate.language_code),
                    declared_language: selected_track
                        .as_ref()
                        .and_then(|t| t.declared_language.clone())
                        .or(candidate.declared_language),
                    declared_language_code: selected_track
                        .as_ref()
                        .and_then(|t| t.declared_language_code.clone())
                        .or(candidate.declared_language_code),
                    quality: candidate.quality,
                    stream_url: url,
                    audio_track_id: selected_track
                        .as_ref()
                        .and_then(|t| t.track_id.clone())
                        .or(candidate.audio_track_id),
                    codec: candidate.codec,
                    audio_codec: selected_track
                        .as_ref()
                        .and_then(|t| t.codec.clone())
                        .or(candidate.audio_codec),
                    channels: selected_track
                        .as_ref()
                        .and_then(|t| t.channels.clone())
                        .or(candidate.channels),
                    sample_duration_seconds: candidate.sample_duration_seconds,
                    detection_method: candidate.detection_method,
                    confidence: candidate.confidence,
                    verification_status: selected_track
                        .as_ref()
                        .map(|t| t.verification_status.clone())
                        .unwrap_or(candidate.verification_status),
                    playability_status: candidate.playability_status,
                    track_selection_metadata: selected_track
                        .and_then(|t| t.track_selection_metadata)
                        .or(candidate.track_selection_metadata),
                    audio_tracks: candidate.audio_tracks,
                    subtitles: candidate.subtitles,
                    provider_metadata: candidate.provider_metadata,
                    normalized_metadata: candidate.normalized_metadata,
                    media_metadata: candidate.media_metadata,
                    detected_language_metadata: candidate.detected_language_metadata,
                    headers: safe_headers,
                });
            }
            Err(err) => {
                log::warn!(
                    "v3_resolve_candidate_failed provider={} episode_id={} err={err}",
                    candidate.provider_id.cache_key(),
                    episode.id
                );
                last_err = Some(err.to_string());
            }
        }
    }

    Err(ResolveFailure::SourceUnavailable(last_err.unwrap_or_else(
        || "The selected source is unavailable.".to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::models::SourceMirror;

    fn make_cand(
        provider: ProviderKind,
        lang: Option<&str>,
        quality: Option<&str>,
        url: &str,
    ) -> ProviderSourceCandidate {
        let code = lang.and_then(normalize_language_code);
        ProviderSourceCandidate {
            source_id: format!("src-{url}"),
            provider_id: provider,
            provider_name: provider_display_name(provider).to_string(),
            provider_episode_id: "ep1".into(),
            provider_season: 1,
            provider_episode: 1,
            release: Release {
                provider,
                filename: "ep.mp4".into(),
                quality: quality.map(str::to_string),
                codec: None,
                language: lang.map(str::to_string),
                size_bytes: None,
                season: Some(1),
                episode: Some(1),
                mirrors: vec![SourceMirror {
                    label: "Test".into(),
                    resolver_url: url.to_string(),
                    headers: vec![],
                    direct_file: true,
                }],
                resource_id: None,
            },
            audio_languages: code.iter().cloned().collect(),
            track_index: None,
            provider_track_id: None,
            language: language_display_name(code.as_deref()),
            language_code: code.clone(),
            declared_language: code.as_deref().map(|c| language_display_name(Some(c))),
            declared_language_code: code.clone(),
            verified_language: code.as_deref().map(|c| language_display_name(Some(c))),
            verified_language_code: code.clone(),
            audio_track_id: None,
            codec: None,
            audio_codec: None,
            channels: None,
            sample_duration_seconds: code.as_ref().map(|_| 30.0),
            detection_method: if code.is_some() {
                "speech_lang_id_model".to_string()
            } else {
                "none_configured".to_string()
            },
            confidence: code.as_ref().map(|_| 0.95),
            verification_status: if code.is_some() {
                "verified".to_string()
            } else {
                "unverified".to_string()
            },
            playability_status: "http_reachable".to_string(),
            track_selection_metadata: None,
            audio_tracks: Vec::new(),
            declared_audio_tracks: Vec::new(),
            subtitles: Vec::new(),
            provider_metadata: ProviderSourceMetadata {
                provider_id: provider_key(provider).to_string(),
                original_url: Some(url.to_string()),
                original_language: lang.map(str::to_string),
                original_quality: quality.map(str::to_string),
                ..Default::default()
            },
            normalized_metadata: NormalizedSourceMetadata {
                quality: quality.map(str::to_string),
                resolution_u64: quality.and_then(parse_quality_target).unwrap_or(0),
                declared_languages: code.iter().cloned().collect(),
                primary_language: language_display_name(code.as_deref()),
                primary_language_code: code.clone(),
                ..Default::default()
            },
            media_metadata: build_media_probe_metadata("http_reachable", 0),
            detected_language_metadata: DetectedLanguageMetadata {
                declared_language: code.as_deref().map(|c| language_display_name(Some(c))),
                declared_language_code: code.clone(),
                detected_language: code.as_deref().map(|c| language_display_name(Some(c))),
                detected_language_code: code.clone(),
                confidence: code.as_ref().map(|_| 0.95),
                detection_method: if code.is_some() {
                    "speech_lang_id_model".to_string()
                } else {
                    "none_configured".to_string()
                },
                verification_status: if code.is_some() {
                    "verified".to_string()
                } else {
                    "unverified".to_string()
                },
            },
            quality: quality.map(str::to_string),
            resolution_u64: quality.and_then(parse_quality_target).unwrap_or(0),
            stream_url: Some(url.to_string()),
            headers: HashMap::new(),
            available: true,
        }
    }

    #[test]
    fn test_audio_availability_distinguishes_missing_and_unknown() {
        let srcs = vec![
            make_cand(
                ProviderKind::FourKHdHub,
                Some("en"),
                Some("1080p"),
                "https://cdn.example/a.mp4",
            ),
            make_cand(
                ProviderKind::AniZone,
                Some("ja"),
                Some("720p"),
                "https://cdn.example/b.m3u8",
            ),
        ];
        let audio = compute_audio_availability(&srcs, false);
        assert_eq!(audio.hindi, AudioStatus::Missing);
        assert_eq!(audio.english, AudioStatus::Available);
        assert_eq!(audio.japanese, AudioStatus::Available);
        assert_eq!(audio.available_languages, vec!["en", "ja"]);

        let unknown_srcs = vec![make_cand(
            ProviderKind::MovieBox,
            None,
            Some("720p"),
            "https://cdn.example/c.mp4",
        )];
        let audio_unknown = compute_audio_availability(&unknown_srcs, false);
        assert_eq!(audio_unknown.hindi, AudioStatus::Unknown);
        assert_eq!(audio_unknown.english, AudioStatus::Unknown);
    }

    #[test]
    fn test_multi_language_provider_string_parses_independently() {
        let mut c = make_cand(
            ProviderKind::FourKHdHub,
            None,
            Some("1080p"),
            "https://cdn.example/a.mkv",
        );
        c.release.language = Some("Hindi, English, Japanese".to_string());
        assert_eq!(
            extract_declared_languages(&c.release),
            vec!["hi", "en", "ja"]
        );

        c.release.language =
            Some("Hindi AMZN DDP 2.0 + English TrueHD 5.1 + Japanese FLAC 2.0".to_string());
        assert_eq!(
            extract_declared_languages(&c.release),
            vec!["hi", "en", "ja"]
        );
    }

    #[test]
    fn test_ranking_respects_requested_quality_and_language() {
        let mut srcs = vec![
            make_cand(
                ProviderKind::MovieBox,
                Some("en"),
                Some("720p"),
                "https://cdn.example/1.mp4",
            ),
            make_cand(
                ProviderKind::Dramachi,
                Some("hi"),
                Some("1080p"),
                "https://cdn.example/2.mp4",
            ),
            make_cand(
                ProviderKind::FourKHdHub,
                Some("hi"),
                Some("480p"),
                "https://cdn.example/3.mp4",
            ),
        ];
        rank_candidates(&mut srcs, Some("hi"), Some("1080p"));
        assert_eq!(srcs[0].provider_id, ProviderKind::Dramachi);
        assert_eq!(srcs[0].quality.as_deref(), Some("1080p"));
    }

    #[test]
    fn test_default_ranking_prefers_hindi_then_english_then_japanese() {
        let mut srcs = vec![
            make_cand(
                ProviderKind::AniZone,
                Some("ja"),
                Some("1080p"),
                "https://cdn.example/ja.mp4",
            ),
            make_cand(
                ProviderKind::MovieBox,
                Some("en"),
                Some("1080p"),
                "https://cdn.example/en.mp4",
            ),
            make_cand(
                ProviderKind::Dramachi,
                Some("hi"),
                Some("720p"),
                "https://cdn.example/hi.mp4",
            ),
        ];
        rank_candidates(&mut srcs, None, None);
        assert_eq!(srcs[0].audio_languages, vec!["hi"]);
        assert_eq!(srcs[1].audio_languages, vec!["en"]);
        assert_eq!(srcs[2].audio_languages, vec!["ja"]);
    }

    #[test]
    fn test_strict_provider_audio_does_not_infer_from_unverified_filename() {
        let mut c = make_cand(
            ProviderKind::MovieBox,
            None,
            Some("1080p"),
            "https://cdn.example/a.mp4",
        );
        c.release.filename = "Naruto.Ep01.[Hindi].mkv".into();
        assert_eq!(extract_verified_language(&c.release), None);
        c.release.language = Some("hi".into());
        assert_eq!(
            extract_verified_language(&c.release),
            Some("hi".to_string())
        );
    }

    #[test]
    fn test_parse_dash_mpd_multi_audio_tracks() {
        let mpd = r#"
        <MPD>
          <Period>
            <AdaptationSet id="1" contentType="video" mimeType="video/mp4">
              <Representation id="v1" codecs="hev1" bandwidth="2000000"/>
            </AdaptationSet>
            <AdaptationSet id="2" contentType="audio" mimeType="audio/mp4" lang="hi" codecs="mp4a.40.2">
              <AudioChannelConfiguration value="2"/>
              <Representation id="a1" bandwidth="128000"/>
            </AdaptationSet>
            <AdaptationSet id="3" contentType="audio" mimeType="audio/mp4" lang="en" codecs="ec-3">
              <AudioChannelConfiguration value="6"/>
              <Representation id="a2" bandwidth="256000"/>
            </AdaptationSet>
            <AdaptationSet id="4" contentType="audio" mimeType="audio/mp4" lang="und" codecs="mp4a.40.2">
              <Representation id="a3" bandwidth="128000"/>
            </AdaptationSet>
          </Period>
        </MPD>"#;
        let tracks = parse_dash_mpd_audio_tracks(mpd);
        assert_eq!(tracks.len(), 3);
        assert_eq!(tracks[0].language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[1].language_code.as_deref(), Some("en"));
        assert_eq!(tracks[2].language_code, None);
    }

    #[test]
    fn test_parse_hls_m3u8_multi_audio_tracks() {
        let m3u8 = r#"#EXTM3U
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Hindi",LANGUAGE="hi",CHANNELS="2",DEFAULT=YES,AUTOSELECT=YES,URI="hi.m3u8"
#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Japanese",LANGUAGE="ja",CHANNELS="2",DEFAULT=NO,AUTOSELECT=YES,URI="ja.m3u8"
#EXT-X-STREAM-INF:BANDWIDTH=1500000,AUDIO="aud"
video.m3u8"#;
        let tracks = parse_hls_m3u8_audio_tracks(m3u8);
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[1].language_code.as_deref(), Some("ja"));
    }

    #[tokio::test]
    async fn test_incorrect_provider_language_tag_is_not_trusted_when_unverified() {
        let mut fake_hi = make_cand(
            ProviderKind::MovieBox,
            Some("hi"),
            Some("1080p"),
            "https://cdn.example/fake.mp4",
        );
        fake_hi.verification_status = "unverified".to_string();
        fake_hi.language = "Unknown".to_string();
        fake_hi.language_code = None;
        fake_hi.audio_languages.clear();

        let ep = CanonicalEpisode {
            id: "NP-E-001".into(),
            anime_id: "NP-A-001".into(),
            season_id: "NP-S-001".into(),
            season_number: 1,
            episode_number: 1,
            title: "Ep 1".into(),
            metadata_status: "verified".into(),
            provenance: crate::providers::models::EpisodeProvenance::ProviderMetadata,
            verification: crate::providers::models::EpisodeVerification::Verified,
            overview: None,
            provider_mappings: vec![],
            audio: None,
            sources: None,
            cached_provider_coverage: None,
            cached_provider_details: Vec::new(),
            pipeline_stats: PipelineStats::default(),
            excluded_sources_count: 0,
            excluded_reasons: Vec::new(),
            excluded_details: Vec::new(),
            matching_diagnostics: Vec::new(),
        };
        let reg = ProviderRegistry::from_env();
        let res = resolve_episode_stream(&reg, &ep, &[fake_hi], None, Some("hi"), None).await;
        assert!(matches!(res, Err(ResolveFailure::AudioUnavailable(_))));
    }

    fn make_track_item(
        id: Option<&str>,
        idx: Option<usize>,
        code: &str,
        orig: &str,
        codec: &str,
    ) -> AudioTrackItem {
        AudioTrackItem {
            track_id: id.map(str::to_string),
            track_index: idx,
            language: language_display_name(Some(code)),
            language_code: Some(code.to_string()),
            original_language_code: Some(orig.to_string()),
            declared_language: Some(language_display_name(Some(code))),
            declared_language_code: Some(code.to_string()),
            verified_language: None,
            verified_language_code: None,
            codec: Some(codec.to_string()),
            channels: Some("2".to_string()),
            sample_rate: Some("48000".to_string()),
            is_default: Some(idx == Some(0)),
            source: Some("ffprobe".to_string()),
            verification_status: "unverified".to_string(),
            track_selection_metadata: None,
        }
    }

    #[test]
    fn test_merge_duplicate_url_preserves_two_audio_tracks() {
        let mut first = make_cand(
            ProviderKind::FourKHdHub,
            Some("hi"),
            Some("1080p"),
            "https://cdn.example/same.mkv",
        );
        first.audio_tracks = vec![make_track_item(Some("1"), Some(0), "hi", "hin", "aac")];
        first.provider_metadata.original_track_ids = vec!["1".to_string()];
        first.provider_metadata.contributing_variants = vec!["mb-hi:Black Clover (Hindi)".into()];

        let mut second = make_cand(
            ProviderKind::FourKHdHub,
            Some("en"),
            Some("1080p"),
            "https://cdn.example/same.mkv",
        );
        second.audio_tracks = vec![make_track_item(Some("2"), Some(1), "en", "eng", "eac3")];
        second.provider_metadata.original_track_ids = vec!["2".to_string()];
        second.provider_metadata.contributing_variants =
            vec!["mb-en:Black Clover (English)".into()];

        merge_source_candidate(&mut first, second);
        assert_eq!(first.audio_tracks.len(), 2);
        assert_eq!(first.audio_tracks[0].track_id.as_deref(), Some("1"));
        assert_eq!(first.audio_tracks[1].track_id.as_deref(), Some("2"));
        assert_eq!(first.provider_metadata.original_track_ids, vec!["1", "2"]);
        assert_eq!(
            first.provider_metadata.contributing_variants,
            vec!["mb-hi:Black Clover (Hindi)", "mb-en:Black Clover (English)"]
        );
    }

    #[test]
    fn test_merge_duplicate_audio_tracks_deduplicates_cleanly() {
        let mut tracks = vec![make_track_item(Some("1"), Some(0), "hi", "hin", "aac")];
        let incoming = vec![
            make_track_item(Some("1"), Some(0), "hi", "hin", "aac"),
            make_track_item(Some("2"), Some(1), "ja", "jpn", "aac"),
        ];
        merge_audio_tracks(&mut tracks, incoming);
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].declared_language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[1].declared_language_code.as_deref(), Some("ja"));
    }

    #[test]
    fn test_provider_track_ids_and_declared_language_survive_pipeline() {
        let mpd = r#"<MPD><Period>
            <AdaptationSet id="aud-hin-01" contentType="audio" mimeType="audio/mp4" lang="hin-IN" codecs="mp4a.40.2">
                <AudioChannelConfiguration value="2"/>
            </AdaptationSet>
            <AdaptationSet id="aud-jpn-02" contentType="audio" mimeType="audio/mp4" lang="jpn" codecs="ec-3">
                <AudioChannelConfiguration value="6"/>
            </AdaptationSet>
        </Period></MPD>"#;
        let tracks = parse_dash_mpd_audio_tracks(mpd);
        assert_eq!(tracks.len(), 2);
        assert_eq!(tracks[0].audio_track_id, "aud-hin-01");
        assert_eq!(tracks[0].original_language_code.as_deref(), Some("hin-IN"));
        assert_eq!(tracks[0].language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[1].audio_track_id, "aud-jpn-02");
        assert_eq!(tracks[1].original_language_code.as_deref(), Some("jpn"));
        assert_eq!(tracks[1].language_code.as_deref(), Some("ja"));

        let rel = Release {
            provider: ProviderKind::FourKHdHub,
            filename: "Black.Clover.S01E01.1080p.mkv".into(),
            quality: Some("1080p".into()),
            codec: Some("H.265".into()),
            language: Some("Hindi, English, Japanese".into()),
            size_bytes: Some(500_000_000),
            season: Some(1),
            episode: Some(1),
            mirrors: vec![],
            resource_id: Some("prov-src-42".into()),
        };
        let declared = extract_declared_languages(&rel);
        assert_eq!(declared, vec!["hi", "en", "ja"]);
    }

    #[test]
    fn test_ffprobe_metadata_does_not_overwrite_provider_metadata() {
        let mut cand = make_cand(
            ProviderKind::FourKHdHub,
            Some("Hindi, English"),
            Some("1080p"),
            "https://cdn.example/bc.mkv",
        );
        cand.provider_metadata.original_language = Some("Hindi, English".to_string());
        cand.provider_metadata.provider_source_id = Some("hub-99".to_string());
        cand.audio_tracks = vec![make_track_item(Some("1"), Some(0), "ja", "jpn", "aac")];
        cand.media_metadata = build_media_probe_metadata("media_probe_validated", 1);

        let public = to_public_source_item(&cand);
        assert_eq!(
            public.provider_metadata.original_language.as_deref(),
            Some("Hindi, English")
        );
        assert_eq!(
            public.provider_metadata.provider_source_id.as_deref(),
            Some("hub-99")
        );
        assert_eq!(public.audio_tracks[0].source.as_deref(), Some("ffprobe"));
        assert_eq!(public.audio_tracks[0].verification_status, "unverified");
    }

    #[test]
    fn test_http_200_does_not_claim_media_probe_validated() {
        let meta = build_media_probe_metadata("http_reachable", 0);
        assert!(meta.http_reachable);
        assert!(!meta.manifest_validated);
        assert!(!meta.media_probe_validated);
        assert_eq!(meta.device_playback_status, "untested");
    }

    #[tokio::test]
    async fn test_mismatched_episode_mappings_are_excluded() {
        let ep = CanonicalEpisode {
            id: "NP-E-MISMATCH".into(),
            anime_id: "NP-A-001".into(),
            season_id: "NP-S-001".into(),
            season_number: 1,
            episode_number: 1,
            title: "Asta and Yuno".into(),
            metadata_status: "verified".into(),
            provenance: crate::providers::models::EpisodeProvenance::ProviderMetadata,
            verification: crate::providers::models::EpisodeVerification::Verified,
            overview: None,
            provider_mappings: vec![crate::v3::models::ProviderEpisodeMapping {
                provider_id: ProviderKind::Dramachi,
                provider_anime_id: "40867".into(),
                variant_title: Some("Black Clover".into()),
                variant_language: None,
                variant_language_code: None,
                provider_season_number: 1,
                provider_episode_number: 99,
                provider_episode_id: Some("dc-ep-99".into()),
                provider_season_id: None,
                title: Some("Wrong Episode".into()),
                provenance: crate::providers::models::EpisodeProvenance::ProviderMetadata,
                verification: crate::providers::models::EpisodeVerification::Unverified,
                outcome: EpisodeMappingOutcome::EpisodeMismatch,
                confidence: 0.0,
                matching_evidence: vec!["episode_mismatch:expected=1:got=99".into()],
                exclusion_reason: Some("episode_mismatch".into()),
            }],
            audio: None,
            sources: None,
            cached_provider_coverage: None,
            cached_provider_details: Vec::new(),
            pipeline_stats: PipelineStats::default(),
            excluded_sources_count: 0,
            excluded_reasons: Vec::new(),
            excluded_details: Vec::new(),
            matching_diagnostics: Vec::new(),
        };
        let reg = ProviderRegistry::from_env();
        let res = collect_episode_sources(&reg, &ep).await;
        assert!(res.candidates.is_empty());
        assert_eq!(res.excluded_sources, 1);
        assert_eq!(res.excluded_details[0].stage, "matching");
        assert_eq!(res.excluded_details[0].reason, "episode_mismatch");
        assert_eq!(res.matching_diagnostics.len(), 1);
        assert_eq!(
            res.matching_diagnostics[0].matching_status,
            EpisodeMappingOutcome::EpisodeMismatch
        );
        assert_eq!(res.matching_diagnostics[0].provider_anime_id, "40867");
        assert_eq!(
            res.matching_diagnostics[0].provider_episode_id.as_deref(),
            Some("dc-ep-99")
        );
    }

    #[test]
    fn test_anizone_english_metadata_survives_http_403() {
        let rel = Release {
            provider: ProviderKind::AniZone,
            filename: "ijxazsfw Episode 1".into(),
            quality: None,
            codec: None,
            language: Some("English, Japanese".into()),
            size_bytes: None,
            season: Some(1),
            episode: Some(1),
            mirrors: vec![],
            resource_id: Some("1".into()),
        };
        let failure = ValidationFailure {
            reason: "http_403".to_string(),
            http_status_code: Some(403),
            playability_status: "access_forbidden".to_string(),
            access_status: "forbidden".to_string(),
        };
        let rec = build_validation_excluded_record(
            "anizone",
            &rel,
            "AniZone HLS",
            "https://cdn.anizone.example/ep1/master.m3u8?token=secret_should_redact",
            &failure,
        );
        assert_eq!(rec.http_status_code, Some(403));
        assert_eq!(rec.playability_status.as_deref(), Some("access_forbidden"));
        assert_eq!(rec.declared_language.as_deref(), Some("English"));
        assert_eq!(rec.declared_language_code.as_deref(), Some("en"));
        assert_eq!(rec.declared_audio_tracks.len(), 2);
        assert_eq!(
            rec.declared_audio_tracks[0].source.as_deref(),
            Some("provider_declared")
        );
        assert_eq!(
            rec.declared_audio_tracks[0].verification_status,
            "unverified"
        );
        let mm = rec.media_metadata.as_ref().unwrap();
        assert!(!mm.http_reachable);
        assert_eq!(mm.http_status_code, Some(403));
        assert_eq!(mm.access_status, "forbidden");
        assert_eq!(mm.device_playback_status, "untested");
        let pm = rec.provider_metadata.as_ref().unwrap();
        assert!(
            !pm.original_url
                .as_deref()
                .unwrap()
                .contains("secret_should_redact")
        );
    }

    #[tokio::test]
    async fn test_html_response_rejected_as_direct_media() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut req_buf = [0u8; 1024];
                let _ = sock.read(&mut req_buf).await;
                let body = "<html><body>Access Denied</body></html>";
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        let url = format!("http://{addr}/video.mp4");
        let err = validate_stream_url_and_inspect(&url, &HashMap::new())
            .await
            .unwrap_err();
        assert_eq!(err.reason, "non_media_html_response");
        assert_eq!(err.playability_status, "unavailable");
    }

    #[test]
    fn test_different_urls_and_variants_remain_separate() {
        let mut seen: HashMap<(ProviderKind, String), usize> = HashMap::new();
        let mut list: Vec<ProviderSourceCandidate> = Vec::new();
        for (url, quality) in [
            ("https://cdn.example/hi-1080p.mp4", "1080p"),
            ("https://cdn.example/en-1080p.mp4", "1080p"),
            ("https://cdn.example/hi-720p.mp4", "720p"),
        ] {
            let c = make_cand(ProviderKind::MovieBox, Some("hi"), Some(quality), url);
            let key = (c.provider_id, url.to_string());
            if let Some(&pos) = seen.get(&key) {
                merge_source_candidate(&mut list[pos], c);
            } else {
                seen.insert(key, list.len());
                list.push(c);
            }
        }
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_audio_availability_separates_declared_observed_and_verified() {
        let mut c = make_cand(
            ProviderKind::FourKHdHub,
            None,
            Some("1080p"),
            "https://cdn.example/multi.mkv",
        );
        c.audio_languages.clear();
        c.verification_status = "unverified".to_string();
        c.normalized_metadata.declared_languages = vec!["hi".into(), "en".into()];
        c.audio_tracks = vec![make_track_item(Some("1"), Some(0), "ja", "jpn", "aac")];

        let avail = compute_audio_availability(&[c], false);
        assert!(avail.available_languages.is_empty());
        assert!(avail.verified_languages.is_empty());
        assert_eq!(avail.declared_languages, vec!["hi", "en"]);
        assert_eq!(avail.observed_track_languages, vec!["ja"]);
        assert_eq!(avail.language_verification_status, "unverified");
    }

    #[tokio::test]
    async fn test_redirect_to_loopback_blocked_in_production_client() {
        // Confirm validate_url_syntax (used by http_client_builder_base redirect policy)
        // rejects loopback and private IP redirect destinations.
        assert!(crate::net::validate_url_syntax("http://127.0.0.1/secret", true).is_err());
        assert!(crate::net::validate_url_syntax("http://169.254.169.254/latest", true).is_err());
        assert!(crate::net::validate_url_syntax("https://localhost/admin", false).is_err());
    }

    #[tokio::test]
    async fn test_h_synthetic_episode_rejected_from_source_matching() {
        let ep = CanonicalEpisode {
            id: "NP-E-SYNTH".into(),
            anime_id: "NP-A-001".into(),
            season_id: "NP-S-002".into(),
            season_number: 2,
            episode_number: 1,
            title: "Episode 1".into(),
            metadata_status: "unverified".into(),
            provenance: crate::providers::models::EpisodeProvenance::SyntheticFallback,
            verification: crate::providers::models::EpisodeVerification::Unverified,
            overview: None,
            provider_mappings: vec![crate::v3::models::ProviderEpisodeMapping {
                provider_id: ProviderKind::MovieBox,
                provider_anime_id: "mb-1".into(),
                variant_title: Some("Test".into()),
                variant_language: None,
                variant_language_code: None,
                provider_season_number: 2,
                provider_episode_number: 1,
                provider_episode_id: None,
                provider_season_id: None,
                title: None,
                provenance: crate::providers::models::EpisodeProvenance::SyntheticFallback,
                verification: crate::providers::models::EpisodeVerification::Unverified,
                outcome: EpisodeMappingOutcome::Matched,
                confidence: 0.0,
                matching_evidence: vec![],
                exclusion_reason: None,
            }],
            audio: None,
            sources: None,
            cached_provider_coverage: None,
            cached_provider_details: Vec::new(),
            pipeline_stats: PipelineStats::default(),
            excluded_sources_count: 0,
            excluded_reasons: Vec::new(),
            excluded_details: Vec::new(),
            matching_diagnostics: Vec::new(),
        };
        let reg = ProviderRegistry::from_env();
        let res = collect_episode_sources(&reg, &ep).await;
        assert!(res.candidates.is_empty());
        assert_eq!(res.excluded_details.len(), 1);
        assert_eq!(
            res.excluded_details[0].reason,
            "canonical_episode_unverified"
        );
    }

    #[tokio::test]
    async fn test_verified_canonical_episode_allows_unverified_matched_mapping() {
        let ep = CanonicalEpisode {
            id: "NP-E-VERIFIED".into(),
            anime_id: "NP-A-001".into(),
            season_id: "NP-S-001".into(),
            season_number: 1,
            episode_number: 7,
            title: "Greetings from a Stuffed Toy".into(),
            metadata_status: "verified".into(),
            provenance: crate::providers::models::EpisodeProvenance::ProviderListing,
            verification: crate::providers::models::EpisodeVerification::Verified,
            overview: None,
            provider_mappings: vec![
                crate::v3::models::ProviderEpisodeMapping {
                    provider_id: ProviderKind::MovieBox,
                    provider_anime_id: "mb-nonexistent".into(),
                    variant_title: Some("Bleach (Hindi)".into()),
                    variant_language: Some("Hindi".into()),
                    variant_language_code: Some("hi".into()),
                    provider_season_number: 1,
                    provider_episode_number: 7,
                    provider_episode_id: None,
                    provider_season_id: None,
                    title: Some("Episode 7".into()),
                    provenance: crate::providers::models::EpisodeProvenance::SyntheticFallback,
                    verification: crate::providers::models::EpisodeVerification::Unverified,
                    outcome: EpisodeMappingOutcome::Matched,
                    confidence: 0.6,
                    matching_evidence: vec!["controlled_numbered_fallback".into()],
                    exclusion_reason: None,
                },
                crate::v3::models::ProviderEpisodeMapping {
                    provider_id: ProviderKind::Dramachi,
                    provider_anime_id: "dc-1".into(),
                    variant_title: Some("Bleach".into()),
                    variant_language: None,
                    variant_language_code: None,
                    provider_season_number: 1,
                    provider_episode_number: 8,
                    provider_episode_id: None,
                    provider_season_id: None,
                    title: Some("Episode 8".into()),
                    provenance: crate::providers::models::EpisodeProvenance::ProviderListing,
                    verification: crate::providers::models::EpisodeVerification::Verified,
                    outcome: EpisodeMappingOutcome::EpisodeMismatch,
                    confidence: 0.0,
                    matching_evidence: vec![],
                    exclusion_reason: Some("episode_mismatch".into()),
                },
            ],
            audio: None,
            sources: None,
            cached_provider_coverage: None,
            cached_provider_details: Vec::new(),
            pipeline_stats: PipelineStats::default(),
            excluded_sources_count: 0,
            excluded_reasons: Vec::new(),
            excluded_details: Vec::new(),
            matching_diagnostics: Vec::new(),
        };
        let reg = ProviderRegistry::from_env();
        let res = collect_episode_sources(&reg, &ep).await;
        // MovieBox was attempted (not rejected at matching stage); only Dramachi was excluded at matching stage.
        assert_eq!(res.excluded_details.len(), 1);
        assert_eq!(res.excluded_details[0].provider, "dramachi");
        assert_eq!(res.excluded_details[0].reason, "episode_mismatch");
        assert!(res.provider_coverage.contains_key("moviebox"));
    }

    #[test]
    fn test_same_site_cookie_check_blocks_unrelated_domains() {
        assert!(is_same_site_url(
            "https://cdn.anizone.to/stream/master.m3u8",
            "https://anizone.to/anime/bleach/7"
        ));
        assert!(is_same_site_url(
            "https://anizone.to/stream/master.m3u8",
            "https://anizone.to/anime/bleach/7"
        ));
        assert!(!is_same_site_url(
            "https://othercdn.example.com/stream/master.m3u8",
            "https://anizone.to/anime/bleach/7"
        ));
        assert!(!is_same_site_url(
            "http://anizone.to/stream/master.m3u8",
            "https://anizone.to/anime/bleach/7"
        ));
    }
}
