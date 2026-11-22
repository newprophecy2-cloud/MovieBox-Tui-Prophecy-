use crate::providers::models::{ProviderKind, Release};
use crate::unified::normalize_language_code;
use crate::v3::audio_id::{
    AudioIdConfig, SpeechIdResult, extract_ffprobe_audio_streams, identify_track_audio_language,
};
use crate::v3::models::{
    AudioAvailability, AudioStatus, AudioTrackItem, CanonicalEpisode, EpisodeMappingOutcome,
    ExcludedSourceRecord, PipelineStats, ProviderCoverageDetail, ProviderSourceCandidate,
    ProviderSourcesGroup, PublicSourceItem, ResolveSuccessResponse, TrackSelectionMetadata,
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
    pub language_code: Option<String>,
    pub language: String,
    pub audio_codec: Option<String>,
    pub channels: Option<String>,
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
                language_code: language_code.clone(),
                language,
                audio_codec,
                channels,
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
        let language_code = normalize_manifest_lang(attrs.get("LANGUAGE").map(String::as_str));
        let language = language_display_name(language_code.as_deref());
        let audio_codec = attrs.get("CODECS").cloned();
        let channels = attrs.get("CHANNELS").cloned();
        let audio_track_id = match (&group_id, &rendition_name) {
            (Some(g), Some(n)) => format!("{g}:{n}"),
            (_, Some(n)) => n.clone(),
            _ => format!("audio-{track_index}"),
        };

        tracks.push(ManifestAudioTrack {
            track_index,
            audio_track_id,
            language_code: language_code.clone(),
            language,
            audio_codec,
            channels,
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
            checked_sources: false,
        };
    }

    let mut langs = Vec::new();
    let mut seen = HashSet::new();
    let mut any_explicit_lang = false;
    let mut any_unknown_lang = false;

    for src in sources {
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

    if !any_explicit_lang {
        return AudioAvailability {
            hindi: AudioStatus::Unknown,
            english: AudioStatus::Unknown,
            japanese: AudioStatus::Unknown,
            available_languages: Vec::new(),
            checked_sources: true,
        };
    }

    langs.sort_by_key(|code| match code.as_str() {
        "hi" => 0,
        "en" => 1,
        "ja" => 2,
        _ => 3,
    });

    let status_for = |target: &str| -> AudioStatus {
        if seen.contains(target) {
            AudioStatus::Available
        } else if any_unknown_lang {
            AudioStatus::Unknown
        } else {
            AudioStatus::Missing
        }
    };

    AudioAvailability {
        hindi: status_for("hi"),
        english: status_for("en"),
        japanese: status_for("ja"),
        available_languages: langs,
        checked_sources: true,
    }
}

async fn validate_stream_url_and_inspect(
    url: &str,
    headers: &HashMap<String, String>,
) -> Result<(String, Vec<ManifestAudioTrack>), String> {
    if crate::net::validate_public_url(url, true).await.is_err() {
        return Err("invalid_or_private_url".to_string());
    }

    let lower = url.to_ascii_lowercase();
    let is_mpd = lower.contains(".mpd");
    let is_m3u8 = lower.contains(".m3u8");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()
        .map_err(|_| "http_client_error".to_string())?;

    let mut req = client.get(url);
    if !is_mpd && !is_m3u8 {
        req = req.header("Range", "bytes=0-1023");
    }
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }

    let resp = req.send().await.map_err(|_| "request_failed".to_string())?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("http_{}", status.as_u16()));
    }

    if is_mpd || is_m3u8 {
        let bytes = resp
            .bytes()
            .await
            .map_err(|_| "manifest_read_failed".to_string())?;
        if bytes.len() > 512 * 1024 {
            return Err("manifest_too_large".to_string());
        }
        let text = String::from_utf8_lossy(&bytes);
        if is_mpd {
            if !text.contains("<MPD") {
                return Err("invalid_mpd_manifest".to_string());
            }
            return Ok((
                "manifest_validated".to_string(),
                parse_dash_mpd_audio_tracks(&text),
            ));
        }
        if !text.contains("#EXTM3U") {
            return Err("invalid_m3u8_manifest".to_string());
        }
        return Ok((
            "manifest_validated".to_string(),
            parse_hls_m3u8_audio_tracks(&text),
        ));
    }

    Ok(("http_reachable".to_string(), Vec::new()))
}

pub async fn collect_episode_sources(
    registry: &ProviderRegistry,
    episode: &CanonicalEpisode,
) -> SourceCollectionResult {
    let mut coverage = BTreeMap::new();
    let mut stats = PipelineStats::default();
    let mut excluded_details: Vec<ExcludedSourceRecord> = Vec::new();
    let mut excluded_reasons: Vec<String> = Vec::new();
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
        };
    }

    // Bug #13: Only accept confidently matched episode mappings; record non-matched separately
    let mut matched_mappings = Vec::new();
    for m in &episode.provider_mappings {
        let pkey = provider_key(m.provider_id).to_string();
        if m.outcome == EpisodeMappingOutcome::Matched {
            matched_mappings.push(m.clone());
        } else {
            excluded_details.push(ExcludedSourceRecord {
                provider: pkey.clone(),
                source_id: None,
                stage: "matching".to_string(),
                reason: "episode_mismatch".to_string(),
            });
            if excluded_reasons.len() < 25 {
                excluded_reasons.push(format!("{pkey}:episode_mismatch"));
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
                    excluded_source_count: 0,
                    errors: Vec::new(),
                });

        match outcome {
            Ok(releases) => {
                any_succeeded = true;
                coverage.insert(pkey.clone(), "success".to_string());
                pdet.request_status = "success".to_string();
                stats.raw_sources += releases.len();
                for rel in releases {
                    if rel.mirrors.is_empty() {
                        pdet.excluded_source_count += 1;
                        pdet.errors.push("empty_mirrors".to_string());
                        excluded_details.push(ExcludedSourceRecord {
                            provider: pkey.clone(),
                            source_id: None,
                            stage: "parsing".to_string(),
                            reason: "empty_mirrors".to_string(),
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

    let validate_futs = mirror_tasks
        .into_iter()
        .map(|(mapping, single_rel, mirror)| {
            let reg = registry.clone();
            async move {
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
                                "resolve_failed".to_string(),
                            ));
                        }
                    }
                };

                let mut headers = HashMap::new();
                for (k, v) in raw_headers {
                    if k.eq_ignore_ascii_case("referer")
                        || k.eq_ignore_ascii_case("user-agent")
                        || k.eq_ignore_ascii_case("origin")
                    {
                        headers.insert(k, v);
                    }
                }

                match validate_stream_url_and_inspect(&resolved_url, &headers).await {
                    Ok((mut playability_status, manifest_tracks)) => {
                        // Bug #5: Integrate FFprobe metadata extraction for non-manifest direct streams
                        let ffprobe_tracks = if manifest_tracks.is_empty() {
                            let probed =
                                extract_ffprobe_audio_streams(&resolved_url, &headers).await;
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
                            resolved_url,
                            headers,
                            playability_status,
                            manifest_tracks,
                            ffprobe_tracks,
                        ))
                    }
                    Err(reason) => Err((pkey, "validation".to_string(), reason)),
                }
            }
        });

    let validated = futures::future::join_all(validate_futs).await;
    let audio_id_cfg = AudioIdConfig::from_env();
    let mut candidates = Vec::new();
    let mut idx = 0usize;
    let mut seen_urls: HashSet<(ProviderKind, String)> = HashSet::new();

    for item in validated {
        let (
            mapping,
            rel,
            resolved_url,
            headers,
            playability_status,
            manifest_tracks,
            ffprobe_tracks,
        ) = match item {
            Ok(v) => {
                stats.resolved_sources += 1;
                stats.validated_sources += 1;
                v
            }
            Err((pkey, stage, reason)) => {
                if stage == "validation" {
                    stats.resolved_sources += 1;
                }
                if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
                    pdet.excluded_source_count += 1;
                    pdet.errors.push(reason.clone());
                }
                excluded_details.push(ExcludedSourceRecord {
                    provider: pkey.clone(),
                    source_id: None,
                    stage,
                    reason: reason.clone(),
                });
                if excluded_reasons.len() < 25 {
                    excluded_reasons.push(format!("{pkey}:{reason}"));
                }
                continue;
            }
        };

        let pkey = provider_key(mapping.provider_id).to_string();
        if !seen_urls.insert((mapping.provider_id, resolved_url.clone())) {
            if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
                pdet.excluded_source_count += 1;
            }
            excluded_details.push(ExcludedSourceRecord {
                provider: pkey,
                source_id: None,
                stage: "deduplication".to_string(),
                reason: "duplicate_url".to_string(),
            });
            continue;
        }

        if let Some(pdet) = provider_detail_map.get_mut(&pkey) {
            pdet.validated_source_count += 1;
        }

        let provider_ep_id = mapping.provider_episode_id.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                mapping.provider_season_number, mapping.provider_episode_number
            )
        });

        // Bug #4: Preserve multi-language provider declarations independently
        let declared_langs = extract_declared_languages(&rel);
        let declared_audio_tracks: Vec<AudioTrackItem> = declared_langs
            .iter()
            .map(|code| AudioTrackItem {
                track_id: None,
                track_index: None,
                language: language_display_name(Some(code)),
                language_code: Some(code.clone()),
                declared_language: Some(language_display_name(Some(code))),
                declared_language_code: Some(code.clone()),
                verified_language: None,
                verified_language_code: None,
                codec: None,
                channels: None,
                sample_rate: None,
                is_default: None,
                verification_status: "unverified".to_string(),
                track_selection_metadata: None,
            })
            .collect();

        // Bug #6: ONE source URL = ONE ProviderSourceCandidate with Vec<AudioTrackItem>
        let mut audio_tracks: Vec<AudioTrackItem> = Vec::new();
        let mut verified_langs_for_source: Vec<String> = Vec::new();
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
                let declared_code = track.language_code.clone();
                let display_code = verified_code.clone().or_else(|| declared_code.clone());
                audio_tracks.push(AudioTrackItem {
                    track_id: Some(track.audio_track_id),
                    track_index: Some(track.track_index),
                    language: language_display_name(display_code.as_deref()),
                    language_code: display_code,
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
                    is_default: None,
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
                let declared_code = ftrack.declared_language_code.clone();
                let display_code = verified_code.clone().or_else(|| declared_code.clone());
                audio_tracks.push(AudioTrackItem {
                    track_id: ftrack.provider_track_id.clone(),
                    track_index: Some(ftrack.track_index),
                    language: language_display_name(display_code.as_deref()),
                    language_code: display_code,
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

        idx += 1;
        let primary_speech = first_speech.unwrap_or(SpeechIdResult {
            language_code: None,
            confidence: None,
            sample_duration_seconds: None,
            detection_method: "none_configured".to_string(),
            verification_status: "unverified".to_string(),
        });
        let primary_declared_code = audio_tracks
            .first()
            .and_then(|t| t.declared_language_code.clone())
            .or_else(|| declared_langs.first().cloned());
        let primary_verified_code = verified_langs_for_source.first().cloned();
        let primary_display_code = primary_verified_code
            .clone()
            .or_else(|| primary_declared_code.clone());
        let primary_track = audio_tracks.first().cloned();

        candidates.push(ProviderSourceCandidate {
            source_id: format!("src-{idx}"),
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
            stream_url: Some(resolved_url),
            headers,
            available: true,
        });
    }

    rank_candidates(&mut candidates, None, None);
    stats.returned_sources = candidates.len();
    let excluded_sources = excluded_details.len();
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
        };
        let reg = ProviderRegistry::from_env();
        let res = resolve_episode_stream(&reg, &ep, &[fake_hi], None, Some("hi"), None).await;
        assert!(matches!(res, Err(ResolveFailure::AudioUnavailable(_))));
    }
}
