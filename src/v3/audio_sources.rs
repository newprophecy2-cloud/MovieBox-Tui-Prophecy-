use crate::providers::models::{ProviderKind, Release};
use crate::unified::normalize_language_code;
use crate::v3::models::{
    AudioAvailability, AudioStatus, CanonicalEpisode, ProviderSourceCandidate,
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

pub fn language_display_name(code: Option<&str>) -> String {
    match code {
        Some("hi") => "Hindi".to_string(),
        Some("en") => "English".to_string(),
        Some("ja") => "Japanese".to_string(),
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
    // Handle BCP-47 like "en-US" or "hi-IN"
    let base = trimmed.split(['-', '_']).next().unwrap_or(trimmed);
    normalize_language_code(base)
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
        i += 1; // skip '='
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

async fn inspect_manifest_audio_tracks(
    url: &str,
    headers: &HashMap<String, String>,
) -> Vec<ManifestAudioTrack> {
    let lower = url.to_ascii_lowercase();
    let is_mpd = lower.contains(".mpd");
    let is_m3u8 = lower.contains(".m3u8");
    if !is_mpd && !is_m3u8 {
        return Vec::new();
    }
    if crate::net::validate_public_url(url, true).await.is_err() {
        return Vec::new();
    }

    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
    else {
        return Vec::new();
    };
    let mut req = client.get(url);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let Ok(resp) = req.send().await else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let Ok(bytes) = resp.bytes().await else {
        return Vec::new();
    };
    if bytes.len() > 512 * 1024 {
        return Vec::new();
    }
    let text = String::from_utf8_lossy(&bytes);
    if is_mpd {
        parse_dash_mpd_audio_tracks(&text)
    } else {
        parse_hls_m3u8_audio_tracks(&text)
    }
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

    // Order by priority: hi -> en -> ja -> others
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

/// Provider-wise verified audio detection from Release metadata.
/// Never infers language from shared anime titles or unverified filenames.
pub fn extract_verified_language(rel: &Release) -> Option<String> {
    rel.language.as_deref().and_then(normalize_language_code)
}

pub async fn collect_episode_sources(
    registry: &ProviderRegistry,
    episode: &CanonicalEpisode,
) -> (Vec<ProviderSourceCandidate>, bool, BTreeMap<String, String>) {
    let mut coverage = BTreeMap::new();
    if episode.provider_mappings.is_empty() {
        return (Vec::new(), true, coverage);
    }

    let futs = episode.provider_mappings.iter().map(|mapping| {
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
    let mut raw_releases = Vec::new();
    let mut any_succeeded = false;

    for (mapping, outcome) in outcomes {
        let pkey = provider_key(mapping.provider_id).to_string();
        match outcome {
            Ok(releases) => {
                any_succeeded = true;
                coverage.insert(pkey, "success".to_string());
                for rel in releases {
                    if is_candidate_valid(&rel) {
                        raw_releases.push((mapping.clone(), rel));
                    }
                }
            }
            Err(err) => {
                coverage.insert(pkey, "failed".to_string());
                log::warn!(
                    "v3_episode_sources_failed provider={} episode_id={} err={err}",
                    mapping.provider_id.cache_key(),
                    episode.id
                );
            }
        }
    }

    // Inspect DASH/MPD and HLS/M3U8 manifests concurrently for direct stream URLs
    let inspect_futs = raw_releases.iter().map(|(mapping, rel)| async move {
        let primary_mirror = rel.mirrors.first();
        let mut headers = HashMap::new();
        if let Some(m) = primary_mirror {
            for (k, v) in &m.headers {
                if k.eq_ignore_ascii_case("referer")
                    || k.eq_ignore_ascii_case("user-agent")
                    || k.eq_ignore_ascii_case("origin")
                {
                    headers.insert(k.clone(), v.clone());
                }
            }
        }
        let tracks = if let Some(m) = primary_mirror.filter(|m| m.direct_file) {
            inspect_manifest_audio_tracks(&m.resolver_url, &headers).await
        } else {
            Vec::new()
        };
        (mapping.clone(), rel.clone(), headers, tracks)
    });

    let inspected = futures::future::join_all(inspect_futs).await;
    let mut candidates = Vec::new();
    let mut idx = 0usize;
    let mut seen_records: HashSet<(ProviderKind, String, Option<String>)> = HashSet::new();

    for (mapping, rel, headers, tracks) in inspected {
        let primary_mirror = rel.mirrors.first();
        let primary_url = primary_mirror
            .map(|m| m.resolver_url.trim().to_string())
            .unwrap_or_default();
        let stream_url = primary_mirror
            .filter(|m| m.direct_file)
            .map(|m| m.resolver_url.clone());
        let provider_ep_id = mapping.provider_episode_id.clone().unwrap_or_else(|| {
            format!(
                "{}:{}",
                mapping.provider_season_number, mapping.provider_episode_number
            )
        });

        if tracks.is_empty() {
            if !primary_url.is_empty()
                && !seen_records.insert((mapping.provider_id, primary_url.clone(), None))
            {
                continue;
            }
            idx += 1;
            let lang_code = extract_verified_language(&rel);
            let audio_languages: Vec<String> = lang_code.iter().cloned().collect();
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
                audio_languages,
                track_index: None,
                language: language_display_name(lang_code.as_deref()),
                language_code: lang_code,
                audio_track_id: None,
                audio_codec: None,
                channels: None,
                track_selection_metadata: None,
                stream_url,
                headers,
                available: true,
            });
        } else {
            // One independently selectable audio track = one source entry (same stream_url preserved)
            for track in tracks {
                if !primary_url.is_empty()
                    && !seen_records.insert((
                        mapping.provider_id,
                        primary_url.clone(),
                        Some(track.audio_track_id.clone()),
                    ))
                {
                    continue;
                }
                idx += 1;
                let lang_code = track
                    .language_code
                    .clone()
                    .or_else(|| extract_verified_language(&rel));
                let audio_languages: Vec<String> = lang_code.iter().cloned().collect();
                let language = language_display_name(lang_code.as_deref());
                candidates.push(ProviderSourceCandidate {
                    source_id: format!("src-{idx}"),
                    provider_id: mapping.provider_id,
                    provider_name: provider_display_name(mapping.provider_id).to_string(),
                    provider_episode_id: provider_ep_id.clone(),
                    provider_season: mapping.provider_season_number,
                    provider_episode: mapping.provider_episode_number,
                    resolution_u64: rel.resolution_u64(),
                    quality: rel.quality.clone(),
                    codec: rel.codec.clone(),
                    release: rel.clone(),
                    audio_languages,
                    track_index: Some(track.track_index),
                    language,
                    language_code: lang_code,
                    audio_track_id: Some(track.audio_track_id),
                    audio_codec: track.audio_codec,
                    channels: track.channels,
                    track_selection_metadata: Some(track.track_selection_metadata),
                    stream_url: stream_url.clone(),
                    headers: headers.clone(),
                    available: true,
                });
            }
        }
    }

    rank_candidates(&mut candidates, None, None);
    (candidates, !any_succeeded, coverage)
}

pub fn to_public_source_item(c: &ProviderSourceCandidate) -> PublicSourceItem {
    PublicSourceItem {
        source_id: c.source_id.clone(),
        provider: c.provider_name.clone(),
        stream_url: c.stream_url.clone(),
        quality: c.quality.clone(),
        track_index: c.track_index,
        language: c.language.clone(),
        language_code: c.language_code.clone(),
        audio_track_id: c.audio_track_id.clone(),
        codec: c.codec.clone(),
        audio_codec: c.audio_codec.clone(),
        channels: c.channels.clone(),
        track_selection_metadata: c.track_selection_metadata.clone(),
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

fn is_candidate_valid(release: &Release) -> bool {
    release.mirrors.iter().any(|m| {
        let u = m.resolver_url.trim();
        !u.is_empty() && crate::net::validate_url_syntax(u, true).is_ok()
    })
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
            && !exact.audio_languages.contains(req_lang)
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
            .filter(|c| c.audio_languages.contains(req_lang))
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
        match registry
            .resolve_release(candidate.provider_id, &candidate.release)
            .await
        {
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
                    track_index: candidate.track_index,
                    language: candidate.language,
                    language_code: candidate.language_code,
                    quality: candidate.quality,
                    stream_url: url,
                    audio_track_id: candidate.audio_track_id,
                    codec: candidate.codec,
                    audio_codec: candidate.audio_codec,
                    channels: candidate.channels,
                    track_selection_metadata: candidate.track_selection_metadata,
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
            language: language_display_name(code.as_deref()),
            language_code: code,
            audio_track_id: None,
            codec: None,
            audio_codec: None,
            channels: None,
            track_selection_metadata: None,
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
        assert_eq!(tracks[0].track_index, 0);
        assert_eq!(tracks[0].language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[0].language, "Hindi");
        assert_eq!(tracks[0].audio_codec.as_deref(), Some("mp4a.40.2"));
        assert_eq!(tracks[0].channels.as_deref(), Some("2"));
        assert_eq!(
            tracks[0]
                .track_selection_metadata
                .adaptation_set_id
                .as_deref(),
            Some("2")
        );

        assert_eq!(tracks[1].track_index, 1);
        assert_eq!(tracks[1].language_code.as_deref(), Some("en"));
        assert_eq!(tracks[1].channels.as_deref(), Some("6"));

        assert_eq!(tracks[2].track_index, 2);
        assert_eq!(tracks[2].language_code, None);
        assert_eq!(tracks[2].language, "Unknown");
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
        assert_eq!(tracks[0].track_index, 0);
        assert_eq!(tracks[0].language_code.as_deref(), Some("hi"));
        assert_eq!(tracks[0].audio_track_id, "aud:Hindi");
        assert_eq!(
            tracks[0].track_selection_metadata.group_id.as_deref(),
            Some("aud")
        );
        assert_eq!(
            tracks[0].track_selection_metadata.rendition_name.as_deref(),
            Some("Hindi")
        );

        assert_eq!(tracks[1].track_index, 1);
        assert_eq!(tracks[1].language_code.as_deref(), Some("ja"));
    }
}
