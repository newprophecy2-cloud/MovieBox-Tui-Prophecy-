use crate::providers::models::{ProviderKind, Release};
use crate::unified::normalize_language_code;
use crate::v3::models::{
    AudioAvailability, AudioStatus, CanonicalEpisode, ProviderSourceCandidate,
    ProviderSourcesGroup, PublicSourceItem, ResolveSuccessResponse,
};
use crate::v3::providers::{ProviderRegistry, provider_display_name};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug)]
pub enum ResolveFailure {
    AudioUnavailable(String),
    SourceUnavailable(String),
    ResolutionFailed(String),
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

/// Section 9: Provider-wise audio detection.
/// Uses ONLY verified provider source metadata (`rel.language`).
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
    let mut candidates = Vec::new();
    let mut any_succeeded = false;
    let mut idx = 0usize;
    let mut seen_records: HashSet<(ProviderKind, String, Option<String>)> = HashSet::new();

    for (mapping, outcome) in outcomes {
        let pkey = crate::v3::providers::provider_key(mapping.provider_id).to_string();
        match outcome {
            Ok(releases) => {
                any_succeeded = true;
                coverage.insert(pkey, "success".to_string());
                for rel in releases {
                    if !is_candidate_valid(&rel) {
                        continue;
                    }
                    let primary_mirror = rel.mirrors.first();
                    let primary_url = primary_mirror
                        .map(|m| m.resolver_url.trim().to_string())
                        .unwrap_or_default();
                    let audio_track_id: Option<String> = None;
                    if !primary_url.is_empty()
                        && !seen_records.insert((
                            mapping.provider_id,
                            primary_url.clone(),
                            audio_track_id.clone(),
                        ))
                    {
                        continue;
                    }
                    idx += 1;
                    let lang_code = extract_verified_language(&rel);
                    let audio_languages: Vec<String> = lang_code.iter().cloned().collect();
                    let language = language_display_name(lang_code.as_deref());
                    let resolution_u64 = rel.resolution_u64();
                    let quality = rel.quality.clone();
                    let codec = rel.codec.clone();
                    let stream_url = primary_mirror
                        .filter(|m| m.direct_file)
                        .map(|m| m.resolver_url.clone());
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

                    candidates.push(ProviderSourceCandidate {
                        source_id: format!("src-{idx}"),
                        provider_id: mapping.provider_id,
                        provider_name: provider_display_name(mapping.provider_id).to_string(),
                        provider_episode_id: mapping.provider_episode_id.clone().unwrap_or_else(
                            || {
                                format!(
                                    "{}:{}",
                                    mapping.provider_season_number, mapping.provider_episode_number
                                )
                            },
                        ),
                        provider_season: mapping.provider_season_number,
                        provider_episode: mapping.provider_episode_number,
                        release: rel,
                        audio_languages,
                        language,
                        language_code: lang_code,
                        audio_track_id,
                        codec,
                        audio_codec: None,
                        channels: None,
                        quality,
                        resolution_u64,
                        stream_url,
                        headers,
                        available: true,
                    });
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

    rank_candidates(&mut candidates, None, None);
    (candidates, !any_succeeded, coverage)
}

pub fn to_public_source_item(c: &ProviderSourceCandidate) -> PublicSourceItem {
    PublicSourceItem {
        source_id: c.source_id.clone(),
        provider: c.provider_name.clone(),
        stream_url: c.stream_url.clone(),
        quality: c.quality.clone(),
        language: c.language.clone(),
        language_code: c.language_code.clone(),
        audio_track_id: c.audio_track_id.clone(),
        codec: c.codec.clone(),
        audio_codec: c.audio_codec.clone(),
        channels: c.channels.clone(),
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
                    language: candidate.language,
                    language_code: candidate.language_code,
                    quality: candidate.quality,
                    stream_url: url,
                    audio_track_id: candidate.audio_track_id,
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
            language: language_display_name(code.as_deref()),
            language_code: code,
            audio_track_id: None,
            codec: None,
            audio_codec: None,
            channels: None,
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
}
