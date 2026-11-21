use crate::providers::models::{ProviderKind, Release};
use crate::unified::normalize_language_code;
use crate::v3::models::{
    AudioAvailability, AudioStatus, CanonicalEpisode, ProviderSourceCandidate,
};
use crate::v3::providers::ProviderRegistry;
use std::collections::HashSet;

#[derive(Debug)]
pub enum ResolveFailure {
    AudioUnavailable(String),
    SourceUnavailable(String),
    ResolutionFailed(String),
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

pub async fn collect_episode_sources(
    registry: &ProviderRegistry,
    episode: &CanonicalEpisode,
) -> (Vec<ProviderSourceCandidate>, bool) {
    if episode.provider_mappings.is_empty() {
        return (Vec::new(), true);
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

    for (mapping, outcome) in outcomes {
        match outcome {
            Ok(releases) => {
                any_succeeded = true;
                for rel in releases {
                    if !is_candidate_valid(&rel) {
                        continue;
                    }
                    idx += 1;
                    let audio_languages = rel
                        .language
                        .as_deref()
                        .and_then(normalize_language_code)
                        .into_iter()
                        .collect::<Vec<_>>();
                    let resolution_u64 = rel.resolution_u64();
                    let quality = rel.quality.clone();
                    candidates.push(ProviderSourceCandidate {
                        source_id: format!("{}-src-{idx}", episode.id),
                        provider_id: mapping.provider_id,
                        provider_episode_id: mapping
                            .provider_episode_id
                            .clone()
                            .unwrap_or_else(|| format!("{}:{}", mapping.provider_season_number, mapping.provider_episode_number)),
                        provider_season: mapping.provider_season_number,
                        provider_episode: mapping.provider_episode_number,
                        release: rel,
                        audio_languages,
                        quality,
                        resolution_u64,
                        available: true,
                    });
                }
            }
            Err(err) => {
                log::warn!(
                    "v3_episode_sources_failed provider={} episode_id={} err={err}",
                    mapping.provider_id.cache_key(),
                    episode.id
                );
            }
        }
    }

    rank_candidates(&mut candidates, None, None);
    (candidates, !any_succeeded)
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
            .map(|req| if a.audio_languages.contains(req) { 0usize } else { 1 })
            .unwrap_or(0);
        let b_lang = target_lang
            .as_ref()
            .map(|req| if b.audio_languages.contains(req) { 0usize } else { 1 })
            .unwrap_or(0);
        let a_q = target_q
            .map(|q| a.resolution_u64.abs_diff(q))
            .unwrap_or(0);
        let b_q = target_q
            .map(|q| b.resolution_u64.abs_diff(q))
            .unwrap_or(0);

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
    requested_audio: Option<&str>,
    preferred_quality: Option<&str>,
) -> Result<(String, Vec<(String, String)>), ResolveFailure> {
    if sources.is_empty() {
        return Err(ResolveFailure::SourceUnavailable(
            "No playable source is available.".to_string(),
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
            "Requested audio language '{raw_audio}' is not supported or unavailable."
        )));
    }

    let mut ranked = sources.to_vec();
    if let Some(ref req_lang) = normalized_req {
        let matching: Vec<_> = ranked
            .iter()
            .filter(|c| c.audio_languages.contains(req_lang))
            .cloned()
            .collect();
        if matching.is_empty() {
            // Check whether ANY source explicitly reported language metadata
            let any_known_lang = ranked.iter().any(|c| !c.audio_languages.is_empty());
            if any_known_lang {
                return Err(ResolveFailure::AudioUnavailable(format!(
                    "Requested audio language '{req_lang}' is unavailable for this episode."
                )));
            }
            return Err(ResolveFailure::AudioUnavailable(format!(
                "Requested audio language '{req_lang}' could not be confirmed for this episode."
            )));
        }
        ranked = matching;
    }

    rank_candidates(&mut ranked, normalized_req.as_deref(), preferred_quality);

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
                let safe_headers: Vec<(String, String)> = headers
                    .into_iter()
                    .filter(|(k, _)| {
                        k.eq_ignore_ascii_case("referer")
                            || k.eq_ignore_ascii_case("user-agent")
                            || k.eq_ignore_ascii_case("origin")
                    })
                    .collect();
                return Ok((url, safe_headers));
            }
            Err(err) => {
                log::warn!(
                    "v3_source_resolve_failed provider={} episode_id={} err={err}",
                    candidate.provider_id.cache_key(),
                    episode.id
                );
                last_err = Some(err);
            }
        }
    }

    Err(ResolveFailure::ResolutionFailed(
        last_err.unwrap_or_else(|| "All eligible sources failed to resolve.".to_string()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::models::SourceMirror;

    fn make_cand(provider: ProviderKind, lang: Option<&str>, quality: Option<&str>, url: &str) -> ProviderSourceCandidate {
        ProviderSourceCandidate {
            source_id: "s1".into(),
            provider_id: provider,
            provider_episode_id: "1".into(),
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
            audio_languages: lang.and_then(normalize_language_code).into_iter().collect(),
            quality: quality.map(str::to_string),
            resolution_u64: quality.and_then(parse_quality_target).unwrap_or(0),
            available: true,
        }
    }

    #[test]
    fn test_audio_availability_distinguishes_missing_and_unknown() {
        let srcs = vec![
            make_cand(ProviderKind::FourKHdHub, Some("en"), Some("1080p"), "https://cdn.example/a.mp4"),
            make_cand(ProviderKind::AniZone, Some("ja"), Some("720p"), "https://cdn.example/b.m3u8"),
        ];
        let audio = compute_audio_availability(&srcs, false);
        assert_eq!(audio.hindi, AudioStatus::Missing);
        assert_eq!(audio.english, AudioStatus::Available);
        assert_eq!(audio.japanese, AudioStatus::Available);
        assert_eq!(audio.available_languages, vec!["en", "ja"]);

        let unknown_srcs = vec![make_cand(ProviderKind::MovieBox, None, Some("720p"), "https://cdn.example/c.mp4")];
        let audio_unknown = compute_audio_availability(&unknown_srcs, false);
        assert_eq!(audio_unknown.hindi, AudioStatus::Unknown);
        assert_eq!(audio_unknown.english, AudioStatus::Unknown);
    }

    #[test]
    fn test_ranking_respects_requested_quality_and_language() {
        let mut srcs = vec![
            make_cand(ProviderKind::MovieBox, Some("en"), Some("720p"), "https://cdn.example/1.mp4"),
            make_cand(ProviderKind::Dramachi, Some("hi"), Some("1080p"), "https://cdn.example/2.mp4"),
            make_cand(ProviderKind::FourKHdHub, Some("hi"), Some("480p"), "https://cdn.example/3.mp4"),
        ];
        rank_candidates(&mut srcs, Some("hi"), Some("1080p"));
        assert_eq!(srcs[0].provider_id, ProviderKind::Dramachi);
        assert_eq!(srcs[0].quality.as_deref(), Some("1080p"));
    }
}
