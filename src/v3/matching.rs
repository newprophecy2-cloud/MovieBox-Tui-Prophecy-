use crate::providers::models::{Episode, MediaDetails, MediaType, ProviderKind};
use crate::v3::models::{
    CanonicalAnime, CanonicalEpisode, CanonicalSeason, EpisodeMappingOutcome, MatchConfidence,
    NormalizedAnime, ProviderAnimeRef, ProviderEpisodeMapping,
};
use crate::v3::session::{
    format_anime_id, format_episode_id, format_season_id, generate_slug_token,
};
use std::collections::{BTreeMap, HashSet};

fn fold_latin_char(ch: char) -> char {
    match ch {
        'á' | 'à' | 'â' | 'ä' | 'ã' | 'å' | 'Á' | 'À' | 'Â' | 'Ä' | 'Ã' | 'Å' => 'a',
        'é' | 'è' | 'ê' | 'ë' | 'É' | 'È' | 'Ê' | 'Ë' => 'e',
        'í' | 'ì' | 'î' | 'ï' | 'Í' | 'Ì' | 'Î' | 'Ï' => 'i',
        'ó' | 'ò' | 'ô' | 'ö' | 'õ' | 'Ó' | 'Ò' | 'Ô' | 'Ö' | 'Õ' => 'o',
        'ú' | 'ù' | 'û' | 'ü' | 'Ú' | 'Ù' | 'Û' | 'Ü' => 'u',
        'ñ' | 'Ñ' => 'n',
        'ç' | 'Ç' => 'c',
        _ => ch,
    }
}

fn is_season_range_token(tok: &str) -> bool {
    let lower = tok.trim().to_ascii_lowercase();
    if let Some((left, right)) = lower.split_once('-') {
        let l_ok = left.starts_with('s')
            && left.len() >= 2
            && left[1..].chars().all(|c| c.is_ascii_digit());
        let r_ok = right.starts_with('s')
            && right.len() >= 2
            && right[1..].chars().all(|c| c.is_ascii_digit());
        return l_ok && r_ok;
    }
    // Single trailing "S1" / "S01" pack suffix (not "Season 2")
    lower.starts_with('s')
        && lower.len() >= 2
        && lower.len() <= 3
        && lower[1..].chars().all(|c| c.is_ascii_digit())
}

fn strip_trailing_split_season(trimmed: &str) -> Option<&str> {
    let lower = trimmed.to_ascii_lowercase();
    for suffix in [
        "second season",
        "third season",
        "fourth season",
        "fifth season",
        "final season",
        "2nd season",
        "3rd season",
        "4th season",
        "1st season",
        "season 1",
        "season 2",
        "season 3",
        "season 4",
        "season 5",
        "part 1",
        "part 2",
        "part 3",
        "cour 1",
        "cour 2",
        "hindi dubbed",
        "english dubbed",
        "hindi dub",
        "english dub",
        "dual audio",
        "multi audio",
        "dubbed",
        "subbed",
    ] {
        if lower.ends_with(suffix) && trimmed.len() > suffix.len() {
            let prefix = trimmed[..trimmed.len() - suffix.len()].trim_end();
            let prefix =
                prefix.trim_end_matches(|c: char| c == ':' || c == '-' || c.is_whitespace());
            if !prefix.is_empty() {
                return Some(prefix);
            }
        }
    }
    None
}

fn infer_season_from_title(raw: &str) -> Option<usize> {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("second season") || lower.contains("2nd season") || lower.contains("season 2")
    {
        return Some(2);
    }
    if lower.contains("third season") || lower.contains("3rd season") || lower.contains("season 3")
    {
        return Some(3);
    }
    if lower.contains("fourth season") || lower.contains("4th season") || lower.contains("season 4")
    {
        return Some(4);
    }
    None
}

/// Extracts explicit language metadata from a provider listing title (e.g., `"Black Clover (Hindi)"`
/// -> `(Some("Hindi"), Some("hi"))`). Never invents a language for untagged titles.
pub fn extract_title_language_metadata(raw_title: &str) -> (Option<String>, Option<String>) {
    let lower = raw_title.to_ascii_lowercase();
    let patterns: &[(&[&str], &str, &str)] = &[
        (
            &["(hindi)", "[hindi]", "hindi dub", "hindi dubbed"],
            "Hindi",
            "hi",
        ),
        (
            &["(english)", "[english]", "english dub", "english dubbed"],
            "English",
            "en",
        ),
        (
            &["(japanese)", "[japanese]", "japanese dub", "japanese audio"],
            "Japanese",
            "ja",
        ),
        (&["(tamil)", "[tamil]", "tamil dub"], "Tamil", "ta"),
        (&["(telugu)", "[telugu]", "telugu dub"], "Telugu", "te"),
        (&["(bengali)", "[bengali]", "bengali dub"], "Bengali", "bn"),
        (&["(korean)", "[korean]", "korean dub"], "Korean", "ko"),
        (&["(spanish)", "[spanish]", "spanish dub"], "Spanish", "es"),
        (&["(french)", "[french]", "french dub"], "French", "fr"),
        (&["(german)", "[german]", "german dub"], "German", "de"),
    ];
    for (needles, name, code) in patterns {
        if needles.iter().any(|n| lower.contains(n)) {
            return (Some((*name).to_string()), Some((*code).to_string()));
        }
    }
    (None, None)
}

fn has_dub_or_pack_tag(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    if [
        "[hindi]",
        "[english]",
        "[japanese]",
        "[sub]",
        "[dub]",
        "(hindi)",
        "(english)",
        "(dub)",
        "(sub)",
        "hindi dub",
        "english dub",
        "hindi dubbed",
        "english dubbed",
        "dubbed",
        "subbed",
        "dual audio",
        "multi audio",
        "second season",
        "third season",
        "fourth season",
        "final season",
        "2nd season",
        "3rd season",
        "season 2",
        "season 3",
    ]
    .iter()
    .any(|tag| lower.contains(tag))
    {
        return true;
    }
    raw.split_whitespace().any(is_season_range_token)
}

pub fn normalize_title_for_match(raw: &str) -> String {
    let mut s = raw.trim().to_string();
    // Strip trailing bracketed/parenthesized tags, S1-S2 suffixes, and split-season suffixes
    loop {
        let trimmed = s.trim();
        let stripped = if let Some(idx) = trimmed.rfind('[')
            && trimmed.ends_with(']')
        {
            trimmed[..idx].trim()
        } else if let Some(idx) = trimmed.rfind('(')
            && trimmed.ends_with(')')
        {
            let inner = trimmed[idx + 1..trimmed.len() - 1]
                .trim()
                .to_ascii_lowercase();
            let is_year = inner.len() == 4 && inner.chars().all(|c| c.is_ascii_digit());
            let is_lang_or_mode = matches!(
                inner.as_str(),
                "dub"
                    | "sub"
                    | "dubbed"
                    | "subbed"
                    | "hindi"
                    | "english"
                    | "japanese"
                    | "hindi dub"
                    | "english dub"
                    | "dual audio"
                    | "multi audio"
                    | "tv"
                    | "uncensored"
            );
            if is_year || is_lang_or_mode {
                trimmed[..idx].trim()
            } else {
                break;
            }
        } else if let Some((head, tail)) = trimmed.rsplit_once(char::is_whitespace)
            && is_season_range_token(tail)
        {
            head.trim()
        } else if let Some(prefix) = strip_trailing_split_season(trimmed) {
            prefix
        } else {
            break;
        };
        if stripped.is_empty() || stripped == trimmed {
            break;
        }
        s = stripped.to_string();
    }

    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for raw_ch in s.chars() {
        let ch = fold_latin_char(raw_ch);
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }
    out.trim().to_string()
}

fn has_special_or_sequel_marker(raw: &str) -> Option<&'static str> {
    let lower = raw.to_ascii_lowercase();
    for marker in [
        "ova",
        "special",
        "movie",
        "theatrical",
        "shippuden",
        "shippûden",
        "boruto",
        "brotherhood",
    ] {
        if lower.contains(marker) {
            return Some(match marker {
                "shippûden" => "shippuden",
                other => other,
            });
        }
    }
    None
}

pub fn anime_match_confidence(a: &NormalizedAnime, b: &CanonicalAnime) -> MatchConfidence {
    if a.media_type != b.media_type {
        return MatchConfidence::NoMatch;
    }
    let norm_a = normalize_title_for_match(&a.title);
    let norm_b = normalize_title_for_match(&b.title);
    if norm_a.is_empty() || norm_b.is_empty() {
        return MatchConfidence::NoMatch;
    }
    if has_special_or_sequel_marker(&a.title) != has_special_or_sequel_marker(&b.title) {
        return MatchConfidence::NoMatch;
    }

    let title_matched = norm_a == norm_b
        || b.alternative_titles
            .iter()
            .any(|alt| normalize_title_for_match(alt) == norm_a)
        || a.alternative_titles
            .iter()
            .any(|alt| normalize_title_for_match(alt) == norm_b);

    if !title_matched {
        return MatchConfidence::NoMatch;
    }

    if let (Some(ya), Some(yb)) = (a.year, b.year) {
        let diff = ya.abs_diff(yb);
        if diff > 4 {
            // Allow wider year gap ONLY when at least one side has an explicit dub/language/season-pack/split-season tag
            // (e.g. "Naruto Shippuden" 2007 vs "Naruto: Shippuden [Hindi]" 2017, "Black Clover" 2017 vs "Black Clover Second Season" 2026),
            // preventing false-positive merges of unrelated remakes like Hunter x Hunter (1999 vs 2011).
            let has_pack_or_dub = has_dub_or_pack_tag(&a.title) || has_dub_or_pack_tag(&b.title);
            if !(has_pack_or_dub && diff <= 15) {
                return MatchConfidence::NoMatch;
            }
        }
    }

    MatchConfidence::HighConfidenceMatch
}

pub fn query_matches_anime(query: &str, anime: &CanonicalAnime) -> bool {
    let norm_q = normalize_title_for_match(query);
    if norm_q.is_empty() {
        return true;
    }
    let q_tokens: Vec<&str> = norm_q.split_whitespace().filter(|t| t.len() >= 2).collect();
    if q_tokens.is_empty() {
        return true;
    }

    let check_title = |candidate: &str| -> bool {
        let norm_c = normalize_title_for_match(candidate);
        if norm_c.contains(&norm_q) {
            return true;
        }
        let c_tokens: HashSet<&str> = norm_c.split_whitespace().collect();
        q_tokens.iter().all(|qt| c_tokens.contains(qt))
    };

    check_title(&anime.title) || anime.alternative_titles.iter().any(|alt| check_title(alt))
}

pub fn merge_search_results(
    normalized: Vec<NormalizedAnime>,
    max_results: usize,
) -> Vec<CanonicalAnime> {
    let mut groups: Vec<CanonicalAnime> = Vec::new();
    let mut used_tokens: HashSet<String> = HashSet::new();

    for item in normalized {
        if item.title.trim().is_empty() {
            continue;
        }
        if let Some(existing) = groups
            .iter_mut()
            .find(|g| anime_match_confidence(&item, g) == MatchConfidence::HighConfidenceMatch)
        {
            if !existing.provider_records.iter().any(|r| {
                r.provider_id == item.provider_id && r.provider_anime_id == item.provider_anime_id
            }) {
                let (declared_language, declared_language_code) =
                    extract_title_language_metadata(&item.title);
                existing.provider_records.push(ProviderAnimeRef {
                    provider_id: item.provider_id,
                    provider_anime_id: item.provider_anime_id.clone(),
                    title: item.title.clone(),
                    declared_language,
                    declared_language_code,
                    year: item.year,
                    media_type: item.media_type,
                    details: None,
                    details_failed: false,
                });
            }
            match (existing.year, item.year) {
                (None, Some(y)) => existing.year = Some(y),
                (Some(ey), Some(iy)) if iy < ey => existing.year = Some(iy),
                _ => {}
            }
            if existing.poster_url.is_none() {
                existing.poster_url = item.poster_url;
            }
            if existing.description.is_none() {
                existing.description = item.description;
            }
            // Prefer clean title without [Tag] or S1-S2 pack suffix
            if has_dub_or_pack_tag(&existing.title) && !has_dub_or_pack_tag(&item.title) {
                if !existing.alternative_titles.contains(&existing.title) {
                    existing.alternative_titles.push(existing.title.clone());
                }
                existing.title = item.title;
            } else if item.title != existing.title
                && !existing.alternative_titles.contains(&item.title)
            {
                existing.alternative_titles.push(item.title);
            }
            continue;
        }

        let mut token = generate_slug_token();
        while !used_tokens.insert(token.clone()) {
            token = generate_slug_token();
        }
        let (declared_language, declared_language_code) =
            extract_title_language_metadata(&item.title);
        groups.push(CanonicalAnime {
            id: format_anime_id(&token),
            slug_token: token,
            title: item.title.clone(),
            alternative_titles: item.alternative_titles,
            media_type: item.media_type,
            year: item.year,
            description: item.description,
            poster_url: item.poster_url,
            genres: item.genres,
            provider_records: vec![ProviderAnimeRef {
                provider_id: item.provider_id,
                provider_anime_id: item.provider_anime_id,
                title: item.title,
                declared_language,
                declared_language_code,
                year: item.year,
                media_type: item.media_type,
                details: None,
                details_failed: false,
            }],
            seasons: Vec::new(),
            total_seasons: 0,
            total_episodes: 0,
            season_structure_status: "pending".to_string(),
            details_hydrated: false,
        });
    }

    groups.sort_by(|a, b| {
        b.provider_records
            .len()
            .cmp(&a.provider_records.len())
            .then_with(|| a.title.cmp(&b.title))
    });
    groups.truncate(max_results);
    groups
}

pub fn build_canonical_structure(anime: &mut CanonicalAnime) {
    let mut best_year = anime.year;
    let mut best_desc = anime.description.clone();
    let mut best_poster = anime.poster_url.clone();
    let mut genres_set = HashSet::new();
    let mut genres = Vec::new();

    for rec in &anime.provider_records {
        if let Some(d) = &rec.details {
            if best_year.is_none()
                && let Some(y) = d
                    .year
                    .as_deref()
                    .and_then(|s| s.trim().get(..4))
                    .and_then(|s| s.parse::<u32>().ok())
            {
                best_year = Some(y);
            }
            if best_desc.is_none() && d.description.as_ref().is_some_and(|s| !s.trim().is_empty()) {
                best_desc = d.description.clone();
            }
            if best_poster.is_none() && d.poster_url.as_ref().is_some_and(|s| !s.trim().is_empty())
            {
                best_poster = d.poster_url.clone();
            }
            for g in &d.genres {
                let key = g.trim().to_ascii_lowercase();
                if !key.is_empty() && genres_set.insert(key) {
                    genres.push(g.clone());
                }
            }
        }
    }
    anime.year = best_year;
    anime.description = best_desc;
    anime.poster_url = best_poster;
    anime.genres = genres;

    let any_details = anime
        .provider_records
        .iter()
        .any(|rec| rec.details.is_some());

    // One-by-one episode mapping across all provider variants (preserving every MovieBox/provider variant)
    type EpisodeBucketEntry<'a> = (
        ProviderKind,
        String,
        String,
        Option<String>,
        Option<String>,
        bool,
        &'a Episode,
    );
    let mut episode_buckets: BTreeMap<(bool, usize, usize), Vec<EpisodeBucketEntry<'_>>> =
        BTreeMap::new();

    for rec in &anime.provider_records {
        let Some(d) = &rec.details else {
            continue;
        };
        if d.media_type == MediaType::Movie && d.seasons.is_empty() {
            // Movie treated as canonical S1E1 while preserving provider (0,0) coordinates
            episode_buckets.entry((false, 1, 1)).or_default().push((
                rec.provider_id,
                rec.provider_anime_id.clone(),
                rec.title.clone(),
                rec.declared_language.clone(),
                rec.declared_language_code.clone(),
                false,
                &MOVIES_SYNTHETIC_EP,
            ));
            continue;
        }
        let split_season_override = if d.seasons.len() == 1 && d.seasons[0].number <= 1 {
            infer_season_from_title(&rec.title)
        } else {
            None
        };
        for s in &d.seasons {
            for ep in &s.episodes {
                let canonical_season = split_season_override.unwrap_or(ep.season);
                let key = (canonical_season == 0, canonical_season, ep.number);
                episode_buckets.entry(key).or_default().push((
                    rec.provider_id,
                    rec.provider_anime_id.clone(),
                    rec.title.clone(),
                    rec.declared_language.clone(),
                    rec.declared_language_code.clone(),
                    split_season_override.is_some(),
                    ep,
                ));
            }
        }
    }

    let anime_norm_title = normalize_title_for_match(&anime.title);
    let mut seasons_map: BTreeMap<(bool, usize), Vec<CanonicalEpisode>> = BTreeMap::new();
    let mut seen_episode_ids = HashSet::new();

    for ((is_special, season_num, ep_num), entries) in episode_buckets {
        let season_id = format_season_id(&anime.slug_token, season_num);
        let episode_id = format_episode_id(&anime.slug_token, season_num, ep_num);
        if !seen_episode_ids.insert(episode_id.clone()) {
            continue;
        }

        // Prefer a descriptive (non-generic) canonical episode title across all provider entries
        let mut title: Option<String> = None;
        let mut overview: Option<String> = None;
        for (_, _, _, _, _, _, ep) in &entries {
            if let Some(t) = ep.title.as_ref().filter(|s| !s.trim().is_empty()) {
                let trimmed = t.trim();
                let is_generic = is_generic_episode_title(trimmed, &anime_norm_title, ep_num);
                match &title {
                    None => title = Some(trimmed.to_string()),
                    Some(existing)
                        if is_generic_episode_title(existing, &anime_norm_title, ep_num)
                            && !is_generic =>
                    {
                        title = Some(trimmed.to_string());
                    }
                    _ => {}
                }
            }
            if overview.is_none()
                && let Some(o) = ep.overview.as_ref().filter(|s| !s.trim().is_empty())
            {
                overview = Some(o.trim().to_string());
            }
        }

        // Count occurrences per (provider_id, provider_anime_id) to detect ambiguous duplicate mappings
        let mut variant_counts: std::collections::HashMap<(ProviderKind, String), usize> =
            std::collections::HashMap::new();
        for (prov, prov_anime_id, _, _, _, _, _) in &entries {
            *variant_counts
                .entry((*prov, prov_anime_id.clone()))
                .or_insert(0) += 1;
        }

        let mut mappings = Vec::new();
        let mut seen_provider_variants = HashSet::new();

        for (
            prov,
            prov_anime_id,
            variant_title,
            variant_lang,
            variant_lang_code,
            used_split_override,
            ep,
        ) in &entries
        {
            let variant_key = (*prov, prov_anime_id.clone());
            let is_ambiguous = variant_counts.get(&variant_key).copied().unwrap_or(0) > 1;
            if !seen_provider_variants.insert(variant_key) {
                continue;
            }
            let (outcome, confidence, matching_evidence, exclusion_reason) = evaluate_episode_match(
                anime.media_type,
                &anime_norm_title,
                season_num,
                ep_num,
                title.as_deref(),
                ep,
                *used_split_override,
                is_ambiguous,
            );
            mappings.push(ProviderEpisodeMapping {
                provider_id: *prov,
                provider_anime_id: prov_anime_id.clone(),
                variant_title: Some(variant_title.clone()),
                variant_language: variant_lang.clone(),
                variant_language_code: variant_lang_code.clone(),
                provider_season_number: ep.season,
                provider_episode_number: ep.number,
                provider_episode_id: ep.provider_episode_id.clone(),
                title: ep.title.clone(),
                outcome,
                confidence,
                matching_evidence,
                exclusion_reason,
            });
        }

        let (final_title, metadata_status) = if anime.media_type == MediaType::Movie {
            (anime.title.clone(), "verified".to_string())
        } else if let Some(t) = title {
            let is_generic = is_generic_episode_title(&t, &anime_norm_title, ep_num);
            let status = if is_generic { "unverified" } else { "verified" };
            (t, status.to_string())
        } else {
            (format!("Episode {ep_num}"), "unverified".to_string())
        };

        let canonical_ep = CanonicalEpisode {
            id: episode_id,
            anime_id: anime.id.clone(),
            season_id: season_id.clone(),
            season_number: season_num,
            episode_number: ep_num,
            title: final_title,
            metadata_status,
            overview,
            provider_mappings: mappings,
            audio: None,
            sources: None,
            cached_provider_coverage: None,
            cached_provider_details: Vec::new(),
            pipeline_stats: crate::v3::models::PipelineStats::default(),
            excluded_sources_count: 0,
            excluded_reasons: Vec::new(),
            excluded_details: Vec::new(),
            matching_diagnostics: Vec::new(),
        };
        seasons_map
            .entry((is_special, season_num))
            .or_default()
            .push(canonical_ep);
    }

    let mut seasons = Vec::with_capacity(seasons_map.len());
    let mut total_episodes = 0usize;
    for ((_, season_num), eps) in seasons_map {
        if eps.is_empty() {
            continue;
        }
        total_episodes += eps.len();
        seasons.push(CanonicalSeason {
            id: format_season_id(&anime.slug_token, season_num),
            anime_id: anime.id.clone(),
            season_number: season_num,
            episodes: eps,
        });
    }

    anime.seasons = seasons;
    if anime.media_type == MediaType::Movie {
        anime.total_seasons = 0;
        anime.total_episodes = 0;
    } else {
        anime.total_seasons = anime.seasons.len();
        anime.total_episodes = total_episodes;
    }
    anime.season_structure_status = if !any_details {
        "unavailable".to_string()
    } else if anime.provider_records.iter().any(|r| r.details_failed) {
        "partial".to_string()
    } else {
        "verified".to_string()
    };
    anime.details_hydrated = true;
}

static MOVIES_SYNTHETIC_EP: Episode = Episode {
    season: 0,
    number: 0,
    title: None,
    overview: None,
    provider_episode_id: None,
    provider_season_id: None,
};

/// Recognizes generic / provider-numbered episode titles such as:
/// `"Episode 1"`, `"Episode 01"`, `"Ep 1"`, `"Black Clover S01E01"`, `"Black Clover Episode 1"`, `"001"`.
pub fn is_generic_episode_title(raw: &str, anime_norm_title: &str, ep_num: usize) -> bool {
    let norm = normalize_title_for_match(raw);
    if norm.is_empty() {
        return true;
    }
    let stripped = if !anime_norm_title.is_empty() {
        norm.strip_prefix(anime_norm_title).unwrap_or(&norm).trim()
    } else {
        norm.as_str()
    };
    if stripped.is_empty() {
        return true;
    }
    if stripped.chars().all(|c| c.is_ascii_digit()) {
        return stripped.parse::<usize>().ok() == Some(ep_num);
    }
    for prefix in ["episode ", "ep ", "e ", "e"] {
        if let Some(rest) = stripped.strip_prefix(prefix) {
            let digits: String = rest
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !digits.is_empty() && digits.parse::<usize>().ok() == Some(ep_num) {
                let tail = rest.trim()[digits.len()..].trim();
                if tail.is_empty()
                    || matches!(
                        tail,
                        "dub" | "sub" | "dubbed" | "subbed" | "hd" | "1080p" | "720p" | "480p"
                    )
                {
                    return true;
                }
            }
        }
    }
    // Check SxxExx pattern (e.g., "s01e01", "s1e1")
    let tokens: Vec<&str> = stripped.split_whitespace().collect();
    if tokens.iter().any(|t| {
        if let Some(rest) = t.strip_prefix('s')
            && let Some((s_part, e_part)) = rest.split_once('e')
            && !s_part.is_empty()
            && !e_part.is_empty()
            && s_part.chars().all(|c| c.is_ascii_digit())
            && e_part.chars().all(|c| c.is_ascii_digit())
        {
            return e_part.parse::<usize>().ok() == Some(ep_num);
        }
        false
    }) {
        return true;
    }
    false
}

/// Structured 5-state episode matching across heterogeneous provider ID and numbering formats.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_episode_match(
    media_type: MediaType,
    anime_norm_title: &str,
    requested_season: usize,
    requested_episode: usize,
    canonical_title: Option<&str>,
    candidate: &Episode,
    used_split_season_override: bool,
    is_ambiguous: bool,
) -> (EpisodeMappingOutcome, f32, Vec<String>, Option<String>) {
    let mut evidence = Vec::new();

    if media_type == MediaType::Movie && candidate.season == 0 && candidate.number == 0 {
        evidence.push("movie_canonical_match".to_string());
        return (EpisodeMappingOutcome::Matched, 1.0, evidence, None);
    }

    if candidate.number == 0 {
        evidence.push("missing_provider_episode_number".to_string());
        return (
            EpisodeMappingOutcome::Unmatched,
            0.0,
            evidence,
            Some("missing_episode_metadata".to_string()),
        );
    }

    let season_matches = used_split_season_override || candidate.season == requested_season;
    let episode_matches = candidate.number == requested_episode;

    if !season_matches || !episode_matches {
        if !season_matches {
            evidence.push(format!(
                "season_mismatch:expected={requested_season}:got={}",
                candidate.season
            ));
        }
        if !episode_matches {
            evidence.push(format!(
                "episode_mismatch:expected={requested_episode}:got={}",
                candidate.number
            ));
        }
        return (
            EpisodeMappingOutcome::EpisodeMismatch,
            0.0,
            evidence,
            Some("episode_mismatch".to_string()),
        );
    }

    evidence.push(if used_split_season_override {
        "split_season_match".to_string()
    } else {
        "season_match".to_string()
    });
    evidence.push("episode_number_match".to_string());

    if is_ambiguous {
        evidence.push("multiple_provider_candidates_for_episode".to_string());
        return (
            EpisodeMappingOutcome::Ambiguous,
            0.5,
            evidence,
            Some("ambiguous_episode_candidates".to_string()),
        );
    }

    match (canonical_title, candidate.title.as_deref()) {
        (Some(a), Some(b)) => {
            let na = normalize_title_for_match(a);
            let nb = normalize_title_for_match(b);
            if !na.is_empty() && na == nb {
                evidence.push("title_exact_match".to_string());
                (EpisodeMappingOutcome::Matched, 1.0, evidence, None)
            } else if is_generic_episode_title(b, anime_norm_title, requested_episode)
                || is_generic_episode_title(a, anime_norm_title, requested_episode)
            {
                evidence.push("provider_numbered_title".to_string());
                (EpisodeMappingOutcome::Matched, 0.9, evidence, None)
            } else {
                evidence.push("title_diverged".to_string());
                (
                    EpisodeMappingOutcome::ProbableMatch,
                    0.65,
                    evidence,
                    Some("title_confirmation_required".to_string()),
                )
            }
        }
        _ => {
            evidence.push("numbered_episode_match".to_string());
            (EpisodeMappingOutcome::Matched, 0.85, evidence, None)
        }
    }
}

#[allow(dead_code)]
fn _keep_details_type(_: &MediaDetails) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::models::Season;

    fn norm(
        p: ProviderKind,
        id: &str,
        title: &str,
        year: Option<u32>,
        mt: MediaType,
    ) -> NormalizedAnime {
        NormalizedAnime {
            provider_id: p,
            provider_anime_id: id.to_string(),
            title: title.to_string(),
            alternative_titles: Vec::new(),
            year,
            media_type: mt,
            description: None,
            poster_url: None,
            genres: Vec::new(),
        }
    }

    #[test]
    fn test_identity_matching_merges_same_anime_and_keeps_movies_separate() {
        let items = vec![
            norm(
                ProviderKind::AniZone,
                "az1",
                "Naruto Shippuden",
                Some(2007),
                MediaType::Series,
            ),
            norm(
                ProviderKind::Dramachi,
                "dc1",
                "Naruto Shippuden [Hindi]",
                Some(2007),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb1",
                "Naruto Shippuden: The Movie",
                Some(2007),
                MediaType::Movie,
            ),
        ];
        let merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 2);
        let series = merged
            .iter()
            .find(|a| a.media_type == MediaType::Series)
            .unwrap();
        assert_eq!(series.provider_records.len(), 2);
        assert_eq!(series.title, "Naruto Shippuden");
    }

    #[test]
    fn test_season_and_episode_reconciliation_preserves_all_episodes() {
        let mut anime = CanonicalAnime {
            id: "NP-A-112233".into(),
            slug_token: "112233".into(),
            title: "Test".into(),
            alternative_titles: vec![],
            media_type: MediaType::Series,
            year: Some(2020),
            description: None,
            poster_url: None,
            genres: vec![],
            provider_records: vec![
                ProviderAnimeRef {
                    provider_id: ProviderKind::AniZone,
                    provider_anime_id: "az".into(),
                    title: "Test".into(),
                    declared_language: None,
                    declared_language_code: None,
                    year: Some(2020),
                    media_type: MediaType::Series,
                    details: Some(MediaDetails {
                        id: crate::providers::models::ProviderMediaId {
                            provider: ProviderKind::AniZone,
                            value: "az".into(),
                        },
                        title: "Test".into(),
                        media_type: MediaType::Series,
                        year: None,
                        description: None,
                        tagline: None,
                        imdb_rating: None,
                        director: None,
                        stars: None,
                        prints: None,
                        audios: None,
                        poster_url: None,
                        duration: None,
                        genres: vec![],
                        seasons: vec![
                            Season {
                                number: 1,
                                episodes: (1..=3)
                                    .map(|n| Episode {
                                        season: 1,
                                        number: n,
                                        title: Some(format!("Ep {n}")),
                                        overview: None,
                                        provider_episode_id: Some(format!("{n}")),
                                        provider_season_id: Some("1".into()),
                                    })
                                    .collect(),
                                provider_season_id: Some("1".into()),
                            },
                            Season {
                                number: 2,
                                episodes: vec![Episode {
                                    season: 2,
                                    number: 1,
                                    title: Some("S2E1".into()),
                                    overview: None,
                                    provider_episode_id: Some("4".into()),
                                    provider_season_id: Some("2".into()),
                                }],
                                provider_season_id: Some("2".into()),
                            },
                        ],
                        dubs: vec![],
                    }),
                    details_failed: false,
                },
                ProviderAnimeRef {
                    provider_id: ProviderKind::Dramachi,
                    provider_anime_id: "dc".into(),
                    title: "Test".into(),
                    declared_language: None,
                    declared_language_code: None,
                    year: Some(2020),
                    media_type: MediaType::Series,
                    details: Some(MediaDetails {
                        id: crate::providers::models::ProviderMediaId {
                            provider: ProviderKind::Dramachi,
                            value: "dc".into(),
                        },
                        title: "Test".into(),
                        media_type: MediaType::Series,
                        year: None,
                        description: None,
                        tagline: None,
                        imdb_rating: None,
                        director: None,
                        stars: None,
                        prints: None,
                        audios: None,
                        poster_url: None,
                        duration: None,
                        genres: vec![],
                        seasons: vec![Season {
                            number: 1,
                            episodes: (1..=2)
                                .map(|n| Episode {
                                    season: 1,
                                    number: n,
                                    title: Some(format!("Ep {n}")),
                                    overview: None,
                                    provider_episode_id: Some(format!("dc-{n}")),
                                    provider_season_id: Some("1".into()),
                                })
                                .collect(),
                            provider_season_id: Some("1".into()),
                        }],
                        dubs: vec![],
                    }),
                    details_failed: false,
                },
            ],
            seasons: vec![],
            total_seasons: 0,
            total_episodes: 0,
            season_structure_status: "pending".into(),
            details_hydrated: false,
        };

        build_canonical_structure(&mut anime);
        assert_eq!(anime.total_seasons, 2);
        assert_eq!(anime.total_episodes, 4);
        assert_eq!(anime.seasons[0].episodes.len(), 3);
        assert_eq!(anime.seasons[0].episodes[0].provider_mappings.len(), 2);
        assert_eq!(anime.seasons[0].episodes[2].provider_mappings.len(), 1);
    }

    #[test]
    fn test_naruto_shippuden_merges_s1_s2_and_hindi_keeps_boruto_and_movies_separate() {
        let items = vec![
            norm(
                ProviderKind::AniZone,
                "az1",
                "Naruto Shippuden",
                Some(2007),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb1",
                "Naruto: Shippuden S1-S2",
                Some(2009),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb2",
                "Naruto: Shippuden [Hindi]",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb3",
                "Boruto: Naruto Next Generations S1-S3",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::AniZone,
                "az2",
                "Naruto",
                Some(2002),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb4",
                "Naruto Shippuden: The Movie",
                Some(2007),
                MediaType::Movie,
            ),
        ];
        let merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 4);
        let shippuden = merged
            .iter()
            .find(|a| a.title == "Naruto Shippuden" && a.media_type == MediaType::Series)
            .expect("Naruto Shippuden canonical record should exist");
        assert_eq!(shippuden.provider_records.len(), 3);
        assert_eq!(shippuden.year, Some(2007));
    }

    #[test]
    fn test_remake_with_large_year_gap_without_dub_tag_stays_separate() {
        let items = vec![
            norm(
                ProviderKind::AniZone,
                "az1",
                "Hunter x Hunter",
                Some(1999),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb1",
                "Hunter x Hunter",
                Some(2011),
                MediaType::Series,
            ),
        ];
        let merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_season_range_and_language_normalization() {
        assert_eq!(
            normalize_title_for_match("Naruto: Shippuden S1-S2"),
            "naruto shippuden"
        );
        assert_eq!(
            normalize_title_for_match("Naruto: Shippûden [Hindi]"),
            "naruto shippuden"
        );
        assert_eq!(
            normalize_title_for_match("Black Clover [English] (2017)"),
            "black clover"
        );
        assert_eq!(
            normalize_title_for_match("Boruto: Naruto Next Generations S1-S3"),
            "boruto naruto next generations"
        );
        assert_eq!(
            normalize_title_for_match("Black Clover Second Season"),
            "black clover"
        );
    }

    #[test]
    fn test_black_clover_merges_all_four_providers_and_second_season() {
        let items = vec![
            norm(
                ProviderKind::MovieBox,
                "mb1",
                "Black Clover S1-S2",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb2",
                "Black Clover [Hindi] S1-S2",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::AniZone,
                "az1",
                "Black Clover",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::AniZone,
                "az2",
                "Black Clover Second Season",
                Some(2026),
                MediaType::Series,
            ),
            norm(
                ProviderKind::Dramachi,
                "dc1",
                "Black Clover",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::FourKHdHub,
                "fk1",
                "Black Clover",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb3",
                "Black Clover: Sword of the Wizard King",
                Some(2023),
                MediaType::Movie,
            ),
        ];
        let merged = merge_search_results(items, 50);
        let series: Vec<_> = merged
            .iter()
            .filter(|a| a.media_type == MediaType::Series)
            .collect();
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].title, "Black Clover");
        assert_eq!(series[0].provider_records.len(), 6);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_naruto_2002_and_2005_merge_into_one_canonical_record() {
        let items = vec![
            norm(
                ProviderKind::MovieBox,
                "mb1",
                "Naruto",
                Some(2005),
                MediaType::Series,
            ),
            norm(
                ProviderKind::AniZone,
                "az1",
                "Naruto",
                Some(2002),
                MediaType::Series,
            ),
            norm(
                ProviderKind::Dramachi,
                "dc1",
                "Naruto",
                Some(2002),
                MediaType::Series,
            ),
            norm(
                ProviderKind::FourKHdHub,
                "fk1",
                "Naruto",
                Some(2002),
                MediaType::Series,
            ),
        ];
        let merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].provider_records.len(), 4);
        assert_eq!(merged[0].year, Some(2002));
    }

    #[test]
    fn test_moviebox_language_variants_group_and_preserve_all_ids_and_mappings() {
        let items = vec![
            norm(
                ProviderKind::MovieBox,
                "mb-hi-1",
                "Black Clover (Hindi)",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb-en-2",
                "Black Clover (English)",
                Some(2017),
                MediaType::Series,
            ),
            norm(
                ProviderKind::MovieBox,
                "mb-raw-3",
                "Black Clover",
                Some(2017),
                MediaType::Series,
            ),
        ];
        let mut merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 1);
        let anime = &mut merged[0];
        assert_eq!(anime.title, "Black Clover");
        assert_eq!(anime.provider_records.len(), 3);

        // Verify explicit language tags extracted and untagged listing does NOT invent Japanese
        let hi_rec = anime
            .provider_records
            .iter()
            .find(|r| r.provider_anime_id == "mb-hi-1")
            .unwrap();
        assert_eq!(hi_rec.declared_language.as_deref(), Some("Hindi"));
        assert_eq!(hi_rec.declared_language_code.as_deref(), Some("hi"));

        let en_rec = anime
            .provider_records
            .iter()
            .find(|r| r.provider_anime_id == "mb-en-2")
            .unwrap();
        assert_eq!(en_rec.declared_language.as_deref(), Some("English"));
        assert_eq!(en_rec.declared_language_code.as_deref(), Some("en"));

        let raw_rec = anime
            .provider_records
            .iter()
            .find(|r| r.provider_anime_id == "mb-raw-3")
            .unwrap();
        assert_eq!(raw_rec.declared_language, None);
        assert_eq!(raw_rec.declared_language_code, None);

        // Hydrate details on all 3 variants and verify all 3 map to S1E1 (none dropped!)
        for rec in &mut anime.provider_records {
            rec.details = Some(MediaDetails {
                id: crate::providers::models::ProviderMediaId {
                    provider: ProviderKind::MovieBox,
                    value: rec.provider_anime_id.clone(),
                },
                title: rec.title.clone(),
                media_type: MediaType::Series,
                year: Some("2017".into()),
                description: None,
                tagline: None,
                imdb_rating: None,
                director: None,
                stars: None,
                prints: None,
                audios: None,
                poster_url: None,
                duration: None,
                genres: vec![],
                seasons: vec![Season {
                    number: 1,
                    episodes: vec![Episode {
                        season: 1,
                        number: 1,
                        title: Some("Asta and Yuno".into()),
                        overview: None,
                        provider_episode_id: Some(format!("ep-{}", rec.provider_anime_id)),
                        provider_season_id: None,
                    }],
                    provider_season_id: None,
                }],
                dubs: vec![],
            });
        }
        build_canonical_structure(anime);
        assert_eq!(anime.seasons.len(), 1);
        let ep1 = &anime.seasons[0].episodes[0];
        assert_eq!(ep1.provider_mappings.len(), 3);
        let mapped_ids: Vec<&str> = ep1
            .provider_mappings
            .iter()
            .map(|m| m.provider_anime_id.as_str())
            .collect();
        assert!(mapped_ids.contains(&"mb-hi-1"));
        assert!(mapped_ids.contains(&"mb-en-2"));
        assert!(mapped_ids.contains(&"mb-raw-3"));
        assert!(
            ep1.provider_mappings
                .iter()
                .all(|m| m.outcome == EpisodeMappingOutcome::Matched)
        );
    }

    #[test]
    fn test_dramachi_structured_episode_matching_states() {
        // 1. Different provider episode ID + generic provider title ("Black Clover S01E01") matches "Asta and Yuno"
        let dc_ep1 = Episode {
            season: 1,
            number: 1,
            title: Some("Black Clover S01E01".into()),
            overview: None,
            provider_episode_id: Some("dc-fid-991".into()),
            provider_season_id: None,
        };
        let (outcome, score, evidence, reason) = evaluate_episode_match(
            MediaType::Series,
            "black clover",
            1,
            1,
            Some("Asta and Yuno"),
            &dc_ep1,
            false,
            false,
        );
        assert_eq!(outcome, EpisodeMappingOutcome::Matched);
        assert!(score >= 0.85);
        assert!(evidence.contains(&"season_match".to_string()));
        assert!(evidence.contains(&"episode_number_match".to_string()));
        assert_eq!(reason, None);

        // 2. Wrong episode produces EpisodeMismatch
        let wrong_ep = Episode {
            season: 1,
            number: 5,
            title: Some("Black Clover S01E05".into()),
            overview: None,
            provider_episode_id: Some("dc-fid-995".into()),
            provider_season_id: None,
        };
        let (outcome, _, _, reason) = evaluate_episode_match(
            MediaType::Series,
            "black clover",
            1,
            1,
            Some("Asta and Yuno"),
            &wrong_ep,
            false,
            false,
        );
        assert_eq!(outcome, EpisodeMappingOutcome::EpisodeMismatch);
        assert_eq!(reason.as_deref(), Some("episode_mismatch"));

        // 3. Missing episode number produces Unmatched
        let missing_ep = Episode {
            season: 1,
            number: 0,
            title: None,
            overview: None,
            provider_episode_id: None,
            provider_season_id: None,
        };
        let (outcome, _, _, reason) = evaluate_episode_match(
            MediaType::Series,
            "black clover",
            1,
            1,
            Some("Asta and Yuno"),
            &missing_ep,
            false,
            false,
        );
        assert_eq!(outcome, EpisodeMappingOutcome::Unmatched);
        assert_eq!(reason.as_deref(), Some("missing_episode_metadata"));

        // 4. Multiple candidates for same provider/episode produces Ambiguous
        let (outcome, _, _, reason) = evaluate_episode_match(
            MediaType::Series,
            "black clover",
            1,
            1,
            Some("Asta and Yuno"),
            &dc_ep1,
            false,
            true,
        );
        assert_eq!(outcome, EpisodeMappingOutcome::Ambiguous);
        assert_eq!(reason.as_deref(), Some("ambiguous_episode_candidates"));

        // 5. Distinct non-generic titles produce ProbableMatch (never auto-attached)
        let diverged_ep = Episode {
            season: 1,
            number: 1,
            title: Some("Completely Different Recap Episode".into()),
            overview: None,
            provider_episode_id: Some("dc-fid-999".into()),
            provider_season_id: None,
        };
        let (outcome, _, _, reason) = evaluate_episode_match(
            MediaType::Series,
            "black clover",
            1,
            1,
            Some("Asta and Yuno"),
            &diverged_ep,
            false,
            false,
        );
        assert_eq!(outcome, EpisodeMappingOutcome::ProbableMatch);
        assert_eq!(reason.as_deref(), Some("title_confirmation_required"));
    }
}
