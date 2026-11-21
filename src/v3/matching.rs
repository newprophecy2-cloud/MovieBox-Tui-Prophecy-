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
    // Strip trailing bracketed/parenthesized tags like [Hindi], [English], (2022), (Dub), and S1-S2 suffixes
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
                "dub" | "sub" | "hindi" | "english" | "japanese" | "tv" | "uncensored"
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
        "part 2",
        "part 3",
        "season 2",
        "second season",
        "season 3",
        "third season",
        "final season",
        "shippuden",
        "shippûden",
        "boruto",
        "brotherhood",
    ] {
        if lower.contains(marker) {
            return Some(match marker {
                "shippûden" => "shippuden",
                "second season" => "season 2",
                "third season" => "season 3",
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
        if diff > 1 {
            // Allow wider year gap ONLY when at least one side has an explicit dub/language/season-pack tag
            // (e.g. "Naruto Shippuden" 2007 vs "Naruto: Shippuden [Hindi]" 2017 or "S1-S2" 2009),
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
                existing.provider_records.push(ProviderAnimeRef {
                    provider_id: item.provider_id,
                    provider_anime_id: item.provider_anime_id.clone(),
                    title: item.title.clone(),
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

    // One-by-one episode mapping across all providers (Spec Section 11)
    let mut episode_buckets: BTreeMap<(bool, usize, usize), Vec<(ProviderKind, String, &Episode)>> =
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
                &MOVIES_SYNTHETIC_EP,
            ));
            continue;
        }
        for s in &d.seasons {
            for ep in &s.episodes {
                let key = (ep.season == 0, ep.season, ep.number);
                episode_buckets.entry(key).or_default().push((
                    rec.provider_id,
                    rec.provider_anime_id.clone(),
                    ep,
                ));
            }
        }
    }

    let mut seasons_map: BTreeMap<(bool, usize), Vec<CanonicalEpisode>> = BTreeMap::new();
    let mut seen_episode_ids = HashSet::new();

    for ((is_special, season_num, ep_num), entries) in episode_buckets {
        let season_id = format_season_id(&anime.slug_token, season_num);
        let episode_id = format_episode_id(&anime.slug_token, season_num, ep_num);
        if !seen_episode_ids.insert(episode_id.clone()) {
            continue;
        }

        let mut title: Option<String> = None;
        let mut overview: Option<String> = None;
        let mut mappings = Vec::new();
        let mut seen_providers = HashSet::new();

        for (prov, prov_anime_id, ep) in &entries {
            if !seen_providers.insert(*prov) {
                continue;
            }
            if title.is_none()
                && let Some(t) = ep.title.as_ref().filter(|s| !s.trim().is_empty())
            {
                title = Some(t.trim().to_string());
            }
            if overview.is_none()
                && let Some(o) = ep.overview.as_ref().filter(|s| !s.trim().is_empty())
            {
                overview = Some(o.trim().to_string());
            }
            let (outcome, confidence) = compute_episode_mapping_outcome(title.as_deref(), ep);
            mappings.push(ProviderEpisodeMapping {
                provider_id: *prov,
                provider_anime_id: prov_anime_id.clone(),
                provider_season_number: ep.season,
                provider_episode_number: ep.number,
                provider_episode_id: ep.provider_episode_id.clone(),
                title: ep.title.clone(),
                outcome,
                confidence,
            });
        }

        let (final_title, metadata_status) = if anime.media_type == MediaType::Movie {
            (anime.title.clone(), "verified".to_string())
        } else if let Some(t) = title {
            let is_generic = normalize_title_for_match(&t) == format!("episode {ep_num}");
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

fn compute_episode_mapping_outcome(
    canonical_title: Option<&str>,
    candidate: &Episode,
) -> (EpisodeMappingOutcome, f32) {
    match (canonical_title, candidate.title.as_deref()) {
        (Some(a), Some(b)) => {
            let na = normalize_title_for_match(a);
            let nb = normalize_title_for_match(b);
            if !na.is_empty() && na == nb {
                (EpisodeMappingOutcome::Matched, 1.0)
            } else if na.starts_with("episode ") || nb.starts_with("episode ") {
                (EpisodeMappingOutcome::PossibleMatch, 0.7)
            } else {
                (EpisodeMappingOutcome::PossibleMatch, 0.65)
            }
        }
        _ => (EpisodeMappingOutcome::Matched, 0.85),
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
    }
}
