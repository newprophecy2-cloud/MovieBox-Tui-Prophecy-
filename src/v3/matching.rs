use crate::providers::models::{Episode, MediaDetails, MediaType, ProviderKind};
use crate::v3::models::{
    CanonicalAnime, CanonicalEpisode, CanonicalSeason, EpisodeMappingOutcome, MatchConfidence,
    NormalizedAnime, ProviderAnimeRef, ProviderEpisodeMapping,
};
use crate::v3::session::{
    format_anime_id, format_episode_id, format_season_id, generate_slug_token,
};
use std::collections::{BTreeMap, HashSet};

pub fn normalize_title_for_match(raw: &str) -> String {
    let mut s = raw.trim().to_string();
    // Strip trailing bracketed/parenthesized tags like [Hindi], [English], (2022), S1-S4
    loop {
        let trimmed = s.trim();
        let stripped = if let Some(idx) = trimmed.rfind('[')
            && trimmed.ends_with(']')
        {
            trimmed[..idx].trim()
        } else if let Some(idx) = trimmed.rfind('(')
            && trimmed.ends_with(')')
        {
            let inner = &trimmed[idx + 1..trimmed.len() - 1];
            if inner.len() == 4 && inner.chars().all(|c| c.is_ascii_digit()) {
                trimmed[..idx].trim()
            } else {
                break;
            }
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
    for ch in s.chars() {
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
        "season 2",
        "season 3",
        "shippuden",
        "boruto",
        "brotherhood",
    ] {
        if lower.contains(marker) {
            return Some(marker);
        }
    }
    None
}

pub fn anime_match_confidence(a: &NormalizedAnime, b: &CanonicalAnime) -> MatchConfidence {
    if a.media_type != b.media_type {
        return MatchConfidence::NoMatch;
    }
    if let (Some(ya), Some(yb)) = (a.year, b.year)
        && ya.abs_diff(yb) > 1
    {
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
    if norm_a == norm_b {
        return MatchConfidence::HighConfidenceMatch;
    }
    if b.alternative_titles
        .iter()
        .any(|alt| normalize_title_for_match(alt) == norm_a)
    {
        return MatchConfidence::HighConfidenceMatch;
    }
    MatchConfidence::NoMatch
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
            if existing.year.is_none() {
                existing.year = item.year;
            }
            if existing.poster_url.is_none() {
                existing.poster_url = item.poster_url;
            }
            if existing.description.is_none() {
                existing.description = item.description;
            }
            // Prefer clean title without [Tag] if current title has brackets
            if existing.title.contains('[') && !item.title.contains('[') {
                existing.alternative_titles.push(existing.title.clone());
                existing.title = item.title;
            } else if item.title != existing.title && !existing.alternative_titles.contains(&item.title) {
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
                && let Some(y) = d.year.as_deref().and_then(|s| s.trim().get(..4)).and_then(|s| s.parse::<u32>().ok())
            {
                best_year = Some(y);
            }
            if best_desc.is_none() && d.description.as_ref().is_some_and(|s| !s.trim().is_empty()) {
                best_desc = d.description.clone();
            }
            if best_poster.is_none() && d.poster_url.as_ref().is_some_and(|s| !s.trim().is_empty()) {
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

    // Collect highest season count & highest total episode count separately (Spec Section 9.3 / 9.4)
    let mut highest_season_count = 0usize;
    let mut highest_total_episodes = 0usize;
    let mut any_details = false;

    for rec in &anime.provider_records {
        if let Some(d) = &rec.details {
            any_details = true;
            let sc = d.seasons.iter().filter(|s| s.number > 0 && !s.episodes.is_empty()).count();
            let ec: usize = d.seasons.iter().map(|s| s.episodes.len()).sum();
            if sc > highest_season_count {
                highest_season_count = sc;
            }
            if ec > highest_total_episodes {
                highest_total_episodes = ec;
            }
        }
    }

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

        let canonical_ep = CanonicalEpisode {
            id: episode_id,
            anime_id: anime.id.clone(),
            season_id: season_id.clone(),
            season_number: season_num,
            episode_number: ep_num,
            title: title.unwrap_or_else(|| format!("Episode {ep_num}")),
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
        total_episodes += eps.len();
        seasons.push(CanonicalSeason {
            id: format_season_id(&anime.slug_token, season_num),
            anime_id: anime.id.clone(),
            season_number: season_num,
            episodes: eps,
        });
    }

    anime.seasons = seasons;
    anime.total_seasons = anime.seasons.len().max(highest_season_count);
    anime.total_episodes = total_episodes.max(highest_total_episodes);
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

    fn norm(p: ProviderKind, id: &str, title: &str, year: Option<u32>, mt: MediaType) -> NormalizedAnime {
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
            norm(ProviderKind::AniZone, "az1", "Naruto Shippuden", Some(2007), MediaType::Series),
            norm(ProviderKind::Dramachi, "dc1", "Naruto Shippuden [Hindi]", Some(2007), MediaType::Series),
            norm(ProviderKind::MovieBox, "mb1", "Naruto Shippuden: The Movie", Some(2007), MediaType::Movie),
        ];
        let merged = merge_search_results(items, 50);
        assert_eq!(merged.len(), 2);
        let series = merged.iter().find(|a| a.media_type == MediaType::Series).unwrap();
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
}
