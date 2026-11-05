use crate::providers::models::{CatalogItem, MediaType, ProviderKind, ProviderMediaId};
use unicode_normalization::UnicodeNormalization;

/// A catalog record with equivalent records from other providers attached.
#[derive(Debug, Clone, PartialEq)]
pub struct UnifiedCatalogItem {
    pub primary: CatalogItem,
    pub provider_mappings: Vec<ProviderMediaId>,
    /// Full provider search records retained for metadata merging and persistence.
    pub provider_items: Vec<CatalogItem>,
}

/// A normalized title key plus franchise and release-year hints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedAnimeTitle {
    pub key: String,
    pub franchise_discriminator: Vec<String>,
    pub year_marker: Option<i32>,
}

/// Normalize a title conservatively: punctuation and case are ignored, but sequel,
/// season, remake, movie, and special qualifiers remain in the key.
pub fn normalize_anime_title(title: &str) -> NormalizedAnimeTitle {
    let (mut tokens, mut year_marker) = title_tokens_and_year(title, true);

    // A year written without brackets is treated as a marker only at the very end.
    if tokens.last().and_then(|token| parse_year(token)).is_some() {
        year_marker = year_marker.or_else(|| tokens.last().and_then(|token| parse_year(token)));
        tokens.pop();
    }

    let key = tokens.join(" ");
    let franchise_discriminator = discriminator_tokens(&tokens);
    NormalizedAnimeTitle {
        key,
        franchise_discriminator,
        year_marker,
    }
}

/// Group records only when the title key and media type match and both records
/// have the same known year. Unknown or contradictory year evidence is kept
/// separate rather than being treated as permission to merge by title alone.
/// Repeated records from a provider are retained as separate results.
pub fn group_catalog_items(items: Vec<CatalogItem>) -> Vec<UnifiedCatalogItem> {
    #[derive(Debug)]
    struct CandidateGroup {
        indices: Vec<usize>,
        year: Option<i32>,
        providers: Vec<ProviderKind>,
        isolated: bool,
    }

    let normalized: Vec<NormalizedAnimeTitle> = items
        .iter()
        .map(|item| normalize_anime_title(&item.title))
        .collect();
    let years: Vec<(Option<i32>, bool)> = items
        .iter()
        .zip(&normalized)
        .map(|(item, title)| {
            let metadata_year = item.year.as_deref().and_then(parse_year);
            match (metadata_year, title.year_marker) {
                (Some(a), Some(b)) if a != b => (None, true),
                (Some(year), _) | (_, Some(year)) => (Some(year), false),
                (None, None) => (None, false),
            }
        })
        .collect();

    // Vector buckets preserve first-seen order without imposing enum ordering on
    // MediaType (which intentionally has no Hash/Ord implementation).
    let mut buckets: Vec<(String, MediaType, Vec<usize>)> = Vec::new();
    for (index, item) in items.iter().enumerate() {
        let key = normalized[index].key.clone();
        if let Some((_, _, indices)) = buckets.iter_mut().find(|(existing_key, media_type, _)| {
            *existing_key == key && *media_type == item.media_type
        }) {
            indices.push(index);
        } else {
            buckets.push((key, item.media_type, vec![index]));
        }
    }

    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (_, _, indices) in buckets {
        let mut candidates: Vec<CandidateGroup> = Vec::new();

        for index in indices {
            let (item_year, contradictory_markers) = years[index];
            let isolated = contradictory_markers || item_year.is_none();
            let target_year = item_year;
            let provider = items[index].id.provider;

            let existing = if isolated {
                None
            } else {
                candidates.iter_mut().find(|candidate| {
                    !candidate.isolated
                        && candidate.year == target_year
                        && !candidate.providers.contains(&provider)
                })
            };
            if let Some(candidate) = existing {
                candidate.indices.push(index);
                candidate.providers.push(provider);
            } else {
                candidates.push(CandidateGroup {
                    indices: vec![index],
                    year: target_year,
                    providers: vec![provider],
                    isolated,
                });
            }
        }
        groups.extend(candidates.into_iter().map(|group| group.indices));
    }

    groups.sort_by_key(|indices| indices[0]);
    groups
        .into_iter()
        .map(|indices| {
            let primary_index = indices[0];
            let primary = items[primary_index].clone();
            let provider_items: Vec<CatalogItem> = indices
                .into_iter()
                .skip(1)
                .map(|index| items[index].clone())
                .collect();
            let provider_mappings = provider_items
                .iter()
                .map(|item| item.id.clone())
                .collect();
            UnifiedCatalogItem {
                primary,
                provider_mappings,
                provider_items,
            }
        })
        .collect()
}

/// Provider-specific identification of an episode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpisodeMapping {
    pub provider: ProviderKind,
    pub season: usize,
    pub episode: usize,
    pub title: Option<String>,
}

/// Return false only when two supplied, non-empty titles clearly conflict.
/// Missing titles remain unknown rather than being treated as a mismatch.
pub fn episode_titles_compatible(left: Option<&str>, right: Option<&str>) -> bool {
    let normalize = |title: Option<&str>| {
        title
            .map(normalize_nonempty_title)
            .filter(|normalized| !normalized.is_empty())
    };
    match (normalize(left), normalize(right)) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

/// Group overlapping episode records by position and compatible titles. Records
/// from the same provider are never deduplicated or put in the same group.
pub fn merge_episode_mappings(mappings: Vec<EpisodeMapping>) -> Vec<Vec<EpisodeMapping>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (index, mapping) in mappings.iter().enumerate() {
        let normalized_title = mapping
            .title
            .as_deref()
            .map(normalize_nonempty_title)
            .filter(|title| !title.is_empty());
        let target = groups.iter_mut().find(|group| {
            group.iter().all(|&existing_index| {
                let existing = &mappings[existing_index];
                let existing_title = existing
                    .title
                    .as_deref()
                    .map(normalize_nonempty_title)
                    .filter(|title| !title.is_empty());
                existing.season == mapping.season
                    && existing.episode == mapping.episode
                    && existing.provider != mapping.provider
                    && episode_titles_compatible(
                        existing_title.as_deref(),
                        normalized_title.as_deref(),
                    )
            })
        });
        if let Some(group) = target {
            group.push(index);
        } else {
            groups.push(vec![index]);
        }
    }

    groups
        .into_iter()
        .map(|indices| {
            indices
                .into_iter()
                .map(|index| mappings[index].clone())
                .collect()
        })
        .collect()
}

/// A verified provider mapping attached to one canonical episode coordinate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEpisodeMapping {
    pub provider: ProviderKind,
    pub provider_episode_id: String,
    pub provider_episode_number: Option<usize>,
}

/// A provider's episode record used by the catalog reconciliation layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderEpisodeRecord {
    pub provider: ProviderKind,
    pub provider_episode_id: String,
    pub season: usize,
    pub episode: usize,
    pub title: Option<String>,
    pub overview: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalEpisode {
    pub season: usize,
    pub episode: usize,
    pub title: Option<String>,
    pub overview: Option<String>,
    pub mappings: Vec<CanonicalEpisodeMapping>,
    /// Provider records that could not be safely attached because their
    /// metadata conflicts with the canonical coordinate.
    pub unresolved_mappings: Vec<CanonicalEpisodeMapping>,
}

/// Merge provider episode lists without turning overlapping provider records
/// into duplicate episodes. Conflicting titles remain unresolved rather than
/// corrupting the canonical catalog.
pub fn aggregate_episode_records(records: Vec<ProviderEpisodeRecord>) -> Vec<CanonicalEpisode> {
    let mut result = Vec::new();
    for record in records {
        let mapping = CanonicalEpisodeMapping {
            provider: record.provider,
            provider_episode_id: record.provider_episode_id,
            provider_episode_number: Some(record.episode),
        };
        let Some(existing) = result.iter_mut().find(|episode: &&mut CanonicalEpisode| {
            episode.season == record.season && episode.episode == record.episode
        }) else {
            result.push(CanonicalEpisode {
                season: record.season,
                episode: record.episode,
                title: record.title,
                overview: record.overview,
                mappings: vec![mapping],
                unresolved_mappings: Vec::new(),
            });
            continue;
        };
        if episode_titles_compatible(existing.title.as_deref(), record.title.as_deref()) {
            if existing.title.is_none() {
                existing.title = record.title;
            }
            if existing.overview.is_none() {
                existing.overview = record.overview;
            }
            if !existing.mappings.iter().any(|candidate| {
                candidate.provider == mapping.provider
                    && candidate.provider_episode_id == mapping.provider_episode_id
            }) {
                existing.mappings.push(mapping);
            }
        } else {
            existing.unresolved_mappings.push(mapping);
        }
    }
    result.sort_by_key(|episode| (episode.season, episode.episode));
    result
}

pub fn missing_episode_numbers(
    episodes: &[CanonicalEpisode],
    season: usize,
    expected_through: usize,
) -> Vec<usize> {
    let present: std::collections::HashSet<usize> = episodes
        .iter()
        .filter(|episode| episode.season == season)
        .map(|episode| episode.episode)
        .collect();
    (1..=expected_through)
        .filter(|number| !present.contains(number))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedAudioTrack {
    pub provider: ProviderKind,
    pub source_id: String,
    pub language_code: String,
    pub codec: Option<String>,
    pub channels: Option<u16>,
    pub verified: bool,
}

pub fn normalize_language_code(value: &str) -> Option<String> {
    let normalized = value.trim().replace('_', "-").to_ascii_lowercase();
    let code = match normalized.as_str() {
        "japanese" | "jp" | "jpn" | "ja" => "ja",
        "english" | "eng" | "en" => "en",
        "hindi" | "hin" | "hi" => "hi",
        "bengali" | "bangla" | "ben" | "bn" => "bn",
        "tamil" | "tam" | "ta" => "ta",
        "telugu" | "tel" | "te" => "te",
        "korean" | "kor" | "ko" => "ko",
        "chinese" | "zho" | "zh" => "zh",
        value if value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_lowercase()) => value,
        _ if normalized.len() >= 4
            && normalized
                .as_bytes()
                .get(..2)
                .is_some_and(|prefix| prefix.iter().all(u8::is_ascii_lowercase))
            && normalized.as_bytes().get(2) == Some(&b'-') =>
        {
            normalized.as_str()
        }
        _ => return None,
    };
    Some(code.to_string())
}

pub fn aggregate_verified_audio_tracks(tracks: Vec<VerifiedAudioTrack>) -> Vec<VerifiedAudioTrack> {
    let mut result = Vec::new();
    for mut track in tracks {
        let Some(language) = normalize_language_code(&track.language_code) else {
            continue;
        };
        track.language_code = language;
        if !track.verified {
            continue;
        }
        if !result.iter().any(|existing: &VerifiedAudioTrack| {
            existing.source_id == track.source_id && existing.language_code == track.language_code
        }) {
            result.push(track);
        }
    }
    result.sort_by(|left, right| {
        left.language_code
            .cmp(&right.language_code)
            .then_with(|| left.provider.cache_key().cmp(right.provider.cache_key()))
            .then_with(|| left.source_id.cmp(&right.source_id))
    });
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRouteCandidate {
    pub provider: ProviderKind,
    pub source_id: String,
    pub language_code: Option<String>,
    pub codec: Option<String>,
    pub verified: bool,
    pub expired: bool,
    pub provider_health: u8,
    pub priority: usize,
}

pub fn choose_source_route(
    candidates: &[SourceRouteCandidate],
    requested_language: Option<&str>,
    failed_source_ids: &std::collections::HashSet<String>,
) -> Option<SourceRouteCandidate> {
    let requested = requested_language.and_then(normalize_language_code);
    candidates
        .iter()
        .filter(|candidate| {
            candidate.verified
                && !candidate.expired
                && !failed_source_ids.contains(&candidate.source_id)
                && requested.as_deref().is_none_or(|language| {
                    candidate
                        .language_code
                        .as_deref()
                        .and_then(normalize_language_code)
                        .as_deref()
                        == Some(language)
                })
        })
        .min_by_key(|candidate| {
            (
                candidate.priority,
                std::cmp::Reverse(candidate.provider_health),
                candidate.codec.is_none(),
                candidate.source_id.clone(),
            )
        })
        .cloned()
}

fn title_tokens_and_year(input: &str, remove_bracketed_years: bool) -> (Vec<String>, Option<i32>) {
    let normalized: String = input.nfkc().collect();
    let chars: Vec<char> = normalized.chars().collect();
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut year_marker = None;
    let mut index = 0;

    while index < chars.len() {
        let open = chars[index];
        let close = match open {
            '(' => Some(')'),
            '[' => Some(']'),
            '{' => Some('}'),
            _ => None,
        };
        if remove_bracketed_years
            && let Some(close) = close
            && let Some(relative_end) = chars[index + 1..].iter().position(|&ch| ch == close)
        {
            let end = index + 1 + relative_end;
            let inside: String = chars[index + 1..end].iter().collect();
            if let Some(year) = parse_year(inside.trim()) {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                year_marker = year_marker.or(Some(year));
                index = end + 1;
                continue;
            }
        }

        let ch = chars[index];
        if ch.is_alphanumeric() {
            current.extend(ch.to_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
        index += 1;
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    (tokens, year_marker)
}

fn normalize_nonempty_title(title: &str) -> String {
    title
        .nfkc()
        .flat_map(char::to_lowercase)
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_year(value: &str) -> Option<i32> {
    let year = value.trim().parse::<i32>().ok()?;
    (1800..=2099).contains(&year).then_some(year)
}

fn discriminator_tokens(tokens: &[String]) -> Vec<String> {
    const QUALIFIERS: &[&str] = &[
        "season",
        "series",
        "part",
        "chapter",
        "movie",
        "film",
        "special",
        "ova",
        "ona",
        "remake",
        "reboot",
        "sequel",
        "prequel",
        "spinoff",
        "spin",
        "brotherhood",
    ];
    let mut result = Vec::new();
    let mut include_next_number = false;
    for token in tokens {
        if include_next_number && token.chars().all(|ch| ch.is_ascii_digit()) {
            result.push(token.clone());
            include_next_number = false;
            continue;
        }
        include_next_number = false;
        if token.chars().all(|ch| ch.is_ascii_digit()) {
            result.push(token.clone());
            continue;
        }
        if QUALIFIERS.contains(&token.as_str())
            || (token.len() > 1
                && token.starts_with('s')
                && token[1..].chars().all(|ch| ch.is_ascii_digit()))
        {
            result.push(token.clone());
            include_next_number =
                matches!(token.as_str(), "season" | "series" | "part" | "chapter");
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(provider: ProviderKind, id: &str, title: &str, year: Option<&str>) -> CatalogItem {
        CatalogItem {
            id: ProviderMediaId {
                provider,
                value: id.to_string(),
            },
            title: title.to_string(),
            media_type: MediaType::Series,
            year: year.map(str::to_string),
            poster_url: None,
            season_count: None,
        }
    }

    fn episode(
        provider: ProviderKind,
        season: usize,
        number: usize,
        title: Option<&str>,
    ) -> EpisodeMapping {
        EpisodeMapping {
            provider,
            season,
            episode: number,
            title: title.map(str::to_string),
        }
    }

    #[test]
    fn collapses_bracketed_year_from_black_clover_title() {
        let normalized = normalize_anime_title("Black Clover (2017)");
        assert_eq!(normalized.key, "black clover");
        assert_eq!(normalized.year_marker, Some(2017));
        let grouped = group_catalog_items(vec![
            item(ProviderKind::MovieBox, "a", "Black Clover (2017)", Some("2017")),
            item(ProviderKind::Dramachi, "b", "BLACK-CLOVER", Some("2017")),
        ]);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[0].provider_mappings.len(), 1);
    }

    #[test]
    fn sequel_season_remake_movie_and_special_qualifiers_stay_distinct() {
        let base = normalize_anime_title("Example");
        for variant in [
            "Example 2",
            "Example Season 2",
            "Example Remake",
            "Example Movie",
            "Example Special",
        ] {
            assert_ne!(base.key, normalize_anime_title(variant).key, "{variant}");
        }
        assert!(
            normalize_anime_title("Example Season 2")
                .franchise_discriminator
                .contains(&"season".to_string())
        );
        assert!(
            normalize_anime_title("Example 2")
                .franchise_discriminator
                .contains(&"2".to_string())
        );
    }

    #[test]
    fn known_year_conflict_prevents_merge() {
        let grouped = group_catalog_items(vec![
            item(ProviderKind::MovieBox, "a", "Example", Some("2017")),
            item(ProviderKind::Dramachi, "b", "Example (2019)", Some("2019")),
        ]);
        assert_eq!(grouped.len(), 2);
    }

    #[test]
    fn ambiguous_unknown_year_is_not_assigned_to_either_year_group() {
        let grouped = group_catalog_items(vec![
            item(ProviderKind::MovieBox, "a", "Example", Some("2017")),
            item(ProviderKind::Addons, "unknown", "Example", None),
            item(ProviderKind::Dramachi, "b", "Example", Some("2019")),
        ]);
        assert_eq!(grouped.len(), 3);
        assert!(
            grouped
                .iter()
                .all(|group| group.provider_mappings.is_empty())
        );
    }

    #[test]
    fn matching_titles_with_unknown_years_remain_separate() {
        let grouped = group_catalog_items(vec![
            item(ProviderKind::FourKHdHub, "a", "Example", None),
            item(ProviderKind::Dramachi, "b", "Example", None),
        ]);
        assert_eq!(grouped.len(), 2);
        assert!(grouped.iter().all(|group| group.provider_mappings.is_empty()));
    }

    #[test]
    fn same_provider_catalog_records_remain_distinct() {
        let grouped = group_catalog_items(vec![
            item(ProviderKind::MovieBox, "a", "Example", None),
            item(ProviderKind::MovieBox, "b", "Example", None),
        ]);
        assert_eq!(grouped.len(), 2);
        assert!(
            grouped
                .iter()
                .all(|group| group.provider_mappings.is_empty())
        );
    }

    #[test]
    fn episode_overlap_merges_but_gaps_and_same_provider_records_do_not() {
        let groups = merge_episode_mappings(vec![
            episode(ProviderKind::MovieBox, 1, 1, Some("Beginning")),
            episode(ProviderKind::Dramachi, 1, 1, Some(" beginning! ")),
            episode(ProviderKind::MovieBox, 1, 1, Some("Beginning")),
            episode(ProviderKind::Dramachi, 1, 3, Some("Later")),
            episode(ProviderKind::MovieBox, 1, 4, Some("End")),
        ]);
        assert_eq!(groups.len(), 4);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[2].len(), 1);
        assert_eq!(groups[3].len(), 1);
    }

    #[test]
    fn episode_title_conflict_prevents_merge_but_missing_title_can_overlap() {
        let groups = merge_episode_mappings(vec![
            episode(ProviderKind::MovieBox, 1, 1, Some("Arrival")),
            episode(ProviderKind::Dramachi, 1, 1, Some("Departure")),
            episode(ProviderKind::FourKHdHub, 1, 1, None),
        ]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
    }

    #[test]
    fn episode_title_compatibility_is_conservative() {
        assert!(episode_titles_compatible(
            Some("A New Beginning!"),
            Some("a-new beginning")
        ));
        assert!(episode_titles_compatible(Some("Arrival"), None));
        assert!(!episode_titles_compatible(
            Some("Arrival"),
            Some("Departure")
        ));
    }

    #[test]
    fn aggregate_episode_records_deduplicates_ranges_and_keeps_conflicts_unresolved() {
        let records = (1..=51)
            .map(|episode| ProviderEpisodeRecord {
                provider: ProviderKind::FourKHdHub,
                provider_episode_id: format!("a-{episode}"),
                season: 1,
                episode,
                title: Some(format!("Episode {episode}")),
                overview: None,
            })
            .chain((52..=100).map(|episode| ProviderEpisodeRecord {
                provider: ProviderKind::Dramachi,
                provider_episode_id: format!("b-{episode}"),
                season: 1,
                episode,
                title: Some(format!("Episode {episode}")),
                overview: None,
            }))
            .chain((80..=170).map(|episode| ProviderEpisodeRecord {
                provider: ProviderKind::BdixCircleFtp,
                provider_episode_id: format!("c-{episode}"),
                season: 1,
                episode,
                title: Some(format!("Episode {episode}")),
                overview: None,
            }))
            .chain([ProviderEpisodeRecord {
                provider: ProviderKind::Addons,
                provider_episode_id: "conflict".into(),
                season: 1,
                episode: 90,
                title: Some("Different story".into()),
                overview: None,
            }])
            .collect();
        let episodes = aggregate_episode_records(records);
        assert_eq!(episodes.len(), 170);
        assert_eq!(episodes[0].mappings.len(), 1);
        assert_eq!(episodes[89].mappings.len(), 2);
        assert_eq!(episodes[89].unresolved_mappings.len(), 1);
        assert!(missing_episode_numbers(&episodes, 1, 170).is_empty());
    }

    #[test]
    fn audio_union_normalizes_and_discards_unverified_or_unknown_tracks() {
        let tracks = aggregate_verified_audio_tracks(vec![
            VerifiedAudioTrack {
                provider: ProviderKind::FourKHdHub,
                source_id: "a".into(),
                language_code: "Japanese".into(),
                codec: Some("aac".into()),
                channels: Some(2),
                verified: true,
            },
            VerifiedAudioTrack {
                provider: ProviderKind::Dramachi,
                source_id: "b".into(),
                language_code: "hi-IN".into(),
                codec: Some("aac".into()),
                channels: None,
                verified: true,
            },
            VerifiedAudioTrack {
                provider: ProviderKind::Addons,
                source_id: "unverified".into(),
                language_code: "bn".into(),
                codec: None,
                channels: None,
                verified: false,
            },
            VerifiedAudioTrack {
                provider: ProviderKind::Addons,
                source_id: "unknown".into(),
                language_code: "Klingon".into(),
                codec: None,
                channels: None,
                verified: true,
            },
        ]);
        assert_eq!(
            tracks
                .iter()
                .map(|track| track.language_code.as_str())
                .collect::<Vec<_>>(),
            ["hi-in", "ja"]
        );
    }

    #[test]
    fn source_route_respects_requested_language_and_skips_failed_sources() {
        let candidates = vec![
            SourceRouteCandidate {
                provider: ProviderKind::FourKHdHub,
                source_id: "failed-hi".into(),
                language_code: Some("hi".into()),
                codec: Some("h264".into()),
                verified: true,
                expired: false,
                provider_health: 100,
                priority: 0,
            },
            SourceRouteCandidate {
                provider: ProviderKind::Dramachi,
                source_id: "working-hi".into(),
                language_code: Some("Hindi".into()),
                codec: Some("h264".into()),
                verified: true,
                expired: false,
                provider_health: 80,
                priority: 1,
            },
            SourceRouteCandidate {
                provider: ProviderKind::FourKHdHub,
                source_id: "english".into(),
                language_code: Some("en".into()),
                codec: Some("h264".into()),
                verified: true,
                expired: false,
                provider_health: 100,
                priority: 0,
            },
        ];
        let failed = std::collections::HashSet::from(["failed-hi".to_string()]);
        let selected = choose_source_route(&candidates, Some("Hindi"), &failed).unwrap();
        assert_eq!(selected.source_id, "working-hi");
        assert_eq!(choose_source_route(&candidates, Some("bn"), &failed), None);
    }
}
