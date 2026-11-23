use crate::providers::models::MediaType;
use crate::v3::matching::{build_canonical_structure, merge_search_results, query_matches_anime};
use crate::v3::models::CanonicalAnime;
use crate::v3::providers::{ProviderRegistry, provider_key};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const ANILIST_ENDPOINT: &str = "https://graphql.anilist.co";
const HOME_QUERY: &str = r#"
query HomeFeeds {
  trending: Page(page: 1, perPage: 12) {
    media(type: ANIME, sort: TRENDING_DESC, isAdult: false) {
      id format status seasonYear averageScore genres description(asHtml: false)
      episodes
      title { romaji english userPreferred }
      coverImage { extraLarge large }
      bannerImage
    }
  }
  seasonal: Page(page: 1, perPage: 12) {
    media(type: ANIME, sort: POPULARITY_DESC, status: RELEASING, isAdult: false) {
      id format status seasonYear averageScore genres description(asHtml: false)
      episodes
      title { romaji english userPreferred }
      coverImage { extraLarge large }
      bannerImage
    }
  }
  popular: Page(page: 1, perPage: 12) {
    media(type: ANIME, sort: POPULARITY_DESC, isAdult: false) {
      id format status seasonYear averageScore genres description(asHtml: false)
      episodes
      title { romaji english userPreferred }
      coverImage { extraLarge large }
      bannerImage
    }
  }
  movies: Page(page: 1, perPage: 12) {
    media(type: ANIME, format: MOVIE, sort: POPULARITY_DESC, isAdult: false) {
      id format status seasonYear averageScore genres description(asHtml: false)
      episodes
      title { romaji english userPreferred }
      coverImage { extraLarge large }
      bannerImage
    }
  }
}
"#;

#[derive(Debug, Clone, Serialize)]
pub struct HomeAnimeItem {
    pub anime_id: String,
    pub anilist_id: i64,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_english: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title_romaji: Option<String>,
    pub media_type: MediaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub year: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    pub genres: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poster_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poster_fallback_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner_url: Option<String>,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub verified_episodes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_episode_id: Option<String>,
    pub counts_status: &'static str,
    pub available_providers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HomeSections {
    pub trending: Vec<HomeAnimeItem>,
    pub seasonal: Vec<HomeAnimeItem>,
    pub popular: Vec<HomeAnimeItem>,
    pub movies: Vec<HomeAnimeItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HomeResponse {
    pub status: &'static str,
    pub session_id: String,
    pub cached: bool,
    pub sections: HomeSections,
}

#[derive(Debug, Clone)]
pub struct CachedHomeData {
    pub sections: HomeSections,
    pub seeded_anime: Vec<CanonicalAnime>,
    pub built_at: Instant,
}

#[derive(Debug, Deserialize)]
struct AniListGqlResp {
    data: Option<AniListHomeData>,
}

#[derive(Debug, Deserialize)]
struct AniListHomeData {
    trending: Option<AniListPage>,
    seasonal: Option<AniListPage>,
    popular: Option<AniListPage>,
    movies: Option<AniListPage>,
}

#[derive(Debug, Deserialize)]
struct AniListPage {
    media: Vec<AniListMedia>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AniListMedia {
    id: i64,
    format: Option<String>,
    status: Option<String>,
    season_year: Option<u32>,
    average_score: Option<i64>,
    #[serde(default)]
    genres: Vec<String>,
    description: Option<String>,
    episodes: Option<usize>,
    title: Option<AniListTitle>,
    cover_image: Option<AniListCover>,
    banner_image: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AniListTitle {
    romaji: Option<String>,
    english: Option<String>,
    user_preferred: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AniListCover {
    extra_large: Option<String>,
    large: Option<String>,
}

fn pick_title(media: &AniListMedia) -> Option<String> {
    let t = media.title.as_ref()?;
    t.english
        .as_deref()
        .or(t.user_preferred.as_deref())
        .or(t.romaji.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

pub async fn build_home_catalog(registry: &ProviderRegistry) -> Option<CachedHomeData> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(12))
        .build()
        .ok()?;
    let resp = client
        .post(ANILIST_ENDPOINT)
        .json(&serde_json::json!({ "query": HOME_QUERY }))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let parsed: AniListGqlResp = resp.json().await.ok()?;
    let data = parsed.data?;

    let trending_raw = data.trending.map(|p| p.media).unwrap_or_default();
    let seasonal_raw = data.seasonal.map(|p| p.media).unwrap_or_default();
    let popular_raw = data.popular.map(|p| p.media).unwrap_or_default();
    let movies_raw = data.movies.map(|p| p.media).unwrap_or_default();

    let mut unique_media: Vec<AniListMedia> = Vec::new();
    let mut seen_ids = HashSet::new();
    for m in trending_raw
        .iter()
        .chain(seasonal_raw.iter())
        .chain(popular_raw.iter())
        .chain(movies_raw.iter())
    {
        if seen_ids.insert(m.id) {
            unique_media.push(m.clone());
        }
    }

    let sem = Arc::new(Semaphore::new(4));
    let tasks = unique_media.into_iter().map(|media| {
        let reg = registry.clone();
        let permit_sem = sem.clone();
        async move {
            let _permit = permit_sem.acquire().await.ok()?;
            let title = pick_title(&media)?;
            let is_movie = media
                .format
                .as_deref()
                .is_some_and(|f| f.eq_ignore_ascii_case("MOVIE"));
            let (normalized, _, _, _) = reg.search_all(&title).await;
            if normalized.is_empty() {
                return None;
            }
            let mut candidates = merge_search_results(normalized, usize::MAX);
            candidates.retain(|a| query_matches_anime(&title, a));
            if candidates.is_empty() {
                return None;
            }
            // Prefer exact media_type match and highest provider count.
            candidates.sort_by_key(|a| {
                let type_bonus = if (is_movie && a.media_type == MediaType::Movie)
                    || (!is_movie && a.media_type != MediaType::Movie)
                {
                    100
                } else {
                    0
                };
                std::cmp::Reverse(type_bonus + a.provider_records.len())
            });

            // Hydrate top candidate (or fallback to second if first yields 0 verified episodes).
            for mut anime in candidates.into_iter().take(2) {
                let futs = anime.provider_records.iter().map(|rec| {
                    let r = reg.clone();
                    let pid = rec.provider_id;
                    let aid = rec.provider_anime_id.clone();
                    async move { (pid, aid.clone(), r.fetch_details(pid, &aid).await) }
                });
                let outcomes = futures::future::join_all(futs).await;
                for (pid, aid, res) in outcomes {
                    if let Some(rec) = anime
                        .provider_records
                        .iter_mut()
                        .find(|r| r.provider_id == pid && r.provider_anime_id == aid)
                    {
                        match res {
                            Ok(det) => {
                                rec.details = Some(det);
                                rec.details_failed = false;
                            }
                            Err(_) => rec.details_failed = true,
                        }
                    }
                }
                build_canonical_structure(&mut anime);
                if anime.verified_episodes > 0 {
                    let anilist_poster = media
                        .cover_image
                        .as_ref()
                        .and_then(|c| c.extra_large.clone().or_else(|| c.large.clone()));
                    let provider_poster = anime.poster_url.clone();
                    if anilist_poster.is_some() {
                        anime.poster_url = anilist_poster.clone();
                    }
                    let primary_ep = if anime.media_type == MediaType::Movie {
                        anime
                            .seasons
                            .first()
                            .and_then(|s| s.episodes.first())
                            .map(|e| e.id.clone())
                    } else {
                        None
                    };
                    let avail_providers: Vec<String> = anime
                        .provider_records
                        .iter()
                        .filter(|r| !r.details_failed)
                        .map(|r| provider_key(r.provider_id).to_string())
                        .collect();
                    let item = HomeAnimeItem {
                        anime_id: anime.id.clone(),
                        anilist_id: media.id,
                        title: title.clone(),
                        title_english: media.title.as_ref().and_then(|t| t.english.clone()),
                        title_romaji: media.title.as_ref().and_then(|t| t.romaji.clone()),
                        media_type: anime.media_type,
                        format: media.format.clone(),
                        status: media.status.clone(),
                        year: media.season_year.or(anime.year),
                        score: media.average_score.map(|s| s as f64 / 10.0),
                        genres: if media.genres.is_empty() {
                            anime.genres.clone()
                        } else {
                            media.genres.clone()
                        },
                        description: media.description.clone().or_else(|| anime.description.clone()),
                        poster_url: anilist_poster.or_else(|| provider_poster.clone()),
                        poster_fallback_url: provider_poster,
                        banner_url: media.banner_image.clone(),
                        total_seasons: anime.total_seasons,
                        total_episodes: media.episodes.unwrap_or(anime.total_episodes),
                        verified_episodes: anime.verified_episodes,
                        primary_episode_id: primary_ep,
                        counts_status: "hydrated",
                        available_providers: avail_providers,
                    };
                    return Some((media.id, item, anime));
                }
            }
            None
        }
    });

    let resolved: Vec<(i64, HomeAnimeItem, CanonicalAnime)> =
        futures::future::join_all(tasks).await.into_iter().flatten().collect();

    let mut by_anilist: std::collections::HashMap<i64, HomeAnimeItem> =
        std::collections::HashMap::with_capacity(resolved.len());
    let mut seeded_anime = Vec::with_capacity(resolved.len());
    for (al_id, item, anime) in resolved {
        by_anilist.insert(al_id, item);
        seeded_anime.push(anime);
    }

    let map_section = |list: Vec<AniListMedia>| -> Vec<HomeAnimeItem> {
        list.into_iter()
            .filter_map(|m| by_anilist.get(&m.id).cloned())
            .collect()
    };

    Some(CachedHomeData {
        sections: HomeSections {
            trending: map_section(trending_raw),
            seasonal: map_section(seasonal_raw),
            popular: map_section(popular_raw),
            movies: map_section(movies_raw),
        },
        seeded_anime,
        built_at: Instant::now(),
    })
}
