use super::models::*;
use crate::providers::models::{
    AudioTrackOption, CatalogItem, Episode, MediaDetails, MediaType, ProviderError, ProviderKind,
    ProviderMediaId, Release, Season, SourceMirror,
};
use reqwest::Client;
use std::time::Duration;

const DEFAULT_BASE_URL: &str = "https://api.nodeobjects.com/";
const IMAGE_CDN_BASE: &str = "https://static.nodeobjects.com/thumbnail/";

fn encode_param(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC).to_string()
}

#[derive(Debug, thiserror::Error)]
pub enum DramachiError {
    #[error("Network request failed: {0}")]
    Network(#[from] reqwest::Error),
    #[error("Failed to parse Dramachi response: {0}")]
    Parsing(String),
    #[error("Item not found")]
    NotFound,
}

impl From<DramachiError> for ProviderError {
    fn from(err: DramachiError) -> Self {
        match err {
            DramachiError::Network(e) => ProviderError::Network(e.to_string()),
            DramachiError::Parsing(e) => ProviderError::Parsing(e),
            DramachiError::NotFound => ProviderError::NotFound,
        }
    }
}

type TitleCache = std::sync::Arc<
    std::sync::Mutex<Option<(String, std::time::Instant, DramachiTitleDetailsResponse)>>,
>;
type EpListCache = std::sync::Arc<
    std::sync::Mutex<
        std::collections::HashMap<(String, String), (std::time::Instant, Vec<DramachiEpisodeItem>)>,
    >,
>;

#[derive(Debug, Clone)]
pub struct DramachiClient {
    client: Client,
    base_url: String,
    title_cache: TitleCache,
    eplist_cache: EpListCache,
}

fn season_from_episode_result(
    season_number: usize,
    result: Result<Vec<DramachiEpisodeItem>, DramachiError>,
) -> Result<Season, DramachiError> {
    let episodes = result?;
    let mut seen_ep_numbers = std::collections::HashSet::new();
    let mut mapped = Vec::new();
    for (index, episode) in episodes.iter().enumerate() {
        let number = parse_episode_number(&episode.f_title).unwrap_or(index + 1);
        if seen_ep_numbers.insert(number) {
            mapped.push(Episode {
                season: season_number,
                number,
                title: Some(clean_episode_title(&episode.f_title)),
                overview: None,
            });
        }
    }
    mapped.sort_by_key(|episode| episode.number);
    Ok(Season {
        number: season_number,
        episodes: mapped,
    })
}

impl Default for DramachiClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DramachiClient {
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        let client = crate::net::http_client_builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_default();
        Self {
            client,
            base_url: base_url.into(),
            title_cache: std::sync::Arc::new(std::sync::Mutex::new(None)),
            eplist_cache: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        }
    }

    async fn fetch_title_details(
        &self,
        title_id: &str,
    ) -> Result<DramachiTitleDetailsResponse, DramachiError> {
        if let Ok(lock) = self.title_cache.lock()
            && let Some((cached_id, ts, data)) = lock.as_ref()
            && cached_id == title_id
            && ts.elapsed() < Duration::from_secs(60)
        {
            return Ok(data.clone());
        }
        let url = format!(
            "{}?interface=title_v2&id={}",
            self.base_url,
            encode_param(title_id)
        );
        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let data: DramachiTitleDetailsResponse = resp
            .json()
            .await
            .map_err(|e| DramachiError::Parsing(format!("failed to parse title details: {e}")))?;
        if let Ok(mut lock) = self.title_cache.lock() {
            *lock = Some((
                title_id.to_string(),
                std::time::Instant::now(),
                data.clone(),
            ));
        }
        Ok(data)
    }

    pub async fn search(
        &self,
        query: &str,
        page: usize,
    ) -> Result<Vec<CatalogItem>, DramachiError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        let url = format!(
            "{}?interface=search&q={}&filter=all&page={}",
            self.base_url,
            encode_param(trimmed),
            page.max(1)
        );

        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let data: DramachiSearchResponse = resp
            .json()
            .await
            .map_err(|e| DramachiError::Parsing(format!("failed to parse search response: {e}")))?;

        let items = data.data.unwrap_or_default();
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            let is_movie = item
                .content
                .as_deref()
                .map(|c| c.eq_ignore_ascii_case("movies") || c.eq_ignore_ascii_case("movie"))
                .unwrap_or(false);

            let media_type = if is_movie {
                MediaType::Movie
            } else {
                MediaType::Series
            };

            let poster_url = item
                .thumb
                .filter(|t| !t.trim().is_empty())
                .map(|t| format!("{IMAGE_CDN_BASE}{}", t.trim()));

            results.push(CatalogItem {
                id: ProviderMediaId {
                    provider: ProviderKind::Dramachi,
                    value: item.id,
                },
                title: item.title,
                media_type,
                year: item.year.filter(|y| !y.trim().is_empty()),
                poster_url,
                season_count: None,
            });
        }

        Ok(results)
    }

    pub async fn details(&self, id: &str) -> Result<MediaDetails, DramachiError> {
        let (title_id, target_dub_rip) = match id.split_once("::") {
            Some((t_id, rip)) => (t_id.trim(), Some(rip.trim())),
            None => (id.trim(), None),
        };

        if title_id.is_empty() {
            return Err(DramachiError::NotFound);
        }

        let data = self.fetch_title_details(title_id).await?;

        let album = data
            .album
            .as_ref()
            .and_then(|a| a.first())
            .ok_or(DramachiError::NotFound)?;

        let is_movie = album
            .content
            .as_deref()
            .map(|c| c.eq_ignore_ascii_case("movies") || c.eq_ignore_ascii_case("movie"))
            .unwrap_or(false);

        let media_type = if is_movie {
            MediaType::Movie
        } else {
            MediaType::Series
        };

        let poster_url = album
            .thumb
            .as_deref()
            .filter(|t| !t.trim().is_empty())
            .map(|t| format!("{IMAGE_CDN_BASE}{}", t.trim()));

        let genres: Vec<String> = album
            .genres
            .as_deref()
            .map(|g| {
                g.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        let stars: Vec<String> = data
            .cast
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.name)
            .filter(|n| !n.trim().is_empty())
            .collect();

        let mut sorted_season_keys: Vec<String> = data
            .seasons
            .as_ref()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        sorted_season_keys.sort_by_key(|k| extract_leading_season_number(k).unwrap_or(usize::MAX));

        let mut seasons_vec = Vec::new();
        let mut dubs_vec = Vec::new();

        if let Some(seasons_map) = data.seasons {
            for (season_key, group) in &seasons_map {
                if let Some(versions) = &group.versions {
                    for version in versions {
                        let label = if seasons_map.len() > 1 {
                            format!("{}: {}", season_key, version.version_name)
                        } else {
                            version.version_name.clone()
                        };
                        let composite_subject_id = format!("{}::{}", title_id, version.rip);
                        dubs_vec.push(AudioTrackOption {
                            subject_id: composite_subject_id,
                            language: version.version_name.clone(),
                            label,
                        });
                    }
                }
            }

            dubs_vec.sort_by(|a, b| {
                let priority = |s: &str| -> usize {
                    let l = s.to_ascii_lowercase();
                    if l.contains("original") || l == "orig" {
                        0
                    } else if l.contains("english") || l.contains("eng") {
                        1
                    } else if l.contains("dub") {
                        2
                    } else {
                        3
                    }
                };
                priority(&a.language).cmp(&priority(&b.language))
            });

            if media_type == MediaType::Series {
                let mut season_rips = Vec::new();
                for key in &sorted_season_keys {
                    let rip_for_season = seasons_map
                        .get(key)
                        .and_then(|g| g.versions.as_ref())
                        .and_then(|versions| {
                            if let Some(target) = target_dub_rip {
                                versions
                                    .iter()
                                    .find(|v| v.rip == target)
                                    .map(|v| v.rip.clone())
                            } else {
                                None
                            }
                            .or_else(|| versions.first().map(|v| v.rip.clone()))
                        })
                        .unwrap_or_else(|| key.clone());

                    let season_num = extract_leading_season_number(key).unwrap_or(1);
                    season_rips.push((season_num, rip_for_season));
                }

                let season_futures: Vec<_> = season_rips
                    .into_iter()
                    .map(|(season_num, rip)| {
                        let rip_clone = rip.clone();
                        async move {
                            let episodes = self.fetch_episodes(title_id, &rip_clone).await;
                            season_from_episode_result(season_num, episodes)
                        }
                    })
                    .collect();

                seasons_vec = futures::future::try_join_all(season_futures).await?;
            }
        }

        let primary_id = target_dub_rip
            .map(|rip| format!("{title_id}::{rip}"))
            .unwrap_or_else(|| title_id.to_string());

        Ok(MediaDetails {
            id: ProviderMediaId {
                provider: ProviderKind::Dramachi,
                value: primary_id,
            },
            title: album.title.clone(),
            media_type,
            year: album.year.clone().filter(|y| !y.trim().is_empty()),
            description: album.storyline.clone().filter(|s| !s.trim().is_empty()),
            tagline: None,
            imdb_rating: None,
            director: album.director.clone().filter(|d| !d.trim().is_empty()),
            stars: if stars.is_empty() {
                None
            } else {
                Some(stars.join(", "))
            },
            prints: None,
            audios: None,
            poster_url,
            duration: None,
            genres,
            seasons: seasons_vec,
            dubs: dubs_vec,
        })
    }

    pub async fn fetch_episodes(
        &self,
        title_id: &str,
        rip: &str,
    ) -> Result<Vec<DramachiEpisodeItem>, DramachiError> {
        let key = (title_id.to_string(), rip.to_string());
        if let Ok(lock) = self.eplist_cache.lock()
            && let Some((ts, cached)) = lock.get(&key)
            && ts.elapsed() < Duration::from_secs(60)
        {
            return Ok(cached.clone());
        }
        let url = format!(
            "{}?interface=eplist&season={}&id={}",
            self.base_url,
            encode_param(rip),
            encode_param(title_id)
        );

        let resp = self.client.get(&url).send().await?.error_for_status()?;
        let data: DramachiEpisodeListResponse = resp
            .json()
            .await
            .map_err(|e| DramachiError::Parsing(format!("failed to parse episode list: {e}")))?;

        let mut episodes = data.episode_list.unwrap_or_default();
        episodes.sort_by_key(|ep| parse_episode_number(&ep.f_title).unwrap_or(usize::MAX));
        if let Ok(mut lock) = self.eplist_cache.lock() {
            if lock.len() >= 16 {
                lock.clear();
            }
            lock.insert(key, (std::time::Instant::now(), episodes.clone()));
        }
        Ok(episodes)
    }

    pub async fn fetch_file_stream(
        &self,
        fid: &str,
        disk: &str,
    ) -> Result<(String, String), DramachiError> {
        let url = format!(
            "{}?interface=getFile&fid={}&findex={}",
            self.base_url,
            encode_param(fid),
            encode_param(disk)
        );

        let resp = self.client.get(&url).send().await?;
        let data: DramachiFileInfoResponse = resp.json().await.map_err(|e| {
            DramachiError::Parsing(format!("failed to parse getFile response: {e}"))
        })?;

        let host_info = data
            .host_info
            .ok_or_else(|| DramachiError::Parsing("missing hostInfo in getFile".to_string()))?;

        let file_info = data
            .file_info
            .as_ref()
            .and_then(|f| f.first())
            .ok_or(DramachiError::NotFound)?;

        let raw_url = file_info
            .url
            .as_deref()
            .ok_or_else(|| DramachiError::Parsing("missing url in fileInfo".to_string()))?;

        let filename = file_info
            .filename
            .clone()
            .or_else(|| file_info.f_title.clone())
            .unwrap_or_else(|| format!("{fid}.mkv"));

        let stream_url = format!(
            "https://{}/cdn/{}",
            host_info.host.trim_end_matches('/'),
            raw_url.trim_start_matches('/')
        );

        Ok((stream_url, filename))
    }

    pub async fn episode_streams(
        &self,
        id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, DramachiError> {
        let (title_id, target_rip) = match id.split_once("::") {
            Some((t_id, rip)) => (t_id.trim(), Some(rip.trim())),
            None => (id.trim(), None),
        };

        if title_id.is_empty() {
            return Err(DramachiError::NotFound);
        }

        let rip_to_query = if let Some(rip) = target_rip {
            rip.to_string()
        } else {
            let data = self.fetch_title_details(title_id).await?;

            let mut resolved_rip = None;
            if let Some(seasons_map) = &data.seasons {
                if season > 0 {
                    let target_season_key = format!("Season {:02}", season);
                    if let Some(group) = seasons_map.get(&target_season_key) {
                        resolved_rip = group
                            .versions
                            .as_ref()
                            .and_then(|v| v.first())
                            .map(|v| v.rip.clone());
                    }
                }
                if resolved_rip.is_none() {
                    resolved_rip = seasons_map
                        .values()
                        .next()
                        .and_then(|g| g.versions.as_ref())
                        .and_then(|v| v.first())
                        .map(|v| v.rip.clone());
                }
            }

            resolved_rip.unwrap_or_else(|| {
                if season > 0 {
                    format!("Season {:02}", season)
                } else {
                    "hd Rip".to_string()
                }
            })
        };

        let episodes = self.fetch_episodes(title_id, &rip_to_query).await?;
        if episodes.is_empty() {
            return Ok(Vec::new());
        }

        let matched_items: Vec<&DramachiEpisodeItem> = if season == 0 && episode == 0 {
            episodes.iter().collect()
        } else {
            let exact = episodes
                .iter()
                .filter(|ep| parse_episode_number(&ep.f_title) == Some(episode))
                .collect::<Vec<_>>();

            if !exact.is_empty() {
                exact
            } else if episode > 0 && episode <= episodes.len() {
                vec![&episodes[episode - 1]]
            } else {
                Vec::new()
            }
        };

        let stream_futs: Vec<_> = matched_items
            .into_iter()
            .filter(|ep| !ep.fid.is_empty() && !ep.disk.is_empty())
            .map(|ep| {
                let ep_fid = ep.fid.clone();
                let ep_disk = ep.disk.clone();
                let ep_quality = ep.quality.clone();
                let ep_size = ep.size.clone();
                async move {
                    match self.fetch_file_stream(&ep_fid, &ep_disk).await {
                        Ok((stream_url, filename)) => {
                            let quality = ep_quality.filter(|q| !q.is_empty());
                            let size_bytes = crate::providers::models::parse_size_bytes(
                                ep_size.as_deref().unwrap_or_default(),
                            );

                            Some(Release {
                                provider: ProviderKind::Dramachi,
                                filename,
                                quality,
                                codec: None,
                                language: None,
                                size_bytes,
                                season: if season > 0 { Some(season) } else { None },
                                episode: if episode > 0 { Some(episode) } else { None },
                                mirrors: vec![SourceMirror {
                                    label: "Dramachi CDN".to_string(),
                                    resolver_url: stream_url,
                                    headers: vec![],
                                    direct_file: true,
                                }],
                                resource_id: Some(ep_fid),
                            })
                        }
                        Err(err) => {
                            log::warn!("failed to resolve Dramachi stream for fid {ep_fid}: {err}");
                            None
                        }
                    }
                }
            })
            .collect();

        let mut releases: Vec<Release> = futures::future::join_all(stream_futs)
            .await
            .into_iter()
            .flatten()
            .collect();

        releases.sort_by_key(|r| std::cmp::Reverse(r.resolution_i64()));

        Ok(releases)
    }
}

fn extract_leading_season_number(name: &str) -> Option<usize> {
    let lower = name.to_ascii_lowercase();
    let after_season = lower.strip_prefix("season")?.trim();
    let num_str: String = after_season
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    num_str.parse::<usize>().ok()
}

fn parse_episode_number(f_title: &str) -> Option<usize> {
    let cleaned = clean_episode_title(f_title);
    let upper = cleaned.to_ascii_uppercase();
    for prefix in ["EPISODE", "EP.", "EP", "E"] {
        let mut search_from = 0;
        while let Some(rel_pos) = upper[search_from..].find(prefix) {
            let pos = search_from + rel_pos;
            let prev_char = upper[..pos].chars().next_back();
            let valid_boundary = match prev_char {
                None => true,
                Some(ch) => !ch.is_alphabetic() || (prefix == "E" && ch.is_ascii_digit()),
            };
            let after = &upper[pos + prefix.len()..];
            let after_trimmed = after.trim_start_matches([' ', '-', '.', '_']);
            let num_str: String = after_trimmed
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if valid_boundary
                && !num_str.is_empty()
                && let Ok(num) = num_str.parse::<usize>()
            {
                return Some(num);
            }
            search_from = pos + prefix.len();
        }
    }

    if let Some(last_token) = cleaned.split_whitespace().last()
        && let Ok(num) = last_token.parse::<usize>()
    {
        return Some(num);
    }

    None
}
fn clean_episode_title(f_title: &str) -> String {
    let mut title = f_title.trim();
    for suffix in &[
        " 720p",
        " 1080p",
        " 540p",
        " 480p",
        " 360p",
        " DUB",
        " hi DUB",
        " ENG Subbed Full",
    ] {
        if let Some(stripped) = title.strip_suffix(suffix) {
            title = stripped;
        }
    }
    title.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_episode_number_formats() {
        assert_eq!(parse_episode_number("Queen of Tears S01E16"), Some(16));
        assert_eq!(parse_episode_number("Squid Game S02E07 DUB"), Some(7));
        assert_eq!(parse_episode_number("Parasite 2019 001"), Some(1));
        assert_eq!(parse_episode_number("Parasite 2019 002"), Some(2));
        assert_eq!(parse_episode_number("The Mentalist S07E12"), Some(12));
    }

    #[test]
    fn empty_episode_response_does_not_create_a_placeholder_episode() {
        let season = season_from_episode_result(1, Ok(Vec::new())).unwrap();
        assert!(season.episodes.is_empty());
    }

    #[test]
    fn episode_fetch_failure_is_preserved_as_an_error() {
        assert!(matches!(
            season_from_episode_result(1, Err(DramachiError::NotFound)),
            Err(DramachiError::NotFound)
        ));
    }

    #[test]
    fn test_extract_leading_season_number() {
        assert_eq!(extract_leading_season_number("Season 01"), Some(1));
        assert_eq!(extract_leading_season_number("Season 2"), Some(2));
        assert_eq!(extract_leading_season_number("Season 07"), Some(7));
        assert_eq!(extract_leading_season_number("hd Rip"), None);
    }

    #[test]
    fn test_parse_size_to_bytes() {
        assert_eq!(
            crate::providers::models::parse_size_bytes("218.32 MB"),
            Some(228925112)
        );
        assert_eq!(
            crate::providers::models::parse_size_bytes("1.5 GB"),
            Some(1610612736)
        );
        assert_eq!(crate::providers::models::parse_size_bytes(""), None);
    }
}
