use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::models::BrowseMetrics;
use crate::providers::Provider;
use crate::providers::bdix::circleftp::CircleFtpClient;
use crate::providers::bdix::dhakaflix::client::DhakaFlixClient;
use crate::providers::fourkhdhub::FourKHdHubClient;
use crate::providers::models::{CatalogItem, MediaDetails, ProviderError, ProviderKind};
use crate::providers::moviebox::client::MovieBoxClient;

#[derive(Clone)]
pub struct MovieBoxService {
    pub client: MovieBoxClient,
    pub fourk_client: Option<FourKHdHubClient>,
    pub circleftp_client: CircleFtpClient,
    pub dhakaflix_client: DhakaFlixClient,
    pub addon_client: crate::providers::addons::AddonClient,
    pub dramachi_client: crate::providers::dramachi::DramachiClient,
    pub http_client: reqwest::Client,
}

impl Default for MovieBoxService {
    fn default() -> Self {
        Self::new()
    }
}

impl MovieBoxService {
    pub fn new() -> Self {
        let http_client = crate::net::http_client_builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_default();

        Self {
            client: MovieBoxClient::new(),
            fourk_client: FourKHdHubClient::new().ok(),
            circleftp_client: CircleFtpClient::new(),
            dhakaflix_client: DhakaFlixClient::new(),
            addon_client: crate::providers::addons::AddonClient::new(),
            dramachi_client: crate::providers::dramachi::DramachiClient::new(),
            http_client,
        }
    }

    pub fn capabilities(&self, provider: ProviderKind) -> crate::providers::ProviderCapabilities {
        match provider {
            ProviderKind::MovieBox => crate::providers::ProviderCapabilities {
                supports_search: false,
                supports_pagination: false,
                supports_series: false,
                supports_subtitles: false,
                supports_homepage: false,
            },
            ProviderKind::FourKHdHub => self
                .fourk_client
                .as_ref()
                .map(Provider::capabilities)
                .unwrap_or_default(),
            ProviderKind::BdixCircleFtp => Provider::capabilities(&self.circleftp_client),
            ProviderKind::BdixDhakaFlix => Provider::capabilities(&self.dhakaflix_client),
            ProviderKind::Addons => Provider::capabilities(&self.addon_client),
            ProviderKind::Dramachi => Provider::capabilities(&self.dramachi_client),
        }
    }

    pub async fn suggest(&self, _query: &str) -> Result<Vec<String>, String> {
        Err("MovieBox is disabled".to_string())
    }

    pub async fn search_typed(
        &self,
        provider: ProviderKind,
        query: &str,
        page: usize,
    ) -> Result<Vec<CatalogItem>, ProviderError> {
        if provider == ProviderKind::MovieBox {
            return Err(ProviderError::Unavailable(
                "MovieBox is disabled".to_string(),
            ));
        }
        match provider {
            ProviderKind::MovieBox => Provider::search(&self.client, query, page).await,
            ProviderKind::FourKHdHub => {
                let fourk = self.fourk_client.as_ref().ok_or_else(|| {
                    ProviderError::Unavailable("4KHDHub is unavailable".to_string())
                })?;
                Provider::search(fourk, query, page).await
            }
            ProviderKind::BdixCircleFtp => {
                Provider::search(&self.circleftp_client, query, page).await
            }
            ProviderKind::BdixDhakaFlix => {
                Provider::search(&self.dhakaflix_client, query, page).await
            }
            ProviderKind::Addons => Provider::search(&self.addon_client, query, page).await,
            ProviderKind::Dramachi => Provider::search(&self.dramachi_client, query, page).await,
        }
    }

    pub async fn search_language_typed(
        &self,
        provider: ProviderKind,
        query: &str,
        page: usize,
    ) -> Result<Vec<CatalogItem>, ProviderError> {
        let intent = crate::language::parse_query(query);
        let mut items = self.search_typed(provider, &intent.title, page).await?;
        if let Some(language) = intent.language {
            let mut matching = Vec::new();
            let mut other = Vec::new();
            for mut item in items {
                if crate::language::decorate_catalog_item(&mut item, Some(language)) {
                    matching.push(item);
                } else {
                    other.push(item);
                }
            }
            matching.extend(other);
            items = matching;
        }
        Ok(items)
    }

    pub async fn fetch_addon_catalog(
        &self,
        manifest_url: &str,
        r#type: &str,
        catalog_id: &str,
    ) -> Result<Vec<CatalogItem>, String> {
        let manifest_clone = manifest_url.to_string();
        let type_clone = r#type.to_string();
        let cat_id_clone = catalog_id.to_string();
        if let Ok(Some(cached)) = tokio::task::spawn_blocking(move || {
            crate::cache::get_addon_catalog_cache_typed(&manifest_clone, &type_clone, &cat_id_clone)
        })
        .await
        {
            return Ok(cached);
        }

        let base_url = crate::providers::addons::AddonClient::base_addon_url(manifest_url);
        let metas = self
            .addon_client
            .fetch_catalog(&base_url, r#type, catalog_id, None)
            .await
            .map_err(|e| e.to_string())?;

        let items: Vec<CatalogItem> = metas
            .iter()
            .map(crate::providers::addons::adapter::meta_to_catalog_item)
            .collect();

        let manifest_clone = manifest_url.to_string();
        let type_clone = r#type.to_string();
        let cat_id_clone = catalog_id.to_string();
        let items_clone = items.clone();
        tokio::task::spawn_blocking(move || {
            crate::cache::set_addon_catalog_cache_typed(
                &manifest_clone,
                &type_clone,
                &cat_id_clone,
                &items_clone,
            );
        });
        Ok(items)
    }

    pub async fn details_typed(
        &self,
        provider: ProviderKind,
        subject_id: &str,
    ) -> Result<MediaDetails, ProviderError> {
        if provider == ProviderKind::MovieBox {
            return Err(ProviderError::Unavailable(
                "MovieBox is disabled".to_string(),
            ));
        }
        match provider {
            ProviderKind::MovieBox => Provider::details(&self.client, subject_id).await,
            ProviderKind::FourKHdHub => {
                let fourk = self.fourk_client.as_ref().ok_or_else(|| {
                    ProviderError::Unavailable("4KHDHub is unavailable".to_string())
                })?;
                Provider::details(fourk, subject_id).await
            }
            ProviderKind::BdixCircleFtp => {
                Provider::details(&self.circleftp_client, subject_id).await
            }
            ProviderKind::BdixDhakaFlix => {
                Provider::details(&self.dhakaflix_client, subject_id).await
            }
            ProviderKind::Addons => Provider::details(&self.addon_client, subject_id).await,
            ProviderKind::Dramachi => Provider::details(&self.dramachi_client, subject_id).await,
        }
    }

    pub async fn homepage(
        &self,
        _tab_id: &str,
        _page: usize,
    ) -> Result<
        (
            Vec<CatalogItem>,
            std::collections::HashMap<String, BrowseMetrics>,
        ),
        String,
    > {
        Err("MovieBox is disabled".to_string())
    }

    pub async fn moviebox_classification_metadata(
        &self,
        _subject_id: &str,
    ) -> Option<(Option<String>, Option<String>, Option<String>)> {
        None
    }

    pub async fn fetch_collection_resolutions(
        &self,
        _subject_id: &str,
    ) -> Result<Vec<u32>, String> {
        Err("MovieBox is disabled".to_string())
    }

    pub async fn get_ext_captions(
        &self,
        _subject_id: &str,
        _resource_id: &str,
        _sibling_ids: &[String],
        _season: usize,
        _episode: usize,
    ) -> Result<Vec<crate::providers::models::SubtitleOption>, String> {
        Err("MovieBox is disabled".to_string())
    }

    pub async fn fetch_poster_bytes(&self, url: &str) -> Option<Vec<u8>> {
        const MAX_POSTER_BYTES: usize = 5 * 1024 * 1024;
        if crate::net::validate_public_url(url, true).await.is_err() {
            log::warn!(
                "poster destination rejected: {}",
                crate::logging::sanitize_url(url)
            );
            return None;
        }
        let mut response = self
            .http_client
            .get(url)
            .header("User-Agent", crate::net::APP_HTTP_USER_AGENT)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?;
        let content_len = response.content_length();
        if content_len.is_some_and(|len| len > MAX_POSTER_BYTES as u64) {
            log::warn!(
                "poster at {} exceeds size limit ({} bytes), skipping",
                crate::logging::sanitize_url(url),
                content_len.unwrap_or(0)
            );
            return None;
        }
        let mut buf = Vec::with_capacity(
            content_len
                .map(|l| (l as usize).min(MAX_POSTER_BYTES))
                .unwrap_or(64 * 1024),
        );
        while let Ok(Some(chunk)) = response.chunk().await {
            if buf.len().saturating_add(chunk.len()) > MAX_POSTER_BYTES {
                return None;
            }
            buf.extend_from_slice(&chunk);
        }
        if buf.is_empty() { None } else { Some(buf) }
    }

    pub async fn fetch_and_decode_cached_poster(
        &self,
        namespace: &str,
        id: &str,
        url: &str,
    ) -> Option<Arc<image::DynamicImage>> {
        let ns = namespace.to_string();
        let id_owned = id.to_string();
        if let Ok(Some(bytes)) = tokio::task::spawn_blocking(move || {
            crate::cache::get_namespaced_image_cache(&ns, &id_owned)
        })
        .await
            && let Some(img) = decode_poster(bytes).await
        {
            return Some(img);
        }
        let bytes = Arc::<[u8]>::from(self.fetch_poster_bytes(url).await?);
        let ns = namespace.to_string();
        let id_owned = id.to_string();
        let bytes_for_cache = Arc::clone(&bytes);
        tokio::task::spawn_blocking(move || {
            crate::cache::set_namespaced_image_cache(&ns, &id_owned, &bytes_for_cache);
        });
        decode_poster_arc(bytes).await
    }

    pub async fn download_subtitle_file(
        &self,
        url: &str,
        headers: &[(String, String)],
        preferred_filename: Option<&str>,
    ) -> Result<PathBuf, String> {
        crate::net::validate_public_url(url, true)
            .await
            .map_err(|_| "subtitle destination rejected".to_string())?;
        let mut request = self.http_client.get(url);
        let sub_host = crate::proxy::extract_host_authority(url);
        let referer_host = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("referer"))
            .and_then(|(_, val)| crate::proxy::extract_host_authority(val));
        let same_host = sub_host.is_some() && sub_host == referer_host;
        for (name, value) in headers {
            if crate::proxy::should_forward_header(same_host, name) {
                request = request.header(name.as_str(), value.as_str());
            }
        }

        let response = tokio::time::timeout(std::time::Duration::from_secs(8), request.send())
            .await
            .map_err(|_| "timed out".to_string())?
            .map_err(|e| {
                if e.is_timeout() {
                    "timed out".to_string()
                } else {
                    "connection failed".to_string()
                }
            })?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status().as_u16()));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|_| "read error".to_string())?;

        let extension = subtitle_extension_from_url(url);
        let base_dir = resolve_subtitle_dir();
        let _ = tokio::fs::create_dir_all(&base_dir).await;

        let url_clean = url.split(['?', '#']).next().unwrap_or(url);
        let url_tag = &crate::cache::md5_hex(url_clean)[..8];
        let file_stem = if let Some(pref) = preferred_filename {
            let base = crate::download::safe_file_stem(pref);
            format!("{base}_{url_tag}")
        } else {
            format!(
                "{}_{}_{url_tag}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            )
        };
        let path = base_dir.join(format!("{file_stem}.{extension}"));
        tokio::fs::write(&path, bytes)
            .await
            .map_err(|e| format!("write error ({})", e.kind()))?;

        Ok(path)
    }
}

pub async fn decode_poster(bytes: Vec<u8>) -> Option<Arc<image::DynamicImage>> {
    tokio::task::spawn_blocking(move || {
        let img = image::load_from_memory(&bytes).ok()?;
        const MAX_DIM: u32 = 512;
        let downscaled = if img.width().max(img.height()) <= MAX_DIM {
            img
        } else {
            img.resize(MAX_DIM, MAX_DIM, image::imageops::FilterType::Triangle)
        };
        Some(Arc::new(downscaled))
    })
    .await
    .ok()?
}

async fn decode_poster_arc(bytes: Arc<[u8]>) -> Option<Arc<image::DynamicImage>> {
    tokio::task::spawn_blocking(move || {
        let img = image::load_from_memory(&bytes).ok()?;
        const MAX_DIM: u32 = 512;
        let downscaled = if img.width().max(img.height()) <= MAX_DIM {
            img
        } else {
            img.resize(MAX_DIM, MAX_DIM, image::imageops::FilterType::Triangle)
        };
        Some(Arc::new(downscaled))
    })
    .await
    .ok()?
}

pub fn metric_value(item: &serde_json::Value, keys: &[&str]) -> Option<f64> {
    let mut containers = vec![item];
    if let Some(metadata) = item.get("metadata") {
        containers.push(metadata);
    }
    if let Some(meta) = item.get("meta") {
        containers.push(meta);
    }

    containers.into_iter().find_map(|container| {
        keys.iter().find_map(|key| {
            let value = container.get(*key)?;
            value
                .as_f64()
                .or_else(|| value.as_i64().map(|number| number as f64))
                .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        })
    })
}

pub fn extract_browse_metrics(item: &serde_json::Value) -> BrowseMetrics {
    BrowseMetrics {
        trending: metric_value(
            item,
            &["__browse_rank", "imdb_trending", "imdbTrending", "trending"],
        ),
        rating: metric_value(
            item,
            &["imdbRatingValue", "imdbRate", "imdb_rating", "imdbRating"],
        ),
        recent_rating: metric_value(
            item,
            &[
                "imdb_rating_30d",
                "imdbRating30Days",
                "imdbRatingLast30Days",
                "imdb_rating_recent",
                "imdbRatingValue",
                "imdbRate",
            ],
        ),
        popularity: metric_value(
            item,
            &[
                "__browse_rank",
                "imdb_popularity",
                "imdbPopularity",
                "popularity",
                "viewers",
            ],
        ),
        genres: text_values(item, &["genre", "genres"]),
        category: text_value(item, &["category", "contentType", "mediaType", "kind"]),
        country: text_value(item, &["country", "countries", "region", "area"]),
        original_language: text_value(
            item,
            &["originalLanguage", "original_language", "language", "lang"],
        ),
    }
}

fn text_value(item: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        let value = item.get(*key)?;
        value
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn text_values(item: &serde_json::Value, keys: &[&str]) -> Vec<String> {
    keys.iter()
        .find_map(|key| {
            let value = item.get(*key)?;
            if let Some(array) = value.as_array() {
                let values = array
                    .iter()
                    .filter_map(|entry| entry.as_str())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>();
                return (!values.is_empty()).then_some(values);
            }
            value.as_str().map(|text| {
                text.split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default()
}

pub fn resolve_subtitle_dir() -> PathBuf {
    if crate::config::is_termux_environment() {
        if let Some(home) = dirs::home_dir() {
            let storage = home.join("storage/downloads/moviebox_subs");
            if home.join("storage/downloads").exists() {
                let _ = std::fs::create_dir_all(&storage);
                return storage;
            }
        }
    }
    crate::config::cache_dir().join("subs")
}

pub fn subtitle_extension_from_url(url: &str) -> &'static str {
    let clean = url.split(['?', '#']).next().unwrap_or(url);
    match clean.rsplit('.').next() {
        Some(ext) if ext.eq_ignore_ascii_case("srt") => "srt",
        Some(ext) if ext.eq_ignore_ascii_case("vtt") => "vtt",
        Some(ext) if ext.eq_ignore_ascii_case("ass") => "ass",
        Some(ext) if ext.eq_ignore_ascii_case("ssa") => "ssa",
        Some(ext) if ext.eq_ignore_ascii_case("sub") => "sub",
        _ => "srt",
    }
}

pub fn ensure_moviebox_subdir(path: &Path) -> PathBuf {
    let is_already_mb = path
        .file_name()
        .map(|name| {
            let s = name.to_string_lossy();
            s.eq_ignore_ascii_case("MovieBox-TUI") || s.eq_ignore_ascii_case("MovieBox")
        })
        .unwrap_or(false);

    if is_already_mb {
        path.to_path_buf()
    } else {
        path.join("MovieBox-TUI")
    }
}

pub fn resolve_download_dir(custom_dir: Option<&Path>) -> PathBuf {
    if let Some(custom) = custom_dir {
        if std::fs::create_dir_all(custom).is_ok() {
            let probe = custom.join(format!(".mb_probe_{}", std::process::id()));
            if std::fs::write(&probe, b"ok").is_ok() {
                let _ = std::fs::remove_file(&probe);
                return ensure_moviebox_subdir(custom);
            }
        }
    }

    let base_dir = dirs::download_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join("Downloads")))
        .unwrap_or_else(|| PathBuf::from("."));

    if crate::config::is_termux_environment() {
        if let Some(home) = dirs::home_dir() {
            let android_storage = home.join("storage/downloads");
            if android_storage.exists() {
                return ensure_moviebox_subdir(&android_storage);
            }
        }
    }

    ensure_moviebox_subdir(&base_dir)
}

pub fn find_matching_resource_item(
    items: &[serde_json::Value],
    season: usize,
    episode: usize,
) -> Option<&serde_json::Value> {
    items.iter().find(|item| {
        let parse_num = |k: &str| -> Option<usize> {
            item.get(k).and_then(|v| {
                v.as_u64()
                    .map(|n| n as usize)
                    .or_else(|| v.as_i64().map(|n| n as usize))
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
        };
        let se = parse_num("se");
        let ep_num = parse_num("ep");
        (season == 0 && episode == 0) || (se == Some(season) && ep_num == Some(episode))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn typed_moviebox_dispatch_is_rejected_without_provider_requests() {
        let service = MovieBoxService::new();
        assert_eq!(
            service.suggest("must not be sent").await.unwrap_err(),
            "MovieBox is disabled"
        );
        assert!(service.homepage("2", 1).await.is_err());
        assert!(
            service
                .moviebox_classification_metadata("must not be sent")
                .await
                .is_none()
        );
        assert!(
            service
                .fetch_collection_resolutions("must not be sent")
                .await
                .is_err()
        );
        assert!(
            service
                .get_ext_captions("must not be sent", "id", &[], 0, 0)
                .await
                .is_err()
        );
        assert!(matches!(
            service.search_typed(ProviderKind::MovieBox, "must not be sent", 0).await,
            Err(ProviderError::Unavailable(message)) if message == "MovieBox is disabled"
        ));
        assert!(matches!(
            service.details_typed(ProviderKind::MovieBox, "must not be sent").await,
            Err(ProviderError::Unavailable(message)) if message == "MovieBox is disabled"
        ));
        assert!(!service.capabilities(ProviderKind::MovieBox).supports_search);
    }

    #[tokio::test]
    async fn test_decode_poster_caps_dimensions() {
        let mut buffer = std::io::Cursor::new(Vec::new());
        let img = image::DynamicImage::new_rgb8(800, 600);
        img.write_to(&mut buffer, image::ImageFormat::Png).unwrap();
        let bytes = buffer.into_inner();

        let decoded = decode_poster(bytes).await.unwrap();
        assert!(decoded.width() <= 512);
        assert!(decoded.height() <= 512);
        assert_eq!(decoded.width(), 512);
        assert_eq!(decoded.height(), 384);
    }

    #[test]
    fn test_find_matching_resource_item_integer_and_string_values() {
        let items = vec![
            serde_json::json!({
                "id": "1001",
                "se": 1,
                "ep": 1
            }),
            serde_json::json!({
                "id": "1002",
                "se": "2",
                "ep": "5"
            }),
        ];

        let matched = find_matching_resource_item(&items, 2, 5);
        assert!(matched.is_some());
        assert_eq!(matched.unwrap().get("id").unwrap().as_str(), Some("1002"));
    }

    #[test]
    fn test_find_matching_resource_item_series_does_not_fall_back_to_episode_one() {
        let items = vec![
            serde_json::json!({
                "id": "1001",
                "se": 1,
                "ep": 1
            }),
            serde_json::json!({
                "id": "1002",
                "se": 1,
                "ep": 2
            }),
        ];

        let matched = find_matching_resource_item(&items, 2, 3);
        assert!(matched.is_none());
    }

    #[test]
    fn test_find_matching_resource_item_movie_matches_first() {
        let items = vec![
            serde_json::json!({
                "id": "movie_res_1"
            }),
            serde_json::json!({
                "id": "movie_res_2"
            }),
        ];

        let matched = find_matching_resource_item(&items, 0, 0);
        assert!(matched.is_some());
        assert_eq!(
            matched.unwrap().get("id").unwrap().as_str(),
            Some("movie_res_1")
        );
    }

    #[test]
    fn test_subtitle_extension_from_url_strips_signed_query_and_fragment() {
        assert_eq!(
            subtitle_extension_from_url(
                "https://cacdn.hakunaymatata.com/subtitle/5cc122.vtt?Policy=eyJ&Signature=abc&Key-Pair-Id=KMH"
            ),
            "vtt"
        );
        assert_eq!(
            subtitle_extension_from_url("https://cdn.example.com/subs/track.ASS?token=123#cue1"),
            "ass"
        );
        assert_eq!(
            subtitle_extension_from_url("https://cdn.example.com/subs/track.ssa"),
            "ssa"
        );
        assert_eq!(
            subtitle_extension_from_url("https://cdn.example.com/subs/track.sub?exp=999"),
            "sub"
        );
        assert_eq!(
            subtitle_extension_from_url("https://cdn.example.com/subs/unknown?format=text"),
            "srt"
        );
    }

    #[test]
    fn test_extract_browse_metadata_for_verified_classification() {
        let anime = extract_browse_metrics(&serde_json::json!({
            "genre": ["Action", "Anime"],
            "category": "anime",
            "originalLanguage": "Japanese"
        }));
        assert_eq!(anime.category.as_deref(), Some("anime"));
        assert_eq!(anime.original_language.as_deref(), Some("Japanese"));
        assert!(anime.genres.iter().any(|genre| genre == "Anime"));

        let kdrama = extract_browse_metrics(&serde_json::json!({
            "genres": "Drama, Romance",
            "country": "South Korea",
            "language": "Korean"
        }));
        assert_eq!(kdrama.country.as_deref(), Some("South Korea"));
        assert_eq!(kdrama.original_language.as_deref(), Some("Korean"));
        assert_eq!(kdrama.genres, vec!["Drama", "Romance"]);
    }
}
