use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, PlaybackSource, ProviderError, ProviderKind,
    ProviderMediaId, Release, Season, SourceMirror,
};
use crate::providers::{Provider, ProviderCapabilities, ReleaseProvider};
use reqwest::{Client, Url};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const BASE_URL: &str = "https://anizone.to";
const DEFAULT_SUB_SERVER_URL: &str = "https://anizone-prophecy-sub-server.onrender.com";
const DEFAULT_UA: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0";
const MAX_HTML_BYTES: usize = 4 * 1024 * 1024;
const MAX_PAGES: usize = 64;
const PAGE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum AniZoneError {
    #[error("network request failed: {0}")]
    Network(#[from] reqwest::Error),
    #[error("invalid AniZone response: {0}")]
    Parse(String),
    #[error("AniZone item was not found")]
    NotFound,
}

impl From<AniZoneError> for ProviderError {
    fn from(error: AniZoneError) -> Self {
        match error {
            AniZoneError::Network(error) => ProviderError::Network(error.to_string()),
            AniZoneError::Parse(message) => ProviderError::Parsing(message),
            AniZoneError::NotFound => ProviderError::NotFound,
        }
    }
}

#[derive(Clone)]
pub struct AniZoneClient {
    client: Client,
    base_url: Url,
    sub_server_url: Option<Url>,
    prefer_sub_server: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
struct EpisodePage {
    items: Vec<Value>,
    snapshot: String,
    cursor: Option<String>,
    has_more: bool,
    csrf: String,
    cookies: HashMap<String, String>,
    series_title: Option<String>,
}

#[derive(Debug, Clone)]
struct WatchData {
    hls: String,
    subtitles: Vec<SubtitleDescriptor>,
}

#[derive(Debug, Clone)]
struct SubtitleDescriptor {
    url: String,
    label: String,
    language: String,
    format: String,
    is_default: bool,
}

impl Default for AniZoneClient {
    fn default() -> Self {
        Self::new()
    }
}

impl AniZoneClient {
    pub fn new() -> Self {
        let mut client = Self::with_base_url(BASE_URL).expect("static AniZone URL must parse");
        let sub_url = match std::env::var("ANIZONE_SUB_SERVER_URL") {
            Ok(val) => {
                let trimmed = val.trim();
                if trimmed.is_empty() {
                    Some(DEFAULT_SUB_SERVER_URL.to_string())
                } else if trimmed.eq_ignore_ascii_case("direct")
                    || trimmed.eq_ignore_ascii_case("off")
                {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            Err(_) => Some(DEFAULT_SUB_SERVER_URL.to_string()),
        };
        if let Some(raw) = sub_url
            && let Ok(parsed) = Url::parse(raw.trim_end_matches('/'))
            && (parsed.scheme() == "https" || parsed.scheme() == "http")
        {
            client.sub_server_url = Some(parsed);
        }
        client
    }

    pub fn with_base_url(base: &str) -> Result<Self, AniZoneError> {
        let base_url =
            Url::parse(base).map_err(|_| AniZoneError::Parse("invalid base URL".into()))?;
        if base_url.scheme() != "https" {
            return Err(AniZoneError::Parse("AniZone requires HTTPS".into()));
        }
        let client = crate::net::http_client_builder()
            .timeout(PAGE_TIMEOUT)
            .build()
            .map_err(AniZoneError::Network)?;
        Ok(Self {
            client,
            base_url,
            sub_server_url: None,
            prefer_sub_server: Arc::new(AtomicBool::new(false)),
        })
    }

    fn should_fallback_to_sub_server(&self, err: &AniZoneError) -> bool {
        if self.sub_server_url.is_none() {
            return false;
        }
        match err {
            AniZoneError::Network(req_err) => req_err.status().is_some_and(|s| {
                s == reqwest::StatusCode::FORBIDDEN || s == reqwest::StatusCode::TOO_MANY_REQUESTS
            }),
            _ => false,
        }
    }

    async fn fetch_sub_server_json(&self, url: Url) -> Result<Value, AniZoneError> {
        let response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;
        let status = response.status();
        let bytes = response.bytes().await?;
        if bytes.len() > MAX_HTML_BYTES {
            return Err(AniZoneError::Parse(
                "AniZone sub-server response exceeded size limit".into(),
            ));
        }

        if !status.is_success() {
            if let Ok(body) = serde_json::from_slice::<Value>(&bytes) {
                let code = body
                    .pointer("/error/code")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let message = body
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("AniZone sub-server error");
                if code == "NOT_FOUND" || status == reqwest::StatusCode::NOT_FOUND {
                    return Err(AniZoneError::NotFound);
                }
                return Err(AniZoneError::Parse(format!(
                    "HTTP {}: {message}",
                    status.as_u16()
                )));
            }
            if status == reqwest::StatusCode::NOT_FOUND {
                return Err(AniZoneError::NotFound);
            }
            return Err(AniZoneError::Parse(format!(
                "AniZone sub-server returned HTTP {}",
                status.as_u16()
            )));
        }

        let body: Value = serde_json::from_slice(&bytes)
            .map_err(|_| AniZoneError::Parse("invalid AniZone sub-server JSON".into()))?;
        if body.get("status").and_then(Value::as_str) == Some("error") {
            let code = body
                .pointer("/error/code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let message = body
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("AniZone sub-server error");
            if code == "NOT_FOUND" {
                return Err(AniZoneError::NotFound);
            }
            return Err(AniZoneError::Parse(message.to_string()));
        }
        Ok(body)
    }

    async fn search_via_sub_server(
        &self,
        sub_base: &Url,
        query: &str,
        page: usize,
    ) -> Result<Vec<CatalogItem>, AniZoneError> {
        let mut url = sub_base
            .join("api/v1/anime/search")
            .map_err(|e| AniZoneError::Parse(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("q", query)
            .append_pair("page", &page.max(1).to_string());
        let body = self.fetch_sub_server_json(url).await?;
        let items = body
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| AniZoneError::Parse("missing search data array".into()))?;
        Ok(items
            .iter()
            .filter_map(|item| {
                let slug = item.get("id")?.as_str()?.to_string();
                let title = item.get("title")?.as_str()?.to_string();
                let kind = item
                    .get("media_type")
                    .and_then(Value::as_str)
                    .unwrap_or("series");
                let year = item
                    .get("year")
                    .and_then(Value::as_i64)
                    .map(|y| y.to_string());
                let poster_url = item
                    .get("poster")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                Some(CatalogItem {
                    id: ProviderMediaId {
                        provider: ProviderKind::AniZone,
                        value: slug,
                    },
                    title,
                    media_type: if kind.eq_ignore_ascii_case("movie") {
                        MediaType::Movie
                    } else {
                        MediaType::Series
                    },
                    year,
                    poster_url,
                    season_count: None,
                })
            })
            .collect())
    }

    async fn details_via_sub_server(
        &self,
        sub_base: &Url,
        slug: &str,
    ) -> Result<MediaDetails, AniZoneError> {
        let url = sub_base
            .join(&format!("api/v1/anime/{slug}"))
            .map_err(|e| AniZoneError::Parse(e.to_string()))?;
        let body = self.fetch_sub_server_json(url).await?;
        let data = body
            .get("data")
            .ok_or_else(|| AniZoneError::Parse("missing details data".into()))?;
        let title = data
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| slug.replace('-', " "));
        let seasons_arr = data
            .get("seasons")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let seasons = seasons_arr
            .into_iter()
            .filter_map(|s| {
                let s_num = s.get("number").and_then(Value::as_u64).unwrap_or(1) as usize;
                let eps = s
                    .get("episodes")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|ep| {
                        let number = ep.get("number").and_then(Value::as_u64)? as usize;
                        let ep_season =
                            ep.get("season")
                                .and_then(Value::as_u64)
                                .unwrap_or(s_num as u64) as usize;
                        let ep_id = ep
                            .get("provider_episode_id")
                            .or_else(|| ep.get("id"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| number.to_string());
                        Some(Episode {
                            season: ep_season,
                            number,
                            title: ep.get("title").and_then(Value::as_str).map(str::to_string),
                            overview: ep
                                .get("description")
                                .and_then(Value::as_str)
                                .map(str::to_string),
                            provider_episode_id: Some(ep_id),
                            provider_season_id: Some(ep_season.to_string()),
                        })
                    })
                    .collect::<Vec<_>>();
                Some(Season {
                    number: s_num,
                    episodes: eps,
                    provider_season_id: Some(s_num.to_string()),
                })
            })
            .collect::<Vec<_>>();
        Ok(MediaDetails {
            id: ProviderMediaId {
                provider: ProviderKind::AniZone,
                value: slug.to_string(),
            },
            title,
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
            genres: Vec::new(),
            seasons,
            dubs: Vec::new(),
        })
    }

    async fn releases_via_sub_server(
        &self,
        sub_base: &Url,
        slug: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, AniZoneError> {
        let url = sub_base
            .join(&format!("api/v1/anime/{slug}/episodes/{episode}/sources"))
            .map_err(|e| AniZoneError::Parse(e.to_string()))?;
        let body = self.fetch_sub_server_json(url).await?;
        let data = body
            .get("data")
            .ok_or_else(|| AniZoneError::Parse("missing sources data".into()))?;
        let sources = data
            .get("sources")
            .and_then(Value::as_array)
            .ok_or(AniZoneError::NotFound)?;
        if sources.is_empty() {
            return Err(AniZoneError::NotFound);
        }
        let sub_arr = data.get("subtitles").and_then(Value::as_array);
        let sub_count = sub_arr.map(|s| s.len()).unwrap_or(0);
        let best_sub = sub_arr.and_then(|subs| {
            subs.iter()
                .max_by_key(|item| {
                    let is_default = item
                        .get("default")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let has_lang = item
                        .get("language")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty());
                    let has_label = item
                        .get("label")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty());
                    let is_vtt = item
                        .get("format")
                        .and_then(Value::as_str)
                        .is_some_and(|s| s.eq_ignore_ascii_case("vtt"));
                    (is_default, has_lang, has_label, is_vtt)
                })
                .and_then(|item| item.get("url").and_then(Value::as_str))
                .map(str::to_string)
        });
        let resource_id = data
            .get("source_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| episode.to_string());

        let mut releases = Vec::new();
        for (idx, src) in sources.iter().enumerate() {
            let Some(hls) = src
                .get("url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .map(str::to_string)
            else {
                continue;
            };
            let mut headers = Vec::new();
            if let Some(hdr_obj) = src.get("headers").and_then(Value::as_object) {
                for (k, v) in hdr_obj {
                    if let Some(val) = v.as_str() {
                        headers.push((k.clone(), val.to_string()));
                    }
                }
            }
            if headers.is_empty() {
                headers.push((
                    "Referer".to_string(),
                    format!("{BASE_URL}/anime/{slug}/{episode}"),
                ));
                headers.push(("User-Agent".to_string(), DEFAULT_UA.to_string()));
            }
            if sub_count > 0 {
                headers.push(("X-AniZone-Subtitle-Count".into(), sub_count.to_string()));
            }
            if let Some(ref sub_url) = best_sub {
                headers.push(("X-AniZone-Subtitle-Url".into(), sub_url.clone()));
            }
            let quality = src
                .get("quality")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            let codec = src
                .get("codec")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
            // Preserve AniZone's provider-declared English catalog language metadata
            // (or explicit source language if provided; remains unverified until independently confirmed).
            let language = src
                .get("language")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| "English".to_string());
            let label = src
                .get("label")
                .or_else(|| src.get("name"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    if sources.len() > 1 {
                        format!("AniZone HLS #{}", idx + 1)
                    } else {
                        "AniZone HLS".to_string()
                    }
                });
            let src_id = src
                .get("source_id")
                .or_else(|| src.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| resource_id.clone());

            releases.push(Release {
                provider: ProviderKind::AniZone,
                filename: format!("{slug} Episode {episode}"),
                quality,
                codec,
                language: Some(language),
                size_bytes: None,
                season: Some(season),
                episode: Some(episode),
                mirrors: vec![SourceMirror {
                    label,
                    resolver_url: hls,
                    headers,
                    direct_file: false,
                }],
                resource_id: Some(src_id),
            });
        }
        if releases.is_empty() {
            return Err(AniZoneError::Parse("missing AniZone source URL".into()));
        }
        Ok(releases)
    }

    fn url(&self, path: &str) -> Url {
        self.base_url
            .join(path.trim_start_matches('/'))
            .expect("validated base URL")
    }

    async fn fetch_text(
        &self,
        url: Url,
        headers: &[(&str, String)],
    ) -> Result<(String, Vec<String>), AniZoneError> {
        let mut last_err = None;
        for attempt in 0..=2u32 {
            let mut request = self
                .client
                .get(url.clone())
                .header("User-Agent", DEFAULT_UA)
                .header("Accept-Language", "en-US,en;q=0.9")
                .header(
                    "Accept",
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                );
            for (name, value) in headers {
                request = request.header(*name, value);
            }
            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        let cookies = response
                            .headers()
                            .get_all(reqwest::header::SET_COOKIE)
                            .iter()
                            .filter_map(|value| value.to_str().ok().map(ToOwned::to_owned))
                            .collect::<Vec<_>>();
                        let bytes = response.bytes().await?;
                        if bytes.len() > MAX_HTML_BYTES {
                            return Err(AniZoneError::Parse("response exceeded size limit".into()));
                        }
                        let text = String::from_utf8(bytes.to_vec())
                            .map_err(|_| AniZoneError::Parse("response was not UTF-8".into()))?;
                        return Ok((text, cookies));
                    }
                    let retryable = matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504);
                    let err = response.error_for_status().unwrap_err();
                    if !retryable || attempt == 2 {
                        return Err(AniZoneError::Network(err));
                    }
                    last_err = Some(err);
                }
                Err(err) => {
                    if attempt == 2 {
                        return Err(AniZoneError::Network(err));
                    }
                    last_err = Some(err);
                }
            }
            tokio::time::sleep(Duration::from_millis(500 * u64::from(attempt + 1))).await;
        }
        Err(AniZoneError::Network(
            last_err.expect("retry loop must capture an error"),
        ))
    }

    async fn fetch_page(&self, path: &str) -> Result<(String, Vec<String>), AniZoneError> {
        self.fetch_text(self.url(path), &[("Referer", format!("{BASE_URL}/"))])
            .await
    }

    pub async fn search(&self, query: &str, page: usize) -> Result<Vec<CatalogItem>, AniZoneError> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if self.prefer_sub_server.load(Ordering::Relaxed)
            && let Some(sub_base) = &self.sub_server_url
        {
            return self.search_via_sub_server(sub_base, query, page).await;
        }
        match self.search_direct(query, page).await {
            Ok(items) => Ok(items),
            Err(err) if self.should_fallback_to_sub_server(&err) => {
                if let Some(sub_base) = &self.sub_server_url {
                    log::warn!("anizone direct search blocked ({err}); falling back to sub-server");
                    self.prefer_sub_server.store(true, Ordering::Relaxed);
                    return self.search_via_sub_server(sub_base, query, page).await;
                }
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn search_direct(
        &self,
        query: &str,
        page: usize,
    ) -> Result<Vec<CatalogItem>, AniZoneError> {
        let mut url = self.url("/anime");
        url.query_pairs_mut().append_pair("search", query);
        let (html, cookies) = self
            .fetch_text(url, &[("Referer", format!("{BASE_URL}/"))])
            .await?;
        let mut state = initial_page(&html, &cookies)?;
        let target_page = page.max(1);
        let mut seen_cursors = HashSet::new();
        for _ in 1..target_page {
            if !state.has_more {
                state.items.clear();
                break;
            }
            let cursor = state
                .cursor
                .clone()
                .ok_or_else(|| AniZoneError::Parse("missing search pagination cursor".into()))?;
            if !seen_cursors.insert(cursor) {
                return Err(AniZoneError::Parse(
                    "repeated search pagination cursor".into(),
                ));
            }
            state = self.load_page("/anime", state).await?;
        }
        parse_search_items_from_values(&state.items)?
            .into_iter()
            .map(|item| {
                Ok(CatalogItem {
                    id: ProviderMediaId {
                        provider: ProviderKind::AniZone,
                        value: item.slug,
                    },
                    title: item.title,
                    media_type: if item.kind == "movie" {
                        MediaType::Movie
                    } else {
                        MediaType::Series
                    },
                    year: item.year.map(|year| year.to_string()),
                    poster_url: item.cover,
                    // Search exposes an episode count, not an authoritative
                    // season count. Details is the source of season metadata.
                    season_count: None,
                })
            })
            .collect()
    }

    pub async fn details(&self, slug: &str) -> Result<MediaDetails, AniZoneError> {
        let slug = validate_slug(slug)?;
        if self.prefer_sub_server.load(Ordering::Relaxed)
            && let Some(sub_base) = &self.sub_server_url
        {
            return self.details_via_sub_server(sub_base, slug).await;
        }
        match self.details_direct(slug).await {
            Ok(details) => Ok(details),
            Err(err) if self.should_fallback_to_sub_server(&err) => {
                if let Some(sub_base) = &self.sub_server_url {
                    log::warn!(
                        "anizone direct details blocked ({err}); falling back to sub-server"
                    );
                    self.prefer_sub_server.store(true, Ordering::Relaxed);
                    return self.details_via_sub_server(sub_base, slug).await;
                }
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn details_direct(&self, slug: &str) -> Result<MediaDetails, AniZoneError> {
        let data = self.scrape_series(slug).await?;
        let title = data.series_title.unwrap_or_else(|| slug.replace('-', " "));
        let episodes = parse_episodes(&data.items);
        let season_number = episodes
            .iter()
            .filter_map(|episode| episode.season)
            .min()
            .unwrap_or(1);
        Ok(MediaDetails {
            id: ProviderMediaId {
                provider: ProviderKind::AniZone,
                value: slug.to_string(),
            },
            title,
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
            genres: Vec::new(),
            seasons: vec![Season {
                number: season_number,
                episodes: episodes
                    .into_iter()
                    .map(|episode| Episode {
                        season: episode.season.unwrap_or(season_number),
                        number: episode.number,
                        title: episode.title,
                        overview: episode.description,
                        provider_episode_id: Some(episode.source_number.to_string()),
                        provider_season_id: episode.season.map(|season| season.to_string()),
                    })
                    .collect(),
                provider_season_id: Some(season_number.to_string()),
            }],
            dubs: Vec::new(),
        })
    }

    async fn scrape_series(&self, slug: &str) -> Result<EpisodePage, AniZoneError> {
        let (html, cookies) = self.fetch_page(&format!("/anime/{slug}")).await?;
        let mut state = initial_page(&html, &cookies)?;
        let mut seen_cursors = HashSet::new();
        let mut page_count = 1usize;
        while state.has_more && state.cursor.is_some() && page_count < MAX_PAGES {
            let cursor = state.cursor.clone().unwrap_or_default();
            if !seen_cursors.insert(cursor) {
                return Err(AniZoneError::Parse(
                    "repeated episode pagination cursor".into(),
                ));
            }
            let previous_items = state.items.clone();
            let mut next = self.load_page(&format!("/anime/{slug}"), state).await?;
            next.items = merge_items(previous_items, next.items);
            state = next;
            page_count += 1;
        }
        if state.has_more {
            return Err(AniZoneError::Parse(
                "episode pagination exceeded request limit".into(),
            ));
        }
        if state.items.is_empty() {
            return Err(AniZoneError::NotFound);
        }
        Ok(state)
    }

    async fn load_page(
        &self,
        referer_path: &str,
        state: EpisodePage,
    ) -> Result<EpisodePage, AniZoneError> {
        let cursor = state
            .cursor
            .clone()
            .ok_or_else(|| AniZoneError::Parse("missing pagination cursor".into()))?;
        let body = serde_json::json!({"components":[{"snapshot":state.snapshot,"updates":{},"calls":[{"path":"","method":"loadPage","params":[cursor]}]}]});
        let response = self
            .client
            .post(self.url("/livewire/update"))
            .header("User-Agent", DEFAULT_UA)
            .header("Accept", "application/json, text/plain, */*")
            .header("Content-Type", "application/json")
            .header("X-Livewire", "")
            .header("X-CSRF-TOKEN", &state.csrf)
            .header("X-Requested-With", "XMLHttpRequest")
            .header("Origin", BASE_URL)
            .header("Referer", format!("{BASE_URL}{referer_path}"))
            .header("Cookie", cookie_header(&state.cookies))
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        let response_cookies = response
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok().map(ToOwned::to_owned))
            .collect::<Vec<_>>();
        let bytes = response.bytes().await?;
        if bytes.len() > MAX_HTML_BYTES {
            return Err(AniZoneError::Parse(
                "pagination response exceeded size limit".into(),
            ));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| AniZoneError::Parse("invalid Livewire JSON".into()))?;
        let component = value
            .get("components")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .ok_or_else(|| AniZoneError::Parse("missing Livewire component".into()))?;
        let snapshot = component
            .get("snapshot")
            .and_then(Value::as_str)
            .ok_or_else(|| AniZoneError::Parse("missing Livewire snapshot".into()))?
            .to_string();
        let dispatches = component
            .pointer("/effects/dispatches")
            .and_then(Value::as_array)
            .ok_or_else(|| AniZoneError::Parse("missing Livewire dispatch".into()))?;
        let dispatch = dispatches
            .iter()
            .find(|item| item.get("name").and_then(Value::as_str) == Some("items-loaded"))
            .ok_or_else(|| AniZoneError::Parse("missing items-loaded event".into()))?;
        let params = dispatch
            .get("params")
            .ok_or_else(|| AniZoneError::Parse("missing pagination params".into()))?;
        let items = params
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| AniZoneError::Parse("missing pagination items".into()))?;
        let cookies = merge_cookies(state.cookies, response_cookies.iter().map(String::as_str));
        Ok(EpisodePage {
            items,
            snapshot,
            cursor: params
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_string),
            has_more: params
                .get("hasMore")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            csrf: state.csrf,
            cookies,
            series_title: state.series_title,
        })
    }

    pub async fn releases(
        &self,
        slug: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, AniZoneError> {
        let (season, episode) = if season == 0 && episode == 0 {
            (1, 1)
        } else {
            (season, episode)
        };
        if season != 1 {
            return Err(AniZoneError::Parse(
                "AniZone does not expose an explicit multi-season mapping".into(),
            ));
        }
        let slug = validate_slug(slug)?;
        if self.prefer_sub_server.load(Ordering::Relaxed)
            && let Some(sub_base) = &self.sub_server_url
        {
            return self
                .releases_via_sub_server(sub_base, slug, season, episode)
                .await;
        }
        match self.releases_direct(slug, season, episode).await {
            Ok(releases) => Ok(releases),
            Err(err) if self.should_fallback_to_sub_server(&err) => {
                if let Some(sub_base) = &self.sub_server_url {
                    log::warn!(
                        "anizone direct releases blocked ({err}); falling back to sub-server"
                    );
                    self.prefer_sub_server.store(true, Ordering::Relaxed);
                    return self
                        .releases_via_sub_server(sub_base, slug, season, episode)
                        .await;
                }
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    async fn releases_direct(
        &self,
        slug: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, AniZoneError> {
        let data = self.scrape_series(slug).await?;
        let source_number = parse_episodes(&data.items)
            .into_iter()
            .find(|item| item.number == episode)
            .map(|item| item.source_number)
            .ok_or(AniZoneError::NotFound)?;
        let watch = self.scrape_watch(slug, source_number).await?;
        let mut headers = vec![
            (
                "Referer".to_string(),
                format!("{BASE_URL}/anime/{slug}/{source_number}"),
            ),
            ("User-Agent".to_string(), DEFAULT_UA.to_string()),
        ];
        if !watch.subtitles.is_empty() {
            headers.push((
                "X-AniZone-Subtitle-Count".into(),
                watch.subtitles.len().to_string(),
            ));
        }
        Ok(vec![Release {
            provider: ProviderKind::AniZone,
            filename: format!("{slug} Episode {episode}"),
            quality: None,
            codec: None,
            // Preserve AniZone's provider-declared English catalog language metadata
            // (remains unverified until independently confirmed).
            language: Some("English".to_string()),
            size_bytes: None,
            season: Some(season),
            episode: Some(episode),
            mirrors: vec![SourceMirror {
                label: "AniZone HLS".into(),
                resolver_url: watch.hls,
                headers,
                direct_file: false,
            }],
            resource_id: Some(source_number.to_string()),
        }])
    }

    pub async fn resolve_release(&self, release: &Release) -> Result<PlaybackSource, AniZoneError> {
        let slug = release
            .filename
            .split(" Episode ")
            .next()
            .unwrap_or_default();
        let source_number = release
            .resource_id
            .as_deref()
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| AniZoneError::Parse("missing AniZone episode identity".into()))?;
        let slug = validate_slug(slug)?;
        if let Some(mirror) = release.mirrors.first()
            && mirror.resolver_url.starts_with("https://")
        {
            let subtitle = mirror
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("X-AniZone-Subtitle-Url"))
                .map(|(_, v)| v.clone());
            let headers = mirror
                .headers
                .iter()
                .filter(|(k, _)| !k.to_ascii_lowercase().starts_with("x-anizone-"))
                .cloned()
                .collect();
            return Ok(PlaybackSource {
                provider: ProviderKind::AniZone,
                url: mirror.resolver_url.clone(),
                headers,
                subtitle,
                source_label: "AniZone HLS".into(),
                max_height: None,
            });
        }
        let watch = self.scrape_watch(slug, source_number).await?;
        let subtitle = watch
            .subtitles
            .iter()
            .max_by_key(|item| {
                (
                    item.is_default,
                    !item.language.is_empty(),
                    !item.label.is_empty(),
                    item.format.eq_ignore_ascii_case("vtt"),
                )
            })
            .map(|item| item.url.clone());
        Ok(PlaybackSource {
            provider: ProviderKind::AniZone,
            url: watch.hls,
            headers: vec![
                (
                    "Referer".into(),
                    format!("{BASE_URL}/anime/{slug}/{source_number}"),
                ),
                ("User-Agent".into(), DEFAULT_UA.into()),
            ],
            subtitle,
            source_label: "AniZone HLS".into(),
            max_height: None,
        })
    }

    async fn scrape_watch(&self, slug: &str, episode: usize) -> Result<WatchData, AniZoneError> {
        let (html, _) = self.fetch_page(&format!("/anime/{slug}/{episode}")).await?;
        let player = json_call_argument(&html, "vidstackPlayer")
            .ok_or_else(|| AniZoneError::Parse("player payload not found".into()))?;
        let hls = normalize_url(
            player
                .get("src")
                .and_then(Value::as_str)
                .ok_or_else(|| AniZoneError::Parse("player HLS source missing".into()))?,
        )?;
        let subtitles = player
            .get("subtitles")
            .and_then(Value::as_array)
            .unwrap_or(&Vec::new())
            .iter()
            .filter_map(|item| {
                let url = normalize_url(item.get("file")?.as_str()?).ok()?;
                Some(SubtitleDescriptor {
                    url,
                    label: item
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    language: item
                        .get("language")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                    format: item
                        .get("format")
                        .and_then(Value::as_str)
                        .unwrap_or("vtt")
                        .to_string(),
                    is_default: item
                        .get("default")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
            })
            .collect();
        Ok(WatchData { hls, subtitles })
    }
}

impl Provider for AniZoneClient {
    fn id(&self) -> ProviderKind {
        ProviderKind::AniZone
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_search: true,
            supports_pagination: true,
            supports_series: true,
            supports_subtitles: true,
            supports_homepage: false,
        }
    }
    async fn search(&self, query: &str, page: usize) -> Result<Vec<CatalogItem>, ProviderError> {
        self.search(query, page).await.map_err(Into::into)
    }
    async fn details(&self, id: &str) -> Result<MediaDetails, ProviderError> {
        self.details(id).await.map_err(Into::into)
    }
}

impl ReleaseProvider for AniZoneClient {
    async fn episode_streams(
        &self,
        id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        self.releases(id, season, episode).await.map_err(Into::into)
    }
}

#[derive(Debug)]
struct SearchItem {
    slug: String,
    title: String,
    kind: String,
    year: Option<i64>,
    cover: Option<String>,
    #[allow(dead_code)]
    episode_count: Option<usize>,
}
#[derive(Debug)]
struct ParsedEpisode {
    season: Option<usize>,
    number: usize,
    source_number: usize,
    title: Option<String>,
    description: Option<String>,
}

fn validate_slug(slug: &str) -> Result<&str, AniZoneError> {
    if !slug.is_empty()
        && slug.len() <= 180
        && slug
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        Ok(slug)
    } else {
        Err(AniZoneError::Parse("invalid AniZone slug".into()))
    }
}
fn normalize_url(value: &str) -> Result<String, AniZoneError> {
    let unescaped = value.replace("\\/", "/");
    let trimmed = unescaped.trim();
    let _ = crate::net::validate_url_syntax(trimmed, false).map_err(AniZoneError::Parse)?;
    Ok(trimmed.to_string())
}
#[cfg(test)]
fn parse_search_items(html: &str) -> Result<Vec<SearchItem>, AniZoneError> {
    let value = json_argument(html, "items")
        .ok_or_else(|| AniZoneError::Parse("search items payload not found".into()))?;
    let items = value
        .as_array()
        .ok_or_else(|| AniZoneError::Parse("search items payload is not an array".into()))?;
    parse_search_items_from_values(items)
}
fn parse_search_items_from_values(items: &[Value]) -> Result<Vec<SearchItem>, AniZoneError> {
    Ok(items
        .iter()
        .filter_map(|item| {
            let slug = item.get("slug")?.as_str()?.to_string();
            if !slug
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            {
                return None;
            }
            let title = pick_title(item.get("title_list")).or_else(|| {
                item.get("main_title")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })?;
            Some(SearchItem {
                slug,
                title,
                kind: item
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
                year: item.get("start_year").and_then(Value::as_i64),
                cover: item
                    .get("cover")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                episode_count: item
                    .get("episode_count")
                    .and_then(Value::as_u64)
                    .and_then(|count| usize::try_from(count).ok()),
            })
        })
        .collect())
}
fn pick_title(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_object)
        .and_then(|map| {
            ["1", "5", "8"]
                .iter()
                .find_map(|key| map.get(*key).and_then(Value::as_str))
                .or_else(|| map.values().find_map(Value::as_str))
        })
        .map(ToOwned::to_owned)
}
fn parse_episodes(items: &[Value]) -> Vec<ParsedEpisode> {
    let mut seen = HashSet::new();
    let mut episodes = items
        .iter()
        .filter_map(|item| {
            let source_number = item
                .get("slug")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<usize>().ok())
                .or_else(|| {
                    item.get("url").and_then(Value::as_str).and_then(|value| {
                        value.trim_end_matches('/').rsplit('/').next()?.parse().ok()
                    })
                })?;
            if source_number == 0 || !seen.insert(source_number) {
                return None;
            }
            Some(ParsedEpisode {
                season: parse_season_number(item),
                number: source_number,
                source_number,
                title: pick_title(item.get("title_list"))
                    .or_else(|| Some(format!("Episode {source_number}"))),
                description: item
                    .get("summary")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            })
        })
        .collect::<Vec<_>>();
    episodes.sort_by_key(|item| item.number);
    episodes
}
fn parse_season_number(item: &Value) -> Option<usize> {
    ["season", "season_number", "seasonNumber"]
        .iter()
        .find_map(|key| item.get(*key).and_then(Value::as_u64))
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
}
fn merge_items(existing: Vec<Value>, incoming: Vec<Value>) -> Vec<Value> {
    let mut merged = existing;
    let mut seen = merged
        .iter()
        .filter_map(episode_identity)
        .collect::<HashSet<_>>();
    for item in incoming {
        if episode_identity(&item).is_none_or(|id| seen.insert(id)) {
            merged.push(item);
        }
    }
    merged
}
fn episode_identity(item: &Value) -> Option<String> {
    item.get("slug")
        .and_then(Value::as_str)
        .or_else(|| item.get("url").and_then(Value::as_str))
        .map(ToOwned::to_owned)
}
fn initial_page(html: &str, cookies: &[String]) -> Result<EpisodePage, AniZoneError> {
    let items = json_argument(html, "items")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| AniZoneError::Parse("episode items payload not found".into()))?;
    let items_marker = html
        .find("items: JSON.parse('")
        .ok_or_else(|| AniZoneError::Parse("episode items marker not found".into()))?;
    let snapshot = html_attribute_before(html, items_marker)
        .ok_or_else(|| AniZoneError::Parse("Livewire snapshot not found".into()))?;
    let csrf = html_attribute(html, "csrf-token")
        .ok_or_else(|| AniZoneError::Parse("CSRF token not found".into()))?;
    Ok(EpisodePage {
        items,
        snapshot,
        cursor: js_property_string(html, "nextCursor"),
        has_more: js_property_bool(html, "hasMore").unwrap_or(false),
        csrf,
        cookies: merge_cookies(HashMap::new(), cookies.iter().map(String::as_str)),
        series_title: json_argument(html, "anmTitles").and_then(|value| pick_title(Some(&value))),
    })
}
fn js_property_string(html: &str, name: &str) -> Option<String> {
    let marker = format!("{name}:");
    let start = html.find(&marker)? + marker.len();
    let value = html[start..].trim_start();
    if value.starts_with("null") {
        return None;
    }
    let quote = value.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let end = value[1..].find(quote)? + 1;
    Some(decode_js_string(&value[1..end]))
}
fn js_property_bool(html: &str, name: &str) -> Option<bool> {
    let marker = format!("{name}:");
    let value = html[html.find(&marker)? + marker.len()..].trim_start();
    if value.starts_with("true") {
        Some(true)
    } else if value.starts_with("false") {
        Some(false)
    } else {
        None
    }
}
fn json_argument(html: &str, name: &str) -> Option<Value> {
    let marker = format!("{name}: JSON.parse('");
    let start = html.find(&marker)? + marker.len();
    let mut raw = String::new();
    let mut escaped = false;
    for ch in html[start..].chars() {
        if !escaped && ch == '\'' {
            break;
        }
        if escaped {
            raw.push('\\');
            raw.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            raw.push(ch);
        }
    }
    let decoded = decode_js_string(&raw);
    serde_json::from_str(&decoded).ok()
}
fn json_call_argument(html: &str, name: &str) -> Option<Value> {
    let marker = format!("{name}(JSON.parse('");
    let start = html.find(&marker)? + marker.len();
    let mut raw = String::new();
    let mut escaped = false;
    for ch in html[start..].chars() {
        if !escaped && ch == '\'' {
            break;
        }
        if escaped {
            raw.push('\\');
            raw.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            raw.push(ch);
        }
    }
    serde_json::from_str(&decode_js_string(&raw)).ok()
}
fn decode_js_string(raw: &str) -> String {
    let mut output = String::new();
    let chars: Vec<char> = raw.chars().collect();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != '\\' {
            output.push(chars[index]);
            index += 1;
            continue;
        }
        index += 1;
        if index >= chars.len() {
            break;
        }
        match chars[index] {
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'b' => output.push('\u{0008}'),
            'f' => output.push('\u{000c}'),
            'u' if index + 4 < chars.len() => {
                let hex: String = chars[index + 1..index + 5].iter().collect();
                if let Ok(value) = u32::from_str_radix(&hex, 16) {
                    if let Some(code) = char::from_u32(value) {
                        output.push(code);
                        index += 4;
                    }
                }
            }
            other => output.push(other),
        }
        index += 1;
    }
    output
}
fn html_attribute(html: &str, name: &str) -> Option<String> {
    let marker = if name == "csrf-token" {
        "content=\""
    } else {
        "wire:snapshot=\""
    };
    let start = if name == "csrf-token" {
        html.find("name=\"csrf-token\"")?
            .checked_add(html[html.find("name=\"csrf-token\"")?..].find(marker)? + marker.len())?
    } else {
        html.find(marker)?.checked_add(marker.len())?
    };
    let end = html[start..].find('"')?;
    Some(html_decode(&html[start..start + end]))
}
fn html_attribute_before(html: &str, before: usize) -> Option<String> {
    let prefix = &html[..before];
    let marker = "wire:snapshot=\"";
    let start = prefix.rfind(marker)? + marker.len();
    let end = prefix[start..].find('"')?;
    Some(html_decode(&prefix[start..start + end]))
}
fn html_decode(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}
fn merge_cookies(
    mut jar: HashMap<String, String>,
    values: impl IntoIterator<Item = impl AsRef<str>>,
) -> HashMap<String, String> {
    for value in values {
        if let Some((name, rest)) = value.as_ref().split_once('=') {
            jar.insert(
                name.trim().to_string(),
                rest.split(';').next().unwrap_or_default().to_string(),
            );
        }
    }
    jar
}
fn cookie_header(cookies: &HashMap<String, String>) -> String {
    cookies
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decodes_search_json_argument_and_filters_bad_slugs() {
        let html = r#"items: JSON.parse('[{\"slug\":\"naruto\",\"main_title\":\"Naruto\",\"start_year\":2002},{\"slug\":\"bad/slug\",\"main_title\":\"Bad\"}]')"#;
        let items = parse_search_items(html).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].slug, "naruto");
    }
    #[test]
    fn deduplicates_and_sorts_episode_items() {
        let items = vec![
            serde_json::json!({"slug":"2","title_list":{"1":"Two"}}),
            serde_json::json!({"slug":"1","title_list":{"1":"One"}}),
            serde_json::json!({"slug":"2"}),
        ];
        let parsed = parse_episodes(&items);
        assert_eq!(
            parsed.iter().map(|item| item.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
    #[test]
    fn rejects_private_or_non_https_media_urls() {
        assert!(normalize_url("http://127.0.0.1/a").is_err());
        assert!(normalize_url("https://example.com/a.m3u8").is_ok());
    }
    #[test]
    fn decodes_unicode_js_escapes() {
        assert_eq!(
            decode_js_string(r#"Naruto \u30ca\u30eb\u30c8"#),
            "Naruto ナルト"
        );
    }

    #[test]
    fn parses_vidstack_player_call_payload() {
        let html =
            r#"vidstackPlayer(JSON.parse('{\"src\":\"https:\\/\\/example.com\\/episode.m3u8\"}'))"#;
        let player = json_call_argument(html, "vidstackPlayer").unwrap();
        assert_eq!(
            player["src"].as_str(),
            Some("https://example.com/episode.m3u8")
        );
    }

    #[test]
    fn parses_series_title_and_livewire_pagination_state() {
        let html = r#"<meta name="csrf-token" content="csrf"><section wire:snapshot="{&quot;component&quot;:1}" x-data="{ anmTitles: JSON.parse('{\u00225\u0022:\u0022Black Clover (2017)\u0022}'), items: JSON.parse('[{\u0022slug\u0022:\u00221\u0022}]'), nextCursor: 'cursor-2', hasMore: true }">"#;
        let page = initial_page(html, &[]).unwrap();
        assert_eq!(page.series_title.as_deref(), Some("Black Clover (2017)"));
        assert_eq!(page.cursor.as_deref(), Some("cursor-2"));
        assert!(page.has_more);
        assert_eq!(page.snapshot, "{\"component\":1}");
    }

    #[test]
    fn episode_page_accumulation_deduplicates_by_provider_identity() {
        let merged = merge_items(
            vec![serde_json::json!({"slug":"1","title":"first"})],
            vec![
                serde_json::json!({"slug":"1","title":"duplicate"}),
                serde_json::json!({"slug":"2","title":"second"}),
            ],
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0]["title"], "first");
        assert_eq!(merged[1]["slug"], "2");
    }

    #[test]
    fn search_parser_preserves_cover_and_episode_count() {
        let html = r#"items: JSON.parse('[{\"slug\":\"black-clover\",\"main_title\":\"Black Clover\",\"type\":\"TV Series\",\"start_year\":2017,\"cover\":\"https:\\/\\/example.com\\/cover.jpg\",\"episode_count\":170}]')"#;
        let item = parse_search_items(html).unwrap().remove(0);
        assert_eq!(item.cover.as_deref(), Some("https://example.com/cover.jpg"));
        assert_eq!(item.episode_count, Some(170));
    }

    #[tokio::test]
    async fn sub_server_enumerates_multiple_sources_and_preserves_english_and_http_errors() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            for i in 0..2 {
                if let Ok((mut sock, _)) = listener.accept().await {
                    let mut req_buf = [0u8; 1024];
                    let _ = sock.read(&mut req_buf).await;
                    let resp = if i == 0 {
                        let body = serde_json::json!({
                            "status": "success",
                            "data": {
                                "source_id": "az-ep1",
                                "sources": [
                                    {
                                        "url": "https://cdn1.example.com/1080.m3u8",
                                        "quality": "1080p",
                                        "codec": "h264",
                                        "headers": {"Referer": "https://anizone.to/"}
                                    },
                                    {
                                        "url": "https://cdn2.example.com/720.m3u8",
                                        "quality": "720p",
                                        "codec": "hevc",
                                        "language": "English"
                                    }
                                ],
                                "subtitles": [
                                    {"url": "https://cdn.example.com/en.vtt", "language": "en", "format": "vtt", "default": true}
                                ]
                            }
                        })
                        .to_string();
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    } else {
                        let body = "<html><body>502 Bad Gateway</body></html>";
                        format!(
                            "HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                }
            }
        });

        let client = AniZoneClient::new();
        let sub_base = Url::parse(&format!("http://{addr}/")).unwrap();
        let releases = client
            .releases_via_sub_server(&sub_base, "black-clover", 1, 1)
            .await
            .unwrap();
        assert_eq!(releases.len(), 2);
        assert_eq!(releases[0].language.as_deref(), Some("English"));
        assert_eq!(releases[0].quality.as_deref(), Some("1080p"));
        assert_eq!(releases[1].language.as_deref(), Some("English"));
        assert_eq!(releases[1].quality.as_deref(), Some("720p"));
        assert_eq!(releases[1].codec.as_deref(), Some("hevc"));
        assert_eq!(
            releases[0].mirrors[0].resolver_url,
            "https://cdn1.example.com/1080.m3u8"
        );
        assert_eq!(
            releases[1].mirrors[0].resolver_url,
            "https://cdn2.example.com/720.m3u8"
        );

        let err = client
            .releases_via_sub_server(&sub_base, "black-clover", 1, 2)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("502"));
    }
}
