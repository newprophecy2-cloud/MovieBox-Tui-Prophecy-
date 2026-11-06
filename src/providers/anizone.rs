use crate::providers::models::{
    CatalogItem, Episode, MediaDetails, MediaType, PlaybackSource, ProviderError, ProviderKind,
    ProviderMediaId, Release, Season, SourceMirror,
};
use crate::providers::{Provider, ProviderCapabilities, ReleaseProvider};
use reqwest::{Client, Url};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const BASE_URL: &str = "https://anizone.to";
const DEFAULT_UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
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
        Self::with_base_url(BASE_URL).expect("static AniZone URL must parse")
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
        Ok(Self { client, base_url })
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
        let mut request = self
            .client
            .get(url)
            .header("User-Agent", DEFAULT_UA)
            .header("Accept-Language", "en-US,en;q=0.9")
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            );
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let response = request.send().await?.error_for_status()?;
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
        Ok((text, cookies))
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
                    season_count: item.episode_count,
                })
            })
            .collect()
    }

    pub async fn details(&self, slug: &str) -> Result<MediaDetails, AniZoneError> {
        let slug = validate_slug(slug)?;
        let data = self.scrape_series(slug).await?;
        let title = data.series_title.unwrap_or_else(|| slug.replace('-', " "));
        let episodes = parse_episodes(&data.items);
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
                number: 1,
                episodes: episodes
                    .into_iter()
                    .map(|episode| Episode {
                        season: 1,
                        number: episode.number,
                        title: episode.title,
                        overview: episode.description,
                    })
                    .collect(),
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
        if season != 1 {
            return Err(AniZoneError::Parse(
                "AniZone does not expose an explicit multi-season mapping".into(),
            ));
        }
        let slug = validate_slug(slug)?;
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
            language: None,
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
        let watch = self
            .scrape_watch(validate_slug(slug)?, source_number)
            .await?;
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
    episode_count: Option<usize>,
}
#[derive(Debug)]
struct ParsedEpisode {
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
    let value = value.replace("\\/", "/");
    let url = crate::net::validate_url_syntax(&value, false).map_err(AniZoneError::Parse)?;
    Ok(url.to_string())
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
}
