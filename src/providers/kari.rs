use crate::providers::models::{
    CatalogItem, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId, Release,
    Season, SourceMirror,
};
use crate::providers::{Provider, ProviderCapabilities, ReleaseProvider};
use reqwest::header::{ACCEPT, REFERER, USER_AGENT};
use serde_json::{Value, json};
use std::time::Duration;

const ANILIST_URL: &str = "https://graphql.anilist.co";
const ANILIST_QUERY: &str = r#"query ($search: String, $id: Int) {
  Page(page: 1, perPage: 20) { media(search: $search, type: ANIME) {
    id idMal format episodes seasonYear title { english romaji native }
    description(asHtml: false) coverImage { large } genres
  }}
  Media(id: $id, type: ANIME) { id idMal format episodes seasonYear title { english romaji native }
    description(asHtml: false) coverImage { large } genres }
}"#;

fn encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[derive(Clone)]
pub struct KariClient {
    pub kind: ProviderKind,
    http: reqwest::Client,
}

impl KariClient {
    pub fn new(kind: ProviderKind) -> Self {
        let http = crate::net::http_client_builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { kind, http }
    }

    fn base(&self) -> &'static str {
        match self.kind {
            ProviderKind::Anivault => "https://anivault-api-production.up.railway.app",
            ProviderKind::Anipm => "https://ani.pm",
            ProviderKind::Anicine => "https://megaplay.buzz",
            ProviderKind::Anikoto => "https://anikototv.to",
            ProviderKind::Miruro => "https://miruro.cx",
            ProviderKind::Anidream => "https://anidream.to",
            _ => "",
        }
    }

    fn provider_name(&self) -> &'static str {
        self.kind.cache_key()
    }

    async fn anilist(&self, search: Option<&str>, id: Option<i64>) -> Result<Value, ProviderError> {
        let body = json!({
            "query": ANILIST_QUERY,
            "variables": {"search": search, "id": id},
        });
        let response = self
            .http
            .post(ANILIST_URL)
            .header(ACCEPT, "application/json")
            .header(USER_AGENT, crate::net::APP_HTTP_USER_AGENT)
            .json(&body)
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        if !status.is_success() {
            return Err(ProviderError::Unavailable(format!(
                "HTTP status {}",
                status.as_u16()
            )));
        }
        if value.get("errors").is_some() {
            return Err(ProviderError::Unavailable(
                "AniList returned an error".into(),
            ));
        }
        Ok(value)
    }

    fn title(media: &Value) -> String {
        ["english", "romaji", "native"]
            .iter()
            .find_map(|key| {
                media
                    .get("title")?
                    .get(*key)?
                    .as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
            })
            .unwrap_or_default()
    }

    fn media_to_catalog(&self, media: &Value) -> Option<CatalogItem> {
        let id = media.get("id")?.as_i64()?.to_string();
        let title = Self::title(media);
        if title.is_empty() {
            return None;
        }
        let media_type = if media
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|v| v.eq_ignore_ascii_case("MOVIE"))
        {
            MediaType::Movie
        } else {
            MediaType::Series
        };
        Some(CatalogItem {
            id: ProviderMediaId {
                provider: self.kind,
                value: id,
            },
            title,
            media_type,
            year: media
                .get("seasonYear")
                .and_then(Value::as_i64)
                .map(|v| v.to_string()),
            poster_url: media
                .get("coverImage")
                .and_then(|v| v.get("large"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            season_count: Some(1),
        })
    }

    fn details_from_media(&self, media: &Value) -> Result<MediaDetails, ProviderError> {
        let item = self.media_to_catalog(media).ok_or_else(|| {
            ProviderError::Parsing("AniList media has no usable title or ID".into())
        })?;
        let count = media
            .get("episodes")
            .and_then(Value::as_i64)
            .unwrap_or(1)
            .max(1) as usize;
        let episodes = (1..=count)
            .flat_map(|episode| {
                [
                    crate::providers::models::Episode {
                        season: 1,
                        number: episode,
                        title: Some(format!("Episode {episode}")),
                        overview: None,
                    },
                    crate::providers::models::Episode {
                        season: 1,
                        number: episode,
                        title: Some(format!("Episode {episode} (Dub)")),
                        overview: None,
                    },
                ]
            })
            .collect();
        Ok(MediaDetails {
            id: item.id,
            title: item.title,
            media_type: item.media_type,
            year: item.year,
            description: media
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            tagline: None,
            imdb_rating: None,
            director: None,
            stars: None,
            prints: None,
            audios: Some("sub,dub".into()),
            poster_url: item.poster_url,
            duration: None,
            genres: media
                .get("genres")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            seasons: vec![Season {
                number: 1,
                episodes,
            }],
            dubs: vec![],
        })
    }

    async fn resolve_anivault(
        &self,
        id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        let url = format!(
            "{}/api/watch/anivault/{}/{}/sub",
            self.base(),
            encode_component(id),
            episode
        );
        let value = self
            .http
            .get(&url)
            .header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?
            .json::<Value>()
            .await
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        self.release_from_watch(value, season, episode).await
    }

    async fn resolve_anipm(
        &self,
        id: &str,
        _season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        let client = &self.http;
        let headers = |b: reqwest::RequestBuilder| {
            b.header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
                .header(REFERER, "https://ani.pm/")
                .header(ACCEPT, "application/json")
        };
        let boot = headers(client.get(format!(
            "{}/api/anime/playback-bootstrap/anilist/{}?ep={}&lang=sub",
            self.base(),
            id,
            episode
        )))
        .send()
        .await
        .map_err(|e| ProviderError::Network(e.to_string()))?;
        let boot: Value = boot
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        let selection = boot
            .get("settlarSelection")
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                ProviderError::Unavailable("Ani.pm did not return a playback selection".into())
            })?;
        let session = headers(client.get(format!("{}/api/anime/settlar/session?selection={}&provider=anipm&ep={}&channel=sub&telemetry=0", self.base(), encode_component(selection), episode))).send().await.map_err(|e| ProviderError::Network(e.to_string()))?;
        let session: Value = session
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        let embed = session
            .get("embedUrl")
            .and_then(Value::as_str)
            .ok_or_else(|| ProviderError::Unavailable("Ani.pm returned no embed URL".into()))?;
        let parsed =
            reqwest::Url::parse(embed).map_err(|e| ProviderError::Parsing(e.to_string()))?;
        let token = parsed
            .query_pairs()
            .find(|(k, _)| k == "t")
            .map(|(_, v)| v.into_owned())
            .ok_or_else(|| {
                ProviderError::Unavailable("Ani.pm embed has no session token".into())
            })?;
        let playback = client
            .get(format!(
                "https://embed.settlar.io/api/embed/session?t={}",
                encode_component(&token)
            ))
            .header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
            .header(REFERER, embed)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let playback: Value = playback
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        let source = playback
            .get("source")
            .and_then(Value::as_str)
            .filter(|v| crate::net::validate_url_syntax(v, false).is_ok())
            .ok_or_else(|| {
                ProviderError::Unavailable("Ani.pm returned no valid media source".into())
            })?;
        Ok(vec![Release {
            provider: self.kind,
            filename: format!("{}-{}.m3u8", id, episode),
            quality: Some("1080p".into()),
            codec: None,
            language: Some("ja".into()),
            size_bytes: None,
            season: Some(1),
            episode: Some(episode),
            mirrors: vec![SourceMirror {
                label: "Ani.pm Settlar".into(),
                resolver_url: source.into(),
                headers: vec![("Referer".into(), "https://embed.settlar.io/".into())],
                direct_file: false,
            }],
            resource_id: None,
            subtitles: Vec::new(),
        }])
    }

    async fn release_from_watch(
        &self,
        value: Value,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        let mut mirrors = Vec::new();
        for (key, label, direct_file) in [
            ("m3u8", "AniVault HLS", false),
            ("mp4", "AniVault MP4", true),
            ("rawStreamUrl", "AniVault direct", false),
        ] {
            if let Some(url) = value
                .get(key)
                .and_then(Value::as_str)
                .filter(|v| crate::net::validate_url_syntax(v, false).is_ok())
            {
                mirrors.push(SourceMirror {
                    label: label.into(),
                    resolver_url: url.into(),
                    headers: vec![("Referer".into(), self.base().to_string())],
                    direct_file,
                });
            }
        }
        if mirrors.is_empty() {
            return Err(ProviderError::Unavailable(
                "Kari provider returned no valid media URL".into(),
            ));
        }
        Ok(vec![Release {
            provider: self.kind,
            filename: format!("{}-{}.media", self.provider_name(), episode),
            quality: Some("Auto".into()),
            codec: None,
            language: Some("ja".into()),
            size_bytes: None,
            season: Some(season),
            episode: Some(episode),
            mirrors,
            resource_id: None,
            subtitles: Vec::new(),
        }])
    }
}

impl Provider for KariClient {
    fn id(&self) -> ProviderKind {
        self.kind
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            supports_search: true,
            supports_pagination: false,
            supports_series: true,
            supports_subtitles: true,
            supports_homepage: false,
        }
    }
    async fn search(&self, query: &str, _page: usize) -> Result<Vec<CatalogItem>, ProviderError> {
        match self.kind {
            ProviderKind::Anikoto => {
                return crate::providers::kari_anime::anikoto::search(&self.http, query).await;
            }
            ProviderKind::Miruro => {
                return crate::providers::kari_anime::miruro::search(&self.http, query).await;
            }
            ProviderKind::Anidream => {
                return crate::providers::kari_anime::anidream::search(&self.http, query).await;
            }
            _ => {}
        }
        let value = self.anilist(Some(query), None).await?;
        let media = value
            .get("data")
            .and_then(|v| v.get("Page"))
            .and_then(|v| v.get("media"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ProviderError::Parsing("AniList search response missing media".into())
            })?;
        let out = media
            .iter()
            .filter_map(|v| self.media_to_catalog(v))
            .collect::<Vec<_>>();
        if out.is_empty() {
            Err(ProviderError::NotFound)
        } else {
            Ok(out)
        }
    }
    async fn details(&self, id: &str) -> Result<MediaDetails, ProviderError> {
        match self.kind {
            ProviderKind::Anikoto => {
                return crate::providers::kari_anime::anikoto::details(&self.http, id).await;
            }
            ProviderKind::Miruro => {
                return crate::providers::kari_anime::miruro::details(&self.http, id).await;
            }
            ProviderKind::Anidream => {
                return crate::providers::kari_anime::anidream::details(&self.http, id).await;
            }
            _ => {}
        }
        let id = id.parse::<i64>().map_err(|_| {
            ProviderError::Parsing("provider ID is not an AniList numeric ID".into())
        })?;
        let value = self.anilist(None, Some(id)).await?;
        let media = value
            .get("data")
            .and_then(|v| v.get("Media"))
            .ok_or(ProviderError::NotFound)?;
        self.details_from_media(media)
    }
}

impl ReleaseProvider for KariClient {
    async fn episode_streams(
        &self,
        id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        if id.trim().is_empty() || season == 0 || episode == 0 {
            return Err(ProviderError::Parsing("invalid episode identity".into()));
        }
        match self.kind {
            ProviderKind::Anivault => self.resolve_anivault(id, season, episode).await,
            ProviderKind::Anipm => self.resolve_anipm(id, season, episode).await,
            ProviderKind::Anicine => {
                crate::providers::kari_anime::anicine::resolve(&self.http, id, season, episode)
                    .await
            }
            ProviderKind::Anikoto => {
                crate::providers::kari_anime::anikoto::resolve(&self.http, id, season, episode)
                    .await
            }
            ProviderKind::Miruro => {
                crate::providers::kari_anime::miruro::resolve(&self.http, id, season, episode).await
            }
            ProviderKind::Anidream => {
                crate::providers::kari_anime::anidream::resolve(&self.http, id, season, episode)
                    .await
            }
            _ => Err(ProviderError::Unavailable("invalid Kari provider".into())),
        }
    }
}
