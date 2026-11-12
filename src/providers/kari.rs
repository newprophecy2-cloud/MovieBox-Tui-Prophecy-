use crate::providers::models::{
    CatalogItem, MediaDetails, MediaType, ProviderError, ProviderKind, ProviderMediaId, Release,
    Season, SourceMirror,
};
use crate::providers::{Provider, ProviderCapabilities, ReleaseProvider};
use reqwest::header::{ACCEPT, REFERER, USER_AGENT};
use serde_json::Value;
use std::time::Duration;

fn encode_component(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AniPmNativeId {
    source: String,
    id: i64,
    slug: String,
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

    async fn anipm_json(&self, path: &str) -> Result<Value, ProviderError> {
        let url = format!("{}{}", self.base(), path);
        let response = self
            .http
            .get(&url)
            .header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
            .header(REFERER, "https://ani.pm/")
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let status = response.status();
        if status.as_u16() == 429 {
            let retry = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            return Err(ProviderError::RateLimited(retry));
        }
        if status.as_u16() == 404 {
            return Err(ProviderError::NotFound);
        }
        if !status.is_success() {
            return Err(ProviderError::Unavailable(format!(
                "Ani.pm HTTP status {}",
                status.as_u16()
            )));
        }
        response
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(format!("Ani.pm JSON: {e}")))
    }

    async fn search_anipm(&self, query: &str) -> Result<Vec<CatalogItem>, ProviderError> {
        let mut url = reqwest::Url::parse(&format!("{}/api/anime/search", self.base()))
            .map_err(|e| ProviderError::Parsing(e.to_string()))?;
        url.query_pairs_mut().append_pair("q", query.trim());
        let response = self
            .http
            .get(url)
            .header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
            .header(REFERER, "https://ani.pm/")
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let status = response.status();
        if status.as_u16() == 429 {
            let retry = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            return Err(ProviderError::RateLimited(retry));
        }
        if !status.is_success() {
            return Err(ProviderError::Unavailable(format!(
                "Ani.pm search HTTP status {}",
                status.as_u16()
            )));
        }
        let value: Value = response
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(format!("Ani.pm search JSON: {e}")))?;
        let rows = value
            .get("items")
            .and_then(Value::as_array)
            .ok_or_else(|| ProviderError::Parsing("Ani.pm search has no items array".into()))?;
        let mut output = Vec::new();
        for row in rows {
            let source = row
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            // External-catalog records lack a provider-native ID; native-only mode
            // accepts only records the provider exposes through its Settlar catalog.
            if !source.eq_ignore_ascii_case("settlar") {
                continue;
            }
            let Some(native_id) = row
                .get("id")
                .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
            else {
                continue;
            };
            let title = row
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let slug = row.get("slug").and_then(Value::as_str).unwrap_or("").trim();
            if native_id <= 0 || title.is_empty() || slug.is_empty() {
                continue;
            }
            let identity = AniPmNativeId {
                source: source.to_ascii_lowercase(),
                id: native_id,
                slug: slug.to_owned(),
            };
            let value = serde_json::to_string(&identity)
                .map_err(|e| ProviderError::Parsing(e.to_string()))?;
            let poster = row
                .get("poster")
                .and_then(Value::as_str)
                .and_then(|raw| reqwest::Url::parse(self.base()).ok()?.join(raw).ok())
                .map(|u| u.to_string());
            output.push(CatalogItem {
                id: ProviderMediaId {
                    provider: self.kind,
                    value,
                },
                title: title.to_owned(),
                media_type: if row
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|v| v.eq_ignore_ascii_case("movie"))
                {
                    MediaType::Movie
                } else {
                    MediaType::Series
                },
                year: row
                    .get("year")
                    .and_then(Value::as_i64)
                    .map(|n| n.to_string()),
                poster_url: poster,
                season_count: Some(1),
            });
        }
        if output.is_empty() {
            Err(ProviderError::NotFound)
        } else {
            Ok(output)
        }
    }

    async fn details_anipm(&self, raw_id: &str) -> Result<MediaDetails, ProviderError> {
        let identity: AniPmNativeId = serde_json::from_str(raw_id)
            .map_err(|_| ProviderError::Parsing("Ani.pm native identity is invalid".into()))?;
        if identity.source != "settlar" || identity.id <= 0 || identity.slug.is_empty() {
            return Err(ProviderError::Parsing(
                "Ani.pm identity is not a supported native Settlar resource".into(),
            ));
        }
        let mut value = match self
            .anipm_json(&format!("/api/anime/series/{}", identity.id))
            .await
        {
            Ok(v) => v,
            Err(ProviderError::NotFound) => {
                self.anipm_json(&format!("/api/anime/ani/{}", identity.id))
                    .await?
            }
            Err(e) => return Err(e),
        };
        if value.get("id").and_then(Value::as_i64) != Some(identity.id) {
            return Err(ProviderError::Parsing(
                "Ani.pm detail identity mismatch".into(),
            ));
        }
        let title = value
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        if title.is_empty() {
            return Err(ProviderError::Parsing("Ani.pm detail has no title".into()));
        }
        let mut episodes = Vec::new();
        let mut dub_count = 0usize;
        for row in value
            .get_mut("episodes")
            .and_then(Value::as_array_mut)
            .into_iter()
            .flatten()
        {
            if row.get("sub").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let Some(number) = row
                .get("number")
                .and_then(Value::as_u64)
                .map(|n| n as usize)
                .filter(|n| *n > 0)
            else {
                continue;
            };
            if row.get("dub").and_then(Value::as_bool) == Some(true) {
                dub_count += 1;
            }
            episodes.push(crate::providers::models::Episode {
                season: 1,
                number,
                title: row.get("title").and_then(Value::as_str).map(str::to_owned),
                overview: row
                    .get("description")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            });
        }
        episodes.sort_by_key(|e| e.number);
        episodes.dedup_by_key(|e| e.number);
        let poster = value
            .get("poster")
            .and_then(Value::as_str)
            .and_then(|raw| reqwest::Url::parse(self.base()).ok()?.join(raw).ok())
            .map(|u| u.to_string());
        let genres = value
            .get("genres")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        Ok(MediaDetails {
            id: ProviderMediaId {
                provider: self.kind,
                value: raw_id.to_owned(),
            },
            title,
            media_type: if value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|v| v.eq_ignore_ascii_case("movie"))
            {
                MediaType::Movie
            } else {
                MediaType::Series
            },
            year: value
                .get("year")
                .and_then(Value::as_i64)
                .map(|n| n.to_string()),
            description: value
                .get("synopsis")
                .and_then(Value::as_str)
                .map(str::to_owned),
            tagline: None,
            imdb_rating: None,
            director: None,
            stars: None,
            prints: None,
            audios: Some(if dub_count > 0 {
                "sub; dub availability is not exposed by this API contract".into()
            } else {
                "sub".into()
            }),
            poster_url: poster,
            duration: value
                .get("duration")
                .and_then(Value::as_str)
                .map(str::to_owned),
            genres,
            seasons: vec![Season {
                number: 1,
                episodes,
            }],
            dubs: vec![],
        })
    }

    async fn resolve_anipm(
        &self,
        id: &str,
        _season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        let identity: AniPmNativeId = serde_json::from_str(id)
            .map_err(|_| ProviderError::Parsing("Ani.pm native identity is invalid".into()))?;
        if identity.source != "settlar" || identity.id <= 0 {
            return Err(ProviderError::Parsing(
                "Ani.pm playback requires a native Settlar ID".into(),
            ));
        }
        let client = &self.http;
        let headers = |b: reqwest::RequestBuilder| {
            b.header(USER_AGENT, crate::net::DEFAULT_BROWSER_USER_AGENT)
                .header(REFERER, "https://ani.pm/")
                .header(ACCEPT, "application/json")
        };
        let boot_url = format!(
            "{}/api/anime/playback-bootstrap/settlar/{}?ep={}&lang=sub",
            self.base(),
            identity.id,
            episode
        );
        let boot = headers(client.get(boot_url))
            .send()
            .await
            .map_err(|e| ProviderError::Network(e.to_string()))?;
        let status = boot.status();
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimited(None));
        }
        if status.as_u16() == 404 {
            return Err(ProviderError::NotFound);
        }
        if !status.is_success() {
            return Err(ProviderError::Unavailable(format!(
                "Ani.pm playback bootstrap HTTP {}",
                status.as_u16()
            )));
        }
        let boot: Value = boot
            .json()
            .await
            .map_err(|e| ProviderError::Parsing(format!("Ani.pm bootstrap JSON: {e}")))?;
        if boot.get("source").and_then(Value::as_str) != Some("settlar")
            || boot.get("id").and_then(Value::as_i64) != Some(identity.id)
            || boot.get("episode").and_then(Value::as_u64) != Some(episode as u64)
            || boot.get("effectiveLanguage").and_then(Value::as_str) != Some("sub")
        {
            return Err(ProviderError::Parsing(
                "Ani.pm native playback response identity/audio mismatch".into(),
            ));
        }
        let selection = boot
            .get("settlarSelection")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                ProviderError::Unavailable(
                    "Ani.pm has no native subtitle selection for this episode".into(),
                )
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
            ProviderKind::Anipm => self.search_anipm(query).await,
            ProviderKind::Anikoto => crate::providers::kari_anime::anikoto::search(&self.http, query).await,
            ProviderKind::Miruro => crate::providers::kari_anime::miruro::search(&self.http, query).await,
            ProviderKind::Anidream => Err(ProviderError::Unavailable("AniDream is blocked by the native-only identity requirement: original Kari maps the browse ID to an external catalog ID and its playback contract has no verified native identity".into())),
            ProviderKind::Anivault => Err(ProviderError::Unavailable("AniVault native search is blocked: its documented /api/search endpoint returned HTTP 404 and no provider-native episode identity has been verified".into())),
            ProviderKind::Anicine => Err(ProviderError::Unavailable("AniCine native search is blocked: no verified provider-native catalog endpoint has been identified".into())),
            _ => Err(ProviderError::Unavailable("invalid Kari provider".into())),
        }
    }
    async fn details(&self, id: &str) -> Result<MediaDetails, ProviderError> {
        match self.kind {
            ProviderKind::Anipm => self.details_anipm(id).await,
            ProviderKind::Anikoto => crate::providers::kari_anime::anikoto::details(&self.http, id).await,
            ProviderKind::Miruro => crate::providers::kari_anime::miruro::details(&self.http, id).await,
            ProviderKind::Anidream => Err(ProviderError::Unavailable("AniDream native details are blocked because the verified upstream workflow requires an external catalog-scoped ID".into())),
            ProviderKind::Anivault => Err(ProviderError::Unavailable("AniVault native details are blocked: no native search/detail identity is available from the verified upstream API".into())),
            ProviderKind::Anicine => Err(ProviderError::Unavailable("AniCine native details are blocked: no provider-native anime catalog route has been verified".into())),
            _ => Err(ProviderError::Unavailable("invalid Kari provider".into())),
        }
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
            ProviderKind::Anivault => Err(ProviderError::Unavailable("AniVault playback is blocked until a verified native ID path is available".into())),
            ProviderKind::Anipm => self.resolve_anipm(id, season, episode).await,
            ProviderKind::Anicine => Err(ProviderError::Unavailable("AniCine playback is blocked until its external-catalog mapping can be replaced with a verified native identity".into())),
            ProviderKind::Anikoto => {
                crate::providers::kari_anime::anikoto::resolve(&self.http, id, season, episode)
                    .await
            }
            ProviderKind::Miruro => {
                crate::providers::kari_anime::miruro::resolve(&self.http, id, season, episode).await
            }
ProviderKind::Anidream => Err(ProviderError::Unavailable("AniDream playback is blocked because its verified server function requires an external catalog-scoped ID".into())),
            _ => Err(ProviderError::Unavailable("invalid Kari provider".into())),
        }
    }
}
