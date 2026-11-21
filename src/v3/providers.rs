use crate::providers::anizone::AniZoneClient;
use crate::providers::dramachi::DramachiClient;
use crate::providers::fourkhdhub::FourKHdHubClient;
use crate::providers::models::{
    CatalogItem, MediaDetails, MediaType, ProviderError, ProviderKind, Release, ResolutionIntent,
};
use crate::providers::moviebox::client::MovieBoxClient;
use crate::providers::{Provider, ReleaseProvider};
use crate::v3::models::{NormalizedAnime, ProviderFailureInfo};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Debug, Clone)]
pub struct ProviderEntry {
    pub provider_id: ProviderKind,
    pub display_name: &'static str,
    pub enabled: bool,
    pub timeout: Duration,
}

#[derive(Clone)]
pub struct ProviderRegistry {
    pub entries: Vec<ProviderEntry>,
    pub moviebox: MovieBoxClient,
    pub anizone: AniZoneClient,
    pub dramachi: DramachiClient,
    pub fourkhdhub: Option<FourKHdHubClient>,
    pub concurrency: Arc<Semaphore>,
}

fn env_flag(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(val) => {
            let v = val.trim().to_ascii_lowercase();
            if v == "false" || v == "0" || v == "no" || v == "off" {
                false
            } else if v == "true" || v == "1" || v == "yes" || v == "on" {
                true
            } else {
                default
            }
        }
        Err(_) => default,
    }
}

pub fn provider_key(p: ProviderKind) -> &'static str {
    match p {
        ProviderKind::MovieBox => "moviebox",
        ProviderKind::AniZone => "anizone",
        ProviderKind::Dramachi => "dramachi",
        ProviderKind::FourKHdHub => "fourkhdhub",
        _ => "unknown",
    }
}

pub fn provider_display_name(p: ProviderKind) -> &'static str {
    match p {
        ProviderKind::MovieBox => "MovieBox",
        ProviderKind::AniZone => "AniZone",
        ProviderKind::Dramachi => "DramaChi",
        ProviderKind::FourKHdHub => "FourKHDHub",
        _ => "Unknown",
    }
}

impl ProviderRegistry {
    pub fn from_env() -> Self {
        let fourkhdhub = FourKHdHubClient::new().ok();
        let entries = vec![
            ProviderEntry {
                provider_id: ProviderKind::MovieBox,
                display_name: "MovieBox",
                enabled: env_flag("PV_PROVIDER_MOVIEBOX_ENABLED", true),
                timeout: Duration::from_secs(15),
            },
            ProviderEntry {
                provider_id: ProviderKind::AniZone,
                display_name: "AniZone",
                enabled: env_flag("PV_PROVIDER_ANIZONE_ENABLED", true),
                timeout: Duration::from_secs(20),
            },
            ProviderEntry {
                provider_id: ProviderKind::Dramachi,
                display_name: "DramaChi",
                enabled: env_flag("PV_PROVIDER_DRAMACHI_ENABLED", true),
                timeout: Duration::from_secs(15),
            },
            ProviderEntry {
                provider_id: ProviderKind::FourKHdHub,
                display_name: "FourKHDHub",
                enabled: env_flag("PV_PROVIDER_FOURKHDHUB_ENABLED", true) && fourkhdhub.is_some(),
                timeout: Duration::from_secs(20),
            },
        ];

        Self {
            entries,
            moviebox: MovieBoxClient::new(),
            anizone: AniZoneClient::new(),
            dramachi: DramachiClient::new(),
            fourkhdhub,
            concurrency: Arc::new(Semaphore::new(8)),
        }
    }

    pub fn enabled_providers(&self) -> Vec<ProviderEntry> {
        self.entries.iter().filter(|e| e.enabled).cloned().collect()
    }

    pub fn timeout_for(&self, provider: ProviderKind) -> Duration {
        self.entries
            .iter()
            .find(|e| e.provider_id == provider)
            .map(|e| e.timeout)
            .unwrap_or_else(|| Duration::from_secs(15))
    }

    pub async fn search_all(
        &self,
        query: &str,
    ) -> (
        Vec<NormalizedAnime>,
        Vec<ProviderFailureInfo>,
        usize,
        BTreeMap<String, String>,
    ) {
        let mut coverage = BTreeMap::new();
        for entry in &self.entries {
            coverage.insert(
                provider_key(entry.provider_id).to_string(),
                if entry.enabled { "failed" } else { "disabled" }.to_string(),
            );
        }

        let enabled = self.enabled_providers();
        let total_enabled = enabled.len();
        let futs = enabled.into_iter().map(|entry| {
            let reg = self.clone();
            let q = query.to_string();
            async move {
                let _permit = reg.concurrency.acquire().await.ok();
                let res =
                    tokio::time::timeout(entry.timeout, reg.search_one(entry.provider_id, &q))
                        .await;
                (entry, res)
            }
        });

        let outcomes = futures::future::join_all(futs).await;
        let mut results = Vec::new();
        let mut failures = Vec::new();

        for (entry, outcome) in outcomes {
            let key = provider_key(entry.provider_id).to_string();
            match outcome {
                Ok(Ok(items)) => {
                    coverage.insert(key, "success".to_string());
                    for item in items {
                        results.push(normalize_catalog_item(item));
                    }
                }
                Ok(Err(err)) => {
                    log::warn!(
                        "v3_search_provider_failed provider={} err={err}",
                        entry.display_name
                    );
                    coverage.insert(key.clone(), "failed".to_string());
                    failures.push(ProviderFailureInfo {
                        provider: key,
                        reason: err.to_string(),
                    });
                }
                Err(_) => {
                    log::warn!("v3_search_provider_timeout provider={}", entry.display_name);
                    coverage.insert(key.clone(), "failed".to_string());
                    failures.push(ProviderFailureInfo {
                        provider: key,
                        reason: "timeout".to_string(),
                    });
                }
            }
        }

        (results, failures, total_enabled, coverage)
    }

    async fn search_one(
        &self,
        provider: ProviderKind,
        query: &str,
    ) -> Result<Vec<CatalogItem>, ProviderError> {
        match provider {
            ProviderKind::MovieBox => Provider::search(&self.moviebox, query, 1).await,
            ProviderKind::AniZone => Provider::search(&self.anizone, query, 1).await,
            ProviderKind::Dramachi => Provider::search(&self.dramachi, query, 1).await,
            ProviderKind::FourKHdHub => match &self.fourkhdhub {
                Some(c) => Provider::search(c, query, 1).await,
                None => Err(ProviderError::Unavailable(
                    "FourKHDHub not configured".into(),
                )),
            },
            _ => Err(ProviderError::Unavailable("Unsupported provider".into())),
        }
    }

    pub async fn fetch_details(
        &self,
        provider: ProviderKind,
        provider_anime_id: &str,
    ) -> Result<MediaDetails, ProviderError> {
        let _permit = self.concurrency.acquire().await.ok();
        let timeout = self.timeout_for(provider);
        let fut = async {
            match provider {
                ProviderKind::MovieBox => {
                    Provider::details(&self.moviebox, provider_anime_id).await
                }
                ProviderKind::AniZone => Provider::details(&self.anizone, provider_anime_id).await,
                ProviderKind::Dramachi => {
                    Provider::details(&self.dramachi, provider_anime_id).await
                }
                ProviderKind::FourKHdHub => match &self.fourkhdhub {
                    Some(c) => Provider::details(c, provider_anime_id).await,
                    None => Err(ProviderError::Unavailable(
                        "FourKHDHub not configured".into(),
                    )),
                },
                _ => Err(ProviderError::Unavailable("Unsupported provider".into())),
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| ProviderError::Unavailable(format!("{provider} details timed out")))?
    }

    pub async fn fetch_episode_sources(
        &self,
        provider: ProviderKind,
        provider_anime_id: &str,
        season: usize,
        episode: usize,
    ) -> Result<Vec<Release>, ProviderError> {
        let _permit = self.concurrency.acquire().await.ok();
        let timeout = self.timeout_for(provider);
        let fut = async {
            match provider {
                ProviderKind::MovieBox => {
                    ReleaseProvider::episode_streams(
                        &self.moviebox,
                        provider_anime_id,
                        season,
                        episode,
                    )
                    .await
                }
                ProviderKind::AniZone => {
                    ReleaseProvider::episode_streams(
                        &self.anizone,
                        provider_anime_id,
                        season,
                        episode,
                    )
                    .await
                }
                ProviderKind::Dramachi => {
                    ReleaseProvider::episode_streams(
                        &self.dramachi,
                        provider_anime_id,
                        season,
                        episode,
                    )
                    .await
                }
                ProviderKind::FourKHdHub => match &self.fourkhdhub {
                    Some(c) => {
                        ReleaseProvider::episode_streams(c, provider_anime_id, season, episode)
                            .await
                    }
                    None => Err(ProviderError::Unavailable(
                        "FourKHDHub not configured".into(),
                    )),
                },
                _ => Err(ProviderError::Unavailable("Unsupported provider".into())),
            }
        };
        tokio::time::timeout(timeout, fut)
            .await
            .map_err(|_| ProviderError::Unavailable(format!("{provider} sources timed out")))?
    }

    pub async fn resolve_release(
        &self,
        provider: ProviderKind,
        release: &Release,
    ) -> Result<(String, Vec<(String, String)>), String> {
        let _permit = self.concurrency.acquire().await.ok();
        let timeout = self.timeout_for(provider);
        match provider {
            ProviderKind::AniZone => {
                let playback = tokio::time::timeout(timeout, self.anizone.resolve_release(release))
                    .await
                    .map_err(|_| "AniZone resolution timed out".to_string())?
                    .map_err(|e| e.to_string())?;
                Ok((playback.url, playback.headers))
            }
            ProviderKind::FourKHdHub => {
                let client = self
                    .fourkhdhub
                    .as_ref()
                    .ok_or_else(|| "FourKHDHub not configured".to_string())?;
                let playback = tokio::time::timeout(
                    timeout,
                    client.resolve_release(release, ResolutionIntent::Playback),
                )
                .await
                .map_err(|_| "FourKHDHub resolution timed out".to_string())?
                .map_err(|e| e.user_message().to_string())?;
                Ok((playback.url, playback.headers))
            }
            ProviderKind::MovieBox | ProviderKind::Dramachi => {
                let url = release
                    .direct_url()
                    .filter(|u| !u.trim().is_empty())
                    .ok_or_else(|| "Source does not expose a playable URL".to_string())?;
                let headers = release
                    .mirrors
                    .first()
                    .map(|m| m.headers.clone())
                    .unwrap_or_default();
                Ok((url.to_string(), headers))
            }
            _ => Err("Unsupported provider".to_string()),
        }
    }
}

pub fn normalize_catalog_item(item: CatalogItem) -> NormalizedAnime {
    let year = item
        .year
        .as_deref()
        .and_then(|y| y.trim().get(..4))
        .and_then(|y| y.parse::<u32>().ok());
    NormalizedAnime {
        provider_id: item.id.provider,
        provider_anime_id: item.id.value,
        title: item.title.trim().to_string(),
        alternative_titles: Vec::new(),
        year,
        media_type: item.media_type,
        description: None,
        poster_url: item.poster_url,
        genres: Vec::new(),
    }
}

pub fn is_valid_media_type(mt: MediaType) -> bool {
    matches!(mt, MediaType::Series | MediaType::Movie)
}
