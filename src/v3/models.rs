use crate::providers::models::{MediaDetails, MediaType, ProviderKind, Release};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchConfidence {
    HighConfidenceMatch,
    PossibleMatch,
    NoMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpisodeMappingOutcome {
    Matched,
    PossibleMatch,
    Unmatched,
    MissingFromProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioStatus {
    Available,
    Missing,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct NormalizedAnime {
    pub provider_id: ProviderKind,
    pub provider_anime_id: String,
    pub title: String,
    pub alternative_titles: Vec<String>,
    pub year: Option<u32>,
    pub media_type: MediaType,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub genres: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProviderAnimeRef {
    pub provider_id: ProviderKind,
    pub provider_anime_id: String,
    pub title: String,
    pub year: Option<u32>,
    pub media_type: MediaType,
    pub details: Option<MediaDetails>,
    pub details_failed: bool,
}

#[derive(Debug, Clone)]
pub struct ProviderEpisodeMapping {
    pub provider_id: ProviderKind,
    pub provider_anime_id: String,
    pub provider_season_number: usize,
    pub provider_episode_number: usize,
    pub provider_episode_id: Option<String>,
    pub title: Option<String>,
    pub outcome: EpisodeMappingOutcome,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct AudioAvailability {
    pub hindi: AudioStatus,
    pub english: AudioStatus,
    pub japanese: AudioStatus,
    pub available_languages: Vec<String>,
    #[serde(skip_serializing)]
    pub checked_sources: bool,
}

#[derive(Debug, Clone)]
pub struct ProviderSourceCandidate {
    pub source_id: String,
    pub provider_id: ProviderKind,
    pub provider_episode_id: String,
    pub provider_season: usize,
    pub provider_episode: usize,
    pub release: Release,
    pub audio_languages: Vec<String>,
    pub quality: Option<String>,
    pub resolution_u64: u64,
    pub available: bool,
}

#[derive(Debug, Clone)]
pub struct CanonicalEpisode {
    pub id: String,
    pub anime_id: String,
    pub season_id: String,
    pub season_number: usize,
    pub episode_number: usize,
    pub title: String,
    pub overview: Option<String>,
    pub provider_mappings: Vec<ProviderEpisodeMapping>,
    pub audio: Option<AudioAvailability>,
    pub sources: Option<Vec<ProviderSourceCandidate>>,
}

#[derive(Debug, Clone)]
pub struct CanonicalSeason {
    pub id: String,
    pub anime_id: String,
    pub season_number: usize,
    pub episodes: Vec<CanonicalEpisode>,
}

#[derive(Debug, Clone)]
pub struct CanonicalAnime {
    pub id: String,
    pub slug_token: String,
    pub title: String,
    pub alternative_titles: Vec<String>,
    pub media_type: MediaType,
    pub year: Option<u32>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub genres: Vec<String>,
    pub provider_records: Vec<ProviderAnimeRef>,
    pub seasons: Vec<CanonicalSeason>,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub season_structure_status: String,
    pub details_hydrated: bool,
}

#[derive(Debug)]
pub struct TemporarySession {
    pub session_id: String,
    pub created_at: Instant,
    pub last_accessed_at: Instant,
    pub expires_at: Instant,
    pub max_expires_at: Instant,
    pub anime_records: HashMap<String, CanonicalAnime>,
    pub season_index: HashMap<String, String>,
    pub episode_index: HashMap<String, (String, String)>,
    pub next_anime_seq: usize,
}

#[derive(Debug, Serialize)]
pub struct SearchResultItem {
    pub id: String,
    pub title: String,
    pub media_type: MediaType,
    pub year: Option<u32>,
    pub poster_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub status: &'static str,
    pub session_id: String,
    pub results: Vec<SearchResultItem>,
    pub total: usize,
    pub partial: bool,
    pub provider_failures: Vec<ProviderFailureInfo>,
}

#[derive(Debug, Serialize)]
pub struct ProviderFailureInfo {
    pub provider: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct AnimeDetailsResponse {
    pub status: &'static str,
    pub id: String,
    pub title: String,
    pub media_type: MediaType,
    pub year: Option<u32>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub genres: Vec<String>,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub season_structure_status: String,
}

#[derive(Debug, Serialize)]
pub struct SeasonItemResponse {
    pub id: String,
    pub season_number: usize,
    pub episode_count: usize,
}

#[derive(Debug, Serialize)]
pub struct AnimeSeasonsResponse {
    pub status: &'static str,
    pub anime_id: String,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub seasons: Vec<SeasonItemResponse>,
}

#[derive(Debug, Serialize)]
pub struct EpisodeItemResponse {
    pub id: String,
    pub episode_number: usize,
    pub title: String,
}

#[derive(Debug, Serialize)]
pub struct SeasonEpisodesResponse {
    pub status: &'static str,
    pub season_id: String,
    pub total_episodes: usize,
    pub episodes: Vec<EpisodeItemResponse>,
}

#[derive(Debug, Serialize)]
pub struct EpisodeDetailsResponse {
    pub status: &'static str,
    pub episode_id: String,
    pub anime_id: String,
    pub season_id: String,
    pub season_number: usize,
    pub episode_number: usize,
    pub title: String,
    pub overview: Option<String>,
    pub audio: AudioAvailability,
}

#[derive(Debug, Serialize)]
pub struct PublicSourceItem {
    pub id: String,
    pub quality: Option<String>,
    pub audio_languages: Vec<String>,
    pub available: bool,
}

#[derive(Debug, Serialize)]
pub struct EpisodeSourcesResponse {
    pub status: &'static str,
    pub episode_id: String,
    pub sources: Vec<PublicSourceItem>,
    pub total: usize,
}

#[derive(Debug, Deserialize)]
pub struct ResolveRequest {
    #[serde(default)]
    pub audio_language: Option<String>,
    #[serde(default)]
    pub preferred_quality: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ResolveSuccessResponse {
    pub status: &'static str,
    pub episode_id: String,
    pub stream_url: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
    pub error: ApiErrorBody,
}
