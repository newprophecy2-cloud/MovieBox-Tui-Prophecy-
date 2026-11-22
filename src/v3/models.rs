use crate::providers::models::{MediaDetails, MediaType, ProviderKind, Release};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackSelectionMetadata {
    pub format: String,
    pub track_index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adaptation_set_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rendition_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language_code: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PipelineStats {
    pub raw_sources: usize,
    pub parsed_sources: usize,
    pub matched_sources: usize,
    pub resolved_sources: usize,
    pub validated_sources: usize,
    pub returned_sources: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExcludedSourceRecord {
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    pub stage: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderCoverageDetail {
    pub provider: String,
    pub request_status: String,
    pub source_count: usize,
    pub validated_source_count: usize,
    pub excluded_source_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderSourceMetadata {
    pub provider_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_source_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mirror_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filename: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_quality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub original_track_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NormalizedSourceMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    pub resolution_u64: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video_codec: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_languages: Vec<String>,
    pub primary_language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_language_code: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MediaProbeMetadata {
    pub http_reachable: bool,
    pub manifest_validated: bool,
    pub media_probe_validated: bool,
    pub playability_status: String,
    pub device_playback_status: String,
    pub probed_track_count: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DetectedLanguageMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detected_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    pub detection_method: String,
    pub verification_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AudioTrackItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_index: Option<usize>,
    pub language: String,
    pub language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_rate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_default: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    pub verification_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_selection_metadata: Option<TrackSelectionMetadata>,
}

#[derive(Debug, Clone)]
pub struct ProviderSourceCandidate {
    pub source_id: String,
    pub provider_id: ProviderKind,
    pub provider_name: String,
    pub provider_episode_id: String,
    pub provider_season: usize,
    pub provider_episode: usize,
    pub release: Release,
    pub audio_languages: Vec<String>,
    pub track_index: Option<usize>,
    pub provider_track_id: Option<String>,
    pub language: String,
    pub language_code: Option<String>,
    pub declared_language: Option<String>,
    pub declared_language_code: Option<String>,
    pub verified_language: Option<String>,
    pub verified_language_code: Option<String>,
    pub audio_track_id: Option<String>,
    pub codec: Option<String>,
    pub audio_codec: Option<String>,
    pub channels: Option<String>,
    pub sample_duration_seconds: Option<f32>,
    pub detection_method: String,
    pub confidence: Option<f32>,
    pub verification_status: String,
    pub playability_status: String,
    pub track_selection_metadata: Option<TrackSelectionMetadata>,
    pub audio_tracks: Vec<AudioTrackItem>,
    pub declared_audio_tracks: Vec<AudioTrackItem>,
    pub provider_metadata: ProviderSourceMetadata,
    pub normalized_metadata: NormalizedSourceMetadata,
    pub media_metadata: MediaProbeMetadata,
    pub detected_language_metadata: DetectedLanguageMetadata,
    pub quality: Option<String>,
    pub resolution_u64: u64,
    pub stream_url: Option<String>,
    pub headers: HashMap<String, String>,
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
    pub metadata_status: String,
    pub overview: Option<String>,
    pub provider_mappings: Vec<ProviderEpisodeMapping>,
    pub audio: Option<AudioAvailability>,
    pub sources: Option<Vec<ProviderSourceCandidate>>,
    pub cached_provider_coverage: Option<BTreeMap<String, String>>,
    pub cached_provider_details: Vec<ProviderCoverageDetail>,
    pub pipeline_stats: PipelineStats,
    pub excluded_sources_count: usize,
    pub excluded_reasons: Vec<String>,
    pub excluded_details: Vec<ExcludedSourceRecord>,
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

#[derive(Debug, Clone, Serialize)]
pub struct ProviderIdsMap {
    pub moviebox: Option<String>,
    pub anizone: Option<String>,
    pub dramachi: Option<String>,
    pub fourkhdhub: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchAnimeItem {
    pub anime_id: String,
    pub title: String,
    pub media_type: MediaType,
    pub year: Option<u32>,
    pub poster_url: Option<String>,
    pub description: Option<String>,
    pub genres: Vec<String>,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub provider_ids: ProviderIdsMap,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub status: &'static str,
    pub session_id: String,
    pub query: String,
    pub total: usize,
    pub partial: bool,
    pub provider_coverage: BTreeMap<String, String>,
    pub provider_failures: Vec<ProviderFailureInfo>,
    pub results: Vec<SearchAnimeItem>,
    pub all_provider_stream_sources: Vec<PublicSourceItem>,
}

#[derive(Debug, Serialize)]
pub struct ProviderFailureInfo {
    pub provider: String,
    pub reason: String,
}

#[derive(Debug, Serialize)]
pub struct AnimeDetailsResponse {
    pub status: &'static str,
    pub anime_id: String,
    pub title: String,
    pub alternative_titles: Vec<String>,
    pub media_type: MediaType,
    pub year: Option<u32>,
    pub description: Option<String>,
    pub poster_url: Option<String>,
    pub genres: Vec<String>,
    pub total_seasons: usize,
    pub total_episodes: usize,
    pub season_structure_status: String,
    pub provider_coverage: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct SeasonItemResponse {
    pub season_id: String,
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
    pub episode_id: String,
    pub episode_number: usize,
    pub title: String,
    pub metadata_status: String,
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
    pub metadata_status: String,
    pub overview: Option<String>,
    pub provider_coverage: BTreeMap<String, String>,
    pub audio: AudioAvailability,
    pub available_languages: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublicSourceItem {
    pub source_id: String,
    pub provider: String,
    pub stream_url: Option<String>,
    pub quality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_track_id: Option<String>,
    pub language: String,
    pub language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_track_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_duration_seconds: Option<f32>,
    pub detection_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    pub verification_status: String,
    pub playability_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_selection_metadata: Option<TrackSelectionMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audio_tracks: Vec<AudioTrackItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_audio_tracks: Vec<AudioTrackItem>,
    pub provider_metadata: ProviderSourceMetadata,
    pub normalized_metadata: NormalizedSourceMetadata,
    pub media_metadata: MediaProbeMetadata,
    pub detected_language_metadata: DetectedLanguageMetadata,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    pub availability_status: &'static str,
    pub available: bool,
}

#[derive(Debug, Serialize)]
pub struct ProviderSourcesGroup {
    pub provider: String,
    pub sources: Vec<PublicSourceItem>,
}

#[derive(Debug, Serialize)]
pub struct EpisodeSourcesResponse {
    pub status: &'static str,
    pub episode_id: String,
    pub total_sources: usize,
    pub verified_sources: usize,
    pub excluded_sources: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excluded_details: Vec<ExcludedSourceRecord>,
    pub pipeline_stats: PipelineStats,
    pub available_languages: Vec<String>,
    pub provider_coverage: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub provider_details: Vec<ProviderCoverageDetail>,
    pub providers: Vec<ProviderSourcesGroup>,
}

#[derive(Debug, Deserialize)]
pub struct ResolveRequest {
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub audio_language: Option<String>,
    #[serde(default)]
    pub preferred_quality: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ResolveSuccessResponse {
    pub status: &'static str,
    pub episode_id: String,
    pub source_id: String,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_track_id: Option<String>,
    pub language: String,
    pub language_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_language_code: Option<String>,
    pub quality: Option<String>,
    pub stream_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_track_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_codec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channels: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_duration_seconds: Option<f32>,
    pub detection_method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    pub verification_status: String,
    pub playability_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_selection_metadata: Option<TrackSelectionMetadata>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audio_tracks: Vec<AudioTrackItem>,
    pub provider_metadata: ProviderSourceMetadata,
    pub normalized_metadata: NormalizedSourceMetadata,
    pub media_metadata: MediaProbeMetadata,
    pub detected_language_metadata: DetectedLanguageMetadata,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorResponse {
    pub status: &'static str,
    pub code: &'static str,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub episode_id: Option<String>,
}
