use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

const MIGRATION_001: &str = include_str!("../migrations/001_catalog.sql");
const MIGRATION_002: &str = include_str!("../migrations/002_codec_verification.sql");
const MIGRATION_003: &str = include_str!("../migrations/003_anime_import_jobs.sql");
const MIGRATION_004: &str = include_str!("../migrations/004_movie_sources.sql");
const MIGRATION_005: &str = include_str!("../migrations/005_unified_content_mappings.sql");
const MIGRATION_006: &str = include_str!("../migrations/006_task3_task4_integrity.sql");
const MIGRATION_007: &str = include_str!("../migrations/007_task3_task4_ownership.sql");
const MIGRATION_008: &str = include_str!("../migrations/008_task3_task4_lifecycle.sql");
const MIGRATION_009: &str = include_str!("../migrations/009_task3_episode_identity.sql");

#[derive(Clone)]
pub struct CatalogStore {
    client: Arc<Mutex<Client>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogTitle {
    pub id: String,
    pub id_type: &'static str,
    pub mal_id: Option<i64>,
    pub canonical_title: String,
    pub normalized_title: String,
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TitleUpsert {
    pub mal_id: Option<i64>,
    pub canonical_title: String,
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ImportTitle {
    pub mal_id: Option<i64>,
    pub canonical_title: String,
    #[serde(default = "default_anime")]
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: Option<String>,
    #[serde(default)]
    pub alternative_titles: Vec<AlternativeTitleInput>,
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(default)]
    pub providers: Vec<ProviderTitleInput>,
    #[serde(default)]
    pub seasons: Vec<SeasonInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct AlternativeTitleInput {
    pub title: String,
    pub language: Option<String>,
    pub title_type: Option<String>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProviderTitleInput {
    pub provider_key: String,
    pub display_name: Option<String>,
    pub provider_title_id: String,
    pub provider_title: Option<String>,
    pub provider_url: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub episodes: Vec<ProviderEpisodeInput>,
    #[serde(default)]
    pub sources: Vec<ProviderSourceInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProviderEpisodeInput {
    #[serde(default)]
    pub provider_key: Option<String>,
    pub display_name: Option<String>,
    pub season_number: Option<i32>,
    pub provider_episode_id: String,
    pub provider_episode_number: Option<i32>,
    #[serde(default)]
    pub sources: Vec<ProviderSourceInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SeasonInput {
    pub season_number: i32,
    pub canonical_label: Option<String>,
    pub episode_count_reported: Option<i32>,
    #[serde(default)]
    pub episodes: Vec<EpisodeInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct EpisodeInput {
    pub episode_number: i32,
    #[serde(default)]
    pub episode_type: Option<String>,
    #[serde(default)]
    pub episode_coordinate: Option<String>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub special_identifier: Option<String>,
    pub episode_title: Option<String>,
    pub overview: Option<String>,
    pub release_date: Option<String>,
    pub thumbnail_url: Option<String>,
    #[serde(default)]
    pub provider_episode_ids: Vec<ProviderEpisodeInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeInput {
    pub canonical_title: String,
    #[serde(default = "default_anime")]
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: Option<String>,
    #[serde(default)]
    pub provider_titles: Vec<UnifiedAnimeProviderTitleInput>,
    #[serde(default)]
    pub seasons: Vec<UnifiedAnimeSeasonInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeProviderTitleInput {
    pub provider_key: String,
    pub display_name: Option<String>,
    pub provider_title_id: String,
    pub provider_title: Option<String>,
    pub provider_url: Option<String>,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub last_successful_resolution: bool,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeSeasonInput {
    pub season_number: i32,
    pub canonical_label: Option<String>,
    pub episode_count_reported: Option<i32>,
    #[serde(default)]
    pub provider_seasons: Vec<UnifiedAnimeProviderSeasonInput>,
    #[serde(default)]
    pub episodes: Vec<UnifiedAnimeEpisodeInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeProviderSeasonInput {
    pub provider_key: String,
    pub provider_season_id: Option<String>,
    pub provider_season_number: Option<i32>,
    #[serde(default)]
    pub metadata: Value,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeEpisodeInput {
    pub episode_number: i32,
    #[serde(default)]
    pub episode_type: Option<String>,
    #[serde(default)]
    pub episode_coordinate: Option<String>,
    #[serde(default)]
    pub absolute_episode_number: Option<i32>,
    #[serde(default)]
    pub special_identifier: Option<String>,
    pub episode_title: Option<String>,
    pub overview: Option<String>,
    pub release_date: Option<String>,
    pub thumbnail_url: Option<String>,
    #[serde(default)]
    pub provider_episodes: Vec<UnifiedAnimeProviderEpisodeInput>,
    #[serde(default)]
    pub provider_lookups: Vec<UnifiedAnimeProviderLookupInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeProviderEpisodeInput {
    pub provider_key: String,
    pub provider_episode_id: String,
    pub provider_episode_number: Option<i32>,
    #[serde(default)]
    pub metadata: Value,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct UnifiedAnimeProviderLookupInput {
    pub provider_key: String,
    pub provider_title_id: String,
    pub provider_season_number: i32,
    pub provider_episode_number: i32,
    pub lookup_reference: String,
    pub mapping_status: String,
    #[serde(default)]
    pub metadata: Value,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProviderSourceInput {
    pub provider_source_id: String,
    pub source_reference: Option<String>,
    pub quality: Option<String>,
    pub codec: Option<String>,
    pub audio_codec: Option<String>,
    pub container: Option<String>,
    pub resolution_width: Option<i32>,
    pub resolution_height: Option<i32>,
    pub source_status: Option<String>,
    // Kept in serialized responses for compatibility, but never accepted from
    // ordinary import payloads. Only the trusted verifier may set this state.
    #[serde(skip_deserializing)]
    pub codec_verification_status: Option<String>,
    pub expires_at: Option<String>,
    #[serde(default)]
    pub audio_tracks: Vec<TrackInput>,
    #[serde(default)]
    pub subtitle_tracks: Vec<TrackInput>,
}
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TrackInput {
    pub language_code: Option<String>,
    pub codec: Option<String>,
    pub format: Option<String>,
    #[serde(default)]
    pub is_default: bool,
    #[serde(default)]
    pub is_forced: bool,
    #[serde(default)]
    pub verified: bool,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Serialize)]
pub struct CatalogJob {
    pub id: String,
    pub job_type: String,
    pub status: String,
    pub inserted_count: i64,
    pub updated_count: i64,
    pub skipped_count: i64,
    pub failed_count: i64,
    pub titles_imported: i64,
    pub seasons_imported: i64,
    pub episodes_imported: i64,
    pub sources_imported: i64,
    pub tracks_imported: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anime_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_episodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed_episodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_episodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_episodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_warning_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_size: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress_percent: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_progress_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AnimeImportEpisodeRef {
    pub ordinal: i64,
    pub season_index: i32,
    pub episode_index: i32,
    pub season_number: i32,
    pub provider_season: i32,
    pub episode_number: i32,
    pub provider_episode_id: String,
    pub episode_metadata: Value,
}

#[derive(Debug, Clone)]
pub struct ClaimedAnimeImportJob {
    pub id: String,
    pub anime_name: String,
    pub provider_key: String,
    pub provider_title_id: String,
    pub base_import: Value,
    pub total_episodes: i64,
    pub batch_size: i32,
    pub metadata_imported: bool,
    pub metadata_attempt_count: i32,
}
#[derive(Debug, Clone, Copy, Default)]
pub struct ImportSummary {
    pub inserted: i64,
    pub updated: i64,
    pub skipped: i64,
    pub seasons: i64,
    pub episodes: i64,
    pub sources: i64,
    pub tracks: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct CodecVerificationJob {
    pub source_id: String,
    pub provider_source_id: String,
    pub provider_content_id: String,
    pub content_type: String,
    pub identity_fingerprint: String,
    pub lease_token: Uuid,
    pub attempt_count: i32,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogGenre {
    pub id: String,
    pub canonical_name: String,
    pub normalized_name: String,
}
#[derive(Debug, Serialize)]
pub struct CatalogDetail {
    pub title: CatalogTitle,
    pub alternative_titles: Vec<Value>,
    pub genres: Vec<Value>,
    pub provider_mappings: Vec<Value>,
    pub seasons: Vec<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<Value>,
}
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("database error")]
    Database(#[from] tokio_postgres::Error),
    #[error("invalid title: {0}")]
    Validation(String),
}

/// Persisted Provider mapping lifecycle. Transitions are intentionally explicit
/// so transient refresh failures cannot masquerade as identity rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderMappingStatus {
    Unverified,
    Verified,
    Unresolved,
    Stale,
    Rejected,
}

impl ProviderMappingStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unverified => "unverified",
            Self::Verified => "verified",
            Self::Unresolved => "unresolved",
            Self::Stale => "stale",
            Self::Rejected => "rejected",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "unverified" => Self::Unverified,
            "verified" => Self::Verified,
            "unresolved" => Self::Unresolved,
            "stale" => Self::Stale,
            "rejected" => Self::Rejected,
            _ => return None,
        })
    }
}

/// Validate a mapping lifecycle transition. Temporary failures should use
/// `Unresolved` or retain the current status, never `Rejected`.
pub fn mapping_status_transition(
    current: ProviderMappingStatus,
    next: ProviderMappingStatus,
) -> bool {
    use ProviderMappingStatus::*;
    matches!(
        (current, next),
        (Unverified, Unverified | Verified | Unresolved | Rejected)
            | (Verified, Verified | Stale | Unresolved | Rejected)
            | (Unresolved, Unresolved | Verified | Stale | Rejected)
            | (Stale, Stale | Verified | Unresolved | Rejected)
            | (Rejected, Rejected)
    )
}

fn validate_transition_evidence(reason: &str, evidence: &Value) -> Result<(), CatalogError> {
    if reason.trim().is_empty() {
        return Err(CatalogError::Validation(
            "mapping transition reason must not be empty".into(),
        ));
    }
    if evidence.is_null() {
        return Err(CatalogError::Validation(
            "mapping transition evidence must not be null".into(),
        ));
    }
    if let Some(object) = evidence.as_object() {
        let forbidden = ["password", "secret", "token", "credential", "api_key"];
        if object.keys().any(|key| {
            let normalized = key.to_ascii_lowercase();
            forbidden.iter().any(|word| normalized.contains(word))
        }) {
            return Err(CatalogError::Validation(
                "mapping transition evidence contains sensitive metadata".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationDecision {
    VerifiedReplacement,
    Unresolved { reason: String },
    Rejected { reason: String },
}

/// Decide whether a changed Provider ID may replace an existing mapping.
/// Exact Provider identity and strong title/year/media evidence are required;
/// absence of evidence never counts as proof.
pub fn reconcile_provider_identity(
    provider_matches: bool,
    new_id_matches_response: bool,
    existing_title: &str,
    candidate_title: &str,
    existing_year: Option<i32>,
    candidate_year: Option<i32>,
    existing_media_type: &str,
    candidate_media_type: &str,
    conflicting_owner: bool,
) -> ReconciliationDecision {
    if conflicting_owner {
        return ReconciliationDecision::Unresolved {
            reason: "candidate Provider ID already belongs to another canonical record".into(),
        };
    }
    if !provider_matches || !new_id_matches_response {
        return ReconciliationDecision::Rejected {
            reason: "Provider identity or exact title ID mismatch".into(),
        };
    }
    if existing_media_type != candidate_media_type
        || existing_year
            .zip(candidate_year)
            .is_some_and(|(left, right)| left != right)
    {
        return ReconciliationDecision::Unresolved {
            reason: "known metadata conflicts".into(),
        };
    }
    let normalize = |value: &str| crate::unified::normalize_anime_title(value).key;
    if normalize(existing_title) != normalize(candidate_title) {
        return ReconciliationDecision::Unresolved {
            reason: "title evidence is ambiguous".into(),
        };
    }
    ReconciliationDecision::VerifiedReplacement
}

#[derive(Debug, Clone, Serialize)]
pub struct MappingIntegrityReport {
    pub duplicate_provider_titles: i64,
    pub duplicate_provider_episodes: i64,
    pub duplicate_lookup_coordinates: i64,
    pub conflicting_lookup_owners: i64,
    pub orphan_provider_titles: i64,
    pub orphan_provider_episodes: i64,
    pub orphan_lookup_mappings: i64,
    pub missing_canonical_anime: i64,
    pub missing_canonical_seasons: i64,
    pub missing_canonical_episodes: i64,
    pub cross_anime_season_ownership: i64,
    pub invalid_mapping_status: i64,
    pub empty_provider_ids: i64,
    pub stale_mappings: i64,
    pub unresolved_provider_titles: i64,
    pub unresolved_provider_episodes: i64,
    pub reconciliation_conflicts: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MappingTransitionResult {
    pub mapping_type: String,
    pub mapping_id: String,
    pub previous_status: String,
    pub new_status: String,
}

#[derive(Debug, Clone)]
pub struct ProviderTitleReconciliation {
    pub canonical_title_id: String,
    pub provider_key: String,
    pub old_provider_title_id: String,
    pub new_provider_title_id: String,
    pub old_title: String,
    pub new_title: String,
    pub old_year: Option<i32>,
    pub new_year: Option<i32>,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct ProviderEpisodeReconciliation {
    pub canonical_episode_id: String,
    pub provider_key: String,
    pub provider_title_id: String,
    pub old_provider_episode_id: String,
    pub new_provider_episode_id: String,
    pub season_number: i32,
    pub episode_number: i32,
}

fn default_anime() -> String {
    "anime".into()
}
impl CatalogStore {
    pub async fn connect_from_env() -> Result<Option<Self>, CatalogError> {
        let Some(url) = std::env::var("DATABASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
        else {
            return Ok(None);
        };
        let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("postgres connection stopped: {e}")
            }
        });
        let store = Self {
            client: Arc::new(Mutex::new(client)),
        };
        store.migrate().await?;
        Ok(Some(store))
    }
    pub async fn migrate(&self) -> Result<(), CatalogError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        tx.query_one("SELECT pg_advisory_xact_lock($1)", &[&6_731_009_i64])
            .await?;
        tx.batch_execute(MIGRATION_001).await?;
        tx.batch_execute(MIGRATION_002).await?;
        tx.batch_execute(MIGRATION_003).await?;
        tx.batch_execute(MIGRATION_004).await?;
        tx.batch_execute(MIGRATION_005).await?;
        tx.batch_execute(MIGRATION_006).await?;
        tx.batch_execute(MIGRATION_007).await?;
        tx.batch_execute(MIGRATION_008).await?;
        tx.batch_execute(MIGRATION_009).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Read-only integrity diagnostics. This never deletes or repairs records.
    pub async fn mapping_integrity_report(&self) -> Result<MappingIntegrityReport, CatalogError> {
        let client = self.client.lock().await;
        let row = client.query_one(
            "SELECT
                (SELECT count(*) FROM (SELECT provider_id, provider_title_id FROM provider_titles GROUP BY 1,2 HAVING count(*) > 1) d),
                (SELECT count(*) FROM (SELECT provider_id, provider_episode_id FROM episode_provider_mappings GROUP BY 1,2 HAVING count(*) > 1) d),
                (SELECT count(*) FROM (SELECT provider_id, provider_title_id, provider_season_number, provider_episode_number FROM episode_provider_lookup_mappings GROUP BY 1,2,3,4 HAVING count(*) > 1) d),
                (SELECT count(*) FROM (SELECT provider_id, provider_title_id, provider_season_number, provider_episode_number FROM episode_provider_lookup_mappings WHERE mapping_status='resolved' GROUP BY 1,2,3,4 HAVING count(DISTINCT episode_id) > 1) d),
                (SELECT count(*) FROM provider_titles pt WHERE NOT EXISTS (SELECT 1 FROM catalog_titles ct WHERE ct.id=pt.title_id)),
                (SELECT count(*) FROM episode_provider_mappings epm WHERE NOT EXISTS (SELECT 1 FROM episodes e WHERE e.id=epm.episode_id)),
                (SELECT count(*) FROM episode_provider_lookup_mappings l WHERE NOT EXISTS (SELECT 1 FROM episodes e WHERE e.id=l.episode_id)),
                (SELECT count(*) FROM provider_titles pt WHERE NOT EXISTS (SELECT 1 FROM catalog_titles ct WHERE ct.id=pt.title_id)),
                (SELECT count(*) FROM episodes e WHERE e.season_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM seasons s WHERE s.id=e.season_id)),
                (SELECT count(*) FROM episode_provider_mappings epm WHERE NOT EXISTS (SELECT 1 FROM episodes e WHERE e.id=epm.episode_id)),
                (SELECT count(*) FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id IS DISTINCT FROM s.title_id),
                (SELECT count(*) FROM provider_titles WHERE match_status NOT IN ('unverified','verified','unresolved','stale','rejected')) + (SELECT count(*) FROM episode_provider_mappings WHERE mapping_status NOT IN ('resolved','unresolved')),
                (SELECT count(*) FROM provider_titles WHERE length(btrim(provider_title_id))=0) + (SELECT count(*) FROM episode_provider_mappings WHERE length(btrim(provider_episode_id))=0),
                (SELECT count(*) FROM provider_titles WHERE match_status='stale'),
                (SELECT count(*) FROM provider_titles WHERE match_status IN ('unresolved','stale','rejected')),
                (SELECT count(*) FROM episode_provider_mappings WHERE mapping_status='unresolved'),
                (SELECT count(*) FROM provider_mapping_transition_history WHERE reason ILIKE '%conflict%' OR reason ILIKE '%ambiguous%')",
            &[],
        ).await?;
        Ok(MappingIntegrityReport {
            duplicate_provider_titles: row.get(0),
            duplicate_provider_episodes: row.get(1),
            duplicate_lookup_coordinates: row.get(2),
            conflicting_lookup_owners: row.get(3),
            orphan_provider_titles: row.get(4),
            orphan_provider_episodes: row.get(5),
            orphan_lookup_mappings: row.get(6),
            missing_canonical_anime: row.get(7),
            missing_canonical_seasons: row.get(8),
            missing_canonical_episodes: row.get(9),
            cross_anime_season_ownership: row.get(10),
            invalid_mapping_status: row.get(11),
            empty_provider_ids: row.get(12),
            stale_mappings: row.get(13),
            unresolved_provider_titles: row.get(14),
            unresolved_provider_episodes: row.get(15),
            reconciliation_conflicts: row.get(16),
        })
    }

    /// Apply a validated title mapping status transition and append an audit row.
    pub async fn transition_provider_title_status(
        &self,
        mapping_id: &str,
        next: ProviderMappingStatus,
        reason: &str,
        evidence: Value,
    ) -> Result<MappingTransitionResult, CatalogError> {
        let id = parse_uuid(mapping_id)?;
        validate_transition_evidence(reason, &evidence)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                "SELECT match_status FROM provider_titles WHERE id=$1 FOR UPDATE",
                &[&id],
            )
            .await?;
        let previous: String = row.get(0);
        let current = ProviderMappingStatus::parse(&previous).ok_or_else(|| {
            CatalogError::Validation("unknown Provider title mapping status".into())
        })?;
        if !mapping_status_transition(current, next) {
            return Err(CatalogError::Validation(format!(
                "invalid mapping transition {previous} -> {}",
                next.as_str()
            )));
        }
        tx.execute("UPDATE provider_titles SET match_status=$2, last_checked_at=now(), updated_at=now() WHERE id=$1", &[&id, &next.as_str()]).await?;
        tx.execute("INSERT INTO provider_mapping_transition_history(mapping_type,mapping_id,previous_status,new_status,reason,evidence) VALUES('title',$1,$2,$3,$4,$5)", &[&id, &previous, &next.as_str(), &reason, &evidence]).await?;
        tx.commit().await?;
        Ok(MappingTransitionResult {
            mapping_type: "title".into(),
            mapping_id: mapping_id.into(),
            previous_status: previous,
            new_status: next.as_str().into(),
        })
    }

    /// Reconcile a changed Provider title ID without deleting the old mapping.
    pub async fn reconcile_provider_title_mapping(
        &self,
        input: ProviderTitleReconciliation,
    ) -> Result<ReconciliationDecision, CatalogError> {
        let title_id = parse_uuid(&input.canonical_title_id)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let provider_id: Uuid = tx
            .query_one(
                "SELECT id FROM provider_registry WHERE provider_key=$1",
                &[&input.provider_key],
            )
            .await?
            .get(0);
        let old = tx.query_opt("SELECT id, title_id, match_status FROM provider_titles WHERE provider_id=$1 AND provider_title_id=$2 FOR UPDATE", &[&provider_id, &input.old_provider_title_id]).await?;
        let Some(old) = old else {
            return Ok(ReconciliationDecision::Unresolved {
                reason: "old Provider title mapping was not found".into(),
            });
        };
        let owner: Uuid = old.get("title_id");
        if owner != title_id {
            return Ok(ReconciliationDecision::Unresolved {
                reason: "old mapping belongs to another canonical title".into(),
            });
        }
        let conflict = tx.query_opt("SELECT title_id FROM provider_titles WHERE provider_id=$1 AND provider_title_id=$2 FOR UPDATE", &[&provider_id, &input.new_provider_title_id]).await?.is_some_and(|row| row.get::<_, Uuid>(0) != title_id);
        let decision = reconcile_provider_identity(
            true,
            !input.new_provider_title_id.trim().is_empty(),
            &input.old_title,
            &input.new_title,
            input.old_year,
            input.new_year,
            &input.content_type,
            &input.content_type,
            conflict,
        );
        if !matches!(decision, ReconciliationDecision::VerifiedReplacement) {
            return Ok(decision);
        }
        tx.execute("INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,match_status,last_checked_at) VALUES($1,$2,$3,$4,'verified',now()) ON CONFLICT(provider_id,provider_title_id) DO NOTHING", &[&title_id, &provider_id, &input.new_provider_title_id, &input.new_title]).await?;
        let old_id: Uuid = old.get("id");
        tx.execute("UPDATE provider_titles SET match_status='stale', updated_at=now() WHERE id=$1 AND match_status='verified'", &[&old_id]).await?;
        tx.execute("INSERT INTO provider_mapping_transition_history(mapping_type,mapping_id,previous_status,new_status,reason,evidence) VALUES('title',$1,$2,'stale','Provider ID reconciled',jsonb_build_object('new_provider_title_id',$3)) ON CONFLICT DO NOTHING", &[&old_id, &old.get::<_, String>("match_status"), &input.new_provider_title_id]).await?;
        tx.commit().await?;
        Ok(decision)
    }

    /// Reconcile an Episode ID change while retaining the old mapping history.
    pub async fn reconcile_provider_episode_mapping(
        &self,
        input: ProviderEpisodeReconciliation,
    ) -> Result<ReconciliationDecision, CatalogError> {
        let episode_id = parse_uuid(&input.canonical_episode_id)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let provider_id: Uuid = tx
            .query_one(
                "SELECT id FROM provider_registry WHERE provider_key=$1",
                &[&input.provider_key],
            )
            .await?
            .get(0);
        let owner = tx.query_opt("SELECT episode_id FROM episode_provider_mappings WHERE provider_id=$1 AND provider_episode_id=$2 FOR UPDATE", &[&provider_id, &input.new_provider_episode_id]).await?;
        if let Some(row) = owner {
            if row.get::<_, Uuid>(0) != episode_id {
                return Ok(ReconciliationDecision::Unresolved {
                    reason: "new Provider Episode ID already has another owner".into(),
                });
            }
        }
        let old = tx.query_opt("SELECT id, episode_id, mapping_status FROM episode_provider_mappings WHERE provider_id=$1 AND provider_episode_id=$2 FOR UPDATE", &[&provider_id, &input.old_provider_episode_id]).await?;
        let Some(old) = old else {
            return Ok(ReconciliationDecision::Unresolved {
                reason: "old Provider Episode mapping was not found".into(),
            });
        };
        if old.get::<_, Uuid>("episode_id") != episode_id {
            return Ok(ReconciliationDecision::Unresolved {
                reason: "old Provider Episode mapping belongs to another canonical Episode".into(),
            });
        }
        tx.execute("INSERT INTO episode_provider_mappings(episode_id,provider_id,provider_episode_id,provider_episode_number,mapping_status) VALUES($1,$2,$3,$4,'resolved') ON CONFLICT(provider_id,provider_episode_id) DO NOTHING", &[&episode_id, &provider_id, &input.new_provider_episode_id, &input.episode_number]).await?;
        tx.execute("UPDATE episode_provider_mappings SET mapping_status='unresolved', metadata=metadata||jsonb_build_object('reconciled_to',$3), updated_at=now() WHERE id=$1 AND mapping_status='resolved'", &[&old.get::<_, Uuid>("id"), &input.provider_title_id, &input.new_provider_episode_id]).await?;
        tx.commit().await?;
        Ok(ReconciliationDecision::VerifiedReplacement)
    }

    pub async fn create_anime_import_job(
        &self,
        anime_name: &str,
        provider_key: &str,
        provider_title_id: &str,
        base_import: &ImportTitle,
        batch_size: i32,
        total_episodes: i64,
    ) -> Result<(String, bool), CatalogError> {
        if !(1..=50).contains(&batch_size) || total_episodes <= 0 {
            return Err(CatalogError::Validation("invalid anime import job".into()));
        }
        let id = new_id();
        let base = serde_json::to_value(base_import)
            .map_err(|e| CatalogError::Validation(format!("invalid import snapshot: {e}")))?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let active_key = format!("{provider_key}:{provider_title_id}");
        if let Some(row) = tx.query_opt("SELECT job_id FROM catalog_anime_import_jobs WHERE active_import_key=$1 FOR UPDATE", &[&active_key]).await? {
            let id: String = row.get("job_id");
            tx.commit().await?;
            return Ok((id, false));
        }
        tx.execute("INSERT INTO catalog_jobs(id,job_type,status,started_at) VALUES($1,'import_by_name','staging',now())", &[&id]).await?;
        let inserted = tx.execute("INSERT INTO catalog_anime_import_jobs(job_id,active_import_key,anime_name,provider_key,provider_title_id,base_import,total_episodes,batch_size) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(active_import_key) DO NOTHING", &[&id,&active_key,&anime_name,&provider_key,&provider_title_id,&base,&total_episodes,&batch_size]).await?;
        if inserted == 0 {
            tx.execute("DELETE FROM catalog_jobs WHERE id=$1", &[&id])
                .await?;
            let existing = tx
                .query_one(
                    "SELECT job_id FROM catalog_anime_import_jobs WHERE active_import_key=$1",
                    &[&active_key],
                )
                .await?;
            let existing_id: String = existing.get("job_id");
            tx.commit().await?;
            return Ok((existing_id, false));
        }
        tx.commit().await?;
        Ok((id, true))
    }

    pub async fn append_anime_import_episode_batch(
        &self,
        job_id: &str,
        episodes: &[AnimeImportEpisodeRef],
    ) -> Result<(), CatalogError> {
        if episodes.is_empty() || episodes.len() > 50 {
            return Err(CatalogError::Validation(
                "episode staging batch must contain 1 to 50 records".into(),
            ));
        }
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        for episode in episodes {
            let metadata = serde_json::json!({"provider_episode_id":episode.provider_episode_id,"episode":episode.episode_metadata});
            tx.execute("INSERT INTO catalog_anime_import_episodes(job_id,ordinal,season_index,episode_index,season_number,provider_season,episode_number,episode_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", &[&job_id,&episode.ordinal,&episode.season_index,&episode.episode_index,&episode.season_number,&episode.provider_season,&episode.episode_number,&metadata]).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn activate_anime_import_job(&self, job_id: &str) -> Result<i64, CatalogError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let row = tx.query_one("SELECT j.total_episodes,(SELECT count(*) FROM catalog_anime_import_episodes e WHERE e.job_id=j.job_id) AS staged FROM catalog_anime_import_jobs j WHERE j.job_id=$1 FOR UPDATE", &[&job_id]).await?;
        let expected: i64 = row.get("total_episodes");
        let staged: i64 = row.get("staged");
        if staged != expected {
            return Err(CatalogError::Validation(
                "staged episode count does not match provider count".into(),
            ));
        }
        tx.execute(
            "UPDATE catalog_jobs SET status='queued' WHERE id=$1 AND status='staging'",
            &[&job_id],
        )
        .await?;
        tx.commit().await?;
        Ok(staged)
    }

    pub async fn recover_staged_anime_import_jobs(&self) -> Result<(), CatalogError> {
        let client = self.client.lock().await;
        client.execute("UPDATE catalog_jobs c SET status='queued' FROM catalog_anime_import_jobs j WHERE c.id=j.job_id AND c.status='staging' AND j.total_episodes=(SELECT count(*) FROM catalog_anime_import_episodes e WHERE e.job_id=j.job_id)", &[]).await?;
        client.execute("UPDATE catalog_jobs SET status='failed',failed_count=1,completed_at=now() WHERE status='staging' AND started_at < now()-interval '10 minutes'", &[]).await?;
        client.execute("UPDATE catalog_anime_import_jobs SET active_import_key=NULL WHERE job_id IN (SELECT id FROM catalog_jobs WHERE status='failed' AND job_type='import_by_name' AND started_at < now()-interval '10 minutes')", &[]).await?;
        Ok(())
    }

    pub async fn fail_staging_anime_import_job(
        &self,
        job_id: &str,
        code: &str,
    ) -> Result<(), CatalogError> {
        let client = self.client.lock().await;
        client.execute("UPDATE catalog_jobs SET status='failed',failed_count=1,completed_at=now(),error_summary=$2 WHERE id=$1 AND status='staging'", &[&job_id,&code]).await?;
        client.execute("UPDATE catalog_anime_import_jobs SET active_import_key=NULL,failure_summary=$2,updated_at=now(),last_progress_at=now() WHERE job_id=$1", &[&job_id,&code]).await?;
        Ok(())
    }

    pub async fn claim_anime_import_job(
        &self,
    ) -> Result<Option<ClaimedAnimeImportJob>, CatalogError> {
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let row = tx.query_opt(
            "SELECT j.job_id,j.anime_name,j.provider_key,j.provider_title_id,j.base_import,j.total_episodes,j.batch_size,j.metadata_imported,j.metadata_attempt_count FROM catalog_anime_import_jobs j JOIN catalog_jobs c ON c.id=j.job_id WHERE c.status IN ('queued','running') AND (j.lease_until IS NULL OR j.lease_until < now()) AND EXISTS (SELECT 1 FROM catalog_anime_import_episodes e WHERE e.job_id=j.job_id AND e.status='pending') ORDER BY j.created_at FOR UPDATE OF j SKIP LOCKED LIMIT 1",
            &[],
        ).await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let id: String = row.get("job_id");
        tx.execute("UPDATE catalog_anime_import_jobs SET lease_until=now()+interval '2 minutes',updated_at=now() WHERE job_id=$1", &[&id]).await?;
        tx.execute(
            "UPDATE catalog_jobs SET status='running' WHERE id=$1",
            &[&id],
        )
        .await?;
        let job = ClaimedAnimeImportJob {
            id,
            anime_name: row.get("anime_name"),
            provider_key: row.get("provider_key"),
            provider_title_id: row.get("provider_title_id"),
            base_import: row.get("base_import"),
            total_episodes: row.get("total_episodes"),
            batch_size: row.get("batch_size"),
            metadata_imported: row.get("metadata_imported"),
            metadata_attempt_count: row.get("metadata_attempt_count"),
        };
        tx.commit().await?;
        Ok(Some(job))
    }

    pub async fn pending_anime_import_episodes(
        &self,
        job_id: &str,
        limit: i64,
    ) -> Result<Vec<(i64, i32, i32, i32, i32, i32, Value)>, CatalogError> {
        let rows = self.client.lock().await.query(
            "SELECT ordinal,season_index,episode_index,season_number,provider_season,episode_number,episode_metadata FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='pending' ORDER BY ordinal LIMIT $2",
            &[&job_id,&limit.clamp(1,50)],
        ).await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get("ordinal"),
                    r.get("season_index"),
                    r.get("episode_index"),
                    r.get("season_number"),
                    r.get("provider_season"),
                    r.get("episode_number"),
                    r.get("episode_metadata"),
                )
            })
            .collect())
    }

    pub async fn mark_anime_import_metadata(&self, job_id: &str) -> Result<(), CatalogError> {
        self.client.lock().await.execute(
            "UPDATE catalog_anime_import_jobs SET metadata_imported=true,last_progress_at=now(),updated_at=now(),lease_until=now()+interval '2 minutes' WHERE job_id=$1",
            &[&job_id],
        ).await?;
        Ok(())
    }

    pub async fn fail_anime_import_metadata(
        &self,
        job_id: &str,
        code: &str,
    ) -> Result<(), CatalogError> {
        let client = self.client.lock().await;
        client.execute("UPDATE catalog_anime_import_jobs SET metadata_attempt_count=metadata_attempt_count+1,failure_summary=$2,last_progress_at=now(),updated_at=now(),lease_until=CASE WHEN metadata_attempt_count+1 >= 5 THEN NULL ELSE now()+interval '2 minutes' END WHERE job_id=$1", &[&job_id,&code]).await?;
        client.execute("UPDATE catalog_jobs SET status=CASE WHEN (SELECT metadata_attempt_count FROM catalog_anime_import_jobs WHERE job_id=$1) >= 5 THEN 'failed' ELSE 'running' END,failed_count=CASE WHEN (SELECT metadata_attempt_count FROM catalog_anime_import_jobs WHERE job_id=$1) >= 5 THEN 1 ELSE 0 END,completed_at=CASE WHEN (SELECT metadata_attempt_count FROM catalog_anime_import_jobs WHERE job_id=$1) >= 5 THEN now() ELSE NULL END WHERE id=$1", &[&job_id]).await?;
        client.execute("UPDATE catalog_anime_import_jobs SET active_import_key=NULL WHERE job_id=$1 AND metadata_attempt_count >= 5", &[&job_id]).await?;
        Ok(())
    }

    pub async fn complete_anime_import_episode(
        &self,
        job_id: &str,
        ordinal: i64,
        warning: Option<&str>,
    ) -> Result<(), CatalogError> {
        let client = self.client.lock().await;
        client.execute(
            "UPDATE catalog_anime_import_episodes SET status='completed',source_warning_code=$3,updated_at=now() WHERE job_id=$1 AND ordinal=$2 AND status='pending'",
            &[&job_id,&ordinal,&warning],
        ).await?;
        client.execute(
            "UPDATE catalog_anime_import_jobs SET processed_episodes=(SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status IN ('completed','failed')),source_warning_count=(SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND source_warning_code IS NOT NULL),last_progress_at=now(),updated_at=now(),lease_until=now()+interval '2 minutes' WHERE job_id=$1",
            &[&job_id],
        ).await?;
        let pending: i64 = client.query_one("SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='pending'", &[&job_id]).await?.get(0);
        if pending == 0 {
            let failed: i64 = client.query_one("SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='failed'", &[&job_id]).await?.get(0);
            let warnings: i64 = client.query_one("SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND source_warning_code IS NOT NULL", &[&job_id]).await?.get(0);
            let status = if failed > 0 {
                "failed"
            } else if warnings > 0 {
                "completed_with_warnings"
            } else {
                "completed"
            };
            client.execute("UPDATE catalog_jobs SET status=$2,failed_count=$3,completed_at=now() WHERE id=$1", &[&job_id,&status,&(failed+warnings)]).await?;
            client
                .execute(
                    "UPDATE catalog_anime_import_jobs SET lease_until=NULL,active_import_key=NULL WHERE job_id=$1",
                    &[&job_id],
                )
                .await?;
        }
        Ok(())
    }

    pub async fn release_anime_import_job_lease(&self, job_id: &str) -> Result<(), CatalogError> {
        self.client.lock().await.execute("UPDATE catalog_anime_import_jobs SET lease_until=NULL,updated_at=now() WHERE job_id=$1 AND EXISTS (SELECT 1 FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='pending')", &[&job_id]).await?;
        Ok(())
    }

    pub async fn fail_anime_import_episode(
        &self,
        job_id: &str,
        ordinal: i64,
        code: &str,
    ) -> Result<(), CatalogError> {
        let client = self.client.lock().await;
        client.execute("UPDATE catalog_anime_import_episodes SET attempt_count=attempt_count+1,last_error_code=$3,status=CASE WHEN attempt_count+1 >= 5 THEN 'failed' ELSE 'pending' END,updated_at=now() WHERE job_id=$1 AND ordinal=$2 AND status='pending'", &[&job_id,&ordinal,&code]).await?;
        client.execute("UPDATE catalog_anime_import_jobs SET last_progress_at=now(),updated_at=now(),lease_until=now()+interval '2 minutes',failure_summary=$2 WHERE job_id=$1", &[&job_id,&format!("episode checkpoint error: {code}")]).await?;
        client.execute("UPDATE catalog_anime_import_jobs SET processed_episodes=(SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status IN ('completed','failed')) WHERE job_id=$1", &[&job_id]).await?;
        let pending: i64 = client.query_one("SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='pending'", &[&job_id]).await?.get(0);
        if pending == 0 {
            let failed: i64 = client.query_one("SELECT count(*) FROM catalog_anime_import_episodes WHERE job_id=$1 AND status='failed'", &[&job_id]).await?.get(0);
            client.execute("UPDATE catalog_jobs SET status='failed',failed_count=$2,completed_at=now() WHERE id=$1", &[&job_id,&failed]).await?;
            client
                .execute(
                    "UPDATE catalog_anime_import_jobs SET lease_until=NULL,active_import_key=NULL WHERE job_id=$1",
                    &[&job_id],
                )
                .await?;
        }
        Ok(())
    }
    pub async fn search(
        &self,
        q: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        let q = q.trim();
        if q.is_empty() {
            return Err(CatalogError::Validation("q must not be empty".into()));
        }
        let p = format!("%{}%", normalize(q));
        let rows=self.client.lock().await.query("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE normalized_title LIKE $1 AND catalog_status <> 'deleted' ORDER BY canonical_title LIMIT $2 OFFSET $3",&[&p,&limit.clamp(1,100),&offset.max(0)]).await?;
        Ok(rows.iter().map(title_from_row).collect())
    }
    pub async fn enqueue_pending_import(&self, q: &str) -> Result<(), CatalogError> {
        let n = normalize(q);
        if n.is_empty() {
            return Err(CatalogError::Validation("query must not be empty".into()));
        }
        self.client.lock().await.execute("INSERT INTO pending_import_requests(normalized_query,candidate_title) VALUES($1,$2) ON CONFLICT(normalized_query) DO UPDATE SET request_count=pending_import_requests.request_count+1,last_requested_at=now(),status=CASE WHEN pending_import_requests.status='processed' THEN 'pending' ELSE pending_import_requests.status END",&[&n,&q.trim()]).await?;
        Ok(())
    }
    pub async fn list_titles(
        &self,
        kind: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        let l = limit.clamp(1, 100);
        let o = offset.max(0);
        let rows=match kind{Some(k)=>self.client.lock().await.query("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE content_type=$1 AND catalog_status='active' ORDER BY updated_at DESC LIMIT $2 OFFSET $3",&[&k,&l,&o]).await?,None=>self.client.lock().await.query("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE catalog_status='active' ORDER BY updated_at DESC LIMIT $1 OFFSET $2",&[&l,&o]).await?};
        Ok(rows.iter().map(title_from_row).collect())
    }
    pub async fn get_title(&self, id: &str) -> Result<Option<CatalogTitle>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE id::text=$1 AND catalog_status <> 'deleted'",&[&id]).await?;
        Ok(r.as_ref().map(title_from_row))
    }
    pub async fn find_title_by_provider(
        &self,
        provider_key: &str,
        provider_title_id: &str,
    ) -> Result<Option<CatalogTitle>, CatalogError> {
        let r = self.client.lock().await.query_opt(
            "SELECT ct.id::text,ct.mal_id,ct.canonical_title,ct.normalized_title,ct.content_type,ct.synopsis,ct.release_year,ct.poster_url,ct.catalog_status FROM catalog_titles ct JOIN provider_titles pt ON pt.title_id=ct.id JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pr.provider_key=$1 AND pt.provider_title_id=$2 AND ct.catalog_status <> 'deleted' LIMIT 1",
            &[&provider_key, &provider_title_id],
        ).await?;
        Ok(r.as_ref().map(title_from_row))
    }
    pub async fn verified_provider_aliases(
        &self,
        mappings: &[(String, String)],
    ) -> Result<HashMap<String, Vec<String>>, CatalogError> {
        if mappings.is_empty() {
            return Ok(HashMap::new());
        }
        let client = self.client.lock().await;
        let mut aliases = HashMap::new();
        let mut provider_keys = Vec::new();
        let mut provider_title_ids = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for (provider_key, provider_title_id) in mappings {
            if seen.insert((provider_key.clone(), provider_title_id.clone())) {
                provider_keys.push(provider_key.clone());
                provider_title_ids.push(provider_title_id.clone());
            }
        }
        let rows = client
            .query(
                "SELECT DISTINCT pr.provider_key, pt.provider_title_id, ata.title FROM title_alternative_titles ata JOIN provider_titles pt ON pt.title_id=ata.title_id JOIN provider_registry pr ON pr.id=pt.provider_id JOIN UNNEST($1::text[], $2::text[]) AS requested(provider_key, provider_title_id) ON requested.provider_key=pr.provider_key AND requested.provider_title_id=pt.provider_title_id WHERE pt.match_status='verified' ORDER BY pr.provider_key, pt.provider_title_id, ata.title",
                &[&provider_keys, &provider_title_ids],
            )
            .await?;
        for row in rows {
            let provider_key: String = row.get("provider_key");
            let provider_title_id: String = row.get("provider_title_id");
            let title: String = row.get("title");
            if !title.trim().is_empty() {
                aliases
                    .entry(format!("{provider_key}:{provider_title_id}"))
                    .or_insert_with(Vec::new)
                    .push(title);
            }
        }
        Ok(aliases)
    }
    pub async fn get_title_detail(&self, id: &str) -> Result<Option<CatalogDetail>, CatalogError> {
        let c = self.client.lock().await;
        let Some(r)=c.query_opt("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE id::text=$1 AND catalog_status <> 'deleted'",&[&id]).await? else{return Ok(None)};
        let tid: String = r.get("id");
        let alternatives=c.query("SELECT id::text AS id,title,language,title_type,source FROM title_alternative_titles WHERE title_id=$1::text::uuid ORDER BY title", &[&tid]).await?.iter().map(row_json).collect();
        let genres=c.query("SELECT g.id::text AS id,g.canonical_name,g.normalized_name,tg.source FROM genres g JOIN title_genres tg ON tg.genre_id=g.id WHERE tg.title_id=$1::text::uuid ORDER BY g.normalized_name", &[&tid]).await?.iter().map(row_json).collect();
        let providers=c.query("SELECT pt.id::text AS id,pr.provider_key,pr.display_name,pt.provider_title_id,pt.provider_title,pt.provider_url,pt.metadata,pt.match_status,pt.match_confidence FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pt.title_id=$1::text::uuid ORDER BY pr.provider_key", &[&tid]).await?.iter().map(row_json).collect();
        let seasons=c.query("SELECT id::text AS id,season_number,canonical_label,episode_count_reported FROM seasons WHERE title_id=$1::text::uuid ORDER BY season_number", &[&tid]).await?.iter().map(row_json).collect();
        let title = title_from_row(&r);
        let sources = if title.content_type == "movie" {
            c.query("SELECT ps.id::text AS id,ps.title_id::text AS title_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.last_error_code,ps.expires_at::text AS expires_at,(ps.expires_at IS NULL OR ps.expires_at>now()) AS source_not_expired FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.title_id=$1::text::uuid AND ps.source_status NOT IN('blocked','invalid') AND (ps.expires_at IS NULL OR ps.expires_at>now()) ORDER BY ps.resolution_height DESC NULLS LAST", &[&tid]).await?.iter().map(row_json).filter(source_is_playback_eligible).collect()
        } else {
            Vec::new()
        };
        Ok(Some(CatalogDetail {
            title,
            alternative_titles: alternatives,
            genres,
            provider_mappings: providers,
            seasons,
            sources,
        }))
    }
    pub async fn list_genres(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogGenre>, CatalogError> {
        let r=self.client.lock().await.query("SELECT id::text,canonical_name,normalized_name FROM genres ORDER BY normalized_name LIMIT $1 OFFSET $2",&[&limit.clamp(1,100),&offset.max(0)]).await?;
        Ok(r.iter()
            .map(|x| CatalogGenre {
                id: x.get("id"),
                canonical_name: x.get("canonical_name"),
                normalized_name: x.get("normalized_name"),
            })
            .collect())
    }
    pub async fn list_seasons(&self, title_id: &str) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT id::text AS id,season_number,canonical_label,episode_count_reported FROM seasons WHERE title_id=$1::text::uuid ORDER BY season_number",&[&title_id]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn list_episodes(
        &self,
        title_id: &str,
        season: i32,
    ) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT e.id::text AS id,e.episode_number,e.episode_type,e.episode_coordinate,e.absolute_episode_number,e.special_identifier,e.episode_title,e.overview,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id=$1::text::uuid AND s.season_number=$2 ORDER BY e.episode_number",&[&title_id,&season]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn get_episode(&self, id: &str) -> Result<Option<Value>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT e.id::text AS id,e.title_id::text AS title_id,e.season_id::text AS season_id,e.episode_number,e.episode_type,e.episode_coordinate,e.absolute_episode_number,e.special_identifier,e.episode_title,e.overview,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.id::text=$1", &[&id]).await?;
        Ok(r.as_ref().map(row_json))
    }
    pub async fn list_sources(&self, id: &str) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT ps.id::text AS id,ps.episode_id::text AS episode_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.expires_at::text AS expires_at FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.episode_id=$1::text::uuid AND ps.source_status NOT IN('blocked','invalid') AND (ps.expires_at IS NULL OR ps.expires_at>now()) ORDER BY ps.resolution_height DESC NULLS LAST", &[&id]).await?;
        Ok(r.iter()
            .map(row_json)
            .filter(source_is_playback_eligible)
            .collect())
    }
    pub async fn get_source(&self, id: &str) -> Result<Option<Value>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT ps.id::text AS id,ps.episode_id::text AS episode_id,ps.title_id::text AS title_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.last_error_code,ps.expires_at::text AS expires_at,(ps.expires_at IS NULL OR ps.expires_at>now()) AS source_not_expired FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.id::text=$1", &[&id]).await?;
        Ok(r.as_ref().map(|row| {
            let mut source = row_json(row);
            let eligible = source_is_playback_eligible(&source);
            if let Some(object) = source.as_object_mut() {
                object.remove("source_not_expired");
                object.insert("playback_eligible".into(), Value::Bool(eligible));
            }
            source
        }))
    }

    pub(crate) async fn claim_codec_verification_job(
        &self,
        lease_seconds: i32,
    ) -> Result<Option<CodecVerificationJob>, CatalogError> {
        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        let candidate = transaction
            .query_opt(
                "SELECT ps.id::text AS source_id,ps.provider_source_id,ps.source_identity_fingerprint,COALESCE(pt.provider_title_id,epm.provider_episode_id) AS provider_content_id,CASE WHEN ps.title_id IS NOT NULL THEN 'movie' ELSE ct.content_type END AS content_type FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id LEFT JOIN episode_provider_mappings epm ON epm.episode_id=ps.episode_id AND epm.provider_id=ps.provider_id LEFT JOIN episodes e ON e.id=ps.episode_id LEFT JOIN catalog_titles ct ON ct.id=COALESCE(ps.title_id,e.title_id) LEFT JOIN provider_titles pt ON pt.title_id=ps.title_id AND pt.provider_id=ps.provider_id LEFT JOIN catalog_codec_verification_jobs j ON j.source_id=ps.id WHERE pr.provider_key='fourkhdhub' AND ps.source_status IN ('active','playable') AND ps.codec_verification_status='unverified' AND (ps.expires_at IS NULL OR ps.expires_at>now()) AND ((ps.episode_id IS NOT NULL AND epm.provider_episode_id IS NOT NULL) OR (ps.title_id IS NOT NULL AND ct.content_type='movie' AND pt.provider_title_id IS NOT NULL AND pt.match_status='verified')) AND (j.source_id IS NULL OR (j.status='pending' AND j.next_attempt_at<=now()) OR (j.status='running' AND j.lease_until<=now())) ORDER BY COALESCE(j.next_attempt_at,ps.created_at),ps.created_at LIMIT 1 FOR UPDATE OF ps SKIP LOCKED",
                &[],
            )
            .await?;
        let Some(candidate) = candidate else {
            transaction.commit().await?;
            return Ok(None);
        };
        let source_id: String = candidate.get("source_id");
        let provider_source_id: String = candidate.get("provider_source_id");
        let identity_fingerprint: String = candidate.get("source_identity_fingerprint");
        let provider_content_id: String = candidate.get("provider_content_id");
        let content_type: String = candidate.get("content_type");
        let lease = transaction
            .query_one(
                "INSERT INTO catalog_codec_verification_jobs(source_id,status,attempt_count,next_attempt_at,lease_until,lease_token,updated_at) VALUES($1::text::uuid,'running',1,now(),now()+make_interval(secs=>$2),gen_random_uuid(),now()) ON CONFLICT(source_id) DO UPDATE SET status='running',attempt_count=catalog_codec_verification_jobs.attempt_count+1,lease_until=EXCLUDED.lease_until,lease_token=EXCLUDED.lease_token,last_error_code=NULL,updated_at=now() RETURNING lease_token,attempt_count",
                &[&source_id, &lease_seconds],
            )
            .await?;
        let job = CodecVerificationJob {
            source_id,
            provider_source_id,
            provider_content_id,
            content_type,
            identity_fingerprint,
            lease_token: lease.get("lease_token"),
            attempt_count: lease.get("attempt_count"),
        };
        transaction.commit().await?;
        Ok(Some(job))
    }

    pub(crate) async fn complete_codec_verification(
        &self,
        job: &CodecVerificationJob,
        codec: &str,
        blocked: bool,
    ) -> Result<(), CatalogError> {
        let normalized = codec.trim().to_ascii_lowercase();
        if (blocked && !crate::codec_verification::is_hevc_codec(&normalized))
            || (!blocked && !crate::codec_verification::is_allowed_codec(&normalized))
        {
            return Err(CatalogError::Validation(
                "codec verification result is not permitted by policy".into(),
            ));
        }
        let verification_status = if blocked { "blocked" } else { "verified" };
        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        let updated = transaction
            .execute(
                "UPDATE provider_sources SET video_codec=$4,codec_verification_status=$5,last_validated_at=now(),last_error_code=NULL,updated_at=now() WHERE id=$1::text::uuid AND provider_source_id=$2 AND source_identity_fingerprint=$3 AND source_status IN ('active','playable') AND codec_verification_status='unverified' AND (expires_at IS NULL OR expires_at>now()) AND (($6='movie' AND provider_sources.title_id IS NOT NULL AND EXISTS(SELECT 1 FROM provider_titles pt WHERE pt.title_id=provider_sources.title_id AND pt.provider_id=provider_sources.provider_id AND pt.provider_title_id=$7 AND pt.match_status='verified')) OR ($6<>'movie' AND provider_sources.episode_id IS NOT NULL AND EXISTS(SELECT 1 FROM episode_provider_mappings epm WHERE epm.episode_id=provider_sources.episode_id AND epm.provider_id=provider_sources.provider_id AND epm.provider_episode_id=$7))) AND EXISTS(SELECT 1 FROM catalog_codec_verification_jobs j WHERE j.source_id=provider_sources.id AND j.status='running' AND j.lease_token=$8 AND j.lease_until>now())",
                &[&job.source_id, &job.provider_source_id, &job.identity_fingerprint, &normalized, &verification_status, &job.content_type, &job.provider_content_id, &job.lease_token],
            )
            .await?;
        if updated == 1 {
            transaction
                .execute(
                    "UPDATE catalog_codec_verification_jobs SET status='completed',lease_until=NULL,lease_token=NULL,last_error_code=NULL,updated_at=now() WHERE source_id=$1::text::uuid AND lease_token=$2",
                    &[&job.source_id, &job.lease_token],
                )
                .await?;
        } else {
            requeue_or_close_stale_job(&transaction, job).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn fail_codec_verification(
        &self,
        job: &CodecVerificationJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), CatalogError> {
        let code = if error_code.len() <= 64
            && error_code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            error_code
        } else {
            "VERIFICATION_FAILED"
        };
        let will_retry = retryable && job.attempt_count < 3;
        let backoff = if job.attempt_count <= 1 { 15 } else { 60 };
        let status = if will_retry { "pending" } else { "failed" };
        let mut client = self.client.lock().await;
        let transaction = client.transaction().await?;
        let updated = transaction
            .execute(
                "UPDATE provider_sources SET last_error_code=$4 WHERE id=$1::text::uuid AND provider_source_id=$2 AND source_identity_fingerprint=$3 AND source_status IN ('active','playable') AND codec_verification_status='unverified' AND (expires_at IS NULL OR expires_at>now()) AND (($5='movie' AND provider_sources.title_id IS NOT NULL AND EXISTS(SELECT 1 FROM provider_titles pt WHERE pt.title_id=provider_sources.title_id AND pt.provider_id=provider_sources.provider_id AND pt.provider_title_id=$6 AND pt.match_status='verified')) OR ($5<>'movie' AND provider_sources.episode_id IS NOT NULL AND EXISTS(SELECT 1 FROM episode_provider_mappings epm WHERE epm.episode_id=provider_sources.episode_id AND epm.provider_id=provider_sources.provider_id AND epm.provider_episode_id=$6))) AND EXISTS(SELECT 1 FROM catalog_codec_verification_jobs j WHERE j.source_id=provider_sources.id AND j.status='running' AND j.lease_token=$7 AND j.lease_until>now())",
                &[&job.source_id, &job.provider_source_id, &job.identity_fingerprint, &code, &job.content_type, &job.provider_content_id, &job.lease_token],
            )
            .await?;
        if updated == 1 {
            transaction
                .execute(
                    "UPDATE catalog_codec_verification_jobs SET status=$3,lease_until=NULL,lease_token=NULL,next_attempt_at=CASE WHEN $3='pending' THEN now()+make_interval(secs=>$4) ELSE now() END,last_error_code=$5,updated_at=now() WHERE source_id=$1::text::uuid AND lease_token=$2",
                    &[&job.source_id, &job.lease_token, &status, &backoff, &code],
                )
                .await?;
        } else {
            requeue_or_close_stale_job(&transaction, job).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    pub async fn list_tracks(&self, id: &str) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT id::text AS id,track_type,language_code,codec,format,is_default,is_forced,verified,track_metadata FROM source_tracks WHERE source_id=$1::text::uuid ORDER BY track_type,language_code",&[&id]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn upsert(&self, input: TitleUpsert) -> Result<(CatalogTitle, bool), CatalogError> {
        validate(&input.canonical_title, &input.content_type)?;
        let c = self.client.lock().await;
        upsert_title(&c, &input).await
    }
    pub async fn upsert_unified_anime(
        &self,
        input: UnifiedAnimeInput,
    ) -> Result<String, CatalogError> {
        validate_unified_anime(&input)?;
        let mut client = self.client.lock().await;
        let tx = client.transaction().await?;
        let result = upsert_unified_anime_in_tx(&tx, &input).await?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn import_title(&self, input: ImportTitle) -> Result<ImportSummary, CatalogError> {
        validate(&input.canonical_title, &input.content_type)?;
        let c = self.client.lock().await;
        c.batch_execute("BEGIN").await?;
        let result = import_in_tx(&c, &input).await;
        match result {
            Ok(s) => {
                c.batch_execute("COMMIT").await?;
                Ok(s)
            }
            Err(e) => {
                let _ = c.batch_execute("ROLLBACK").await;
                Err(e)
            }
        }
    }
    pub async fn create_job(&self, kind: &str) -> Result<CatalogJob, CatalogError> {
        let id = new_id();
        self.client.lock().await.execute("INSERT INTO catalog_jobs(id,job_type,status,started_at) VALUES($1,$2,'running',now())",&[&id,&kind]).await?;
        Ok(empty_job(id, kind.into()))
    }
    pub async fn finish_job(&self, j: &CatalogJob) -> Result<(), CatalogError> {
        self.client.lock().await.execute("UPDATE catalog_jobs SET status=$2,completed_at=now(),inserted_count=$3,updated_count=$4,skipped_count=$5,failed_count=$6,error_summary=$7 WHERE id=$1",&[&j.id,&j.status,&j.inserted_count,&j.updated_count,&j.skipped_count,&j.failed_count,&format!("titles={},seasons={},episodes={},sources={},tracks={}",j.titles_imported,j.seasons_imported,j.episodes_imported,j.sources_imported,j.tracks_imported)]).await?;
        Ok(())
    }
    pub async fn get_job(&self, id: &str) -> Result<Option<CatalogJob>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT c.id,c.job_type,c.status,c.inserted_count,c.updated_count,c.skipped_count,c.failed_count,c.error_summary,c.started_at::text AS started_at,c.completed_at::text AS completed_at,a.anime_name,a.total_episodes,a.processed_episodes,a.source_warning_count,a.batch_size,a.failure_summary,a.last_progress_at::text AS last_progress_at,(SELECT count(*) FROM catalog_anime_import_episodes e WHERE e.job_id=c.id AND e.status='pending') AS pending_episodes,(SELECT count(*) FROM catalog_anime_import_episodes e WHERE e.job_id=c.id AND e.status='failed') AS failed_episodes FROM catalog_jobs c LEFT JOIN catalog_anime_import_jobs a ON a.job_id=c.id WHERE c.id=$1",&[&id]).await?;
        Ok(r.map(|r| {
            let text: String = r
                .get::<_, Option<String>>("error_summary")
                .unwrap_or_default();
            let n = |k: &str| {
                text.split(&format!("{k}="))
                    .nth(1)
                    .and_then(|x| x.split(',').next())
                    .and_then(|x| x.parse().ok())
                    .unwrap_or(0)
            };
            CatalogJob {
                id: r.get("id"),
                job_type: r.get("job_type"),
                status: r.get("status"),
                inserted_count: r.get("inserted_count"),
                updated_count: r.get("updated_count"),
                skipped_count: r.get("skipped_count"),
                failed_count: r.get("failed_count"),
                titles_imported: n("titles"),
                seasons_imported: n("seasons"),
                episodes_imported: n("episodes"),
                sources_imported: n("sources"),
                tracks_imported: n("tracks"),
                anime_title: r.get("anime_name"),
                total_episodes: r.get("total_episodes"),
                processed_episodes: r.get("processed_episodes"),
                pending_episodes: r.get("pending_episodes"),
                failed_episodes: r.get("failed_episodes"),
                source_warning_count: r.get("source_warning_count"),
                batch_size: r.get("batch_size"),
                progress_percent: r.get::<_, Option<i64>>("total_episodes").map(|total| {
                    if total <= 0 {
                        100
                    } else {
                        ((r.get::<_, i64>("processed_episodes") * 100 / total).clamp(0, 100)) as i32
                    }
                }),
                resumable: r
                    .get::<_, Option<i64>>("pending_episodes")
                    .map(|pending| pending > 0),
                failure_summary: r.get("failure_summary"),
                started_at: r.get("started_at"),
                last_progress_at: r.get("last_progress_at"),
                completed_at: r.get("completed_at"),
            }
        }))
    }
    pub async fn inspect(
        &self,
        q: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        match q.filter(|x| !x.trim().is_empty()) {
            Some(x) => self.search(x, limit, offset).await,
            None => self.list_titles(None, limit, offset).await,
        }
    }
}
fn nonempty_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EpisodeIdentity {
    episode_type: String,
    coordinate: String,
    identity_key: String,
}

fn episode_identity(
    episode_number: i32,
    episode_type: Option<&str>,
    episode_coordinate: Option<&str>,
    absolute_episode_number: Option<i32>,
    special_identifier: Option<&str>,
) -> Result<EpisodeIdentity, CatalogError> {
    if episode_number <= 0 {
        return Err(CatalogError::Validation(
            "episode_number must be positive".into(),
        ));
    }
    let kind = nonempty_text(episode_type)
        .unwrap_or("regular")
        .to_ascii_lowercase();
    if kind.len() > 64
        || !kind
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(CatalogError::Validation("invalid episode_type".into()));
    }
    let coordinate = nonempty_text(episode_coordinate)
        .map(str::to_owned)
        .or_else(|| absolute_episode_number.map(|n| n.to_string()))
        .or_else(|| nonempty_text(special_identifier).map(str::to_owned))
        .unwrap_or_else(|| episode_number.to_string());
    if coordinate.len() > 128
        || coordinate.contains(':')
        || coordinate.chars().any(char::is_whitespace)
    {
        return Err(CatalogError::Validation(
            "invalid episode coordinate".into(),
        ));
    }
    if absolute_episode_number.is_some_and(|n| n <= 0) {
        return Err(CatalogError::Validation(
            "absolute_episode_number must be positive".into(),
        ));
    }
    let special = nonempty_text(special_identifier).unwrap_or("");
    if special.len() > 128 || special.contains(':') || special.chars().any(char::is_whitespace) {
        return Err(CatalogError::Validation(
            "invalid special_identifier".into(),
        ));
    }
    Ok(EpisodeIdentity {
        identity_key: format!("{kind}:{coordinate}"),
        episode_type: kind,
        coordinate,
    })
}

fn validate_unified_anime(input: &UnifiedAnimeInput) -> Result<(), CatalogError> {
    if !matches!(input.content_type.as_str(), "anime" | "movie") {
        return Err(CatalogError::Validation(
            "invalid unified content_type".into(),
        ));
    }
    validate(&input.canonical_title, &input.content_type)?;
    if input
        .release_year
        .is_some_and(|year| !(1800..=3000).contains(&year))
    {
        return Err(CatalogError::Validation(
            "release_year is out of range".into(),
        ));
    }
    if let Some(status) = nonempty_text(input.catalog_status.as_deref()) {
        if !matches!(status, "active" | "pending" | "disabled" | "deleted") {
            return Err(CatalogError::Validation("invalid catalog_status".into()));
        }
    }
    for provider in &input.provider_titles {
        if provider.provider_key.trim().is_empty()
            || provider.provider_key.chars().count() > 200
            || provider.provider_title_id.trim().is_empty()
            || provider.provider_title_id.chars().count() > 500
        {
            return Err(CatalogError::Validation(
                "invalid provider title mapping".into(),
            ));
        }
    }
    for season in &input.seasons {
        if season.season_number < 0 || season.episode_count_reported.is_some_and(|n| n < 0) {
            return Err(CatalogError::Validation("invalid season metadata".into()));
        }
        for provider_season in &season.provider_seasons {
            if provider_season.provider_key.trim().is_empty()
                || (nonempty_text(provider_season.provider_season_id.as_deref()).is_none()
                    && provider_season.provider_season_number.is_none())
                || provider_season
                    .provider_season_number
                    .is_some_and(|n| n < 0)
            {
                return Err(CatalogError::Validation(
                    "invalid provider season mapping".into(),
                ));
            }
        }
        for episode in &season.episodes {
            let _identity = episode_identity(
                episode.episode_number,
                episode.episode_type.as_deref(),
                episode.episode_coordinate.as_deref(),
                episode.absolute_episode_number,
                episode.special_identifier.as_deref(),
            )?;
            if let Some(date) = nonempty_text(episode.release_date.as_deref()) {
                let bytes = date.as_bytes();
                if bytes.len() != 10
                    || bytes[4] != b'-'
                    || bytes[7] != b'-'
                    || !bytes
                        .iter()
                        .enumerate()
                        .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
                {
                    return Err(CatalogError::Validation(
                        "release_date must use YYYY-MM-DD".into(),
                    ));
                }
            }
            for provider_episode in &episode.provider_episodes {
                if provider_episode.provider_key.trim().is_empty()
                    || provider_episode.provider_key.chars().count() > 200
                    || provider_episode.provider_episode_id.trim().is_empty()
                    || provider_episode.provider_episode_id.chars().count() > 500
                    || provider_episode
                        .provider_episode_number
                        .is_some_and(|n| n <= 0)
                {
                    return Err(CatalogError::Validation(
                        "invalid provider episode mapping".into(),
                    ));
                }
            }
            for lookup in &episode.provider_lookups {
                if lookup.provider_key.trim().is_empty()
                    || lookup.provider_key.chars().count() > 200
                    || lookup.provider_title_id.trim().is_empty()
                    || lookup.provider_title_id.chars().count() > 500
                    || lookup.provider_season_number < 0
                    || lookup.provider_episode_number <= 0
                    || lookup.lookup_reference.trim().is_empty()
                    || lookup.lookup_reference.chars().count() > 2000
                    || !matches!(lookup.mapping_status.as_str(), "resolved" | "unresolved")
                {
                    return Err(CatalogError::Validation(
                        "invalid provider episode lookup mapping".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn ensure_unified_provider(
    tx: &tokio_postgres::Transaction<'_>,
    provider_key: &str,
    display_name: Option<&str>,
) -> Result<Uuid, CatalogError> {
    let display_name = nonempty_text(display_name).unwrap_or(provider_key);
    let row = tx.query_one(
        "INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,false) ON CONFLICT(provider_key) DO UPDATE SET display_name=CASE WHEN provider_registry.display_name='' THEN EXCLUDED.display_name ELSE provider_registry.display_name END RETURNING id::text",
        &[&provider_key, &display_name],
    ).await?;
    parse_uuid(&row.get::<_, String>(0))
}

async fn upsert_unified_anime_in_tx(
    tx: &tokio_postgres::Transaction<'_>,
    input: &UnifiedAnimeInput,
) -> Result<String, CatalogError> {
    let normalized = crate::unified::normalize_anime_title(&input.canonical_title).key;
    let lock_scope = format!(
        "{}:{}",
        input.content_type,
        input
            .release_year
            .map_or_else(|| "unknown".to_string(), |year| year.to_string())
    );
    // Serialize find-or-create for the same normalized identity across backend
    // instances; the lock is transaction-scoped and does not alter schema.
    tx.query_one(
        "SELECT pg_advisory_xact_lock(hashtext($1),hashtext($2))",
        &[&normalized, &lock_scope],
    )
    .await?;

    // A provider identity is stronger evidence than title text. Check every
    // supplied identity before using the conservative normalized-title match.
    let mut matched_id: Option<String> = None;
    for provider in &input.provider_titles {
        if let Some(row) = tx.query_opt(
            "SELECT pt.title_id::text AS id FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pr.provider_key=$1 AND pt.provider_title_id=$2 LIMIT 1",
            &[&provider.provider_key, &provider.provider_title_id],
        ).await? {
            matched_id = Some(row.get("id"));
            break;
        }
    }
    if matched_id.is_none() {
        'episode_mappings: for season in &input.seasons {
            for episode in &season.episodes {
                for mapping in &episode.provider_episodes {
                    if let Some(row) = tx.query_opt(
                        "SELECT e.title_id::text AS id FROM episode_provider_mappings epm JOIN provider_registry pr ON pr.id=epm.provider_id JOIN episodes e ON e.id=epm.episode_id WHERE pr.provider_key=$1 AND epm.provider_episode_id=$2 LIMIT 1",
                        &[&mapping.provider_key, &mapping.provider_episode_id],
                    ).await? {
                        matched_id = Some(row.get("id"));
                        break 'episode_mappings;
                    }
                }
            }
        }
    }
    if matched_id.is_none() {
        let provider_keys = input
            .provider_titles
            .iter()
            .map(|provider| provider.provider_key.clone())
            .collect::<Vec<_>>();
        let row = tx.query_opt(
            "SELECT ct.id::text AS id FROM catalog_titles ct WHERE ct.normalized_title=$1 AND ct.content_type=$2 AND ct.catalog_status<>'deleted' AND $3::int IS NOT NULL AND ct.release_year=$3 AND NOT EXISTS (SELECT 1 FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pt.title_id=ct.id AND pr.provider_key=ANY($4::text[])) ORDER BY ct.updated_at DESC LIMIT 1 FOR UPDATE",
            &[&normalized, &input.content_type, &input.release_year, &provider_keys],
        ).await?;
        matched_id = row.map(|row| row.get("id"));
    }

    let title_id = if let Some(id) = matched_id {
        let synopsis = nonempty_text(input.synopsis.as_deref());
        let poster_url = nonempty_text(input.poster_url.as_deref());
        let status = nonempty_text(input.catalog_status.as_deref());
        let row = tx.query_one(
            "UPDATE catalog_titles SET canonical_title=CASE WHEN canonical_title='' THEN $2 ELSE canonical_title END,normalized_title=CASE WHEN canonical_title='' THEN $3 ELSE normalized_title END,synopsis=COALESCE(NULLIF($4,''),synopsis),release_year=COALESCE($5,release_year),poster_url=COALESCE(NULLIF($6,''),poster_url),catalog_status=COALESCE($7,catalog_status),metadata_source='unified_import',metadata_updated_at=now(),updated_at=now() WHERE id=$1::text::uuid RETURNING id::text",
            &[&id, &input.canonical_title, &normalized, &synopsis, &input.release_year, &poster_url, &status],
        ).await?;
        row.get::<_, String>(0)
    } else {
        let synopsis = nonempty_text(input.synopsis.as_deref());
        let poster_url = nonempty_text(input.poster_url.as_deref());
        let status = nonempty_text(input.catalog_status.as_deref()).unwrap_or("active");
        let row = tx.query_one(
            "INSERT INTO catalog_titles(canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status,metadata_source,metadata_updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,'unified_import',now()) RETURNING id::text",
            &[&input.canonical_title, &normalized, &input.content_type, &synopsis, &input.release_year, &poster_url, &status],
        ).await?;
        row.get::<_, String>(0)
    };
    let title_uuid = parse_uuid(&title_id)?;

    for provider in &input.provider_titles {
        let provider_id =
            ensure_unified_provider(tx, &provider.provider_key, provider.display_name.as_deref())
                .await?;
        let provider_title = nonempty_text(provider.provider_title.as_deref());
        let provider_url = nonempty_text(provider.provider_url.as_deref());
        let metadata = if provider.metadata.is_null()
            || provider
                .metadata
                .as_object()
                .is_some_and(|object| object.is_empty())
        {
            serde_json::json!({})
        } else {
            provider.metadata.clone()
        };
        let updated = tx.execute(
            "INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,provider_url,metadata,match_status,last_checked_at) VALUES($1,$2,$3,$4,$5,$6,'unverified',CASE WHEN $7 THEN now() ELSE NULL END) ON CONFLICT(provider_id,provider_title_id) DO UPDATE SET provider_title=COALESCE(NULLIF(EXCLUDED.provider_title,''),provider_titles.provider_title),provider_url=COALESCE(NULLIF(EXCLUDED.provider_url,''),provider_titles.provider_url),metadata=CASE WHEN EXCLUDED.metadata='{}'::jsonb THEN provider_titles.metadata ELSE EXCLUDED.metadata END,last_checked_at=CASE WHEN $7 THEN now() ELSE provider_titles.last_checked_at END,updated_at=now() WHERE provider_titles.title_id=EXCLUDED.title_id",
            &[&title_uuid, &provider_id, &provider.provider_title_id, &provider_title, &provider_url, &metadata, &provider.last_successful_resolution],
        ).await?;
        if updated == 0 {
            return Err(CatalogError::Validation(
                "provider title mapping already belongs to a different canonical title".into(),
            ));
        }
    }

    for season in &input.seasons {
        let label = nonempty_text(season.canonical_label.as_deref());
        let row = tx.query_one(
            "INSERT INTO seasons(title_id,season_number,canonical_label,episode_count_reported) VALUES($1,$2,$3,$4) ON CONFLICT(title_id,season_number) DO UPDATE SET canonical_label=COALESCE(NULLIF(EXCLUDED.canonical_label,''),seasons.canonical_label),episode_count_reported=COALESCE(EXCLUDED.episode_count_reported,seasons.episode_count_reported),updated_at=now() RETURNING id::text",
            &[&title_uuid, &season.season_number, &label, &season.episode_count_reported],
        ).await?;
        let season_uuid = parse_uuid(&row.get::<_, String>(0))?;

        for mapping in &season.provider_seasons {
            let provider_id = ensure_unified_provider(tx, &mapping.provider_key, None).await?;
            let provider_season_id = nonempty_text(mapping.provider_season_id.as_deref());
            let metadata = if mapping.metadata.is_null()
                || mapping
                    .metadata
                    .as_object()
                    .is_some_and(|object| object.is_empty())
            {
                serde_json::json!({})
            } else {
                mapping.metadata.clone()
            };
            tx.execute(
                "INSERT INTO provider_season_mappings(season_id,provider_id,provider_season_id,provider_season_number,metadata) VALUES($1,$2,$3,$4,$5) ON CONFLICT(provider_id,season_id) DO UPDATE SET provider_season_id=COALESCE(NULLIF(EXCLUDED.provider_season_id,''),provider_season_mappings.provider_season_id),provider_season_number=COALESCE(EXCLUDED.provider_season_number,provider_season_mappings.provider_season_number),metadata=CASE WHEN EXCLUDED.metadata='{}'::jsonb THEN provider_season_mappings.metadata ELSE provider_season_mappings.metadata||EXCLUDED.metadata END,updated_at=now()",
                &[&season_uuid, &provider_id, &provider_season_id, &mapping.provider_season_number, &metadata],
            ).await?;
        }

        for episode in &season.episodes {
            let identity = episode_identity(
                episode.episode_number,
                episode.episode_type.as_deref(),
                episode.episode_coordinate.as_deref(),
                episode.absolute_episode_number,
                episode.special_identifier.as_deref(),
            )?;
            let episode_title = nonempty_text(episode.episode_title.as_deref());
            let overview = nonempty_text(episode.overview.as_deref());
            let release_date = nonempty_text(episode.release_date.as_deref());
            let thumbnail_url = nonempty_text(episode.thumbnail_url.as_deref());
            let existing = tx.query_opt(
                "SELECT id::text AS id,episode_title FROM episodes WHERE title_id=$1 AND season_id=$2 AND episode_type=$3 AND identity_key=$4 FOR UPDATE",
                &[&title_uuid, &season_uuid, &identity.episode_type, &identity.identity_key],
            ).await?;
            let (episode_id, conflict) = if let Some(existing) = existing {
                let existing_title: Option<String> = existing.get("episode_title");
                let conflict = !crate::unified::episode_titles_compatible(
                    existing_title.as_deref(),
                    episode_title,
                );
                let id: String = existing.get("id");
                if !conflict {
                    tx.execute(
                        "UPDATE episodes SET episode_title=COALESCE(NULLIF($2,''),episode_title),overview=COALESCE(NULLIF($3,''),overview),release_date=COALESCE(NULLIF($4::text,'')::date,release_date),thumbnail_url=COALESCE(NULLIF($5,''),thumbnail_url),updated_at=now() WHERE id=$1::text::uuid",
                        &[&id, &episode_title, &overview, &release_date, &thumbnail_url],
                    ).await?;
                }
                (id, conflict)
            } else {
                let row = tx.query_one(
                    "INSERT INTO episodes(title_id,season_id,episode_number,episode_type,episode_coordinate,absolute_episode_number,special_identifier,identity_key,episode_title,overview,release_date,thumbnail_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11::text,'')::date,$12) RETURNING id::text",
                    &[&title_uuid, &season_uuid, &episode.episode_number, &identity.episode_type, &identity.coordinate, &episode.absolute_episode_number, &episode.special_identifier, &identity.identity_key, &episode_title, &overview, &release_date, &thumbnail_url],
                ).await?;
                (row.get::<_, String>(0), false)
            };
            let episode_uuid = parse_uuid(&episode_id)?;

            for mapping in &episode.provider_episodes {
                let provider_id = ensure_unified_provider(tx, &mapping.provider_key, None).await?;
                let provider_metadata = if mapping.metadata.is_null() {
                    serde_json::json!({})
                } else {
                    mapping.metadata.clone()
                };
                let metadata = serde_json::json!({
                    "season_number": season.season_number,
                    "episode_number": episode.episode_number,
                    "episode_title": episode_title,
                    "overview": overview,
                    "release_date": release_date,
                    "thumbnail_url": thumbnail_url,
                    "provider_metadata": provider_metadata
                });
                let mapping_status = if conflict { "unresolved" } else { "resolved" };
                tx.execute(
                    "INSERT INTO episode_provider_mappings(episode_id,provider_id,provider_episode_id,provider_episode_number,mapping_status,metadata) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(provider_id,provider_episode_id) DO UPDATE SET episode_id=CASE WHEN episode_provider_mappings.episode_id=EXCLUDED.episode_id THEN episode_provider_mappings.episode_id ELSE episode_provider_mappings.episode_id END,provider_episode_number=CASE WHEN episode_provider_mappings.episode_id=EXCLUDED.episode_id THEN COALESCE(EXCLUDED.provider_episode_number,episode_provider_mappings.provider_episode_number) ELSE episode_provider_mappings.provider_episode_number END,mapping_status=CASE WHEN episode_provider_mappings.episode_id<>EXCLUDED.episode_id OR episode_provider_mappings.mapping_status='unresolved' OR EXCLUDED.mapping_status='unresolved' THEN 'unresolved' ELSE 'resolved' END,metadata=episode_provider_mappings.metadata||EXCLUDED.metadata",
                    &[&episode_uuid, &provider_id, &mapping.provider_episode_id, &mapping.provider_episode_number, &mapping_status, &metadata],
                ).await?;
            }
            for lookup in &episode.provider_lookups {
                let provider_id = ensure_unified_provider(tx, &lookup.provider_key, None).await?;
                let metadata = if lookup.metadata.is_null() {
                    serde_json::json!({})
                } else {
                    lookup.metadata.clone()
                };
                let conflicting_lookup = tx.query_opt(
                    "SELECT episode_id FROM episode_provider_lookup_mappings WHERE provider_id=$1 AND provider_title_id=$2 AND provider_season_number=$3 AND provider_episode_number=$4 AND episode_id<>$5 FOR UPDATE",
                    &[&provider_id, &lookup.provider_title_id, &lookup.provider_season_number, &lookup.provider_episode_number, &episode_uuid],
                ).await?;
                let effective_status = if conflicting_lookup.is_some() {
                    "unresolved"
                } else {
                    lookup.mapping_status.as_str()
                };
                tx.execute(
                    "INSERT INTO episode_provider_lookup_mappings(episode_id,provider_id,provider_title_id,provider_season_number,provider_episode_number,lookup_reference,mapping_status,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(provider_id,provider_title_id,provider_season_number,provider_episode_number,episode_id) DO UPDATE SET lookup_reference=EXCLUDED.lookup_reference,mapping_status=CASE WHEN episode_provider_lookup_mappings.mapping_status='unresolved' OR EXCLUDED.mapping_status='unresolved' THEN 'unresolved' ELSE 'resolved' END,metadata=episode_provider_lookup_mappings.metadata||EXCLUDED.metadata,updated_at=now()",
                    &[&episode_uuid, &provider_id, &lookup.provider_title_id, &lookup.provider_season_number, &lookup.provider_episode_number, &lookup.lookup_reference, &effective_status, &metadata],
                ).await?;
            }
        }
    }
    Ok(title_id)
}

async fn requeue_or_close_stale_job(
    transaction: &tokio_postgres::Transaction<'_>,
    job: &CodecVerificationJob,
) -> Result<(), CatalogError> {
    let still_eligible: bool = transaction
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM provider_sources WHERE id=$1::text::uuid AND source_status IN ('active','playable') AND codec_verification_status='unverified' AND (expires_at IS NULL OR expires_at>now()))",
            &[&job.source_id],
        )
        .await?
        .get(0);
    let next_status = if still_eligible {
        "pending"
    } else {
        "completed"
    };
    let attempts = if still_eligible {
        0_i32
    } else {
        job.attempt_count
    };
    transaction
        .execute(
            "UPDATE catalog_codec_verification_jobs SET status=$3,attempt_count=$4,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error_code=CASE WHEN $3='pending' THEN NULL ELSE 'SOURCE_NO_LONGER_ELIGIBLE' END,updated_at=now() WHERE source_id=$1::text::uuid AND lease_token=$2",
            &[&job.source_id, &job.lease_token, &next_status, &attempts],
        )
        .await?;
    Ok(())
}

fn empty_job(id: String, kind: String) -> CatalogJob {
    CatalogJob {
        id,
        job_type: kind,
        status: "running".into(),
        inserted_count: 0,
        updated_count: 0,
        skipped_count: 0,
        failed_count: 0,
        titles_imported: 0,
        seasons_imported: 0,
        episodes_imported: 0,
        sources_imported: 0,
        tracks_imported: 0,
        anime_title: None,
        total_episodes: None,
        processed_episodes: None,
        pending_episodes: None,
        failed_episodes: None,
        source_warning_count: None,
        batch_size: None,
        progress_percent: None,
        resumable: None,
        failure_summary: None,
        started_at: None,
        last_progress_at: None,
        completed_at: None,
    }
}
async fn upsert_title(c: &Client, i: &TitleUpsert) -> Result<(CatalogTitle, bool), CatalogError> {
    let n = normalize_catalog_title(&i.canonical_title, &i.content_type);
    let st = i.catalog_status.clone().unwrap_or_else(|| "active".into());
    let r=c.query_one("INSERT INTO catalog_titles(mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status,metadata_source,metadata_updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'import',now()) ON CONFLICT(mal_id) WHERE mal_id IS NOT NULL DO UPDATE SET canonical_title=EXCLUDED.canonical_title,normalized_title=EXCLUDED.normalized_title,content_type=EXCLUDED.content_type,synopsis=EXCLUDED.synopsis,release_year=EXCLUDED.release_year,poster_url=EXCLUDED.poster_url,catalog_status=EXCLUDED.catalog_status,metadata_updated_at=now(),updated_at=now() RETURNING id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status,(xmax=0) AS inserted",&[&i.mal_id,&i.canonical_title,&n,&i.content_type,&i.synopsis,&i.release_year,&i.poster_url,&st]).await?;
    Ok((title_from_row(&r), r.get("inserted")))
}
async fn import_in_tx(c: &Client, i: &ImportTitle) -> Result<ImportSummary, CatalogError> {
    let existing_provider_title = if i.mal_id.is_none() {
        if let Some(provider) = i.providers.first() {
            c.query_opt(
                "SELECT pt.title_id::text AS id FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pr.provider_key=$1 AND pt.provider_title_id=$2",
                &[&provider.provider_key, &provider.provider_title_id],
            ).await?
        } else {
            None
        }
    } else {
        None
    };
    let existing_compatible_title = if existing_provider_title.is_none() && i.mal_id.is_none() {
        let normalized = normalize_catalog_title(&i.canonical_title, &i.content_type);
        let provider_keys = i
            .providers
            .iter()
            .map(|provider| provider.provider_key.clone())
            .collect::<Vec<_>>();
        c.query_opt(
            "SELECT ct.id::text AS id FROM catalog_titles ct WHERE ct.normalized_title=$1 AND ct.content_type=$2 AND ct.catalog_status <> 'deleted' AND (($2='anime' AND $3::int IS NOT NULL AND ct.release_year=$3) OR ($2<>'anime' AND ($3::int IS NULL OR ct.release_year IS NULL OR ct.release_year=$3))) AND NOT EXISTS (SELECT 1 FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pt.title_id=ct.id AND pr.provider_key=ANY($4::text[])) ORDER BY (ct.release_year IS NULL), ct.updated_at DESC LIMIT 1",
            &[&normalized, &i.content_type, &i.release_year, &provider_keys],
        )
        .await?
    } else {
        None
    };
    let existing_title = existing_provider_title.or(existing_compatible_title);
    let (t, ins) = if let Some(existing) = existing_title {
        let id: String = existing.get("id");
        let normalized = normalize_catalog_title(&i.canonical_title, &i.content_type);
        let status = i.catalog_status.clone().unwrap_or_else(|| "active".into());
        let updated = c.query_one(
            "UPDATE catalog_titles SET canonical_title=$2,normalized_title=$3,content_type=$4,synopsis=$5,release_year=$6,poster_url=$7,catalog_status=$8,metadata_source='import',metadata_updated_at=now(),updated_at=now() WHERE id=$1::text::uuid RETURNING id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status",
            &[&id,&i.canonical_title,&normalized,&i.content_type,&i.synopsis,&i.release_year,&i.poster_url,&status],
        ).await?;
        (title_from_row(&updated), false)
    } else {
        upsert_title(
            c,
            &TitleUpsert {
                mal_id: i.mal_id,
                canonical_title: i.canonical_title.clone(),
                content_type: i.content_type.clone(),
                synopsis: i.synopsis.clone(),
                release_year: i.release_year,
                poster_url: i.poster_url.clone(),
                catalog_status: i.catalog_status.clone(),
            },
        )
        .await?
    };
    let title_uuid = parse_uuid(&t.id)?;
    let mut s = ImportSummary {
        inserted: ins as i64,
        updated: (!ins) as i64,
        ..Default::default()
    };
    for a in &i.alternative_titles {
        if a.title.trim().is_empty() {
            continue;
        }
        c.execute("INSERT INTO title_alternative_titles(title_id,title,normalized_title,language,title_type,source) VALUES($1,$2,$3,$4,$5,'import') ON CONFLICT DO NOTHING",&[&title_uuid,&a.title,&normalize(&a.title),&a.language,&a.title_type]).await?;
    }
    for g in &i.genres {
        if g.trim().is_empty() {
            continue;
        }
        let r=c.query_one("INSERT INTO genres(canonical_name,normalized_name) VALUES($1,$2) ON CONFLICT(normalized_name) DO UPDATE SET canonical_name=EXCLUDED.canonical_name RETURNING id::text",&[&g,&normalize(g)]).await?;
        let gid: String = r.get(0);
        c.execute("INSERT INTO title_genres(title_id,genre_id,source) VALUES($1,$2::text::uuid,'import') ON CONFLICT DO NOTHING",&[&title_uuid,&gid]).await?;
    }
    for season in &i.seasons {
        let sr=c.query_one("INSERT INTO seasons(title_id,season_number,canonical_label,episode_count_reported) VALUES($1,$2,$3,$4) ON CONFLICT(title_id,season_number) DO UPDATE SET canonical_label=EXCLUDED.canonical_label,episode_count_reported=EXCLUDED.episode_count_reported,updated_at=now() RETURNING id::text",&[&title_uuid,&season.season_number,&season.canonical_label,&season.episode_count_reported]).await?;
        let sid: String = sr.get(0);
        let sid_uuid = parse_uuid(&sid)?;
        s.seasons += 1;
        for e in &season.episodes {
            let identity = episode_identity(
                e.episode_number,
                e.episode_type.as_deref(),
                e.episode_coordinate.as_deref(),
                e.absolute_episode_number,
                e.special_identifier.as_deref(),
            )?;
            let existing_episode = c
                .query_opt(
                    "SELECT episode_title FROM episodes WHERE title_id=$1 AND season_id=$2 AND episode_type=$3 AND identity_key=$4",
                    &[&title_uuid, &sid_uuid, &identity.episode_type, &identity.identity_key],
                )
                .await?;
            if let Some(existing_episode) = existing_episode {
                let existing_title: Option<String> = existing_episode.get("episode_title");
                if !crate::unified::episode_titles_compatible(
                    existing_title.as_deref(),
                    e.episode_title.as_deref(),
                ) {
                    return Err(CatalogError::Validation(format!(
                        "conflicting titles for anime season {} episode {}",
                        season.season_number, e.episode_number
                    )));
                }
            }
            let inserted=c.query_opt("INSERT INTO episodes(title_id,season_id,episode_number,episode_type,episode_coordinate,absolute_episode_number,special_identifier,identity_key,episode_title,overview,release_date,thumbnail_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::date,$12) ON CONFLICT DO NOTHING RETURNING id::text",&[&title_uuid,&sid_uuid,&e.episode_number,&identity.episode_type,&identity.coordinate,&e.absolute_episode_number,&e.special_identifier,&identity.identity_key,&e.episode_title,&e.overview,&e.release_date,&e.thumbnail_url]).await?;
            let eid: String = if let Some(row) = inserted {
                row.get(0)
            } else {
                c.query_one("SELECT id::text FROM episodes WHERE title_id=$1 AND season_id=$2 AND episode_type=$3 AND identity_key=$4", &[&title_uuid, &sid_uuid, &identity.episode_type, &identity.identity_key]).await?.get(0)
            };
            s.episodes += 1;
            for pe in &e.provider_episode_ids {
                let provider_key = episode_provider_key(pe)?;
                let provider = c
                    .query_one(
                        "INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,true) ON CONFLICT(provider_key) DO UPDATE SET display_name=EXCLUDED.display_name,updated_at=now() RETURNING id::text",
                        &[
                            &provider_key,
                            &pe.display_name
                                .clone()
                                .unwrap_or_else(|| provider_key.to_string()),
                        ],
                    )
                    .await?;
                let provider_id: String = provider.get(0);
                map_episode(c, &eid, pe, provider_key, Some(&provider_id), &mut s).await?;
            }
        }
    }
    for p in &i.providers {
        let pr=c.query_one("INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,true) ON CONFLICT(provider_key) DO UPDATE SET display_name=EXCLUDED.display_name,updated_at=now() RETURNING id::text",&[&p.provider_key,&p.display_name.clone().unwrap_or_else(||p.provider_key.clone())]).await?;
        let pid: String = pr.get(0);
        let pid_uuid = parse_uuid(&pid)?;
        let metadata = if p.metadata.is_null() {
            Value::Object(Default::default())
        } else {
            p.metadata.clone()
        };
        let updated = c.execute("INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,provider_url,metadata,match_status,last_checked_at) VALUES($1,$2,$3,$4,$5,$6,'verified',now()) ON CONFLICT(provider_id,provider_title_id) DO UPDATE SET provider_title=EXCLUDED.provider_title,provider_url=EXCLUDED.provider_url,metadata=EXCLUDED.metadata,match_status='verified',updated_at=now() WHERE provider_titles.title_id=EXCLUDED.title_id",&[&title_uuid,&pid_uuid,&p.provider_title_id,&p.provider_title,&p.provider_url,&metadata]).await?;
        if updated == 0 {
            return Err(CatalogError::Validation(
                "provider title mapping already belongs to a different canonical title".into(),
            ));
        }
        if t.content_type == "movie" {
            map_movie_sources(
                c,
                &title_uuid,
                &p.sources,
                &p.provider_key,
                &pid_uuid,
                &mut s,
            )
            .await?;
        } else if !p.sources.is_empty() {
            return Err(CatalogError::Validation(
                "provider title sources are only valid for movie titles".into(),
            ));
        }
        for pe in &p.episodes {
            let season = pe.season_number.unwrap_or(1);
            let ep = pe.provider_episode_number.unwrap_or(1);
            let r=c.query_opt("SELECT e.id::text FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id=$1 AND s.season_number=$2 AND e.episode_number=$3",&[&title_uuid,&season,&ep]).await?;
            if let Some(r) = r {
                let eid: String = r.get(0);
                map_episode(c, &eid, pe, &p.provider_key, Some(&pid), &mut s).await?;
            }
        }
    }
    Ok(s)
}
async fn map_movie_sources(
    c: &Client,
    title_id: &Uuid,
    sources: &[ProviderSourceInput],
    provider_key: &str,
    provider_id: &Uuid,
    summary: &mut ImportSummary,
) -> Result<(), CatalogError> {
    if !sources.is_empty() {
        let current_source_ids = sources
            .iter()
            .map(|source| source.provider_source_id.clone())
            .collect::<Vec<_>>();
        c.execute(
            "UPDATE provider_sources SET source_status='invalid',updated_at=now() WHERE title_id=$1 AND provider_id=$2 AND source_status NOT IN ('blocked','invalid') AND NOT (provider_source_id=ANY($3::text[]))",
            &[title_id, provider_id, &current_source_ids],
        )
        .await?;
    }
    for source in sources {
        ensure_source_owner(
            c,
            provider_id,
            &source.provider_source_id,
            None,
            Some(title_id),
        )
        .await?;
        let identity_fingerprint =
            source_identity_fingerprint(provider_key, provider_id, "movie", title_id, source);
        let incoming_status = import_source_status(source.source_status.as_deref());
        let row = c
            .query_one(
                "INSERT INTO provider_sources(episode_id,title_id,provider_id,provider_source_id,source_reference,quality_label,video_codec,audio_codec,container_format,resolution_width,resolution_height,source_status,codec_verification_status,expires_at,source_identity_fingerprint) VALUES(NULL,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'unverified',$12::text::timestamptz,$13) ON CONFLICT(provider_id,provider_source_id) DO UPDATE SET episode_id=NULL,title_id=EXCLUDED.title_id,source_reference=EXCLUDED.source_reference,quality_label=EXCLUDED.quality_label,video_codec=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint AND provider_sources.codec_verification_status IN ('verified','blocked') THEN provider_sources.video_codec ELSE EXCLUDED.video_codec END,audio_codec=EXCLUDED.audio_codec,container_format=EXCLUDED.container_format,resolution_width=EXCLUDED.resolution_width,resolution_height=EXCLUDED.resolution_height,source_status=CASE WHEN provider_sources.source_status IN ('blocked','invalid') THEN provider_sources.source_status ELSE EXCLUDED.source_status END,codec_verification_status=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint AND provider_sources.codec_verification_status IN ('verified','blocked') THEN provider_sources.codec_verification_status ELSE 'unverified' END,last_validated_at=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint THEN provider_sources.last_validated_at ELSE NULL END,last_error_code=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint THEN provider_sources.last_error_code ELSE NULL END,expires_at=EXCLUDED.expires_at,source_identity_fingerprint=EXCLUDED.source_identity_fingerprint,updated_at=now() RETURNING id::text,source_status,codec_verification_status,(expires_at IS NULL OR expires_at>now()) AS source_not_expired",
                &[title_id, provider_id, &source.provider_source_id, &source.source_reference, &source.quality, &source.codec, &source.audio_codec, &source.container, &source.resolution_width, &source.resolution_height, &incoming_status, &source.expires_at, &identity_fingerprint],
            )
            .await?;
        let source_uuid = parse_uuid(&row.get::<_, String>("id"))?;
        let stored_status: String = row.get("source_status");
        let verification_status: String = row.get("codec_verification_status");
        let not_expired: bool = row.get("source_not_expired");
        if provider_key == "fourkhdhub"
            && matches!(stored_status.as_str(), "active" | "playable")
            && verification_status == "unverified"
            && not_expired
        {
            c.execute(
                "INSERT INTO catalog_codec_verification_jobs(source_id,status,attempt_count,next_attempt_at,updated_at) VALUES($1,'pending',0,now(),now()) ON CONFLICT(source_id) DO UPDATE SET status='pending',attempt_count=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error_code=NULL,updated_at=now() WHERE catalog_codec_verification_jobs.status<>'running",
                &[&source_uuid],
            )
            .await?;
        }
        c.execute(
            "DELETE FROM source_tracks WHERE source_id=$1",
            &[&source_uuid],
        )
        .await?;
        for (kind, tracks) in [
            ("audio", &source.audio_tracks),
            ("subtitle", &source.subtitle_tracks),
        ] {
            for track in tracks {
                let language_code = track
                    .language_code
                    .as_deref()
                    .and_then(crate::unified::normalize_language_code);
                let inserted = c.execute("INSERT INTO source_tracks(source_id,track_type,language_code,codec,format,is_default,is_forced,verified,track_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING", &[&source_uuid,&kind,&language_code,&track.codec,&track.format,&track.is_default,&track.is_forced,&track.verified,&track.metadata]).await?;
                summary.tracks += i64::from(inserted > 0);
            }
        }
        summary.sources += 1;
    }
    Ok(())
}

async fn ensure_source_owner(
    c: &Client,
    provider_id: &Uuid,
    provider_source_id: &str,
    episode_id: Option<&Uuid>,
    title_id: Option<&Uuid>,
) -> Result<(), CatalogError> {
    if let Some(existing) = c
        .query_opt(
            "SELECT episode_id::text AS episode_id,title_id::text AS title_id FROM provider_sources WHERE provider_id=$1 AND provider_source_id=$2",
            &[provider_id, &provider_source_id],
        )
        .await?
    {
        let existing_episode: Option<String> = existing.get("episode_id");
        let existing_title: Option<String> = existing.get("title_id");
        if existing_episode.as_deref() != episode_id.map(Uuid::to_string).as_deref()
            || existing_title.as_deref() != title_id.map(Uuid::to_string).as_deref()
        {
            return Err(CatalogError::Validation(
                "provider source is already associated with different catalog content".into(),
            ));
        }
    }
    Ok(())
}

async fn map_episode(
    c: &Client,
    eid: &str,
    pe: &ProviderEpisodeInput,
    provider_key: &str,
    pid: Option<&String>,
    s: &mut ImportSummary,
) -> Result<(), CatalogError> {
    let pid = match pid {
        Some(x) => parse_uuid(x)?,
        None => return Ok(()),
    };
    let eid = parse_uuid(eid)?;
    let inserted = c.execute("INSERT INTO episode_provider_mappings(episode_id,provider_id,provider_episode_id,provider_episode_number,mapping_status) VALUES($1,$2,$3,$4,'resolved') ON CONFLICT(provider_id,provider_episode_id) DO UPDATE SET provider_episode_number=EXCLUDED.provider_episode_number WHERE episode_provider_mappings.episode_id=EXCLUDED.episode_id",&[&eid,&pid,&pe.provider_episode_id,&pe.provider_episode_number]).await?;
    if inserted == 0 {
        return Err(CatalogError::Validation(
            "provider episode mapping already belongs to a different canonical episode".into(),
        ));
    }
    if !pe.sources.is_empty() {
        let current_source_ids = pe
            .sources
            .iter()
            .map(|source| source.provider_source_id.clone())
            .collect::<Vec<_>>();
        c.execute(
            "UPDATE provider_sources SET source_status='invalid',updated_at=now() WHERE episode_id=$1 AND provider_id=$2 AND source_status NOT IN ('blocked','invalid') AND NOT (provider_source_id=ANY($3::text[]))",
            &[&eid, &pid, &current_source_ids],
        )
        .await?;
    }
    for src in &pe.sources {
        ensure_source_owner(c, &pid, &src.provider_source_id, Some(&eid), None).await?;
        let identity_fingerprint =
            source_identity_fingerprint(provider_key, &pid, "episode", &eid, src);
        let incoming_status = import_source_status(src.source_status.as_deref());
        let r = c
            .query_one(
                "INSERT INTO provider_sources(episode_id,title_id,provider_id,provider_source_id,source_reference,quality_label,video_codec,audio_codec,container_format,resolution_width,resolution_height,source_status,codec_verification_status,expires_at,source_identity_fingerprint) VALUES($1,NULL,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'unverified',$12::text::timestamptz,$13) ON CONFLICT(provider_id,provider_source_id) DO UPDATE SET episode_id=EXCLUDED.episode_id,title_id=NULL,source_reference=EXCLUDED.source_reference,quality_label=EXCLUDED.quality_label,video_codec=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint AND provider_sources.codec_verification_status IN ('verified','blocked') THEN provider_sources.video_codec ELSE EXCLUDED.video_codec END,audio_codec=EXCLUDED.audio_codec,container_format=EXCLUDED.container_format,resolution_width=EXCLUDED.resolution_width,resolution_height=EXCLUDED.resolution_height,source_status=CASE WHEN provider_sources.source_status IN ('blocked','invalid') THEN provider_sources.source_status ELSE EXCLUDED.source_status END,codec_verification_status=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint AND provider_sources.codec_verification_status IN ('verified','blocked') THEN provider_sources.codec_verification_status ELSE 'unverified' END,last_validated_at=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint THEN provider_sources.last_validated_at ELSE NULL END,last_error_code=CASE WHEN provider_sources.source_identity_fingerprint=EXCLUDED.source_identity_fingerprint THEN provider_sources.last_error_code ELSE NULL END,expires_at=EXCLUDED.expires_at,source_identity_fingerprint=EXCLUDED.source_identity_fingerprint,updated_at=now() RETURNING id::text,source_status,codec_verification_status,(expires_at IS NULL OR expires_at>now()) AS source_not_expired",
                &[&eid,&pid,&src.provider_source_id,&src.source_reference,&src.quality,&src.codec,&src.audio_codec,&src.container,&src.resolution_width,&src.resolution_height,&incoming_status,&src.expires_at,&identity_fingerprint],
            )
            .await?;
        let sid: String = r.get("id");
        let stored_status: String = r.get("source_status");
        let verification_status: String = r.get("codec_verification_status");
        let not_expired: bool = r.get("source_not_expired");
        let sid = parse_uuid(&sid)?;
        if provider_key == "fourkhdhub"
            && matches!(stored_status.as_str(), "active" | "playable")
            && verification_status == "unverified"
            && not_expired
        {
            c.execute(
                "INSERT INTO catalog_codec_verification_jobs(source_id,status,attempt_count,next_attempt_at,updated_at) VALUES($1,'pending',0,now(),now()) ON CONFLICT(source_id) DO UPDATE SET status='pending',attempt_count=0,next_attempt_at=now(),lease_until=NULL,lease_token=NULL,last_error_code=NULL,updated_at=now() WHERE catalog_codec_verification_jobs.status<>'running'",
                &[&sid],
            )
            .await?;
        }
        c.execute("DELETE FROM source_tracks WHERE source_id=$1", &[&sid])
            .await?;
        for (kind, tracks) in [
            ("audio", &src.audio_tracks),
            ("subtitle", &src.subtitle_tracks),
        ] {
            for tr in tracks {
                let language_code = tr
                    .language_code
                    .as_deref()
                    .and_then(crate::unified::normalize_language_code);
                let params: [&(dyn tokio_postgres::types::ToSql + Sync); 9] = [
                    &sid,
                    &kind,
                    &language_code,
                    &tr.codec,
                    &tr.format,
                    &tr.is_default,
                    &tr.is_forced,
                    &tr.verified,
                    &tr.metadata,
                ];
                let inserted = c.execute("INSERT INTO source_tracks(source_id,track_type,language_code,codec,format,is_default,is_forced,verified,track_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING", &params).await?;
                s.tracks += i64::from(inserted > 0);
            }
        }
        s.sources += 1;
    }
    Ok(())
}

fn import_source_status(status: Option<&str>) -> &'static str {
    match status {
        Some("active") => "active",
        Some("playable") => "playable",
        Some("blocked") => "blocked",
        Some("invalid") => "invalid",
        Some("unverified") => "unverified",
        _ => "unverified",
    }
}

fn source_identity_fingerprint(
    provider_key: &str,
    provider_id: &Uuid,
    owner_kind: &str,
    owner_id: &Uuid,
    source: &ProviderSourceInput,
) -> String {
    let identity = serde_json::to_vec(&(
        provider_key,
        provider_id.to_string(),
        owner_kind,
        owner_id.to_string(),
        &source.provider_source_id,
        &source.source_reference,
        &source.quality,
        &source.codec,
        &source.audio_codec,
        &source.container,
        source.resolution_width,
        source.resolution_height,
        &source.expires_at,
    ))
    .expect("source identity fields serialize");
    let digest = Sha256::digest(identity);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        fingerprint.push(HEX[(byte >> 4) as usize] as char);
        fingerprint.push(HEX[(byte & 0x0f) as usize] as char);
    }
    fingerprint
}

fn parse_uuid(value: &str) -> Result<Uuid, CatalogError> {
    Uuid::parse_str(value)
        .map_err(|_| CatalogError::Validation("database returned an invalid UUID".into()))
}

fn title_from_row(r: &Row) -> CatalogTitle {
    CatalogTitle {
        id: r.get("id"),
        id_type: "catalog_title",
        mal_id: r.get("mal_id"),
        canonical_title: r.get("canonical_title"),
        normalized_title: r.get("normalized_title"),
        content_type: r.get("content_type"),
        synopsis: r.get("synopsis"),
        release_year: r.get("release_year"),
        poster_url: r.get("poster_url"),
        catalog_status: r.get("catalog_status"),
    }
}
fn row_json(r: &Row) -> Value {
    let mut o = serde_json::Map::new();
    for col in r.columns() {
        let n = col.name();
        let v = if let Ok(x) = r.try_get::<_, String>(n) {
            Value::String(x)
        } else if let Ok(x) = r.try_get::<_, Option<String>>(n) {
            x.map(Value::String).unwrap_or(Value::Null)
        } else if let Ok(x) = r.try_get::<_, i32>(n) {
            Value::from(x)
        } else if let Ok(x) = r.try_get::<_, Option<i32>>(n) {
            x.map(Value::from).unwrap_or(Value::Null)
        } else if let Ok(x) = r.try_get::<_, bool>(n) {
            Value::from(x)
        } else if let Ok(x) = r.try_get::<_, Option<bool>>(n) {
            x.map(Value::from).unwrap_or(Value::Null)
        } else if let Ok(x) = r.try_get::<_, Value>(n) {
            x
        } else {
            Value::Null
        };
        o.insert(n.into(), v);
    }
    Value::Object(o)
}
fn validate(t: &str, k: &str) -> Result<(), CatalogError> {
    if t.trim().is_empty() || t.chars().count() > 300 {
        return Err(CatalogError::Validation(
            "canonical_title must contain 1..300 characters".into(),
        ));
    }
    if !matches!(k, "anime" | "movie" | "k_drama") {
        return Err(CatalogError::Validation(
            "content_type must be anime, movie, or k_drama".into(),
        ));
    }
    Ok(())
}
fn episode_provider_key(input: &ProviderEpisodeInput) -> Result<&str, CatalogError> {
    input
        .provider_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .ok_or_else(|| {
            CatalogError::Validation(
                "episode provider mappings require a non-empty provider_key".into(),
            )
        })
}
fn source_codec_is_known_non_hevc(codec: &str) -> bool {
    crate::codec_verification::is_allowed_codec(codec)
}
fn source_is_playback_eligible(source: &Value) -> bool {
    if source
        .get("source_not_expired")
        .is_some_and(|value| value != &Value::Bool(true))
    {
        return false;
    }
    let status = source
        .get("source_status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !matches!(status, "active" | "playable") {
        return false;
    }
    if source
        .get("codec_verification_status")
        .and_then(Value::as_str)
        != Some("verified")
    {
        return false;
    }
    source
        .get("video_codec")
        .and_then(Value::as_str)
        .is_some_and(source_codec_is_known_non_hevc)
}
fn normalize_catalog_title(value: &str, content_type: &str) -> String {
    if matches!(content_type, "anime" | "movie") {
        crate::unified::normalize_anime_title(value).key
    } else {
        normalize(value)
    }
}

fn normalize(v: &str) -> String {
    v.trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
fn new_id() -> String {
    format!(
        "{:032x}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
}
#[cfg(test)]
mod tests {
    use super::{
        ProviderEpisodeInput, ProviderMappingStatus, ProviderSourceInput, ReconciliationDecision,
        episode_identity, episode_provider_key, mapping_status_transition, normalize,
        reconcile_provider_identity, source_codec_is_known_non_hevc, source_identity_fingerprint,
        source_is_playback_eligible, validate_transition_evidence,
    };
    use serde_json::json;

    #[test]
    fn normalizes_search_text() {
        assert_eq!(normalize("  One   Piece "), "one piece");
    }

    #[test]
    fn mapping_lifecycle_allows_safe_transitions_and_rejects_revival() {
        assert!(mapping_status_transition(
            ProviderMappingStatus::Unverified,
            ProviderMappingStatus::Verified
        ));
        assert!(mapping_status_transition(
            ProviderMappingStatus::Verified,
            ProviderMappingStatus::Stale
        ));
        assert!(mapping_status_transition(
            ProviderMappingStatus::Stale,
            ProviderMappingStatus::Verified
        ));
        assert!(!mapping_status_transition(
            ProviderMappingStatus::Rejected,
            ProviderMappingStatus::Verified
        ));
    }

    #[test]
    fn reconciliation_requires_strong_identity_evidence() {
        assert_eq!(
            reconcile_provider_identity(
                true,
                true,
                "Example",
                "Example",
                Some(2024),
                Some(2024),
                "anime",
                "anime",
                false
            ),
            ReconciliationDecision::VerifiedReplacement
        );
        assert!(matches!(
            reconcile_provider_identity(
                true,
                true,
                "Example",
                "Other",
                Some(2024),
                Some(2024),
                "anime",
                "anime",
                false
            ),
            ReconciliationDecision::Unresolved { .. }
        ));
        assert!(matches!(
            reconcile_provider_identity(
                true,
                true,
                "Example",
                "Example",
                Some(2024),
                Some(2024),
                "anime",
                "movie",
                false
            ),
            ReconciliationDecision::Unresolved { .. }
        ));
        assert!(matches!(
            reconcile_provider_identity(
                true,
                true,
                "Example",
                "Example",
                Some(2024),
                Some(2024),
                "anime",
                "anime",
                true
            ),
            ReconciliationDecision::Unresolved { .. }
        ));
    }

    #[test]
    fn provider_source_candidates_fail_closed_for_hevc_and_unknown_codecs() {
        assert!(source_codec_is_known_non_hevc("h264"));
        assert!(source_codec_is_known_non_hevc("H.264 / AVC"));
        assert!(!source_codec_is_known_non_hevc("AV1"));
        assert!(!source_codec_is_known_non_hevc("hevc"));
        assert!(!source_codec_is_known_non_hevc("mystery-codec"));
        assert!(!source_codec_is_known_non_hevc("not-h264"));
    }

    #[test]
    fn ordinary_import_payload_cannot_set_codec_verification_state() {
        let source: ProviderSourceInput = serde_json::from_value(json!({
            "provider_source_id": "source-1",
            "codec": "h264",
            "codec_verification_status": "verified"
        }))
        .unwrap();
        assert_eq!(source.codec_verification_status, None);
    }

    #[test]
    fn source_identity_fingerprint_changes_with_media_mapping() {
        let provider_id = uuid::Uuid::new_v4();
        let episode_id = uuid::Uuid::new_v4();
        let source = ProviderSourceInput {
            provider_source_id: "opaque-release-id".into(),
            source_reference: Some("provider-ref-a".into()),
            quality: Some("1080p".into()),
            codec: Some("h264".into()),
            ..Default::default()
        };
        let original = source_identity_fingerprint(
            "fourkhdhub",
            &provider_id,
            "episode",
            &episode_id,
            &source,
        );
        assert_eq!(
            original,
            source_identity_fingerprint(
                "fourkhdhub",
                &provider_id,
                "episode",
                &episode_id,
                &source
            )
        );
        assert_ne!(
            original,
            source_identity_fingerprint("fourkhdhub", &provider_id, "movie", &episode_id, &source)
        );
        let changed = ProviderSourceInput {
            source_reference: Some("provider-ref-b".into()),
            ..source
        };
        assert_ne!(
            original,
            source_identity_fingerprint(
                "fourkhdhub",
                &provider_id,
                "episode",
                &episode_id,
                &changed
            )
        );
    }
    #[test]
    fn catalog_playback_requires_verified_non_hevc_codec() {
        let base = serde_json::json!({
            "source_status": "playable",
            "source_not_expired": true,
            "codec_verification_status": "verified",
            "video_codec": "H.264"
        });
        assert!(source_is_playback_eligible(&base));

        let mut unverified = base.clone();
        unverified["codec_verification_status"] = serde_json::json!("unverified");
        assert!(!source_is_playback_eligible(&unverified));

        let mut hevc = base.clone();
        hevc["video_codec"] = serde_json::json!("H.265 / HEVC");
        assert!(!source_is_playback_eligible(&hevc));

        let mut blocked = base;
        blocked["source_status"] = serde_json::json!("blocked");
        assert!(!source_is_playback_eligible(&blocked));

        let expired = serde_json::json!({
            "source_status": "playable",
            "source_not_expired": false,
            "codec_verification_status": "verified",
            "video_codec": "H.264"
        });
        assert!(!source_is_playback_eligible(&expired));
    }

    #[test]
    fn canonical_episode_provider_mapping_requires_a_provider_key() {
        let mut mapping: ProviderEpisodeInput = serde_json::from_value(json!({
            "provider_episode_id": "provider-episode-1"
        }))
        .unwrap();
        assert!(episode_provider_key(&mapping).is_err());

        mapping.provider_key = Some("  example-provider  ".into());
        assert_eq!(episode_provider_key(&mapping).unwrap(), "example-provider");

        mapping.provider_key = Some("   ".into());
        assert!(episode_provider_key(&mapping).is_err());
    }

    #[test]
    fn typed_episode_identity_separates_coordinates_and_types_without_float_matching() {
        let regular = episode_identity(1, None, None, None, None).unwrap();
        let decimal = episode_identity(1, Some("regular"), Some("1.5"), None, None).unwrap();
        let special = episode_identity(1, Some("special"), None, None, Some("special-1")).unwrap();
        let absolute = episode_identity(1, Some("regular"), None, Some(101), None).unwrap();
        let movie = episode_identity(1, Some("movie"), Some("feature-a"), None, None).unwrap();
        assert_eq!(regular.identity_key, "regular:1");
        assert_eq!(decimal.identity_key, "regular:1.5");
        assert_ne!(regular.identity_key, special.identity_key);
        assert_eq!(absolute.coordinate, "101");
        assert_eq!(movie.identity_key, "movie:feature-a");
        assert!(episode_identity(1, Some("regular"), Some("1:2"), None, None).is_err());
    }

    #[test]
    fn lifecycle_evidence_rejects_empty_or_sensitive_metadata() {
        assert!(
            validate_transition_evidence(
                "verified by detail lookup",
                &json!({"source": "details"})
            )
            .is_ok()
        );
        assert!(validate_transition_evidence("", &json!({})).is_err());
        assert!(
            validate_transition_evidence("verified", &json!({"api_token": "redacted"})).is_err()
        );
        assert!(validate_transition_evidence("verified", &serde_json::Value::Null).is_err());
    }
}
