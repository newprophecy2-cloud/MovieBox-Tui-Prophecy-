use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
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
    pub episode_title: Option<String>,
    pub overview: Option<String>,
    pub release_date: Option<String>,
    pub thumbnail_url: Option<String>,
    #[serde(default)]
    pub provider_episode_ids: Vec<ProviderEpisodeInput>,
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
        let client = self.client.lock().await;
        client.batch_execute(MIGRATION_001).await?;
        client.batch_execute(MIGRATION_002).await?;
        client.batch_execute(MIGRATION_003).await?;
        client.batch_execute(MIGRATION_004).await?;
        Ok(())
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
        let r=self.client.lock().await.query("SELECT e.id::text AS id,e.episode_number,e.episode_title,e.overview,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id=$1::text::uuid AND s.season_number=$2 ORDER BY e.episode_number",&[&title_id,&season]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn get_episode(&self, id: &str) -> Result<Option<Value>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT e.id::text AS id,e.title_id::text AS title_id,e.season_id::text AS season_id,e.episode_number,e.episode_title,e.overview,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.id::text=$1", &[&id]).await?;
        Ok(r.as_ref().map(row_json))
    }
    pub async fn list_sources(&self, id: &str) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT ps.id::text AS id,ps.episode_id::text AS episode_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.expires_at::text AS expires_at FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.episode_id=$1::text::uuid AND ps.source_status NOT IN('blocked','invalid') AND (ps.expires_at IS NULL OR ps.expires_at>now()) ORDER BY ps.resolution_height DESC NULLS LAST", &[&id]).await?;
        Ok(r.iter()
            .map(row_json)
            .filter(source_is_playback_eligible)
            .collect())
    }
    pub async fn list_title_sources(&self, id: &str) -> Result<Vec<Value>, CatalogError> {
        let r = self.client.lock().await.query(
            "SELECT ps.id::text AS id,ps.title_id::text AS title_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.expires_at::text AS expires_at,(ps.expires_at IS NULL OR ps.expires_at>now()) AS source_not_expired FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.title_id=$1::text::uuid AND ps.source_status NOT IN('blocked','invalid') AND (ps.expires_at IS NULL OR ps.expires_at>now()) ORDER BY ps.resolution_height DESC NULLS LAST",
            &[&id],
        ).await?;
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
    let n = normalize(&i.canonical_title);
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
    let (t, ins) = if let Some(existing) = existing_provider_title {
        let id: String = existing.get("id");
        let normalized = normalize(&i.canonical_title);
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
            let er=c.query_one("INSERT INTO episodes(title_id,season_id,episode_number,episode_title,overview,release_date,thumbnail_url) VALUES($1,$2,$3,$4,$5,$6::text::date,$7) ON CONFLICT(title_id,season_id,episode_number) DO UPDATE SET episode_title=EXCLUDED.episode_title,overview=EXCLUDED.overview,release_date=EXCLUDED.release_date,thumbnail_url=EXCLUDED.thumbnail_url,updated_at=now() RETURNING id::text",&[&title_uuid,&sid_uuid,&e.episode_number,&e.episode_title,&e.overview,&e.release_date,&e.thumbnail_url]).await?;
            let eid: String = er.get(0);
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
        c.execute("INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,provider_url,metadata,match_status,last_checked_at) VALUES($1,$2,$3,$4,$5,$6,'verified',now()) ON CONFLICT(provider_id,provider_title_id) DO UPDATE SET title_id=EXCLUDED.title_id,provider_title=EXCLUDED.provider_title,provider_url=EXCLUDED.provider_url,metadata=EXCLUDED.metadata,match_status='verified',updated_at=now()",&[&title_uuid,&pid_uuid,&p.provider_title_id,&p.provider_title,&p.provider_url,&metadata]).await?;
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
                let inserted = c.execute("INSERT INTO source_tracks(source_id,track_type,language_code,codec,format,is_default,is_forced,verified,track_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING", &[&source_uuid,&kind,&track.language_code,&track.codec,&track.format,&track.is_default,&track.is_forced,&track.verified,&track.metadata]).await?;
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
    c.execute("INSERT INTO episode_provider_mappings(episode_id,provider_id,provider_episode_id,provider_episode_number) VALUES($1,$2,$3,$4) ON CONFLICT(provider_id,provider_episode_id) DO UPDATE SET episode_id=EXCLUDED.episode_id,provider_episode_number=EXCLUDED.provider_episode_number",&[&eid,&pid,&pe.provider_episode_id,&pe.provider_episode_number]).await?;
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
                let inserted = c.execute("INSERT INTO source_tracks(source_id,track_type,language_code,codec,format,is_default,is_forced,verified,track_metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING",&[&sid,&kind,&tr.language_code,&tr.codec,&tr.format,&tr.is_default,&tr.is_forced,&tr.verified,&tr.metadata]).await?;
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
        ProviderEpisodeInput, ProviderSourceInput, episode_provider_key, normalize,
        source_codec_is_known_non_hevc, source_identity_fingerprint, source_is_playback_eligible,
    };
    use serde_json::json;

    #[test]
    fn normalizes_search_text() {
        assert_eq!(normalize("  One   Piece "), "one piece");
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
}
