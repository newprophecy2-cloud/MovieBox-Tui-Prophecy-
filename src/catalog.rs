use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

const MIGRATION_001: &str = include_str!("../migrations/001_catalog.sql");

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
    pub episodes: Vec<ProviderEpisodeInput>,
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
        self.client
            .lock()
            .await
            .batch_execute(MIGRATION_001)
            .await?;
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
    pub async fn get_title_detail(&self, id: &str) -> Result<Option<CatalogDetail>, CatalogError> {
        let c = self.client.lock().await;
        let Some(r)=c.query_opt("SELECT id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status FROM catalog_titles WHERE id::text=$1 AND catalog_status <> 'deleted'",&[&id]).await? else{return Ok(None)};
        let tid: String = r.get("id");
        let alternatives=c.query("SELECT id::text AS id,title,language,title_type,source FROM title_alternative_titles WHERE title_id=$1::text::uuid ORDER BY title", &[&tid]).await?.iter().map(row_json).collect();
        let genres=c.query("SELECT g.id::text AS id,g.canonical_name,g.normalized_name,tg.source FROM genres g JOIN title_genres tg ON tg.genre_id=g.id WHERE tg.title_id=$1::text::uuid ORDER BY g.normalized_name", &[&tid]).await?.iter().map(row_json).collect();
        let providers=c.query("SELECT pt.id::text AS id,pr.provider_key,pr.display_name,pt.provider_title_id,pt.provider_title,pt.provider_url,pt.match_status,pt.match_confidence FROM provider_titles pt JOIN provider_registry pr ON pr.id=pt.provider_id WHERE pt.title_id=$1::text::uuid ORDER BY pr.provider_key", &[&tid]).await?.iter().map(row_json).collect();
        let seasons=c.query("SELECT id::text AS id,season_number,canonical_label,episode_count_reported FROM seasons WHERE title_id=$1::text::uuid ORDER BY season_number", &[&tid]).await?.iter().map(row_json).collect();
        Ok(Some(CatalogDetail {
            title: title_from_row(&r),
            alternative_titles: alternatives,
            genres,
            provider_mappings: providers,
            seasons,
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
        let r=self.client.lock().await.query("SELECT e.id::text AS id,e.episode_number,e.episode_title,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id=$1::text::uuid AND s.season_number=$2 ORDER BY e.episode_number",&[&title_id,&season]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn get_episode(&self, id: &str) -> Result<Option<Value>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT e.id::text AS id,e.title_id::text AS title_id,e.season_id::text AS season_id,e.episode_number,e.episode_title,e.release_date::text AS release_date,e.thumbnail_url,s.season_number FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.id::text=$1", &[&id]).await?;
        Ok(r.as_ref().map(row_json))
    }
    pub async fn list_sources(&self, id: &str) -> Result<Vec<Value>, CatalogError> {
        let r=self.client.lock().await.query("SELECT ps.id::text AS id,ps.episode_id::text AS episode_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.expires_at::text AS expires_at FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.episode_id=$1::text::uuid AND ps.source_status NOT IN('blocked','invalid') AND ps.codec_verification_status='verified' AND lower(coalesce(ps.video_codec,'')) NOT LIKE '%hevc%' AND lower(coalesce(ps.video_codec,'')) NOT LIKE '%h265%' AND lower(coalesce(ps.video_codec,'')) NOT LIKE '%x265%' AND (ps.expires_at IS NULL OR ps.expires_at>now()) ORDER BY ps.resolution_height DESC NULLS LAST", &[&id]).await?;
        Ok(r.iter().map(row_json).collect())
    }
    pub async fn get_source(&self, id: &str) -> Result<Option<Value>, CatalogError> {
        let r=self.client.lock().await.query_opt("SELECT ps.id::text AS id,ps.episode_id::text AS episode_id,ps.provider_source_id,pr.provider_key,ps.source_reference,ps.quality_label,ps.video_codec,ps.audio_codec,ps.container_format,ps.resolution_width,ps.resolution_height,ps.source_status,ps.codec_verification_status,ps.expires_at::text AS expires_at,(ps.source_status NOT IN('blocked','invalid') AND (ps.expires_at IS NULL OR ps.expires_at>now())) AS playback_eligible FROM provider_sources ps JOIN provider_registry pr ON pr.id=ps.provider_id WHERE ps.id::text=$1", &[&id]).await?;
        Ok(r.as_ref().map(row_json))
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
        let r=self.client.lock().await.query_opt("SELECT id,job_type,status,inserted_count,updated_count,skipped_count,failed_count,error_summary FROM catalog_jobs WHERE id=$1",&[&id]).await?;
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
    }
}
async fn upsert_title(c: &Client, i: &TitleUpsert) -> Result<(CatalogTitle, bool), CatalogError> {
    let n = normalize(&i.canonical_title);
    let st = i.catalog_status.clone().unwrap_or_else(|| "active".into());
    let r=c.query_one("INSERT INTO catalog_titles(mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status,metadata_source,metadata_updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'import',now()) ON CONFLICT(mal_id) WHERE mal_id IS NOT NULL DO UPDATE SET canonical_title=EXCLUDED.canonical_title,normalized_title=EXCLUDED.normalized_title,content_type=EXCLUDED.content_type,synopsis=EXCLUDED.synopsis,release_year=EXCLUDED.release_year,poster_url=EXCLUDED.poster_url,catalog_status=EXCLUDED.catalog_status,metadata_updated_at=now(),updated_at=now() RETURNING id::text,mal_id,canonical_title,normalized_title,content_type,synopsis,release_year,poster_url,catalog_status,(xmax=0) AS inserted",&[&i.mal_id,&i.canonical_title,&n,&i.content_type,&i.synopsis,&i.release_year,&i.poster_url,&st]).await?;
    Ok((title_from_row(&r), r.get("inserted")))
}
async fn import_in_tx(c: &Client, i: &ImportTitle) -> Result<ImportSummary, CatalogError> {
    let (t, ins) = upsert_title(
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
    .await?;
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
            let er=c.query_one("INSERT INTO episodes(title_id,season_id,episode_number,episode_title,release_date,thumbnail_url) VALUES($1,$2,$3,$4,$5::text::date,$6) ON CONFLICT(title_id,season_id,episode_number) DO UPDATE SET episode_title=EXCLUDED.episode_title,release_date=EXCLUDED.release_date,thumbnail_url=EXCLUDED.thumbnail_url,updated_at=now() RETURNING id::text",&[&title_uuid,&sid_uuid,&e.episode_number,&e.episode_title,&e.release_date,&e.thumbnail_url]).await?;
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
                map_episode(c, &eid, pe, Some(&provider_id), &mut s).await?;
            }
        }
    }
    for p in &i.providers {
        let pr=c.query_one("INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,true) ON CONFLICT(provider_key) DO UPDATE SET display_name=EXCLUDED.display_name,updated_at=now() RETURNING id::text",&[&p.provider_key,&p.display_name.clone().unwrap_or_else(||p.provider_key.clone())]).await?;
        let pid: String = pr.get(0);
        let pid_uuid = parse_uuid(&pid)?;
        c.execute("INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,provider_url,match_status,last_checked_at) VALUES($1,$2,$3,$4,$5,'verified',now()) ON CONFLICT(provider_id,provider_title_id) DO UPDATE SET title_id=EXCLUDED.title_id,provider_title=EXCLUDED.provider_title,provider_url=EXCLUDED.provider_url,match_status='verified',updated_at=now()",&[&title_uuid,&pid_uuid,&p.provider_title_id,&p.provider_title,&p.provider_url]).await?;
        for pe in &p.episodes {
            let season = pe.season_number.unwrap_or(1);
            let ep = pe.provider_episode_number.unwrap_or(1);
            let r=c.query_opt("SELECT e.id::text FROM episodes e JOIN seasons s ON s.id=e.season_id WHERE e.title_id=$1 AND s.season_number=$2 AND e.episode_number=$3",&[&title_uuid,&season,&ep]).await?;
            if let Some(r) = r {
                let eid: String = r.get(0);
                map_episode(c, &eid, pe, Some(&pid), &mut s).await?;
            }
        }
    }
    Ok(s)
}
async fn map_episode(
    c: &Client,
    eid: &str,
    pe: &ProviderEpisodeInput,
    pid: Option<&String>,
    s: &mut ImportSummary,
) -> Result<(), CatalogError> {
    let pid = match pid {
        Some(x) => parse_uuid(x)?,
        None => return Ok(()),
    };
    let eid = parse_uuid(eid)?;
    c.execute("INSERT INTO episode_provider_mappings(episode_id,provider_id,provider_episode_id,provider_episode_number) VALUES($1,$2,$3,$4) ON CONFLICT(provider_id,provider_episode_id) DO UPDATE SET episode_id=EXCLUDED.episode_id,provider_episode_number=EXCLUDED.provider_episode_number",&[&eid,&pid,&pe.provider_episode_id,&pe.provider_episode_number]).await?;
    for src in &pe.sources {
        let r=c.query_one("INSERT INTO provider_sources(episode_id,provider_id,provider_source_id,source_reference,quality_label,video_codec,audio_codec,container_format,resolution_width,resolution_height,source_status,codec_verification_status,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::text::timestamptz) ON CONFLICT(provider_id,provider_source_id) DO UPDATE SET episode_id=EXCLUDED.episode_id,source_reference=EXCLUDED.source_reference,quality_label=EXCLUDED.quality_label,video_codec=EXCLUDED.video_codec,audio_codec=EXCLUDED.audio_codec,container_format=EXCLUDED.container_format,resolution_width=EXCLUDED.resolution_width,resolution_height=EXCLUDED.resolution_height,source_status=EXCLUDED.source_status,codec_verification_status=EXCLUDED.codec_verification_status,expires_at=EXCLUDED.expires_at,updated_at=now() RETURNING id::text",&[&eid,&pid,&src.provider_source_id,&src.source_reference,&src.quality,&src.codec,&src.audio_codec,&src.container,&src.resolution_width,&src.resolution_height,&src.source_status.clone().unwrap_or_else(||"unverified".into()),&src.codec_verification_status.clone().unwrap_or_else(||"unverified".into()),&src.expires_at]).await?;
        let sid: String = r.get(0);
        let sid = parse_uuid(&sid)?;
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
    use super::{ProviderEpisodeInput, episode_provider_key, normalize};
    use serde_json::json;

    #[test]
    fn normalizes_search_text() {
        assert_eq!(normalize("  One   Piece "), "one piece");
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
