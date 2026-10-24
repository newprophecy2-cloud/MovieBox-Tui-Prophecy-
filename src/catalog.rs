use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls, Row};

const MIGRATION_001: &str = include_str!("../migrations/001_catalog.sql");

#[derive(Clone)]
pub struct CatalogStore {
    client: Arc<Mutex<Client>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogTitle {
    pub id: String,
    pub mal_id: Option<i64>,
    pub canonical_title: String,
    pub normalized_title: String,
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: String,
}

#[derive(Debug, Deserialize)]
pub struct TitleUpsert {
    pub mal_id: Option<i64>,
    pub canonical_title: String,
    pub content_type: String,
    pub synopsis: Option<String>,
    pub release_year: Option<i32>,
    pub poster_url: Option<String>,
    pub catalog_status: Option<String>,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct CatalogGenre {
    pub id: String,
    pub canonical_name: String,
    pub normalized_name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("database error")]
    Database(#[from] tokio_postgres::Error),
    #[error("invalid title: {0}")]
    Validation(String),
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
            if let Err(error) = connection.await {
                log::error!("postgres connection stopped: {error}");
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
        query: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(CatalogError::Validation("q must not be empty".into()));
        }
        let limit = limit.clamp(1, 100);
        let offset = offset.max(0);
        let pattern = format!("%{}%", normalize(query));
        let rows = self.client.lock().await.query(
            "SELECT id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status FROM catalog_titles WHERE normalized_title LIKE $1 AND catalog_status <> 'deleted' ORDER BY canonical_title LIMIT $2 OFFSET $3",
            &[&pattern, &limit, &offset],
        ).await?;
        Ok(rows.iter().map(title_from_row).collect())
    }

    pub async fn enqueue_pending_import(&self, query: &str) -> Result<(), CatalogError> {
        let normalized_query = normalize(query);
        if normalized_query.is_empty() {
            return Err(CatalogError::Validation("query must not be empty".into()));
        }
        self.client.lock().await.execute(
            "INSERT INTO pending_import_requests (normalized_query, candidate_title) VALUES ($1,$2) ON CONFLICT (normalized_query) DO UPDATE SET request_count = pending_import_requests.request_count + 1, last_requested_at = now(), status = CASE WHEN pending_import_requests.status = 'processed' THEN 'pending' ELSE pending_import_requests.status END",
            &[&normalized_query, &query.trim()],
        ).await?;
        Ok(())
    }

    pub async fn list_titles(
        &self,
        content_type: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        let limit = limit.clamp(1, 100);
        let offset = offset.max(0);
        let rows = match content_type {
            Some(content_type) => self.client.lock().await.query(
                "SELECT id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status FROM catalog_titles WHERE content_type=$1 AND catalog_status='active' ORDER BY updated_at DESC LIMIT $2 OFFSET $3",
                &[&content_type, &limit, &offset],
            ).await?,
            None => self.client.lock().await.query(
                "SELECT id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status FROM catalog_titles WHERE catalog_status='active' ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
                &[&limit, &offset],
            ).await?,
        };
        Ok(rows.iter().map(title_from_row).collect())
    }

    pub async fn get_title(&self, id: &str) -> Result<Option<CatalogTitle>, CatalogError> {
        let row = self.client.lock().await.query_opt(
            "SELECT id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status FROM catalog_titles WHERE id::text=$1 AND catalog_status <> 'deleted'",
            &[&id],
        ).await?;
        Ok(row.as_ref().map(title_from_row))
    }

    pub async fn list_genres(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogGenre>, CatalogError> {
        let rows = self.client.lock().await.query(
            "SELECT id::text, canonical_name, normalized_name FROM genres ORDER BY normalized_name LIMIT $1 OFFSET $2",
            &[&limit.clamp(1, 100), &offset.max(0)],
        ).await?;
        Ok(rows
            .iter()
            .map(|row| CatalogGenre {
                id: row.get("id"),
                canonical_name: row.get("canonical_name"),
                normalized_name: row.get("normalized_name"),
            })
            .collect())
    }

    pub async fn upsert(&self, input: TitleUpsert) -> Result<(CatalogTitle, bool), CatalogError> {
        validate_title(&input)?;
        let normalized = normalize(&input.canonical_title);
        let status = input.catalog_status.unwrap_or_else(|| "active".into());
        let row = self.client.lock().await.query_one(
            "INSERT INTO catalog_titles (mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status, metadata_source, metadata_updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'admin',now()) ON CONFLICT (mal_id) WHERE mal_id IS NOT NULL DO UPDATE SET canonical_title=EXCLUDED.canonical_title, normalized_title=EXCLUDED.normalized_title, content_type=EXCLUDED.content_type, synopsis=EXCLUDED.synopsis, release_year=EXCLUDED.release_year, poster_url=EXCLUDED.poster_url, catalog_status=EXCLUDED.catalog_status, metadata_updated_at=now(), updated_at=now() RETURNING id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status, (xmax = 0) AS inserted",
            &[&input.mal_id, &input.canonical_title, &normalized, &input.content_type, &input.synopsis, &input.release_year, &input.poster_url, &status],
        ).await?;
        let inserted: bool = row.get("inserted");
        Ok((title_from_row(&row), inserted))
    }

    pub async fn create_job(&self, job_type: &str) -> Result<CatalogJob, CatalogError> {
        let id = new_id();
        self.client.lock().await.execute(
            "INSERT INTO catalog_jobs (id, job_type, status, started_at) VALUES ($1,$2,'running',now())",
            &[&id, &job_type],
        ).await?;
        Ok(CatalogJob {
            id,
            job_type: job_type.into(),
            status: "running".into(),
            inserted_count: 0,
            updated_count: 0,
            skipped_count: 0,
            failed_count: 0,
        })
    }

    pub async fn finish_job(&self, job: &CatalogJob) -> Result<(), CatalogError> {
        self.client.lock().await.execute(
            "UPDATE catalog_jobs SET status=$2, completed_at=now(), inserted_count=$3, updated_count=$4, skipped_count=$5, failed_count=$6 WHERE id=$1",
            &[&job.id, &job.status, &job.inserted_count, &job.updated_count, &job.skipped_count, &job.failed_count],
        ).await?;
        Ok(())
    }

    pub async fn get_job(&self, id: &str) -> Result<Option<CatalogJob>, CatalogError> {
        let row = self.client.lock().await.query_opt(
            "SELECT id, job_type, status, inserted_count, updated_count, skipped_count, failed_count FROM catalog_jobs WHERE id=$1",
            &[&id],
        ).await?;
        Ok(row.map(|row| CatalogJob {
            id: row.get("id"),
            job_type: row.get("job_type"),
            status: row.get("status"),
            inserted_count: row.get("inserted_count"),
            updated_count: row.get("updated_count"),
            skipped_count: row.get("skipped_count"),
            failed_count: row.get("failed_count"),
        }))
    }

    pub async fn inspect(
        &self,
        query: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        match query.filter(|v| !v.trim().is_empty()) {
            Some(query) => self.search(query, limit, offset).await,
            None => {
                let rows = self.client.lock().await.query("SELECT id::text, mal_id, canonical_title, normalized_title, content_type, synopsis, release_year, poster_url, catalog_status FROM catalog_titles WHERE catalog_status <> 'deleted' ORDER BY updated_at DESC LIMIT $1 OFFSET $2", &[&limit.clamp(1,100), &offset.max(0)]).await?;
                Ok(rows.iter().map(title_from_row).collect())
            }
        }
    }
}

fn title_from_row(row: &Row) -> CatalogTitle {
    CatalogTitle {
        id: row.get("id"),
        mal_id: row.get("mal_id"),
        canonical_title: row.get("canonical_title"),
        normalized_title: row.get("normalized_title"),
        content_type: row.get("content_type"),
        synopsis: row.get("synopsis"),
        release_year: row.get("release_year"),
        poster_url: row.get("poster_url"),
        catalog_status: row.get("catalog_status"),
    }
}

fn validate_title(input: &TitleUpsert) -> Result<(), CatalogError> {
    if input.canonical_title.trim().is_empty() || input.canonical_title.chars().count() > 300 {
        return Err(CatalogError::Validation(
            "canonical_title must contain 1..300 characters".into(),
        ));
    }
    if !matches!(input.content_type.as_str(), "anime" | "movie" | "k_drama") {
        return Err(CatalogError::Validation(
            "content_type must be anime, movie, or k_drama".into(),
        ));
    }
    Ok(())
}

fn normalize(value: &str) -> String {
    value
        .trim()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
fn new_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:032x}")
}

#[cfg(test)]
mod tests {
    use super::normalize;
    #[test]
    fn normalizes_search_text() {
        assert_eq!(normalize("  One   Piece "), "one piece");
    }
}
