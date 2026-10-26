-- Durable, resumable imports for large anime catalogs. The existing catalog
-- rows remain authoritative; this schema only stores job snapshots/checkpoints.
CREATE TABLE IF NOT EXISTS catalog_anime_import_jobs (
    job_id TEXT PRIMARY KEY REFERENCES catalog_jobs(id) ON DELETE CASCADE,
    active_import_key TEXT UNIQUE,
    anime_name TEXT NOT NULL,
    provider_key TEXT NOT NULL,
    provider_title_id TEXT NOT NULL,
    base_import JSONB NOT NULL,
    total_episodes BIGINT NOT NULL CHECK (total_episodes >= 0),
    processed_episodes BIGINT NOT NULL DEFAULT 0 CHECK (processed_episodes >= 0),
    duplicate_episodes BIGINT NOT NULL DEFAULT 0 CHECK (duplicate_episodes >= 0),
    source_warning_count BIGINT NOT NULL DEFAULT 0 CHECK (source_warning_count >= 0),
    batch_size INTEGER NOT NULL CHECK (batch_size BETWEEN 1 AND 50),
    metadata_imported BOOLEAN NOT NULL DEFAULT FALSE,
    metadata_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (metadata_attempt_count >= 0),
    lease_until TIMESTAMPTZ,
    failure_summary TEXT,
    last_progress_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS catalog_anime_import_jobs_claim_idx
    ON catalog_anime_import_jobs (lease_until, created_at);
CREATE INDEX IF NOT EXISTS catalog_anime_import_jobs_provider_idx
    ON catalog_anime_import_jobs (provider_key, provider_title_id);

CREATE TABLE IF NOT EXISTS catalog_anime_import_episodes (
    job_id TEXT NOT NULL REFERENCES catalog_anime_import_jobs(job_id) ON DELETE CASCADE,
    ordinal BIGINT NOT NULL CHECK (ordinal >= 0),
    season_index INTEGER NOT NULL CHECK (season_index >= 0),
    episode_index INTEGER NOT NULL CHECK (episode_index >= 0),
    season_number INTEGER NOT NULL,
    provider_season INTEGER NOT NULL,
    episode_number INTEGER NOT NULL,
    episode_metadata JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error_code TEXT,
    source_warning_code TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, ordinal)
);
CREATE INDEX IF NOT EXISTS catalog_anime_import_episodes_pending_idx
    ON catalog_anime_import_episodes (job_id, status, ordinal);
