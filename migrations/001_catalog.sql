CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS catalog_titles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mal_id BIGINT UNIQUE,
    canonical_title TEXT NOT NULL CHECK (length(canonical_title) BETWEEN 1 AND 300),
    normalized_title TEXT NOT NULL,
    content_type TEXT NOT NULL CHECK (content_type IN ('anime', 'movie', 'k_drama')),
    synopsis TEXT,
    release_date DATE,
    release_year INTEGER,
    poster_url TEXT,
    banner_url TEXT,
    catalog_status TEXT NOT NULL DEFAULT 'active' CHECK (catalog_status IN ('active', 'pending', 'disabled', 'deleted')),
    metadata_source TEXT NOT NULL DEFAULT 'unknown',
    metadata_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS catalog_titles_normalized_title_idx ON catalog_titles (normalized_title);
CREATE INDEX IF NOT EXISTS catalog_titles_type_status_idx ON catalog_titles (content_type, catalog_status, updated_at DESC);

CREATE TABLE IF NOT EXISTS title_alternative_titles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE,
    title TEXT NOT NULL, normalized_title TEXT NOT NULL, language TEXT, title_type TEXT, source TEXT NOT NULL DEFAULT 'unknown'
);
CREATE INDEX IF NOT EXISTS title_alternative_titles_normalized_idx ON title_alternative_titles (normalized_title);

CREATE TABLE IF NOT EXISTS genres (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), canonical_name TEXT NOT NULL, normalized_name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS title_genres (
    title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, genre_id UUID NOT NULL REFERENCES genres(id) ON DELETE CASCADE, source TEXT NOT NULL DEFAULT 'unknown', PRIMARY KEY (title_id, genre_id)
);

CREATE TABLE IF NOT EXISTS provider_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), provider_key TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT false, capabilities JSONB NOT NULL DEFAULT '{}'::jsonb, safe_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS provider_titles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, provider_id UUID NOT NULL REFERENCES provider_registry(id) ON DELETE CASCADE, provider_title_id TEXT NOT NULL, provider_title TEXT, provider_url TEXT, match_status TEXT NOT NULL DEFAULT 'unverified', match_confidence REAL, last_checked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(provider_id, provider_title_id)
);

CREATE TABLE IF NOT EXISTS seasons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, season_number INTEGER NOT NULL, canonical_label TEXT, episode_count_reported INTEGER, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(title_id, season_number)
);
CREATE TABLE IF NOT EXISTS episodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, season_id UUID REFERENCES seasons(id) ON DELETE CASCADE, episode_number INTEGER NOT NULL, episode_title TEXT, release_date DATE, thumbnail_url TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(title_id, season_id, episode_number)
);
CREATE TABLE IF NOT EXISTS episode_provider_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), episode_id UUID NOT NULL REFERENCES episodes(id) ON DELETE CASCADE, provider_id UUID NOT NULL REFERENCES provider_registry(id) ON DELETE CASCADE, provider_episode_id TEXT NOT NULL, provider_episode_number INTEGER, UNIQUE(provider_id, provider_episode_id)
);
CREATE TABLE IF NOT EXISTS provider_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), episode_id UUID NOT NULL REFERENCES episodes(id) ON DELETE CASCADE, provider_id UUID NOT NULL REFERENCES provider_registry(id) ON DELETE CASCADE, provider_source_id TEXT NOT NULL, source_reference TEXT, quality_label TEXT, video_codec TEXT, audio_codec TEXT, container_format TEXT, resolution_width INTEGER, resolution_height INTEGER, source_status TEXT NOT NULL DEFAULT 'unverified', codec_verification_status TEXT NOT NULL DEFAULT 'unverified', last_validated_at TIMESTAMPTZ, expires_at TIMESTAMPTZ, last_error_code TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(provider_id, provider_source_id)
);
CREATE TABLE IF NOT EXISTS source_tracks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), source_id UUID NOT NULL REFERENCES provider_sources(id) ON DELETE CASCADE, track_type TEXT NOT NULL CHECK (track_type IN ('audio', 'subtitle')), language_code TEXT, codec TEXT, format TEXT, is_default BOOLEAN NOT NULL DEFAULT false, is_forced BOOLEAN NOT NULL DEFAULT false, verified BOOLEAN NOT NULL DEFAULT false, track_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS catalog_sections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), section_key TEXT NOT NULL UNIQUE, title TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, is_active BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS catalog_section_items (
    section_id UUID NOT NULL REFERENCES catalog_sections(id) ON DELETE CASCADE, title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, position INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(section_id, title_id)
);
CREATE TABLE IF NOT EXISTS title_activity_events (
    id BIGSERIAL PRIMARY KEY, title_id UUID REFERENCES catalog_titles(id) ON DELETE CASCADE, event_type TEXT NOT NULL, dedupe_key TEXT NOT NULL, occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(event_type, dedupe_key)
);
CREATE INDEX IF NOT EXISTS title_activity_recent_idx ON title_activity_events (event_type, occurred_at DESC);

CREATE TABLE IF NOT EXISTS pending_import_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), normalized_query TEXT NOT NULL UNIQUE, mal_id BIGINT, candidate_title TEXT, requested_by TEXT, status TEXT NOT NULL DEFAULT 'pending', request_count BIGINT NOT NULL DEFAULT 1, last_requested_at TIMESTAMPTZ NOT NULL DEFAULT now(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(), processed_at TIMESTAMPTZ, error_code TEXT
);
CREATE TABLE IF NOT EXISTS catalog_jobs (
    id TEXT PRIMARY KEY, job_type TEXT NOT NULL, status TEXT NOT NULL, requested_by TEXT, started_at TIMESTAMPTZ, completed_at TIMESTAMPTZ, inserted_count BIGINT NOT NULL DEFAULT 0, updated_count BIGINT NOT NULL DEFAULT 0, skipped_count BIGINT NOT NULL DEFAULT 0, failed_count BIGINT NOT NULL DEFAULT 0, error_summary TEXT
);
CREATE TABLE IF NOT EXISTS remux_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_key_hash TEXT NOT NULL UNIQUE, source_id UUID REFERENCES provider_sources(id) ON DELETE SET NULL, status TEXT NOT NULL, output_profile TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS user_playback_progress (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), title_id UUID NOT NULL REFERENCES catalog_titles(id) ON DELETE CASCADE, device_key_hash TEXT NOT NULL, position_seconds DOUBLE PRECISION NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(title_id, device_key_hash)
);
