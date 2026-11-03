CREATE TABLE IF NOT EXISTS provider_season_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    season_id UUID NOT NULL REFERENCES seasons(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES provider_registry(id) ON DELETE CASCADE,
    provider_season_id TEXT,
    provider_season_number INTEGER,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(provider_id, season_id)
);
CREATE INDEX IF NOT EXISTS provider_season_mappings_provider_idx
    ON provider_season_mappings (provider_id, provider_season_id);

ALTER TABLE episode_provider_mappings
    ADD COLUMN IF NOT EXISTS mapping_status TEXT NOT NULL DEFAULT 'resolved';
ALTER TABLE episode_provider_mappings
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS episode_provider_lookup_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id UUID NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
    provider_id UUID NOT NULL REFERENCES provider_registry(id) ON DELETE CASCADE,
    provider_title_id TEXT NOT NULL,
    provider_season_number INTEGER NOT NULL CHECK (provider_season_number >= 0),
    provider_episode_number INTEGER NOT NULL CHECK (provider_episode_number > 0),
    lookup_reference TEXT NOT NULL,
    mapping_status TEXT NOT NULL DEFAULT 'resolved'
        CHECK (mapping_status IN ('resolved', 'unresolved')),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (
        provider_id,
        provider_title_id,
        provider_season_number,
        provider_episode_number,
        episode_id
    )
);
CREATE INDEX IF NOT EXISTS episode_provider_lookup_mappings_lookup_idx
    ON episode_provider_lookup_mappings (
        provider_id,
        provider_title_id,
        provider_season_number,
        provider_episode_number
    );
