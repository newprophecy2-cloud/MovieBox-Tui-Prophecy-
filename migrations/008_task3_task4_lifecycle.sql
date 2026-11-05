-- Task 3/4 lifecycle history and lookup ownership enforcement.
CREATE TABLE IF NOT EXISTS provider_mapping_transition_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mapping_type TEXT NOT NULL CHECK (mapping_type IN ('title', 'episode', 'lookup')),
    mapping_id UUID NOT NULL,
    previous_status TEXT NOT NULL,
    new_status TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS provider_mapping_transition_history_lookup_idx
    ON provider_mapping_transition_history (mapping_type, mapping_id, created_at DESC);

-- Only resolved lookup coordinates are active owners. Multiple unresolved
-- historical candidates remain diagnosable without permitting two active owners.
CREATE UNIQUE INDEX IF NOT EXISTS episode_provider_lookup_active_owner_idx
    ON episode_provider_lookup_mappings (
        provider_id,
        provider_title_id,
        provider_season_number,
        provider_episode_number
    ) WHERE mapping_status = 'resolved';
