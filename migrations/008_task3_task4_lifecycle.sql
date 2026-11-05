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

-- A unique index alone cannot be created safely when an older deployment
-- already contains duplicate history. Serialize each coordinate with an
-- advisory transaction lock, then reject a second active owner. This protects
-- missing-row races without deleting historical data.
DROP INDEX IF EXISTS episode_provider_lookup_active_owner_idx;
CREATE INDEX IF NOT EXISTS episode_provider_lookup_coordinate_idx
    ON episode_provider_lookup_mappings (
        provider_id,
        provider_title_id,
        provider_season_number,
        provider_episode_number,
        mapping_status
    );
CREATE OR REPLACE FUNCTION prevent_duplicate_active_lookup_owner()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.mapping_status = 'resolved' THEN
        PERFORM pg_advisory_xact_lock(
            hashtextextended(
                format('%s:%s:%s:%s', NEW.provider_id, NEW.provider_title_id,
                       NEW.provider_season_number, NEW.provider_episode_number),
                9137
            )
        );
        IF EXISTS (
            SELECT 1
            FROM episode_provider_lookup_mappings existing
            WHERE existing.provider_id = NEW.provider_id
              AND existing.provider_title_id = NEW.provider_title_id
              AND existing.provider_season_number = NEW.provider_season_number
              AND existing.provider_episode_number = NEW.provider_episode_number
              AND existing.mapping_status = 'resolved'
              AND existing.episode_id IS DISTINCT FROM NEW.episode_id
        ) THEN
            RAISE EXCEPTION 'lookup coordinate already has a resolved canonical owner'
                USING ERRCODE = '23505';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS episode_provider_lookup_active_owner_guard
    ON episode_provider_lookup_mappings;
CREATE TRIGGER episode_provider_lookup_active_owner_guard
BEFORE INSERT OR UPDATE OF provider_id, provider_title_id,
    provider_season_number, provider_episode_number, mapping_status
ON episode_provider_lookup_mappings
FOR EACH ROW EXECUTE FUNCTION prevent_duplicate_active_lookup_owner();
