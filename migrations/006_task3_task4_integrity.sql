-- Task 3/4 additive integrity checks. This migration intentionally avoids destructive
-- backfills so existing valid records remain intact.
DO $$ BEGIN
    ALTER TABLE episode_provider_mappings
        ADD CONSTRAINT episode_provider_mappings_status_check
        CHECK (mapping_status IN ('resolved', 'unresolved'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE episode_provider_mappings
        ADD CONSTRAINT episode_provider_mappings_provider_episode_id_nonempty
        CHECK (length(btrim(provider_episode_id)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE episode_provider_lookup_mappings
        ADD CONSTRAINT episode_provider_lookup_reference_nonempty
        CHECK (length(btrim(lookup_reference)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE INDEX IF NOT EXISTS episode_provider_mappings_identity_idx
    ON episode_provider_mappings (provider_id, provider_episode_id, episode_id, mapping_status);
