-- Task 3/4 ownership integrity. Additive and safe for existing data.
-- Existing rows are checked by the diagnostic API before any cleanup is attempted.
DO $$ BEGIN
    ALTER TABLE provider_titles
        ADD CONSTRAINT provider_titles_match_status_check
        CHECK (match_status IN ('unverified', 'verified', 'unresolved', 'stale', 'rejected'));
EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE seasons
        ADD CONSTRAINT seasons_title_id_id_key UNIQUE (title_id, id);
EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL; END $$;

-- NOT VALID preserves existing deployments while enforcing the relationship for
-- all newly inserted or updated Episodes. Existing invalid rows remain visible
-- to mapping_integrity_report() and can be reconciled explicitly.
DO $$ BEGIN
    ALTER TABLE episodes
        ADD CONSTRAINT episodes_season_belongs_to_title_fk
        FOREIGN KEY (title_id, season_id)
        REFERENCES seasons(title_id, id)
        NOT VALID;
EXCEPTION WHEN duplicate_object OR duplicate_table THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS provider_titles_lifecycle_idx
    ON provider_titles (match_status, last_checked_at);
CREATE INDEX IF NOT EXISTS episode_provider_mappings_lifecycle_idx
    ON episode_provider_mappings (mapping_status, provider_id);
