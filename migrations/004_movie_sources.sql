ALTER TABLE provider_sources
    ADD COLUMN IF NOT EXISTS title_id UUID REFERENCES catalog_titles(id) ON DELETE CASCADE;

ALTER TABLE provider_sources
    ALTER COLUMN episode_id DROP NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_sources_exactly_one_owner_check'
    ) THEN
        ALTER TABLE provider_sources
            ADD CONSTRAINT provider_sources_exactly_one_owner_check
            CHECK ((episode_id IS NOT NULL) <> (title_id IS NOT NULL));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS provider_sources_title_id_idx
    ON provider_sources (title_id)
    WHERE title_id IS NOT NULL;
