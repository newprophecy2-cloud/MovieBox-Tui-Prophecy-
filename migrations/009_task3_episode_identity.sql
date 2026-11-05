-- Task 3 additive episode identity model.
-- Legacy rows retain their opaque IDs and receive a stable legacy identity key.
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS episode_type TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS episode_coordinate TEXT;
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS absolute_episode_number INTEGER;
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS special_identifier TEXT;
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS provider_original_number TEXT;
ALTER TABLE episodes ADD COLUMN IF NOT EXISTS identity_key TEXT;

UPDATE episodes
SET episode_coordinate = COALESCE(NULLIF(btrim(episode_coordinate), ''), episode_number::text),
    identity_key = COALESCE(NULLIF(btrim(identity_key), ''), 'legacy:' || id::text)
WHERE episode_coordinate IS NULL OR btrim(episode_coordinate) = '' OR identity_key IS NULL OR btrim(identity_key) = '';

DO $$ BEGIN
    ALTER TABLE episodes
        ADD CONSTRAINT episodes_episode_type_nonempty CHECK (length(btrim(episode_type)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
DO $$ BEGIN
    ALTER TABLE episodes
        ADD CONSTRAINT episodes_identity_key_nonempty CHECK (length(btrim(identity_key)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- The original key cannot distinguish specials, decimals, movies, or other
-- nonstandard coordinates. Keep it for existing installations only until the
-- typed identity index below is in place.
DO $$ BEGIN
    ALTER TABLE episodes DROP CONSTRAINT episodes_title_id_season_id_episode_number_key;
EXCEPTION WHEN undefined_object THEN NULL; END $$;
CREATE UNIQUE INDEX IF NOT EXISTS episodes_typed_identity_idx
    ON episodes (title_id, COALESCE(season_id, '00000000-0000-0000-0000-000000000000'::uuid), episode_type, identity_key);
CREATE INDEX IF NOT EXISTS episodes_coordinate_lookup_idx
    ON episodes (title_id, season_id, episode_type, episode_coordinate);

-- Read-only diagnostics can distinguish historical invalid ownership from
-- current rows without deleting or reassigning anything.
CREATE INDEX IF NOT EXISTS episodes_cross_anime_diagnostic_idx
    ON episodes (title_id, season_id);
