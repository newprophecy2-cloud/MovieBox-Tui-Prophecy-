ALTER TABLE provider_sources
    ADD COLUMN IF NOT EXISTS source_identity_fingerprint TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS catalog_codec_verification_jobs (
    source_id UUID PRIMARY KEY REFERENCES provider_sources(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    lease_token UUID,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (status = 'running' AND lease_until IS NOT NULL AND lease_token IS NOT NULL)
        OR status <> 'running'
    )
);

CREATE INDEX IF NOT EXISTS catalog_codec_verification_jobs_ready_idx
    ON catalog_codec_verification_jobs (status, next_attempt_at, lease_until);
