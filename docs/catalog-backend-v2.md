# Catalog backend v2

## Implemented in this change

The backend now has a versioned PostgreSQL migration at `migrations/001_catalog.sql` and a Rust catalog store in `src/catalog.rs`. When `DATABASE_URL` is configured, startup connects to PostgreSQL and applies the idempotent migration. PostgreSQL is the durable catalog authority; Redis remains optional optimization only.

The new `GET /api/v1/search?q=...` route reads only from PostgreSQL and never performs provider discovery. The existing `/api/v1/anime/search` route is preserved for APK compatibility and remains the legacy provider-backed contract.

Protected manual routes require `ADMIN_TOKEN` via `X-Admin-Token` or `Authorization: Bearer ...`:

- `POST /api/v1/admin/catalog/titles/upsert`
- `POST /api/v1/admin/catalog/import` with `{ "titles": [...] }` and a maximum batch size of 500
- `POST /api/v1/admin/catalog/update-all`
- `GET /api/v1/admin/catalog/inspect`
- `GET /api/v1/admin/catalog/jobs/{job_id}`

Explicit Web and Android playback contract families are registered. They intentionally do not fall back to live provider discovery; until a verified stored source mapping is available they return `NO_COMPATIBLE_SOURCE` or `DATABASE_UNAVAILABLE`.

## Migration and startup

Set `DATABASE_URL` and restart the service. The service applies the migration at startup. The database user needs permission to create the application schema and the `pgcrypto` extension during initial provisioning. In a locked-down production setup, apply the migration with a privileged deploy role first, then run the application with a least-privilege role.

For a deployment that requires the catalog, set `REQUIRE_DATABASE=true`. In that mode `/ready` returns HTTP 503 with a dependency status when PostgreSQL is missing or initialization fails; `/health` remains a lightweight process liveness check. Render is configured for this mode. Local development defaults to `REQUIRE_DATABASE=false` in `.env.example`.

The current migration creates canonical titles, alternative titles, genres, provider registry/mappings, seasons, episodes, provider sources, source tracks, sections, activity events, pending import requests, catalog jobs, remux-session metadata, and privacy-safe playback progress.

## Important limitations

This change does not claim that the full 700+ MAL import is complete, that all providers are mapped into PostgreSQL, or that Web remux session sharing is production-ready. The legacy provider routes are retained for compatibility. A future phase must populate the catalog from real authorized metadata, add database-backed episode/source selection, complete Web session management, and add PostgreSQL/Redis integration tests against controlled fixtures.

Provider-backed browse aggregation now has a six-second bounded homepage deadline and one-second per-item classification deadline. A slow provider therefore produces a bounded upstream error instead of holding the HTTP connection until the client or proxy timeout.

## Import and navigation completion

The catalog store now persists the complete expanded import graph using the existing tables. Imports execute one PostgreSQL transaction per title and report title/season/episode/source/track counters. Re-imports update canonical records using existing identities and keep records omitted from a later payload retained. The migration adds repeat-safe indexes for alternative titles, tracks, episode navigation, episode mappings, and source eligibility.

Catalog UUIDs are intentionally separate from the opaque provider IDs used by the legacy anime API. Search results advertise `id_type: catalog_title`; clients continue through `/api/v1/catalog/...` routes. The catalog source list and playback route are database-backed and fail closed unless a source is active, unexpired, codec-verified, and recognized as a non-HEVC codec. No provider data is fabricated and no authorization headers are returned.

Run the local checks with:

```bash
cargo fmt --all -- --check
cargo test --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --locked --release
```

For the database flow, set `DATABASE_URL`, `ADMIN_TOKEN`, and `REQUIRE_DATABASE=true`, start PostgreSQL, start the server, then import a fixture through `/api/v1/admin/catalog/import` and follow the catalog routes listed in `docs/catalog-api.md`. Apply `migrations/001_catalog.sql` twice to verify idempotency before running the API smoke test.
