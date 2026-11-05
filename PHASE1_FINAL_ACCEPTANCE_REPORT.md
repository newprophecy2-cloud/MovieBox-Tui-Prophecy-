# Prophecy Verse Backend — Phase 1 Final Acceptance Report

## A. Repository information

| Field | Result |
|---|---|
| Repository | `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-` |
| Target branch | `feature/unified-content-engine-completion` |
| Starting commit for this acceptance run | `180aa2bf9670b3b4a2d998413a53ab3652a45801` |
| Previous Phase 1 implementation commit | `6be6eb36134fb99710391c11d9d8c27e0135d368` |
| Acceptance implementation commit | `dc73ce5ad78fb3b7ab1c2c7a49cd0185a2f5ea4a` |
| Final report commit | `4f6a43fab4678933777105dd84b2df7fa7e04915` |
| Push status | PASSED; acceptance implementation pushed to the target branch |
| Working tree status before report packaging | CLEAN and synchronized with origin |

Only the backend feature branch was changed. The APK repository, Firebase, website, authentication, analytics, playback, remux, transcoding, streaming, production VPS, and production databases were not modified.

## B. Five-task completion matrix

| Task | Status | Evidence |
|---|---|---|
| PostgreSQL validation | PASSED | PostgreSQL 16.15 disposable local instance; all five migrations applied; `tests/phase1_acceptance.rs` passed 2/2; existing `anime_import_jobs` PostgreSQL test passed 1/1 |
| Alias batch optimization | PASSED | `CatalogStore::verified_provider_aliases` changed from one query per mapping to one parameterized `UNNEST` batch query; verified-only, empty-input, duplicate-input, deterministic ordering, and unverified-mapping tests passed |
| Provider failure tests | PARTIALLY PASSED | Deterministic timeout, outage, empty-response, duplicate-page, provider-identity, and pagination-termination tests passed 4/4; route-level JSON/SSE integration was reviewed but not independently driven through a mocked HTTP provider harness |
| APK compatibility | PARTIALLY PASSED | Actual APK Retrofit/Moshi contract inspected; backend JSON compatibility test passed; no APK code was changed; an Android runtime/network integration test was not run |
| Final regression | PARTIALLY PASSED | `cargo check`, `cargo check --all-targets`, `cargo test --lib`, `cargo test --tests`, `cargo test unified::tests`, `git diff --check` passed; repository-wide `cargo fmt --all -- --check` reports pre-existing formatting drift in legacy files |

Overall status: **IMPLEMENTATION COMPLETE — FINAL ACCEPTANCE PENDING**. The mandatory implementation and PostgreSQL validation requirements are complete. Full acceptance remains pending because the repository-wide formatting check is not clean on the existing legacy source and route-level mocked JSON/SSE plus Android runtime compatibility tests were not run.

## C. Modified files

### `src/catalog.rs`

Replaced the sequential verified-alias lookup with a single parameterized PostgreSQL query. The implementation collects provider keys and provider title IDs, removes duplicate input pairs, uses `UNNEST($1::text[], $2::text[])` for exact pair matching, retains `match_status='verified'`, orders results deterministically, ignores blank aliases, and returns an empty map without querying for empty input. No migration was added.

### `src/api.rs`

Extracted the production bounded pagination and provider-ID deduplication loop into `search_provider_pages`. The production search flow still uses the existing three-provider semaphore, provider capability checks, three-page maximum for paginated providers, one-page behavior for non-paginated providers, and overall timeout. The extracted helper enables deterministic tests for timeout isolation, provider outage, empty responses, duplicate pages, cross-provider identity, and pagination termination. Added a backend-side JSON compatibility test using the actual APK search DTO fields.

### `tests/phase1_acceptance.rs`

Added service-gated disposable PostgreSQL acceptance tests covering migration application, canonical title insertion, provider registry and provider-title mappings, verified/unverified alias behavior, duplicate mapping protection, advisory-lock convergence under concurrent upserts, rollback on invalid input, and persistence isolation. The tests are ignored by the ordinary suite and run explicitly with `DATABASE_URL` and `--ignored`, matching the repository's existing service-gated convention.

### `PHASE1_FINAL_ACCEPTANCE_REPORT.md`

This report.

The previously generated `ProphecyVerse-Backend-Phase1-Final-Report.md` was superseded by this exact acceptance report filename and is excluded from the final archive to avoid stale status information.

## D. Test results

The following commands were run on the final acceptance source tree:

| Command | Exit/status | Result |
|---|---:|---|
| `cargo check` | 0 | PASSED |
| `cargo check --all-targets` | 0 | PASSED |
| `cargo test --lib` | 0 | PASSED — 572 passed, 0 failed, 0 ignored |
| `cargo test --tests` | 0 | PASSED — all runnable integration tests passed; service-gated PostgreSQL and live-network tests remained ignored by design |
| `cargo test unified::tests` | 0 | PASSED — 13 passed, 0 failed |
| `cargo test deterministic_ --lib` | 0 | PASSED — 4 passed, 0 failed |
| `cargo test --test phase1_acceptance -- --test-threads=1` with disposable PostgreSQL | 0 | PASSED — 2 passed, 0 failed |
| `cargo test --test anime_import_jobs -- --ignored --test-threads=1` with disposable PostgreSQL | 0 | PASSED — 1 passed, 0 failed |
| `git diff --check` | 0 | PASSED |
| `cargo fmt --all -- --check` | 1 | PARTIALLY PASSED / BLOCKED by pre-existing formatting drift in legacy source files; no whole-tree formatting rewrite was applied |

The final library suite count increased from the previous 567 tests to 572 because five deterministic acceptance/compatibility tests were added.

## E. PostgreSQL results

### Provisioning

A disposable local PostgreSQL 16.15 instance was provisioned inside the sandbox. The test-only database was `phase1_test`, accessed only through a test-only role. No production database URL, credential, or service was used.

The database was reset by dropping and recreating only its disposable `public` schema before the acceptance tests. The cluster listened on the local loopback interface at port 5432.

### Migrations

`CatalogStore::connect_from_env()` applied all existing migrations in order:

1. `001_catalog.sql`
2. `002_codec_verification.sql`
3. `003_anime_import_jobs.sql`
4. `004_movie_sources.sql`
5. `005_unified_content_mappings.sql`

Migration application passed in both PostgreSQL acceptance tests.

### Persistence scenarios

The acceptance suite passed the following scenarios:

- New canonical title insertion.
- Existing catalog title persistence.
- Provider registry initialization.
- Multiple provider mappings attached to a canonical title.
- Verified provider mapping retrieval.
- Unverified mapping exclusion from verified alias lookup.
- Alternative-title insertion and deterministic lookup.
- Duplicate provider mapping rejection through the existing unique constraint.
- Empty, duplicate, and missing alias lookup inputs.
- Concurrent upserts converging to one canonical title through the transaction-scoped advisory lock.
- Invalid unified input returning an error without leaving a partially inserted title.
- Existing large anime import staging, resume, idempotency, lease release, and restart behavior.

### PostgreSQL test results

- `phase1_postgres_persistence_and_alias_batch_acceptance`: PASSED.
- `phase1_postgres_advisory_lock_and_rollback_acceptance`: PASSED.
- `large_anime_jobs_stage_all_records_resume_and_upsert_idempotently`: PASSED.

No credentials are included in this report.

## F. Alias lookup performance

### Previous implementation

The previous implementation iterated over every `(provider_key, provider_title_id)` pair and executed one parameterized PostgreSQL query per pair. For `n` mappings, the lookup required approximately `n` database round trips, in addition to the application loop.

### New implementation

The new implementation deduplicates the input pairs and performs one parameterized query using two PostgreSQL text arrays and `UNNEST`. It joins the requested provider/title pairs against the verified provider mappings and returns all aliases in deterministic order. For a non-empty batch, the lookup requires one database round trip, reducing round-trip complexity from approximately `O(n)` to `O(1)` per batch. Empty input returns immediately without a query.

### Measurement

No production-scale benchmark was run. Correctness and round-trip behavior were validated against PostgreSQL 16.15. The remaining limitation is that latency and throughput under production-sized catalog tables still require a representative benchmark environment.

## G. APK compatibility

### Actual APK request contract

The APK repository was inspected at commit `7bca2b47dd23a16912e080266425b121decd05eb` without modifying it.

The actual Retrofit declaration is:

```text
GET /api/v1/anime/search
Query: q: String
Query: limit: Int? = 20
Query: cursor: String? = null
Response: Response<ProphecySearchResponse>
```

The APK's Moshi DTOs require or accept the following search response fields:

```text
ProphecySearchResponse:
  results: List<ProphecySearchResultItem> = emptyList()
  next_cursor: String? = null
  partial: Boolean? = null

ProphecySearchResultItem:
  id: String
  title: String
  media_type: String? = null
  year: String? = null
  poster_url: String? = null
  season_count: Int? = null
```

The APK treats returned IDs as backend IDs and persists validated results to Room before rendering. It catches a failed ProphecyVerse search and continues with an empty remote result set. It does not require a playable source for a search item.

### Backend response contract

The backend preserves `GET /api/v1/anime/search`. It continues returning `results`, `next_cursor`, `partial`, and compatible catalog item fields. Newer backend fields such as `canonical_id`, `provider_mappings`, `provider_status`, language metadata, and an optional message are additive and are ignored by the APK's Moshi DTO because the DTO does not use strict unknown-field rejection.

Opaque backend IDs and provider mapping IDs remain unchanged. No API route or APK code was changed.

### Compatibility tests

The backend compatibility test passed for:

- Normal successful search.
- Empty results.
- Partial provider failure.
- Canonical and provider IDs present as additive fields.
- Multiple provider mappings.
- Results without a playable source.
- Optional cursor, media type, year, poster, and season count fields.

An Android runtime/network integration test against the APK was not run in the sandbox. Therefore APK compatibility is marked PARTIALLY PASSED rather than fully accepted.

## H. Provider failure and search regression

The production pagination helper remains bounded and provider-capability-aware:

- Maximum three providers concurrently through the existing semaphore.
- Dramachi pagination remains bounded to three pages.
- 4KHDHub remains single-page because its capability is non-paginated.
- Duplicate records from the same provider/page are removed by full provider identity.
- Different providers with the same provider title ID remain distinguishable.
- Empty first-page responses terminate safely.
- Duplicate-only later pages terminate safely.
- Provider timeout does not cancel an unrelated successful provider in the deterministic fixture.
- Provider outage is isolated from successful provider results.
- Search results remain independent of playable-source availability.

The existing unified grouping tests continue to pass for verified aliases, unverified aliases, conflicting years, media types, same-provider separation, sequels, remakes, specials, and movies.

A full mocked HTTP route harness for both JSON and SSE partial-failure payloads was not added, so those route-level scenarios remain a limitation of this acceptance run.

## I. Security and preservation checks

- No secrets, API keys, passwords, or credentials were added.
- No production `.env` file was added.
- The disposable PostgreSQL role and database were local test-only resources.
- The alias query uses PostgreSQL parameters and does not construct SQL from user input.
- No production database was contacted.
- No force push or history rewrite was performed.
- No APK, Firebase, website, authentication, analytics, playback, remux, streaming, or production-service files were modified.

## J. Final archive

The required archive filename is:

```text
ProphecyVerse-Backend-Phase1-Final-Acceptance.zip
```

The archive contains the complete Rust source, `Cargo.toml`, `Cargo.lock`, migrations, required documentation, this report, and `.env.example`. It excludes `.git/`, `target/`, build caches, production `.env`, keys, credentials, and temporary PostgreSQL data.

Archive size and SHA-256 are recorded after final packaging below.

## Final Git state

The acceptance implementation was pushed in commit:

```text
dc73ce5ad78fb3b7ab1c2c7a49cd0185a2f5ea4a
```

The final report commit is `4f6a43fab4678933777105dd84b2df7fa7e04915`. The final archive SHA-256 is recorded in the final delivery message because embedding a ZIP checksum inside the ZIP-contained report would change the checksum itself. The final branch must remain clean and synchronized with origin.
