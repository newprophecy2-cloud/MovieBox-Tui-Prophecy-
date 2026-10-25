# Render test-failure remediation audit — 2026-10-25

## Scope

This remediation used the public Render black-box report as evidence and then inspected the current `main` source. No production catalog import, playback, download, real admin mutation, or destructive request was executed.

## Confirmed root causes

### Browse aggregation timeouts

The public browse routes dispatch through `home_compat_route`, then `home_route`, `classified_list_route`, or `home_section_route`. `homepage_items_uncapped` launches three MovieBox homepage requests concurrently for tabs `2`, `5`, and `8`, then performs additional `moviebox_classification_metadata` calls for series items. The HTTP client has a 15-second request timeout and the previous route-level browse deadline was the general 45-second `OVERALL_TIMEOUT`. Because the server writes headers only after the route result is assembled, a proxy with an 8-second budget observed a timeout with no headers.

The remediation adds a six-second `BROWSE_TIMEOUT` around homepage aggregation and a one-second `CLASSIFICATION_TIMEOUT` per classification lookup. Slow provider calls are cancelled by the timeout and return a structured retryable upstream error. This is a bounded behavior improvement, not a claim that the provider is healthy.

### PostgreSQL unavailable

The repository has actual PostgreSQL code and `migrations/001_catalog.sql`; this is not design-only. Startup reads `DATABASE_URL`, connects, applies the migration, and stores the catalog client. The public Render response `DATABASE_UNAVAILABLE` and diagnostics did not expose a database connection, but public black-box testing cannot reveal whether the variable is absent, invalid, unreachable, or rejected by PostgreSQL without privileged Render access. The source now logs only boolean initialization state and never logs the connection string.

Render configuration now declares `DATABASE_URL` and `REQUIRE_DATABASE=true`. `/health` remains lightweight. `/ready` returns HTTP 503 with a PostgreSQL dependency status when the catalog is mandatory but not initialized.

### Route contract mismatches

`/api/v1/genres` and `/api/v1/catalog/titles/{title_id}` were previously guarded by `state.catalog.is_some()`, so a missing database fell through to route-not-found (404) even though OpenAPI documented database-unavailable behavior. The handlers are now registered unconditionally and return structured 503 `DATABASE_UNAVAILABLE` when PostgreSQL is not initialized.

`/api/subtitles/extract` previously returned `SSRF_BLOCKED` for both a missing URL and an unsafe URL. It now returns `INVALID_REQUEST` for a missing `url` and reserves `SSRF_BLOCKED` for a rejected destination.

`/api/v1/latest` and `/api/v1/k-dramas/{id}` remain intentional 501 contracts because reliable release-date data and verified K-Drama detail behavior are not implemented. No fabricated catalog data was added.

## Local before/after checks

The remediation was run locally with `PORT=18080 REQUIRE_DATABASE=true` and no `DATABASE_URL`:

| Route | Before remediation evidence | After remediation local result |
|---|---|---|
| `/health` | 200 on Render | 200 in 0.003s |
| `/ready` | 200 despite no catalog dependency signal | 503 `not_ready` in 0.001s |
| `/api/v1/home` | Render probe: no headers within 8s | 503 structured upstream timeout at 6.003s |
| `/api/v1/genres` | Render 404 route-not-found | 503 `DATABASE_UNAVAILABLE` in 0.001s |
| `/api/v1/catalog/titles/render-invalid` | Render 404 route-not-found | 503 `DATABASE_UNAVAILABLE` in 0.001s |
| `/api/subtitles/extract` without URL | 400 with inconsistent `SSRF_BLOCKED` code | 400 `INVALID_REQUEST` in 0.001s |

The public Render service must be redeployed before these source changes can be observed there. No post-push Render deployment is claimed until the live endpoints are retested.

## Verification

Passed locally:

- `cargo fmt --all -- --check`
- `cargo check --locked`
- `cargo clippy --all-targets --all-features -- -D warnings`
- 39 `api::tests` passed
- local black-box health/readiness/route checks
- `git diff --check`

Not run:

- PostgreSQL integration/migration test against a real database
- Redis integration/invalidation test
- real Render redeploy verification
- provider playback/media test
- load test with p50/p95/p99
- production backup/restore

## Safe deployment steps

1. Provision a managed PostgreSQL database and apply the migration with a deployment role.
2. Set `DATABASE_URL` and a strong `ADMIN_TOKEN` in Render without printing values.
3. Keep `REQUIRE_DATABASE=true` for the catalog-first Render deployment.
4. Deploy the commit and confirm `/health` is 200 and `/ready` is 200 only after PostgreSQL initializes.
5. If `/ready` remains 503, inspect Render logs for the boolean initialization/error message without exposing the connection string.
6. Run controlled catalog upsert/import only after database connectivity is approved; do not bulk import during initial diagnosis.
7. Retest `/api/v1/search`, `/api/v1/genres`, `/api/v1/catalog/titles/{id}`, and the browse routes with bounded client timeouts.
