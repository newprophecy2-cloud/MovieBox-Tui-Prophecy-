

# Third Completion Phase — Remaining Workflow Implementation

## Verified starting point

The actual branch HEAD before this phase was `bbf9420c08a6c07ef8f657bd0c6fdc8d3024de3e`. The branch was clean and matched `origin/feature/unified-content-engine-completion`. The previously reported SHA `36c9baf9e97a104003d271ca959645365158c981` was not treated as the current HEAD.

## Remaining defects addressed

The previous implementation had lifecycle and reconciliation helpers but did not persist lifecycle transitions or provide durable reconciliation operations. Lookup conflict detection used a row lock, which does not lock a missing row and therefore did not fully protect concurrent inserts.

This phase adds migration 008 with a `provider_mapping_transition_history` table and a partial unique index for active resolved lookup ownership. Multiple unresolved historical candidates remain diagnosable, while only one resolved Provider/title/season/episode coordinate can be active.

`CatalogStore::transition_provider_title_status()` now validates the current status, validates the requested transition, locks the mapping row, updates timestamps, and records an audit history row transactionally. `reconcile_provider_title_mapping()` and `reconcile_provider_episode_mapping()` preserve old mappings, reject ownership conflicts, insert only safe new mappings, and record stale/reconciled state without silently transferring ownership.

The lookup import path retains its application-level conflict detection, while migration 008 supplies the database-level active-owner uniqueness guarantee needed for concurrent inserts.

## Migration 008

File: `migrations/008_task3_task4_lifecycle.sql`

It creates:

- `provider_mapping_transition_history`
- A lifecycle-history lookup index
- A partial unique index enforcing one active resolved lookup owner per Provider/title/season/episode coordinate

The migration is wired into `CatalogStore::migrate()` after migrations 001 through 007. It does not delete records or automatically clean up historical conflicts.

## Final validation after the last code change

Passed:

- `cargo fmt --all -- --check` — exit 0.
- `cargo check` — exit 0.
- `cargo check --all-targets` — exit 0.
- `cargo test --lib` — 575 passed, 0 failed.
- `cargo test --tests --quiet` — all non-ignored integration tests passed.
- `cargo test --lib unified::tests` — 14 passed, 0 failed.
- `git diff --check` — exit 0.

The PostgreSQL-dependent tests remain ignored because this sandbox has no PostgreSQL server, `psql`, Docker, Podman, or isolated `DATABASE_URL`. PostgreSQL migration execution, constraint validation, concurrent database writes, rollback, and persistent reconciliation tests therefore remain blocked and are not claimed as passed.

## Updated acceptance matrix

TASK 3:

- **PARTIALLY COMPLETE** — Provider identity validation, secondary-Provider resolution, conservative Episode aggregation, same-Provider collision protection, and Rust regression coverage pass. PostgreSQL-backed route fixtures and complete typed nonstandard Episode identity coverage remain unavailable or incomplete.

TASK 4:

- **PARTIALLY COMPLETE** — Five-state lifecycle representation, transition validation, transition history persistence, ownership-preserving reconciliation APIs, lookup active-owner uniqueness, composite ownership migration, diagnostics, and Rust tests are implemented. PostgreSQL execution is still required to validate migrations, rollback, uniqueness under concurrency, and durable reconciliation behavior.
