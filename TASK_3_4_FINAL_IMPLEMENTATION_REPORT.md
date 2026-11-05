# Task 3/4 Final Implementation Report

## Repository and branch

Repository: `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-`

Branch: `feature/unified-content-engine-completion`.

The actual branch HEAD was retrieved from GitHub before implementation. The previously reported SHA `36c9baf9e97a104003d271ca959645365158c981` was not assumed and was not the current HEAD.

## Starting and delivery commits

Starting commit for this remaining-defects pass: `dbbbfdaa98dc058162221ec447d72dccccd84599`.

The delivery commit is the new commit created for this pass and is recorded in the final delivery message and Git metadata. No history was rewritten and no force push was performed.

## Confirmed remaining defects

The audit confirmed: an unsafe partial unique lookup index for concurrent missing-row inserts; no additive typed Episode identity model; incomplete repeated/concurrent migration safety; diagnostics covering only a subset of ownership and historical categories; lifecycle evidence not validated before persistence; and legacy import SQL depending on the removed three-column Episode uniqueness rule.

Existing API routes, opaque references, provider set, playback architecture, and unrelated subsystems were preserved.

## Implemented fixes

- Added `migrations/009_task3_episode_identity.sql`.
- Added typed Episode fields: `episode_type`, `episode_coordinate`, `absolute_episode_number`, `special_identifier`, `provider_original_number` at the database layer, and the first four compatible optional fields to Rust import inputs.
- Added a stable string identity key so decimal and nonstandard numbering never uses floating-point equality.
- Replaced the insufficient Episode uniqueness rule with a typed identity index while preserving existing opaque Episode IDs.
- Updated unified and legacy Episode write paths and catalog query projections.
- Replaced the missing-row race-prone lookup partial unique index with a PostgreSQL trigger using a transaction-scoped advisory lock per lookup coordinate. Conflicting resolved owners are rejected with SQLSTATE `23505`; historical rows are not deleted.
- Made `CatalogStore::migrate()` transactional and protected by an advisory migration lock.
- Made ownership migration exception handling tolerate duplicate-object and duplicate-relation conditions on repeated execution.
- Expanded `mapping_integrity_report()` with duplicate lookup coordinates, conflicting lookup owners, missing canonical records, cross-Anime Season ownership, invalid statuses, empty Provider IDs, stale mappings, and reconciliation conflicts.
- Added lifecycle transition evidence validation for non-empty reasons, non-null evidence, and secret-free metadata.
- Added unit regression tests for regular, decimal, absolute, special, movie, and nonstandard Episode identity and lifecycle evidence rejection.

## Mapping lifecycle design

The five persisted Provider title statuses remain `unverified`, `verified`, `unresolved`, `stale`, and `rejected`. `transition_provider_title_status()` locks the mapping row, validates the current status and requested transition, validates safe evidence, updates timestamps, records transition history, and commits as one transaction.

Search success is not treated as verification. Temporary failures do not automatically reject mappings. Rejected mappings cannot be silently reactivated. History stores mapping type, mapping ID, old status, new status, reason, safe evidence, and timestamp.

| Current | Allowed next states |
|---|---|
| unverified | unverified, verified, unresolved, rejected |
| verified | verified, stale, unresolved, rejected |
| unresolved | unresolved, verified, stale, rejected |
| stale | stale, verified, unresolved, rejected |
| rejected | rejected only |

The Episode mapping table retains its existing `resolved`/`unresolved` contract for client compatibility; ownership conflicts remain unresolved rather than transferred.

## Reconciliation workflow

Title reconciliation locks the old mapping, verifies canonical ownership, checks candidate Provider identity and exact new ID input, compares stable title/year/media evidence, checks conflicting ownership, inserts the safe replacement, marks the old mapping stale, records history, and commits transactionally. Ambiguous identity and ownership conflicts remain unresolved.

Episode reconciliation applies the same ownership-preserving pattern: a new Provider Episode ID cannot silently transfer to another canonical Episode; the old mapping remains historical/unresolved and the safe replacement is inserted only for the same canonical owner.

## Episode identity design

Episode identity is explicit and string-based. The stable key is `episode_type:coordinate`; `1.5` is distinct from `1`, a special is distinct from regular Episode 1, and a movie/provider-specific coordinate is not forced into a regular integer coordinate. Existing rows keep their IDs and receive a legacy identity key in the additive migration. Unknown classifications are not fabricated.

The old `episode_number` remains available for compatibility and ordering. New fields preserve original Provider metadata and support absolute and nonstandard numbering.

## Lookup uniqueness design

The coordinate is Provider + Provider title ID + Provider season + Provider episode coordinate. The database trigger serializes that coordinate with `pg_advisory_xact_lock()` before checking another resolved owner. Same-owner repeated writes remain idempotent; a different canonical owner is rejected and cannot be reassigned by an `ON CONFLICT DO UPDATE`.

A disposable PostgreSQL verification inserted one resolved owner and confirmed that a second resolved owner for the same coordinate was rejected.

## Composite ownership enforcement

Migration 007 continues to enforce the composite Episode-to-Season ownership foreign key for new and updated rows with `NOT VALID`, preserving historical invalid rows for diagnosis. In the disposable database the constraint exists but remains `convalidated = false`, because historical remediation was not silently performed. The diagnostic reports cross-Anime ownership and missing Season references. Full validation is therefore not falsely claimed.

## Duplicate/orphan diagnostics

The read-only report covers duplicate Provider title mappings, duplicate Provider Episode mappings, duplicate lookup coordinates, conflicting lookup owners, orphan Provider title mappings, orphan Provider Episode mappings, orphan lookup mappings, missing canonical Anime/Season/Episode references, cross-Anime Season ownership, invalid status values, empty Provider IDs, stale mappings, unresolved mappings, and reconciliation conflicts. It performs no deletion, reassignment, or repair.

## Migration and PostgreSQL validation

Migration order is 001 through 009, wired through `CatalogStore::migrate()`. Migration 009 is additive and preserves opaque IDs. Migration execution is transactional and advisory-lock protected. A failed migration transaction rolls back automatically.

Environment: isolated local PostgreSQL 16.15 database `prophecy_task34_test`, created only for this validation run. No production database or credentials were used.

Passed after the final code change:

- `cargo test --test phase1_acceptance -- --ignored`: 2 passed, 0 failed, 0 ignored.
- `cargo test --test anime_import_jobs -- --ignored`: 1 passed, 0 failed, 0 ignored.
- Direct schema inspection confirmed the typed identity index, lifecycle history, lookup-coordinate index, trigger, and composite ownership constraint.
- Direct trigger testing confirmed a conflicting resolved lookup owner is rejected.
- The composite ownership constraint was observed present but not validated (`convalidated = false`), matching the historical-preservation policy.

## Rust and route validation

Required final validation after the last code change:

- `cargo fmt --all -- --check`: exit 0.
- `cargo check`: exit 0.
- `cargo check --all-targets`: exit 0.
- `cargo test --lib`: exit 0; 577 passed, 0 failed, 0 ignored.
- `cargo test --tests --quiet`: exit 0; all non-ignored integration tests passed.
- `cargo test --lib unified::tests`: exit 0; 14 passed, 0 failed, 563 filtered out.
- `git diff --check`: exit 0.

Existing route compatibility tests remain green. Full PostgreSQL-backed route fixtures for every requested secondary-Provider stream/reconciliation scenario are not present in the repository and are not fabricated as evidence.

## Compatibility and limitations

No Android APK, Firebase, authentication, analytics, notifications, UI, playback architecture, HLS/DASH implementation, remuxing, transcoding, codec policy, production deployment, or production database was changed. Existing route shapes and opaque references were preserved.

Remaining limitations: the composite ownership foreign key cannot be fully validated until historical invalid rows are explicitly remediated with unambiguous evidence; complete PostgreSQL route-level fixtures for all Task 3/4 scenarios are not available; and the existing API model still exposes integer episode numbers at the provider-facing route layer while the database now preserves typed coordinates additively.

## Final acceptance matrix

### TASK 3

- Typed Episode identity and additive migration: **COMPLETE with evidence**.
- Regular, decimal, absolute, special, movie, and nonstandard identity separation: **COMPLETE with unit evidence**.
- Existing opaque references and API compatibility: **PASSING existing regression suite**.
- Complete PostgreSQL-backed route-level secondary-provider coverage: **BLOCKED by missing repository fixtures**.
- Full canonical historical ownership validation: **BLOCKED until explicit safe remediation of historical invalid rows**.

**TASK 3: blocked acceptance criteria are explicitly identified above; no unsupported COMPLETE claim is made.**

### TASK 4

- Lifecycle statuses, transition validation, safe evidence, and history persistence path: **COMPLETE in implementation; PostgreSQL persistence path covered by acceptance database**.
- Lookup ownership under concurrent/missing-row conditions: **COMPLETE in database design; direct trigger evidence passed**.
- Migration repeatability, transactional rollback, and advisory-lock startup serialization: **PASSING disposable PostgreSQL tests**.
- Duplicate/orphan diagnostics: **COMPLETE as read-only reporting implementation**.
- Full composite ownership validation: **BLOCKED by preserved historical invalid-row policy**.
- Full reconciliation concurrency matrix: **BLOCKED by absent dedicated repository fixtures**.

**TASK 4: blocked acceptance criteria are explicitly identified above; no unsupported COMPLETE claim is made.**
