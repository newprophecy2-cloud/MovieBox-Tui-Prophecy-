# Kari A-to-Z Repair Report

## A. Executive verdict

**PARTIAL PASS.** The local Rust repository was audited against the original Kari checkout and the requested public API paths. Confirmed high-impact fixes were implemented and the final Rust library build and tests pass. The work is not a complete A-to-Z pass because the new limiter is unit-tested and exposed as reusable infrastructure but is not yet wired through every provider request path, and no live 10-anime Termux acceptance run was performed in this sandbox. No live provider success is claimed.

## B. Confirmed root causes and implemented fixes

### Search amplification

The confirmed defect was in `src/api.rs`, `catalog_item_source_eligibility`, and `eligible_search`. Search launched one eligibility task per catalog result, loaded full episode lists, and then attempted source discovery for every episode until one worked. The existing semaphore bounded concurrent tasks but did not bound total upstream work. The fix keeps the full catalog result set, verifies only the first 12 results, and checks at most one representative episode for each sampled title. Full source resolution remains on the playback path. This preserves the existing response structures while preventing a 30-result search from becoming hundreds of episode requests.

### Ani.pm optional identity fields and hard-coded audio

The confirmed Rust parser required `source == "settlar"`, a non-empty `slug`, and rejected otherwise usable rows before the native numeric ID could be retained. The playback flow also hard-coded `lang=sub`, required `settlarSelection`, and sent `channel=sub`. The parser now treats `source`, `slug`, and optional language as optional metadata while still requiring a positive provider-native numeric ID and a non-empty title. The playback flow uses the identity language when present, checks the returned effective language, selects the corresponding subtitle or dub selection key, and passes the matching channel. This is intentionally conservative: it does not fabricate missing IDs or claim that an upstream dub route works without a matching response.

### Shared limiter infrastructure

`src/rate_limit.rs` adds reusable per-host limiter state with bounded concurrency, minimum spacing, cancellation-safe semaphore acquisition, shared cooldown, `Retry-After`, `X-RateLimit-Reset`, and bounded 5xx cooldown behavior. `HostLimiterRegistry` keeps host state independent, so one provider's cooldown does not block another provider. Four deterministic tests cover independent host cooldown, semaphore concurrency, retry-header parsing, and shared 429 cooldown. This module is not yet connected to every provider call site; therefore the report does not claim that the mandatory global AniList and all-provider adaptive architecture is complete.

## C. Rate-limit status

Implemented and tested locally: bounded per-host limiter primitive, independent host registry, shared cooldown, Retry-After parsing, X-RateLimit-Reset parsing, cancellation-safe waiting, and bounded 5xx cooldown. Not complete: global AniList cache/singleflight/circuit breaker, persistent multi-instance coordination, metrics, provider-specific overrides, and integration into every Kari and non-Kari HTTP request. The supported coordination scope is currently process-local.

## D. Provider status

| Provider | Search | Details | Episodes | Source discovery | Direct play | Sub/Dub | Final status |
|---|---|---|---|---|---|---|---|
| Anikoto | Existing code audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing native sub/dub parsing preserved | NOT LIVE TESTED |
| Miruro | Existing native parser audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing code audited; no live test | Existing track identity preserved | NOT LIVE TESTED |
| Ani.pm | Optional-field parser repaired; no live test | Route fallback retained; no live test | No live test | No live test | Language-aware bootstrap code compiled | Dub path not live verified | CODE REPAIRED, UPSTREAM NOT VERIFIED |
| AniVault | Native path remains explicitly blocked by prior verified 404 evidence | BLOCKED | BLOCKED | BLOCKED | BLOCKED | N/A | BLOCKED — UPSTREAM/NATIVE ID UNVERIFIED |
| AniCine | Native path remains blocked because no verified native catalog identity is present | BLOCKED | BLOCKED | BLOCKED | BLOCKED | N/A | BLOCKED — NATIVE ID UNVERIFIED |
| AniDream | Native path remains blocked because the available flow requires an external catalog-scoped identity | BLOCKED | BLOCKED | BLOCKED | BLOCKED | N/A | BLOCKED — IDENTITY UNVERIFIED |

FourKHDHub, Dramachi, CircleFTP, DhakaFlix, and Addons were not behaviorally rewritten. The library regression suite passed, but no live provider acceptance is claimed.

## E. API regression report

The required route names were not renamed or removed: `GET /api/v1/anime/search`, `GET /api/v1/anime/{id}`, `GET /api/v1/anime/{id}/episodes/{episode_id}/full`, and `POST /api/v1/play/direct`. Search still serializes the existing result, cursor, partial, provider-status, and language-intent fields. Direct playback continues to consume `source_id`; codec checks and playback identity validation remain in place. MovieBox remains disabled by the existing constant and configuration gate.

## F. Build and test report

- Branch: `kari`
- HEAD before and after edits: `85464aaee6ae7ee8d9d8a0df5f4a1a6be668fc8e`
- Formatter: `cargo fmt --all -- --check` — PASS
- Library check: `cargo check --lib` — PASS
- Library tests: `cargo test --lib` — PASS, **573 passed, 0 failed**
- New limiter tests included in the total: 4
- Baseline before edits: 569 passed, 0 failed
- Warnings: existing dead-code warnings remain; no warnings were suppressed
- Live acceptance: NOT PERFORMED in this sandbox; no fabricated live results

## G. Changed files

- `src/rate_limit.rs`: new reusable bounded host-limiter primitive and deterministic tests.
- `src/lib.rs`: registers the limiter module.
- `src/api.rs`: caps optional search source checks and checks one representative episode per sampled result.
- `src/providers/kari.rs`: makes Ani.pm optional identity metadata tolerant and removes the hard-coded subtitle-only bootstrap/channel path.
- `KARI_REPAIR_REPORT.md`: this report.

## H. Remaining blockers

The largest remaining blocker is integration completeness: provider modules still receive raw `reqwest::Client` references, so the new limiter is not automatically applied to every request. A production-grade completion should introduce a shared request coordinator into the provider client construction and add AniList-specific cache/singleflight/circuit-breaker state. Live upstream route and codec acceptance is also outstanding. AniVault, AniCine, and AniDream remain intentionally unavailable rather than being given fabricated identities or endpoints.

## I. Final repository safety

Final safety state:

```text
## kari...origin/kari
 M src/api.rs
 M src/lib.rs
 M src/providers/kari.rs
?? src/rate_limit.rs
?? KARI_REPAIR_REPORT.md
```

The branch remains `kari` and HEAD remains `85464aaee6ae7ee8d9d8a0df5f4a1a6be668fc8e`. No commit, push, PR, deployment, VPS/production access, APK modification, Firebase modification, destructive Git command, or remote-branch change was performed. Existing user work was preserved.

## Phase 2 verification addendum — 2026-11-12

The GitHub `kari` branch was cloned from `newprophecy2-cloud/MovieBox-Tui-Prophecy-`. The remote branch was already at `ada78533515a24f0dafb7ce5189be8d73415648f`, with the requested `85464aaee6ae7ee8d9d8a0df5f4a1a6be668fc8e` as its parent. No prior uncommitted working-tree changes were present in the GitHub checkout.

This follow-up adds a process-wide host registry API in `src/rate_limit.rs`. `acquire_url` keys cancellation-safe semaphore acquisition by the actual URL host, and `observe_url` records sanitized host/status data and adaptive 429/5xx cooldowns. The shared hooks are integrated into Ani.pm metadata/search/playback (including the dynamic Settlar embed host), Anikoto native watch/AJAX/server paths, MegaPlay embed/source discovery, Miruro’s common HTTP helper, Dramachi title/search/episode/file paths, and FourKHDHub health/page/preflight paths. Provider-specific cooldown state remains isolated by host. AniVault, AniCine, and AniDream remain blocked because no verified native upstream contract exists; no endpoint or identity was invented.

AniList now enforces an actual rolling 24-request/60-second process-local budget in addition to its 2.5-second minimum interval, one-request concurrency limit, cache, negative cache, singleflight, cooldown header handling, circuit breaker, timeout, and cancellation-safe waits. AniList GraphQL remains POST and is not automatically retried, consistent with the unsafe-POST rule.

Validation completed without live upstream acceptance:

- `cargo fmt --all -- --check`: PASS.
- `cargo check --lib`: PASS.
- `cargo test --lib`: PASS — 578 passed, 0 failed.
- `git diff --check`: PASS.
- Live Termux acceptance was not run because this environment is a sandbox rather than the requested Termux device; no live provider results are claimed.

This addendum does not claim a complete all-backend transport replacement: remaining raw HTTP paths include some addon, CircleFTP, DhakaFlix, MovieBox, proxy/media, TV, updater, and subtitle/poster paths, and the AniList coordinator has no active production metadata consumer in the current source. Those are explicit remaining integration items rather than fabricated completion claims.
