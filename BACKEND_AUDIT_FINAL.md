# Backend Audit and Repair — Final Report

**Repository:** `newprophecy2-cloud/MovieBox-Tui-Prophecy-`  
**Repair branch:** `audit/full-backend-anime-tests`  
**Audit date:** 2026-10-23  
**Baseline report:** [`BACKEND_BASELINE_AUDIT.md`](./BACKEND_BASELINE_AUDIT.md)

## Executive result

The backend was audited from the current repository state, deterministic validation was run before and after repair, and three confirmed issues were fixed without weakening the existing resolver or URL-security behavior.

The repaired branch is suitable for code review and deployment validation, but production readiness is not declared until the public deployment is rebuilt and the remaining external-provider/runtime assumptions are verified.

## Implemented repairs

### 1. Strict codec-aware search eligibility

`src/api.rs` now requires explicit H.264-compatible metadata (`H.264`, `AVC`, `AVC1`, `AVC3`, or `x264`) in addition to a valid public mirror URL before a release is counted as a verified search source.

The following are rejected from the verified search set:

- Missing or blank codec metadata.
- HEVC/H.265/x265.
- AV1.
- Other unknown codec labels.

The change does not rewrite provider source metadata and does not claim that an incompatible source is absent. It only prevents unverified or incompatible sources from being advertised as verified search results under the strict Android playback policy.

Regression coverage now proves that valid URLs with unknown codecs are rejected, H.264 is accepted, and HEVC/H.265/x265/AV1 are rejected.

### 2. Process-wide source-check concurrency limit

The expensive source-eligibility semaphore moved into `ApiState` and is shared across requests. The configured limit remains eight concurrent eligibility tasks, but concurrent clients can no longer multiply that limit by creating a new semaphore for every request.

Per-item timeout and overall provider timeout behavior remain in place.

### 3. Render startup optimization

`render.yaml` now starts the already-built release artifact:

```yaml
startCommand: ./target/release/moviebox-tui
```

This removes the unnecessary `cargo run --release` invocation from service startup while preserving the locked release build and `/health` health check.

### 4. Documentation alignment

`README.md` now documents the strict codec policy, process-wide source-check limit, and direct release-binary startup behavior.

## Validation evidence

The following checks passed after the Rust/config changes:

```text
cargo fmt --all -- --check                         PASS
cargo test --locked search_source_eligibility      PASS
cargo check --locked                               PASS
cargo test --locked                                PASS: 458 passed, 0 failed
cargo clippy --locked --all-targets --all-features -- -D warnings  PASS
git diff --check                                   PASS
```

The full suite reported 15 ignored live-provider tests. The ignored status is intentional because those tests require external providers and media tools.

A local service smoke test also returned quickly and successfully for `/health`, `/ready`, `/api/v1`, `/openapi.json`, and `/api/v1/diagnostics`. A provider-dependent local Naruto search did not return within the bounded 30-second client timeout; it is recorded as a latency/provider-environment limitation, not as a successful request.

## Live resolver evidence

The repository smoke test was run against the deployed API:

```bash
python3 scripts/live_resolver_test.py \
  --base-url https://moviebox-tui-prophecy.onrender.com \
  --timeout 120
```

Observed result:

```text
Dan Da Dan  FAIL: source request failed (RemoteDisconnected)
Lookism     PASS: final_host=pixeldrain.dev; detected H.264 video and audio/subtitle tracks
Blue Lock   PASS: final_host=pixeldrain.dev; detected H.264 video and audio/subtitle tracks
Summary: 2/3 resolver cases passed
```

The two successful cases demonstrate automatic resolver traversal through the public API to a final media host and H.264 media. Dan Da Dan failed with an upstream connection disconnect; no backend code regression is established from one external-provider failure. It should be rerun after provider recovery or after deployment rebuild.

## Remaining risks and follow-up

1. Search eligibility still relies on provider metadata plus validated mirror syntax for the search gate. Final media preflight remains provider/resolution-specific and is performed on playback/resolver paths; a future hardening phase could add bounded final-media probing to the search gate, but that would materially increase latency and dependence on `ffprobe`.
2. `ffprobe` is required by audio/subtitle inspection and the live resolver script. Render/runtime provisioning should explicitly verify that it is installed before enabling those routes as a production SLO.
3. Release and playback caches do not fully coalesce concurrent cache misses. This is an optimization opportunity, not a confirmed correctness failure.
4. Live provider tests remain environment-sensitive and may fail because of rate limits, HTML changes, expired signed links, or upstream disconnects.
5. The public Render deployment must be rebuilt after this branch is merged or deployed; the live resolver result above may reflect the previous deployment until that happens.
6. No Hostinger VPS migration or production infrastructure change was performed in this audit.

## Acceptance decision

- **Build/test quality:** PASS.
- **Strict codec policy:** PASS for deterministic metadata eligibility tests.
- **Global source-check concurrency:** PASS by code inspection and compilation; a dedicated load test remains a follow-up.
- **Render startup configuration:** PASS in repository configuration; deployment runtime still needs rebuild verification.
- **Live resolver:** PARTIAL, 2/3 cases passed; one upstream disconnect remains unresolved.
- **Overall production readiness:** CONDITIONAL, pending deployment rebuild, ffprobe/runtime verification, and repeat live smoke tests.
