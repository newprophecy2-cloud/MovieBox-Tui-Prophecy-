# MovieBox-TUI Prophecy — Phase 2 Backend Audit

**Baseline:** `c25a6f99787f0d61fa68c3a5d839b0d4d827dc72` (`c25a6f9`)

**Scope:** focused evidence-based audit and implementation of the remaining high-priority backend requirements. No deployment was performed.

## Status Matrix

| Priority | Requirement | Status | Current implementation / evidence | Exact files changed in Phase 2 | Tests / result | Remaining limitation |
|---:|---|---|---|---|---|---|
| 1 | Verify `c25a6f9` request and transcoding fixes | **Verified** | Confirmed the commit adds 64 KiB request buffering, 10s read timeout, HTTP/1.1 request-line validation, 256 connection permits, and final transcoding URL validation. | `src/api.rs` (existing Phase 1 change) | `cargo check --locked`, focused tests, full regression | The custom server remains custom; verification is code/test based, not a formal protocol-fuzz campaign. |
| 2 | Audit outbound HTTP, redirects, DNS, manifests, segments, subtitles and FFmpeg inputs | **Fixed and Verified** for covered paths | Added shared URL syntax policy, redirect target filtering, production DNS resolver filtering, async public-destination validation in the proxy, and validation for addon manifest/catalog/meta/stream requests. Proxy validates every extracted target before manifest/segment/subtitle fetch. | `src/net.rs`, `src/proxy.rs`, `src/providers/addons/client.rs`, `src/api.rs` | URL policy tests; proxy DASH fixture tests; addon integration suite; full tests and Clippy pass | Provider-specific code still contains duplicated validators, and no OS-level network sandbox exists. DNS is checked before request and by the resolver, but a complete formal rebinding proof is not claimed. |
| 3 | Determine whether rejecting all HTTP media breaks legitimate providers | **Fixed and Verified** as a compatibility policy | HTTP is permitted for public media/TV/addon destinations because existing fixtures and TV parser inputs contain `http://` streams. Private/local HTTP is rejected in production. FFmpeg transcoding remains HTTPS-only. | `src/net.rs`, `src/proxy.rs`, `src/api.rs` | `net::tests::outbound_url_policy_allows_public_http_media_but_rejects_private_targets`; existing TV/addon tests; proxy local fixtures | Public HTTP is transport-insecure. It is preserved for compatibility, not recommended for new provider configuration. |
| 4 | Evaluate replacing custom TCP HTTP server | **Partially Verified** | Existing API is a hand-written TCP server with 36 OpenAPI paths, custom SSE, streaming responses, range handling, and APK-facing response contracts. A framework migration would risk route parsing, connection semantics, SSE, headers, and media streaming compatibility. No migration was made because the Phase 2 evidence did not justify the contract risk. | No framework migration; request hardening remains in `src/api.rs` | Format/check/test/Clippy/release checks pass | A maintained framework remains a future option. It requires contract tests and client compatibility testing before migration. |
| 5 | Audit 4KHDHub, DramaChi, MovieBox, Addons, CircleFTP and DhakaFlix | **Partially Verified** | Provider dispatch and deterministic fixtures were inspected. 4KHDHub is first in API priority; DramaChi fallback suppression is tested; Addons/CircleFTP/DhakaFlix have separate adapters. MovieBox API catalog remains explicitly disabled because upstream episode mapping is unverified. | No provider implementation change in Phase 2; provider paths inspected | Existing provider/parser/adapter tests pass; 15 live provider tests remain ignored | No authorized live provider run was performed. No live response or media URL is claimed. |
| 6 | Source IDs, release fingerprints, episode matching, deduplication and pagination | **Fixed and Verified** for confirmed defects | Search cursor was validated but ignored; it now applies offset pagination and emits a next cursor. Release fingerprint previously hashed mirror labels but not mirror URLs; it now includes resolver URL and direct-file flag, preventing same-label mirror collisions. | `src/api.rs` | New fingerprint regression test; existing source reorder/episode ID tests; full suite pass | Search pagination is based on a fresh merged provider snapshot per request, not a server-side snapshot token. Provider page fetching remains page-1 based in the current API aggregation. |
| 7 | HLS/DASH manifests, relative URLs, segments, Range, seeking, redirects, expired-source recovery | **Partially Verified** | DASH MPD rewriting, relative/host-scoped rewriting, segment prefetch, range chunking, byte-range responses, cache/in-flight deduplication and expired-link detection exist. Shared destination validation now covers proxy target requests. | `src/net.rs`, `src/proxy.rs` | Existing DASH rewrite, range, chunk-cache and warmup fixtures pass | HLS playlist proxying is not implemented as a dedicated path; ignored live MPD tests were not run. Redirect and expired-source behavior has deterministic coverage only where existing fixtures exercise it. |
| 8 | Direct play, remuxing, audio-only and H.265-to-H.264 transcoding | **Partially Verified** | Direct playback and FFmpeg H.264 transcode paths exist; FFprobe/FFmpeg are installed in the sandbox. A local synthetic HEVC+AAC fixture was probed and transcoded to H.264+AAC MPEG-TS successfully. | `src/api.rs` (validation), existing `src/proxy.rs`/media paths inspected | Local fixture: input `hevc+aac`; output `h264+aac`, 320x180, 2 seconds. Full Rust suite pass | No real provider stream was transcoded. Audio-only transcoding and full API streaming path were not independently fixture-tested. Remux-specific output compatibility is not established. |
| 9 | Audio and subtitle selection end-to-end, separated from APK capability | **Partially Verified** | Backend probes FFprobe streams and returns confirmed embedded audio/subtitle track metadata. TUI selection and player command contracts have deterministic tests. | No new Phase 2 file beyond security validation paths; inspected `src/api.rs`, playback and player code | Existing track parsing, subtitle/player contract tests pass | Actual APK deserialization, UI selection and device decoder behavior were not available. External subtitle fetch live behavior was not run. |
| 10 | Cancellation, disconnects, FFmpeg termination, temp cleanup, concurrency, graceful shutdown | **Partially Verified** | FFmpeg uses `kill_on_drop`, explicit kill paths and a two-slot semaphore. Proxy uses connection guards, in-flight segment guards and an idle watchdog. TUI request/download cancellation and cleanup exist. | No new dedicated cancellation implementation in Phase 2; inspected `src/api.rs`, `src/proxy.rs`, `src/download.rs`, TUI request/playback code | Existing cancellation, download cleanup, proxy cache and playback tests pass | API `run()` has no explicit graceful shutdown signal path; custom server shutdown under SIGTERM and client-disconnect stress were not separately tested. |
| 11 | Reproducible P50/P95/P99 latency benchmarks | **Blocked** | Existing performance audit test is a correctness/performance matrix, not a provider latency benchmark. Search/details/episodes/source resolution require upstream providers. | No benchmark implementation added | No P50/P95/P99 numbers reported; this is intentional | Requires authorized stable provider fixtures or recorded replay servers, controlled cache state and repeated samples. A single Render cold-start observation is not used as a benchmark. |
| 12 | Regression, security, integration, build and release checks | **Verified** | Formatting, locked check, deterministic unit/integration tests, Clippy and release build pass. Working tree was clean before Phase 2 edits; final diff was checked for whitespace. | `src/api.rs`, `src/net.rs`, `src/proxy.rs`, `src/providers/addons/client.rs` | Final deterministic run: 578 tests across lib/integration targets passed; 15 live tests ignored; `cargo clippy --all-targets --all-features -- -D warnings` and release build were run after the final code changes | Ignored live tests remain unverified and are not counted as passed. |
| 13 | Deployment recommendation and rollback plan | **Verified** | No automatic deployment was performed. Recommendation: deploy only after live provider/media tests and benchmark fixture work. Roll back to `c25a6f9` if Phase 2 is deployed and a regression appears. | This report only | `git diff --check`; deployment not invoked | Render was not updated by this phase; no live verification of a Phase 2 deployment is claimed. |

## Phase 2 Implementation Details

### Shared outbound policy

`src/net.rs` now provides:

- `is_public_ip`
- `validate_url_syntax`
- `validate_public_url`
- A resolver that filters private/local DNS results outside the test environment
- A redirect policy that rejects malformed, credential-bearing, local/private-literal, and disallowed-scheme redirect targets

The proxy validates each extracted request target before it is used for an upstream manifest, segment, subtitle, or media fetch. Addon manifest, catalog, metadata, and stream endpoint requests validate their endpoint before Reqwest sends them.

Tests allow loopback fixture servers only in the repository’s detected test environment. This is test infrastructure behavior, not a production bypass.

### HTTP compatibility policy

The policy deliberately does **not** reject every `http://` media URL. Existing TV/M3U fixtures and configured provider behavior include HTTP streams. Public HTTP is therefore supported for direct media/proxy compatibility. It is not permitted for FFmpeg transcoding, and private/local HTTP destinations are rejected in production.

### Correctness fixes

Search pagination now uses the decoded cursor as an offset and returns a cursor when more merged results remain. Source fingerprints now include each mirror’s resolver URL and direct-file property in addition to existing release fields and labels.

## Framework Migration Decision

Migration to Axum, Hyper, or another maintained HTTP framework was not justified for this phase. The current server exposes streaming media, custom SSE events, range behavior, opaque IDs, structured errors, and 36 documented paths. A migration without APK contract tests and streaming compatibility fixtures would create more risk than evidence-based benefit. Request framing and resource bounds were hardened in place.

## Media Fixture Evidence

The sandbox has:

```text
ffmpeg  6.1.1
ffprobe 6.1.1
```

A 2-second synthetic 320x180 HEVC+AAC MKV was generated locally. FFprobe identified `hevc` video and `aac` audio. A bounded FFmpeg conversion produced MPEG-TS with `h264` video and `aac` audio. This verifies the toolchain and codec command shape only; it is not evidence that an external provider source or every client will work.

## Deployment Recommendation

Do not deploy Phase 2 automatically. Before deployment:

1. Run the ignored provider/media tests only in an authorized environment.
2. Add replay fixtures for search, details, episodes, source resolution, DASH/HLS, range and expired-source recovery.
3. Run repeated latency samples and report P50/P95/P99 with cache state and sample count.
4. Validate APK response compatibility and device playback.
5. Deploy through the existing Render configuration with `/health` and `/ready` checks.
6. After deployment, verify the deployed commit explicitly; do not infer deployment from a Git push.

Rollback is a redeploy of `c25a6f9` or a revert of the Phase 2 commit. No force push or destructive operation is required.

## Final Assessment

Phase 2 fixed and verified the confirmed pagination/fingerprint defects and added a shared outbound guard across the main proxy/addon paths. The backend is not classified as fully production-ready because live provider behavior, HLS, APK/device playback, audio-only media, graceful API shutdown, and reproducible provider latency percentiles remain partially verified or blocked.
