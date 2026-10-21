# Phase 3 Backend Audit — 2026-10-21

Repository: `newprophecy2-cloud/MovieBox-Tui-Prophecy-`
Baseline: `ca16ae5`
Phase 3 scope: the five non-MovieBox providers only — 4KHDHub, DramaChi, CircleFTP, DhakaFlix, and Addons.

## Executive result

The baseline was clean and reproducible. The Phase 3 implementation adds HLS playlist proxying/rewrite support, extends outbound URL hardening to poster/subtitle fetches and FFprobe, and closes an IPv4-mapped-IPv6 SSRF/DNS-rebinding classification gap. Deterministic regression, integration, Clippy, and release checks pass.

MovieBox remains excluded from the Phase 3 API provider execution list (`src/api.rs`); no MovieBox provider execution path was added. Existing legacy TUI/MovieBox code was not rewritten in this phase.

No deployment was performed.

## Feature-by-feature matrix

| Requirement | Status | Current implementation and evidence | Files changed | Remaining limitations |
|---|---|---|---|---|
| 4KHDHub priority | **Verified** | API provider order starts with 4KHDHub, followed by DramaChi, BDIX providers, and Addons. Existing suppression removes DramaChi only when a usable 4KHDHub series result exists. The live movie-resolution and Game of Thrones series-resolution tests passed. | None in this phase | Live evidence covered the repository's existing movie/series cases, not a new anime title matrix. |
| DramaChi fallback | **Verified** | DramaChi is registered and dispatched through `MovieBoxService`; fallback suppression retains it when the primary series result is absent or unusable. Existing live DramaChi search/details/streams test passed. | None in this phase | No new live anime query was fabricated or claimed. |
| CircleFTP | **Partially Verified** | Client, parser, direct-file release mapping, quality/codec/language detection, and deterministic parser tests exist. A sandbox reachability probe to `http://new.circleftp.net:5000/api/posts` timed out. | None in this phase | Requires access to the intended BDIX network; no external response was treated as a pass. |
| DhakaFlix | **Partially Verified** | Client has configured BDIX server list, dead-server TTL, concurrent search, quality-based deduplication, and stream mapping. All four configured private BDIX server probes timed out from the sandbox. | None in this phase | Requires a Bangladeshi ISP/BDIX route. The private addresses are intentional provider configuration, not public-cloud endpoints. |
| Addons | **Verified** | Cinemeta manifest fixture, catalog/meta conversion, stream aggregation, filtering, deduplication, audio/token parsing, enable/disable lifecycle, and invalid-manifest behavior are covered by deterministic tests. | None in this phase | No arbitrary live third-party addon was used; availability depends on the user's configured addon. |
| MovieBox excluded from Phase 3 | **Verified** | `MOVIEBOX_PROVIDER_ENABLED` remains false and the API `PROVIDERS` array contains exactly the five requested non-MovieBox providers. MovieBox source routes return the existing disabled response. | None in this phase | Legacy MovieBox/TUI implementation remains in the repository for compatibility and was not removed. |
| Source IDs/fingerprints/deduplication | **Verified** | Existing opaque references, release fingerprints, release reordering, same-label/different-mirror distinction, cursor pagination, and provider-isolated cache tests pass. | None in this phase | No live expired-source recovery was claimed beyond existing 4KHDHub resolver behavior. |
| HLS manifests, relative URLs, segments, keys | **Fixed and Verified** | Proxy previously rewrote DASH only. It now detects `.m3u8` and HLS MIME types, resolves relative URLs against the manifest URL, rewrites playlist lines and `URI="..."` attributes (including keys), caches rewritten manifests, and serves the HLS MIME type. Deterministic tests cover relative segments, variants, key URI, query/fragment detection. | `src/proxy.rs` | End-to-end external HLS playback was not claimed. Nested playlist hosts must satisfy the existing target-host allowlist; this is intentionally conservative. |
| DASH, Range, seeking, redirects | **Partially Verified** | Existing DASH rewrite, segment caching, Range forwarding, redirect policy, and security tests pass. | None in this phase | No new live DASH run was performed in Phase 3; existing ignored MovieBox live tests were not run because MovieBox is out of scope. |
| Direct play/remux/audio-only/H.265→H.264 | **Partially Verified** | Existing FFmpeg/FFprobe and transcode implementation remained intact; baseline and release checks pass. FFprobe now pre-validates the source destination before invoking the external tool. | `src/api.rs` | No provider-owned live media URL was fabricated. Existing synthetic media verification from the prior phase is not repeated as a new Phase 3 provider result. |
| Audio/subtitle track discovery | **Partially Verified** | Backend exposes FFprobe-derived audio and embedded subtitle track metadata and normalizes common language codes. APK/player capability is separate from backend metadata support. | `src/api.rs` | End-to-end APK selection against a live provider source was not claimed. |
| Cancellation, disconnects, FFmpeg termination, cleanup, concurrency | **Partially Verified** | Existing bounded connection/transcode semaphores, request timeouts, child-process lifecycle, and temporary-file protections remain covered by existing tests/build checks. | None in this phase | A new forced client-disconnect stress run and OS-level process/temporary-file measurement were not available as deterministic provider tests. |
| SSRF/DNS rebinding across outbound fetches | **Fixed and Verified** | Shared public URL validation was extended to poster fetches, subtitle downloads, and FFprobe inputs. IPv4-mapped IPv6 addresses now inherit IPv4 private/loopback/link-local classification. Existing provider/proxy URL-policy tests plus new mapped-address tests pass. | `src/net.rs`, `src/service.rs`, `src/api.rs` | FFmpeg performs its own network resolution after preflight; preflight reduces risk but is not a cryptographic pinning guarantee. |
| HTTP framework migration | **Not Implemented** | No migration was justified in this phase. The custom server's API routes, request bounds, connection limits, SSE behavior, APK-compatible JSON contracts, and existing test coverage make an unscoped framework migration high risk. | None | A separate migration RFC/compatibility harness would be required before replacing the server. |
| Latency P50/P95/P99 | **Partially Verified** | Reproducible local performance audit passed: `truncate_width` 10,000 ops, optimized 1.704575 ms; `md5_hex` 10,000 ops, 21.014671 ms; M3U parse 500 channels ×100, 322.875892 ms; TUI frame averages 813.09 µs (80×24), 1,193.83 µs (120×30), 1,401.85 µs (160×40). | None | Provider search/details/source/playback startup P50/P95/P99 were not reported because there were not enough controlled provider samples; no fabricated percentile values are included. |

## Changes made

1. `src/proxy.rs`
   - Added HLS URL detection.
   - Rewrites HLS relative/absolute playlist entries and `URI` attributes through the local proxy.
   - Applies manifest size limits and caches rewritten HLS manifests.
   - Returns an HLS MIME type for cached and freshly rewritten playlists.
   - Added deterministic HLS rewrite tests.
2. `src/net.rs`
   - Treats IPv4-mapped IPv6 addresses as their mapped IPv4 address for public/private checks.
   - Added loopback/private/public mapped-address regression tests.
3. `src/service.rs`
   - Applies shared destination validation before poster and subtitle downloads.
4. `src/api.rs`
   - Applies shared destination validation before FFprobe input execution.

## Tests and verification

Pre-change baseline from `ca16ae5`:

- `cargo fmt --all -- --check`: passed
- `cargo check --locked`: passed
- `cargo test --locked`: passed
- `cargo clippy --all-targets --all-features -- -D warnings`: passed
- `cargo build --release --locked`: passed

Post-change verification:

- 448 library tests passed.
- 595 listed integration-test entries were present; the full `cargo test --locked` run passed all runnable tests and reported the existing 15 live tests as ignored.
- Addons manifest/stream fixture tests passed.
- Existing content pipeline, error handling, player contract, settings, TUI acceptance, update lifecycle, and performance audit tests passed.
- HLS rewrite targeted regression passed.
- IPv4-mapped-IPv6 URL-policy regression passed.
- `cargo fmt --all -- --check` passed.
- `cargo clippy --all-targets --all-features -- -D warnings` passed.
- `cargo build --release --locked` passed.
- Local release smoke: `/health` returned HTTP 200, `/ready` returned HTTP 200, `/openapi.json` returned HTTP 200. A deliberately wrong `/api/v1/search` path returned the expected route-not-found response; the smoke shell's final `printf` had a formatting error, not the server.

Authorized live tests executed from the repository's existing ignored test set, excluding MovieBox:

- `test_live_fourkhdhub_movie_resolution`: passed.
- `test_live_fourkhdhub_game_of_thrones_resolution`: passed.
- `test_live_dramachi_search_details_and_streams`: passed.

Not executed or not passed:

- MovieBox live tests: intentionally not executed because MovieBox is outside Phase 3 scope.
- CircleFTP/DhakaFlix live provider tests: no deterministic test fixture was available and sandbox probes timed out outside the BDIX network.
- No claim is made for an exact live anime-title matrix, APK player invocation, or provider latency percentiles without controlled samples.

## Deployment recommendation and rollback

**Recommendation: do not deploy automatically.** The patch is suitable for a reviewed staging build because deterministic checks and the non-MovieBox live checks pass, but BDIX validation requires a real BDIX-connected environment and HLS end-to-end playback should be manually checked with an authorized source.

Before deployment, run the release binary on a BDIX-connected test machine, verify CircleFTP and DhakaFlix search/details/stream behavior, test one authorized HLS master playlist with relative segments and one DASH source, and confirm the Android/APK client consumes the unchanged API contracts.

Rollback is a single Git revert of the Phase 3 commit. The previous known-good baseline is `ca16ae5`; restore that commit or revert the Phase 3 commit, rebuild the release artifact, and repeat `/health`, `/ready`, and provider smoke checks. No Render deployment or production state change was made by this task.
