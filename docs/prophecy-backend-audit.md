# MovieBox-TUI Prophecy Backend Audit & Update Report

## A. Executive Summary

The repository was audited from the `main` branch at starting commit `32f5e5dcb31d765cdce8b112683479e683b5a301`. The existing backend is a Rust application with a hand-written asynchronous TCP HTTP server, provider adapters, in-process caches, FFmpeg/FFprobe integration, and Render deployment configuration. It was not replaced or migrated to a new framework.

This update makes two confirmed hardening changes in `src/api.rs`:

1. API request parsing now waits for a complete HTTP header block with a bounded 10-second read timeout and a 64 KiB request limit. Invalid request lines return a structured 400 response, and the server caps active API connections at 256.
2. H.264 transcoding now rejects credential-bearing URLs, non-HTTPS sources, known intermediate/archive endpoints, literal private addresses, and DNS names that resolve to private, loopback, link-local, unique-local, unspecified, or otherwise prohibited destinations. The validated URL—not the unvalidated resolved value—is passed to FFmpeg.

The final local verification passed: formatting, locked dependency check, all deterministic tests, Clippy with `-D warnings`, and the release build. Live Render health, readiness, and OpenAPI smoke checks also returned HTTP 200. Live provider/media tests remain intentionally unexecuted because they are marked ignored and require authorized upstream access and external tools.

## B. Repository Baseline

- Repository: `newprophecy2-cloud/MovieBox-Tui-Prophecy-`
- Branch: `main`
- Starting commit: `32f5e5dcb31d765cdce8b112683479e683b5a301`
- Working tree before edits: clean
- Rust package: `moviebox-tui` version `0.1.26`
- Minimum declared Rust version: `1.90.0`
- Main runtime: Tokio plus a hand-written TCP HTTP server
- HTTP client: reusable configured Reqwest clients with connection pooling
- Cache: in-process release/playback maps in the API and filesystem/TUI caches elsewhere
- Deployment: `render.yaml`, binds to `0.0.0.0:$PORT`, default port `8080`
- Existing deterministic baseline: 442 tests passed; 15 live-network tests were ignored
- Initial Clippy invocation was blocked only because the sandbox toolchain did not have the Clippy component; after installing the component, the pre-existing dead-code and one lint were corrected/isolated and the final Clippy run passed

## C. Feature Implementation Matrix

| Feature | Previous state | Change made | Verification | Status |
|---|---|---|---|---|
| Bounded API request parsing | One read of up to 64 KiB could process an incomplete request and had no slow-client deadline | Read until `CRLFCRLF`, cap at 64 KiB, enforce a 10-second read timeout, validate the HTTP/1.1 request line | Local release binary smoke test; `cargo test`, `cargo clippy` | Verified |
| API connection exhaustion control | Every accepted connection spawned a task without an API-level active connection cap | Added a 256-permit connection semaphore and `SERVER_BUSY` response | `cargo check`, `cargo test`, release build | Verified |
| Transcoding URL validation | Literal private-IP filtering existed, but DNS names were not checked at the point immediately before FFmpeg execution | Added credential rejection and asynchronous DNS resolution; every resolved address must be public and permitted | Unit tests for credentials/private IPv4/IPv6; release build | Verified for the implemented validation layer |
| FFmpeg input safety | FFmpeg already used argument-based invocation and bounded transcode slots | Preserve argument-based invocation and pass only the validated destination URL | Existing transcode tests plus full suite | Verified at code/test level; no real transcode fixture run |
| Provider registry and fallback | Existing provider dispatch and deterministic provider priority were present | No broad rewrite; preserved existing policy and tests | Existing provider/search tests | Existing behavior preserved |
| Stable source identity | Existing opaque source references include provider, content identity, episode, release fingerprint, and index fallback | No incompatible format change | Existing source fingerprint tests | Preserved |
| API contract compatibility | Existing routes and response fields already existed in the repository | No route rename/removal; additions remain additive | OpenAPI/local and Render smoke checks | Preserved |

## D. Provider Report

The repository contains adapters for 4KHDHub, Dramachi, MovieBox, CircleFTP, DhakaFlix, Stremio-style addons, and TV/M3U. Dispatch is centralized in `MovieBoxService` and provider capabilities are exposed through the provider trait.

4KHDHub is ordered first in the API provider priority list. Dramachi is retained as a fallback when a usable primary series result is not present. The current API search orchestration runs configured providers concurrently, then applies deterministic provider ordering and removes Dramachi series results when a usable 4KHDHub series result exists. This behavior is covered by existing unit tests.

MovieBox is intentionally disabled in the API catalog path because the repository records its upstream episode mapping as unverified. The implementation preserves that explicit decision rather than presenting unverified MovieBox catalog data as working. MovieBox provider code remains present for the existing TUI and live verification suite.

Addons are supported through configured manifests and stream addons. Existing tests reject invalid schemes, magnets, local-file sources, and unsupported stream entries. CircleFTP and DhakaFlix remain separate adapters and are not treated as identical implementations.

Live provider capability verification was not claimed. The repository’s 15 live provider/media tests remain ignored and require external provider availability, authorization, and in some cases `yt-dlp`, FFmpeg, or a desktop player.

## E. API Route Report

The actual server routes are registered in `src/api.rs`. The OpenAPI document exposed by the local binary contained 36 paths at verification time. The main groups are:

- Process and documentation: `/`, `/health`, `/ready`, `/api/v1`, `/openapi.json`, `/docs`
- Search: `/api/v1/anime/search`, `/api/v1/anime/search/stream`
- Anime: `/api/v1/anime/{anime_id}`, `/api/v1/anime/{anime_id}/seasons`, `/api/v1/anime/{anime_id}/episodes`
- Episode sources: `/api/v1/episodes/{episode_id}/sources`, `/api/v1/episodes/{episode_id}/sources/stream`
- Source resolution: `/api/v1/sources/{source_id}`, `/api/v1/sources/{source_id}/playback`, `/api/v1/sources/{source_id}/play`, `/api/v1/sources/{source_id}/watch`
- Tracks: `/api/v1/sources/{source_id}/audio`, `/audio-tracks`, `/subtitles`, `/subtitle-tracks`
- Media operations: `/api/v1/sources/{source_id}/download`, `/api/v1/sources/{source_id}/transcode`
- Catalog paths retained by the implementation: home, latest, trending, popular, movies, K-dramas, and categories paths; MovieBox-dependent paths return the explicit `PROVIDER_DISABLED` or `UNSUPPORTED_CATEGORY` response while the provider is disabled

Existing response conventions include opaque URL-safe IDs, structured `error.code`, human-readable messages, provider labels where appropriate, retryability, and request IDs. No existing route was renamed or removed.

## F. Media Pipeline Report

- Source IDs use an opaque serialized structure and a release fingerprint. The resolver first matches the fingerprint and only then uses the legacy index fallback, reducing accidental remapping when ordering changes.
- Direct playback remains preferred for sources that already expose a playable URL.
- FFprobe is used for embedded audio and subtitle discovery where a directly probeable URL exists.
- FFmpeg transcoding uses argument-based process invocation, a two-slot semaphore, startup and I/O timeouts, and child-process cleanup on failure or disconnect.
- The implemented H.265-to-H.264 path outputs H.264 video, AAC audio, `yuv420p`, and MPEG-TS over the existing streaming response. No claim is made that every client or source is compatible.
- The new URL validation runs immediately before transcoding and re-checks DNS destinations. It does not bypass DRM or access controls.
- Existing proxy code handles DASH manifest rewriting, segment fetching, bounded segment caching, range parsing, and host matching. This update did not claim universal HLS/DASH compatibility.
- Audio/subtitle backend discovery is separate from APK UI/decoder support. APK-side capability negotiation was not available in this repository-only task.
- Session cancellation and cleanup are implemented for the existing sidecar/transcode paths, but full device-level playback recovery still requires client cooperation.

## G. Security Report

Implemented or preserved controls include:

- Request-size, request-line, slow-client, active-connection, provider-timeout, and transcode-slot limits.
- Argument-based FFmpeg/FFprobe invocation without shell interpolation.
- Redaction of common secret-bearing response headers such as cookies, authorization, API keys, and auth tokens.
- Opaque IDs that do not directly expose signed upstream URLs.
- Rejection of non-HTTP URLs in existing addon/content filters.
- New transcoding destination checks for HTTPS, credentials, known intermediate paths, literal private IPs, and DNS-resolved private destinations.

Residual risks remain. The project still uses a custom HTTP parser rather than a maintained HTTP framework. Full DNS rebinding protection for every outbound provider request and redirect is not established by this patch. Provider-specific redirects, addon-manifest trust, and all poster/subtitle fetch paths require a broader network-security review before claiming complete SSRF protection. No security claim is based solely on a passing test suite.

## H. Performance Report

No fabricated before/after performance claim is made. Existing local performance tests passed, and the new connection/request limits reduce unbounded resource use. Render smoke timing showed a cold-start-sensitive `/health` request of approximately 24.6 seconds on the first request and `/ready` of approximately 2.3 seconds in the observed run. These are single observations, not a benchmark or SLA.

A proper performance study still needs repeated samples, cache-state control, percentile measurements, and an authorized provider/media fixture. No uncontrolled load test was run against public providers.

## I. Test Results

The following final commands passed in the sandbox:

```text
cargo fmt --all -- --check                 PASS
cargo check --locked                       PASS
cargo test --locked                        PASS: 444 unit/integration tests; 15 live tests ignored
cargo clippy --all-targets --all-features -- -D warnings  PASS
cargo build --release --locked             PASS
```

Additional verification:

- Local release `/health`: HTTP 200, `{"status":"ok"}`
- Local release `/ready`: HTTP 200
- Local release `/openapi.json`: HTTP 200, 36 paths
- Local malformed request smoke test: connection was closed without a false success response
- Render `/health`: HTTP 200
- Render `/ready`: HTTP 200
- Render `/openapi.json`: HTTP 200
- Ignored live-provider/media tests were not run; no live playback or full-episode download was claimed

## J. Deployment Report

The code was not deployed by this task. The current Render service was queried non-destructively only. The repository’s `render.yaml` remains the deployment source and uses `$PORT` with `/health` as the health check. The previous working commit is `32f5e5d`; rollback is available by redeploying that commit or reverting the update commit. No force push, secret rotation, VPS setup, or destructive deployment operation was performed.

## K. Remaining Work

1. Replace or more comprehensively harden the hand-written HTTP parser with a maintained framework only after preserving the current route and response contract.
2. Apply one shared outbound URL/redirect/DNS policy to every provider, addon, poster, subtitle, probe, proxy, and media-fetch path.
3. Add authorized sanitized HLS/DASH and FFprobe fixtures for deterministic manifest, codec, range, subtitle, and cancellation tests.
4. Run ignored live-provider tests only against authorized, available upstreams and record provider-specific failures separately.
5. Verify APK response deserialization and playback behavior with the actual client or a contract fixture.
6. Measure repeated local and deployed latency/throughput/resource samples before claiming performance improvements.
7. Decide whether the intentionally disabled MovieBox API catalog should remain disabled or be re-enabled after its episode mapping is independently verified.

## L. Final Engineering Assessment

**Audit completed for the inspected architecture and current routes.**

**Implemented but not fully verified** for production-wide provider, media, APK, and SSRF behavior. The implemented patch is verified against the available local deterministic test environment and the non-destructive Render smoke endpoints. The project should not be classified as universally production-ready until the remaining provider, media-fixture, client-compatibility, and outbound-network items are addressed.
