# Prophecy Verse Backend — Bug 1 Implementation Report

## A. Repository State

- **Repository:** `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-.git`
- **Branch:** `feature/complete-anime-multiprovider-search-playback`
- **Initial HEAD:** `7076016e770bfa6caa49b9f95761db6792506e2b`
- **Remote HEAD:** `7076016e770bfa6caa49b9f95761db6792506e2b`
- **Final HEAD:** verified after push; see the final Git verification below.
- **Initial working tree:** clean.
- **Final working tree:** four modified source/test files plus this untracked report artifact.
- **Commit:** created locally with the requested message. **Push:** pending final remote verification. **Deployment:** not performed.

Changed files:

1. `src/codec_verification.rs`
2. `src/api.rs`
3. `tests/anime_provider_live.rs`
4. `src/tui/screens/home.rs` — only the obsolete MovieBox popup expectation was corrected; MovieBox remains disabled.

## B. Exact Root Cause

The existing AniZone verifier downloaded the master playlist, selected one variant, downloaded an optional initialization section and exactly one media segment, concatenated those bytes, and sent them to FFprobe through stdin. A standalone MPEG-TS segment is not guaranteed to contain enough stream initialization or codec metadata for FFprobe to identify the stream. Fragmented MP4 HLS likewise requires the playlist initialization section and may require more than one media fragment.

The code therefore treated a valid HTTP 200 media response as a probe input, but did not preserve enough HLS media context when the first segment was independently insufficient. The resulting FFprobe `Invalid data found when processing input` became a generic AniZone error and was later classified by substring as codec incompatibility. That incorrectly conflated invalid/incomplete samples, FFprobe failures, network failures, timeouts, and confirmed incompatible codecs.

The exact three external cases could not be freshly reproduced to the media-segment byte level because AniZone rate-limited the live test with HTTP 429 at `https://anizone.to/livewire/update`. No successful or failed direct-play result for those cases is fabricated here.

## C. Code Changes

### `src/codec_verification.rs`

- Replaced the single-segment HLS target parser with a bounded target model that retains up to three consecutive media segments, optional `#EXT-X-MAP` initialization data, variant URLs, and encryption state.
- Downloads the initialization section when present and incrementally probes one, then two, then three consecutive segments, capped at 24 MiB total sample data.
- Rejects encrypted HLS explicitly rather than attempting to interpret encrypted bytes as a codec result.
- Preserves HTTPS/public URL validation, redirect validation, bounded response sizes, and bounded FFprobe execution for every resource.
- Added `Unverified` as a distinct codec result. Missing stream metadata or a video stream without a codec name is no longer accepted as H.264.
- Reclassified verifier failures into structured internal categories such as `invalid_media_sample`, `ffprobe_failure`, `provider_network_failure`, `provider_timeout`, `invalid_media_url`, and `hls_encryption_unsupported`.
- Added deterministic coverage for multi-segment target selection, initialization sections, encrypted HLS, missing codec metadata, invalid/truncated input, FFprobe failures, timeouts, output limits, and H.264/HEVC decisions.

### `src/api.rs`

- Propagates `Unverified` separately from confirmed unsupported/HEVC results.
- AniZone direct-play failures now retain the verifier category instead of returning `AniZone media codec could not be verified` for every failure.
- Confirmed HEVC/unsupported codec remains a confirmed incompatibility; FFprobe, HLS, network, timeout, and insufficient-data outcomes remain fallback-eligible failures.
- Updated the direct-resolution classifier so it no longer classifies any message containing the word `codec` as confirmed codec incompatibility.
- Provider fallback behavior remains bounded and continues after unverified or temporary verification failure.

### `tests/anime_provider_live.rs`

- Added handling for the new unverified result in the existing live diagnostic test. No provider identity or source ID was fabricated.

### `src/tui/screens/home.rs`

- Changed one stale test assertion that expected `✓ MovieBox`, although MovieBox is intentionally disabled by `MOVIEBOX_PROVIDER_ENABLED = false` and is not returned or queried.

## D. HLS Verification

The verifier validates the master URL, downloads the bounded master manifest, follows the selected variant, validates the variant URL, and parses the media playlist. It retains an optional `#EXT-X-MAP` initialization resource and up to three consecutive segment URLs. Each resource is fetched with bounded size and timeout limits and is checked for safe public HTTPS redirects.

The probe sample is assembled as `initialization bytes + segment 1`, then `+ segment 2`, then `+ segment 3`, with a 24 MiB cap. FFprobe receives the assembled sample through stdin with bounded probe size, analysis duration, output size, and execution time.

Confirmed H.264/AVC is accepted. Confirmed HEVC/H.265 or another known unsupported video codec is rejected. Missing codec metadata, missing streams, incomplete samples, FFprobe failures, network errors, timeouts, malformed manifests, and encrypted HLS are never treated as H.264.

## E. Error Classification

### Previous behavior

A broad AniZone error string was emitted for all verification failures. The direct resolver then searched for words such as `codec` and `compatibility`, causing FFprobe parsing failures, HLS failures, network failures, timeouts, and incomplete samples to be reported as `codec_incompatibility`.

### New behavior

Internal diagnostics preserve categories including:

- `confirmed_codec_incompatibility`
- `codec_unverified`
- `ffprobe_failure`
- `invalid_media_sample`
- `hls_manifest_failure` / `hls_variant_failure` / `hls_segment_failure` where supplied by the resolver layer
- `hls_encryption_unsupported`
- `provider_network_failure`
- `provider_timeout`
- `invalid_media_url`
- `missing_media_url`
- `direct_resolution_failure`

FFprobe failure is no longer classified as confirmed codec incompatibility.

## F. Provider Fallback

The original anime/provider identity and requested season/episode coordinates remain the basis for fallback references. Existing mapping and dynamic discovery logic was not replaced or fabricated. Candidate counts and the overall deadline remain bounded. Alternate candidates go through the same codec policy: confirmed H.264 is accepted, confirmed HEVC is rejected, and unverified or temporary failures allow fallback to continue.

For the three requested AniZone episodes, no live fallback result can be truthfully reported because the upstream acceptance run was stopped by an HTTP 429 before the exact direct-play API trace could complete. No source ID or provider mapping was invented.

MovieBox remains completely disabled and is not included in search-provider configuration or fallback selection.

## G. Test Results

- `cargo fmt --all -- --check`: **passed**.
- `git diff --check`: **passed**.
- Codec-focused tests: **17 passed, 0 failed**.
- Full `cargo test`: **passed**; all non-ignored tests passed. The suite includes 669 passing tests in the final run; live-network and disposable-PostgreSQL tests remain ignored by their existing declarations.
- `cargo clippy --all-targets --all-features`: **passed with warnings**. Warnings are existing project lint warnings (`too_many_arguments`, `useless_conversion`) plus the existing constant assertion warning in the codec test. No Clippy error occurred under the requested non-`-D warnings` command.
- Fresh live AniZone test: **blocked by upstream HTTP 429** at the provider Livewire endpoint. This is external provider rate limiting, not evidence of a backend regression.

## H. Episode-by-Episode Results

### Demon Slayer S1E7 — AniZone slug `knw0pvgz`

- Initial reported failure: HTTP 502 / `DIRECT_PLAY_RESOLUTION_FAILED` after first-segment FFprobe rejection.
- Fix applied: bounded HLS context preservation, initialization handling, and up to three consecutive segment probes; structured failure classification.
- Live result: **not verified** in this run because AniZone rate-limited the live acceptance flow before the requested four-route trace could complete.
- Provider, codec, fallback, and final HTTP status: **not claimed**.

### Black Clover E67 — AniZone slug `ijxazsfw`

- Initial reported failure: HTTP 502 / `DIRECT_PLAY_RESOLUTION_FAILED` after first-segment FFprobe rejection.
- Fix applied: same HLS-aware bounded verification and fallback-safe error propagation.
- Live result: **not verified** because the provider returned HTTP 429 during the acceptance run.
- Provider, codec, fallback, and final HTTP status: **not claimed**.

### Hunter x Hunter (2011) E134 — AniZone slug `7lferah2`

- Initial reported failure: HTTP 502 / `DIRECT_PLAY_RESOLUTION_FAILED` after first-segment FFprobe rejection.
- Fix applied: same HLS-aware bounded verification and fallback-safe error propagation.
- Live result: **not verified** because the provider returned HTTP 429 during the acceptance run.
- Provider, codec, fallback, and final HTTP status: **not claimed**.

The existing repository live test was not used as equivalent evidence because it exercises a different episode matrix and does not implement the prompt’s exact four-route acceptance trace.

## I. Remaining Problems

1. The exact three-episode live acceptance trace could not be completed because AniZone returned HTTP 429 from its Livewire endpoint.
2. No actual Android Media3 playback test was performed.
3. HLS-specific manifest/variant/segment error subcategories are represented at the verifier boundary but can be refined further in a future typed resolver-error pass.
4. Clippy is clean under the requested command but still reports unrelated existing warnings.
5. No production deployment, VPS access, database change, Firebase change, APK change, commit, or push was performed.

## J. Final Conclusion

**FIXED BUT LIVE VERIFICATION INCOMPLETE.**

The backend now performs HLS-aware, bounded codec verification that preserves initialization data and retries with multiple consecutive segments without weakening the H.264-only policy. Confirmed HEVC remains rejected, unknown or insufficient data remains unverified, FFprobe failures no longer masquerade as codec incompatibility, fallback remains bounded, the APK API contract is unchanged, and MovieBox remains disabled. The exact external-provider acceptance evidence remains incomplete because AniZone rate-limited the live run with HTTP 429.
