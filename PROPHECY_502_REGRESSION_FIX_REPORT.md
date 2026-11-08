# Prophecy Verse Backend — HTTP 502 Direct-Play Regression Fix Report

## A. Root Cause

The current playback path was stricter than the historical working behavior in two ways:

1. `playback_route` invoked runtime codec verification for every resolved playback candidate, including releases that already carried an accepted H.264/AVC declaration.
2. `resolve_valid_direct_source`, used by `POST /api/v1/play/direct`, also invoked codec verification for every non-AniZone release. This removed the historical trusted-metadata fast path and caused unnecessary FFprobe/media-download work.

As a result, sources that were already eligible by reliable H.264 metadata could be rejected because of transient mirror HTTP failures, HTML responses, range/probe failures, or verification timeouts. When candidates were exhausted, the API returned the direct-play resolution failure path, commonly as HTTP 502.

A second classification defect treated every verification error and timeout in `playback_route` as codec incompatibility. A network failure, HTTP failure, incomplete media sample, FFprobe failure, or timeout is not proof that the codec is incompatible.

## B. Regression Evidence

Repository verification:

- Repository: `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-.git`
- Branch: `feature/complete-anime-multiprovider-search-playback`
- Current/original HEAD: `3f07fe9af6eed9e7e80642ab25af89ef4528a409`
- Historical baseline: `6dc4e68eef74d48a62e1f0e72e0c69b6c7c40ea1`
- Suspected regression: `a0f7c9c7bfce07438c30c6500c6ac38bc45ffe20`
- Earlier HLS-related commit: `8f4c08c1`

The `a0f7c9c7` diff changed the older behavior from probing only unverified FourKHDHub/Dramachi candidates to probing more broadly. In particular, the old `resolve_valid_direct_source` condition was:

```rust
release.provider in {FourKHdHub, Dramachi}
&& source_codec_eligibility(release.codec) == Unverified
```

The current code at the starting HEAD had an unconditional non-AniZone verification branch. The current `playback_route` also performed a verification call for both the native-stream branch and the non-native branch, resulting in a redundant verification decision.

The source comparison proves the behavioral regression. Live historical playback could not be independently reproduced because no stable 13-episode provider matrix or running service at `127.0.0.1:18080` was available in this isolated checkout. Therefore, no unavailable live result is claimed as passed.

## C. Files Modified

### Source

- `src/api.rs`
  - Added `release_requires_playback_codec_verification`.
  - Restored the trusted H.264 metadata fast path for non-HLS direct playback.
  - Kept runtime verification for AniZone HLS sources.
  - Kept runtime verification for unknown/unverified FourKHDHub and Dramachi candidates.
  - Classified verification errors and timeouts as provider/verification failures so fallback can continue instead of marking them as codec incompatibility.
  - Added a focused unit test for the verification policy.

### Report

- `PROPHECY_502_REGRESSION_FIX_REPORT.md`
  - This report is being included in the authorized GitHub update.

No other backend source files, routes, schemas, providers, APK files, Firebase configuration, production configuration, or database files were modified.

## D. Before vs After

### Before

- A source with trusted H.264 metadata could still be downloaded and FFprobed again during direct-play resolution.
- AniZone HLS sources were verified, as required.
- Unknown FourKHDHub/Dramachi sources were verified, as required.
- Verification errors and timeouts in `playback_route` were grouped with confirmed codec incompatibility.
- A valid source could lose most or all of the overall deadline to repeated verification attempts.

### After

- Explicitly accepted H.264/AVC metadata skips redundant runtime codec verification for non-HLS direct playback after normal provider and public-URL validation.
- AniZone always remains subject to HLS manifest/variant/segment/media probing.
- Unknown or unverified FourKHDHub and Dramachi sources remain subject to runtime FFprobe verification.
- Confirmed HEVC/H.265, unsupported codecs, and missing video streams remain incompatible and are rejected.
- Network/media/probe errors and timeouts are logged as verification/provider failures and fallback continues.
- The existing overall deadline and provider priority remain unchanged.

## E. Codec Safety

The existing codec policy remains fail-closed:

- H.264/AVC evidence accepted by the existing allowlist is treated as verified metadata and avoids unnecessary repeated probing.
- A runtime FFprobe result of H.264/AVC is accepted.
- A runtime FFprobe result of HEVC/H.265 is rejected.
- Mixed video streams containing HEVC remain blocked by the existing classifier.
- Unknown, missing, unsupported, or unverified codec evidence is not treated as H.264; eligible FourKHDHub/Dramachi candidates are probed and other candidates are tried.
- AniZone HLS is not accepted solely because the master playlist returns HTTP 200; the existing variant/segment/media verification remains in place.
- Invalid URLs, unsafe redirects, HTML/non-media responses, HTTP failures, and probe failures do not produce a codec acceptance.

## F. Provider Fallback

The existing fallback sequence and provider priority were preserved. The targeted behavior is now:

1. Try the requested provider/source.
2. Accept a source with valid provider resolution, public URL validation, and trusted H.264 metadata without an unnecessary second codec probe.
3. Probe unknown eligible FourKHDHub/Dramachi sources.
4. Verify AniZone HLS sources through the existing HLS verification path.
5. Reject confirmed incompatible sources.
6. Continue to the next eligible provider/source after network, HTTP, media-probe, or timeout failures.
7. Never enable or call MovieBox.

Focused fallback tests passed, including the existing test that skips failed/timeout alternatives and returns a later compatible source.

## G. Testing Results

### Formatting

- `cargo fmt --check` — **passed**
- `git diff --check` — **passed**

### Focused regression tests

- `cargo test api::tests::playback_verification_only_skips_trusted_non_hls_codec_metadata` — **passed**
- `cargo test api::tests::direct_play_route_http_falls_back_after_original_fetch_failure_and_timeout` — **passed**
- `cargo test codec_verification::tests` — **15 passed, 0 failed**
- Existing direct-play same-provider fallback test — **passed**
- Existing playback fallback timeout-isolation test — **passed**

### Lint

- `cargo clippy --all-targets --all-features` — **passed with pre-existing warnings**

The warnings are unrelated existing warnings, including `too_many_arguments`, `type_complexity`, `useless_conversion`, and an existing constant assertion warning. No clippy error was introduced by this fix.

### Full test suite

- `cargo test` — **failed only at the existing unrelated test**:
  - `tui::screens::home::tests::test_render_provider_popup`
  - The test expects the disabled MovieBox provider to render (`assertion failed: rendered.contains("  ✓ MovieBox")`).

This failure is consistent with the repository’s current intentional MovieBox-disabled behavior and is outside the direct-play source-verification scope. The playback and codec tests completed successfully.

### Live/API/13-episode acceptance

Not completed in this isolated checkout:

- No running backend was available at `http://127.0.0.1:18080`.
- No stable provider-source matrix was available for a reproducible 13-episode run.
- No live success, response time, selected provider, or HTTP result is invented.

## H. Performance

No live before/after response-time comparison is claimed. The code-level performance improvement is deterministic: trusted non-HLS H.264 metadata no longer causes redundant range download and FFprobe operations during direct-play resolution. This reduces avoidable delay and preserves the overall bounded deadline for candidates that genuinely require verification.

## I. Protected Functionality

Confirmed unchanged by scope and diff review:

- Anime Search
- Anime Details
- Episode Mapping
- Dynamic Cross-Provider Episode Mapping
- Multi-Provider Search
- Existing provider integrations
- Provider priority
- Existing API routes and response structures
- APK playback architecture and direct-play URL flow
- Database schema
- Firebase
- MovieBox disabled state

No remux or transcoding was introduced into the APK direct-play flow.

## J. Git Status

- Branch: `feature/complete-anime-multiprovider-search-playback`
- Original HEAD: `3f07fe9af6eed9e7e80642ab25af89ef4528a409`
- Modified backend source: `src/api.rs`
- Local report artifact: `PROPHECY_502_REGRESSION_FIX_REPORT.md`
- GitHub remote: unchanged
- Source commit created: **Yes** — `3af2dcc10f014ebae468eb13a183f0207e360bf7`
- Report commit: included in the final push immediately after the source commit
- Push performed: **Yes**, to the same required branch
- Production deployment: **No**
- Production VPS/database/Firebase/APK changes: **No**

The source change was committed and the report is included in the final authorized branch update. No unrelated files were included.

## K. Remaining Problems

1. The live 13-episode acceptance matrix could not be rerun without an available backend instance and stable provider URLs.
2. The existing unrelated MovieBox UI test still fails because it expects a provider that is intentionally disabled.
3. Actual provider mirror availability, AniZone live HLS stages, and production response-time improvement remain unverified in this isolated environment.
4. No historical URL was assumed to remain valid after its expiration.

## Conclusion

The smallest source-level regression fix was applied in `src/api.rs`. It restores the historical trusted H.264 compatibility optimization without weakening codec safety, preserves HLS verification, keeps unknown codecs fail-closed, continues fallback after network/probe/timeouts, and leaves MovieBox disabled. The authorized source commit and report were pushed to the required branch. No production deployment or production modification occurred.
