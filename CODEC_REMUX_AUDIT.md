# Codec and Remux Audit Report

## Scope and safety

This audit was performed locally in an isolated checkout. Production APIs, production environment variables, production database rows, and deployment/restart operations were not touched. No source was manually marked verified.

## Starting state

- Repository: `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-`
- Starting branch: `main`
- Starting HEAD: `28d861cbce3d49b3695df6e372b57983d6bd585d`
- Starting `git status --short`: clean
- Working branch: `manus/codec-policy-remux-audit`
- Working tree after changes: `src/api.rs`, `src/catalog.rs`, and `src/codec_verification.rs` modified, plus this report

## Findings and changes

### Authoritative codec policy

The shared codec helpers in `src/codec_verification.rs` now implement an H.264-only allowlist. Recognized AVC forms include `h264`, `H.264`, `avc`, `avc1`, `H.264 / AVC`, common explicit H.264 profile labels, and six-hex-digit AVC codec tags such as `avc1.640028`.

AV1, VP8, VP9, MPEG-4, VC-1, Theora, ProRes, Xvid, DivX, and other non-H.264 codecs are now unverified rather than advertised as compatible. HEVC detection uses recognized codec identifiers at the start of the normalized label (`hevc`, `h265`, `x265`, `hvc1`, and `hev1`) rather than arbitrary substring matching. Values such as `not-h264` and `not-hevc` do not pass the policy. Mixed video streams are blocked if any video stream is HEVC.

The catalog eligibility tests now use the same policy, so a stored `verified` status with a non-H.264 codec does not grant playback.

### Playback-path enforcement

The actual API dispatch and call chains were traced for:

| Path | Handler/call chain | Policy result |
|---|---|---|
| Catalog source details | `/api/v1/catalog/sources/{id}` → `catalog_source_route` | Metadata remains available; no playable URL is newly granted. |
| Catalog Remux | `/api/v1/catalog/sources/{id}/remux` → `catalog_source_remux_route` → `catalog_live_source_path` → `source_route` → `transcode_source` | Requires current catalog `playback_eligible`; HEVC returns `SOURCE_CODEC_BLOCKED`; unverified state returns `CODEC_UNVERIFIED`. |
| Catalog Play | `/api/v1/catalog/sources/{id}/play` → `catalog_source_play_route` → `catalog_live_source_path`/`source_route` | Same strict catalog eligibility and stable error codes. |
| Opaque source playback | `/api/v1/sources/{id}/playback`, `/play`, `/watch` → `source_route` → `write_verified_playback_response` | Requires a fresh FFprobe-based H.264 result before returning a playable URL. |
| Opaque source default response | `/api/v1/sources/{id}` | No longer returns a direct media URL without a positive H.264 verdict; it fails closed with `CODEC_UNVERIFIED`. |
| Generic proxy/direct media | `/api/stream-proxy` → `stream_proxy_route` | Performs bounded FFprobe classification before direct passthrough or Remux. HEVC is rejected before media bytes; unknown, missing, unsupported, or failed verification returns `CODEC_UNVERIFIED`. |
| Generic Remux | `/api/stream-proxy?remux=true` → `remux_media_url` | Uses the same generic URL codec gate before FFmpeg starts. |
| HLS/DASH sidecar | `src/proxy.rs` manifest/segment/cache handlers | The sidecar is fed by resolved playback URLs; API playback URL issuance is now codec-gated. The sidecar itself does not independently probe every segment. |
| Media-info and subtitle extraction | `/api/media-info`, `/api/subtitles/extract` | These are inspection/metadata operations rather than playable media URL issuance; URL validation and bounded process/network behavior remain in place. |

## Remux investigation and evidence

The current checkout contains an advisory short Remux preflight followed by an authoritative streaming FFmpeg path. The preflight is no longer treated as proof of playback success or as a reason by itself to reject a catalog source whose current authoritative verdict is verified H.264. The actual streaming path still requires fragmented-MP4 startup (`moof`), drains FFmpeg stderr, checks process exit, kills on disconnect/error, and does not append JSON after binary headers are committed.

A definitive production-specific root cause for a live verified-H.264 failure cannot be established from this checkout alone because no production logs or source requests were accessed. The reproducible defects found locally were:

1. The shared allowlist incorrectly treated AV1/VP9 and several other codecs as verified-compatible.
2. The generic URL proxy could return arbitrary validated media bytes without a codec verdict.
3. An opaque-source response path exposed a direct media URL without the fresh verification required by the strict policy.

Controlled local evidence:

- `tests/fixtures/codec-h264.mkv`: FFprobe reports one `h264` video stream.
- `tests/fixtures/codec-hevc.mkv`: FFprobe reports one `hevc` video stream.
- The production Remux flags applied to the H.264 fixture produced a complete 1,730-byte fragmented MP4. FFprobe reported MP4 format and an H.264 video stream. This validates complete output structurally, not merely the first bytes.
- The available H.264 fixture is video-only, so the E-AC3 audio-conversion case remains an integration test still requiring a suitable controlled audio fixture.

## Tests and commands

Baseline from starting HEAD:

- `cargo fmt --check`: unavailable initially because `cargo-fmt` was not installed.
- `cargo test --locked`: 489 passed, 1 failed. The failure was the pre-existing unrelated `tui::app::playback::tests::test_explicit_player_incompatible_does_not_launch_alternative` assertion.
- `cargo clippy --locked --all-targets -- -D warnings`: unavailable initially because `cargo-clippy` was not installed.
- `cargo build --release --locked`: passed.

Edited branch:

- `cargo fmt --check`: passed.
- Focused codec policy tests: passed.
- Mixed-video-stream HEVC regression test: passed.
- `cargo test --locked`: 489 passed, 1 failed with the same unrelated TUI assertion; no new test failure was observed.
- `cargo clippy --locked --all-targets -- -D warnings`: passed.
- `cargo build --release --locked`: passed.
- Complete local FFprobe/FFmpeg fixture validation: passed for H.264 classification, HEVC classification, and complete fragmented-MP4 output.

## Security and resource controls

Existing URL syntax checks, public-destination/DNS checks, redirect restrictions in the guarded verifier, bounded FFprobe/FFmpeg timeouts, output limits, `kill_on_drop`, Remux concurrency limits, provider header filtering, and sanitized logging were preserved. No automatic transcoding fallback was introduced. Generic proxy verification runs before direct response headers are committed.

One limitation remains: FFprobe invoked directly on a generic URL relies on the existing validated destination plus FFmpeg/FFprobe network behavior; it is not a replacement for a fully buffered, redirect-aware local-media probe. The catalog verifier continues to use the guarded bounded sample path for supported FourKHDHub sources.

## API compatibility notes

- Stable codec error codes are preserved/used: `SOURCE_CODEC_BLOCKED` for confirmed HEVC and `CODEC_UNVERIFIED` for all non-positive H.264 states.
- The opaque default source route no longer exposes an unverified direct URL. This is an intentional strict-policy compatibility change and should be documented for clients that previously consumed that URL field.
- Remux remains GET-only and does not claim byte-range seeking.

## Commit and next steps

- No local commit was created.
- Nothing was pushed or deployed.
- Production database and services remain untouched.

Run next from the working branch:

```sh
cd /home/ubuntu/work/MovieBox-Tui-Prophecy-
. "$HOME/.cargo/env"
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo build --release --locked
```

Before deployment, add a controlled local integration fixture containing H.264 plus E-AC3 and exercise the full HTTP response lifecycle, including client disconnect, timeout, FFmpeg early EOF, and proxy redirect cases. Do not deploy until the pre-existing TUI test failure is separately resolved or explicitly accepted.
