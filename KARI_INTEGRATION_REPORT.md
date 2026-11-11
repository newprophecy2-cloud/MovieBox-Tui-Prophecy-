# Kari Anime Provider Integration Report

## Scope and result

This change completes native Rust adapters for AniCine, Anikoto, Miruro, and AniDream in the existing Prophecy Verse backend. Provider dispatch is integrated through `KariClient` for search, details, and episode-source resolution. Provider-specific API IDs remain opaque and are preserved through the existing identity model.

The implementation is source-informed by Kari at commit `9d7e95aa3ef374a25cdece15adcf7559664f4696` (`https://github.com/Dhairya3391/kari`). The behavior below is validated against code fixtures and unit tests. Live upstream availability and end-to-end playback have not been tested, so no such verification is claimed.

## Provider implementation

- **AniCine:** uses the MegaPlay-backed sub/dub route, maps AniList IDs to MAL IDs when required, extracts embed file IDs, decrypts MegaPlay's AES-256-CBC payload, signs CDN paths with HMAC-SHA256, and carries valid subtitle tracks forward.
- **Anikoto:** searches and resolves AniList title variants against the site's HTML/AJAX endpoints, selects episode servers, and uses the shared MegaPlay decryption/signing and subtitle path.
- **Miruro:** parses SvelteKit/devalue search and watch data, preserves provider identity, selects supported HLS/MP4 sources, and retains subtitle metadata and server referers.
- **AniDream:** implements TanStack Start Seroval request/response handling and playback lookup, uses AniList-backed metadata rather than synthetic metadata, and retains available subtitle data.

The provider resolvers expose only audio/subtitle modes that the current `ReleaseProvider` contract can actually select. Unknown codec information remains unknown; the adapters do not claim H.264 verification from metadata alone. Playback is limited to supported HLS/MP4 mirrors and retains source headers where required.

## Shared model and compatibility

`Release` now carries subtitle tracks so provider subtitles can flow through the backend playback-source conversion. Existing provider and test initializers were updated accordingly. Playback-source response fields and API route paths/request-response structures remain unchanged; the existing subtitle field is populated from available release tracks.

MovieBox remains disabled by default. This task does not alter its provider dispatch or configuration defaults. The four Kari providers continue to use distinct provider identities and existing provider-aware caching/opaque IDs.

## Changed files

The implementation changes `Cargo.toml` and `Cargo.lock` for AES and legacy HMAC/SHA-2 compatibility, and updates `src/providers/kari.rs`, `src/providers/models.rs`, `src/providers/mod.rs`, and `src/api.rs`. Provider-specific code and fixtures live under `src/providers/kari_anime/`. Existing release construction sites in provider adapters and TUI/API tests were updated for the new subtitle field. This report is also updated in the task commit.

No unrelated worktree changes were included in the task commit. No deployment, production service, VPS, APK, or remote runtime was accessed.

## Verification

- `cargo fmt --all -- --check` — passed.
- `cargo check` — passed.
- `cargo test` — passed: all 574 library tests and all non-ignored integration tests passed. Database-dependent and live-network tests remain ignored by their existing test annotations.
- Kari provider unit coverage includes AniCine, Anikoto, Miruro, AniDream, and the shared MegaPlay protocol. The encrypted MegaPlay fixture decryption test passed.
- `git diff --check` — passed before commit.

Cargo emits non-fatal dead-code warnings for MegaPlay metadata fields (`Track.default`, cue timings, and intro/outro) that are parsed but not currently consumed by playback.

## Git status

The implementation was committed as `42dec35` (`Complete Kari anime provider integrations`) on the existing `kari` branch and pushed successfully to `origin/kari`. The implementation commit includes the required `Co-authored-by: Manus <dev-agent@manus.ai>` trailer. This report update records the completed push.
