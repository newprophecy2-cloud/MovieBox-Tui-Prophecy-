# Kari 6 Anime Provider Integration Report

## 1. Repository information

- Target: `newprophecy2-cloud/MovieBox-Tui-Prophecy-`
- Working directory: `/tmp/prophecy_repo`
- Base commit: `847cfe8` (`Fix direct play provider validation`)
- Working branch: `kari`
- Kari source: `https://github.com/Dhairya3391/kari`
- Inspected Kari revision: `9d7e95a` (`fix(tui): manage provider priorities across modes`)

No VPS, SSH session, deployment, production service, APK repository, or production configuration was accessed.

## 2. Existing backend architecture

The backend exposes its existing API from `src/api.rs`, with provider dispatch and orchestration through `src/service.rs`. Provider metadata and release/source identity are represented by `ProviderKind`, `CatalogItem`, `MediaDetails`, `Release`, `SourceMirror`, and the `Provider`/`ReleaseProvider` traits in `src/providers/models.rs` and `src/providers/mod.rs`.

Search and details are dispatched by provider identity in `Service`; episode source resolution is cached by the existing API release cache and routed by `releases_uncached`. Existing public API routes and response structures were not renamed or removed. Existing HTTP behavior continues to use the project network client and existing URL validation. Existing codec/playback policy remains unchanged; Kari releases intentionally carry `codec: None` until actual media verification establishes a codec.

The six new providers have independent enum identities, cache keys, API routing, service clients, labels, and opt-in configuration flags. Search orchestration can include each enabled provider independently; a provider error is returned as that provider's error and does not require changing the public API contract.

## 3. Six-provider Kari source analysis

### AniVault

Kari source inspected: `internal/provider/anivault/client.go`, `resolve.go`, and response/helper files. The relevant flow uses AniVault's Railway API and its `/api/watch/...` playback payload. The Rust adapter uses the AniVault API base, sends a browser user agent and JSON accept header, validates returned media URLs, preserves HLS/MP4/direct mirror type, and retains the provider/season/episode identity. Search/details metadata currently use the AniList-backed adapter because AniVault's catalog is AniList-keyed.

Status: **IMPLEMENTED (adapter), BUILD VERIFIED, SOURCE NOT VERIFIED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

### Ani.pm

Kari source inspected: `internal/provider/anipm/client.go` and the Settlar/session helpers. The Rust adapter follows the bootstrap → Settlar selection → session → embed-token → playback-source sequence, sends the required Ani.pm and Settlar referers, validates the returned source URL, and returns it as an HLS mirror without claiming a verified codec.

Status: **IMPLEMENTED (adapter), BUILD VERIFIED, SOURCE NOT VERIFIED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

### AniCine

Kari source inspected: `internal/provider/anicine/client.go`, `anime.go`, and shared MegaPlay helpers. Kari requires MegaPlay embed discovery, encrypted `getSources` response decryption, CDN signing, subtitle extraction, and actual media verification. The provider identity, metadata adapter, configuration, service dispatch, and release route are registered, but the complete native MegaPlay decrypt/sign/verification path is not claimed as operational in this change.

Status: **REGISTERED/ADAPTER PRESENT, BUILD VERIFIED, SOURCE BLOCKED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

### Anikoto

Kari source inspected: `internal/provider/anikoto/client.go`, `direct.go`, and the HTML/AJAX search and server-list helpers. The upstream flow requires AniList-to-Anikoto slug resolution, HTML/AJAX fallback matching, server selection, MegaPlay handling, and provider-specific subtitle behavior. The provider identity and backend routing are present, but live source resolution is deliberately reported unavailable rather than fabricating a URL.

Status: **REGISTERED/ADAPTER PRESENT, BUILD VERIFIED, SOURCE BLOCKED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

### Miruro

Kari source inspected: `internal/provider/miruro/client.go`, `resolve.go`, and its SvelteKit `__data.json`/devalue parsing helpers. The upstream flow requires exact show-ID and slug mapping, SvelteKit payload decoding, track/provider ranking, and stream/subtitle extraction. The provider identity and backend routing are present; the adapter returns an explicit unavailable result until this provider-specific decoder and live media path are fully ported.

Status: **REGISTERED/ADAPTER PRESENT, BUILD VERIFIED, SOURCE BLOCKED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

### AniDream

Kari source inspected: `internal/provider/anidream/client.go` and `seroval.go`. The upstream TanStack Start application requires hashed `_serverFn` discovery, Seroval request encoding/response decoding, and provider-specific playback extraction. The provider identity and backend routing are present; no placeholder media result is returned while that server-function flow remains unverified.

Status: **REGISTERED/ADAPTER PRESENT, BUILD VERIFIED, SOURCE BLOCKED, MEDIA NOT VERIFIED, H.264 NOT VERIFIED, PLAYBACK NOT VERIFIED**.

## 4. Compatibility analysis

| Area | Result |
|---|---|
| Provider identity | Supported; six distinct `ProviderKind` values and cache keys |
| Search/details contract | Supported through the existing `Provider` trait and AniList-backed metadata adapter |
| Episode identity | Supported by existing opaque provider/subject/season/episode references |
| Service dispatch | Supported; each provider has its own client slot and dispatch arms |
| Release dispatch | Supported; all six provider identities route through `releases_uncached` |
| Configuration | Supported; six independent safe-default flags plus environment overrides |
| HTTP headers | Implemented for AniVault and Ani.pm flows; remaining providers require their full upstream-specific flow |
| HLS/MP4 | URL and mirror metadata are preserved where returned; actual media verification remains required |
| Codec policy | Preserved; no unknown source is falsely labeled H.264 |
| Cache separation | Uses existing provider-aware opaque references and release cache path |
| Failure isolation | Provider-specific `ProviderError` results; no fake fallback source |
| External limitations | AniCine, Anikoto, Miruro, and AniDream upstream-specific resolver paths remain blocked/not verified |

## 5. File changes

### New files

- `src/providers/kari.rs` — shared native Rust Kari adapter, AniList metadata mapping, AniVault and Ani.pm resolver flows, URL validation, and explicit blocked-provider errors.
- `KARI_INTEGRATION_REPORT.md` — this report.

### Modified files

- `.env.example` — documents six opt-in provider flags.
- `src/providers/models.rs` — adds six provider identities, labels, parsing, and source labels.
- `src/providers/mod.rs` — registers six provider-facing native modules and constructors.
- `src/service.rs` — adds six clients and search/details/capability dispatch.
- `src/api.rs` — registers enabled Kari providers in search and routes release resolution.
- `src/config.rs` — adds safe-default provider flags and environment overrides; MovieBox default remains disabled.
- `src/tui/screens/details.rs`, `src/tui/widgets/badge.rs`, `src/tui/state.rs` — preserves exhaustive provider presentation/state handling.

### Unchanged critical files

The public route paths, request/response route shapes, APK source, Kari source checkout, VPS, deployment files, and production services were not changed.

## 6. Implementation summary

The implementation uses the existing native Rust provider traits rather than a subprocess or a second runtime. The six provider identities are preserved from search result through details and episode source requests. AniList metadata is used only as a shared catalog mapping for the adapter; unknown media sources are not marked verified. AniVault and Ani.pm use their observed upstream request sequences and required referer behavior. The other four providers return explicit unavailable errors until their provider-specific decoders and live playback flows are ported and verified.

## 7. Verification results

Commands run:

- `cargo fmt --all` — completed.
- `cargo fmt --all -- --check` — passed after final normalization.
- `cargo check` — **BUILD VERIFIED** after fixing the reported `.await` and exhaustive-label errors; Cargo finished successfully.
- `cargo test` — not run, per the latest user instruction.

No live provider search, episode, source, segment, FFprobe, codec, or playback test is claimed here. Those statuses are explicitly **NOT VERIFIED** or **BLOCKED** above.

## 8. Playback verification

No provider is claimed as media-verified. Returned URLs alone are not treated as proof of playable media. Codec remains unknown (`None`) for unverified releases, so existing H.264-only playback policy is not weakened.

## 9. Existing functionality protection

Existing public API routes remain in place. Existing providers remain registered. MovieBox is disabled in the default configuration and the new Kari flags default to disabled. No APK code or Firebase configuration was touched. No deployment or VPS access occurred.

## 10. Remaining limitations

The full Kari-specific native resolver/decryption/parser work for AniCine, Anikoto, Miruro, and AniDream remains unresolved. AniVault/Ani.pm live source and media verification was not performed. The user requested that the test suite not be run, so unit/integration test results are not available. The final formatting check should be rerun after the last one-line import formatting change.

## 11. Git safety

- Current branch: `kari`
- Base commit: `847cfe8`
- Commit: `b00146a` (`Integrate Kari anime provider adapters`)
- Push: completed to `origin/kari`.
- No VPS access, SSH, deployment, production modification, APK modification, or remote execution occurred.
