# Prophecy Verse — Direct Play Fix Report

## Baseline

- Repository: `newprophecy2-cloud/MovieBox-Tui-Prophecy-`
- Branch: `main`
- Starting commit: `6dc4e68eef74d48a62e1f0e72e0c69b6c7c40ea1`
- Production `GET /openapi.json`: HTTP 200, but production does not advertise `POST /api/v1/play/direct`.
- Original direct-play behavior: `src/api.rs::direct_play_route` accepted only URL paths ending in `.m3u8`, `.m3u`, or `.mpd`; direct-file and extensionless signed sources returned HTTP 415 `DIRECT_PLAY_COMPATIBILITY_UNKNOWN`.

## Root causes

1. `src/api.rs::direct_url_type` used URL extension as the only compatibility signal and ignored existing provider metadata.
2. Codec labels such as `H.264` were not normalized before comparison.
3. `src/providers/fourkhdhub/client.rs::resolve_release` returned the first HubCloud DirectPlayback candidate before running its existing bounded preflight. Local integration exposed this: the returned URL redirected to an HTML page even though the route returned HTTP 200.

## Changes

- Added conservative direct-play classification using provider metadata, declared content type, supported codec labels, and direct-file evidence.
- Added FourKHDHub resolver evidence: supported-codec FourKHDHub sources are accepted only after the resolver’s existing bounded HTTP preflight path has selected the candidate. Other ambiguous providers remain fail-closed.
- Removed the early HubCloud DirectPlayback return so candidate URLs pass the normal preflight validation before direct-play resolution succeeds.
- Preserved the original validated provider URL and only safe `Referer`/`User-Agent` headers.
- Preserved signed query parameters; no proxy, relay, remux, transcoding, FFmpeg, or FFprobe was added to the direct-play handler.
- Added explicit OpenAPI success schema for `hls`, `dash`, and `direct` responses.
- Added regression coverage for safe headers, HLS, DASH, direct video metadata, extensionless signed URLs, and ambiguous sources.

## Test results

| Test | Result | Evidence |
|---|---|---|
| Formatting | PASS | `cargo fmt --all -- --check` |
| Compile | PASS | `cargo check --locked` |
| Direct-play tests | PASS | 3 passed, 0 failed |
| FourKHDHub tests | PASS | 20 passed, 0 failed |
| Full suite after final fix | PASS | `cargo test --locked`: 686 passed, 0 failed, 16 ignored |
| Local Lookism search | PASS | Local backend `GET /api/v1/anime/search?q=Lookism` → HTTP 200 |
| Local Episode 1 lookup | PASS | Local `GET /api/v1/anime/{anime_id}/episodes` → HTTP 200; Episode 1 found |
| Local source resolution | PASS | Local `/full` → HTTP 200; `sources_status=ok`; one source; H.264, 2160p, Hindi/English/Korean |
| Local direct play | PASS | Local `POST /api/v1/play/direct` with the freshly returned opaque source ID → HTTP 200, `success=true`, `url_type=direct`, two safe headers |
| Direct provider validation | PASS | Bounded 1 KiB request with returned safe headers → `206 Partial Content`, `video/matroska`, no HTML body |
| No server-side relay | PASS | Direct handler returns the validated provider URL; no media relay path is invoked |

The 16 ignored tests are live-network tests and were not used as evidence. The local Lookism integration used the live provider’s GET resolution through the locally running backend, but the POST was sent only to `127.0.0.1:18080`. No production POST, deployment, restart, or configuration change was performed.

## Remaining blockers

- The production deployment still needs to include these changes before production direct play can be tested.
- Production OpenAPI currently omits `/api/v1/play/direct`.
- No production POST or deployment was performed.

## Final Git state before push

- Starting HEAD: `6dc4e68eef74d48a62e1f0e72e0c69b6c7c40ea1`
- Intended changed files: `src/api.rs`, `src/providers/fourkhdhub/client.rs`, `DIRECT_PLAY_FIX_REPORT.md`
