# Prophecy Verse Remux Root-Cause Audit

## A. Repository and baseline

The audit was performed locally in an isolated checkout. No production VPS, database, systemd unit, domain, environment variable, or deployed process was accessed or modified. No production Remux request was issued during this audit.

- Repository: `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-`
- Starting branch: `manus/codec-policy-remux-audit`
- Starting commit: `153d154fff231712188e21ef37069da6c4261873`
- Starting working tree: clean
- Remote `origin/main`: same starting commit
- Final working branch remains isolated; production was not touched

Files inspected included `README.md`, `Cargo.toml`, `src/api.rs`, `src/net.rs`, `src/proxy.rs`, `src/remux_cache.rs`, `src/codec_verification.rs`, `src/catalog.rs`, provider resolver modules, all Rust integration tests, migrations, and the existing H.264/HEVC fixtures.

The relevant request path is:

```text
GET /api/v1/sources/{opaque_source_id}/remux
  → route → source_route → transcode_source
  → resolve_transcode_playback_source
  → FourKHDHub resolver when applicable
  → validate_transcode_destination
  → bounded Remux semaphore
  → advisory FFmpeg/FFprobe preflight
  → authoritative FFmpeg fragmented-MP4 stream
  → stream_ffmpeg_remux
```

Catalog Remux follows the catalog source route, checks stored eligibility, re-resolves the provider mapping, and then enters the same source route. The generic `/api/stream-proxy?remux=true` path uses a separate validated URL flow and the same streaming helper.

The authoritative production FFmpeg command in `transcode_source` is:

```text
ffmpeg -hide_banner -loglevel error -nostdin
  [-headers trusted-provider-headers]
  -rw_timeout 120000000 -i <validated-public-https-url>
  -map 0:v:0 -map 0:a?
  -c:v copy
  -c:a aac -b:a 192k -ac 2
  -sn -dn -max_interleave_delta 0
  -movflags +frag_keyframe+empty_moov+default_base_moof
  -flush_packets 1 -f mp4 pipe:1
```

The preflight uses the same video/audio mapping and writes a bounded two-second fragmented MP4 sample to a pipe, then FFprobe validates H.264 video and AAC audio. The authoritative stream waits for a `moof` atom before committing HTTP 200 headers, drains FFmpeg stderr, streams stdout with backpressure, waits/reaps the child, and kills it on startup failure, write failure, read failure, timeout, or disconnect.

The source-resolution and codec-verification paths use opaque source IDs, public-HTTPS/SSRF validation, provider restrictions, trusted resolver headers, bounded process/network timeouts, and the strict H.264-only policy introduced in the previous audit.

## B. Root-cause matrix

| Issue | Evidence | Root cause | Fix | Regression test | Status |
|---|---|---|---|---|---|
| 20-second capture was partial | The supplied test intentionally terminated `curl` at approximately 20 seconds after receiving 217,115,376 bytes. It reported HTTP 200, `video/mp4`, Remux mode, and a valid ISO Base Media signature. | The capture duration was shorter than the approximately 1,475-second source. A partial client-side file cannot establish a server premature-termination defect. The server code handles client write failure by killing and reaping FFmpeg rather than continuing work after disconnect. | No change to successful-stream semantics. Preserved cancellation-safe child cleanup and documented the distinction between client cancellation and completed streaming. | Local complete fragmented-MP4 fixture validation; source code review of all `stream_ffmpeg_remux` error paths. A live production disconnect test was not run. | VERIFIED NOT A BUG |
| E-AC-3 browser compatibility | The supplied source has three E-AC-3 tracks. The prior command used `-map 0:a:0? -c:a aac`, so it converted only the first audio track. Controlled fixture output contained one AAC audio track and retained only Hindi metadata. | Audio mapping selected only the first source audio stream. Conversion itself was correct for that selected track, but English and Korean were silently omitted. | Changed all Remux commands and preflight to `-map 0:a?`, retaining all source audio streams and converting each to AAC stereo at 192 kbps. | `api::tests::remux_converts_and_preserves_all_eac3_audio_languages` runs FFmpeg and FFprobe against H.264 plus Hindi/English/Korean E-AC-3 and asserts one H.264 video, three AAC tracks, preserved language tags, and `ftyp`/`moov`/`moof`/`mdat`. | FIXED |
| Output validator checked only the first audio stream | `browser_mp4_streams_compatible` previously used `.find()` for audio, so a later incompatible audio track could be missed. | Validation did not cover every mapped audio stream. | Validator now collects every audio stream and requires every one to be AAC. | `fragmented_mp4_remux_requires_browser_compatible_codecs` rejects H.264 + AAC + AC-3. | FIXED |
| Fragmented MP4 startup and metadata | The implementation uses `empty_moov`, `frag_keyframe`, `default_base_moof`, `flush_packets`, and waits for `moof` before sending headers. The controlled output was FFprobe-readable and contained all required atoms. | No confirmed current bug. The earlier partial capture was not enough to classify the server output as invalid. | Preserved the startup gate and bounded first-fragment buffer. | Multi-audio Remux test verifies the four atoms and FFprobe output. Existing startup failure/timeout branches return structured errors before headers. | VERIFIED NOT A BUG |
| Error status after headers | `stream_ffmpeg_remux` validates startup before writing HTTP headers. Once 200 and media bytes are committed, later failures return an I/O error to the server and cannot change the already-sent status. | No bug found; HTTP semantics are correctly constrained by header commitment. | Preserved pre-header structured errors and midstream logging. | Existing startup/error branches plus full test suite; no fake post-200 JSON is appended. | VERIFIED NOT A BUG |
| Client disconnect/process cleanup | A failed `write_all` records the error, kills FFmpeg, waits up to five seconds, and awaits stderr draining. Startup/header/write failures also kill and reap. | No confirmed leak in the reviewed paths. A real socket-disconnect integration test is not currently present. | No unnecessary behavior change; retained `kill_on_drop`, explicit kill, wait, and semaphore release. | Code-path audit and existing build/test validation. A dedicated child-process-count test remains outstanding. | UNCONFIRMED |
| Seeking and `Accept-Ranges: none` | Remux responses explicitly advertise `Accept-Ranges: none`; byte-range requests return `501 SEEK_UNAVAILABLE`; output is streamed through a pipe and not persisted. | This is an intentional limitation of nonpersistent fragmented streaming, not fake Range support. | No change. Documentation and OpenAPI retain the limitation. | Existing route/range tests and source inspection. No unsupported Range behavior was added. | VERIFIED NOT A BUG |
| Subtitle handling | Production Remux uses `-sn -dn`; it does not claim to embed subtitles. Separate subtitle/track routes probe and report embedded tracks. | MP4 subtitle preservation is intentionally excluded from the Remux contract. Silently carrying SubRip into browser MP4 would not be a safe universal behavior. | No change; documented as separate track discovery rather than embedded Remux output. | Existing probe metadata tests and subtitle route contract tests. A full embedded subtitle Remux test is not applicable while `-sn` remains explicit. | VERIFIED NOT A BUG |
| Audio selection semantics | The supplied source exposes Hindi, English, and Korean audio. No Remux query parameter or API contract currently selects one language; `/api/v1/watch` is a separate Hindi-only route. | Remux had implicit first-track selection, which was inconsistent with the absence of a selection contract. | Preserve all audio languages in Remux instead of silently choosing one. Explicit single-language selection remains a future API design change. | Multi-audio fixture test verifies all three languages. | FIXED |
| Codec policy bypasses | Earlier audit found broad non-H.264 allowlisting and arbitrary generic proxy playback without a positive codec verdict. Current code now uses strict H.264/AVC classification and gates generic proxy/source playback. | Policy was broader than the stated H.264-only requirement, and some URL playback paths trusted labels or URL validation alone. | Strict shared allowlist, bounded FFprobe checks, HEVC blocking, and fail-closed `CODEC_UNVERIFIED`. | Codec unit tests, mixed-stream HEVC test, focused and full suites. | FIXED |
| Duplicate preflight work | Each Remux performs a bounded advisory FFmpeg preflight and then starts a second authoritative FFmpeg process. | The design validates compatibility before committing headers but duplicates upstream read/FFmpeg work. | Not changed because removing it would alter the startup/error contract and requires a larger design decision. | Existing bounded preflight behavior is covered indirectly; no performance acceptance threshold was provided. | NOT FIXED |
| Concurrency limit | `ApiState` owns a two-permit Remux semaphore; `try_acquire_owned` returns 429 when full and the permit is dropped after `stream_ffmpeg_remux` returns, including spawn failure. | No confirmed concurrency bug in code review. | Preserved existing limit and release behavior. | Existing semaphore-related unit coverage and full suite; no live multi-client socket test. | VERIFIED NOT A BUG |
| Persistent Remux cache | `X-Remux-Cache-State: disabled` is emitted and `remux_cache.rs` explicitly refuses persistent storage. | Caching is intentionally disabled to avoid mixing credentials, source URLs, audio choices, and user requests. | No cache added. | Existing cache-disabled implementation and route headers. | VERIFIED NOT A BUG |
| Arbitrary generic proxy architecture | `/api/stream-proxy` accepts a validated public URL rather than an opaque source ID. It retains SSRF/public-destination checks and codec gating. | This is an existing compatibility route, but it is broader than the preferred source-ID-only security model. Removing it would be an API-breaking architecture change. | No removal in this audit; preserved validation and fail-closed codec behavior. | Existing URL-security tests plus generic codec gate review. | NOT FIXED |

## C. Code changes

### `src/api.rs`

- Changed preflight, catalog/source Remux, and generic Remux audio mapping from `0:a:0?` to `0:a?`.
- Updated `browser_mp4_streams_compatible` to validate all audio streams as AAC rather than checking only the first.
- Added an executable regression test that performs the production-style FFmpeg conversion and FFprobe verification on three E-AC-3 language tracks.
- No source-ID format, provider resolver, authentication, database schema, or response status contract was changed by this audit fix.

### `README.md`

- Corrected source eligibility documentation to match the strict H.264/AVC-only policy actually enforced by code.

### `tests/fixtures/codec-h264-multi-eac3.mkv`

- Added a small deterministic test fixture containing H.264 video and Hindi, English, and Korean E-AC-3 audio tracks.
- It is used only for local regression testing; it is not a production source and is not presented as evidence that the PixelDrain source itself works.

### API compatibility impact

Remux output now contains all source audio tracks instead of only the first. Each is converted to AAC stereo, with language metadata preserved. This is a compatibility improvement for clients that support audio-track selection. Clients that assumed only one audio track should continue to work with the default first track. No new audio query parameter was invented.

### Resource and security implications

Mapping all audio tracks increases FFmpeg CPU and output bandwidth relative to one-track conversion, but avoids silent language loss and remains bounded by the existing two Remux slots. The source is still streamed; the entire episode is not loaded into memory. Existing URL validation, trusted headers, SSRF controls, codec policy, process timeouts, and cleanup remain intact.

## D. Test results

Baseline before this audit fix:

```text
cargo test --locked: 491 passed, 0 failed
cargo clippy --locked --all-targets -- -D warnings: passed
cargo build --release --locked: passed
```

The baseline formatting check initially reported a formatting difference from the earlier inserted test; it was corrected with `cargo fmt` before final validation.

Final commands and results:

```text
cargo fmt --check
PASS

cargo test --locked api::tests::remux_converts_and_preserves_all_eac3_audio_languages
PASS; focused test passed; 491 filtered out

cargo test --locked
PASS; 492 passed, 0 failed, 15 ignored

cargo clippy --locked --all-targets -- -D warnings
PASS

cargo build --release --locked
PASS
```

The focused Remux test verified:

- one H.264 video stream;
- three AAC audio streams;
- Hindi (`hin`), English (`eng`), and Korean (`kor`) metadata preserved;
- fragmented MP4 `ftyp`, `moov`, `moof`, and `mdat` atoms present;
- FFprobe successfully parsed the output.

The test suite includes existing security, provider, API contract, source eligibility, codec verification, update lifecycle, and TUI tests. No live production test was run, and the ignored live-network tests were not used as proof.

Cancellation, cleanup, and concurrency were verified by code-path inspection and existing tests, but not by a new live socket/process-count integration test. No full 1.23 GB download or full-length transcode was performed.

## E. Remaining issues

1. The exact production client-disconnect behavior remains unconfirmed because no production request or VPS process telemetry was accessed. Required evidence: a controlled smoke test with a bounded byte/time cap plus server-side FFmpeg PID, exit, and memory/CPU observation.
2. The Remux path performs an advisory preflight and then a second authoritative FFmpeg process. Required evidence for redesign: time-to-first-byte and upstream/CPU measurements showing the duplicate work is materially harmful, followed by a tested single-process startup-validation design.
3. The generic `/api/stream-proxy` route remains URL-based rather than source-ID-only. Required evidence for a safe change: a complete inventory of clients using the route and a compatibility/deprecation plan.
4. Embedded subtitles remain explicitly excluded from Remux output. Required evidence for support: a product contract for browser-compatible subtitle embedding or a separate sidecar delivery design and tests for English, Hindi, forced, and SDH tracks.
5. No Remux query parameter selects Hindi, English, or Korean. The safe current behavior is to preserve all tracks. Required evidence for single-track selection: an approved API contract defining language/index selection and default-track semantics.
6. The production PixelDrain source itself was not re-requested. The supplied 20-second capture is consistent with client cancellation and does not prove full-episode completion. The source-specific result remains UNCONFIRMED until a controlled smoke test is separately authorized.

## F. Production readiness

**READY FOR CONTROLLED SMOKE TEST**

This is not a deployment claim. Local tests, codec verification, fragmented-MP4 validation, and the E-AC-3 multi-language regression all pass. A bounded production smoke test may be prepared, but it must use the existing opaque source-resolution flow, avoid logging or exposing the direct URL, cap duration and bytes, monitor process/resource usage, distinguish expected client cancellation from server failure, and include rollback instructions. No production smoke test was run automatically.

Suggested bounded smoke-test plan, requiring separate authorization:

1. Resolve the existing verified opaque source through `/api/v1/sources/{source_id}` without printing the resolved URL or headers.
2. Request `/api/v1/sources/{source_id}/remux` with a client-side byte/time cap well below a full episode.
3. Record only status, content type, Remux/cache headers, received byte count, elapsed time, and sanitized server request ID.
4. On intentional client cancellation, confirm the corresponding FFmpeg child exits and the semaphore permit is released.
5. Do not download the full source or deploy/restart anything as part of the smoke test.
6. Roll back by reverting the application commit and restarting only through the separately approved deployment procedure; no rollback was performed here.

## G. Git state

Files modified after the previously pushed commit:

- `README.md`
- `src/api.rs`
- `tests/fixtures/codec-h264-multi-eac3.mkv`
- `CODEC_REMUX_AUDIT.md`

The current changes are not yet committed or pushed. The previous audit commit remains at `153d154fff231712188e21ef37069da6c4261873` on remote `main`. Production was not touched.
