# Phase 3.3 — 4KHDHub and DramaChi Provider Report

## Scope

This pass focused on the repository's provider path, resolver path, API-compatible source references, and the existing non-MovieBox live verification suite. MovieBox was not enabled or used as a fallback. No Render deployment was performed.

Branch: `phase3.3-fourkhdhub-dramachi-hardening`

Baseline: `18f51f7e628851f3444878036cf2d58ae69330c7`

## Implementation updates

### 4KHDHub

- Resolver failures are no longer swallowed into the generic `no working mirror links extracted` message. Resolver and preflight errors are retained and surfaced with sanitized, actionable reasons such as expired HubDrive pages, timeout, HTTP 403/404, invalid HTML/media content, and expired mirror links.
- Existing HubCloud/HubDrive/GreenMotors/PixelDrain/direct-file resolution and public URL validation are preserved.
- The live movie and series tests now require at least one actual playable HTTPS source. They no longer pass merely because every failed mirror returned within the timeout budget.
- The exact provider matrix now checks duplicate same-title catalog entries for the requested season/episode before selecting one. This avoids selecting the first `One Piece` catalog entry when another provider entry contains the requested S01E01.

### DramaChi

No speculative provider change was made because its live search → details → episode stream path is working. Its exact matrix output was captured honestly; the unavailable Kino's Journey result was not fabricated or aliased to another title.

## Verified live evidence

| Test | Result | Evidence |
|---|---|---|
| 4KHDHub Inception movie | **Verified** | Search/details/releases worked. First stale/timeout mirrors were skipped; the second release resolved to an HTTPS `filescloud...workers.dev` Matroska source. The strict test passed. |
| 4KHDHub Game of Thrones S01E01 | **Verified** | Multiple stale/403/HTML/timeout mirrors were rejected; a later PixelDrain mirror resolved successfully. The strict test passed. |
| DramaChi Squid Game search/details/streams | **Verified** | Live test passed and returned a direct HTTP(S) stream. |
| DramaChi Naruto Shippuden S01E01 | **Verified** | Live probe returned HTTP 206, `application/octet-stream`. |
| DramaChi Naruto Shippuden S01E32 | **Verified** | Live probe returned HTTP 206, `application/octet-stream`. |
| DramaChi One Piece Episode 1 | **Verified** | Live probe returned HTTP 206, `video/mp4`. |
| DramaChi Barakamon S01E01 | **Verified** | Live probe returned HTTP 206, `video/mp4`. |
| DramaChi Kino's Journey (2003) Episode 1 | **NOT_AVAILABLE** | Search returned zero catalog results. No adaptation was substituted. |

## Exact 4KHDHub anime matrix

- Naruto Shippuden S01E01: **NOT_AVAILABLE** — provider returned `Naruto` and a Naruto movie, not a verified Shippuden catalog entry.
- Naruto Shippuden S01E32: **NOT_AVAILABLE** — same catalog evidence.
- One Piece Episode 1: duplicate catalog selection was corrected; the season-capable entry was selected, but the current live mirrors failed or timed out during this run. This remains **RESOLUTION_BLOCKED**, not a fabricated success.
- Barakamon S01E01: **NOT_AVAILABLE** — zero provider catalog results.
- Kino's Journey (2003) Episode 1: **NOT_AVAILABLE** — no matching 2003 catalog result; search candidates were unrelated Kings titles.

The provider diagnostic also confirmed that 4KHDHub exposes multiple `One Piece` catalog records: one with provider season 22 and another with seasons 1–2. The matrix now chooses an episode-capable record rather than blindly using the first duplicate.

## Files changed

- `src/providers/fourkhdhub/client.rs` — preserve resolver/preflight failure reasons.
- `tests/live_stream_verification.rs` — require at least one successful 4KHDHub source for the live movie/series tests.
- `tests/phase31_provider_verification.rs` — choose a title result with the requested episode when duplicate provider catalog entries exist.
- `docs/phase3.3-fourkhdhub-dramachi-report.md` — this report.

## Local verification

- `cargo fmt --all -- --check` — passed after formatting.
- `cargo check --locked` — passed.
- `cargo clippy --all-targets --all-features -- -D warnings` — passed before the final test-only matrix change; rerun in final verification.
- Focused strict 4KHDHub Inception live test — passed with an actual resolved HTTPS source.
- Focused strict 4KHDHub Game of Thrones live test — passed with an actual resolved HTTPS source.
- Focused DramaChi live test — passed.
- Exact DramaChi matrix — 4 targets working, Kino's Journey not available.
- Exact 4KHDHub matrix — provider-unavailable or mirror-resolution-blocked results reported without fabrication.

## Honest readiness status

- **DramaChi:** working for the tested live search/details/stream path and 4/5 requested anime targets; Kino's Journey (2003) is genuinely unavailable from the current provider catalog.
- **4KHDHub:** resolver is working for verified movie and series cases and now skips bad mirrors with actionable errors. It is not possible to claim every requested anime is available when the provider catalog or current mirrors do not provide them.
- **APK playback:** backend source resolution is not the same as target-device playback certification; no unsupported claim is made.

## Deployment

Do not deploy automatically. Review the branch, rerun the exact matrix from the intended network if needed, and verify target APK playback separately. Rollback is a Git revert to `18f51f7` or the prior known-good branch commit.
