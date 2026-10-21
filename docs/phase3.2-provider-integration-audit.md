# Phase 3.2 Provider Integration Audit

## Scope and baseline

Repository: `newprophecy2-cloud/MovieBox-Tui-Prophecy-`

Starting commit: `a6436a60ef02ee917df9a00621dc7db804a81fc6`

Branch: `phase3.2-provider-integration-fixes`

Scope was limited to CircleFTP, DhakaFlix, and Stremio-style HTTP Addons. MovieBox was not enabled, called, or used as a fallback or live-test target. Render was not deployed.

## Executive summary

The request path was traced as:

`API request → provider priority/selection → search → metadata/details → episode/source mapping → provider URL construction → shared URL validation → source/probe/playback response`.

Two confirmed defects were fixed:

1. DhakaFlix converted every failed, timed-out, non-2xx, or malformed host response into an apparent successful empty search. It now preserves valid empty results, marks failed hosts unhealthy, tries the remaining configured hosts, and returns an explicit unavailable error when all attempted hosts fail.
2. Addon stream responses without a `streams` array were silently treated as a valid empty result. They now fail as malformed responses. Addon metadata details also now enforce an explicit `movie`/`series` type hint so a requested series cannot silently become a movie, or vice versa.

CircleFTP had no proven parser or request-construction defect that could be safely fixed from this environment. Its configured endpoint is currently unreachable from the sandbox, so live schema and stream behavior remain blocked.

The default live Cinemeta manifest was reachable and catalog/meta-capable, but it declared no stream resource. Therefore the Addon integration remains `ADDON_UNCONFIGURED`; manifest reachability is not reported as stream availability.

## Architecture and implementation matrix

| Provider | Current implementation | Evidence | Status |
|---|---|---|---|
| CircleFTP | `CircleFtpClient` posts/searches `http://new.circleftp.net:5000/api/posts`, parses `posts`, fetches `/posts/{id}`, maps `content[].episodes[].link` to direct releases, and uses shared downstream URL validation before probing/playback. | `src/providers/bdix/circleftp/client.rs`, `parser.rs`, `mod.rs`; deterministic parser test passed; live endpoint timed out with HTTP 000 after 8.002s and resolved remote `15.1.1.50`. | `PROVIDER_UNREACHABLE_FROM_TEST_ENV` |
| DhakaFlix | `DhakaFlixClient` posts the configured search body to all healthy configured hosts concurrently, deduplicates by title/year and quality, then queries item listings for video files. | `src/providers/bdix/dhakaflix/client.rs`; all four configured private hosts timed out from the sandbox; deterministic mapping tests passed. | `NETWORK_ACCESS_REQUIRED` |
| HTTP Addons | Manifest, catalog search, metadata, episode video mapping, stream request, filtering, metadata extraction, deduplication and shared public URL validation are implemented. | `src/providers/addons/{client,models,adapter,aggregator,mod}.rs`; live Cinemeta manifest fetched successfully but had no stream resource. | `ADDON_UNCONFIGURED` for stream resolution |

## Root-cause table

| Provider | Symptom | Proven root cause | Code fix | Regression test | Live verification |
|---|---|---|---|---|---|
| CircleFTP | Search cannot be live verified | TCP/HTTP request to the configured public hostname timed out; no response schema was obtained. | None; changing host, scheme, path, or schema would be guesswork. | Existing `test_circleftp_search_to_catalog_maps_movies_and_series` passed. | Blocked: endpoint timed out from sandbox. |
| DhakaFlix | Failed hosts previously appeared as an empty catalog | Search swallowed request, status, body-read, and JSON/schema failures and returned `Ok([])`. | Propagate non-2xx and parse failures per host; mark failed host; return `Unavailable` if all attempted hosts fail. | `all_failed_servers_are_not_reported_as_empty_success`; `one_valid_empty_server_response_is_a_real_empty_result`; existing stream mapping test passed. | Blocked: all four private configured hosts timed out from public sandbox. |
| DhakaFlix | Malformed stream response previously appeared as no releases | `streams()` ignored JSON and missing `items` errors. | Require valid JSON and an `items` array; return `Parse` otherwise. | Existing valid stream fixture passed; malformed behavior is covered by the strict response path. | Blocked by private BDIX routing. |
| HTTP Addons | `{}` or another object without `streams` became a successful empty stream result | `fetch_streams()` returned `Ok(Vec::new())` for any non-array object without `streams`. | Require a `streams` array, while retaining compatibility with a raw stream array. | `missing_streams_field_is_not_treated_as_empty_success`; `stream_array_response_is_supported`. | Default live Cinemeta manifest has no stream resource; no authorized stream-capable addon was configured. |
| HTTP Addons | Type-hinted metadata could be silently returned as the opposite media type | Details accepted a valid ID/title before enforcing the requested type prefix. | Enforce movie vs series/tv/anime compatibility, with a narrow empty-type/video-list inference for series. | `type_hint_rejects_movie_for_series_request`; `type_hint_rejects_series_for_movie_request`. | Live stream resolution not applicable without a configured stream addon. |

## Anime verification matrix

The BDIX hosts were not reachable, so CircleFTP and DhakaFlix live anime rows are blocked rather than marked unavailable globally. No fabricated provider entries or stream URLs were created.

| Test IDs | Provider | Result | Evidence |
|---|---|---|---|
| CF-01..CF-05 | CircleFTP | `PROVIDER_UNREACHABLE_FROM_TEST_ENV` | Configured hostname resolved, but `/api/posts` timed out; no catalog response was available. |
| DF-01..DF-05 | DhakaFlix | `NETWORK_ACCESS_REQUIRED` | `172.16.50.7`, `.14`, `.12`, and `.9` all failed TCP connection from sandbox. |
| ST-01..ST-04 | Default Cinemeta metadata path | Metadata/catalog capability only; stream stage `ADDON_UNCONFIGURED` | Live manifest was reachable and reported catalog/meta resources plus 8 catalogs, but no stream resource. |

The repository's existing exact matrix test confirms that the required targets are represented without MovieBox. Its provider live tests remain explicitly ignored and were not presented as passed live verification.

## Network diagnostics

Environment: Manus Sandbox, public/non-BDIX network.

- CircleFTP DNS: `new.circleftp.net → 15.1.1.50`.
- CircleFTP request: `http://new.circleftp.net:5000/api/posts`; HTTP `000`, remote `15.1.1.50`, timeout after `8.002274s`, zero response bytes.
- DhakaFlix: all configured `172.16.x.x` destinations timed out on TCP connect after approximately 3 seconds.
- No BDIX routing was inferred or claimed.
- The private-address URL policy was not weakened.

## Security review

Existing shared URL and DNS controls remain intact. The changes do not disable private-network, loopback, credential-bearing URL, redirect, IPv4-mapped IPv6, or nested media URL safeguards. Addon stream URLs still pass the existing HTTP-scheme and downstream public-destination validation path. No credentials, signed URLs, cookies, or authorization headers were added to logs or this report.

No full media episode was downloaded. No DRM, authentication, paywall, or provider restriction was bypassed.

## Changed files

- `src/providers/bdix/dhakaflix/client.rs`: strict search/stream response handling, all-host failure classification, and regression tests.
- `src/providers/bdix/dhakaflix/mod.rs`: maps the new unavailable classification to `ProviderError::Unavailable`.
- `src/providers/addons/client.rs`: strict stream response parser and malformed/array response tests.
- `src/providers/addons/mod.rs`: media-type hint enforcement and regression tests.
- `docs/phase3.2-provider-integration-audit.md`: this evidence report.

## Test report

Deterministic/fixture tests:

- `cargo fmt --all -- --check` — passed.
- `cargo check --locked` — passed.
- Targeted Addon client tests — 2 passed.
- Targeted Addon type-hint tests — 2 passed.
- Targeted DhakaFlix tests — 5 passed.
- `cargo test --locked -- --test-threads=1` — passed: 454 library tests, plus all integration suites; live provider tests remained ignored as explicitly marked.
- `cargo clippy --all-targets --all-features -- -D warnings` — passed.
- `cargo build --release --locked` — passed.

One earlier parallel full-test run observed a single existing MPV command test failure. The exact test was rerun three times serially and passed each time; the final complete serial suite passed. No Phase 3.2 test failure remained.

Live/blocked tests:

- CircleFTP live endpoint — blocked by timeout from the sandbox.
- DhakaFlix live hosts — blocked by unavailable BDIX/private routing.
- Addon live manifest — Cinemeta manifest fetched successfully; it is metadata/catalog-only for this configuration and has no stream resource.
- No live stream-capable addon test was claimed because none was configured or authorized.

Performance: the existing deterministic performance audit passed. Provider live latency, request counts, and source startup latency were not reported because the required CircleFTP/DhakaFlix network and stream-capable addon were unavailable.

## Remaining blockers and unblock actions

1. **CircleFTP:** run the bounded search/details/release tests from an authorized network where `new.circleftp.net:5000` is reachable. Capture sanitized status/content type and rerun the CircleFTP live matrix.
2. **DhakaFlix:** run each configured private host independently from an authorized BDIX-connected machine. Rerun search, failover, item listing, Range, and stream probes without downloading complete media.
3. **HTTP Addons:** configure an authorized stream-capable Stremio addon separately from Cinemeta, validate its manifest resource declaration, then run metadata → episode ID → stream endpoint → URL validation tests. Do not commit credentials.
4. **Playback:** even after a source is verified, device playback remains untested unless run on the target APK/device; backend source verification must not be treated as codec or subtitle-rendering proof.

## Deployment recommendation and rollback

Do not deploy automatically. First run the blocked live checks from the required authorized networks and configure a legitimate stream addon if stream resolution is required. If deployed later, keep the current production configuration unchanged and roll back by reverting the Phase 3.2 commit or redeploying the prior known-good commit `a6436a6`.

## Git result

- Starting commit: `a6436a60ef02ee917df9a00621dc7db804a81fc6`.
- Branch: `phase3.2-provider-integration-fixes`.
- Render: not deployed.
- MovieBox: excluded from provider execution and testing.
- Final commit hash and push result are recorded after commit/push in the delivery message.
