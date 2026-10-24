<div align="center">

# MovieBox-TUI

**Provider-aware anime and media streaming backend with a terminal-oriented MovieBox client library.**

[![CI](https://img.shields.io/github/actions/workflow/status/newprophecy2-cloud/MovieBox-Tui-Prophecy-/ci.yml?branch=main&label=CI&logo=github&style=flat)](https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/moviebox-tui.svg?logo=rust&style=flat)](https://crates.io/crates/moviebox-tui)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg?style=flat)](#license)

</div>

> MovieBox-TUI does not host media. It discovers publicly available provider sources and returns or launches provider-controlled streams. Users are responsible for complying with local laws and provider terms.

## Overview

MovieBox-TUI is a Rust media-discovery and playback project. The current binary starts an HTTP API that exposes provider search, verified anime metadata, seasons and episodes, fresh source candidates, playback resolution, subtitles, downloads, and SSE progress events. The repository also contains the terminal UI, player-launch, download, parser, provider, and local-library modules used by the broader client.

The backend is designed for clients that need a stable API while provider-specific details remain behind opaque IDs. Clients should pass IDs returned by the API back unchanged rather than decoding or constructing them.

The current API is provider-backed and does not use PostgreSQL as a catalog store. Redis is optional and is used only for search-result caching; if it is unavailable, the API falls back to in-process memory.

## Current capabilities

- **Anime and media search:** Concurrent provider search across MovieBox, 4KHDHub, Dramachi, and configured addon sources.
- **Verified source eligibility:** Search and playback candidates require a valid public mirror and a recognized video-codec label; absent or unknown codec metadata is not treated as playable.
- **Codec policy:** HEVC/H.265/x265 is blocked across search eligibility, public source lists, direct playback, and remux routes. H.265-to-H.264 transcoding is not performed.
- **Bounded search work:** Each catalog item has a bounded source-eligibility budget so large series cannot make search latency grow without limit.
- **Episode metadata:** Fresh anime details, season lists, episode lists, and individual episode lookups.
- **Source discovery:** Fresh source candidates with quality, codec, language, size, downloadability, and opaque source IDs.
- **Automatic playback resolution:** FourKHDHub mirrors, including GreenMotors, HubCloud, and HubDrive chains, are resolved and preflighted to final playable streams.
- **Playback validation:** Candidate mirrors are checked for public HTTPS destinations and media compatibility. Remux output is streamed through bounded in-memory pipes and is never cached to disk; byte-range seeking on remux routes returns `501 SEEK_UNAVAILABLE`.
- **SSE responses:** Incremental search and source events for responsive clients.
- **Audio and subtitles:** Provider-backed embedded audio/subtitle discovery where the resolved media exposes those tracks.
- **Download metadata:** Direct-download capability is reported only when a provider source advertises an authorized direct file.
- **Terminal client modules:** Vim-style navigation, mouse interaction, themes, local favorites/history, external player launching, and local download support remain in the library.
- **Local-first state:** Configuration, favorites, watch history, and cache data stay on the local machine. No telemetry or analytics are required.

## Architecture

```text
Client
  │
  ├── Search JSON / SSE
  ├── Anime details → seasons → episodes
  └── Episode sources → source resolution → playable URL
                                      │
                                      ├── MovieBox
                                      ├── 4KHDHub
                                      │    └── GreenMotors / HubCloud / HubDrive
                                      ├── Dramachi
                                      └── Configured addons
```

### Playback resolution flow

For FourKHDHub sources, the backend follows the provider resolver chain automatically:

```text
source_id
  → provider mirror
  → GreenMotors / HubCloud / HubDrive resolver page
  → extracted final candidate
  → public HTTPS and media preflight
  → final playback URL
```

The normal `/api/v1/sources/{source_id}` endpoint returns resolved playback metadata for FourKHDHub sources. The explicit `/playback`, `/play`, and `/watch` variants are also available for clients that want to state the playback intent explicitly.

Intermediate HTML resolver pages are not considered playable media. If all provider mirrors fail preflight, the API returns a structured `502` or `504` response instead of presenting the intermediate page as a video URL.

## Requirements

### Build requirements

- Rust 1.90.0 or newer
- A working C/C++ toolchain supported by Rust
- `cargo`

### Runtime requirements for the HTTP API

The API does not require a desktop session, VLC, mpv, IINA, or a database. Provider access is request-specific and depends on upstream availability.

### Optional terminal-client requirements

- `mpv`, VLC, or IINA for external playback
- `ffmpeg` and `yt-dlp` for supported DASH downloads
- Kitty, Sixel, or iTerm2-compatible terminal graphics for cover art
- Termux packages and `termux-am` for Android workflows

## Installation and source build

### Cargo

```bash
cargo install moviebox-tui --locked
```

### From source

```bash
git clone https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-.git
cd MovieBox-Tui-Prophecy-
cargo build --release --locked
```

### Run locally

The API listens on `0.0.0.0` and uses port `8080` by default:

```bash
cargo run --release --locked
```

To select a port:

```bash
PORT=8080 cargo run --release --locked
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

Expected response:

```json
{"status":"ok"}
```

## API quick start

### Search

```bash
curl --get 'http://127.0.0.1:8080/api/v1/anime/search' \
  --data-urlencode 'q=Blue Lock' \
  --data 'limit=10'
```

Search responses include `results`, `partial`, optional `next_cursor`, and provider status information. A partial response means one or more providers were unavailable or source checks could not complete; it does not mean the returned results are fabricated.

Search accepts every source with a valid public mirror URL, including unknown, H.264, AV1, and other codec metadata. The only codec-family exclusion is explicit HEVC/H.265/x265, which is blocked by policy.

### Stream search events

```bash
curl --no-buffer --get 'http://127.0.0.1:8080/api/v1/anime/search/stream' \
  --data-urlencode 'q=Naruto'
```

The SSE endpoint emits events such as:

- `search_result`
- `search_provider_error`
- `search_complete`

### Anime details, seasons, and episodes

Use the opaque `id` returned by search:

```bash
ANIME_ID='returned-by-search'

curl "http://127.0.0.1:8080/api/v1/anime/$ANIME_ID"
curl "http://127.0.0.1:8080/api/v1/anime/$ANIME_ID/seasons"
curl "http://127.0.0.1:8080/api/v1/anime/$ANIME_ID/episodes"
```

Episode IDs are also opaque. A valid episode reference can be read directly:

```bash
EPISODE_ID='returned-by-episode-list'
curl "http://127.0.0.1:8080/api/v1/episodes/$EPISODE_ID"
```

### Sources and playback

```bash
curl "http://127.0.0.1:8080/api/v1/episodes/$EPISODE_ID/sources"

SOURCE_ID='returned-by-source-list'
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID"
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID/playback"
```

The resolved response includes fields such as:

```json
{
  "provider": "fourkhdhub",
  "resolved": true,
  "mode": "watch",
  "url": "https://final-provider-stream.example/...",
  "headers": [],
  "source_label": "Download HubCloud"
}
```

Temporary signed URLs may expire. Clients should request a fresh source resolution when playback fails because a provider URL has expired.

### Source SSE

```bash
curl --no-buffer \
  "http://127.0.0.1:8080/api/v1/episodes/$EPISODE_ID/sources/stream"
```

Source SSE events include `source_result`, `source_error`, and `source_complete`.

### Audio, subtitles, downloads, and transcode

```bash
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID/audio"
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID/subtitles"
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID/download"
curl "http://127.0.0.1:8080/api/v1/sources/$SOURCE_ID/transcode"
```

Audio and subtitle responses are only confirmed when the resolved media can be inspected. Download support is reported only for sources that advertise a permitted direct file; the API does not turn arbitrary resolver pages into downloads.

## API routes

| Method | Route | Description |
|---|---|---|
| `GET` | `/` | API information |
| `GET` | `/health` | Lightweight liveness check |
| `GET` | `/ready` | Local service-layer readiness |
| `GET` | `/api/v1` | API resource summary |
| `GET` | `/openapi.json` | OpenAPI document |
| `GET` | `/docs` | Human-readable API documentation |
| `GET` | `/api/v1/anime/search` | Concurrent JSON provider search |
| `GET` | `/api/v1/anime/search/stream` | Incremental SSE provider search |
| `GET` | `/api/v1/anime/{anime_id}` | Anime details |
| `GET` | `/api/v1/anime/{anime_id}/seasons` | Season list |
| `GET` | `/api/v1/anime/{anime_id}/episodes` | Episode list |
| `GET` | `/api/v1/anime/{anime_id}/episodes/{episode_id}/full` | Details, selected episode, and source candidates |
| `GET` | `/api/v1/anime/{anime_id}/episodes/{episode_id}/stream` | Resolved episode playback |
| `GET` | `/api/v1/episodes/{episode_id}` | Episode details |
| `GET` | `/api/v1/episodes/{episode_id}/sources` | Fresh source candidates |
| `GET` | `/api/v1/episodes/{episode_id}/sources/stream` | Incremental SSE source discovery |
| `GET` | `/api/v1/sources/{source_id}` | Source metadata and automatic playback resolution |
| `GET` | `/api/v1/sources/{source_id}/playback` | Explicit playback resolution |
| `GET` | `/api/v1/sources/{source_id}/play` | Playback alias |
| `GET` | `/api/v1/sources/{source_id}/watch` | Watch/playback response |
| `GET` | `/api/v1/sources/{source_id}/audio` | Embedded audio tracks |
| `GET` | `/api/v1/sources/{source_id}/subtitles` | Embedded subtitle tracks |
| `GET` | `/api/v1/sources/{source_id}/download` | Direct-download metadata |
| `GET` | `/api/v1/sources/{source_id}/transcode` | Supported server-side media transcode |

## Providers

Built-in provider adapters include:

- MovieBox
- 4KHDHub
- Dramachi
- BDIX CircleFTP and DhakaFlix modules where enabled
- Configured Stremio-compatible catalog and stream addons

Provider order is used for priority and tie-breaking. Providers are queried concurrently, and one provider failure does not erase successful results from other providers. Provider errors are surfaced through `provider_status` or SSE error events.

## Configuration and data

Configuration is local and can be directed to isolated locations for development or testing:

```bash
export MOVIEBOX_CONFIG_DIR="$HOME/.config/moviebox-tui"
export MOVIEBOX_DATA_DIR="$HOME/.local/share/moviebox-tui"
```

The application keeps user configuration, history, favorites, and cache data locally. Do not commit provider credentials, addon secrets, signed URLs, cookies, or local configuration files.

Expensive source-eligibility checks use a process-wide concurrency limit in addition to each item's deadline, so concurrent API clients cannot multiply upstream verification work without bound.

## Security behavior

- Public IDs are opaque and must be treated as API values.
- Provider source URLs are validated before outbound use.
- Private, loopback, local, and invalid destinations are rejected.
- Resolver pages are not accepted as final media URLs.
- HTML, archive, and obvious error responses are rejected during playback preflight.
- Sensitive request headers such as cookies, authorization, API keys, and auth tokens are not returned as public headers.
- API errors include structured codes, a message, optional provider information, retryability, and a request ID.
- No unrestricted Render-side media proxy or media storage is provided.

## Development and validation

Run the standard checks from the repository root:

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
```

The deterministic test suite covers provider parsers, resolver payload decoding, source identity, API contracts, error handling, player contracts, downloads, history, favorites, settings, and TUI behavior. Live provider tests are marked ignored because they depend on external services:

```bash
cargo test --test live_stream_verification -- --ignored
```

Live tests may fail when a provider is rate-limited, unavailable, changes its HTML, or returns expired mirrors. Such failures should be diagnosed separately from deterministic regressions.

### Live resolver verification

The repository includes a safe live resolver check for GreenMotors/HubCloud-backed FourKHDHub sources. It follows the explicit playback route, rejects intermediate resolver hosts, and uses `ffprobe` to confirm final media metadata without printing signed URLs:

```bash
python3 scripts/live_resolver_test.py --base-url https://moviebox-tui-prophecy.onrender.com --timeout 180
```

The script reports source count, provider label, final host, and detected media tracks. A provider may be slow or temporarily unavailable; rerun with a longer timeout rather than treating a single upstream timeout as a code regression.

## Deployment

The repository includes `render.yaml` for an HTTP service deployment. The service should:

- Build with `cargo build --release --locked`.
- Start the prebuilt `./target/release/moviebox-tui` binary rather than invoking Cargo again at service startup.
- Listen on Render's `$PORT`.
- Use `/health` for the health check.
- Keep provider configuration and secrets outside the repository.

A deployment must be rebuilt after backend changes before the public API reflects them. The repository itself does not store media or require a database.

## Contributing

1. Fork the repository or create a feature branch.
2. Keep provider URLs, cookies, tokens, and local configuration out of commits.
3. Add deterministic regression coverage for parser, API, resolver, and security changes.
4. Run formatting, locked compilation, tests, and clippy before opening a pull request.
5. Describe live-provider limitations separately from code regressions.

Repository: [newprophecy2-cloud/MovieBox-Tui-Prophecy-](https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-)

## Privacy

MovieBox-TUI does not require telemetry, analytics, or user tracking. Watch history, favorites, and configuration remain local to the client environment.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE).

## Disclaimer

MovieBox-TUI does not host, upload, or store media. It uses provider adapters to discover publicly available metadata and streams. Users must comply with applicable laws, copyright rules, and third-party provider terms.
