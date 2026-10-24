# Prophecy Verse API: Current Behavior and Compatibility Notes

This document describes the Rust API implementation in this repository, not a proposed or future contract. The current API is provider-backed: it does not use PostgreSQL as a catalog store. The MovieBox provider is enabled in code and enabled by default, but routes that depend on it return `503 PROVIDER_DISABLED` when the local configuration disables it. Redis is optional and used only for search-result caching; without Redis, the API falls back to process memory.

## Catalog and search routes

- `GET /api/v1/home` requests the MovieBox homepage and returns provider-backed sections: `featured`, `trending`, `popular`, `movies`, `anime`, and `kdramas`.
- `GET /api/v1/home/featured`, `/api/v1/trending`, `/api/v1/trending/{anime|movies|k-dramas}`, and `/api/v1/popular` with the same subroutes return sections from upstream provider data. Ranking fields are provider-reported; these endpoints do not represent internal activity rankings.
- `GET /api/v1/movies` lists MovieBox movie entries. `GET /api/v1/movies/{movie_id}` returns provider details. Movie source, stream, and playback subroutes are dispatched separately.
- `GET /api/v1/k-dramas` returns series classified using provider metadata, not title-only guesses. `GET /api/v1/categories` returns supported categories and provider capabilities. K-Drama details (`/api/v1/k-dramas/{id}`) are not implemented and return `501 UNSUPPORTED_CATEGORY`.
- `GET /api/v1/latest` returns `501 UNSUPPORTED_CATEGORY` because available providers do not expose reliable release-date metadata consistently.
- `GET /api/v1/anime/search` and `/api/v1/anime/search/stream` perform live provider search (JSON and SSE). They are not database-backed catalog searches.
- `GET /api/v1/watch` is a compatibility route that searches providers and resolves a playable source at request time.

Listing routes that accept `page` and `limit` bound the requested limit in code. Provider failures return structured errors rather than fabricated catalog entries.

## Source eligibility and codec policy

Public source lists omit releases that are explicitly HEVC/H.265/x265 or whose codec metadata is absent or unrecognized. Search eligibility and playback selection also require a syntactically valid public mirror. Recognized codec labels are accepted only when they do not match the blocked HEVC family. Metadata is not rewritten or presented as FFprobe-verified merely because the provider supplied a label.

Playback, direct-watch, source-detail, audio/subtitle, download, and remux request paths reject a selected source when its codec is explicitly blocked or unverified. Stable error codes are `SOURCE_CODEC_BLOCKED` and `CODEC_UNVERIFIED` with HTTP `415`. H.265-to-H.264 transcoding is not allowed. Clients should refresh source lists if an older opaque source ID now returns one of these errors.

## Media handling and seeking

Remuxed media is streamed from FFmpeg stdout to the client. The API does not create, serve, or retain a persistent remux MP4 cache. The short FFmpeg preflight is piped into FFprobe in memory and capped at 32 MiB; startup removes the application-owned legacy `cache/remux` directory from older versions when possible.

Remux endpoints `/api/v1/sources/{source_id}/transcode`, `/remux`, and remux-mode requests through `/api/stream-proxy` do not support byte-range seeking. A `Range` request receives HTTP `501 SEEK_UNAVAILABLE`, with `X-Remux-Cache-State: disabled`. Ordinary GET streaming remains available when the source passes eligibility and remux checks. `HEAD` is not supported for these remux routes. Direct upstream proxy behavior remains distinct from the remux path.

## Compatibility and architecture limits

Existing `/api/v1/anime/*`, `/api/v1/episodes/*`, `/api/v1/sources/*`, `/api/v1/watch`, and legacy root aliases remain in place. Opaque identifiers must be passed back unchanged; clients must not construct IDs themselves.

The following requested capabilities are not implemented in this branch: a PostgreSQL schema or migrations, database-backed public catalog/search, manual authenticated import/update jobs, durable pending-import state, admin routes, separate Web and Android playback APIs, stored-source-only playback, source-session coordination, and measured production load/performance results. Current search and some playback flows still contact providers. No PostgreSQL, Render, Redis service, or Hostinger VPS deployment is implied by this code or documentation.
