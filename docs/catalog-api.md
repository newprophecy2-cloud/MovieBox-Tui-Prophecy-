# Prophecy Verse API: Current Behavior and Compatibility Notes

This document describes the Rust API implementation in this repository, not a proposed or future contract. The new catalog routes use PostgreSQL when `DATABASE_URL` is configured; Redis remains optional and is used only for search-result caching. The legacy APK-compatible provider routes remain provider-backed and are intentionally preserved during migration.

## Catalog and search routes

- `GET /api/v1/home` requests the MovieBox homepage and returns provider-backed sections: `featured`, `trending`, `popular`, `movies`, `anime`, and `kdramas`.
- `GET /api/v1/home/featured`, `/api/v1/trending`, `/api/v1/trending/{anime|movies|k-dramas}`, and `/api/v1/popular` with the same subroutes return sections from upstream provider data. Ranking fields are provider-reported; these endpoints do not represent internal activity rankings.
- `GET /api/v1/movies` lists MovieBox movie entries. `GET /api/v1/movies/{movie_id}` returns provider details. Movie source, stream, and playback subroutes are dispatched separately.
- `GET /api/v1/k-dramas` returns series classified using provider metadata, not title-only guesses. `GET /api/v1/categories` returns supported categories and provider capabilities. K-Drama details (`/api/v1/k-dramas/{id}`) are not implemented and return `501 UNSUPPORTED_CATEGORY`.
- `GET /api/v1/latest` returns `501 UNSUPPORTED_CATEGORY` because available providers do not expose reliable release-date metadata consistently.
- `GET /api/v1/anime/search` and `/api/v1/anime/search/stream` perform live provider search (JSON and SSE). They are not database-backed catalog searches.
- `GET /api/v1/search?q=...` is the new PostgreSQL-only catalog search. It never performs live provider discovery. An empty result creates or increments a durable `pending_import_requests` record.
- `GET /api/v1/home`, `/api/v1/trending`, `/api/v1/popular`, `/api/v1/latest`, `/api/v1/movies`, and `/api/v1/k-dramas` use PostgreSQL catalog listings when `DATABASE_URL` is configured; without the database they preserve the legacy provider/compatibility behavior.
- `GET /api/v1/genres` and `GET /api/v1/catalog/titles/{title_id}` expose canonical PostgreSQL genre and title-detail records when the catalog is configured.
- Protected manual catalog operations are available under `/api/v1/admin/catalog/*` and require `ADMIN_TOKEN`.
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

The following requested capabilities remain incomplete: real MAL-based 700+ catalog import, provider-to-PostgreSQL episode/source population, Redis catalog invalidation and distributed coordination, fully functional Web remux sessions, Android direct-play source lookup, source-session sharing, tested database backup/restore, and measured production load/performance results. The legacy `/api/v1/anime/search` and playback routes still contact providers for APK compatibility; the new `/api/v1/search` and new Web/Android contract families do not perform provider discovery. No PostgreSQL, Render, Redis service, or Hostinger VPS deployment is implied by this code or documentation.

## PostgreSQL catalog import contract

`POST /api/v1/admin/catalog/import` is a synchronous, transactional import endpoint. It accepts the original compact payload and the expanded form below. The endpoint requires `X-Admin-Token` or `Authorization: Bearer <ADMIN_TOKEN>`, accepts at most 500 titles, returns `201` only when every title commits, and returns `422` with a completed job summary when one or more title transactions fail.

```json
{
  "titles": [{
    "mal_id": 1001,
    "canonical_title": "One Piece",
    "content_type": "anime",
    "synopsis": "...",
    "release_year": 1999,
    "poster_url": "https://example.invalid/poster.jpg",
    "alternative_titles": [{"title": "ワンピース", "language": "ja", "title_type": "native"}],
    "genres": ["Action", "Adventure"],
    "providers": [{
      "provider_key": "example-provider",
      "display_name": "Example Provider",
      "provider_title_id": "one-piece",
      "provider_url": "https://provider.invalid/title/one-piece",
      "episodes": [{
        "season_number": 1,
        "provider_episode_id": "one-piece-s01e01",
        "provider_episode_number": 1,
        "sources": [{
          "provider_source_id": "source-1",
          "source_reference": "https://media.invalid/one-piece-01.mp4",
          "quality": "1080p",
          "codec": "H.264",
          "audio_codec": "aac",
          "container": "mp4",
          "resolution_width": 1920,
          "resolution_height": 1080,
          "source_status": "active",
          "codec_verification_status": "verified",
          "expires_at": null,
          "audio_tracks": [{"language_code": "en", "codec": "aac", "format": "aac", "is_default": true}],
          "subtitle_tracks": [{"language_code": "en", "format": "webvtt", "is_default": true}]
        }]
      }]
    }],
    "seasons": [{
      "season_number": 1,
      "canonical_label": "Season 1",
      "episode_count_reported": 1,
      "episodes": [{"episode_number": 1, "episode_title": "Episode 1", "provider_episode_ids": []}]
    }]
  }]
}
```

The title, alternatives, genres, provider mapping, seasons, episodes, episode mappings, sources, and tracks are written in one PostgreSQL transaction per title. A failed source or episode rolls back the complete title transaction. MAL ID, provider title ID, provider source ID, season/episode identity, and track identity are used for idempotent re-imports; records absent from a later import are retained rather than deleted.

Import responses include `inserted_count`, `updated_count`, `skipped_count`, `failed_count`, `titles_imported`, `seasons_imported`, `episodes_imported`, `sources_imported`, and `tracks_imported` in the job object.

When provider episode mappings are supplied on a canonical episode instead of under `providers[].episodes`, each mapping must include `provider_key`; `display_name` is optional. This explicit identity is required to attach the episode mapping, sources, and tracks to the correct provider. Mappings without it fail that title transaction rather than being silently ignored.

## Catalog UUID route family

`GET /api/v1/search?q=...` searches PostgreSQL and returns records with `id_type: "catalog_title"`. These UUIDs are for the catalog route family and must not be passed to the legacy provider route family.

- `GET /api/v1/catalog/titles/{title_id}`: canonical detail, alternatives, genres, provider mappings, and season summaries.
- `GET /api/v1/catalog/titles/{title_id}/seasons`: seasons.
- `GET /api/v1/catalog/titles/{title_id}/seasons/{season}/episodes`: episodes.
- `GET /api/v1/catalog/episodes/{episode_id}`: one episode.
- `GET /api/v1/catalog/episodes/{episode_id}/sources`: eligible, unexpired, verified non-HEVC sources.
- `GET /api/v1/catalog/sources/{source_id}`: stored source metadata.
- `GET /api/v1/catalog/sources/{source_id}/tracks`: audio and subtitle tracks.
- `GET /api/v1/catalog/sources/{source_id}/play`: stored playback resolution.

The existing `/api/v1/anime/...`, `/api/v1/episodes/...`, and `/api/v1/sources/...` routes remain opaque provider-ID routes for APK compatibility. Catalog playback rejects blocked HEVC/H.265/x265, unverified, unknown, or expired sources. Provider live lookup is not silently used by the catalog route family.
