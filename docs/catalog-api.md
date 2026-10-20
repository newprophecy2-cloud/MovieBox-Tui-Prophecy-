# Prophecy Verse Catalog API

These additive routes use the existing MovieBox homepage metadata adapter. Existing anime, episode, source, audio, subtitle, download, playback, and watch routes are unchanged.

## Routes

- `GET /api/v1/home` returns `featured`, `trending`, `popular`, `movies`, and `anime` sections from the MovieBox homepage response.
- `GET /api/v1/home/featured` returns the deterministic first page of the real MovieBox homepage ordering.
- `GET /api/v1/trending`, `/api/v1/trending/anime`, and `/api/v1/trending/movies` use the provider's exposed trending metric. Items without that metric are not promoted into the ranked result.
- `GET /api/v1/popular`, `/api/v1/popular/anime`, and `/api/v1/popular/movies` use the provider's exposed popularity metric. The API does not invent view counts or ratings.
- `GET /api/v1/movies` lists real MovieBox items whose normalized media type is `movie`; `GET /api/v1/movies/{movie_id}` resolves details using the opaque provider-preserving identifier returned by the list route.
- `GET /api/v1/categories` reports currently supported categories and provider capabilities.

All supported listing routes accept `page` and `limit`. `limit` is bounded to 1–100. `next_page` is an availability hint based on a non-empty provider page; provider-specific cursors are not fabricated.

## Honest unsupported categories

`/api/v1/latest`, the K-Drama list/details routes, and K-Drama trending/popular routes return `501 UNSUPPORTED_CATEGORY`. The currently normalized provider metadata does not reliably expose release dates or verified Korean country/original-language classification, so these routes do not fabricate content.

## Provider and ranking semantics

The current catalog implementation is backed by MovieBox only because it is the existing provider with a real homepage/collection endpoint and adapter. The response identifies `provider: moviebox` and includes a `ranking_basis` field. `featured` and media-type sections preserve MovieBox homepage ordering; trending and popular sections use only the corresponding metrics parsed from the provider payload.

Home requests use a bounded upstream timeout. A provider failure returns a structured `503 UPSTREAM_UNAVAILABLE` response rather than an apparently successful response containing fake items. Catalog routes never call playback resolution.

## Backward compatibility

No existing public route was renamed, removed, redirected, or changed. The existing `/api/v1/anime/*`, `/api/v1/episodes/*`, `/api/v1/sources/*`, `/api/v1/sources/{source_id}/watch`, and `/api/v1/sources/{source_id}/playback` contracts remain outside this additive update.
