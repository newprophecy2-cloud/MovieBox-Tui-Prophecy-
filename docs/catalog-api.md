# Prophecy Verse Catalog API

**Current implementation status:** the MovieBox catalog provider is intentionally disabled in `src/api.rs` (`MOVIEBOX_PROVIDER_ENABLED = false`). MovieBox-backed catalog routes such as home, trending, popular, movies, K-Dramas, and categories return `503 MOVIEBOX_PROVIDER_DISABLED` until that provider is explicitly re-enabled. The route descriptions below document the intended contract and must not be interpreted as a claim that these endpoints are currently live.

The intended catalog adapter uses MovieBox movie (`tabId=2`), drama/series (`tabId=5`), and anime (`tabId=8`) collections. Series details are checked for classification metadata with bounded concurrency. Existing provider-backed anime, episode, source, audio, subtitle, download, playback, and watch routes are separate from these disabled MovieBox catalog routes.
## Routes

- `GET /api/v1/home` returns `featured`, `trending`, `popular`, `movies`, `anime`, and `kdramas` sections from real MovieBox collection responses.
- `GET /api/v1/home/featured` returns the deterministic first page of the real MovieBox homepage ordering.
- `GET /api/v1/trending`, `/api/v1/trending/anime`, and `/api/v1/trending/movies` use the provider's exposed trending metric. Items without that metric are not promoted into the ranked result.
- `GET /api/v1/popular`, `/api/v1/popular/anime`, and `/api/v1/popular/movies` use the provider's exposed popularity metric. The API does not invent view counts or ratings.
- `GET /api/v1/movies` lists real MovieBox items whose normalized media type is `movie`; `GET /api/v1/movies/{movie_id}` resolves details using the opaque provider-preserving identifier returned by the list route.
- `GET /api/v1/k-dramas` returns only MovieBox series with verified Korean metadata: a K-Drama category, Korean/South Korean country, or Korean original language from the subject details response.
- `GET /api/v1/categories` reports currently supported categories and provider capabilities.

All supported listing routes accept `page` and `limit`. `limit` is bounded to 1–100. `next_page` is an availability hint based on a non-empty provider page; provider-specific cursors are not fabricated.

## Honest unsupported categories

`/api/v1/latest` returns `501 UNSUPPORTED_CATEGORY` because reliable release-date metadata is not exposed consistently by the available providers.

K-Drama list and trending/popular routes are now supported as metadata-backed filters. An item is included only when it is a series and the provider payload contains a verified K-Drama category, Korean/South Korean country, or Korean original-language value. Missing metadata means the item is excluded; title-only matching is deliberately not used.

## Provider and ranking semantics

The current catalog implementation is backed by MovieBox only because it is the existing provider with a real homepage/collection endpoint and adapter. The response identifies `provider: moviebox` and includes a `ranking_basis` field. `featured` and movie sections preserve MovieBox homepage ordering. Anime sections require provider category/genre/language metadata; K-Drama sections require provider country/language/category metadata. Trending and popular sections use only the corresponding metrics parsed from the provider payload.

Home requests use a bounded upstream timeout and a concurrency limit for details enrichment. A provider failure returns a structured `503 UPSTREAM_UNAVAILABLE` response rather than an apparently successful response containing fake items. Catalog routes never call playback resolution.

## Backward compatibility

No existing public route was renamed, removed, redirected, or changed. The existing `/api/v1/anime/*`, `/api/v1/episodes/*`, `/api/v1/sources/*`, `/api/v1/sources/{source_id}/watch`, and `/api/v1/sources/{source_id}/playback` contracts remain outside this additive update.
