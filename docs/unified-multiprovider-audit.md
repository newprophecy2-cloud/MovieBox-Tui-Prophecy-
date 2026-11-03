# Unified Multi-Provider Content Engine — Phase 1

**Repository:** `newprophecy2-cloud/MovieBox-Tui-Prophecy-`
**Implementation branch:** `feature/unified-content-engine-completion`
**Baseline:** `3828477c4eeb53e76fd00acb9b1a4cbe5c22a22b`

## Architecture

The live provider-backed `/api/v1/anime/*` routes now use the existing provider service and the PostgreSQL catalog as an optional persistence layer. Search and detail fan-out remain bounded and respect the current provider configuration. MovieBox remains excluded from anime discovery; disabled providers are not re-enabled, and cached search items are filtered against current eligibility before use.

The existing provider-bound opaque IDs remain accepted. Search cards now carry an additive canonical ID and an opaque unified ID envelope that retains the primary provider reference and the other mapped provider references. If PostgreSQL is available, the canonical UUID and provider title mappings are persisted. Without PostgreSQL, the API uses a deterministic title/type/year digest and retains provider references inside the opaque search ID for the current client flow.

## Unified Search and Identity

`GET /api/v1/anime/search` and the SSE search route continue to fan out to configured search providers concurrently. Search results are normalized and grouped by the existing conservative matcher: normalized title, media type, year compatibility, and preserved sequel/season/remake qualifiers. Provider-specific search records are retained so the card can choose a valid poster and preserve the source title and metadata in storage. The response still contains the prior fields and adds `canonical_id` and `provider_mappings`.

When a catalog store is configured, each grouped card is upserted transactionally. Existing provider-title mappings are checked before title matching; metadata upserts preserve existing values when new data is empty. Provider registry rows are not enabled as a side effect of search. Search-provider eligibility and source safety rules remain unchanged.

## Provider Mapping Persistence

The existing catalog tables remain the canonical title and episode persistence foundation. Additive migration `migrations/005_unified_content_mappings.sql` adds provider-season mappings and mapping status/metadata for episode-provider mappings. Provider title, season, and available episode records are upserted transactionally; failed provider requests do not delete existing mappings.

The catalog is still optional unless deployment configuration requires it. When it is unavailable, the live API can resolve mappings carried in the unified opaque search ID, but those mappings do not become durable until a later request reaches the catalog successfully.

## Unified Details and Seasons

For a unified search ID, `GET /api/v1/anime/{anime_id}` loads persisted provider title mappings where available, combines them with references carried in the ID, filters them through current provider eligibility, and requests details concurrently with per-provider timeouts. Successful providers contribute even when another provider fails. The response retains the existing detail fields and adds canonical ID, provider mappings, per-provider status, and a partial-result flag.

Season lists are combined by canonical anime and season number. The API exposes a stable canonical season ID and the catalog preserves provider season numbers. Current provider models do not expose stable upstream season IDs or season titles, so those IDs remain absent rather than being synthesized. A provider season mapping can therefore contain a real provider season number while omitting the provider-season ID.

## Unified Episodes and Gap Filling

The live details path gathers the episode records exposed by all successful mapped providers and returns the union of observed records in season/episode order. This fills gaps when another provider actually supplies the missing episode; it does not invent episode numbers, titles, or provider IDs. The response exposes a stable canonical episode ID and retains API-generated provider lookup references for the provider anime ID and episode coordinates, allowing existing provider-specific episode routes to resolve supported records.

Cross-provider episodes are combined only when season and episode number agree, providers differ, and both records have the same normalized non-empty title. Conflicting titles and missing titles remain separate response records. This is intentionally conservative because the shared provider `Episode` model does not carry dependable air dates, episode types, special/OVA flags, thumbnails, durations, or stable provider episode IDs for the configured adapters.

Canonical season and episode metadata are persisted. Each observed provider episode also stores its actual provider anime ID, provider season/episode coordinates, original provider title, and an API-local opaque lookup reference in a dedicated table; this is explicitly separate from an upstream episode ID. Coordinate/title conflicts remain separate records in the live API response and their persisted lookup mappings are marked unresolved. The pre-existing catalog `episodes` uniqueness constraint permits only one stored episode row per canonical season/number, so conflicting records are not silently overwritten and their provider lookup metadata is retained as unresolved rather than represented as separate catalog episode rows. No guessed upstream episode ID is inserted.

## API Compatibility and Phase Boundary

Legacy provider-bound IDs and existing routes continue to be accepted. The new fields and unified opaque ID envelope are additive. Existing playback, source selection, audio, remux, direct-play, and player behavior were not changed; the unified episode response uses the existing primary-provider path for playback. No Phase 2 source aggregation or audio-selection behavior is included.

## Known Limitations

- Search matching uses metadata available in the current `CatalogItem` model. It cannot reliably compare native titles, alternative-title catalogs, franchise identifiers, or season/remake metadata that providers do not return.
- The existing source-eligibility rule requires a positively recognized allowed codec and a valid mirror. DramaChi currently supplies releases without a codec, so those results may be excluded by the existing fail-closed policy; this implementation does not bypass that restriction.
- Provider season IDs and upstream provider episode IDs are not consistently represented in the shared provider models. Season numbers and durable API-local episode lookup references are available; the API does not fabricate upstream provider episode IDs when an adapter omits them.
- Episode-title equality is deliberately conservative. Translated titles that differ are kept separate unless the provider model gains other reliable matching signals.
- The existing catalog episode table cannot represent two independently resolved canonical episode rows at the same season/episode coordinate. The additive lookup table preserves conflicting provider records as unresolved, but a future catalog model change is needed to persist multiple separate canonical rows for that coordinate.
- Persistence requires the existing PostgreSQL catalog. Without it, mapping continuity is limited to references carried by the search response ID and is not a substitute for durable storage.
- Phase 2 playback/source aggregation, audio aggregation, quality selection, APK changes, deployment, and production service changes remain out of scope.
