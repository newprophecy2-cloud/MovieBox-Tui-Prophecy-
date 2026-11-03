# Unified Multi-Provider Content: Repository Audit

**Audit date:** 2026-11-03  
**Repository:** `newprophecy2-cloud/MovieBox-Tui-Prophecy-`  
**Base:** `main` / `origin/main`, commit `847cfe8bc32c06fd1da2994d62b336aafd2eae86`  
**Working state at audit:** fresh clone; clean before branch creation. No production systems, data, or environment variables were changed.

## Executive summary

The repository already has two distinct content systems: (1) legacy provider-backed `/api/v1/anime/*` routes whose identifiers are provider-bound opaque references, and (2) a PostgreSQL-backed `/api/v1/catalog/*` API with canonical title UUIDs, provider mappings, seasons, episodes, provider episode mappings, provider sources, and source tracks. The second is the correct persistence foundation for a unified identity system; a third catalog should not be created.

The existing PostgreSQL schema is more advanced than the live provider-backed anime API. It can represent one canonical title with multiple providers, one canonical episode with provider-specific episode mappings, and source-specific audio/subtitle tracks. However, current live anime search only queries FourKHDHub, and catalog-by-name import, codec verification, and catalog playback resolution are FourKHDHub-specific. The database catalog is optional at startup and the legacy provider routes must continue to work without it.

A safe integration therefore needs to preserve legacy provider IDs and endpoints, reuse catalog tables and migrations for durable canonical identity, and add provider-aware orchestration behind additive APIs or opt-in workflows. Search-time title similarity cannot safely be the sole identity key. In the absence of trusted external IDs and reliable year/type metadata, ambiguous titles must remain separate.

## Current provider-backed API

`src/api.rs` implements the routes directly over a small Tokio HTTP server. The live provider-backed routes include `/api/v1/anime/search`, its SSE variant, anime details/seasons/episodes, episode sources/SSE, source playback/audio/subtitles/download, direct play, and native/remux media paths. `/openapi.json` is constructed in code; there is no checked-in OpenAPI document.

Legacy anime/search IDs encode a provider and provider-specific item ID, with season/episode coordinates where relevant. Source IDs additionally encode a provider release fingerprint and compatibility index. Clients are expected to pass IDs back unchanged. Existing handlers validate anime/episode relationships; codec checks require positively recognized allowed codecs and block H.265/HEVC. These IDs and playback paths must not be reinterpreted or bypassed.

Search currently deduplicates exact `(provider, provider ID)` pairs only. It performs detail/source eligibility checks and reports partial provider state, but does not merge one anime across providers. `configured_search_providers()` intentionally returns only FourKHDHub when enabled. This is a deliberate deployment choice and is distinct from provider availability in `MovieBoxService`.

## Providers and configuration observed

The current checkout contains MovieBox, FourKHDHub, DramaChi, CircleFTP (BDIX), DhakaFlix (BDIX), and Addons. **There is no AniZone provider module in this branch**, despite AniZone being named in the supplied brief; those references appear to describe a different/older branch and cannot be implemented by reusing a current AniZone client.

The provider service has typed search/details/release dispatch for the built-in clients. Default local config enables MovieBox, FourKHDHub, and DramaChi; disables BDIX and Addons. The live anime search allowlist is still FourKHDHub-only. Provider capabilities do not establish source reliability by themselves, and an installed client must not be activated solely because it exists.

## Existing PostgreSQL catalog (reuse rather than replace)

Migrations `001_catalog.sql` through `004_movie_sources.sql` define:

- `catalog_titles`: stable UUID canonical IDs, optional unique MAL ID, normalized/canonical title, content type, year, metadata/status.
- `provider_registry` and `provider_titles`: provider-scoped identifiers and metadata mapped to canonical titles.
- `seasons`, `episodes`, and `episode_provider_mappings` for canonical and provider-specific episode identity.
- `provider_sources` and `source_tracks` for provider-specific source records and source-scoped audio/subtitle metadata.
- Existing import jobs/checkpoints, codec verification jobs, and indexes/uniqueness constraints.

`CatalogStore::import_title` wraps title upserts in transactions and supports idempotent mapping updates. PostgreSQL is optional by default; `REQUIRE_DATABASE=true` makes it a readiness dependency. Redis is an optional cache with an in-process fallback.

Important limitations: no unique normalized-title constraint exists (appropriately avoiding false merges); no-MAL imports currently match using provider identity in the import flow; canonical episode mapping uses season/episode coordinates; live catalog source resolution/codec verification only supports FourKHDHub. Imported source codec verification is not caller-trusted and must remain so.

## Compatibility and safety requirements

1. Keep legacy `/api/v1/anime/*` route behavior and the meaning of existing provider-bound IDs.
2. Do not silently reinterpret a legacy opaque ID as a global catalog UUID.
3. Keep PostgreSQL optional for legacy routes and do not add a third durable catalog.
4. Preserve source URL validation, final media preflight, positive codec verification, the H.265/HEVC block, and native/remux/direct-play contracts.
5. Do not claim audio languages from provider dub labels alone; actual track data belongs to the exact provider source. Never combine unrelated video/audio streams.
6. Use bounded fan-out/timeouts and report provider failures independently.
7. Do not enable a provider without confirming its configuration/capabilities and enforcing the current source safety policy.

## Validation baseline

Documented checks include `cargo fmt --all -- --check`, `cargo check --locked`, `cargo test --locked`, `cargo clippy --locked --all-targets -- -D warnings`, and release build. CI runs locked all-target/all-feature checks, Clippy, nextest, platform builds and packaging checks. Existing API regression coverage is largely unit-level; there is no end-to-end test that exercises cross-provider merge, episode gap filling, audio routing, or provider fallback through the HTTP API. Database import integration tests require a disposable PostgreSQL database and are ignored by default.

## Implementation direction

- Use a deterministic conservative identity matcher; exact normalized title/type plus non-conflicting year and preserved franchise qualifiers only. Do not fuzzy-merge or collapse same-provider duplicate records.
- Keep provider-specific mapping references intact and API-generated; clients must not construct provider IDs.
- Build unified episode lists from per-provider episode references, retaining every mapping; same-number/title conflicts remain distinct rather than being merged blindly.
- Extend existing catalog ingestion/mapping/storage and use existing codec/source verification rather than adding parallel persistence or unverified playback shortcuts.
- Make any API additions additive or versioned; preserve old routes and test both contracts.
- Treat the difference between this checkout (no AniZone module) and the attached example as a known scope constraint.

This is an audit of the checked-out backend, not a claim that the complete multi-provider acceptance criteria have been implemented.


## Work implemented on `feature/unified-multi-provider-content`

This iteration adds a conservative foundation; it does **not** claim the full system in the brief is complete.

- `src/unified.rs` adds deterministic title normalization and cross-provider grouping. It requires an exact normalized title and matching media type, preserves season/sequel/movie/special qualifiers, checks year consistency, leaves ambiguous year buckets separate, and never collapses two distinct records from the same provider. It also groups episode records only when season/episode coordinates agree and any supplied titles are compatible.
- `src/api.rs` uses the grouping for JSON and SSE anime search responses. The legacy top-level `id` remains the primary provider’s existing API-generated opaque reference. When there are other confirmed mappings, the response adds `provider_mappings`, each with a provider and an opaque API-generated ID. The OpenAPI description documents this additive field.
- `src/catalog.rs` checks an existing episode’s title before an import upsert. If a second import supplies a clearly different non-empty title for the same canonical season/episode, the transaction returns a validation error rather than overwriting/merging it silently. Compatible title punctuation/case variation and unknown titles remain allowable.
- `src/lib.rs` exposes the new domain module. No new tables or migrations were added; the existing catalog schema remains the persistence foundation.

Example of the additive search shape when a secondary mapping exists:

```json
{
  "id": "<existing opaque primary provider ID>",
  "title": "Black Clover",
  "media_type": "series",
  "provider_mappings": [
    {"id": "<API-generated opaque secondary provider ID>", "provider": "dramachi"}
  ]
}
```

The top-level ID format and its provider meaning are unchanged. Clients must pass each API-generated mapping ID back unchanged; they must not construct provider IDs. The list contains secondary mappings; the top-level ID represents the primary mapping.

## Acceptance gaps / limitations

- The current branch still has **no AniZone provider implementation**. AniZone-specific examples cannot be exercised from this checkout without adding and validating a new provider client.
- The live HTTP anime search is still configured for FourKHDHub only. This iteration deliberately did not enable DramaChi: its current release adapter returns `codec: None`, so its candidates fail the existing positive H.264 eligibility check; BDIX and addons remain disabled by default, and MovieBox/API restrictions were preserved. Consequently, the new grouping does not yet produce live cross-provider results in the default deployment.
- Search-card mapping references are not yet consumed to aggregate cross-provider Details, Seasons, Episodes, episode Sources, audio choices, or language-aware playback fallback. There is no provider-neutral live import/orchestration worker added.
- The existing PostgreSQL catalog can store multiple provider mappings and source-scoped tracks, but import-by-name and source verification/playback remain FourKHDHub-specific. Multi-provider data must be imported against one trusted canonical identity (for example, a reliable shared MAL ID); automatic cross-provider durable identity matching is not implemented here.
- The episode collision guard fails the import transaction on conflicting titles because the existing schema uses a unique `(title, season, episode_number)` identity and has no representation for two distinct logical episodes with the same coordinate. Resolving such source data requires explicit metadata/canonical episode assignment.
- No database migration, audio discovery worker, source refresh/fallback routing change, API v2 details flow, or provider outage integration test was added. The project is **not production-ready for the full acceptance criteria** until these remaining flows and provider adapters are implemented and exercised.

## Verification performed

- `cargo fmt --all -- --check`: passed.
- `cargo check --all-targets --all-features --locked`: passed.
- `cargo test --all-features --locked`: passed (the repository’s complete checked-in suite ran; ignored live/database tests were not run).
- Focused unified matching/search mapping regressions: passed.
- `cargo clippy --all-targets --all-features --locked -- -D warnings`: failed on five lint findings in unchanged existing code paths (`src/api.rs` needless borrow at the media source handler; `src/api_native_stream.rs` too-many-arguments and identical-if-branches; `src/proxy.rs` two too-many-arguments findings). No Clippy failure pointed to the new matcher/catalog logic.
- No live provider requests, PostgreSQL integration database, APK, production service, or deployment was used.
