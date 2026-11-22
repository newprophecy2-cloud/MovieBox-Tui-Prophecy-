# Prophecy Verse V3 — Repair Report

## Summary of Repairs (BUG-01 through BUG-09)

1. **BUG-01 & BUG-09 — AniZone Sub-Server Multi-Source Enumeration & Language Preservation**
   - **Original behavior:** `AniZoneClient::releases_via_sub_server` only inspected `sources.first()`, dropping additional mirrors/qualities/languages.
   - **New behavior:** Iterates all entries in `data.sources`, preserving per-source `url`, `headers`, `quality`, `codec`, `label`, `resource_id`, and `language` (falling back to `Some("English")` when omitted so the English `provider_declared` declaration is never lost).
   - **Root cause:** Single-element `.first()` extraction.
   - **Changed files:** `src/providers/anizone.rs`
   - **Compatibility:** Additive; preserves `Release` schema.
   - **Regression test:** `providers::anizone::tests::sub_server_enumerates_multiple_sources_and_preserves_english_and_http_errors`

2. **BUG-02 — AniZone Sub-Server HTTP Status Handling**
   - **Original behavior:** `fetch_sub_server_json` called `response.json().await?` before checking `status.is_success()`, causing HTML error pages (403/502/503) to fail with raw JSON decode errors.
   - **New behavior:** Reads bounded response bytes (`MAX_HTML_BYTES`), checks `!status.is_success()` first, parses structured error JSON if present, and maps HTTP 404 / non-2xx statuses explicitly.
   - **Changed files:** `src/providers/anizone.rs`
   - **Regression test:** `providers::anizone::tests::sub_server_enumerates_multiple_sources_and_preserves_english_and_http_errors`

3. **BUG-03 — Structured Subtitle Pipeline**
   - **Original behavior:** AniZone `X-AniZone-Subtitle-Url` was placed in `mirror.headers`, stripped by `audio_sources.rs`, and never exposed on `PublicSourceItem` or `ResolveSuccessResponse`.
   - **New behavior:** Added `SubtitleItem` and `subtitles: Vec<SubtitleItem>` on `ProviderSourceCandidate`, `PublicSourceItem`, and `ResolveSuccessResponse`. `collect_episode_sources` extracts and SSRF-validates subtitle URLs into `subtitles` without forwarding internal `X-AniZone-*` headers.
   - **Changed files:** `src/v3/models.rs`, `src/v3/audio_sources.rs`

4. **BUG-04 — MovieBox Variant Accounting in Episode Matching & Coverage**
   - **Original behavior:** `build_canonical_structure` skipped `ProviderAnimeRef` records whose `details` failed or lacked the episode, causing variants such as MovieBox English (`9162827393101350104`) to vanish from `matching_diagnostics`. `build_anime_provider_coverage` only checked the first variant per provider.
   - **New behavior:** Explicitly records `EpisodeMappingOutcome::MissingFromProvider` with `provider_details_unavailable` or `episode_not_in_provider_variant` in `provider_mappings` / `matching_diagnostics`, and aggregates `provider_coverage` across all variants (`success` / `partial` / `failed`).
   - **Changed files:** `src/v3/matching.rs`, `src/v3/router.rs`
   - **Regression test:** `v3::matching::tests::test_missing_provider_variant_recorded_in_episode_mappings`

5. **BUG-05 — MovieBox Nested Subject & Top-Level Seasons Parsing**
   - **Original behavior:** `get_details` checked only top-level `details["subjectType"]`, and `moviebox_details_json_to_media_details` checked only `subject["seasons"]`.
   - **New behavior:** Checks both top-level and nested `subject` for `subjectType`/`stype` and `seasons`.
   - **Changed files:** `src/providers/moviebox/mod.rs`, `src/providers/moviebox/adapt.rs`
   - **Regression test:** `providers::moviebox::tests::test_details_parses_top_level_seasons_with_nested_subject`

6. **BUG-06 — SSRF Redirect Validation in Stream Validator**
   - **Original behavior:** `validate_stream_url_and_inspect` constructed a bare `reqwest::Client::builder()` per request without `FallbackResolver` or redirect policy.
   - **New behavior:** Uses a shared `LazyLock<reqwest::Client>` built from `crate::net::http_client_builder_base()` so every redirect hop is validated against private/loopback/link-local destinations.
   - **Changed files:** `src/v3/audio_sources.rs`
   - **Regression test:** `v3::audio_sources::tests::test_redirect_to_loopback_blocked_in_production_client`

7. **BUG-07 — Duplicate Merge vs Exclusion Statistics**
   - **Original behavior:** `merged_duplicate_url` incremented `excluded_source_count` and `excluded_sources`.
   - **New behavior:** Tracks `merged_duplicates_count` and `failed_validation_count` separately on `PipelineStats`, `ProviderCoverageDetail`, and `EpisodeSourcesResponse`, while excluding `merged_duplicate_url` from `excluded_sources`.
   - **Changed files:** `src/v3/models.rs`, `src/v3/audio_sources.rs`, `src/v3/router.rs`

8. **BUG-08 — Separated Audio Language Evidence Levels**
   - **Original behavior:** `available_languages` only reflected independently `verified` spoken languages (`[]` when no speech ID model is configured).
   - **New behavior:** Preserves `available_languages` for backward compatibility and exposes `declared_languages`, `manifest_languages`, `observed_track_languages`, `verified_languages`, and `language_verification_status` on `AudioAvailability` and `EpisodeSourcesResponse`.
   - **Changed files:** `src/v3/models.rs`, `src/v3/audio_sources.rs`, `src/v3/router.rs`
   - **Regression test:** `v3::audio_sources::tests::test_audio_availability_separates_declared_observed_and_verified`
