# Prophecy Verse V3 — Full Backend Audit Report

**Repository:** `newprophecy2-cloud/MovieBox-Tui-Prophecy-`  
**Branch:** `prophecy-backend-v3`  
**Audit Baseline Commit:** `78e407c` (`78e407c588fb2e8c0e0bec9bc5bfe2e5c678e356`)  
**Production Backend:** `https://prophecy-verse-v3-backend.onrender.com`  
**Audit Date:** 2026-11-22  

---

## 1. Repository & Architecture Verification

- **Origin:** `https://github.com/newprophecy2-cloud/MovieBox-Tui-Prophecy-.git`
- **Branch:** `prophecy-backend-v3` (clean working tree prior to audit)
- **Entrypoint:** `src/main.rs` → `moviebox_tui::api::run().await` → `src/v3/router.rs::run()`
- **Deployment:** `Dockerfile.backend` (Rust 1.90 + `ffmpeg` / `ffprobe` in `debian:bookworm-slim`, port `10000`)
- **Registered V3 Routes (7 anime routes + `/health` + `/ready`):**
  1. `GET /api/v1/anime/search?q={query}`
  2. `GET /api/v1/anime/{anime_id}`
  3. `GET /api/v1/anime/{anime_id}/seasons`
  4. `GET /api/v1/seasons/{season_id}/episodes`
  5. `GET /api/v1/episodes/{episode_id}`
  6. `GET /api/v1/episodes/{episode_id}/sources`
  7. `POST /api/v1/episodes/{episode_id}/resolve`
- **Architecture Preserved:** RAM-only `SessionStore` (`src/v3/session.rs`), no A–Z catalog, AniList not used as source of truth, all 4 providers active (`MovieBox`, `AniZone`, `DramaChi`, `FourKHDHub`).

---

## 2. Live Baseline Reproduction (`Black Clover`)

Live verification against `https://prophecy-verse-v3-backend.onrender.com` on `78e407c` (`NP-A-83FB35` / `NP-A-A47A42`):
- **Search:** Returned 1 canonical `Black Clover` series containing 7 `provider_variants`:
  - MovieBox `2316544915590759384` (`Black Clover [Hindi] S1-S2`, `hi`)
  - MovieBox `3380137095141441488` (`Black Clover S1-S2`, untagged)
  - MovieBox `9162827393101350104` (`Black Clover [English]`, `en`)
  - AniZone `ijxazsfw` (`Black Clover`), `pplul9jk` (`Black Clover Second Season`)
  - DramaChi `40867` (`Black Clover`)
  - FourKHDHub `/black-clover-series-122/` (`Black Clover`)
- **Seasons / Episodes:** `2` seasons (`S1`: 178 episodes, `S2`: 8 episodes, total `186`), `S1E1` = `"Asta and Yuno"`.
- **Reproduced Anomalies:**
  1. MovieBox English variant `9162827393101350104` appeared in Search and Details `provider_variants`, **but vanished from `matching_diagnostics` and `sources`** for S1E1 without any diagnostic entry.
  2. `available_languages` was `[]` despite declared (`hi`, `en`, `ja`) and FFprobe-observed (`hi`) tracks, because `available_languages` only aggregates `verified` spoken-language results.
  3. `excluded_sources: 11` mixed 8 `merged_duplicate_url` merges with 3 actual HTTP 403 validation failures.
  4. Subtitle metadata from AniZone/MovieBox was not surfaced on `PublicSourceItem` or `ResolveSuccessResponse`.

---

## 3. Confirmed Issues Register

### BUG-01 (HIGH) — AniZone Sub-Server Drops All Sources After `sources.first()`
- **File:** `src/providers/anizone.rs`
- **Function:** `AniZoneClient::releases_via_sub_server` (line 328)
- **Root Cause:** `let first = sources.first().ok_or(AniZoneError::NotFound)?;` only reads the first element of `data.sources`, discarding all other mirrors/qualities/languages.
- **Expected:** Enumerate every valid entry in `data.sources` into `Release` / `SourceMirror` records.

### BUG-02 (HIGH) — AniZone Sub-Server Parses JSON Before Checking HTTP Status
- **File:** `src/providers/anizone.rs`
- **Function:** `AniZoneClient::fetch_sub_server_json` (lines 145–166)
- **Root Cause:** `response.json().await?` executes before verifying `status.is_success()`. Non-JSON error pages (e.g., 403/502/503 HTML) fail with a raw `reqwest` JSON decode error instead of explicit HTTP status handling.
- **Expected:** Check HTTP status first, safely handle error payloads, and parse JSON only when appropriate.

### BUG-03 (HIGH) — Subtitle Metadata Lost Between Provider & V3 API
- **File:** `src/providers/anizone.rs` (lines 349–379, 794–799), `src/v3/models.rs`, `src/v3/audio_sources.rs` (lines 953–960)
- **Root Cause:** AniZone stores `X-AniZone-Subtitle-Url` and `X-AniZone-Subtitle-Count` in `mirror.headers`, which `audio_sources.rs` strips via its header allowlist (`referer`, `user-agent`, `origin`). `Release`, `ProviderSourceCandidate`, `PublicSourceItem`, and `ResolveSuccessResponse` have no structured `subtitles` field.
- **Expected:** Represent subtitles cleanly in a dedicated `subtitles` field without forwarding internal metadata as HTTP headers.

### BUG-04 (HIGH) — MovieBox English Variant (`9162827393101350104`) Vanishes Silently from Episode Matching Diagnostics
- **File:** `src/v3/matching.rs` (`build_canonical_structure`, lines 432–525) & `src/v3/router.rs` (`build_anime_provider_coverage`, lines 524–552)
- **Root Cause:** When a `ProviderAnimeRef` in `anime.provider_records` has `details == None` (details failed) or its `seasons` do not contain `(season_num, ep_num)`, `build_canonical_structure` skips it without adding a `MissingFromProvider` / `Unmatched` diagnostic entry. Additionally, `build_anime_provider_coverage` only inspects `.find(|r| r.provider_id == entry.provider_id)` (the first variant).
- **Expected:** Every `ProviderAnimeRef` in `CanonicalAnime.provider_records` must be accounted for in `matching_diagnostics` for the episode, even when its details failed or it lacks the episode.

### BUG-05 (HIGH) — MovieBox `get_details` / `moviebox_details_json_to_media_details` Nested `subject` & Top-Level `seasons` Mismatch
- **File:** `src/providers/moviebox/mod.rs` (`get_details`, lines 269–285) & `src/providers/moviebox/adapt.rs` (`moviebox_details_json_to_media_details`, lines 351–354)
- **Root Cause:** `get_details` checks `details.get("subjectType")` at top-level even when `subjectType` is nested under `details["subject"]["subjectType"]`, and inserts `season-info` at top-level `details["seasons"]`, whereas `moviebox_details_json_to_media_details` only reads `subject.get("seasons")`.
- **Expected:** Check both top-level and nested `subject` for `subjectType`/`stype` and `seasons`.

### BUG-06 (CRITICAL) — SSRF / Redirect Validation Bypass in `validate_stream_url_and_inspect`
- **File:** `src/v3/audio_sources.rs` (`validate_stream_url_and_inspect`, line 373)
- **Root Cause:** Builds a fresh `reqwest::Client::builder()` per request instead of `crate::net::http_client_builder_base()`, bypassing `FallbackResolver` and `Policy::custom` SSRF redirect validation on HTTP 301/302 redirects (and losing connection pooling).
- **Expected:** Use a shared `LazyLock<reqwest::Client>` built from `crate::net::http_client_builder_base()` with bounded timeout.

### BUG-07 (MEDIUM) — Duplicate Merges Counted as Excluded Sources
- **File:** `src/v3/audio_sources.rs` (`collect_episode_sources`) & `src/v3/models.rs` (`PipelineStats`, `EpisodeSourcesResponse`, `ProviderCoverageDetail`)
- **Root Cause:** `merged_duplicate_url` increments `excluded_source_count` and `excluded_sources`, conflating merged duplicates with inaccessible/failed sources.
- **Expected:** Track `merged_duplicates_count` and `failed_validation_count` separately from `excluded_sources` while preserving backward-compatible fields.

### BUG-08 (MEDIUM) — Language Evidence Levels Not Exposed Separately on `AudioAvailability` / `EpisodeSourcesResponse`
- **File:** `src/v3/audio_sources.rs` (`compute_audio_availability`) & `src/v3/models.rs`
- **Root Cause:** `available_languages` only includes `verified` spoken languages, returning `[]` even when `declared_languages`, `manifest_languages`, and `observed_track_languages` exist.
- **Expected:** Keep `available_languages` backward-compatible (verified languages) and expose `declared_languages`, `manifest_languages`, `observed_track_languages`, `verified_languages`, and `language_verification_status` explicitly.

### BUG-09 (MEDIUM) — AniZone Sub-Server Language Parsing
- **File:** `src/providers/anizone.rs` (`releases_via_sub_server`)
- **Root Cause:** Hardcodes `Some("English")` without checking if an individual source entry provides an explicit language field, while still needing `English` (`en`) as the `provider_declared` default when omitted.
- **Expected:** Parse source-level language metadata if present, falling back to `Some("English")` (`provider_declared`) so English is never lost.

---

## 4. Not a Bug / Confirmed Working Baseline

- **FourKHDHub quality & grouping (`src/providers/fourkhdhub/parser.rs`):** `1080p`/`720p` no longer misclassified as `2160p`; distinct `(filename, quality, codec, language)` variants stay separate; signed R2 URLs preserved byte-for-byte.
- **DramaChi `S01E001` matching (`src/v3/matching.rs`):** `is_generic_episode_title` + `evaluate_episode_match` correctly matches `"Black Clover S01E001"` to `"Asta and Yuno"` with score `0.90`.
- **Session isolation & cleanup (`src/v3/session.rs`):** Random 192-bit session tokens, idle/max TTL enforcement, bounded `max_active_sessions` eviction, and periodic cleanup task verified.
- **AniZone 403 / MovieBox 403 handling:** Access restrictions are respected without bypass and reported in `excluded_details` with `http_status_code: 403` and `playability_status: "access_forbidden"`.
