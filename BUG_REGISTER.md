# Prophecy Verse V3 — Confirmed Bug Register

| ID | Provider / Module | Severity | File | Function | Status |
|---|---|---|---|---|---|
| BUG-01 | AniZone | HIGH | `src/providers/anizone.rs` | `releases_via_sub_server` | Tested |
| BUG-02 | AniZone | HIGH | `src/providers/anizone.rs` | `fetch_sub_server_json` | Tested |
| BUG-03 | AniZone / V3 Subtitles | HIGH | `src/providers/anizone.rs`, `src/v3/models.rs`, `src/v3/audio_sources.rs` | `releases_via_sub_server`, `collect_episode_sources` | Tested |
| BUG-04 | MovieBox / V3 Matching | HIGH | `src/v3/matching.rs`, `src/v3/router.rs` | `build_canonical_structure`, `build_anime_provider_coverage` | Tested |
| BUG-05 | MovieBox | HIGH | `src/providers/moviebox/mod.rs`, `src/providers/moviebox/adapt.rs` | `get_details`, `moviebox_details_json_to_media_details` | Tested |
| BUG-06 | V3 Security / Validation | CRITICAL | `src/v3/audio_sources.rs` | `validate_stream_url_and_inspect` | Tested |
| BUG-07 | V3 Deduplication / Stats | MEDIUM | `src/v3/audio_sources.rs`, `src/v3/models.rs` | `collect_episode_sources` | Tested |
| BUG-08 | V3 Audio Language Model | MEDIUM | `src/v3/audio_sources.rs`, `src/v3/models.rs` | `compute_audio_availability` | Tested |
| BUG-09 | AniZone | MEDIUM | `src/providers/anizone.rs` | `releases_via_sub_server` | Tested |
