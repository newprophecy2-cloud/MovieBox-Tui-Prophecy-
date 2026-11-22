# Prophecy Verse V3 — Test Report

## Commands Executed & Results

1. **Formatting Check:**
   - `cargo fmt --all -- --check` → **passed (exit 0)**
2. **Targeted Suites:**
   - `cargo test --lib v3` → **42 passed, 0 failed**
   - `cargo test --lib anizone` → **10 passed, 0 failed**
   - `cargo test --lib moviebox` → **55 passed, 0 failed**
   - `cargo test --lib dramachi` → **12 passed, 0 failed**
   - `cargo test --lib fourkhdhub` → **27 passed, 0 failed**
   - `cargo test --lib providers::models` → **4 passed, 0 failed**
3. **Full Library Test Suite:**
   - `cargo test --lib` → **609 passed, 0 failed, 0 ignored** (20.35s)
4. **Binary Build:**
   - `cargo build --bin moviebox-tui` → **passed (exit 0)**

## Remaining Limitations (Test Scope)

- Unit tests use deterministic fixtures and local mock HTTP servers (`TcpListener`) for HTTP error and HTML rejection checks; live provider behavior depends on upstream CDN/API availability and is verified separately in Phase 5.
- `PV_AUDIO_LANG_ID_URL` and `GEMINI_API_KEY` are not configured in Render, so live spoken-language verification remains `"unverified"` (`verified_languages: []`).
