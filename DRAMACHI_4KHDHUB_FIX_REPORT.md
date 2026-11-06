# Dramachi and 4KHDHub Backend Fix Report

**Branch:** `feature/complete-anime-multiprovider-search-playback`  
**Implementation commit:** `156b7845bbeea47297dbd1955ca344208838e9b2`

## Summary

This change reduces Dramachi codec-verification startup work and makes the 4KHDHub resolver try multiple trusted links instead of selecting only one. It preserves the existing backend playback flow, codec fail-closed behavior, and URL/public-address validation. No APK, Firebase, unrelated provider, or production deployment changes were made.

## Dramachi: diagnosis and change

The existing verifier requested an initial byte range capped at 32 MiB and passed that sample to FFprobe. Its `MediaUnavailable` result is not a unique diagnosis: the current implementation can return that error for request transport failure, a non-success HTTP response, a body-stream read failure, or an unexpected `Content-Range`. Consequently, the repository code identifies the expensive verification path, but does not by itself establish which upstream condition caused the reported production failure. No reproducible live Dramachi URL or request trace was available to distinguish those possibilities.

The updated verifier uses a 1 MiB initial range for Dramachi, then retries with a bounded 4 MiB sample only if FFprobe cannot inspect the first sample. This reduces routine transfer and limits fallback work; metadata beyond the bounded sample can still remain uninspectable. The existing 4KHDHub sample limit is unchanged. Unsupported and unknown codecs are not accepted as compatible.

## 4KHDHub: diagnosis and change

HubCloud resolution previously selected only the first matching download/resolver link on a drive page. If that link was expired, unavailable, or otherwise unusable, other links on the same page were not considered. Candidate URLs that were extracted could also fail preflight; the existing resolver logs only safe host, error class, and HTTP status rather than provider URLs or response contents. The specific Tower of God S1E7 mirror failures were not reproduced against the live provider, so no particular mirror failure reason is claimed here.

The resolver now collects multiple HTTPS links from the existing resolver-link selector, limits the number attempted, rejects untrusted hosts, and continues after individual resolver fetch failures. The mirror-resolution and preflight timeouts were increased modestly. Media preflight and existing playback-source selection remain in place, including public URL validation and SSRF protections.

## Validation

- `cargo fmt --all -- --check` — passed.
- `cargo test --all-targets --locked` — passed: 602 library tests and all enabled integration tests passed. Provider live tests are ignored by default.
- `cargo build --release --locked` — passed.
- An explicit attempt to run the ignored live 4KHDHub integration test failed before resolving playback: the current provider listing did not contain the test's expected Black Clover episode 67. Thus the live test did not verify Tower of God S1E7 or establish a currently working mirror.

## Changed implementation files

- `src/codec_verification.rs`
- `src/providers/fourkhdhub/client.rs`
- `src/providers/fourkhdhub/hubcloud.rs`

The implementation was pushed in commit `156b7845bbeea47297dbd1955ca344208838e9b2`. This report is a separate documentation update.
