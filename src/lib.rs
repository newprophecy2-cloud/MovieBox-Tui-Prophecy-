#![recursion_limit = "512"]

pub mod api;
pub(crate) mod api_native_stream;
pub mod cache;
pub mod catalog;
pub mod codec_verification;
pub mod config;
pub mod download;
pub mod favorites;
pub mod history;
pub mod hls_chunk_cache;
pub mod language;
pub mod logging;
pub mod models;
pub mod net;
pub(crate) mod playback_planner;
pub mod player;
pub(crate) mod progressive_remux;
pub mod providers;
pub mod proxy;
#[cfg(test)]
#[allow(
    dead_code,
    reason = "the legacy disk-cache module is compiled only for its regression tests"
)]
mod remux_cache;
pub mod service;
pub mod tui;
pub mod updater;
