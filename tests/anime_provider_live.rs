use moviebox_tui::providers::{
    ReleaseProvider, anizone::AniZoneClient, fourkhdhub::FourKHdHubClient, models::ResolutionIntent,
};
use std::time::Duration;

struct AcceptanceLogger;

static ACCEPTANCE_LOGGER: AcceptanceLogger = AcceptanceLogger;
static ACCEPTANCE_LOGGER_INIT: std::sync::Once = std::sync::Once::new();

impl log::Log for AcceptanceLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info
    }

    fn log(&self, record: &log::Record<'_>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let message = record.args().to_string();
        if message.contains("direct_play_timing")
            || message.contains("direct_play_verification")
            || message.contains("dramachi_request")
            || message.contains("4khdhub_candidate")
        {
            println!("RUNTIME_STAGE {message}");
        }
    }

    fn flush(&self) {}
}

fn init_acceptance_logging() {
    ACCEPTANCE_LOGGER_INIT.call_once(|| {
        let _ = log::set_logger(&ACCEPTANCE_LOGGER);
        log::set_max_level(log::LevelFilter::Info);
    });
}

const TARGETS: [(&str, &str, usize); 3] = [
    ("Black Clover", "Black Clover", 67),
    ("Hunter x Hunter", "Hunter x Hunter", 135),
    ("Naruto Shippuden", "Naruto Shippuden", 67),
];

#[derive(Debug)]
struct RangeEvidence {
    status: u16,
    content_range: Option<String>,
    content_length: Option<u64>,
    body: Vec<u8>,
}

async fn fetch_live_range(
    url: &str,
    headers: &[(String, String)],
    requested_range: &str,
) -> RangeEvidence {
    let validated = moviebox_tui::net::validate_public_url(url, false)
        .await
        .expect("resolved source must remain a public HTTPS URL");
    let client = moviebox_tui::net::http_client_builder_base()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("safe HTTP client");
    let mut request = client
        .get(validated)
        .header(reqwest::header::RANGE, requested_range);
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("referer") || name.eq_ignore_ascii_case("user-agent") {
            request = request.header(name, value);
        }
    }
    let mut response = request.send().await.expect("upstream range request");
    let status = response.status().as_u16();
    let content_range = response
        .headers()
        .get(reqwest::header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let content_length = response.content_length();
    let limit = requested_range
        .split_once('=')
        .and_then(|(_, range)| range.split_once('-'))
        .and_then(|(start, end)| {
            Some(
                end.parse::<usize>()
                    .ok()?
                    .checked_sub(start.parse().ok()?)?
                    + 1,
            )
        })
        .unwrap_or(2048);
    let mut body = Vec::with_capacity(limit);
    while let Some(chunk) = response.chunk().await.expect("read range response") {
        let remaining = limit.saturating_sub(body.len());
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if body.len() >= limit {
            break;
        }
    }
    RangeEvidence {
        status,
        content_range,
        content_length,
        body,
    }
}

fn assert_live_partial_range(evidence: &RangeEvidence, start: usize, end: usize) {
    assert_eq!(evidence.status, 206, "range response: {evidence:?}");
    let content_range = evidence
        .content_range
        .as_deref()
        .expect("206 response must carry Content-Range");
    let (offsets, total) = content_range
        .strip_prefix("bytes ")
        .and_then(|value| value.split_once('/'))
        .expect("valid Content-Range syntax");
    assert_eq!(offsets, format!("{start}-{end}"));
    assert!(total.parse::<u64>().is_ok_and(|total| total > end as u64));
    let expected_length = (end - start + 1) as u64;
    assert_eq!(evidence.content_length, Some(expected_length));
    assert_eq!(evidence.body.len() as u64, expected_length);
}

async fn verify_live_byte_ranges(url: &str, headers: &[(String, String)], provider: &str) {
    let zero = fetch_live_range(url, headers, "bytes=0-1023").await;
    let nonzero = fetch_live_range(url, headers, "bytes=1024-2047").await;
    let combined = fetch_live_range(url, headers, "bytes=0-2047").await;
    println!(
        "RANGE_ACCEPTANCE provider={provider} requests=[bytes=0-1023,bytes=1024-2047,bytes=0-2047] status=[{},{},{}] content_range={:?} content_length={:?}",
        zero.status,
        nonzero.status,
        combined.status,
        [
            zero.content_range.as_deref(),
            nonzero.content_range.as_deref(),
            combined.content_range.as_deref()
        ],
        [
            zero.content_length,
            nonzero.content_length,
            combined.content_length
        ]
    );
    if [zero.status, nonzero.status, combined.status]
        .iter()
        .all(|status| *status == 200)
    {
        println!(
            "RANGE_LIMITATION provider={provider} upstream returned HTTP 200 for all Range requests; seeking is not supported by this source."
        );
        return;
    }
    assert_live_partial_range(&zero, 0, 1023);
    assert_live_partial_range(&nonzero, 1024, 2047);
    assert_live_partial_range(&combined, 0, 2047);
    assert_eq!(&zero.body[..], &combined.body[..1024]);
    assert_eq!(&nonzero.body[..], &combined.body[1024..2048]);
}

#[tokio::test]
#[ignore = "live provider verification against the current AniZone site"]
async fn live_anizone_requested_titles_and_episodes_resolve_hls() {
    let client = AniZoneClient::new();
    for (query, expected_title, episode_number) in TARGETS {
        let search = client
            .search(query, 1)
            .await
            .unwrap_or_else(|error| panic!("AniZone search {query:?}: {error}"));
        let item = search
            .iter()
            .find(|item| {
                item.title
                    .to_ascii_lowercase()
                    .contains(&expected_title.to_ascii_lowercase())
            })
            .unwrap_or_else(|| panic!("AniZone did not return {expected_title}"));
        let details = client
            .details(&item.id.value)
            .await
            .unwrap_or_else(|error| panic!("AniZone details for {}: {error}", item.title));
        assert!(
            details
                .seasons
                .iter()
                .flat_map(|season| &season.episodes)
                .any(|episode| episode.number == episode_number),
            "AniZone episode {episode_number} missing for {}",
            item.title
        );
        let releases = client
            .episode_streams(&item.id.value, 1, episode_number)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "AniZone release for {} E{episode_number}: {error}",
                    item.title
                )
            });
        let playback = client
            .resolve_release(&releases[0])
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "AniZone playback for {} E{episode_number}: {error}",
                    item.title
                )
            });
        assert!(playback.url.starts_with("https://") && playback.url.contains("m3u8"));
    }
}

#[tokio::test]
#[ignore = "live provider verification against current 4KHDHub listing and mirror endpoints"]
async fn live_4khdhub_requested_titles_map_and_resolve() {
    let client = FourKHdHubClient::new().expect("4KHDHub client");
    for (query, expected_title, episode_number) in TARGETS {
        let search = client
            .search(query)
            .await
            .unwrap_or_else(|error| panic!("4KHDHub search {query:?}: {error}"));
        let item = search
            .iter()
            .find(|item| {
                item.title
                    .to_ascii_lowercase()
                    .contains(&expected_title.to_ascii_lowercase())
            })
            .unwrap_or_else(|| panic!("4KHDHub did not return {expected_title}"));
        let details = client
            .details(&item.id.value)
            .await
            .unwrap_or_else(|error| panic!("4KHDHub details for {}: {error}", item.title));
        let mapped = details
            .seasons
            .iter()
            .flat_map(|season| &season.episodes)
            .find(|episode| episode.number == episode_number)
            .unwrap_or_else(|| {
                panic!(
                    "4KHDHub episode {episode_number} missing for {}",
                    item.title
                )
            });
        let releases = client
            .releases(&item.id.value, mapped.season, mapped.number)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "4KHDHub release for {} S{}E{}: {error}",
                    item.title, mapped.season, mapped.number
                )
            });
        assert!(
            !releases.is_empty(),
            "4KHDHub has no release for {} E{episode_number}",
            item.title
        );
        let playback = client
            .resolve_release(&releases[0], ResolutionIntent::Playback)
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "4KHDHub playback for {} E{episode_number}: {error}",
                    item.title
                )
            });
        assert!(playback.url.starts_with("https://"));
    }
}

#[tokio::test]
#[ignore = "live upstream Range acceptance; records actual status and headers for two providers"]
async fn live_dramachi_upstream_byte_ranges() {
    init_acceptance_logging();
    let client = moviebox_tui::providers::dramachi::DramachiClient::new();
    let results = client
        .search("Squid Game", 1)
        .await
        .expect("Dramachi search");
    let item = results.first().expect("Dramachi search result");
    let source_started = std::time::Instant::now();
    let releases = client
        .episode_streams(&item.id.value, 1, 1)
        .await
        .expect("Dramachi episode source");
    println!(
        "DRAMACHI_ACCEPTANCE stage=episode_source_resolution elapsed_ms={}",
        source_started.elapsed().as_millis()
    );
    let release = releases.first().expect("Dramachi episode release");
    let url = release.direct_url().expect("Dramachi direct media URL");
    let headers = release
        .mirrors
        .first()
        .map(|mirror| mirror.headers.as_slice())
        .unwrap_or_default();
    let mut playback = moviebox_tui::providers::models::PlaybackSource::bare(
        moviebox_tui::providers::models::ProviderKind::Dramachi,
        url,
        None,
    );
    playback.headers = headers.to_vec();
    let codec_started = std::time::Instant::now();
    let codec_result = moviebox_tui::codec_verification::verify_playback_source(&playback).await;
    println!(
        "DRAMACHI_ACCEPTANCE stage=codec_verification elapsed_ms={} result={codec_result:?}",
        codec_started.elapsed().as_millis()
    );
    match codec_result {
        Ok(moviebox_tui::codec_verification::CodecProbeResult::Allowed(codec)) => {
            assert!(codec.eq_ignore_ascii_case("h264"));
            println!("DRAMACHI_ACCEPTANCE direct_play_policy=allowed-h264");
        }
        Ok(moviebox_tui::codec_verification::CodecProbeResult::BlockedHevc(codec)) => {
            println!("DRAMACHI_ACCEPTANCE direct_play_policy=blocked-hevc codec={codec}");
        }
        Ok(moviebox_tui::codec_verification::CodecProbeResult::Unsupported(codec)) => {
            println!("DRAMACHI_ACCEPTANCE direct_play_policy=blocked-unknown-codec codec={codec}");
        }
        Ok(moviebox_tui::codec_verification::CodecProbeResult::NoVideoStream) => {
            println!("DRAMACHI_ACCEPTANCE direct_play_policy=blocked-no-video-stream");
        }
        Err(error) => panic!("live source could not be probed safely: {error:?}"),
    }
    verify_live_byte_ranges(url, headers, "Dramachi").await;
}

#[tokio::test]
#[ignore = "live upstream Range acceptance; records actual status and headers for two providers"]
async fn live_4khdhub_upstream_byte_ranges() {
    init_acceptance_logging();
    let client = FourKHdHubClient::new().expect("4KHDHub client");
    let results = client
        .search("The Iceblade Sorcerer Shall Rule the World")
        .await
        .expect("4KHDHub search");
    let item = results
        .iter()
        .find(|item| {
            item.title
                .to_ascii_lowercase()
                .contains("iceblade sorcerer")
        })
        .expect("4KHDHub Iceblade listing");
    let details = client
        .details(&item.id.value)
        .await
        .expect("4KHDHub details");
    let episode = details
        .seasons
        .iter()
        .flat_map(|season| &season.episodes)
        .find(|episode| episode.season == 1 && episode.number == 1)
        .expect("4KHDHub S01E01");
    let releases = client
        .releases(&item.id.value, episode.season, episode.number)
        .await
        .expect("4KHDHub release list");
    let release = releases.first().expect("4KHDHub release");
    let playback = client
        .resolve_release(release, ResolutionIntent::Playback)
        .await
        .expect("4KHDHub resolved playback");
    verify_live_byte_ranges(&playback.url, &playback.headers, "4KHDHub").await;
}
