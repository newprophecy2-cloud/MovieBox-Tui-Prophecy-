//! Phase 3.1 live verification harness.
//!
//! This test intentionally excludes MovieBox. Run only with authorized access:
//! cargo test --locked --test phase31_provider_verification -- --ignored --nocapture

use moviebox_tui::providers::Provider as ProviderTrait;
use moviebox_tui::providers::addons::AddonClient;
use moviebox_tui::providers::dramachi::DramachiClient;
use moviebox_tui::providers::fourkhdhub::FourKHdHubClient;
use moviebox_tui::providers::models::MediaType;

const TARGETS: &[(&str, usize, usize)] = &[
    ("Naruto Shippuden", 1, 1),
    ("Naruto Shippuden", 1, 32),
    ("One Piece", 1, 1),
    ("Barakamon", 1, 1),
    ("Kino's Journey (2003)", 1, 1),
];

fn normalized(text: &str) -> String {
    text.to_ascii_lowercase()
        .replace([':', '-', '(', ')', '\'', ','], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn title_matches(result: &str, target: &str) -> bool {
    let result = normalized(result);
    let target = normalized(target);
    if target.contains("kino") && target.contains("2003") {
        return result.contains("kino")
            && (result.contains("2003") || result.contains("beautiful world"));
    }
    result.contains(&target) || target.split_whitespace().all(|part| result.contains(part))
}

fn release_host(url: Option<&str>) -> String {
    url.and_then(|raw| reqwest::Url::parse(raw).ok())
        .and_then(|url| url.host_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "none".to_string())
}

async fn safe_probe(url: &str) -> String {
    let client = match moviebox_tui::net::http_client_builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(client) => client,
        Err(_) => return "client_error".to_string(),
    };
    match client
        .get(url)
        .header(reqwest::header::RANGE, "bytes=0-8191")
        .send()
        .await
    {
        Ok(response) => format!(
            "status={} content_type={} final_host={}",
            response.status().as_u16(),
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("unknown"),
            response.url().host_str().unwrap_or("unknown")
        ),
        Err(error) if error.is_timeout() => "timeout".to_string(),
        Err(_) => "request_error".to_string(),
    }
}

#[tokio::test]
#[ignore = "authorized live verification; excludes MovieBox"]
async fn exact_matrix_fourkhdhub() {
    let client = match FourKHdHubClient::new() {
        Ok(client) => client,
        Err(error) => {
            println!("FOURK PROVIDER_UNREACHABLE init={error}");
            return;
        }
    };
    for &(target, season, episode) in TARGETS {
        let search = match client.search(target).await {
            Ok(results) => results,
            Err(error) => {
                println!("FOURK {target} PROVIDER_UNREACHABLE search={error}");
                continue;
            }
        };
        let Some(item) = search
            .iter()
            .find(|item| title_matches(&item.title, target))
        else {
            let candidates = search
                .iter()
                .take(8)
                .map(|item| item.title.as_str())
                .collect::<Vec<_>>()
                .join(" | ");
            println!(
                "FOURK {target} NOT_AVAILABLE catalog_results={} candidates={candidates}",
                search.len()
            );
            continue;
        };
        let details = match client.details(&item.id.value).await {
            Ok(details) => details,
            Err(error) => {
                println!("FOURK {target} PARSER_ERROR details={error}");
                continue;
            }
        };
        if details.media_type == MediaType::Movie && (season > 0 || episode > 0) {
            println!("FOURK {target} PARSER_ERROR type_mismatch=movie_for_episode_target");
            continue;
        }
        let requested_episode = if details.media_type == MediaType::Movie {
            (0, 0)
        } else {
            (season, episode)
        };
        let releases = match client
            .releases(&item.id.value, requested_episode.0, requested_episode.1)
            .await
        {
            Ok(releases) => releases,
            Err(error) => {
                println!("FOURK {target} PARSER_ERROR releases={error}");
                continue;
            }
        };
        let Some(release) = releases.first() else {
            println!("FOURK {target} RESOLUTION_FAILED no_release");
            continue;
        };
        match client
            .resolve_release(
                release,
                moviebox_tui::providers::models::ResolutionIntent::Playback,
            )
            .await
        {
            Ok(playback) => println!(
                "FOURK {target} WORKING media={:?} season={} episode={} host={}",
                details.media_type,
                requested_episode.0,
                requested_episode.1,
                release_host(Some(&playback.url))
            ),
            Err(error) => println!("FOURK {target} RESOLUTION_FAILED error={error}"),
        }
    }
}

#[tokio::test]
#[ignore = "authorized live verification; excludes MovieBox"]
async fn exact_matrix_dramachi() {
    let client = DramachiClient::new();
    for &(target, season, episode) in TARGETS {
        let search = match client.search(target, 1).await {
            Ok(results) => results,
            Err(error) => {
                println!("DRAMACHI {target} PROVIDER_UNREACHABLE search={error}");
                continue;
            }
        };
        let Some(item) = search
            .iter()
            .find(|item| title_matches(&item.title, target))
        else {
            let candidates = search
                .iter()
                .take(8)
                .map(|item| item.title.as_str())
                .collect::<Vec<_>>()
                .join(" | ");
            println!(
                "DRAMACHI {target} NOT_AVAILABLE catalog_results={} candidates={candidates}",
                search.len()
            );
            continue;
        };
        let details = match client.details(&item.id.value).await {
            Ok(details) => details,
            Err(error) => {
                println!("DRAMACHI {target} PARSER_ERROR details={error}");
                continue;
            }
        };
        let releases = match client
            .episode_streams(&item.id.value, season, episode)
            .await
        {
            Ok(releases) => releases,
            Err(error) => {
                println!("DRAMACHI {target} RESOLUTION_FAILED streams={error}");
                continue;
            }
        };
        let Some(release) = releases.first() else {
            println!("DRAMACHI {target} RESOLUTION_FAILED no_release");
            continue;
        };
        let probe = match release.direct_url() {
            Some(url) => safe_probe(url).await,
            None => "no_direct_url".to_string(),
        };
        println!(
            "DRAMACHI {target} WORKING media={:?} season={} episode={} quality={:?} codec={:?} language={:?} host={} probe={probe}",
            details.media_type,
            season,
            episode,
            release.quality,
            release.codec,
            release.language,
            release_host(release.direct_url())
        );
    }
}

#[tokio::test]
#[ignore = "authorized live verification; configured addons only; excludes MovieBox"]
async fn exact_matrix_configured_addons() {
    let client = AddonClient::new();
    for &(target, season, episode) in TARGETS {
        let search = match client.search(target, 1).await {
            Ok(results) => results,
            Err(error) => {
                println!("ADDONS {target} PROVIDER_UNREACHABLE_OR_UNCONFIGURED search={error}");
                continue;
            }
        };
        let Some(item) = search
            .iter()
            .find(|item| title_matches(&item.title, target))
        else {
            let candidates = search
                .iter()
                .take(8)
                .map(|item| item.title.as_str())
                .collect::<Vec<_>>()
                .join(" | ");
            println!(
                "ADDONS {target} NOT_AVAILABLE catalog_results={} candidates={candidates}",
                search.len()
            );
            continue;
        };
        match client.details(&item.id.value).await {
            Ok(details) => println!(
                "ADDONS {target} DETAILS_WORKING media={:?} seasons={} episode_target_series={}",
                details.media_type,
                details.seasons.len(),
                details.media_type == MediaType::Series
            ),
            Err(error) => {
                println!("ADDONS {target} PARSER_ERROR details={error}");
                continue;
            }
        }
        let _ = (season, episode);
        println!(
            "ADDONS {target} STREAM_RESULT requires configured stream addon; catalog_id={}",
            item.id.value
        );
    }
}

#[test]
fn exact_matrix_contains_required_targets_without_moviebox() {
    assert_eq!(TARGETS.len(), 5);
    assert!(TARGETS.iter().any(|(title, _, _)| title.contains("Kino")));
    assert!(
        !TARGETS
            .iter()
            .any(|(title, _, _)| title.eq_ignore_ascii_case("MovieBox"))
    );
}
