use moviebox_tui::providers::{
    ReleaseProvider, anizone::AniZoneClient, fourkhdhub::FourKHdHubClient, models::ResolutionIntent,
};

const TARGETS: [(&str, &str, usize); 3] = [
    ("Black Clover", "Black Clover", 67),
    ("Hunter x Hunter", "Hunter x Hunter", 135),
    ("Naruto Shippuden", "Naruto Shippuden", 67),
];

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
