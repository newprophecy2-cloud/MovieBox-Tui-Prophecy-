use moviebox_tui::providers::anizone::AniZoneClient;

#[tokio::test]
#[ignore = "live AniZone network verification"]
async fn live_anizone_search_details_and_fresh_stream() {
    let client = AniZoneClient::new();
    let mut slugs = Vec::new();
    for query in ["Naruto", "Black Clover", "One Piece"] {
        let results = client
            .search(query, 1)
            .await
            .expect("AniZone search should succeed");
        assert!(!results.is_empty(), "no AniZone results for {query}");
        slugs.push(results[0].id.value.clone());
    }

    for slug in slugs {
        let details = client
            .details(&slug)
            .await
            .expect("AniZone details should succeed");
        let episodes = details.seasons[0].episodes.len();
        assert!(episodes > 0, "AniZone returned no episodes for {slug}");
        for episode in [1, (episodes / 2).max(1), episodes]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
        {
            let releases = client
                .releases(&slug, 1, episode)
                .await
                .expect("fresh AniZone HLS extraction should succeed");
            assert_eq!(releases.len(), 1);
            assert!(
                releases[0]
                    .direct_url()
                    .is_some_and(|url| url.ends_with(".m3u8") || url.contains("m3u8"))
            );
            assert_eq!(releases[0].codec.as_deref(), Some("h264"));
        }
    }
}
