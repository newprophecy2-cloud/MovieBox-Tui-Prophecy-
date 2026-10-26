use moviebox_tui::catalog::{
    AnimeImportEpisodeRef, CatalogStore, EpisodeInput, ImportTitle, ProviderEpisodeInput,
    ProviderTitleInput, SeasonInput,
};
use serde_json::json;
use uuid::Uuid;

fn base_import(provider_title_id: &str) -> ImportTitle {
    ImportTitle {
        canonical_title: format!("Isolated import fixture {provider_title_id}"),
        content_type: "anime".into(),
        catalog_status: Some("active".into()),
        providers: vec![ProviderTitleInput {
            provider_key: "fourkhdhub".into(),
            display_name: Some("FourKHDHub".into()),
            provider_title_id: provider_title_id.into(),
            provider_title: Some("Isolated fixture".into()),
            metadata: json!({"test_fixture": true}),
            ..Default::default()
        }],
        seasons: vec![SeasonInput {
            season_number: 1,
            canonical_label: Some("Season 1".into()),
            episode_count_reported: Some(1000),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn staged_episode(ordinal: i64) -> AnimeImportEpisodeRef {
    let episode_number = ordinal as i32 + 1;
    AnimeImportEpisodeRef {
        ordinal,
        season_index: 0,
        episode_index: episode_number - 1,
        season_number: 1,
        provider_season: 1,
        episode_number,
        provider_episode_id: format!("fixture:{episode_number}"),
        episode_metadata: json!({
            "episode_number": episode_number,
            "episode_title": format!("Fixture episode {episode_number}"),
            "provider_episode_ids": []
        }),
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn large_anime_jobs_stage_all_records_resume_and_upsert_idempotently() {
    let store = CatalogStore::connect_from_env()
        .await
        .expect("connect to isolated PostgreSQL")
        .expect("DATABASE_URL is required");

    let mut recoverable_job = String::new();
    for count in [201_i64, 500, 1000] {
        let provider_title_id = format!("fixture-{}", Uuid::new_v4());
        let base = base_import(&provider_title_id);
        let (job_id, created) = store
            .create_anime_import_job(
                "synthetic isolated test",
                "fourkhdhub",
                &provider_title_id,
                &base,
                25,
                count,
            )
            .await
            .expect("create job");
        assert!(created, "fixture job should be newly created");
        for start in (0..count).step_by(25) {
            let batch = (start..(start + 25).min(count))
                .map(staged_episode)
                .collect::<Vec<_>>();
            store
                .append_anime_import_episode_batch(&job_id, &batch)
                .await
                .expect("stage bounded episode batch");
        }
        assert_eq!(
            store.activate_anime_import_job(&job_id).await.unwrap(),
            count
        );
        let job = store.get_job(&job_id).await.unwrap().unwrap();
        assert_eq!(job.total_episodes, Some(count));
        assert_eq!(job.pending_episodes, Some(count));
        if count == 201 {
            recoverable_job = job_id;
            let first = staged_episode(0);
            let mut import = base.clone();
            import.seasons[0].episodes = vec![EpisodeInput {
                episode_number: 1,
                episode_title: Some("Fixture episode 1".into()),
                ..Default::default()
            }];
            import.providers[0].episodes = vec![ProviderEpisodeInput {
                provider_key: None,
                season_number: Some(1),
                provider_episode_id: first.provider_episode_id,
                provider_episode_number: Some(1),
                ..Default::default()
            }];
            store.import_title(import.clone()).await.unwrap();
            store.import_title(import).await.unwrap();
            let title = store
                .find_title_by_provider("fourkhdhub", &provider_title_id)
                .await
                .unwrap()
                .unwrap();
            let episodes = store.list_episodes(&title.id, 1).await.unwrap();
            assert_eq!(
                episodes.len(),
                1,
                "retry must not create duplicate episodes"
            );
        }
    }

    let claimed = store.claim_anime_import_job().await.unwrap().unwrap();
    assert_eq!(claimed.id, recoverable_job);
    let batch = store
        .pending_anime_import_episodes(&claimed.id, i64::from(claimed.batch_size))
        .await
        .unwrap();
    assert_eq!(batch.len(), 25);
    for (ordinal, ..) in batch {
        store
            .complete_anime_import_episode(&claimed.id, ordinal, None)
            .await
            .unwrap();
    }
    store
        .release_anime_import_job_lease(&claimed.id)
        .await
        .unwrap();
    drop(store);

    let restarted = CatalogStore::connect_from_env()
        .await
        .unwrap()
        .expect("reconnect to disposable PostgreSQL");
    let resumed = restarted.claim_anime_import_job().await.unwrap().unwrap();
    assert_eq!(resumed.id, recoverable_job);
    let status = restarted.get_job(&resumed.id).await.unwrap().unwrap();
    assert_eq!(status.processed_episodes, Some(25));
    assert_eq!(status.pending_episodes, Some(176));
    assert_eq!(status.resumable, Some(true));
}
