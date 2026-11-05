use moviebox_tui::catalog::{
    CatalogStore, TitleUpsert, UnifiedAnimeInput, UnifiedAnimeProviderTitleInput,
};
use serde_json::json;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

async fn connect_raw() -> Client {
    let url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must point to disposable PostgreSQL");
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        connection
            .await
            .expect("PostgreSQL connection remains healthy");
    });
    client
}

async fn fresh_store() -> CatalogStore {
    let admin = connect_raw().await;
    admin
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .expect("reset disposable public schema");
    drop(admin);
    CatalogStore::connect_from_env()
        .await
        .expect("apply all catalog migrations")
        .expect("DATABASE_URL is required")
}

fn unified_input(provider_id: &str, title: &str) -> UnifiedAnimeInput {
    UnifiedAnimeInput {
        canonical_title: title.into(),
        content_type: "anime".into(),
        release_year: Some(2024),
        provider_titles: vec![UnifiedAnimeProviderTitleInput {
            provider_key: "fixture_provider".into(),
            display_name: Some("Fixture Provider".into()),
            provider_title_id: provider_id.into(),
            provider_title: Some(title.into()),
            metadata: json!({"fixture": true}),
            last_successful_resolution: true,
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn phase1_postgres_persistence_and_alias_batch_acceptance() {
    let store = fresh_store().await;
    let title = store
        .upsert(TitleUpsert {
            mal_id: None,
            canonical_title: "Fixture Canonical Title".into(),
            content_type: "anime".into(),
            synopsis: Some("fixture synopsis".into()),
            release_year: Some(2024),
            poster_url: None,
            catalog_status: Some("active".into()),
        })
        .await
        .expect("insert canonical title")
        .0;
    let unverified_title = store
        .upsert(TitleUpsert {
            mal_id: None,
            canonical_title: "Fixture Unverified Title".into(),
            content_type: "anime".into(),
            synopsis: None,
            release_year: Some(2024),
            poster_url: None,
            catalog_status: Some("active".into()),
        })
        .await
        .expect("insert unverified canonical title")
        .0;

    let raw = connect_raw().await;
    let provider_one: Uuid = raw
        .query_one(
            "INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,true) RETURNING id",
            &[&"fixture_one", &"Fixture One"],
        )
        .await
        .expect("insert provider one")
        .get(0);
    let provider_two: Uuid = raw
        .query_one(
            "INSERT INTO provider_registry(provider_key,display_name,enabled) VALUES($1,$2,true) RETURNING id",
            &[&"fixture_two", &"Fixture Two"],
        )
        .await
        .expect("insert provider two")
        .get(0);
    let _provider_title_one: Uuid = raw
        .query_one(
            "INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,match_status) VALUES($1,$2,$3,$4,'verified') RETURNING id",
            &[&Uuid::parse_str(&title.id).unwrap(), &provider_one, &"one-id", &"Verified Alias One"],
        )
        .await
        .expect("insert verified provider mapping")
        .get(0);
    raw.execute(
        "INSERT INTO provider_titles(title_id,provider_id,provider_title_id,provider_title,match_status) VALUES($1,$2,$3,$4,'unverified')",
        &[&Uuid::parse_str(&unverified_title.id).unwrap(), &provider_two, &"two-id", &"Unverified Alias Two"],
    )
    .await
    .expect("insert unverified provider mapping");
    raw.execute(
        "INSERT INTO title_alternative_titles(title_id,title,normalized_title,language,title_type,source) VALUES($1,$2,$3,$4,$5,'fixture')",
        &[&Uuid::parse_str(&title.id).unwrap(), &"Verified Alias", &"verified alias", &Some("en".to_string()), &Some("alias".to_string())],
    )
    .await
    .expect("insert verified alias");
    raw.execute(
        "INSERT INTO title_alternative_titles(title_id,title,normalized_title,language,title_type,source) VALUES($1,$2,$3,$4,$5,'fixture')",
        &[&Uuid::parse_str(&unverified_title.id).unwrap(), &"Unverified Alias", &"unverified alias", &Some("en".to_string()), &Some("alias".to_string())],
    )
    .await
    .expect("insert unverified alias row");

    let aliases = store
        .verified_provider_aliases(&[
            ("fixture_one".into(), "one-id".into()),
            ("fixture_one".into(), "one-id".into()),
            ("fixture_two".into(), "two-id".into()),
            ("missing".into(), "missing".into()),
        ])
        .await
        .expect("batch alias lookup");
    assert_eq!(
        aliases.get("fixture_one:one-id"),
        Some(&vec!["Verified Alias".into()])
    );
    assert!(!aliases.contains_key("fixture_two:two-id"));
    assert!(!aliases.contains_key("missing:missing"));

    let duplicate = raw
        .execute(
            "INSERT INTO provider_titles(title_id,provider_id,provider_title_id) VALUES($1,$2,$3)",
            &[
                &Uuid::parse_str(&title.id).unwrap(),
                &provider_one,
                &"one-id",
            ],
        )
        .await;
    assert!(
        duplicate.is_err(),
        "duplicate provider mapping must be rejected"
    );

    let mapping_count: i64 = raw
        .query_one(
            "SELECT count(*) FROM provider_titles WHERE provider_id=$1 AND provider_title_id=$2",
            &[&provider_one, &"one-id"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(mapping_count, 1);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run with DATABASE_URL and --ignored"]
async fn phase1_postgres_advisory_lock_and_rollback_acceptance() {
    let store = fresh_store().await;
    let input = unified_input("concurrent-id", "Concurrent Fixture");
    let second_store = CatalogStore::connect_from_env()
        .await
        .unwrap()
        .expect("second isolated catalog connection");
    let (left, right) = tokio::join!(
        store.upsert_unified_anime(input.clone()),
        second_store.upsert_unified_anime(input),
    );
    let left = left.expect("first concurrent upsert");
    let right = right.expect("second concurrent upsert");
    assert_eq!(
        left, right,
        "advisory lock must converge concurrent upserts"
    );

    let raw = connect_raw().await;
    let title_count: i64 = raw
        .query_one(
            "SELECT count(*) FROM catalog_titles WHERE normalized_title=$1 AND release_year=2024",
            &[&"concurrent fixture"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(title_count, 1);

    let invalid = store
        .upsert_unified_anime(UnifiedAnimeInput {
            canonical_title: "Should Roll Back".into(),
            content_type: "unsupported".into(),
            ..Default::default()
        })
        .await;
    assert!(invalid.is_err(), "invalid input must roll back");
    let rollback_count: i64 = raw
        .query_one(
            "SELECT count(*) FROM catalog_titles WHERE canonical_title='Should Roll Back'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(rollback_count, 0);
}
