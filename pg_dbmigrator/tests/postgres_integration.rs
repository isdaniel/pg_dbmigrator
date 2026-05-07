//! Integration tests that require live PostgreSQL instances.
//!
//! Skipped automatically when the required env vars are absent, so
//! `cargo test` still works on a bare workstation. In CI the
//! `codecov.yml` workflow provisions two PG containers and sets:
//!
//! - `PG_SOURCE_URL` → source with `wal_level=logical`
//! - `PG_TARGET_URL` → vanilla target

use std::env;

use pg_dbmigrator::tls::connect_with_sslmode;

fn source_url() -> Option<String> {
    env::var("PG_SOURCE_URL").ok()
}

fn target_url() -> Option<String> {
    env::var("PG_TARGET_URL").ok()
}

macro_rules! skip_without_pg {
    ($url:expr) => {
        match $url {
            Some(u) => u,
            None => {
                eprintln!("skipping: PG env vars not set");
                return;
            }
        }
    };
}

// ─── tls::connect_with_sslmode ────────────────────────────────────────────────

fn append_sslmode_disable(raw: &str) -> String {
    let mut parsed = url::Url::parse(raw).expect("valid URL");
    parsed.query_pairs_mut().append_pair("sslmode", "disable");
    parsed.to_string()
}

#[tokio::test]
async fn connect_source_with_sslmode_disable() {
    let url = skip_without_pg!(source_url());
    let conn_str = append_sslmode_disable(&url);
    let client = connect_with_sslmode(&conn_str).await.unwrap();
    let row = client.query_one("SELECT 1 AS x", &[]).await.unwrap();
    let x: i32 = row.get(0);
    assert_eq!(x, 1);
}

#[tokio::test]
async fn connect_target_with_sslmode_disable() {
    let url = skip_without_pg!(target_url());
    let conn_str = append_sslmode_disable(&url);
    let client = connect_with_sslmode(&conn_str).await.unwrap();
    let row = client.query_one("SELECT version()", &[]).await.unwrap();
    let ver: String = row.get(0);
    assert!(ver.contains("PostgreSQL"));
}

// ─── preflight::verify_source_logical_replication_ready ──────────────────────

#[tokio::test]
async fn verify_source_logical_replication_ready_passes() {
    let url = skip_without_pg!(source_url());
    pg_dbmigrator::preflight::verify_source_logical_replication_ready(&url)
        .await
        .unwrap();
}

// ─── preflight::verify_publication_exists ─────────────────────────────────────

#[tokio::test]
async fn verify_publication_missing_returns_error() {
    let url = skip_without_pg!(source_url());
    let result =
        pg_dbmigrator::preflight::verify_publication_exists(&url, "nonexistent_pub_xyz").await;
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(msg.contains("nonexistent_pub_xyz"));
}

#[tokio::test]
async fn verify_publication_exists_after_creation() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute("CREATE PUBLICATION test_integ_pub FOR ALL TABLES")
        .await
        .unwrap_or(());
    let result = pg_dbmigrator::preflight::verify_publication_exists(&url, "test_integ_pub").await;
    assert!(result.is_ok());
    client
        .batch_execute("DROP PUBLICATION IF EXISTS test_integ_pub")
        .await
        .ok();
}

// ─── preflight::ensure_target_database_exists ─────────────────────────────────

#[tokio::test]
async fn ensure_target_database_already_exists() {
    let url = skip_without_pg!(target_url());
    pg_dbmigrator::preflight::ensure_target_database_exists(&url, "target_db")
        .await
        .unwrap();
}

#[tokio::test]
async fn ensure_target_database_creates_new() {
    let url = skip_without_pg!(target_url());
    let db_name = "test_integ_create_db";
    let maint_conn = pg_dbmigrator::preflight::maintenance_connection_string(&url);
    let client = connect_with_sslmode(&maint_conn).await.unwrap();
    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .ok();

    pg_dbmigrator::preflight::ensure_target_database_exists(&url, db_name)
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await
        .unwrap();
    let exists: bool = row.get(0);
    assert!(exists);

    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS {db_name}"))
        .await
        .ok();
}

// ─── preflight::ensure_pglogical_not_interfering ─────────────────────────────

#[tokio::test]
async fn ensure_pglogical_not_interfering_passes_on_vanilla() {
    let url = skip_without_pg!(target_url());
    pg_dbmigrator::preflight::ensure_pglogical_not_interfering(&url)
        .await
        .unwrap();
}

// ─── sequences module ─────────────────────────────────────────────────────────

#[tokio::test]
async fn collect_source_sequences_returns_empty_on_fresh_db() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute("DROP SEQUENCE IF EXISTS test_integ_seq")
        .await
        .ok();
    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&client, &[])
        .await
        .unwrap();
    let found = seqs.iter().any(|s| s.name == "test_integ_seq");
    assert!(!found);
}

#[tokio::test]
async fn collect_and_apply_sequences_round_trip() {
    let source_url = skip_without_pg!(source_url());
    let target_url = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url).await.unwrap();
    let target = connect_with_sslmode(&target_url).await.unwrap();

    source
        .batch_execute(
            "CREATE SEQUENCE IF NOT EXISTS test_seq_integ START 1; \
             SELECT nextval('test_seq_integ'); \
             SELECT nextval('test_seq_integ'); \
             SELECT nextval('test_seq_integ');",
        )
        .await
        .unwrap();

    target
        .batch_execute("CREATE SEQUENCE IF NOT EXISTS test_seq_integ START 1")
        .await
        .unwrap();

    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&source, &[])
        .await
        .unwrap();
    let our_seq = seqs.iter().find(|s| s.name == "test_seq_integ").unwrap();
    assert!(our_seq.last_value.is_some());
    assert!(our_seq.last_value.unwrap() >= 3);

    let applied =
        pg_dbmigrator::sequences::apply_sequences_to_target(&target, std::slice::from_ref(our_seq))
            .await
            .unwrap();
    assert_eq!(applied, 1);

    let row = target
        .query_one("SELECT last_value FROM test_seq_integ", &[])
        .await
        .unwrap();
    let val: i64 = row.get(0);
    assert!(val >= 3);

    source
        .batch_execute("DROP SEQUENCE IF EXISTS test_seq_integ")
        .await
        .ok();
    target
        .batch_execute("DROP SEQUENCE IF EXISTS test_seq_integ")
        .await
        .ok();
}

#[tokio::test]
async fn collect_sequences_with_schema_filter() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_schema_a; \
             CREATE SEQUENCE IF NOT EXISTS integ_schema_a.filtered_seq START 1; \
             SELECT nextval('integ_schema_a.filtered_seq');",
        )
        .await
        .unwrap();

    let filter = vec!["integ_schema_a".to_string()];
    let seqs = pg_dbmigrator::sequences::collect_source_sequences(&client, &filter)
        .await
        .unwrap();
    assert!(seqs.iter().any(|s| s.name == "filtered_seq"));
    assert!(!seqs.iter().any(|s| s.schema == "public"));

    client
        .batch_execute(
            "DROP SEQUENCE IF EXISTS integ_schema_a.filtered_seq; \
             DROP SCHEMA IF EXISTS integ_schema_a",
        )
        .await
        .ok();
}

#[tokio::test]
async fn sync_sequences_end_to_end() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    source
        .batch_execute(
            "CREATE SEQUENCE IF NOT EXISTS sync_e2e_seq START 1; \
             SELECT setval('sync_e2e_seq', 42);",
        )
        .await
        .unwrap();
    target
        .batch_execute("CREATE SEQUENCE IF NOT EXISTS sync_e2e_seq START 1")
        .await
        .unwrap();

    let applied = pg_dbmigrator::sequences::sync_sequences(&source_url_val, &target_url_val, &[])
        .await
        .unwrap();
    assert!(applied >= 1);

    let row = target
        .query_one("SELECT last_value FROM sync_e2e_seq", &[])
        .await
        .unwrap();
    let val: i64 = row.get(0);
    assert_eq!(val, 42);

    source
        .batch_execute("DROP SEQUENCE IF EXISTS sync_e2e_seq")
        .await
        .ok();
    target
        .batch_execute("DROP SEQUENCE IF EXISTS sync_e2e_seq")
        .await
        .ok();
}

// ─── native_apply::PgSubscriptionLagProvider ─────────────────────────────────

#[tokio::test]
async fn lag_provider_connect_fails_without_slot() {
    let url = skip_without_pg!(source_url());
    let provider = pg_dbmigrator::native_apply::PgSubscriptionLagProvider::connect(
        &url,
        "nonexistent_slot_xyz",
    )
    .await;
    assert!(provider.is_ok());
    let p = provider.unwrap();
    use pg_dbmigrator::native_apply::SubscriptionLagProvider;
    let result = p.sample().await;
    assert!(result.is_err());
}

// ─── native_apply::force_clean_stale_state ───────────────────────────────────

#[tokio::test]
async fn force_clean_stale_state_is_idempotent() {
    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_nonexist_sub".into(),
        slot_name: "integ_nonexist_slot".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    let result = pg_dbmigrator::native_apply::force_clean_stale_state(
        &source_url_val,
        &target_url_val,
        &online,
    )
    .await;
    assert!(result.is_ok());
}

// ─── native_apply::wait_for_slot_inactive ────────────────────────────────────

#[tokio::test]
async fn wait_for_slot_inactive_returns_ok_for_missing_slot() {
    let url = skip_without_pg!(source_url());
    let reporter = pg_dbmigrator::progress::CollectingReporter::new();
    let result =
        pg_dbmigrator::native_apply::wait_for_slot_inactive(&url, "absent_slot_xyz", &reporter)
            .await;
    assert!(result.is_ok());
}

// ─── native_apply::cleanup_target_subscription ───────────────────────────────

#[tokio::test]
async fn cleanup_target_subscription_noop_when_absent() {
    let url = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_absent_sub".into(),
        slot_name: "integ_absent_slot".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    let result = pg_dbmigrator::native_apply::cleanup_target_subscription(&url, &online).await;
    assert!(result.is_ok());
}

// ─── native_apply::disable_target_subscription ───────────────────────────────

#[tokio::test]
async fn disable_target_subscription_noop_when_absent() {
    let url = skip_without_pg!(target_url());
    let online = pg_dbmigrator::OnlineOptions {
        subscription_name: "integ_no_sub".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };
    pg_dbmigrator::native_apply::disable_target_subscription(&url, &online).await;
}

// ─── snapshot::prepare_replication_slot ───────────────────────────────────────

#[tokio::test]
async fn prepare_replication_slot_creates_and_exports_snapshot() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();

    client
        .batch_execute("CREATE PUBLICATION integ_snap_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = pg_dbmigrator::OnlineOptions {
        slot_name: "integ_snap_slot".into(),
        publication: "integ_snap_pub".into(),
        subscription_name: "integ_snap_sub".into(),
        ..pg_dbmigrator::OnlineOptions::default()
    };

    let result = pg_dbmigrator::snapshot::prepare_replication_slot(&url, &online).await;
    match result {
        Ok(prepared) => {
            assert!(prepared.snapshot_name.is_some());
            drop(prepared.stream);
            // Clean up the slot
            client
                .batch_execute(
                    "SELECT pg_drop_replication_slot('integ_snap_slot') \
                     WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'integ_snap_slot')",
                )
                .await
                .ok();
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("already exists") || msg.contains("replication"),
                "unexpected error: {msg}"
            );
        }
    }

    client
        .batch_execute("DROP PUBLICATION IF EXISTS integ_snap_pub")
        .await
        .ok();
}

// ─── Full online apply loop (short-circuit) ──────────────────────────────────

#[tokio::test]
async fn native_apply_with_cancel_exits_cleanly() {
    use pg_dbmigrator::cutover::CutoverHandle;
    use pg_dbmigrator::native_apply::{run_native_apply, SubscriptionLagProvider};
    use pg_dbmigrator::progress::CollectingReporter;
    use pg_dbmigrator::OnlineOptions;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio_util::sync::CancellationToken;

    let source_url_val = skip_without_pg!(source_url());
    let target_url_val = skip_without_pg!(target_url());

    let source = connect_with_sslmode(&source_url_val).await.unwrap();
    let target = connect_with_sslmode(&target_url_val).await.unwrap();

    source
        .batch_execute("CREATE PUBLICATION integ_apply_pub FOR ALL TABLES")
        .await
        .unwrap_or(());

    let online = OnlineOptions {
        slot_name: "integ_apply_slot".into(),
        publication: "integ_apply_pub".into(),
        subscription_name: "integ_apply_sub".into(),
        drop_subscription_on_cutover: true,
        ..OnlineOptions::default()
    };

    // Create the slot so CREATE SUBSCRIPTION can reference it
    source
        .batch_execute("SELECT pg_create_logical_replication_slot('integ_apply_slot', 'pgoutput')")
        .await
        .unwrap_or(());

    // Use a mock lag provider since we just want to test the loop mechanics
    #[derive(Debug)]
    struct MockProvider {
        s: AtomicU64,
        c: AtomicU64,
    }
    #[async_trait::async_trait]
    impl SubscriptionLagProvider for MockProvider {
        async fn sample(&self) -> pg_dbmigrator::Result<(u64, u64)> {
            Ok((self.s.load(Ordering::SeqCst), self.c.load(Ordering::SeqCst)))
        }
    }
    let provider = MockProvider {
        s: AtomicU64::new(100),
        c: AtomicU64::new(100),
    };

    let cancel = CancellationToken::new();
    let cancel2 = cancel.clone();
    let reporter = CollectingReporter::new();
    let cutover = CutoverHandle::new();

    // Cancel after a short delay
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        cancel2.cancel();
    });

    let result = run_native_apply(
        &target,
        &provider,
        &online,
        &source_url_val,
        cutover,
        &reporter,
        cancel,
    )
    .await;

    // The loop should exit due to cancel; the CREATE SUBSCRIPTION may or
    // may not succeed depending on PG state, but the cancellation path
    // should not panic.
    match result {
        Ok(stats) => {
            assert!(!stats.cutover_triggered);
        }
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("subscription")
                    || msg.contains("slot")
                    || msg.contains("does not exist"),
                "unexpected error: {msg}"
            );
        }
    }

    // Cleanup
    target
        .batch_execute(
            "DO $$ BEGIN \
               IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = 'integ_apply_sub') THEN \
                 EXECUTE 'ALTER SUBSCRIPTION integ_apply_sub DISABLE'; \
                 EXECUTE 'ALTER SUBSCRIPTION integ_apply_sub SET (slot_name = NONE)'; \
                 EXECUTE 'DROP SUBSCRIPTION integ_apply_sub'; \
               END IF; \
             END $$;",
        )
        .await
        .ok();
    source
        .batch_execute(
            "SELECT pg_drop_replication_slot(slot_name) \
             FROM pg_replication_slots \
             WHERE slot_name = 'integ_apply_slot'",
        )
        .await
        .ok();
    source
        .batch_execute("DROP PUBLICATION IF EXISTS integ_apply_pub")
        .await
        .ok();
}

// ─── preflight::verify_pg_tools_installed (live) ─────────────────────────────

#[tokio::test]
async fn verify_pg_tools_installed_succeeds_in_ci() {
    // In CI with PostgreSQL client tools available this should pass.
    // On bare workstations without pg tools it may fail, but since we
    // skip_without_pg this only runs in CI.
    let _url = skip_without_pg!(source_url());
    pg_dbmigrator::preflight::verify_pg_tools_installed()
        .await
        .unwrap();
}

// ─── analyze::run_target_analyze ─────────────────────────────────────────────

#[tokio::test]
async fn run_target_analyze_whole_database() {
    let url = skip_without_pg!(target_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_analyze; \
             CREATE TABLE IF NOT EXISTS integ_analyze.t1 (id int PRIMARY KEY, v text);",
        )
        .await
        .unwrap();

    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &[], false).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_analyze CASCADE")
        .await
        .ok();
}

#[tokio::test]
async fn run_target_analyze_with_schema_filter() {
    let url = skip_without_pg!(target_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_analyze_s; \
             CREATE TABLE IF NOT EXISTS integ_analyze_s.t1 (id int PRIMARY KEY, v text); \
             CREATE TABLE IF NOT EXISTS integ_analyze_s.t2 (id int PRIMARY KEY, n int);",
        )
        .await
        .unwrap();

    let schemas = vec!["integ_analyze_s".to_string()];
    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &schemas, false).await;
    assert!(result.is_ok());

    // Verbose mode
    let result = pg_dbmigrator::analyze::run_target_analyze(&url, &schemas, true).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_analyze_s CASCADE")
        .await
        .ok();
}

// ─── analyze::run_source_vacuum ──────────────────────────────────────────────

#[tokio::test]
async fn run_source_vacuum_whole_database() {
    let url = skip_without_pg!(source_url());
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &[], false).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn run_source_vacuum_with_schema_filter() {
    let url = skip_without_pg!(source_url());
    let client = connect_with_sslmode(&url).await.unwrap();
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS integ_vacuum_s; \
             CREATE TABLE IF NOT EXISTS integ_vacuum_s.t1 (id int PRIMARY KEY, v text);",
        )
        .await
        .unwrap();

    let schemas = vec!["integ_vacuum_s".to_string()];
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &schemas, false).await;
    assert!(result.is_ok());

    // Verbose mode
    let result = pg_dbmigrator::analyze::run_source_vacuum(&url, &schemas, true).await;
    assert!(result.is_ok());

    client
        .batch_execute("DROP SCHEMA integ_vacuum_s CASCADE")
        .await
        .ok();
}

// ─── analyze::maybe_vacuum_source / maybe_analyze_target ─────────────────────

#[tokio::test]
async fn maybe_vacuum_source_runs_when_not_skipped() {
    let url = skip_without_pg!(source_url());
    let config = pg_dbmigrator::MigrationConfig {
        source: pg_dbmigrator::EndpointConfig::parse(&url).unwrap(),
        skip_source_vacuum: false,
        ..pg_dbmigrator::MigrationConfig::default()
    };
    let result = pg_dbmigrator::analyze::maybe_vacuum_source(&config).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn maybe_analyze_target_runs_when_not_skipped() {
    let url = skip_without_pg!(target_url());
    let config = pg_dbmigrator::MigrationConfig {
        target: pg_dbmigrator::EndpointConfig::parse(&url).unwrap(),
        skip_analyze: false,
        ..pg_dbmigrator::MigrationConfig::default()
    };
    let result = pg_dbmigrator::analyze::maybe_analyze_target(&config).await;
    assert!(result.is_ok());
}
