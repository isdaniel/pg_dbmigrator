use std::env;
use std::fmt::Write as FmtWrite;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures::SinkExt;
use pg_dbmigrator::config::DumpScope;
use pg_dbmigrator::{
    CutoverHandle, EndpointConfig, MigrationConfig, MigrationMode, MigrationStage, Migrator,
    OnlineOptions, ProgressEvent, ProgressReporter,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const ROWS_PER_GB: u64 = 1_000_000;
const BATCH_SIZE: u64 = 1_000_000;
const PARALLEL_JOBS: usize = 8;
const NUM_TABLES: usize = 8;

fn table_name(i: usize) -> String {
    format!("benchmark_data_{:02}", i + 1)
}

struct FastRng(u64);

impl FastRng {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { 1 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }
}

#[derive(Parser)]
#[command(name = "benchmark", about = "pg_dbmigrator benchmark tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    #[arg(long, env = "BENCHMARK_SOURCE_HOST")]
    source_host: String,

    #[arg(long, env = "BENCHMARK_TARGET_HOST")]
    target_host: String,

    #[arg(long, env = "BENCHMARK_USER")]
    user: String,

    #[arg(long, env = "BENCHMARK_PASSWORD")]
    password: String,

    #[arg(long, env = "BENCHMARK_DB", default_value = "benchmark_db")]
    database: String,
}

#[derive(Subcommand)]
enum Commands {
    Seed {
        #[arg(long, value_delimiter = ',')]
        size: Vec<u64>,
    },
    Run {
        #[arg(long)]
        size: u64,
        #[arg(long, default_value = "online")]
        mode: RunMode,
    },
    Full {
        #[arg(long, value_delimiter = ',')]
        size: Vec<u64>,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
enum RunMode {
    Offline,
    Online,
    Both,
}

impl Cli {
    fn source_uri(&self, db: &str) -> String {
        format!(
            "postgresql://{}:{}@{}:5432/{}?sslmode=require",
            self.user, self.password, self.source_host, db
        )
    }

    fn target_uri(&self, db: &str) -> String {
        format!(
            "postgresql://{}:{}@{}:5432/{}?sslmode=require",
            self.user, self.password, self.target_host, db
        )
    }
}

async fn connect(uri: &str) -> Result<tokio_postgres::Client> {
    let certs = rustls_native_certs::load_native_certs();
    let mut root_store = rustls::RootCertStore::empty();
    for cert in certs.certs {
        root_store.add(cert).ok();
    }
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(tls_config);

    let (client, connection) = tokio_postgres::connect(uri, tls)
        .await
        .context("failed to connect to database")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!("connection task error: {e}");
        }
    });

    Ok(client)
}

async fn ensure_database_exists(admin_uri: &str, db_name: &str) -> Result<()> {
    let client = connect(admin_uri).await?;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?
        .get(0);

    if !exists {
        info!("creating database {db_name}");
        client
            .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
            .await
            .context("failed to create database")?;
    } else {
        info!("database {db_name} already exists");
    }
    Ok(())
}

async fn drop_and_recreate_database(admin_uri: &str, db_name: &str) -> Result<()> {
    let client = connect(admin_uri).await?;

    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?
        .get(0);

    if exists {
        info!("terminating connections to {db_name}");
        client
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
                &[&db_name],
            )
            .await
            .ok();

        tokio::time::sleep(Duration::from_secs(2)).await;

        info!("dropping database {db_name}");
        client
            .batch_execute(&format!("DROP DATABASE IF EXISTS \"{db_name}\""))
            .await
            .context("failed to drop database")?;
    }

    info!("creating database {db_name}");
    client
        .batch_execute(&format!("CREATE DATABASE \"{db_name}\""))
        .await
        .context("failed to create database")?;

    Ok(())
}

async fn seed_single_table(
    uri: &str,
    table_idx: usize,
    rows_per_table: u64,
    start: Instant,
) -> Result<()> {
    let client = connect(uri).await?;
    let tbl = table_name(table_idx);

    let current_rows: i64 = client
        .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
        .await?
        .get(0);
    let current_rows = current_rows as u64;

    if current_rows >= rows_per_table {
        info!("[{tbl}] already has {current_rows} rows (target {rows_per_table}), skipping");
        return Ok(());
    }

    let rows_needed = rows_per_table - current_rows;
    let batches = rows_needed.div_ceil(BATCH_SIZE);

    let start_id: u64 = if current_rows > 0 {
        let max_id: i64 = client
            .query_one(&format!("SELECT MAX(id) FROM {tbl}"), &[])
            .await?
            .get(0);
        max_id as u64 + 1
    } else {
        1
    };

    info!(
        "[{tbl}] COPY-seeding {} rows in {} batches (start_id={start_id})",
        format_rows(rows_needed as i64),
        batches,
    );

    let mut rng = FastRng::new((table_idx as u64 + 1) * 0x517cc1b727220a95);
    let mut next_id = start_id;

    for i in 0..batches {
        let remaining = rows_needed - (i * BATCH_SIZE);
        let batch_count = remaining.min(BATCH_SIZE) as usize;

        let sink = client
            .copy_in(&format!(
                "COPY {tbl} (id, payload, created_at, val) FROM STDIN"
            ))
            .await
            .with_context(|| format!("COPY IN start failed for {tbl}"))?;

        let mut sink = Box::pin(sink);

        const CHUNK: usize = 50_000;
        for chunk_start in (0..batch_count).step_by(CHUNK) {
            let chunk_end = (chunk_start + CHUNK).min(batch_count);
            let chunk_len = chunk_end - chunk_start;
            let mut buf = String::with_capacity(chunk_len * 1040);

            for _ in 0..chunk_len {
                write!(buf, "{next_id}\t").unwrap();
                for _ in 0..31 {
                    let a = rng.next_u64();
                    let b = rng.next_u64();
                    write!(buf, "{a:016x}{b:016x}").unwrap();
                }
                writeln!(buf, "\t2026-01-01 00:00:00\t{:.10}", rng.next_f64()).unwrap();
                next_id += 1;
            }

            sink.send(Bytes::from(buf.into_bytes()))
                .await
                .with_context(|| format!("COPY send failed for {tbl}"))?;
        }

        sink.as_mut()
            .finish()
            .await
            .with_context(|| format!("COPY finish failed for {tbl}"))?;

        let rows_so_far = ((i + 1) * BATCH_SIZE).min(rows_needed);
        let pct = ((current_rows + rows_so_far) as f64 / rows_per_table as f64) * 100.0;
        let elapsed = start.elapsed();

        info!(
            "[{tbl}] {}/{} rows ({:.1}%) - elapsed: {:.0}s",
            format_rows((current_rows + rows_so_far) as i64),
            format_rows(rows_per_table as i64),
            pct,
            elapsed.as_secs_f64(),
        );
    }

    client
        .batch_execute(&format!(
            "SELECT setval(pg_get_serial_sequence('{tbl}', 'id'), \
             (SELECT MAX(id) FROM {tbl}))"
        ))
        .await
        .ok();

    Ok(())
}

async fn seed_data(source_uri: &str, target_rows: u64) -> Result<()> {
    let client = connect(source_uri).await?;
    let rows_per_table = target_rows / NUM_TABLES as u64;

    info!(
        "target: {} total rows across {} tables ({} rows/table)",
        format_rows(target_rows as i64),
        NUM_TABLES,
        format_rows(rows_per_table as i64),
    );

    let mut new_tables = Vec::new();
    for t in 0..NUM_TABLES {
        let tbl = table_name(t);
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1 AND relkind IN ('r','u'))",
                &[&tbl],
            )
            .await?
            .get(0);

        if !exists {
            info!("[{tbl}] creating as UNLOGGED (no PK) for fast bulk COPY");
            client
                .batch_execute(&format!(
                    "CREATE UNLOGGED TABLE {tbl} (
                        id BIGSERIAL,
                        payload TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL,
                        val DOUBLE PRECISION NOT NULL
                    )"
                ))
                .await
                .with_context(|| format!("failed to create table {tbl}"))?;
            new_tables.push(t);
        } else {
            let current_rows: i64 = client
                .query_one(&format!("SELECT count(*) FROM {tbl}"), &[])
                .await?
                .get(0);
            if (current_rows as u64) < rows_per_table {
                info!(
                    "[{tbl}] needs more rows ({} < {}), dropping indexes for faster COPY",
                    format_rows(current_rows),
                    format_rows(rows_per_table as i64),
                );
                drop_table_indexes(&client, t).await?;
            }
        }
    }

    let start = Instant::now();

    info!("starting parallel COPY across {} tables", NUM_TABLES);
    let mut handles = Vec::with_capacity(NUM_TABLES);
    for t in 0..NUM_TABLES {
        let uri = source_uri.to_string();
        handles.push(tokio::spawn(async move {
            seed_single_table(&uri, t, rows_per_table, start).await
        }));
    }

    for (t, handle) in handles.into_iter().enumerate() {
        handle
            .await
            .with_context(|| format!("seed task for {} panicked", table_name(t)))?
            .with_context(|| format!("seed task for {} failed", table_name(t)))?;
    }

    let copy_elapsed = start.elapsed();
    info!(
        "parallel COPY complete in {:.1} minutes",
        copy_elapsed.as_secs_f64() / 60.0
    );

    if !new_tables.is_empty() {
        info!("converting {} UNLOGGED tables to LOGGED", new_tables.len());
        for &t in &new_tables {
            let tbl = table_name(t);
            let t0 = Instant::now();
            client
                .batch_execute(&format!("ALTER TABLE {tbl} SET LOGGED"))
                .await
                .with_context(|| format!("failed to set {tbl} to LOGGED"))?;
            info!("[{tbl}] SET LOGGED in {:.1}s", t0.elapsed().as_secs_f64());
        }
    }

    ensure_indexes(&client).await?;

    let elapsed = start.elapsed();
    info!(
        "seeding complete in {:.1} minutes (COPY: {:.1}m, total: {:.1}m)",
        elapsed.as_secs_f64() / 60.0,
        copy_elapsed.as_secs_f64() / 60.0,
        elapsed.as_secs_f64() / 60.0,
    );

    Ok(())
}

async fn drop_table_indexes(client: &tokio_postgres::Client, table_idx: usize) -> Result<()> {
    let tbl = table_name(table_idx);

    for idx in &[format!("idx_{tbl}_created_at"), format!("idx_{tbl}_val")] {
        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                &[idx],
            )
            .await?
            .get(0);
        if exists {
            let t0 = Instant::now();
            info!("[{tbl}] dropping index {idx}");
            client
                .batch_execute(&format!("DROP INDEX {idx}"))
                .await
                .with_context(|| format!("failed to drop index {idx}"))?;
            info!("[{tbl}] dropped {idx} in {:.1}s", t0.elapsed().as_secs_f64());
        }
    }

    let pk_name: Option<String> = client
        .query_opt(
            &format!(
                "SELECT conname FROM pg_constraint \
                 WHERE conrelid = '{tbl}'::regclass AND contype = 'p'"
            ),
            &[],
        )
        .await?
        .map(|r| r.get(0));

    if let Some(pk) = pk_name {
        let t0 = Instant::now();
        info!("[{tbl}] dropping PRIMARY KEY ({pk})");
        client
            .batch_execute(&format!("ALTER TABLE {tbl} DROP CONSTRAINT {pk}"))
            .await
            .with_context(|| format!("failed to drop PK on {tbl}"))?;
        info!("[{tbl}] dropped PK in {:.1}s", t0.elapsed().as_secs_f64());
    }

    Ok(())
}

async fn ensure_indexes(client: &tokio_postgres::Client) -> Result<()> {
    info!("ensuring PK + indexes on all {} tables", NUM_TABLES);

    for t in 0..NUM_TABLES {
        let tbl = table_name(t);

        let has_pk: bool = client
            .query_one(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM pg_constraint \
                     WHERE conrelid = '{tbl}'::regclass AND contype = 'p')"
                ),
                &[],
            )
            .await?
            .get(0);
        if !has_pk {
            let t0 = Instant::now();
            info!("[{tbl}] adding PRIMARY KEY");
            client
                .batch_execute(&format!("ALTER TABLE {tbl} ADD PRIMARY KEY (id)"))
                .await
                .with_context(|| format!("failed to add PK on {tbl}"))?;
            info!("[{tbl}] PK created in {:.1}s", t0.elapsed().as_secs_f64());
        }

        let idx_created = format!("idx_{tbl}_created_at");
        let idx_val = format!("idx_{tbl}_val");

        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                &[&idx_created],
            )
            .await?
            .get(0);
        if !exists {
            let t0 = Instant::now();
            info!("[{tbl}] creating index {idx_created}");
            client
                .batch_execute(&format!("CREATE INDEX {idx_created} ON {tbl} (created_at)"))
                .await
                .with_context(|| format!("failed to create index {idx_created}"))?;
            info!(
                "[{tbl}] {idx_created} created in {:.1}s",
                t0.elapsed().as_secs_f64()
            );
        }

        let exists: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = $1)",
                &[&idx_val],
            )
            .await?
            .get(0);
        if !exists {
            let t0 = Instant::now();
            info!("[{tbl}] creating index {idx_val}");
            client
                .batch_execute(&format!("CREATE INDEX {idx_val} ON {tbl} (val)"))
                .await
                .with_context(|| format!("failed to create index {idx_val}"))?;
            info!(
                "[{tbl}] {idx_val} created in {:.1}s",
                t0.elapsed().as_secs_f64()
            );
        }
    }

    info!("all indexes ready");
    Ok(())
}

async fn get_db_size(uri: &str) -> Result<(i64, i64)> {
    let client = connect(uri).await?;
    let size: i64 = client
        .query_one("SELECT pg_database_size(current_database())", &[])
        .await?
        .get(0);

    let mut total_rows: i64 = 0;
    for t in 0..NUM_TABLES {
        let tbl = table_name(t);
        let rows: i64 = client
            .query_one(
                &format!(
                    "SELECT COALESCE(reltuples, 0)::bigint FROM pg_class WHERE relname = '{tbl}'"
                ),
                &[],
            )
            .await
            .map(|r| r.get(0))
            .unwrap_or(0);
        total_rows += rows;
    }
    Ok((size, total_rows))
}

fn build_migration_config(
    source_uri: &str,
    target_uri: &str,
    mode: MigrationMode,
) -> Result<MigrationConfig> {
    Ok(MigrationConfig {
        mode,
        source: EndpointConfig::parse(source_uri)?,
        target: EndpointConfig::parse(target_uri)?,
        dump_scope: DumpScope::All,
        drop_target_first: true,
        jobs: PARALLEL_JOBS,
        dump_compress: Some("zstd:3".into()),
        split_sections: true,
        allow_restore_errors: true,
        skip_source_vacuum: true,
        skip_analyze: true,
        online: OnlineOptions {
            force_clean: true,
            ..OnlineOptions::default()
        },
        ..MigrationConfig::default()
    })
}

#[derive(Debug)]
struct BenchmarkReporter {
    start: Instant,
    caught_up_elapsed: Arc<Mutex<Option<Duration>>>,
    cutover_handle: Option<CutoverHandle>,
}

impl BenchmarkReporter {
    fn new(cutover_handle: Option<CutoverHandle>) -> Self {
        Self {
            start: Instant::now(),
            caught_up_elapsed: Arc::new(Mutex::new(None)),
            cutover_handle,
        }
    }

    async fn caught_up_time(&self) -> Option<Duration> {
        *self.caught_up_elapsed.lock().await
    }
}

#[async_trait::async_trait]
impl ProgressReporter for BenchmarkReporter {
    async fn report(&self, event: ProgressEvent) {
        let elapsed = self.start.elapsed();
        info!(
            stage = ?event.stage,
            elapsed_s = elapsed.as_secs(),
            "{}",
            event.message
        );

        if event.stage == MigrationStage::CaughtUp {
            *self.caught_up_elapsed.lock().await = Some(elapsed);
            if let Some(handle) = &self.cutover_handle {
                info!("auto-triggering cutover after CaughtUp");
                handle.request();
            }
        }
    }
}

struct BenchmarkResult {
    target_gb: u64,
    actual_db_size_bytes: i64,
    rows: i64,
    offline_secs: Option<f64>,
    online_caught_up_secs: Option<f64>,
}

impl BenchmarkResult {
    fn actual_gb(&self) -> f64 {
        self.actual_db_size_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    fn print_table_row(&self) {
        let offline_s = self
            .offline_secs
            .map(|s| format!("{:.0}", s))
            .unwrap_or_else(|| "-".into());
        let offline_min = self
            .offline_secs
            .map(|s| format!("{:.1}", s / 60.0))
            .unwrap_or_else(|| "-".into());
        let online_s = self
            .online_caught_up_secs
            .map(|s| format!("{:.0}", s))
            .unwrap_or_else(|| "-".into());
        let online_min = self
            .online_caught_up_secs
            .map(|s| format!("{:.1}", s / 60.0))
            .unwrap_or_else(|| "-".into());

        println!(
            "| {} GB | {:.0} GB | {:>13} | {} | {:>11} | {:>13} | {:>22} | {:>12} |",
            self.target_gb,
            self.actual_gb(),
            format_rows(self.rows),
            NUM_TABLES,
            offline_s,
            offline_min,
            online_s,
            online_min,
        );
    }

    fn offline_throughput_gb_min(&self) -> Option<f64> {
        self.offline_secs.map(|s| self.actual_gb() / (s / 60.0))
    }

    fn online_throughput_gb_min(&self) -> Option<f64> {
        self.online_caught_up_secs
            .map(|s| self.actual_gb() / (s / 60.0))
    }
}

fn format_rows(rows: i64) -> String {
    let s = rows.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

async fn run_offline_benchmark(cli: &Cli) -> Result<f64> {
    info!("=== OFFLINE MIGRATION BENCHMARK ===");

    let config = build_migration_config(
        &cli.source_uri(&cli.database),
        &cli.target_uri(&cli.database),
        MigrationMode::Offline,
    )?;

    let reporter = Arc::new(BenchmarkReporter::new(None));
    let migrator = Migrator::new(config).with_reporter(reporter.clone());

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        cancel_clone.cancel();
    });

    let start = Instant::now();
    migrator.run(cancel).await?;
    let elapsed = start.elapsed();

    info!(
        "offline migration completed in {:.1} seconds ({:.1} minutes)",
        elapsed.as_secs_f64(),
        elapsed.as_secs_f64() / 60.0
    );

    Ok(elapsed.as_secs_f64())
}

async fn run_online_benchmark(cli: &Cli) -> Result<f64> {
    info!("=== ONLINE MIGRATION BENCHMARK ===");

    let dump_dir = PathBuf::from(format!("/tmp/dump_online-{}", std::process::id()));
    if dump_dir.exists() {
        info!("cleaning up previous dump directory: {}", dump_dir.display());
        std::fs::remove_dir_all(&dump_dir)
            .with_context(|| format!("failed to remove {}", dump_dir.display()))?;
    }

    let config = build_migration_config(
        &cli.source_uri(&cli.database),
        &cli.target_uri(&cli.database),
        MigrationMode::Online,
    )?;

    let migrator = Migrator::new(config);
    let cutover = migrator.cutover_handle();

    let reporter = Arc::new(BenchmarkReporter::new(Some(cutover.clone())));
    let migrator = migrator.with_reporter(reporter.clone());

    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();
    let cutover_for_signal = cutover.clone();
    tokio::spawn(async move {
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            if !cutover_for_signal.is_requested() {
                info!("Ctrl+C — requesting cutover");
                cutover_for_signal.request();
            } else {
                info!("Ctrl+C again — cancelling");
                cancel_clone.cancel();
                return;
            }
        }
    });

    migrator.run(cancel).await?;

    let caught_up_time = reporter
        .caught_up_time()
        .await
        .unwrap_or(reporter.start.elapsed());

    info!(
        "online migration caught up in {:.1} seconds ({:.1} minutes)",
        caught_up_time.as_secs_f64(),
        caught_up_time.as_secs_f64() / 60.0
    );

    Ok(caught_up_time.as_secs_f64())
}

async fn run_benchmark_for_size(
    cli: &Cli,
    target_gb: u64,
    mode: &RunMode,
) -> Result<BenchmarkResult> {
    let target_rows = target_gb * ROWS_PER_GB;

    info!(
        "=== BENCHMARK {target_gb} GB ({} rows, {NUM_TABLES} tables) ===",
        format_rows(target_rows as i64)
    );

    let (actual_size, rows) = get_db_size(&cli.source_uri(&cli.database)).await?;
    info!(
        "source DB size: {:.1} GB, rows: {}",
        actual_size as f64 / (1024.0 * 1024.0 * 1024.0),
        format_rows(rows)
    );

    let mut result = BenchmarkResult {
        target_gb,
        actual_db_size_bytes: actual_size,
        rows,
        offline_secs: None,
        online_caught_up_secs: None,
    };

    let run_offline = matches!(mode, RunMode::Offline | RunMode::Both);
    let run_online = matches!(mode, RunMode::Online | RunMode::Both);

    if run_offline {
        drop_and_recreate_database(&cli.target_uri("postgres"), &cli.database).await?;
        match run_offline_benchmark(cli).await {
            Ok(secs) => result.offline_secs = Some(secs),
            Err(e) => warn!("offline migration failed: {e}"),
        }
    }

    if run_online {
        drop_and_recreate_database(&cli.target_uri("postgres"), &cli.database).await?;
        match run_online_benchmark(cli).await {
            Ok(secs) => result.online_caught_up_secs = Some(secs),
            Err(e) => warn!("online migration failed: {e}"),
        }
    }

    result.print_table_row();
    Ok(result)
}

fn print_results_table(results: &[BenchmarkResult]) {
    println!();
    println!("## Benchmark Results");
    println!();
    println!(
        "| Target Size | Actual DB Size | Rows | Tables | Offline (s) | Offline (min) | Online to CaughtUp (s) | Online (min) |"
    );
    println!(
        "|-------------|---------------|------|--------|-------------|---------------|------------------------|--------------|"
    );
    for r in results {
        r.print_table_row();
    }

    println!();
    println!("## Throughput Analysis");
    println!();
    println!("| Size | Offline Throughput | Online Throughput |");
    println!("|------|-------------------|-------------------|");
    for r in results {
        let offline_tp = r
            .offline_throughput_gb_min()
            .map(|t| format!("{:.2} GB/min", t))
            .unwrap_or_else(|| "-".into());
        let online_tp = r
            .online_throughput_gb_min()
            .map(|t| format!("{:.2} GB/min", t))
            .unwrap_or_else(|| "-".into());
        println!(
            "| {} GB ({:.0} GB actual) | {} | {} |",
            r.target_gb,
            r.actual_gb(),
            offline_tp,
            online_tp,
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,pg_dbmigrator=info")),
        )
        .with_target(false)
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    let path = env::var("PATH").unwrap_or_default();
    env::set_var("PATH", format!("/usr/lib/postgresql/18/bin:{path}"));

    let cli = Cli::parse();

    match &cli.command {
        Commands::Seed { size } => {
            ensure_database_exists(&cli.source_uri("postgres"), &cli.database).await?;
            let max_size = *size.iter().max().unwrap_or(&200);
            let target_rows = max_size * ROWS_PER_GB;
            seed_data(&cli.source_uri(&cli.database), target_rows).await?;
        }

        Commands::Run { size, mode } => {
            run_benchmark_for_size(&cli, *size, mode).await?;
        }

        Commands::Full { size } => {
            let sizes = if size.is_empty() {
                vec![10, 50, 100, 200, 300]
            } else {
                size.clone()
            };

            ensure_database_exists(&cli.source_uri("postgres"), &cli.database).await?;

            let mut results = Vec::new();

            for &target_gb in &sizes {
                let target_rows = target_gb * ROWS_PER_GB;

                info!("--- Preparing {target_gb} GB benchmark ---");
                seed_data(&cli.source_uri(&cli.database), target_rows).await?;

                let result = run_benchmark_for_size(&cli, target_gb, &RunMode::Online).await?;
                results.push(result);
            }

            print_results_table(&results);
        }
    }

    Ok(())
}
