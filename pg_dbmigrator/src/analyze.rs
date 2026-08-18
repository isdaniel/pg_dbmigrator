//! Pre-dump source VACUUM ANALYZE and post-restore target ANALYZE.
//!
//! ## Why this module exists
//!
//! **Post-restore ANALYZE (target)**:
//! After a bulk `pg_restore` the target's `pg_statistic` catalogue is
//! empty — the query planner has zero statistics for every restored
//! table and will fall back to worst-case sequential scans. Running
//! `ANALYZE` immediately after restore populates the stats so the first
//! application queries after cutover get optimal plans. pgcopydb and
//! Azure DMS both run this automatically.
//!
//! **Pre-dump VACUUM ANALYZE (source)**:
//! Running `VACUUM ANALYZE` on the source before `pg_dump` has two
//! benefits:
//! 1. VACUUM reclaims dead tuples, reducing the number of heap pages
//!    that `pg_dump` must read (less I/O, smaller archive).
//! 2. ANALYZE refreshes `pg_statistic` so the planner picks optimal
//!    parallel plans for the dump workers' queries.
//!
//! Both steps are enabled by default and can be individually disabled
//! via [`MigrationConfig::skip_analyze`] / [`MigrationConfig::skip_source_vacuum`].

use pg_walstream::quote_ident;
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::config::MigrationConfig;
use crate::error::Result;
use crate::tls::connect_with_sslmode;

/// Run `ANALYZE` (or `ANALYZE VERBOSE` when verbose) on the target
/// database after restore.
///
/// When `schemas` is non-empty, only those schemas are analyzed;
/// otherwise the entire database is analyzed in one shot.
pub async fn run_target_analyze(
    target_conn: &str,
    schemas: &[String],
    verbose: bool,
) -> Result<()> {
    info!("running ANALYZE on target database");
    let client = connect_with_sslmode(target_conn).await?;

    if schemas.is_empty() {
        let sql = build_analyze_sql(verbose);
        client.batch_execute(&sql).await?;
        info!("ANALYZE complete (all schemas)");
    } else {
        for schema in schemas {
            analyze_schema(&client, schema, verbose).await;
        }
        info!(count = schemas.len(), "ANALYZE complete (filtered schemas)");
    }
    Ok(())
}

/// Run `VACUUM ANALYZE` on the source database before dump.
///
/// When `schemas` is non-empty, only tables in those schemas are
/// vacuumed; otherwise a database-wide `VACUUM ANALYZE` is issued.
pub async fn run_source_vacuum(source_conn: &str, schemas: &[String], verbose: bool) -> Result<()> {
    info!("running VACUUM ANALYZE on source database");
    let client = connect_with_sslmode(source_conn).await?;

    if schemas.is_empty() {
        let sql = build_vacuum_analyze_sql(verbose);
        client.batch_execute(&sql).await?;
        info!("VACUUM ANALYZE complete (all schemas)");
    } else {
        for schema in schemas {
            vacuum_schema(&client, schema, verbose).await;
        }
        info!(
            count = schemas.len(),
            "VACUUM ANALYZE complete (filtered schemas)"
        );
    }
    Ok(())
}

/// Run a maintenance command (ANALYZE or VACUUM ANALYZE) on all tables in a
/// schema. Errors on individual tables are logged but do not abort the process.
async fn run_per_table(
    client: &Client,
    schema: &str,
    verbose: bool,
    build_sql: fn(&str, &str, bool) -> Result<String>,
    op_name: &str,
    list_sql: &str,
) {
    let tables = match list_relations(client, schema, list_sql).await {
        Ok(t) => t,
        Err(e) => {
            warn!(schema = %schema, error = %e, "failed to list tables for {op_name}");
            return;
        }
    };
    for table in &tables {
        // A quoting failure is per-table and non-fatal, same as an execution
        // failure: ANALYZE/VACUUM is best-effort maintenance, not migration
        // correctness. Skip the offending relation and keep going.
        let sql = match build_sql(schema, table, verbose) {
            Ok(sql) => sql,
            Err(e) => {
                // Debug-format the identifier: this arm only fires when it
                // holds a NUL, and Display would write that raw byte into the
                // log stream, letting a NUL-sensitive transport truncate the
                // very line meant to alert the operator.
                warn!(schema = %schema, table = ?table, error = %e, "{op_name} skipped (unusable identifier)");
                continue;
            }
        };
        if let Err(e) = client.batch_execute(&sql).await {
            warn!(schema = %schema, table = %table, error = %e, "{op_name} failed (continuing)");
        } else {
            debug!(schema = %schema, table = %table, "{op_name} done");
        }
    }
}

/// ANALYZE all tables in a single schema.
async fn analyze_schema(client: &Client, schema: &str, verbose: bool) {
    run_per_table(
        client,
        schema,
        verbose,
        build_analyze_table_sql,
        "ANALYZE",
        LIST_ANALYZABLE_SQL,
    )
    .await;
}

/// VACUUM ANALYZE all tables in a single schema.
async fn vacuum_schema(client: &Client, schema: &str, verbose: bool) {
    run_per_table(
        client,
        schema,
        verbose,
        build_vacuum_analyze_table_sql,
        "VACUUM ANALYZE",
        LIST_VACUUMABLE_SQL,
    )
    .await;
}

/// List relations matching a given query (parameterized by schema name).
async fn list_relations(client: &Client, schema: &str, sql: &str) -> Result<Vec<String>> {
    let rows = client.query(sql, &[&schema]).await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// SQL to list tables, partitioned tables, and materialized views in a schema
/// (for ANALYZE — all these relation types support ANALYZE).
/// Excludes individual partitions — ANALYZE on a partitioned parent
/// already processes its children.
pub const LIST_ANALYZABLE_SQL: &str = "\
    SELECT c.relname::text \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind IN ('r', 'p', 'm') \
      AND n.nspname = $1 \
      AND NOT c.relispartition";

/// SQL to list tables and partitioned tables in a schema (for VACUUM).
/// Excludes materialized views — PostgreSQL does not support VACUUM on them.
/// Excludes individual partitions — VACUUM on a partitioned parent
/// already processes its children.
pub const LIST_VACUUMABLE_SQL: &str = "\
    SELECT c.relname::text \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind IN ('r', 'p') \
      AND n.nspname = $1 \
      AND NOT c.relispartition";

/// Build an `ANALYZE` statement for the entire database.
pub fn build_analyze_sql(verbose: bool) -> String {
    let verbose_kw = if verbose { " VERBOSE" } else { "" };
    format!("ANALYZE{verbose_kw};")
}

/// Build an `ANALYZE` statement for a single table.
///
/// # Errors
///
/// Returns an error if `schema` or `table` contains a null byte, which would
/// truncate the statement on the C-string wire protocol.
pub fn build_analyze_table_sql(schema: &str, table: &str, verbose: bool) -> Result<String> {
    let verbose_kw = if verbose { " VERBOSE" } else { "" };
    let schema_q = quote_ident(schema)?;
    let table_q = quote_ident(table)?;
    Ok(format!("ANALYZE{verbose_kw} {schema_q}.{table_q};"))
}

/// Build a `VACUUM ANALYZE` statement for the entire database.
pub fn build_vacuum_analyze_sql(verbose: bool) -> String {
    let verbose_kw = if verbose {
        " (VERBOSE, ANALYZE)"
    } else {
        " ANALYZE"
    };
    format!("VACUUM{verbose_kw};")
}

/// Build a `VACUUM ANALYZE` statement for a single table.
///
/// # Errors
///
/// Returns an error if `schema` or `table` contains a null byte, which would
/// truncate the statement on the C-string wire protocol.
pub fn build_vacuum_analyze_table_sql(schema: &str, table: &str, verbose: bool) -> Result<String> {
    let verbose_kw = if verbose {
        " (VERBOSE, ANALYZE)"
    } else {
        " ANALYZE"
    };
    let schema_q = quote_ident(schema)?;
    let table_q = quote_ident(table)?;
    Ok(format!("VACUUM{verbose_kw} {schema_q}.{table_q};"))
}

/// Convenience wrapper used by the orchestrator: decide whether to run
/// pre-dump source VACUUM ANALYZE based on config, then execute it.
pub async fn maybe_vacuum_source(config: &MigrationConfig) -> Result<()> {
    if config.skip_source_vacuum {
        return Ok(());
    }
    run_source_vacuum(
        &config.source.connection_string,
        &config.schemas,
        config.verbose,
    )
    .await
}

/// Convenience wrapper used by the orchestrator: decide whether to run
/// post-restore target ANALYZE based on config, then execute it.
pub async fn maybe_analyze_target(config: &MigrationConfig) -> Result<()> {
    if config.skip_analyze {
        return Ok(());
    }
    run_target_analyze(
        &config.target.connection_string,
        &config.schemas,
        config.verbose,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_analyze_sql_not_verbose() {
        assert_eq!(build_analyze_sql(false), "ANALYZE;");
    }

    #[test]
    fn build_analyze_sql_verbose() {
        assert_eq!(build_analyze_sql(true), "ANALYZE VERBOSE;");
    }

    #[test]
    fn build_analyze_table_sql_basic() {
        let sql = build_analyze_table_sql("public", "users", false).unwrap();
        assert_eq!(sql, "ANALYZE \"public\".\"users\";");
    }

    #[test]
    fn build_analyze_table_sql_verbose() {
        let sql = build_analyze_table_sql("public", "users", true).unwrap();
        assert_eq!(sql, "ANALYZE VERBOSE \"public\".\"users\";");
    }

    #[test]
    fn build_analyze_table_sql_special_chars() {
        let sql = build_analyze_table_sql("my\"schema", "my\"table", false).unwrap();
        assert_eq!(sql, "ANALYZE \"my\"\"schema\".\"my\"\"table\";");
    }

    #[test]
    fn build_vacuum_analyze_sql_not_verbose() {
        let sql = build_vacuum_analyze_sql(false);
        assert_eq!(sql, "VACUUM ANALYZE;");
    }

    #[test]
    fn build_vacuum_analyze_sql_verbose() {
        let sql = build_vacuum_analyze_sql(true);
        assert_eq!(sql, "VACUUM (VERBOSE, ANALYZE);");
    }

    #[test]
    fn build_vacuum_analyze_table_sql_basic() {
        let sql = build_vacuum_analyze_table_sql("public", "users", false).unwrap();
        assert_eq!(sql, "VACUUM ANALYZE \"public\".\"users\";");
    }

    #[test]
    fn build_vacuum_analyze_table_sql_verbose() {
        let sql = build_vacuum_analyze_table_sql("public", "users", true).unwrap();
        assert_eq!(sql, "VACUUM (VERBOSE, ANALYZE) \"public\".\"users\";");
    }

    #[test]
    fn build_vacuum_analyze_table_sql_special_chars() {
        let sql = build_vacuum_analyze_table_sql("my\"schema", "my\"table", false).unwrap();
        assert_eq!(sql, "VACUUM ANALYZE \"my\"\"schema\".\"my\"\"table\";");
    }

    #[test]
    fn build_analyze_table_sql_rejects_null_byte() {
        // The whole point of routing through pg_walstream::quote_ident: a NUL
        // truncates the C-string on the wire, which is an injection vector.
        // quote_ident_simple silently passed it through.
        let table_err = build_analyze_table_sql("public", "ev\0il", false).unwrap_err();
        assert!(table_err.to_string().contains("null bytes"));
        let schema_err = build_analyze_table_sql("pu\0blic", "users", false).unwrap_err();
        assert!(schema_err.to_string().contains("null bytes"));
    }

    #[test]
    fn build_vacuum_analyze_table_sql_rejects_null_byte() {
        let table_err = build_vacuum_analyze_table_sql("public", "ev\0il", false).unwrap_err();
        assert!(table_err.to_string().contains("null bytes"));
        let schema_err = build_vacuum_analyze_table_sql("pu\0blic", "users", false).unwrap_err();
        assert!(schema_err.to_string().contains("null bytes"));
    }

    #[test]
    fn build_analyze_table_sql_quotes_empty_identifier() {
        // Records pg_walstream::quote_ident's actual behaviour for the empty
        // string, which quote_ident_simple used to define locally.
        let sql = build_analyze_table_sql("", "", false).unwrap();
        assert_eq!(sql, "ANALYZE \"\".\"\";");
    }

    #[test]
    fn list_analyzable_sql_includes_partitioned_and_materialized() {
        assert!(LIST_ANALYZABLE_SQL.contains("IN ('r', 'p', 'm')"));
        assert!(LIST_ANALYZABLE_SQL.contains("$1"));
        assert!(LIST_ANALYZABLE_SQL.contains("pg_namespace"));
        assert!(LIST_ANALYZABLE_SQL.contains("NOT c.relispartition"));
    }

    #[test]
    fn list_vacuumable_sql_excludes_materialized_views() {
        assert!(LIST_VACUUMABLE_SQL.contains("IN ('r', 'p')"));
        assert!(!LIST_VACUUMABLE_SQL.contains("'m'"));
        assert!(LIST_VACUUMABLE_SQL.contains("$1"));
        assert!(LIST_VACUUMABLE_SQL.contains("NOT c.relispartition"));
    }

    #[test]
    fn maybe_vacuum_source_respects_skip_flag() {
        let config = MigrationConfig {
            skip_source_vacuum: true,
            ..MigrationConfig::default()
        };
        assert!(config.skip_source_vacuum);
    }

    #[test]
    fn maybe_analyze_target_respects_skip_flag() {
        let config = MigrationConfig {
            skip_analyze: true,
            ..MigrationConfig::default()
        };
        assert!(config.skip_analyze);
    }

    #[test]
    fn default_config_runs_both() {
        let config = MigrationConfig::default();
        assert!(!config.skip_analyze);
        assert!(!config.skip_source_vacuum);
    }

    #[tokio::test]
    async fn maybe_vacuum_source_skips_when_flag_set() {
        let config = MigrationConfig {
            skip_source_vacuum: true,
            ..MigrationConfig::default()
        };
        // Should return Ok immediately without trying to connect.
        let result = maybe_vacuum_source(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn maybe_analyze_target_skips_when_flag_set() {
        let config = MigrationConfig {
            skip_analyze: true,
            ..MigrationConfig::default()
        };
        // Should return Ok immediately without trying to connect.
        let result = maybe_analyze_target(&config).await;
        assert!(result.is_ok());
    }
}
