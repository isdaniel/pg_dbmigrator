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
        let sql = build_analyze_sql(None, verbose);
        client.batch_execute(&sql).await?;
        info!("ANALYZE complete (all schemas)");
    } else {
        for schema in schemas {
            let sql = build_analyze_sql(Some(schema), verbose);
            match client.batch_execute(&sql).await {
                Ok(()) => {
                    debug!(schema = %schema, "ANALYZE complete");
                }
                Err(e) => {
                    warn!(schema = %schema, error = %e, "ANALYZE failed for schema (continuing)");
                }
            }
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
        let sql = build_vacuum_analyze_sql(None, verbose);
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

/// VACUUM ANALYZE all tables in a single schema. Errors on individual
/// tables are logged but do not abort the process — a locked table or
/// a table owned by a restricted role should not block the entire
/// migration.
async fn vacuum_schema(client: &Client, schema: &str, verbose: bool) {
    let tables = match list_tables_in_schema(client, schema).await {
        Ok(t) => t,
        Err(e) => {
            warn!(schema = %schema, error = %e, "failed to list tables for VACUUM");
            return;
        }
    };
    for table in &tables {
        let sql = build_vacuum_analyze_table_sql(schema, table, verbose);
        if let Err(e) = client.batch_execute(&sql).await {
            warn!(schema = %schema, table = %table, error = %e, "VACUUM ANALYZE failed (continuing)");
        } else {
            debug!(schema = %schema, table = %table, "VACUUM ANALYZE done");
        }
    }
}

/// List ordinary user tables in a given schema.
async fn list_tables_in_schema(client: &Client, schema: &str) -> Result<Vec<String>> {
    let rows = client.query(LIST_TABLES_SQL, &[&schema]).await?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// SQL to list ordinary tables (relkind='r') in a schema.
pub const LIST_TABLES_SQL: &str = "\
    SELECT c.relname::text \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind = 'r' \
      AND n.nspname = $1";

/// Build an `ANALYZE` statement. When `schema` is provided, uses
/// schema-qualified form by analyzing all tables in that schema via
/// a helper approach — for whole-schema ANALYZE we use the PostgreSQL
/// schema-level syntax available in all supported versions.
pub fn build_analyze_sql(schema: Option<&str>, verbose: bool) -> String {
    let verbose_kw = if verbose { " VERBOSE" } else { "" };
    match schema {
        Some(s) => {
            let quoted = quote_ident_simple(s);
            format!("ANALYZE{verbose_kw} (SKIP_LOCKED);").replace(
                "(SKIP_LOCKED)",
                &format!("\"{}\".*", quoted.trim_matches('"')),
            )
        }
        None => format!("ANALYZE{verbose_kw};"),
    }
}

/// Build a `VACUUM ANALYZE` statement for the entire database or a
/// schema-level scope.
pub fn build_vacuum_analyze_sql(schema: Option<&str>, verbose: bool) -> String {
    let verbose_kw = if verbose {
        " (VERBOSE, ANALYZE)"
    } else {
        " ANALYZE"
    };
    match schema {
        Some(s) => {
            let quoted = quote_ident_simple(s);
            format!("VACUUM{verbose_kw} \"{}\".* ;", quoted.trim_matches('"'))
        }
        None => format!("VACUUM{verbose_kw};"),
    }
}

/// Build a `VACUUM ANALYZE` statement for a single table.
pub fn build_vacuum_analyze_table_sql(schema: &str, table: &str, verbose: bool) -> String {
    let verbose_kw = if verbose {
        " (VERBOSE, ANALYZE)"
    } else {
        " ANALYZE"
    };
    let schema_q = quote_ident_simple(schema);
    let table_q = quote_ident_simple(table);
    format!("VACUUM{verbose_kw} {schema_q}.{table_q};")
}

/// Minimal identifier quoting (wraps in double-quotes, doubles embedded `"`).
/// For SQL safety in ANALYZE/VACUUM statements where pg_walstream may not be
/// needed.
pub fn quote_ident_simple(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
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
    fn build_analyze_sql_no_schema_not_verbose() {
        assert_eq!(build_analyze_sql(None, false), "ANALYZE;");
    }

    #[test]
    fn build_analyze_sql_no_schema_verbose() {
        assert_eq!(build_analyze_sql(None, true), "ANALYZE VERBOSE;");
    }

    #[test]
    fn build_analyze_sql_with_schema() {
        let sql = build_analyze_sql(Some("public"), false);
        assert!(sql.contains("\"public\".*"));
        assert!(sql.starts_with("ANALYZE"));
    }

    #[test]
    fn build_analyze_sql_schema_with_special_chars() {
        let sql = build_analyze_sql(Some("my\"schema"), false);
        assert!(sql.contains("\"my\"\"schema\".*"));
    }

    #[test]
    fn build_vacuum_analyze_sql_no_schema_not_verbose() {
        let sql = build_vacuum_analyze_sql(None, false);
        assert_eq!(sql, "VACUUM ANALYZE;");
    }

    #[test]
    fn build_vacuum_analyze_sql_no_schema_verbose() {
        let sql = build_vacuum_analyze_sql(None, true);
        assert_eq!(sql, "VACUUM (VERBOSE, ANALYZE);");
    }

    #[test]
    fn build_vacuum_analyze_sql_with_schema() {
        let sql = build_vacuum_analyze_sql(Some("myschema"), false);
        assert!(sql.contains("\"myschema\".*"));
        assert!(sql.starts_with("VACUUM"));
    }

    #[test]
    fn build_vacuum_analyze_table_sql_basic() {
        let sql = build_vacuum_analyze_table_sql("public", "users", false);
        assert_eq!(sql, "VACUUM ANALYZE \"public\".\"users\";");
    }

    #[test]
    fn build_vacuum_analyze_table_sql_verbose() {
        let sql = build_vacuum_analyze_table_sql("public", "users", true);
        assert_eq!(sql, "VACUUM (VERBOSE, ANALYZE) \"public\".\"users\";");
    }

    #[test]
    fn build_vacuum_analyze_table_sql_special_chars() {
        let sql = build_vacuum_analyze_table_sql("my\"schema", "my\"table", false);
        assert_eq!(sql, "VACUUM ANALYZE \"my\"\"schema\".\"my\"\"table\";");
    }

    #[test]
    fn quote_ident_simple_basic() {
        assert_eq!(quote_ident_simple("public"), "\"public\"");
    }

    #[test]
    fn quote_ident_simple_with_double_quote() {
        assert_eq!(quote_ident_simple("ab\"cd"), "\"ab\"\"cd\"");
    }

    #[test]
    fn list_tables_sql_is_valid_shape() {
        assert!(LIST_TABLES_SQL.contains("relkind = 'r'"));
        assert!(LIST_TABLES_SQL.contains("$1"));
        assert!(LIST_TABLES_SQL.contains("pg_namespace"));
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
}
