//! Post-migration verification: compare per-table `count(*)` between source
//! and target so the operator can confirm the copy is complete before cutover.
//!
//! The diff logic ([`VerifyReport`]) is pure and unit-tested without a
//! database; [`verify_row_counts`] performs the I/O.

use tokio_postgres::error::SqlState;
use tracing::{info, warn};

use crate::config::MigrationConfig;
use crate::error::Result;
use crate::preflight::{filter_tables_by_exclusions, quote_qualified_name};
use crate::tls::connect_with_sslmode;

/// Lists user tables to verify: ordinary tables that are not partition
/// children (`relkind='r' AND NOT relispartition`) plus partitioned parents
/// (`relkind='p'`). Counting the parent aggregates its partitions, so child
/// partitions are intentionally excluded to avoid double counting.
///
/// ponytail: exact `count(*)` per table is O(rows); acceptable for a
/// correctness gate. Upgrade path is a sampled/checksum mode if it ever bites.
const LIST_TABLES_SQL: &str = "\
    SELECT n.nspname::text, c.relname::text \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE ((c.relkind = 'r' AND NOT c.relispartition) OR c.relkind = 'p') \
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
      AND n.nspname NOT LIKE 'pg_temp_%' \
      AND n.nspname NOT LIKE 'pg_toast_temp_%'";

/// One table's row count on both endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableCount {
    /// Schema name (unquoted).
    pub schema: String,
    /// Table name (unquoted).
    pub table: String,
    /// `count(*)` on the source.
    pub source: i64,
    /// `count(*)` on the target.
    pub target: i64,
}

/// Result of a verification pass: the per-table counts and a derived view of
/// which tables disagree.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    rows: Vec<TableCount>,
}

impl VerifyReport {
    /// Build a report from collected per-table counts.
    pub fn new(rows: Vec<TableCount>) -> Self {
        Self { rows }
    }

    /// All per-table counts, in the order they were collected.
    pub fn rows(&self) -> &[TableCount] {
        &self.rows
    }

    /// Tables whose source and target counts differ.
    pub fn mismatches(&self) -> Vec<&TableCount> {
        self.rows.iter().filter(|r| r.source != r.target).collect()
    }

    /// `true` when every table matched.
    pub fn is_ok(&self) -> bool {
        self.mismatches().is_empty()
    }

    /// One-line human summary, mirroring `PreflightReport::summary_line`.
    pub fn summary_line(&self) -> String {
        let total = self.rows.len();
        let bad = self.mismatches().len();
        if bad == 0 {
            format!("verify: {total} table(s) matched")
        } else {
            format!("verify: {bad}/{total} table(s) MISMATCHED")
        }
    }
}

/// Select which qualified `schema.table` names to verify, mirroring
/// pg_dump's include semantics: `--schema` and `--table` are additive
/// (union), not intersecting. With neither set, all candidate tables are
/// verified. Exclusions are always applied afterwards.
///
/// A candidate is kept when `schemas.is_empty() && tables.is_empty()`, or
/// when its schema (the part before the first `.`) is in `schemas`, or when
/// its full `schema.table` is in `tables`. The surviving set is then passed
/// through [`filter_tables_by_exclusions`].
pub(crate) fn select_tables_to_verify(
    candidates: &[String],
    schemas: &[String],
    tables: &[String],
    exclude_tables: &[String],
    exclude_schemas: &[String],
) -> Vec<String> {
    let no_includes = schemas.is_empty() && tables.is_empty();
    let kept: Vec<String> = candidates
        .iter()
        .filter(|qt| {
            if no_includes {
                return true;
            }
            let schema = qt.split_once('.').map(|(s, _)| s).unwrap_or("");
            schemas.iter().any(|inc| inc == schema) || tables.iter().any(|t| t == *qt)
        })
        .cloned()
        .collect();
    filter_tables_by_exclusions(&kept, exclude_tables, exclude_schemas)
}

/// Compare per-table `count(*)` between the source and the target for every
/// table in the configured include set (honouring `--schema`, `--table`,
/// `--exclude-schema`, `--exclude-table`).
///
/// Opens one connection to each endpoint and reuses it across tables. The two
/// count queries for each table run concurrently. Returns a [`VerifyReport`];
/// the caller decides whether a mismatch is fatal.
///
/// A table listed on the source but missing on the target (SQLSTATE
/// `42P01`/`3F000`) is reported as a mismatch with `target = 0` rather than
/// aborting the run; any other target error still propagates.
pub async fn verify_row_counts(cfg: &MigrationConfig) -> Result<VerifyReport> {
    let source = connect_with_sslmode(&cfg.source.connection_string).await?;
    let target = connect_with_sslmode(&cfg.target.connection_string).await?;

    // Enumerate candidate tables from the source, then apply the same
    // include/exclude filtering the dump/publication paths use.
    let rows = source.query(LIST_TABLES_SQL, &[]).await?;
    let qualified: Vec<String> = rows
        .iter()
        .map(|r| {
            let schema: String = r.get(0);
            let table: String = r.get(1);
            format!("{schema}.{table}")
        })
        .collect();

    let qualified = select_tables_to_verify(
        &qualified,
        &cfg.schemas,
        &cfg.tables,
        &cfg.exclude_tables,
        &cfg.exclude_schemas,
    );

    let mut out = Vec::with_capacity(qualified.len());
    for qt in qualified {
        let (schema, table) = match qt.split_once('.') {
            Some((s, t)) => (s.to_string(), t.to_string()),
            None => continue,
        };
        let q = format!("SELECT count(*) FROM {}", quote_qualified_name(&qt)?);
        let (s_res, t_res) = tokio::join!(source.query_one(&q, &[]), target.query_one(&q, &[]));
        let source_count: i64 = s_res?.get(0);
        // ponytail: a target table missing on the target (relation/schema does
        // not exist) is reported as a mismatch with target=0 instead of aborting
        // the run. An empty source table also absent on the target reads as
        // 0 vs 0 (a match) — acceptable for a row-count gate.
        let target_count: i64 = match t_res {
            Ok(row) => row.get(0),
            Err(e)
                if e.code() == Some(&SqlState::UNDEFINED_TABLE)
                    || e.code() == Some(&SqlState::UNDEFINED_SCHEMA) =>
            {
                warn!(
                    schema = %schema, table = %table,
                    source = source_count,
                    "table missing on target"
                );
                0
            }
            Err(e) => return Err(e.into()),
        };
        if source_count != target_count {
            warn!(
                schema = %schema, table = %table,
                source = source_count, target = target_count,
                "row-count mismatch"
            );
        }
        out.push(TableCount {
            schema,
            table,
            source: source_count,
            target: target_count,
        });
    }

    let report = VerifyReport::new(out);
    info!("{}", report.summary_line());
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tc(schema: &str, table: &str, s: i64, t: i64) -> TableCount {
        TableCount {
            schema: schema.into(),
            table: table.into(),
            source: s,
            target: t,
        }
    }

    #[test]
    fn mismatches_empty_when_all_equal() {
        let r = VerifyReport::new(vec![tc("public", "a", 5, 5), tc("public", "b", 0, 0)]);
        assert!(r.is_ok());
        assert!(r.mismatches().is_empty());
        assert_eq!(r.summary_line(), "verify: 2 table(s) matched");
    }

    #[test]
    fn mismatches_lists_only_unequal_rows() {
        let r = VerifyReport::new(vec![tc("public", "a", 5, 5), tc("public", "b", 7, 3)]);
        assert!(!r.is_ok());
        let m = r.mismatches();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].table, "b");
        assert_eq!(r.summary_line(), "verify: 1/2 table(s) MISMATCHED");
    }

    #[test]
    fn list_tables_sql_excludes_system_schemas_and_partitions() {
        assert!(LIST_TABLES_SQL.contains("NOT c.relispartition"));
        assert!(LIST_TABLES_SQL.contains("information_schema"));
        assert!(LIST_TABLES_SQL.contains("relkind = 'p'"));
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn selects_all_when_no_filter_set() {
        let candidates = s(&["app.a", "public.users", "other.x"]);
        let got = select_tables_to_verify(&candidates, &[], &[], &[], &[]);
        assert_eq!(got, candidates);
    }

    #[test]
    fn selects_schema_tables_when_only_schemas_set() {
        let candidates = s(&["app.a", "app.b", "public.users", "other.x"]);
        let got = select_tables_to_verify(&candidates, &s(&["app"]), &[], &[], &[]);
        assert_eq!(got, s(&["app.a", "app.b"]));
    }

    #[test]
    fn selects_exact_tables_when_only_tables_set() {
        let candidates = s(&["app.a", "app.b", "public.users", "other.x"]);
        let got = select_tables_to_verify(&candidates, &[], &s(&["public.users"]), &[], &[]);
        assert_eq!(got, s(&["public.users"]));
    }

    #[test]
    fn unions_when_both_schemas_and_tables_set() {
        let candidates = s(&["app.a", "app.b", "public.users", "other.x"]);
        let got =
            select_tables_to_verify(&candidates, &s(&["app"]), &s(&["public.users"]), &[], &[]);
        assert_eq!(got, s(&["app.a", "app.b", "public.users"]));
    }

    #[test]
    fn applies_exclusions_after_union() {
        let candidates = s(&["app.a", "app.b", "public.users", "other.x"]);
        // exclude one unioned table and one unioned schema entry.
        let got = select_tables_to_verify(
            &candidates,
            &s(&["app"]),
            &s(&["public.users"]),
            &s(&["app.b"]),
            &[],
        );
        assert_eq!(got, s(&["app.a", "public.users"]));

        let got_sch = select_tables_to_verify(
            &candidates,
            &s(&["app"]),
            &s(&["public.users"]),
            &[],
            &s(&["public"]),
        );
        assert_eq!(got_sch, s(&["app.a", "app.b"]));
    }
}
