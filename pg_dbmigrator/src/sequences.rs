//! Sequence sync at cutover (online mode only).
//!
//! ## Why this module exists
//!
//! PostgreSQL logical replication does **not** replicate sequence advances: `nextval()` is not a logged WAL operation, and apply workers ignore it. The classic failure mode of an "online" migration! that finishes successfully is therefore:
//!
//! 1. `pg_dump --snapshot=…` captures sequence values at snapshot time
//!    (e.g. `last_value=100`).
//! 2. `pg_restore` applies those values to the target.
//! 3. The streaming apply phase replicates *rows* (including rows whose
//!    `id` came from `nextval()` on the source — `id=101, 102, …`).
//! 4. The target's sequence is **still at 100** because the apply worker
//!    never saw the `nextval()` calls.
//! 5. After cutover the application connects to the target and runs
//!    `INSERT … DEFAULT` — the target's sequence returns `101`, but
//!    `id=101` is already in the table → duplicate-key violation.
//!
//! ## What this module does
//!
//! Just before `run_native_apply` returns from a cutover, the orchestrator calls [`sync_sequences`]. We:
//!
//! 1. Open fresh non-replication connections to the source and target.
//! 2. Read every user sequence's current `last_value` from the source
//!    via [`pg_sequence_last_value(regclass)`].
//! 3. Issue `setval('"schema"."seq"', last_value, true)` on the target.
//!
//! Sequences that the source has never advanced (`pg_sequence_last_value`
//! returns `NULL`) are skipped — touching them on the target would
//! disturb their initial-state semantics for no benefit.
//!
//! [`pg_sequence_last_value(regclass)`]: https://www.postgresql.org/docs/current/functions-info.html

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::tls::connect_with_sslmode;

/// One sequence's source-side state, ready to be applied to the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSequence {
    /// Schema (e.g. `public`).
    pub schema: String,
    /// Sequence name (e.g. `widgets_id_seq`).
    pub name: String,
    /// `pg_sequence_last_value(regclass)`. `None` when the sequence has
    /// never been advanced on the source — those are skipped on apply.
    pub last_value: Option<i64>,
}

/// SQL fragment that lists all user sequences and their current
/// `last_value`. The query excludes the system catalogs and
/// `information_schema` so we don't trample shared metadata.
///
/// Public for unit tests so we can confirm the query shape stays stable.
pub const COLLECT_SEQUENCES_SQL_NO_FILTER: &str = "\
    SELECT n.nspname::text, c.relname::text, \
           pg_sequence_last_value(c.oid::regclass) AS last_value \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind = 'S' \
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
      AND n.nspname NOT LIKE 'pg_temp_%' \
      AND n.nspname NOT LIKE 'pg_toast_temp_%'";

/// Same as [`COLLECT_SEQUENCES_SQL_NO_FILTER`] but restricted to a
/// caller-supplied list of schemas. The schema list is bound as a
/// `text[]` parameter (`$1`) — never interpolated — so this is safe
/// against SQL injection regardless of what the operator passed on the
/// command line.
pub const COLLECT_SEQUENCES_SQL_WITH_SCHEMA_FILTER: &str = "\
    SELECT n.nspname::text, c.relname::text, \
           pg_sequence_last_value(c.oid::regclass) AS last_value \
    FROM pg_class c \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE c.relkind = 'S' \
      AND n.nspname = ANY($1::text[])";

/// Read every user sequence's current `last_value` from the source.
///
/// `schema_filter`:
/// * empty → every user schema is scanned.
/// * non-empty → only sequences in those schemas are returned.
pub async fn collect_source_sequences(
    source: &Client,
    schema_filter: &[String],
) -> Result<Vec<SourceSequence>> {
    let rows = if schema_filter.is_empty() {
        source.query(COLLECT_SEQUENCES_SQL_NO_FILTER, &[]).await?
    } else {
        source
            .query(COLLECT_SEQUENCES_SQL_WITH_SCHEMA_FILTER, &[&schema_filter])
            .await?
    };

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(SourceSequence {
            schema: row.get(0),
            name: row.get(1),
            last_value: row.get(2),
        });
    }
    Ok(out)
}

/// Abstraction over the target-side database operations needed by the
/// sequence sync logic, following the [`CommandRunner`](crate::dump::CommandRunner)
/// pattern so unit tests can substitute a mock without a real PostgreSQL.
#[async_trait]
pub(crate) trait SeqSyncTarget: Send + Sync {
    async fn execute_setval(&self, sql: &str, last_value: i64) -> Result<u64>;
    async fn batch_execute_sql(&self, sql: &str) -> Result<()>;
    async fn query_batch_applied(&self) -> Result<i32>;
}

#[async_trait]
impl SeqSyncTarget for Client {
    async fn execute_setval(&self, sql: &str, last_value: i64) -> Result<u64> {
        Ok(self.execute(sql, &[&last_value]).await?)
    }
    async fn batch_execute_sql(&self, sql: &str) -> Result<()> {
        Ok(Client::batch_execute(self, sql).await?)
    }
    async fn query_batch_applied(&self) -> Result<i32> {
        let row = self
            .query_one("SELECT applied FROM _seq_sync_result", &[])
            .await?;
        Ok(row.get(0))
    }
}

/// Apply the source-side sequence state to the target via `setval(...)`.
///
/// Sequences whose source-side `last_value` is `None` are skipped — they
/// have never been advanced on the source, and re-`setval`-ing them
/// would change the target's "is_called=false" semantics for no reason.
///
/// Failures on individual sequences are logged with `warn!` but do not
/// abort the whole sync — a broken sequence on a managed-PG target
/// (e.g. owned by a role we can't act on) shouldn't block the rest of
/// the migration from completing. The function returns the number of
/// sequences successfully applied.
///
/// When more than one sequence needs syncing, a single PL/pgSQL `DO`
/// block is executed to reduce network round-trips. Each `setval()`
/// inside the block is wrapped in its own exception handler so a failure
/// on one sequence doesn't prevent the others from being applied. If the
/// batched approach fails entirely (e.g. older PG without PL/pgSQL), we
/// fall back to individual statements.
pub async fn apply_sequences_to_target(
    target: &Client,
    sequences: &[SourceSequence],
) -> Result<usize> {
    apply_sequences_impl(target, sequences).await
}

async fn apply_sequences_impl(
    target: &dyn SeqSyncTarget,
    sequences: &[SourceSequence],
) -> Result<usize> {
    let actionable: Vec<&SourceSequence> = sequences
        .iter()
        .filter(|s| {
            if s.last_value.is_none() {
                debug!(
                    schema = %s.schema,
                    name = %s.name,
                    "skipping sequence: never advanced on source",
                );
            }
            s.last_value.is_some()
        })
        .collect();

    if actionable.is_empty() {
        return Ok(0);
    }

    if actionable.len() == 1 {
        return apply_single(target, actionable[0]).await;
    }

    match apply_batch(target, &actionable).await {
        Ok(applied) => Ok(applied),
        Err(e) => {
            warn!(
                error = %e,
                "batch sequence sync failed — falling back to individual statements"
            );
            apply_individually(target, &actionable).await
        }
    }
}

/// Build a PL/pgSQL `DO` block that applies all sequences in one round-trip.
/// Each setval is wrapped in a sub-block with exception handling so that a
/// failure on one sequence does not abort the rest.
///
/// Public for unit tests.
pub fn build_batch_setval_sql(sequences: &[&SourceSequence]) -> Result<String> {
    let mut body = String::new();
    body.push_str(
        "CREATE TEMP TABLE IF NOT EXISTS _seq_sync_result (applied int) ON COMMIT DROP;\n",
    );
    body.push_str("TRUNCATE _seq_sync_result;\n");
    body.push_str("DO $seq_sync$\nDECLARE\n  _applied int := 0;\nBEGIN\n");
    for seq in sequences {
        let last_value = seq.last_value.unwrap_or(0);
        let qualified = format!(
            "{}.{}",
            pg_walstream::quote_ident(&seq.schema)?,
            pg_walstream::quote_ident(&seq.name)?,
        );
        let qualified_lit = pg_walstream::quote_literal(&qualified)?;
        body.push_str("  BEGIN\n");
        body.push_str(&format!(
            "    PERFORM setval({qualified_lit}::regclass, {last_value}::bigint, true);\n"
        ));
        body.push_str("    _applied := _applied + 1;\n");
        body.push_str("  EXCEPTION WHEN OTHERS THEN\n");
        body.push_str(&format!(
            "    RAISE WARNING 'setval failed for {}: %', SQLERRM;\n",
            qualified.replace('\'', "''").replace('%', "%%")
        ));
        body.push_str("  END;\n");
    }
    body.push_str("  INSERT INTO _seq_sync_result VALUES (_applied);\n");
    body.push_str("END;\n$seq_sync$;");
    Ok(body)
}

/// Execute the batched PL/pgSQL block and parse the applied count from the
/// RAISE NOTICE output.
async fn apply_batch(target: &dyn SeqSyncTarget, sequences: &[&SourceSequence]) -> Result<usize> {
    let sql = build_batch_setval_sql(sequences)?;
    target.batch_execute_sql(&sql).await?;
    let applied = target.query_batch_applied().await?;
    for seq in sequences {
        debug!(schema = %seq.schema, name = %seq.name, "synced sequence (batch)");
    }
    Ok(applied as usize)
}

/// Apply a single sequence (used when there's only one).
async fn apply_single(target: &dyn SeqSyncTarget, seq: &SourceSequence) -> Result<usize> {
    let last_value = seq.last_value.unwrap_or(0);
    let sql = build_setval_sql(&seq.schema, &seq.name)?;
    match target.execute_setval(&sql, last_value).await {
        Ok(_) => {
            debug!(schema = %seq.schema, name = %seq.name, last_value, "synced sequence");
            Ok(1)
        }
        Err(e) => {
            warn!(
                schema = %seq.schema,
                name = %seq.name,
                error = %e,
                "failed to sync sequence (continuing)"
            );
            Ok(0)
        }
    }
}

/// Sequential per-statement fallback.
async fn apply_individually(
    target: &dyn SeqSyncTarget,
    sequences: &[&SourceSequence],
) -> Result<usize> {
    let mut applied = 0usize;
    for seq in sequences {
        let last_value = seq.last_value.unwrap_or(0);
        let sql = build_setval_sql(&seq.schema, &seq.name)?;
        match target.execute_setval(&sql, last_value).await {
            Ok(_) => {
                applied += 1;
                debug!(schema = %seq.schema, name = %seq.name, last_value, "synced sequence");
            }
            Err(e) => {
                warn!(
                    schema = %seq.schema,
                    name = %seq.name,
                    error = %e,
                    "failed to sync sequence (continuing)"
                );
            }
        }
    }
    Ok(applied)
}

/// Build the SQL that advances `<schema>.<name>` to a parameter-bound
/// `bigint` (`$1`).
///
/// The schema- and sequence-name halves are escaped with
/// [`pg_walstream::quote_ident`] (doubles `"`) and the resulting
/// `"schema"."name"` string is then escaped with
/// [`pg_walstream::quote_literal`] (doubles `'`). Both layers are
/// required: identifiers may contain single quotes, and literals may
/// contain double quotes. The final SQL passes a single-quoted
/// `regclass` string to `setval`, which is the canonical way to refer
/// to a schema-qualified sequence.
///
/// Public so unit tests can validate the SQL shape without touching
/// PostgreSQL.
pub fn build_setval_sql(schema: &str, name: &str) -> Result<String> {
    let qualified = format!(
        "{}.{}",
        pg_walstream::quote_ident(schema)?,
        pg_walstream::quote_ident(name)?,
    );
    let qualified_lit = pg_walstream::quote_literal(&qualified)?;
    Ok(format!(
        "SELECT setval({qualified_lit}::regclass, $1::bigint, true)"
    ))
}

/// Top-level helper called from the orchestrator after a cutover-driven
/// exit. Opens its own connections and never reuses the lag-poller's
/// state — sequence sync is a one-shot administrative step that
/// shouldn't entangle with the streaming apply path.
pub async fn sync_sequences(
    source_conn: &str,
    target_conn: &str,
    schema_filter: &[String],
) -> Result<usize> {
    info!("syncing sequences from source to target");
    let source = connect_with_sslmode(source_conn).await?;
    let target = connect_with_sslmode(target_conn).await?;
    let seqs = collect_source_sequences(&source, schema_filter).await?;
    let total = seqs.len();
    info!(total, "collected sequences from source");
    let applied = apply_sequences_to_target(&target, &seqs).await?;
    info!(applied, skipped = total - applied, "sequence sync complete");
    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_sql_no_filter_includes_pg_sequence_last_value_function() {
        // Critical query shape: pg_sequence_last_value(regclass) is the
        // only way to read a sequence's last_value cheaply without
        // grabbing a row-lock on the sequence relation. Don't lose it.
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("pg_sequence_last_value"));
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("c.relkind = 'S'"));
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("pg_catalog"));
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("information_schema"));
    }

    #[test]
    fn collect_sql_with_schema_filter_uses_parameterised_array() {
        // Schemas are bound as an array param — never string-interpolated
        // — so an operator that passes `--schema 'foo''bar'` cannot break
        // out into arbitrary SQL.
        assert!(COLLECT_SEQUENCES_SQL_WITH_SCHEMA_FILTER.contains("$1::text[]"));
        assert!(COLLECT_SEQUENCES_SQL_WITH_SCHEMA_FILTER.contains("ANY"));
    }

    #[test]
    fn source_sequence_serde_roundtrip() {
        let s = SourceSequence {
            schema: "public".into(),
            name: "widgets_id_seq".into(),
            last_value: Some(100),
        };
        let json = serde_json::to_string(&s).unwrap();
        let s2: SourceSequence = serde_json::from_str(&json).unwrap();
        assert_eq!(s, s2);
    }

    #[test]
    fn source_sequence_handles_never_advanced() {
        let s = SourceSequence {
            schema: "public".into(),
            name: "fresh_seq".into(),
            last_value: None,
        };
        // The "skip if last_value is None" rule is the contract that
        // protects untouched sequences on the source from being
        // re-baselined on the target.
        assert!(s.last_value.is_none());
    }

    #[test]
    fn build_setval_sql_quotes_identifiers_and_literal() {
        // Plain identifiers — both halves get double-quoted and the
        // qualified name is wrapped in single quotes for the regclass
        // cast.
        let sql = build_setval_sql("public", "widgets_id_seq").unwrap();
        assert_eq!(
            sql,
            "SELECT setval('\"public\".\"widgets_id_seq\"'::regclass, $1::bigint, true)"
        );
    }

    #[test]
    fn build_setval_sql_escapes_embedded_double_quote() {
        // An identifier containing `"` must round-trip — `quote_ident`
        // doubles it.
        let sql = build_setval_sql("we\"ird", "seq").unwrap();
        assert!(sql.contains("\"we\"\"ird\""));
        assert!(sql.contains("'\"we\"\"ird\".\"seq\"'::regclass"));
    }

    #[test]
    fn build_setval_sql_escapes_embedded_single_quote() {
        // An identifier containing `'` (rare but legal) must be
        // double-up-escaped at the literal layer so SQL can parse the
        // resulting `'...'::regclass` string literal cleanly.
        let sql = build_setval_sql("public", "o'reilly").unwrap();
        assert!(sql.contains("''"));
        assert!(sql.starts_with("SELECT setval('"));
        assert!(sql.contains("::regclass, $1::bigint, true)"));
    }

    #[test]
    fn build_setval_sql_handles_spaces_in_identifiers() {
        let sql = build_setval_sql("my schema", "my seq").unwrap();
        assert!(sql.contains("\"my schema\""));
        assert!(sql.contains("\"my seq\""));
    }

    #[test]
    fn build_setval_sql_handles_backtick_in_identifiers() {
        let sql = build_setval_sql("pub`lic", "seq`name").unwrap();
        assert!(sql.contains("\"pub`lic\""));
        assert!(sql.contains("\"seq`name\""));
    }

    #[test]
    fn source_sequence_none_last_value_serde_roundtrip() {
        let s = SourceSequence {
            schema: "public".into(),
            name: "fresh".into(),
            last_value: None,
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("null"));
        let s2: SourceSequence = serde_json::from_str(&json).unwrap();
        assert_eq!(s, s2);
        assert!(s2.last_value.is_none());
    }

    #[test]
    fn collect_sql_no_filter_excludes_temp_schemas() {
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("pg_temp_%"));
        assert!(COLLECT_SEQUENCES_SQL_NO_FILTER.contains("pg_toast_temp_%"));
    }

    #[test]
    fn collect_sql_with_schema_filter_uses_any_operator() {
        assert!(COLLECT_SEQUENCES_SQL_WITH_SCHEMA_FILTER.contains("ANY($1::text[])"));
    }

    #[test]
    fn build_batch_setval_sql_produces_valid_plpgsql() {
        let seqs = [
            SourceSequence {
                schema: "public".into(),
                name: "users_id_seq".into(),
                last_value: Some(42),
            },
            SourceSequence {
                schema: "public".into(),
                name: "orders_id_seq".into(),
                last_value: Some(100),
            },
        ];
        let refs: Vec<&SourceSequence> = seqs.iter().collect();
        let sql = build_batch_setval_sql(&refs).unwrap();
        assert!(sql.contains("CREATE TEMP TABLE IF NOT EXISTS _seq_sync_result"));
        assert!(sql.contains("DO $seq_sync$"));
        assert!(sql.ends_with("$seq_sync$;"));
        assert!(sql.contains("PERFORM setval"));
        assert!(sql.contains("42::bigint"));
        assert!(sql.contains("100::bigint"));
        assert!(sql.contains("EXCEPTION WHEN OTHERS"));
        assert!(sql.contains("_applied := _applied + 1"));
        assert!(sql.contains("INSERT INTO _seq_sync_result VALUES (_applied)"));
    }

    #[test]
    fn build_batch_setval_sql_escapes_special_chars() {
        let seqs = [SourceSequence {
            schema: "my\"schema".into(),
            name: "o'reilly_seq".into(),
            last_value: Some(7),
        }];
        let refs: Vec<&SourceSequence> = seqs.iter().collect();
        let sql = build_batch_setval_sql(&refs).unwrap();
        assert!(sql.contains("\"my\"\"schema\""));
        assert!(sql.contains("7::bigint"));
    }

    #[test]
    fn build_batch_setval_sql_escapes_percent_in_raise_warning() {
        let seqs = [SourceSequence {
            schema: "public".into(),
            name: "pct%seq".into(),
            last_value: Some(1),
        }];
        let refs: Vec<&SourceSequence> = seqs.iter().collect();
        let sql = build_batch_setval_sql(&refs).unwrap();
        assert!(
            sql.contains("%%"),
            "percent signs in identifiers must be doubled for RAISE WARNING"
        );
    }

    #[test]
    fn build_batch_setval_sql_empty_input() {
        let refs: Vec<&SourceSequence> = vec![];
        let sql = build_batch_setval_sql(&refs).unwrap();
        assert!(sql.contains("DO $seq_sync$"));
        assert!(!sql.contains("PERFORM setval"));
        assert!(sql.contains("INSERT INTO _seq_sync_result VALUES (_applied)"));
    }

    // ── Mock-based async tests ──────────────────────────────────────────────

    use crate::error::MigrationError;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct MockTarget {
        setval_results: Mutex<VecDeque<Result<u64>>>,
        batch_exec_result: Mutex<Option<Result<()>>>,
        batch_applied: Mutex<Option<Result<i32>>>,
    }

    impl MockTarget {
        fn ok(applied_count: i32) -> Self {
            Self {
                setval_results: Mutex::new(VecDeque::new()),
                batch_exec_result: Mutex::new(Some(Ok(()))),
                batch_applied: Mutex::new(Some(Ok(applied_count))),
            }
        }

        fn batch_fails() -> Self {
            Self {
                setval_results: Mutex::new(VecDeque::new()),
                batch_exec_result: Mutex::new(Some(Err(MigrationError::config(
                    "batch not supported",
                )))),
                batch_applied: Mutex::new(None),
            }
        }

        fn with_setval_results(mut self, results: Vec<Result<u64>>) -> Self {
            self.setval_results = Mutex::new(results.into());
            self
        }
    }

    #[async_trait]
    impl SeqSyncTarget for MockTarget {
        async fn execute_setval(&self, _sql: &str, _last_value: i64) -> Result<u64> {
            self.setval_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(1))
        }
        async fn batch_execute_sql(&self, _sql: &str) -> Result<()> {
            self.batch_exec_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or(Ok(()))
        }
        async fn query_batch_applied(&self) -> Result<i32> {
            self.batch_applied.lock().unwrap().take().unwrap_or(Ok(0))
        }
    }

    fn seq(schema: &str, name: &str, val: Option<i64>) -> SourceSequence {
        SourceSequence {
            schema: schema.into(),
            name: name.into(),
            last_value: val,
        }
    }

    #[tokio::test]
    async fn apply_all_none_last_value_returns_zero() {
        let target = MockTarget::ok(0);
        let seqs = vec![seq("public", "s1", None), seq("public", "s2", None)];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 0);
    }

    #[tokio::test]
    async fn apply_empty_sequences_returns_zero() {
        let target = MockTarget::ok(0);
        let applied = apply_sequences_impl(&target, &[]).await.unwrap();
        assert_eq!(applied, 0);
    }

    #[tokio::test]
    async fn apply_single_sequence_success() {
        let target = MockTarget::ok(0).with_setval_results(vec![Ok(1)]);
        let seqs = vec![seq("public", "users_id_seq", Some(42))];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn apply_single_sequence_failure_returns_zero() {
        let target = MockTarget::ok(0)
            .with_setval_results(vec![Err(MigrationError::config("permission denied"))]);
        let seqs = vec![seq("public", "users_id_seq", Some(42))];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 0);
    }

    #[tokio::test]
    async fn apply_batch_success() {
        let target = MockTarget::ok(2);
        let seqs = vec![seq("public", "s1", Some(10)), seq("public", "s2", Some(20))];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn apply_batch_reports_partial_success() {
        let target = MockTarget::ok(1);
        let seqs = vec![seq("public", "s1", Some(10)), seq("public", "s2", Some(20))];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn apply_batch_failure_falls_back_to_individual() {
        let target = MockTarget::batch_fails().with_setval_results(vec![Ok(1), Ok(1)]);
        let seqs = vec![seq("public", "s1", Some(10)), seq("public", "s2", Some(20))];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn apply_individually_mixed_results() {
        let target = MockTarget::batch_fails().with_setval_results(vec![
            Ok(1),
            Err(MigrationError::config("fail")),
            Ok(1),
        ]);
        let seqs = vec![
            seq("public", "s1", Some(1)),
            seq("public", "s2", Some(2)),
            seq("public", "s3", Some(3)),
        ];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn apply_filters_none_and_routes_remaining() {
        let target = MockTarget::ok(0).with_setval_results(vec![Ok(1)]);
        let seqs = vec![
            seq("public", "never_used", None),
            seq("public", "used_seq", Some(99)),
        ];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn apply_filters_none_and_routes_to_batch() {
        let target = MockTarget::ok(2);
        let seqs = vec![
            seq("public", "skip_me", None),
            seq("public", "s1", Some(10)),
            seq("public", "s2", Some(20)),
        ];
        let applied = apply_sequences_impl(&target, &seqs).await.unwrap();
        assert_eq!(applied, 2);
    }
}
