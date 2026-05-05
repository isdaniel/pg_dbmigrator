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
pub async fn apply_sequences_to_target(
    target: &Client,
    sequences: &[SourceSequence],
) -> Result<usize> {
    let mut applied = 0usize;
    for seq in sequences {
        let Some(last_value) = seq.last_value else {
            debug!(
                schema = %seq.schema,
                name = %seq.name,
                "skipping sequence: never advanced on source",
            );
            continue;
        };
        let sql = build_setval_sql(&seq.schema, &seq.name)?;
        match target.execute(&sql, &[&last_value]).await {
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
}
