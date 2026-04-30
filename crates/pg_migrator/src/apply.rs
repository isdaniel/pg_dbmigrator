//! Apply replication change events to a target PostgreSQL database.
//!
//! This module turns [`pg_walstream::ChangeEvent`] values into parameterised
//! SQL statements and executes them against a [`tokio_postgres`] connection.
//!
//! The translation strategy is intentionally simple and correct:
//!
//! * `INSERT` → `INSERT INTO "schema"."table" ("c1", "c2") VALUES ($1, $2)`
//! * `UPDATE` → `UPDATE "schema"."table" SET "c1" = $1 WHERE "key1" = $2`
//! * `DELETE` → `DELETE FROM "schema"."table" WHERE "key1" = $1`
//! * `TRUNCATE` → `TRUNCATE "schema"."table"`
//!
//! Only column names that come back via the relation messages from the
//! pgoutput plugin are used. The values are bound as text parameters, which
//! lets PostgreSQL coerce them into their declared types on the target.

use pg_walstream::{ChangeEvent, ColumnValue, EventType, RowData};
use tokio_postgres::types::{IsNull, ToSql, Type};
use tokio_postgres::Client;
use tracing::{debug, warn};

use crate::error::{MigrationError, Result};

/// Statement produced from a [`ChangeEvent`]. Useful for testing the
/// translation without an actual database connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStatement {
    /// SQL string with `$1`, `$2`, ... placeholders.
    pub sql: String,
    /// Bound text parameter values. `None` represents SQL `NULL`.
    pub params: Vec<Option<String>>,
}

/// Translate a single [`ChangeEvent`] into a [`PreparedStatement`].
///
/// Returns `Ok(None)` for events that are book-keeping only (BEGIN, COMMIT,
/// keepalive, etc.) and have no SQL impact on the target.
pub fn statement_for(event: &ChangeEvent) -> Result<Option<PreparedStatement>> {
    Ok(match &event.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => Some(build_insert(schema, table, data)),
        EventType::Update {
            schema,
            table,
            new_data,
            key_columns,
            ..
        } => Some(build_update(schema, table, new_data, key_columns)?),
        EventType::Delete {
            schema,
            table,
            old_data,
            key_columns,
            ..
        } => Some(build_delete(schema, table, old_data, key_columns)?),
        EventType::Truncate(rels) => Some(build_truncate(rels)?),
        // No-op events that the apply loop should silently advance past.
        EventType::Begin { .. }
        | EventType::Commit { .. }
        | EventType::Relation { .. }
        | EventType::Type { .. }
        | EventType::Origin { .. }
        | EventType::Message { .. }
        | EventType::StreamStart { .. }
        | EventType::StreamStop
        | EventType::StreamCommit { .. }
        | EventType::StreamAbort { .. }
        | EventType::BeginPrepare { .. }
        | EventType::Prepare { .. }
        | EventType::CommitPrepared { .. }
        | EventType::RollbackPrepared { .. }
        | EventType::StreamPrepare { .. } => None,
    })
}

fn quote_ident(name: &str) -> String {
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

fn build_insert(schema: &str, table: &str, data: &RowData) -> PreparedStatement {
    let mut cols: Vec<String> = Vec::with_capacity(data.len());
    let mut placeholders: Vec<String> = Vec::with_capacity(data.len());
    let mut params: Vec<Option<String>> = Vec::with_capacity(data.len());

    for (idx, (name, value)) in data.iter().enumerate() {
        cols.push(quote_ident(name));
        placeholders.push(format!("${}", idx + 1));
        params.push(value_as_text(value));
    }

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        qualified(schema, table),
        cols.join(", "),
        placeholders.join(", ")
    );
    PreparedStatement { sql, params }
}

fn build_update(
    schema: &str,
    table: &str,
    new_data: &RowData,
    key_columns: &[std::sync::Arc<str>],
) -> Result<PreparedStatement> {
    if key_columns.is_empty() {
        return Err(MigrationError::apply(format!(
            "UPDATE on {}.{} requires a replica identity (no key columns)",
            schema, table
        )));
    }

    let mut params: Vec<Option<String>> = Vec::with_capacity(new_data.len() + key_columns.len());
    let mut set_clauses: Vec<String> = Vec::with_capacity(new_data.len());

    for (idx, (name, value)) in new_data.iter().enumerate() {
        set_clauses.push(format!("{} = ${}", quote_ident(name), idx + 1));
        params.push(value_as_text(value));
    }

    let mut where_clauses: Vec<String> = Vec::with_capacity(key_columns.len());
    for (i, key) in key_columns.iter().enumerate() {
        let placeholder = new_data.len() + i + 1;
        where_clauses.push(format!("{} = ${}", quote_ident(key), placeholder));
        let v = new_data
            .get(key)
            .ok_or_else(|| MigrationError::apply(format!("missing key column {key}")))?;
        params.push(value_as_text(v));
    }

    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        qualified(schema, table),
        set_clauses.join(", "),
        where_clauses.join(" AND ")
    );
    Ok(PreparedStatement { sql, params })
}

fn build_delete(
    schema: &str,
    table: &str,
    old_data: &RowData,
    key_columns: &[std::sync::Arc<str>],
) -> Result<PreparedStatement> {
    if key_columns.is_empty() {
        return Err(MigrationError::apply(format!(
            "DELETE on {}.{} requires a replica identity (no key columns)",
            schema, table
        )));
    }

    let mut params: Vec<Option<String>> = Vec::with_capacity(key_columns.len());
    let mut where_clauses: Vec<String> = Vec::with_capacity(key_columns.len());
    for (i, key) in key_columns.iter().enumerate() {
        where_clauses.push(format!("{} = ${}", quote_ident(key), i + 1));
        let v = old_data
            .get(key)
            .ok_or_else(|| MigrationError::apply(format!("missing key column {key}")))?;
        params.push(value_as_text(v));
    }

    let sql = format!(
        "DELETE FROM {} WHERE {}",
        qualified(schema, table),
        where_clauses.join(" AND ")
    );
    Ok(PreparedStatement { sql, params })
}

fn build_truncate(rels: &[std::sync::Arc<str>]) -> Result<PreparedStatement> {
    if rels.is_empty() {
        return Err(MigrationError::apply(
            "TRUNCATE event with no relations".to_string(),
        ));
    }
    // The pgoutput TRUNCATE message identifies relations by OID, not name.
    // The high-level wrapper in pg_walstream rewrites these into
    // `schema.table` strings — we trust those here.
    let parts: Vec<String> = rels
        .iter()
        .map(|r| {
            let s = r.as_ref();
            if let Some((sch, tab)) = s.split_once('.') {
                qualified(sch, tab)
            } else {
                quote_ident(s)
            }
        })
        .collect();
    Ok(PreparedStatement {
        sql: format!("TRUNCATE {}", parts.join(", ")),
        params: Vec::new(),
    })
}

fn value_as_text(v: &ColumnValue) -> Option<String> {
    match v {
        ColumnValue::Null => None,
        ColumnValue::Text(b) => Some(String::from_utf8_lossy(b).into_owned()),
        ColumnValue::Binary(b) => {
            // Binary columns are encoded as escaped bytea text.
            let mut s = String::with_capacity(b.len() * 2 + 2);
            s.push_str("\\x");
            for byte in b.iter() {
                use std::fmt::Write as _;
                let _ = write!(s, "{byte:02x}");
            }
            Some(s)
        }
    }
}

/// Newtype wrapper that lets us pass `Option<String>` text values to
/// `tokio_postgres` while telling the server to treat them as `TEXT`. The
/// server is responsible for coercing them into the column type.
struct TextParam<'a>(Option<&'a str>);

impl<'a> std::fmt::Debug for TextParam<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(s) => write!(f, "TextParam({s:?})"),
            None => write!(f, "TextParam(NULL)"),
        }
    }
}

impl<'a> ToSql for TextParam<'a> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            Some(s) => s.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        // We declare ourselves compatible with every PostgreSQL type because
        // the underlying value is plain text; the server performs the cast.
        true
    }

    tokio_postgres::types::to_sql_checked!();
}

/// Apply the events to `client`. Stops at the first error.
pub async fn apply_events(
    client: &Client,
    events: impl IntoIterator<Item = ChangeEvent>,
) -> Result<usize> {
    let mut count = 0usize;
    for event in events {
        if let Some(stmt) = statement_for(&event)? {
            execute_prepared(client, &stmt).await?;
            count += 1;
        } else {
            debug!(event = ?event.event_type, "skipping non-DML event");
        }
    }
    Ok(count)
}

/// Execute a single [`PreparedStatement`] against the client. Mostly factored
/// out so it can be reused by the streaming loop.
pub async fn execute_prepared(client: &Client, stmt: &PreparedStatement) -> Result<u64> {
    let params: Vec<TextParam<'_>> = stmt
        .params
        .iter()
        .map(|v| TextParam(v.as_deref()))
        .collect();
    let dyn_params: Vec<&(dyn ToSql + Sync)> =
        params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();

    let affected = client.execute(stmt.sql.as_str(), &dyn_params).await?;
    if affected == 0 {
        warn!(sql = %stmt.sql, "statement affected 0 rows");
    }
    Ok(affected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_walstream::{ChangeEvent, ColumnValue, Lsn, RelationColumn, ReplicaIdentity, RowData};
    use std::sync::Arc;

    fn row(pairs: &[(&str, ColumnValue)]) -> RowData {
        let mut r = RowData::with_capacity(pairs.len());
        for (k, v) in pairs {
            r.push(Arc::from(*k), v.clone());
        }
        r
    }

    #[test]
    fn quote_ident_escapes_double_quotes() {
        assert_eq!(quote_ident("col"), "\"col\"");
        assert_eq!(quote_ident(r#"co"l"#), "\"co\"\"l\"");
    }

    #[test]
    fn insert_builds_expected_sql() {
        let data = row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Alice")),
        ]);
        let event = ChangeEvent::insert("public", "users", 1, data, Lsn::new(0));
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(
            stmt.sql,
            "INSERT INTO \"public\".\"users\" (\"id\", \"name\") VALUES ($1, $2)"
        );
        assert_eq!(stmt.params, vec![Some("1".into()), Some("Alice".into())]);
    }

    #[test]
    fn update_uses_key_columns_in_where() {
        let new = row(&[
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("Bob")),
        ]);
        let event = ChangeEvent {
            event_type: EventType::Update {
                schema: Arc::from("public"),
                table: Arc::from("users"),
                relation_oid: 1,
                old_data: None,
                new_data: new,
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec![Arc::from("id")],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(
            stmt.sql,
            "UPDATE \"public\".\"users\" SET \"id\" = $1, \"name\" = $2 WHERE \"id\" = $3"
        );
        assert_eq!(
            stmt.params,
            vec![Some("1".into()), Some("Bob".into()), Some("1".into())]
        );
    }

    #[test]
    fn update_without_key_columns_errors() {
        let new = row(&[("name", ColumnValue::text("Bob"))]);
        let event = ChangeEvent {
            event_type: EventType::Update {
                schema: Arc::from("public"),
                table: Arc::from("users"),
                relation_oid: 1,
                old_data: None,
                new_data: new,
                replica_identity: ReplicaIdentity::Nothing,
                key_columns: Vec::new(),
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        let err = statement_for(&event).unwrap_err();
        assert!(matches!(err, MigrationError::Apply(_)));
    }

    #[test]
    fn delete_uses_key_columns() {
        let old = row(&[
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Carol")),
        ]);
        let event = ChangeEvent {
            event_type: EventType::Delete {
                schema: Arc::from("public"),
                table: Arc::from("users"),
                relation_oid: 1,
                old_data: old,
                replica_identity: ReplicaIdentity::Default,
                key_columns: vec![Arc::from("id")],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(
            stmt.sql,
            "DELETE FROM \"public\".\"users\" WHERE \"id\" = $1"
        );
        assert_eq!(stmt.params, vec![Some("42".into())]);
    }

    #[test]
    fn truncate_quotes_relation_names() {
        let event = ChangeEvent {
            event_type: EventType::Truncate(vec![
                Arc::from("public.users"),
                Arc::from("audit.events"),
            ]),
            lsn: Lsn::new(0),
            metadata: None,
        };
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(
            stmt.sql,
            "TRUNCATE \"public\".\"users\", \"audit\".\"events\""
        );
        assert!(stmt.params.is_empty());
    }

    #[test]
    fn null_value_is_translated_to_none() {
        let data = row(&[("id", ColumnValue::text("1")), ("note", ColumnValue::Null)]);
        let event = ChangeEvent::insert("public", "t", 1, data, Lsn::new(0));
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(stmt.params[0], Some("1".into()));
        assert_eq!(stmt.params[1], None);
    }

    #[test]
    fn binary_value_is_hex_encoded() {
        let data = row(&[(
            "blob",
            ColumnValue::Binary(bytes::Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
        )]);
        let event = ChangeEvent::insert("public", "t", 1, data, Lsn::new(0));
        let stmt = statement_for(&event).unwrap().unwrap();
        assert_eq!(stmt.params, vec![Some("\\xdeadbeef".into())]);
    }

    #[test]
    fn book_keeping_events_yield_no_statement() {
        // Use a Relation event as a representative no-op.
        let event = ChangeEvent {
            event_type: EventType::Relation {
                relation_id: 1,
                namespace: Arc::from("public"),
                relation_name: Arc::from("users"),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![RelationColumn {
                    name: Arc::from("id"),
                    type_id: 23,
                    type_modifier: -1,
                    is_key: true,
                }],
            },
            lsn: Lsn::new(0),
            metadata: None,
        };
        assert!(statement_for(&event).unwrap().is_none());
    }
}
