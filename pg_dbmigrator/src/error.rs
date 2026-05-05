//! Error types used throughout the migrator.

use std::io;

use thiserror::Error;

/// Result alias used by all fallible APIs in this crate.
pub type Result<T> = std::result::Result<T, MigrationError>;

/// All the error categories produced by the migration pipeline.
///
/// Each variant maps to a specific failure surface so that callers (CLI,
/// orchestrator, tests) can pattern match on the kind of failure they want
/// to react to.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Returned when the user-supplied configuration is rejected before any
    /// work is started.
    #[error("invalid configuration: {0}")]
    Config(String),

    /// A connection string could not be parsed.
    #[error("invalid connection string: {0}")]
    InvalidConnectionString(String),

    /// `pg_dump` / `pg_restore` / `psql` could not be spawned, or returned a
    /// non-zero exit code.
    #[error("external command `{command}` failed: {message}")]
    ExternalCommand {
        /// Name of the external program (e.g. `pg_dump`).
        command: String,
        /// Human readable description of what went wrong.
        message: String,
    },

    /// A required external tool (e.g. `pg_dump`, `pg_restore`) is not
    /// installed or not on `$PATH`. Surfaced by the pre-flight check at the
    /// start of `Migrator::run`, before any work is done.
    #[error(
        "required tool `{tool}` is not installed or not on $PATH: {reason}\n\
         hint: install the matching PostgreSQL client tools (e.g. on Ubuntu \
         `apt install postgresql-client-NN` where NN matches your source \
         server's major version) and ensure they are on $PATH"
    )]
    MissingTool {
        /// Name of the missing tool (e.g. `pg_dump`).
        tool: String,
        /// What the lookup actually returned (e.g. "not found in $PATH").
        reason: String,
    },

    /// Generic I/O failure from spawning a child process or reading a file.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Wrapper for any error returned by [`tokio_postgres`].
    ///
    /// Note: `tokio_postgres::Error`'s `Display` for database errors is just
    /// `"db error"` — the actual severity/message/detail/hint live inside the
    /// `DbError` accessible only via `as_db_error()`. We format them
    /// explicitly so the operator sees the real PostgreSQL message.
    #[error("{}", format_pg_error(.0))]
    Postgres(#[from] tokio_postgres::Error),

    /// Wrapper for any error coming out of [`pg_walstream`].
    #[error("replication error: {0}")]
    Replication(#[from] pg_walstream::ReplicationError),

    /// The replication apply path encountered an event it cannot handle.
    #[error("apply error: {0}")]
    Apply(String),

    /// The pipeline was cancelled by the caller via the cancellation token.
    #[error("operation cancelled")]
    Cancelled,
}

/// Format a `tokio_postgres::Error` with the full database error detail.
///
/// `tokio_postgres::Error::Display` for `Kind::Db` emits only `"db error"`;
/// the severity, message, detail, and hint are inside the `DbError` struct
/// accessible via `as_db_error()`. This function extracts them so log lines
/// and error reports carry the real PostgreSQL diagnostic.
fn format_pg_error(err: &tokio_postgres::Error) -> String {
    if let Some(db) = err.as_db_error() {
        let mut msg = format!("postgres error: {}: {}", db.severity(), db.message());
        if let Some(detail) = db.detail() {
            msg.push_str("\nDETAIL: ");
            msg.push_str(detail);
        }
        if let Some(hint) = db.hint() {
            msg.push_str("\nHINT: ");
            msg.push_str(hint);
        }
        msg
    } else {
        format!("postgres error: {err}")
    }
}

impl MigrationError {
    /// Convenience constructor for [`MigrationError::Config`].
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    /// Convenience constructor for [`MigrationError::ExternalCommand`].
    pub fn external(command: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ExternalCommand {
            command: command.into(),
            message: message.into(),
        }
    }

    /// Convenience constructor for [`MigrationError::Apply`].
    pub fn apply(msg: impl Into<String>) -> Self {
        Self::Apply(msg.into())
    }

    /// Convenience constructor for [`MigrationError::MissingTool`].
    pub fn missing_tool(tool: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::MissingTool {
            tool: tool.into(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_helper_constructs_variant() {
        let err = MigrationError::config("bad");
        assert!(matches!(err, MigrationError::Config(ref m) if m == "bad"));
        assert_eq!(err.to_string(), "invalid configuration: bad");
    }

    #[test]
    fn external_helper_formats_message() {
        let err = MigrationError::external("pg_dump", "exit 1");
        assert_eq!(err.to_string(), "external command `pg_dump` failed: exit 1");
    }

    #[test]
    fn apply_helper_constructs_variant() {
        let err = MigrationError::apply("oops");
        assert!(matches!(err, MigrationError::Apply(ref m) if m == "oops"));
    }

    #[test]
    fn missing_tool_helper_constructs_variant() {
        let err = MigrationError::missing_tool("pg_dump", "not found in $PATH");
        match err {
            MigrationError::MissingTool {
                ref tool,
                ref reason,
            } => {
                assert_eq!(tool, "pg_dump");
                assert_eq!(reason, "not found in $PATH");
            }
            _ => panic!("expected MissingTool variant"),
        }
        let msg = err.to_string();
        assert!(msg.contains("pg_dump"));
        assert!(msg.contains("not installed or not on $PATH"));
        assert!(msg.contains("postgresql-client"));
    }

    #[test]
    fn cancelled_display() {
        let err = MigrationError::Cancelled;
        assert_eq!(err.to_string(), "operation cancelled");
    }

    #[test]
    fn from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::BrokenPipe, "pipe broke");
        let err: MigrationError = io_err.into();
        assert!(matches!(err, MigrationError::Io(_)));
        assert!(err.to_string().contains("pipe broke"));
    }

    #[test]
    fn format_pg_error_non_db_error() {
        let err = "host not found".parse::<std::net::IpAddr>().unwrap_err();
        let pg_err = tokio_postgres::Error::__private_api_timeout();
        let formatted = format_pg_error(&pg_err);
        assert!(formatted.starts_with("postgres error:"));
        let _ = err;
    }

    #[test]
    fn invalid_connection_string_display() {
        let err = MigrationError::InvalidConnectionString("bad://url".into());
        assert_eq!(err.to_string(), "invalid connection string: bad://url");
    }

    #[test]
    fn replication_error_display() {
        let rep_err = pg_walstream::ReplicationError::Protocol("test message".into());
        let err: MigrationError = rep_err.into();
        let msg = err.to_string();
        assert!(msg.contains("replication error"));
    }

    #[test]
    fn external_command_fields_accessible_via_match() {
        let err = MigrationError::external("pg_restore", "exit code 2");
        match &err {
            MigrationError::ExternalCommand { command, message } => {
                assert_eq!(command, "pg_restore");
                assert_eq!(message, "exit code 2");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn io_error_preserves_kind() {
        let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "forbidden");
        let err: MigrationError = io_err.into();
        match err {
            MigrationError::Io(ref e) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
            _ => panic!("expected Io variant"),
        }
        assert!(err.to_string().contains("forbidden"));
    }

    #[test]
    fn config_error_debug_format() {
        let err = MigrationError::config("test cfg error");
        let dbg = format!("{:?}", err);
        assert!(dbg.contains("Config"));
        assert!(dbg.contains("test cfg error"));
    }
}
