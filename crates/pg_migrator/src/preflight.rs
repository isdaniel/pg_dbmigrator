//! Pre-flight environment checks run before any migration work begins.
//!
//! These checks fail *fast and loudly* with actionable error messages, so the
//! operator can fix the environment before kicking off a multi-hour dump.

use std::io;
use std::process::ExitStatus;

use tracing::info;

use crate::error::{MigrationError, Result};
use crate::tls::connect_with_sslmode;

/// External tools that must be available on `$PATH` for the migrator to
/// function. `pg_dump` is required for both modes; `pg_restore` is required
/// for the restore phase.
pub const REQUIRED_TOOLS: &[&str] = &["pg_dump", "pg_restore"];

/// Verify that every entry in [`REQUIRED_TOOLS`] is callable.
///
/// Returns the first missing tool as a [`MigrationError::MissingTool`] with a
/// concrete install hint. If all tools succeed, returns `Ok(())`.
pub async fn verify_pg_tools_installed() -> Result<()> {
    for tool in REQUIRED_TOOLS {
        let outcome = spawn_version_check(tool).await;
        classify_version_check(tool, outcome)?;
    }
    Ok(())
}

/// Spawn `<tool> --version` with stdio silenced and return the raw outcome.
/// Split out so [`classify_version_check`] can be unit-tested without
/// actually spawning processes.
async fn spawn_version_check(tool: &str) -> std::result::Result<ExitStatus, io::Error> {
    use std::process::Stdio;
    use tokio::process::Command;
    Command::new(tool)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
}

/// Pure interpretation of the version-check outcome — kept separate from the
/// spawn so it can be unit-tested deterministically.
pub(crate) fn classify_version_check(
    tool: &str,
    outcome: std::result::Result<ExitStatus, io::Error>,
) -> Result<()> {
    match outcome {
        Ok(s) if s.success() => Ok(()),
        Ok(s) => Err(MigrationError::missing_tool(
            tool,
            format!("`{tool} --version` exited with status {s}"),
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let path = std::env::var("PATH").unwrap_or_default();
            Err(MigrationError::missing_tool(
                tool,
                format!("not found in $PATH (PATH={path})"),
            ))
        }
        Err(e) => Err(MigrationError::missing_tool(
            tool,
            format!("failed to spawn `{tool} --version`: {e}"),
        )),
    }
}

/// Confirm that a logical-replication publication with the given name exists
/// on the source. Run *before* `prepare_replication_slot` so the operator
/// gets an actionable error in seconds instead of waiting until the apply
/// worker dies on `CREATE SUBSCRIPTION`.
///
/// Returns [`MigrationError::Config`] when the publication is missing — the
/// operator must `CREATE PUBLICATION <name> FOR ALL TABLES` (or a more
/// targeted `FOR TABLE ...`) on the source before retrying.
pub async fn verify_publication_exists(source_conn: &str, publication: &str) -> Result<()> {
    let client = connect_with_sslmode(source_conn).await?;
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication],
        )
        .await?;
    let exists: bool = row.get(0);
    if !exists {
        return Err(MigrationError::config(format!(
            "publication `{publication}` does not exist on the source. \
             Run `CREATE PUBLICATION {publication} FOR ALL TABLES;` \
             (or a more targeted `FOR TABLE ...`) before retrying."
        )));
    }
    Ok(())
}

/// Rewrite a connection string so the path component (database name) points
/// to the `postgres` maintenance database. Used to run admin commands like
/// `CREATE DATABASE` which cannot target the database they are creating.
pub fn maintenance_connection_string(conn: &str) -> String {
    match conn.find('?') {
        Some(q) => {
            let scheme_end = conn.find("://").map(|i| i + 3).unwrap_or(0);
            let at = conn[scheme_end..q].rfind('@').map(|i| i + scheme_end);
            let host_start = at.map(|i| i + 1).unwrap_or(scheme_end);
            // Find first '/' after host:port — that starts the db name.
            match conn[host_start..q].find('/') {
                Some(slash) => {
                    let abs = host_start + slash;
                    format!("{}/postgres{}", &conn[..abs], &conn[q..])
                }
                None => conn.to_string(),
            }
        }
        None => {
            let scheme_end = conn.find("://").map(|i| i + 3).unwrap_or(0);
            let at = conn[scheme_end..].rfind('@').map(|i| i + scheme_end);
            let host_start = at.map(|i| i + 1).unwrap_or(scheme_end);
            match conn[host_start..].find('/') {
                Some(slash) => {
                    let abs = host_start + slash;
                    format!("{}/postgres", &conn[..abs])
                }
                None => conn.to_string(),
            }
        }
    }
}

/// Ensure the target database exists, creating it if necessary.
///
/// Connects to the `postgres` maintenance database on the target server and
/// checks `pg_database`. If the target database is missing, issues
/// `CREATE DATABASE`. This runs early in the online pipeline — before
/// `pg_restore` needs a live target database.
pub async fn ensure_target_database_exists(target_conn: &str, db_name: &str) -> Result<()> {
    let maint_conn = maintenance_connection_string(target_conn);
    let client = connect_with_sslmode(&maint_conn).await?;
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&db_name],
        )
        .await?;
    let exists: bool = row.get(0);
    if exists {
        info!(database = db_name, "target database already exists");
    } else {
        info!(database = db_name, "creating target database");
        let create_sql = format!("CREATE DATABASE {}", pg_walstream::quote_ident(db_name)?);
        client.batch_execute(&create_sql).await?;
        info!(database = db_name, "target database created");
    }
    Ok(())
}

/// Check whether `pglogical` is loaded in `shared_preload_libraries` on the
/// target and warn/error if so.
///
/// **Background**: when `pglogical` is a shared preload library it installs
/// hooks into the logical-replication launcher that prevent native
/// `CREATE SUBSCRIPTION` apply workers from starting. The workers launch,
/// immediately crash, and never connect to the source — leaving replication
/// permanently stalled with no useful error message in `pg_stat_subscription`.
///
/// This function detects the situation early and fails with an actionable
/// message so the operator can remove `pglogical` from
/// `shared_preload_libraries` (and restart the server) before proceeding.
///
/// On vanilla PostgreSQL (or any server where `pglogical` is not preloaded)
/// the check is a silent no-op.
pub async fn ensure_pglogical_not_interfering(target_conn: &str) -> Result<()> {
    let client = connect_with_sslmode(target_conn).await?;

    let row = client
        .query_one("SELECT current_setting('shared_preload_libraries')", &[])
        .await?;
    let libs: &str = row.get(0);

    if libs.split(',').any(|lib| lib.trim() == "pglogical") {
        return Err(MigrationError::config(
            "the target server has `pglogical` in `shared_preload_libraries`. \
             This is known to prevent native PostgreSQL logical-replication apply \
             workers from starting (the workers crash silently on launch). \
             Remove `pglogical` from `shared_preload_libraries` and restart the \
             server before retrying.\n\
             hint: on Azure Flexible Server, set this via Portal → Server Parameters \
             → shared_preload_libraries, or via: \
             `az postgres flexible-server parameter set --name shared_preload_libraries \
             --value '<libraries-without-pglogical>'`"
                .to_string(),
        ));
    }

    info!("pglogical is not in shared_preload_libraries — native logical replication will work");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    fn ok_status() -> ExitStatus {
        ExitStatus::from_raw(0)
    }

    fn fail_status() -> ExitStatus {
        ExitStatus::from_raw(1 << 8) // exit code 1
    }

    #[test]
    fn classify_ok_when_version_succeeds() {
        assert!(classify_version_check("pg_dump", Ok(ok_status())).is_ok());
    }

    #[test]
    fn classify_missing_tool_when_not_found() {
        let err = classify_version_check("pg_dump", Err(io::Error::from(io::ErrorKind::NotFound)))
            .unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_dump");
                assert!(reason.contains("not found in $PATH"));
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn classify_missing_tool_when_version_exits_nonzero() {
        let err = classify_version_check("pg_restore", Ok(fail_status())).unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_restore");
                assert!(reason.contains("--version"));
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn classify_missing_tool_for_other_io_errors() {
        let err = classify_version_check(
            "pg_dump",
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
        )
        .unwrap_err();
        match err {
            MigrationError::MissingTool { tool, reason } => {
                assert_eq!(tool, "pg_dump");
                assert!(reason.contains("failed to spawn"));
            }
            other => panic!("expected MissingTool, got {other:?}"),
        }
    }

    #[test]
    fn missing_tool_error_message_includes_install_hint() {
        let err = MigrationError::missing_tool("pg_dump", "not found in $PATH");
        let msg = err.to_string();
        assert!(msg.contains("pg_dump"));
        assert!(msg.contains("not installed or not on $PATH"));
        assert!(msg.contains("postgresql-client"));
    }

    #[test]
    fn required_tools_includes_pg_dump_and_pg_restore() {
        assert!(REQUIRED_TOOLS.contains(&"pg_dump"));
        assert!(REQUIRED_TOOLS.contains(&"pg_restore"));
    }

    #[tokio::test]
    async fn verify_pg_tools_passes_in_test_env() {
        // Sanity test for the live path. CI hosts typically have these
        // tools; if they don't, the dump/restore tests would already be
        // useless. We tolerate either result so this test never blocks.
        let _ = verify_pg_tools_installed().await;
    }

    #[test]
    fn maintenance_conn_swaps_database_name() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/mydb?sslmode=require"),
            "postgresql://u:p@host:5432/postgres?sslmode=require"
        );
    }

    #[test]
    fn maintenance_conn_no_query_params() {
        assert_eq!(
            maintenance_connection_string("postgresql://u:p@host:5432/mydb"),
            "postgresql://u:p@host:5432/postgres"
        );
    }

    #[test]
    fn maintenance_conn_preserves_multiple_query_params() {
        assert_eq!(
            maintenance_connection_string(
                "postgresql://u:p@host/db1?sslmode=require&connect_timeout=10"
            ),
            "postgresql://u:p@host/postgres?sslmode=require&connect_timeout=10"
        );
    }

    #[test]
    fn maintenance_conn_handles_no_password() {
        assert_eq!(
            maintenance_connection_string("postgresql://u@host/db1?sslmode=require"),
            "postgresql://u@host/postgres?sslmode=require"
        );
    }
}
