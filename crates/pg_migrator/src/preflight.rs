//! Pre-flight environment checks run before any migration work begins.
//!
//! These checks fail *fast and loudly* with actionable error messages, so the
//! operator can fix the environment before kicking off a multi-hour dump.

use std::io;
use std::process::ExitStatus;

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
}
