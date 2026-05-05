//! Resume token persisted between runs of [`crate::Migrator::run`].
//!
//! ## Why
//!
//! A crash *after* the multi-hour `pg_dump` + `pg_restore` should not
//! force the operator to redo the bulk copy. With `--resume` set, the
//! orchestrator loads the on-disk token, verifies the surrounding config
//! still matches, and skips every stage already marked complete —
//! typically jumping directly into the apply / lag-poll loop and
//! re-attaching to the pre-existing replication slot.
//!
//! ## What
//!
//! A small JSON file written next to the dump archive (default:
//! `<dump_path>.resume.json`). Each save is atomic: written to a sibling
//! `.tmp` file, then `rename`d into place so a crash *during* the save
//! never produces a half-written token.
//!
//! ## What this is NOT
//!
//! - Not a recovery story for a *dropped* replication slot. Once the
//!   slot disappears on the source the WAL position is lost and resume
//!   cannot rewind history. The orchestrator validates slot existence
//!   before honouring a resume.
//! - Not a substitute for `--force-clean`. Resume *re-uses* a half-built
//!   target; force-clean *erases* it.

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::config::{MigrationConfig, MigrationMode};
use crate::error::{MigrationError, Result};

/// Schema-version of the on-disk token. Bump when an incompatible field
/// change is introduced; mismatched tokens are refused (operator must
/// `--force-clean` and start over).
pub const RESUME_SCHEMA_VERSION: u32 = 1;

/// Stages that can be marked complete on a [`ResumeToken`].
///
/// Restore is treated as one atomic unit even when `split_sections` is
/// enabled — partial section completion would require per-table tracking
/// which is out of scope for this token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletedStage {
    /// Replication slot + exported snapshot have been created on the source.
    PrepareSnapshot,
    /// `pg_dump` finished successfully and the archive is on disk.
    Dump,
    /// `pg_restore` (or all three sections, if split) finished.
    Restore,
}

/// Persisted state used by [`crate::Migrator::run`] when `--resume` is set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeToken {
    /// On-disk schema version. Compared against
    /// [`RESUME_SCHEMA_VERSION`] on load; mismatches are refused.
    pub schema_version: u32,
    /// Stable hash over the migration-defining fields of
    /// [`MigrationConfig`]. A mismatch on resume aborts so we never
    /// attach a half-built target to a different source / slot / table
    /// set.
    pub config_hash: String,
    /// `"online"` or `"offline"`.
    pub mode: String,
    /// Stages already finished. Saved after each successful transition.
    pub completed: BTreeSet<CompletedStage>,
    /// Pinned dump archive path. Required for resume so the orchestrator
    /// knows where the on-disk dump lives.
    pub dump_path: PathBuf,
    /// Slot name the source carries (online only).
    pub slot_name: Option<String>,
    /// Subscription name on the target (online only).
    pub subscription_name: Option<String>,
    /// Publication name on the source (online only).
    pub publication: Option<String>,
    /// Exported snapshot name from `PrepareSnapshot`. Only meaningful
    /// while the slot is alive — informational once the slot has been
    /// promoted to a subscription.
    pub snapshot_name: Option<String>,
    /// Most recent `confirmed_flush_lsn` observed by the apply lag
    /// poller. Useful for the operator's sanity check after a resume.
    pub last_applied_lsn: Option<u64>,
    /// RFC-3339 timestamp of the last save.
    pub updated_at: String,
}

impl ResumeToken {
    /// Construct a fresh, empty token for a given config + dump path.
    pub fn new(cfg: &MigrationConfig, dump_path: PathBuf) -> Self {
        let mode = match cfg.mode {
            MigrationMode::Offline => "offline",
            MigrationMode::Online => "online",
        };
        Self {
            schema_version: RESUME_SCHEMA_VERSION,
            config_hash: config_hash(cfg),
            mode: mode.to_string(),
            completed: BTreeSet::new(),
            dump_path,
            slot_name: if cfg.mode == MigrationMode::Online {
                Some(cfg.online.slot_name.clone())
            } else {
                None
            },
            subscription_name: if cfg.mode == MigrationMode::Online {
                Some(cfg.online.subscription_name.clone())
            } else {
                None
            },
            publication: if cfg.mode == MigrationMode::Online {
                Some(cfg.online.publication.clone())
            } else {
                None
            },
            snapshot_name: None,
            last_applied_lsn: None,
            updated_at: now_rfc3339(),
        }
    }

    /// Mark `stage` as complete and refresh `updated_at`.
    pub fn mark(&mut self, stage: CompletedStage) {
        self.completed.insert(stage);
        self.updated_at = now_rfc3339();
    }

    /// Whether `stage` has been recorded.
    pub fn has(&self, stage: CompletedStage) -> bool {
        self.completed.contains(&stage)
    }

    /// Load a token from `path`. Returns `Ok(None)` when the file is
    /// absent (a fresh resume just hasn't started yet); returns an
    /// `Err` for any other I/O / parse / schema-mismatch problem.
    pub async fn load(path: &Path) -> Result<Option<Self>> {
        let bytes = match tokio::fs::read(path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(MigrationError::Io(e)),
        };
        let token: Self = serde_json::from_slice(&bytes).map_err(|e| {
            MigrationError::config(format!(
                "resume token at {} is not valid JSON: {e}",
                path.display()
            ))
        })?;
        if token.schema_version != RESUME_SCHEMA_VERSION {
            return Err(MigrationError::config(format!(
                "resume token at {} has schema version {} (expected {}); \
                 retry with --force-clean to start fresh",
                path.display(),
                token.schema_version,
                RESUME_SCHEMA_VERSION,
            )));
        }
        Ok(Some(token))
    }

    /// Persist the token to `path` atomically: write to `<path>.tmp`
    /// then `rename` into place. A crash mid-write therefore never
    /// leaves a half-written file.
    pub async fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self).map_err(|e| {
            MigrationError::config(format!("failed to serialise resume token: {e}"))
        })?;
        tokio::fs::write(&tmp, &bytes).await?;
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }

    /// Verify the token is compatible with `cfg`. Returns
    /// [`MigrationError::Config`] on mismatch with a hint to
    /// `--force-clean`.
    pub fn check_compatible(&self, cfg: &MigrationConfig) -> Result<()> {
        let expected = config_hash(cfg);
        if self.config_hash != expected {
            return Err(MigrationError::config(format!(
                "resume token's config_hash {} does not match current config {} — \
                 either restore the original CLI flags or retry with --force-clean",
                self.config_hash, expected,
            )));
        }
        let mode = match cfg.mode {
            MigrationMode::Offline => "offline",
            MigrationMode::Online => "online",
        };
        if self.mode != mode {
            return Err(MigrationError::config(format!(
                "resume token was written in `{}` mode; current run is `{}`",
                self.mode, mode,
            )));
        }
        Ok(())
    }
}

/// Default location for the resume token: `<dump_path>.resume.json`.
pub fn default_resume_path(dump_path: &Path) -> PathBuf {
    let mut s = dump_path.as_os_str().to_os_string();
    s.push(".resume.json");
    PathBuf::from(s)
}

/// Stable hash over the migration-defining fields of [`MigrationConfig`].
///
/// Uses the standard library's `DefaultHasher`, which is `SipHash-1-3` —
/// not cryptographic, but identical between processes given the same
/// inputs. We only need a tamper-evident sanity check, not a security
/// boundary.
pub fn config_hash(cfg: &MigrationConfig) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    // Mode.
    match cfg.mode {
        MigrationMode::Offline => 0u8.hash(&mut h),
        MigrationMode::Online => 1u8.hash(&mut h),
    }
    // Endpoints — host / port / database (NOT the password).
    cfg.source.host.hash(&mut h);
    cfg.source.port.hash(&mut h);
    cfg.source.database.hash(&mut h);
    cfg.target.host.hash(&mut h);
    cfg.target.port.hash(&mut h);
    cfg.target.database.hash(&mut h);
    // Scope: schemas + tables — sorted so flag order doesn't matter.
    let mut schemas = cfg.schemas.clone();
    schemas.sort();
    schemas.hash(&mut h);
    let mut tables = cfg.tables.clone();
    tables.sort();
    tables.hash(&mut h);
    // Online identity.
    if cfg.mode == MigrationMode::Online {
        cfg.online.slot_name.hash(&mut h);
        cfg.online.publication.hash(&mut h);
        cfg.online.subscription_name.hash(&mut h);
    }
    format!("{:016x}", h.finish())
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{EndpointConfig, OnlineOptions};

    fn cfg() -> MigrationConfig {
        MigrationConfig {
            mode: MigrationMode::Online,
            source: EndpointConfig::parse("postgresql://u:p@src:5432/db").unwrap(),
            target: EndpointConfig::parse("postgresql://u:p@dst:5432/db").unwrap(),
            online: OnlineOptions {
                slot_name: "slot_a".into(),
                publication: "pub_a".into(),
                subscription_name: "sub_a".into(),
                ..OnlineOptions::default()
            },
            ..MigrationConfig::default()
        }
    }

    #[test]
    fn config_hash_is_stable_for_identical_inputs() {
        assert_eq!(config_hash(&cfg()), config_hash(&cfg()));
    }

    #[test]
    fn config_hash_changes_when_slot_name_changes() {
        let mut a = cfg();
        let mut b = cfg();
        a.online.slot_name = "slot_a".into();
        b.online.slot_name = "slot_b".into();
        assert_ne!(config_hash(&a), config_hash(&b));
    }

    #[test]
    fn config_hash_ignores_password() {
        let mut a = cfg();
        let mut b = cfg();
        a.source.password = "one".into();
        b.source.password = "two".into();
        assert_eq!(config_hash(&a), config_hash(&b));
    }

    #[test]
    fn config_hash_ignores_schema_table_order() {
        let mut a = cfg();
        let mut b = cfg();
        a.schemas = vec!["public".into(), "app".into()];
        b.schemas = vec!["app".into(), "public".into()];
        assert_eq!(config_hash(&a), config_hash(&b));
    }

    #[test]
    fn mark_and_has_round_trip() {
        let mut t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        assert!(!t.has(CompletedStage::Dump));
        t.mark(CompletedStage::Dump);
        assert!(t.has(CompletedStage::Dump));
        assert!(!t.has(CompletedStage::Restore));
    }

    #[tokio::test]
    async fn load_returns_none_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.json");
        assert!(ResumeToken::load(&path).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn save_then_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.json");
        let mut t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        t.mark(CompletedStage::PrepareSnapshot);
        t.mark(CompletedStage::Dump);
        t.snapshot_name = Some("00000003-deadbeef-1".into());
        t.last_applied_lsn = Some(0x1234_5678);
        t.save(&path).await.unwrap();

        let loaded = ResumeToken::load(&path).await.unwrap().unwrap();
        assert_eq!(loaded.config_hash, t.config_hash);
        assert!(loaded.has(CompletedStage::PrepareSnapshot));
        assert!(loaded.has(CompletedStage::Dump));
        assert!(!loaded.has(CompletedStage::Restore));
        assert_eq!(loaded.snapshot_name.as_deref(), Some("00000003-deadbeef-1"));
        assert_eq!(loaded.last_applied_lsn, Some(0x1234_5678));
    }

    #[tokio::test]
    async fn check_compatible_rejects_mismatched_config() {
        let t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        let mut other = cfg();
        other.online.slot_name = "different".into();
        let err = t.check_compatible(&other).unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[tokio::test]
    async fn check_compatible_rejects_mode_change() {
        let t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        let mut other = cfg();
        other.mode = MigrationMode::Offline;
        // Hash will also differ due to mode flip; just assert error type.
        let err = t.check_compatible(&other).unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[tokio::test]
    async fn load_rejects_unknown_schema_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resume.json");
        let mut t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        t.schema_version = RESUME_SCHEMA_VERSION + 1;
        t.save(&path).await.unwrap();
        let err = ResumeToken::load(&path).await.unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
    }

    #[test]
    fn default_resume_path_appends_suffix() {
        let p = default_resume_path(Path::new("/tmp/dump_online-12345"));
        assert_eq!(p, PathBuf::from("/tmp/dump_online-12345.resume.json"));
    }

    #[tokio::test]
    async fn load_rejects_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        tokio::fs::write(&path, b"not json at all {{{")
            .await
            .unwrap();
        let err = ResumeToken::load(&path).await.unwrap_err();
        assert!(matches!(err, MigrationError::Config(_)));
        assert!(err.to_string().contains("not valid JSON"));
    }

    #[test]
    fn check_compatible_accepts_matching_config() {
        let c = cfg();
        let t = ResumeToken::new(&c, PathBuf::from("/tmp/dump"));
        t.check_compatible(&c).unwrap();
    }

    #[test]
    fn resume_token_new_offline_has_no_online_fields() {
        let c = MigrationConfig {
            mode: MigrationMode::Offline,
            source: EndpointConfig::parse("postgresql://u:p@src:5432/db").unwrap(),
            target: EndpointConfig::parse("postgresql://u:p@dst:5432/db").unwrap(),
            ..MigrationConfig::default()
        };
        let t = ResumeToken::new(&c, PathBuf::from("/tmp/dump"));
        assert_eq!(t.mode, "offline");
        assert!(t.slot_name.is_none());
        assert!(t.subscription_name.is_none());
        assert!(t.publication.is_none());
    }

    #[test]
    fn resume_token_new_online_populates_online_fields() {
        let c = cfg();
        let t = ResumeToken::new(&c, PathBuf::from("/tmp/dump"));
        assert_eq!(t.mode, "online");
        assert_eq!(t.slot_name.as_deref(), Some("slot_a"));
        assert_eq!(t.subscription_name.as_deref(), Some("sub_a"));
        assert_eq!(t.publication.as_deref(), Some("pub_a"));
    }

    #[tokio::test]
    async fn save_creates_intermediate_directories() {
        let dir = tempfile::tempdir().unwrap();
        let nested_path = dir.path().join("a").join("b").join("resume.json");
        let t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        t.save(&nested_path).await.unwrap();
        assert!(nested_path.exists());
    }

    #[test]
    fn resume_token_serde_roundtrip() {
        let mut t = ResumeToken::new(&cfg(), PathBuf::from("/tmp/dump"));
        t.mark(CompletedStage::PrepareSnapshot);
        t.mark(CompletedStage::Dump);
        t.snapshot_name = Some("snap".into());
        t.last_applied_lsn = Some(42);
        let json = serde_json::to_string(&t).unwrap();
        let t2: ResumeToken = serde_json::from_str(&json).unwrap();
        assert_eq!(t2.config_hash, t.config_hash);
        assert_eq!(t2.mode, t.mode);
        assert!(t2.completed.contains(&CompletedStage::PrepareSnapshot));
        assert!(t2.completed.contains(&CompletedStage::Dump));
        assert!(!t2.completed.contains(&CompletedStage::Restore));
        assert_eq!(t2.snapshot_name.as_deref(), Some("snap"));
        assert_eq!(t2.last_applied_lsn, Some(42));
    }

    #[test]
    fn completed_stage_ordering() {
        assert!(CompletedStage::PrepareSnapshot < CompletedStage::Dump);
        assert!(CompletedStage::Dump < CompletedStage::Restore);
    }
}
