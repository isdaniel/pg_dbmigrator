# Agent.md — pg_dbmigrator

> Read this before modifying source. Covers architecture, invariants, and rules.

## 1. Architecture

**Modes**: `Offline` (pg_dump → pg_restore) | `Online` (slot+snapshot → dump → restore → CREATE SUBSCRIPTION → WAL apply → cutover)

**Pipeline**: `Validate → SourceVacuum → PrepareSnapshot* → Dump → Restore → Analyze → StreamApply* → Lag* → CaughtUp* → Cutover* → SourceCleanup* → Complete`
(`*` = online-only; SourceVacuum/Analyze skippable via `--skip-source-vacuum`/`--skip-analyze`)

### Offline mode

1. Pre-flight: verify pg_dump/pg_restore on PATH, validate config
2. `VACUUM ANALYZE` on source (skip with `--skip-source-vacuum`)
3. `pg_dump` (directory format, parallel `--jobs`, `--compress=lz4:1`)
4. `pg_restore` — split-section by default (pre-data → data → post-data for index-free COPY)
5. `ANALYZE` on target (skip with `--skip-analyze`)

### Online mode

1. **Validate**: pre-flight source (`wal_level=logical`, `max_replication_slots > 0`, `max_wal_senders > 0`); ensure publication exists (auto-create if missing)
2. **PrepareSnapshot**: `pg_walstream` creates replication slot with `EXPORT_SNAPSHOT` — snapshot kept alive by holding the stream connection open
3. **Dump**: `pg_dump --snapshot=<exported_id>` for a consistent baseline
4. **Restore**: `pg_restore` into target (same as offline)
5. **Analyze**: `ANALYZE` on target
6. **StreamApply**: orchestrator drops the pg_walstream stream connection, then issues `CREATE SUBSCRIPTION ... WITH (create_slot=false, slot_name='<existing>', enabled=true, copy_data=false)` on the target. PG's built-in apply worker streams WAL from `confirmed_flush_lsn`.
7. **Lag polling**: polls `pg_current_wal_flush_lsn()` on source every `poll_interval`, emits `Lag` heartbeat (lag_bytes, source_lsn, applied_lsn)
8. **CaughtUp**: when lag ≤ `lag_threshold_bytes`, one-shot advisory event fires
9. **Cutover**: operator presses Ctrl+C → sequence sync → source cleanup → done

### Cutover logic

1. First Ctrl+C → `CutoverHandle::request()` → apply loop exits on next poll
2. `ALTER SUBSCRIPTION ... DISABLE` + `DROP SUBSCRIPTION` (unless `--keep-subscription`)
3. `sync_sequences()` — setval() on all target sequences (PG logical replication doesn't replay nextval())
4. Source cleanup: drop auto-created publication + drop slot (unless `--keep-slot`)
5. Return `MigrationOutcome::cutover_triggered() == true`
6. Second Ctrl+C → `CancellationToken` cancel (abort escape hatch)

Cutover is **always operator-driven** — `lag_threshold_bytes` is purely advisory, never triggers cutover.

### Online resume flow

`--resume` after interrupt: load token → skip dump/restore → `disable_target_subscription` → `wait_for_slot_inactive` → re-enable subscription in place (preserves replication origin) → apply resumes from slot's LSN. Supports multiple consecutive cancel+resume cycles without data loss.

### Publication/subscription lifecycle

- **Before dump**: if `auto_create_publication` (default true) and publication missing → `CREATE PUBLICATION <name> FOR ALL TABLES` (or scoped to tables/schemas)
- **Apply phase**: subscription created by `run_native_apply`, torn down after cutover (unless `--keep-subscription`)
- **After cutover**: if pub was auto-created → `DROP PUBLICATION IF EXISTS`; if `drop_slot_on_cutover` (default true) → drop slot. All cleanup is best-effort (warnings only).
- CLI: `--no-auto-create-publication`, `--keep-slot`, `--keep-subscription`

### Workspace layout

```
Cargo.toml                        # workspace root (central versions/deps)
pg_dbmigrator/src/lib.rs          # library entry
pg_dbmigrator/src/bin/pg_dbmigrator/{main,args}.rs  # CLI
pg_dbmigrator/tests/postgres_integration.rs         # Rust integration tests (needs PG)
examples/{offline,online}_migration/                # example binaries
tests/integration/                # shell e2e tests (Docker PG 17)
docker-compose.test.yml           # source :55432, target :55433
```

### Module map

| Module | Role |
|--------|------|
| `config.rs` | `MigrationConfig`/`OnlineOptions`, validation, performance defaults |
| `error.rs` | `MigrationError` (thiserror) + `Result<T>` |
| `dump.rs` | pg_dump wrapper, `CommandRunner` trait, argv builder, stderr progress parsing |
| `restore.rs` | pg_restore/psql wrapper |
| `analyze.rs` | Pre-dump VACUUM ANALYZE + post-restore ANALYZE |
| `snapshot.rs` | Replication slot creation + exported snapshot |
| `native_apply.rs` | CREATE SUBSCRIPTION, lag polling, `ApplyStats`, `drop_source_{publication,slot}` |
| `orchestrator.rs` | `Migrator` — wires all stages |
| `progress.rs` | `ProgressReporter` trait + Tracing/Collecting impls |
| `preflight.rs` | Source validation, `ensure_publication_exists` (auto-create) |
| `sequences.rs` | Source→target setval at cutover |
| `resume.rs` | Resume token persistence |
| `tls.rs` | TLS-aware connection helper |

### Critical invariants

1. **Slot before dump**: `prepare_replication_slot` MUST run before pg_dump. `START_REPLICATION` MUST be deferred until AFTER dump completes (else snapshot is invalidated).
2. **Cutover is operator-driven**: apply loop NEVER exits on CaughtUp alone.
3. **Resume preserves replication origin**: subscription is disabled/re-enabled (not dropped/recreated) to avoid duplicate-key violations.
4. **Sequence sync at cutover**: migrator runs setval() on all target sequences after cutover.
5. **Publication lifecycle**: auto-created publications are tracked (`pub_auto_created`) and dropped after cutover; pre-existing ones are never dropped.

---

## 2. Design Principles (mandatory reading)

1. **Clean library / CLI separation**
   - Library returns `pg_dbmigrator::Result<T>` (`MigrationError`) only.
   - Only CLI/examples may use `anyhow`.
   - No `unwrap()`/`expect()`/`panic!()` in production paths.

2. **Side effects vs. pure logic**
   - Anything spawning a process or opening a socket sits behind a trait (`CommandRunner`, `ProgressReporter`).
   - Pure functions (`build_pg_dump_args`, `build_pg_restore_args`, `make_create_subscription_sql`, `parse_pg_lsn`) must not perform I/O — unit-testable without PostgreSQL.

3. **Configuration as data**
   - All `*Config` structs: `Serialize + Deserialize + Clone + Debug`.
   - Cross-field invariants in `validate(&self) -> Result<()>`.
   - New optional fields must have a `Default`.

4. **Cancellation everywhere**
   - Every long-running async function takes `CancellationToken`, checks `cancel.is_cancelled()` before each iteration.
   - Use `MigrationError::Cancelled` for cancellation errors.

5. **Connection-string secrecy**
   - Always call `endpoint.redacted()` before logging.
   - Passwords via `PGPASSWORD` env var (never in argv where `ps` exposes them).

6. **Contract with pg_walstream**
   - Slot creation before dump. START_REPLICATION only after dump.
   - Pinned at `0.6.2` via path dep to `../pg-walstream`. Verify `ChangeEvent`/`EventType` compatibility before bumping.

7. **No unsolicited optimisation**
   - Make only the changes the task requires. No silent async conversions, container swaps, or new crate additions.

---

## 3. Code Style

- **Logging**: `tracing` structured fields (`info!(slot = %name, "msg")`). No `println!`/`eprintln!`.
- **Types**: `#[derive(Debug, Clone)]` on all `pub` types unless impossible (trait objects, etc.).
- **Errors**: use `MigrationError::config()`/`::external()`/`::apply()` helpers. Never construct enum directly.
- **Async**: tokio runtime, `#[async_trait]`, `CancellationToken` in all loops. Never `std::thread::sleep`.
- **SQL safety**: `quote_ident`/`quote_literal` from pg_walstream. No string concatenation for values.
- **Module layout**: `use` → public types → public functions → private helpers → `#[cfg(test)] mod tests`.
- **Doc comments**: every `pub` item gets `///` covering purpose, call ordering, edge cases.

---

## 4. Testing

- Every `.rs` file has `#[cfg(test)] mod tests`
- Min **85% code coverage** (Codecov enforced)
- Test naming: `<behaviour>_<condition>` (e.g. `validate_rejects_zero_jobs`)
- No real PG in unit tests — use `RecordingRunner`, `CollectingReporter`, `StaticLagProvider`
- Integration tests: shell scripts in `tests/integration/`, registered in `run_all.sh` and `.github/workflows/ci.yml`

### Integration tests

| Script | Scenario |
|--------|----------|
| `run_offline.sh` | Simple dump → restore |
| `run_offline_split_sections.sh` | pre-data/data/post-data phases |
| `run_offline_resume.sh` | Cancel + resume token |
| `run_offline_sigint_cancel.sh` | SIGINT mid-dump → fast cancel |
| `run_offline_analyze.sh` | VACUUM ANALYZE + ANALYZE |
| `run_online.sh` | Full online: two CaughtUp + cutover |
| `run_online_updates.sh` | DML during dump+restore |
| `run_online_sustained.sh` | 60s mutations + equality gate |
| `run_online_lag_cadence.sh` | Adaptive poll cadence |
| `run_online_cancel_resume.sh` | Cancel mid-apply + resume |
| `run_online_multi_resume_sustained.sh` | 2× cancel+resume + mutations |
| `run_online_sequence_sync.sh` | Sequence sync (no PK collision) |
| `run_online_auto_pub_lifecycle.sh` | Auto-create pub + post-cutover cleanup |
| `run_online_keep_slot.sh` | keep-slot flag + pre-existing pub retained |

### Verify before commit

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace           # unit tests, no PG required
make integration                 # e2e, requires Docker
```

---

## 5. Standard Workflow for New Features

1. **Config** — extend `MigrationConfig`/`OnlineOptions` + `Default` + `validate()` + unit tests
2. **Pure functions** — update argv/SQL builders + tests for each branch
3. **Orchestrator** — wire logic in `Migrator::run_offline`/`run_online`, emit progress events via `self.report(stage, message)`
4. **CLI** — add `#[arg(...)]` in args.rs, map in `into_config` (kebab-case flags) + tests
5. **Unit tests** — ≥85% coverage for all new/modified code
6. **Integration tests** — add script in `tests/integration/`, register in `run_all.sh` + CI job in `.github/workflows/ci.yml`
7. **Documentation** — update README + examples if user-visible behaviour changes
8. **Validate** — run full lint+test suite (see §4)

---

## 6. Hard Rules (do-not list)

- ❌ No `anyhow` in library (CLI/examples only)
- ❌ No `unwrap()`/`expect()`/`panic!()` in production paths
- ❌ No `println!`/`eprintln!` — use `tracing`
- ❌ No logging passwords or full connection strings (use `redacted()`)
- ❌ No bypassing `CommandRunner` for direct `tokio::process::Command`
- ❌ No `START_REPLICATION` before pg_dump finishes
- ❌ No hard-coded crate versions in crate manifest (use workspace deps)
- ❌ No unrelated cleanup in task PRs
- ❌ No new deps without license/maintenance/equivalence check

---

## 7. Change-management Checklist (before commit / PR)

- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test --workspace` green (all unit tests pass)
- [ ] `make integration` green (requires Docker)
- [ ] Unit tests added for all new/modified code (≥ 85% coverage)
- [ ] Every modified or new `pub` API has `///` documentation
- [ ] Pure functions you changed have matching tests
- [ ] If config/CLI behaviour changed → update README, examples, and this file
- [ ] No un-redacted connection strings or passwords in logs

---

## 8. Tips for AI Agents

- **Small workspace** — most files are 200–400 lines. Read the entire file before editing. Use grep to confirm blast radius.
- **Validate after editing** — run `cargo test -p pg_dbmigrator` after every change.
- **New stage / mode** → must update ALL of: `MigrationStage` enum, `Migrator` entry point, CLI args, README, and this file.
- **pg_walstream** lives in sibling workspace (`../pg-walstream`). Pinned at `0.6.2` via path dep. Do NOT vendor/fork — propose changes upstream.
- **Library/CLI boundary** — library returns `pg_dbmigrator::Result<T>` only. `anyhow` is CLI/examples only.
- **Versions** — all dependency versions live in workspace `Cargo.toml` under `[workspace.dependencies]`. Crate manifests use `xxx.workspace = true`.

---

## 9. CI Pipeline

Jobs: `build-and-unit-test` → per-script integration jobs (parallel) → `publish` (on tag `v*.*.*`)
All integration jobs use `.github/actions/setup-integration` (Docker Compose + PG 17 client tools).
