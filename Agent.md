# Agent.md — pg_dbmigrator Development Guide

> This document is intended for both AI coding agents and human contributors.
> It captures the architecture, conventions, and non-negotiable rules for the
> `pg_dbmigrator` Rust PostgreSQL migration tool.
> **Read this file in full before modifying any source.**

---

## 1. Project Overview


| Mode      | Behaviour                                                                                                                                                                                                  |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Offline` | `pg_dump` → `pg_restore`. One-shot copy. Sequences are carried inside the dump, no extra step.                                                                                                             |
| `Online`  | Create a logical replication slot with `EXPORT_SNAPSHOT` → snapshot-consistent dump → restore → `CREATE SUBSCRIPTION` on target so PostgreSQL's apply worker streams WAL → on cutover, sync sequence values. |

### Apply path

After `pg_dump` / `pg_restore` the orchestrator drops the
[`pg_walstream`] stream connection (whose only job was to keep the exported
snapshot alive across the dump) and issues
`CREATE SUBSCRIPTION ... WITH (create_slot=false, slot_name='<existing>',
enabled=true, copy_data=false)` on the target. The pre-existing slot is
re-used — `create_slot=false` is the critical bit — and the built-in apply
worker streams WAL from the source's `confirmed_flush_lsn`.

`pg_walstream` is now used **only** as a slot-creation helper inside
[snapshot.rs](crates/pg_dbmigrator/src/snapshot.rs). There is no longer an
in-process apply path; the previous `OnlineApplyEngine` enum and
`--apply-engine` CLI flag have been removed.

[`pg_walstream`]: https://github.com/isdaniel/pg-walstream

### Workspace layout

```
Cargo.toml                          # workspace root, central versions/deps
.cargo/config.toml                  # `cargo pg_dbmigrator` alias
crates/
  pg_dbmigrator/                      # library crate (package: pg_dbmigrator)
  pg_dbmigrator-cli/                  # CLI crate (package: pg_dbmigrator-cli, bin: pg_dbmigrator)
examples/offline_migration/         # integration example for the library
examples/online_migration/
```

> **Single source of truth.** Versions, edition, authors, and external
> dependency versions live in the workspace [Cargo.toml](Cargo.toml) under
> `[workspace.package]` / `[workspace.dependencies]`. Sub-crates reference
> them via `xxx.workspace = true`. **Do not** hard-code versions in sub-crate
> manifests.

### Library module map

| File                                                                       | Responsibility                                                              |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| [crates/pg_dbmigrator/src/lib.rs](crates/pg_dbmigrator/src/lib.rs)                   | Crate entry point, re-exports, `#![deny]`/`#![warn]` lints                  |
| [crates/pg_dbmigrator/src/config.rs](crates/pg_dbmigrator/src/config.rs)             | `MigrationConfig` / `EndpointConfig` / `OnlineOptions` and validation       |
| [crates/pg_dbmigrator/src/error.rs](crates/pg_dbmigrator/src/error.rs)               | `MigrationError` (`thiserror`) + `Result<T>` alias                          |
| [crates/pg_dbmigrator/src/dump.rs](crates/pg_dbmigrator/src/dump.rs)                 | `pg_dump` wrapper, `CommandRunner` trait, pure argv builder                 |
| [crates/pg_dbmigrator/src/restore.rs](crates/pg_dbmigrator/src/restore.rs)           | `pg_restore` / `psql` wrapper                                               |
| [crates/pg_dbmigrator/src/snapshot.rs](crates/pg_dbmigrator/src/snapshot.rs)         | Replication slot creation + exported snapshot retrieval                     |
| [crates/pg_dbmigrator/src/native_apply.rs](crates/pg_dbmigrator/src/native_apply.rs) | `CREATE SUBSCRIPTION` apply path + `pg_replication_slots` lag polling, `ApplyStats`, `parse_pg_lsn` |
| [crates/pg_dbmigrator/src/orchestrator.rs](crates/pg_dbmigrator/src/orchestrator.rs) | `Migrator`, wires all stages together                                       |
| [crates/pg_dbmigrator/src/progress.rs](crates/pg_dbmigrator/src/progress.rs)         | `ProgressReporter` trait + Tracing/Collecting implementations               |
| [crates/pg_dbmigrator/src/preflight.rs](crates/pg_dbmigrator/src/preflight.rs)       | Pre-migration checks (target empty, source `wal_level=logical`, slot/sender capacity) |
| [crates/pg_dbmigrator/src/sequences.rs](crates/pg_dbmigrator/src/sequences.rs)       | Source→target sequence value sync at cutover (closes the PG logical-replication sequence gap) |

### Pipeline stages (`MigrationStage`)

`Validate → PrepareSnapshot* → Dump → Restore → StreamApply* → Lag* → CaughtUp* → Cutover* → Complete`
(stages marked `*` are Online-only). Any new stage must be added in **both**
[progress.rs](crates/pg_dbmigrator/src/progress.rs) (the enum) and
[orchestrator.rs](crates/pg_dbmigrator/src/orchestrator.rs) /
[replicate.rs](crates/pg_dbmigrator/src/replicate.rs) (the reporting site).

### Online cutover model

The customer drives cutover with **SIGINT (Ctrl+C)**, mirroring the Azure
DMS "Cut over" button:

1. After `Restore`, `native_apply::run_native_apply` issues
   `CREATE SUBSCRIPTION` on the target and polls
   `pg_replication_slots.confirmed_flush_lsn` against
   `pg_current_wal_flush_lsn()` on the source every
   `CutoverConfig::poll_interval`.
2. Each poll emits a `Lag` heartbeat (`lag_bytes`, `source_lsn`,
   `received_lsn`, `applied_lsn` in `detail`) — the operator's
   continuous bytes-behind read-out.
3. When the lag drops at or below `CutoverConfig::lag_threshold_bytes`, a
   one-shot `CaughtUp` event is emitted.
4. The CLI (`crates/pg_dbmigrator-cli/src/main.rs`) installs a SIGINT handler that
   calls `CutoverHandle::request()` on the first Ctrl+C. The apply loop
   sees the request on its next poll, runs `ALTER SUBSCRIPTION ... DISABLE`
   and (unless `--keep-subscription` is set) `DROP SUBSCRIPTION`, emits
   `Cutover`, and `Migrator::run` returns with
   `MigrationOutcome::cutover_triggered() == true`.
5. A second SIGINT cancels via the `CancellationToken` (escape hatch). See
   `classify_sigint` in `crates/pg_dbmigrator-cli/src/main.rs`.

Cutover is **always operator-driven**: the apply loop never exits on
`CaughtUp` alone. The `lag_threshold_bytes` knob is purely advisory —
it controls when the one-shot `CaughtUp` event fires, never whether the
loop terminates.

### Sequence sync at cutover (online only)

PostgreSQL logical replication does **not** replicate sequence values —
the target's sequences stay frozen at whatever `pg_dump`/`pg_restore`
baked in. Without intervention, the first INSERT after cutover can
collide with rows the apply worker streamed from the source.

[`sequences.rs`](crates/pg_dbmigrator/src/sequences.rs) closes that gap:

1. After the operator presses Ctrl+C and `run_native_apply` returns with
   `cutover_triggered = true`, the orchestrator calls
   `sync_sequences(source, target, schemas)`.
2. `collect_source_sequences` reads `pg_class` joined with
   `pg_sequence_last_value(...)`; `apply_sequences_to_target` runs
   `setval('"schema"."name"'::regclass, $1::bigint, true)` on the target
   for each one.
3. Per-sequence failures are logged with `warn!` and counted, but never
   abort the migration — a managed-PG role-permission issue should not
   roll back an otherwise-successful cutover. The aggregate count is
   emitted as a `Cutover` progress event.
4. The behaviour can be turned off via
   `OnlineOptions.sync_sequences_on_cutover = false` (CLI flag
   `--no-sequence-sync`).

The SQL layer escapes both halves of the qualified name: `quote_ident`
for any embedded `"`, then `quote_literal` for any embedded `'` so the
resulting `'...'::regclass` string literal parses cleanly.

### Filtering schemas / tables

`MigrationConfig.exclude_schemas` and `exclude_tables` propagate to
`pg_dump` as `--exclude-schema=` / `--exclude-table=` arguments. Use
them to skip very large or transient tables (or schemas owned by
background services) that should not be part of the cutover. Online
mode still captures their changes via the slot if they are in the
publication, so combine with a narrower `pg_dbmigrator_pub` definition
for full coverage.

---

## 2. Design Principles (mandatory reading)

1. **Clean library / CLI separation**
   - The library only returns `pg_dbmigrator::Result<T>` (`MigrationError`).
   - Only the CLI is allowed to use `anyhow`; it converts library errors into
     terminal output and exit codes.
   - **No** `unwrap()` / `expect()` / `panic!()` in production code paths.
     Panics are reserved for true invariant violations (i.e. bugs).

2. **Side effects vs. pure logic**
   - Anything that spawns a process or opens a socket must sit behind a
     trait (e.g. `CommandRunner`, `ProgressReporter`).
   - Pure functions (`build_pg_dump_args`, `build_pg_restore_args`,
     `ensure_replication_qs`, `statement_for`) **must not** perform I/O so
     that unit tests can validate them without PostgreSQL.

3. **Configuration as data**
   - All `*Config` structs derive `Serialize + Deserialize + Clone + Debug`.
   - Cross-field invariants live in `validate(&self) -> Result<()>`;
     `Migrator::run` calls `config.validate()` first.
   - When adding optional fields, provide a `Default` so existing call sites
     keep working with `..Default::default()`.

4. **Cancellation everywhere**
   - Every long-running async function takes a `CancellationToken` and
     checks `cancel.is_cancelled()` before each loop iteration.
   - Cancellation is part of the success path; if you must return an error
     for it, use `MigrationError::Cancelled` so the upper layer can detect it.
     See the `is_cancellation` helper in
     [replicate.rs](crates/pg_dbmigrator/src/replicate.rs#L40).

5. **Connection-string secrecy**
   - `EndpointConfig` keeps the original libpq URI verbatim. **Always** call
     `endpoint.redacted()` before logging it (the password is masked).
   - When you add new fields that may carry secrets, extend `redacted()` and
     add a unit test for it.

6. **Contract with `pg_walstream`**
   - Slot creation (`prepare_replication_slot`) must happen **before**
     `pg_dump`. `START_REPLICATION` must only be called **after** the dump
     completes — otherwise the exported snapshot is invalidated.
     The orchestrator already follows this order; do not reshuffle it.
   - We pin `pg_walstream = 0.6.2` via a path dep to the sibling workspace
     `../pg-walstream`. Before bumping the version, verify compatibility of
     the `ChangeEvent` and `EventType` enums.

7. **No unsolicited optimisation**
   - Do not silently turn sync APIs into async, swap `Vec<String>` for
     `SmallVec`, or pull in new crates. **Make only the changes the task
     requires (or that correctness requires).**

---

## 3. Code Style

> Core Rust style guidance lives in [`rust-skills`] / [`rust-async`] /
> [`rust-docs`]. The rules below are **specific to this project**.

[`rust-skills`]: https://example.invalid
[`rust-async`]: https://example.invalid
[`rust-docs`]: https://example.invalid

### 3.1 Crate-level lints

[lib.rs](crates/pg_dbmigrator/src/lib.rs#L42-L43) sets:

```rust
#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]
```

When adding a new `pub` type:
- Default to `#[derive(Debug, Clone)]` unless there is a concrete reason not
  to (trait object, contains `tokio_postgres::Client`, etc.).
- If the type contains a non-`Debug` member (e.g. `LogicalReplicationStream`),
  attach `#[allow(missing_debug_implementations)]` and explain why in a doc
  comment (see
  [snapshot.rs](crates/pg_dbmigrator/src/snapshot.rs#L26-L29)).

### 3.2 Error handling

- Library errors always go through
  [`MigrationError`](crates/pg_dbmigrator/src/error.rs#L17):
  - String-bearing variants must be built via the helpers
    `MigrationError::config(...)`, `::external(...)`, `::apply(...)`. Do not
    construct the enum directly.
  - When introducing a new error category, add a variant **and** a helper
    **and** a unit test.
- CLI/example code uses `anyhow::Result` plus `.context("...")`.
- **Never** `unwrap()` in production paths. Tests may use it for brevity.

### 3.3 Logging

- Use `tracing` macros (`info!` / `debug!` / `warn!` / `error!`).
  **Forbidden**: `println!` / `eprintln!` (the only exception is `--help`,
  which clap prints itself).
- Prefer structured fields:
  `info!(slot = %opts.slot_name, "preparing slot")` over baked-in formatting.
- Connection strings must be passed through `redacted()` before they reach
  any log line.

### 3.4 Async

- The whole library targets `tokio` as the runtime; tests use
  `#[tokio::test]` (with `(flavor = "multi_thread")` when needed).
- Public traits with async methods use `#[async_trait]` (already in
  workspace deps).
- Long-running loops, `select!`, and waits must be cancellable via the
  `CancellationToken`. **Never** call `std::thread::sleep`.

### 3.5 Naming and module organisation

- One responsibility per module. Each file starts with a `//!` doc comment
  describing **why the module exists** and **what it deliberately does not
  do**. See [apply.rs](crates/pg_dbmigrator/src/apply.rs#L1-L15) for the pattern.
- File ordering: `use` → public types → public functions → private helpers
  → `#[cfg(test)] mod tests`.
- Do not introduce `mod.rs`-style folders; keep the flat `lib.rs` + sibling
  files layout.

### 3.6 Doc comments

- Every `pub` item has a `///` comment that covers purpose, required call
  ordering, and edge cases.
- Configuration fields are documented **per field** (see
  [config.rs](crates/pg_dbmigrator/src/config.rs#L11-L33)).
- Crate- and module-level docs include an `# Examples` block, marked
  `no_run` so doc-tests do not actually connect to PostgreSQL (see
  [lib.rs](crates/pg_dbmigrator/src/lib.rs#L19-L37)).

---

## 4. Testing Discipline

- **Every `.rs` file must have a `#[cfg(test)] mod tests`**, even if it
  only holds two or three cases.
- Test naming convention: `<behaviour>_<condition>`, e.g.
  `validate_rejects_zero_jobs`,
  `build_args_includes_jobs_only_for_directory_format`.
- Do not depend on a real PostgreSQL instance:
  - Use `RecordingRunner` for dump/restore tests (see
    [orchestrator.rs](crates/pg_dbmigrator/src/orchestrator.rs#L246-L276)).
  - Use `CollectingReporter` for progress tests
    ([progress.rs](crates/pg_dbmigrator/src/progress.rs#L83)).
  - Use `statement_for(&event)` for SQL translation assertions.
- Modifying any pure function (`build_pg_dump_args`,
  `build_pg_restore_args`, `statement_for`, `ensure_replication_qs`)
  **requires** updating or adding the corresponding tests in the same PR.
- Integration scenarios live in [examples/](examples/); they double as docs
  and smoke tests and may connect to a real database.

### Test commands

```bash
# Whole workspace
cargo test --workspace

# Library only
cargo test -p pg_dbmigrator

# With logs
RUST_LOG=debug cargo test -p pg_dbmigrator -- --nocapture
```

> `cargo test --workspace` must pass **without** a running PostgreSQL.
> If a new test genuinely needs a live database, move it under examples/
> or mark it `#[ignore]`.

---

## 5. Standard Workflow for New Features

When you receive a new requirement, work in this order:

1. **Locate the affected module** using the module map in §1.
2. **Start with config** — extend `MigrationConfig` / `OnlineOptions`,
   provide `Default`, add `validate()` rules, add unit tests.
3. **Then update pure functions** — e.g. teach `build_pg_dump_args` about
   the new field; add tests for each new branch.
4. **Wire the orchestrator last** — call the new logic from
   `Migrator::run_offline` / `run_online` and emit progress events via
   `self.report(stage, message)`.
5. **Mirror it in the CLI** — add `#[arg(...)]` in
   [args.rs](crates/pg_dbmigrator-cli/src/args.rs) and map it in `into_config`. Use
   kebab-case for flag names.
6. **Update README + examples** if user-visible behaviour changes.
7. **Run lint and tests**:
   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```

---

## 6. Hard Rules (do-not list)

- ❌ Do not pull `anyhow` into the library (CLI/examples only).
- ❌ Do not `unwrap()` / `expect()` / `panic!()` in production paths.
- ❌ Do not `println!` to stdout/stderr; use `tracing`.
- ❌ Do not log passwords or full connection strings.
- ❌ Do not bypass `CommandRunner` and call `tokio::process::Command`
  directly.
- ❌ Do not invoke `START_REPLICATION` before `pg_dump` finishes (it would
  invalidate the snapshot).
- ❌ Do not hard-code external crate versions in sub-crate manifests.
- ❌ Do not “tidy up” unrelated code while doing your task.
- ❌ Do not add a new dependency without checking license, maintenance
  status, and whether the workspace already provides an equivalent.

---

## 7. Security Considerations

- Connection strings may contain passwords: any serialisation, log line, or
  error message must go through `redacted()`.
- `pg_dump` / `pg_restore` are external processes. We pass the password via
  the `PGPASSWORD` environment variable (see
  [dump.rs](crates/pg_dbmigrator/src/dump.rs#L165-L172)). **Do not** put it in
  argv where `ps` could expose it.
- SQL injection surface: `apply::statement_for` uses quoted identifiers
  (`quote_ident`) and parameter placeholders (`$1`, `$2`, …). **Do not**
  switch to string concatenation for values.
- Cancellation means the user wants to stop. After cancel, do not send
  feedback, write files, or take other actions that could leave partial
  state behind.

---

## 8. Change-management Checklist (before commit / PR)

- [ ] `cargo fmt --all` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test --workspace` green
- [ ] Every modified or new `pub` API has `///` documentation
- [ ] Pure functions you changed have matching tests
- [ ] If config/CLI behaviour changed, README, examples, and this file are
      updated
- [ ] No un-redacted connection strings or passwords appear in logs

---

## 9. Tips for AI Agents

- This is a **small, self-contained** Rust workspace. Before editing,
  use semantic search and grep to confirm the blast radius, then read full
  files for context.
- Read the entire file before editing it (most files are 200–400 lines and
  fit in a single read).
- After editing, validate with the editor's diagnostics and run the
  matching `cargo test` subset.
- If asked to add a new stage / new mode, update **all** of:
  the `MigrationStage` enum, the `Migrator` entry point, the CLI args,
  the README, and this file.
- If asked to change `pg_walstream` behaviour, note that the crate lives in
  a sibling workspace (`../pg-walstream`) and is a separate project.
  **Do not** vendor or fork its source into this repo; propose the change
  upstream instead.
