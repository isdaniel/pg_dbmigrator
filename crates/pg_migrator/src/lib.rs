//! # pg_migrator
//!
//! A Rust library for migrating PostgreSQL databases between two endpoints.
//!
//! * **Offline** — performs `pg_dump` against the source and `pg_restore` (or
//!   `psql`) against the target. Equivalent to a one-shot dump-and-load.
//! * **Online** — first creates a logical replication slot on the source with
//!   `EXPORT_SNAPSHOT`, runs a snapshot-consistent `pg_dump` / `pg_restore`,
//!   and then continues by streaming WAL changes through
//!   [`pg_walstream`](https://crates.io/crates/pg_walstream) and applying them
//!   to the target.
//!
//! The crate is split into small, single-purpose modules so that it can be
//! consumed both as a library and from the bundled CLI binary.
//!
//! ## High level usage
//!
//! ```no_run
//! use pg_migrator::{MigrationConfig, MigrationMode, Migrator, EndpointConfig};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn run() -> pg_migrator::Result<()> {
//! let cfg = MigrationConfig {
//!     mode: MigrationMode::Offline,
//!     source: EndpointConfig::parse("postgresql://user:pw@src/db")?,
//!     target: EndpointConfig::parse("postgresql://user:pw@dst/db")?,
//!     ..MigrationConfig::default()
//! };
//!
//! let migrator = Migrator::new(cfg);
//! migrator.run(CancellationToken::new()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the `examples/` directory for full programs.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod apply;
pub mod config;
pub mod cutover;
pub mod dump;
pub mod error;
pub mod orchestrator;
pub mod preflight;
pub mod progress;
pub mod replicate;
pub mod restore;
pub mod snapshot;
pub mod tls;

pub use config::{
    CutoverConfig, EndpointConfig, MigrationConfig, MigrationMode, OnlineOptions,
    ReplicationApplyConfig,
};
pub use cutover::{CutoverHandle, LagSampler, Transition};
pub use error::{MigrationError, Result};
pub use orchestrator::Migrator;
pub use progress::{MigrationStage, ProgressEvent, ProgressReporter};
