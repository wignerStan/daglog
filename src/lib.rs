//! # daglog
//!
//! An append-only [JSONL] event log with a verified hash-chain **and** a
//! branch/merge DAG — git-style forks and merges over a single
//! newline-delimited JSON stream.
//!
//! Each record carries:
//! - a content hash over its own canonical fields,
//! - the **stream** predecessor (the previous line in the file — the linear
//!   hash-chain that makes tampering detectable), and
//! - a set of **semantic parents** (`parent_ids`) that form a separate DAG, so
//!   one event can merge several branches the way a git merge commit does.
//!
//! ```
//! # use daglog::{EventBuilder, EventLog, validate_log};
//! # use serde_json::json;
//! let mut log = EventLog::in_memory();
//! let a = log.append(EventBuilder::new(json!("base")).build().unwrap()).unwrap();
//! let b = log.append(EventBuilder::new(json!("branch-b")).parent(&a).build().unwrap()).unwrap();
//! let c = log.append(EventBuilder::new(json!("branch-c")).parent(&a).build().unwrap()).unwrap();
//! // a merge: parents point back at both branch tips.
//! let _m = log.append(EventBuilder::new(json!("merge")).parents([&b, &c]).build().unwrap()).unwrap();
//! validate_log(&log.replay().unwrap().records).unwrap(); // hash-chain + parent-DAG + acyclicity
//! ```
//!
//! ## What it is / is not
//!
//! It **is**: a tamper-evident, append-only event store with fork/merge over a
//! single portable `.jsonl` file. One file is the whole database; replay
//! reconstructs the DAG.
//!
//! It **is not**: a database, an indexed query engine, or a network protocol.
//! For heavy read access, project the stream into whatever index you need.
//!
//! ## Layout
//!
//! The crate separates the pure invariants from the storage mechanism:
//!
//! - [`vocabulary`] — the shared record types: [`EventId`],
//!   [`vocabulary::Hash`], [`EventRecord`], and their canonical-identity hash
//!   material.
//! - [`validation`] — the pure invariants: hash-chain verification, parent-DAG
//!   dangling-reference + cycle detection. Effect-free: it imports nothing
//!   from the port or the adapters.
//! - [`port`] — the [`EventStore`] trait + [`StoreError`], the contract
//!   backends implement.
//! - [`io`] — JSONL file atoms ([`events_file`] etc.), the [`FileStore`]
//!   filesystem backend, and the [`InMemoryStore`] buffer backend.
//! - [`pg`] (`pg` feature) — a `PostgreSQL` JSONB backend ([`PgStore`]);
//!   zero-network unless the `pg` feature is enabled.
//! - [`EventLog`] — the high-level store: open/append/replay, generic over the
//!   [`EventStore`] port. Owns the error union that routes store failures
//!   ([`LogError::Store`]) vs invariant breaches ([`LogError::Validation`]).
//!
//! [JSONL]: https://jsonlines.org

// In test code, the strict production lints (panic, redundant_clone,
// needless_borrow, unwrap/expect) are relaxed — tests use these idioms for
// clear failure messages and convenient fixture construction. unwrap/expect are
// additionally relaxed via clippy.toml's allow-*-in-tests.
#![cfg_attr(
    test,
    allow(
        clippy::panic,
        clippy::redundant_clone,
        clippy::needless_borrow,
        clippy::unwrap_used,
        clippy::expect_used
    )
)]

pub mod io;
pub mod port;
pub mod utils;
pub mod validation;
pub mod vocabulary;

#[cfg(feature = "pg")]
pub mod pg;

pub use io::{FileStore, InMemoryStore, JsonlError, events_file};
#[cfg(feature = "pg")]
pub use log::PgLog;
pub use log::{EventLog, FileLog, LogError, MemoryLog, Replay};
pub use port::{EventStore, StoreError};
pub use validation::{ValidationError, validate_log};
pub use vocabulary::{BuildError, EventBuilder, EventId, EventRecord, Hash};

#[cfg(feature = "pg")]
pub use pg::{PgStore, PgStoreError};

mod log;

#[cfg(test)]
mod tests;
