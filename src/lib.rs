//! # jsonldag
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
//! # use jsonldag::{EventBuilder, EventLog, validate_log};
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
//! ## FCIS module layout
//!
//! The crate keeps the FCIS layering at the module level so the ownership seams
//! are explicit and testable, even though this is a single library crate (not a
//! monorepo):
//!
//! - [`utils`] — axisless, deterministic atoms (FNV-1a digest, JSONL line
//!   framing). No business types.
//! - [`vocabulary`] (`meaning_seed`) — the shared record types: [`EventId`],
//!   [`vocabulary::Hash`], [`EventRecord`].
//! - [`validation`] (`meaning_core`) — the pure invariants: hash-chain
//!   verification, parent-DAG dangling-reference + cycle detection.
//! - [`port`] — the required-capability port [`EventStore`] + [`StoreError`],
//!   the contract every store adapter implements.
//! - [`io`] (`run_kit`) — direct-call JSONL read/append atoms + the concrete
//!   adapter types ([`FileStore`], [`InMemoryStore`]).
//! - [`EventLog`] (`use_flow`) — the high-level store: open/append/replay,
//!   generic over the [`EventStore`] port.
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

pub use io::{events_file, FileStore, InMemoryStore, JsonlError};
pub use log::{EventLog, FileLog, LogError, MemoryLog, Replay};
pub use port::{EventStore, StoreError};
pub use validation::{validate_log, ValidationError};
pub use vocabulary::{BuildError, EventBuilder, EventId, EventRecord, Hash};

mod log;

#[cfg(test)]
mod tests;
