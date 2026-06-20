//! The required-capability port: the [`EventStore`] trait + [`StoreError`].
//!
//! This is the local contract the `use_flow` ([`crate::EventLog`]) depends on.
//! In FCIS terms it is a **same-axis local port**, not an `axis_link`: daglog
//! is a single-axis library, and the doctrine says same-axis capability seams
//! stay as local traits inside the owner capsule rather than being elevated to
//! `axis_link(required_port)` (which exists for *cross-axis* plugin seams).
//!
//! Ownership direction (the doctrine's required-port rule):
//!
//! 1. **port** (here) — the capability contract. Knows only the shared
//!    `EventRecord` type (from [`crate::vocabulary`]) + [`StoreError`].
//!    Deliberately imports NO concrete adapter module: a reader can understand
//!    the contract without loading any backend.
//! 2. **adapters** ([`crate::io`], [`crate::pg`]) — concrete `effect_tool`
//!    types (`FileStore`, `InMemoryStore`, `PgStore`). Each adapter owns its
//!    own `impl EventStore` block, importing the port (`adapter -> port`), so
//!    the wiring lives with the mechanism, never in the contract module.

use crate::vocabulary::EventRecord;

/// The error an [`EventStore`] implementation can return. JSONL-backed
/// adapters surface [`crate::JsonlError`]; a non-JSONL adapter (e.g. the
/// `pg` backend) carries its own error via the `Other` arm.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// A JSONL I/O or parse failure.
    #[error(transparent)]
    Jsonl(#[from] crate::JsonlError),
    /// A backend-specific failure (e.g. a Postgres transport error).
    #[error("{0}")]
    Other(String),
}

/// The required-capability port a concrete store adapter implements.
///
/// The `use_flow` ([`crate::EventLog`]) depends on this trait, never on a
/// concrete backend. Implementations own the persistence mechanism (a file, a
/// buffer, a database) and must keep `read_records` + `append_record`
/// consistent: a record appended by `append_record` must appear in the next
/// `read_records`.
pub trait EventStore {
    /// Read every record in stream (append) order.
    ///
    /// # Errors
    /// See the implementor; typically [`StoreError::Jsonl`] for file-backed
    /// stores.
    fn read_records(&self) -> Result<Vec<EventRecord>, StoreError>;

    /// Append one record as the new stream tip. Must be durable to the extent
    /// the backend promises before returning.
    ///
    /// # Errors
    /// See the implementor.
    fn append_record(&mut self, record: &EventRecord) -> Result<(), StoreError>;
}
