//! The storage port: the [`EventStore`] trait + [`StoreError`].
//!
//! This is the contract the high-level store ([`crate::EventLog`]) depends on,
//! kept separate from the concrete backends so a reader can understand the
//! contract without loading any backend.
//!
//! Ownership direction:
//!
//! 1. **port** (here) — the capability contract. Knows only the shared
//!    `EventRecord` type (from [`crate::vocabulary`]) + [`StoreError`].
//!    Deliberately imports NO concrete backend module.
//! 2. **backends** ([`crate::io`], [`crate::pg`]) — concrete store types
//!    (`FileStore`, `InMemoryStore`, `PgStore`). Each owns its own
//!    `impl EventStore` block, importing the port (`backend -> port`), so the
//!    wiring lives with the mechanism, never in the contract module.

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
/// The high-level store ([`crate::EventLog`]) depends on this trait, never on a
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
