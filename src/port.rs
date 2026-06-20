//! The required-capability port: the [`EventStore`] trait + [`StoreError`].
//!
//! This is the contract the crate revolves around. It is a **root-level**
//! module (not buried in the `use_flow`) so the layering reads in ownership
//! order:
//!
//! 1. **port** (here) — the capability contract. Knows only the shared
//!    `EventRecord` type (from [`crate::vocabulary`]) + [`StoreError`].
//! 2. **io** (`run_kit`) — concrete adapter *types* (`FileStore`,
//!    `InMemoryStore`). Owns the mechanism (a path / a buffer).
//! 3. **port** (here) — the `impl EventStore for FileStore/InMemoryStore`
//!    blocks, tying the contract to the adapter types. This lives with the
//!    port, not the adapter, so the contract module is the single place that
//!    binds "capability" to "concrete mechanism".
//! 4. **log** (`use_flow`) — `EventLog<S: EventStore>`, a *consumer* of the port.
//!
//! A reader sees the trait at the crate root and knows immediately it is the
//! central contract; adapters implement it; the `use_flow` depends on it; none of
//! them own it.

use crate::io::{self, FileStore, InMemoryStore};
use crate::vocabulary::EventRecord;

/// The error an [`EventStore`] implementation can return. Today every concrete
/// adapter is JSONL-backed, so this wraps [`crate::JsonlError`]; a non-JSONL
/// adapter carries its own error via the `Other` arm.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// A JSONL I/O or parse failure.
    #[error(transparent)]
    Jsonl(#[from] crate::JsonlError),
    /// A backend-specific failure (e.g. a remote store transport error).
    #[error("{0}")]
    Other(String),
}

/// The required-capability port a concrete store adapter implements.
///
/// The `use_flow` ([`crate::EventLog`]) depends on this trait, never on a
/// concrete backend. Implementations own the persistence mechanism (a file, a
/// buffer, a remote store) and must keep `read_records` + `append_record`
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

// Adapter impls live with the port (the contract module binds capability to
// concrete mechanism). The adapter *types* come from run_kit (`io`); the port
// owns the `impl` so the wiring is in one place.

impl EventStore for FileStore {
    fn read_records(&self) -> Result<Vec<EventRecord>, StoreError> {
        Ok(io::read_all::<EventRecord>(self.path())?)
    }

    fn append_record(&mut self, record: &EventRecord) -> Result<(), StoreError> {
        Ok(io::append_line(self.path(), record)?)
    }
}

impl EventStore for InMemoryStore {
    fn read_records(&self) -> Result<Vec<EventRecord>, StoreError> {
        Ok(self.records.clone())
    }

    fn append_record(&mut self, record: &EventRecord) -> Result<(), StoreError> {
        self.records.push(record.clone());
        Ok(())
    }
}

// `pg` backend: the PgStore adapter (run_kit) implements the same port. Gated
// behind the `pg` feature so the crate stays zero-network by default. PgStore's
// own errors map to StoreError::Other (the non-JSONL backend arm).
#[cfg(feature = "pg")]
impl EventStore for crate::pg::PgStore {
    fn read_records(&self) -> Result<Vec<EventRecord>, StoreError> {
        Ok(Self::read_records(self)?)
    }

    fn append_record(&mut self, record: &EventRecord) -> Result<(), StoreError> {
        Ok(Self::append_record(self, record)?)
    }
}

#[cfg(feature = "pg")]
impl From<crate::pg::PgStoreError> for StoreError {
    fn from(e: crate::pg::PgStoreError) -> Self {
        Self::Other(e.to_string())
    }
}
