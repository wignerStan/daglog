//! FCIS `use_flow` — the high-level store.
//!
//! The `use_flow` depends on the [`EventStore`] **capability port** (a trait),
//! not on a concrete backend. Concrete adapters live in [`crate::io`]
//! (`FileStore`, `InMemoryStore`) — they own the mechanism (a path / a buffer)
//! and implement the port. [`EventLog`] wires the port to the domain: it runs
//! the append protocol (assign stream links + reject dangling parents) and the
//! replay/validate surface, delegating all persistence to the injected store.
//!
//! This is the "define a trait, not an impl" seam: the store is a *required
//! capability* the consumer depends on, not an owned mechanism a struct method
//! surface hides. A caller can supply any `EventStore` (a test fake, a remote
//! store, an encrypted backend) without the `use_flow` knowing how bytes land.

use std::path::Path;

use crate::io::{self, FileStore, InMemoryStore, JsonlError};
use crate::port::{EventStore, StoreError};
use crate::validation::validate_log;
use crate::vocabulary::{EventId, EventRecord, Hash};

/// The stream tip (last record's id + hash), or `(None, empty hash)` for an
/// empty log — the predecessor link the next appended record will commit to.
fn tip_of(records: &[EventRecord]) -> (Option<EventId>, Hash) {
    records.last().map_or_else(
        || (None, Hash::default()),
        |r| (Some(r.id.clone()), r.content_hash.clone()),
    )
}

/// An append-only event log with a hash-chain stream + branch/merge DAG.
///
/// Generic over the [`EventStore`] port: the persistence mechanism is injected,
/// not owned. Open one with [`EventLog::open`] (file), [`EventLog::in_memory`],
/// or [`EventLog::new`] with any custom `EventStore`. Append records with
/// [`EventLog::append`]; the log assigns the stream predecessor link (the
/// previous line) so the linear tamper-detection chain is correct. Replay with
/// [`EventLog::replay`] and verify with [`validate_log`](crate::validate_log).
pub struct EventLog<S: EventStore> {
    store: S,
}

/// The default file-backed log: `EventLog<FileStore>`.
pub type FileLog = EventLog<FileStore>;
/// The default in-memory log: `EventLog<InMemoryStore>`.
pub type MemoryLog = EventLog<InMemoryStore>;
/// The Postgres-backed log (requires the `pg` feature): `EventLog<PgStore>`.
#[cfg(feature = "pg")]
pub type PgLog = EventLog<crate::pg::PgStore>;

/// The replayed contents of a log: every record in stream order plus the
/// current tip (the last record's id + hash).
#[derive(Debug, Clone)]
pub struct Replay {
    /// Every record in stream (file) order.
    pub records: Vec<EventRecord>,
    /// The id of the last record, if any.
    pub tip_id: Option<EventId>,
    /// The content hash of the last record, or an empty hash if empty.
    pub tip_hash: Hash,
}

impl Replay {
    /// Number of records in the log.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the log has zero records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// An error from the store API.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// An I/O or parse failure from the underlying store.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Appended a record whose parent ids are not all present in the current
    /// log. This is checked at append time so a dangling merge never lands on
    /// disk.
    #[error("append rejected: parent `{0}` is not in the log")]
    UnknownParent(String),
}

impl From<JsonlError> for LogError {
    fn from(e: JsonlError) -> Self {
        Self::Store(StoreError::from(e))
    }
}

impl<S: EventStore> EventLog<S> {
    /// Build a log over an injected store. The caller supplies any
    /// [`EventStore`] (file, in-memory, test fake, remote).
    #[must_use]
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    /// Borrow the underlying store.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Replay the log: read every record in stream order and compute the tip.
    ///
    /// # Errors
    /// [`LogError::Store`] on read/parse failure.
    pub fn replay(&self) -> Result<Replay, LogError> {
        let records = self.store.read_records()?;
        let (tip_id, tip_hash) = tip_of(&records);
        Ok(Replay {
            records,
            tip_id,
            tip_hash,
        })
    }

    /// Append one record. The log assigns the stream predecessor link (the
    /// previous line's id + hash) and persists it via the injected store before
    /// returning.
    ///
    /// `record`'s `parent_ids` are checked against the current log contents: a
    /// parent that is not present rejects the append (so a dangling merge never
    /// lands on disk). To start a new branch, append a record with no parents.
    ///
    /// # Errors
    /// [`LogError::UnknownParent`] if a parent id is absent, or
    /// [`LogError::Store`] on persistence failure.
    pub fn append(&mut self, mut record: EventRecord) -> Result<EventId, LogError> {
        // Read the current contents once; derive both the stream tip and the
        // known-id set from that single pass.
        let existing = self.store.read_records()?;
        let (tip_id, tip_hash) = tip_of(&existing);

        let known: std::collections::BTreeSet<&str> =
            existing.iter().map(|r| r.id.0.as_str()).collect();
        for parent in &record.parent_ids {
            if !known.contains(parent.0.as_str()) {
                return Err(LogError::UnknownParent(parent.0.clone()));
            }
        }

        record.stream_prev_id = tip_id;
        record.stream_prev_hash = tip_hash;
        // The stream link is NOT part of the content hash (content is
        // position-independent); only the stored content_hash matters.

        let id = record.id.clone();
        self.store.append_record(&record)?;
        Ok(id)
    }

    /// Validate the current log against every invariant (hash-chain + parent
    /// DAG + acyclicity). A thin wrapper over
    /// [`validate_log`](crate::validate_log) that replays first.
    ///
    /// # Errors
    /// [`ValidationError::Replay`](crate::ValidationError) on read failure, or
    /// the first other [`ValidationError`](crate::ValidationError) on an
    /// invariant breach.
    pub fn validate(&self) -> Result<(), crate::validation::ValidationError> {
        let replay = self.replay().map_err(|e| match e {
            LogError::Store(s) => crate::validation::ValidationError::from(s),
            LogError::UnknownParent(p) => crate::validation::ValidationError::DanglingParent {
                id: String::new(),
                parent_id: p,
            },
        })?;
        validate_log(&replay.records)
    }
}

impl EventLog<FileStore> {
    /// Open (or create) an on-disk log at `<store_dir>/events.jsonl`. The file
    /// is created on first append; replay reads whatever is there.
    #[must_use]
    pub fn open(store_dir: impl AsRef<Path>) -> Self {
        Self::new(FileStore::new(io::events_file(store_dir.as_ref())))
    }

    /// Open a log backed by an explicit events file path.
    #[must_use]
    pub fn open_file(events_path: impl AsRef<Path>) -> Self {
        Self::new(FileStore::new(events_path.as_ref().to_path_buf()))
    }

    /// The path backing the file log.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.store.path()
    }
}

impl EventLog<InMemoryStore> {
    /// Create an empty in-memory log (no persistence). Useful for tests.
    #[must_use]
    pub fn in_memory() -> Self {
        Self::new(InMemoryStore::new())
    }
}

impl Default for EventLog<InMemoryStore> {
    fn default() -> Self {
        Self::in_memory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EventBuilder;

    #[test]
    fn append_assigns_stream_chain_and_validates() {
        let mut log = EventLog::in_memory();
        let a = log
            .append(
                EventBuilder::new(serde_json::json!({"k": "a"}))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let b = log
            .append(
                EventBuilder::new(serde_json::json!({"k": "b"}))
                    .parent(a.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let _c = log
            .append(
                EventBuilder::new(serde_json::json!({"k": "c"}))
                    .parent(b.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let replay = log.replay().unwrap();
        assert_eq!(replay.len(), 3);
        log.validate().unwrap();
    }

    #[test]
    fn branch_and_merge_round_trip_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = EventLog::open(dir.path());
        let root = log
            .append(
                EventBuilder::new(serde_json::json!({"branch": "main"}))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let b1 = log
            .append(
                EventBuilder::new(serde_json::json!({"branch": "b1"}))
                    .parent(root.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let b2 = log
            .append(
                EventBuilder::new(serde_json::json!({"branch": "b2"}))
                    .parent(root.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let _merge = log
            .append(
                EventBuilder::new(serde_json::json!({"merge": true}))
                    .parents(&[b1.clone(), b2.clone()])
                    .build()
                    .unwrap(),
            )
            .unwrap();
        log.validate().unwrap();

        // Re-open from disk: a brand-new EventLog over the same file replays
        // the full branch/merge history.
        let reopened = EventLog::open(dir.path());
        let replay = reopened.replay().unwrap();
        assert_eq!(replay.len(), 4);
        reopened.validate().unwrap();
    }

    #[test]
    fn dangling_parent_rejected_at_append() {
        let mut log = EventLog::in_memory();
        let err = log
            .append(
                EventBuilder::new(serde_json::json!({}))
                    .parent(EventId::from("ghost"))
                    .build()
                    .unwrap(),
            )
            .unwrap_err();
        assert!(matches!(err, LogError::UnknownParent(_)));
    }

    #[test]
    fn parentless_appends_continue_linear_chain() {
        let mut log = EventLog::in_memory();
        log.append(
            EventBuilder::new(serde_json::json!({"n": 1}))
                .build()
                .unwrap(),
        )
        .unwrap();
        log.append(
            EventBuilder::new(serde_json::json!({"n": 2}))
                .build()
                .unwrap(),
        )
        .unwrap();
        log.append(
            EventBuilder::new(serde_json::json!({"n": 3}))
                .build()
                .unwrap(),
        )
        .unwrap();
        // No semantic parents, but the stream chain still links them linearly,
        // so validation passes.
        log.validate().unwrap();
        let replay = log.replay().unwrap();
        assert_eq!(replay.len(), 3);
    }

    #[test]
    fn custom_store_adapter_plugs_into_eventlog() {
        // A hand-rolled EventStore proves the port is real: the use_flow works
        // over any adapter, not just the built-in file/in-memory ones.
        use std::cell::RefCell;
        struct CountingStore(RefCell<Vec<EventRecord>>);
        impl EventStore for CountingStore {
            fn read_records(&self) -> Result<Vec<EventRecord>, StoreError> {
                Ok(self.0.borrow().clone())
            }
            fn append_record(&mut self, record: &EventRecord) -> Result<(), StoreError> {
                self.0.borrow_mut().push(record.clone());
                Ok(())
            }
        }
        let mut log = EventLog::new(CountingStore(RefCell::new(Vec::new())));
        log.append(
            EventBuilder::new(serde_json::json!({"x": 1}))
                .build()
                .unwrap(),
        )
        .unwrap();
        log.validate().unwrap();
    }
}
