//! JSONL mechanism layer — holds two FCIS bands:
//!
//! - **`run_kit`** atoms ([`append_line`], [`read_all`], [`events_file`]):
//!   vocabulary-free, generic, direct-call JSONL helpers. No capability seam,
//!   no semantic types. These match the doctrine's run-kit shape (open-input,
//!   direct-call, no trait dispatch).
//! - **`effect_tool`** adapter ([`FileStore`]): a concrete **filesystem**
//!   mechanism adapter that implements the [`EventStore`](crate::EventStore)
//!   port. The doctrine classifies a filesystem adapter dispatched through a
//!   capability seam as `effect_tool`, not `run_kit`.
//! - **`run_kit`**/local adapter ([`InMemoryStore`]): the no-mechanism buffer
//!   backend (a `Vec`). No external mechanism, so it is not `effect_tool`; it
//!   is the substitution/test-double backend.
//!
//! The atoms and the adapter types share one module because the adapters are
//! thin wrappers over the atoms; the role split is documented per-item.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

/// An I/O error from the JSONL atoms, with the path + operation context.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JsonlError {
    /// A filesystem operation (open/read/write/mkdir) failed.
    #[error("{op} failed for {path}: {message}")]
    Io {
        /// The operation that failed (`"read"`, `"write"`, `"open-append"`, ...).
        op: &'static str,
        /// The path the operation targeted.
        path: String,
        /// The underlying error message.
        message: String,
    },
    /// A JSONL line could not be parsed as `T`.
    #[error("failed to parse line {line} of {path} as JSON: {message}")]
    Parse {
        /// The path of the file containing the bad line.
        path: String,
        /// The 1-based line number.
        line: usize,
        /// The underlying serde error message.
        message: String,
    },
}

/// Append one record as a JSONL line to `path`, creating the file if needed.
/// Opens in append mode (atomic for line-sized writes on POSIX).
///
/// # Errors
/// [`JsonlError::Io`] if the file cannot be opened/written, or framing
/// serialization fails.
pub fn append_line<T: Serialize>(path: &Path, record: &T) -> Result<(), JsonlError> {
    let line = crate::utils::frame_line(record).map_err(|e| JsonlError::Io {
        op: "serialize",
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    append_bytes(path, &line)
}

/// Read every JSONL line from `path` into `Vec<T>`, in file order. Blank lines
/// are skipped; a missing file yields `Vec::new()` (a fresh log).
///
/// # Errors
/// [`JsonlError::Io`] on read failure, [`JsonlError::Parse`] on a malformed
/// line.
pub fn read_all<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, JsonlError> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => {
            return Err(JsonlError::Io {
                op: "read",
                path: path.display().to_string(),
                message: e.to_string(),
            });
        },
    };
    let text = String::from_utf8_lossy(&bytes);
    let mut out = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(trimmed) {
            Ok(value) => out.push(value),
            Err(e) => {
                return Err(JsonlError::Parse {
                    path: path.display().to_string(),
                    line: idx + 1,
                    message: e.to_string(),
                });
            },
        }
    }
    Ok(out)
}

fn append_bytes(path: &Path, bytes: &[u8]) -> Result<(), JsonlError> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| JsonlError::Io {
                op: "mkdir",
                path: parent.display().to_string(),
                message: e.to_string(),
            })?;
        }
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| JsonlError::Io {
            op: "open-append",
            path: path.display().to_string(),
            message: e.to_string(),
        })?;
    file.write_all(bytes).map_err(|e| JsonlError::Io {
        op: "write",
        path: path.display().to_string(),
        message: e.to_string(),
    })?;
    Ok(())
}

/// Canonical events filename for a store directory.
#[must_use]
pub fn events_file(store_dir: &Path) -> PathBuf {
    store_dir.join("events.jsonl")
}

/// A file-backed JSONB-line store: owns one `events.jsonl` path.
///
/// FCIS `effect_tool`: a concrete **filesystem** mechanism adapter dispatched
/// through the [`EventStore`](crate::EventStore) port. (A generic open-input
/// helper with no capability seam would be `run_kit`; because callers reach the
/// filesystem through the port, this is the effect adapter.) The
/// `impl EventStore` lives in this module.
#[derive(Debug, Clone)]
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    /// Build a file store over `path`.
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The events file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// An in-memory store: owns a buffer of records. No persistence.
///
/// FCIS `run_kit`/local: no external mechanism (a `Vec`), so it is not
/// `effect_tool`. It is the no-op substitution/test-double backend. The
/// `impl EventStore` lives in this module.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    /// The record buffer. `pub(crate)` so this module's `EventStore` impl can
    /// read/push without a public mutator surface.
    pub(crate) records: Vec<crate::vocabulary::EventRecord>,
}

impl InMemoryStore {
    /// Build an empty in-memory store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

// Adapter port impls live WITH the adapter (the doctrine's `adapter -> port`
// direction): each effect_tool/run_kit adapter imports the port and implements
// it, so the wiring is owned by the mechanism, never by the contract module.

impl crate::port::EventStore for FileStore {
    fn read_records(&self) -> Result<Vec<crate::vocabulary::EventRecord>, crate::port::StoreError> {
        Ok(read_all::<crate::vocabulary::EventRecord>(self.path())?)
    }

    fn append_record(
        &mut self,
        record: &crate::vocabulary::EventRecord,
    ) -> Result<(), crate::port::StoreError> {
        Ok(append_line(self.path(), record)?)
    }
}

impl crate::port::EventStore for InMemoryStore {
    fn read_records(&self) -> Result<Vec<crate::vocabulary::EventRecord>, crate::port::StoreError> {
        Ok(self.records.clone())
    }

    fn append_record(
        &mut self,
        record: &crate::vocabulary::EventRecord,
    ) -> Result<(), crate::port::StoreError> {
        self.records.push(record.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_read_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        append_line(&path, &serde_json::json!({"n": 1})).unwrap();
        append_line(&path, &serde_json::json!({"n": 2})).unwrap();
        let rows: Vec<serde_json::Value> = read_all(&path).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["n"], 1);
        assert_eq!(rows[1]["n"], 2);
    }

    #[test]
    fn missing_file_reads_empty() {
        let path = Path::new("/tmp/daglog-nonexistent-xyz/events.jsonl");
        let rows: Vec<serde_json::Value> = read_all(&path).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn blank_lines_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, "{\"n\":1}\n\n  \n{\"n\":2}\n").unwrap();
        let rows: Vec<serde_json::Value> = read_all(&path).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn malformed_line_errors_with_line_number() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        std::fs::write(&path, "{\"n\":1}\nnot-json\n").unwrap();
        let err = read_all::<serde_json::Value>(&path).unwrap_err();
        match err {
            JsonlError::Parse { line, .. } => assert_eq!(line, 2),
            JsonlError::Io { .. } => panic!("expected Parse, got Io"),
        }
    }
}
