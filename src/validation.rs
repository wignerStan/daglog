//! FCIS `meaning_core` — the pure invariants over a replayed event stream.
//! Pure: takes `&[EventRecord]`, returns `Result`. No I/O.
//!
//! Three checks:
//! 1. **Content hash** — every record's stored hash matches a fresh computation
//!    (no field was edited after append).
//! 2. **Stream chain** — every record's `stream_prev_id`/`stream_prev_hash`
//!    matches the immediately-preceding line (the linear tamper-detection
//!    chain). Ids are unique and the schema version is current.
//! 3. **Parent DAG** — every `parent_id` resolves to a known record (no
//!    dangling edges) and the parent graph is acyclic (no merge cycle).

use std::collections::{BTreeMap, BTreeSet};

use crate::vocabulary::{EventRecord, Hash, RECORD_SCHEMA_VERSION};

/// A violation of one of the log invariants.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValidationError {
    /// A record declares a `schema_version` this validator does not recognize.
    #[error("record `{id}` has unsupported schema version `{version}`")]
    UnsupportedSchema {
        /// The id of the offending record.
        id: String,
        /// The unrecognized schema version string.
        version: String,
    },
    /// The same id appears more than once in the stream.
    #[error("duplicate record id `{0}`")]
    DuplicateId(String),
    /// A record's stored content hash does not match a recomputation (a field
    /// was edited after append, or the hash is wrong).
    #[error("record `{id}` content hash mismatch: expected {expected}, stored {actual}")]
    ContentHash {
        /// The id of the offending record.
        id: String,
        /// The freshly-recomputed hash.
        expected: Hash,
        /// The hash stored on the record.
        actual: Hash,
    },
    /// A record's stream-predecessor pointer does not match the previous line.
    #[error(
        "record `{id}` breaks the stream chain: expected prev {expected_prev:?}, found {actual_prev:?}"
    )]
    StreamChain {
        /// The id of the offending record.
        id: String,
        /// The predecessor id the chain expected.
        expected_prev: Option<String>,
        /// The predecessor id the record actually stored.
        actual_prev: Option<String>,
    },
    /// A record's stream-predecessor hash does not match the previous line.
    #[error("record `{id}` stream-prev hash mismatch: expected {expected}, stored {actual}")]
    StreamPrevHash {
        /// The id of the offending record.
        id: String,
        /// The predecessor hash the chain expected.
        expected: Hash,
        /// The predecessor hash the record actually stored.
        actual: Hash,
    },
    /// A parent edge points at a record id not present in the stream.
    #[error("record `{id}` has dangling parent `{parent_id}`")]
    DanglingParent {
        /// The id of the record with the dangling edge.
        id: String,
        /// The parent id that does not resolve to any record.
        parent_id: String,
    },
    /// The parent graph contains a cycle (a merge that loops back on itself).
    #[error("parent graph contains a cycle involving `{0}`")]
    ParentCycle(String),
    /// A read/parse failure while replaying the stream (the log could not be
    /// read far enough to validate the invariants).
    #[error(transparent)]
    Replay(#[from] crate::port::StoreError),
}

impl From<crate::io::JsonlError> for ValidationError {
    fn from(e: crate::io::JsonlError) -> Self {
        Self::Replay(crate::port::StoreError::from(e))
    }
}

/// Validate a replayed event stream against every invariant. Records MUST be in
/// file (stream) order.
///
/// # Errors
/// Returns the first [`ValidationError`] found.
pub fn validate_log(records: &[EventRecord]) -> Result<(), ValidationError> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut graph: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut prev_id: Option<String> = None;
    let mut prev_hash = Hash(String::new());

    for record in records {
        if record.schema_version != RECORD_SCHEMA_VERSION {
            return Err(ValidationError::UnsupportedSchema {
                id: record.id.0.clone(),
                version: record.schema_version.clone(),
            });
        }
        if !seen.insert(record.id.0.clone()) {
            return Err(ValidationError::DuplicateId(record.id.0.clone()));
        }

        let expected_hash = record.compute_hash();
        if record.content_hash != expected_hash {
            return Err(ValidationError::ContentHash {
                id: record.id.0.clone(),
                expected: expected_hash,
                actual: record.content_hash.clone(),
            });
        }

        let actual_prev_id = record.stream_prev_id.as_ref().map(|i| i.0.clone());
        if actual_prev_id != prev_id {
            return Err(ValidationError::StreamChain {
                id: record.id.0.clone(),
                expected_prev: prev_id,
                actual_prev: actual_prev_id,
            });
        }
        if record.stream_prev_hash != prev_hash {
            return Err(ValidationError::StreamPrevHash {
                id: record.id.0.clone(),
                expected: prev_hash,
                actual: record.stream_prev_hash.clone(),
            });
        }

        graph.insert(
            record.id.0.clone(),
            record.parent_ids.iter().map(|p| p.0.clone()).collect(),
        );
        prev_id = Some(record.id.0.clone());
        prev_hash = record.content_hash.clone();
    }

    // Dangling parents: every parent must resolve to a known record.
    for record in records {
        for parent in &record.parent_ids {
            if !seen.contains(&parent.0) {
                return Err(ValidationError::DanglingParent {
                    id: record.id.0.clone(),
                    parent_id: parent.0.clone(),
                });
            }
        }
    }

    detect_parent_cycles(&graph)?;
    Ok(())
}

/// DFS three-colour cycle detection over the parent graph. Each node is
/// unmarked, in-progress (Temporary), or done (Permanent); encountering an
/// in-progress node while recursing is a back-edge → cycle.
fn detect_parent_cycles(graph: &BTreeMap<String, Vec<String>>) -> Result<(), ValidationError> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mark {
        Temporary,
        Permanent,
    }

    fn visit(
        node: &str,
        graph: &BTreeMap<String, Vec<String>>,
        marks: &mut BTreeMap<String, Mark>,
    ) -> Result<(), ValidationError> {
        match marks.get(node).copied() {
            Some(Mark::Temporary) => {
                return Err(ValidationError::ParentCycle(node.to_string()));
            },
            Some(Mark::Permanent) => return Ok(()),
            None => {},
        }
        marks.insert(node.to_string(), Mark::Temporary);
        if let Some(parents) = graph.get(node) {
            for parent in parents {
                visit(parent, graph, marks)?;
            }
        }
        marks.insert(node.to_string(), Mark::Permanent);
        Ok(())
    }

    let mut marks = BTreeMap::new();
    for node in graph.keys() {
        visit(node, graph, &mut marks)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EventBuilder, EventId};

    fn rec(id: &str, parents: &[&str]) -> EventRecord {
        let mut b = EventBuilder::new(serde_json::json!({})).id(EventId::from(id));
        for p in parents {
            b = b.parent(EventId::from(*p));
        }
        b.build().unwrap()
    }

    fn with_stream_links(records: Vec<EventRecord>) -> Vec<EventRecord> {
        let mut out = Vec::new();
        let mut prev_id = None;
        let mut prev_hash = Hash(String::new());
        for mut r in records {
            r.stream_prev_id = prev_id.clone();
            r.stream_prev_hash = prev_hash.clone();
            prev_id = Some(r.id.clone());
            prev_hash = r.content_hash.clone();
            out.push(r);
        }
        out
    }

    #[test]
    fn valid_linear_log_passes() {
        let records = with_stream_links(vec![rec("a", &[]), rec("b", &["a"]), rec("c", &["b"])]);
        validate_log(&records).unwrap();
    }

    #[test]
    fn branch_and_merge_passes() {
        let records = with_stream_links(vec![
            rec("a", &[]),
            rec("b", &["a"]),
            rec("c", &["a"]),
            rec("m", &["b", "c"]),
        ]);
        validate_log(&records).unwrap();
    }

    #[test]
    fn edited_content_is_detected() {
        let mut records = with_stream_links(vec![rec("a", &[])]);
        records[0].payload = serde_json::json!({"tampered": true});
        let err = validate_log(&records).unwrap_err();
        assert!(matches!(err, ValidationError::ContentHash { .. }));
    }

    #[test]
    fn broken_stream_chain_is_detected() {
        // Second record claims a prev that is not the first record.
        let mut a = rec("a", &[]);
        let mut b = rec("b", &["a"]);
        b.stream_prev_id = Some(EventId::from("nonexistent"));
        b.stream_prev_hash = a.content_hash.clone();
        a.stream_prev_id = None;
        a.stream_prev_hash = Hash(String::new());
        let err = validate_log(&[a, b]).unwrap_err();
        assert!(matches!(err, ValidationError::StreamChain { .. }));
    }

    #[test]
    fn dangling_parent_is_detected() {
        let records = with_stream_links(vec![rec("a", &["ghost"])]);
        let err = validate_log(&records).unwrap_err();
        assert!(matches!(err, ValidationError::DanglingParent { .. }));
    }

    #[test]
    fn parent_cycle_is_detected() {
        // a -> b -> a (each names the other as parent) is a cycle.
        let records = with_stream_links(vec![rec("a", &["b"]), rec("b", &["a"])]);
        let err = validate_log(&records).unwrap_err();
        assert!(matches!(err, ValidationError::ParentCycle(_)));
    }

    #[test]
    fn duplicate_id_is_detected() {
        let mut records = with_stream_links(vec![rec("a", &[])]);
        let dup = rec("a", &[]);
        records.push(dup);
        // The duplicate will trip either DuplicateId or the stream chain; both
        // are validation failures.
        assert!(validate_log(&records).is_err());
    }
}
