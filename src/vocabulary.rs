//! The shared record vocabulary: identifiers, hashes, and the [`EventRecord`]
//! and the [`EventRecord`] itself. Pure data, no logic (logic is
//! [`crate::validation`] and [`crate::log`]).

use serde::{Deserialize, Serialize};

use crate::utils::stable_digest;

/// An error from [`EventBuilder::build`].
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    /// Built without the `uuid` feature and no explicit id was supplied.
    #[error("an event id is required (build with the `uuid` feature, or supply EventBuilder::id)")]
    MissingId,
}

/// Schema version stamped on every record. Bumped when the wire shape changes;
/// [`crate::validation`] rejects a stream whose version it does not recognize.
pub const RECORD_SCHEMA_VERSION: &str = "daglog.record.v1";

/// A record identifier. Opaque to the DAG (any unique string); the library
/// supplies random v4 UUIDs via [`EventId::new`] when the `uuid` feature is on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub String);

impl EventId {
    /// Generate a fresh random v4 UUID id. Requires the `uuid` feature
    /// (default).
    #[cfg(feature = "uuid")]
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Borrow the id string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "uuid")]
impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<String> for EventId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for EventId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<&Self> for EventId {
    fn from(value: &Self) -> Self {
        value.clone()
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A content hash. Prefixed canonical form (e.g. `fnv1a64:...`), computed by
/// [`crate::utils::stable_digest`] over the record's canonical material.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Hash(pub String);

impl Hash {
    /// Borrow the hash string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One append-only record in the log. This is the on-disk JSONL line shape.
///
/// Three independent links make up the structure:
/// - `content_hash` — hash over this record's own canonical material.
/// - `stream_prev_id` / `stream_prev_hash` — the **previous line** in the file.
///   This is the linear hash-chain: every line commits to the line before it,
///   so a single edit anywhere in the middle breaks the chain and is detected
///   on replay.
/// - `parent_ids` — the **semantic parents** forming the branch/merge DAG. An
///   empty `parent_ids` is a root (a new branch start). One parent continues a
///   branch. Two or more parents is a merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRecord {
    /// The wire-schema version stamped on this record.
    pub schema_version: String,
    /// This record's unique id.
    pub id: EventId,
    /// The semantic-parent DAG edges. Empty = root; one = linear; many = merge.
    #[serde(default)]
    pub parent_ids: Vec<EventId>,
    /// Free-form application payload (the actual event content). Its bytes are
    /// part of the content hash, so two records with different payloads have
    /// different hashes.
    pub payload: serde_json::Value,
    /// ISO-8601-ish timestamp string. Not interpreted by the DAG; part of the
    /// hash so it is tamper-evident.
    #[serde(default)]
    pub timestamp: String,
    /// Free-form actor attribution. Part of the hash.
    #[serde(default)]
    pub actor: String,
    /// Free-form source attribution. Part of the hash.
    #[serde(default)]
    pub source: String,
    /// The content hash over this record's canonical material.
    pub content_hash: Hash,
    /// The previous record *in the file stream* (the linear chain predecessor).
    /// `None` for the first record. Set by [`crate::EventLog::append`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_prev_id: Option<EventId>,
    /// The content hash of the stream predecessor. Empty string for the first
    /// record. Set by [`crate::EventLog::append`].
    #[serde(default)]
    pub stream_prev_hash: Hash,
}

/// The canonical material hashed for `content_hash`. Field order is fixed so
/// the hash is stable across machines; it MUST stay a prefix of the
/// `EventRecord` fields (never include `content_hash` or the stream links).
#[derive(Debug, Serialize)]
struct HashMaterial<'a> {
    schema_version: &'a str,
    id: &'a EventId,
    parent_ids: &'a [EventId],
    payload: &'a serde_json::Value,
    timestamp: &'a str,
    actor: &'a str,
    source: &'a str,
}

impl EventRecord {
    /// Recompute this record's content hash from its fields. Two records are
    /// content-equal iff their content hashes match. The stream links and the
    /// stored `content_hash` are deliberately excluded.
    #[must_use]
    pub fn compute_hash(&self) -> Hash {
        let material = HashMaterial {
            schema_version: &self.schema_version,
            id: &self.id,
            parent_ids: &self.parent_ids,
            payload: &self.payload,
            timestamp: &self.timestamp,
            actor: &self.actor,
            source: &self.source,
        };
        // Serialization cannot fail for these types; fall back to empty hash.
        let bytes = serde_json::to_vec(&material).unwrap_or_default();
        Hash(stable_digest(&bytes))
    }

    /// True iff the stored `content_hash` matches a fresh computation.
    #[must_use]
    pub fn hash_is_valid(&self) -> bool {
        self.content_hash == self.compute_hash()
    }
}

/// Builds an [`EventRecord`] minus the stream links (which the log assigns on
/// append). The caller supplies the payload; the builder fills id, timestamp,
/// and parents.
///
/// ```
/// # use daglog::{EventBuilder, EventId};
/// let rec = EventBuilder::new(serde_json::json!({"kind":"open"}))
///     .id(EventId::from("evt-1"))
///     .parent(EventId::from("evt-0"))
///     .actor("ci")
///     .build()
///     .unwrap();
/// assert!(rec.hash_is_valid());
/// ```
#[derive(Debug, Clone)]
pub struct EventBuilder {
    id: Option<EventId>,
    parent_ids: Vec<EventId>,
    payload: serde_json::Value,
    timestamp: String,
    actor: String,
    source: String,
}

impl EventBuilder {
    /// Start a record carrying `payload`. A freshly-built record with no parents
    /// set is a root (branch start).
    #[must_use]
    pub const fn new(payload: serde_json::Value) -> Self {
        Self {
            id: None,
            parent_ids: Vec::new(),
            payload,
            timestamp: String::new(),
            actor: String::new(),
            source: String::new(),
        }
    }

    /// Supply an explicit id. If unset, [`EventBuilder::build`] generates a
    /// random one (requires the `uuid` feature).
    #[must_use]
    pub fn id(mut self, id: EventId) -> Self {
        self.id = Some(id);
        self
    }

    /// Add one semantic parent (continues a branch). Accepts anything that
    /// converts into an [`EventId`] (an owned id, a `&EventId`, or a `&str`).
    #[must_use]
    pub fn parent(mut self, parent: impl Into<EventId>) -> Self {
        self.parent_ids.push(parent.into());
        self
    }

    /// Add several semantic parents (a merge). Accepts an iterable of ids or
    /// `&EventId`/`&str` references.
    #[must_use]
    pub fn parents(mut self, parents: impl IntoIterator<Item = impl Into<EventId>>) -> Self {
        self.parent_ids.extend(parents.into_iter().map(Into::into));
        self
    }

    /// Set the timestamp string (part of the content hash).
    #[must_use]
    pub fn timestamp(mut self, ts: impl Into<String>) -> Self {
        self.timestamp = ts.into();
        self
    }

    /// Set the actor attribution (part of the content hash).
    #[must_use]
    pub fn actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = actor.into();
        self
    }

    /// Set the source attribution (part of the content hash).
    #[must_use]
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = source.into();
        self
    }

    /// Finalize: assign an id if none, compute the content hash, and return the
    /// record (still missing stream links, which [`crate::EventLog::append`]
    /// sets).
    ///
    /// # Errors
    /// Never errors in practice (id generation is infallible); the `Result` is
    /// for forward-compatibility if hash material serialization ever can fail.
    pub fn build(self) -> Result<EventRecord, BuildError> {
        #[cfg(feature = "uuid")]
        let id = self.id.unwrap_or_default();
        #[cfg(not(feature = "uuid"))]
        let id = self.id.ok_or(crate::BuildError::MissingId)?;
        let record = EventRecord {
            schema_version: RECORD_SCHEMA_VERSION.to_string(),
            id,
            parent_ids: self.parent_ids,
            payload: self.payload,
            timestamp: self.timestamp,
            actor: self.actor,
            source: self.source,
            content_hash: Hash(String::new()),
            stream_prev_id: None,
            stream_prev_hash: Hash(String::new()),
        };
        let mut record = record;
        record.content_hash = record.compute_hash();
        Ok(record)
    }
}
