//! FCIS `run_kit` (pg backend) — a `PostgreSQL` JSONB store adapter.
//!
//! `PgStore` is the concrete adapter for the [`EventStore`](crate::EventStore)
//! port when the log lives in Postgres. Each [`EventRecord`](crate::EventRecord)
//! is one row:
//!
//! ```sql
//! CREATE TABLE events (
//!   seq    BIGSERIAL PRIMARY KEY,   -- stream order (the linear chain)
//!   record JSONB        NOT NULL     -- the full EventRecord
//! );
//! ```
//!
//! `seq` captures stream (append) order so `read_records` can replay the chain
//! in the same order the hash-chain expects. `record` is the whole record as
//! JSONB — the content hash + stream links are inside it, so validation is
//! identical to the file backend. Index `((record->>'id'))` for parent-DAG
//! lookups if you need them.
//!
//! This module is gated behind the `pg` feature; the crate is zero-network
//! unless a caller opts in. The [`EventStore`](crate::EventStore) impl for
//! [`PgStore`] lives in [`crate::port`] with the other adapter impls.
//!
//! # Async runtime
//!
//! The [`EventStore`](crate::EventStore) trait is synchronous, but `sqlx` is
//! async. Each call borrows a short-lived [`tokio`] runtime to block on the
//! query. For high-throughput use, prefer driving the pool from your own async
//! runtime and wrapping it via a custom adapter — the sync port is the
//! low-friction default.

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::vocabulary::EventRecord;

/// A `PostgreSQL` JSONB store adapter.
///
/// Owns the connection options + table name; the [`EventStore`](crate::EventStore)
/// impl (in [`crate::port`]) reads / appends rows.
///
/// Build with [`PgStore::new`] (parsed URL) or [`PgStore::with_options`]. The
/// table must exist and have the shape documented at the top of this module —
/// see [`PgStore::create_table_sql`] for the DDL.
#[derive(Debug, Clone)]
pub struct PgStore {
    connect_options: PgConnectOptions,
    table: String,
    max_connections: u32,
}

impl PgStore {
    /// Build a store over `url` (e.g. `postgres://user:pass@host/db`) writing to
    /// `table`.
    ///
    /// # Errors
    /// [`PgStoreError::Connect`] if the URL cannot be parsed.
    pub fn new(url: &str, table: &str) -> Result<Self, PgStoreError> {
        let connect_options: PgConnectOptions = url
            .parse()
            .map_err(|e| PgStoreError::Connect(format!("invalid postgres URL: {e}")))?;
        Ok(Self {
            connect_options,
            table: sanitize_table(table),
            max_connections: 5,
        })
    }

    /// Build a store from pre-built [`PgConnectOptions`] (use this for
    /// non-URL option sources — TLS, statement timeouts, etc.).
    #[must_use]
    pub fn with_options(connect_options: PgConnectOptions, table: &str) -> Self {
        Self {
            connect_options,
            table: sanitize_table(table),
            max_connections: 5,
        }
    }

    /// Cap the connection pool size (default 5).
    #[must_use]
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n.max(1);
        self
    }

    /// The DDL this adapter expects. Run it once to provision the table:
    ///
    /// ```sql
    /// CREATE TABLE IF NOT EXISTS <table> (
    ///   seq    BIGSERIAL PRIMARY KEY,
    ///   record JSONB NOT NULL
    /// );
    /// CREATE INDEX IF NOT EXISTS <table>_record_id_idx
    ///   ON <table> ((record->>'id'));
    /// ```
    ///
    /// Returns the DDL with the table name substituted. Safe to execute via the
    /// same connection this store uses (or any psql session).
    #[must_use]
    pub fn create_table_sql(&self) -> String {
        let t = &self.table;
        format!(
            "CREATE TABLE IF NOT EXISTS {t} (\n  \
             seq    BIGSERIAL PRIMARY KEY,\n  \
             record JSONB NOT NULL\n\
             );\n\
             CREATE INDEX IF NOT EXISTS {t}_record_id_idx\n  \
             ON {t} ((record->>'id'));"
        )
    }

    /// Borrow the table name.
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Read every record in `seq` (stream) order. Blocks on a short-lived
    /// runtime to run the async query.
    ///
    /// # Errors
    /// [`PgStoreError::Query`] on any DB/parse failure.
    pub fn read_records(&self) -> Result<Vec<EventRecord>, PgStoreError> {
        block_on(async {
            let pool = self.pool().await?;
            // ORDER BY seq reproduces append (stream) order — the hash-chain
            // predecessor links were assigned in that order, so validation
            // matches the file backend exactly.
            let query = format!("SELECT record FROM {} ORDER BY seq", self.table);
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&query).fetch_all(&pool).await?;
            rows.into_iter()
                .map(|(value,)| serde_json_value_to_record(value))
                .collect()
        })
    }

    /// Append one record as a new row (its `seq` is assigned by the DB).
    ///
    /// # Errors
    /// [`PgStoreError::Serialize`] if the record cannot be serialized, or
    /// [`PgStoreError::Query`] on DB failure.
    pub fn append_record(&mut self, record: &EventRecord) -> Result<(), PgStoreError> {
        let value =
            serde_json::to_value(record).map_err(|e| PgStoreError::Serialize(e.to_string()))?;
        block_on(async {
            let pool = self.pool().await?;
            let query = format!("INSERT INTO {} (record) VALUES ($1)", self.table);
            sqlx::query(&query).bind(value).execute(&pool).await?;
            Ok(())
        })
    }

    /// Run the [`create_table_sql`](Self::create_table_sql) DDL against the DB.
    /// Convenience for provisioning; idempotent (`IF NOT EXISTS`).
    ///
    /// # Errors
    /// [`PgStoreError::Query`] on DB failure.
    pub fn ensure_table(&self) -> Result<(), PgStoreError> {
        let ddl = self.create_table_sql();
        block_on(async {
            let pool = self.pool().await?;
            sqlx::query(&ddl).execute(&pool).await?;
            Ok(())
        })
    }

    async fn pool(&self) -> Result<sqlx::Pool<sqlx::Postgres>, PgStoreError> {
        PgPoolOptions::new()
            .max_connections(self.max_connections)
            .connect_with(self.connect_options.clone())
            .await
            .map_err(|e| PgStoreError::Connect(e.to_string()))
    }
}

/// An error from the Postgres backend.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PgStoreError {
    /// Could not parse the connection URL / connect to the DB.
    #[error("postgres connect error: {0}")]
    Connect(String),
    /// A record could not be (de)serialized to/from JSON.
    #[error("record serialize error: {0}")]
    Serialize(String),
    /// A query failed, or a stored JSONB value did not deserialize to an
    /// [`EventRecord`].
    #[error("postgres query error: {0}")]
    Query(String),
}

impl From<sqlx::Error> for PgStoreError {
    fn from(e: sqlx::Error) -> Self {
        Self::Query(e.to_string())
    }
}

/// The table name MUST be a bare identifier (no schema/dots) because it is
/// substituted into raw SQL. Restrict to `[A-Za-z0-9_]`; reject anything else
/// so the surface is not a SQL-injection vector.
fn sanitize_table(table: &str) -> String {
    let cleaned: String = table
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    if cleaned.is_empty() {
        "events".to_string()
    } else {
        cleaned
    }
}

fn serde_json_value_to_record(value: serde_json::Value) -> Result<EventRecord, PgStoreError> {
    serde_json::from_value::<EventRecord>(value)
        .map_err(|e| PgStoreError::Query(format!("record JSONB did not deserialize: {e}")))
}

/// Block on `fut` using a short-lived multi-thread runtime. The
/// [`EventStore`](crate::EventStore) port is sync; `sqlx` is async, so each
/// call borrows a runtime. A caller that already owns a runtime should drive
/// the pool directly rather than pay this cost per call.
///
/// `fut` must itself yield a `Result<T, PgStoreError>` so a runtime-creation
/// failure can surface as a typed error rather than a panic.
fn block_on<F, T>(fut: F) -> Result<T, PgStoreError>
where
    F: std::future::Future<Output = Result<T, PgStoreError>>,
{
    // A fresh runtime per call is simple and correct for the low-friction sync
    // port. It is NOT reentrant: do not call this from inside an existing
    // runtime (use an async adapter instead).
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| PgStoreError::Connect(format!("tokio runtime: {e}")))?;
    runtime.block_on(fut)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_table_strips_non_ident_chars() {
        assert_eq!(sanitize_table("events"), "events");
        assert_eq!(sanitize_table("ev;ents--"), "events");
        assert_eq!(sanitize_table("my_log_1"), "my_log_1");
        // Empty / all-unsafe -> safe default.
        assert_eq!(sanitize_table(";"), "events");
        assert_eq!(sanitize_table(""), "events");
    }

    #[test]
    fn create_table_sql_substitutes_table_name() {
        let store = PgStore::new("postgres://u:p@h/d", "incidents").unwrap();
        let ddl = store.create_table_sql();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS incidents"));
        assert!(ddl.contains("record JSONB NOT NULL"));
        assert!(ddl.contains("seq    BIGSERIAL PRIMARY KEY"));
        assert!(ddl.contains("incidents_record_id_idx"));
        assert_eq!(store.table(), "incidents");
    }

    #[test]
    fn invalid_url_is_rejected() {
        assert!(PgStore::new("not a url %%", "events").is_err());
    }

    #[test]
    fn with_options_keeps_table_sanitize() {
        let opts: PgConnectOptions = "postgres://u:p@h/d".parse().unwrap();
        let store = PgStore::with_options(opts, "weird name!");
        assert_eq!(store.table(), "weirdname");
    }
}
