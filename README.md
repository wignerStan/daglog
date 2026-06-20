# jsonldag

An append-only **JSONL** event log with a verified hash-chain **and** a
branch/merge DAG — git-style forks and merges over a single
newline-delimited JSON stream.

Each record carries:

- a **content hash** over its own canonical fields,
- a **stream predecessor** (the previous line in the file — the linear
  hash-chain that makes tampering detectable), and
- a set of **semantic parents** (`parent_ids`) that form a separate DAG, so
  one event can merge several branches the way a git merge commit does.

One `.jsonl` file is the whole database; replay reconstructs the DAG. No
database, no daemon, no lock server.

## Example

```rust
use jsonldag::{EventBuilder, EventLog, validate_log};
use serde_json::json;

let mut log = EventLog::in_memory();
let a = log.append(EventBuilder::new(json!("base")).build().unwrap()).unwrap();
let b = log.append(EventBuilder::new(json!("branch-b")).parent(&a).build().unwrap()).unwrap();
let c = log.append(EventBuilder::new(json!("branch-c")).parent(&a).build().unwrap()).unwrap();
// a merge: parents point back at both branch tips.
let _m = log.append(EventBuilder::new(json!("merge")).parents([&b, &c]).build().unwrap()).unwrap();

// Replay + validate the full hash-chain, parent-DAG, and acyclicity invariants.
validate_log(&log.replay().unwrap().records).unwrap();
```

Persist to disk instead by opening over a directory:

```rust
use jsonldag::EventLog;
let mut log = EventLog::open(".myapp/events"); // <dir>/events.jsonl
```

Reopen later from the same path and the whole branch/merge history replays from
the single file.

## Invariants (verified on every replay)

`validate_log` enforces:

1. **Content hash** — every record's stored hash matches a fresh computation
   (no field edited after append).
2. **Stream chain** — every record's stream-predecessor id + hash match the
   previous line (linear tamper detection). Ids are unique and the schema
   version is current.
3. **Parent DAG** — every `parent_id` resolves to a known record (no dangling
   edges) and the parent graph is acyclic (no merge cycle).

## What it is / is not

It **is**: a tamper-evident, append-only event store with fork/merge over a
single portable `.jsonl` file.

It **is not**: a database, an indexed query engine, or a network protocol.
For heavy read access, project the stream into whatever index you need
(SQLite, a search index, …).

## Features

- `uuid` (default) — enables `EventId::new()` random v4 id generation. Disable
  it (`--no-default-features`) for a no-randomness build where the caller
  supplies every id.
- `pg` — enables the PostgreSQL JSONB backend store (`PgStore`). Off by default
  — the crate is zero-network unless a caller opts in. Requires the `tokio`
  runtime (each sync call borrows a short-lived runtime to drive the async
  `sqlx` query).

## PostgreSQL backend (`pg` feature)

When the log lives in Postgres, swap the store for `PgStore`. Each record is one
row `(seq BIGSERIAL, record JSONB)`; `seq` captures stream order so replay
reconstructs the same hash-chain the file backend does.

```rust
# #[cfg(feature = "pg")] {
# use jsonldag::{EventBuilder, EventLog, PgStore};
# use serde_json::json;
// 1. provision the table once (idempotent DDL):
let store = PgStore::new("postgres://user:pass@host/db", "events").unwrap();
store.ensure_table().unwrap();
// 2. open a log over it:
let mut log = EventLog::new(store);
log.append(EventBuilder::new(json!("base")).build().unwrap()).unwrap();
log.validate().unwrap(); // hash-chain + parent-DAG + acyclicity
# }
```

For high-throughput async use, drive the `sqlx` pool from your own runtime via a
custom `EventStore` adapter instead of the per-call borrowed runtime.

## FCIS module layout

The crate keeps an explicit ownership layering (FCIS) at the module level so the
seams are testable, even though it ships as one library crate:

| Module | FCIS role | Responsibility |
|---|---|---|
| [`utils`] | utils | deterministic atoms: FNV-1a digest, JSONL line framing |
| [`vocabulary`] | meaning_seed | shared record types: `EventId`, `Hash`, `EventRecord`, `EventBuilder` |
| [`validation`] | meaning_core | pure invariants: hash-chain, parent-DAG, cycle detection (effect-free) |
| [`port`] | local capability port | the `EventStore` trait + `StoreError`; imports no adapter |
| [`io`] | run_kit + effect_tool | JSONL atoms (run_kit) + `FileStore` filesystem adapter (effect_tool) + `InMemoryStore` buffer backend (run_kit) |
| [`pg`] *(feature)* | effect_tool | Postgres JSONB database adapter (`PgStore`); zero-network unless `pg` is on |
| [`log`] | use_flow | the high-level `EventLog<S: EventStore>`: open/append/replay/validate |

The store is a **local same-axis capability port** (`trait EventStore`), not a
concrete struct: `EventLog` depends on the port, and any adapter (file,
in-memory, a test
fake, a remote store) plugs in via `EventLog::new(your_store)`.

[`utils`]: https://docs.rs/jsonldag/latest/jsonldag/utils
[`vocabulary`]: https://docs.rs/jsonldag/latest/jsonldag/vocabulary
[`validation`]: https://docs.rs/jsonldag/latest/jsonldag/validation
[`port`]: https://docs.rs/jsonldag/latest/jsonldag/port
[`io`]: https://docs.rs/jsonldag/latest/jsonldag/io
[`pg`]: https://docs.rs/jsonldag/latest/jsonldag/pg
[`log`]: https://docs.rs/jsonldag/latest/jsonldag/log

## License

Dual-licensed under MIT or Apache-2.0, at your option.
