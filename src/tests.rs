//! Integration tests covering the full append → replay → validate cycle,
//! including the branch/merge git-style workflow.

use crate::{validate_log, EventBuilder, EventId, EventLog};

#[test]
fn doc_example_compiles_and_runs() {
    let mut log = EventLog::in_memory();
    let a = log
        .append(
            EventBuilder::new(serde_json::json!("base"))
                .build()
                .unwrap(),
        )
        .unwrap();
    let b = log
        .append(
            EventBuilder::new(serde_json::json!("branch-b"))
                .parent(a.clone())
                .build()
                .unwrap(),
        )
        .unwrap();
    let c = log
        .append(
            EventBuilder::new(serde_json::json!("branch-c"))
                .parent(a.clone())
                .build()
                .unwrap(),
        )
        .unwrap();
    let _m = log
        .append(
            EventBuilder::new(serde_json::json!("merge"))
                .parents([&b, &c])
                .build()
                .unwrap(),
        )
        .unwrap();
    validate_log(&log.replay().unwrap().records).unwrap();
}

#[test]
fn multi_generation_diamond_on_disk_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut log = EventLog::open(dir.path());
        let r = log
            .append(
                EventBuilder::new(serde_json::json!({"gen": 0}))
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let a = log
            .append(
                EventBuilder::new(serde_json::json!({"gen": 1, "side": "a"}))
                    .parent(r.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let b = log
            .append(
                EventBuilder::new(serde_json::json!({"gen": 1, "side": "b"}))
                    .parent(r.clone())
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let _tip = log
            .append(
                EventBuilder::new(serde_json::json!({"gen": 2, "merge": true}))
                    .parents(&[a.clone(), b.clone()])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
    // Reopen cold and validate: the whole diamond re-reads from one file.
    let reopened = EventLog::open(dir.path());
    let replay = reopened.replay().unwrap();
    assert_eq!(replay.len(), 4);
    assert_eq!(
        replay.records[3].parent_ids.len(),
        2,
        "tip is a 2-parent merge"
    );
    validate_log(&replay.records).unwrap();
}

#[test]
fn content_hash_distinguishes_payloads() {
    let r1 = EventBuilder::new(serde_json::json!({"v": 1}))
        .id(EventId::from("x"))
        .build()
        .unwrap();
    let r2 = EventBuilder::new(serde_json::json!({"v": 2}))
        .id(EventId::from("x"))
        .build()
        .unwrap();
    assert_ne!(r1.content_hash, r2.content_hash);
}

#[test]
fn default_features_generate_unique_ids() {
    // The `uuid` feature is on by default; two builder-built records get
    // distinct ids.
    let r1 = EventBuilder::new(serde_json::json!({})).build().unwrap();
    let r2 = EventBuilder::new(serde_json::json!({})).build().unwrap();
    assert_ne!(r1.id, r2.id);
}
