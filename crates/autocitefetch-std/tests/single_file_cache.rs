//! Tests of [`SingleFileCacheStore`], autocitefetch's thin wrapper around
//! cachette's `JsonlStore`: where its files go, what a line looks like, the
//! storage policy, ephemeral items, and reading a file written by 0.1.1. The
//! storage engine itself (logs, locks, merges) is tested in cachette.

use std::time::Duration;

use autocitefetch::{CacheStore, StoragePolicy, Timestamp};
use autocitefetch_std::SingleFileCacheStore;
use serde_json::json;

const HOUR: Duration = Duration::from_secs(3600);

fn files_in(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn put_flush_reopen_roundtrip_leaves_one_file_with_the_0_1_1_line_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("cache");
    let now = Timestamp::from_millis(1_000_000);

    let store = SingleFileCacheStore::new(&dir).unwrap();
    store.put("doi:10.1/x", &json!({"title": "T"}), HOUR, now).unwrap();
    store.flush().unwrap();
    drop(store);

    // Only the committable file is left, under the default base.
    assert_eq!(files_in(&dir), ["citations.jsonl"]);
    let text = std::fs::read_to_string(dir.join("citations.jsonl")).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], r#"{"schema":1}"#);
    // The 0.1.1 shape: a concrete payload and two expiry times, nothing else
    // (no time of write, use or cost), and no `id` stamped into the item.
    let line: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(line["id"], "doi:10.1/x");
    assert_eq!(line["rec"]["payload"], json!({"concrete": {"title": "T"}}));
    let mut keys: Vec<&String> = line.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["id", "rec"]);
    let mut rec_keys: Vec<&String> = line["rec"].as_object().unwrap().keys().collect();
    rec_keys.sort();
    assert_eq!(rec_keys, ["expires", "payload", "stale_after"]);

    // Reopened, the item and its expiry times read back.
    let store = SingleFileCacheStore::new(&dir).unwrap();
    let got = store.get("doi:10.1/x").unwrap().expect("persisted");
    assert_eq!(got.item, json!({"title": "T"}));
    assert_eq!(got.expiry.expires.as_millis(), line["rec"]["expires"].as_i64().unwrap());
    assert_eq!(got.expiry.stale_after.as_millis(), line["rec"]["stale_after"].as_i64().unwrap());
}

#[test]
fn with_base_renames_the_whole_family() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::with_base(tmp.path(), ".citations").unwrap();
    store.put("doi:10.1/x", &json!({"title": "T"}), HOUR, Timestamp::from_millis(0)).unwrap();
    // While the run is in flight, the log and its lock hide under `._.citations`.
    assert!(
        files_in(tmp.path()).iter().all(|n| n.starts_with("._.citations.")),
        "{:?}",
        files_in(tmp.path())
    );
    store.flush().unwrap();
    assert_eq!(files_in(tmp.path()), [".citations.jsonl"]);
}

#[test]
fn ephemeral_items_are_readable_in_the_run_and_never_written() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    let now = Timestamp::from_millis(0);
    store
        .put("manual:Some text", &json!({"_ready_formatted": {"flm": "Some text"}}), Duration::ZERO, now)
        .unwrap();
    assert!(store.get("manual:Some text").unwrap().is_some(), "readable in the run");
    store.flush().unwrap();
    // The item is in no file: no log is left, and the main file holds only its
    // header.
    assert_eq!(files_in(tmp.path()), ["citations.jsonl"]);
    let text = std::fs::read_to_string(tmp.path().join("citations.jsonl")).unwrap();
    assert!(!text.contains("manual:"), "{text}");
    assert_eq!(text.lines().filter(|l| !l.trim().is_empty()).count(), 1, "{text}");
    drop(store);
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    assert!(store.get("manual:Some text").unwrap().is_none(), "gone on reopen");
}

#[test]
fn the_storage_policy_is_the_one_given() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    assert!(!store.policy().keep_reference && !store.policy().keep_abstract);
    let store = store.with_policy(StoragePolicy {
        keep_abstract: true,
        ..StoragePolicy::default()
    });
    assert!(store.policy().keep_abstract);
    assert!(!store.policy().keep_reference);
}

#[test]
fn a_file_written_by_0_1_1_is_read_as_it_is() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("citations.jsonl"),
        concat!(
            r#"{"schema":1}"#, "\n",
            r#"{"id":"arxiv:1211.1037","rec":{"payload":{"chained":{"prefix":"doi","key":"10.1103/physreva.86.052329","set_properties":{"arxivid":"1211.1037"}}},"stale_after":1700783584969,"expires":1700979481212}}"#, "\n",
            r#"{"id":"doi:10.1103/physreva.86.052329","rec":{"payload":{"concrete":{"DOI":"10.1103/PhysRevA.86.052329","author":[{"family":"Faist","given":"Philippe"}],"id":"doi:10.1103/physreva.86.052329","title":"A & B <c>"}},"stale_after":1722610890625,"expires":1728263613282}}"#, "\n",
        ),
    )
    .unwrap();

    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    // The concrete entry reads back with its expiry times.
    let doi = store.get("doi:10.1103/physreva.86.052329").unwrap().expect("concrete entry");
    assert_eq!(doi.item["title"], "A & B <c>");
    assert_eq!(doi.expiry.expires.as_millis(), 1728263613282);
    assert_eq!(doi.expiry.stale_after.as_millis(), 1722610890625);
    // A 0.1.1 chain pointer reads as absent, so the citation is fetched again.
    assert!(store.get("arxiv:1211.1037").unwrap().is_none());
}
