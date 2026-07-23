//! Integration test for the real [`SingleFileCacheStore`] on the local
//! filesystem: a put/flush/reopen roundtrip, and verification that compaction
//! leaves exactly one committed `citations.jsonl` behind with the sidecars
//! removed.

use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::{CacheRecord, CacheStore, Payload, Timestamp};
use autocitefetch_std::SingleFileCacheStore;

/// Blocking driver: every store op resolves on the first poll (real blocking
/// I/O), so a no-op waker suffices.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete (a backend unexpectedly pended)");
}

fn record(expires_ms: i64) -> CacheRecord {
    CacheRecord {
        payload: Payload::Concrete(serde_json::json!({"id": "x", "title": "T"})),
        stale_after: Timestamp::from_millis(expires_ms / 2),
        expires: Timestamp::from_millis(expires_ms),
    }
}

#[test]
fn put_flush_reopen_roundtrip_leaves_single_committed_file() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    // Open, write two entries, flush (compact).
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open store");
    block_on(store.put("doi:10.1/a", record(1000))).expect("put a");
    block_on(store.put("arxiv:2101.00001", record(2000))).expect("put b");
    block_on(store.flush()).expect("flush");

    // Exactly one committed JSONL file, and no sidecar `*.log` files remain.
    let main = dir.join("citations.jsonl");
    assert!(main.is_file(), "citations.jsonl should exist after flush");
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read cache dir") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy().into_owned();
        if name.ends_with(".log") {
            logs.push(name);
        }
    }
    assert!(
        logs.is_empty(),
        "sidecars should be gone after flush, found: {logs:?}"
    );

    // Drop the store (and its lock file handle) before reopening.
    drop(store);

    // Reopen over the same directory: both entries survive.
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen store");
    assert!(
        block_on(reopened.get("doi:10.1/a"))
            .expect("get a")
            .is_some(),
        "doi entry should survive reopen"
    );
    assert!(
        block_on(reopened.get("arxiv:2101.00001"))
            .expect("get b")
            .is_some(),
        "arxiv entry should survive reopen"
    );
    let entries = block_on(reopened.entries()).expect("entries");
    assert_eq!(entries.len(), 2, "both entries present after reopen");
}
