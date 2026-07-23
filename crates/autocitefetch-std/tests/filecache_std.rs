//! Integration tests for the real [`SingleFileCacheStore`] on the local
//! filesystem: put/flush/reopen roundtrips, the committed file's shape and
//! permissions, compaction locking, and — the important one —
//! [`concurrent_writer_appends_are_not_eaten_by_a_peer_flush`], which pins the
//! rule that a compaction may unlink nothing but its own sidecar.

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

    // Drop the store before reopening. (It holds no lock: the compaction guard
    // is taken and released inside `flush`.)
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

/// Regression test for the compaction data-loss bug: `flush()` used to delete
/// *every* sidecar it folded, including live peers' logs. A peer whose `put`
/// landed after the folding read but before the unlink had its acknowledged
/// write destroyed. Two stores over one directory, one putting in a loop and
/// one flushing in a loop: not a single acknowledged put may go missing.
#[test]
fn concurrent_writer_appends_are_not_eaten_by_a_peer_flush() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const N: usize = 400;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    // Two independent stores over the same directory. They are created
    // sequentially so their `<pid>-<nanos>` writer ids — and therefore their
    // sidecar logs — differ.
    let writer = block_on(SingleFileCacheStore::new(&dir)).expect("open writer");
    let flusher = block_on(SingleFileCacheStore::new(&dir)).expect("open flusher");

    let done = Arc::new(AtomicBool::new(false));
    let done_w = Arc::clone(&done);

    let writing = std::thread::spawn(move || {
        for i in 0..N {
            block_on(writer.put(&format!("doi:{i:04}"), record(1_000 + i as i64)))
                .expect("put acknowledged");
            std::thread::sleep(std::time::Duration::from_micros(300));
        }
        done_w.store(true, Ordering::SeqCst);
        // Fold this writer's own log into the main file so the assertion below
        // inspects a fully compacted directory.
        block_on(writer.flush()).expect("writer flush");
    });

    let flushing = std::thread::spawn(move || {
        while !done.load(Ordering::SeqCst) {
            block_on(flusher.put("doi:flusher", record(9_000))).expect("peer put");
            block_on(flusher.flush()).expect("peer flush");
        }
    });

    writing.join().expect("writer thread");
    flushing.join().expect("flusher thread");

    // Everything the writer was told was stored must still be there.
    let reader = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    let missing: Vec<String> = (0..N)
        .map(|i| format!("doi:{i:04}"))
        .filter(|id| block_on(reader.get(id)).expect("get").is_none())
        .collect();
    assert!(
        missing.is_empty(),
        "{} of {N} acknowledged puts were destroyed by the peer's flush: {:?}…",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
}

/// The committed file is meant to be read, hand-edited and merged by humans and
/// diffed by git, so its shape is a contract: a schema header on line 0, then
/// exactly one entry per line, sorted by id.
#[test]
fn main_file_shape_is_header_plus_sorted_entries() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    // Put them out of order on purpose.
    for id in ["doi:10.2/z", "arxiv:2101.00001", "doi:10.1/a"] {
        block_on(store.put(id, record(1000))).expect("put");
    }
    block_on(store.flush()).expect("flush");

    let text = std::fs::read_to_string(dir.join("citations.jsonl")).expect("read main file");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 4, "header + one line per entry: {lines:?}");
    assert_eq!(lines[0], r#"{"schema":1}"#, "line 0 is the schema header");
    assert!(text.ends_with('\n'), "file ends with a newline");

    let ids: Vec<String> = lines[1..]
        .iter()
        .map(|l| {
            let v: serde_json::Value = serde_json::from_str(l).expect("each line is one entry");
            assert!(v.get("rec").is_some(), "entry carries its record");
            v["id"].as_str().expect("entry carries its id").to_string()
        })
        .collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "entries are sorted by id");
    assert_eq!(ids, ["arxiv:2101.00001", "doi:10.1/a", "doi:10.2/z"]);
}

/// The original roundtrip test only asserted `is_some()`, which a corrupted or
/// swapped payload would pass. Assert the payload and *both* timestamps.
#[test]
fn reopen_returns_the_same_payload_and_expiry() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    let concrete = serde_json::json!({
        "id": "doi:10.1/a",
        "title": "Ünïcødé, \"quotes\" and \\backslashes\\",
        "author": [{"family": "Doe", "given": "J."}],
        "issued": {"date-parts": [[2021, 3, 14]]},
    });
    let stored = CacheRecord {
        payload: Payload::Concrete(concrete.clone()),
        stale_after: Timestamp::from_millis(1_700_000_000_123),
        expires: Timestamp::from_millis(1_800_000_000_456),
    };
    let chained = CacheRecord {
        payload: Payload::Chained {
            prefix: "doi".into(),
            key: "10.1/a".into(),
            set_properties: serde_json::json!({"note": "via arXiv"}),
        },
        stale_after: Timestamp::from_millis(1),
        expires: Timestamp::from_millis(2),
    };

    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    block_on(store.put("doi:10.1/a", stored.clone())).expect("put concrete");
    block_on(store.put("arxiv:2101.00001", chained.clone())).expect("put chained");
    block_on(store.flush()).expect("flush");
    drop(store);

    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");

    let got = block_on(reopened.get("doi:10.1/a")).expect("get").expect("present");
    assert_eq!(got.stale_after, stored.stale_after);
    assert_eq!(got.expires, stored.expires);
    match got.payload {
        Payload::Concrete(v) => assert_eq!(v, concrete),
        other => panic!("expected a concrete payload, got {other:?}"),
    }

    let got = block_on(reopened.get("arxiv:2101.00001")).expect("get").expect("present");
    assert_eq!(got.stale_after, chained.stale_after);
    assert_eq!(got.expires, chained.expires);
    match got.payload {
        Payload::Chained { prefix, key, set_properties } => {
            assert_eq!(prefix, "doi");
            assert_eq!(key, "10.1/a");
            assert_eq!(set_properties, serde_json::json!({"note": "via arXiv"}));
        }
        other => panic!("expected a chained payload, got {other:?}"),
    }
}

/// A compaction renames a temp file over `citations.jsonl`; without care that
/// hands the destination the temp file's private 0600 mode, and in the shared
/// cache directory this design supports the next user's `open` then fails with
/// EACCES.
#[cfg(unix)]
#[test]
fn flush_preserves_main_file_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    let main = dir.join("citations.jsonl");

    // A brand-new committed file gets the umask-respecting default, i.e. the
    // same mode a plain `File::create` in that directory would produce.
    let probe = dir.join("umask-probe");
    std::fs::File::create(&probe).expect("probe");
    let want_default = std::fs::metadata(&probe).expect("stat probe").permissions().mode() & 0o777;
    std::fs::remove_file(&probe).expect("rm probe");

    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("first flush");
    let created = std::fs::metadata(&main).expect("stat main").permissions().mode() & 0o777;
    assert_eq!(
        created, want_default,
        "a new citations.jsonl should respect the umask, not a hard-coded 0600"
    );

    // An existing file's mode is carried across the rename.
    std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    block_on(store.put("doi:10.1/b", record(2000))).expect("put");
    block_on(store.flush()).expect("second flush");
    let after = std::fs::metadata(&main).expect("stat main").permissions().mode() & 0o777;
    assert_eq!(after, 0o644, "flush must not tighten the committed file's mode");

    // ...including a group-shared mode, the case that actually breaks users.
    std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o664)).expect("chmod");
    block_on(store.put("doi:10.1/c", record(3000))).expect("put");
    block_on(store.flush()).expect("third flush");
    let after = std::fs::metadata(&main).expect("stat main").permissions().mode() & 0o777;
    assert_eq!(after, 0o664);
}

/// The store scans the cache directory for `citations.*.log`, but it may only
/// ever *delete* its own. A user's unrelated file that happens to match must
/// come out of a compaction byte-for-byte intact.
#[test]
fn flush_does_not_delete_foreign_citations_star_log_files() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    let foreign = [
        ("citations.import-notes.log", "hand-written import notes\n"),
        ("citations.log", "a log named without a writer segment\n"),
        ("unrelated.log", "nothing to do with the cache\n"),
    ];
    for (name, body) in foreign {
        std::fs::write(dir.join(name), body).expect("seed foreign file");
    }

    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush");

    for (name, body) in foreign {
        let path = dir.join(name);
        assert!(path.is_file(), "{name} was deleted by a compaction");
        assert_eq!(
            std::fs::read_to_string(&path).expect("read back"),
            body,
            "{name} was modified by a compaction"
        );
    }
    // And the cache itself still works.
    assert!(block_on(store.get("doi:10.1/a")).expect("get").is_some());
}

/// A *directory* whose name matches the sidecar pattern used to make the whole
/// cache permanently unopenable (`read` on it fails with EISDIR and the load
/// propagated that).
#[test]
fn open_on_a_directory_containing_a_subdir_named_citations_x_log() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    std::fs::create_dir_all(dir.join("citations.oops.log")).expect("make the decoy directory");

    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open must tolerate it");
    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush must tolerate it");

    assert!(block_on(store.get("doi:10.1/a")).expect("get").is_some());
    assert!(
        dir.join("citations.oops.log").is_dir(),
        "the decoy directory is left alone"
    );
    drop(store);
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen must tolerate it");
    assert!(block_on(reopened.get("doi:10.1/a")).expect("get").is_some());
}

#[test]
fn flush_creates_and_keeps_citations_lock() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    let lock = dir.join("citations.lock");
    assert!(!lock.exists(), "the lockfile is created by the first flush");

    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush");
    assert!(lock.is_file(), "flush creates the lockfile");

    // A second flush reuses it and must never truncate or remove it.
    std::fs::write(&lock, b"sentinel").expect("write sentinel");
    block_on(store.flush()).expect("second flush");
    assert!(lock.is_file(), "flush keeps the lockfile");
    assert_eq!(
        std::fs::read(&lock).expect("read lock"),
        b"sentinel",
        "the lockfile is locked, never rewritten"
    );
}

/// Compaction is mutually exclusive: a flush must wait for a lock held by
/// another writer instead of rewriting the file underneath it.
#[test]
fn a_second_flush_blocks_while_the_first_holds_the_lock() {
    use std::time::{Duration, Instant};

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    // Make sure the lockfile exists before we grab it.
    block_on(store.flush()).expect("priming flush");
    block_on(store.put("doi:10.1/b", record(2000))).expect("put");

    // Hold the compaction lock exactly the way a peer's `flush` would.
    let held = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(dir.join("citations.lock"))
        .expect("open lockfile");
    fs4::FileExt::lock_exclusive(&held).expect("take the lock");

    let flushing = std::thread::spawn(move || {
        block_on(store.flush()).expect("flush");
    });

    std::thread::sleep(Duration::from_millis(250));
    assert!(
        !flushing.is_finished(),
        "flush must block while another writer holds the compaction lock"
    );

    fs4::FileExt::unlock(&held).expect("release the lock");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !flushing.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        flushing.is_finished(),
        "flush must proceed once the lock is released"
    );
    flushing.join().expect("flusher thread");

    let text = std::fs::read_to_string(dir.join("citations.jsonl")).expect("read main file");
    assert_eq!(text.lines().count(), 3, "both entries committed: {text}");
}
