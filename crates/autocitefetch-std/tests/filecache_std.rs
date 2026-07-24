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

/// An *ephemeral* (TTL-0) record — the `manual` source's case: no fresh window,
/// so `stale_after == expires`. Must never be persisted.
fn ephemeral(now_ms: i64) -> CacheRecord {
    CacheRecord {
        payload: Payload::Concrete(
            serde_json::json!({"_ready_formatted": {"flm": "Bohr, N. (1913)"}}),
        ),
        stale_after: Timestamp::from_millis(now_ms),
        expires: Timestamp::from_millis(now_ms),
    }
}

/// Names of the sidecar append logs in `dir` — files ending in `.log` but not
/// `.log.lock` (the companion liveness lock). Sorted for stable assertions.
fn list_sidecars(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".log") {
                out.push(name);
            }
        }
    }
    out.sort();
    out
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

    // Exactly one file, the committed JSONL one: every throwaway file the store
    // made — the sidecar, its companion liveness lock, the compaction lockfile,
    // the staging file — is unlinked by whoever created it. A CLI run drops its
    // cache in the user's working directory, so leftovers are litter there.
    let main = dir.join("citations.jsonl");
    assert!(main.is_file(), "citations.jsonl should exist after flush");
    let mut left: Vec<String> = std::fs::read_dir(&dir)
        .expect("read cache dir")
        .map(|e| e.expect("dir entry").file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(
        left,
        ["citations.jsonl"],
        "a finished run must leave nothing but the committable file"
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

/// The naming contract users gitignore against: `citations.jsonl` is the only
/// file that wears the bare base name, and everything else the store puts in the
/// directory — sidecars, companion liveness locks, the compaction lockfile, the
/// atomic-replace staging file — hides under `._citations`. So `._citations*`
/// ignores the whole throwaway family and nothing else.
#[test]
fn only_the_main_file_is_outside_the_underscore_dot_prefix() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    // A *live* peer, so the directory also holds a sidecar and companion lock
    // that this store's flush is not allowed to reap.
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    let peer = block_on(SingleFileCacheStore::new(&dir)).expect("open peer");
    block_on(peer.put("doi:10.1/peer", record(1000))).expect("peer put");
    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush");

    let mut stray = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("read cache dir") {
        let name = entry.expect("entry").file_name().to_string_lossy().into_owned();
        if name != "citations.jsonl" && !name.starts_with("._citations") {
            stray.push(name);
        }
    }
    assert!(
        stray.is_empty(),
        "every file but citations.jsonl must hide under `._citations`, found: {stray:?}"
    );
    // ...and the family really is there to be covered by the rule: the live
    // peer's sidecar (which our flush may not reap) and its companion lock.
    let sidecars = list_sidecars(&dir);
    assert_eq!(
        sidecars.len(),
        1,
        "the live peer's sidecar is in the directory, found {sidecars:?}"
    );
    assert!(
        dir.join(format!("{}.lock", sidecars[0])).is_file(),
        "...and it is vouched for by its companion liveness lock"
    );
}

/// Bug #1 on the real filesystem: `put; remove; flush` must leave the id gone,
/// both in the reopened store and in the committed file's bytes. The old
/// order-free fold applied a global "records beat tombstones" rule, so the
/// entry resurrected on flush.
#[test]
fn put_then_remove_then_flush_removes_on_disk() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    block_on(store.put("doi:10.1/gone", record(1000))).expect("put");
    block_on(store.remove("doi:10.1/gone")).expect("remove");
    block_on(store.flush()).expect("flush");

    assert!(
        block_on(store.get("doi:10.1/gone")).expect("get").is_none(),
        "remove after put must survive a flush"
    );
    // The committed file holds only the header — the entry is not in it.
    let text = std::fs::read_to_string(dir.join("citations.jsonl")).expect("read main");
    assert!(
        !text.contains("doi:10.1/gone"),
        "the removed id must not be in the committed file: {text}"
    );

    drop(store);
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    assert!(
        block_on(reopened.get("doi:10.1/gone")).expect("get").is_none(),
        "the removal must survive a reopen"
    );
}

/// Bug #2 on the real filesystem: a re-fetch that shortens `expires` (a stepped-
/// back clock, a reduced TTL) must win because it is the newer write. The old
/// max-`expires` rule kept the larger stale value and pinned the entry expired.
#[test]
fn refetch_with_smaller_expires_wins_on_disk() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    // Commit the larger-expiry copy, then re-fetch a smaller one into a fresh
    // sidecar and compact.
    block_on(store.put("doi:10.1/a", record(5000))).expect("put large");
    block_on(store.flush()).expect("first flush");
    block_on(store.put("doi:10.1/a", record(1000))).expect("re-fetch smaller");
    block_on(store.flush()).expect("second flush");

    drop(store);
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    let got = block_on(reopened.get("doi:10.1/a"))
        .expect("get")
        .expect("present");
    assert_eq!(
        got.expires,
        Timestamp::from_millis(1000),
        "the newer, smaller-expiry re-fetch must win"
    );
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

/// Reaping a *crashed* writer's sidecar. Writer A records an entry and then is
/// dropped without flushing — simulating a crash, which releases the liveness
/// lock A held for its whole life and leaves its `._citations.<A>.log` behind.
/// A fresh writer B must fold A's orphaned data into the committed file and,
/// finding A's liveness lock free, reap the stray sidecar (and its companion).
#[test]
fn a_crashed_writers_sidecar_is_reaped() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    let a = block_on(SingleFileCacheStore::new(&dir)).expect("open A");
    block_on(a.put("doi:crashed", record(1234))).expect("A put");

    // A's stray sidecar is on disk before the crash.
    let before = list_sidecars(&dir);
    assert_eq!(before.len(), 1, "exactly A's sidecar present, found {before:?}");
    drop(a); // crash: the lifetime lock is released, the sidecar orphaned.

    let b = block_on(SingleFileCacheStore::new(&dir)).expect("open B");
    block_on(b.flush()).expect("B flush");

    // A's entry survived into the committed file.
    assert!(
        block_on(b.get("doi:crashed")).expect("get").is_some(),
        "the crashed writer's data must be folded into the main file"
    );
    let text = std::fs::read_to_string(dir.join("citations.jsonl")).expect("read main");
    assert!(text.contains("doi:crashed"), "committed file holds A's entry");

    // The stray sidecar — and its now-orphaned companion lock — are reaped.
    let after = list_sidecars(&dir);
    assert!(
        after.is_empty(),
        "the crashed writer's sidecar must be reaped, found {after:?}"
    );
    for name in &before {
        assert!(
            !dir.join(format!("{name}.lock")).exists(),
            "the crashed writer's orphaned companion lock must be reaped too"
        );
    }
}

/// The mirror of the above: a *live* writer's sidecar must survive a peer's
/// flush. A holds its liveness lock; B compacts while A is alive and must fold
/// A's log read-only without unlinking it — and A's own later work must not be
/// lost. If reaping ignored the lock, B would delete A's live sidecar here.
#[test]
fn a_live_writers_sidecar_is_not_reaped() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    let a = block_on(SingleFileCacheStore::new(&dir)).expect("open A");
    block_on(a.put("doi:from-a", record(1234))).expect("A put");

    let sidecars = list_sidecars(&dir);
    assert_eq!(sidecars.len(), 1, "A's sidecar present, found {sidecars:?}");
    let a_sidecar = sidecars[0].clone();

    // B compacts while A is still alive and holding its liveness lock.
    let b = block_on(SingleFileCacheStore::new(&dir)).expect("open B");
    block_on(b.flush()).expect("B flush");

    // The discriminating assertion: A's sidecar is untouched.
    assert!(
        dir.join(&a_sidecar).is_file(),
        "a live writer's sidecar must survive a peer's flush"
    );

    // A keeps working: a later put + flush must not be lost, and A's earlier
    // acknowledged put (which B folded) must still be present too.
    block_on(a.put("doi:from-a-again", record(5678))).expect("A second put");
    block_on(a.flush()).expect("A flush");

    drop(a);
    drop(b);
    let reader = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    assert!(
        block_on(reader.get("doi:from-a")).expect("get").is_some(),
        "A's first put (folded by B) must survive"
    );
    assert!(
        block_on(reader.get("doi:from-a-again"))
            .expect("get")
            .is_some(),
        "A's later put must not be lost"
    );
}

/// The liveness lock is given back at every flush (that is what keeps the
/// directory clean), so a writer that appends *again* must take a fresh one —
/// otherwise its second sidecar would sit there unvouched-for and a peer's flush
/// would read it as a crashed writer's leftover and reap it. Same shape as
/// `a_live_writers_sidecar_is_not_reaped`, but with A's flush in the middle.
#[test]
fn a_sidecar_written_after_a_flush_is_still_protected() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");

    let a = block_on(SingleFileCacheStore::new(&dir)).expect("open A");
    block_on(a.put("doi:before", record(1234))).expect("A put");
    block_on(a.flush()).expect("A flush");
    assert!(
        list_sidecars(&dir).is_empty(),
        "A's flush leaves no sidecar behind"
    );

    // A goes back to work: this append must re-take the companion lock.
    block_on(a.put("doi:after", record(5678))).expect("A second put");
    let sidecars = list_sidecars(&dir);
    assert_eq!(sidecars.len(), 1, "A's new sidecar, found {sidecars:?}");
    assert!(
        dir.join(format!("{}.lock", sidecars[0])).is_file(),
        "a fresh sidecar must come with a fresh companion lock"
    );

    // B compacts while A is alive: A's new sidecar must survive, unreaped.
    let b = block_on(SingleFileCacheStore::new(&dir)).expect("open B");
    block_on(b.flush()).expect("B flush");
    assert!(
        dir.join(&sidecars[0]).is_file(),
        "a live writer's post-flush sidecar must survive a peer's compaction"
    );

    block_on(a.flush()).expect("A flush again");
    drop(a);
    drop(b);
    let reader = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    for id in ["doi:before", "doi:after"] {
        assert!(
            block_on(reader.get(id)).expect("get").is_some(),
            "{id} must survive"
        );
    }
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

    let got = block_on(reopened.get("doi:10.1/a"))
        .expect("get")
        .expect("present");
    assert_eq!(got.stale_after, stored.stale_after);
    assert_eq!(got.expires, stored.expires);
    match got.payload {
        Payload::Concrete(v) => assert_eq!(v, concrete),
        other => panic!("expected a concrete payload, got {other:?}"),
    }

    let got = block_on(reopened.get("arxiv:2101.00001"))
        .expect("get")
        .expect("present");
    assert_eq!(got.stale_after, chained.stale_after);
    assert_eq!(got.expires, chained.expires);
    match got.payload {
        Payload::Chained {
            prefix,
            key,
            set_properties,
        } => {
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
    let want_default = std::fs::metadata(&probe)
        .expect("stat probe")
        .permissions()
        .mode()
        & 0o777;
    std::fs::remove_file(&probe).expect("rm probe");

    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("first flush");
    let created = std::fs::metadata(&main)
        .expect("stat main")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        created, want_default,
        "a new citations.jsonl should respect the umask, not a hard-coded 0600"
    );

    // An existing file's mode is carried across the rename.
    std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o644)).expect("chmod");
    block_on(store.put("doi:10.1/b", record(2000))).expect("put");
    block_on(store.flush()).expect("second flush");
    let after = std::fs::metadata(&main)
        .expect("stat main")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        after, 0o644,
        "flush must not tighten the committed file's mode"
    );

    // ...including a group-shared mode, the case that actually breaks users.
    std::fs::set_permissions(&main, std::fs::Permissions::from_mode(0o664)).expect("chmod");
    block_on(store.put("doi:10.1/c", record(3000))).expect("put");
    block_on(store.flush()).expect("third flush");
    let after = std::fs::metadata(&main)
        .expect("stat main")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(after, 0o664);
}

/// The store scans the cache directory for `._citations.*.log`, but it may only
/// ever *delete* its own. A user's unrelated file that happens to match must
/// come out of a compaction byte-for-byte intact — as must one under the bare
/// base, which is no longer part of the family at all.
#[test]
fn flush_does_not_delete_foreign_citations_star_log_files() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    let foreign = [
        ("._citations.import-notes.log", "hand-written import notes\n"),
        ("._citations.log", "a log named without a writer segment\n"),
        ("citations.4711-1.log", "a log under the bare base\n"),
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
    std::fs::create_dir_all(dir.join("._citations.oops.log")).expect("make the decoy directory");

    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open must tolerate it");
    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush must tolerate it");

    assert!(block_on(store.get("doi:10.1/a")).expect("get").is_some());
    assert!(
        dir.join("._citations.oops.log").is_dir(),
        "the decoy directory is left alone"
    );
    drop(store);
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen must tolerate it");
    assert!(block_on(reopened.get("doi:10.1/a")).expect("get").is_some());
}

/// The compaction lockfile is created by the flush that needs it and unlinked by
/// that same flush, while its lock is still held — so it is not one of the files
/// a finished run leaves behind. (Before, it stayed forever: one stray
/// `._citations.lock` in whatever directory the cache lived in.)
#[test]
fn flush_creates_the_compaction_lockfile_and_takes_it_away_again() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
    let lock = dir.join("._citations.lock");
    assert!(!lock.exists(), "nothing takes the lock before a flush");

    block_on(store.put("doi:10.1/a", record(1000))).expect("put");
    block_on(store.flush()).expect("flush");
    assert!(
        !lock.exists(),
        "the compaction lockfile must not outlive the flush that took it"
    );

    // Repeatable: a second flush takes and gives back a fresh one, and the
    // cache still works either side of it.
    block_on(store.put("doi:10.1/b", record(2000))).expect("put");
    block_on(store.flush()).expect("second flush");
    assert!(!lock.exists(), "and again on the next flush");
    assert_eq!(block_on(store.entries()).expect("entries").len(), 2);
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
        .open(dir.join("._citations.lock"))
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

/// Review item #7 on the real filesystem: a TTL-0 (ephemeral) record is served
/// within the run — `get()` returns it before and after a `flush()` — but never
/// lands in `citations.jsonl` or any sidecar, and is gone after a reopen. A
/// normal record put alongside it is committed and survives.
#[test]
fn ephemeral_records_are_never_committed_to_disk() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");

    block_on(store.put("doi:keep", record(1000))).expect("put normal");
    block_on(store.put("manual:Bohr, N. (1913)", ephemeral(0))).expect("put ephemeral");

    // Usable within the run, before the flush.
    assert!(
        block_on(store.get("manual:Bohr, N. (1913)"))
            .expect("get")
            .is_some()
    );

    block_on(store.flush()).expect("flush");

    // Still usable after the flush, within the same run.
    assert!(
        block_on(store.get("manual:Bohr, N. (1913)"))
            .expect("get")
            .is_some(),
        "an ephemeral record must survive a flush within the run"
    );
    assert!(block_on(store.get("doi:keep")).expect("get").is_some());

    // The committed file holds the normal id, never the ephemeral text — and
    // nothing else in the directory (any `.log` sidecar) mentions it either.
    let main = std::fs::read_to_string(dir.join("citations.jsonl")).expect("read main");
    assert!(main.contains("doi:keep"), "normal record committed: {main}");
    assert!(
        !main.contains("Bohr"),
        "ephemeral text must never reach citations.jsonl: {main}"
    );
    for entry in std::fs::read_dir(&dir).expect("read dir") {
        let path = entry.expect("entry").path();
        let bytes = std::fs::read(&path).unwrap_or_default();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("Bohr"),
            "ephemeral text leaked into {}: {text}",
            path.display()
        );
    }

    drop(store);
    // Reopen (a fresh process would): normal survives, ephemeral is gone.
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    assert!(
        block_on(reopened.get("doi:keep")).expect("get").is_some(),
        "the normal record survives a reopen"
    );
    assert!(
        block_on(reopened.get("manual:Bohr, N. (1913)"))
            .expect("get")
            .is_none(),
        "the ephemeral record must be gone after a reopen"
    );
}

// --- minimal backends for a manager-level ephemeral test -------------------

/// A fetcher that is never actually called (the `manual` source does no I/O);
/// it exists only to satisfy `CitationManager::new`.
struct NoFetch;
impl autocitefetch::Fetcher for NoFetch {
    fn fetch(
        &self,
        _req: autocitefetch::Request,
    ) -> autocitefetch::BoxFuture<'_, Result<autocitefetch::Response, autocitefetch::FetchError>> {
        Box::pin(async { Err(autocitefetch::FetchError::Status(599)) })
    }
}

struct FixedClock(i64);
impl autocitefetch::Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0)
    }
}

struct InstantTimer;
impl autocitefetch::Timer for InstantTimer {
    fn sleep(&self, _dur: std::time::Duration) -> autocitefetch::BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// End-to-end through the manager: a `manual:` citation is resolved and readable
/// within the run (after `retrieve`, which flushes), yet its text never reaches
/// the committed file and it is gone after a restart (a fresh store over the
/// same directory). This is the two-phase `retrieve`→`get` flow the ephemeral
/// policy must not break.
#[test]
fn manual_citation_is_usable_in_run_but_never_persisted() {
    use autocitefetch::CitationManager;
    use autocitefetch::source::ManualSource;

    let tmp = tempfile::tempdir().expect("temp dir");
    let dir = tmp.path().join("cache");
    let text = "Bohr, N. (1913). On the Constitution of Atoms.";

    {
        let store = block_on(SingleFileCacheStore::new(&dir)).expect("open");
        let mgr = CitationManager::new(NoFetch, store, FixedClock(1_000), InstantTimer)
            .register("manual", ManualSource::new("flm")).unwrap();

        let cites = vec![("manual".to_string(), text.to_string())];
        let report = block_on(mgr.retrieve(&cites)).expect("retrieve");
        assert!(report.is_complete(), "failures: {:?}", report.failures);

        // Within the run (retrieve has flushed), get() still resolves it.
        let item = block_on(mgr.get("manual", text)).expect("get within run");
        assert_eq!(item["_ready_formatted"]["flm"], text);
    } // the store (owned by the manager) drops here — simulating process exit.

    // The committed file must not carry the citation text.
    let main = std::fs::read_to_string(dir.join("citations.jsonl")).unwrap_or_default();
    assert!(
        !main.contains("Bohr"),
        "manual citation text must never be committed: {main}"
    );

    // Restart: a fresh store over the same directory has no trace of it.
    let reopened = block_on(SingleFileCacheStore::new(&dir)).expect("reopen");
    assert!(
        block_on(reopened.get(&format!("manual:{text}")))
            .expect("get")
            .is_none(),
        "the ephemeral manual entry must be gone after a restart"
    );
}
