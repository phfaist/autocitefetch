//! A single-file, git-committable [`CacheStore`] backed by an in-memory
//! `BTreeMap` and kept durable through per-writer append logs plus a locked
//! whole-file compaction.
//!
//! # Layout
//!
//! Three kinds of file live side-by-side in one directory, all derived from a
//! `base` name (the std host uses `"citations"`):
//!
//! * `{base}.jsonl` — the **main** file. Line 0 is a header (`{"schema":1}`);
//!   every following line is one entry, `{"id":…,"rec":…}`, sorted by id.
//!   One entry per line keeps git diffs minimal, and it is the only file worth
//!   committing to version control.
//! * `{base}.{writer_id}.log` — a **sidecar** append log, one per writer. Each
//!   line is either an entry or a tombstone (`{"id":…,"del":true}`). Writes go
//!   here lock-free; there is no header. These are throwaway and should be
//!   *gitignored* by users (`*.log`), as should the lockfile.
//! * `{base}.lock` — the compaction lockfile. The **only** thing that ever
//!   takes the lock is [`FileCacheStore::flush`]; likewise gitignore it.
//!
//! # How it stays consistent
//!
//! `put`/`remove` mutate the in-memory map and append a single line to *this*
//! writer's sidecar — no lock, no whole-file rewrite. [`flush`](FileCacheStore::flush)
//! (called by the manager at the end of `retrieve`/`prune`) takes the
//! exclusive lock, folds the main file and every sidecar into one map, writes
//! it back atomically, and deletes the sidecars it folded. All real I/O is
//! injected through the [`CacheFs`] trait so the core stays `no_std`.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::BoxFuture;
use crate::store::{CacheRecord, CacheStore, StoreError};

/// The main file's header line (line 0). Not an entry — always skipped when
/// reading, always re-emitted when compacting.
const HEADER: &str = r#"{"schema":1}"#;

/// A filesystem operation failed. Mirrors [`StoreError`]'s shape; the file
/// store maps these into `StoreError` at the trait boundary.
#[derive(Clone, Debug)]
pub struct FsError(pub String);

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl core::error::Error for FsError {}

/// An opaque, held lock guard. The host returns one from
/// [`CacheFs::lock_exclusive`]; dropping it releases the lock. The trait is
/// intentionally empty — the core only ever *holds* a guard for the duration
/// of a compaction and lets `Drop` do the work.
pub trait CacheGuard {}

/// Host-provided filesystem, injected so the store can run `no_std` (native
/// files, or something else entirely on WASM). All methods are async and
/// object-safe; paths use `/` separators and are built by the store.
pub trait CacheFs {
    /// Read a whole file. `Ok(None)` means the file is absent (not an error).
    fn read(&self, path: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, FsError>>;

    /// Append `bytes` to `path`, creating it if absent. Need not `fsync`.
    fn append(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>>;

    /// Replace `path`'s contents durably and all-or-nothing (temp file +
    /// `fsync` + rename + parent-dir `fsync`).
    fn atomic_replace(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>>;

    /// List the file names (not full paths) directly inside `dir`.
    fn list(&self, dir: &str) -> BoxFuture<'_, Result<Vec<String>, FsError>>;

    /// Remove a file; a no-op if it is already absent.
    fn remove(&self, path: &str) -> BoxFuture<'_, Result<(), FsError>>;

    /// Take an exclusive advisory lock on `path`, returning a held guard.
    /// Dropping the guard releases the lock.
    fn lock_exclusive(&self, path: &str) -> BoxFuture<'_, Result<Box<dyn CacheGuard>, FsError>>;
}

// --- on-disk line shapes ---------------------------------------------------

/// A main-file / sidecar entry line as read back: `{"id":…,"rec":…}`.
#[derive(Deserialize)]
struct EntryLine {
    id: String,
    rec: CacheRecord,
}

/// An entry line for writing, borrowing its parts to avoid a clone.
#[derive(Serialize)]
struct EntryLineRef<'a> {
    id: &'a str,
    rec: &'a CacheRecord,
}

/// A tombstone line for writing: `{"id":…,"del":true}`.
#[derive(Serialize)]
struct TombstoneRef<'a> {
    id: &'a str,
    del: bool,
}

/// A sidecar line as read back — either an entry (`rec` present) or a
/// tombstone (`del: true`). Both fields default so each form parses.
#[derive(Deserialize)]
struct SidecarLine {
    id: String,
    #[serde(default)]
    rec: Option<CacheRecord>,
    #[serde(default)]
    del: bool,
}

/// Map an [`FsError`] into a [`StoreError`] at the trait boundary.
fn fs_store(e: FsError) -> StoreError {
    StoreError(e.0)
}

/// Map a serialization failure into a [`StoreError`].
fn ser_store(e: impl fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

/// A [`CacheStore`] persisted as one committable JSONL file plus lock-free
/// per-writer sidecar logs, compacted under a lock. Generic over the injected
/// [`CacheFs`]; carries no clock.
pub struct FileCacheStore<Fs: CacheFs> {
    fs: Fs,
    dir: String,
    base: String,
    #[allow(dead_code)]
    writer_id: String,
    /// `{dir}/{base}.jsonl`.
    main: String,
    /// `{dir}/{base}.{writer_id}.log` — this writer's append log.
    sidecar: String,
    /// `{dir}/{base}.lock`.
    lockfile: String,
    /// The authoritative in-memory view; reads clone out of it.
    mem: RefCell<BTreeMap<String, CacheRecord>>,
}

impl<Fs: CacheFs> FileCacheStore<Fs> {
    /// Open a store over `fs` rooted at `dir`, using `base` for file names and
    /// `writer_id` to name this writer's sidecar. Loads the current on-disk
    /// state (main file folded with any uncompacted sidecars) into memory.
    pub async fn open(
        fs: Fs,
        dir: impl Into<String>,
        base: impl Into<String>,
        writer_id: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let dir = dir.into();
        let base = base.into();
        let writer_id = writer_id.into();
        let main = alloc::format!("{dir}/{base}.jsonl");
        let sidecar = alloc::format!("{dir}/{base}.{writer_id}.log");
        let lockfile = alloc::format!("{dir}/{base}.lock");

        let (map, _folded) = load_merged(&fs, &dir, &base).await.map_err(fs_store)?;

        Ok(FileCacheStore {
            fs,
            dir,
            base,
            writer_id,
            main,
            sidecar,
            lockfile,
            mem: RefCell::new(map),
        })
    }

    /// Serialize a single entry line (`{"id":…,"rec":…}`) with a trailing
    /// newline, ready to append to a sidecar.
    fn entry_line(id: &str, rec: &CacheRecord) -> Result<String, StoreError> {
        let mut s = serde_json::to_string(&EntryLineRef { id, rec }).map_err(ser_store)?;
        s.push('\n');
        Ok(s)
    }

    /// Serialize a single tombstone line (`{"id":…,"del":true}`) with a
    /// trailing newline.
    fn tombstone_line(id: &str) -> Result<String, StoreError> {
        let mut s = serde_json::to_string(&TombstoneRef { id, del: true }).map_err(ser_store)?;
        s.push('\n');
        Ok(s)
    }
}

impl<Fs: CacheFs> CacheStore for FileCacheStore<Fs> {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.mem.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }

    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        // Serialize the sidecar line first (borrowing `record`), then move the
        // record into the in-memory map — no clone, no RefCell borrow held
        // across the await below.
        let line = Self::entry_line(id, &record);
        self.mem.borrow_mut().insert(id.to_string(), record);
        let sidecar = self.sidecar.clone();
        Box::pin(async move {
            let line = line?;
            self.fs
                .append(&sidecar, line.as_bytes())
                .await
                .map_err(fs_store)?;
            Ok(())
        })
    }

    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        let line = Self::tombstone_line(id);
        self.mem.borrow_mut().remove(id);
        let sidecar = self.sidecar.clone();
        Box::pin(async move {
            let line = line?;
            self.fs
                .append(&sidecar, line.as_bytes())
                .await
                .map_err(fs_store)?;
            Ok(())
        })
    }

    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        let all: Vec<_> = self
            .mem
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::pin(async move { Ok(all) })
    }

    /// Compaction: the only operation that locks and the only one that
    /// rewrites the whole file. Folds the main file and every sidecar into one
    /// map, writes it back atomically, deletes the folded sidecars, and
    /// refreshes the in-memory view.
    fn flush(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            // Hold the guard for the whole critical section; dropping it at the
            // end releases the lock.
            let _guard = self
                .fs
                .lock_exclusive(&self.lockfile)
                .await
                .map_err(fs_store)?;

            let (map, folded) = load_merged(&self.fs, &self.dir, &self.base)
                .await
                .map_err(fs_store)?;

            let buf = serialize_main(&map)?;
            self.fs
                .atomic_replace(&self.main, buf.as_bytes())
                .await
                .map_err(fs_store)?;

            // Prompt deletion of every sidecar we folded — safe because the
            // lock serializes compaction.
            for name in &folded {
                let path = alloc::format!("{}/{}", self.dir, name);
                self.fs.remove(&path).await.map_err(fs_store)?;
            }

            *self.mem.borrow_mut() = map;
            Ok(())
        })
    }
}

/// Insert `rec` under `id` keeping the MAX-`expires` copy: overwrite only if
/// the id is absent or the incoming record's hard expiry is strictly later.
fn insert_max_expires(map: &mut BTreeMap<String, CacheRecord>, id: String, rec: CacheRecord) {
    match map.get(&id) {
        Some(existing) if rec.expires <= existing.expires => {}
        _ => {
            map.insert(id, rec);
        }
    }
}

/// Parse a main file into `map`. Line 0 is the header and is skipped; every
/// other non-blank line is parsed as an entry, and lines that fail to parse
/// (e.g. a torn final line from a crash) are silently skipped.
fn parse_main_into(bytes: &[u8], map: &mut BTreeMap<String, CacheRecord>) {
    for (idx, raw) in bytes.split(|&b| b == b'\n').enumerate() {
        if idx == 0 {
            continue; // header
        }
        let Ok(s) = core::str::from_utf8(raw) else {
            continue;
        };
        let s = s.trim();
        if s.is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<EntryLine>(s) {
            map.insert(entry.id, entry.rec);
        }
    }
}

/// Fold a sidecar log into `map`, recording re-added and deleted ids. Entries
/// dedup by MAX-`expires`; tombstones are collected. Unparseable lines (a torn
/// tail, blank lines) are skipped.
fn fold_sidecar_into(
    bytes: &[u8],
    map: &mut BTreeMap<String, CacheRecord>,
    added: &mut BTreeSet<String>,
    deleted: &mut BTreeSet<String>,
) {
    for raw in bytes.split(|&b| b == b'\n') {
        let Ok(s) = core::str::from_utf8(raw) else {
            continue;
        };
        let s = s.trim();
        if s.is_empty() {
            continue;
        }
        let Ok(line) = serde_json::from_str::<SidecarLine>(s) else {
            continue;
        };
        if line.del {
            deleted.insert(line.id);
        } else if let Some(rec) = line.rec {
            insert_max_expires(map, line.id.clone(), rec);
            added.insert(line.id);
        }
    }
}

/// Whether `name` is a sidecar log for `base` (`{base}.*.log`) — but not the
/// main file (`{base}.jsonl`) or the lockfile (`{base}.lock`).
fn is_sidecar(name: &str, base: &str) -> bool {
    // e.g. base = "citations": accept "citations.<writer>.log".
    let prefix = alloc::format!("{base}.");
    name.starts_with(&prefix) && name.ends_with(".log")
}

/// Read the main file and every sidecar for `base` under `dir`, fold them into
/// one map (records win over tombstones per the MAX-`expires` rule), and
/// return the merged map together with the names of the sidecars folded.
async fn load_merged<Fs: CacheFs>(
    fs: &Fs,
    dir: &str,
    base: &str,
) -> Result<(BTreeMap<String, CacheRecord>, Vec<String>), FsError> {
    let mut map = BTreeMap::new();

    let main_path = alloc::format!("{dir}/{base}.jsonl");
    if let Some(bytes) = fs.read(&main_path).await? {
        parse_main_into(&bytes, &mut map);
    }

    let mut added: BTreeSet<String> = BTreeSet::new();
    let mut deleted: BTreeSet<String> = BTreeSet::new();
    let mut folded: Vec<String> = Vec::new();

    let mut names = fs.list(dir).await?;
    names.sort(); // deterministic fold order (only matters for equal-`expires` ties)
    for name in names {
        if !is_sidecar(&name, base) {
            continue;
        }
        let path = alloc::format!("{dir}/{name}");
        if let Some(bytes) = fs.read(&path).await? {
            fold_sidecar_into(&bytes, &mut map, &mut added, &mut deleted);
        }
        folded.push(name);
    }

    // Records always win over tombstones: only drop ids that were deleted and
    // never re-added by a sidecar entry.
    for id in &deleted {
        if !added.contains(id) {
            map.remove(id);
        }
    }

    Ok((map, folded))
}

/// Serialize `map` into a main-file buffer: the header line, then one sorted
/// entry per line (the `BTreeMap` iterates in id order).
fn serialize_main(map: &BTreeMap<String, CacheRecord>) -> Result<String, StoreError> {
    let mut buf = String::new();
    buf.push_str(HEADER);
    buf.push('\n');
    for (id, rec) in map {
        let line = serde_json::to_string(&EntryLineRef { id, rec }).map_err(ser_store)?;
        buf.push_str(&line);
        buf.push('\n');
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::Timestamp;
    use crate::store::Payload;
    use core::future::Future;
    use core::task::{Context, Poll, Waker};

    // --- an always-ready block_on (the mock fs never truly pends) ----------

    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        let mut fut = core::pin::pin!(fut);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    // --- an in-memory CacheFs mock -----------------------------------------

    #[derive(Default)]
    struct MemFs {
        files: RefCell<BTreeMap<String, Vec<u8>>>,
    }

    struct NoopGuard;
    impl CacheGuard for NoopGuard {}

    impl CacheFs for MemFs {
        fn read(&self, path: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, FsError>> {
            let v = self.files.borrow().get(path).cloned();
            Box::pin(async move { Ok(v) })
        }
        fn append(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>> {
            self.files
                .borrow_mut()
                .entry(path.to_string())
                .or_default()
                .extend_from_slice(bytes);
            Box::pin(async move { Ok(()) })
        }
        fn atomic_replace(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>> {
            self.files
                .borrow_mut()
                .insert(path.to_string(), bytes.to_vec());
            Box::pin(async move { Ok(()) })
        }
        fn list(&self, dir: &str) -> BoxFuture<'_, Result<Vec<String>, FsError>> {
            let prefix = alloc::format!("{dir}/");
            let names: Vec<String> = self
                .files
                .borrow()
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix))
                // only direct children (no nested separators)
                .filter(|rest| !rest.contains('/'))
                .map(|rest| rest.to_string())
                .collect();
            Box::pin(async move { Ok(names) })
        }
        fn remove(&self, path: &str) -> BoxFuture<'_, Result<(), FsError>> {
            self.files.borrow_mut().remove(path);
            Box::pin(async move { Ok(()) })
        }
        fn lock_exclusive(
            &self,
            _path: &str,
        ) -> BoxFuture<'_, Result<Box<dyn CacheGuard>, FsError>> {
            Box::pin(async move { Ok(Box::new(NoopGuard) as Box<dyn CacheGuard>) })
        }
    }

    fn rec(expires_ms: i64) -> CacheRecord {
        CacheRecord {
            payload: Payload::Concrete(serde_json::json!({"id": "x", "title": "t"})),
            stale_after: Timestamp::from_millis(expires_ms / 2),
            expires: Timestamp::from_millis(expires_ms),
        }
    }

    /// Directly seed a sidecar file's raw bytes on the mock fs.
    fn write_raw(fs: &MemFs, path: &str, contents: &str) {
        fs.files
            .borrow_mut()
            .insert(path.to_string(), contents.as_bytes().to_vec());
    }

    #[test]
    fn put_flush_reopen_roundtrip() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.put("doi:2", rec(2000))).unwrap();
        block_on(store.flush()).unwrap();

        // The sidecar is gone; only the main file remains.
        let inner_fs = &store.fs;
        assert!(
            inner_fs
                .files
                .borrow()
                .contains_key("cache/citations.jsonl")
        );
        assert!(
            !inner_fs
                .files
                .borrow()
                .contains_key("cache/citations.w1.log")
        );

        // Reopen over the same fs contents: both entries survive.
        let files = store.fs.files.borrow().clone();
        let fs2 = MemFs {
            files: RefCell::new(files),
        };
        let store2 = block_on(FileCacheStore::open(fs2, "cache", "citations", "w2")).unwrap();
        assert!(block_on(store2.get("doi:1")).unwrap().is_some());
        assert!(block_on(store2.get("doi:2")).unwrap().is_some());
        let entries = block_on(store2.entries()).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn max_expires_wins_across_sidecar_entries() {
        let fs = MemFs::default();
        // Two entries for the same id in one sidecar: later `expires` must win.
        let smaller = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap();
        let larger = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(5000)).unwrap();
        // Write the *smaller* one last, to prove ordering doesn't decide it.
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{larger}{smaller}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        block_on(store.flush()).unwrap();
        let got = block_on(store.get("doi:1")).unwrap().unwrap();
        assert_eq!(got.expires, Timestamp::from_millis(5000));
    }

    #[test]
    fn tombstone_loses_to_record_but_deletes_orphan() {
        let fs = MemFs::default();
        // id B lives in the already-committed main file...
        let main = alloc::format!(
            "{HEADER}\n{}",
            FileCacheStore::<MemFs>::entry_line("doi:B", &rec(1000)).unwrap()
        );
        write_raw(&fs, "cache/citations.jsonl", &main);
        // ...and the sidecar has: a record + tombstone for A (record wins),
        // plus an orphan tombstone for B (no competing record -> B removed).
        let a_rec = FileCacheStore::<MemFs>::entry_line("doi:A", &rec(1000)).unwrap();
        let a_del = FileCacheStore::<MemFs>::tombstone_line("doi:A").unwrap();
        let b_del = FileCacheStore::<MemFs>::tombstone_line("doi:B").unwrap();
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{a_rec}{a_del}{b_del}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        block_on(store.flush()).unwrap();
        assert!(
            block_on(store.get("doi:A")).unwrap().is_some(),
            "record wins"
        );
        assert!(
            block_on(store.get("doi:B")).unwrap().is_none(),
            "orphan tombstone deletes"
        );
    }

    #[test]
    fn torn_tail_is_tolerated() {
        let fs = MemFs::default();
        let good = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap();
        // A truncated (torn) final line: valid prefix of JSON, no newline.
        let torn = r#"{"id":"doi:2","rec":{"payload":{"conc"#;
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{good}{torn}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        block_on(store.flush()).unwrap();
        assert!(
            block_on(store.get("doi:1")).unwrap().is_some(),
            "intact line kept"
        );
        assert!(
            block_on(store.get("doi:2")).unwrap().is_none(),
            "torn line skipped"
        );
    }

    #[test]
    fn multiple_sidecars_folded_and_deleted() {
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        write_raw(
            &fs,
            "cache/citations.w2.log",
            &FileCacheStore::<MemFs>::entry_line("doi:2", &rec(1000)).unwrap(),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w3")).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        assert!(files.contains_key("cache/citations.jsonl"));
        assert!(!files.contains_key("cache/citations.w1.log"));
        assert!(!files.contains_key("cache/citations.w2.log"));
        drop(files);
        assert_eq!(block_on(store.entries()).unwrap().len(), 2);
    }
}
