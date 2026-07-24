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
//! exclusive lock, folds the main file and every sidecar into one map, and
//! writes it back atomically. All real I/O is injected through the [`CacheFs`]
//! trait so the core stays `no_std`.
//!
//! # Merge order (last write wins)
//!
//! Folding the directory is a **last-write-wins replay over a total order**: the
//! main file first (it is the baseline written at the previous compaction), then
//! each sidecar in sorted name order, and within a sidecar in line order —
//! append order is that writer's own time order. An entry line inserts, a
//! tombstone removes. The consequences that matter:
//!
//! * `put(id); remove(id)` **removes** — the tombstone is folded after the
//!   entry, so it wins.
//! * a re-fetch that produces a *smaller* `expires` (a wall clock stepped back
//!   by NTP or a VM restore, or a reduced TTL) **wins**, because it is the newer
//!   write. The store deliberately does **not** keep the max-`expires` copy:
//!   doing so pinned the entry `Expired` forever and drove a permanent re-fetch
//!   loop.
//! * a sidecar always beats the committed main-file copy for the same id.
//!
//! The one thing this order *cannot* make exact is a cross-writer race: two live
//! writers appending the same id to their own sidecars concurrently are ordered
//! only by sidecar **name**, so the higher-named writer wins regardless of which
//! actually wrote last. This is an accepted known limitation — the concurrent
//! shared-directory setup is best-effort, and each writer's *own* sequence of
//! operations is always honored exactly.
//!
//! # Which files compaction may delete
//!
//! Exactly one: `{base}.{writer_id}.log`, *this* store's own sidecar, and only
//! when that compaction actually folded it in. Every other file in the
//! directory — peers' sidecars, the main file, the lockfile, anything the user
//! happens to keep there — is only ever **read**.
//!
//! This is not fussiness, it is the correctness argument. The lock serializes
//! compaction against *compaction*; it does **not** serialize compaction
//! against `append`, which takes no lock at all. A peer's `put` can therefore
//! land after this writer's fold has read that peer's sidecar and before the
//! rewrite completes — an acknowledged, on-disk write that the peer has been
//! promised. Deleting the peer's sidecar at that point destroys it (and, since
//! `flush` rebuilds `mem` purely from disk, the peer's own next flush would
//! then erase its in-memory copy too). Re-folding somebody else's log instead
//! is idempotent — the fold is a deterministic last-write-wins replay of the
//! main file then every sidecar in a fixed order, so re-applying the same bytes
//! reaches the same map — and each peer reaps its own log on its next flush.
//!
//! Known trade-off: a sidecar whose owning writer **crashed** is now never
//! reaped (its writer id is never reused, so nobody claims it). It accumulates
//! in the directory, and its lines are re-folded on every compaction forever.
//! For entry lines that is merely wasted work; for a *tombstone* it is worse —
//! an orphaned `{"id":…,"del":true}` re-deletes that id on every compaction,
//! so a subsequent re-fetch is committed and then dropped again on the next
//! flush. Reaping such a log safely needs a way to prove the owner is gone — a
//! `CacheFs::try_lock_exclusive`, which a live appender would hold and a dead
//! one would not — and that is a trait change, left as follow-up work. Until
//! then, deleting stray `{base}.*.log` files is a safe manual cleanup while no
//! writer is running.
//!
//! Because a store deletes its own sidecar, `flush` must not be polled
//! concurrently with a `put`/`remove` *on the same store*; the core is
//! single-cooperative-task and the manager awaits `flush` on its own, so this
//! holds today. Concurrency **between** stores is fully supported.
//!
//! # Corrupt or newer-schema main files
//!
//! Torn-line tolerance is right for a sidecar (a crash can leave a half-written
//! append) and *wrong* for the main file, which only ever appears via an
//! atomic temp-file + `fsync` + rename and so can never legitimately be torn.
//! A main-file line this build cannot parse is therefore a hard error rather
//! than a skip: skipping it would delete it on the next compaction. In
//! particular a `{"schema":N}` header for an unknown `N` makes `open`/`flush`
//! **fail loudly** instead of overwriting a newer build's cache. See
//! [`parse_main_into`].

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;
use core::fmt;

use serde::{Deserialize, Serialize};

use crate::BoxFuture;
use crate::store::{CacheRecord, CacheStore, StoreError};

/// The on-disk format version of the main file. Written on line 0 and
/// **validated** on read: an unknown version makes the load fail rather than
/// silently truncate a cache written by a different build. Bump this only
/// together with an actual format change.
const SCHEMA_VERSION: u32 = 1;

/// The main file's header line (line 0). Not an entry — ignored when reading,
/// always re-emitted when compacting. Kept in sync with [`SCHEMA_VERSION`] by
/// `tests::header_constant_matches_schema_version`.
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

/// The main file's header line as read back: `{"schema":N}`. Disjoint from
/// [`EntryLine`] (neither shape parses as the other), so a line can be
/// classified by trying both.
#[derive(Deserialize)]
struct HeaderLine {
    schema: u32,
}

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
    writer_id: String,
    /// `{dir}/{base}.jsonl`.
    main: String,
    /// `{base}.{writer_id}.log` — the bare *name* of this writer's append log,
    /// as it appears in a [`CacheFs::list`] listing. Compared against the
    /// folded set before compaction unlinks it.
    sidecar_name: String,
    /// `{dir}/{base}.{writer_id}.log` — the same file, as a path.
    sidecar: String,
    /// `{dir}/{base}.lock`.
    lockfile: String,
    /// The authoritative in-memory view; reads clone out of it.
    mem: RefCell<BTreeMap<String, CacheRecord>>,
}

impl<Fs: CacheFs> fmt::Debug for FileCacheStore<Fs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCacheStore")
            .field("main", &self.main)
            .field("writer_id", &self.writer_id)
            .field("entries", &self.mem.borrow().len())
            .finish()
    }
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
        let sidecar_name = alloc::format!("{base}.{writer_id}.log");
        let sidecar = alloc::format!("{dir}/{sidecar_name}");
        let lockfile = alloc::format!("{dir}/{base}.lock");

        let merged = load_merged(&fs, &dir, &base).await.map_err(fs_store)?;

        Ok(FileCacheStore {
            fs,
            dir,
            base,
            writer_id,
            main,
            sidecar_name,
            sidecar,
            lockfile,
            mem: RefCell::new(merged.map),
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
        // across the await below. Note the map is updated in this synchronous
        // prologue while the append happens in the returned future: a future
        // that is created and then dropped without being polled leaves `mem`
        // one entry ahead of disk (harmless — the next `flush` re-reads disk
        // and the entry simply reverts).
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
    /// map, writes it back atomically, deletes **only this writer's own**
    /// sidecar, and refreshes the in-memory view.
    ///
    /// See the module docs for why peers' sidecars are read but never unlinked
    /// (the lock serializes compaction against compaction, never against a
    /// lock-free `append`).
    fn flush(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            // Hold the guard for the whole critical section; dropping it at the
            // end releases the lock.
            let _guard = self
                .fs
                .lock_exclusive(&self.lockfile)
                .await
                .map_err(fs_store)?;

            let merged = load_merged(&self.fs, &self.dir, &self.base)
                .await
                .map_err(fs_store)?;

            // Defence in depth: compaction must never be the operation that
            // empties a populated cache. An empty fold is legitimate only when
            // tombstones explain it (a `prune` that removed everything);
            // otherwise the main file's entries went missing for a reason we do
            // not understand, and overwriting it would make that permanent.
            if merged.map.is_empty() && merged.main_entries > 0 && !merged.saw_tombstone {
                return Err(StoreError(alloc::format!(
                    "{}: refusing to compact {} committed entries down to nothing (writer {})",
                    self.main,
                    merged.main_entries,
                    self.writer_id
                )));
            }

            let buf = serialize_main(&merged.map)?;
            self.fs
                .atomic_replace(&self.main, buf.as_bytes())
                .await
                .map_err(fs_store)?;

            // Reap our own log — and only if this compaction actually folded it
            // in, so the unlink can never drop writes we did not just persist.
            // A failed unlink is not a failed flush: the data is already
            // durable in the main file and the stale sidecar is simply re-folded
            // (idempotently) next time.
            if merged.folded.iter().any(|name| name == &self.sidecar_name) {
                let _ = self.fs.remove(&self.sidecar).await;
            }

            *self.mem.borrow_mut() = merged.map;
            Ok(())
        })
    }
}

/// Parse a main file into `map`, returning how many entry lines it contributed.
///
/// Every non-blank line must be either an entry (`{"id":…,"rec":…}`) or a
/// `{"schema":N}` header for the version this build understands; anything else
/// is an error. There is deliberately **no** torn-line tolerance here: the main
/// file is only ever produced by [`CacheFs::atomic_replace`] (temp file +
/// `fsync` + rename), so it cannot legitimately be half-written, and skipping a
/// line we merely fail to understand would delete it on the next compaction.
/// The two cases that matters for are a corrupted file and a file written by a
/// *newer* build — both now surface as a refusal from `open`/`flush` instead of
/// a silent truncation.
///
/// The header is recognized (and ignored) wherever it appears rather than only
/// on line 0, so a hand-merged or concatenated file still loads, and a file
/// with no header at all loads all of its entries.
///
/// Lines are `trim`ed, which is also what makes a `\r\n`-terminated file (a
/// Windows editor, or git with `core.autocrlf`) read back correctly — this file
/// is meant to be committed and hand-edited.
fn parse_main_into(
    bytes: &[u8],
    path: &str,
    map: &mut BTreeMap<String, CacheRecord>,
) -> Result<usize, FsError> {
    let mut entries = 0usize;
    for (idx, raw) in bytes.split(|&b| b == b'\n').enumerate() {
        let lineno = idx + 1;
        let Ok(s) = core::str::from_utf8(raw) else {
            return Err(FsError(alloc::format!(
                "{path}: line {lineno} is not valid UTF-8"
            )));
        };
        let s = s.trim();
        if s.is_empty() {
            continue;
        }
        if let Ok(header) = serde_json::from_str::<HeaderLine>(s) {
            if header.schema != SCHEMA_VERSION {
                return Err(FsError(alloc::format!(
                    "{path}: line {lineno}: cache schema version {} is not supported by this \
                     build (which writes version {SCHEMA_VERSION}); refusing to read the file \
                     rather than overwrite it",
                    header.schema
                )));
            }
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<EntryLine>(s) {
            map.insert(entry.id, entry.rec);
            entries += 1;
            continue;
        }
        return Err(FsError(alloc::format!(
            "{path}: line {lineno} is neither a header nor an entry; refusing to load the file \
             rather than silently drop the line on the next compaction"
        )));
    }
    Ok(entries)
}

/// Fold a sidecar log into `map` **in line order**, returning whether it
/// carried any tombstone. Append order *is* this writer's time order, so a plain
/// last-write-wins fold is correct: an entry line does `map.insert` (a later
/// write for an id overwrites an earlier one, whatever its `expires`), and a
/// tombstone does `map.remove` (a `remove` written after a `put` for the same id
/// actually removes it). Because the caller folds the main file first and then
/// each sidecar, a sidecar write always wins over the committed copy.
///
/// Unparseable lines are skipped — unlike the main file, a sidecar is appended
/// to without `fsync`, so a crash legitimately leaves a torn tail. `trim` also
/// absorbs `\r\n`.
fn fold_sidecar_into(bytes: &[u8], map: &mut BTreeMap<String, CacheRecord>) -> bool {
    let mut saw_tombstone = false;
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
            map.remove(&line.id);
            saw_tombstone = true;
        } else if let Some(rec) = line.rec {
            map.insert(line.id, rec);
        }
    }
    saw_tombstone
}

/// Whether `name` is a sidecar log belonging to this store's family:
/// `{base}.{writer}.log` with a **non-empty** writer segment.
///
/// Rejected: the main file (`{base}.jsonl`), the lockfile (`{base}.lock`), a
/// bare `{base}.log` with no writer segment at all, and anything under a
/// different base.
///
/// The writer segment itself cannot be validated — `writer_id` is
/// caller-supplied (the std host happens to use `<pid>-<nanos>`, but nothing in
/// the core requires that shape), so `citations.notes.log` is genuinely
/// indistinguishable from the log of a writer called `notes`. Matching one is
/// harmless in both directions: a match only ever causes a **read**, lines that
/// do not parse are skipped, and compaction unlinks nothing but this store's
/// own sidecar. It must also stay permissive enough to always match our own
/// `sidecar_name`, since `flush` folds and reaps by that name.
///
/// (A foreign `.log` that happens to contain entry-shaped JSON — say a copy of
/// an old `citations.jsonl` — would still be folded in and could resurrect
/// stale entries. Distinguishing that properly needs a self-identifying header
/// line in the sidecar format; noted as follow-up, not fixed here.)
fn is_sidecar(name: &str, base: &str) -> bool {
    let Some(rest) = name.strip_prefix(base) else {
        return false;
    };
    let Some(rest) = rest.strip_prefix('.') else {
        return false;
    };
    let Some(writer) = rest.strip_suffix(".log") else {
        return false;
    };
    !writer.is_empty()
}

/// One folded view of the whole cache directory.
struct Merged {
    /// The merged map, built last-write-wins in a total fold order: the main
    /// file first (the baseline written at the last compaction), then each
    /// sidecar in sorted name order, and within a sidecar in line order.
    map: BTreeMap<String, CacheRecord>,
    /// Names (not paths) of the sidecars actually read and folded, sorted.
    /// `flush` consults this before unlinking its own log.
    folded: Vec<String>,
    /// How many entry lines the main file contributed.
    main_entries: usize,
    /// Whether any sidecar carried a tombstone. Only used by the "never compact
    /// a populated cache down to nothing" guard: an all-tombstoned empty result
    /// is legitimate, an inexplicably empty one is not.
    saw_tombstone: bool,
}

/// Read the main file and every sidecar for `base` under `dir` and fold them
/// into one map with **last-write-wins** semantics over a deterministic total
/// order: the main file first, then each sidecar in sorted name order, and
/// within each sidecar in line order (append order == that writer's time
/// order). A later write for an id therefore beats an earlier one, and any
/// sidecar beats the committed main-file copy — so `put;remove` removes,
/// `put big; put small` keeps the *small* (newer) expiry, and a re-fetch in a
/// fresh sidecar overrides the committed record.
///
/// The one irreducible ambiguity is cross-writer: two live writers that write
/// the same id concurrently are ordered only by sidecar name, so the
/// higher-named writer wins regardless of real time. That is a documented known
/// limitation (see the module docs); the same-writer cases above are exact.
///
/// A failure to read the *main* file is fatal; a failure to read an individual
/// *sidecar* is not — a directory that happens to be named like one, a
/// permission error, or a peer reaping its own log mid-scan should not make the
/// whole cache unopenable. Such a sidecar is left out of `folded`, so nothing
/// unlinks it and its contents stay on disk for a later pass.
async fn load_merged<Fs: CacheFs>(fs: &Fs, dir: &str, base: &str) -> Result<Merged, FsError> {
    let mut map = BTreeMap::new();

    let main_path = alloc::format!("{dir}/{base}.jsonl");
    let mut main_entries = 0usize;
    if let Some(bytes) = fs.read(&main_path).await? {
        main_entries = parse_main_into(&bytes, &main_path, &mut map)?;
    }

    let mut folded: Vec<String> = Vec::new();
    let mut saw_tombstone = false;

    let mut names = fs.list(dir).await?;
    names.sort(); // deterministic cross-writer fold order (see module docs)
    for name in names {
        if !is_sidecar(&name, base) {
            continue;
        }
        let path = alloc::format!("{dir}/{name}");
        match fs.read(&path).await {
            Ok(Some(bytes)) => saw_tombstone |= fold_sidecar_into(&bytes, &mut map),
            Ok(None) => {}
            Err(_) => continue,
        }
        folded.push(name);
    }

    Ok(Merged {
        map,
        folded,
        main_entries,
        saw_tombstone,
    })
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

    /// Bounded like every other test file's driver: a mock that pends must
    /// panic, not hang `cargo test` forever.
    fn block_on<F: Future>(fut: F) -> F::Output {
        let mut cx = Context::from_waker(Waker::noop());
        let mut fut = core::pin::pin!(fut);
        for _ in 0..1_000_000 {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
        panic!("future did not complete (a mock unexpectedly pended)");
    }

    // --- an in-memory CacheFs mock -----------------------------------------

    #[derive(Default)]
    struct MemFs {
        files: RefCell<BTreeMap<String, Vec<u8>>>,
        /// When set, every `remove` fails — used to prove a failed sidecar
        /// unlink does not fail an otherwise successful compaction.
        remove_fails: core::cell::Cell<bool>,
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
            if self.remove_fails.get() {
                return Box::pin(async move { Err(FsError("remove denied".into())) });
            }
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
            ..Default::default()
        };
        let store2 = block_on(FileCacheStore::open(fs2, "cache", "citations", "w2")).unwrap();
        assert!(block_on(store2.get("doi:1")).unwrap().is_some());
        assert!(block_on(store2.get("doi:2")).unwrap().is_some());
        let entries = block_on(store2.entries()).unwrap();
        assert_eq!(entries.len(), 2);
    }

    /// Within one sidecar the LAST-written line for an id wins, regardless of its
    /// `expires` — append order is that writer's time order, so a re-fetch that
    /// happens to shorten the expiry must not be discarded (bug #2).
    #[test]
    fn last_write_wins_within_a_sidecar() {
        let fs = MemFs::default();
        let larger = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(5000)).unwrap();
        let smaller = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap();
        // Write the larger-expires line first, the smaller one last: last wins.
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{larger}{smaller}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        block_on(store.flush()).unwrap();
        let got = block_on(store.get("doi:1")).unwrap().unwrap();
        assert_eq!(got.expires, Timestamp::from_millis(1000));
    }

    /// A tombstone written *after* a record for the same id removes it (bug #1),
    /// and an orphan tombstone (no competing record) still deletes.
    #[test]
    fn tombstone_after_record_removes_it_and_deletes_orphan() {
        let fs = MemFs::default();
        // id B lives in the already-committed main file...
        let main = alloc::format!(
            "{HEADER}\n{}",
            FileCacheStore::<MemFs>::entry_line("doi:B", &rec(1000)).unwrap()
        );
        write_raw(&fs, "cache/citations.jsonl", &main);
        // ...and the sidecar has: a record then a tombstone for A (tombstone
        // wins, it is the later write), plus an orphan tombstone for B (removes
        // the committed main-file record).
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
            block_on(store.get("doi:A")).unwrap().is_none(),
            "tombstone after record removes it"
        );
        assert!(
            block_on(store.get("doi:B")).unwrap().is_none(),
            "orphan tombstone deletes the main-file record"
        );
    }

    /// The mirror image: a record written *after* a tombstone for the same id
    /// re-adds it, again because the last write in line order wins.
    #[test]
    fn record_after_tombstone_re_adds_it() {
        let fs = MemFs::default();
        let a_del = FileCacheStore::<MemFs>::tombstone_line("doi:A").unwrap();
        let a_rec = FileCacheStore::<MemFs>::entry_line("doi:A", &rec(1000)).unwrap();
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{a_del}{a_rec}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        block_on(store.flush()).unwrap();
        assert!(
            block_on(store.get("doi:A")).unwrap().is_some(),
            "record after tombstone re-adds it"
        );
    }

    /// Bug #1 end-to-end through the public API: `put(id); remove(id); flush()`
    /// must leave the id gone, on disk and across a reopen — the tombstone is the
    /// last write for that id, so it wins.
    #[test]
    fn put_then_remove_then_flush_actually_removes() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.remove("doi:1")).unwrap();
        block_on(store.flush()).unwrap();

        assert!(
            block_on(store.get("doi:1")).unwrap().is_none(),
            "remove after put must actually remove"
        );
        assert!(block_on(store.entries()).unwrap().is_empty());

        // ...and it stays gone across a reopen over the same on-disk bytes.
        let files = store.fs.files.borrow().clone();
        let fs2 = MemFs {
            files: RefCell::new(files),
            ..Default::default()
        };
        let store2 = block_on(FileCacheStore::open(fs2, "cache", "citations", "w2")).unwrap();
        assert!(block_on(store2.get("doi:1")).unwrap().is_none());
    }

    /// Bug #2: a re-fetch that produces a *smaller* `expires` (wall clock stepped
    /// back, or a reduced default TTL) wins because it is the newer write — both
    /// within a single sidecar and when it lands in a sidecar over a larger
    /// committed main-file copy. The old max-`expires` rule kept the stale larger
    /// value and pinned the entry `Expired` forever.
    #[test]
    fn refetch_with_smaller_expires_wins() {
        let larger = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(5000)).unwrap();
        let smaller = FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap();

        // (a) within one sidecar: larger written first, smaller (the re-fetch) last.
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &alloc::format!("{larger}{smaller}"),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        assert_eq!(
            block_on(store.get("doi:1")).unwrap().unwrap().expires,
            Timestamp::from_millis(1000),
            "smaller re-fetch within one sidecar wins"
        );

        // (b) sidecar over main file: main committed the larger copy, a fresh
        // sidecar holds the smaller re-fetch.
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!("{HEADER}\n{larger}"),
        );
        write_raw(&fs, "cache/citations.w1.log", &smaller);
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        assert_eq!(
            block_on(store.get("doi:1")).unwrap().unwrap().expires,
            Timestamp::from_millis(1000),
            "smaller re-fetch in a sidecar beats the larger committed copy"
        );
    }

    /// The documented cross-writer limitation: two writers that each commit a
    /// different value for the same id are ordered only by sidecar **name**, so
    /// the higher-named writer wins regardless of `expires` magnitude. Pinned so
    /// the tiebreak stays deterministic.
    #[test]
    fn cross_writer_conflict_resolved_by_sidecar_name_order() {
        let fs = MemFs::default();
        // w1 writes the *larger* expiry, w2 the smaller — name order, not
        // magnitude, must decide, so w2 (folded last) wins.
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(9000)).unwrap(),
        );
        write_raw(
            &fs,
            "cache/citations.w2.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w3")).unwrap();
        assert_eq!(
            block_on(store.get("doi:1")).unwrap().unwrap().expires,
            Timestamp::from_millis(1000),
            "later-sorted sidecar name wins, independent of expires"
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

    /// Peers' logs are folded into the committed file but **left alone**: they
    /// belong to writers that may be appending to them right now, without any
    /// lock. Only this store's own sidecar is reaped.
    #[test]
    fn peer_sidecars_are_folded_but_only_our_own_is_deleted() {
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
        block_on(store.put("doi:3", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        assert!(files.contains_key("cache/citations.jsonl"));
        assert!(
            files.contains_key("cache/citations.w1.log"),
            "a peer's sidecar must survive our compaction"
        );
        assert!(
            files.contains_key("cache/citations.w2.log"),
            "a peer's sidecar must survive our compaction"
        );
        assert!(
            !files.contains_key("cache/citations.w3.log"),
            "our own sidecar is reaped"
        );
        drop(files);
        assert_eq!(block_on(store.entries()).unwrap().len(), 3);
    }

    /// Re-folding a peer's log is idempotent, so the peer's writes survive an
    /// unbounded number of other writers' compactions.
    #[test]
    fn refolding_a_peer_sidecar_is_idempotent() {
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.peer.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        for _ in 0..3 {
            block_on(store.flush()).unwrap();
        }
        assert_eq!(block_on(store.entries()).unwrap().len(), 1);
        assert!(block_on(store.get("doi:1")).unwrap().is_some());
    }

    /// A failed unlink must not turn a durably persisted compaction into an
    /// error, nor skip the in-memory refresh that follows it.
    #[test]
    fn a_failing_sidecar_unlink_does_not_fail_the_flush() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        store.fs.remove_fails.set(true);

        block_on(store.flush()).expect("a failed unlink is not a failed flush");
        assert!(
            store
                .fs
                .files
                .borrow()
                .contains_key("cache/citations.jsonl")
        );
        assert_eq!(block_on(store.entries()).unwrap().len(), 1);
    }

    // --- main-file parsing -------------------------------------------------

    #[test]
    fn header_constant_matches_schema_version() {
        let parsed: HeaderLine = serde_json::from_str(HEADER).unwrap();
        assert_eq!(parsed.schema, SCHEMA_VERSION);
    }

    /// A main file with no header at all must keep *every* entry — the old
    /// unconditional "line 0 is the header" skip silently ate the first one and
    /// the next compaction made that permanent.
    #[test]
    fn headerless_main_file_keeps_its_first_entry() {
        let fs = MemFs::default();
        let a = FileCacheStore::<MemFs>::entry_line("doi:A", &rec(1000)).unwrap();
        let b = FileCacheStore::<MemFs>::entry_line("doi:B", &rec(2000)).unwrap();
        write_raw(&fs, "cache/citations.jsonl", &alloc::format!("{a}{b}"));

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        assert!(
            block_on(store.get("doi:A")).unwrap().is_some(),
            "first entry"
        );
        assert!(block_on(store.get("doi:B")).unwrap().is_some());
        assert_eq!(block_on(store.entries()).unwrap().len(), 2);

        // ...and compaction re-emits it with a header, still complete.
        block_on(store.flush()).unwrap();
        let files = store.fs.files.borrow();
        let main = core::str::from_utf8(&files["cache/citations.jsonl"]).unwrap();
        assert!(main.starts_with(HEADER));
        assert_eq!(main.lines().count(), 3);
    }

    /// A header line anywhere (e.g. after a careless hand-merge of two copies)
    /// is ignored rather than treated as a broken entry.
    #[test]
    fn mid_file_header_line_is_ignored() {
        let fs = MemFs::default();
        let a = FileCacheStore::<MemFs>::entry_line("doi:A", &rec(1000)).unwrap();
        let b = FileCacheStore::<MemFs>::entry_line("doi:B", &rec(2000)).unwrap();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!("{HEADER}\n{a}{HEADER}\n{b}"),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        assert_eq!(block_on(store.entries()).unwrap().len(), 2);
    }

    #[test]
    fn empty_main_file_loads_zero_entries() {
        let fs = MemFs::default();
        write_raw(&fs, "cache/citations.jsonl", "");
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        assert!(block_on(store.entries()).unwrap().is_empty());
        block_on(store.flush()).expect("an empty cache compacts fine");
    }

    /// A newer build's cache must not be silently discarded: refuse to read it
    /// (and therefore refuse to overwrite it) instead.
    #[test]
    fn unknown_schema_version_refuses_to_open() {
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            "{\"schema\":9}\n{\"id\":\"doi:A\",\"future_shape\":true}\n",
        );
        let err = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap_err();
        assert!(
            err.0.contains("schema version 9"),
            "unexpected error: {}",
            err.0
        );
    }

    /// The same refusal applies to a store that is already open: `flush` must
    /// not overwrite a main file it can no longer read.
    #[test]
    fn unknown_schema_version_refuses_to_flush() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:A", rec(1000))).unwrap();
        // A newer build compacted the file underneath us.
        write_raw(&store.fs, "cache/citations.jsonl", "{\"schema\":2}\n");

        let err = block_on(store.flush()).unwrap_err();
        assert!(
            err.0.contains("schema version 2"),
            "unexpected error: {}",
            err.0
        );
        // The newer file is untouched.
        let files = store.fs.files.borrow();
        assert_eq!(
            core::str::from_utf8(&files["cache/citations.jsonl"]).unwrap(),
            "{\"schema\":2}\n"
        );
    }

    /// Torn-line tolerance is correct for a sidecar and destructive for the
    /// main file, which can only ever appear via an atomic rename.
    #[test]
    fn corrupt_line_in_main_file_is_not_silently_dropped() {
        let fs = MemFs::default();
        let good = FileCacheStore::<MemFs>::entry_line("doi:A", &rec(1000)).unwrap();
        let torn = r#"{"id":"doi:B","rec":{"payload":{"conc"#;
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!("{HEADER}\n{good}{torn}"),
        );

        let err = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap_err();
        assert!(
            err.0.contains("line 3") && err.0.contains("neither a header nor an entry"),
            "unexpected error: {}",
            err.0
        );
    }

    /// A sidecar (the newer write) always wins over the committed main file for
    /// the same id — whether its `expires` is *later* or *earlier* than the
    /// committed copy. Recency decides, not the max-`expires` rule of the old
    /// code; the earlier-expiry half is the fix for bug #2.
    #[test]
    fn recency_decides_between_main_and_sidecar() {
        // (a) sidecar's expiry is later -> sidecar wins.
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!(
                "{HEADER}\n{}",
                FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap()
            ),
        );
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(5000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        assert_eq!(
            block_on(store.get("doi:1")).unwrap().unwrap().expires,
            Timestamp::from_millis(5000)
        );

        // (b) sidecar's expiry is *earlier* -> the sidecar STILL wins, because
        // it is the newer write. Under the old max-`expires` rule the committed
        // record survived and the entry stayed stale forever.
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!(
                "{HEADER}\n{}",
                FileCacheStore::<MemFs>::entry_line("doi:1", &rec(5000)).unwrap()
            ),
        );
        write_raw(
            &fs,
            "cache/citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w2")).unwrap();
        assert_eq!(
            block_on(store.get("doi:1")).unwrap().unwrap().expires,
            Timestamp::from_millis(1000)
        );
    }

    /// Emptying the cache legitimately (a prune that removed everything) must
    /// still compact, i.e. the "never compact to nothing" guard keys off
    /// tombstones and not off emptiness alone.
    #[test]
    fn pruning_everything_still_compacts() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();
        block_on(store.remove("doi:1")).unwrap();
        block_on(store.flush()).expect("prune-to-empty is legitimate");

        assert!(block_on(store.entries()).unwrap().is_empty());
        let files = store.fs.files.borrow();
        assert_eq!(
            core::str::from_utf8(&files["cache/citations.jsonl"]).unwrap(),
            alloc::format!("{HEADER}\n")
        );
    }

    // --- sidecar name matching ---------------------------------------------

    #[test]
    fn is_sidecar_matches_only_writer_logs() {
        // Real sidecars, whatever the caller-supplied writer id looks like.
        assert!(is_sidecar("citations.w1.log", "citations"));
        assert!(is_sidecar("citations.4711-1234567890.log", "citations"));
        // No writer segment at all.
        assert!(!is_sidecar("citations.log", "citations"));
        // The committed file and the lockfile are never sidecars.
        assert!(!is_sidecar("citations.jsonl", "citations"));
        assert!(!is_sidecar("citations.lock", "citations"));
        // Another base entirely, and a stray temp file.
        assert!(!is_sidecar("unrelated.log", "citations"));
        assert!(!is_sidecar(".tmpAb12Cd", "citations"));
        assert!(!is_sidecar("citationsX.w1.log", "citations"));
        // Indistinguishable from writer id "jsonl" — accepted on purpose (see
        // `is_sidecar`'s docs); folding is read-only and nothing unlinks it.
        assert!(is_sidecar("citations.jsonl.log", "citations"));
    }

    /// A file the store never created is folded (harmlessly) but must never be
    /// unlinked by a compaction.
    #[test]
    fn foreign_log_files_are_never_deleted() {
        let fs = MemFs::default();
        write_raw(&fs, "cache/citations.import-notes.log", "not json at all\n");
        write_raw(&fs, "cache/citations.log", "nor is this\n");
        write_raw(&fs, "cache/unrelated.log", "nor this\n");

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        for name in [
            "cache/citations.import-notes.log",
            "cache/citations.log",
            "cache/unrelated.log",
        ] {
            assert!(files.contains_key(name), "{name} must survive a compaction");
        }
    }
}
