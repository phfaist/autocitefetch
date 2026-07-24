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
//! * `._{base}.{writer_id}.log` — a **sidecar** append log, one per writer. Each
//!   line is either an entry or a tombstone (`{"id":…,"del":true}`). Writes go
//!   here lock-free; there is no header. These are throwaway and should be
//!   *gitignored* by users, as should the lockfile.
//! * `._{base}.lock` — the compaction lockfile. The **only** thing that ever
//!   takes the lock is [`FileCacheStore::flush`]; likewise gitignore it.
//!
//! Every one of those throwaway files is **transient**: each is unlinked by the
//! writer that owns it as soon as it has served its purpose (see "Nothing is
//! left lying around" below), so a completed run leaves `{base}.jsonl` alone in
//! the directory. They are still worth gitignoring — they exist for as long as a
//! run does, and a crash can leave one behind.
//!
//! The bare `{base}` name belongs to the committable file alone: **every other
//! file the store creates carries the `._{base}` prefix** (`temp_base`) — the
//! sidecars, the compaction lockfile, the per-writer liveness companions, and
//! (in the std host) the staging file the atomic replace renames from. Two
//! reasons: the `._` makes them hidden, and it makes a single ignore rule
//! `._{base}*` cover the whole throwaway family without also swallowing
//! `{base}.jsonl`, which is exactly the file the user wants committed.
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
//! # Ephemeral (TTL-0) records are memory-only
//!
//! A record with no fresh window (`stale_after == expires`, what a zero TTL
//! produces — see `is_ephemeral`) is *ephemeral*: it lives only for the
//! current run. `put` keeps it in the in-memory map so a same-run `get` still
//! serves it, but never appends it to a sidecar; `flush` never writes it to
//! `citations.jsonl` (and carries the in-memory copies forward across its own
//! disk reload); and an ephemeral record read back from an older on-disk file
//! is dropped, not resurrected. The net effect: the `manual` source's citation
//! text (the canonical TTL-0 case) is usable within a run but never lands in
//! the committable file and is gone on the next `open`. This is a persistence
//! policy, keyed on the record's timestamps — nothing here knows about the
//! `manual` prefix.
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
//! # Nothing is left lying around
//!
//! Every throwaway file has an owner that unlinks it, so a run that ends cleanly
//! leaves only `{base}.jsonl`:
//!
//! * the **sidecar** is unlinked by its own writer at every `flush`, once its
//!   lines are folded into the main file;
//! * the **liveness companion** (`._{base}.{writer}.log.lock`) is taken lazily —
//!   immediately *before* the first append creates the sidecar — and unlinked at
//!   the end of the same `flush`, once the sidecar it vouches for is gone. It is
//!   re-taken if the store appends again, so the invariant "our sidecar exists ⇒
//!   we hold our companion" never breaks;
//! * the **compaction lockfile** is unlinked at the end of the `flush` that took
//!   it;
//! * the std host's staging file is consumed by the rename that publishes it.
//!
//! Both lock files are unlinked **while their lock is still held**, never after
//! releasing it. That ordering is what makes the unlink safe: a peer either sees
//! the file and finds the lock held (owner alive — hands off), or does not see
//! the file at all. There is no instant at which the path exists *and* its lock
//! is free while its owner is still using it.
//!
//! A file that was unlinked by its holder can still be locked by a peer that
//! opened it a moment earlier, and that peer would then be holding a lock on a
//! path other processes no longer reach. [`CacheFs::lock_exclusive`] and
//! [`CacheFs::try_lock_exclusive`] are therefore specified to verify, after
//! acquiring, that the file they locked is still the one at the path — see their
//! docs. A host that cannot check (no file identity available) is no worse off
//! than before this store unlinked anything, since the window is bounded by a
//! peer's open→acquire gap.
//!
//! # Which files compaction may delete
//!
//! Its own sidecar and companion always, the compaction lockfile it holds, and a
//! peer's sidecar only once that peer is proven dead. The main file and anything
//! else the user keeps in the directory are only ever **read**.
//!
//! Deleting a *live* peer's sidecar is the data-loss bug this design guards
//! against. The compaction lock serializes compaction against *compaction*; it
//! does **not** serialize compaction against `append`, which takes no lock at
//! all. A live peer's `put` can therefore land after this writer's fold has
//! read that peer's sidecar and before the rewrite completes — an acknowledged,
//! on-disk write that the peer has been promised. Unlinking the sidecar then
//! destroys it (and, since `flush` rebuilds `mem` purely from disk, the peer's
//! own next flush would erase its in-memory copy too). Re-folding a live peer's
//! log instead is idempotent — the fold is a deterministic last-write-wins
//! replay of the main file then every sidecar in a fixed order, so re-applying
//! the same bytes reaches the same map.
//!
//! # Liveness locks — reaping a crashed writer's sidecar safely
//!
//! A sidecar left behind by a *crashed* writer must still be reclaimed: its
//! writer id is never reused, so nobody rewrites it, and its lines are re-folded
//! on every compaction forever. For a plain entry that is only wasted work; for
//! a **tombstone** it is a correctness bug — an orphaned `{"id":…,"del":true}`
//! re-deletes that id on every compaction, so a later re-fetch is committed and
//! then dropped again on the next flush.
//!
//! The distinction between a live owner and a dead one is an OS-advisory
//! **liveness lock**. A writer takes an exclusive [`CacheFs::try_lock_exclusive`]
//! on a companion file `._{base}.{writer_id}.log.lock` sitting next to its
//! sidecar, and holds it for exactly as long as that sidecar exists: it is taken
//! lazily, just before the first `put`/`remove` appends (and so creates) the log,
//! and dropped at the end of the `flush` that unlinks the log again. The lock is
//! held by the process, not written into any file, so the kernel releases it
//! automatically when the process exits or crashes. When `flush` folds a
//! **peer's** sidecar it then tries that peer's companion lock:
//!
//! * lock **held** (`Ok(None)`) ⇒ the owner is alive and may be appending
//!   lock-free right now ⇒ fold read-only, never delete (the rule above);
//! * lock **acquired** (`Ok(Some)`) ⇒ the OS released it on the owner's death
//!   ⇒ the sidecar is abandoned, so — its lines already captured by this same
//!   fold — delete both it and its now-orphaned companion, then drop the guard.
//!
//! The companion lock is a *separate* file from the sidecar precisely so that a
//! writer reaping its **own** sidecar never has to unlink the file its liveness
//! lock lives on while it may still need to append again. A writer therefore
//! never `try_lock`s its own companion (that would self-deadlock); it deletes
//! its own `.log` unconditionally, and only then — in the same `flush` — unlinks
//! and releases its companion, in that order (see "Nothing is left lying
//! around"). The next append takes a fresh one.
//!
//! That leaves one file to collect: a companion whose `.log` is *already* gone.
//! A writer normally unlinks its own in the same `flush` that unlinks its log,
//! but a crash in between — or one after taking the lock and before the first
//! append — strands it. The rule above cannot reach it (that loop walks sidecar
//! logs, and this one has none), so `flush` sweeps them separately, using the
//! same lock as the oracle: held ⇒ a live writer that is about to append, leave
//! it; acquired ⇒ its owner is gone, unlink it.
//!
//! A foreign `._{base}.*.log` that no managed writer ever created has **no**
//! companion lock file, so `flush` finds none to probe and leaves it untouched
//! — a stray or hand-placed log is folded (read-only) but never reaped, the
//! same guarantee as for the main file. A failed unlink is never a failed
//! flush: the data is already durable in the main file and a surviving sidecar
//! is merely re-folded (idempotently) next time.
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
//! `parse_main_into`.

use alloc::boxed::Box;
use alloc::collections::{BTreeMap, BTreeSet};
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
/// [`CacheFs::lock_exclusive`] or [`CacheFs::try_lock_exclusive`]; dropping it
/// releases the lock. The trait is intentionally empty — the core only ever
/// *holds* a guard (for a compaction, or for a store's whole lifetime as a
/// liveness signal) and lets `Drop` do the work.
pub trait CacheGuard {}

/// Host-provided filesystem, injected so the store can run `no_std` (native
/// files, or something else entirely on WASM). All methods are async and
/// object-safe; paths use `/` separators and are built by the store.
pub trait CacheFs {
    /// The held-lock type [`try_lock_exclusive`](CacheFs::try_lock_exclusive)
    /// hands back and that a store keeps alive for its whole lifetime. A
    /// concrete associated type rather than a `Box<dyn CacheGuard>` on purpose:
    /// it lets a [`FileCacheStore`] stay `Send` whenever the host's guard is
    /// (the std store is moved across threads in tests, even though its futures
    /// are `!Send`), which a boxed `dyn CacheGuard` field would forfeit.
    type Guard: CacheGuard;

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

    /// Take an exclusive advisory lock on `path`, creating the file if absent,
    /// and return a held guard. Dropping the guard releases the lock.
    ///
    /// The store **unlinks lock files while holding their lock** (see the
    /// "Nothing is left lying around" section of the module docs), so an
    /// implementation must not hand back a guard on a file that is no longer the
    /// one at `path`: after acquiring, check that the locked file is still the
    /// file `path` names, and if it is not, re-open and re-acquire. Skipping the
    /// check costs mutual exclusion — the stale holder and a newcomer that
    /// created a fresh file at the same path would both believe they hold it.
    /// A host with no way to compare file identity may accept the lock as-is;
    /// the window is a peer's open→acquire gap.
    fn lock_exclusive(&self, path: &str) -> BoxFuture<'_, Result<Box<dyn CacheGuard>, FsError>>;

    /// **Non-blocking** exclusive advisory lock on `path`, creating the file if
    /// absent (like [`lock_exclusive`](CacheFs::lock_exclusive)). Returns
    /// `Ok(Some(guard))` when the lock was free and is now held by the returned
    /// guard, or `Ok(None)` when some other holder already has it. Used as a
    /// liveness probe: a writer holds one of these on its own sidecar's
    /// companion for as long as that sidecar exists, so a peer's `flush` can
    /// reclaim the sidecar only once the lock comes free (i.e. the owner process
    /// is gone).
    ///
    /// The same verify-after-acquire rule as [`lock_exclusive`](CacheFs::lock_exclusive)
    /// applies, and here the conservative answer is `Ok(None)`: a lock taken on a
    /// file that has since been unlinked proves nothing about its owner, and
    /// reporting it as free is what would let a live peer's sidecar be reaped.
    fn try_lock_exclusive(
        &self,
        path: &str,
    ) -> BoxFuture<'_, Result<Option<Self::Guard>, FsError>>;
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

/// Whether a record is *ephemeral* — a TTL-0 entry that must live only in
/// memory for the current run and never touch the committable file.
///
/// Such a record has no fresh window, so its soft and hard expiries coincide
/// (`stale_after == expires`); that is exactly what
/// [`TtlPolicy::make_record`](crate::cache::TtlPolicy::make_record)'s zero-TTL
/// branch produces. A normal record always has `stale_after < expires`, so
/// `>=` is a safe, source-agnostic predicate (it is keyed on the timestamps,
/// not on the `manual` prefix). Ephemeral records are skipped by `put`'s
/// sidecar append, excluded from [`serialize_main`], and dropped when read back
/// off disk — so they are never persisted and vanish on the next `open`.
fn is_ephemeral(rec: &CacheRecord) -> bool {
    rec.stale_after >= rec.expires
}

/// A [`CacheStore`] persisted as one committable JSONL file plus lock-free
/// per-writer sidecar logs, compacted under a lock. Generic over the injected
/// [`CacheFs`]; carries no clock.
pub struct FileCacheStore<Fs: CacheFs> {
    fs: Fs,
    dir: String,
    base: String,
    writer_id: String,
    /// `{dir}/{base}.jsonl` — the one file *not* under [`temp_base`].
    main: String,
    /// `._{base}.{writer_id}.log` — the bare *name* of this writer's append log,
    /// as it appears in a [`CacheFs::list`] listing. Compared against the
    /// folded set before compaction unlinks it.
    sidecar_name: String,
    /// `{dir}/._{base}.{writer_id}.log` — the same file, as a path.
    sidecar: String,
    /// `{dir}/._{base}.{writer_id}.log.lock` — the companion file this writer's
    /// liveness lock is taken on. A *separate* file from the sidecar so that
    /// reaping the sidecar never has to disturb the lock.
    sidecar_lock: String,
    /// `{dir}/._{base}.lock`.
    lockfile: String,
    /// The liveness lock on `sidecar_lock`, held for exactly as long as this
    /// writer's sidecar exists: taken by `ensure_lifelock` just before the first
    /// append creates the log, released (and the file unlinked) by the `flush`
    /// that unlinks the log again. Its being held is the signal a peer's `flush`
    /// reads to decide our sidecar is live and must not be reaped; the OS
    /// releases it if this process crashes. `None` whenever we have no sidecar
    /// on disk — and also if the lock could not be taken at all, in which case
    /// we simply forgo the protection rather than fail the write.
    lifelock: RefCell<Option<Fs::Guard>>,
    /// The authoritative in-memory view; reads clone out of it.
    mem: RefCell<BTreeMap<String, CacheRecord>>,
}

impl<Fs: CacheFs> fmt::Debug for FileCacheStore<Fs> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileCacheStore")
            .field("main", &self.main)
            .field("writer_id", &self.writer_id)
            .field("live", &self.lifelock.borrow().is_some())
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
        // Only the committable file wears the bare base name; every throwaway
        // file in the family hides under `._{base}` so one ignore rule covers
        // them all (see `temp_base`).
        let main = alloc::format!("{dir}/{base}.jsonl");
        let temp_base = temp_base(&base);
        let sidecar_name = alloc::format!("{temp_base}.{writer_id}.log");
        let sidecar = alloc::format!("{dir}/{sidecar_name}");
        // The companion liveness-lock path for this writer's sidecar: a
        // *separate* file from the sidecar, so reaping our own `.log` never has
        // to disturb the lock we take on this one.
        let sidecar_lock = alloc::format!("{sidecar}.lock");
        let lockfile = alloc::format!("{dir}/{temp_base}.lock");

        let merged = load_merged(&fs, &dir, &base).await.map_err(fs_store)?;

        // The liveness lock is deliberately *not* taken here: it vouches for a
        // sidecar, and we have none until something is appended. Taking it at
        // `open` meant a store that only ever reads still created a file — and
        // left it behind, since nothing but a *later* process's orphan sweep
        // could reap it. `ensure_lifelock` takes it just before the first
        // append instead; `flush` gives it back.
        Ok(FileCacheStore {
            fs,
            dir,
            base,
            writer_id,
            main,
            sidecar_name,
            sidecar,
            sidecar_lock,
            lockfile,
            lifelock: RefCell::new(None),
            mem: RefCell::new(merged.map),
        })
    }

    /// Take this writer's liveness lock, if it is not already held. Called
    /// immediately before any append, so that a sidecar on disk always has a
    /// held companion lock vouching for it — that is the whole signal a peer's
    /// `flush` uses to tell "owner alive, hands off" from "crashed, reap it".
    ///
    /// Failing to take it is not an error: the writer id is unique per process
    /// run, so a collision means something unexpected is holding the file, and
    /// the honest response is to forgo *our* crash-detection protection (a peer
    /// then sees a held lock and leaves our sidecar alone — the safe direction)
    /// rather than to fail an otherwise fine write.
    async fn ensure_lifelock(&self) {
        let held = self.lifelock.borrow().is_some();
        if held {
            return;
        }
        if let Ok(guard @ Some(_)) = self.fs.try_lock_exclusive(&self.sidecar_lock).await {
            *self.lifelock.borrow_mut() = guard;
        }
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
        // An *ephemeral* (TTL-0) record lives only for this run: it goes into
        // the in-memory view so `get()` still serves it, but is deliberately
        // **never** appended to a sidecar (nor, therefore, folded into the
        // committable `citations.jsonl`). Otherwise arbitrary manual citation
        // text would land in a git-tracked file and be pinned there by the
        // grace window for a fortnight. See [`is_ephemeral`].
        let ephemeral = is_ephemeral(&record);
        // Serialize the sidecar line first (borrowing `record`), then move the
        // record into the in-memory map — no clone, no RefCell borrow held
        // across the await below. Note the map is updated in this synchronous
        // prologue while the append happens in the returned future: a future
        // that is created and then dropped without being polled leaves `mem`
        // one entry ahead of disk (harmless — the next `flush` re-reads disk
        // and the entry simply reverts). An ephemeral record is *always* "ahead
        // of disk" by design.
        let line = if ephemeral {
            None
        } else {
            Some(Self::entry_line(id, &record))
        };
        self.mem.borrow_mut().insert(id.to_string(), record);
        let sidecar = self.sidecar.clone();
        Box::pin(async move {
            if let Some(line) = line {
                let line = line?;
                // Before the append, never after: the sidecar must not exist on
                // disk for even an instant without a held companion lock, or a
                // peer's flush could read it as a crashed writer's leftover and
                // reap it out from under us.
                self.ensure_lifelock().await;
                self.fs
                    .append(&sidecar, line.as_bytes())
                    .await
                    .map_err(fs_store)?;
            }
            Ok(())
        })
    }

    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        let line = Self::tombstone_line(id);
        self.mem.borrow_mut().remove(id);
        let sidecar = self.sidecar.clone();
        Box::pin(async move {
            let line = line?;
            // Same ordering rule as `put`: the companion lock first, so the log
            // this append may create is never unvouched-for.
            self.ensure_lifelock().await;
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
    /// map, writes it back atomically, reaps this writer's own sidecar plus any
    /// **provably-dead** peer's sidecar (see the liveness-lock section of the
    /// module docs), and refreshes the in-memory view.
    ///
    /// A *live* peer's sidecar is read but never unlinked — the compaction lock
    /// serializes compaction against compaction, never against a lock-free
    /// `append`, so unlinking a live peer's log would destroy an acknowledged
    /// write. The liveness lock is what distinguishes a live peer from a crash.
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

            // Reap the sidecars this fold captured and can prove safe to unlink.
            // A failed unlink is never a failed flush: the data is already
            // durable in the main file and any surviving sidecar is simply
            // re-folded (idempotently) next time.
            let mut own_sidecar_gone = true;
            for name in &merged.folded {
                if name == &self.sidecar_name {
                    // Our own log: reap unconditionally, exactly as before. We
                    // still hold our liveness lock — it lives on the *separate*
                    // `sidecar_lock` companion, never on the log we delete here,
                    // so this unlink can never orphan it, and we must never
                    // `try_lock` our own companion (that would self-deadlock).
                    // Whether it actually went decides if the companion may be
                    // given back below: a log that is still there must keep its
                    // lock held, or a peer would read it as abandoned.
                    own_sidecar_gone = self.fs.remove(&self.sidecar).await.is_ok();
                    continue;
                }
                // A peer's log. Reap it only if its owner is provably gone. A
                // managed sidecar always has a companion `{name}.lock` on disk;
                // a foreign `*.log` no writer ever created has none, so probe
                // first — never create a companion for, and therefore never
                // reap, a file no writer owns.
                let peer_lock = alloc::format!("{}/{name}.lock", self.dir);
                if !matches!(self.fs.read(&peer_lock).await, Ok(Some(_))) {
                    continue;
                }
                // Held (`Ok(None)`) ⇒ owner alive, leave it be. Acquired
                // (`Ok(Some)`) ⇒ the OS released it on the owner's death, so the
                // log is abandoned; its lines were already captured by the fold
                // above, so delete both it and its now-orphaned companion.
                if let Ok(Some(_reaped)) = self.fs.try_lock_exclusive(&peer_lock).await {
                    let peer_log = alloc::format!("{}/{name}", self.dir);
                    let _ = self.fs.remove(&peer_log).await;
                    let _ = self.fs.remove(&peer_lock).await;
                    // `_reaped` drops here, releasing the lock we just took.
                }
            }

            // Reap *orphaned* companion locks — a `._{base}.{writer}.log.lock`
            // whose `.log` is already gone. The loop above cannot: it iterates
            // sidecar logs, and there is no log left to find one from. A writer
            // that exits normally unlinks its own companion below, so what this
            // collects is what a crash strands: between unlinking the log and
            // unlinking the companion, or before the first append ever happened.
            //
            // The lock is again the liveness oracle: held ⇒ a live writer that
            // is between its lock and its append, leave it alone; acquired ⇒ the
            // OS released it on the owner's exit, so nothing owns the file. Our
            // own companion is skipped by name rather than probed — we may hold
            // it, and asking for it again would at best tell us nothing.
            let own_lock_name = alloc::format!("{}.lock", self.sidecar_name);
            for name in &merged.orphan_locks {
                if name == &own_lock_name {
                    continue;
                }
                let path = alloc::format!("{}/{name}", self.dir);
                if let Ok(Some(_reaped)) = self.fs.try_lock_exclusive(&path).await {
                    let _ = self.fs.remove(&path).await;
                }
            }

            // Give our own liveness companion back, now that the sidecar it
            // vouched for is gone: there is nothing left for a peer to reap, so
            // nothing left to vouch for, and holding on would strand the file in
            // the user's directory for the next process to sweep up (one per run
            // — the litter this cleans up). The next append takes a fresh lock.
            //
            // Order matters: unlink *while still holding* the lock, then
            // release. A peer therefore either finds the file and its lock held
            // (owner alive, hands off) or does not find the file at all; the
            // file never sits there unlocked while we are still using it. If our
            // own log did not actually go away, we keep the lock instead —
            // an unvouched-for sidecar reads as a crashed writer's leftover.
            //
            // Only when we really hold it: a companion we never took is not ours
            // to unlink, and this store deletes nothing it does not own.
            let hold_lifelock = self.lifelock.borrow().is_some();
            if own_sidecar_gone && hold_lifelock {
                let _ = self.fs.remove(&self.sidecar_lock).await;
                *self.lifelock.borrow_mut() = None;
            }

            // Carry forward this run's *ephemeral* (TTL-0) records. They were
            // never written to any sidecar (see `put`) nor to the main file
            // (see `serialize_main`), so the disk-only `merged.map` above does
            // not contain them — and overwriting `mem` with it verbatim would
            // forget them mid-run, breaking a same-run `get()` of a `manual:`
            // citation after `retrieve` (which flushes). Re-inserting them keeps
            // them memory-only: still absent from disk, still gone on restart.
            let mut merged_map = merged.map;
            {
                let mem = self.mem.borrow();
                for (id, rec) in mem.iter() {
                    if is_ephemeral(rec) {
                        merged_map.insert(id.clone(), rec.clone());
                    }
                }
            }
            *self.mem.borrow_mut() = merged_map;

            // Finally, unlink the compaction lockfile — the last throwaway file
            // of the run — so a directory whose writers have all finished holds
            // nothing but the committable `{base}.jsonl`. As with the companion
            // above this happens *while the lock is still held* (`_guard` drops
            // just below): a peer either blocks on the file it found or creates
            // a fresh one, never both. `CacheFs::lock_exclusive`'s contract asks
            // implementations to notice they locked an unlinked file and retry,
            // which is what makes a waiter's turn correct here.
            let _ = self.fs.remove(&self.lockfile).await;
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
            // An ephemeral (TTL-0) record must never have been persisted; a
            // cache written by an older build may still hold one, so drop it on
            // read rather than resurrect it. It is deliberately *not* counted
            // in `entries`: dropping it is a legitimate reason for the map to
            // shrink, and counting it would trip the "refusing to compact to
            // nothing" guard in `flush`.
            if is_ephemeral(&entry.rec) {
                continue;
            }
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
            // Drop an ephemeral (TTL-0) record read off disk rather than
            // resurrect it — a stale sidecar from an older build could carry
            // one. New writes never put one here (see `put`).
            if !is_ephemeral(&rec) {
                map.insert(line.id, rec);
            }
        }
    }
    saw_tombstone
}

/// The prefix every file in the family *except* the committable `{base}.jsonl`
/// carries: `._{base}`.
///
/// The leading `._` is what keeps the throwaway files (sidecars, the compaction
/// lockfile, the liveness companions, the std host's staging file) hidden and,
/// more importantly, ignorable as a group: `._{base}*` matches all of them and
/// none of `{base}.jsonl`. Everything that builds or matches one of those names
/// goes through here so the convention lives in one place.
fn temp_base(base: &str) -> String {
    alloc::format!("._{base}")
}

/// Whether `name` is a sidecar log belonging to this store's family:
/// `{temp_base}.{writer}.log` with a **non-empty** writer segment, where
/// `temp_base` is [`temp_base`]`(base)` — i.e. `._{base}`.
///
/// Rejected: the main file (`{base}.jsonl`), the lockfile (`._{base}.lock`), a
/// bare `._{base}.log` with no writer segment at all, and anything under a
/// different base. The main file cannot match by construction: it does not carry
/// the `._` prefix at all.
///
/// The writer segment itself cannot be validated — `writer_id` is
/// caller-supplied (the std host happens to use `<pid>-<nanos>`, but nothing in
/// the core requires that shape), so `._citations.notes.log` is genuinely
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
fn is_sidecar(name: &str, temp_base: &str) -> bool {
    let Some(rest) = name.strip_prefix(temp_base) else {
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

/// The sidecar log a companion liveness lock belongs to, if `name` is one:
/// `._{base}.{writer}.log.lock` → `._{base}.{writer}.log`.
///
/// The remainder must itself be a [`is_sidecar`] name, which is what keeps the
/// **compaction lockfile** out: `._{base}.lock` is one `.lock` suffix away from a
/// companion, and classifying it as one would hand the orphan sweep the very
/// file every writer synchronizes on. `._{base}` has no writer segment, so it is
/// not a sidecar name and the file is not a companion.
fn companion_log_name<'a>(name: &'a str, temp_base: &str) -> Option<&'a str> {
    let log = name.strip_suffix(".lock")?;
    is_sidecar(log, temp_base).then_some(log)
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
    /// Names of companion liveness locks (`._{base}.{writer}.log.lock`) whose
    /// `.log` is *not* in this listing. A writer keeps its companion for its
    /// whole life but reaps its own `.log` at every flush, so this is either a
    /// live writer between flushes (its lock is still held) or a writer that has
    /// exited (nothing else will ever reap it — the peer-reaping loop is driven
    /// by `.log` names, and that log is gone). `flush` tells the two apart with
    /// the lock itself and deletes the second kind.
    orphan_locks: Vec<String>,
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

    // Everything but the main file lives under `._{base}`; computed once rather
    // than per name, since the listing is filtered three times below.
    let temp_base = temp_base(base);

    // Which sidecars exist at all, so a companion lock can be told apart from an
    // *orphaned* one. Collected up front because the listing is walked once and
    // a companion may sort before or after the log it belongs to.
    let sidecars: BTreeSet<&str> = names
        .iter()
        .map(String::as_str)
        .filter(|n| is_sidecar(n, &temp_base))
        .collect();
    let orphan_locks: Vec<String> = names
        .iter()
        .filter(|n| companion_log_name(n, &temp_base).is_some_and(|log| !sidecars.contains(log)))
        .cloned()
        .collect();

    for name in &names {
        if !is_sidecar(name, &temp_base) {
            continue;
        }
        let path = alloc::format!("{dir}/{name}");
        match fs.read(&path).await {
            Ok(Some(bytes)) => saw_tombstone |= fold_sidecar_into(&bytes, &mut map),
            Ok(None) => {}
            Err(_) => continue,
        }
        folded.push(name.clone());
    }

    Ok(Merged {
        map,
        folded,
        orphan_locks,
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
        // Ephemeral (TTL-0) records are memory-only and must never be
        // committed. Defence in depth: the fold already drops them and `put`
        // never appends one, so `map` should not contain one here — but a
        // record that *became* ephemeral, or slipped in before this policy,
        // still must not reach `citations.jsonl`.
        if is_ephemeral(rec) {
            continue;
        }
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

    use alloc::collections::BTreeSet;
    use alloc::rc::Rc;

    #[derive(Default)]
    struct MemFs {
        files: RefCell<BTreeMap<String, Vec<u8>>>,
        /// Paths whose advisory lock is currently *held*. `try_lock_exclusive`
        /// returns `None` for a path already in here and otherwise inserts it,
        /// so the two-writer reap tests are meaningful rather than vacuous. The
        /// `Rc` lets a held-lock guard share the set and drop-release its path.
        locks: Rc<RefCell<BTreeSet<String>>>,
        /// When set, every `remove` fails — used to prove a failed sidecar
        /// unlink does not fail an otherwise successful compaction.
        remove_fails: core::cell::Cell<bool>,
    }

    struct NoopGuard;
    impl CacheGuard for NoopGuard {}

    /// A held liveness lock on the `MemFs`: releases its path on drop, exactly
    /// as an OS advisory lock is released when the holding process exits.
    struct MemGuard {
        locks: Rc<RefCell<BTreeSet<String>>>,
        path: String,
    }
    impl CacheGuard for MemGuard {}
    impl Drop for MemGuard {
        fn drop(&mut self) {
            self.locks.borrow_mut().remove(&self.path);
        }
    }

    impl CacheFs for MemFs {
        type Guard = MemGuard;
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
        fn try_lock_exclusive(
            &self,
            path: &str,
        ) -> BoxFuture<'_, Result<Option<MemGuard>, FsError>> {
            // Create the lock file if absent, mirroring the std impl's
            // `create(true)` — so an existence probe over a real companion
            // behaves the same on the mock.
            self.files
                .borrow_mut()
                .entry(path.to_string())
                .or_default();
            // `insert` returns false when the path is already held.
            let acquired = self.locks.borrow_mut().insert(path.to_string());
            let out = if acquired {
                Ok(Some(MemGuard {
                    locks: self.locks.clone(),
                    path: path.to_string(),
                }))
            } else {
                Ok(None)
            };
            Box::pin(async move { out })
        }
    }

    fn rec(expires_ms: i64) -> CacheRecord {
        CacheRecord {
            payload: Payload::Concrete(serde_json::json!({"id": "x", "title": "t"})),
            stale_after: Timestamp::from_millis(expires_ms / 2),
            expires: Timestamp::from_millis(expires_ms),
        }
    }

    /// An *ephemeral* (TTL-0) record: no fresh window, so `stale_after ==
    /// expires` — exactly what `TtlPolicy::make_record`'s zero-TTL branch
    /// produces for a `manual:` citation.
    fn ephemeral_rec(now_ms: i64) -> CacheRecord {
        CacheRecord {
            payload: Payload::Concrete(
                serde_json::json!({"_ready_formatted": {"flm": "Bohr (1913)"}}),
            ),
            stale_after: Timestamp::from_millis(now_ms),
            expires: Timestamp::from_millis(now_ms),
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
                .contains_key("cache/._citations.w1.log")
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

    /// A TTL-0 (ephemeral) record is usable within the run — `get()` returns it
    /// both before and after a `flush()` — but is NEVER appended to a sidecar,
    /// never written to the committed file, and gone on reopen. A normal record
    /// put alongside it is persisted and survives the reopen (review item #7).
    #[test]
    fn ephemeral_records_are_memory_only() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();

        block_on(store.put("doi:keep", rec(1000))).unwrap();
        block_on(store.put("manual:Bohr (1913)", ephemeral_rec(0))).unwrap();

        // Both readable this run, before the flush.
        assert!(block_on(store.get("doi:keep")).unwrap().is_some());
        assert!(block_on(store.get("manual:Bohr (1913)")).unwrap().is_some());

        block_on(store.flush()).unwrap();

        // ...and the ephemeral one still readable after the flush (its in-memory
        // copy is carried across the disk reload).
        assert!(
            block_on(store.get("manual:Bohr (1913)")).unwrap().is_some(),
            "an ephemeral record must survive a flush within the same run"
        );
        assert!(block_on(store.get("doi:keep")).unwrap().is_some());

        // The committed file holds the normal id but not the ephemeral text,
        // and no sidecar retains it either.
        {
            let files = store.fs.files.borrow();
            let main = core::str::from_utf8(&files["cache/citations.jsonl"]).unwrap();
            assert!(main.contains("doi:keep"), "normal record is committed: {main}");
            assert!(
                !main.contains("Bohr"),
                "ephemeral text must never reach the committed file: {main}"
            );
            for (path, bytes) in files.iter() {
                if path.ends_with(".log") {
                    let text = core::str::from_utf8(bytes).unwrap();
                    assert!(
                        !text.contains("Bohr"),
                        "ephemeral text leaked into sidecar {path}: {text}"
                    );
                }
            }
        }

        // Reopen over the same on-disk bytes: normal survives, ephemeral is gone.
        let disk = store.fs.files.borrow().clone();
        let fs2 = MemFs {
            files: RefCell::new(disk),
            ..Default::default()
        };
        let store2 = block_on(FileCacheStore::open(fs2, "cache", "citations", "w2")).unwrap();
        assert!(
            block_on(store2.get("doi:keep")).unwrap().is_some(),
            "the normal record survives a reopen"
        );
        assert!(
            block_on(store2.get("manual:Bohr (1913)")).unwrap().is_none(),
            "the ephemeral record must be gone after a reopen"
        );
    }

    /// An ephemeral record left behind in a pre-existing on-disk file by an
    /// older build — in the main file *or* a sidecar — is dropped on read, not
    /// resurrected, and does not survive compaction.
    #[test]
    fn pre_existing_on_disk_ephemeral_records_are_dropped() {
        let fs = MemFs::default();
        let keep = FileCacheStore::<MemFs>::entry_line("doi:keep", &rec(1000)).unwrap();
        let eph_main =
            FileCacheStore::<MemFs>::entry_line("manual:old-main", &ephemeral_rec(0)).unwrap();
        write_raw(
            &fs,
            "cache/citations.jsonl",
            &alloc::format!("{HEADER}\n{keep}{eph_main}"),
        );
        let eph_side =
            FileCacheStore::<MemFs>::entry_line("manual:old-side", &ephemeral_rec(0)).unwrap();
        write_raw(&fs, "cache/._citations.peer.log", &eph_side);

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        assert!(block_on(store.get("doi:keep")).unwrap().is_some());
        assert!(
            block_on(store.get("manual:old-main")).unwrap().is_none(),
            "a main-file ephemeral record must be dropped on load"
        );
        assert!(
            block_on(store.get("manual:old-side")).unwrap().is_none(),
            "a sidecar ephemeral record must be dropped on load"
        );

        block_on(store.flush()).unwrap();
        let files = store.fs.files.borrow();
        let main = core::str::from_utf8(&files["cache/citations.jsonl"]).unwrap();
        assert!(main.contains("doi:keep"));
        assert!(
            !main.contains("old-main") && !main.contains("old-side"),
            "compaction must not re-commit the dropped ephemeral records: {main}"
        );
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
            "cache/._citations.w1.log",
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
            "cache/._citations.w1.log",
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
            "cache/._citations.w1.log",
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
            "cache/._citations.w1.log",
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
        write_raw(&fs, "cache/._citations.w1.log", &smaller);
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
            "cache/._citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(9000)).unwrap(),
        );
        write_raw(
            &fs,
            "cache/._citations.w2.log",
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
            "cache/._citations.w1.log",
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
            "cache/._citations.w1.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        write_raw(
            &fs,
            "cache/._citations.w2.log",
            &FileCacheStore::<MemFs>::entry_line("doi:2", &rec(1000)).unwrap(),
        );

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w3")).unwrap();
        block_on(store.put("doi:3", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        assert!(files.contains_key("cache/citations.jsonl"));
        assert!(
            files.contains_key("cache/._citations.w1.log"),
            "a peer's sidecar must survive our compaction"
        );
        assert!(
            files.contains_key("cache/._citations.w2.log"),
            "a peer's sidecar must survive our compaction"
        );
        assert!(
            !files.contains_key("cache/._citations.w3.log"),
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
            "cache/._citations.peer.log",
            &FileCacheStore::<MemFs>::entry_line("doi:1", &rec(1000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        for _ in 0..3 {
            block_on(store.flush()).unwrap();
        }
        assert_eq!(block_on(store.entries()).unwrap().len(), 1);
        assert!(block_on(store.get("doi:1")).unwrap().is_some());
    }

    /// The liveness-lock reap branch. A crashed peer — sidecar **and** a
    /// companion `.log.lock` on disk, but nothing holding the lock — is folded
    /// and then unlinked together with its orphaned companion; a live peer —
    /// companion lock **held**, as if its owner process were alive — is folded
    /// read-only and left in place. Exercises both `try_lock_exclusive` arms in
    /// `flush` (`Ok(Some)` reaps, `Ok(None)` leaves alone).
    #[test]
    fn crashed_peer_is_reaped_but_live_peer_is_not() {
        let fs = MemFs::default();
        // A crashed writer: sidecar + companion lock both exist, lock unheld.
        write_raw(
            &fs,
            "cache/._citations.dead.log",
            &FileCacheStore::<MemFs>::entry_line("doi:dead", &rec(1000)).unwrap(),
        );
        write_raw(&fs, "cache/._citations.dead.log.lock", "");
        // A live writer: sidecar + companion both exist, and the companion is
        // held (as its owner's lifelock would hold it for the store's life).
        write_raw(
            &fs,
            "cache/._citations.alive.log",
            &FileCacheStore::<MemFs>::entry_line("doi:alive", &rec(1000)).unwrap(),
        );
        write_raw(&fs, "cache/._citations.alive.log.lock", "");
        fs.locks
            .borrow_mut()
            .insert("cache/._citations.alive.log.lock".to_string());

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.flush()).unwrap();

        // Both writers' entries were folded into the committed file.
        assert!(block_on(store.get("doi:dead")).unwrap().is_some());
        assert!(block_on(store.get("doi:alive")).unwrap().is_some());

        let files = store.fs.files.borrow();
        assert!(
            !files.contains_key("cache/._citations.dead.log"),
            "a crashed peer's sidecar must be reaped"
        );
        assert!(
            !files.contains_key("cache/._citations.dead.log.lock"),
            "the crashed peer's orphaned companion lock must be reaped too"
        );
        assert!(
            files.contains_key("cache/._citations.alive.log"),
            "a live peer's sidecar must never be reaped"
        );
        assert!(
            files.contains_key("cache/._citations.alive.log.lock"),
            "a live peer's companion lock must never be reaped"
        );
    }

    /// A companion lock whose `.log` is already gone — what a writer that
    /// crashed between unlinking its log and unlinking its companion (or before
    /// its first append) strands. Nothing else can reap it (the peer loop is
    /// driven by `.log` names), so without this sweep it would sit there
    /// forever. A live writer between its lock and its append looks exactly the
    /// same on disk, so the lock still decides.
    #[test]
    fn orphaned_companion_locks_are_reaped_only_when_unheld() {
        let fs = MemFs::default();
        // An exited writer: companion only, unheld.
        write_raw(&fs, "cache/._citations.gone.log.lock", "");
        // A live writer that has taken its lock and not appended yet: companion
        // only, but still held.
        write_raw(&fs, "cache/._citations.busy.log.lock", "");
        fs.locks
            .borrow_mut()
            .insert("cache/._citations.busy.log.lock".to_string());

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        assert!(
            !files.contains_key("cache/._citations.gone.log.lock"),
            "an unheld orphaned companion must be reaped"
        );
        assert!(
            files.contains_key("cache/._citations.busy.log.lock"),
            "a held companion means its owner is alive — never reap it"
        );
    }

    /// The whole point of the exercise: a store that opens, writes and flushes
    /// leaves the committable file **alone** in the directory. Every throwaway
    /// file — our sidecar, our liveness companion, the compaction lockfile — is
    /// unlinked by the writer that created it.
    #[test]
    fn a_finished_run_leaves_only_the_committable_file() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        let names: Vec<&str> = files.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            ["cache/citations.jsonl"],
            "a finished run must leave nothing but the committable file"
        );
    }

    /// A store that only ever *reads* must not create files at all — the
    /// liveness companion is taken lazily, immediately before the first append,
    /// because it exists solely to vouch for a sidecar.
    #[test]
    fn a_read_only_store_creates_no_files() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        assert!(block_on(store.get("doi:1")).unwrap().is_none());
        assert!(
            store.fs.files.borrow().is_empty(),
            "opening and reading must not create a single file: {:?}",
            store.fs.files.borrow().keys().collect::<Vec<_>>()
        );
    }

    /// After a flush the companion is released and gone, but the store is still
    /// usable: a later append must take a **fresh** lock, so the invariant "a
    /// sidecar on disk always has a held companion" holds for its whole life.
    #[test]
    fn a_later_append_retakes_the_liveness_lock() {
        let fs = MemFs::default();
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();
        assert!(store.lifelock.borrow().is_none(), "given back at flush");

        block_on(store.put("doi:2", rec(2000))).unwrap();
        assert!(
            store.lifelock.borrow().is_some(),
            "a new sidecar must come with a held companion lock"
        );
        let files = store.fs.files.borrow();
        assert!(files.contains_key("cache/._citations.w1.log"));
        assert!(files.contains_key("cache/._citations.w1.log.lock"));
        drop(files);

        block_on(store.flush()).unwrap();
        assert_eq!(block_on(store.entries()).unwrap().len(), 2);
    }

    /// The compaction lockfile is `._{base}.lock`, one `.lock` suffix away from a
    /// companion's `._{base}.{writer}.log.lock`. It must never be classified as
    /// one: the orphan sweep would then unlink the file every writer
    /// synchronizes on, at a moment its holder had not chosen.
    #[test]
    fn the_compaction_lockfile_is_not_mistaken_for_an_orphaned_companion() {
        let tb = temp_base("citations");
        assert_eq!(companion_log_name("._citations.lock", &tb), None);
        assert_eq!(
            companion_log_name("._citations.w1.log.lock", &tb),
            Some("._citations.w1.log")
        );
        // Not a companion either: no writer segment, a foreign base, a bare log.
        assert_eq!(companion_log_name("._citations.log.lock", &tb), None);
        assert_eq!(companion_log_name("._other.w1.log.lock", &tb), None);
        assert_eq!(companion_log_name("._citations.w1.log", &tb), None);
    }

    /// A foreign `._citations.*.log` with **no** companion lock file is folded but
    /// never reaped — the reap path must not create a companion for, and then
    /// delete, a file no writer ever managed.
    #[test]
    fn peer_log_without_a_companion_lock_is_never_reaped() {
        let fs = MemFs::default();
        write_raw(
            &fs,
            "cache/._citations.orphan.log",
            &FileCacheStore::<MemFs>::entry_line("doi:x", &rec(1000)).unwrap(),
        );
        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        assert!(
            files.contains_key("cache/._citations.orphan.log"),
            "a companion-less peer log must be left untouched"
        );
        assert!(
            !files.contains_key("cache/._citations.orphan.log.lock"),
            "the reap probe must not create a companion lock for it"
        );
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
            "cache/._citations.w1.log",
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
            "cache/._citations.w1.log",
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
        let tb = temp_base("citations");
        assert_eq!(tb, "._citations");
        // Real sidecars, whatever the caller-supplied writer id looks like.
        assert!(is_sidecar("._citations.w1.log", &tb));
        assert!(is_sidecar("._citations.4711-1234567890.log", &tb));
        // No writer segment at all.
        assert!(!is_sidecar("._citations.log", &tb));
        // The committed file and the lockfile are never sidecars.
        assert!(!is_sidecar("citations.jsonl", &tb));
        assert!(!is_sidecar("._citations.lock", &tb));
        // The `._` prefix is required: a log under the *bare* base is a foreign
        // file, not one of ours (it is also what an older layout wrote).
        assert!(!is_sidecar("citations.w1.log", &tb));
        // Another base entirely, and a stray temp file.
        assert!(!is_sidecar("unrelated.log", &tb));
        assert!(!is_sidecar(".tmpAb12Cd", &tb));
        assert!(!is_sidecar("._citationsX.w1.log", &tb));
        // Indistinguishable from writer id "jsonl" — accepted on purpose (see
        // `is_sidecar`'s docs); folding is read-only and nothing unlinks it.
        assert!(is_sidecar("._citations.jsonl.log", &tb));
    }

    /// A file the store never created is folded (harmlessly) but must never be
    /// unlinked by a compaction — whether it matches the sidecar pattern
    /// (`._citations.import-notes.log`) or, now that the throwaway family hides
    /// under `._`, merely looks like it used to (`citations.w1.log`).
    #[test]
    fn foreign_log_files_are_never_deleted() {
        let fs = MemFs::default();
        write_raw(&fs, "cache/._citations.import-notes.log", "not json at all\n");
        write_raw(&fs, "cache/._citations.log", "nor is this\n");
        write_raw(&fs, "cache/citations.w1.log", "nor this\n");
        write_raw(&fs, "cache/unrelated.log", "nor this either\n");

        let store = block_on(FileCacheStore::open(fs, "cache", "citations", "w1")).unwrap();
        block_on(store.put("doi:1", rec(1000))).unwrap();
        block_on(store.flush()).unwrap();

        let files = store.fs.files.borrow();
        for name in [
            "cache/._citations.import-notes.log",
            "cache/._citations.log",
            "cache/citations.w1.log",
            "cache/unrelated.log",
        ] {
            assert!(files.contains_key(name), "{name} must survive a compaction");
        }
    }
}
