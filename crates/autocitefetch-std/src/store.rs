//! A `std`-backed [`CacheFs`] and the [`SingleFileCacheStore`] convenience
//! store built on top of it.
//!
//! The cache is a single committable `citations.jsonl` file plus lock-free
//! per-writer append logs (`._citations.<writer>.log`) and a compaction lockfile
//! (`._citations.lock`), all living in a user-chosen directory. Only
//! `citations.jsonl` is worth committing to version control — and since every
//! other file the store creates hides under the `._citations` prefix, ignoring
//! the lot takes **one rule**:
//!
//! ```gitignore
//! ._citations*
//! ```
//!
//! Those throwaway files are **transient**: each is unlinked by the writer that
//! created it, so a run that finishes leaves `citations.jsonl` by itself. The
//! ignore rule still earns its keep — they exist for as long as a run does, and
//! a crash can strand one.
//!
//! # What the store touches in that directory
//!
//! * It **writes** `citations.jsonl`, `._citations.jsonl.tmp` (the staging file
//!   the atomic replace renames from), `._citations.lock`, its own
//!   `._citations.<writer>.log`, and its own companion liveness lock
//!   `._citations.<writer>.log.lock` (held from just before the first append
//!   until the flush that folds it away; the OS releases it if the process
//!   crashes).
//! * It **deletes** its own `._citations.<writer>.log` and
//!   `._citations.<writer>.log.lock`, the `._citations.lock` it took, and — only
//!   once their owning process is proven dead via that companion lock — a
//!   *crashed* peer's `._citations.<peer>.log` plus its
//!   `._citations.<peer>.log.lock`. A **live** peer's sidecar, the main file,
//!   and any file you put there yourself are never removed.
//! * It **reads** every `._citations.<something>.log` in the directory and folds
//!   it into the cache. A stray `._citations.notes.log` of your own has no
//!   companion `.log.lock`, so it is never reaped, but its contents are parsed
//!   (and, unless they happen to be cache entries, ignored). Prefer a different
//!   name, or a different directory, for unrelated files. A file under the bare
//!   base (`citations.notes.log`) is not part of the family at all and is not
//!   even read.
//!
//! All the interesting logic lives in the core [`FileCacheStore`]; this module
//! only provides the real filesystem operations it needs.

use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use autocitefetch::{
    BoxFuture, CacheFs, CacheGuard, CacheRecord, CacheStore, FileCacheStore, FsError, StoreError,
};

/// A [`CacheFs`] over the local filesystem via `std::fs`.
pub struct StdCacheFs;

impl CacheFs for StdCacheFs {
    type Guard = StdGuard;

    fn read(&self, path: &str) -> BoxFuture<'_, Result<Option<Vec<u8>>, FsError>> {
        let path = path.to_string();
        Box::pin(async move {
            match std::fs::read(&path) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(fs_err(e)),
            }
        })
    }

    fn append(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>> {
        let path = path.to_string();
        let bytes = bytes.to_vec();
        Box::pin(async move {
            // Create-if-absent + append; no fsync (the append log is
            // throwaway — durability comes from compaction's atomic_replace).
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(fs_err)?;
            f.write_all(&bytes).map_err(fs_err)?;
            Ok(())
        })
    }

    fn atomic_replace(&self, path: &str, bytes: &[u8]) -> BoxFuture<'_, Result<(), FsError>> {
        let path = path.to_string();
        let bytes = bytes.to_vec();
        Box::pin(async move { atomic_replace(Path::new(&path), &bytes).map_err(fs_err) })
    }

    fn list(&self, dir: &str) -> BoxFuture<'_, Result<Vec<String>, FsError>> {
        let dir = dir.to_string();
        Box::pin(async move {
            let mut out = Vec::new();
            match std::fs::read_dir(&dir) {
                Ok(rd) => {
                    for entry in rd {
                        let entry = entry.map_err(fs_err)?;
                        // Files only: a *directory* named like a sidecar
                        // (`._citations.x.log/`) would otherwise be handed to the
                        // core, whose `read` of it fails with EISDIR. Skipping
                        // it here keeps such a directory from making the whole
                        // cache unopenable.
                        match entry.file_type() {
                            Ok(ft) if ft.is_file() => {}
                            // A symlink to a file is fine; `is_file()` on the
                            // *link* is false, so follow it before rejecting.
                            Ok(ft) if ft.is_symlink() => {
                                if !std::fs::metadata(entry.path())
                                    .map(|m| m.is_file())
                                    .unwrap_or(false)
                                {
                                    continue;
                                }
                            }
                            // Unreadable metadata: leave it out rather than
                            // fail the listing.
                            _ => continue,
                        }
                        if let Some(name) = entry.file_name().to_str() {
                            out.push(name.to_string());
                        }
                    }
                    Ok(out)
                }
                // A not-yet-created dir simply has no files.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(out),
                Err(e) => Err(fs_err(e)),
            }
        })
    }

    fn remove(&self, path: &str) -> BoxFuture<'_, Result<(), FsError>> {
        let path = path.to_string();
        Box::pin(async move {
            match std::fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(fs_err(e)),
            }
        })
    }

    fn lock_exclusive(&self, path: &str) -> BoxFuture<'_, Result<Box<dyn CacheGuard>, FsError>> {
        let path = path.to_string();
        Box::pin(async move {
            match lock_the_file_at(&path, Blocking::Yes)? {
                Some(file) => Ok(Box::new(StdGuard { file }) as Box<dyn CacheGuard>),
                // Only reachable by exhausting the retry budget, i.e. the
                // lockfile was replaced under us over and over. Failing the
                // flush is the honest answer: we cannot claim exclusion.
                None => Err(FsError(format!(
                    "{path}: could not take a stable lock — the file kept being \
                     replaced while we were acquiring it"
                ))),
            }
        })
    }

    fn try_lock_exclusive(
        &self,
        path: &str,
    ) -> BoxFuture<'_, Result<Option<StdGuard>, FsError>> {
        let path = path.to_string();
        Box::pin(async move {
            Ok(lock_the_file_at(&path, Blocking::No)?.map(|file| StdGuard { file }))
        })
    }
}

/// Whether to wait for a held lock or give up immediately.
#[derive(Clone, Copy, PartialEq)]
enum Blocking {
    Yes,
    No,
}

/// How many times [`lock_the_file_at`] re-opens after finding it locked a file
/// that is no longer the one at the path. Each retry needs a peer to have
/// unlinked and recreated the lockfile inside our open→acquire window, so this
/// is a generous bound on something that should not happen twice in a row.
const LOCK_RACE_RETRIES: usize = 16;

/// Take an exclusive advisory lock on the file **currently at** `path`, creating
/// it if absent.
///
/// The verification is the point. The store unlinks lock files while still
/// holding their lock (that is what keeps the cache directory clean), so a
/// process that opened one a moment earlier can end up acquiring a lock on a
/// file that no longer has a name — while a newcomer creates a fresh file at the
/// same path and locks *that* freely. Both would believe they hold the lock. So
/// after acquiring we compare the locked file with the one the path now names,
/// and start over if they differ: the loser of that race simply takes the new
/// file's lock, which is the one everyone else is contending for.
///
/// `Ok(None)` means "not acquired": already held by someone else, or the retry
/// budget ran out. For a liveness probe that is the conservative answer (treat
/// the owner as alive); [`CacheFs::lock_exclusive`] turns it into an error,
/// since blocking mode has no other way to fail to acquire.
fn lock_the_file_at(path: &str, blocking: Blocking) -> Result<Option<std::fs::File>, FsError> {
    for _ in 0..LOCK_RACE_RETRIES {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false) // a lock file: never wipe it, we only lock it
            .open(path)
            .map_err(fs_err)?;
        // Fully-qualified so these resolve to fs4's trait methods and not the
        // inherent `File::lock`/`try_lock`/`unlock` std stabilized in 1.89
        // (> our MSRV). fs4 returns `Ok(())` on acquire and a `WouldBlock` error
        // when the lock is already held elsewhere — that latter case is
        // `Ok(None)`, not an error (it is exactly the "owner alive" signal).
        if blocking == Blocking::Yes {
            fs4::FileExt::lock_exclusive(&file).map_err(fs_err)?;
        } else {
            match fs4::FileExt::try_lock_exclusive(&file) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(fs_err(e)),
            }
        }
        // `false` = the path now names a different file (or none): our lock
        // guards nothing. `None` = this platform cannot tell, so accept it —
        // no worse than never having checked.
        if locked_the_file_at_path(&file, path) != Some(false) {
            return Ok(Some(file));
        }
        // Dropping `file` releases the lock on the dead inode before we retry.
    }
    Ok(None)
}

/// Whether `file` is still the file `path` names: `Some(true)`/`Some(false)`, or
/// `None` when the platform gives us no way to compare.
///
/// On unix a `(dev, ino)` pair identifies a file independently of its name, which
/// is exactly the question after an unlink. Elsewhere we say "cannot tell":
/// Windows has no stable-Rust equivalent (`file_index` is unstable), and there an
/// open file usually cannot be unlinked at all, so the race this guards against
/// does not arise.
#[cfg(unix)]
fn locked_the_file_at_path(file: &std::fs::File, path: &str) -> Option<bool> {
    use std::os::unix::fs::MetadataExt;
    let locked = file.metadata().ok()?;
    // No file at the path (the usual outcome of losing this race) ⇒ not ours.
    let Ok(named) = std::fs::metadata(path) else {
        return Some(false);
    };
    Some(locked.dev() == named.dev() && locked.ino() == named.ino())
}

#[cfg(not(unix))]
fn locked_the_file_at_path(_file: &std::fs::File, _path: &str) -> Option<bool> {
    None
}

/// A held exclusive lock. Owns the locked `File`; dropping it releases the
/// advisory lock. Public only because it is [`StdCacheFs`]'s
/// [`autocitefetch::CacheFs::Guard`] associated type; it has no
/// API of its own beyond being held and dropped. Being `Send` (it owns just a
/// `File`) is what keeps a `FileCacheStore<StdCacheFs>` movable across threads.
pub struct StdGuard {
    file: std::fs::File,
}

impl CacheGuard for StdGuard {}

impl Drop for StdGuard {
    fn drop(&mut self) {
        // Fully-qualified to fs4's trait method (see the note in
        // `lock_exclusive`); dropping the `File` would release the lock anyway.
        let _ = fs4::FileExt::unlock(&self.file);
    }
}

/// Convert any `std` error into an [`FsError`].
fn fs_err(e: impl std::fmt::Display) -> FsError {
    FsError(e.to_string())
}

/// Durably, all-or-nothing replace `path`'s contents with `bytes`: write a
/// temp file in the *same* directory, fsync it, atomically rename it over the
/// target, then (best-effort) fsync the parent directory so the rename itself
/// is durable. A failure of that last directory fsync is *not* reported: it is
/// unsupported on some filesystems, and the rename has already happened.
///
/// Two details that are easy to get wrong:
///
/// * **Permissions.** A rename replaces the destination's mode with the temp
///   file's, so a fresh 0600 temp file silently turns a shared, group-readable
///   `citations.jsonl` into a private one and the next user's `open` fails with
///   EACCES. The destination's mode is therefore carried onto the temp file
///   before the rename; when there is no destination yet, the temp file is
///   created with `File::create` so the process umask decides (rather than a
///   hard-coded 0600, which `tempfile::NamedTempFile` would impose).
/// * **Temp file name.** Deliberately fixed (`._{file}.tmp` beside the target)
///   rather than random: a random `.tmpXXXXXX` left behind by a crash is never
///   reaped by anything, whereas a fixed name is simply overwritten by the next
///   compaction. This is safe only because the sole caller is
///   `FileCacheStore::flush`, which holds the exclusive compaction lock for the
///   whole operation. The `._` prefix is the core's throwaway-file convention
///   (see [`autocitefetch::FileCacheStore`]): staging `citations.jsonl` writes
///   `._citations.jsonl.tmp`, so the one `._{base}*` ignore rule that covers the
///   sidecars and lockfiles covers this file too, and only `{base}.jsonl` is
///   left committable.
fn atomic_replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    // Same directory as the target (a rename must not cross filesystems), name
    // prefixed `._`. A path with no file name cannot be renamed onto anyway, so
    // the fallback there only has to be harmless.
    let mut tmp_name = std::ffi::OsString::from("._");
    tmp_name.push(path.file_name().unwrap_or_else(|| "cache".as_ref()));
    tmp_name.push(".tmp");
    let tmp_path = dir.join(tmp_name);

    let mut tmp = std::fs::File::create(&tmp_path)?;
    // Match the destination's permissions *before* writing any content, so the
    // bytes are never briefly visible under a looser mode than the cache had.
    if let Ok(meta) = std::fs::metadata(path) {
        let _ = tmp.set_permissions(meta.permissions());
    }
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.sync_all()?;
    drop(tmp);

    // Atomic rename over the destination.
    if let Err(e) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    // fsync the directory so the rename survives a crash (best effort).
    if let Ok(dir_file) = std::fs::File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

/// The base file name used by [`SingleFileCacheStore::new`] (`citations.jsonl`
/// etc.).
pub const DEFAULT_BASE: &str = "citations";

/// A ready-to-use single-file cache store over the local filesystem.
///
/// Thin wrapper over [`FileCacheStore`]`<`[`StdCacheFs`]`>`: keeps one
/// `citations.jsonl` file (plus per-writer `._citations.*.log` sidecars and a
/// `._citations.lock`) in a directory of your choosing. Construct with
/// [`SingleFileCacheStore::new`], or with [`SingleFileCacheStore::with_base`] to
/// rename the whole family.
pub struct SingleFileCacheStore(FileCacheStore<StdCacheFs>);

impl SingleFileCacheStore {
    /// Open (creating the directory if needed) a single-file cache in `dir`,
    /// named after [`DEFAULT_BASE`].
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::with_base(dir, DEFAULT_BASE).await
    }

    /// Like [`new`](Self::new), but with a caller-chosen `base` for every file
    /// in the family: the committable `{base}.jsonl`, and — all under the
    /// `._{base}` throwaway prefix — `._{base}.lock`,
    /// `._{base}.<writer>.log` with its companion `.log.lock`, and
    /// `._{base}.jsonl.tmp`. Use it to keep the cache out of the way in a
    /// directory that is not its own — a CLI dropping the cache in the user's
    /// working directory wants `".citations"`, so the committable file is the
    /// hidden `.citations.jsonl` and the throwaway files are `._.citations*`.
    ///
    /// The writer id is `"<pid>-<nanos-since-epoch>"`, unique per process run
    /// without needing an RNG dependency, so concurrent processes never share
    /// a sidecar log.
    pub async fn with_base(dir: impl AsRef<Path>, base: &str) -> Result<Self, StoreError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(|e| StoreError(e.to_string()))?;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let writer_id = format!("{}-{}", std::process::id(), nanos);

        // The core builds every path by string concatenation, so a lossy
        // conversion here would leave it operating on a U+FFFD-mangled path
        // that does not exist: reads would return `Ok(None)` and compaction
        // would fail against a missing directory — a cache that silently never
        // persists. Refuse instead.
        let dir = dir
            .to_str()
            .ok_or_else(|| StoreError("cache dir path is not valid UTF-8".into()))?
            .to_owned();
        let inner = FileCacheStore::open(StdCacheFs, dir, base, writer_id).await?;
        Ok(SingleFileCacheStore(inner))
    }

    /// Access the underlying generic store.
    pub fn inner(&self) -> &FileCacheStore<StdCacheFs> {
        &self.0
    }
}

// Delegate the store trait straight through to the inner `FileCacheStore`.
impl CacheStore for SingleFileCacheStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        self.0.get(id)
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.0.put(id, record)
    }
    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        self.0.remove(id)
    }
    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        self.0.entries()
    }
    fn flush(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.0.flush()
    }
}
