//! A `std`-backed [`CacheFs`] and the [`SingleFileCacheStore`] convenience
//! store built on top of it.
//!
//! The cache is a single committable `citations.jsonl` file plus lock-free
//! per-writer append logs (`citations.<writer>.log`) and a compaction lockfile
//! (`citations.lock`), all living in a user-chosen directory. Only
//! `citations.jsonl` is worth committing to version control; users should
//! **gitignore the sidecars, the lockfile and the compaction temp file**, e.g.:
//!
//! ```gitignore
//! citations.*.log
//! citations.*.log.lock
//! citations.lock
//! citations.jsonl.tmp
//! ```
//!
//! # What the store touches in that directory
//!
//! * It **writes** `citations.jsonl`, `citations.jsonl.tmp`, `citations.lock`,
//!   its own `citations.<writer>.log`, and its own companion liveness lock
//!   `citations.<writer>.log.lock` (held open for the store's whole life; the
//!   OS releases it if the process crashes).
//! * It **deletes** its own `citations.<writer>.log`, and — only once their
//!   owning process is proven dead via that companion lock — a *crashed* peer's
//!   `citations.<peer>.log` plus its `citations.<peer>.log.lock`. A **live**
//!   peer's sidecar, the main file, and any file you put there yourself are
//!   never removed.
//! * It **reads** every `citations.<something>.log` in the directory and folds
//!   it into the cache. A stray `citations.notes.log` of your own has no
//!   companion `.log.lock`, so it is never reaped, but its contents are parsed
//!   (and, unless they happen to be cache entries, ignored). Prefer a different
//!   name, or a different directory, for unrelated files.
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
                        // (`citations.x.log/`) would otherwise be handed to the
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
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false) // a lock file: never wipe it, we only lock it
                .open(&path)
                .map_err(fs_err)?;
            // Blocking exclusive advisory lock; released when the guard's owned
            // `File` is dropped (fs4 unlocks on close; we also unlock in Drop).
            // Fully-qualified so it resolves to fs4's trait method and not the
            // inherent `File::lock`/`unlock` std stabilized in 1.89 (> our MSRV).
            fs4::FileExt::lock_exclusive(&file).map_err(fs_err)?;
            Ok(Box::new(StdGuard { file }) as Box<dyn CacheGuard>)
        })
    }

    fn try_lock_exclusive(
        &self,
        path: &str,
    ) -> BoxFuture<'_, Result<Option<StdGuard>, FsError>> {
        let path = path.to_string();
        Box::pin(async move {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false) // a lock file: never wipe it, we only lock it
                .open(&path)
                .map_err(fs_err)?;
            // Non-blocking exclusive advisory lock. Fully-qualified to fs4's
            // trait method for the same MSRV reason as `lock_exclusive` (std's
            // inherent `File::try_lock` stabilized in 1.89, past our MSRV).
            // fs4 returns `Ok(())` on acquire and a `WouldBlock` error when the
            // lock is already held elsewhere — that latter case is `Ok(None)`,
            // not an error (it is exactly the "owner alive" signal).
            match fs4::FileExt::try_lock_exclusive(&file) {
                Ok(()) => Ok(Some(StdGuard { file })),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
                Err(e) => Err(fs_err(e)),
            }
        })
    }
}

/// A held exclusive lock. Owns the locked `File`; dropping it releases the
/// advisory lock. Public only because it is [`StdCacheFs`]'s
/// [`CacheFs::Guard`](autocitefetch::CacheFs::Guard) associated type; it has no
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
/// * **Temp file name.** Deliberately fixed (`{path}.tmp`) rather than random:
///   a random `.tmpXXXXXX` left behind by a crash is never reaped by anything,
///   whereas a fixed name is simply overwritten by the next compaction. This is
///   safe only because the sole caller is `FileCacheStore::flush`, which holds
///   the exclusive compaction lock for the whole operation.
fn atomic_replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp_path = path.as_os_str().to_os_string();
    tmp_path.push(".tmp");
    let tmp_path = std::path::PathBuf::from(tmp_path);

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

/// The base file name used by [`SingleFileCacheStore`] (`citations.jsonl` etc.).
const BASE: &str = "citations";

/// A ready-to-use single-file cache store over the local filesystem.
///
/// Thin wrapper over [`FileCacheStore`]`<`[`StdCacheFs`]`>`: keeps one
/// `citations.jsonl` file (plus per-writer `*.log` sidecars and a `.lock`) in
/// a directory of your choosing. Construct with [`SingleFileCacheStore::new`].
pub struct SingleFileCacheStore(FileCacheStore<StdCacheFs>);

impl SingleFileCacheStore {
    /// Open (creating the directory if needed) a single-file cache in `dir`.
    ///
    /// The writer id is `"<pid>-<nanos-since-epoch>"`, unique per process run
    /// without needing an RNG dependency, so concurrent processes never share
    /// a sidecar log.
    pub async fn new(dir: impl AsRef<Path>) -> Result<Self, StoreError> {
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
        let inner = FileCacheStore::open(StdCacheFs, dir, BASE, writer_id).await?;
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
