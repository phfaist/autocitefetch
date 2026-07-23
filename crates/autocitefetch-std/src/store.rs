//! A `std`-backed [`CacheFs`] and the [`SingleFileCacheStore`] convenience
//! store built on top of it.
//!
//! The cache is a single committable `citations.jsonl` file plus lock-free
//! per-writer append logs (`citations.<writer>.log`) and a compaction lockfile
//! (`citations.lock`), all living in a user-chosen directory. Only
//! `citations.jsonl` is worth committing to version control; users should
//! **gitignore the sidecars and lockfile**, e.g.:
//!
//! ```gitignore
//! citations.*.log
//! citations.lock
//! ```
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
}

/// A held exclusive lock. Owns the locked `File`; dropping it releases the
/// advisory lock.
struct StdGuard {
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
/// target, then fsync the parent directory so the rename itself is durable.
fn atomic_replace(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    // Atomic rename over the destination.
    tmp.persist(path).map_err(|e| e.error)?;
    // fsync the directory so the rename survives a crash.
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

        let dir = dir.to_string_lossy().into_owned();
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
