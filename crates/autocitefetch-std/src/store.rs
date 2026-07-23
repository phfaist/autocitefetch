use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use autocitefetch::{BoxFuture, CacheRecord, CacheStore, StoreError};

/// A [`CacheStore`] that keeps one JSON file per entry in a directory.
///
/// One-file-per-entry makes writes genuinely incremental (no whole-cache
/// re-serialize on every store) and lets the OS handle concurrency. Entry ids
/// are hex-encoded to form safe filenames.
pub struct DirCacheStore {
    dir: PathBuf,
}

impl DirCacheStore {
    /// Open (creating if needed) a cache directory.
    pub fn new(dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(DirCacheStore { dir })
    }

    fn path_for(&self, id: &str) -> PathBuf {
        let mut name = hex_encode(id.as_bytes());
        name.push_str(".json");
        self.dir.join(name)
    }
}

impl CacheStore for DirCacheStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let path = self.path_for(id);
        Box::pin(async move {
            match fs::read(&path) {
                Ok(bytes) => {
                    let rec = serde_json::from_slice(&bytes).map_err(store_err)?;
                    Ok(Some(rec))
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        let path = self.path_for(id);
        let dir = self.dir.clone();
        Box::pin(async move {
            let bytes = serde_json::to_vec(&record).map_err(store_err)?;
            atomic_write(&dir, &path, &bytes).map_err(store_err)?;
            Ok(())
        })
    }

    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        let path = self.path_for(id);
        Box::pin(async move {
            match fs::remove_file(&path) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(e) => Err(store_err(e)),
            }
        })
    }

    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        let dir = self.dir.clone();
        Box::pin(async move {
            let mut out = Vec::new();
            for entry in fs::read_dir(&dir).map_err(store_err)? {
                let entry = entry.map_err(store_err)?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                let Some(id_bytes) = hex_decode(stem) else { continue };
                let Ok(id) = String::from_utf8(id_bytes) else { continue };
                let bytes = fs::read(&path).map_err(store_err)?;
                let rec: CacheRecord = serde_json::from_slice(&bytes).map_err(store_err)?;
                out.push((id, rec));
            }
            Ok(out)
        })
    }
}

fn store_err(e: impl std::fmt::Display) -> StoreError {
    StoreError(e.to_string())
}

/// Write `bytes` to `path` atomically (temp file in the same dir + rename).
fn atomic_write(dir: &Path, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("entry");
    let tmp = dir.join(format!(".tmp-{file_name}"));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let bytes = s.as_bytes();
    if bytes.len() % 2 != 0 {
        return None;
    }
    let val = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        out.push((val(bytes[i])? << 4) | val(bytes[i + 1])?);
        i += 2;
    }
    Some(out)
}
