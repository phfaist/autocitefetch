//! The `bib` source: look keys up in local/remote CSL-JSON bibliography files.
//!
//! Files are fetched through [`RetrieveCtx::fetcher`] (which resolves `file:`
//! URLs to local reads), parsed as CSL-JSON, and indexed by each entry's
//! `id`. A file may be either a JSON array of items or a JSON object mapping
//! id → item. (YAML bib files are deferred to a `std`-side helper.)

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use hashbrown::HashMap;

use crate::csl::CslValue;
use crate::error::Error;
use crate::fetch::Request;
use crate::source::{Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

/// See module docs.
pub struct BibliographyFileSource {
    /// File URLs (or `file:` paths) to load, in order. Later files win on
    /// duplicate ids.
    files: Vec<String>,
    ttl: Duration,
}

impl BibliographyFileSource {
    /// Build from a list of bibliography file locations.
    pub fn new(files: impl IntoIterator<Item = String>) -> Self {
        BibliographyFileSource {
            files: files.into_iter().collect(),
            ttl: Duration::from_secs(60),
        }
    }

    /// Override the cache lifetime of entries loaded from these files.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }
}

impl Source for BibliographyFileSource {
    fn prefix(&self) -> &str {
        "bib"
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }

    fn default_ttl(&self) -> Duration {
        self.ttl
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            // Load & index every configured file.
            let mut index: HashMap<String, CslValue> = HashMap::new();
            for file in &self.files {
                match load_file(file, ctx).await {
                    Ok(items) => {
                        for (id, item) in items {
                            index.insert(id, item);
                        }
                    }
                    Err(e) => {
                        // A file that fails to load fails only its own keys;
                        // record nothing here and let per-key lookup report
                        // NotFound, but surface the load error once.
                        return keys
                            .into_iter()
                            .map(|k| Resolution::failed(k, clone_err(&e)))
                            .collect();
                    }
                }
            }

            keys.into_iter()
                .map(|k| match index.get(&k) {
                    Some(item) => Resolution::concrete(k, item.clone()),
                    None => {
                        let msg = alloc::format!("key `{k}` not found in bibliography files");
                        Resolution::failed(k, Error::NotFound(msg))
                    }
                })
                .collect()
        })
    }
}

/// Fetch one bibliography file and return its `(id, item)` pairs.
async fn load_file(url: &str, ctx: &RetrieveCtx<'_>) -> Result<Vec<(String, CslValue)>, Error> {
    let resp = ctx
        .fetcher
        .fetch(Request::get(url))
        .await
        .map_err(Error::Fetch)?;
    if !resp.is_success() {
        return Err(Error::Source(alloc::format!(
            "bibliography file `{url}` returned status {}",
            resp.status
        )));
    }
    let value: CslValue =
        serde_json::from_slice(&resp.body).map_err(|e| Error::Parse(alloc::format!("{e}")))?;

    let mut out = Vec::new();
    match value {
        // Array of items, each with an `id`.
        CslValue::Array(items) => {
            for item in items {
                if let Some(id) = item.get("id").and_then(CslValue::as_str) {
                    out.push((id.into(), item));
                }
            }
        }
        // Object mapping id -> item.
        CslValue::Object(map) => {
            for (id, item) in map {
                out.push((id, item));
            }
        }
        _ => {
            return Err(Error::Parse(alloc::format!(
                "bibliography file `{url}` is neither an array nor an object"
            )));
        }
    }
    Ok(out)
}

/// `Error` is not `Clone` (it wraps non-`Clone` payloads); duplicate the
/// message so every key of a failed file gets its own error.
fn clone_err(e: &Error) -> Error {
    Error::Source(alloc::format!("{e}"))
}
