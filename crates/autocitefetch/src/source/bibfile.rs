//! The `bib` source: look keys up in local/remote bibliography files.
//!
//! Files are fetched through [`RetrieveCtx::fetcher`] (which resolves `file:`
//! URLs to local reads) and indexed by each entry's `id`. A file may be either
//! an array of items (each with an `id`) or an object mapping id → item.
//!
//! **Format-agnostic parsing.** The bytes → [`CslValue`] step is a pluggable
//! [`BibParser`]. The default ([`BibliographyFileSource::new`]) parses JSON
//! (already a core dependency). For any other format, pass a parser with
//! [`BibliographyFileSource::with_parser`] — e.g.
//! `|b| serde_yaml::from_slice(b).map_err(|e| e.to_string())`. This works
//! because `CslValue` (`serde_json::Value`) is a `Deserialize` type, so *any*
//! serde deserializer can produce it, and the core depends on no format crate
//! but serde_json. Alternatively, parse everything host-side and pass the data
//! directly via [`BibliographyFileSource::from_entries`].

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

/// Parses raw bibliography-file bytes into a [`CslValue`] (an array of items or
/// an id → item object). The host chooses the format; on failure it returns a
/// human-readable message. A plain function pointer, so non-capturing closures
/// like `|b| serde_yaml::from_slice(b).map_err(|e| e.to_string())` coerce to it.
pub type BibParser = fn(&[u8]) -> Result<CslValue, String>;

/// The built-in JSON parser used by [`BibliographyFileSource::new`].
fn parse_json(bytes: &[u8]) -> Result<CslValue, String> {
    serde_json::from_slice(bytes).map_err(|e| alloc::format!("{e}"))
}

/// See module docs.
pub struct BibliographyFileSource {
    /// File URLs (or `file:` paths) to load, in order. Later files win on
    /// duplicate ids.
    files: Vec<String>,
    /// Entries supplied directly as data (host already parsed them). Files, when
    /// present, are layered on top.
    preloaded: HashMap<String, CslValue>,
    /// Host-pluggable bytes → CslValue parser (defaults to JSON).
    parser: BibParser,
    ttl: Duration,
}

impl BibliographyFileSource {
    /// Build from a list of bibliography file locations, parsed as JSON.
    pub fn new(files: impl IntoIterator<Item = String>) -> Self {
        BibliographyFileSource {
            files: files.into_iter().collect(),
            preloaded: HashMap::new(),
            parser: parse_json,
            ttl: Duration::from_secs(60),
        }
    }

    /// Build from already-parsed entries (id → CSL item), no fetching. Use this
    /// when the host loads and parses the bibliography itself.
    pub fn from_entries(entries: impl IntoIterator<Item = (String, CslValue)>) -> Self {
        BibliographyFileSource {
            files: Vec::new(),
            preloaded: entries.into_iter().collect(),
            parser: parse_json,
            ttl: Duration::from_secs(60),
        }
    }

    /// Use a custom bytes → [`CslValue`] parser (e.g. a serde_yaml/toml reader),
    /// enabling any serde-supported format without the core depending on it.
    pub fn with_parser(mut self, parser: BibParser) -> Self {
        self.parser = parser;
        self
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
            // Start from the host-supplied entries, then overlay each fetched
            // file (later sources win on duplicate ids).
            let mut index: HashMap<String, CslValue> = self.preloaded.clone();
            for file in &self.files {
                match load_file(file, ctx, self.parser).await {
                    Ok(items) => {
                        for (id, item) in items {
                            index.insert(id, item);
                        }
                    }
                    Err(e) => {
                        // A file that fails to load fails the whole chunk's keys
                        // (surfacing the load error once).
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
                        let msg = alloc::format!("key `{k}` not found in bibliography");
                        Resolution::failed(k, Error::NotFound(msg))
                    }
                })
                .collect()
        })
    }
}

/// Fetch one bibliography file, parse it with `parser`, and return its
/// `(id, item)` pairs.
async fn load_file(
    url: &str,
    ctx: &RetrieveCtx<'_>,
    parser: BibParser,
) -> Result<Vec<(String, CslValue)>, Error> {
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
    let value: CslValue = parser(&resp.body).map_err(Error::Parse)?;

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
