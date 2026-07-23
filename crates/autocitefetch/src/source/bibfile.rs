//! The `bib` source: look keys up in local/remote bibliography files.
//!
//! Files are fetched through [`RetrieveCtx::fetcher`] (which resolves `file:`
//! URLs to local reads) and indexed by each entry's `id`. A file may be either
//! an array of items (each with an `id`) or an object mapping id → item. In
//! both forms an item that is not a JSON object is rejected: `set_id` would
//! otherwise silently degrade it to a bare `{"id": "bib:…"}` shell and the
//! caller would cache an empty entry believing it succeeded.
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
    /// duplicate ids. Empty when built with
    /// [`BibliographyFileSource::from_entries`].
    files: Vec<String>,
    /// Entries supplied directly as data (host already parsed them). Empty when
    /// built with [`BibliographyFileSource::new`] — the two constructors are
    /// mutually exclusive and no public API combines them, so in practice
    /// exactly one of `files` / `preloaded` is populated. (The lookup below
    /// nevertheless layers files over preloaded, so adding a combining builder
    /// later would need no change here.)
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
            // Only the *fetched* files are indexed here; the host-supplied
            // entries are consulted as a fallback below. Cloning `preloaded`
            // into this map would deep-copy the whole bibliography (10k JSON
            // values, say) just to answer a handful of keys.
            let mut from_files: HashMap<String, Option<CslValue>> = HashMap::new();
            for file in &self.files {
                match load_file(file, ctx, self.parser).await {
                    Ok(items) => {
                        // Later files win on duplicate ids.
                        for (id, item) in items {
                            from_files.insert(id, item);
                        }
                    }
                    Err(msg) => {
                        // A file that fails to load fails the whole chunk's keys
                        // (surfacing the load error once). `Error` is not
                        // `Clone`, so `load_file` hands back the bare message
                        // and each key gets its own single-wrapped error.
                        return keys
                            .into_iter()
                            .map(|k| Resolution::failed(k, Error::Source(msg.clone())))
                            .collect();
                    }
                }
            }

            keys.into_iter()
                .map(|k| {
                    // Files are layered over the host-supplied entries.
                    match from_files.get(&k) {
                        Some(Some(item)) => Resolution::concrete(k, item.clone()),
                        // Present in a file, but not a JSON object.
                        Some(None) => {
                            let e = Error::Parse(alloc::format!(
                                "bibliography entry `{k}` is not a JSON object"
                            ));
                            Resolution::failed(k, e)
                        }
                        None => match self.preloaded.get(&k) {
                            Some(item) if item.is_object() => {
                                Resolution::concrete(k, item.clone())
                            }
                            // Same guard for host-supplied data.
                            Some(_) => {
                                let e = Error::Parse(alloc::format!(
                                    "bibliography entry `{k}` is not a JSON object"
                                ));
                                Resolution::failed(k, e)
                            }
                            // `Error::NotFound`'s Display already renders
                            // "citation `…` not found"; pass it the id, not a
                            // sentence, or the two nest into gibberish.
                            None => {
                                let e = Error::NotFound(crate::csl::cite_id("bib", &k));
                                Resolution::failed(k, e)
                            }
                        },
                    }
                })
                .collect()
        })
    }
}

/// Fetch one bibliography file, parse it with `parser`, and return its
/// `(id, item)` pairs.
///
/// `None` as the item means "this id is present in the file but its value is
/// not a JSON object" — kept (rather than dropped) so a request for that key
/// gets an accurate error instead of a misleading "not found".
///
/// On failure returns the bare message, *not* an [`Error`]: the caller has to
/// hand one error to every key of the chunk and `Error` is not `Clone`, so
/// re-wrapping a formatted `Error` would double the `source error:` prefix.
async fn load_file(
    url: &str,
    ctx: &RetrieveCtx<'_>,
    parser: BibParser,
) -> Result<Vec<(String, Option<CslValue>)>, String> {
    let resp = ctx
        .fetcher
        .fetch(Request::get(url))
        .await
        .map_err(|e| alloc::format!("bibliography file `{url}` fetch failed: {e}"))?;
    if !resp.is_success() {
        return Err(alloc::format!(
            "bibliography file `{url}` returned status {}",
            resp.status
        ));
    }
    let value: CslValue =
        parser(&resp.body).map_err(|m| alloc::format!("bibliography file `{url}`: {m}"))?;

    let mut out = Vec::new();
    match value {
        // Array of items, each carrying its own `id`. An item that is not an
        // object has no `id` to index it by, so it is necessarily dropped —
        // deliberate, and the only case where a malformed entry stays silent.
        CslValue::Array(items) => {
            for item in items {
                if !item.is_object() {
                    continue;
                }
                if let Some(id) = item.get("id").and_then(CslValue::as_str) {
                    out.push((id.into(), Some(item)));
                }
            }
        }
        // Object mapping id -> item. Here the id survives a malformed value, so
        // record it as such rather than accepting a non-object as an entry.
        CslValue::Object(map) => {
            for (id, item) in map {
                let item = if item.is_object() { Some(item) } else { None };
                out.push((id, item));
            }
        }
        _ => {
            return Err(alloc::format!(
                "bibliography file `{url}` is neither an array nor an object"
            ));
        }
    }
    Ok(out)
}
