//! The `manual` source: the key *is* the pre-formatted citation text.
//!
//! `manual` is the prefix this is *conventionally* registered under, not one it
//! declares: the host names it at
//! [`register`](crate::manager::CitationManager::register) time and may pick any
//! other. See the [`source`](crate::source) module docs.
//!
//! This is the escape hatch for citations that have no online source — the
//! consumer passes the already-formatted text as the key and it is stored
//! verbatim under the extension field `_ready_formatted`, bypassing CSL
//! rendering:
//!
//! ```json
//! { "_ready_formatted": { "flm": "Bohr, N. (1913). On the Constitution of …" } }
//! ```
//!
//! The inner key is the **format name** the text is written in, and it is
//! *configuration*, not an assumption: the source is told it at construction
//! ([`ManualSource::new`]). Nothing here can know whether the host renders FLM,
//! LaTeX, HTML or plain text, and a downstream renderer needs to be able to tell
//! — the same reasoning as the chain-target prefix in
//! [`ArxivSource::chain_dois_to`](crate::source::ArxivSource::chain_dois_to).
//! (The JS reference hard-codes `flm` here; the Python one flattens the whole
//! thing to a `_formatted_flm_text` key.)
//!
//! Because the key *is* the output, this source overrides
//! [`Source::normalize_key`] with the **identity**: the default policy (trim +
//! lowercase, right for identifier-shaped keys) would rewrite the citation text
//! itself. Two manual keys differing only in case or padding stay two distinct
//! citations.
//!
//! Entries carry **TTL 0**, so they are re-resolved on every run and never
//! served from cache — which is free, since resolving is just copying the key.
//!
//! TTL 0 makes an entry *ephemeral*: it is kept in the store's in-memory view
//! for the duration of the run (so `get()` still returns it after `retrieve`),
//! but is **never persisted** — a persistent [`FileCacheStore`] keeps it out of
//! the committable `citations.jsonl` and its sidecar logs entirely, and it is
//! gone the moment the store is reopened. So arbitrary manual citation text
//! never lands in a git-tracked file. (This is enforced by the store, keyed on
//! the record's timestamps, not by anything special about this prefix.)
//!
//! [`FileCacheStore`]: crate::filecache::FileCacheStore

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::csl::CslValue;
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

/// See module docs.
pub struct ManualSource {
    /// The inner key under `_ready_formatted` — the name of the markup format
    /// the citation text is written in. No default: only the host knows what it
    /// will hand the text to.
    format_name: String,
}

impl ManualSource {
    /// `format_name` names the markup the keys are written in (`"flm"`,
    /// `"latex"`, `"html"`, …) and becomes the inner key of the emitted
    /// `_ready_formatted` object.
    pub fn new(format_name: impl Into<String>) -> Self {
        ManualSource {
            format_name: format_name.into(),
        }
    }
}

impl Source for ManualSource {
    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }

    fn default_ttl(&self) -> Duration {
        Duration::ZERO
    }

    fn normalize_key(&self, key: &str) -> String {
        // Identity. The key *is* the pre-formatted citation text, so **both**
        // its case and its surrounding whitespace are significant: the default
        // policy (trim + lowercase) would silently rewrite the rendered
        // citation. Two manual keys differing only in case or padding are
        // therefore two distinct citations.
        String::from(key)
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let mut formatted = serde_json::Map::new();
                    formatted.insert(self.format_name.clone(), CslValue::String(k.clone()));
                    let mut obj = serde_json::Map::new();
                    obj.insert("_ready_formatted".into(), CslValue::Object(formatted));
                    Resolution {
                        key: k,
                        outcome: Outcome::Concrete {
                            csl: CslValue::Object(obj),
                            ttl: Some(Duration::ZERO),
                        },
                    }
                })
                .collect()
        })
    }
}
