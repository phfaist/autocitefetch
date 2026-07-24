//! The `manual` source: the key *is* the pre-formatted citation text.
//!
//! This is the escape hatch for citations that have no online source — the
//! consumer passes the already-formatted text as the key and it is stored
//! verbatim under the extension field `_formatted_text`, bypassing CSL
//! rendering.
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
#[derive(Default)]
pub struct ManualSource;

impl ManualSource {
    pub fn new() -> Self {
        ManualSource
    }
}

impl Source for ManualSource {
    fn prefix(&self) -> &str {
        "manual"
    }

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
                    let mut obj = serde_json::Map::new();
                    obj.insert("_formatted_text".into(), CslValue::String(k.clone()));
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
