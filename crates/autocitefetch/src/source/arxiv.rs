//! The `arxiv` source: the arXiv Atom API.
//!
//! **Status: scaffolding.** The trait wiring, rate limits, and chaining
//! declaration are in place; the Atom-feed retrieval itself is not yet
//! implemented (returns [`Outcome::Failed`] for every key).
//!
//! Planned behaviour (mirrors the JS/Python references, with fixes):
//! * `GET https://export.arxiv.org/api/query?id_list=…&max_results=…`
//!   (chunk of ≤100 ids), fetched via [`RetrieveCtx::fetcher`].
//! * Parse the Atom feed (a small `no_std` XML pull parser — crate TBD)
//!   pulling `id`, `title`, `author/name`, `published`, and `arxiv:doi`.
//! * Map to CSL-JSON (`type: "article-journal"`, hand-built) with extension
//!   fields `arxivid`, `arxiv_version_number`.
//! * Version resolution for versionless ids: prefer a versionless answer,
//!   else the highest returned version.
//! * If a DOI is known and `chain_to_doi` is set, emit [`Outcome::Chained`]
//!   to the `doi` source with `set_properties = { arxivid }` instead of
//!   storing arXiv metadata directly.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::error::Error;
use crate::source::{Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

/// See module docs.
pub struct ArxivSource {
    /// When true, resolved DOIs are chained to the `doi` source.
    pub chain_to_doi: bool,
}

impl Default for ArxivSource {
    fn default() -> Self {
        ArxivSource { chain_to_doi: true }
    }
}

impl ArxivSource {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Source for ArxivSource {
    fn prefix(&self) -> &str {
        "arxiv"
    }

    fn chunk_size(&self) -> usize {
        100
    }

    fn min_interval(&self) -> Duration {
        // arXiv asks for no more than one request every ~3 seconds.
        Duration::from_millis(3100)
    }

    fn default_ttl(&self) -> Duration {
        Duration::from_secs(10 * 24 * 60 * 60)
    }

    fn chains_to(&self) -> &[&'static str] {
        if self.chain_to_doi {
            &["doi"]
        } else {
            &[]
        }
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    Resolution::failed(k, Error::Source("arxiv source not yet implemented".into()))
                })
                .collect()
        })
    }
}
