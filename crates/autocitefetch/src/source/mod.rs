//! The [`Source`] trait and the types a source returns.
//!
//! A source is a provider of bibliographic info for one prefix (arXiv, DOI,
//! …). It implements a single method, [`Source::retrieve_chunk`]; the manager
//! handles routing, cache lookup, chunking, rate-limiting, and chaining.

pub mod arxiv;
pub mod bibfile;
pub mod doi;
pub mod manual;

pub use arxiv::ArxivSource;
pub use bibfile::{BibParser, BibliographyFileSource};
pub use doi::DoiSource;
pub use manual::ManualSource;

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::csl::CslValue;
use crate::env::{Clock, Timer};
use crate::error::Error;
use crate::fetch::Fetcher;
use crate::BoxFuture;

/// What a source resolved a single requested key into.
#[derive(Debug)]
pub enum Outcome {
    /// Concrete CSL-JSON metadata, with an optional per-entry TTL override
    /// (falls back to the source's [`Source::default_ttl`] when `None`).
    Concrete {
        csl: CslValue,
        ttl: Option<Duration>,
    },
    /// A pointer to another `(prefix, key)`. `set_properties` is merged into
    /// the resolved target at read time (e.g. re-attaching `arxivid`).
    Chained {
        prefix: String,
        key: String,
        set_properties: CslValue,
    },
    /// This key could not be resolved. The manager applies its
    /// stale-while-revalidate / grace policy before surfacing the error.
    Failed(Error),
}

/// The result of resolving one requested key.
#[derive(Debug)]
pub struct Resolution {
    /// The originally requested key (prefix stripped).
    pub key: String,
    pub outcome: Outcome,
}

impl Resolution {
    /// Resolved to concrete CSL-JSON, with the source's default TTL.
    pub fn concrete(key: impl Into<String>, csl: CslValue) -> Self {
        Resolution {
            key: key.into(),
            outcome: Outcome::Concrete { csl, ttl: None },
        }
    }

    /// Resolved to a pointer at `(target_prefix, target_key)`; `set_properties`
    /// is merged into the target at read time (see [`Outcome::Chained`]).
    pub fn chained(
        key: impl Into<String>,
        target_prefix: impl Into<String>,
        target_key: impl Into<String>,
        set_properties: CslValue,
    ) -> Self {
        Resolution {
            key: key.into(),
            outcome: Outcome::Chained {
                prefix: target_prefix.into(),
                key: target_key.into(),
                set_properties,
            },
        }
    }

    /// Could not be resolved. Still counts as "one `Resolution` per requested
    /// key" — see [`Source::retrieve_chunk`].
    pub fn failed(key: impl Into<String>, err: Error) -> Self {
        Resolution {
            key: key.into(),
            outcome: Outcome::Failed(err),
        }
    }
}

/// Everything a source needs from the host to do its work for one batch.
pub struct RetrieveCtx<'a> {
    pub fetcher: &'a dyn Fetcher,
    pub timer: &'a dyn Timer,
    pub clock: &'a dyn Clock,
}

/// A provider of bibliographic information for one citation prefix.
///
/// Object-safe: sources live in the manager as `Box<dyn Source>` so users can
/// register their own at runtime.
pub trait Source {
    /// The citation prefix this source answers to (e.g. `"arxiv"`).
    fn prefix(&self) -> &str;

    /// Max number of keys to send in one [`Source::retrieve_chunk`] call.
    fn chunk_size(&self) -> usize {
        512
    }

    /// Minimum delay between successive chunks (rate limiting).
    fn min_interval(&self) -> Duration {
        Duration::from_millis(1000)
    }

    /// Default lifetime for entries this source stores.
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(30 * 24 * 60 * 60)
    }

    /// Prefixes this source may chain *to* (e.g. arXiv → `["doi"]`).
    ///
    /// **Advisory / introspection only** — the manager does not consult it.
    /// Chain discovery is dynamic: an [`Outcome::Chained`] resolution pushes
    /// its target onto the retrieval worklist, which is drained in further
    /// passes, so ordering needs no static declaration. Declare it anyway to
    /// document the source's shape (and so a host can warn about a chain
    /// target whose prefix has no registered source).
    fn chains_to(&self) -> &[&'static str] {
        &[]
    }

    /// Resolve a batch of keys (already stripped of the prefix, and already
    /// filtered to cache-misses / stale entries by the manager).
    ///
    /// # Contract
    ///
    /// **Return exactly one [`Resolution`] per requested key**, with
    /// `Resolution::key` equal to the requested key — the manager matches
    /// results to requests by `res.key`, not by position. Use
    /// [`Outcome::Failed`] (via [`Resolution::failed`]) for a key you could
    /// not resolve, including a plain miss; a key you omit is silently
    /// *neither stored nor reported*, so the caller sees no failure and no
    /// entry. Extra resolutions for keys that were not requested are ignored.
    ///
    /// Do **not** implement retries here: the `ctx.fetcher` handed to a source
    /// is already wrapped in a
    /// [`RetryingFetcher`](crate::retry::RetryingFetcher).
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>>;
}
