//! The [`Source`] trait and the types a source returns.
//!
//! A source is a provider of bibliographic info (arXiv, DOI, …). It implements
//! a single method, [`Source::retrieve_chunk`]; the manager handles routing,
//! cache lookup, chunking, rate-limiting, and chaining.
//!
//! **A source does not own a prefix.** The prefix is chosen by the *host*, at
//! [`register`](crate::manager::CitationManager::register) time: `(prefix,
//! source)` is a binding the manager holds, not a property of the source type.
//! So one source type can serve several prefixes in one manager (two
//! [`BibliographyFileSource`]s over different files, registered as `bib` and
//! `theses`), and a prefix that clashes with the host's own naming can simply be
//! registered under a different one (`DoiSource` as `dx`). A source that needs to
//! know the prefix it is currently answering for — to build an id for an error
//! message, say — reads [`RetrieveCtx::prefix`]; one that *points* at another
//! source ([`Outcome::Chained`]) must be told that target prefix as
//! configuration, the way [`ArxivSource::chain_dois_to`] does.

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
use crate::report::Reporter;
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
    /// the resolved target at read time, **overriding** any colliding target
    /// field (e.g. re-attaching `arxivid`).
    ///
    /// `prefix` is a prefix in the *host's* namespace, so a source that chains
    /// must take it as configuration rather than hard-coding one: nothing
    /// guarantees the target source was registered under the name this source
    /// would have guessed (see [`ArxivSource::chain_dois_to`]). A
    /// prefix with no registered source is reported as a failure on the target,
    /// attributed back to the citation that pulled it in.
    Chained {
        prefix: String,
        key: String,
        set_properties: CslValue,
    },
    /// This key could not be resolved because the source was *unreachable* or
    /// erroring (transport failure, 5xx/timeout, a whole file that would not
    /// load, a malformed feed). The manager applies its stale-while-revalidate
    /// / grace policy: within the grace window a still-cached copy keeps being
    /// served and the error is **not** reported — "try again later".
    Failed(Error),
    /// The source is reachable and definitively has **no such key**: a file
    /// that loaded fine but does not contain the id, an id absent from a 200
    /// feed, a doi.org 404. This is the *opposite* of [`Outcome::Failed`] — it
    /// is authoritative, not transient. The manager therefore **always** reports
    /// it, never consults the grace window, and drops any stale cached copy so
    /// [`get`](crate::manager::CitationManager::get) stops serving now-known-wrong
    /// data.
    Missing(Error),
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
    /// is merged into the target at read time, overriding colliding target
    /// fields (see [`Outcome::Chained`]).
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

    /// Could not be resolved because the source was unreachable / erroring
    /// (transport, 5xx, an unloadable file). Grace-served if a cached copy is
    /// still within its window — see [`Outcome::Failed`]. Still counts as "one
    /// `Resolution` per requested key" — see [`Source::retrieve_chunk`].
    pub fn failed(key: impl Into<String>, err: Error) -> Self {
        Resolution {
            key: key.into(),
            outcome: Outcome::Failed(err),
        }
    }

    /// The source is reachable and authoritatively has no such key (see
    /// [`Outcome::Missing`]). Always reported, never grace-served. Mirrors
    /// [`Resolution::failed`]; still one `Resolution` per requested key.
    pub fn missing(key: impl Into<String>, err: Error) -> Self {
        Resolution {
            key: key.into(),
            outcome: Outcome::Missing(err),
        }
    }
}

/// Everything a source needs from the host to do its work for one batch.
pub struct RetrieveCtx<'a> {
    pub fetcher: &'a dyn Fetcher,
    pub timer: &'a dyn Timer,
    pub clock: &'a dyn Clock,
    /// The prefix this source is currently answering for — the one the host
    /// bound it to with
    /// [`register`](crate::manager::CitationManager::register), not anything the
    /// source declares. Since the same source may be registered under several
    /// prefixes, this is the only correct way to build a `"prefix:key"` id for a
    /// message about the keys in *this* batch.
    pub prefix: &'a str,
    /// Where to announce progress. A [`NopReporter`](crate::report::NopReporter)
    /// unless the host called
    /// [`with_reporter`](crate::manager::CitationManager::with_reporter).
    ///
    /// **A source rarely needs to touch this.** The chunk-level events a
    /// progress display is built from are emitted by
    /// [`driver`](crate::driver) around `retrieve_chunk`, so every source —
    /// including a third-party one — is instrumented without cooperating.
    /// Reach for it only to announce something source-specific that the driver
    /// cannot see, and emit *milestones* sparingly:
    /// [`report`](crate::report::Reporter::report) is synchronous and runs
    /// inside the retrieval loop.
    pub reporter: &'a dyn Reporter,
}

/// A provider of bibliographic information, bound to a citation prefix by the
/// host at registration time.
///
/// Object-safe: sources live in the manager as `Box<dyn Source>` so users can
/// register their own at runtime.
///
/// The trait carries no prefix. See the [module docs](self) for why, and read
/// [`RetrieveCtx::prefix`] when a source needs the one it is serving.
pub trait Source {
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

    /// Canonicalize a requested key before anything is keyed on it.
    ///
    /// **Default: trim surrounding whitespace, then ASCII-lowercase.** For
    /// nearly every source a key is an *identifier* — an arXiv id, a DOI, a
    /// bibliography key — where surrounding whitespace is incidental
    /// (`\cite{arXiv: 1211.1037}` yields `" 1211.1037"`) and, for the
    /// case-insensitive identifier families, case is not meaningful either
    /// (`10.1103/PhysRevA.86.052329` and `10.1103/physreva.86.052329` are the
    /// same DOI). Canonicalizing here means a source never has to special-case
    /// either: its keys arrive canonical.
    ///
    /// The manager applies this **once, centrally**, at every point a
    /// `(prefix, key)` becomes a cache id — before routing, `seen`-dedup,
    /// bucketing, storage, *and* lookup — so all spellings of a key collapse to
    /// one cache id and one fetch, and a later
    /// [`get`](crate::manager::CitationManager::get) with any spelling finds the
    /// entry. [`retrieve_chunk`](Source::retrieve_chunk) therefore only ever sees
    /// already-normalized keys (**do not re-normalize inside a source**), and the
    /// canonical `"prefix:key"` echoed in a resolved item's `id` and in a
    /// [`CiteFailure`] is the normalized form — so a caller who requested
    /// `doi:10.1103/PhysRevA.86.052329` reads back
    /// `"id": "doi:10.1103/physreva.86.052329"`.
    ///
    /// Whitespace handling is [`str::trim`] only: *internal* whitespace is left
    /// alone, since in an identifier it means malformed input that a source's own
    /// validation should reject (see `doi.rs`) rather than something to silently
    /// repair. Case folding is ASCII-only: identifier schemes that declare
    /// themselves case-insensitive (DOI) are ASCII, and full Unicode folding
    /// would make the cache id locale-surprising for no gain.
    ///
    /// Override when a key is **not** a case-insensitive identifier:
    ///
    /// * [`ManualSource`] returns the key *unchanged* — it **is** the
    ///   pre-formatted citation text, so both case and surrounding whitespace are
    ///   significant.
    /// * [`ArxivSource`] and [`BibliographyFileSource`] trim but do **not**
    ///   lowercase — arXiv's old-style ids carry a case-significant subject class
    ///   (`math.AG/0601001`) and a bibliography key is an opaque label matched
    ///   byte-for-byte against the file. See their module docs.
    ///
    /// Implementations must be **idempotent** (`f(f(k)) == f(k)`): the manager
    /// applies this more than once along a chain.
    ///
    /// [`CiteFailure`]: crate::manager::CiteFailure
    fn normalize_key(&self, key: &str) -> String {
        let mut key = String::from(key.trim());
        key.make_ascii_lowercase();
        key
    }

    /// Prefixes this source may chain *to* (e.g. an [`ArxivSource`] delegating
    /// DOI lookups to `doi` → `["doi"]`).
    ///
    /// **Advisory / introspection only** — the manager does not consult it.
    /// Chain discovery is dynamic: an [`Outcome::Chained`] resolution pushes
    /// its target onto the retrieval worklist, which is drained in further
    /// passes, so ordering needs no static declaration. Declare it anyway to
    /// document the source's shape — and so a host can check, up front, that
    /// every prefix a source will point at actually has a source registered
    /// under it. That check is the reason this returns *borrowed* prefixes
    /// rather than `&'static str`s: a chain target is configuration (see
    /// [`ArxivSource::chain_dois_to`]), so it is generally owned by
    /// the source instance, not baked into its type.
    fn chains_to(&self) -> Vec<&str> {
        Vec::new()
    }

    /// Resolve a batch of keys (already stripped of the prefix, and already
    /// filtered to cache-misses / stale entries by the manager).
    ///
    /// # Contract
    ///
    /// **Return exactly one [`Resolution`] per requested key**, with
    /// `Resolution::key` equal to the requested key — the manager matches
    /// results to requests by `res.key`, not by position. For a key you could
    /// not resolve, choose the outcome by *why*:
    ///
    /// * [`Outcome::Failed`] (via [`Resolution::failed`]) when the source was
    ///   **unreachable or erroring** (transport failure, 5xx, an unloadable
    ///   file) — the manager may keep serving a still-cached copy within its
    ///   grace window and suppress the error ("try again later").
    /// * [`Outcome::Missing`] (via [`Resolution::missing`]) when the source is
    ///   **reachable and authoritatively has no such key** (the id is absent
    ///   from a file that loaded fine, or from a successful API response) — the
    ///   manager always reports it and drops any stale cached copy.
    ///
    /// A key you omit is silently *neither stored nor reported*, so the caller
    /// sees no failure and no entry. Extra resolutions for keys that were not
    /// requested are ignored.
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
