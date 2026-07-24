//! [`CitationManager`] — routing, the retrieval driver, and chain resolution.

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use futures_util::stream::{StreamExt, iter};
use hashbrown::{HashMap, HashSet};

use crate::cache::TtlPolicy;
use crate::csl::{self, CslValue};
use crate::driver::drive_source;
use crate::env::{Clock, Timer, Timestamp};
use crate::error::{Error, Result};
use crate::fetch::Fetcher;
use crate::report::{Event, NopReporter, Reporter, Resolved, Wait};
use crate::retry::{RetryPolicy, RetryingFetcher};
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::store::{CacheStore, Payload};

/// Upper bound on how many sources' `drive_source` calls run concurrently
/// within a single retrieval pass.
///
/// Buckets are keyed by prefix, so the number of futures in a pass is bounded
/// by the number of *registered* sources (and each source's own fetches are
/// serial inside `drive_source`). With fewer than nine sources registered this
/// constant is therefore inert; it exists so a host that registers dozens of
/// sources cannot open an unbounded number of connections at once.
const MAX_CONCURRENT_SOURCES: usize = 8;

/// A citation still to be looked at this retrieval.
///
/// `depth` is how many chain links away from a *requested* citation it is (0
/// for the ones the caller asked for). `origin` is the originally-requested
/// `(prefix, key)` this item descends from: it *is* `(prefix, key)` for a
/// directly-requested cite, and stays pinned to the root request as the item is
/// re-pushed across chain hops — so a failure discovered on any chained target
/// can be attributed back to the request that pulled it in.
struct WorkItem {
    prefix: String,
    key: String,
    depth: usize,
    origin: (String, String),
}

/// Per-id bookkeeping accumulated in the dedup set: the `depth` and `origin`
/// (see [`WorkItem`]) of the *first* work item that reached this id. Carried
/// into `store_resolutions` so a resolution can be re-attributed to the request
/// that pulled its id in.
#[derive(Clone)]
struct ItemMeta {
    depth: usize,
    origin: (String, String),
}

/// What one driven source contributes to a pass: its prefix, the keys it was
/// asked for, the resolutions it returned, and when its last chunk started.
type PassResult = (String, Vec<String>, Vec<Resolution>, Option<Timestamp>);

/// The mutable per-pass state that applying a resolution feeds: newly
/// discovered chain targets, and per-citation failures.
struct PassSink<'a> {
    worklist: &'a mut Vec<WorkItem>,
    report: &'a mut RetrieveReport,
}

/// A single citation that could not be resolved (and had no usable cached
/// copy). Retrieval is per-citation tolerant: one failure does not abort the
/// batch.
///
/// `prefix`/`key` identify the citation that *actually* failed — which, for a
/// failure discovered while following a chain, is the chain *target*, not the
/// citation the caller requested. `origin` closes that gap:
///
/// * `None` when the failed citation is itself one of the requested cites (a
///   direct failure — its `(prefix, key)` already matches the caller's input);
/// * `Some((prefix, key))` naming the originally-requested cite when this
///   failure is on a chained descendant of a *different* request.
///
/// So every requested cite that ultimately fails is discoverable in
/// [`RetrieveReport::failures`] either directly (a failure whose `(prefix, key)`
/// matches it) or indirectly (a failure whose `origin` is it): a caller joining
/// the report back against its input `cites` can always tell which of its
/// requested cites did not resolve, without waiting for
/// [`get`](CitationManager::get) to break on the chain later.
#[derive(Clone, Debug)]
pub struct CiteFailure {
    pub prefix: String,
    pub key: String,
    pub message: String,
    /// The originally-requested cite this failure is attributed to, or `None`
    /// when the failed cite *is* the requested one. See the type-level docs.
    pub origin: Option<(String, String)>,
}

impl CiteFailure {
    /// Build a failure for the citation `(prefix, key)` that actually failed,
    /// attributing it to `origin` — the root requested cite the failing item
    /// descended from. The stored [`CiteFailure::origin`] is `None` when the
    /// failing cite *is* that request, and `Some(origin)` otherwise, so a caller
    /// never sees a redundant self-origin.
    fn new(prefix: &str, key: &str, message: String, origin: &(String, String)) -> Self {
        let origin = if origin.0 == prefix && origin.1 == key {
            None
        } else {
            Some(origin.clone())
        };
        CiteFailure {
            prefix: prefix.to_string(),
            key: key.to_string(),
            message,
            origin,
        }
    }
}

/// Outcome of a [`CitationManager::retrieve`] call: the citations that could
/// not be resolved. The guarantee callers rely on is that *every* requested
/// cite which ultimately fails appears here — directly (a failure whose
/// `(prefix, key)` is it) or via a chained-target failure whose
/// [`CiteFailure::origin`] points back to it.
#[derive(Clone, Debug, Default)]
pub struct RetrieveReport {
    pub failures: Vec<CiteFailure>,
}

impl RetrieveReport {
    pub fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }
}

/// The central orchestrator: owns the sources, the injected backends, and the
/// TTL policy. Generic over the four host capabilities so any of them can be
/// mixed independently.
pub struct CitationManager<F, S, C, T> {
    sources: HashMap<String, Box<dyn Source>>,
    fetcher: F,
    store: S,
    clock: C,
    timer: T,
    policy: TtlPolicy,
    /// Backoff/retry policy for the transparent retrying fetcher wrapper.
    retry_policy: RetryPolicy,
    /// Safety bound on chain length, applied both while *following* a chain in
    /// [`CitationManager::get`] and while *discovering* one in
    /// [`CitationManager::retrieve`].
    max_chain_depth: usize,
    /// Top-level CSL fields stripped from every item on its way into the store
    /// — see [`CitationManager::with_dropped_csl_fields`]. Empty by default.
    dropped_csl_fields: Vec<String>,
    /// Where progress is announced — see [`CitationManager::with_reporter`].
    /// A [`NopReporter`] by default.
    ///
    /// Unlike the four backends this is *not* a generic parameter:
    /// [`RetrieveCtx`] erases the others to `&dyn` anyway, so genericity would
    /// buy nothing at the point of use, and an `Rc` lets the host share one
    /// reporter with whatever else it builds.
    reporter: Rc<dyn Reporter>,
}

impl<F, S, C, T> CitationManager<F, S, C, T>
where
    F: Fetcher,
    S: CacheStore,
    C: Clock,
    T: Timer,
{
    /// Create a manager from the four host backends. Register sources with
    /// [`CitationManager::register`].
    pub fn new(fetcher: F, store: S, clock: C, timer: T) -> Self {
        CitationManager {
            sources: HashMap::new(),
            fetcher,
            store,
            clock,
            timer,
            policy: TtlPolicy::default(),
            retry_policy: RetryPolicy::default(),
            max_chain_depth: 16,
            dropped_csl_fields: Vec::new(),
            reporter: Rc::new(NopReporter),
        }
    }

    /// Announce progress to `reporter`. Builder-style; without it the manager is
    /// silent and every emission costs one vtable call to an empty body.
    ///
    /// ```ignore
    /// let mgr = CitationManager::new(fetcher, store, clock, timer)
    ///     .with_reporter(Rc::new(StderrReporter::new()))
    ///     .register("doi", DoiSource::new())?;
    /// ```
    ///
    /// The reporter sees the whole retrieval: pass planning, per-source chunk
    /// progress (from [`driver`](crate::driver), so third-party sources are
    /// covered too), per-citation outcomes, every HTTP request, and every point
    /// the core blocks — rate-limit pacing, retry backoff, cache compaction.
    /// It is handed to sources as [`RetrieveCtx::reporter`] and to the
    /// [`RetryingFetcher`] that wraps the host fetcher.
    ///
    /// [`Reporter::report`] is synchronous by design; see the
    /// [module docs](crate::report) for why that matters here.
    pub fn with_reporter(mut self, reporter: Rc<dyn Reporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Bind `source` to `prefix`. Builder-style, so registrations compose as
    /// `.register("arxiv", a)?.register("doi", b)?`.
    ///
    /// **The prefix is the host's choice, not the source's.** A [`Source`]
    /// declares no prefix, so the same source *type* — even two configurations
    /// of it — can serve as many prefixes as the host likes:
    ///
    /// ```ignore
    /// let mgr = CitationManager::new(fetcher, store, clock, timer)
    ///     .register("doi", DoiSource::new())?
    ///     .register("bib", BibliographyFileSource::new(["file:refs.json".into()]))?
    ///     .register("theses", BibliographyFileSource::new(["file:theses.json".into()]))?;
    /// ```
    ///
    /// The binding is a map entry: registering a prefix that is already bound
    /// **replaces** the previous source, which is how a host overrides a
    /// built-in with its own. Nothing is cached across that swap, so entries
    /// stored by the old source stay in the cache under the same ids until they
    /// expire.
    ///
    /// A source that chains to another prefix must be *configured* with it (see
    /// [`ArxivSource::chain_dois_to`]) — it cannot assume the name
    /// its target was registered under.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidPrefix`] if `prefix` is empty or contains a
    /// `':'`. Citation ids are `"prefix:key"` and are split on the *first*
    /// colon, so such a prefix would make ids ambiguous (`("a", "b:c")` and
    /// `("a:b", "c")` collide in the cache) — and an empty prefix produces
    /// `":key"`, which breaks the [`CitationManager::get_by_id`] round-trip. The
    /// prefix is host-supplied data, so a bad one is rejected as a runtime error
    /// rather than panicking.
    ///
    /// [`ArxivSource::chain_dois_to`]:
    ///     crate::source::ArxivSource::chain_dois_to
    pub fn register(
        mut self,
        prefix: impl Into<String>,
        source: impl Source + 'static,
    ) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.is_empty() || prefix.contains(':') {
            return Err(Error::InvalidPrefix(prefix));
        }
        self.sources.insert(prefix, Box::new(source));
        Ok(self)
    }

    /// Override the TTL policy. Builder-style.
    pub fn with_policy(mut self, policy: TtlPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Override the retry/backoff policy applied to every fetch. Builder-style.
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Override the maximum number of chain links to follow (default 16).
    /// Builder-style.
    ///
    /// The same bound applies to retrieval and to reading: `retrieve` refuses
    /// to *fetch* a link this crate's `get` could never *reach*.
    pub fn with_max_chain_depth(mut self, depth: usize) -> Self {
        self.max_chain_depth = depth;
        self
    }

    /// Drop these **top-level** CSL fields from every item before it is stored.
    /// Builder-style; empty by default (nothing is dropped).
    ///
    /// ```ignore
    /// let mgr = CitationManager::new(fetcher, store, clock, timer)
    ///     .with_dropped_csl_fields(["reference", "abstract"])
    ///     .register("doi", DoiSource::new())?;
    /// ```
    ///
    /// Sources hand back whatever their upstream returned, verbatim — doi.org's
    /// CSL-JSON in particular often carries a `reference` array holding the
    /// paper's *entire* bibliography, dwarfing the metadata anyone actually
    /// cites. This is the one place to throw such fields away.
    ///
    /// Two things are worth being precise about:
    ///
    /// * **When.** The fields are removed inside `retrieve`, on the way from a
    ///   source's [`Outcome`] into [`CacheStore::put`] — so they reach neither
    ///   the store's in-memory view nor its persisted file, and never come back
    ///   from `get`. Removal happens *before* the `id` is stamped on, so listing
    ///   `"id"` here cannot strip the id the entry is keyed on and echoed under.
    /// * **What.** Only top-level keys of the item, and — equally — of a chained
    ///   pointer's `set_properties`, which would otherwise re-introduce a
    ///   dropped field when it is merged over the target at read time. Nested
    ///   occurrences are untouched, as is anything a source does *internally*
    ///   with the field before returning (arXiv's DOI extraction, say).
    ///
    /// This only affects entries written from here on. Items already in a
    /// persistent cache keep their fields until they expire and are refetched;
    /// clear the cache if that matters.
    ///
    /// [`Outcome`]: crate::source::Outcome
    pub fn with_dropped_csl_fields<I, K>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        self.dropped_csl_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Canonicalize a requested key through the routed source's
    /// [`Source::normalize_key`] — applied **once, centrally**, so normalization
    /// is consistent across routing, `seen`-dedup, bucketing, storage, and
    /// lookup.
    ///
    /// The default source policy trims surrounding whitespace and lowercases, so
    /// `" 1211.1037 "`/`"1211.1037"` and `"10.1103/PhysRevA.86.052329"`/
    /// `"10.1103/physreva.86.052329"` each collapse to one cache id and one
    /// fetch. Sources whose keys are not case-insensitive identifiers narrow that
    /// (`manual` keeps its key verbatim; `arxiv`/`bib` trim only). A key whose
    /// prefix has **no registered source** has no policy to consult and is used
    /// verbatim — that unknown-prefix cite is reported exactly as it was passed
    /// in.
    ///
    /// Consequence for callers: the `id` echoed back by
    /// [`get`](Self::get) — and the `prefix`/`key` of a [`CiteFailure`] — is the
    /// *normalized* form, not the spelling that was requested.
    fn normalize_key(&self, prefix: &str, key: String) -> String {
        match self.sources.get(prefix) {
            Some(src) => src.normalize_key(&key),
            // Unknown prefix: no policy to consult, so the key is used verbatim.
            None => key,
        }
    }

    /// Populate the cache for every `(prefix, key)` in `cites` that is missing
    /// or stale, following chained pointers. Returns per-citation failures;
    /// only backend (store) errors abort with `Err`.
    ///
    /// A store error discards the report (the per-citation failures collected
    /// so far are lost) — but the buffered writes are still flushed, so a
    /// file-backed cache is never left with un-folded sidecars.
    pub async fn retrieve(&self, cites: &[(String, String)]) -> Result<RetrieveReport> {
        self.reporter.report(&Event::RetrieveStarted { cites: cites.len() });
        let mut report = RetrieveReport::default();
        let outcome = self.run_passes(cites, &mut report).await;
        // Durably compact buffered writes *even if* a pass aborted: everything
        // written before the error is still worth keeping.
        //
        // Bracketed with the wait events here rather than inside
        // `FileCacheStore`: `flush` is the only operation that locks, and giving
        // the store an `Rc<dyn Reporter>` would make it `!Send` — the
        // concurrency regression tests move stores across threads.
        self.reporter.report(&Event::WaitStarted {
            what: Wait::CacheFlush,
            expected: None,
        });
        let flushed = self.store.flush().await;
        self.reporter.report(&Event::WaitFinished {
            what: Wait::CacheFlush,
        });
        let considered = outcome?;
        flushed?;
        self.reporter.report(&Event::RetrieveFinished {
            considered,
            failed: report.failures.len(),
        });
        Ok(report)
    }

    /// The worklist loop behind [`CitationManager::retrieve`].
    ///
    /// Returns how many distinct citation ids were considered — requested ones
    /// plus every chain target pulled in — which is the denominator
    /// [`Event::RetrieveFinished`] reports.
    async fn run_passes(
        &self,
        cites: &[(String, String)],
        report: &mut RetrieveReport,
    ) -> Result<usize> {
        // id → the depth and origin (see `WorkItem`) at which it was first
        // reached. Doubles as the dedup set; the depth is what stops an
        // ill-behaved source from making `retrieve` walk an unbounded chain, and
        // the origin lets a resolution be re-attributed to the request that
        // pulled its id in. First writer wins — and since every directly-
        // requested cite is in this initial batch (depth 0, origin itself), it
        // is always recorded before any chain could reach the same id in a later
        // pass, so a requested cite is never mis-attributed to another request
        // that merely chains to it. (A target reached only via chains from two
        // *different* requests is attributed to whichever request's pass hit it
        // first.)
        let mut seen: HashMap<String, ItemMeta> = HashMap::new();
        let mut worklist: Vec<WorkItem> = cites
            .iter()
            .map(|(p, k)| {
                // Normalize the requested key up front so `origin` matches the
                // (also-normalized) coordinates a direct failure is reported
                // under — otherwise a whitespace-bearing requested key would
                // fail the `origin == (prefix, key)` self-check and be
                // attributed to itself as if it were a chained descendant.
                let key = self.normalize_key(p, k.clone());
                WorkItem {
                    origin: (p.clone(), key.clone()),
                    prefix: p.clone(),
                    key,
                    depth: 0,
                }
            })
            .collect();
        // Per-prefix start time of the most recent chunk. Carried across passes
        // so a source's `min_interval` is not reset every pass (the arXiv→DOI
        // chain guarantees at least two passes hit the `doi` source).
        let mut last_start: HashMap<String, Timestamp> = HashMap::new();
        // 1-based, for `Event::PassStarted`/`PassFinished` only.
        let mut pass = 0usize;

        while !worklist.is_empty() {
            let batch: Vec<WorkItem> = core::mem::take(&mut worklist);
            pass += 1;

            // One clock reading for the whole pass: classifying a large batch
            // against a drifting `now` would make the freshness cutoff depend
            // on a citation's position in the batch.
            let now = self.clock.now();

            // Decide, per citation, what needs fetching this pass.
            let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
            // Progress bookkeeping only: how many of this batch were served
            // from a record that did not need refetching.
            let mut cached = 0usize;
            for WorkItem {
                prefix,
                key,
                depth,
                origin,
            } in batch
            {
                // Normalize the key per the routed source's policy *before*
                // anything keys on it (the id below, `seen`-dedup, bucketing,
                // storage), so requests differing only in whitespace/case
                // collapse to one fetch. Chained targets re-entering the
                // worklist pass through here too.
                let key = self.normalize_key(&prefix, key);
                let id = csl::cite_id(&prefix, &key);
                if seen
                    .insert(
                        id.clone(),
                        ItemMeta {
                            depth,
                            origin: origin.clone(),
                        },
                    )
                    .is_some()
                {
                    continue;
                }
                if depth >= self.max_chain_depth {
                    // `get()` follows at most `max_chain_depth` links, so
                    // fetching this one could not help anyone — and without the
                    // bound a source chaining `k -> k+1` would loop forever.
                    let message = Error::Chain(alloc::format!(
                        "`{id}` is more than {} links from a requested citation",
                        self.max_chain_depth
                    ))
                    .to_string();
                    report
                        .failures
                        .push(CiteFailure::new(&prefix, &key, message, &origin));
                    continue;
                }
                if !self.sources.contains_key(&prefix) {
                    let message = Error::UnknownPrefix(prefix.clone()).to_string();
                    report
                        .failures
                        .push(CiteFailure::new(&prefix, &key, message, &origin));
                    continue;
                }

                match self.store.get(&id).await? {
                    Some(rec) => {
                        if self.policy.should_refetch(&rec, now, &id) {
                            // Expired, or stale-and-the-draw-said-refetch: bucket
                            // it. Do *not* pre-push a chained pointer's target
                            // here — the refetch may replace the pointer (a
                            // retracted or override-suppressed DOI), and fetching
                            // the old target would waste a rate-limited request
                            // and report a failure for a citation nobody asked
                            // for if that dead target 404s. `store_resolutions`
                            // pushes the *new* target when it stores the pointer.
                            buckets.entry(prefix).or_default().push(key);
                        } else {
                            cached += 1;
                            if let Payload::Chained {
                                prefix: tp,
                                key: tk,
                                ..
                            } = &rec.payload
                            {
                                // A record we are keeping (Fresh, or Stale but
                                // the draw said serve-as-is): a chained pointer
                                // we keep must still pull its target in, or a
                                // later `get()` breaks on the missing link. The
                                // target inherits this item's origin so a
                                // failure on it still points back to the same
                                // request.
                                worklist.push(WorkItem {
                                    prefix: tp.clone(),
                                    key: tk.clone(),
                                    depth: depth + 1,
                                    origin,
                                });
                            }
                        }
                    }
                    None => {
                        buckets.entry(prefix).or_default().push(key);
                    }
                }
            }

            self.reporter.report(&Event::PassStarted {
                pass,
                cached,
                to_fetch: buckets.values().map(Vec::len).sum(),
            });

            // Snapshot each source's pacing state before building the futures,
            // so the concurrent phase does not borrow `last_start`.
            let bucket_list: Vec<(String, Vec<String>, Option<Timestamp>)> = buckets
                .into_iter()
                .map(|(prefix, keys)| {
                    let last = last_start.get(&prefix).copied();
                    (prefix, keys, last)
                })
                .collect();

            // Drive every source in this pass concurrently: the `drive_source`
            // calls (one per prefix) can overlap instead of running one after
            // another. Each future borrows `&self` immutably (shared `ctx` +
            // `self.sources`); the single-threaded cooperative executor
            // interleaves their awaits, so this is data-race free even though
            // the store is interior-mutable.
            //
            // Caveat: overlap only materializes if the host backends actually
            // yield. The shipped `std` backends do not — `UreqFetcher` blocks
            // the thread and `BlockingTimer` sleeps it — so with them the
            // sources still run one after another. The win is for hosts whose
            // fetcher/timer are genuinely async (WASM `fetch()`/`setTimeout`).
            //
            // Transparently interpose the retrying fetcher: `retrying` is a
            // local that outlives the whole `buffer_unordered` pass, and every
            // source future reaches the network through `ctx.fetcher`
            // (= `&retrying`) — so retries happen without any source knowing.
            // Both `retrying` and the futures hold only shared borrows of
            // `self`, so they coexist with the interior-mutable store.
            let retrying = RetryingFetcher::new(&self.fetcher, &self.timer, self.retry_policy)
                .with_reporter(&*self.reporter);
            let source_futures = bucket_list.into_iter().map(|(prefix, keys, last)| {
                let retrying = &retrying;
                async move {
                    // Invariant, not input handling: only prefixes that passed
                    // `self.sources.contains_key` above were bucketed, so the
                    // lookup cannot miss. (Bad prefixes are rejected in
                    // `register`; unknown ones were reported and skipped.)
                    let source = self
                        .sources
                        .get(&prefix)
                        .expect("prefix presence checked above");
                    // The context is built *per bucket* rather than once per
                    // pass because it names the prefix this source was
                    // registered under — a source declares no prefix of its own,
                    // and the same source may be bound to several, so this is
                    // the only place that binding is known. Everything else in
                    // it is a shared borrow of the same pass-long locals.
                    let ctx = RetrieveCtx {
                        fetcher: retrying,
                        timer: &self.timer,
                        clock: &self.clock,
                        prefix: &prefix,
                        reporter: &*self.reporter,
                    };
                    // Keep the keys we asked for: a source that silently omits
                    // one must not leave the citation unstored *and*
                    // unreported.
                    let requested = keys.clone();
                    let (resolutions, started) =
                        drive_source(source.as_ref(), keys, &ctx, last).await;
                    (prefix, requested, resolutions, started)
                }
            });
            let results: Vec<PassResult> = iter(source_futures)
                .buffer_unordered(MAX_CONCURRENT_SOURCES)
                .collect()
                .await;

            // Apply the collected results sequentially. Keeping the store
            // writes / `worklist` / `report` updates serial keeps them simple
            // and correct; the concurrency win is in the overlap above. The
            // source is re-looked-up by prefix for `default_ttl()`.
            for (prefix, requested, resolutions, started) in results {
                if let Some(t) = started {
                    last_start.insert(prefix.clone(), t);
                }
                // Invariant, not input handling: `prefix` came from a bucket
                // built only from registered prefixes, so this lookup is
                // guaranteed to hit.
                let source = self
                    .sources
                    .get(&prefix)
                    .expect("prefix presence checked above");
                let mut sink = PassSink {
                    worklist: &mut worklist,
                    report,
                };
                self.store_resolutions(
                    &prefix,
                    source.as_ref(),
                    requested,
                    resolutions,
                    &seen,
                    &mut sink,
                )
                .await?;
            }

            // `worklist` was drained into `batch` at the top of the pass, so
            // whatever is in it now is exactly what this pass discovered — the
            // number a progress denominator has to grow by.
            self.reporter.report(&Event::PassFinished {
                pass,
                discovered: worklist.len(),
            });
        }

        Ok(seen.len())
    }

    async fn store_resolutions(
        &self,
        prefix: &str,
        source: &dyn Source,
        requested: Vec<String>,
        resolutions: Vec<Resolution>,
        seen: &HashMap<String, ItemMeta>,
        sink: &mut PassSink<'_>,
    ) -> Result<()> {
        let now = self.clock.now();
        let mut handled: HashSet<String> = HashSet::new();

        for res in resolutions {
            // Contract: exactly one `Resolution` per requested key. A duplicate
            // would double-write the store (or double-report a failure), so
            // only the first one for a key counts.
            if !handled.insert(res.key.clone()) {
                continue;
            }
            let id = csl::cite_id(prefix, &res.key);
            // Depth and origin (see `WorkItem`) of the request this id descends
            // from. It is always in `seen` (bucketing inserted it before this
            // fetch); the self-origin fallback is defensive.
            let meta = seen.get(&id).cloned().unwrap_or_else(|| ItemMeta {
                depth: 0,
                origin: (prefix.to_string(), res.key.clone()),
            });

            match res.outcome {
                Outcome::Concrete { mut csl, ttl } => {
                    // Strip the host's unwanted fields (`reference`, …) here, so
                    // they never reach the store — see
                    // `with_dropped_csl_fields`. Deliberately *before* `set_id`:
                    // a host that lists `"id"` must not be able to remove the id
                    // this entry is keyed on.
                    csl::remove_fields(&mut csl, &self.dropped_csl_fields);
                    if !csl::set_id(&mut csl, &id) {
                        // A bare array/string/number/null is not a CSL item.
                        // Storing it used to silently replace the payload with
                        // a `{"id": …}` stub and report success.
                        let err = Error::Source(alloc::format!(
                            "source returned a non-object CSL payload for `{id}`"
                        ));
                        self.note_failure(prefix, &res.key, err, now, &meta, sink)
                            .await?;
                        continue;
                    }
                    let ttl = ttl.unwrap_or_else(|| source.default_ttl());
                    let record = self
                        .policy
                        .make_record(Payload::Concrete(csl), now, ttl, &id);
                    self.store.put(&id, record).await?;
                    self.reporter.report(&Event::CiteResolved {
                        prefix,
                        key: &res.key,
                        how: Resolved::Concrete,
                    });
                }
                Outcome::Chained {
                    prefix: tp,
                    key: tk,
                    mut set_properties,
                } => {
                    // Normalize the target key by the *target* source's policy so
                    // the stored pointer references exactly the id its target
                    // will land under (the worklist push below normalizes it
                    // again at the loop top — the policy is idempotent): a
                    // chained `doi` key with incidental whitespace, or in the
                    // DOI's registered mixed case, must not spawn a duplicate
                    // entry. This is why a source emitting a chain pointer needs
                    // no case/whitespace handling of its own.
                    let tk = self.normalize_key(&tp, tk);
                    if tp == prefix && tk == res.key {
                        // Would otherwise be stored happily and only surface
                        // as "chain too deep" `max_chain_depth` reads later.
                        let err = Error::Chain(alloc::format!("`{id}` chains to itself"));
                        self.note_failure(prefix, &res.key, err, now, &meta, sink)
                            .await?;
                        continue;
                    }
                    // A pointer's `set_properties` override the concrete target
                    // at read time, so a dropped field left in here would come
                    // straight back out of `get`.
                    csl::remove_fields(&mut set_properties, &self.dropped_csl_fields);
                    let payload = Payload::Chained {
                        prefix: tp.clone(),
                        key: tk.clone(),
                        set_properties,
                    };
                    let record = self
                        .policy
                        .make_record(payload, now, source.default_ttl(), &id);
                    self.store.put(&id, record).await?;
                    self.reporter.report(&Event::CiteResolved {
                        prefix,
                        key: &res.key,
                        how: Resolved::Chained {
                            prefix: &tp,
                            key: &tk,
                        },
                    });
                    // The newly discovered target inherits this item's origin, so
                    // a failure further down the chain still points back to the
                    // original request.
                    sink.worklist.push(WorkItem {
                        prefix: tp,
                        key: tk,
                        depth: meta.depth + 1,
                        origin: meta.origin,
                    });
                }
                Outcome::Failed(err) => {
                    self.note_failure(prefix, &res.key, err, now, &meta, sink)
                        .await?;
                }
                Outcome::Missing(err) => {
                    self.note_missing(prefix, &res.key, err, &meta, sink)
                        .await?;
                }
            }
        }

        // A key the source never answered: the manager matches resolutions by
        // `res.key`, so without this the citation would be neither stored nor
        // reported and `retrieve` would claim success for something `get` can
        // never return.
        for key in requested {
            if handled.contains(&key) {
                continue;
            }
            let id = csl::cite_id(prefix, &key);
            let meta = seen.get(&id).cloned().unwrap_or_else(|| ItemMeta {
                depth: 0,
                origin: (prefix.to_string(), key.clone()),
            });
            let err = Error::Source(alloc::format!(
                "source `{prefix}` returned no resolution for key `{key}`"
            ));
            self.note_failure(prefix, &key, err, now, &meta, sink)
                .await?;
        }

        Ok(())
    }

    /// Record a per-citation failure — unless a cached copy is still within the
    /// grace window, in which case the stale entry keeps being used and nothing
    /// is reported (stale-while-revalidate).
    async fn note_failure(
        &self,
        prefix: &str,
        key: &str,
        err: Error,
        now: Timestamp,
        meta: &ItemMeta,
        sink: &mut PassSink<'_>,
    ) -> Result<()> {
        let id = csl::cite_id(prefix, key);
        let kept = match self.store.get(&id).await? {
            Some(rec) if self.policy.usable_within_grace(&rec, now) => Some(rec),
            _ => None,
        };
        // Announced either way: a grace-served failure never reaches the
        // `RetrieveReport`, so without this the one moment stale-while-
        // revalidate actually kicks in would be entirely invisible.
        self.reporter.report(&Event::CiteFailed {
            prefix,
            key,
            err: &err,
            grace_served: kept.is_some(),
        });
        match kept {
            // Keeping a chained pointer alive means its target must be present
            // too, otherwise `get()` breaks on the next link. Nothing else
            // pushes it: the pointer was stale, so it was not pre-pushed during
            // bucketing. The target inherits this item's depth+origin.
            Some(rec) => {
                if let Payload::Chained {
                    prefix: tp,
                    key: tk,
                    ..
                } = rec.payload
                {
                    sink.worklist.push(WorkItem {
                        prefix: tp,
                        key: tk,
                        depth: meta.depth + 1,
                        origin: meta.origin.clone(),
                    });
                }
            }
            None => sink.report.failures.push(CiteFailure::new(
                prefix,
                key,
                err.to_string(),
                &meta.origin,
            )),
        }
        Ok(())
    }

    /// Record an *authoritative* "no such key" from a reachable source.
    ///
    /// Unlike [`note_failure`](Self::note_failure), this **always** reports and
    /// **never** consults the grace window: a `Missing` result means the source
    /// answered and the id is genuinely gone, so a still-cached copy is now
    /// known to be wrong. That stale entry is therefore also removed, so a later
    /// [`get`](Self::get) errors instead of serving now-invalid data — rather
    /// than the entry lingering (and being grace-served / re-reported) for the
    /// whole 14-day window. The removed entry may be a chain target of some
    /// other citation; that citation's `get` then fails on the dead link, which
    /// is correct — the target really no longer resolves.
    async fn note_missing(
        &self,
        prefix: &str,
        key: &str,
        err: Error,
        meta: &ItemMeta,
        sink: &mut PassSink<'_>,
    ) -> Result<()> {
        let id = csl::cite_id(prefix, key);
        self.store.remove(&id).await?;
        // Never grace-served: `Missing` is authoritative.
        self.reporter.report(&Event::CiteFailed {
            prefix,
            key,
            err: &err,
            grace_served: false,
        });
        sink.report.failures.push(CiteFailure::new(
            prefix,
            key,
            err.to_string(),
            &meta.origin,
        ));
        Ok(())
    }

    /// Read a resolved CSL-JSON item, following chain pointers and merging
    /// their `set_properties`. Accumulated `set_properties` **override** the
    /// concrete target's fields (and among themselves the set closest to the
    /// request wins).
    ///
    /// The returned item's `id` is the requested `"prefix:key"` with the key
    /// **normalized** by the routed source's [`Source::normalize_key`] — the
    /// same canonical form `retrieve` stored it under. For a source using the
    /// default policy that means trimmed and lowercased, so
    /// `get("doi", "10.1103/PhysRevA.86.052329")` returns
    /// `"id": "doi:10.1103/physreva.86.052329"`. (Only the cache *id* is
    /// canonicalized; CSL fields — including `DOI` — keep their payload casing.)
    pub async fn get(&self, prefix: &str, key: &str) -> Result<CslValue> {
        // Normalize the key the same way `retrieve` did, so `get("arxiv",
        // "1211.1037 ")` finds the entry stored under the canonical id. An
        // unknown prefix has no policy to consult, so its key is left verbatim.
        let key = self.normalize_key(prefix, key.to_string());
        let requested_id = csl::cite_id(prefix, &key);
        let mut current_id = requested_id.clone();
        // Properties accumulated along the chain; earlier (closer to the
        // request) ones take precedence.
        let mut accumulated = CslValue::Object(serde_json::Map::new());

        for _ in 0..self.max_chain_depth {
            let rec = match self.store.get(&current_id).await? {
                Some(rec) => rec,
                // Name both ids: a missing *chain target* is not a citation the
                // caller ever asked for, so reporting only its id is confusing.
                None if current_id == requested_id => {
                    return Err(Error::NotFound(requested_id));
                }
                None => {
                    return Err(Error::Chain(alloc::format!(
                        "`{requested_id}` chains to `{current_id}`, which is not in the cache"
                    )));
                }
            };

            match rec.payload {
                Payload::Concrete(mut csl) => {
                    // Accumulated `set_properties` override the concrete target
                    // (both reference impls do `{ ...target, ...set_properties }`).
                    csl::merge_over(&mut csl, &accumulated);
                    // Forced last, so the requested id always wins even if a
                    // `set_properties` carried an `id` of its own.
                    csl::set_id(&mut csl, &requested_id);
                    return Ok(csl);
                }
                Payload::Chained {
                    prefix: tp,
                    key: tk,
                    set_properties,
                } => {
                    // Accumulation only: a set already seen (closer to the
                    // request) wins over this further one, so merge the new set
                    // in as defaults *under* it. The accumulated whole then
                    // overrides the concrete target at the `Concrete` arm above.
                    csl::merge_defaults(&mut accumulated, &set_properties);
                    // Normalize the hop the same way it was normalized when
                    // stored, so a pointer written before this policy existed (or
                    // by a source that emitted the target's key in some other
                    // spelling) still lands on the canonical target id.
                    let tk = self.normalize_key(&tp, tk);
                    let next_id = csl::cite_id(&tp, &tk);
                    if next_id == current_id {
                        return Err(Error::Chain(alloc::format!(
                            "`{current_id}` chains to itself"
                        )));
                    }
                    current_id = next_id;
                }
            }
        }

        Err(Error::Chain(alloc::format!(
            "chain from `{requested_id}` is longer than the {} link limit",
            self.max_chain_depth
        )))
    }

    /// Convenience: read by full `"prefix:key"` id.
    pub async fn get_by_id(&self, id: &str) -> Result<CslValue> {
        match id.split_once(':') {
            Some((prefix, key)) => self.get(prefix, key).await,
            None => Err(Error::InvalidId(id.to_string())),
        }
    }

    /// Drop hard-expired entries that are also past their grace window.
    pub async fn prune(&self) -> Result<usize> {
        let now = self.clock.now();
        let mut removed = 0;
        for (id, rec) in self.store.entries().await? {
            // `grace` is non-negative, so being past `expires + grace` already
            // implies `classify(..) == Expired`; testing both would be
            // redundant.
            if !self.policy.usable_within_grace(&rec, now) {
                self.store.remove(&id).await?;
                removed += 1;
            }
        }
        // Durably compact the removals before returning.
        self.reporter.report(&Event::WaitStarted {
            what: Wait::CacheFlush,
            expected: None,
        });
        let flushed = self.store.flush().await;
        self.reporter.report(&Event::WaitFinished {
            what: Wait::CacheFlush,
        });
        flushed?;
        Ok(removed)
    }

    /// Access the underlying store (for advanced callers/tests).
    pub fn store(&self) -> &S {
        &self.store
    }
}
