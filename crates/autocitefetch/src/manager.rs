//! [`CitationManager`] — routing, the retrieval driver, and chain resolution.

use alloc::boxed::Box;
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

/// A citation still to be looked at this retrieval: `(prefix, key, depth)`,
/// where `depth` is how many chain links away from a *requested* citation it
/// is (0 for the ones the caller asked for).
type WorkItem = (String, String, usize);

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
#[derive(Clone, Debug)]
pub struct CiteFailure {
    pub prefix: String,
    pub key: String,
    pub message: String,
}

/// Outcome of a [`CitationManager::retrieve`] call.
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
        }
    }

    /// Register a source under its declared prefix. Builder-style, so chains
    /// compose as `.register(a)?.register(b)?`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidPrefix`] if the source's prefix is empty or
    /// contains a `':'`. Citation ids are `"prefix:key"` and are split on the
    /// *first* colon, so such a prefix would make ids ambiguous (`("a", "b:c")`
    /// and `("a:b", "c")` collide in the cache) — and an empty prefix produces
    /// `":key"`, which breaks the [`CitationManager::get_by_id`] round-trip. The
    /// prefix is host-supplied data, so a bad one is rejected as a runtime error
    /// rather than panicking.
    pub fn register(mut self, source: impl Source + 'static) -> Result<Self> {
        let prefix = source.prefix();
        if prefix.is_empty() || prefix.contains(':') {
            return Err(Error::InvalidPrefix(prefix.to_string()));
        }
        self.sources.insert(prefix.to_string(), Box::new(source));
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

    /// Normalize a requested key according to the routed source's whitespace
    /// policy — applied **once, centrally**, so trimming is consistent across
    /// routing, `seen`-dedup, bucketing, storage, and lookup.
    ///
    /// A source that declares [`Source::trim_key_whitespace`] (the default) has
    /// stray leading/trailing whitespace stripped here, so `" 1211.1037 "` and
    /// `"1211.1037"` collapse to one cache id and one fetch. A source that opts
    /// out (e.g. `manual`, whose key *is* free-form citation text) keeps its key
    /// verbatim — as does a key whose prefix has no registered source, since
    /// there is no policy to consult (that unknown-prefix cite is reported
    /// unchanged).
    fn normalize_key(&self, prefix: &str, key: String) -> String {
        match self.sources.get(prefix) {
            Some(src) if src.trim_key_whitespace() => {
                let trimmed = key.trim();
                // Skip the reallocation when nothing was trimmed (the common case).
                if trimmed.len() == key.len() {
                    key
                } else {
                    trimmed.to_string()
                }
            }
            _ => key,
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
        let mut report = RetrieveReport::default();
        let outcome = self.run_passes(cites, &mut report).await;
        // Durably compact buffered writes *even if* a pass aborted: everything
        // written before the error is still worth keeping.
        let flushed = self.store.flush().await;
        outcome?;
        flushed?;
        Ok(report)
    }

    /// The worklist loop behind [`CitationManager::retrieve`].
    async fn run_passes(
        &self,
        cites: &[(String, String)],
        report: &mut RetrieveReport,
    ) -> Result<()> {
        // id → the chain depth at which it was first requested. Doubles as the
        // dedup set; the depth is what stops an ill-behaved source from making
        // `retrieve` walk an unbounded chain.
        let mut depths: HashMap<String, usize> = HashMap::new();
        let mut worklist: Vec<WorkItem> = cites
            .iter()
            .map(|(p, k)| (p.clone(), k.clone(), 0))
            .collect();
        // Per-prefix start time of the most recent chunk. Carried across passes
        // so a source's `min_interval` is not reset every pass (the arXiv→DOI
        // chain guarantees at least two passes hit the `doi` source).
        let mut last_start: HashMap<String, Timestamp> = HashMap::new();

        while !worklist.is_empty() {
            let batch: Vec<WorkItem> = core::mem::take(&mut worklist);

            // One clock reading for the whole pass: classifying a large batch
            // against a drifting `now` would make the freshness cutoff depend
            // on a citation's position in the batch.
            let now = self.clock.now();

            // Decide, per citation, what needs fetching this pass.
            let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
            for (prefix, key, depth) in batch {
                // Trim the key per the routed source's policy *before* anything
                // keys on it (the id below, `seen`-dedup, bucketing, storage),
                // so whitespace-only-different requests collapse to one fetch.
                // Chained targets re-entering the worklist pass through here too.
                let key = self.normalize_key(&prefix, key);
                let id = csl::cite_id(&prefix, &key);
                if depths.insert(id.clone(), depth).is_some() {
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
                    report.failures.push(CiteFailure {
                        prefix,
                        key,
                        message,
                    });
                    continue;
                }
                if !self.sources.contains_key(&prefix) {
                    report.failures.push(CiteFailure {
                        prefix: prefix.clone(),
                        key,
                        message: Error::UnknownPrefix(prefix).to_string(),
                    });
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
                        } else if let Payload::Chained {
                            prefix: tp,
                            key: tk,
                            ..
                        } = &rec.payload
                        {
                            // A record we are keeping (Fresh, or Stale but the
                            // draw said serve-as-is): a chained pointer we keep
                            // must still pull its target in, or a later `get()`
                            // breaks on the missing link.
                            worklist.push((tp.clone(), tk.clone(), depth + 1));
                        }
                    }
                    None => {
                        buckets.entry(prefix).or_default().push(key);
                    }
                }
            }

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
            // Transparently interpose the retrying fetcher: `retrying` and
            // `ctx` are locals that outlive the whole `buffer_unordered` pass,
            // and every source future reaches the network through
            // `ctx.fetcher` (= `&retrying`) — so retries happen without any
            // source knowing. All of `retrying`, `ctx`, and the futures hold
            // only shared borrows of `self`, so they coexist with the
            // interior-mutable store just like the original shared `ctx` did.
            let retrying = RetryingFetcher::new(&self.fetcher, &self.timer, self.retry_policy);
            let ctx = RetrieveCtx {
                fetcher: &retrying,
                timer: &self.timer,
                clock: &self.clock,
            };
            let source_futures = bucket_list.into_iter().map(|(prefix, keys, last)| {
                let ctx = &ctx;
                async move {
                    // Invariant, not input handling: only prefixes that passed
                    // `self.sources.contains_key` above were bucketed, so the
                    // lookup cannot miss. (Bad prefixes are rejected in
                    // `register`; unknown ones were reported and skipped.)
                    let source = self
                        .sources
                        .get(&prefix)
                        .expect("prefix presence checked above");
                    // Keep the keys we asked for: a source that silently omits
                    // one must not leave the citation unstored *and*
                    // unreported.
                    let requested = keys.clone();
                    let (resolutions, started) =
                        drive_source(source.as_ref(), keys, ctx, last).await;
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
                    &depths,
                    &mut sink,
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn store_resolutions(
        &self,
        prefix: &str,
        source: &dyn Source,
        requested: Vec<String>,
        resolutions: Vec<Resolution>,
        depths: &HashMap<String, usize>,
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
            let depth = depths.get(&id).copied().unwrap_or(0);

            match res.outcome {
                Outcome::Concrete { mut csl, ttl } => {
                    if !csl::set_id(&mut csl, &id) {
                        // A bare array/string/number/null is not a CSL item.
                        // Storing it used to silently replace the payload with
                        // a `{"id": …}` stub and report success.
                        let err = Error::Source(alloc::format!(
                            "source returned a non-object CSL payload for `{id}`"
                        ));
                        self.note_failure(prefix, &res.key, err, now, depth, sink)
                            .await?;
                        continue;
                    }
                    let ttl = ttl.unwrap_or_else(|| source.default_ttl());
                    let record = self
                        .policy
                        .make_record(Payload::Concrete(csl), now, ttl, &id);
                    self.store.put(&id, record).await?;
                }
                Outcome::Chained {
                    prefix: tp,
                    key: tk,
                    set_properties,
                } => {
                    // Normalize the target key by the *target* source's policy so
                    // the stored pointer references exactly the id its target
                    // will land under (the worklist push below trims it again at
                    // the loop top): a chained `doi` key with incidental
                    // whitespace must not spawn a duplicate entry.
                    let tk = self.normalize_key(&tp, tk);
                    if tp == prefix && tk == res.key {
                        // Would otherwise be stored happily and only surface
                        // as "chain too deep" `max_chain_depth` reads later.
                        let err = Error::Chain(alloc::format!("`{id}` chains to itself"));
                        self.note_failure(prefix, &res.key, err, now, depth, sink)
                            .await?;
                        continue;
                    }
                    let payload = Payload::Chained {
                        prefix: tp.clone(),
                        key: tk.clone(),
                        set_properties,
                    };
                    let record = self
                        .policy
                        .make_record(payload, now, source.default_ttl(), &id);
                    self.store.put(&id, record).await?;
                    sink.worklist.push((tp, tk, depth + 1));
                }
                Outcome::Failed(err) => {
                    self.note_failure(prefix, &res.key, err, now, depth, sink)
                        .await?;
                }
                Outcome::Missing(err) => {
                    self.note_missing(prefix, &res.key, err, sink).await?;
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
            let depth = depths.get(&id).copied().unwrap_or(0);
            let err = Error::Source(alloc::format!(
                "source `{prefix}` returned no resolution for key `{key}`"
            ));
            self.note_failure(prefix, &key, err, now, depth, sink)
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
        depth: usize,
        sink: &mut PassSink<'_>,
    ) -> Result<()> {
        let id = csl::cite_id(prefix, key);
        let kept = match self.store.get(&id).await? {
            Some(rec) if self.policy.usable_within_grace(&rec, now) => Some(rec),
            _ => None,
        };
        match kept {
            // Keeping a chained pointer alive means its target must be present
            // too, otherwise `get()` breaks on the next link. Nothing else
            // pushes it: the pointer was stale, so it was not pre-pushed during
            // bucketing.
            Some(rec) => {
                if let Payload::Chained {
                    prefix: tp,
                    key: tk,
                    ..
                } = rec.payload
                {
                    sink.worklist.push((tp, tk, depth + 1));
                }
            }
            None => sink.report.failures.push(CiteFailure {
                prefix: prefix.to_string(),
                key: key.to_string(),
                message: err.to_string(),
            }),
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
        sink: &mut PassSink<'_>,
    ) -> Result<()> {
        let id = csl::cite_id(prefix, key);
        self.store.remove(&id).await?;
        sink.report.failures.push(CiteFailure {
            prefix: prefix.to_string(),
            key: key.to_string(),
            message: err.to_string(),
        });
        Ok(())
    }

    /// Read a resolved CSL-JSON item, following chain pointers and merging
    /// their `set_properties`. Accumulated `set_properties` **override** the
    /// concrete target's fields (and among themselves the set closest to the
    /// request wins). The returned item's `id` is always the originally
    /// requested `"prefix:key"`.
    pub async fn get(&self, prefix: &str, key: &str) -> Result<CslValue> {
        // Trim the key the same way `retrieve` did, so `get("arxiv",
        // "1211.1037 ")` finds the entry stored under the trimmed id. An unknown
        // prefix has no policy to consult, so its key is left verbatim.
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
                    // by a source that emitted incidental whitespace) still lands
                    // on the trimmed target id.
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
        self.store.flush().await?;
        Ok(removed)
    }

    /// Access the underlying store (for advanced callers/tests).
    pub fn store(&self) -> &S {
        &self.store
    }
}
