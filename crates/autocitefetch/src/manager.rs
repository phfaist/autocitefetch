//! [`CitationManager`] — routing, the retrieval driver, and chain resolution.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use futures_util::stream::{StreamExt, iter};
use hashbrown::{HashMap, HashSet};

use crate::cache::{Freshness, TtlPolicy};
use crate::csl::{self, CslValue};
use crate::driver::drive_source;
use crate::env::{Clock, Timer};
use crate::error::{Error, Result};
use crate::fetch::Fetcher;
use crate::retry::{RetryPolicy, RetryingFetcher};
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::store::{CacheStore, Payload};

/// Upper bound on how many sources' `drive_source` calls run concurrently
/// within a single retrieval pass. Keeps a large fan-out from issuing an
/// unbounded number of in-flight fetches at once.
const MAX_CONCURRENT_SOURCES: usize = 8;

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
    /// Safety bound on chain length while resolving.
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

    /// Register a source under its declared prefix. Builder-style.
    pub fn register(mut self, source: impl Source + 'static) -> Self {
        self.sources
            .insert(source.prefix().to_string(), Box::new(source));
        self
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

    /// Populate the cache for every `(prefix, key)` in `cites` that is missing
    /// or stale, following chained pointers. Returns per-citation failures;
    /// only backend (store) errors abort with `Err`.
    pub async fn retrieve(&self, cites: &[(String, String)]) -> Result<RetrieveReport> {
        let mut report = RetrieveReport::default();
        let mut seen: HashSet<String> = HashSet::new();
        let mut worklist: Vec<(String, String)> = cites.to_vec();

        while !worklist.is_empty() {
            let batch: Vec<(String, String)> = core::mem::take(&mut worklist);

            // Decide, per citation, what needs fetching this pass.
            let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
            for (prefix, key) in batch {
                let id = csl::cite_id(&prefix, &key);
                if !seen.insert(id.clone()) {
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
                        // A cached chained entry: make sure its target is present.
                        if let Payload::Chained {
                            prefix: tp,
                            key: tk,
                            ..
                        } = &rec.payload
                        {
                            worklist.push((tp.clone(), tk.clone()));
                        }
                        match self.policy.classify(&rec, self.clock.now()) {
                            Freshness::Fresh => {}
                            Freshness::Stale | Freshness::Expired => {
                                buckets.entry(prefix).or_default().push(key);
                            }
                        }
                    }
                    None => {
                        buckets.entry(prefix).or_default().push(key);
                    }
                }
            }

            // Drive every source in this pass concurrently: the network-bound
            // `drive_source` calls (one per prefix) now overlap instead of
            // running one after another. Each future borrows `&self`
            // immutably (shared `ctx` + `self.sources`); the single-threaded
            // cooperative executor interleaves their awaits, so this is data-
            // race free even though the store is interior-mutable.
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
            let source_futures = buckets.into_iter().map(|(prefix, keys)| {
                let ctx = &ctx;
                async move {
                    let source = self
                        .sources
                        .get(&prefix)
                        .expect("prefix presence checked above");
                    let resolutions = drive_source(source.as_ref(), keys, ctx).await;
                    (prefix, resolutions)
                }
            });
            let results: Vec<(String, Vec<Resolution>)> = iter(source_futures)
                .buffer_unordered(MAX_CONCURRENT_SOURCES)
                .collect()
                .await;

            // Apply the collected results sequentially. Keeping the store
            // writes / `worklist` / `report` updates serial keeps them simple
            // and correct; the concurrency win is in the overlap above. The
            // source is re-looked-up by prefix for `default_ttl()`.
            for (prefix, resolutions) in results {
                let source = self
                    .sources
                    .get(&prefix)
                    .expect("prefix presence checked above");
                self.store_resolutions(
                    &prefix,
                    source.as_ref(),
                    resolutions,
                    &mut worklist,
                    &mut report,
                )
                .await?;
            }
        }

        // Durably compact any buffered writes before returning.
        self.store.flush().await?;
        Ok(report)
    }

    async fn store_resolutions(
        &self,
        prefix: &str,
        source: &dyn Source,
        resolutions: Vec<crate::source::Resolution>,
        worklist: &mut Vec<(String, String)>,
        report: &mut RetrieveReport,
    ) -> Result<()> {
        let now = self.clock.now();
        for res in resolutions {
            let id = csl::cite_id(prefix, &res.key);
            match res.outcome {
                Outcome::Concrete { mut csl, ttl } => {
                    csl::set_id(&mut csl, &id);
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
                    let payload = Payload::Chained {
                        prefix: tp.clone(),
                        key: tk.clone(),
                        set_properties,
                    };
                    let record = self
                        .policy
                        .make_record(payload, now, source.default_ttl(), &id);
                    self.store.put(&id, record).await?;
                    worklist.push((tp, tk));
                }
                Outcome::Failed(err) => {
                    // Graceful degradation: if a stale/expired copy is still
                    // within the grace window, keep using it and don't report.
                    let keep = match self.store.get(&id).await? {
                        Some(rec) => self.policy.usable_within_grace(&rec, now),
                        None => false,
                    };
                    if !keep {
                        report.failures.push(CiteFailure {
                            prefix: prefix.to_string(),
                            key: res.key,
                            message: err.to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Read a resolved CSL-JSON item, following chain pointers and merging
    /// their `set_properties`. The returned item's `id` is the originally
    /// requested `"prefix:key"`.
    pub async fn get(&self, prefix: &str, key: &str) -> Result<CslValue> {
        let requested_id = csl::cite_id(prefix, key);
        let mut current_id = requested_id.clone();
        // Properties accumulated along the chain; earlier (closer to the
        // request) ones take precedence.
        let mut accumulated = CslValue::Object(serde_json::Map::new());

        for _ in 0..self.max_chain_depth {
            let rec = self
                .store
                .get(&current_id)
                .await?
                .ok_or_else(|| Error::NotFound(current_id.clone()))?;

            match rec.payload {
                Payload::Concrete(mut csl) => {
                    csl::merge_defaults(&mut csl, &accumulated);
                    csl::set_id(&mut csl, &requested_id);
                    return Ok(csl);
                }
                Payload::Chained {
                    prefix: tp,
                    key: tk,
                    set_properties,
                } => {
                    // already-seen wins ⇒ merge the new set as defaults under it
                    csl::merge_defaults(&mut accumulated, &set_properties);
                    current_id = csl::cite_id(&tp, &tk);
                }
            }
        }

        Err(Error::Source(alloc::format!(
            "citation chain too deep resolving `{requested_id}`"
        )))
    }

    /// Convenience: read by full `"prefix:key"` id.
    pub async fn get_by_id(&self, id: &str) -> Result<CslValue> {
        match id.split_once(':') {
            Some((prefix, key)) => self.get(prefix, key).await,
            None => Err(Error::UnknownPrefix(id.to_string())),
        }
    }

    /// Drop hard-expired entries that are also past their grace window.
    pub async fn prune(&self) -> Result<usize> {
        let now = self.clock.now();
        let mut removed = 0;
        for (id, rec) in self.store.entries().await? {
            if !self.policy.usable_within_grace(&rec, now)
                && self.policy.classify(&rec, now) == Freshness::Expired
            {
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
