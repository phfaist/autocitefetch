//! Refresh batching: shaping a source's refetch work into worthwhile requests.
//!
//! [`TtlPolicy::should_refetch`] decides, *per entry*, whether a cached record
//! wants refreshing. On a large citation database that trickles: each
//! `retrieve` finds a handful of entries that went stale since the last run, and
//! refetching them costs a whole rate-limited request apiece (arXiv answers 100
//! ids in the same single request it takes for 3). This module sits between
//! that per-entry decision and the [`driver`](crate::driver) and decides, *per
//! source*, what actually goes on the wire:
//!
//! * too little deferrable work to be worth a request ⇒ **defer** it (the stale
//!   copies keep being served), or
//! * **top up**: pull entries that are not due yet, but getting there, forward
//!   into the same request(s).
//!
//! Every requested entry falls in one [`RefreshClass`]. `Required` work (cache
//! misses, and entries hard-expired for longer than
//! [`RefreshBatching::max_defer`]) is never deferred, and a deferred entry only
//! grows older until it becomes `Required` — so nothing waits forever.
//!
//! [`plan`] is a pure function of the classified keys, so the whole decision is
//! testable without a store, clock or source. The policy is per *prefix*: a
//! [`Source`](crate::source::Source) declares a default through
//! [`Source::refresh_batching`](crate::source::Source::refresh_batching), and
//! the host may override it with
//! [`CitationManager::with_refresh_batching`](crate::manager::CitationManager::with_refresh_batching).

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::cache::{Freshness, TtlPolicy};
use crate::env::{Timestamp, millis_i64};
use crate::store::CacheRecord;

/// How a source's refresh work is shaped into requests.
///
/// The [`Default`] is [`RefreshBatching::EAGER`]: fetch exactly what
/// [`TtlPolicy::should_refetch`] asks for, when it asks — the behavior of a
/// source that declares nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RefreshBatching {
    /// Minimum number of keys worth waking this source for when nothing is
    /// [`Required`](RefreshClass::Required). Below it, the `Due` entries are
    /// deferred — or, with [`top_up`](Self::top_up), topped up to this many
    /// from the `Eligible` ones if there are enough. `0` never defers.
    pub min_batch: usize,
    /// How long past its hard expiry an entry may still be deferred (served
    /// from cache, not reported). Beyond it the entry is `Required`. `0` ⇒ hard
    /// expiry is always `Required`.
    ///
    /// Effectively capped at [`TtlPolicy::grace`], so an entry is never
    /// deferred past the point where
    /// [`prune`](crate::manager::CitationManager::prune) may drop it.
    pub max_defer: Duration,
    /// Pull not-yet-due entries forward into a request. `None` ⇒ never.
    pub top_up: Option<TopUp>,
}

impl RefreshBatching {
    /// Fetch what is due, when it is due; never defer, never top up.
    pub const EAGER: RefreshBatching = RefreshBatching {
        min_batch: 0,
        max_defer: Duration::ZERO,
        top_up: None,
    };
}

impl Default for RefreshBatching {
    fn default() -> Self {
        RefreshBatching::EAGER
    }
}

/// Which not-yet-due entries may be pulled forward, and how many.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TopUp {
    /// Only entries at least this far through their lifetime are
    /// [`Eligible`](RefreshClass::Eligible), so something fetched yesterday is
    /// never refreshed. Measured against the source's nominal
    /// [`default_ttl`](crate::source::Source::default_ttl): an entry qualifies
    /// once its remaining life is at most `(100 − min_age_percent)`% of it.
    /// `0` makes every kept entry eligible. Clamped to `<= 100`.
    pub min_age_percent: u32,
    /// How many eligible entries to add once a request is going out anyway.
    pub fill: Fill,
}

/// How far a [`TopUp`] fills a request that is going out anyway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fill {
    /// Fill the last partial chunk. Free for a batched source (arXiv: 100 ids
    /// cost the same one request as 3); with an unbounded chunk (a bibliography
    /// file) it takes every eligible entry.
    ChunkBoundary,
    /// Add up to this many eligible entries. For per-key sources (doi.org),
    /// where each one is its own request. `Extra(0)` tops up only as far as
    /// needed to reach [`RefreshBatching::min_batch`].
    Extra(usize),
}

/// Where a *cached* entry stands for refresh batching. (A cache miss is always
/// [`Required`](Self::Required) and needs no classification.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshClass {
    /// Must be fetched this pass.
    Required,
    /// Wants refreshing, but may wait for a worthwhile batch.
    Due,
    /// Not due, but old enough to be pulled forward by a [`TopUp`].
    Eligible,
    /// Served as-is; not a candidate.
    Keep,
}

impl RefreshBatching {
    /// Classify a cached record at `now`.
    ///
    /// * Hard-expired for at least `max_defer` (capped at `ttl.grace`) ⇒
    ///   `Required`; for less ⇒ `Due`.
    /// * Stale and [`TtlPolicy::should_refetch`] draws "refetch" ⇒ `Due`.
    /// * Otherwise `Eligible` if [`top_up`](Self::top_up) is set and the entry
    ///   is old enough (see [`TopUp::min_age_percent`]), else `Keep`.
    ///
    /// `nominal_ttl` is the source's
    /// [`default_ttl`](crate::source::Source::default_ttl).
    pub fn classify(
        &self,
        ttl: &TtlPolicy,
        record: &CacheRecord,
        now: Timestamp,
        id: &str,
        nominal_ttl: Duration,
    ) -> RefreshClass {
        if ttl.classify(record, now) == Freshness::Expired {
            let overdue = now.as_millis().saturating_sub(record.expires.as_millis());
            let max_defer = millis_i64(self.max_defer.min(ttl.grace));
            return if overdue >= max_defer {
                RefreshClass::Required
            } else {
                RefreshClass::Due
            };
        }
        if ttl.should_refetch(record, now, id) {
            return RefreshClass::Due;
        }
        match self.top_up {
            Some(t) if is_old_enough(t.min_age_percent, record, now, nominal_ttl) => {
                RefreshClass::Eligible
            }
            _ => RefreshClass::Keep,
        }
    }
}

/// Whether a not-expired record's remaining life is within the last
/// `(100 − min_age_percent)`% of `nominal_ttl`.
fn is_old_enough(min_age_percent: u32, record: &CacheRecord, now: Timestamp, nominal_ttl: Duration) -> bool {
    let min_age_percent = min_age_percent.min(100);
    if min_age_percent == 0 {
        // Checked separately: TTL jitter can leave a just-fetched entry with
        // *more* than `nominal_ttl` to live, and 0% means "any age".
        return true;
    }
    let remaining = record.expires.as_millis().saturating_sub(now.as_millis());
    let horizon = (millis_i64(nominal_ttl) as i128 * (100 - min_age_percent) as i128) / 100;
    (remaining as i128) <= horizon
}

/// What [`plan`] decided for one source in one pass.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    /// Keys to fetch, in order: `Required`, then `Due`, then the pulled-forward
    /// `Eligible` ones, most urgent first.
    pub fetch: Vec<String>,
    /// `Due` keys left for a later run (served from cache meanwhile).
    pub deferred: usize,
    /// `Eligible` keys pulled forward into this pass.
    pub pulled_forward: usize,
}

/// Decide what one source fetches this pass.
///
/// `eligible` must already be sorted most urgent first; the leading ones are
/// taken. The rule:
///
/// 1. Nothing `Required`, some `Due` but fewer than `min_batch`: top up to
///    `min_batch` from `eligible` if there are enough (and `top_up` is set),
///    then [`Fill`] on top; otherwise fetch **nothing** — every `Due` key is
///    deferred.
/// 2. Otherwise a request is going out anyway: fetch `required + due`, then
///    [`Fill`] from `eligible`.
///
/// Eligible entries are never fetched on their own: with nothing `Required` or
/// `Due` the plan is empty, so a run with no real work does not start
/// refreshing the database ahead of time.
pub fn plan(
    policy: &RefreshBatching,
    chunk_size: usize,
    required: Vec<String>,
    due: Vec<String>,
    eligible: Vec<String>,
) -> Plan {
    let mut fetch = required;
    let topping_to_min = fetch.is_empty() && !due.is_empty() && due.len() < policy.min_batch;
    if topping_to_min {
        let enough = policy.top_up.is_some() && due.len() + eligible.len() >= policy.min_batch;
        if !enough {
            return Plan {
                fetch: Vec::new(),
                deferred: due.len(),
                pulled_forward: 0,
            };
        }
    }
    fetch.extend(due);
    let Some(top_up) = policy.top_up else {
        return Plan {
            fetch,
            ..Plan::default()
        };
    };
    if fetch.is_empty() {
        return Plan::default();
    }

    let base = fetch.len();
    let floor = if topping_to_min { policy.min_batch } else { base };
    let target = match top_up.fill {
        Fill::ChunkBoundary => round_up(floor, chunk_size.max(1)),
        Fill::Extra(n) => floor.max(base.saturating_add(n)),
    };
    let extra = target.saturating_sub(base).min(eligible.len());
    fetch.extend(eligible.into_iter().take(extra));
    Plan {
        fetch,
        deferred: 0,
        pulled_forward: extra,
    }
}

/// `n` rounded up to a multiple of `chunk`, saturating (a bibliography file's
/// chunk is `usize::MAX`).
fn round_up(n: usize, chunk: usize) -> usize {
    match n % chunk {
        0 => n,
        r => n.saturating_add(chunk - r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use alloc::vec;

    fn keys(prefix: &str, n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{prefix}{i}")).collect()
    }

    const ARXIV_LIKE: RefreshBatching = RefreshBatching {
        min_batch: 20,
        max_defer: Duration::ZERO,
        top_up: Some(TopUp {
            min_age_percent: 50,
            fill: Fill::ChunkBoundary,
        }),
    };

    #[test]
    fn eager_fetches_exactly_what_is_asked() {
        let p = plan(&RefreshBatching::EAGER, 100, keys("r", 1), keys("d", 2), keys("e", 50));
        assert_eq!(p.fetch, vec!["r0", "d0", "d1"]);
        assert_eq!((p.deferred, p.pulled_forward), (0, 0));
    }

    #[test]
    fn few_due_and_no_top_up_defers_everything() {
        let policy = RefreshBatching {
            top_up: None,
            ..ARXIV_LIKE
        };
        let p = plan(&policy, 100, vec![], keys("d", 3), keys("e", 50));
        assert!(p.fetch.is_empty());
        assert_eq!(p.deferred, 3);
    }

    #[test]
    fn few_due_and_too_few_eligible_defers_everything() {
        let p = plan(&ARXIV_LIKE, 100, vec![], keys("d", 3), keys("e", 16));
        assert!(p.fetch.is_empty());
        assert_eq!(p.deferred, 3);
    }

    #[test]
    fn few_due_tops_up_to_min_then_to_the_chunk_boundary() {
        let p = plan(&ARXIV_LIKE, 100, vec![], keys("d", 3), keys("e", 17));
        assert_eq!(p.fetch.len(), 20);
        assert_eq!(p.pulled_forward, 17);
        let p = plan(&ARXIV_LIKE, 100, vec![], keys("d", 3), keys("e", 500));
        assert_eq!(p.fetch.len(), 100);
        assert_eq!(&p.fetch[..4], &["d0", "d1", "d2", "e0"]);
        assert_eq!(p.pulled_forward, 97);
    }

    #[test]
    fn required_work_always_goes_out_and_fills_its_last_chunk() {
        let p = plan(&ARXIV_LIKE, 100, keys("r", 1), keys("d", 2), keys("e", 500));
        assert_eq!(p.fetch.len(), 100);
        assert_eq!(&p.fetch[..3], &["r0", "d0", "d1"]);
        let p = plan(&ARXIV_LIKE, 100, keys("r", 150), vec![], keys("e", 500));
        assert_eq!(p.fetch.len(), 200);
        let p = plan(&ARXIV_LIKE, 100, keys("r", 100), vec![], keys("e", 500));
        assert_eq!(p.fetch.len(), 100, "a full last chunk has no free room");
    }

    #[test]
    fn enough_due_goes_out_without_topping_to_min() {
        let p = plan(&ARXIV_LIKE, 100, vec![], keys("d", 25), keys("e", 10));
        assert_eq!(p.fetch.len(), 35);
        assert_eq!(p.deferred, 0);
    }

    #[test]
    fn eligible_alone_is_never_fetched() {
        let p = plan(&ARXIV_LIKE, 100, vec![], vec![], keys("e", 500));
        assert_eq!(p, Plan::default());
    }

    #[test]
    fn extra_fill_for_per_key_sources() {
        let policy = RefreshBatching {
            min_batch: 10,
            max_defer: Duration::ZERO,
            top_up: Some(TopUp {
                min_age_percent: 80,
                fill: Fill::Extra(0),
            }),
        };
        // Tops up only to the minimum…
        let p = plan(&policy, 1, vec![], keys("d", 3), keys("e", 50));
        assert_eq!(p.fetch.len(), 10);
        // …and adds nothing when a request is going out anyway.
        let p = plan(&policy, 1, keys("r", 1), keys("d", 2), keys("e", 50));
        assert_eq!(p.fetch.len(), 3);
        let policy = RefreshBatching {
            top_up: Some(TopUp {
                min_age_percent: 80,
                fill: Fill::Extra(5),
            }),
            ..policy
        };
        let p = plan(&policy, 1, keys("r", 1), vec![], keys("e", 50));
        assert_eq!(p.fetch.len(), 6);
        let p = plan(&policy, 1, vec![], keys("d", 3), keys("e", 50));
        assert_eq!(p.fetch.len(), 10, "the minimum already covers 5 extra");
    }

    #[test]
    fn unbounded_chunk_takes_every_eligible_entry() {
        let policy = RefreshBatching {
            min_batch: 0,
            max_defer: Duration::ZERO,
            top_up: Some(TopUp {
                min_age_percent: 0,
                fill: Fill::ChunkBoundary,
            }),
        };
        let p = plan(&policy, usize::MAX, vec![], keys("d", 1), keys("e", 40));
        assert_eq!(p.fetch.len(), 41);
    }

    fn record(stale_after: i64, expires: i64) -> CacheRecord {
        CacheRecord {
            payload: crate::store::Payload::Concrete(serde_json::json!({})),
            stale_after: Timestamp(stale_after),
            expires: Timestamp(expires),
        }
    }

    #[test]
    fn classify_expired_by_max_defer() {
        let ttl = TtlPolicy::default();
        let policy = RefreshBatching {
            max_defer: Duration::from_millis(1000),
            ..RefreshBatching::EAGER
        };
        let rec = record(0, 10_000);
        let nominal = Duration::from_millis(10_000);
        assert_eq!(
            policy.classify(&ttl, &rec, Timestamp(10_500), "x", nominal),
            RefreshClass::Due
        );
        assert_eq!(
            policy.classify(&ttl, &rec, Timestamp(11_000), "x", nominal),
            RefreshClass::Required
        );
        assert_eq!(
            RefreshBatching::EAGER.classify(&ttl, &rec, Timestamp(10_000), "x", nominal),
            RefreshClass::Required
        );
    }

    #[test]
    fn classify_max_defer_is_capped_by_grace() {
        let ttl = TtlPolicy {
            grace: Duration::from_millis(100),
            ..TtlPolicy::default()
        };
        let policy = RefreshBatching {
            max_defer: Duration::from_millis(1000),
            ..RefreshBatching::EAGER
        };
        let rec = record(0, 10_000);
        assert_eq!(
            policy.classify(&ttl, &rec, Timestamp(10_100), "x", Duration::from_millis(10_000)),
            RefreshClass::Required
        );
    }

    #[test]
    fn classify_eligibility_by_age() {
        let ttl = TtlPolicy::default();
        let nominal = Duration::from_millis(10_000);
        // Fresh until 8 000, expires at 10 000; fetched at 0.
        let rec = record(8_000, 10_000);
        let policy = ARXIV_LIKE;
        assert_eq!(policy.classify(&ttl, &rec, Timestamp(4_000), "x", nominal), RefreshClass::Keep);
        assert_eq!(
            policy.classify(&ttl, &rec, Timestamp(5_000), "x", nominal),
            RefreshClass::Eligible
        );
        assert_eq!(
            RefreshBatching::EAGER.classify(&ttl, &rec, Timestamp(5_000), "x", nominal),
            RefreshClass::Keep
        );
        let any_age = RefreshBatching {
            top_up: Some(TopUp {
                min_age_percent: 0,
                fill: Fill::ChunkBoundary,
            }),
            ..RefreshBatching::EAGER
        };
        assert_eq!(
            any_age.classify(&ttl, &rec, Timestamp(0), "x", Duration::from_millis(1)),
            RefreshClass::Eligible
        );
    }
}
