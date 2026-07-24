//! Progress reporting: the [`Reporter`] trait and the [`Event`]s the core emits.
//!
//! This is the fifth host capability, and the only *optional* one — a manager
//! without a reporter behaves exactly as before. Unlike [`Fetcher`], [`Clock`],
//! [`Timer`] and [`CacheStore`] it is not a generic parameter of
//! [`CitationManager`](crate::manager::CitationManager) but an
//! `Rc<dyn Reporter>` field: [`RetrieveCtx`](crate::source::RetrieveCtx) already
//! erases the other backends to `&dyn`, so genericity would be erased one level
//! down anyway, and an `Rc` lets one reporter be shared with anything else the
//! host builds.
//!
//! # Why the trait is synchronous
//!
//! [`Reporter::report`] returns `()`, not a [`BoxFuture`](crate::BoxFuture).
//! That is the load-bearing decision here. An async reporter would turn every
//! emission into a new yield point — inside `buffer_unordered` in
//! [`manager`](crate::manager), inside the pacing loop in
//! [`driver`](crate::driver), inside the backoff loop in
//! [`retry`](crate::retry). A host callback that awaited could then be
//! *re-entered* while a previous report was still pending, and could interleave
//! with other sources' work at points the core never meant to be suspension
//! points. A synchronous `&self` method makes that structurally impossible. A
//! host needing async delivery buffers into a `RefCell<Vec<_>>` of its own and
//! drains it between calls.
//!
//! For the same reason `report` returns nothing: the reporter must never
//! influence control flow. Cancellation is a genuinely different feature (it has
//! to be checked at await points and unwind the worklist) and is deliberately
//! not smuggled in here.
//!
//! # Why events are borrowed
//!
//! Every [`Event`] field is a `&str`, an integer, a [`Duration`] or a borrowed
//! error — never a [`String`], and never a pre-formatted message. Emitting an
//! event therefore allocates nothing, and a [`NopReporter`] costs one vtable
//! call to an empty body. Formatting is the host's job: it has the
//! [`Error`] by reference and can `Display` it if it is actually going to show
//! it. This is also why the trait has no "do you want this event?" hook — there
//! is no payload expensive enough for asking to pay off.
//!
//! # Why every event names its subject
//!
//! The manager drives up to
//! [`MAX_CONCURRENT_SOURCES`](crate::manager) source futures with
//! `buffer_unordered`, so events from different prefixes **interleave**. There
//! is no implicit "current activity" and no push/pop scope: a reporter must not
//! assume [`Event::SourceStarted`] and [`Event::SourceFinished`] for one prefix
//! are adjacent, and any per-source state it keeps must be keyed by
//! `prefix`. (With the shipped blocking `std` backends nothing actually
//! overlaps — see the note in `manager::run_passes` — so a display will render
//! sequential work, truthfully. Swap in a genuinely async fetcher and the same
//! events simply start interleaving.)
//!
//! # Samples and milestones
//!
//! [`Event::sample_key`] splits the vocabulary in two. A *sample* is a periodic
//! reading a throttle may drop ([`Event::SourceProgress`] is the only one). A
//! *milestone* is everything else and must always be delivered. The invariant
//! that makes dropping safe: **every `*Finished` event carries final counts**,
//! so a dropped sample can never leave a progress display stuck at a stale
//! reading. [`ThrottledReporter`] relies on exactly this.
//!
//! # What is not instrumented
//!
//! [`get`](crate::manager::CitationManager::get) emits nothing: it is a local
//! cache read with no waiting in it. And the events for cache compaction are
//! emitted by the *manager*, around its [`CacheStore::flush`] calls, rather than
//! from inside [`FileCacheStore`](crate::filecache::FileCacheStore) — putting an
//! `Rc` in the store would make it `!Send`, and the concurrency regression tests
//! move stores across threads. `flush` is the only operation that locks, so
//! wrapping the call site loses nothing but the ability to distinguish "blocked
//! on the lock" from "folding the sidecars".
//!
//! [`Fetcher`]: crate::fetch::Fetcher
//! [`Clock`]: crate::env::Clock
//! [`Timer`]: crate::env::Timer
//! [`CacheStore`]: crate::store::CacheStore
//! [`CacheStore::flush`]: crate::store::CacheStore::flush
//! [`Error`]: crate::error::Error

use alloc::rc::Rc;
use alloc::string::String;
use core::cell::RefCell;
use core::time::Duration;

use hashbrown::HashMap;

use crate::env::{Clock, Timestamp, millis_i64};
use crate::error::Error;
use crate::fetch::FetchError;

/// A host-provided progress sink.
///
/// Synchronous, `&self`, and returning nothing — see the [module docs](self) for
/// why all three matter. Implementations use interior mutability (a
/// `RefCell`/`Cell`), exactly like [`CacheStore`](crate::store::CacheStore).
///
/// Object-safe: the manager holds an `Rc<dyn Reporter>` and hands sources a
/// `&dyn Reporter` through [`RetrieveCtx`](crate::source::RetrieveCtx).
pub trait Reporter {
    /// Handle one event. Must not panic, and should be cheap: it is called from
    /// inside the retrieval loop, between network requests.
    fn report(&self, ev: &Event<'_>);
}

impl<R: Reporter + ?Sized> Reporter for &R {
    fn report(&self, ev: &Event<'_>) {
        (**self).report(ev);
    }
}

impl<R: Reporter + ?Sized> Reporter for Rc<R> {
    fn report(&self, ev: &Event<'_>) {
        (**self).report(ev);
    }
}

/// A reporter that discards everything. The default, and what a manager built
/// with [`CitationManager::new`](crate::manager::CitationManager::new) carries
/// until [`with_reporter`](crate::manager::CitationManager::with_reporter) is
/// called.
#[derive(Clone, Copy, Debug, Default)]
pub struct NopReporter;

impl Reporter for NopReporter {
    fn report(&self, _ev: &Event<'_>) {}
}

/// How a citation resolved, for [`Event::CiteResolved`].
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Resolved<'a> {
    /// Concrete CSL-JSON was stored for this citation.
    Concrete,
    /// A pointer to another citation was stored; the target is fetched in a
    /// later pass (and reaches this reporter as its own events).
    Chained { prefix: &'a str, key: &'a str },
}

/// What the core is blocked on, for [`Event::WaitStarted`] /
/// [`Event::WaitFinished`].
///
/// These are the moments the tool looks hung: a 3.1 s arXiv pacing gap, a 30 s
/// retry backoff, a cache compaction behind a lock held by another process.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Wait<'a> {
    /// Politeness pacing between a source's chunks
    /// ([`Source::min_interval`](crate::source::Source::min_interval)).
    RateLimit { prefix: &'a str },
    /// Backoff before re-issuing a failed request. `attempt` is the 0-based
    /// index of the attempt that just failed, so the retry about to happen is
    /// number `attempt + 1`.
    Backoff { url: &'a str, attempt: u32 },
    /// Compacting the cache: taking the compaction lock and folding sidecars
    /// into the committed file.
    CacheFlush,
}

/// Something the core did or is about to do.
///
/// `#[non_exhaustive]`, so matching requires a wildcard arm and new events can
/// be added without breaking implementors. Fields are borrowed — see the
/// [module docs](self).
#[derive(Debug)]
#[non_exhaustive]
pub enum Event<'a> {
    // ---- lifecycle of one `retrieve()` --------------------------------------
    /// A [`retrieve`](crate::manager::CitationManager::retrieve) call began,
    /// with `cites` citations requested. Chained targets discovered later are
    /// *not* counted here — the total is genuinely not knowable up front.
    RetrieveStarted { cites: usize },
    /// A retrieval pass has been planned. Emitted after the batch has been
    /// classified (which is when the counts exist) and before any source runs.
    /// `pass` is 1-based; `cached` is how many citations were served from a
    /// record that did not need refetching, `to_fetch` how many keys were
    /// bucketed for a source.
    PassStarted {
        pass: usize,
        cached: usize,
        to_fetch: usize,
    },
    /// A pass finished. `discovered` is how many chain targets it queued for the
    /// next pass — non-zero is exactly what makes a progress denominator grow.
    PassFinished { pass: usize, discovered: usize },
    /// The whole retrieval finished. `considered` counts every distinct citation
    /// id touched, requested or chained; `failed` is
    /// [`RetrieveReport::failures`](crate::manager::RetrieveReport::failures)'
    /// length. Carries final counts, so a throttle dropping samples cannot leave
    /// a stale reading.
    RetrieveFinished { considered: usize, failed: usize },

    // ---- per source, per pass (emitted by `driver`) -------------------------
    /// A source is about to be driven over `keys` keys in `chunks` chunks.
    /// Emitted once per prefix per pass, by the driver — so a third-party
    /// [`Source`](crate::source::Source) is instrumented without cooperating.
    SourceStarted {
        prefix: &'a str,
        keys: usize,
        chunks: usize,
    },
    /// A chunk came back: `done` of `total` keys have now been handed to the
    /// source. **This is the smooth progress signal** — it fires as chunks
    /// complete, whereas [`Event::CiteResolved`] arrives in one burst at the end
    /// of a pass (the manager applies a pass's resolutions serially, after all
    /// its sources have returned). The only droppable event; see
    /// [`Event::sample_key`].
    SourceProgress {
        prefix: &'a str,
        done: usize,
        total: usize,
    },
    /// A source finished this pass, having been handed `done` keys.
    SourceFinished { prefix: &'a str, done: usize },

    // ---- per citation (emitted by `manager`) -------------------------------
    /// A citation resolved and was written to the store.
    CiteResolved {
        prefix: &'a str,
        key: &'a str,
        how: Resolved<'a>,
    },
    /// A citation did not resolve. `grace_served` distinguishes the two cases:
    /// `false` means this is in the [`RetrieveReport`], `true` means the source
    /// was unreachable but a cached copy inside the grace window is still being
    /// served — stale-while-revalidate, invisible in the report and otherwise
    /// invisible entirely.
    ///
    /// [`RetrieveReport`]: crate::manager::RetrieveReport
    CiteFailed {
        prefix: &'a str,
        key: &'a str,
        err: &'a Error,
        grace_served: bool,
    },

    // ---- waiting -----------------------------------------------------------
    /// The core is about to block. `expected` is how long it intends to wait,
    /// when that is known up front (rate limiting and backoff know; a lock does
    /// not).
    ///
    /// The duration is announced rather than the sleep being chopped into ticks:
    /// no extra [`Timer`](crate::env::Timer) calls, it works for a plain logger,
    /// and a richer display can render its own countdown at its own frame rate.
    WaitStarted {
        what: Wait<'a>,
        expected: Option<Duration>,
    },
    /// The wait ended. Always paired with a preceding [`Event::WaitStarted`]
    /// carrying an equal `what`.
    WaitFinished { what: Wait<'a> },

    // ---- network (emitted by `retry`) --------------------------------------
    /// An HTTP request is being issued. `attempt` is 0-based, so a non-zero
    /// value means this is a retry.
    RequestStarted { url: &'a str, attempt: u32 },
    /// A request came back: `Ok(status)` for a response (of any status),
    /// `Err(..)` for a transport failure. Whether it will be retried is
    /// announced by the following [`Wait::Backoff`].
    RequestFinished {
        url: &'a str,
        result: core::result::Result<u16, &'a FetchError>,
    },
}

impl Event<'_> {
    /// The throttling key of a *sample* event, or `None` for a milestone.
    ///
    /// A sample is a periodic reading that may be dropped when they arrive
    /// faster than a display can use them; a milestone must always be
    /// delivered. Only [`Event::SourceProgress`] is a sample, and its key is the
    /// prefix — so a throttle's bookkeeping is bounded by the number of
    /// registered sources, and a slow source cannot starve a fast one of
    /// updates.
    ///
    /// Dropping samples is safe because every `*Finished` event carries final
    /// counts: a dropped last sample cannot leave a bar stuck at 87%.
    pub fn sample_key(&self) -> Option<&str> {
        match self {
            Event::SourceProgress { prefix, .. } => Some(prefix),
            _ => None,
        }
    }

    /// Whether a throttle may drop this event. See [`Event::sample_key`].
    pub fn is_sample(&self) -> bool {
        self.sample_key().is_some()
    }
}

/// A [`Reporter`] decorator that rate-limits *sample* events and passes every
/// milestone straight through.
///
/// This is deliberately a decorator rather than a
/// `Reporter::desired_update_frequency()` hook the core would consult. Inverting
/// control that way would buy one thing — letting the core skip building a
/// payload the reporter would discard — and every payload here is borrowed
/// `&str`s and integers, so it would buy nothing at all. As a decorator it is
/// composable, independently testable, and leaves the core with no opinion about
/// presentation.
///
/// Note that the event rate is already bounded below ~1 Hz by the sources'
/// [`min_interval`](crate::source::Source::min_interval) pacing, so this is
/// rarely needed; the usual complaint is silence during a long
/// [`Wait`], which [`Event::WaitStarted`] addresses instead.
///
/// ```ignore
/// let reporter = Rc::new(ThrottledReporter::new(
///     StderrReporter::new(),
///     SystemClock,
///     Duration::from_millis(250),
/// ));
/// ```
pub struct ThrottledReporter<R, C> {
    inner: R,
    clock: C,
    min_gap: Duration,
    /// Last delivery time per [`Event::sample_key`]. Bounded by the number of
    /// registered prefixes.
    last: RefCell<HashMap<String, Timestamp>>,
}

impl<R, C> ThrottledReporter<R, C> {
    /// Wrap `inner`, delivering at most one sample per `min_gap` per subject.
    pub fn new(inner: R, clock: C, min_gap: Duration) -> Self {
        ThrottledReporter {
            inner,
            clock,
            min_gap,
            last: RefCell::new(HashMap::new()),
        }
    }

    /// The wrapped reporter.
    pub fn inner(&self) -> &R {
        &self.inner
    }
}

impl<R: Reporter, C: Clock> Reporter for ThrottledReporter<R, C> {
    fn report(&self, ev: &Event<'_>) {
        if let Some(key) = ev.sample_key() {
            let now = self.clock.now();
            // Scope the borrow: `inner.report` must never run while the
            // `RefCell` is held, or a reporter that re-entered this one would
            // panic instead of merely being surprising.
            let due = {
                let mut last = self.last.borrow_mut();
                let due = match last.get(key) {
                    Some(prev) => {
                        now.as_millis().saturating_sub(prev.as_millis())
                            >= millis_i64(self.min_gap)
                    }
                    None => true,
                };
                if due {
                    last.insert(String::from(key), now);
                }
                due
            };
            if !due {
                return;
            }
        }
        self.inner.report(ev);
    }
}

#[cfg(test)]
mod tests {
    use core::cell::Cell;

    use super::*;

    /// Counts what it is handed, split by kind.
    #[derive(Default)]
    struct Counting {
        samples: Cell<usize>,
        milestones: Cell<usize>,
    }

    impl Reporter for Counting {
        fn report(&self, ev: &Event<'_>) {
            if ev.is_sample() {
                self.samples.set(self.samples.get() + 1);
            } else {
                self.milestones.set(self.milestones.get() + 1);
            }
        }
    }

    /// A clock the test steps by hand.
    struct StepClock(Cell<i64>);

    impl Clock for StepClock {
        fn now(&self) -> Timestamp {
            Timestamp::from_millis(self.0.get())
        }
    }

    fn progress(prefix: &str) -> Event<'_> {
        Event::SourceProgress {
            prefix,
            done: 1,
            total: 2,
        }
    }

    #[test]
    fn only_source_progress_is_a_sample() {
        assert!(progress("arxiv").is_sample());
        assert_eq!(progress("arxiv").sample_key(), Some("arxiv"));

        let milestones = [
            Event::RetrieveStarted { cites: 1 },
            Event::SourceStarted {
                prefix: "arxiv",
                keys: 1,
                chunks: 1,
            },
            Event::SourceFinished {
                prefix: "arxiv",
                done: 1,
            },
            Event::RetrieveFinished {
                considered: 1,
                failed: 0,
            },
            Event::WaitStarted {
                what: Wait::CacheFlush,
                expected: None,
            },
        ];
        for ev in &milestones {
            assert!(!ev.is_sample(), "{ev:?} must not be droppable");
        }
    }

    #[test]
    fn samples_are_dropped_within_the_gap_milestones_never_are() {
        let clock = StepClock(Cell::new(0));
        let r = ThrottledReporter::new(Counting::default(), clock, Duration::from_millis(100));

        // First sample always passes; the next two are inside the gap.
        r.report(&progress("arxiv"));
        r.clock.0.set(40);
        r.report(&progress("arxiv"));
        r.clock.0.set(99);
        r.report(&progress("arxiv"));
        assert_eq!(r.inner().samples.get(), 1);

        // Exactly at the gap it is due again.
        r.clock.0.set(100);
        r.report(&progress("arxiv"));
        assert_eq!(r.inner().samples.get(), 2);

        // Milestones bypass the gap entirely, however dense.
        for _ in 0..5 {
            r.report(&Event::PassFinished {
                pass: 1,
                discovered: 0,
            });
        }
        assert_eq!(r.inner().milestones.get(), 5);
    }

    #[test]
    fn throttling_is_per_subject_so_one_source_cannot_starve_another() {
        let clock = StepClock(Cell::new(0));
        let r = ThrottledReporter::new(Counting::default(), clock, Duration::from_millis(100));

        r.report(&progress("arxiv"));
        // Same instant, different prefix: not throttled by arxiv's delivery.
        r.report(&progress("doi"));
        assert_eq!(r.inner().samples.get(), 2);

        // …and each keeps its own clock thereafter.
        r.clock.0.set(50);
        r.report(&progress("arxiv"));
        r.report(&progress("doi"));
        assert_eq!(r.inner().samples.get(), 2);
    }
}
