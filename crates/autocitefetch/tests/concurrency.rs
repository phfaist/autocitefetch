//! Exercises the concurrent within-a-pass source execution: two independent
//! sources registered under two prefixes, resolved in a single `retrieve`
//! call. Both must resolve, and — the point of the concurrency — the second
//! source must start before the first one finishes.
//!
//! Overlap is only observable if the host's timer actually yields, so the mock
//! timer here returns `Pending` once (re-waking itself) instead of resolving
//! immediately. With a `for` loop in place of `buffer_unordered` the enter/exit
//! log below becomes `enter/exit/enter/exit` and the test fails.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, FetchError, Fetcher,
    Request, Resolution, Response, RetrieveCtx, Source, StoreError, Timer, Timestamp,
};

// --- a minimal, always-ready block_on (mocks never truly pend) -------------

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete (a mock unexpectedly pended)");
}

// --- mocks -----------------------------------------------------------------

struct NoopFetcher;
impl Fetcher for NoopFetcher {
    fn fetch(&self, _req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        Box::pin(async { Err(FetchError::Status(404)) })
    }
}

#[derive(Default)]
struct MemStore {
    map: RefCell<StdMap<String, CacheRecord>>,
}

impl CacheStore for MemStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.map.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().insert(id.into(), record);
        Box::pin(async move { Ok(()) })
    }
    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().remove(id);
        Box::pin(async move { Ok(()) })
    }
    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        let all: Vec<_> = self
            .map
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::pin(async move { Ok(all) })
    }
}

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0)
    }
}

/// Pends exactly once, re-waking itself so whoever is driving it (here
/// `FuturesUnordered`) re-queues it. This is the cooperative yield point that
/// lets two source futures interleave.
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}
impl Future for YieldOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.yielded {
            Poll::Ready(())
        } else {
            this.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// A timer that yields to the executor instead of resolving inline, so
/// concurrent source futures actually interleave.
struct YieldingTimer;
impl Timer for YieldingTimer {
    fn sleep(&self, _dur: core::time::Duration) -> BoxFuture<'_, ()> {
        Box::pin(YieldOnce::default())
    }
}

/// What a source did and when, so overlap is observable.
type EventLog = Rc<RefCell<Vec<(&'static str, &'static str)>>>;

/// An independent source that resolves every key to a concrete CSL item
/// tagged with the source's own name. Two of these under two prefixes let us
/// verify both are driven within a single (concurrent) pass. Each yields via
/// the timer between its `enter` and `exit` marks, so a concurrent driver
/// produces an interleaved log and a sequential one does not.
struct TagSource {
    prefix: &'static str,
    events: EventLog,
}

impl Source for TagSource {
    fn prefix(&self) -> &str {
        self.prefix
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        let prefix = self.prefix;
        Box::pin(async move {
            self.events.borrow_mut().push((prefix, "enter"));
            // A cooperative yield point: forces this future to await, so with
            // two sources in flight their work actually interleaves.
            ctx.timer.sleep(core::time::Duration::from_millis(0)).await;
            self.events.borrow_mut().push((prefix, "exit"));
            keys.into_iter()
                .map(|k| {
                    let mut m = serde_json::Map::new();
                    m.insert("title".into(), CslValue::String(format!("{prefix} {k}")));
                    m.insert("source".into(), CslValue::String(prefix.into()));
                    Resolution::concrete(k, CslValue::Object(m))
                })
                .collect()
        })
    }
}

// --- test ------------------------------------------------------------------

#[test]
fn two_sources_resolve_concurrently_in_one_pass() {
    let events: EventLog = Rc::new(RefCell::new(Vec::new()));
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), FixedClock(0), YieldingTimer)
        .register(TagSource {
            prefix: "alpha",
            events: events.clone(),
        }).unwrap()
        .register(TagSource {
            prefix: "beta",
            events: events.clone(),
        }).unwrap();

    // Both prefixes requested in a single retrieve call => same pass => the
    // two `drive_source` calls run concurrently.
    let cites = vec![
        ("alpha".to_string(), "k1".to_string()),
        ("beta".to_string(), "k2".to_string()),
        ("alpha".to_string(), "k3".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // The work actually overlapped: both sources entered before either exited.
    // (Which one goes first is nondeterministic — buckets are a HashMap.)
    let log = events.borrow().clone();
    assert_eq!(log.len(), 4, "log: {log:?}");
    assert_eq!(log[0].1, "enter", "log: {log:?}");
    assert_eq!(
        log[1].1, "enter",
        "the second source only started after the first finished — no overlap: {log:?}"
    );
    assert_ne!(log[0].0, log[1].0, "log: {log:?}");
    assert_eq!(log[2].1, "exit", "log: {log:?}");
    assert_eq!(log[3].1, "exit", "log: {log:?}");

    // Both sources resolved their keys.
    let a1 = block_on(mgr.get("alpha", "k1")).unwrap();
    assert_eq!(a1["id"], "alpha:k1");
    assert_eq!(a1["title"], "alpha k1");
    assert_eq!(a1["source"], "alpha");

    let b2 = block_on(mgr.get("beta", "k2")).unwrap();
    assert_eq!(b2["id"], "beta:k2");
    assert_eq!(b2["title"], "beta k2");
    assert_eq!(b2["source"], "beta");

    let a3 = block_on(mgr.get("alpha", "k3")).unwrap();
    assert_eq!(a3["id"], "alpha:k3");
    assert_eq!(a3["source"], "alpha");
}
