//! Transparent retry/backoff: a flaky fetcher that fails a few times then
//! succeeds must be retried behind the source's back, while a hard 404 must
//! pass straight through with no retries.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::source::DoiSource;
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, FetchError, Fetcher, Request,
    Response, StoreError, Timer, Timestamp,
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

/// How the flaky fetcher expresses a transient failure.
#[derive(Clone, Copy)]
enum FailMode {
    /// Surface a retryable HTTP status as an `Ok(Response)` (e.g. doi.org 503).
    Status503,
    /// Surface a retryable transport error as `Err`.
    Transport,
    /// A hard 404 that must NOT be retried.
    Status404,
}

/// Fails the first `fail_first` attempts for the matching url, then serves a
/// 200 with `body`. Counts every attempt so the test can check retry behavior.
struct FlakyFetcher {
    url: String,
    fail_first: u32,
    mode: FailMode,
    body: Vec<u8>,
    attempts: RefCell<StdMap<String, u32>>,
}

impl FlakyFetcher {
    fn new(url: &str, fail_first: u32, mode: FailMode, body: &str) -> Self {
        FlakyFetcher {
            url: url.into(),
            fail_first,
            mode,
            body: body.as_bytes().to_vec(),
            attempts: RefCell::new(StdMap::new()),
        }
    }
}

impl Fetcher for FlakyFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        let n = {
            let mut a = self.attempts.borrow_mut();
            let c = a.entry(req.url.clone()).or_insert(0);
            *c += 1;
            *c
        };

        let matches = req.url == self.url;
        let still_failing = matches && n <= self.fail_first;

        let result = if !matches {
            Err(FetchError::Status(404))
        } else if still_failing {
            match self.mode {
                FailMode::Status503 => Ok(Response {
                    status: 503,
                    headers: Default::default(),
                    body: Vec::new(),
                }),
                FailMode::Transport => Err(FetchError::Transport("connection reset".into())),
                FailMode::Status404 => Ok(Response {
                    status: 404,
                    headers: Default::default(),
                    body: Vec::new(),
                }),
            }
        } else {
            Ok(Response {
                status: 200,
                headers: Default::default(),
                body: self.body.clone(),
            })
        };

        Box::pin(async move { result })
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

/// A timer that records how many times `sleep` was invoked (each recorded
/// sleep is one backoff). The counter is shared via `Rc` so the test can read
/// it after the manager has taken ownership of the timer. Resolves instantly so
/// `block_on` never stalls.
#[derive(Clone, Default)]
struct CountingTimer {
    sleeps: Rc<Cell<u32>>,
}
impl Timer for CountingTimer {
    fn sleep(&self, _dur: Duration) -> BoxFuture<'_, ()> {
        self.sleeps.set(self.sleeps.get() + 1);
        Box::pin(async {})
    }
}

// --- tests -----------------------------------------------------------------

const DOI: &str = "10.1234/retry";
const DOI_URL: &str = "https://doi.org/10.1234/retry";
const BODY: &str = r#"{"type":"article-journal","title":"Retry Worked","DOI":"10.1234/retry"}"#;

/// A flaky source that returns retryable 503s a few times is retried behind the
/// DOI source's back and eventually resolves — with exactly K backoff sleeps.
#[test]
fn retryable_503_is_retried_until_it_succeeds() {
    const K: u32 = 3;
    let timer = CountingTimer::default();
    let sleeps = timer.sleeps.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, K, FailMode::Status503, BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Resolved despite the transient failures.
    let item = block_on(mgr.get("doi", DOI)).unwrap();
    assert_eq!(item["id"], "doi:10.1234/retry");
    assert_eq!(item["title"], "Retry Worked");

    // Exactly K backoff sleeps: one before each of the K retries.
    assert_eq!(sleeps.get(), K, "expected exactly K backoff sleeps");
}

/// The `Err` path is retried too: retryable transport errors back off and retry.
#[test]
fn retryable_transport_error_is_retried_until_it_succeeds() {
    const K: u32 = 2;
    let timer = CountingTimer::default();
    let sleeps = timer.sleeps.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, K, FailMode::Transport, BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    assert_eq!(block_on(mgr.get("doi", DOI)).unwrap()["title"], "Retry Worked");
    assert_eq!(sleeps.get(), K, "expected exactly K backoff sleeps");
}

/// A 404 is a hard, non-retryable status: it passes straight through with zero
/// backoff sleeps and the citation is reported as failed.
#[test]
fn non_retryable_404_is_not_retried() {
    // fail_first is huge, but the mode is 404 → the wrapper must not retry it.
    let timer = CountingTimer::default();
    let sleeps = timer.sleeps.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, 99, FailMode::Status404, BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();

    // Reported as a failure, and never resolvable.
    assert_eq!(report.failures.len(), 1, "the 404 should surface as a failure");
    assert_eq!(report.failures[0].prefix, "doi");
    assert!(block_on(mgr.get("doi", DOI)).is_err());

    // Zero backoff sleeps: a 404 is not retried.
    assert_eq!(sleeps.get(), 0, "a 404 must not be retried");
}
