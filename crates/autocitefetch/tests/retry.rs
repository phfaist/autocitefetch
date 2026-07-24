//! Transparent retry/backoff: a flaky fetcher that fails a few times then
//! succeeds must be retried behind the source's back, while a hard 404 must
//! pass straight through with no retries.
//!
//! The delay *math* (base, doubling, cap, jitter, `Retry-After`) is exercised
//! against [`RetryingFetcher`] directly — going through a source would mix the
//! driver's rate-limiting sleeps into the same timer.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::source::DoiSource;
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, FetchError, Fetcher, Request,
    Response, RetryPolicy, RetryingFetcher, StoreError, Timer, Timestamp,
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
    Status(u16),
    /// Surface a retryable transport error as `Err`.
    Transport,
}

/// Fails the first `fail_first` attempts for the matching url, then serves a
/// 200 with `body`. Counts every attempt so the test can check retry behavior.
struct FlakyFetcher {
    url: String,
    fail_first: u32,
    mode: FailMode,
    /// `Retry-After` header value attached to failure responses, if any.
    retry_after: Option<String>,
    body: Vec<u8>,
    attempts: RefCell<StdMap<String, u32>>,
}

impl FlakyFetcher {
    fn new(url: &str, fail_first: u32, mode: FailMode, body: &str) -> Self {
        FlakyFetcher {
            url: url.into(),
            fail_first,
            mode,
            retry_after: None,
            body: body.as_bytes().to_vec(),
            attempts: RefCell::new(StdMap::new()),
        }
    }
    fn with_retry_after(mut self, value: &str) -> Self {
        self.retry_after = Some(value.into());
        self
    }
    fn failure_response(&self, status: u16) -> Response {
        let mut resp = Response {
            status,
            headers: Default::default(),
            body: Vec::new(),
        };
        if let Some(ra) = &self.retry_after {
            resp.headers.insert("retry-after".into(), ra.clone());
        }
        resp
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
                FailMode::Status(s) => Ok(self.failure_response(s)),
                FailMode::Transport => Err(FetchError::Transport("connection reset".into())),
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

/// A timer that records *how long* each sleep was (each recorded sleep is one
/// backoff). Shared via `Rc` so the test can read it after the manager has
/// taken ownership of the timer. Resolves instantly so `block_on` never stalls.
#[derive(Clone, Default)]
struct RecordingTimer {
    sleeps: Rc<RefCell<Vec<Duration>>>,
}
impl RecordingTimer {
    fn millis(&self) -> Vec<u64> {
        self.sleeps
            .borrow()
            .iter()
            .map(|d| d.as_millis() as u64)
            .collect()
    }
}
impl Timer for RecordingTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        self.sleeps.borrow_mut().push(dur);
        Box::pin(async {})
    }
}

/// Always answers with the same status (plus an optional `Retry-After`), so a
/// policy's delay schedule can be read straight off the timer.
struct AlwaysFails {
    status: u16,
    retry_after: Option<&'static str>,
    calls: Cell<u32>,
}
impl AlwaysFails {
    fn new(status: u16) -> Self {
        AlwaysFails {
            status,
            retry_after: None,
            calls: Cell::new(0),
        }
    }
    fn with_retry_after(mut self, value: &'static str) -> Self {
        self.retry_after = Some(value);
        self
    }
}
impl Fetcher for AlwaysFails {
    fn fetch(&self, _req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.set(self.calls.get() + 1);
        let mut resp = Response {
            status: self.status,
            headers: Default::default(),
            body: Vec::new(),
        };
        if let Some(ra) = self.retry_after {
            resp.headers.insert("retry-after".into(), ra.into());
        }
        Box::pin(async move { Ok(resp) })
    }
}

/// Run one request through a `RetryingFetcher` and return the delays it slept.
fn delays_for(fetcher: &AlwaysFails, policy: RetryPolicy, url: &str) -> Vec<u64> {
    let timer = RecordingTimer::default();
    let retrying = RetryingFetcher::new(fetcher, &timer, policy);
    let resp = block_on(retrying.fetch(Request::get(url))).expect("status is surfaced as Ok");
    assert_eq!(resp.status, fetcher.status);
    timer.millis()
}

fn policy(max_retries: u32, base_ms: u64, cap_ms: u64, honor: bool) -> RetryPolicy {
    RetryPolicy {
        max_retries,
        base: Duration::from_millis(base_ms),
        cap: Duration::from_millis(cap_ms),
        honor_retry_after: honor,
    }
}

// --- tests: interposition through a real source ----------------------------

const DOI: &str = "10.1234/retry";
const DOI_URL: &str = "https://doi.org/10.1234/retry";
const BODY: &str = r#"{"type":"article-journal","title":"Retry Worked","DOI":"10.1234/retry"}"#;

/// A flaky source that returns retryable 503s a few times is retried behind the
/// DOI source's back and eventually resolves — with exactly K backoff sleeps,
/// each one longer than the last.
#[test]
fn retryable_503_is_retried_until_it_succeeds() {
    const K: u32 = 3;
    let timer = RecordingTimer::default();
    let sleeps = timer.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, K, FailMode::Status(503), BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Resolved despite the transient failures.
    let item = block_on(mgr.get("doi", DOI)).unwrap();
    assert_eq!(item["id"], "doi:10.1234/retry");
    assert_eq!(item["title"], "Retry Worked");

    // Exactly K backoff sleeps: one before each of the K retries, and the
    // default policy's 500 ms base doubling each time (plus <=25% jitter).
    let d = sleeps.millis();
    assert_eq!(d.len(), K as usize, "expected exactly K backoff sleeps");
    for (i, ms) in d.iter().enumerate() {
        let nominal = 500u64 << i;
        assert!(
            (nominal..=nominal + nominal / 4).contains(ms),
            "delay {i} = {ms} ms is outside [{nominal}, {}]",
            nominal + nominal / 4
        );
    }
}

/// The `Err` path is retried too: retryable transport errors back off and retry.
#[test]
fn retryable_transport_error_is_retried_until_it_succeeds() {
    const K: u32 = 2;
    let timer = RecordingTimer::default();
    let sleeps = timer.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, K, FailMode::Transport, BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    assert_eq!(block_on(mgr.get("doi", DOI)).unwrap()["title"], "Retry Worked");
    assert_eq!(sleeps.millis().len(), K as usize);
}

/// A 404 is a hard, non-retryable status: it passes straight through with zero
/// backoff sleeps and the citation is reported as failed.
#[test]
fn non_retryable_404_is_not_retried() {
    // fail_first is huge, but the mode is 404 → the wrapper must not retry it.
    let timer = RecordingTimer::default();
    let sleeps = timer.clone();
    let fetcher = FlakyFetcher::new(DOI_URL, 99, FailMode::Status(404), BODY);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("doi".to_string(), DOI.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();

    // Reported as a failure, and never resolvable.
    assert_eq!(report.failures.len(), 1, "the 404 should surface as a failure");
    assert_eq!(report.failures[0].prefix, "doi");
    assert!(block_on(mgr.get("doi", DOI)).is_err());

    // Zero backoff sleeps: a 404 is not retried.
    assert!(sleeps.millis().is_empty(), "a 404 must not be retried");
}

/// A source is retried through the wrapper even when the server sends a
/// `Retry-After`, and the wait is the header's value.
#[test]
fn retry_after_reaches_the_wrapper_through_a_source() {
    let timer = RecordingTimer::default();
    let sleeps = timer.clone();
    let fetcher =
        FlakyFetcher::new(DOI_URL, 1, FailMode::Status(429), BODY).with_retry_after("3");
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), timer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("doi".to_string(), DOI.to_string())];
    assert!(block_on(mgr.retrieve(&cites)).unwrap().is_complete());
    assert_eq!(sleeps.millis(), vec![3000]);
}

// --- tests: the backoff schedule itself ------------------------------------

/// Retries are exhausted after `max_retries`, and the last response is then
/// passed through as-is.
#[test]
fn retries_are_exhausted_then_the_response_passes_through() {
    let fetcher = AlwaysFails::new(503);
    let d = delays_for(&fetcher, policy(2, 500, 30_000, true), "https://x.test/a");
    assert_eq!(d.len(), 2, "max_retries sleeps, then give up");
    assert_eq!(fetcher.calls.get(), 3, "max_retries + 1 attempts");
}

/// Every status in the retryable set is retried; everything else is not.
#[test]
fn the_retryable_status_set_is_429_500_502_503_504() {
    for status in [429, 500, 502, 503, 504] {
        let fetcher = AlwaysFails::new(status);
        let d = delays_for(&fetcher, policy(2, 500, 30_000, true), "https://x.test/a");
        assert_eq!(d.len(), 2, "status {status} should be retried");
    }
    for status in [200, 301, 400, 403, 404, 418, 501] {
        let fetcher = AlwaysFails::new(status);
        let d = delays_for(&fetcher, policy(2, 500, 30_000, true), "https://x.test/a");
        assert!(d.is_empty(), "status {status} must not be retried");
    }
}

/// The exponential term doubles per attempt and the jitter never exceeds 25%.
#[test]
fn backoff_doubles_with_bounded_jitter() {
    let fetcher = AlwaysFails::new(503);
    let d = delays_for(&fetcher, policy(4, 100, 30_000, true), "https://x.test/a");
    assert_eq!(d.len(), 4);
    for (i, ms) in d.iter().enumerate() {
        let nominal = 100u64 << i;
        assert!(
            (nominal..=nominal + nominal / 4 + 1).contains(ms),
            "delay {i} = {ms} ms is outside [{nominal}, {}]",
            nominal + nominal / 4 + 1
        );
    }
}

/// Once the exponential term reaches `cap`, the jitter must survive: capping
/// the *sum* erased it, so during a sustained outage every client retried on
/// exactly the same 30 s cadence. Two urls must still get different delays, and
/// neither may exceed the cap.
#[test]
fn jitter_survives_once_the_backoff_reaches_the_cap() {
    let p = policy(1, 30_000, 30_000, true);
    let a = delays_for(&AlwaysFails::new(503), p, "https://x.test/a");
    let b = delays_for(&AlwaysFails::new(503), p, "https://x.test/bbbb");
    for d in [&a, &b] {
        assert_eq!(d.len(), 1);
        assert!(
            (22_500..=30_000).contains(&d[0]),
            "capped delay {} ms outside the jitter window",
            d[0]
        );
    }
    assert_ne!(a[0], b[0], "the jitter was swallowed by the cap");
}

/// A numeric `Retry-After` wins over the computed backoff, but is clamped into
/// `[base, cap]` — `Retry-After: 0` used to produce a zero-delay hot loop.
#[test]
fn retry_after_is_honored_but_clamped() {
    let p = policy(3, 500, 30_000, true);

    let seven = delays_for(&AlwaysFails::new(503).with_retry_after("7"), p, "https://x.test/a");
    assert_eq!(seven, vec![7000, 7000, 7000]);

    let zero = delays_for(&AlwaysFails::new(429).with_retry_after("0"), p, "https://x.test/a");
    assert_eq!(zero, vec![500, 500, 500], "Retry-After: 0 must be floored");

    let huge =
        delays_for(&AlwaysFails::new(503).with_retry_after("99999"), p, "https://x.test/a");
    assert_eq!(huge, vec![30_000, 30_000, 30_000], "must be capped");

    // The HTTP-date form is unparseable here (no clock) → exponential backoff.
    let date = delays_for(
        &AlwaysFails::new(503).with_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"),
        p,
        "https://x.test/a",
    );
    assert!(
        (500..=625).contains(&date[0]) && (1000..=1250).contains(&date[1]),
        "expected exponential backoff, got {date:?}"
    );
}

/// With `honor_retry_after` off the header is not even parsed, let alone used.
#[test]
fn retry_after_is_ignored_when_the_policy_says_so() {
    let fetcher = AlwaysFails::new(503).with_retry_after("7");
    let d = delays_for(&fetcher, policy(2, 500, 30_000, false), "https://x.test/a");
    assert!(
        (500..=625).contains(&d[0]) && (1000..=1250).contains(&d[1]),
        "expected the exponential schedule, got {d:?}"
    );
}
