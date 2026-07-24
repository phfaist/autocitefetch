//! Progress reporting: what a [`Reporter`] actually sees during a retrieval.
//!
//! These tests double as the readable specification of the event vocabulary,
//! and they exercise properties nothing else could observe from outside —
//! that the pacing sleep and the retry backoff are announced, and that a
//! grace-served failure (invisible in the `RetrieveReport` by design) is
//! reported to the host after all.
//!
//! Mocks are duplicated per test file by convention; these are the usual ones
//! plus a `RecordingReporter`.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::report::{Event, Reporter, Resolved, Wait};
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, Error, FetchError,
    Fetcher, Payload, Request, Resolution, Response, RetrieveCtx, Source, StoreError, Timer,
    Timestamp,
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

// --- the reporter under test ----------------------------------------------

/// Flattens every event to a short stable string. `Rc`-shared so it stays
/// readable after being handed to the manager as an `Rc<dyn Reporter>`.
#[derive(Clone, Default)]
struct RecordingReporter(Rc<RefCell<Vec<String>>>);

impl RecordingReporter {
    fn log(&self) -> Vec<String> {
        self.0.borrow().clone()
    }

    /// Position of the first line equal to `line`; panics if absent, so an
    /// ordering assertion fails with a useful message rather than on `None`.
    fn index_of(&self, line: &str) -> usize {
        let log = self.log();
        log.iter()
            .position(|l| l == line)
            .unwrap_or_else(|| panic!("no `{line}` in:\n{}", log.join("\n")))
    }

    fn count_starting_with(&self, prefix: &str) -> usize {
        self.log().iter().filter(|l| l.starts_with(prefix)).count()
    }

    fn assert_has(&self, line: &str) {
        let log = self.log();
        assert!(
            log.iter().any(|l| l == line),
            "expected `{line}` in:\n{}",
            log.join("\n")
        );
    }

    fn assert_lacks(&self, prefix: &str) {
        let log = self.log();
        assert!(
            !log.iter().any(|l| l.starts_with(prefix)),
            "unexpected `{prefix}…` in:\n{}",
            log.join("\n")
        );
    }
}

impl Reporter for RecordingReporter {
    fn report(&self, ev: &Event<'_>) {
        let line = match *ev {
            Event::RetrieveStarted { cites } => format!("retrieve:start cites={cites}"),
            Event::PassStarted {
                pass,
                cached,
                to_fetch,
            } => format!("pass{pass}:start cached={cached} fetch={to_fetch}"),
            Event::PassFinished { pass, discovered } => {
                format!("pass{pass}:end discovered={discovered}")
            }
            Event::RetrieveFinished { considered, failed } => {
                format!("retrieve:end considered={considered} failed={failed}")
            }
            Event::SourceStarted {
                prefix,
                keys,
                chunks,
            } => format!("{prefix}:start keys={keys} chunks={chunks}"),
            Event::SourceProgress {
                prefix,
                done,
                total,
            } => format!("{prefix}:progress {done}/{total}"),
            Event::SourceFinished { prefix, done } => format!("{prefix}:end done={done}"),
            Event::CiteResolved { prefix, key, how } => match how {
                Resolved::Concrete => format!("resolved {prefix}:{key}"),
                Resolved::Chained {
                    prefix: tp,
                    key: tk,
                } => format!("chained {prefix}:{key} -> {tp}:{tk}"),
                _ => format!("resolved? {prefix}:{key}"),
            },
            Event::CiteFailed {
                prefix,
                key,
                grace_served,
                ..
            } => format!("failed {prefix}:{key} grace={grace_served}"),
            Event::WaitStarted { what, expected } => {
                format!("wait:start {} {:?}", wait_name(what), expected)
            }
            Event::WaitFinished { what } => format!("wait:end {}", wait_name(what)),
            Event::RequestStarted { url, attempt } => format!("req:start {url} #{attempt}"),
            Event::RequestFinished { url, result } => match result {
                Ok(status) => format!("req:end {url} {status}"),
                Err(_) => format!("req:end {url} ERR"),
            },
            _ => "unknown".to_string(),
        };
        self.0.borrow_mut().push(line);
    }
}

fn wait_name(w: Wait<'_>) -> String {
    match w {
        Wait::RateLimit { prefix } => format!("ratelimit({prefix})"),
        Wait::Backoff { url, attempt } => format!("backoff({url},#{attempt})"),
        Wait::CacheFlush => "flush".to_string(),
        _ => "other".to_string(),
    }
}

// --- the usual mocks -------------------------------------------------------

/// Serves a fixed route table — empty in every test here, since the sources
/// that do I/O use [`FlakyFetcher`]. An unrouted URL is a *non-retryable* 404,
/// so a test that expects no fetch fails fast instead of backing off five times.
#[derive(Default)]
struct MockFetcher {
    routes: StdMap<String, (u16, Vec<u8>)>,
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        let result = match self.routes.get(&req.url) {
            Some((status, body)) => Ok(Response {
                status: *status,
                headers: Default::default(),
                body: body.clone(),
            }),
            None => Err(FetchError::Status(404)),
        };
        Box::pin(async move { result })
    }
}

/// Fails the first `fail_times` requests with a retryable status, then serves
/// `body`. Exercises the retry path without any real network.
struct FlakyFetcher {
    fail_times: RefCell<u32>,
    body: String,
}

impl Fetcher for FlakyFetcher {
    fn fetch(&self, _req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        let mut left = self.fail_times.borrow_mut();
        let status = if *left > 0 {
            *left -= 1;
            503
        } else {
            200
        };
        let body = if status == 200 {
            self.body.as_bytes().to_vec()
        } else {
            Vec::new()
        };
        Box::pin(async move {
            Ok(Response {
                status,
                headers: Default::default(),
                body,
            })
        })
    }
}

#[derive(Default)]
struct MemStore {
    map: RefCell<StdMap<String, CacheRecord>>,
}

impl MemStore {
    fn seed(self, id: &str, rec: CacheRecord) -> Self {
        self.map.borrow_mut().insert(id.into(), rec);
        self
    }
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

struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

// --- test sources ----------------------------------------------------------

fn item(id: &str) -> String {
    format!(r#"{{"id":"{id}","title":"t"}}"#)
}

/// Resolves every key to concrete CSL without any I/O. `chunk_size` and
/// `min_interval` are settable so a test can force several chunks and a pacing
/// sleep between them.
struct PlainSource {
    chunk: usize,
    interval: Duration,
}

impl Source for PlainSource {
    fn chunk_size(&self) -> usize {
        self.chunk
    }
    fn min_interval(&self) -> Duration {
        self.interval
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let csl: CslValue = serde_json::from_str(&item(&k)).unwrap();
                    Resolution::concrete(k, csl)
                })
                .collect()
        })
    }
}

/// Chains every key to `doi:10.9999/<key>`, like the real arXiv source.
struct ChainSource;

impl Source for ChainSource {
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let target = format!("10.9999/{k}");
                    Resolution::chained(k, "doi", target, CslValue::Null)
                })
                .collect()
        })
    }
}

/// Always unreachable — the `Outcome::Failed` half of the miss contract, which
/// is what makes the grace window apply.
struct FailingSource;

impl Source for FailingSource {
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| Resolution::failed(k, Error::Source("upstream is down".into())))
                .collect()
        })
    }
}

/// Fetches one URL per key through `ctx.fetcher`, so the retry wrapper is in
/// the path.
struct HttpSource;

impl Source for HttpSource {
    fn chunk_size(&self) -> usize {
        1
    }
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            let mut out = Vec::new();
            for k in keys {
                let url = format!("https://example.test/{k}");
                let res = match ctx.fetcher.fetch(Request::get(&url)).await {
                    Ok(resp) if resp.status == 200 => {
                        match serde_json::from_slice::<CslValue>(&resp.body) {
                            Ok(csl) => Resolution::concrete(k, csl),
                            Err(e) => Resolution::failed(k, Error::Parse(e.to_string())),
                        }
                    }
                    Ok(resp) => Resolution::failed(
                        k,
                        Error::Source(format!("status {}", resp.status)),
                    ),
                    Err(e) => Resolution::failed(k, Error::Fetch(e)),
                };
                out.push(res);
            }
            out
        })
    }
}

fn cite(prefix: &str, key: &str) -> (String, String) {
    (prefix.to_string(), key.to_string())
}

// --- tests -----------------------------------------------------------------

#[test]
fn a_simple_retrieval_is_narrated_end_to_end() {
    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default(),
        FixedClock(0),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register(
        "bib",
        PlainSource {
            chunk: 512,
            interval: Duration::ZERO,
        },
    )
    .unwrap();

    let report = block_on(mgr.retrieve(&[cite("bib", "knuth1984")])).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    for line in [
        "retrieve:start cites=1",
        "pass1:start cached=0 fetch=1",
        "bib:start keys=1 chunks=1",
        "bib:progress 1/1",
        "bib:end done=1",
        "resolved bib:knuth1984",
        "pass1:end discovered=0",
        "wait:start flush None",
        "wait:end flush",
        "retrieve:end considered=1 failed=0",
    ] {
        rep.assert_has(line);
    }

    // The order the whole design depends on: lifecycle brackets everything,
    // and the flush is announced after the last pass, not during it.
    assert!(rep.index_of("retrieve:start cites=1") < rep.index_of("pass1:start cached=0 fetch=1"));
    assert!(rep.index_of("pass1:end discovered=0") < rep.index_of("wait:start flush None"));
    assert!(rep.index_of("wait:end flush") < rep.index_of("retrieve:end considered=1 failed=0"));
}

#[test]
fn source_progress_arrives_before_the_pass_applies_its_resolutions() {
    // The manager applies a pass's resolutions serially *after* every source
    // returned, so `CiteResolved` comes in one burst at the end. The driver's
    // `SourceProgress` is what advances during the pass — a progress display
    // built on the wrong one would jump straight from 0% to 100%.
    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default(),
        FixedClock(0),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register(
        "bib",
        PlainSource {
            chunk: 1,
            interval: Duration::ZERO,
        },
    )
    .unwrap();

    block_on(mgr.retrieve(&[cite("bib", "a"), cite("bib", "b")])).unwrap();

    // Every chunk reported progress before *any* citation was applied.
    assert!(rep.index_of("bib:progress 1/2") < rep.index_of("resolved bib:a"));
    assert!(rep.index_of("bib:progress 2/2") < rep.index_of("resolved bib:a"));
}

#[test]
fn pacing_between_chunks_is_announced_with_its_duration() {
    // Two chunks of one key each, 1100 ms apart: the gap before the second is
    // exactly the kind of silence that reads as a hang.
    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default(),
        FixedClock(0),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register(
        "doi",
        PlainSource {
            chunk: 1,
            interval: Duration::from_millis(1100),
        },
    )
    .unwrap();

    block_on(mgr.retrieve(&[cite("doi", "10.1/a"), cite("doi", "10.1/b")])).unwrap();

    rep.assert_has("wait:start ratelimit(doi) Some(1.1s)");
    rep.assert_has("wait:end ratelimit(doi)");
    // One gap for two chunks: pacing is start→start, and the first chunk has no
    // predecessor to be paced against.
    assert_eq!(rep.count_starting_with("wait:start ratelimit"), 1);
}

#[test]
fn a_chain_shows_two_passes_and_a_growing_denominator() {
    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default(),
        FixedClock(0),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register("arxiv", ChainSource)
    .unwrap()
    .register(
        "doi",
        PlainSource {
            chunk: 512,
            interval: Duration::ZERO,
        },
    )
    .unwrap();

    let report = block_on(mgr.retrieve(&[cite("arxiv", "1211.1037")])).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    rep.assert_has("chained arxiv:1211.1037 -> doi:10.9999/1211.1037");
    // Pass 1 queued the chain target; pass 2 fetched it. This is the event that
    // tells a host its denominator just grew.
    rep.assert_has("pass1:end discovered=1");
    rep.assert_has("pass2:start cached=0 fetch=1");
    rep.assert_has("resolved doi:10.9999/1211.1037");
    // Two citations were touched for one request: the chain target counts.
    rep.assert_has("retrieve:end considered=2 failed=0");
    // Exactly one flush, at the very end — not one per pass.
    assert_eq!(rep.count_starting_with("wait:start flush"), 1);
}

#[test]
fn retry_backoff_is_announced_with_each_attempt() {
    let rep = RecordingReporter::default();
    let fetcher = FlakyFetcher {
        fail_times: RefCell::new(2),
        body: item("doi:10.1/x"),
    };
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .with_reporter(Rc::new(rep.clone()))
        .register("doi", HttpSource)
        .unwrap();

    let report = block_on(mgr.retrieve(&[cite("doi", "10.1/x")])).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let url = "https://example.test/10.1/x";
    // Three attempts: two 503s and the 200 that finally worked.
    assert_eq!(rep.count_starting_with(&format!("req:start {url}")), 3);
    rep.assert_has(&format!("req:end {url} 503"));
    rep.assert_has(&format!("req:end {url} 200"));
    // …separated by two announced backoffs, which are otherwise entirely
    // silent (up to 30 s each with the default policy).
    assert_eq!(rep.count_starting_with("wait:start backoff"), 2);
    rep.assert_has(&format!("wait:end backoff({url},#0)"));
    rep.assert_has(&format!("wait:end backoff({url},#1)"));
}

#[test]
fn a_grace_served_failure_is_reported_even_though_it_is_not_in_the_report() {
    // Stale-while-revalidate: the source is down but a cached copy is still
    // inside the grace window, so `retrieve` reports success. Without the event
    // this — the one moment the grace window does its job — would be invisible.
    let now = 1_000_000_000i64;
    let csl: CslValue = serde_json::from_str(&item("doi:10.1/x")).unwrap();
    let stale = CacheRecord {
        payload: Payload::Concrete(csl),
        stale_after: Timestamp::from_millis(now - 20_000),
        // Hard-expired ten seconds ago, far inside the 14-day grace window.
        expires: Timestamp::from_millis(now - 10_000),
    };

    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default().seed("doi:10.1/x", stale),
        FixedClock(now),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register("doi", FailingSource)
    .unwrap();

    let report = block_on(mgr.retrieve(&[cite("doi", "10.1/x")])).unwrap();
    assert!(
        report.is_complete(),
        "a grace-served failure must not reach the report: {:?}",
        report.failures
    );

    rep.assert_has("failed doi:10.1/x grace=true");
    rep.assert_has("retrieve:end considered=1 failed=0");
    // The cached copy is still what `get` returns.
    let got = block_on(mgr.get("doi", "10.1/x")).unwrap();
    assert_eq!(got.get("title").and_then(|v| v.as_str()), Some("t"));
}

#[test]
fn an_authoritative_miss_is_reported_as_a_real_failure() {
    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default(),
        FixedClock(0),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register("doi", MissingSource)
    .unwrap();

    let report = block_on(mgr.retrieve(&[cite("doi", "10.1/nope")])).unwrap();
    assert_eq!(report.failures.len(), 1);

    rep.assert_has("failed doi:10.1/nope grace=false");
    rep.assert_has("retrieve:end considered=1 failed=1");
    rep.assert_lacks("resolved ");
}

/// Reachable, but authoritatively has no such key.
struct MissingSource;

impl Source for MissingSource {
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let e = Error::NotFound(format!("doi:{k}"));
                    Resolution::missing(k, e)
                })
                .collect()
        })
    }
}

#[test]
fn a_fully_cached_run_reports_cache_hits_and_drives_no_source() {
    let now = 1_000_000_000i64;
    let csl: CslValue = serde_json::from_str(&item("doi:10.1/x")).unwrap();
    let fresh = CacheRecord {
        payload: Payload::Concrete(csl),
        stale_after: Timestamp::from_millis(now + 60_000),
        expires: Timestamp::from_millis(now + 120_000),
    };

    let rep = RecordingReporter::default();
    let mgr = CitationManager::new(
        MockFetcher::default(),
        MemStore::default().seed("doi:10.1/x", fresh),
        FixedClock(now),
        InstantTimer,
    )
    .with_reporter(Rc::new(rep.clone()))
    .register("doi", FailingSource)
    .unwrap();

    block_on(mgr.retrieve(&[cite("doi", "10.1/x")])).unwrap();

    rep.assert_has("pass1:start cached=1 fetch=0");
    // Nothing was driven, so no source events at all — the counts in
    // `PassStarted` are the only thing a display has to work from here.
    rep.assert_lacks("doi:start");
    rep.assert_lacks("req:start");
    rep.assert_has("retrieve:end considered=1 failed=0");
}
