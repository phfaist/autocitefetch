//! Tests of [`SessionCache`]: autocitefetch's cache on a session of a host's
//! cachette `Manager`. `retrieve` then `get` on a manager in a temporary
//! folder, the same on `Manager::none()`, and the instance key that keeps
//! caches with different storage flags apart.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::cachette::{Manager, ManagerSettings, Report, Reporter};
use autocitefetch::source::{DoiSource, ManualSource};
use autocitefetch::{
    BoxFuture, CacheStore, CitationManager, Clock, FetchError, Fetcher, Request, Response,
    StoragePolicy, Timer, Timestamp,
};
use autocitefetch_std::SessionCache;
use serde_json::json;

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

/// A fetcher with a URL → body route table; an unrouted URL fails. `Clone`
/// shares the call log.
#[derive(Clone, Default)]
struct MockFetcher {
    routes: Rc<HashMap<String, Vec<u8>>>,
    calls: Rc<RefCell<Vec<String>>>,
}

impl MockFetcher {
    fn with_routes(routes: &[(&str, &str)]) -> Self {
        MockFetcher {
            routes: Rc::new(
                routes
                    .iter()
                    .map(|(url, body)| (url.to_string(), body.as_bytes().to_vec()))
                    .collect(),
            ),
            calls: Rc::default(),
        }
    }
    fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.borrow_mut().push(req.url.clone());
        let result = match self.routes.get(&req.url) {
            Some(body) => Ok(Response {
                status: 200,
                headers: Default::default(),
                body: body.clone(),
            }),
            None => Err(FetchError::Transport(format!("unrouted: {}", req.url))),
        };
        Box::pin(async move { result })
    }
}

/// One clock for both the cachette manager (which needs `Send + Sync`) and
/// the citation manager.
#[derive(Clone, Copy)]
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

/// A cachette reporter that keeps every report, so a test can check that the
/// manager reported no failure.
#[derive(Clone, Default)]
struct KeepReports(Arc<Mutex<Vec<String>>>);
impl Reporter for KeepReports {
    fn report(&self, report: &Report) {
        self.0.lock().unwrap().push(report.to_string());
    }
}

// --- helpers ---------------------------------------------------------------

const NOW: i64 = 1_700_000_000_000;
const DOI_URL: &str = "https://doi.org/10.1/x";
const MANUAL_TEXT: &str = "A. Author, *Some Book* (2020)";

fn doi_body() -> String {
    json!({"title": "X", "DOI": "10.1/X", "abstract": "A long abstract."}).to_string()
}

fn cites() -> Vec<(String, String)> {
    vec![
        ("doi".to_string(), "10.1/x".to_string()),
        ("manual".to_string(), MANUAL_TEXT.to_string()),
    ]
}

/// A manager over `cache` with the `doi` and `manual` sources.
fn manager(
    fetcher: &MockFetcher,
    cache: SessionCache,
) -> CitationManager<MockFetcher, SessionCache, FixedClock, InstantTimer> {
    CitationManager::new(fetcher.clone(), cache, FixedClock(NOW), InstantTimer)
        .register("doi", DoiSource::new()).unwrap()
        .register("manual", ManualSource::new("flm")).unwrap()
}

/// Retrieve the two cites, then check that `get` answers both.
fn retrieve_and_get(mgr: &CitationManager<MockFetcher, SessionCache, FixedClock, InstantTimer>) {
    let report = block_on(mgr.retrieve(&cites())).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    let doi = mgr.get("doi", "10.1/x").unwrap();
    assert_eq!(doi["title"], "X");
    assert_eq!(doi["id"], "doi:10.1/x");
    assert!(doi.get("abstract").is_none(), "omitted by the default policy");
    assert_eq!(mgr.get("manual", MANUAL_TEXT).unwrap()["_ready_formatted"]["flm"], MANUAL_TEXT);
}

/// The text of every file under `dir`, recursively.
fn all_file_text(dir: &std::path::Path) -> String {
    let mut text = String::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            text.push_str(&all_file_text(&path));
        } else {
            text.push_str(&String::from_utf8_lossy(&std::fs::read(&path).unwrap()));
        }
    }
    text
}

// --- the tests -------------------------------------------------------------

/// `retrieve` then `get` on a manager in a temporary folder. The items are
/// kept by the session: a later `SessionCache` on the same instance reads the
/// DOI entry without a request. The ephemeral `manual` item is in no file.
#[test]
fn retrieve_then_get_on_a_manager_in_a_folder() {
    let root = tempfile::tempdir().unwrap();
    let reports = KeepReports::default();
    let cachette =
        Manager::open_path(root.path(), ManagerSettings::default(), FixedClock(NOW), reports.clone()).unwrap();
    let fetcher = MockFetcher::with_routes(&[(DOI_URL, &doi_body())]);

    let cache = SessionCache::open(&cachette, "project-a", StoragePolicy::default()).unwrap();
    assert!(cache.has_cache());
    let mgr = manager(&fetcher, cache);
    retrieve_and_get(&mgr);
    assert_eq!(fetcher.calls(), [DOI_URL]);
    // Closing the session (dropping it) makes its writes durable.
    drop(mgr);

    let text = all_file_text(root.path());
    assert!(text.contains(r#""concrete":{"#), "the 0.1.1 record shape: {text}");
    assert!(!text.contains("Some Book"), "an ephemeral item reached a file: {text}");
    assert!(!text.contains("A long abstract."), "an omitted field reached a file");

    // A new session on the same instance reads the DOI entry from the
    // instance, not from any memory; nothing is fetched.
    let fetcher = MockFetcher::default();
    let cache = SessionCache::open(&cachette, "project-a", StoragePolicy::default()).unwrap();
    let found = cache.get("doi:10.1/x").unwrap().expect("kept by the session");
    assert_eq!(found.item["title"], "X");
    assert!(found.expiry.stale_after < found.expiry.expires);
    assert!(cache.get(&format!("manual:{MANUAL_TEXT}")).unwrap().is_none(), "ephemeral");
    let mgr = manager(&fetcher, cache);
    let report = block_on(mgr.retrieve(&cites()[..1])).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new());
    assert_eq!(mgr.get("doi", "10.1/x").unwrap()["title"], "X");
    drop(mgr);

    assert_eq!(*reports.0.lock().unwrap(), Vec::<String>::new(), "no failure reported");
}

/// On `Manager::none()` the session keeps nothing, and `retrieve` then `get`
/// works all the same, from the write-through memory.
#[test]
fn retrieve_then_get_on_manager_none() {
    let fetcher = MockFetcher::with_routes(&[(DOI_URL, &doi_body())]);
    let cache = SessionCache::open(&Manager::none(), &(), StoragePolicy::default()).unwrap();
    assert!(!cache.has_cache());
    let mgr = manager(&fetcher, cache);
    retrieve_and_get(&mgr);
    assert_eq!(fetcher.calls(), [DOI_URL]);
    // `prune` and `flush` answer without error; the host's manager prunes.
    assert_eq!(mgr.prune().unwrap(), 0);
    // A removal is remembered too.
    mgr.store().remove("doi:10.1/x").unwrap();
    assert!(mgr.get("doi", "10.1/x").is_err());
}

/// Two policies with different `keep_*` flags open different instances: an
/// item stored by one is not seen by the other, while the same flags (with
/// another TTL policy) open the same instance.
#[test]
fn different_keep_flags_open_different_instances() {
    let root = tempfile::tempdir().unwrap();
    let cachette = Manager::open_path(
        root.path(),
        ManagerSettings::default(),
        FixedClock(NOW),
        KeepReports::default(),
    )
    .unwrap();
    let keep_abstract = StoragePolicy {
        keep_abstract: true,
        ..StoragePolicy::default()
    };
    assert_ne!(
        SessionCache::instance_key("p", &StoragePolicy::default()).unwrap(),
        SessionCache::instance_key("p", &keep_abstract).unwrap()
    );

    let now = Timestamp::from_millis(NOW);
    let hour = Duration::from_secs(3600);
    let one = SessionCache::open(&cachette, "p", keep_abstract).unwrap();
    one.put("doi:10.1/x", &json!({"title": "X", "abstract": "A"}), hour, now).unwrap();
    drop(one);

    let other = SessionCache::open(&cachette, "p", StoragePolicy::default()).unwrap();
    assert!(other.get("doi:10.1/x").unwrap().is_none(), "the instances are mixed");
    drop(other);

    let ttl = autocitefetch::TtlPolicy {
        jitter_percent: 0,
        ..Default::default()
    };
    let same = SessionCache::open(&cachette, "p", StoragePolicy { ttl, ..keep_abstract }).unwrap();
    let found = same.get("doi:10.1/x").unwrap().expect("same flags, same instance");
    assert_eq!(found.item["abstract"], "A");
}
