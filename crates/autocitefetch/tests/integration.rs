//! End-to-end tests: manual source, DOI-via-mock, and arXiv→DOI chaining.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::source::{DoiSource, ManualSource};
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, FetchError, Fetcher,
    Outcome, Request, Resolution, Response, Source, StoreError, Timer, Timestamp,
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

struct MockFetcher {
    routes: StdMap<String, (u16, Vec<u8>)>,
    calls: RefCell<Vec<String>>,
}

impl MockFetcher {
    fn new() -> Self {
        MockFetcher {
            routes: StdMap::new(),
            calls: RefCell::new(Vec::new()),
        }
    }
    fn route(mut self, url: &str, status: u16, body: &str) -> Self {
        self.routes
            .insert(url.into(), (status, body.as_bytes().to_vec()));
        self
    }
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.borrow_mut().push(req.url.clone());
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

struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: core::time::Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A stand-in `arxiv` source that always chains to a DOI, attaching `arxivid`.
struct ChainSource;
impl Source for ChainSource {
    fn prefix(&self) -> &str {
        "arxiv"
    }
    fn chains_to(&self) -> &[&'static str] {
        &["doi"]
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a autocitefetch::RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let mut sp = serde_json::Map::new();
                    sp.insert("arxivid".into(), CslValue::String(k.clone()));
                    Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: "doi".into(),
                            key: format!("10.9999/{k}"),
                            set_properties: CslValue::Object(sp),
                        },
                    }
                })
                .collect()
        })
    }
}

// --- tests -----------------------------------------------------------------

#[test]
fn manual_source_stores_verbatim_text() {
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register(ManualSource::new());

    let cites = vec![("manual".to_string(), "Bohr, N. (1913)".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("manual", "Bohr, N. (1913)")).unwrap();
    assert_eq!(item["_formatted_text"], "Bohr, N. (1913)");
    assert_eq!(item["id"], "manual:Bohr, N. (1913)");
}

#[test]
fn doi_source_parses_content_negotiated_csljson() {
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/PhysRev.47.777",
        200,
        r#"{"type":"article-journal","title":"Can Quantum-Mechanical Description…","DOI":"10.1103/PhysRev.47.777"}"#,
    );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), "10.1103/PhysRev.47.777".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("doi", "10.1103/PhysRev.47.777")).unwrap();
    assert_eq!(item["id"], "doi:10.1103/PhysRev.47.777");
    assert_eq!(item["DOI"], "10.1103/PhysRev.47.777");
    assert!(item["title"].as_str().unwrap().starts_with("Can Quantum"));
}

#[test]
fn arxiv_chains_to_doi_and_merges_set_properties() {
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.9999/1211.1037",
        200,
        r#"{"type":"article-journal","title":"Chained Title","DOI":"10.9999/1211.1037"}"#,
    );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ChainSource)
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1211.1037".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Reading the arXiv key follows the chain to the DOI entry, rewrites the
    // id back to the requested one, and merges the chained `arxivid`.
    let item = block_on(mgr.get("arxiv", "1211.1037")).unwrap();
    assert_eq!(item["id"], "arxiv:1211.1037");
    assert_eq!(item["title"], "Chained Title");
    assert_eq!(item["arxivid"], "1211.1037", "set_properties should be merged in");
    assert_eq!(item["DOI"], "10.9999/1211.1037");
}

#[test]
fn unknown_prefix_is_reported_not_fatal() {
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register(ManualSource::new());

    let cites = vec![
        ("nope".to_string(), "x".to_string()),
        ("manual".to_string(), "ok".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "nope");
    // The good one still resolved.
    assert!(block_on(mgr.get("manual", "ok")).is_ok());
}
