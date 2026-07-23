//! Tests for the bib source's data-in constructor and pluggable parser.
//! Mocks + `block_on` mirror the other integration tests.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::source::BibliographyFileSource;
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, FetchError, Fetcher,
    Request, Response, StoreError, Timer, Timestamp,
};

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete");
}

struct MockFetcher {
    routes: StdMap<String, (u16, Vec<u8>)>,
}
impl MockFetcher {
    fn new() -> Self {
        MockFetcher { routes: StdMap::new() }
    }
    fn route(mut self, url: &str, status: u16, body: &[u8]) -> Self {
        self.routes.insert(url.into(), (status, body.to_vec()));
        self
    }
}
impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        let result = match self.routes.get(&req.url) {
            Some((status, body)) => Ok(Response {
                status: *status,
                headers: Default::default(),
                body: body.clone(),
            }),
            None => Err(FetchError::Transport(format!("no route: {}", req.url))),
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

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(0)
    }
}
struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: core::time::Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[test]
fn from_entries_resolves_without_any_fetch() {
    // No routes: if the source tried to fetch anything, it would error.
    let bib = BibliographyFileSource::from_entries([(
        "knuth1984".to_string(),
        serde_json::json!({"id":"knuth1984","type":"book","title":"The TeXbook"}),
    )]);
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites = vec![("bib".to_string(), "knuth1984".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("bib", "knuth1984")).unwrap();
    assert_eq!(item["title"], "The TeXbook");
    assert_eq!(item["id"], "bib:knuth1984");
}

#[test]
fn with_parser_uses_the_injected_parser() {
    // The file body is deliberately NOT valid JSON. The default JSON parser
    // would fail; a custom parser that ignores the bytes proves injection.
    let file_url = "https://host.example/refs.bib";
    let fetcher = MockFetcher::new().route(file_url, 200, b"THIS IS NOT JSON");

    let parse_custom: fn(&[u8]) -> Result<CslValue, String> = |_bytes| {
        Ok(serde_json::json!([{"id":"x1","type":"article-journal","title":"Custom Parsed"}]))
    };

    let bib = BibliographyFileSource::new([file_url.to_string()]).with_parser(parse_custom);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites = vec![("bib".to_string(), "x1".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("bib", "x1")).unwrap();
    assert_eq!(item["title"], "Custom Parsed");
}
