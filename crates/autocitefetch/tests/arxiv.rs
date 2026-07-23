//! Integration tests for the `arxiv` source: Atom-feed parsing, CSL mapping,
//! version resolution, and DOI chaining (via a mock `Fetcher`).

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::source::{ArxivSource, DoiSource};
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

// --- canned Atom feeds -----------------------------------------------------

// A versionless request whose entry carries an `<arxiv:doi>` (mixed case, to
// exercise lowercasing). The id is `…v1`; the request is versionless.
const FEED_CHAINED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <id>http://arxiv.org/api/query-feed-id</id>
  <entry>
    <id>http://arxiv.org/abs/1211.1037v1</id>
    <published>2012-11-05T18:30:00Z</published>
    <title>A Chained arXiv Paper</title>
    <author><name>Ada Lovelace</name></author>
    <arxiv:doi>10.1103/PhysRevLett.109.170502</arxiv:doi>
    <link href="http://arxiv.org/abs/1211.1037v1" rel="alternate" type="text/html"/>
  </entry>
</feed>"#;

// A versionless request, concrete resolution (chaining disabled). Title spans
// lines (whitespace collapse), authors exercise the family/given split
// including a single-token name.
const FEED_CONCRETE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <id>http://arxiv.org/api/query-feed-id</id>
  <entry>
    <id>http://arxiv.org/abs/0905.2794v3</id>
    <published>2013-03-20T09:15:00Z</published>
    <title>Quantum Error Correction
      for   Beginners</title>
    <author><name>Simon J. Devitt</name></author>
    <author><name>Kae Nemoto</name></author>
    <author><name>Aristotle</name></author>
  </entry>
</feed>"#;

// --- tests -----------------------------------------------------------------

#[test]
fn arxiv_versionless_with_doi_chains_and_merges() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1211.1037&max_results=1";
    // The DOI is lowercased when chaining, so doi.org is asked for the lower form.
    let doi_url = "https://doi.org/10.1103/physrevlett.109.170502";

    let fetcher = MockFetcher::new()
        .route(arxiv_url, 200, FEED_CHAINED)
        .route(
            doi_url,
            200,
            r#"{"type":"article-journal","title":"Resolved via DOI","DOI":"10.1103/PhysRevLett.109.170502"}"#,
        );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1211.1037".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Reading the arXiv key follows the chain to the DOI entry, rewrites the id
    // back to the requested one, and merges the chained `arxivid`.
    let item = block_on(mgr.get("arxiv", "1211.1037")).unwrap();
    assert_eq!(item["id"], "arxiv:1211.1037");
    assert_eq!(item["title"], "Resolved via DOI");
    assert_eq!(item["arxivid"], "1211.1037", "chained set_properties merged in");
    assert_eq!(item["DOI"], "10.1103/PhysRevLett.109.170502");
}

#[test]
fn arxiv_concrete_parses_title_authors_and_issued() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=0905.2794&max_results=1";

    let fetcher = MockFetcher::new().route(arxiv_url, 200, FEED_CONCRETE);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource { chain_to_doi: false })
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "0905.2794".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "0905.2794")).unwrap();

    assert_eq!(item["id"], "arxiv:0905.2794");
    assert_eq!(item["type"], "article-journal");
    // Internal whitespace/newlines collapse to single spaces.
    assert_eq!(item["title"], "Quantum Error Correction for Beginners");

    // Authors: naive last-token = family, rest = given; single token = family.
    let authors = item["author"].as_array().expect("author array");
    assert_eq!(authors.len(), 3);
    assert_eq!(authors[0]["family"], "Devitt");
    assert_eq!(authors[0]["given"], "Simon J.");
    assert_eq!(authors[1]["family"], "Nemoto");
    assert_eq!(authors[1]["given"], "Kae");
    assert_eq!(authors[2]["family"], "Aristotle");
    assert!(authors[2].get("given").is_none(), "single token = family only");

    // issued from `<published>` = 2013-03-20.
    let dp = &item["issued"]["date-parts"][0];
    assert_eq!(dp[0], 2013);
    assert_eq!(dp[1], 3);
    assert_eq!(dp[2], 20);

    // arXiv extension fields: id stripped of version, latest version captured.
    assert_eq!(item["arxivid"], "0905.2794");
    assert_eq!(item["arxiv_version_number"], 3);
}

#[test]
fn arxiv_unknown_id_is_reported_not_fatal() {
    // The feed returns no matching entry for the requested key.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=9999.99999&max_results=1";
    let fetcher = MockFetcher::new().route(
        arxiv_url,
        200,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <entry>
    <id>http://arxiv.org/api/errors#incorrect_id_format</id>
    <title>Error</title>
  </entry>
</feed>"#,
    );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "9999.99999".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "arxiv");
    assert!(block_on(mgr.get("arxiv", "9999.99999")).is_err());
}
