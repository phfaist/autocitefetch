//! Integration tests for the arXiv source's DOI-override map / file and its
//! careful multi-version resolution. Mocks (Fetcher/Store/Clock/Timer +
//! `block_on`) are copied from `tests/arxiv.rs`.

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

// --- helpers ---------------------------------------------------------------

/// Build a minimal arXiv Atom feed from `(id_suffix, optional_doi)` entries,
/// where `id_suffix` is what follows `http://arxiv.org/abs/` (e.g. `1801.5v2`).
fn make_feed(entries: &[(&str, Option<&str>)]) -> String {
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <feed xmlns=\"http://www.w3.org/2005/Atom\" \
         xmlns:arxiv=\"http://arxiv.org/schemas/atom\">\n",
    );
    for (id_suffix, doi) in entries {
        s.push_str("  <entry>\n");
        s.push_str(&format!(
            "    <id>http://arxiv.org/abs/{id_suffix}</id>\n"
        ));
        s.push_str("    <published>2019-01-15T00:00:00Z</published>\n");
        s.push_str(&format!("    <title>Title {id_suffix}</title>\n"));
        s.push_str("    <author><name>Jane Doe</name></author>\n");
        if let Some(d) = doi {
            s.push_str(&format!("    <arxiv:doi>{d}</arxiv:doi>\n"));
        }
        s.push_str("  </entry>\n");
    }
    s.push_str("</feed>");
    s
}

fn kv(k: &str, v: &str) -> (String, Option<String>) {
    (k.to_string(), Some(v.to_string()))
}

/// A suppressing override entry (`None` ⇒ no DOI, keep arXiv metadata).
fn suppress(k: &str) -> (String, Option<String>) {
    (k.to_string(), None)
}

// --- Task 1: DOI-override map ----------------------------------------------

#[test]
fn override_map_injects_doi_when_feed_has_none() {
    // The feed entry carries NO <arxiv:doi>; the inline override injects one and
    // the versionless key chains to that override DOI.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=2001.00001&max_results=1";
    let doi_url = "https://doi.org/10.9999/override.abc";
    let feed = make_feed(&[("2001.00001v2", None)]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed).route(
        doi_url,
        200,
        r#"{"type":"article-journal","title":"Resolved Via Override","DOI":"10.9999/OVERRIDE.abc"}"#,
    );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois([kv("2001.00001", "10.9999/override.abc")]))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "2001.00001".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "2001.00001")).unwrap();
    assert_eq!(item["id"], "arxiv:2001.00001");
    assert_eq!(item["title"], "Resolved Via Override");
    assert_eq!(item["arxivid"], "2001.00001", "chained set_properties merged in");
    // doi.org's uppercase `DOI` is normalized on ingest to a lowercase `doi`
    // key with a lowercased value; the CSL-spec uppercase key does not survive.
    assert_eq!(item["doi"], "10.9999/override.abc");
    assert_eq!(item.get("DOI"), None, "uppercase DOI key must not survive");
}

#[test]
fn override_map_beats_feed_doi() {
    // The feed reports 10.1111/feed.doi, but the override forces a different DOI.
    // Only the OVERRIDE doi.org URL is routed: if the feed DOI had won, the chain
    // would 404 and the citation would fail. Success proves override precedence.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=2002.00002&max_results=1";
    let override_doi_url = "https://doi.org/10.9999/override.win";
    let feed = make_feed(&[("2002.00002v1", Some("10.1111/feed.doi"))]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed).route(
        override_doi_url,
        200,
        r#"{"type":"article-journal","title":"Override Wins"}"#,
    );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois([kv("2002.00002", "10.9999/override.win")]))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "2002.00002".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "override DOI should drive chaining: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "2002.00002")).unwrap();
    assert_eq!(item["title"], "Override Wins");
    assert_eq!(item["arxivid"], "2002.00002");
}

#[test]
fn override_file_entry_drives_chaining() {
    // A DOI supplied only by the loadable JSON file drives chaining.
    let file_url = "https://host.example/arxiv-dois.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=4001.00004&max_results=1";
    let doi_url = "https://doi.org/10.5555/file.only";
    let feed = make_feed(&[("4001.00004v1", None)]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, r#"{"4001.00004":"10.5555/file.only"}"#)
        .route(arxiv_url, 200, &feed)
        .route(
            doi_url,
            200,
            r#"{"type":"article-journal","title":"From File"}"#,
        );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "4001.00004".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "4001.00004")).unwrap();
    assert_eq!(item["title"], "From File");
    assert_eq!(item["arxivid"], "4001.00004");
}

#[test]
fn inline_override_beats_file_override() {
    // Both the file and the inline map name a DOI for 3001.00003; the INLINE one
    // must win. Only the inline doi.org URL is routed, so success proves it.
    let file_url = "https://host.example/arxiv-dois.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=3001.00003&max_results=1";
    let inline_doi_url = "https://doi.org/10.7777/inline.wins";
    let feed = make_feed(&[("3001.00003v1", None)]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, r#"{"3001.00003":"10.5555/file.doi"}"#)
        .route(arxiv_url, 200, &feed)
        .route(
            inline_doi_url,
            200,
            r#"{"type":"article-journal","title":"Inline Wins"}"#,
        );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(
            ArxivSource::new()
                .with_override_dois_file(file_url)
                .with_override_dois([kv("3001.00003", "10.7777/inline.wins")]),
        )
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "3001.00003".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "inline DOI should win over file: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "3001.00003")).unwrap();
    assert_eq!(item["title"], "Inline Wins");
}

#[test]
fn override_file_load_failure_fails_keys_gracefully() {
    // A missing override file must degrade to per-key failures, not a panic.
    let file_url = "https://host.example/missing.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=5001.00005&max_results=1";
    let feed = make_feed(&[("5001.00005v1", None)]);

    // Note: the file URL is deliberately NOT routed (404).
    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "5001.00005".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "arxiv");
    // Pin *which* fetch failed: the arXiv feed itself is routed and healthy, so
    // a regression that broke the feed fetch instead must not pass this test.
    assert!(
        report.failures[0].message.contains("DOI-override file"),
        "the override file must be named as the cause: {}",
        report.failures[0].message
    );
    assert!(block_on(mgr.get("arxiv", "5001.00005")).is_err());
}

#[test]
fn override_file_must_be_a_json_object() {
    // A JSON array is not a `{id: doi}` map. The arXiv feed IS routed, so only
    // the shape error can fail this key — and it must not panic.
    let file_url = "https://host.example/bad-shape.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=5002.00005&max_results=1";
    let feed = make_feed(&[("5002.00005v1", None)]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, "[1,2]")
        .route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url));

    let cites = vec![("arxiv".to_string(), "5002.00005".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("not a JSON object"),
        "unexpected message: {}",
        report.failures[0].message
    );
}

#[test]
fn override_file_values_must_be_string_or_null() {
    let file_url = "https://host.example/bad-value.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=5003.00005&max_results=1";
    let feed = make_feed(&[("5003.00005v1", None)]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, r#"{"x": 5}"#)
        .route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url));

    let cites = vec![("arxiv".to_string(), "5003.00005".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("must be a string or null"),
        "unexpected message: {}",
        report.failures[0].message
    );
}

// --- Task 2: careful multi-version resolution ------------------------------

#[test]
fn versionless_request_selects_highest_version() {
    // The feed returns BOTH v1 and v2 for the same base id (no DOI ⇒ concrete).
    // A versionless request must select the highest version (v2), not the first.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1801.00002&max_results=1";
    let feed = make_feed(&[("1801.00002v1", None), ("1801.00002v2", None)]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1801.00002".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1801.00002")).unwrap();
    assert_eq!(item["id"], "arxiv:1801.00002");
    assert_eq!(item["arxiv_version_number"], 2, "highest version selected");
    assert_eq!(item["title"], "Title 1801.00002v2");
}

#[test]
fn versionless_request_prefers_a_versionless_entry() {
    // If the feed also returns an entry whose id has NO version suffix, that one
    // is the answer to a versionless query even over a numbered entry.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1802.00003&max_results=1";
    let feed = make_feed(&[("1802.00003v5", None), ("1802.00003", None)]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1802.00003".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1802.00003")).unwrap();
    assert_eq!(
        item["arxiv_version_number"],
        serde_json::Value::Null,
        "the versionless entry wins over a numbered one"
    );
    // …and it is that entry's *content* that was kept, not just its version.
    assert_eq!(
        item["title"], "Title 1802.00003",
        "the versionless entry supplied the metadata"
    );
}

#[test]
fn versionless_request_compares_versions_numerically() {
    // v1 / v11 / v2 in feed order: a lexicographic comparison would pick v2.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1803.00004&max_results=1";
    let feed = make_feed(&[
        ("1803.00004v1", None),
        ("1803.00004v11", None),
        ("1803.00004v2", None),
    ]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1803.00004".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1803.00004")).unwrap();
    assert_eq!(item["arxiv_version_number"], 11, "11 > 2, numerically");
    assert_eq!(item["title"], "Title 1803.00004v11");
}

#[test]
fn explicit_version_missing_from_the_feed_fails_rather_than_substituting() {
    // v2 was requested but the feed only returned v3. Silently answering with
    // v3 would misattribute the metadata, so this must be a plain failure.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1804.00005v2&max_results=1";
    let feed = make_feed(&[("1804.00005v3", Some("10.6666/three"))]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1804.00005v2".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "arxiv");
    assert_eq!(report.failures[0].key, "1804.00005v2");
    assert!(
        report.failures[0]
            .message
            .contains("no arXiv entry returned"),
        "unexpected message: {}",
        report.failures[0].message
    );
    assert!(block_on(mgr.get("arxiv", "1804.00005v2")).is_err());
}

#[test]
fn versionless_and_versioned_requests_for_one_paper_coexist_in_a_batch() {
    // Both keys land in the same chunk and select *different* entries of the
    // same paper: the versionless one chains through v3's DOI, the explicit v2
    // stays concrete at v2 (and does not chain, even though it has a DOI).
    let arxiv_url =
        "https://export.arxiv.org/api/query?id_list=1805.00006,1805.00006v2&max_results=2";
    let doi_url = "https://doi.org/10.8888/three";
    let feed = make_feed(&[
        ("1805.00006v3", Some("10.8888/three")),
        ("1805.00006v2", Some("10.8888/TWO")),
    ]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed).route(
        doi_url,
        200,
        r#"{"type":"article-journal","title":"Latest Version Via DOI"}"#,
    );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![
        ("arxiv".to_string(), "1805.00006".to_string()),
        ("arxiv".to_string(), "1805.00006v2".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let latest = block_on(mgr.get("arxiv", "1805.00006")).unwrap();
    assert_eq!(latest["id"], "arxiv:1805.00006");
    assert_eq!(latest["title"], "Latest Version Via DOI");
    assert_eq!(latest["arxivid"], "1805.00006");

    let pinned = block_on(mgr.get("arxiv", "1805.00006v2")).unwrap();
    assert_eq!(pinned["id"], "arxiv:1805.00006v2");
    assert_eq!(pinned["title"], "Title 1805.00006v2");
    assert_eq!(pinned["arxiv_version_number"], 2);
    assert_eq!(pinned["doi"], "10.8888/two", "recorded but not chained");
}

#[test]
fn explicit_version_selects_that_version_and_stays_concrete() {
    // The same multi-version feed; an explicit v1 request must pick v1 and NOT
    // chain, even though that entry carries a DOI. No doi.org route is
    // registered, so any chain attempt would fail the citation.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1801.00002v1&max_results=1";
    let feed = make_feed(&[
        ("1801.00002v1", Some("10.2222/should.not.chain")),
        ("1801.00002v2", None),
    ]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1801.00002v1".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "explicit version must be concrete, not chained: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "1801.00002v1")).unwrap();
    assert_eq!(item["id"], "arxiv:1801.00002v1");
    assert_eq!(item["arxiv_version_number"], 1, "exact version preserved");
    assert_eq!(item["title"], "Title 1801.00002v1");
    // The DOI is still recorded on the concrete entry (lowercased) but not chained.
    assert_eq!(item["doi"], "10.2222/should.not.chain");
}

// --- Task 1b: DOI suppression (None) ---------------------------------------

#[test]
fn override_none_suppresses_feed_doi() {
    // The feed reports a DOI, but the inline override maps the id → None
    // (suppress). No doi.org route is registered: if chaining happened it would
    // 404 and fail. Success + a doi-less concrete entry proves suppression.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=6001.00006&max_results=1";
    let feed = make_feed(&[("6001.00006v1", Some("10.3333/feed.doi"))]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois([suppress("6001.00006")]))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "6001.00006".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "suppressed DOI must not chain: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "6001.00006")).unwrap();
    // Concrete arXiv metadata, NOT a chained DOI entry.
    assert_eq!(item["id"], "arxiv:6001.00006");
    assert_eq!(item["title"], "Title 6001.00006v1");
    assert_eq!(item["arxiv_version_number"], 1);
    assert!(item.get("doi").is_none(), "suppressed DOI should be absent");
}

#[test]
fn override_file_null_suppresses_doi() {
    // A JSON override-file value of `null` suppresses the feed's DOI.
    let file_url = "https://host.example/arxiv-dois.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=7001.00007&max_results=1";
    let feed = make_feed(&[("7001.00007v1", Some("10.4444/feed.doi"))]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, r#"{"7001.00007":null}"#)
        .route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "7001.00007".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "7001.00007")).unwrap();
    assert_eq!(item["title"], "Title 7001.00007v1");
    assert!(item.get("doi").is_none(), "null in file suppresses the DOI");
}

// --- Task 1c: a BLANK override DOI is no DOI, not the empty DOI -------------

#[test]
fn empty_string_override_does_not_chain_to_the_empty_doi_key() {
    // An override of `Some("")` must read like the suppressing `null`, not like
    // a DOI: chaining to the empty key would fetch `https://doi.org/`, fail,
    // and report a failure under an EMPTY `doi` key while discarding the arXiv
    // metadata. No doi.org route is registered, so a chain would fail the test.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=8001.00008&max_results=1";
    let feed = make_feed(&[("8001.00008v1", Some("10.5555/feed.doi"))]);

    let fetcher = MockFetcher::new().route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois([kv("8001.00008", "")]))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "8001.00008".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "a blank override must not chain: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "8001.00008")).unwrap();
    assert_eq!(item["id"], "arxiv:8001.00008");
    assert_eq!(item["title"], "Title 8001.00008v1");
    assert!(item.get("doi").is_none(), "a blank DOI is no DOI");
}

#[test]
fn empty_string_in_override_file_does_not_chain_to_the_empty_doi_key() {
    // Same guard for the file path: `{"<id>": ""}` behaves like `null`.
    let file_url = "https://host.example/arxiv-dois.json";
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=8002.00008&max_results=1";
    let feed = make_feed(&[("8002.00008v1", Some("10.5555/feed.doi"))]);

    let fetcher = MockFetcher::new()
        .route(file_url, 200, r#"{"8002.00008":""}"#)
        .route(arxiv_url, 200, &feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().with_override_dois_file(file_url))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "8002.00008".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "a blank override must not chain: {:?}",
        report.failures
    );

    let item = block_on(mgr.get("arxiv", "8002.00008")).unwrap();
    assert_eq!(item["title"], "Title 8002.00008v1");
    assert!(item.get("doi").is_none(), "a blank DOI is no DOI");
}
