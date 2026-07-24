//! End-to-end tests: manual source, DOI-via-mock, and arXiv→DOI chaining.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
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

/// A handle on what a [`MockFetcher`] was asked to fetch. Shared, so it stays
/// readable after the fetcher is moved into the manager.
#[derive(Clone, Default)]
struct Calls(Rc<RefCell<Vec<Request>>>);

impl Calls {
    fn urls(&self) -> Vec<String> {
        self.0.borrow().iter().map(|r| r.url.clone()).collect()
    }
    fn len(&self) -> usize {
        self.0.borrow().len()
    }
    fn last(&self) -> Request {
        self.0.borrow().last().cloned().expect("no fetch was made")
    }
}

struct MockFetcher {
    routes: StdMap<String, (u16, Vec<u8>)>,
    calls: Calls,
}

impl MockFetcher {
    fn new() -> Self {
        MockFetcher {
            routes: StdMap::new(),
            calls: Calls::default(),
        }
    }
    fn route(mut self, url: &str, status: u16, body: &str) -> Self {
        self.routes
            .insert(url.into(), (status, body.as_bytes().to_vec()));
        self
    }
    /// Take a handle on the call log before handing the fetcher to a manager.
    fn calls(&self) -> Calls {
        self.calls.clone()
    }
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.0.borrow_mut().push(req.clone());
        let result = match self.routes.get(&req.url) {
            Some((status, body)) => Ok(Response {
                status: *status,
                headers: Default::default(),
                body: body.clone(),
            }),
            // Unrouted: a *non-retryable* failure, so a test that expects a
            // URL never to be hit fails fast instead of backing off five times.
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
fn manual_source_preserves_leading_and_trailing_whitespace() {
    // `manual` opts OUT of key trimming (`trim_key_whitespace() == false`): the
    // key IS the formatted citation text, so surrounding whitespace is
    // significant and must survive verbatim into storage and back out of `get`.
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register(ManualSource::new());

    let padded = "  Bohr, N. (1913)  ".to_string();
    let cites = vec![("manual".to_string(), padded.clone())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Stored under (and retrievable by) the padded id, whitespace intact.
    let item = block_on(mgr.get("manual", &padded)).unwrap();
    assert_eq!(item["_formatted_text"], padded, "whitespace preserved verbatim");
    assert_eq!(item["id"], "manual:  Bohr, N. (1913)  ");
    // The trimmed key is a *different* citation here — nothing was stored for it.
    assert!(
        block_on(mgr.get("manual", "Bohr, N. (1913)")).is_err(),
        "a manual key is not collapsed with its trimmed form"
    );
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
    // doi.org's canonical uppercase `DOI` is normalized on ingest to a lowercase
    // `doi` key with a lowercased value; the uppercase key does not survive.
    assert_eq!(item["doi"], "10.1103/physrev.47.777");
    assert_eq!(item.get("DOI"), None, "uppercase DOI key must not survive");
    assert!(item["title"].as_str().unwrap().starts_with("Can Quantum"));
}

#[test]
fn doi_key_surrounding_whitespace_is_trimmed_centrally() {
    // A DOI key arriving with incidental surrounding whitespace is trimmed by the
    // manager (doi keeps the default policy) *before* it reaches the source — so
    // it resolves normally, and the padded and clean forms dedup to one fetch.
    // Note the doi source itself still rejects a key that *contains* internal
    // whitespace (see `a_malformed_doi_is_rejected_without_fetching_anything`);
    // only the surrounding whitespace is stripped here, and only that. The key's
    // own case is preserved (unlike the lowercased `doi` value in the body).
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/PhysRev.47.777",
        200,
        r#"{"type":"article-journal","title":"Trimmed DOI","DOI":"10.1103/PhysRev.47.777"}"#,
    );
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![
        ("doi".to_string(), "  10.1103/PhysRev.47.777  ".to_string()),
        ("doi".to_string(), "10.1103/PhysRev.47.777".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    // doi's chunk_size is 1, so two undeduped keys would be two fetches.
    assert_eq!(calls.len(), 1, "padded + clean DOI must dedup to one fetch: {:?}", calls.urls());

    // Both forms read the one entry, stored under the trimmed (case-preserved) id.
    let item = block_on(mgr.get("doi", "  10.1103/PhysRev.47.777  ")).unwrap();
    assert_eq!(item["id"], "doi:10.1103/PhysRev.47.777", "stored under the trimmed id");
    assert_eq!(item["title"], "Trimmed DOI");
    assert_eq!(item["doi"], "10.1103/physrev.47.777", "the DOI value is still lowercased");
}

#[test]
fn doi_source_normalizes_uppercase_doi_key_and_lowercases_value() {
    // doi.org returns the CSL-spec uppercase `DOI` key, often with a mixed-case
    // value. Ingest must fold it to a lowercase `doi` key with a lowercased
    // value, leaving no uppercase key behind. Every other field is untouched.
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/PhysRevLett.109.170502",
        200,
        r#"{"type":"article-journal","title":"Mixed Case DOI","DOI":"10.1103/PhysRevLett.109.170502","URL":"https://doi.org/10.1103/PhysRevLett.109.170502"}"#,
    );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), "10.1103/PhysRevLett.109.170502".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("doi", "10.1103/PhysRevLett.109.170502")).unwrap();
    assert_eq!(item["doi"], "10.1103/physrevlett.109.170502", "value lowercased");
    assert_eq!(item.get("DOI"), None, "uppercase DOI key must not survive");
    // Only the DOI key is normalized; other verbatim fields keep their casing.
    assert_eq!(item["title"], "Mixed Case DOI");
    assert_eq!(item["URL"], "https://doi.org/10.1103/PhysRevLett.109.170502");
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
    // Chained through the doi source, so the uppercase `DOI` is normalized to a
    // lowercase `doi` key on ingest (this value has no letters to lowercase).
    assert_eq!(item["doi"], "10.9999/1211.1037");
    assert_eq!(item.get("DOI"), None, "uppercase DOI key must not survive");
}

#[test]
fn response_header_lookup_is_case_insensitive_in_both_directions() {
    // `Response::headers` only *asks* fetchers to lowercase names. A host that
    // stores the canonical `Retry-After` used to make `header()` return `None`,
    // so the retry layer ignored an explicit `503 Retry-After: 120` and backed
    // off on its own schedule instead.
    let mut resp = Response {
        status: 503,
        headers: Default::default(),
        body: Vec::new(),
    };
    resp.headers.insert("Retry-After".into(), "120".into());
    resp.headers.insert("content-type".into(), "text/plain".into());

    assert_eq!(resp.header("retry-after"), Some("120"));
    assert_eq!(resp.header("Retry-After"), Some("120"));
    assert_eq!(resp.header("RETRY-AFTER"), Some("120"));
    assert_eq!(resp.header("Content-Type"), Some("text/plain"));
    assert_eq!(resp.header("x-absent"), None);
}

#[test]
fn doi_url_percent_encodes_the_key_but_keeps_slashes() {
    // A real DOI with parentheses, angle brackets, a colon and a semicolon.
    const KEY: &str = "10.1002/(SICI)1096-8628(20000403)91:4<317::AID-AJMG16>3.0.CO;2-9";
    const URL: &str = "https://doi.org/10.1002/%28SICI%291096-8628%2820000403%2991%3A4%3C317%3A%3AAID-AJMG16%3E3.0.CO%3B2-9";

    let fetcher = MockFetcher::new().route(URL, 200, r#"{"type":"article-journal","title":"Encoded"}"#);
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), KEY.to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    // If the encoding were wrong the URL would not match the route and the
    // mock would 404, so this alone pins the whole encoding.
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let urls = calls.urls();
    assert_eq!(urls, vec![URL.to_string()]);
    assert!(
        urls[0].starts_with("https://doi.org/10.1002/"),
        "a DOI's own `/` separator must stay unencoded: {}",
        urls[0]
    );
}

#[test]
fn doi_requests_carry_the_csl_json_accept_header() {
    // Without this, doi.org serves an HTML landing page instead of CSL-JSON.
    let fetcher =
        MockFetcher::new().route("https://doi.org/10.1/x", 200, r#"{"title":"t"}"#);
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), "10.1/x".to_string())];
    block_on(mgr.retrieve(&cites)).unwrap();

    let req = calls.last();
    assert_eq!(
        req.headers.get("accept").map(String::as_str),
        Some("application/vnd.citationstyles.csl+json"),
        "headers were {:?}",
        req.headers
    );
}

#[test]
fn doi_non_success_status_is_one_reported_failure() {
    // A non-404 non-success status (e.g. a 403 the retrying fetcher does not
    // retry) is a reachability `Failed`, reported with the status named. A 404
    // is handled separately — see `doi_404_is_an_authoritative_missing`.
    let fetcher = MockFetcher::new().route("https://doi.org/10.1/forbidden", 403, "Forbidden");
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), "10.1/forbidden".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!(report.failures[0].prefix, "doi");
    assert_eq!(report.failures[0].key, "10.1/forbidden");
    assert!(
        report.failures[0].message.contains("403"),
        "message should name the status: {}",
        report.failures[0].message
    );
    // Nothing was cached, so reading it back fails too.
    assert!(block_on(mgr.get("doi", "10.1/forbidden")).is_err());
}

/// A doi.org 404 is authoritative "no such DOI" (`Outcome::Missing`), so it is
/// always reported and nothing is cached — distinct from a 5xx/403 reachability
/// `Failed`. (That a `Missing` also *removes* a stale grace-window copy is
/// pinned generically in `manager_contract::missing_is_reported_and_removes_...`.)
#[test]
fn doi_404_is_an_authoritative_missing() {
    let fetcher = MockFetcher::new().route("https://doi.org/10.1/gone", 404, "Not Found");
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(DoiSource::new());

    let cites = vec![("doi".to_string(), "10.1/gone".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!(report.failures[0].key, "10.1/gone");
    // The message is the authoritative "not found", not a bare status line.
    assert!(
        report.failures[0].message.contains("not found"),
        "a 404 should read as authoritative not-found: {}",
        report.failures[0].message
    );
    // Nothing was cached.
    assert!(block_on(mgr.get("doi", "10.1/gone")).is_err());
}

#[test]
fn doi_body_that_is_not_json_is_a_parse_failure() {
    // A captive portal / landing page, and a truncated response.
    for body in ["<!DOCTYPE html><html><body>Landing</body></html>", ""] {
        let fetcher = MockFetcher::new().route("https://doi.org/10.1/x", 200, body);
        let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
            .register(DoiSource::new());

        let cites = vec![("doi".to_string(), "10.1/x".to_string())];
        let report = block_on(mgr.retrieve(&cites)).unwrap();
        assert_eq!(report.failures.len(), 1, "body {body:?}: {:?}", report.failures);
        assert!(
            report.failures[0].message.contains("parse error"),
            "body {body:?} should be a parse error, got {}",
            report.failures[0].message
        );
        assert!(block_on(mgr.get("doi", "10.1/x")).is_err(), "body {body:?} must not be cached");
    }
}

#[test]
fn doi_200_with_json_that_is_not_a_csl_object_is_a_failure() {
    // All of these parse as JSON. Accepting them used to cache an empty
    // `{"id":"doi:…"}` shell for 360 days with an empty failure report.
    for body in ["null", "[]", "\"nope\"", "123", "{}"] {
        let fetcher = MockFetcher::new().route("https://doi.org/10.1/x", 200, body);
        let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
            .register(DoiSource::new());

        let cites = vec![("doi".to_string(), "10.1/x".to_string())];
        let report = block_on(mgr.retrieve(&cites)).unwrap();
        assert_eq!(report.failures.len(), 1, "body {body} was accepted: {:?}", report.failures);
        assert!(
            block_on(mgr.get("doi", "10.1/x")).is_err(),
            "body {body} must not be cached"
        );
    }
}

#[test]
fn a_malformed_doi_is_rejected_without_fetching_anything() {
    // Whitespace would corrupt the path; an empty key would fetch doi.org's
    // homepage and cache whatever came back.
    for key in ["10.1103/Phys Rev.47.777", "", "   "] {
        let fetcher = MockFetcher::new();
        let calls = fetcher.calls();
        let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
            .register(DoiSource::new());

        let cites = vec![("doi".to_string(), key.to_string())];
        let report = block_on(mgr.retrieve(&cites)).unwrap();
        assert_eq!(report.failures.len(), 1, "key {key:?}: {:?}", report.failures);
        assert_eq!(calls.len(), 0, "key {key:?} must not reach the network");
    }
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
