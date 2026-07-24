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
/// The chain target prefix is configuration, as it is for the real
/// [`ArxivSource`] — a source cannot assume what the host called its DOI source.
struct ChainSource {
    doi_prefix: &'static str,
}
impl Source for ChainSource {
    fn chains_to(&self) -> Vec<&str> {
        vec![self.doi_prefix]
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
                            prefix: self.doi_prefix.into(),
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
        .register("manual", ManualSource::new("flm")).unwrap();

    let cites = vec![("manual".to_string(), "Bohr, N. (1913)".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("manual", "Bohr, N. (1913)")).unwrap();
    // `_ready_formatted` is a map from format name to text, and the format name
    // is the one the source was constructed with.
    assert_eq!(item["_ready_formatted"]["flm"], "Bohr, N. (1913)");
    assert_eq!(item["id"], "manual:Bohr, N. (1913)");
}

#[test]
fn manual_source_preserves_key_case_and_whitespace() {
    // `manual` overrides `Source::normalize_key` with the IDENTITY: the key IS
    // the formatted citation text, so both surrounding whitespace and case are
    // significant and must survive verbatim into storage and back out of `get`
    // — where every other source would have trimmed, and the default policy
    // would also have lowercased.
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register("manual", ManualSource::new("flm")).unwrap();

    let padded = "  Bohr, N. (1913). ON THE CONSTITUTION of Atoms  ".to_string();
    let cites = vec![("manual".to_string(), padded.clone())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Stored under (and retrievable by) the padded id, whitespace and case intact.
    let item = block_on(mgr.get("manual", &padded)).unwrap();
    assert_eq!(
        item["_ready_formatted"]["flm"], padded,
        "text preserved verbatim"
    );
    assert_eq!(
        item["id"],
        "manual:  Bohr, N. (1913). ON THE CONSTITUTION of Atoms  ",
        "the echoed id keeps the key's whitespace and case"
    );
    // Neither the trimmed nor the lowercased key is the same citation here —
    // nothing was stored for either.
    for other in [
        "Bohr, N. (1913). ON THE CONSTITUTION of Atoms",
        "  bohr, n. (1913). on the constitution of atoms  ",
    ] {
        assert!(
            block_on(mgr.get("manual", other)).is_err(),
            "a manual key must not collapse with {other:?}"
        );
    }
}

#[test]
fn manual_format_name_is_configuration() {
    // The inner key under `_ready_formatted` is whatever the host said the text
    // is written in — nothing here assumes the JS reference's hard-coded `flm`.
    // Two prefixes, two formats, one manager.
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register("manual", ManualSource::new("latex")).unwrap()
        .register("html", ManualSource::new("html")).unwrap();

    let cites = vec![
        ("manual".to_string(), r"Bohr, N. \emph{Phil.\ Mag.} (1913)".to_string()),
        ("html".to_string(), "Bohr, N. <em>Phil. Mag.</em> (1913)".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    for (prefix, key) in &cites {
        let item = block_on(mgr.get(prefix, key)).unwrap();
        let formatted = item["_ready_formatted"].as_object().expect("a format map");
        assert_eq!(formatted.len(), 1, "exactly one format is emitted");
        let format = if prefix == "manual" { "latex" } else { "html" };
        assert_eq!(formatted[format], key.as_str());
    }
}

#[test]
fn doi_source_parses_content_negotiated_csljson() {
    // `doi` keeps the default `Source::normalize_key` (trim + lowercase), so the
    // requested mixed-case DOI reaches doi.org — and the cache — lowercased.
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/physrev.47.777",
        200,
        r#"{"type":"article-journal","title":"Can Quantum-Mechanical Description…","DOI":"10.1103/PhysRev.47.777"}"#,
    );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("doi".to_string(), "10.1103/PhysRev.47.777".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("doi", "10.1103/PhysRev.47.777")).unwrap();
    assert_eq!(item["id"], "doi:10.1103/physrev.47.777", "the cache id is normalized");
    // doi.org already spells the key with the CSL-standard uppercase `DOI`, and
    // the value is stored verbatim — mixed case and all.
    assert_eq!(item["DOI"], "10.1103/PhysRev.47.777");
    assert_eq!(item.get("doi"), None, "lowercase doi key must not survive");
    assert!(item["title"].as_str().unwrap().starts_with("Can Quantum"));
}

#[test]
fn doi_key_surrounding_whitespace_is_trimmed_centrally() {
    // A DOI key arriving with incidental surrounding whitespace is trimmed by the
    // manager (doi keeps the default policy) *before* it reaches the source — so
    // it resolves normally, and the padded and clean forms dedup to one fetch.
    // Note the doi source itself still rejects a key that *contains* internal
    // whitespace (see `a_malformed_doi_is_rejected_without_fetching_anything`):
    // only *surrounding* whitespace is stripped, internal runs are deliberately
    // left for validation to reject rather than silently collapsed.
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/physrev.47.777",
        200,
        r#"{"type":"article-journal","title":"Trimmed DOI","DOI":"10.1103/PhysRev.47.777"}"#,
    );
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![
        ("doi".to_string(), "  10.1103/physrev.47.777  ".to_string()),
        ("doi".to_string(), "10.1103/physrev.47.777".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    // doi's chunk_size is 1, so two undeduped keys would be two fetches.
    assert_eq!(calls.len(), 1, "padded + clean DOI must dedup to one fetch: {:?}", calls.urls());

    // Both forms read the one entry, stored under the trimmed id.
    let item = block_on(mgr.get("doi", "  10.1103/physrev.47.777  ")).unwrap();
    assert_eq!(item["id"], "doi:10.1103/physrev.47.777", "stored under the trimmed id");
    assert_eq!(item["title"], "Trimmed DOI");
    assert_eq!(item["DOI"], "10.1103/PhysRev.47.777", "the DOI *field* is verbatim");
}

#[test]
fn mixed_case_doi_keys_dedup_to_one_fetch_and_one_entry() {
    // DOIs are case-insensitive identifiers, and `doi` keeps the default
    // `Source::normalize_key` (trim + lowercase). So the registered mixed-case
    // spelling and the lowercase one are ONE citation: one fetch (of the
    // lowercased URL), one cache entry, readable through either spelling.
    const MIXED: &str = "10.1103/PhysRevA.86.052329";
    const LOWER: &str = "10.1103/physreva.86.052329";
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.1103/physreva.86.052329",
        200,
        r#"{"type":"article-journal","title":"Case Folded","DOI":"10.1103/PhysRevA.86.052329"}"#,
    );
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![
        ("doi".to_string(), MIXED.to_string()),
        ("doi".to_string(), LOWER.to_string()),
        // Padding on top of mixed case: still the same citation.
        ("doi".to_string(), format!("  {MIXED} ")),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // doi's chunk_size is 1, so three undeduped keys would be three fetches.
    assert_eq!(calls.len(), 1, "case variants must dedup to one fetch: {:?}", calls.urls());
    assert_eq!(calls.urls(), vec!["https://doi.org/10.1103/physreva.86.052329".to_string()]);

    let entries = block_on(mgr.store().entries()).unwrap();
    assert_eq!(entries.len(), 1, "one cache entry expected: {entries:?}");
    assert_eq!(entries[0].0, "doi:10.1103/physreva.86.052329");

    // Either spelling reads that one entry; the echoed id is the normalized one,
    // while the CSL `DOI` field keeps the DOI's registered mixed case.
    for k in [MIXED, LOWER, "  10.1103/PHYSREVA.86.052329  "] {
        let item = block_on(mgr.get("doi", k)).unwrap();
        assert_eq!(item["id"], "doi:10.1103/physreva.86.052329", "for {k:?}");
        assert_eq!(item["title"], "Case Folded");
        assert_eq!(item["DOI"], MIXED, "the CSL DOI field stays verbatim");
    }
}

#[test]
fn doi_source_canonicalizes_doi_key_to_uppercase_keeping_value_verbatim() {
    // The stored CSL uses the standard uppercase `DOI` key with the value kept
    // verbatim (DOIs display in their registered mixed case). A body that
    // already spells it `DOI` is passed through untouched; a body using a
    // nonstandard lowercase `doi` is renamed up to `DOI`, value unchanged.
    // Either way no lowercase `doi` survives, and no other field is touched.
    const MIXED: &str = "10.1103/PhysRevA.86.052329";
    // The *request* keys are lowercased by the manager, so the routes are the
    // lowercased URLs; the response *bodies* still carry mixed-case DOI values.
    let fetcher = MockFetcher::new()
        // Already canonical: uppercase key, mixed-case value.
        .route(
            "https://doi.org/10.1103/physrevlett.109.170502",
            200,
            r#"{"type":"article-journal","title":"Mixed Case DOI","DOI":"10.1103/PhysRevLett.109.170502","URL":"https://doi.org/10.1103/PhysRevLett.109.170502"}"#,
        )
        // Nonstandard lowercase key, mixed-case value.
        .route(
            "https://doi.org/10.1103/physreva.86.052329",
            200,
            r#"{"type":"article-journal","title":"Lowercase Key","doi":"10.1103/PhysRevA.86.052329"}"#,
        );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![
        ("doi".to_string(), "10.1103/PhysRevLett.109.170502".to_string()),
        ("doi".to_string(), MIXED.to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Already-uppercase body: left as-is, value verbatim.
    let item = block_on(mgr.get("doi", "10.1103/PhysRevLett.109.170502")).unwrap();
    assert_eq!(item["DOI"], "10.1103/PhysRevLett.109.170502", "value verbatim");
    assert_eq!(item.get("doi"), None, "lowercase doi key must not survive");
    // Only the DOI *key* is canonicalized; other fields keep their casing.
    assert_eq!(item["title"], "Mixed Case DOI");
    assert_eq!(item["URL"], "https://doi.org/10.1103/PhysRevLett.109.170502");

    // Lowercase-key body: renamed to `DOI`, mixed-case value survives intact.
    let item = block_on(mgr.get("doi", MIXED)).unwrap();
    assert_eq!(item["DOI"], MIXED, "renamed to `DOI`, value untouched");
    assert_eq!(item.get("doi"), None, "lowercase doi key must not survive");
    assert_eq!(item["title"], "Lowercase Key");
}

#[test]
fn arxiv_chains_to_doi_and_merges_set_properties() {
    let fetcher = MockFetcher::new().route(
        "https://doi.org/10.9999/1211.1037",
        200,
        r#"{"type":"article-journal","title":"Chained Title","DOI":"10.9999/1211.1037"}"#,
    );
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("arxiv", ChainSource { doi_prefix: "doi" }).unwrap()
        .register("doi", DoiSource::new()).unwrap();

    let cites = vec![("arxiv".to_string(), "1211.1037".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Reading the arXiv key follows the chain to the DOI entry, rewrites the
    // id back to the requested one, and merges the chained `arxivid`.
    let item = block_on(mgr.get("arxiv", "1211.1037")).unwrap();
    assert_eq!(item["id"], "arxiv:1211.1037");
    assert_eq!(item["title"], "Chained Title");
    assert_eq!(item["arxivid"], "1211.1037", "set_properties should be merged in");
    // Chained through the doi source, which stores the canonical uppercase
    // `DOI` key with the value verbatim.
    assert_eq!(item["DOI"], "10.9999/1211.1037");
    assert_eq!(item.get("doi"), None, "lowercase doi key must not survive");
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
    // Requested in its registered mixed case; the manager lowercases the key, so
    // the URL encodes the lowercased form (the percent-escapes' hex digits are
    // the encoder's own and stay uppercase).
    const KEY: &str = "10.1002/(SICI)1096-8628(20000403)91:4<317::AID-AJMG16>3.0.CO;2-9";
    const URL: &str = "https://doi.org/10.1002/%28sici%291096-8628%2820000403%2991%3A4%3C317%3A%3Aaid-ajmg16%3E3.0.co%3B2-9";

    let fetcher = MockFetcher::new().route(URL, 200, r#"{"type":"article-journal","title":"Encoded"}"#);
    let calls = fetcher.calls();
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();

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
        .register("doi", DoiSource::new()).unwrap();

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
        .register("doi", DoiSource::new()).unwrap();

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
        .register("doi", DoiSource::new()).unwrap();

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
            .register("doi", DoiSource::new()).unwrap();

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
            .register("doi", DoiSource::new()).unwrap();

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
            .register("doi", DoiSource::new()).unwrap();

        let cites = vec![("doi".to_string(), key.to_string())];
        let report = block_on(mgr.retrieve(&cites)).unwrap();
        assert_eq!(report.failures.len(), 1, "key {key:?}: {:?}", report.failures);
        assert_eq!(calls.len(), 0, "key {key:?} must not reach the network");
    }
}

#[test]
fn unknown_prefix_is_reported_not_fatal() {
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock(0), InstantTimer)
        .register("manual", ManualSource::new("flm")).unwrap();

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
