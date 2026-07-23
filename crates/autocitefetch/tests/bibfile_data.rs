//! Tests for the bib source's data-in constructor and pluggable parser.
//! Mocks + `block_on` mirror the other integration tests.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

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
    calls: Rc<Cell<usize>>,
}
impl MockFetcher {
    fn new() -> Self {
        MockFetcher {
            routes: StdMap::new(),
            calls: Rc::new(Cell::new(0)),
        }
    }
    fn route(mut self, url: &str, status: u16, body: &[u8]) -> Self {
        self.routes.insert(url.into(), (status, body.to_vec()));
        self
    }
    /// A handle on the fetch counter, kept after the fetcher is moved away.
    fn calls(&self) -> Rc<Cell<usize>> {
        Rc::clone(&self.calls)
    }
}
impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.set(self.calls.get() + 1);
        let result = match self.routes.get(&req.url) {
            Some((status, body)) => Ok(Response {
                status: *status,
                headers: Default::default(),
                body: body.clone(),
            }),
            // Matches the other test files: an unrouted URL fails immediately.
            // `Transport` would be *retryable*, turning a typo into a 6-attempt
            // retry storm with 5 backoff sleeps.
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

struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(0)
    }
}

/// A clock the test can wind forward, to exercise TTL/staleness.
#[derive(Clone, Default)]
struct MovableClock(Rc<Cell<i64>>);
impl MovableClock {
    fn set_secs(&self, s: i64) {
        self.0.set(s * 1000);
    }
}
impl Clock for MovableClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0.get())
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

/// Resolve `keys` against a `bib` source built from `files`, on a fresh store.
fn retrieve_bib(
    fetcher: MockFetcher,
    bib: BibliographyFileSource,
    keys: &[&str],
) -> Vec<autocitefetch::manager::CiteFailure> {
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock, InstantTimer)
        .register(bib);
    let cites: Vec<(String, String)> = keys
        .iter()
        .map(|k| ("bib".to_string(), k.to_string()))
        .collect();
    block_on(mgr.retrieve(&cites)).unwrap().failures
}

#[test]
fn the_object_form_maps_id_to_item() {
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(
        url,
        200,
        br#"{"k1": {"type":"book","title":"First"},
             "k2": {"type":"article-journal","title":"Second"}}"#,
    );
    let bib = BibliographyFileSource::new([url.to_string()]);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites = vec![
        ("bib".to_string(), "k1".to_string()),
        ("bib".to_string(), "k2".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    assert_eq!(block_on(mgr.get("bib", "k1")).unwrap()["title"], "First");
    let second = block_on(mgr.get("bib", "k2")).unwrap();
    assert_eq!(second["title"], "Second");
    // The id is rewritten to the requested citation, not the file's key.
    assert_eq!(second["id"], "bib:k2");
}

#[test]
fn non_object_entries_in_the_object_form_are_reported_not_silently_emptied() {
    // Each of these used to resolve as a SUCCESS that `set_id` degraded to a
    // bare `{"id":"bib:kN"}` — the user's entry silently became empty.
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(
        url,
        200,
        br#"{"k1": null, "k2": 42, "k3": "text", "k4": [1,2,3], "ok": {"title":"Fine"}}"#,
    );
    let bib = BibliographyFileSource::new([url.to_string()]);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites: Vec<(String, String)> = ["k1", "k2", "k3", "k4", "ok"]
        .iter()
        .map(|k| ("bib".to_string(), k.to_string()))
        .collect();
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 4, "failures: {:?}", report.failures);
    for f in &report.failures {
        assert!(
            f.message.contains("not a JSON object"),
            "{}: {}",
            f.key,
            f.message
        );
    }
    // The healthy sibling in the same file still resolves.
    assert_eq!(block_on(mgr.get("bib", "ok")).unwrap()["title"], "Fine");
    assert!(block_on(mgr.get("bib", "k1")).is_err(), "k1 must not be cached");
}

#[test]
fn non_object_entries_in_the_array_form_are_dropped() {
    // An array item that is not an object carries no `id` to index it by, so
    // it can only be dropped — the requested key then reports as not found.
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(
        url,
        200,
        br#"[null, 42, "text", {"id":"ok","title":"Fine"}]"#,
    );
    let failures = retrieve_bib(
        fetcher,
        BibliographyFileSource::new([url.to_string()]),
        &["ok", "text"],
    );
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert_eq!(failures[0].key, "text");
}

#[test]
fn later_files_win_on_duplicate_ids() {
    // The documented headline behavior of a multi-file `files` list.
    let a = "https://host.example/base.json";
    let b = "https://host.example/overrides.json";
    let fetcher = MockFetcher::new()
        .route(a, 200, br#"[{"id":"dup","title":"From A"},{"id":"only-a","title":"Only A"}]"#)
        .route(b, 200, br#"[{"id":"dup","title":"From B"}]"#);
    let bib = BibliographyFileSource::new([a.to_string(), b.to_string()]);
    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites = vec![
        ("bib".to_string(), "dup".to_string()),
        ("bib".to_string(), "only-a".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(block_on(mgr.get("bib", "dup")).unwrap()["title"], "From B");
    // A key only the earlier file defines is not shadowed away.
    assert_eq!(block_on(mgr.get("bib", "only-a")).unwrap()["title"], "Only A");
}

#[test]
fn a_missing_key_is_reported_with_a_readable_message() {
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(url, 200, br#"[{"id":"here","title":"Here"}]"#);
    let failures = retrieve_bib(fetcher, BibliographyFileSource::new([url.to_string()]), &["zz"]);
    assert_eq!(failures.len(), 1);
    // Regression: `Error::NotFound` renders its payload *as an id*, so passing
    // it a whole sentence produced
    // "citation `key `zz` not found in bibliography` not found".
    assert_eq!(failures[0].message, "citation `bib:zz` not found");
}

#[test]
fn an_unloadable_file_fails_the_keys_with_a_single_error_prefix() {
    let url = "https://host.example/missing.json";
    let fetcher = MockFetcher::new().route(url, 404, b"Not Found");
    let failures = retrieve_bib(
        fetcher,
        BibliographyFileSource::new([url.to_string()]),
        &["a", "b"],
    );
    assert_eq!(failures.len(), 2, "{failures:?}");
    for f in &failures {
        // Regression: re-wrapping an already-`Display`ed `Error` produced
        // "source error: source error: bibliography file `…` returned 404".
        assert_eq!(
            f.message.matches("source error:").count(),
            1,
            "double-prefixed: {}",
            f.message
        );
        assert!(f.message.contains("404"), "{}", f.message);
    }
}

#[test]
fn a_parser_error_fails_every_key_of_the_chunk() {
    let url = "https://host.example/refs.yaml";
    let fetcher = MockFetcher::new().route(url, 200, b"not: [valid");
    let failing: fn(&[u8]) -> Result<CslValue, String> =
        |_| Err("unexpected end of flow sequence".to_string());
    let failures = retrieve_bib(
        fetcher,
        BibliographyFileSource::new([url.to_string()]).with_parser(failing),
        &["a", "b"],
    );
    assert_eq!(failures.len(), 2, "{failures:?}");
    assert!(
        failures[0].message.contains("unexpected end of flow sequence"),
        "the parser's own message should survive: {}",
        failures[0].message
    );
}

#[test]
fn a_body_that_is_neither_an_array_nor_an_object_is_rejected() {
    let url = "https://host.example/refs.json";
    // Valid JSON, wrong shape.
    let fetcher = MockFetcher::new().route(url, 200, br#""just a string""#);
    let failures = retrieve_bib(fetcher, BibliographyFileSource::new([url.to_string()]), &["a"]);
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0].message.contains("neither an array nor an object"),
        "{}",
        failures[0].message
    );
}

#[test]
fn a_file_is_refetched_only_once_its_entries_go_stale() {
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(url, 200, br#"[{"id":"k1","title":"T"}]"#);
    let calls = fetcher.calls();
    let clock = MovableClock::default();
    // TTL 100 s ⇒ hard expiry is jittered into [85 s, 115 s] and soft expiry is
    // 80% of that, i.e. [68 s, 92 s]. So 60 s is unambiguously fresh and 95 s
    // unambiguously not, whatever the id's jitter seed works out to.
    let bib = BibliographyFileSource::new([url.to_string()]).with_ttl(Duration::from_secs(100));
    let mgr = CitationManager::new(fetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(bib);

    let cites = vec![("bib".to_string(), "k1".to_string())];
    block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(calls.get(), 1, "first retrieve loads the file");

    block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(calls.get(), 1, "a fresh entry must not touch the file again");

    clock.set_secs(60);
    block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(calls.get(), 1, "still inside the fresh window");

    clock.set_secs(95);
    block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(calls.get(), 2, "a stale entry triggers a reload");
}

#[test]
fn with_ttl_shortens_the_fresh_window() {
    let url = "https://host.example/refs.json";
    let fetcher = MockFetcher::new().route(url, 200, br#"[{"id":"k1","title":"T"}]"#);
    let calls = fetcher.calls();
    let clock = MovableClock::default();
    // Same instant as above, but a 1 s TTL: long stale by 60 s.
    let bib = BibliographyFileSource::new([url.to_string()]).with_ttl(Duration::from_secs(1));
    let mgr = CitationManager::new(fetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(bib);

    let cites = vec![("bib".to_string(), "k1".to_string())];
    block_on(mgr.retrieve(&cites)).unwrap();
    clock.set_secs(60);
    block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(calls.get(), 2, "a 1 s TTL must have expired by 60 s");
}

#[test]
fn from_entries_rejects_a_non_object_entry() {
    let bib = BibliographyFileSource::from_entries([
        ("bad".to_string(), serde_json::json!("just text")),
        ("good".to_string(), serde_json::json!({"title":"Fine"})),
    ]);
    let mgr = CitationManager::new(MockFetcher::new(), MemStore::default(), FixedClock, InstantTimer)
        .register(bib);

    let cites = vec![
        ("bib".to_string(), "bad".to_string()),
        ("bib".to_string(), "good".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!(report.failures[0].key, "bad");
    assert_eq!(block_on(mgr.get("bib", "good")).unwrap()["title"], "Fine");
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
