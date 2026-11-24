//! The cache holds what a source returned; the options of a run apply on read.
//!
//! The arXiv DOI overrides, the chain target and the dropped fields never
//! change the cache, and changing them takes effect without a request. The
//! storage policy is the one thing that shapes the contents, and it applies on
//! both sides of the cache. A record the cache cannot read is fetched again
//! and replaced.
//!
//! Mocks are local to this file, as in the other test files; the cache is
//! cachette's `MemoryStore` behind `StoreCache`.

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::cachette::{
    self, Expiry, Fields, ImportMode, MemoryStore, NewValue, Record, Store as _,
};
use autocitefetch::source::{ArxivSource, DoiSource};
use autocitefetch::store::decode_item;
use autocitefetch::{
    BoxFuture, CacheStore, CachedItem, CitationManager, Clock, CslValue, FetchError, Fetcher,
    Request, Response, StoragePolicy, StoreCache, StoreError, Timer, Timestamp,
};
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

/// A fetcher with a URL → body route table. An unrouted URL fails with a
/// transport error, so a request that should not happen can never resolve a
/// citation. `Clone` shares the routes and the call log, so a test keeps a
/// handle on them after handing the fetcher to a manager.
#[derive(Clone, Default)]
struct MockFetcher {
    routes: Rc<RefCell<StdMap<String, Vec<u8>>>>,
    calls: Rc<RefCell<Vec<String>>>,
}

impl MockFetcher {
    fn route(self, url: &str, body: &str) -> Self {
        self.routes
            .borrow_mut()
            .insert(url.into(), body.as_bytes().to_vec());
        self
    }
    /// Forget every route and every call: from now on, each request fails.
    fn fail_everything(&self) {
        self.routes.borrow_mut().clear();
        self.calls.borrow_mut().clear();
    }
    fn calls(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.borrow_mut().push(req.url.clone());
        let result = match self.routes.borrow().get(&req.url) {
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

/// One in-memory cache shared by several managers (one per "run"), so a test
/// can change the options between runs and keep the cache.
#[derive(Clone)]
struct Shared(Rc<StoreCache<MemoryStore>>);

impl Shared {
    fn new() -> Self {
        Shared(Rc::new(StoreCache::new(MemoryStore::new())))
    }
    /// The cache contents, as `cachette::export` writes them: every key, record
    /// and field, sorted by key.
    fn contents(&self) -> String {
        export(&self.0)
    }
}

impl CacheStore for Shared {
    fn policy(&self) -> StoragePolicy {
        self.0.policy()
    }
    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError> {
        self.0.get(id)
    }
    fn put(&self, id: &str, item: &CslValue, ttl: Duration, now: Timestamp) -> Result<(), StoreError> {
        self.0.put(id, item, ttl, now)
    }
    fn remove(&self, id: &str) -> Result<(), StoreError> {
        self.0.remove(id)
    }
    fn flush(&self) -> Result<(), StoreError> {
        self.0.flush()
    }
    fn prune(&self, now: Timestamp) -> Result<usize, StoreError> {
        self.0.prune(now)
    }
}

/// Export the store of `cache` as the text of a JSONL file.
fn export(cache: &StoreCache<MemoryStore>) -> String {
    let mut text = String::new();
    cache
        .with_store(|s| {
            cachette::export(s, |line| {
                text.push_str(line);
                Ok(())
            })
        })
        .unwrap();
    text
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

// --- helpers ---------------------------------------------------------------

/// A minimal arXiv Atom feed from `(id_suffix, optional_doi)` entries, where
/// `id_suffix` is what follows `http://arxiv.org/abs/` (e.g. `1801.5v2`).
fn make_feed(entries: &[(&str, Option<&str>)]) -> String {
    let mut s = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <feed xmlns=\"http://www.w3.org/2005/Atom\" \
         xmlns:arxiv=\"http://arxiv.org/schemas/atom\">\n",
    );
    for (id_suffix, doi) in entries {
        s.push_str("  <entry>\n");
        s.push_str(&format!("    <id>http://arxiv.org/abs/{id_suffix}</id>\n"));
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

fn arxiv_url(ids: &[&str]) -> String {
    format!(
        "https://export.arxiv.org/api/query?id_list={}&max_results={}",
        ids.join(","),
        ids.len()
    )
}

fn doi_body(title: &str, doi: &str) -> String {
    json!({
        "type": "article-journal",
        "title": title,
        "DOI": doi,
        "author": [{"family": "Roe", "given": "Richard"}],
        "issued": {"date-parts": [[2020]]},
    })
    .to_string()
}

fn cites(prefix: &str, keys: &[&str]) -> Vec<(String, String)> {
    keys.iter().map(|k| (prefix.to_string(), k.to_string())).collect()
}

fn some(doi: &str) -> Option<String> {
    Some(doi.to_string())
}

// --- 1. options do not change the cache ------------------------------------

/// Retrieve under one set of options (overrides, chain target, dropped
/// fields), then change all three and retrieve again with a fetcher that
/// fails every request: nothing is fetched, the cache contents are
/// identical, and `get` follows the new options.
#[test]
fn the_options_of_a_run_do_not_change_the_cache() {
    // A has a feed DOI; B has none, and the first run's override gives it
    // one; C has a feed DOI and no override in either run.
    let (a, b, c) = ("1111.00001", "2222.00002", "3333.00003");
    let feed = make_feed(&[
        ("1111.00001v1", Some("10.1/a")),
        ("2222.00002v1", None),
        ("3333.00003v1", Some("10.1/c")),
    ]);
    let fetcher = MockFetcher::default()
        .route(&arxiv_url(&[a, b, c]), &feed)
        .route("https://doi.org/10.1/a", &doi_body("DOI A", "10.1/A"))
        .route("https://doi.org/10.1/b", &doi_body("DOI B", "10.1/B"))
        .route("https://doi.org/10.1/c", &doi_body("DOI C", "10.1/C"));
    let cache = Shared::new();

    // Run 1: B's override, chaining to `doi`, `issued` dropped.
    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .with_dropped_csl_fields(["issued"])
        .register("arxiv", ArxivSource::new().with_override_dois([(b.into(), some("10.1/b"))])).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a, b, c]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls().len(), 4, "one arXiv and three doi.org requests");
    let out = mgr.get("arxiv", b).unwrap();
    assert_eq!(out["title"], "DOI B");
    assert_eq!(out["arxivid"], b);
    assert!(out.get("issued").is_none(), "dropped in the output");
    // …but not in the cache: the cache holds what the source returned.
    let stored = cache.get("doi:10.1/b").unwrap().unwrap().item;
    assert_eq!(stored["issued"], json!({"date-parts": [[2020]]}));
    // No chain pointer is stored, and the override is not applied to the
    // stored arXiv item: B's item carries the feed's (absent) DOI.
    assert!(cache.get("arxiv:2222.00002").unwrap().unwrap().item.get("DOI").is_none());
    let before = cache.contents();
    drop(mgr);

    // Run 2: A suppressed, B's override gone, chaining off, `author` dropped
    // instead of `issued`; every request fails.
    fetcher.fail_everything();
    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .with_dropped_csl_fields(["author"])
        .register("arxiv", ArxivSource::new().chain_dois_to(None).with_override_dois([(a.into(), None)])).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a, b, c]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new(), "no request for cached keys");
    assert_eq!(cache.contents(), before, "the cache contents changed");

    // The output follows the new options.
    let out_a = mgr.get("arxiv", a).unwrap();
    assert_eq!(out_a["title"], "Title 1111.00001v1", "arXiv metadata, not chained");
    assert!(out_a.get("DOI").is_none(), "suppressed by the override");
    let out_b = mgr.get("arxiv", b).unwrap();
    assert_eq!(out_b["title"], "Title 2222.00002v1");
    assert!(out_b.get("DOI").is_none(), "the override is gone");
    let out_c = mgr.get("arxiv", c).unwrap();
    assert_eq!(out_c["title"], "Title 3333.00003v1", "chaining is off");
    assert_eq!(out_c["DOI"], "10.1/c");
    for out in [&out_a, &out_b, &out_c] {
        assert!(out.get("author").is_none(), "newly dropped: {out}");
        assert!(out.get("issued").is_some(), "no longer dropped: {out}");
    }
    let out_doi = mgr.get("doi", "10.1/b").unwrap();
    assert!(out_doi.get("author").is_none() && out_doi.get("issued").is_some());
}

// --- 2. a changed override -------------------------------------------------

/// Changing an override fetches the new DOI target, and nothing else: the
/// arXiv entry is read from the cache under the new override.
#[test]
fn a_changed_override_fetches_the_new_doi_target_only() {
    let a = "1111.00001";
    let fetcher = MockFetcher::default()
        .route(&arxiv_url(&[a]), &make_feed(&[("1111.00001v1", Some("10.1/old"))]))
        .route("https://doi.org/10.1/old", &doi_body("Old", "10.1/old"));
    let cache = Shared::new();

    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .register("arxiv", ArxivSource::new()).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(mgr.get("arxiv", a).unwrap()["title"], "Old");
    drop(mgr);

    fetcher.fail_everything();
    let fetcher = fetcher.route("https://doi.org/10.1/new", &doi_body("New", "10.1/new"));
    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .register("arxiv", ArxivSource::new().with_override_dois([(a.into(), some("10.1/new"))])).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), ["https://doi.org/10.1/new"]);
    let out = mgr.get("arxiv", a).unwrap();
    assert_eq!(out["title"], "New");
    assert_eq!(out["arxivid"], a);
    assert_eq!(out["id"], "arxiv:1111.00001");
}

// --- 3. chaining switched off ----------------------------------------------

/// With chaining switched off, a cached arXiv entry answers its own arXiv
/// metadata, without a request, even though it chained when it was fetched.
#[test]
fn chaining_switched_off_answers_arxiv_metadata_from_the_cache() {
    let a = "1111.00001";
    let fetcher = MockFetcher::default()
        .route(&arxiv_url(&[a]), &make_feed(&[("1111.00001v1", Some("10.1/a"))]))
        .route("https://doi.org/10.1/a", &doi_body("DOI A", "10.1/A"));
    let cache = Shared::new();

    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .register("arxiv", ArxivSource::new()).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(mgr.get("arxiv", a).unwrap()["title"], "DOI A");
    drop(mgr);

    fetcher.fail_everything();
    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .register("arxiv", ArxivSource::new().chain_dois_to(None)).unwrap()
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("arxiv", &[a]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new(), "no request");
    let out = mgr.get("arxiv", a).unwrap();
    assert_eq!(out["id"], "arxiv:1111.00001");
    assert_eq!(out["title"], "Title 1111.00001v1");
    assert_eq!(out["DOI"], "10.1/a");
    assert_eq!(out["arxivid"], a);
    assert_eq!(out["arxiv_version_number"], 1);
}

// --- 4. the storage policy -------------------------------------------------

/// A doi.org answer with both bulky fields.
fn fat_doi_body() -> String {
    json!({
        "title": "Fat",
        "DOI": "10.1/fat",
        "abstract": "A long abstract.",
        "reference": [{"key": "r1"}, {"key": "r2"}],
    })
    .to_string()
}

fn fat_fetcher() -> MockFetcher {
    MockFetcher::default().route("https://doi.org/10.1/fat", &fat_doi_body())
}

/// Retrieve `doi:10.1/fat` into a cache with `policy`; answer the manager.
fn retrieve_fat(
    policy: StoragePolicy,
) -> CitationManager<MockFetcher, StoreCache<MemoryStore>, FixedClock, InstantTimer> {
    let cache = StoreCache::new(MemoryStore::new()).with_policy(policy);
    let mgr = CitationManager::new(fat_fetcher(), cache, FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("doi", &["10.1/fat"]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    mgr
}

/// By default `reference` and `abstract` are in neither the stored item nor
/// the output; with a `keep_*` flag the field is in both.
#[test]
fn the_storage_policy_decides_what_is_stored_and_answered() {
    let mgr = retrieve_fat(StoragePolicy::default());
    let stored = mgr.store().get("doi:10.1/fat").unwrap().unwrap().item;
    let out = mgr.get("doi", "10.1/fat").unwrap();
    for field in ["reference", "abstract"] {
        assert!(stored.get(field).is_none(), "{field} stored: {stored}");
        assert!(out.get(field).is_none(), "{field} answered: {out}");
    }
    assert_eq!(out["title"], "Fat");

    let mgr = retrieve_fat(StoragePolicy {
        keep_reference: true,
        ..StoragePolicy::default()
    });
    let stored = mgr.store().get("doi:10.1/fat").unwrap().unwrap().item;
    let out = mgr.get("doi", "10.1/fat").unwrap();
    assert_eq!(stored["reference"], json!([{"key": "r1"}, {"key": "r2"}]));
    assert_eq!(out["reference"], json!([{"key": "r1"}, {"key": "r2"}]));
    assert!(stored.get("abstract").is_none() && out.get("abstract").is_none());

    let mgr = retrieve_fat(StoragePolicy {
        keep_abstract: true,
        ..StoragePolicy::default()
    });
    let stored = mgr.store().get("doi:10.1/fat").unwrap().unwrap().item;
    let out = mgr.get("doi", "10.1/fat").unwrap();
    assert_eq!(stored["abstract"], "A long abstract.");
    assert_eq!(out["abstract"], "A long abstract.");
    assert!(stored.get("reference").is_none() && out.get("reference").is_none());
}

/// Turning a flag off hides the field at once, for items stored before: the
/// cache is carried to a new cache object with the default policy (with
/// `cachette::export` and `cachette::import`, as a browser host saves and
/// loads its `MemoryStore`), and `get` no longer answers the field, without a
/// request.
#[test]
fn turning_a_keep_flag_off_hides_the_field_at_once() {
    let mgr = retrieve_fat(StoragePolicy {
        keep_reference: true,
        keep_abstract: true,
        ..StoragePolicy::default()
    });
    let saved = export(mgr.store());
    assert!(saved.contains("A long abstract."), "{saved}");

    let mut store = MemoryStore::new();
    cachette::import(&mut store, saved.lines().map(Ok::<_, StoreError>), ImportMode::Strict).unwrap();
    let fetcher = MockFetcher::default();
    let mgr = CitationManager::new(fetcher.clone(), StoreCache::new(store), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("doi", &["10.1/fat"]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new(), "no request");

    let out = mgr.get("doi", "10.1/fat").unwrap();
    assert_eq!(out["title"], "Fat");
    assert!(out.get("reference").is_none(), "{out}");
    assert!(out.get("abstract").is_none(), "{out}");
    // The fields are still in the cache: only the read hides them.
    assert_eq!(mgr.store().get("doi:10.1/fat").unwrap().unwrap().item["abstract"], "A long abstract.");
}

// --- 6. an unreadable record -----------------------------------------------

/// A record of neither shape (`{"concrete": {…}}`, a 0.1.1 `{"chained": …}`)
/// reads as absent, even with fresh expiry times: the citation is fetched
/// again, and the record is replaced.
#[test]
fn an_unreadable_record_is_fetched_again_and_replaced() {
    let cache = Shared::new();
    let fields = Fields {
        expiry: Some(Expiry {
            stale_after: Timestamp::from_millis(9_000_000_000),
            expires: Timestamp::from_millis(10_000_000_000),
        }),
        ..Fields::default()
    };
    let unreadable = Record::from_json(r#"{"something":"else"}"#).unwrap();
    cache
        .0
        .with_store(|s| s.put("doi:10.1/x", NewValue::new(unreadable, fields)))
        .unwrap();
    assert!(cache.get("doi:10.1/x").unwrap().is_none(), "reads as absent");

    let fetcher = MockFetcher::default().route("https://doi.org/10.1/x", &doi_body("X", "10.1/x"));
    let mgr = CitationManager::new(fetcher.clone(), cache.clone(), FixedClock(0), InstantTimer)
        .register("doi", DoiSource::new()).unwrap();
    let report = block_on(mgr.retrieve(&cites("doi", &["10.1/x"]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), ["https://doi.org/10.1/x"], "fetched again");

    // The record is replaced by a readable one.
    let stored = cache.0.with_store(|s| s.get("doi:10.1/x").unwrap().unwrap());
    assert_eq!(decode_item(&stored.record).unwrap()["title"], "X");
    assert_eq!(mgr.get("doi", "10.1/x").unwrap()["title"], "X");
}
