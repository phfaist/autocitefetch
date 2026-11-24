//! Tests of [`SingleFileCacheStore`], autocitefetch's thin wrapper around
//! cachette's `JsonlStore`: where its files go, what a line looks like, the
//! storage policy, ephemeral items, and reading a file written by 0.1.1. The
//! storage engine itself (logs, locks, merges) is tested in cachette.
//!
//! The last tests go end to end through `CitationManager`: a cache file
//! written by 0.1.1, and ephemeral (`manual`) items. Mocks are local to this
//! file, as in the other test files; no test reaches the network.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::source::{ArxivSource, DoiSource, ManualSource};
use autocitefetch::{
    BoxFuture, CacheStore, CitationManager, Clock, FetchError, Fetcher, Request, Response,
    StoragePolicy, Timer, Timestamp,
};
use autocitefetch_std::SingleFileCacheStore;
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

const HOUR: Duration = Duration::from_secs(3600);

fn files_in(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn put_flush_reopen_roundtrip_leaves_one_file_with_the_0_1_1_line_shape() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("cache");
    let now = Timestamp::from_millis(1_000_000);

    let store = SingleFileCacheStore::new(&dir).unwrap();
    store.put("doi:10.1/x", &json!({"title": "T"}), HOUR, now).unwrap();
    store.flush().unwrap();
    drop(store);

    // Only the committable file is left, under the default base.
    assert_eq!(files_in(&dir), ["citations.jsonl"]);
    let text = std::fs::read_to_string(dir.join("citations.jsonl")).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], r#"{"schema":1}"#);
    // The 0.1.1 shape: a concrete payload and two expiry times, nothing else
    // (no time of write, use or cost), and no `id` stamped into the item.
    let line: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(line["id"], "doi:10.1/x");
    assert_eq!(line["rec"]["payload"], json!({"concrete": {"title": "T"}}));
    let mut keys: Vec<&String> = line.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["id", "rec"]);
    let mut rec_keys: Vec<&String> = line["rec"].as_object().unwrap().keys().collect();
    rec_keys.sort();
    assert_eq!(rec_keys, ["expires", "payload", "stale_after"]);

    // Reopened, the item and its expiry times read back.
    let store = SingleFileCacheStore::new(&dir).unwrap();
    let got = store.get("doi:10.1/x").unwrap().expect("persisted");
    assert_eq!(got.item, json!({"title": "T"}));
    assert_eq!(got.expiry.expires.as_millis(), line["rec"]["expires"].as_i64().unwrap());
    assert_eq!(got.expiry.stale_after.as_millis(), line["rec"]["stale_after"].as_i64().unwrap());
}

#[test]
fn with_base_renames_the_whole_family() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::with_base(tmp.path(), ".citations").unwrap();
    store.put("doi:10.1/x", &json!({"title": "T"}), HOUR, Timestamp::from_millis(0)).unwrap();
    // While the run is in flight, the log and its lock hide under `._.citations`.
    assert!(
        files_in(tmp.path()).iter().all(|n| n.starts_with("._.citations.")),
        "{:?}",
        files_in(tmp.path())
    );
    store.flush().unwrap();
    assert_eq!(files_in(tmp.path()), [".citations.jsonl"]);
}

#[test]
fn ephemeral_items_are_readable_in_the_run_and_never_written() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    let now = Timestamp::from_millis(0);
    store
        .put("manual:Some text", &json!({"_ready_formatted": {"flm": "Some text"}}), Duration::ZERO, now)
        .unwrap();
    assert!(store.get("manual:Some text").unwrap().is_some(), "readable in the run");
    store.flush().unwrap();
    // The item is in no file: no log is left, and the main file holds only its
    // header.
    assert_eq!(files_in(tmp.path()), ["citations.jsonl"]);
    let text = std::fs::read_to_string(tmp.path().join("citations.jsonl")).unwrap();
    assert!(!text.contains("manual:"), "{text}");
    assert_eq!(text.lines().filter(|l| !l.trim().is_empty()).count(), 1, "{text}");
    drop(store);
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    assert!(store.get("manual:Some text").unwrap().is_none(), "gone on reopen");
}

#[test]
fn the_storage_policy_is_the_one_given() {
    let tmp = tempfile::tempdir().unwrap();
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    assert!(!store.policy().keep_reference && !store.policy().keep_abstract);
    let store = store.with_policy(StoragePolicy {
        keep_abstract: true,
        ..StoragePolicy::default()
    });
    assert!(store.policy().keep_abstract);
    assert!(!store.policy().keep_reference);
}

/// The cache file of the brief's fixture, as autocitefetch 0.1.1 writes it:
/// an arXiv chain pointer and the concrete DOI entry it points to.
const FIXTURE_0_1_1: &str = concat!(
    r#"{"schema":1}"#, "\n",
    r#"{"id":"arxiv:1211.1037","rec":{"payload":{"chained":{"prefix":"doi","key":"10.1103/physreva.86.052329","set_properties":{"arxivid":"1211.1037"}}},"stale_after":1700783584969,"expires":1700979481212}}"#, "\n",
    r#"{"id":"doi:10.1103/physreva.86.052329","rec":{"payload":{"concrete":{"DOI":"10.1103/PhysRevA.86.052329","author":[{"family":"Faist","given":"Philippe"}],"id":"doi:10.1103/physreva.86.052329","title":"A & B <c>"}},"stale_after":1722610890625,"expires":1728263613282}}"#, "\n",
);

#[test]
fn a_file_written_by_0_1_1_is_read_as_it_is() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("citations.jsonl"), FIXTURE_0_1_1).unwrap();

    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    // The concrete entry reads back with its expiry times.
    let doi = store.get("doi:10.1103/physreva.86.052329").unwrap().expect("concrete entry");
    assert_eq!(doi.item["title"], "A & B <c>");
    assert_eq!(doi.expiry.expires.as_millis(), 1728263613282);
    assert_eq!(doi.expiry.stale_after.as_millis(), 1722610890625);
    // A 0.1.1 chain pointer reads as absent, so the citation is fetched again.
    assert!(store.get("arxiv:1211.1037").unwrap().is_none());
}

// --- end to end, through the manager ---------------------------------------

/// A time at which every line of the fixture is fresh (before the arXiv
/// pointer's `stale_after`, the earliest of them), so that a request for the
/// arXiv id can only come from the pointer reading as absent.
const FRESH: i64 = 1_700_000_000_000;

/// The arXiv API URL for the fixture's arXiv id.
const ARXIV_URL: &str = "https://export.arxiv.org/api/query?id_list=1211.1037&max_results=1";

/// The arXiv feed for the fixture's paper, with the DOI in its registered case.
const ARXIV_FEED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1211.1037v2</id>
    <published>2012-11-05T00:00:00Z</published>
    <title>Arxiv title</title>
    <author><name>Philippe Faist</name></author>
    <arxiv:doi>10.1103/PhysRevA.86.052329</arxiv:doi>
  </entry>
</feed>"#;

/// The line of the main file for `id`, parsed.
fn main_file_line(dir: &std::path::Path, id: &str) -> serde_json::Value {
    let text = std::fs::read_to_string(dir.join("citations.jsonl")).unwrap();
    text.lines()
        .skip(1)
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .find(|line| line["id"] == id)
        .unwrap_or_else(|| panic!("no line for {id} in:\n{text}"))
}

/// A cache written by 0.1.1, with a fresh clock: the DOI entry is read without
/// a request; retrieving the arXiv id makes one arXiv request and no DOI
/// request (its old pointer reads as absent, its target is still fresh); and
/// the pointer line is then replaced by the arXiv metadata.
#[test]
fn a_cache_written_by_0_1_1_is_reused_through_the_manager() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("citations.jsonl"), FIXTURE_0_1_1).unwrap();
    let fetcher = MockFetcher::with_routes(&[(ARXIV_URL, ARXIV_FEED)]);
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    let mgr = CitationManager::new(fetcher.clone(), store, FixedClock(FRESH), InstantTimer)
        .register("arxiv", ArxivSource::new()).unwrap()
        .register("doi", DoiSource::new()).unwrap();

    // `get` of the DOI works without a request (`get` makes none, and nothing
    // was retrieved).
    let doi = mgr.get("doi", "10.1103/PhysRevA.86.052329").unwrap();
    assert_eq!(doi["title"], "A & B <c>");
    assert_eq!(doi["id"], "doi:10.1103/physreva.86.052329");
    // Retrieving the DOI too needs no request: the entry is fresh.
    let doi_cite = [("doi".to_string(), "10.1103/physreva.86.052329".to_string())];
    let report = block_on(mgr.retrieve(&doi_cite)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new());

    // The arXiv id: one arXiv request, no DOI request.
    let arxiv_cite = [("arxiv".to_string(), "1211.1037".to_string())];
    let report = block_on(mgr.retrieve(&arxiv_cite)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), [ARXIV_URL]);
    let item = mgr.get("arxiv", "1211.1037").unwrap();
    assert_eq!(item["id"], "arxiv:1211.1037");
    assert_eq!(item["title"], "A & B <c>", "chained to the reused DOI entry");
    assert_eq!(item["arxivid"], "1211.1037");

    // The pointer line is replaced (`retrieve` flushed): the arXiv metadata,
    // in the 0.1.1 concrete shape. The DOI line is untouched.
    let line = main_file_line(tmp.path(), "arxiv:1211.1037");
    let concrete = &line["rec"]["payload"]["concrete"];
    assert_eq!(concrete["title"], "Arxiv title", "{line}");
    assert_eq!(concrete["arxivid"], "1211.1037");
    assert_eq!(concrete["DOI"], "10.1103/PhysRevA.86.052329");
    assert!(line["rec"]["payload"].get("chained").is_none(), "{line}");
    let doi_line = main_file_line(tmp.path(), "doi:10.1103/physreva.86.052329");
    assert_eq!(doi_line["rec"]["expires"], 1728263613282_i64);
}

/// `manual` items (zero TTL) are readable after `retrieve`, and are in no
/// file: not in the main file, and no log is left behind.
#[test]
fn manual_items_are_readable_after_retrieve_and_in_no_file() {
    let tmp = tempfile::tempdir().unwrap();
    let fetcher = MockFetcher::default();
    let store = SingleFileCacheStore::new(tmp.path()).unwrap();
    let mgr = CitationManager::new(fetcher.clone(), store, FixedClock(FRESH), InstantTimer)
        .register("manual", ManualSource::new("flm")).unwrap();

    let text = "A. Author, *Some Book* (2020)";
    let cite = [("manual".to_string(), text.to_string())];
    let report = block_on(mgr.retrieve(&cite)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(fetcher.calls(), Vec::<String>::new());
    let item = mgr.get("manual", text).unwrap();
    assert_eq!(item["_ready_formatted"]["flm"], text);

    // `retrieve` flushed: only the main file is left, and it holds only its
    // header.
    assert_eq!(files_in(tmp.path()), ["citations.jsonl"]);
    let main = std::fs::read_to_string(tmp.path().join("citations.jsonl")).unwrap();
    assert!(!main.contains("Some Book"), "{main}");
    assert_eq!(main.lines().filter(|l| !l.trim().is_empty()).count(), 1, "{main}");
    // Still readable for the life of the cache object.
    assert_eq!(mgr.get("manual", text).unwrap()["_ready_formatted"]["flm"], text);
}
