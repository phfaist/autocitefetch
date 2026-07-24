//! The manager's contract with a source, and the driver's rate limiting.
//!
//! Covers the things a badly-behaved (or merely unlucky) source can do — omit
//! a key, answer twice, chain to itself, chain forever, hand back a non-object
//! payload — plus chunking/pacing, which no other test reaches because no test
//! elsewhere supplies more keys than `chunk_size` or has a clock that moves.

use std::cell::{Cell, RefCell};
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, CslValue, Error, FetchError,
    Fetcher, Outcome, Payload, Request, Resolution, Response, RetrieveCtx, Source, StoreError,
    Timer, Timestamp,
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

struct NoopFetcher;
impl Fetcher for NoopFetcher {
    fn fetch(&self, _req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        Box::pin(async { Err(FetchError::Status(404)) })
    }
}

/// A `MemStore` that also counts `put`s, so a source answering twice for one
/// key can be caught double-writing.
#[derive(Clone, Default)]
struct MemStore {
    map: Rc<RefCell<StdMap<String, CacheRecord>>>,
    puts: Rc<Cell<u32>>,
}

impl MemStore {
    fn seed(&self, id: &str, payload: Payload, stale_after: i64, expires: i64) {
        self.map.borrow_mut().insert(
            id.into(),
            CacheRecord {
                payload,
                stale_after: Timestamp::from_millis(stale_after),
                expires: Timestamp::from_millis(expires),
            },
        );
    }
}

impl CacheStore for MemStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.map.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.puts.set(self.puts.get() + 1);
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

/// A clock the mocks move forward (request latency, timer sleeps).
#[derive(Clone, Default)]
struct MovableClock {
    ms: Rc<Cell<i64>>,
}
impl MovableClock {
    fn advance(&self, ms: i64) {
        self.ms.set(self.ms.get() + ms);
    }
}
impl Clock for MovableClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.ms.get())
    }
}

struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A timer that makes time actually pass: sleeping moves the shared clock. With
/// it, "when was each request issued" is observable.
struct AdvancingTimer {
    clock: MovableClock,
}
impl Timer for AdvancingTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'_, ()> {
        self.clock.advance(dur.as_millis() as i64);
        Box::pin(async {})
    }
}

/// What a [`ScriptSource`] does with the keys it is handed.
#[derive(Clone, Copy)]
enum Answer {
    /// Resolve every key to a concrete item.
    Concrete,
    /// Chain key `k` to `(target, k)`.
    ChainTo(&'static str),
    /// Chain numeric key `n` to `(own prefix, n + 1)` — an unbounded chain.
    ChainToSuccessor,
    /// Chain every key to itself.
    SelfChain,
    /// Fail every key (reachability failure → grace-served if cached).
    Fail,
    /// Report every key as authoritatively absent (reachable, no such key).
    Missing,
    /// Resolve concretely, but with a JSON array instead of an object.
    NonObject,
    /// Resolve concretely to a *bulky* item — a `reference` list and an
    /// `abstract`, the way doi.org answers — plus a nested `reference` that a
    /// top-level-only drop must leave alone.
    Fat,
    /// Chain key `k` to `(target, k)` with `set_properties` carrying both a
    /// droppable field and one that must survive.
    ChainFatTo(&'static str),
    /// Answer only the key `"ok"`, silently dropping every other key.
    OmitOthers,
    /// Return *two* resolutions for every key.
    Duplicate,
}

/// One configurable source, so each test can express exactly the (mis)behavior
/// it is about.
struct ScriptSource {
    prefix: &'static str,
    answer: Answer,
    chunk: usize,
    interval: Duration,
    /// Simulated per-chunk request latency, in ms.
    latency: i64,
    clock: MovableClock,
    /// `(key, clock reading when the chunk carrying it was issued)`.
    log: Rc<RefCell<Vec<(String, i64)>>>,
}

impl ScriptSource {
    fn new(prefix: &'static str, answer: Answer, clock: &MovableClock) -> Self {
        ScriptSource {
            prefix,
            answer,
            chunk: 512,
            interval: Duration::ZERO,
            latency: 0,
            clock: clock.clone(),
            log: Rc::new(RefCell::new(Vec::new())),
        }
    }
    fn paced(mut self, chunk: usize, interval_ms: u64, latency: i64) -> Self {
        self.chunk = chunk;
        self.interval = Duration::from_millis(interval_ms);
        self.latency = latency;
        self
    }
    fn log(&self) -> Rc<RefCell<Vec<(String, i64)>>> {
        self.log.clone()
    }
}

fn item(title: &str) -> CslValue {
    let mut m = serde_json::Map::new();
    m.insert("title".into(), CslValue::String(title.into()));
    CslValue::Object(m)
}

impl Source for ScriptSource {
    fn chunk_size(&self) -> usize {
        self.chunk
    }
    fn min_interval(&self) -> Duration {
        self.interval
    }
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(3600)
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            let issued = self.clock.now().as_millis();
            for k in &keys {
                self.log.borrow_mut().push((k.clone(), issued));
            }
            self.clock.advance(self.latency);

            let mut out = Vec::new();
            for k in keys {
                match self.answer {
                    Answer::Concrete => {
                        out.push(Resolution::concrete(k.clone(), item(&format!("{}:{k}", self.prefix))))
                    }
                    Answer::ChainTo(target) => out.push(Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: target.into(),
                            key: k,
                            set_properties: CslValue::Object(serde_json::Map::new()),
                        },
                    }),
                    Answer::ChainToSuccessor => {
                        let next = k.parse::<u32>().unwrap() + 1;
                        out.push(Resolution {
                            key: k,
                            outcome: Outcome::Chained {
                                prefix: self.prefix.into(),
                                key: next.to_string(),
                                set_properties: CslValue::Object(serde_json::Map::new()),
                            },
                        });
                    }
                    Answer::SelfChain => out.push(Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: self.prefix.into(),
                            key: k,
                            set_properties: CslValue::Object(serde_json::Map::new()),
                        },
                    }),
                    Answer::Fail => {
                        out.push(Resolution::failed(k, Error::Source("source down".into())))
                    }
                    Answer::Missing => {
                        let id = format!("{}:{k}", self.prefix);
                        out.push(Resolution::missing(k, Error::NotFound(id)))
                    }
                    Answer::NonObject => out.push(Resolution::concrete(
                        k,
                        CslValue::Array(vec![CslValue::String("not a CSL item".into())]),
                    )),
                    Answer::Fat => out.push(Resolution::concrete(
                        k.clone(),
                        serde_json::json!({
                            "title": format!("{}:{k}", self.prefix),
                            "abstract": "a long abstract",
                            "reference": [{"key": "r1"}, {"key": "r2"}],
                            "keep": {"reference": "nested, not top level"},
                        }),
                    )),
                    Answer::ChainFatTo(target) => out.push(Resolution {
                        key: k.clone(),
                        outcome: Outcome::Chained {
                            prefix: target.into(),
                            key: k,
                            set_properties: serde_json::json!({
                                "reference": ["from the pointer"],
                                "arxivid": "1211.1037",
                            }),
                        },
                    }),
                    Answer::OmitOthers => {
                        if k == "ok" {
                            out.push(Resolution::concrete(k, item("ok")));
                        }
                    }
                    Answer::Duplicate => {
                        out.push(Resolution::concrete(k.clone(), item("first")));
                        out.push(Resolution::concrete(k, item("second")));
                    }
                }
            }
            out
        })
    }
}

fn cites(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(p, k)| (p.to_string(), k.to_string()))
        .collect()
}

// --- the one-resolution-per-key contract -----------------------------------

/// A source that drops a key must not leave the citation *both* unstored and
/// unreported: `retrieve` used to claim success for something `get` can never
/// return.
#[test]
fn a_key_the_source_omits_is_reported() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::OmitOthers, &clock)).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "ok"), ("s", "ghost")]))).unwrap();
    assert!(!report.is_complete(), "the dropped key must be reported");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, "ghost");
    assert!(
        report.failures[0].message.contains("no resolution"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    // The key that *was* answered still resolved.
    assert!(block_on(mgr.get("s", "ok")).is_ok());
    assert!(block_on(mgr.get("s", "ghost")).is_err());
}

/// A source answering twice for one key writes the store once (and, for
/// failures, reports once).
#[test]
fn duplicate_resolutions_for_one_key_are_collapsed() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let puts = store.puts.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Duplicate, &clock)).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(puts.get(), 1, "the duplicate must not double-write the store");
    assert_eq!(block_on(mgr.get("s", "k")).unwrap()["title"], "first");
}

/// A concrete payload that is not a JSON object is a source bug: it used to be
/// silently replaced by a bare `{"id": …}` stub and cached as a success.
#[test]
fn a_non_object_concrete_payload_is_reported_not_stubbed() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::NonObject, &clock)).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("non-object"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    assert_eq!(
        block_on(entries.entries()).unwrap().len(),
        0,
        "nothing should have been cached"
    );
}

// --- chains ----------------------------------------------------------------

/// `retrieve` refuses to walk further than `get` could ever follow. Without the
/// bound, a source chaining `k -> k+1` fetched thousands of links for a single
/// requested citation.
#[test]
fn chain_discovery_is_bounded_by_max_chain_depth() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let src = ScriptSource::new("c", Answer::ChainToSuccessor, &clock);
    let log = src.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .with_max_chain_depth(4)
        .register(src.prefix, src).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("c", "0")]))).unwrap();

    // Links at depth 0..3 were fetched; depth 4 was refused and reported.
    assert_eq!(log.borrow().len(), 4, "fetched: {:?}", log.borrow());
    assert_eq!(block_on(entries.entries()).unwrap().len(), 4);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].key, "4");
    assert!(
        report.failures[0].message.contains("links"),
        "unhelpful message: {}",
        report.failures[0].message
    );

    // And reading it fails cleanly rather than looping.
    let err = block_on(mgr.get("c", "0")).unwrap_err();
    assert!(matches!(err, Error::Chain(_)), "got {err}");
}

/// A pointer at itself is caught at store time instead of surfacing as
/// "chain too deep" `max_chain_depth` store reads later.
#[test]
fn a_self_chain_is_rejected_at_store_time() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    let entries = store.clone();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::SelfChain, &clock)).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert!(
        report.failures[0].message.contains("itself"),
        "unhelpful message: {}",
        report.failures[0].message
    );
    assert_eq!(block_on(entries.entries()).unwrap().len(), 0);
}

/// A chain target that is not in the cache names *both* ids: the target alone
/// is a citation the caller never asked for.
#[test]
fn a_missing_chain_target_names_both_ids() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "gone".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let err = block_on(mgr.get("a", "x")).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("a:x") && msg.contains("b:gone"), "got: {msg}");

    // A miss on the *requested* id is still a plain NotFound.
    let err = block_on(mgr.get("a", "absent")).unwrap_err();
    assert!(matches!(err, Error::NotFound(_)), "got {err}");
}

/// A stale chained record must not drag its old target along: the refetch may
/// well replace the pointer (retracted DOI, suppressed by an override), and the
/// dead target's 404 would be reported as a failure for a citation nobody
/// requested — plus one wasted rate-limited request.
#[test]
fn a_stale_pointer_does_not_fetch_its_dead_target() {
    let clock = MovableClock::default();
    clock.advance(1_000_000);
    let store = MemStore::default();
    // `a:x` used to chain to `b:OLD`, and is now hard-expired.
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "OLD".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        0,
        0,
    );

    let a = ScriptSource::new("a", Answer::Concrete, &clock);
    let b = ScriptSource::new("b", Answer::Fail, &clock);
    let b_log = b.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert!(
        b_log.borrow().is_empty(),
        "the superseded target was fetched anyway: {:?}",
        b_log.borrow()
    );
    assert!(
        report.is_complete(),
        "a citation nobody asked for was reported: {:?}",
        report.failures
    );
    assert_eq!(block_on(mgr.get("a", "x")).unwrap()["title"], "a:x");
}

/// …but a chained record kept alive by the grace window *does* still need its
/// target, or `get()` breaks on the next link.
#[test]
fn a_grace_served_pointer_keeps_its_target() {
    let clock = MovableClock::default();
    clock.advance(1000);
    let store = MemStore::default();
    // Hard-expired (but well within the default 14-day grace) pointer whose
    // target is not in the cache yet.
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "t".into(),
            set_properties: CslValue::Object(serde_json::Map::new()),
        },
        0,
        0,
    );

    let a = ScriptSource::new("a", Answer::Fail, &clock);
    let b = ScriptSource::new("b", Answer::Concrete, &clock);
    let b_log = b.log();
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
    assert_eq!(
        b_log.borrow().len(),
        1,
        "the kept pointer's target must be fetched: {:?}",
        b_log.borrow()
    );
    assert_eq!(block_on(mgr.get("a", "x")).unwrap()["title"], "b:t");
}

/// A chain's `set_properties` **override** the concrete target's colliding
/// fields (both reference impls do `{ ...target, ...set_properties }`). Here the
/// chain carries `title:"FROM CHAIN"` and the target has `title:"FROM TARGET"`;
/// the chain must win.
#[test]
fn chained_set_properties_override_the_target() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:x",
        Payload::Chained {
            prefix: "b".into(),
            key: "t".into(),
            set_properties: serde_json::json!({"title": "FROM CHAIN", "extra": "kept"}),
        },
        10_000,
        10_000,
    );
    store.seed(
        "b:t",
        Payload::Concrete(serde_json::json!({"id": "b:t", "title": "FROM TARGET", "year": 1935})),
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let item = block_on(mgr.get("a", "x")).unwrap();
    assert_eq!(item["title"], "FROM CHAIN", "the chain must override the target");
    assert_eq!(item["extra"], "kept", "chain-only field is attached");
    assert_eq!(item["year"], 1935, "target-only field is preserved");
    assert_eq!(item["id"], "a:x", "id is rewritten to the requested one");
}

/// The requested `id` is forced last, so a `set_properties` carrying its own
/// `id` can never override it — even though `set_properties` otherwise win.
#[test]
fn a_set_properties_id_cannot_override_the_requested_id() {
    let clock = MovableClock::default();
    let store = MemStore::default();
    store.seed(
        "a:y",
        Payload::Chained {
            prefix: "b".into(),
            key: "u".into(),
            set_properties: serde_json::json!({"id": "HACKED", "note": "n"}),
        },
        10_000,
        10_000,
    );
    store.seed(
        "b:u",
        Payload::Concrete(serde_json::json!({"id": "b:u", "title": "T"})),
        10_000,
        10_000,
    );
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer);

    let item = block_on(mgr.get("a", "y")).unwrap();
    assert_eq!(item["id"], "a:y", "the requested id must win over set_properties.id");
    assert_eq!(item["note"], "n", "other set_properties still apply");
    assert_eq!(item["title"], "T", "target field with no override is kept");
}

// --- dropped CSL fields ----------------------------------------------------

/// `with_dropped_csl_fields` strips the listed top-level fields *before* the
/// item is stored, so they are absent from the cache record itself — not merely
/// filtered on the way out — while nested occurrences and unlisted fields stay.
#[test]
fn dropped_fields_never_reach_the_store() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_dropped_csl_fields(["reference", "abstract"])
        .register("s", ScriptSource::new("s", Answer::Fat, &clock))
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "x")]))).unwrap();
    assert!(report.is_complete(), "{:?}", report.failures);

    let rec = block_on(mgr.store().get("s:x")).unwrap().expect("stored");
    let Payload::Concrete(stored) = rec.payload else {
        panic!("expected a concrete payload");
    };
    assert!(stored.get("reference").is_none(), "dropped before storage");
    assert!(stored.get("abstract").is_none(), "dropped before storage");
    assert_eq!(stored["title"], "s:x", "unlisted fields are untouched");
    assert_eq!(
        stored["keep"]["reference"], "nested, not top level",
        "only top-level keys are dropped"
    );

    let item = block_on(mgr.get("s", "x")).unwrap();
    assert!(item.get("reference").is_none());
    assert_eq!(item["id"], "s:x");
}

/// A chained pointer's `set_properties` are stripped too. They *override* the
/// concrete target at read time, so a dropped field left in the pointer would
/// walk straight back out of `get` — a field dropped from the arXiv side
/// re-appearing on the DOI item it chains to.
#[test]
fn dropped_fields_are_stripped_from_chained_set_properties() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_dropped_csl_fields(["reference"])
        .register("a", ScriptSource::new("a", Answer::ChainFatTo("b"), &clock))
        .unwrap()
        .register("b", ScriptSource::new("b", Answer::Fat, &clock))
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert!(report.is_complete(), "{:?}", report.failures);

    let rec = block_on(mgr.store().get("a:x")).unwrap().expect("stored");
    let Payload::Chained { set_properties, .. } = rec.payload else {
        panic!("expected a chained payload");
    };
    assert!(
        set_properties.get("reference").is_none(),
        "the pointer must not smuggle a dropped field back in"
    );
    assert_eq!(set_properties["arxivid"], "1211.1037", "other properties survive");

    let item = block_on(mgr.get("a", "x")).unwrap();
    assert!(item.get("reference").is_none(), "neither hop reintroduces it");
    assert_eq!(item["arxivid"], "1211.1037");
    assert_eq!(item["title"], "b:x", "the chain still resolves to its target");
    assert_eq!(item["id"], "a:x");
}

/// Fields are dropped *before* the `id` is stamped on, so a host that lists
/// `"id"` — deliberately or by copy-paste — cannot strip the id the entry is
/// keyed on and echoed under.
#[test]
fn dropping_id_cannot_strip_the_entrys_id() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .with_dropped_csl_fields(["id", "reference"])
        .register("s", ScriptSource::new("s", Answer::Fat, &clock))
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "x")]))).unwrap();
    assert!(report.is_complete(), "{:?}", report.failures);

    let rec = block_on(mgr.store().get("s:x")).unwrap().expect("stored");
    let Payload::Concrete(stored) = rec.payload else {
        panic!("expected a concrete payload");
    };
    assert_eq!(stored["id"], "s:x", "the id survives being listed");
    assert!(stored.get("reference").is_none());
    assert_eq!(block_on(mgr.get("s", "x")).unwrap()["id"], "s:x");
}

// --- failure provenance (origin) -------------------------------------------

/// A directly-requested cite that fails carries `origin == None`: its own
/// `(prefix, key)` already matches the caller's input, so there is nothing to
/// attribute it back to.
#[test]
fn a_direct_failure_has_no_origin() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Fail, &clock)).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    let f = &report.failures[0];
    assert_eq!((f.prefix.as_str(), f.key.as_str()), ("s", "k"));
    assert_eq!(f.origin, None, "a directly-requested failure has no origin");
}

/// A failure discovered on a chain *target* is reported under the target's
/// `(prefix, key)` — but `origin` names the originally-requested cite that
/// pulled it in, so a caller joining `failures` back against its input still
/// finds the request that failed (instead of concluding it succeeded, only for
/// `get()` to break on the chain later).
#[test]
fn a_chained_target_failure_is_attributed_to_the_request() {
    let clock = MovableClock::default();
    // `a:x` chains to `b:x`; the `b` fetch fails.
    let a = ScriptSource::new("a", Answer::ChainTo("b"), &clock);
    let b = ScriptSource::new("b", Answer::Fail, &clock);
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    let f = &report.failures[0];
    // The failure identifies the *target* that actually failed …
    assert_eq!((f.prefix.as_str(), f.key.as_str()), ("b", "x"));
    // … and attributes it back to the requested cite.
    assert_eq!(f.origin, Some(("a".to_string(), "x".to_string())));
    // The requested id itself appears nowhere as a failing `(prefix, key)` —
    // origin is the only way to recover it. And `get` does break on the chain.
    assert!(
        !report.failures.iter().any(|f| f.prefix == "a" && f.key == "x"),
        "the target failure, not the request, is reported directly"
    );
    assert!(block_on(mgr.get("a", "x")).is_err());
}

/// A multi-hop chain attributes a deep failure to the *original* request, not
/// to the intermediate hop that immediately pointed at the failing target.
#[test]
fn a_multi_hop_chain_attributes_back_to_the_original_request() {
    let clock = MovableClock::default();
    // a:x -> b:x -> c:x, and the `c` fetch fails.
    let a = ScriptSource::new("a", Answer::ChainTo("b"), &clock);
    let b = ScriptSource::new("b", Answer::ChainTo("c"), &clock);
    let c = ScriptSource::new("c", Answer::Fail, &clock);
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap()
        .register(c.prefix, c).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    let f = &report.failures[0];
    assert_eq!((f.prefix.as_str(), f.key.as_str()), ("c", "x"));
    assert_eq!(
        f.origin,
        Some(("a".to_string(), "x".to_string())),
        "origin must be the original request, not the intermediate hop `b:x`"
    );
}

/// The end-to-end guarantee: a caller can recover exactly *which of its
/// requested cites* failed by mapping each failure to `origin.unwrap_or((prefix,
/// key))` — whether the failure was direct or on a chained descendant.
#[test]
fn every_failed_request_is_recoverable_from_the_report() {
    let clock = MovableClock::default();
    // `a:x` chains to `b:x` which fails (indirect); `d:k` fails directly.
    let a = ScriptSource::new("a", Answer::ChainTo("b"), &clock);
    let b = ScriptSource::new("b", Answer::Fail, &clock);
    let d = ScriptSource::new("d", Answer::Fail, &clock);
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap()
        .register(d.prefix, d).unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("a", "x"), ("d", "k")]))).unwrap();

    // Re-derive the set of *requested* cites that failed.
    let mut failed_requests: Vec<(String, String)> = report
        .failures
        .iter()
        .map(|f| {
            f.origin
                .clone()
                .unwrap_or_else(|| (f.prefix.clone(), f.key.clone()))
        })
        .collect();
    failed_requests.sort();
    assert_eq!(
        failed_requests,
        vec![
            ("a".to_string(), "x".to_string()),
            ("d".to_string(), "k".to_string()),
        ],
        "both requested cites must be recoverable from the report"
    );
}

// --- authoritative missing vs. reachability failure ------------------------

/// The `Missing` / `Failed` distinction, pinned side by side over an identical
/// hard-expired-but-within-grace cached copy.
///
/// * `Outcome::Failed` (source unreachable) is grace-served: no failure is
///   reported and the stale copy keeps being served (stale-while-revalidate).
/// * `Outcome::Missing` (source reachable, key authoritatively gone) is
///   **always** reported *and* removes the stale copy, so a later `get()` errors
///   instead of serving now-known-wrong data for the whole grace window.
#[test]
fn missing_is_reported_and_removes_stale_while_failed_is_grace_served() {
    // A cached concrete copy, hard-expired at t=1000 but well within the
    // default 14-day grace at the t=30_000 we read at.
    let seed = |store: &MemStore| {
        store.seed(
            "s:k",
            Payload::Concrete(serde_json::json!({"id": "s:k", "title": "OLD"})),
            0,
            1000,
        );
    };

    // Failed → grace-served, not reported, old data still served.
    let clock = MovableClock::default();
    clock.advance(30_000);
    let store = MemStore::default();
    seed(&store);
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Fail, &clock)).unwrap();
    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert!(
        report.is_complete(),
        "a within-grace Failed must not be reported: {:?}",
        report.failures
    );
    assert_eq!(
        block_on(mgr.get("s", "k")).unwrap()["title"],
        "OLD",
        "the stale copy must still be served on a reachability failure"
    );

    // Missing → reported even within grace, and the stale copy is removed.
    let clock = MovableClock::default();
    clock.advance(30_000);
    let store = MemStore::default();
    seed(&store);
    let mgr = CitationManager::new(NoopFetcher, store, clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Missing, &clock)).unwrap();
    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert_eq!(
        report.failures.len(),
        1,
        "an authoritative Missing must be reported even within grace"
    );
    assert_eq!(report.failures[0].key, "k");
    assert!(
        block_on(mgr.get("s", "k")).is_err(),
        "the now-known-wrong stale copy must be removed, not served"
    );
}

// --- chunking and rate limiting --------------------------------------------

/// Requests are spaced by `min_interval` *start to start*, across chunks and
/// across retrieval passes.
///
/// Both used to be broken: `drive_source` was called once per pass and reset
/// its "first chunk, no sleep" flag every time, so the second pass (which the
/// arXiv→DOI chain guarantees) fired immediately after the first; and the wait
/// was a full interval *after* the previous chunk returned, i.e.
/// `interval + latency`.
#[test]
fn pacing_is_start_to_start_and_survives_passes() {
    let clock = MovableClock::default();
    // `a` chains to `b` (so a second pass reaches `b`) but costs no time.
    let a = ScriptSource::new("a", Answer::ChainTo("b"), &clock);
    // One key per request, 1100 ms apart, each request taking 200 ms.
    let b = ScriptSource::new("b", Answer::Concrete, &clock).paced(1, 1100, 200);
    let b_log = b.log();
    let timer = AdvancingTimer {
        clock: clock.clone(),
    };
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), timer)
        .register(a.prefix, a).unwrap()
        .register(b.prefix, b).unwrap();

    let report =
        block_on(mgr.retrieve(&cites(&[("b", "d1"), ("b", "d2"), ("a", "x")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let times: Vec<i64> = b_log.borrow().iter().map(|(_, t)| *t).collect();
    assert_eq!(
        times.len(),
        3,
        "expected three `b` requests (two direct + one chained): {:?}",
        b_log.borrow()
    );
    // 0, then +1100 (not +1300 = interval + latency), then the *next pass*
    // waits out the remainder of the interval instead of firing at once.
    assert_eq!(times, vec![0, 1100, 2200], "log: {:?}", b_log.borrow());
}

// --- reading ---------------------------------------------------------------

/// `get_by_id` round-trips a `"prefix:key"` id, and an id without a `':'` is a
/// malformed id — not "no source registered for prefix `foo`".
#[test]
fn get_by_id_round_trips_and_rejects_a_missing_colon() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Concrete, &clock)).unwrap();

    // A key containing a colon still round-trips: ids split on the *first* one.
    block_on(mgr.retrieve(&cites(&[("s", "k"), ("s", "10.1/x:y")]))).unwrap();
    assert_eq!(block_on(mgr.get_by_id("s:k")).unwrap()["id"], "s:k");
    assert_eq!(
        block_on(mgr.get_by_id("s:10.1/x:y")).unwrap()["id"],
        "s:10.1/x:y"
    );

    let err = block_on(mgr.get_by_id("no-colon-here")).unwrap_err();
    assert!(matches!(err, Error::InvalidId(_)), "got {err}");
    assert!(err.to_string().contains("malformed"), "got: {err}");
}

/// A prefix containing `':'` would make `prefix:key` ids ambiguous, so it is
/// refused at registration time (with an error, not a panic) rather than
/// silently corrupting the cache.
#[test]
fn registering_a_prefix_with_a_colon_errors() {
    let clock = MovableClock::default();
    let err = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("bad:prefix", ScriptSource::new("bad:prefix", Answer::Concrete, &clock))
        .err()
        .expect("a colon-containing prefix must be rejected");
    assert!(matches!(err, Error::InvalidPrefix(_)), "got {err}");
    assert!(err.to_string().contains("bad:prefix"), "got: {err}");
}

/// An empty prefix yields `":key"` ids, which break the `get_by_id` split, so
/// it is refused the same way.
#[test]
fn registering_an_empty_prefix_errors() {
    let clock = MovableClock::default();
    let err = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("", ScriptSource::new("", Answer::Concrete, &clock))
        .err()
        .expect("an empty prefix must be rejected");
    assert!(matches!(err, Error::InvalidPrefix(_)), "got {err}");
}

/// The common case: a normal prefix registers fine and returns the manager for
/// further chaining.
#[test]
fn registering_a_normal_prefix_succeeds() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Concrete, &clock))
        .expect("a colon-free, non-empty prefix must register");
    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);
}

// --- prefixes are host-chosen bindings, not source properties --------------

/// A source that declares no prefix and simply echoes back the one it was
/// *registered* under (`ctx.prefix`), so a test can tell which binding
/// answered. `missing` flips it to the authoritative-miss path — the one where
/// a source has to build a `"prefix:key"` id for its own error message and so
/// must not assume a prefix.
struct EchoSource {
    missing: bool,
}

impl Source for EchoSource {
    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(3600)
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let id = format!("{}:{k}", ctx.prefix);
                    if self.missing {
                        Resolution::missing(k, Error::NotFound(id))
                    } else {
                        Resolution::concrete(k, item(&id))
                    }
                })
                .collect()
        })
    }
}

/// One source *type* — here even one identical configuration of it — can back
/// as many prefixes as the host wants, because the prefix lives in the
/// manager's binding and not in the source. Each registration keeps its own
/// cache ids, and each learns its own prefix from `ctx.prefix`.
#[test]
fn one_source_type_serves_several_host_chosen_prefixes() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("alpha", EchoSource { missing: false })
        .unwrap()
        .register("beta", EchoSource { missing: false })
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("alpha", "k"), ("beta", "k")]))).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Same key, two prefixes, two independent entries — and each source saw the
    // prefix it was bound to, not one baked into its type.
    let a = block_on(mgr.get("alpha", "k")).unwrap();
    assert_eq!(a["id"], "alpha:k");
    assert_eq!(a["title"], "alpha:k", "the source must see its own prefix");

    let b = block_on(mgr.get("beta", "k")).unwrap();
    assert_eq!(b["id"], "beta:k");
    assert_eq!(b["title"], "beta:k", "the source must see its own prefix");
}

/// A built-in registered under a non-default name behaves identically, and the
/// id it builds for its own diagnostics follows the host's naming — a source
/// hard-coding its prefix would report `doi:…` for a citation the user wrote as
/// `dx:…`.
#[test]
fn a_sources_error_message_uses_the_prefix_it_was_registered_under() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("dx", EchoSource { missing: true })
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("dx", "10.1/x")]))).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "dx");
    assert!(
        report.failures[0].message.contains("dx:10.1/x"),
        "the message must name the host's prefix, not one the source assumed: {}",
        report.failures[0].message
    );
}

/// Registering an already-bound prefix replaces the source behind it — the
/// binding is a map entry, which is how a host swaps a built-in for its own.
#[test]
fn re_registering_a_prefix_replaces_the_source() {
    let clock = MovableClock::default();
    let mgr = CitationManager::new(NoopFetcher, MemStore::default(), clock.clone(), InstantTimer)
        .register("s", ScriptSource::new("s", Answer::Fail, &clock))
        .unwrap()
        .register("s", EchoSource { missing: false })
        .unwrap();

    let report = block_on(mgr.retrieve(&cites(&[("s", "k")]))).unwrap();
    assert!(
        report.is_complete(),
        "the replacement source should have answered, not the failing one: {:?}",
        report.failures
    );
    assert_eq!(block_on(mgr.get("s", "k")).unwrap()["title"], "s:k");
}
