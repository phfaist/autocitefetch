//! Offline end-to-end test of the `file:` path in [`UreqFetcher`].
//!
//! Writes a small CSL-JSON bibliography to a temp file and drives a
//! [`CitationManager`] with a [`BibliographyFileSource`] pointing at it,
//! asserting the entry resolves **with no network access** — the fetcher
//! services the request straight off the filesystem.

// The fetcher lives behind the `http` feature; with it off this file is empty.
#![cfg(feature = "http")]

use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::CitationManager;
use autocitefetch::source::BibliographyFileSource;
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};

/// Blocking driver: every backend resolves on the first poll (see the example).
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete (a backend unexpectedly pended)");
}

const BIB_JSON: &str = r#"[
  {
    "id": "knuth1984",
    "type": "book",
    "title": "The TeXbook",
    "author": [{"family": "Knuth", "given": "Donald E."}],
    "issued": {"date-parts": [[1984]]}
  }
]"#;

#[test]
fn bibliography_file_resolves_offline_through_the_fetcher() {
    // A real temp dir: removed even when an assertion below panics. A pid-keyed
    // path cleaned up only on success can leave a populated cache dir behind
    // that a later run with a recycled pid resolves from.
    let tmp = tempfile::tempdir().expect("create temp dir");
    let dir = tmp.path();
    let bib_path = dir.join("refs.json");
    std::fs::write(&bib_path, BIB_JSON).expect("write bib file");
    // A bare path, not a `file:` URL: `file:` inputs are percent-decoded, and
    // a temp path containing a literal `%` would then resolve elsewhere.
    let bib_url = bib_path.display().to_string();

    let store = block_on(SingleFileCacheStore::new(dir.join("cache"))).expect("open cache dir");
    let manager = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
        .register("bib", BibliographyFileSource::new([bib_url])).unwrap();

    let cites = vec![("bib".to_string(), "knuth1984".to_string())];
    let report = block_on(manager.retrieve(&cites)).expect("retrieve should not error");
    assert!(
        report.is_complete(),
        "expected offline resolution, failures: {:?}",
        report.failures
    );

    let item = block_on(manager.get("bib", "knuth1984")).expect("entry should resolve");
    assert_eq!(item["id"], "bib:knuth1984");
    assert_eq!(item["title"], "The TeXbook");
    assert_eq!(item["type"], "book");

    // A missing key in the same file is reported, not fatal.
    let missing = vec![("bib".to_string(), "does-not-exist".to_string())];
    let report = block_on(manager.retrieve(&missing)).expect("retrieve should not error");
    assert_eq!(report.failures.len(), 1);
}

/// Pointing the bib source at a *directory* is an easy mistake. It must fail
/// promptly: the underlying `fs::read` error is permanent, so no retry/backoff
/// (which would block this thread for ~16 s) may be attempted.
#[test]
fn a_directory_instead_of_a_file_fails_without_retrying() {
    use std::time::Instant;

    let tmp = tempfile::tempdir().expect("create temp dir");
    let dir = tmp.path();
    let store = block_on(SingleFileCacheStore::new(dir.join("cache"))).expect("open cache dir");
    let bibdir = dir.join("bibdir");
    std::fs::create_dir_all(&bibdir).expect("create bib dir");

    let manager = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
        .register("bib", BibliographyFileSource::new([bibdir.display().to_string()])).unwrap();

    let started = Instant::now();
    let cites = vec![("bib".to_string(), "whatever".to_string())];
    let report = block_on(manager.retrieve(&cites)).expect("retrieve should not error");
    let elapsed = started.elapsed();

    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert!(
        elapsed < std::time::Duration::from_secs(2),
        "a directory must not be retried with backoff; took {elapsed:?}"
    );
}
