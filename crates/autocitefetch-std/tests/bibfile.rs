//! Offline end-to-end test of the `file:` path in [`UreqFetcher`].
//!
//! Writes a small CSL-JSON bibliography to a temp file and drives a
//! [`CitationManager`] with a [`BibliographyFileSource`] pointing at its
//! `file:` URL, asserting the entry resolves **with no network access** — the
//! fetcher services the request straight off the filesystem.

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
fn bibliography_file_resolves_offline_through_file_url() {
    // Unique temp dir for this test run.
    let dir = std::env::temp_dir().join(format!(
        "autocitefetch-bibfile-test-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let bib_path = dir.join("refs.json");
    std::fs::write(&bib_path, BIB_JSON).expect("write bib file");
    let bib_url = format!("file://{}", bib_path.display());

    let store = block_on(SingleFileCacheStore::new(dir.join("cache"))).expect("open cache dir");
    let manager = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
        .register(BibliographyFileSource::new([bib_url]));

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

    let _ = std::fs::remove_dir_all(&dir);
}
