//! End-to-end example: wire the `std` backends (blocking [`UreqFetcher`],
//! [`SingleFileCacheStore`], [`SystemClock`], [`BlockingTimer`]) into a
//! [`CitationManager`], register the `doi` / `manual` / `bib` sources, resolve
//! a few citations, and print the resulting CSL-JSON.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p autocitefetch-std --example resolve
//! ```
//!
//! The `doi` citation needs network; `manual` and `bib` resolve fully offline
//! (the `bib` entry is read from a temp `file:` URL through the fetcher).

use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::CitationManager;
use autocitefetch::source::{BibliographyFileSource, DoiSource, ManualSource};
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};

/// A minimal blocking driver. Every backend here (`UreqFetcher` blocks the
/// thread, `BlockingTimer` sleeps it, `SingleFileCacheStore` does blocking I/O)
/// resolves its future on the first poll, so a no-op waker suffices — no async
/// runtime is pulled in. On a real async runtime, use that runtime's `block_on`
/// and async `Fetcher`/`Timer` instead.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        // A backend unexpectedly pended (none of the std ones do); yield the
        // thread and retry rather than spin hot.
        std::thread::yield_now();
    }
}

fn main() {
    // A CSL-JSON bibliography written to a temp file, resolved offline via a
    // `file:` URL — demonstrates the fetcher's local-file path.
    let tmp = std::env::temp_dir().join(format!("autocitefetch-example-{}", std::process::id()));
    std::fs::create_dir_all(&tmp).expect("create temp dir");
    let bib_path = tmp.join("refs.json");
    std::fs::write(
        &bib_path,
        br#"[
          {
            "id": "knuth1984",
            "type": "book",
            "title": "The TeXbook",
            "author": [{"family": "Knuth", "given": "Donald E."}],
            "issued": {"date-parts": [[1984]]}
          }
        ]"#,
    )
    .expect("write bib file");
    let bib_url = format!("file://{}", bib_path.display());

    let cache_dir = tmp.join("cache");
    let store = block_on(SingleFileCacheStore::new(&cache_dir)).expect("open cache dir");

    let manager = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
        .register(DoiSource::new())
        .register(ManualSource::new())
        .register(BibliographyFileSource::new([bib_url]));

    let cites = vec![
        // Needs network (doi.org content negotiation).
        ("doi".to_string(), "10.1103/PhysRev.47.777".to_string()),
        // Offline: the key *is* the pre-formatted text.
        ("manual".to_string(), "Einstein, A. (1935)".to_string()),
        // Offline: looked up in the temp bibliography file above.
        ("bib".to_string(), "knuth1984".to_string()),
    ];

    println!("Resolving {} citation(s)...", cites.len());
    match block_on(manager.retrieve(&cites)) {
        Ok(report) => {
            for f in &report.failures {
                eprintln!("  ! {}:{} failed: {}", f.prefix, f.key, f.message);
            }
        }
        Err(e) => {
            eprintln!("retrieve aborted (store error): {e}");
            let _ = std::fs::remove_dir_all(&tmp);
            std::process::exit(1);
        }
    }

    for (prefix, key) in &cites {
        println!("\n=== {prefix}:{key} ===");
        match block_on(manager.get(prefix, key)) {
            Ok(item) => match serde_json::to_string_pretty(&item) {
                Ok(json) => println!("{json}"),
                Err(e) => eprintln!("(could not serialize: {e})"),
            },
            Err(e) => eprintln!("(unresolved: {e})"),
        }
    }

    let _ = std::fs::remove_dir_all(&tmp);
}
