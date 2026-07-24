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
//! (the `bib` entry is read from a temp file through the fetcher).
//!
//! **Offline, this looks hung.** The `doi` fetch fails as a transport error,
//! which is retryable, so the blocking backends spend ~16–19 s of
//! `thread::sleep` exhausting the 5 retries — with no output — before the
//! failure is reported and the two offline citations print.

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
///
/// The poll budget matters: with a `Waker::noop()` there is nothing to wake
/// this loop, so a future that genuinely pends (a backend that is *not*
/// blocking-in-a-future) would spin here forever. Fail loudly instead.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
    panic!("future did not complete (a backend unexpectedly pended — this driver only works with blocking backends)");
}

fn main() {
    // A CSL-JSON bibliography written to a temp file, resolved offline through
    // the fetcher's local-file path.
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
    // A bare filesystem path, not `format!("file://{}", …)`: the fetcher takes
    // plain paths verbatim, whereas a `file:` URL is percent-*decoded* on the
    // way in — so any temp path containing a literal `%` would break.
    let bib_url = bib_path.display().to_string();

    let cache_dir = tmp.join("cache");
    let store = block_on(SingleFileCacheStore::new(&cache_dir)).expect("open cache dir");

    let manager = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
        .register(DoiSource::new())
        .expect("register doi source")
        .register(ManualSource::new())
        .expect("register manual source")
        .register(BibliographyFileSource::new([bib_url]))
        .expect("register bibliography source");

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
