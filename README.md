# autocitefetch

Automatic retrieval of bibliographic citations from multiple sources
(arXiv, doi.org, local bibliography files, manual entries, …) into a canonical
**CSL-JSON** representation.

The core crate is **`#![no_std]`** (with `alloc`) and **executor-agnostic**: all
I/O — URL retrieval, cache persistence, the wall clock, and delays — is injected
through traits. The same core runs on a native `std` host or in a browser
(WASM `fetch()` + IndexedDB + `setTimeout`). It is designed to be paired with a
document-processing system that emits `\cite{arXiv:1211.1037}`-style commands.

## Workspace layout

```
crates/
  autocitefetch/          core library — #![no_std] + alloc
    src/
      lib.rs              crate root, re-exports, BoxFuture
      manager.rs          CitationManager: routing, retrieval loop, chaining, get()
      source/             the Source trait + built-in sources
        mod.rs            Source, Outcome, Resolution, RetrieveCtx
        arxiv.rs          arXiv Atom API              (SCAFFOLD — see Status)
        doi.rs            doi.org content negotiation (implemented)
        manual.rs         key-is-the-text escape hatch (implemented)
        bibfile.rs        CSL-JSON bibliography files (implemented, JSON)
      cache.rs            TTL policy: soft/hard expiry, jitter, freshness
      driver.rs           per-source chunking + rate limiting
      fetch.rs            Fetcher trait + Request/Response
      store.rs            CacheStore trait (per-entry KV) + CacheRecord
      env.rs              Clock + Timer traits, Timestamp
      csl.rs              CSL-JSON helpers (id, merge, field lookup)
      error.rs            crate Error
    tests/integration.rs  end-to-end tests (manual, doi, chaining)
  autocitefetch-std/      std backends for non-WASM consumers
    src/
      clock.rs            SystemClock
      timer.rs            BlockingTimer (thread::sleep)
      store.rs            DirCacheStore (one JSON file per entry, atomic writes)
```

## Model

A citation is a `(prefix, key)` pair, e.g. `("arxiv", "1211.1037")`. The prefix
selects a [`Source`]. Retrieval is two-phase:

1. `manager.retrieve(&cites).await` — routes each key to its source, fetches
   (chunked + rate-limited), writes results to the cache. Returns a
   `RetrieveReport` of per-citation failures (one bad cite does **not** abort
   the batch).
2. `manager.get(prefix, key).await` — reads a resolved CSL-JSON item back,
   following any **chaining** pointers (e.g. arXiv → DOI) and merging their
   `set_properties`, rewriting `id` back to the requested one.

### Chaining

When arXiv finds a DOI, it stores a *pointer* (`Payload::Chained`) to the `doi`
source instead of duplicating metadata. The manager's retrieval loop discovers
chained targets and fetches them; `get()` walks the chain on read.

## Design decisions

| Decision | Choice |
|---|---|
| Execution model | **Async, executor-agnostic** (`core::future::Future`; boxed futures keep traits object-safe). |
| CSL-JSON model | **Generic JSON value** (`serde_json::Value`) — lossless passthrough of doi.org / bib data; arXiv mapping done by hand. |
| Cache interface | **Per-entry key-value** `CacheStore` — incremental writes, maps to IndexedDB / embedded KV. |
| Trait surface | **Separate composable traits** (`Fetcher`, `CacheStore`, `Clock`, `Timer`) assembled on the manager. |

### Improvements over the JS/Python reference implementations

- **Stale-while-revalidate**: soft + hard expiry with a grace window — mildly
  outdated entries are still served when a source is unreachable.
- **TTL jitter** (deterministic, seeded by entry id) — avoids a thundering herd
  when many entries expire together.
- **Per-citation error tolerance** — failures are reported, not fatal.
- **Uniform I/O** — every source (including arXiv, eventually) goes through the
  one `Fetcher`; rate-limit delays are actually awaited (the JS `sleep` no-op and
  Python header-drop bugs are not reproduced).
- **Incremental persistence** — no full-cache rewrite on every store.

## Usage sketch (std)

```rust,ignore
use autocitefetch::CitationManager;
use autocitefetch::source::{ArxivSource, DoiSource, ManualSource, BibliographyFileSource};
use autocitefetch_std::{SystemClock, BlockingTimer, DirCacheStore};

let mgr = CitationManager::new(my_fetcher, DirCacheStore::new(".citecache")?, SystemClock, BlockingTimer)
    .register(ArxivSource::new())
    .register(DoiSource::new())
    .register(ManualSource::new())
    .register(BibliographyFileSource::new(["file:refs.json".into()]));

let report = mgr.retrieve(&[("doi".into(), "10.1103/PhysRev.47.777".into())]).await?;
let item = mgr.get("doi", "10.1103/PhysRev.47.777").await?; // CSL-JSON Value
```

A `Fetcher` (blocking `ureq`/`reqwest` on std, browser `fetch()` on WASM) is the
one backend you must supply yourself; none is bundled so the crates carry no HTTP
dependency.

## Status

Scaffolding is complete and tested end-to-end. Implemented: the manager,
routing, chaining, cache/TTL policy, the driver, and the `doi` / `manual` /
`bib` (JSON) sources. **Not yet implemented:**

- `arxiv.rs` — Atom-feed retrieval, version resolution, DOI chaining (trait
  wiring and rate limits are in place; returns a `Failed` outcome for now). Needs
  a `no_std` XML pull parser.
- Concurrent source execution (the driver loop is sequential; the join point is
  marked in `manager.rs`).
- A bundled `std` HTTP `Fetcher` and a WASM backend crate.
- YAML bibliography files (JSON works).

## Building

```sh
cargo build                                        # workspace (host)
cargo test                                         # end-to-end tests
cargo build -p autocitefetch --target wasm32-unknown-unknown   # WASM core
```

## License

MIT OR Apache-2.0
