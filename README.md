# Autocitefetch

Automatic retrieval of bibliographic citations from multiple sources
(arXiv, doi.org, local bibliography files, manual entries, …) into a canonical
CSL-JSON representation.

**Experimental development status:** This crate is still experimental and under
active development; you can expect its API to change.


## Library structure

The core crate is `no_std` + `alloc` and all environment-related actions (I/O,
URL retrieval, cache persistence, the wall clock, and delays) are injected
through traits. It is designed to be paired with a document-processing system
that emits `\cite{arXiv:1211.1037}`-style commands (see
[FLM](https://github.com/phfaist/flm) and
[`flm-citations`](https://github.com/phfaist/flm-citations)).

**Components:**

- [`autocitefetch`](https://crates.io/crates/autocitefetch)
  ([docs](https://docs.rs/autocitefetch)) — the core library (`no_std` + `alloc`).

- [`autocitefetch-std`](https://crates.io/crates/autocitefetch-std)
  ([docs](https://docs.rs/autocitefetch-std)) — simple trait implementations for
  a standard `std` environment (clock, timer, filesystem cache, a cache on a
  host's cachette manager, blocking HTTP fetcher).

- [`autocitefetch-cli`](https://crates.io/crates/autocitefetch-cli) — a simple
  command-line interface to the library; convert a list of `prefix:key` citation
  keys into CSL-JSON output.


## Command-line tool

You can install the command-line tool directly from *crates.io*: `cargo install
autocitefetch-cli`.

The `autocitefetch` command-line tool (`autocitefetch-cli` crate) reads a
citation list and writes the resolved CSL-JSON as a JSON array. The input is
read as one `prefix:key` string per line.  Usage:

```sh
$ printf 'arxiv:1211.1037\ndoi:10.1103/PhysRev.47.777\n' | autocitefetch
$ autocitefetch cites.txt --bib refs.yaml -o bibliography.json
$ autocitefetch --cite bib:knuth1984 --bib refs.yaml --enable bib
```

Run `autocitefetch --help` for information about options.


## Building

In this repo:

```sh
cargo build                                        # workspace (host)
cargo test                                         # all tests
cargo build -p autocitefetch --target wasm32-unknown-unknown   # WASM core (no_std)
cargo build -p autocitefetch-std --no-default-features         # std backends, no HTTP dep
cargo run   -p autocitefetch-std --example resolve             # live demo (needs network)
cargo install --path crates/autocitefetch-cli                  # the `autocitefetch` binary
```


## Model

A citation is a `(prefix, key)` pair, e.g. `("arxiv", "1211.1037")`. The prefix
selects a `Source`. Retrieval is two-phase:

1. `manager.retrieve(&cites).await` — routes each key to its source, fetches
   (chunked + rate-limited), writes results to the cache. Returns a
   `RetrieveReport` of per-citation failures (one bad cite does **not** abort
   the batch).
2. `manager.get(prefix, key)` — synchronous; reads a resolved CSL-JSON item
   back from the cache, following any **chaining** (e.g. arXiv → DOI) and
   merging its `set_properties`, and sets `id` to the requested one.

The cache holds what each source returned. The options of a run — arXiv DOI
overrides, the chain target, the fields dropped from the output — are applied
when an item is read, so changing them never changes the cache and needs no
request.


### Prefixes and sources

The manager holds a list of `(prefix, source)` pairs, created by
`manager.register(prefix, source)`.  Upon encountering a citation with a given
prefix, it queries the corresponding citation source.

The prefix is a non-empty string that may not contain the character `':'`.


### Chaining

Some arXiv entries have a corresponding "Related DOI", meaning that the paper
preprint was published in some publication venue.  *autocitefetch* caches the
arXiv metadata of such an entry, with its DOI; when the citation is read, the
arXiv source *chains* it to the `doi` prefix, so that the citation resolves to
the full entry of the published version, which the DOI source downloads
(`Source::interpret` answers `Interpreted::Chained`).  This behavior is fully
configurable — a different prefix instead of `doi`, no chaining at all, or
per-paper DOI overrides — and since it applies on read, changing it takes
effect at once for every cached entry.

The manager's retrieval loop discovers chained targets and fetches them.


### The cache

The cache holds one CSL-JSON item per citation, with its expiry times. Its
*storage policy* (`StoragePolicy`) decides what it keeps: the TTL policy, and
whether the bulky fields `reference` (a work's own bibliography) and `abstract`
are kept — both are left out by default.

There are three ways to get a cache:

1. **A single JSONL file** (the default): `autocitefetch_std::SingleFileCacheStore`
   keeps one committable `citations.jsonl` file in a directory of your choosing,
   plus throwaway files under the `._citations` prefix while a run is in flight
   (ignore them with `._citations*`). Cache files written by autocitefetch 0.1.1
   are reused as they are.
2. **Any [cachette](https://crates.io/crates/cachette) store**:
   `autocitefetch::StoreCache::new(store)` works over any `cachette::Store` — a
   `MemoryStore`, a `JsonlStore` on a file system of your own, a store of your
   own. autocitefetch re-exports `cachette`, so you name the version it was
   built with.
3. **A session on a host's `cachette::Manager`**: a program that keeps its
   caches with cachette's manager passes
   `autocitefetch_std::SessionCache::open(&manager, &instance, policy)`; the
   items then live in an instance that the host's manager keeps, sizes and
   prunes.

**In a browser**, where storage answers asynchronously, keep a
`cachette::MemoryStore` for the run: load it with `cachette::import` before,
and save it with `cachette::export` after.

```rust,ignore
use autocitefetch::cachette::{self, ImportMode, MemoryStore};
use autocitefetch::{CitationManager, StoreCache, StoreError};

// Before the run: `saved` is the text the host stored last time.
let mut store = MemoryStore::new();
cachette::import(&mut store, saved.lines().map(Ok::<_, StoreError>), ImportMode::Lenient)?;
let mgr = CitationManager::new(fetcher, StoreCache::new(store), clock, timer)
    .register("arxiv", ArxivSource::new())?;
mgr.retrieve(&cites).await?;
// After the run: save the store, as the text of a JSONL file.
let mut saved = String::new();
mgr.store().with_store(|s| cachette::export(s, |line| { saved.push_str(line); Ok(()) }))?;
```


### Refresh batching

Cached entries go stale over time and are refetched. Rather than spending a
whole rate-limited request on a handful of stale entries, a source can *defer*
small amounts of refresh work (serving the cached copies meanwhile, for a
bounded time), and *top up* a request that goes out anyway with entries that
are nearly due. Each source declares a default (`Source::refresh_batching`;
the arXiv and DOI sources batch, a custom source is eager); the host can
override it per prefix with `CitationManager::with_refresh_batching`, or on the
command line with `--refresh-batching PREFIX:SETTINGS` (e.g. `arxiv:min=50`,
`doi:eager`).


### Progress reporting

Callers can be informed of progress of the citation fetching by registering a
*reporter*; see `CitationManager::with_reporter(Rc<dyn Reporter>)`.


### Library usage sketch (std)

```rust,ignore
use autocitefetch::{CitationManager, StoragePolicy};
use autocitefetch::source::{ArxivSource, DoiSource, ManualSource, BibliographyFileSource};
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};

// Synchronous, like every cache operation. Keep abstracts in the cache.
let store = SingleFileCacheStore::new(".citecache")?
    .with_policy(StoragePolicy { keep_abstract: true, ..StoragePolicy::default() });
// The prefix is yours to choose — a source declares none. `register` returns
// `Result` because it rejects an empty or ':'-containing prefix (which would
// make `prefix:key` ids ambiguous), so chain with `?`.
let mgr = CitationManager::new(UreqFetcher::default(), store, SystemClock::new(), BlockingTimer)
    .register("arxiv", ArxivSource::new())?
    .register("doi", DoiSource::new())?
    .register("manual", ManualSource::new("flm"))?
    .register("bib", BibliographyFileSource::new(["file:refs.json".into()]))?
    // Same source type, second prefix, different files — nothing special needed.
    .register("theses", BibliographyFileSource::new(["file:theses.json".into()]))?;

let report = mgr.retrieve(&[("doi".into(), "10.1103/PhysRev.47.777".into())]).await?;
let item = mgr.get("doi", "10.1103/PhysRev.47.777")?; // CSL-JSON Value
```

With a host's cachette manager, only the cache changes:

```rust,ignore
use autocitefetch::cachette::{DiscardReports, Manager, ManagerSettings, SystemClock};
use autocitefetch_std::SessionCache;

let manager = Manager::open_path(cache_root, ManagerSettings::default(), SystemClock::new(), DiscardReports)?;
// One instance per project; caches with different `keep_*` flags never mix.
let store = SessionCache::open(&manager, "my-project", StoragePolicy::default())?;
```

The core crate bundles no `Fetcher`, so it carries no HTTP dependency at all.
`autocitefetch-std` supplies one — `UreqFetcher`, blocking, behind the default-on
`http` feature (turn it off for a build with no network/TLS dependency). On an
async runtime or in a browser, implement the trait yourself with
`reqwest`/`hyper` or `fetch()`; the `std` backends are blocking and are meant for
a blocking driver (`pollster::block_on`, or the `Waker::noop()` poll loop the
example and tests use).


## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
