# Autocitefetch

Automatic retrieval of bibliographic citations from multiple sources
(arXiv, doi.org, local bibliography files, manual entries, …) into a canonical
CSL-JSON representation.

**Experimental development status:** This crate is still expermental and under active development; you can expect its API to change.


## Library structure

The core crate is `no_std` + `alloc` and all environment-related actions (I/O,
URL retrieval, cache persistence, the wall clock, and delays) are injected
through traits. It is designed to be paired with a document-processing system
that emits `\cite{arXiv:1211.1037}`-style commands (see
[FLM](https://github.com/phfaist/flm) and
[`flm-citations`](https://github.com/phfaist/flm-citations)).

**Components:**

- `crates/autocitefetch` — the core library (`no_std` + `alloc`).

- `crates/autocitefetch-std` — simple trait implementations for a standard `std`
  environment (clock, timer, filesystem cache, blocking HTTP fetcher).

- `crates/autocitefetch-cli` — a simple command-line interface to the library;
  convert a list of `prefix:key` citation keys into CSL-JSON output.


## Command-line tool

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
selects a [`Source`]. Retrieval is two-phase:

1. `manager.retrieve(&cites).await` — routes each key to its source, fetches
   (chunked + rate-limited), writes results to the cache. Returns a
   `RetrieveReport` of per-citation failures (one bad cite does **not** abort
   the batch).
2. `manager.get(prefix, key).await` — reads a resolved CSL-JSON item back,
   following any **chaining** pointers (e.g. arXiv → DOI) and merging their
   `set_properties`, rewriting `id` back to the requested one.


### Prefixes and sources

The manager holds a list of `(prefix, source)` pairs, created by
`manager.register(prefix, source)`.  Upon encountering a citation with a given
prefix, it queries the corresponding citaiton source.

The prefix is a non-empty string that may not contain the character `':'`.


### Chaining

Some arXiv entries have a corresponding "Related DOI", meaning that the paper
preprint was published in some publication venue.  When *autocitefetch*
retrieves such an entry from the arXiv with a related DOI, it stores a *pointer*
to the `doi` prefix so that the DOI source resolver can download the full
citation entry of the published version of the preprint (via
`Payload::Chained`).  (This behavior is fully configurable, including a
different prefix to use instead of `doi`.)

The manager's retrieval loop discovers chained targets and fetches them.


### Progress reporting

Callers can be informed of progress of the citation fetching by registering a
*reporter*; see `CitationManager::with_reporter(Rc<dyn Reporter>)`.


### Library usage sketch (std)

```rust,ignore
use autocitefetch::CitationManager;
use autocitefetch::source::{ArxivSource, DoiSource, ManualSource, BibliographyFileSource};
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};

let store = SingleFileCacheStore::new(".citecache").await?;
// The prefix is yours to choose — a source declares none. `register` returns
// `Result` because it rejects an empty or ':'-containing prefix (which would
// make `prefix:key` ids ambiguous), so chain with `?`.
let mgr = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
    .register("arxiv", ArxivSource::new())?
    .register("doi", DoiSource::new())?
    .register("manual", ManualSource::new("flm"))?
    .register("bib", BibliographyFileSource::new(["file:refs.json".into()]))?
    // Same source type, second prefix, different files — nothing special needed.
    .register("theses", BibliographyFileSource::new(["file:theses.json".into()]))?;

let report = mgr.retrieve(&[("doi".into(), "10.1103/PhysRev.47.777".into())]).await?;
let item = mgr.get("doi", "10.1103/PhysRev.47.777").await?; // CSL-JSON Value
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
