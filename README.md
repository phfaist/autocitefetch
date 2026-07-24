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
        arxiv.rs          arXiv Atom API (xmlparser), version resolution, DOI overrides
        doi.rs            doi.org content negotiation
        manual.rs         key-is-the-text escape hatch
        bibfile.rs        bibliography files (pluggable parser; JSON default)
      cache.rs            TTL policy: soft/hard expiry, jitter, freshness
      driver.rs           per-source chunking + rate limiting
      retry.rs            RetryingFetcher: transparent retry/backoff wrapper
      fetch.rs            Fetcher trait + Request/Response
      store.rs            CacheStore trait (per-entry KV) + CacheRecord
      filecache.rs        FileCacheStore: single-file JSONL cache over a CacheFs
      env.rs              Clock + Timer traits, Timestamp
      csl.rs              CSL-JSON helpers (id, merge, field lookup)
      error.rs            crate Error
    tests/                integration.rs, arxiv.rs, arxiv_dois.rs,
                          bibfile_data.rs, concurrency.rs, retry.rs,
                          cache_policy.rs, manager_contract.rs
  autocitefetch-std/      std backends for non-WASM consumers
    src/
      lib.rs              crate root, re-exports
      clock.rs            SystemClock
      timer.rs            BlockingTimer (thread::sleep)
      store.rs            StdCacheFs + SingleFileCacheStore (citations.jsonl)
      fetcher.rs          UreqFetcher (blocking HTTP + file:) — `http` feature
    tests/                bibfile.rs, filecache_std.rs
    examples/resolve.rs   end-to-end demo (doi + manual + bib)
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

### Cache files (std backend)

`SingleFileCacheStore` keeps three kinds of file in a directory of your choosing:

* `citations.jsonl` — the cache itself: a header line, then one sorted
  `{"id":…,"rec":…}` line per entry. One entry per line keeps git diffs minimal;
  this is the only file worth committing.
* `citations.<writer>.log` — per-writer append logs. Writes go here lock-free
  and are folded into the main file by `flush()` (called at the end of
  `retrieve`/`prune`), which is the only operation that takes the lock or
  rewrites the whole file. `flush()` folds *every* sidecar it finds but only
  deletes **its own** and any left by a **crashed** peer — never a live peer's,
  since that would race its lock-free appends and destroy acknowledged writes.
* `citations.<writer>.log.lock` — a per-writer **liveness lock**, held open for
  the store's whole life. `flush()` reaps a peer's sidecar only when it can take
  that peer's lock (the OS frees it when the owner process crashes); a held lock
  means the owner is alive, so the sidecar is folded read-only and left in place.
* `citations.lock` — the compaction lockfile.
* `citations.jsonl.tmp` — the staging file for the atomic replace.

So commit the first and ignore the rest:

```gitignore
citations.*.log
citations.*.log.lock
citations.lock
citations.jsonl.tmp
```

The store *reads* every `citations.*.log` in this directory, so don't park
unrelated files there under that name. A main file it cannot parse — a corrupt
line, an unresolved merge conflict, or a `{"schema":N}` newer than this build —
makes `open()` fail rather than silently dropping the entries it can't read.

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
- **Probabilistic stale revalidation** — in the soft-stale window an entry is
  refetched only with a probability that ramps from ~0 at the soft expiry to ~1
  at the hard expiry (a deterministic FNV-1a draw over `(id, now)`, no RNG). The
  soft tier thus does real work: the effective TTL is close to the full nominal
  TTL instead of `stale_percent`% of it, and revalidation is spread out.
- **TTL jitter** (deterministic, seeded by entry id) — avoids a thundering herd
  when many entries expire together.
- **Per-citation error tolerance** — failures are reported, not fatal. The
  manager also enforces the source contract: a key a source silently omits (or
  answers twice) is reported rather than vanishing from both cache and report.
- **Bounded chains** — `max_chain_depth` (default 16, configurable) bounds
  `retrieve` as well as `get`, so a cyclic or runaway chain cannot fan out.
- **Start→start rate limiting** — `min_interval` is measured request-start to
  request-start and the timestamp is carried *across* retrieval passes, so the
  arXiv→DOI chain's second pass cannot hit doi.org with zero spacing.
- **Automatic retry/backoff** — a transparent `RetryingFetcher` retries
  transport errors and retryable statuses (429/5xx), honors `Retry-After`, and
  backs off with deterministic jitter — applied to every source, no source code
  changed. Configurable via `RetryPolicy`.
- **Uniform I/O** — every source, including arXiv, goes through the one
  `Fetcher`; rate-limit delays are actually awaited (the JS `sleep` no-op and
  Python header-drop bugs are not reproduced).
- **Incremental, committable persistence** — the `CacheStore` interface is
  per-entry, so nothing rewrites the whole cache on every store. The bundled file
  backend goes further: lock-free per-writer append logs, compacted under a lock
  into one sorted, git-committable `citations.jsonl` (see above).
- **Careful arXiv version resolution** — groups returned entries by base id and
  prefers a versionless entry, else the highest version; explicitly-versioned
  requests stay concrete (fixes the Python reference, which silently drops them).
- **arXiv DOI overrides** — supplied as *data* (`with_override_dois`): `Some(doi)`
  injects/replaces, `None` *suppresses* (keep arXiv metadata, don't chain) — a
  capability the references lack. A JSON file convenience is also provided; other
  formats are parsed host-side and passed as data.
- **Host-parses I/O for config** — the library takes overrides as data, and the
  `bib` source's byte→CSL step is a pluggable parser (`with_parser`), so any
  serde format (YAML, TOML, …) works without the `no_std` core depending on it.
  `BibliographyFileSource::from_entries` skips loading entirely.

## Usage sketch (std)

```rust,ignore
use autocitefetch::CitationManager;
use autocitefetch::source::{ArxivSource, DoiSource, ManualSource, BibliographyFileSource};
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};

let store = SingleFileCacheStore::new(".citecache").await?;
let mgr = CitationManager::new(UreqFetcher::default(), store, SystemClock, BlockingTimer)
    .register(ArxivSource::new())
    .register(DoiSource::new())
    .register(ManualSource::new())
    .register(BibliographyFileSource::new(["file:refs.json".into()]));

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

## Status

Implemented and tested end-to-end: the manager, routing, chaining, cache/TTL
policy, the driver, transparent retry/backoff, the single-file JSONL cache, all
four sources (`arxiv`, `doi`, `manual`, `bib`), concurrent within-pass source
execution, and the `std` backends including a `ureq`-based HTTP `Fetcher` (with
`file:` support). The arXiv source parses the Atom feed with `xmlparser` (a
verified `no_std` crate), decodes XML entity references, does version resolution,
and chains to DOI. 137 tests pass; the core builds for `wasm32-unknown-unknown`;
clippy and rustdoc are warning-free.

**Not yet implemented:**

- A dedicated **WASM backend crate** (browser `fetch()` + IndexedDB + `setTimeout`
  impls of the four traits). The core already compiles for wasm32.
- **YAML** files out of the box (JSON is the built-in; YAML/TOML/etc. work today
  by passing a host parser via `with_parser`, or pre-parsed data).
- An async-runtime `Fetcher`/`Timer` (the bundled std ones are blocking, fine for
  CLI/batch; on tokio, impl the traits with `reqwest` / `tokio::time::sleep`).

## Building

```sh
cargo build                                        # workspace (host)
cargo test                                         # all tests
cargo build -p autocitefetch --target wasm32-unknown-unknown   # WASM core (no_std)
cargo build -p autocitefetch-std --no-default-features         # std backends, no HTTP dep
cargo run   -p autocitefetch-std --example resolve             # live demo (needs network)
```

## License

MIT OR Apache-2.0
