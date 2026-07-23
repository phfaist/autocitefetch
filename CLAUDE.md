# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust workspace that retrieves bibliographic citations from multiple sources (arXiv, doi.org, local
bib files, manual entries) into CSL-JSON. The core crate is `#![no_std]` + `alloc` and
executor-agnostic so the *same* code runs on a native `std` host and in a browser (WASM). It is a
port of two prior libraries (see "Reference implementations" below).

- `crates/autocitefetch` — the core library. `no_std`, `#![forbid(unsafe_code)]`, no I/O of its own.
- `crates/autocitefetch-std` — `std` backends (clock, timer, filesystem cache, blocking HTTP fetcher).

## Commands

```sh
cargo test                                   # all 137 tests (workspace)
cargo test -p autocitefetch --test arxiv_dois override_map_beats_feed_doi   # one integration test
cargo test -p autocitefetch --lib filecache::tests::torn_tail_is_tolerated  # one unit test
cargo doc -p autocitefetch-std --no-deps     # currently warning-free — keep it that way
cargo clippy --workspace --all-targets       # currently clean — keep it that way
cargo build -p autocitefetch --target wasm32-unknown-unknown  # MUST still pass after core changes
cargo build -p autocitefetch-std --no-default-features        # std backends without ureq/TLS
cargo run -p autocitefetch-std --example resolve              # live demo (hits doi.org)
```

The tree is **not** rustfmt-clean, so `cargo fmt --all` would produce large unrelated diffs. Format
only what you touch (or leave formatting alone) rather than running it workspace-wide.

## Architecture

### Two-phase retrieval

`retrieve()` populates the cache and returns only a failure report; `get()` reads items back.
Nothing else returns CSL data.

1. `manager.retrieve(&[(prefix, key), …])` → `RetrieveReport { failures }`. Per-citation tolerant:
   one bad citation never aborts the batch, and only *store* errors produce `Err`.
2. `manager.get(prefix, key)` → `CslValue`. Walks chain pointers, merges `set_properties`, and
   rewrites `id` back to the originally requested `"prefix:key"`.

### The four injected traits

`Fetcher`, `CacheStore`, `Clock`, `Timer` (in `fetch.rs`, `store.rs`, `env.rs`) are the entire host
surface, assembled via `CitationManager::new(fetcher, store, clock, timer).register(source)`.
Every async trait method returns `BoxFuture<'a, T>` (`lib.rs`) — a **`!Send`** boxed future. This is
deliberate: WASM futures are `!Send`, the core assumes a single cooperative task, and boxing keeps
the traits object-safe (`&dyn Fetcher`, `Box<dyn Source>`). Do not add `Send`/`Sync` bounds.

`CacheStore` methods take `&self`; implementations use interior mutability (`RefCell`). The manager
holds only shared borrows while driving sources concurrently, so this is required, not incidental.

### The retrieval loop (`manager.rs`)

A worklist loop, not a fixed pipeline:

- Per pass: dedup against `seen`, look each id up in the store, classify freshness, and bucket the
  misses/stale by prefix. A **fresh** cached `Payload::Chained` entry pushes its target onto the
  worklist so the target is guaranteed present; a stale/expired one does *not* (it is about to be
  refetched, and pre-pushing a superseded pointer would fetch — and report a failure for — a
  citation nobody requested). When a refetch fails and the grace window keeps the old chained
  record, the target is pushed from the failure path instead.
- Worklist items carry a **depth**; `max_chain_depth` (default 16, `with_max_chain_depth`) bounds
  `retrieve` as well as `get`, so retrieval never fetches links `get()` could not reach.
- Buckets are driven concurrently with `buffer_unordered(MAX_CONCURRENT_SOURCES = 8)`; results are
  then applied to the store **serially** to keep writes/worklist/report updates simple.
- New chained targets discovered during a pass feed the next pass; the loop runs until the worklist
  drains, then calls `store.flush()`.

### Retry is interposed, not implemented per source

`retrieve()` wraps `self.fetcher` in a `RetryingFetcher` (`retry.rs`) and hands *that* to sources as
`ctx.fetcher`. Sources call `ctx.fetcher.fetch(..)` and never learn a retry happened. Consequences:
**never add retry logic inside a source**, and any I/O done outside `manager.retrieve` gets no
retries. Retries cover transport errors and 429/500/502/503/504, honor numeric `Retry-After`, and
back off exponentially (`RetryPolicy`: 5 retries, 500 ms base, 30 s cap).

### Chaining

arXiv doesn't duplicate DOI metadata: it stores `Outcome::Chained { prefix, key, set_properties }`,
a pointer. `get()` walks up to `max_chain_depth` (16) links, accumulating `set_properties` with
`csl::merge_defaults` — **properties closer to the request win** — until it hits a
`Payload::Concrete`. Note `Source::chains_to()` exists but the manager does not consume it; chain
discovery is dynamic via the worklist.

### Cache policy (`cache.rs`)

Two-tier expiry per record: `stale_after` (soft, `stale_percent` = 80% of TTL) and `expires` (hard),
plus a `grace` window (14 days) during which a hard-expired entry is still served **if the source is
currently unreachable** (stale-while-revalidate — see `store_resolutions`' `Outcome::Failed` arm).
Hard TTL gets ±15% deterministic jitter seeded by FNV-1a over the entry id, so a batch fetched
together doesn't expire together.

### Sources (`source/`)

Each `Source` declares `prefix`, `chunk_size`, `min_interval`, `default_ttl`, and implements
`retrieve_chunk`. `driver.rs` does the chunking and paces requests **start→start**: it sleeps
`min_interval - elapsed_since_previous_request`, and the manager threads that per-prefix timestamp
across passes, so the second pass of an arXiv→DOI chain cannot hit doi.org with 0 ms spacing.
(Pacing state is per-`retrieve()` call; back-to-back `retrieve()`s on one manager still reset it.)

| prefix | chunk / interval / TTL | notes |
|---|---|---|
| `arxiv` | 100 / 3100 ms / 10 d | Atom feed parsed with `xmlparser`; version resolution; chains to `doi` |
| `doi` | 1 / 1100 ms / 360 d | doi.org content negotiation returns CSL-JSON verbatim — no field mapping |
| `manual` | ∞ / 0 / **0** | key *is* the formatted text, stored under `_formatted_text`; TTL 0 ⇒ ephemeral |
| `bib` | ∞ / 0 / 60 s | file(s) fetched through the `Fetcher` (`file:` URLs), indexed by `id` |

arXiv version resolution: an explicitly-versioned key (`1211.1037v2`) resolves to that exact version
and is emitted **concrete, never chained**; a versionless key picks the best returned entry (a
versionless entry wins, else the highest `vN`) and chains to its DOI. DOI overrides are data
(`with_override_dois`): `Some(doi)` injects/replaces, `None` *suppresses* (keep arXiv metadata,
don't chain).

### Single-file cache (`filecache.rs` + `autocitefetch-std/src/store.rs`)

`FileCacheStore<Fs: CacheFs>` lives in the **core** crate (generic over an injected `CacheFs`); the
std crate supplies only real filesystem ops (`StdCacheFs`) and the `SingleFileCacheStore` wrapper.
Layout in one directory: `citations.jsonl` (header line 0 + one sorted entry per line — the only
file worth committing), per-writer `citations.<pid>-<nanos>.log` append logs (lock-free writes), and
`citations.lock`. `flush()` is the *only* operation that locks or rewrites the whole file: it folds
main + all sidecars, atomically replaces the main file, and then deletes **only its own** sidecar.
It must not delete a peer's: the lock serializes compaction against *compaction*, never against the
lock-free `append`, so unlinking a peer's log destroys any write that landed after the fold read it
(measured: ~300 of 400 acknowledged puts lost). The cost of that fix is that a **crashed** writer's
sidecar is never reaped — reaping it needs a `CacheFs::try_lock_exclusive` that does not exist yet.

Merge rules, all exercised by unit tests: max-`expires` wins on duplicate ids; a record beats a
tombstone unless the id was never re-added. Torn-line tolerance applies to **sidecars only** — the
main file is written via fsync+rename and so can never be legitimately torn, so an unparseable line
or an unknown `{"schema":N}` header there is a hard `Err` rather than a silent skip (silently
skipping then rewriting turned recoverable corruption into permanent loss).

## Invariants when editing

- **All I/O goes through `Fetcher`** — including local file reads (bib files, the arXiv DOI-override
  JSON). Sources must never touch the filesystem or network directly.
- **`retrieve_chunk` must return exactly one `Resolution` per requested key**, using
  `Outcome::Failed` for misses. The manager matches by `res.key`; an omitted key is silently neither
  stored nor reported.
- **No RNG, no ambient clock, no `std`** in the core. Jitter is FNV-1a over a stable seed (id, or
  url+attempt); time only ever comes from the injected `Clock`. `Timestamp` is `i64` ms since epoch.
  Use `hashbrown::HashMap`, `core::error::Error`, `alloc::format!`.
- Host-parses-config principle: the library takes **data**, not file formats. Non-JSON formats reach
  it via `BibliographyFileSource::with_parser` / `from_entries` and `ArxivSource::with_override_dois`.
  Don't add format crates (YAML/TOML/BibTeX) to the core.

## Testing conventions

There is **no async runtime anywhere** — not in tests, examples, or the std crate. Every test defines
a local `block_on` that polls with `Waker::noop()` and panics after ~1M polls ("a mock unexpectedly
pended"). All std backends are blocking-in-a-future (`UreqFetcher` blocks the thread, `BlockingTimer`
sleeps it), so they resolve on first poll. Mocks (`MockFetcher` with a URL→response route table,
`MemStore`, `FixedClock`, `InstantTimer`) are **deliberately duplicated per test file** rather than
shared — follow that pattern; copy from `tests/integration.rs` or `tests/arxiv.rs`.

Tests must not hit the network. Only `examples/resolve.rs` does.

## Adding a source

Implement `Source` in `crates/autocitefetch/src/source/<name>.rs`, re-export it from `source/mod.rs`,
build the URL with the module-local percent-encoder (see `arxiv.rs`/`doi.rs` — deliberately
hand-rolled to avoid a `url` dependency), fetch via `ctx.fetcher`, and return one `Resolution` per
key. Users can also register their own source at runtime; nothing about the built-ins is privileged.

## Docs convention

`README.md` is the design document (workspace layout, design-decision table,
improvements-over-references list, status) and is updated in its own commits when architecture
changes. It has drifted before — if you add a module, a source, or a backend, update the layout tree
and the status paragraph in the same change.

Module-level `//!` docs carry the real design rationale (why `BoxFuture` is `!Send`, why compaction
locks, why jitter is deterministic). Read them before changing behavior; extend them when you do.

## Reference implementations

This is a port of two libraries with the same architecture, useful for behavior parity questions:

- JS: `~/Research/projects/zoodb/zoodb/src/citationmanager/` (`_manager.js`, `_cache.js`, `source/*.js`)
- Python: `~/Research/util/flm-citations/flm_citations/` (`feature.py`, `citesources/*.py`; ignore the FLM glue)

Known reference bugs deliberately **not** reproduced here: the JS `sleep` no-op that defeated
backoff, the JS dead arXiv TTL, Python's `fetch_url` dropping headers, Python's silent drop of
explicitly-versioned arXiv requests. Neither reference has TTL jitter or stale-while-revalidate.
