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
cargo test                                   # all 183 tests (workspace)
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
   one bad citation never aborts the batch, and only *store* errors produce `Err`. Each
   `CiteFailure` carries `prefix`/`key` (the citation that *actually* failed — the chain **target**
   when the failure is on a chained descendant) plus `origin: Option<(String,String)>`: `None` for a
   directly-requested cite, `Some(requested)` naming the request a chained-target failure descends
   from. Guarantee: every requested cite that ultimately fails is discoverable in `failures` either
   directly (matching `(prefix,key)`) or via a failure whose `origin` is it — so a caller joining the
   report against its input `cites` never wrongly concludes a cite succeeded (only for `get()` to
   later break on the chain). `origin.unwrap_or((prefix,key))` recovers the failed request.
2. `manager.get(prefix, key)` → `CslValue`. Walks chain pointers, merges `set_properties`, and
   rewrites `id` back to the originally requested `"prefix:key"`.

### The four injected traits

`Fetcher`, `CacheStore`, `Clock`, `Timer` (in `fetch.rs`, `store.rs`, `env.rs`) are the entire host
surface, assembled via `CitationManager::new(fetcher, store, clock, timer).register(source)?`
(`register` returns `Result` — it rejects an empty or `':'`-containing prefix rather than panicking).
Every async trait method returns `BoxFuture<'a, T>` (`lib.rs`) — a **`!Send`** boxed future. This is
deliberate: WASM futures are `!Send`, the core assumes a single cooperative task, and boxing keeps
the traits object-safe (`&dyn Fetcher`, `Box<dyn Source>`). Do not add `Send`/`Sync` bounds.

`CacheStore` methods take `&self`; implementations use interior mutability (`RefCell`). The manager
holds only shared borrows while driving sources concurrently, so this is required, not incidental.

### The retrieval loop (`manager.rs`)

A worklist loop, not a fixed pipeline:

- **Keys are normalized centrally, once.** Before an id is built, `manager.normalize_key` runs the
  requested key through the *routed source's* `Source::normalize_key(&str) -> String`. The trait
  default is **trim + ASCII-lowercase**; each built-in narrows it as its key space requires:

  | source | policy | why |
  |---|---|---|
  | `doi` | default (trim + lowercase) | DOIs are case-insensitive identifiers ⇒ one cache id per DOI |
  | `arxiv` | **trim only** | old-style ids carry a case-significant subject class (`math.AG/0601001`); the feed `<id>` is matched case-sensitively and `arxivid` is sliced out of it |
  | `bib` | **trim only** | a bib key is an opaque label matched byte-for-byte against the file's `id`; folding it would silently miss, or collide `Bell`/`bell` |
  | `manual` | **identity** | the key *is* the formatted citation text — case and whitespace are the payload |

  This happens at every point a `(prefix, key)` becomes a `cite_id` — the worklist loop head (so
  routing/`seen`-dedup/bucketing/storage all key on the canonical form, and `" 1211.1037 "` /
  `"1211.1037"` or `10.1103/PhysRevA.86.052329` / `10.1103/physreva.86.052329` collapse into one
  fetch/entry), the `Outcome::Chained` store arm (normalized by the **target** source's policy, so a
  chained pointer matches the id its target lands under — this is why `arxiv.rs` emits its DOI chain
  key verbatim and needs no DOI case handling of its own), and both `get`/`get_by_id` and each `get`
  chain hop. An unknown prefix has no source to consult, so its key is used verbatim. Sources thus
  only ever see already-normalized keys; **do not re-normalize inside a source**. Implementations
  must be idempotent — the manager applies the policy more than once along a chain.

  Consequence: the `id` `get()` rewrites, and a `CiteFailure`'s `prefix`/`key`, are the **normalized**
  form — `doi:10.1103/physreva.86.052329` even if the caller typed mixed case. Only the cache *id* is
  folded; CSL payload fields (notably `DOI`) keep their casing. Trimming is `str::trim` only —
  internal whitespace is deliberately *not* collapsed, so `doi.rs`'s validation can still reject a
  malformed key instead of it being silently repaired.
- Per pass: dedup against `seen`, look each id up in the store, and bucket the ones due for a
  (re)fetch by prefix — misses, plus anything `TtlPolicy::should_refetch` returns true for (hard-
  expired always; soft-stale *probabilistically*, see Cache policy). A cached `Payload::Chained`
  entry we are **keeping** (fresh, or stale but the probabilistic draw said serve-as-is) pushes its
  target onto the worklist so the target is guaranteed present; one we are **refetching** does *not*
  (it is about to be replaced, and pre-pushing a superseded pointer would fetch — and report a
  failure for — a citation nobody requested). When a refetch fails and the grace window keeps the
  old chained record, the target is pushed from the failure path instead.
- Worklist items carry a **depth**; `max_chain_depth` (default 16, `with_max_chain_depth`) bounds
  `retrieve` as well as `get`, so retrieval never fetches links `get()` could not reach.
- Worklist items also carry an **origin** — the originally-requested `(prefix, key)` the item
  descends from (itself for a requested cite). Every push of a chain target (kept-pointer during
  bucketing, `Outcome::Chained` store arm, grace-served pointer in `note_failure`) inherits the
  current item's origin, so a multi-hop chain still points back to the root request. The dedup set
  (`seen`) records `(depth, origin)` per id; `store_resolutions` reads it back so any `CiteFailure`
  it builds is attributed correctly. First-writer-wins: a directly-requested cite is in the initial
  batch, so it is recorded before any chain could reach the same id — it is never mis-attributed.
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
`csl::merge_defaults` — **properties closer to the request win** during accumulation — until it hits
a `Payload::Concrete`. At the concrete node the accumulated `set_properties` are applied with
`csl::merge_over`, so they **override** the concrete target's colliding fields (matching both
reference implementations' `{ ...target, ...set_properties }`); the requested `id` is then forced
last, so a `set_properties` carrying an `id` can never win. Note `Source::chains_to()` exists but the
manager does not consume it; chain discovery is dynamic via the worklist.

**The chain key is lowercased; the CSL `DOI` field is not.** `arxiv.rs` emits
`Outcome::Chained { key: doi, .. }` **verbatim** — the manager then runs the target key through the
`doi` source's `normalize_key` (trim + lowercase) both when storing the pointer and when `get()`
walks it, so two entries whose DOIs differ only in case dedup to one `doi:` cache id. That id is an
*internal identifier*. The CSL field is a separate thing: it is the CSL-standard uppercase **`DOI`**
key holding the DOI **verbatim** (DOIs display in their registered mixed case), written that way by
`arxiv.rs`'s `build_csl` and by `doi.rs`'s `canonicalize_doi_key`. Don't "unify" the two —
lowercasing the field breaks CSL compliance, and case-preserving the cache key breaks dedup.

### Cache policy (`cache.rs`)

Two-tier expiry per record: `stale_after` (soft, `stale_percent` = 80% of TTL) and `expires` (hard).
The tiers are **not** a hard cutoff. `TtlPolicy::should_refetch` — the sole refetch decision the
manager consumes — is: `Fresh` (now < `stale_after`) never refetch, `Expired` (now ≥ `expires`)
always, and in the stale window `[stale_after, expires)` refetch only **probabilistically**, with a
probability that ramps from ~0 at `stale_after` to ~1 as `now` nears `expires`. The draw is a
deterministic FNV-1a hash of `(id, now)` (the same jitter family — no RNG, no ambient clock);
mixing `now` in re-rolls each `retrieve`, so an entry left alone now grows likelier to refetch as it
drifts toward `expires`. Consequence: an entry's **effective TTL is close to its full nominal TTL**
(arXiv's 10 d now really refetches near 10 d, not ~8.9 d), and a batch fetched together revalidates
spread out rather than all at once. `classify` (Fresh/Stale/Expired) is unchanged and still used by
`usable_within_grace`, `prune`, and the chained-target freshness check; only the refetch decision
went probabilistic. On top of that a `grace` window (14 days) during which a hard-expired entry is
still served **if the source is currently unreachable** (stale-while-revalidate — see `store_resolutions`'
`Outcome::Failed` arm →
`note_failure`). Grace applies **only to `Outcome::Failed`** (transport/5xx/unloadable-file — "try
again later"). An `Outcome::Missing` — a *reachable* source that authoritatively has no such key (an
id absent from a file that loaded fine or from a 200 API response, a doi.org 404) — is the opposite:
`note_missing` **always** reports it, never consults grace, and **removes** the stale cached entry so
`get()` stops serving now-known-wrong data (a removed entry may be some other citation's chain
target, whose `get()` then fails on the dead link — correct, the target really no longer resolves).
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
| `doi` | 1 / 1100 ms / 360 d | doi.org content negotiation returns CSL-JSON stored verbatim, values included — the only touch is `canonicalize_doi_key`, which renames a nonstandard lowercase `doi` key up to the CSL-standard uppercase `DOI` (doi.org already sends `DOI`, so it is normally a no-op) |
| `manual` | ∞ / 0 / **0** | key *is* the formatted text, stored under `_formatted_text`; TTL 0 ⇒ ephemeral (kept in the store's in-memory view for the run, **never persisted** to `citations.jsonl` or a sidecar, gone on restart — enforced by `FileCacheStore`, keyed on `stale_after == expires`, not on the prefix) |
| `bib` | ∞ / 0 / 60 s | file(s) fetched through the `Fetcher` (`file:` URLs), indexed by `id` |

arXiv version resolution: an explicitly-versioned key (`1211.1037v2`) resolves to that exact version
and is emitted **concrete, never chained**; a versionless key picks the best returned entry (a
versionless entry wins, else the highest `vN`) and chains to its DOI. DOI overrides are data
(`with_override_dois`): `Some(doi)` injects/replaces, `None` *suppresses* (keep arXiv metadata,
don't chain).

The CSL `issued` date is the entry's **`<updated>`** (last-revision date), falling back to
`<published>` when `<updated>` is absent — matching feedparser's `.date` alias and the **JS**
reference. This diverges from the Python reference, which uses the original `<published>` submission
date, so a paper's citation *year* may differ from Python's output. For a versioned request `issued`
is that specific version's date, not v1's. Only the per-entry `<updated>` is read (the feed-level
`<updated>` is ignored).

### Single-file cache (`filecache.rs` + `autocitefetch-std/src/store.rs`)

`FileCacheStore<Fs: CacheFs>` lives in the **core** crate (generic over an injected `CacheFs`); the
std crate supplies only real filesystem ops (`StdCacheFs`) and the `SingleFileCacheStore` wrapper.
Layout in one directory: `citations.jsonl` (header line 0 + one sorted entry per line — the only
file worth committing), per-writer `citations.<pid>-<nanos>.log` append logs (lock-free writes), and
`citations.lock`. `flush()` is the *only* operation that locks or rewrites the whole file: it folds
main + all sidecars, atomically replaces the main file, then reaps its own sidecar **plus any
crashed peer's**. It must never delete a *live* peer's: the compaction lock serializes compaction
against *compaction*, never against the lock-free `append`, so unlinking a live peer's log destroys
any write that landed after the fold read it (measured: ~300 of 400 acknowledged puts lost).

Live-vs-crashed is told apart by an **OS-advisory liveness lock**. On `open` a writer takes and
holds — for the store's whole lifetime — an exclusive `CacheFs::try_lock_exclusive` on a companion
file `citations.<writer>.log.lock` next to its sidecar; the kernel releases it if the process
crashes. When `flush` folds a peer's sidecar it tries that peer's companion lock: **held** (`Ok(None)`)
⇒ owner alive ⇒ fold read-only, never delete; **acquired** (`Ok(Some)`) ⇒ owner gone ⇒ delete the
log and its orphaned companion (the fold already captured its lines). The companion is a *separate*
file from the `.log` so reaping our own sidecar each flush never orphans the lock we hold — a writer
never `try_lock`s its own companion (self-deadlock) and deletes its own `.log` unconditionally. A
foreign `citations.*.log` with no companion is folded but never reaped, same as round 1. The stored
guard is `CacheFs::Guard` (an associated type, not `Box<dyn CacheGuard>`) so `FileCacheStore<StdCacheFs>`
stays `Send` — the concurrent regression test still moves stores across threads. A failed unlink
never fails the flush.

Merge rule (all exercised by unit tests): **last write wins over a deterministic total fold order** —
the main file first (the baseline written at the last compaction), then each sidecar in sorted name
order, and within a sidecar in line order (append order == that writer's time order). An entry line
inserts, a tombstone removes. So `put;remove` removes, a re-fetch with a *smaller* `expires` wins
(it is the newer write — the store does **not** keep the max-`expires` copy, which used to pin the
entry `Expired` forever), and a sidecar always beats the committed main-file copy. The one thing this
order cannot make exact is a **cross-writer** race — two live writers writing the same id
concurrently are ordered only by sidecar name — an accepted known limitation; each writer's own
sequence of ops is honored exactly. Torn-line tolerance applies to **sidecars only** — the main file
is written via fsync+rename and so can never be legitimately torn, so an unparseable line or an
unknown `{"schema":N}` header there is a hard `Err` rather than a silent skip (silently skipping then
rewriting turned recoverable corruption into permanent loss).

**Ephemeral (TTL-0) records are memory-only.** A record with no fresh window (`stale_after == expires`,
what a zero TTL produces — `is_ephemeral`) is kept in `mem` so a same-run `get` works, but is never
appended to a sidecar, never written into `citations.jsonl` (`serialize_main` skips it and `flush`
carries the in-memory copies forward across its own disk reload), and dropped rather than resurrected
when read back off an older on-disk file. This keeps `manual`-source citation text out of the
committed file while preserving the two-phase `retrieve`→`get` flow within a run. Keyed on the
timestamps, not the prefix.

## Invariants when editing

- **All I/O goes through `Fetcher`** — including local file reads (bib files, the arXiv DOI-override
  JSON). Sources must never touch the filesystem or network directly.
- **`retrieve_chunk` must return exactly one `Resolution` per requested key**. Pick the miss outcome
  by *why*: `Outcome::Failed` when the source was unreachable/erroring (grace-served if cached),
  `Outcome::Missing` when it is reachable and authoritatively lacks the key (always reported, drops
  the stale copy). The manager matches by `res.key`; an omitted key is silently neither stored nor
  reported.
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
key. Override `normalize_key` only if the default (trim + lowercase) is wrong for your key space —
and keep it idempotent. Users can also register their own source at runtime; nothing about the built-ins is privileged.

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
explicitly-versioned arXiv requests. Neither reference has TTL jitter, stale-while-revalidate, or
probabilistic stale revalidation (both refetch every entry the instant it goes soft-stale).
