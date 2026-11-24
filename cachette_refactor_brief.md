# Implementation brief: refactor autocitefetch around cachette

This brief is self-contained. It states what to build, the decisions that are
already taken, and how to check the result. Read `ARCH.md` first: it describes
the code as it is today.

## 1. Goal

Three outcomes, in one change:

1. **Storage comes from cachette.** autocitefetch's own JSONL store
   (`filecache.rs`) is deleted and replaced by the
   [`cachette`](https://crates.io/crates/cachette) crate, which is a corrected
   copy of it (see section 3).
2. **The cache holds what a source returned, not the prepared citation.** DOI
   overrides, chaining and dropped fields are applied when a citation is read,
   so changing them never changes the cache.
3. **The cache is pluggable.** autocitefetch stands alone with a JSONL file as
   today, accepts any cachette store, and plugs into a host program's
   cachette-managed cache. Files written by autocitefetch 0.1.1 keep working.

## 2. Decisions already taken

Do not reopen these; they were settled with the maintainer.

| # | Decision |
|---|---|
| D1 | A cached record is one **CSL-JSON item per citation id**: the source's answer for that key. The arXiv source parses its Atom entry into CSL before caching, as today. |
| D2 | **Per-run options never touch the cache**: arXiv DOI overrides, the chain target (`chain_dois_to`), and `with_dropped_csl_fields`. They apply on read. Chain pointers are no longer stored; they are derived on read. |
| D3 | A **storage policy lives on the cache object**: the TTL policy plus two flags, `keep_reference` and `keep_abstract`, both `false` by default. The omitted fields are removed before an item is stored, and again on read. |
| D4 | The cache seam is a **synchronous** trait. cachette's stores are synchronous, and a browser host loads a `MemoryStore` before the run and saves it after. |
| D5 | Three ways to get a cache: a standalone JSONL file (default), any `cachette::Store`, or a session on a host's `cachette::Manager`. |
| D6 | `ArxivSource::with_override_dois_file` is replaced by a **setup-time loader**. Overrides reach the source as data only. |
| D7 | **0.1.1 cache files are reused without a conversion step.** The on-disk record shape stays what 0.1.1 writes for a concrete entry. |
| D8 | `CitationManager::get`, `get_by_id` and `prune` become synchronous. `retrieve` stays async. |

## 3. Background on cachette

- Crate `cachette` 0.1.0 on crates.io; source at
  <https://github.com/phfaist/cachette>. MSRV 1.86, same as this workspace.
- **Store layer** (`no_std` with `default-features = false`): the trait
  `Store` (synchronous, `&mut self`), the stores `JsonlStore`, `MemoryStore`,
  `FilesStore`, and the wrapper `WithEphemeral`. Also `TtlPolicy`, `Freshness`,
  `Expiry`, `PruneRule`, `Clock`, `Timestamp`, `Record`, `Fields`.
- **Manager layer** (feature `std`): `Manager`, `ValuesCacheDeclaration`,
  `InstanceKey`, `ValuesSession`. A session's operations never return errors;
  failures go to the host's cachette `Reporter`.
- `JsonlStore` is copied from this repository's `FileCacheStore`, and
  `TtlPolicy`/`Freshness` from `cache.rs`. The file layout is the same:
  `BASE.jsonl`, `._BASE.WRITER.log`, `._BASE.WRITER.log.lock`, `._BASE.lock`.
- The fixes that cachette made to the copied code are listed in the module
  documentation of its `engine/jsonl.rs` ("Origin of the code and changes from
  the source") and `lifetime.rs`. The main ones: two races in which a merge
  could delete the log of a running writer, errors that were silently ignored,
  a main file with one unreadable line no longer fails to open, and two edge
  cases of the expiry computation.

Checked while preparing this brief:

- A cache written by autocitefetch 0.1.1, including a leftover writer log, opens
  with `cachette::JsonlStore::open_path(dir, "citations")`. Records and expiry
  times read back, and a merge leaves the legacy lines byte-identical.
- `cachette` builds with `--no-default-features` for
  `wasm32-unknown-unknown`.

## 4. Target design

### 4.1 Dependencies

- Workspace: `cachette = { version = "0.1", default-features = false }`.
- `autocitefetch` depends on it as is. Add `pub use cachette;` to the crate
  root so that hosts use the matching version.
- `autocitefetch-std` enables `features = ["std"]` and drops `fs4`.

**Known consequence.** cachette's `std` feature enables `serde/std` and
`serde_json/std`. Today no crate in the workspace enables `serde_json/std`, so
a whole-workspace build compiles the core against alloc-only `serde_json`.
After this change it will not: feature unification turns `std` on for
`cargo build --workspace` and `cargo test --workspace`. The `no_std` promise is
then checked only by the per-package commands. Therefore:

- keep `cargo build -p autocitefetch --target wasm32-unknown-unknown` in CI;
- add `cargo test -p autocitefetch --locked` to CI, which still resolves
  alloc-only features;
- correct the comments that state otherwise (`crates/autocitefetch/Cargo.toml`,
  and the CLI section of `ARCH.md`).

### 4.2 Shared types

Delete autocitefetch's copies and re-export cachette's under the same names:

| autocitefetch item | Becomes |
|---|---|
| `Timestamp`, `Clock` (`env.rs`) | `cachette::{Timestamp, Clock}`. `Timer` stays in `env.rs`. |
| `TtlPolicy`, `Freshness` (`cache.rs`) | `cachette::{TtlPolicy, Freshness}` |
| `StoreError` (`store.rs`) | `cachette::StoreError`; `Error::Store` wraps it |
| `autocitefetch_std::SystemClock` | `cachette::SystemClock` (made with `SystemClock::new()`) |

Method changes on `TtlPolicy`: the methods take an `&Expiry` where they took a
`&CacheRecord`; `should_refetch` is named `should_refresh`; `make_record` is
replaced by `expiry(now, ttl, key)`. cachette documents two edge cases where
its expiry times differ from the old ones (`lifetime.rs`, module header);
adapt any test that asserts the old values.

### 4.3 The cache seam (`crates/autocitefetch/src/store.rs`)

```rust
/// How a cache stores items. Everything that shapes cache contents is here.
#[derive(Clone, Copy, Debug, Default)]
pub struct StoragePolicy {
    pub ttl: TtlPolicy,
    /// Keep the CSL field `reference` (a work's own bibliography).
    pub keep_reference: bool,
    /// Keep the CSL field `abstract`.
    pub keep_abstract: bool,
}

pub struct CachedItem {
    pub item: CslValue,
    pub expiry: Expiry,
}

pub trait CacheStore {
    fn policy(&self) -> StoragePolicy;
    fn get(&self, id: &str) -> Result<Option<CachedItem>, StoreError>;
    fn put(&self, id: &str, item: &CslValue, ttl: Duration, now: Timestamp)
        -> Result<(), StoreError>;
    fn remove(&self, id: &str) -> Result<(), StoreError>;
    fn flush(&self) -> Result<(), StoreError>;
    /// Removes the items past their grace period; answers how many.
    fn prune(&self, now: Timestamp) -> Result<usize, StoreError>;
}
```

Rules:

- Methods take `&self`; implementations use interior mutability, as today.
  Every call is synchronous, so no borrow is ever held across an `.await`.
- The id is the canonical `"prefix:key"`, unchanged.
- A `ttl` of zero stores an **ephemeral** item: readable for the life of the
  cache object, never written to disk. This is what the `manual` source needs.
- **Division of work.** The manager removes the omitted fields before `put` and
  on read, using `policy()`. An implementation only stores what it is given.

### 4.4 Record encoding

The JSON text of a stored record is `{"concrete": <item>}`, exactly what 0.1.1
writes for `Payload::Concrete`. Put two public helpers in `store.rs`, used by
both implementations below:

- `encode_item(&CslValue) -> Result<Record, StoreError>`
- `decode_item(&Record) -> Option<CslValue>`, which answers:
  - the item for `{"concrete": X}` when `X` is a JSON object;
  - `None` for `{"chained": …}`, a pointer written by 0.1.1;
  - `None` for anything else.

A record that decodes to `None` is reported by `get` as absent (`Ok(None)`).
The manager then fetches the citation again, and the `put` replaces the record.

Do not stamp `id` into a stored item. The manager sets it on read.

### 4.5 `StoreCache<S: cachette::Store>` (core)

The implementation of `CacheStore` over any cachette store.

- Holds `RefCell<WithEphemeral<S>>` and a `StoragePolicy`.
- API: `new(store)`, `with_policy(policy)`, `into_inner()`, and
  `with_store(|store| …)` for tests and for a host that calls
  `cachette::export`.
- `put` stores `Fields { expiry: Some(policy.ttl.expiry(now, ttl, id)), ..Default::default() }`.
  Leave `written`, `used` and `cost` unset: lines then keep the 0.1.1 shape, and
  a file under version control changes only when an entry is fetched again. For
  the same reason, never call `record_uses`.
- `get` answers `Ok(None)` for a value without expiry times and for an error of
  the kind `StoreErrorKind::DamagedValue`.
- `prune` calls `Store::prune(PruneRule::Volatile { grace: policy.ttl.grace }, now)`
  and answers `count`.

### 4.6 std backends (`crates/autocitefetch-std`)

**`SingleFileCacheStore`** keeps its name and its defaults, and becomes a thin
wrapper around `StoreCache<JsonlStore<StdFs>>`:

- `new(dir)` (base `citations`) and `with_base(dir, base)`, now synchronous;
- `with_policy(policy)`;
- the files are the same as today, in the same place.

**`SessionCache`** is new. It implements `CacheStore` on a session of a host's
`cachette::Manager`:

- `SessionCache::declaration(&policy) -> ValuesCacheDeclaration`: name
  `autocitefetch.items`, version 1, `Engine::Jsonl`, no blobs,
  `ValueSort::Volatile { lifetime: 30 days, policy: policy.ttl }`, a default
  cost of 1000 ms, no migration.
- `SessionCache::open(&manager, &instance, policy)`, where `instance` is any
  `Serialize` value chosen by the host. The instance key is made from a struct
  that holds the host's value **and the two `keep_*` flags**, so that caches
  with different flags are never mixed.
- `put` calls `session.put` with `PutOptions { lifetime: Some(ttl), cost: None }`.
  The session computes the expiry times itself, with the manager's clock and
  the declaration's TTL policy.
- **Write-through memory.** Keep every `put` and `remove` of this run in a map,
  and answer `get` from it first. A session can be without a cache
  (`Manager::none()`, or a failure that the manager reported); `retrieve`
  followed by `get` must work all the same.
- `flush` does nothing: the session makes its writes durable when it is
  dropped. `prune` answers `Ok(0)`: the host's `Manager::maintain` prunes.

### 4.7 Sources: a read-time step

Keep `retrieve_chunk`, `Resolution` and `Outcome`. Remove `Outcome::Chained`
and `Resolution::chained`. Add to `source/mod.rs`:

```rust
pub enum Interpreted {
    Concrete(CslValue),
    Chained { prefix: String, key: String, set_properties: CslValue },
}

pub trait Source {
    // … unchanged methods …

    /// Turns a cached item into what the citation resolves to, by applying
    /// this source's configuration. Pure and synchronous: no I/O, no clock.
    fn interpret(&self, key: &str, item: CslValue) -> Interpreted {
        Interpreted::Concrete(item)
    }
}
```

The contract, to state in the documentation of the trait:

- `Outcome::Concrete` is **the item that is cached**. It must not depend on the
  per-run configuration of the source.
- `interpret` is called at every read of a cached item, possibly several times
  in one run. It must give the same answer for the same key, item and
  configuration, and must accept every item that this source has cached,
  including items cached by 0.1.1.

### 4.8 Built-in sources

**`arxiv`**

- `retrieve_chunk`: the request, the parsing and the choice of the entry for
  each key are unchanged. Every key that is found resolves to
  `Outcome::Concrete` with `build_csl(entry, feed_doi)`, where `feed_doi` is the
  DOI of the feed (blank counts as none). It consults no override and takes no
  chaining decision. `Failed` and `Missing` are unchanged.
- `interpret(key, item)`:
  1. split the version off the key (`split_version`);
  2. compute the effective DOI with the existing `effective_doi`, from the
     overrides and the item's `DOI`;
  3. write it back: set `DOI`, or remove it when there is none;
  4. when the key has no version, a chain prefix is configured and there is a
     DOI, answer `Chained` to that prefix, with the DOI verbatim as the key and
     `set_properties = {"arxivid": <base id>}`; otherwise answer `Concrete`.
- Remove `with_override_dois_file`. Add
  `load_override_dois(fetcher: &dyn Fetcher, url: &str)`, an async function
  with the body of today's `load_override_file` (same JSON shape, same
  messages), which answers pairs that `with_override_dois` accepts. Document
  that a later `with_override_dois` call wins, that the host supplies a
  `RetryingFetcher` if it wants retries, and that a failure is now an error at
  setup where it used to fail every arXiv key.

**`doi`**, **`bib`**, **`manual`**: `retrieve_chunk` unchanged, default
`interpret`. `canonicalize_doi_key` stays in the fetch step, since it depends on
no option.

### 4.9 The manager (`manager.rs`, `batching.rs`)

- `S: CacheStore` is the new trait. Remove the `policy` field and
  `with_policy`: the TTL policy is `self.store.policy().ttl`.
- `batching::classify` takes an `&Expiry`.

**In `retrieve`:**

- *Classification.* For a cached item that is not `Required`, call
  `interpret`; a `Chained` answer holds its target as a kept pointer does
  today. If the answer chains to the item's own id, report a failure.
- *Storing a `Concrete` outcome*, in this order:
  1. an item that is not a JSON object is a failure, and nothing is stored;
  2. remove the fields that the storage policy omits (top-level keys);
  3. `put` the item, with the outcome's TTL or the source's default;
  4. `interpret` it: `Concrete` is reported as resolved; `Chained` has its
     target key normalized and pushed on the worklist, as today. A chain to
     itself removes the item and is a failure.
- *A failed refresh within the grace period* interprets the kept item, and
  pushes its target when it chains.
- Dropped fields are no longer removed here, and no `id` is set here.

**In `get`** (now synchronous), for each link:

1. read the item; the errors for an absent item are unchanged;
2. `interpret` it with the source registered for that link's prefix; with no
   such source, treat it as `Concrete`;
3. `Chained`: accumulate `set_properties` and follow, as today;
4. `Concrete`: merge the accumulated properties over the item, remove the
   fields that the storage policy omits, remove the dropped fields, set the
   requested `id` last.

Removing fields after the merge means that `set_properties` cannot bring one
back, so the separate treatment of pointers disappears.

**`prune`** (now synchronous) calls `self.store.prune(now)` and then `flush`,
with the same `Wait::CacheFlush` events.

Update the documentation of `with_dropped_csl_fields`: it now affects output
only.

### 4.10 CLI

- Build the cache and call `get` without `block_on`.
- New flags `--keep-abstract` and `--keep-references`, which set the storage
  policy.
- `--drop-field`: rewrite the help text. It removes fields from the output and
  no longer changes `.citations.jsonl`.
- The default location `.citations.jsonl` is unchanged, so an existing file is
  found and reused.

## 5. Files written by 0.1.1

No conversion code. Two facts make reuse work:

- cachette's JSONL engine reads the files as they are (checked, see section 3);
- `decode_item` reads the two record shapes of 0.1.1 (section 4.4).

What a user sees on the first run with an old file:

- **DOI entries are reused**, with their expiry times. They are the costly
  ones: one request per entry, about one per second.
- **arXiv pointers are fetched again**, 100 ids per request. Their arXiv
  metadata was never stored.

Details to keep in mind, none of which needs code:

- Old items carry an `id`, and may carry `reference` or `abstract`. The read
  path handles both. The file shrinks as entries are fetched again; there is no
  eager rewrite.
- An old arXiv entry stored with full metadata carries the DOI as it was after
  the overrides of the time. It is corrected at its next refresh, within about
  10 days.
- The record shape is unchanged, so 0.1.1 can still read a file that the new
  version wrote. It reads the new arXiv items as plain arXiv metadata.

Fixture for the tests, as 0.1.1 writes it (a main file `citations.jsonl`):

```text
{"schema":1}
{"id":"arxiv:1211.1037","rec":{"payload":{"chained":{"prefix":"doi","key":"10.1103/physreva.86.052329","set_properties":{"arxivid":"1211.1037"}}},"stale_after":1700783584969,"expires":1700979481212}}
{"id":"doi:10.1103/physreva.86.052329","rec":{"payload":{"concrete":{"DOI":"10.1103/PhysRevA.86.052329","author":[{"family":"Faist","given":"Philippe"}],"id":"doi:10.1103/physreva.86.052329","title":"A & B <c>"}},"stale_after":1722610890625,"expires":1728263613282}}
```

## 6. What is deleted

- `crates/autocitefetch/src/filecache.rs`, whole.
- The implementations in `cache.rs` and `env.rs` that section 4.2 replaces.
- `CacheRecord`, the public `Payload`, and the async `CacheStore` in `store.rs`.
- `StdCacheFs`, `StdGuard` and the atomic replacement in
  `crates/autocitefetch-std/src/store.rs`; `clock.rs`; the dependency `fs4`.
- `crates/autocitefetch-std/tests/filecache_std.rs`. The storage engine is now
  tested in cachette; keep only tests of autocitefetch's own wrapper.
- From the crate root: the re-exports `CacheFs`, `CacheGuard`, `FileCacheStore`,
  `FsError`, `CacheRecord`, `Payload`. Add `CachedItem`, `StoragePolicy`,
  `StoreCache`, `Interpreted`.

## 7. Tests

Conventions stay as `ARCH.md` states them: no async runtime, a local
`block_on` with `Waker::noop()`, mocks duplicated per test file, no network.

**Migration of the existing tests**

- The `MemStore` mock of each test file is replaced by
  `StoreCache::new(cachette::MemoryStore::new())`.
- Tests that inspect `Payload` values (`cache_policy.rs`,
  `manager_contract.rs`, `reporting.rs`) are rewritten against `CachedItem` and
  against the behavior of `get`.
- Test sources that return `Resolution::chained` implement `interpret`.
- Tests of `with_override_dois_file` (`arxiv_dois.rs`) move to
  `load_override_dois` plus `with_override_dois`.

**New tests**

1. *Options do not change the cache.* Retrieve with one set of overrides, chain
   target and dropped fields; record the cache contents. Change all three and
   retrieve again with a fetcher that fails every request for cached keys: the
   contents are identical, and the output of `get` follows the new options.
2. *A changed override* fetches the new DOI target only, and no arXiv entry.
3. *Chaining switched off* answers arXiv metadata from the cache, without a
   request.
4. *Storage policy.* By default, `reference` and `abstract` are in neither the
   stored item nor the output. With a `keep_*` flag, the field is in both.
   Turning a flag off hides the field at once, for items stored before.
5. *Old files.* With the fixture of section 5 and a fresh clock: `get` of the
   DOI works without a request; `retrieve` of the arXiv id makes one arXiv
   request and no DOI request; the pointer line is then replaced.
6. *An unreadable record* (neither shape) is fetched again and replaced.
7. *Ephemeral items* (`manual`) are readable after `retrieve` and are in no
   file.
8. *`SessionCache`*: `retrieve` then `get` on a manager in a temporary folder;
   the same with `Manager::none()`; two policies with different flags open
   different instances.

## 8. Documentation

- `ARCH.md`: the workspace layout, "Cache files", "Single-file cache", the
  design-decision table, the bullet on dropped fields in "The retrieval loop",
  "Chaining", "Cache policy", the sources table, and the invariants. The
  rationale about locks and merges now belongs to cachette; link to it instead
  of keeping a copy.
- `README.md` and the crate READMEs: the three ways to get a cache, and the
  browser pattern (`MemoryStore`, `cachette::import` before, `cachette::export`
  after).
- Module documentation of `store.rs`, `source/mod.rs`, `source/arxiv.rs`,
  `manager.rs`.
- `examples/resolve.rs`.
- `CHANGELOG.md`: one bullet of two or three sentences per significant change,
  without code details (rule of `CLAUDE.md`).

## 9. Suggested order

Each step leaves the workspace building and the tests passing.

1. Add the dependency; replace the shared types (section 4.2). Mechanical.
2. Introduce the new `CacheStore`, `StoreCache` and the new
   `SingleFileCacheStore`; delete `filecache.rs`. Stored records still hold
   what they hold today.
3. Add `Source::interpret`; move overrides and chaining to it; make `get` and
   `prune` synchronous; stop storing pointers.
4. Add the storage policy and the CLI flags; move dropped fields to the read
   path.
5. Add `SessionCache`.
6. Add the tests of old files; update the documentation and the changelog.

## 10. Done when

- These pass, as `CLAUDE.md` lists them:
  - `cargo test`
  - `cargo clippy --workspace --all-targets`, without warnings
  - `cargo doc --workspace --no-deps`, without warnings
  - `cargo build -p autocitefetch --target wasm32-unknown-unknown`
  - `cargo build -p autocitefetch-std --no-default-features`
  - `cargo test -p autocitefetch` (new in CI, see section 4.1)
- The new tests of section 7 exist and pass.
- No code of the storage engine remains in this repository.
- Do not run `cargo fmt`.

## 11. Out of scope

- The version number and the release. The API changes are breaking; the
  maintainer decides the bump.
- Any change to cachette. If its `std` feature should stop enabling
  `serde_json/std` (section 4.1), that is a request to raise there.
- A WASM backend crate, and an async store.
- An eager rewrite of old files, and a per-entry mark that would refetch
  entries when a `keep_*` flag is turned on. Turning a flag on takes effect as
  entries are fetched again; deleting the cache forces it.
- Seeding a host-managed cache from an old file with `Manager::import`. It
  should work, since the records are readable as they are, but it is untested;
  do not document it without a test.
