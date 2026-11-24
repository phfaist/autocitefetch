# Changelog

## Unreleased

- **Breaking:** cache storage now comes from the `cachette` crate, which
  replaces autocitefetch's own JSONL store and is re-exported for hosts. The
  cache interface is synchronous, and reading a citation and pruning no longer
  need to be awaited; only retrieval stays async.
- **Breaking:** the cache now holds what each source returned. DOI overrides,
  the arXiv chain target and dropped fields are applied when a citation is
  read, so changing them takes effect at once, without a request and without
  touching the cache. A custom source that chains now does so when its items
  are read; the old way of returning a chained result from a fetch is gone.
- **Breaking:** new storage policy on the cache object: the bulky `reference`
  and `abstract` fields are now left out of the cache and the output by default
  (0.1.1 output them unless they were dropped), and can be kept with the CLI
  flags `--keep-references` and `--keep-abstract`. `--drop-field` now affects
  the output only.
- **Breaking:** the expiry policy of cached citations is now set on the cache
  instead of on the citation manager. A host that uses its own expiry policy
  now gives it with the cache's storage policy.
- Three ways to get a cache: the single JSONL file as before, any cachette
  store (for instance an in-memory store that a browser host imports and
  exports), or a session on a host program's cachette cache manager.
- **Breaking:** an arXiv DOI-override JSON file is now read once at setup, and a
  failure to read it is an error there instead of failing every arXiv
  citation.
- Cache files written by 0.1.1 are reused as they are: DOI entries and arXiv
  entries stored with full metadata keep their expiry times, and only the arXiv
  entries stored as pointers to a DOI are fetched again once, 100 per request.
  The file format is unchanged, so 0.1.1 can still read the files written now.

## 0.1.1 — 2026-09-29

- New: **Refresh batching** (`autocitefetch::batching`): per-source shaping of cache
  refresh work. Small amounts of stale work can be deferred (cached copies keep
  being served, within a bounded `max_defer`), and outgoing requests can be
  topped up with entries that are nearly due. 
- Added corresponding CLI flags for refresh batching (`--refresh-batching`)


## 0.1.0 — 2026-09-23

Initial release.
