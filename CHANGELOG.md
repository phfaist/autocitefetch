# Changelog

## 0.1.1 — 2026-09-29

- New: **Refresh batching** (`autocitefetch::batching`): per-source shaping of cache
  refresh work. Small amounts of stale work can be deferred (cached copies keep
  being served, within a bounded `max_defer`), and outgoing requests can be
  topped up with entries that are nearly due. 
- Added corresponding CLI flags for refresh batching (`--refresh-batching`)


## 0.1.0 — 2026-09-23

Initial release.
