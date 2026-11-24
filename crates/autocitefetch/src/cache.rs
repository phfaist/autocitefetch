//! Cache TTL policy: soft/hard expiry, deterministic jitter, probabilistic
//! stale-window revalidation, and the freshness classification the manager
//! uses to decide what to (re)fetch.
//!
//! The implementation is cachette's ([`cachette::TtlPolicy`],
//! [`cachette::Freshness`]), which began as a copy of this module; it is
//! re-exported here under the same names. Its methods take the
//! [`Expiry`](cachette::Expiry) of a value and its key.
//!
//! The two expiry tiers are not a hard/soft *cutoff*: in the stale window
//! `[stale_after, expires)` an entry is refetched only *probabilistically*
//! (see [`TtlPolicy::should_refresh`]), with a probability that ramps from ~0
//! at `stale_after` to ~1 as `now` nears `expires`. This spreads revalidation
//! out over the whole window instead of refetching every entry the instant it
//! goes soft-stale, so an entry's *effective* lifetime is close to its full
//! nominal TTL. The draw is a deterministic FNV-1a hash of `(id, now)` (no RNG,
//! no ambient clock): mixing `now` in means each `retrieve()` re-rolls.
//!
//! The policy in force is the one of the cache:
//! [`StoragePolicy::ttl`](crate::store::StoragePolicy::ttl).

pub use cachette::{Freshness, TtlPolicy};
