//! Cache TTL policy: soft/hard expiry, deterministic jitter, probabilistic
//! stale-window revalidation, and the freshness classification the manager
//! uses to decide what to (re)fetch.
//!
//! The two expiry tiers are not a hard/soft *cutoff*: in the stale window
//! `[stale_after, expires)` an entry is refetched only *probabilistically*
//! (see [`TtlPolicy::should_refetch`]), with a probability that ramps from ~0
//! at `stale_after` to ~1 as `now` nears `expires`. This spreads revalidation
//! out over the whole window instead of refetching every entry the instant it
//! goes soft-stale, so an entry's *effective* lifetime is close to its full
//! nominal TTL rather than `stale_percent`% of it — while still refreshing more
//! eagerly the closer the entry is to hard expiry. The draw is a deterministic
//! FNV-1a hash of `(id, now)` (no RNG, no ambient clock): mixing `now` in means
//! each `retrieve()` re-rolls, so an entry not refetched on one call may be on
//! the next as it drifts toward `expires`.

use core::time::Duration;

use crate::env::{Timestamp, millis_i64};
use crate::store::CacheRecord;

/// How a cached entry stands relative to `now`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    /// Before the soft expiry — use as-is, no network needed.
    Fresh,
    /// Past soft but before hard expiry. Still usable; the manager revalidates
    /// it only *probabilistically* (see [`TtlPolicy::should_refetch`]), with a
    /// probability that ramps from ~0 at `stale_after` to ~1 as `now` nears
    /// `expires`. So most stale entries are served without a refetch and an
    /// entry lives close to its full nominal TTL, not `stale_percent`% of it.
    Stale,
    /// Past hard expiry — refetch. The grace window does not make it fresh
    /// again; it only suppresses the *error* when the refetch fails (and keeps
    /// [`prune`](crate::manager::CitationManager::prune) from dropping it).
    Expired,
}

/// Policy governing how TTLs become concrete expiries.
#[derive(Clone, Copy, Debug)]
pub struct TtlPolicy {
    /// Soft expiry as a percentage of the *jittered* hard TTL (e.g. `80` ⇒
    /// revalidate in the last 20% of the lifetime). Clamped to `<= 100`.
    pub stale_percent: u32,
    /// Maximum jitter applied to the hard TTL, as ± this percentage. Spreads
    /// out entries fetched together so they don't all expire at once. Clamped
    /// to `<= 100` — a larger value could otherwise push the hard expiry
    /// *before* `now`, so entries would be born already expired.
    pub jitter_percent: u32,
    /// After hard expiry, keep *tolerating* a stale entry for this long when
    /// the source is unreachable: within the window a failed refetch is not
    /// reported and [`prune`](crate::manager::CitationManager::prune) leaves
    /// the entry alone (stale-while-revalidate).
    pub grace: Duration,
}

impl Default for TtlPolicy {
    fn default() -> Self {
        TtlPolicy {
            stale_percent: 80,
            jitter_percent: 15,
            grace: Duration::from_secs(14 * 24 * 60 * 60),
        }
    }
}

impl TtlPolicy {
    /// Build a [`CacheRecord`]'s expiry timestamps from `now`, the source TTL,
    /// and the entry id (which seeds the deterministic jitter).
    pub fn make_record(
        &self,
        payload: crate::store::Payload,
        now: Timestamp,
        ttl: Duration,
        id: &str,
    ) -> CacheRecord {
        // Zero TTL ⇒ ephemeral: already expired, never persisted usefully.
        if ttl.is_zero() {
            return CacheRecord {
                payload,
                stale_after: now,
                expires: now,
            };
        }

        // Saturating, *not* a truncating `as i64` cast: `Duration::MAX as i64`
        // is `-1`, which would make a "cache forever" TTL expire immediately
        // and could drive `span` negative (panicking in debug on the `2 * span`
        // below, wrapping into an absurd lifetime in release).
        let ttl_ms = millis_i64(ttl);

        // Deterministic jitter in [-jitter_percent, +jitter_percent], seeded by
        // the id so identical entries are stable but distinct keys spread out.
        // The percentage is clamped like `stale_percent`: beyond 100% the
        // negative half of the window would push `expires` before `now`.
        let jitter_percent = self.jitter_percent.min(100);
        let jitter_ms = if jitter_percent == 0 {
            0
        } else {
            let span = percent_of(ttl_ms, jitter_percent);
            if span <= 0 {
                0
            } else {
                // fnv-1a over the id → signed offset in [-span, +span].
                let h = fnv1a(id.as_bytes());
                let magnitude = (h % (2 * span as u64 + 1)) as i64;
                magnitude - span
            }
        };

        let hard_ms = ttl_ms.saturating_add(jitter_ms).max(0);
        let soft_ms = percent_of(hard_ms, self.stale_percent.min(100));

        CacheRecord {
            payload,
            stale_after: now.saturating_add(Duration::from_millis(soft_ms as u64)),
            expires: now.saturating_add(Duration::from_millis(hard_ms as u64)),
        }
    }

    /// Classify a record relative to `now`.
    pub fn classify(&self, record: &CacheRecord, now: Timestamp) -> Freshness {
        if now < record.stale_after {
            Freshness::Fresh
        } else if now < record.expires {
            Freshness::Stale
        } else {
            Freshness::Expired
        }
    }

    /// Whether the manager should (re)fetch this record now.
    ///
    /// * [`Fresh`](Freshness::Fresh) → `false` (serve as-is).
    /// * [`Expired`](Freshness::Expired) → `true` (always refetch).
    /// * [`Stale`](Freshness::Stale) → a *probabilistic* decision: refetch iff a
    ///   deterministic draw `r ∈ [0, 1)` derived from `(id, now)` is below the
    ///   stale fraction `f = (now − stale_after) / (expires − stale_after)`,
    ///   which ramps from ~0 at `stale_after` to ~1 as `now` nears `expires`.
    ///
    /// The draw is a pure function of `(id, now)` (an FNV-1a hash — the same
    /// family as the TTL jitter, no RNG and no ambient clock), so it is fully
    /// testable and stable within a single `retrieve` pass (which reads the
    /// clock once). Because `now` is mixed in, successive `retrieve` calls
    /// re-roll: an entry left alone now grows more likely to be refetched as it
    /// drifts toward hard expiry.
    ///
    /// A degenerate zero-width stale window (`stale_after == expires`, e.g. an
    /// ephemeral zero-TTL record) is never classified [`Stale`](Freshness::Stale)
    /// by [`classify`](Self::classify) — it is [`Fresh`](Freshness::Fresh)
    /// before that instant and [`Expired`](Freshness::Expired) at or after it —
    /// so it keeps today's always/never behavior and never reaches the draw.
    pub fn should_refetch(&self, record: &CacheRecord, now: Timestamp, id: &str) -> bool {
        match self.classify(record, now) {
            Freshness::Fresh => false,
            Freshness::Expired => true,
            Freshness::Stale => {
                // In this arm `classify` guarantees `stale_after <= now <
                // expires`, so the window is non-empty (`den > 0`) and the
                // fraction is in `[0, 1)`.
                let num = now
                    .as_millis()
                    .saturating_sub(record.stale_after.as_millis())
                    .max(0) as u128;
                let den = record
                    .expires
                    .as_millis()
                    .saturating_sub(record.stale_after.as_millis());
                if den <= 0 {
                    // Unreachable given the classification above; stay defensive
                    // rather than divide by zero — treat as fully due.
                    return true;
                }
                let den = den as u128;
                // Draw `r = draw / 2^64 ∈ [0, 1)`. Compare `r < f` without
                // floats or division: `r < num/den ⇔ draw * den < num * 2^64`.
                // Both products fit in `u128` for any `i64` timestamps (each
                // factor ≤ 2^64, so each product < 2^127).
                let draw = draw_hash(id, now) as u128;
                draw * den < num * (1u128 << 64)
            }
        }
    }

    /// Whether an already-hard-expired record is still *within* the grace
    /// window.
    ///
    /// This governs **error reporting and pruning**, not serving: the manager
    /// uses it to decide whether a failed refetch should be surfaced as a
    /// citation failure (`false` ⇒ report) and whether [`prune`] may drop the
    /// entry. [`get`] never consults it — a cached entry is served for as long
    /// as it is in the store.
    ///
    /// [`prune`]: crate::manager::CitationManager::prune
    /// [`get`]: crate::manager::CitationManager::get
    pub fn usable_within_grace(&self, record: &CacheRecord, now: Timestamp) -> bool {
        now < record.expires.saturating_add(self.grace)
    }
}

/// `value * percent / 100`, exact and clamped to `[0, i64::MAX]`.
///
/// Computed in `i128` so a huge `value` neither saturates the multiplication
/// (which would distort the ratio) nor goes negative.
fn percent_of(value: i64, percent: u32) -> i64 {
    let scaled = (value as i128 * percent as i128) / 100;
    scaled.clamp(0, i64::MAX as i128) as i64
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash. Small, dependency-free, good enough to seed jitter.
fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_continue(FNV_OFFSET, bytes)
}

/// One more FNV-1a round over `bytes`, continuing from a running `hash`.
fn fnv1a_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// A deterministic pseudo-random draw in `[0, 2^64)` for the stale-window
/// refetch coin, as a pure function of `(id, now)`: FNV-1a over the id, then
/// continued over the little-endian bytes of `now`. Mixing `now` in re-rolls
/// the draw on each `retrieve`, so an entry not refetched now becomes more
/// likely to be picked as it drifts toward hard expiry.
fn draw_hash(id: &str, now: Timestamp) -> u64 {
    let h = fnv1a(id.as_bytes());
    fnv1a_continue(h, &now.as_millis().to_le_bytes())
}
