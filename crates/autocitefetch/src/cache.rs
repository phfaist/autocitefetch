//! Cache TTL policy: soft/hard expiry, deterministic jitter, and the
//! freshness classification the manager uses to decide what to (re)fetch.

use core::time::Duration;

use crate::env::Timestamp;
use crate::store::CacheRecord;

/// How a cached entry stands relative to `now`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    /// Before the soft expiry — use as-is, no network needed.
    Fresh,
    /// Past soft but before hard expiry — usable, but revalidate if cheap.
    Stale,
    /// Past hard expiry — do not use unless kept alive by the grace window.
    Expired,
}

/// Policy governing how TTLs become concrete expiries.
#[derive(Clone, Copy, Debug)]
pub struct TtlPolicy {
    /// Soft expiry as a percentage of the full TTL (e.g. `80` ⇒ revalidate in
    /// the last 20% of the lifetime).
    pub stale_percent: u32,
    /// Maximum jitter applied to the hard TTL, as ± this percentage. Spreads
    /// out entries fetched together so they don't all expire at once.
    pub jitter_percent: u32,
    /// After hard expiry, keep serving a stale entry for this long *if* the
    /// source is unreachable (stale-while-revalidate grace window).
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

        let ttl_ms = ttl.as_millis() as i64;

        // Deterministic jitter in [-jitter_percent, +jitter_percent], seeded by
        // the id so identical entries are stable but distinct keys spread out.
        let jitter_ms = if self.jitter_percent == 0 {
            0
        } else {
            let span = ttl_ms.saturating_mul(self.jitter_percent as i64) / 100;
            if span == 0 {
                0
            } else {
                // fnv-1a over the id → signed offset in [-span, +span].
                let h = fnv1a(id.as_bytes());
                let magnitude = (h % (2 * span as u64 + 1)) as i64;
                magnitude - span
            }
        };

        let hard_ms = ttl_ms.saturating_add(jitter_ms).max(0);
        let soft_ms = hard_ms.saturating_mul(self.stale_percent.min(100) as i64) / 100;

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

    /// Whether an already-hard-expired record may still be served because the
    /// source is currently unreachable (within the grace window).
    pub fn usable_within_grace(&self, record: &CacheRecord, now: Timestamp) -> bool {
        now < record.expires.saturating_add(self.grace)
    }
}

/// FNV-1a 64-bit hash. Small, dependency-free, good enough to seed jitter.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
