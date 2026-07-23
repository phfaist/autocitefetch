//! Transparent HTTP retry/backoff: [`RetryingFetcher`] wraps any [`Fetcher`].
//!
//! It sits *between* a source and the host fetcher — sources call
//! `ctx.fetcher.fetch(..)` exactly as before and never learn that retries
//! happened. Retryable transport errors and retryable HTTP statuses (429, 500,
//! 502, 503, 504) are re-issued with exponential backoff plus deterministic
//! jitter; everything else (200, 404, other 4xx, non-retryable errors) passes
//! straight through untouched.
//!
//! The retryable *status* set is shared with [`FetchError::is_retryable`], so a
//! fetcher that surfaces a 503 as `Ok(Response)` and one that surfaces it as
//! `Err(FetchError::Status(503))` are retried the same number of times. They
//! are not fully interchangeable, though: `FetchError::Status` carries no
//! headers, so only the `Ok` path can honour `Retry-After` — a host that turns
//! non-2xx into `Err` gives up that signal.
//!
//! Stays `no_std`: exponential backoff, a `Retry-After` seconds parser, and an
//! FNV-1a-seeded jitter replace any RNG or clock.

use alloc::boxed::Box;
use core::time::Duration;

use crate::env::Timer;
use crate::fetch::{FetchError, Fetcher, Request, Response};
use crate::BoxFuture;

/// How the [`RetryingFetcher`] backs off and how many times it retries.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Maximum number of *retries* after the initial attempt (so the total
    /// number of attempts is `max_retries + 1`).
    pub max_retries: u32,
    /// Base delay for the first retry; doubles each subsequent retry.
    pub base: Duration,
    /// Upper bound on any single backoff delay.
    pub cap: Duration,
    /// Whether a numeric `Retry-After` response header overrides the computed
    /// backoff (clamped to `[base, cap]`, so neither a hostile huge value nor a
    /// `Retry-After: 0` hot loop gets through).
    pub honor_retry_after: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: 5,
            base: Duration::from_millis(500),
            cap: Duration::from_secs(30),
            honor_retry_after: true,
        }
    }
}

/// Whether a status surfaced as `Ok(Response)` is worth retrying.
///
/// Delegates to [`FetchError::is_retryable`] so the two status sets cannot
/// drift apart. Note the two paths are *not* fully equivalent: only this one
/// can honour `Retry-After`, because [`FetchError::Status`] carries no headers.
fn is_retryable_status(status: u16) -> bool {
    FetchError::Status(status).is_retryable()
}

/// A [`Fetcher`] wrapper that transparently retries retryable failures.
///
/// Holds only shared borrows of the inner fetcher and the timer, so it can be
/// built as a short-lived local that shadows a real fetcher for one retrieval
/// pass without taking ownership.
pub struct RetryingFetcher<'a> {
    inner: &'a dyn Fetcher,
    timer: &'a dyn Timer,
    policy: RetryPolicy,
}

impl<'a> RetryingFetcher<'a> {
    /// Wrap `inner`, sleeping via `timer`, using `policy` for backoff.
    pub fn new(inner: &'a dyn Fetcher, timer: &'a dyn Timer, policy: RetryPolicy) -> Self {
        RetryingFetcher {
            inner,
            timer,
            policy,
        }
    }

    /// The delay to wait before the retry that follows `attempt` (0-based:
    /// `attempt == 0` is the delay after the very first try). `retry_after` is
    /// the parsed `Retry-After` header — the caller only passes it when
    /// [`RetryPolicy::honor_retry_after`] is set.
    fn backoff(&self, url: &str, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let cap_ms = millis_u64(self.policy.cap);
        let base_ms = millis_u64(self.policy.base);

        // An explicit, parseable Retry-After wins over the computed backoff,
        // but is bounded by `cap` (so a hostile/huge value can't wedge us) and
        // floored at `base` (so `Retry-After: 0` can't turn the retry loop into
        // a hot loop hammering the server with zero delay).
        if let Some(ra) = retry_after {
            let ra_ms = millis_u64(ra).min(cap_ms).max(base_ms.min(cap_ms));
            return Duration::from_millis(ra_ms);
        }

        // Exponential: base * 2^attempt, saturating on overflow, capped.
        let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let exp_ms = base_ms.saturating_mul(factor).min(cap_ms);

        // Deterministic jitter (no RNG — stay no_std): up to ~25% of the
        // exponential term, seeded by the url and attempt so it's stable per
        // request but spreads distinct urls / successive attempts apart.
        //
        // The jitter is added *below* the cap rather than clamped against it:
        // capping the sum would silently erase the jitter exactly when it
        // matters most (a sustained outage, where every client has reached
        // `cap` and would otherwise retry in lockstep on the same cadence).
        let span = exp_ms / 4 + 1;
        let ceiling = cap_ms.saturating_sub(span - 1);
        let backoff_ms = exp_ms.min(ceiling);
        let jitter = seed(url, attempt) % span;

        Duration::from_millis(backoff_ms.saturating_add(jitter).min(cap_ms))
    }
}

impl Fetcher for RetryingFetcher<'_> {
    fn fetch(&self, req: Request) -> BoxFuture<'_, core::result::Result<Response, FetchError>> {
        Box::pin(async move {
            let mut attempt: u32 = 0;
            loop {
                match self.inner.fetch(req.clone()).await {
                    Ok(resp) => {
                        if attempt < self.policy.max_retries && is_retryable_status(resp.status) {
                            let ra = if self.policy.honor_retry_after {
                                parse_retry_after(&resp)
                            } else {
                                None
                            };
                            let delay = self.backoff(&req.url, attempt, ra);
                            self.timer.sleep(delay).await;
                            attempt += 1;
                            continue;
                        }
                        // 200, 404, other 4xx, or retries exhausted → passthrough.
                        return Ok(resp);
                    }
                    Err(e) => {
                        if attempt < self.policy.max_retries && e.is_retryable() {
                            let delay = self.backoff(&req.url, attempt, None);
                            self.timer.sleep(delay).await;
                            attempt += 1;
                            continue;
                        }
                        // Non-retryable, or retries exhausted → surface the error.
                        return Err(e);
                    }
                }
            }
        })
    }
}

/// A [`Duration`] as whole milliseconds, saturating at [`u64::MAX`] (a plain
/// `as u64` cast of the `u128` would truncate).
fn millis_u64(dur: Duration) -> u64 {
    u64::try_from(dur.as_millis()).unwrap_or(u64::MAX)
}

/// Parse a numeric `Retry-After` header (delay in whole seconds). The HTTP-date
/// form is ignored (we have no clock here) → `None`.
fn parse_retry_after(resp: &Response) -> Option<Duration> {
    let raw = resp.header("retry-after")?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

/// FNV-1a 64-bit over the url and attempt — a dependency-free deterministic
/// seed for jitter (mirrors the cache module's `fnv1a` idea).
fn seed(url: &str, attempt: u32) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in url.as_bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for &b in &attempt.to_le_bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
