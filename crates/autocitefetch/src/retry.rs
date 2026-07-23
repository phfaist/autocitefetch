//! Transparent HTTP retry/backoff: [`RetryingFetcher`] wraps any [`Fetcher`].
//!
//! It sits *between* a source and the host fetcher — sources call
//! `ctx.fetcher.fetch(..)` exactly as before and never learn that retries
//! happened. Retryable transport errors and retryable HTTP statuses (429, 500,
//! 502, 503, 504) are re-issued with exponential backoff plus deterministic
//! jitter; everything else (200, 404, other 4xx, non-retryable errors) passes
//! straight through untouched.
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
    /// backoff (still bounded by `cap`).
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

/// HTTP statuses worth retrying — matches [`FetchError::is_retryable`]'s
/// `Status` set so surfaced-as-error and surfaced-as-`Ok` responses behave the
/// same.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
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
    /// the parsed `Retry-After` header, if any.
    fn backoff(&self, url: &str, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let cap = self.policy.cap;

        // An explicit, parseable Retry-After wins over the computed backoff,
        // but is still bounded by `cap` so a hostile/huge value can't wedge us.
        if self.policy.honor_retry_after {
            if let Some(ra) = retry_after {
                return ra.min(cap);
            }
        }

        // Exponential: base * 2^attempt, saturating on overflow, capped.
        let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
        let backoff = self.policy.base.saturating_mul(factor).min(cap);

        // Deterministic jitter (no RNG — stay no_std): add up to ~25% of the
        // backoff, seeded by the url and attempt so it's stable per request but
        // spreads distinct urls / successive attempts apart.
        let backoff_ms = backoff.as_millis() as u64;
        let span = backoff_ms / 4 + 1;
        let jitter = seed(url, attempt) % span;

        let cap_ms = cap.as_millis() as u64;
        let total = backoff_ms.saturating_add(jitter).min(cap_ms);
        Duration::from_millis(total)
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
                            let ra = parse_retry_after(&resp);
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
