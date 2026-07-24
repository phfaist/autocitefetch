//! The per-source retrieval driver: chunking + rate limiting.
//!
//! Splits a source's key list into `chunk_size` batches and calls
//! `retrieve_chunk` on each, spacing the calls out by the source's
//! `min_interval` via the host [`Timer`](crate::env::Timer). This is *rate
//! limiting* (be polite to arXiv/doi.org), not failure backoff — retrying a
//! failed request is [`retry`](crate::retry)'s job, and it is that module which
//! avoids the reference implementations' `sleep` no-op bug.
//!
//! Two properties matter and are easy to get wrong:
//!
//! * **Pacing is start→start, not gap-based.** A source declaring 1100 ms means
//!   at most one request per 1100 ms, so the wait before a chunk is
//!   `min_interval - (now - previous chunk start)`; sleeping a full interval
//!   *after* the previous chunk returned would space requests by
//!   `interval + request latency` instead. (The JS reference does the same
//!   subtraction; `base.js` measures elapsed time around the request.)
//! * **Pacing survives across retrieval passes.** The manager calls
//!   `drive_source` once per pass per prefix, and the arXiv→DOI chain
//!   guarantees a second pass reaching the `doi` source. `last_start` is
//!   therefore threaded in by the caller and handed back out, so pass 2 does
//!   not fire a request the instant pass 1 finished.
//!
//! This is also where per-source *progress* is announced
//! ([`report`](crate::report)): a chunk boundary is the only place the core can
//! see forward motion within a pass, and doing it here means a source gets
//! progress reporting without implementing anything.

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::env::{Timestamp, millis_i64};
use crate::report::{Event, Wait};
use crate::source::{Resolution, RetrieveCtx, Source};

/// Drive one source over all its requested keys, returning every resolution.
///
/// `last_start` is when this source's most recent chunk was *started* (`None`
/// if it has not run yet in this retrieval). The returned `Option<Timestamp>`
/// is the updated value and must be fed back in on the next call for the same
/// source, otherwise rate limiting resets on every pass.
pub async fn drive_source(
    source: &dyn Source,
    keys: Vec<String>,
    ctx: &RetrieveCtx<'_>,
    last_start: Option<Timestamp>,
) -> (Vec<Resolution>, Option<Timestamp>) {
    let chunk_size = source.chunk_size().max(1);
    let interval_ms = millis_i64(source.min_interval());
    let total = keys.len();

    // Progress is emitted from *here* rather than from the source, so a
    // third-party `Source` is instrumented without cooperating — and rather than
    // from the manager's `store_resolutions`, which runs serially after every
    // source in the pass has returned and so would make a progress bar jump from
    // 0% straight to 100%.
    ctx.reporter.report(&Event::SourceStarted {
        prefix: ctx.prefix,
        keys: total,
        chunks: total.div_ceil(chunk_size),
    });

    let mut out = Vec::with_capacity(total);
    let mut last_start = last_start;

    // `chunks` borrows; collect each batch into an owned Vec for the source.
    let mut start = 0;
    while start < keys.len() {
        let end = start.saturating_add(chunk_size).min(keys.len());
        let batch: Vec<_> = keys[start..end].to_vec();
        start = end;

        if interval_ms > 0 {
            if let Some(prev) = last_start {
                let elapsed = ctx.clock.now().as_millis().saturating_sub(prev.as_millis());
                let remaining = interval_ms.saturating_sub(elapsed.max(0));
                if remaining > 0 {
                    // Announce the pacing gap before sleeping through it: this
                    // is one of the two places the tool goes quiet for seconds
                    // at a time and looks hung (the other is retry backoff).
                    let what = Wait::RateLimit { prefix: ctx.prefix };
                    let dur = Duration::from_millis(remaining as u64);
                    ctx.reporter.report(&Event::WaitStarted {
                        what,
                        expected: Some(dur),
                    });
                    ctx.timer.sleep(dur).await;
                    ctx.reporter.report(&Event::WaitFinished { what });
                }
            }
        }
        last_start = Some(ctx.clock.now());

        let mut res = source.retrieve_chunk(batch, ctx).await;
        out.append(&mut res);

        ctx.reporter.report(&Event::SourceProgress {
            prefix: ctx.prefix,
            done: start,
            total,
        });
    }

    // Carries the final count, so a throttle that dropped the last
    // `SourceProgress` sample cannot leave a display short of the total.
    ctx.reporter.report(&Event::SourceFinished {
        prefix: ctx.prefix,
        done: total,
    });

    (out, last_start)
}
