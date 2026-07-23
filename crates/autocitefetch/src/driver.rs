//! The per-source retrieval driver: chunking + rate limiting.
//!
//! Splits a source's key list into `chunk_size` batches and calls
//! `retrieve_chunk` on each, sleeping `min_interval` *between* chunks (via the
//! host [`Timer`](crate::env::Timer) — so backoff actually happens, unlike the
//! reference `sleep` no-op bug).

use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::source::{Resolution, RetrieveCtx, Source};

/// Drive one source over all its requested keys, returning every resolution.
pub async fn drive_source(
    source: &dyn Source,
    keys: Vec<String>,
    ctx: &RetrieveCtx<'_>,
) -> Vec<Resolution> {
    let chunk_size = source.chunk_size().max(1);
    let interval = source.min_interval();

    let mut out = Vec::with_capacity(keys.len());
    let mut first = true;

    // `chunks` borrows; collect each batch into an owned Vec for the source.
    let mut start = 0;
    while start < keys.len() {
        let end = start.saturating_add(chunk_size).min(keys.len());
        let batch: Vec<_> = keys[start..end].to_vec();
        start = end;

        if !first && interval > Duration::ZERO {
            ctx.timer.sleep(interval).await;
        }
        first = false;

        let mut res = source.retrieve_chunk(batch, ctx).await;
        out.append(&mut res);
    }

    out
}
