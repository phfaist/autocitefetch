//! `--verbose` progress rendering: a [`Reporter`] that writes lines to stderr.
//!
//! Presentation lives *here*, not in `autocitefetch-std`, for the same reason
//! YAML parsing does (see [`crate::formats`]): the CLI is the host, and the
//! libraries take data rather than opinions about how it should look. The core
//! emits borrowed [`Event`]s and formats nothing.
//!
//! Two things about the output are deliberate.
//!
//! **Failures are not printed here.** [`crate::run`] already prints every
//! [`CiteFailure`](autocitefetch::CiteFailure) from the
//! [`RetrieveReport`](autocitefetch::RetrieveReport), with the `origin`
//! attribution that makes a chained-target failure readable. Rendering
//! [`Event::CiteFailed`] too would double every failure line. The one case the
//! report cannot show is a *grace-served* failure — the source was unreachable
//! but a cached copy inside the grace window is still good, so nothing is
//! reported — and that is exactly the case this reporter prints.
//!
//! **Rate-limit waits are only mentioned when they are long enough to look like
//! a hang.** doi.org is paced at 1100 ms per request, so announcing every gap
//! would put a line between every pair of requests; [`WAIT_NOTICE`] is the
//! threshold below which a pause is unremarkable. `-vv` prints them all.

use std::time::Duration;

use autocitefetch::report::{Event, Reporter, Resolved, Wait};

/// Rate-limit pauses shorter than this are not worth a line at `-v`: they read
/// as normal pacing rather than as the tool having stopped. Backoff waits are
/// always announced — they mean something went wrong.
const WAIT_NOTICE: Duration = Duration::from_secs(2);

/// Writes progress to stderr, at one of two levels of detail.
///
/// * `1` (`-v`) — milestones: passes, per-source progress, long waits, retries.
/// * `2` (`-vv`) — everything, including one line per HTTP request and per
///   resolved citation.
pub struct StderrReporter {
    level: u8,
}

impl StderrReporter {
    /// `level` is the `--verbose` count; 0 renders nothing.
    pub fn new(level: u8) -> Self {
        StderrReporter { level }
    }

    /// One line of progress chatter, on the same stream and with the same
    /// prefix as [`crate::warn`] — stdout carries the CSL-JSON and nothing else.
    fn line(&self, msg: &str) {
        eprintln!("{}: {msg}", crate::PROG);
    }
}

impl Reporter for StderrReporter {
    fn report(&self, ev: &Event<'_>) {
        if self.level == 0 {
            return;
        }
        let verbose = self.level >= 2;

        match *ev {
            // `run` already announces the citation count and the source list,
            // which this event cannot know about.
            Event::RetrieveStarted { .. } => {}

            Event::PassStarted {
                pass,
                cached,
                to_fetch,
            } => self.line(&format!(
                "pass {pass}: {cached} cached, {to_fetch} to fetch"
            )),

            // Silent when nothing was discovered: "queued 0" every pass is
            // noise, and a non-zero count is the interesting thing (it is what
            // makes a progress denominator grow).
            Event::PassFinished { pass, discovered } if discovered > 0 => self.line(&format!(
                "pass {pass}: queued {discovered} chained citation(s)"
            )),
            Event::PassFinished { .. } => {}

            Event::SourceStarted {
                prefix,
                keys,
                chunks,
            } => self.line(&format!(
                "{prefix}: fetching {keys} key(s) in {chunks} request(s)"
            )),

            Event::SourceProgress {
                prefix,
                done,
                total,
            } => self.line(&format!("{prefix}: {done}/{total}")),

            // Prints the final count, so a run whose intermediate samples were
            // throttled away still ends on a complete reading.
            Event::SourceFinished { prefix, done } => {
                self.line(&format!("{prefix}: {done} key(s) done"))
            }

            Event::CiteResolved { prefix, key, how } if verbose => match how {
                Resolved::Concrete => self.line(&format!("{prefix}:{key}: resolved")),
                Resolved::Chained {
                    prefix: tp,
                    key: tk,
                } => self.line(&format!("{prefix}:{key} -> {tp}:{tk}")),
                _ => self.line(&format!("{prefix}:{key}: resolved")),
            },
            Event::CiteResolved { .. } => {}

            // Only the grace-served case: see the module docs.
            Event::CiteFailed {
                prefix,
                key,
                err,
                grace_served: true,
            } => self.line(&format!(
                "{prefix}:{key}: {err} — serving the cached copy"
            )),
            Event::CiteFailed { .. } => {}

            Event::WaitStarted { what, expected } => match what {
                Wait::RateLimit { prefix } => {
                    let d = expected.unwrap_or_default();
                    if verbose || d >= WAIT_NOTICE {
                        self.line(&format!("{prefix}: waiting {} (rate limit)", dur(d)));
                    }
                }
                Wait::Backoff { url, attempt } => self.line(&format!(
                    "retrying {url} in {} (attempt {})",
                    dur(expected.unwrap_or_default()),
                    attempt + 2
                )),
                Wait::CacheFlush => self.line("compacting the cache"),
                // A wait this build has no name for is still worth announcing:
                // the point of these events is that the tool is not hung.
                _ => self.line(&format!("waiting {}", dur(expected.unwrap_or_default()))),
            },

            // The pairing exists for displays that start and stop a spinner; a
            // line-based log only needs it when following along in detail.
            Event::WaitFinished { what } if verbose => match what {
                Wait::RateLimit { prefix } => self.line(&format!("{prefix}: ...done waiting")),
                Wait::Backoff { url, .. } => self.line(&format!("...retrying {url} now")),
                Wait::CacheFlush => self.line("...cache compacted"),
                _ => self.line("...done waiting"),
            },
            Event::WaitFinished { .. } => {}

            Event::RequestStarted { url, attempt } if verbose => {
                if attempt == 0 {
                    self.line(&format!("GET {url}"));
                } else {
                    self.line(&format!("GET {url} (attempt {})", attempt + 1));
                }
            }
            Event::RequestStarted { .. } => {}

            Event::RequestFinished { url, result } if verbose => match result {
                Ok(status) => self.line(&format!("{status} <- {url}")),
                Err(e) => self.line(&format!("failed <- {url}: {e}")),
            },
            Event::RequestFinished { .. } => {}

            Event::RetrieveFinished { considered, failed } => self.line(&format!(
                "retrieval done: {considered} citation(s) considered, {failed} failed"
            )),

            // `Event` is `#[non_exhaustive]`: a newer core may emit something
            // this build has never heard of, which is not an error.
            _ => {}
        }
    }
}

/// A wait duration, rendered the way a person reads it.
fn dur(d: Duration) -> String {
    let ms = d.as_millis();
    if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}
