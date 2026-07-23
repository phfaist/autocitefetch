//! The `doi` source: doi.org content negotiation.
//!
//! doi.org, asked for `application/vnd.citationstyles.csl+json`, returns
//! native CSL-JSON — so there is no per-field mapping to do, we store the
//! response verbatim. One DOI per request; ~1 req/s.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::csl::CslValue;
use crate::error::Error;
use crate::fetch::Request;
use crate::source::{Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

const ACCEPT_CSL_JSON: &str = "application/vnd.citationstyles.csl+json";

/// See module docs.
#[derive(Default)]
pub struct DoiSource;

impl DoiSource {
    pub fn new() -> Self {
        DoiSource
    }
}

impl Source for DoiSource {
    fn prefix(&self) -> &str {
        "doi"
    }

    fn chunk_size(&self) -> usize {
        1
    }

    fn min_interval(&self) -> Duration {
        Duration::from_millis(1100)
    }

    fn default_ttl(&self) -> Duration {
        Duration::from_secs(360 * 24 * 60 * 60)
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                out.push(resolve_one(&key, ctx).await);
            }
            out
        })
    }
}

async fn resolve_one(doi: &str, ctx: &RetrieveCtx<'_>) -> Resolution {
    // Guard the two inputs that would otherwise produce a *plausible* request:
    // an empty key fetches doi.org's homepage, whitespace corrupts the path.
    if doi.is_empty() {
        return Resolution::failed(doi, Error::Source("empty DOI".into()));
    }
    if doi.chars().any(char::is_whitespace) {
        return Resolution::failed(
            doi,
            Error::Source(alloc::format!("DOI `{doi}` contains whitespace")),
        );
    }
    let mut url = String::from("https://doi.org/");
    encode_path_into(&mut url, doi);

    let req = Request::get(url).header("accept", ACCEPT_CSL_JSON);
    match ctx.fetcher.fetch(req).await {
        Ok(resp) if resp.is_success() => match serde_json::from_slice::<CslValue>(&resp.body) {
            // A CSL-JSON item is a non-empty JSON *object*. Anything else that
            // happens to parse (`null`, `[]`, `"nope"`, `123`, `{}` — a proxy,
            // a captive portal, or an RA answering with a JSON error envelope)
            // must not be cached: `csl::set_id` would replace it with a fresh
            // object and we would store a plausible-looking `{"id": "doi:…"}`
            // shell for the full 360-day TTL, with no failure reported.
            Ok(csl) if is_csl_item(&csl) => Resolution::concrete(doi, csl),
            Ok(_) => Resolution::failed(
                doi,
                Error::Parse(
                    "doi.org returned 200 but the body is not a non-empty JSON object".into(),
                ),
            ),
            Err(e) => Resolution::failed(doi, Error::Parse(alloc::format!("{e}"))),
        },
        Ok(resp) => Resolution::failed(
            doi,
            Error::Source(alloc::format!("doi.org returned status {}", resp.status)),
        ),
        Err(e) => Resolution::failed(doi, Error::Fetch(e)),
    }
}

/// Whether a parsed body can be a CSL-JSON item: a non-empty JSON object.
fn is_csl_item(v: &CslValue) -> bool {
    v.as_object().is_some_and(|o| !o.is_empty())
}

/// Percent-encode a DOI for use in a URL path. Keeps `/` (DOIs use it as a
/// separator and doi.org expects it unencoded) and the unreserved set.
fn encode_path_into(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in s.as_bytes() {
        let keep = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/');
        if keep {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
}
