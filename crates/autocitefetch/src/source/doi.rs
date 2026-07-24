//! The `doi` source: doi.org content negotiation.
//!
//! doi.org, asked for `application/vnd.citationstyles.csl+json`, returns
//! native CSL-JSON — so there is no per-field mapping to do, we store the
//! response essentially verbatim. One DOI per request; ~1 req/s.
//!
//! The one deliberate exception is the **DOI key**: canonical CSL-JSON spells it
//! uppercase `DOI`, but this workspace uses a uniform lowercase `doi` everywhere
//! (the arXiv source emits lowercase `doi`, and `get`/chaining read one canonical
//! spelling). So on ingest we fold an uppercase `DOI` down to lowercase `doi` and
//! lowercase its value (DOIs are case-insensitive) — see [`normalize_doi_key`].
//! Every other field is stored verbatim.

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
            Ok(mut csl) if is_csl_item(&csl) => {
                normalize_doi_key(&mut csl);
                Resolution::concrete(doi, csl)
            }
            Ok(_) => Resolution::failed(
                doi,
                Error::Parse(
                    "doi.org returned 200 but the body is not a non-empty JSON object".into(),
                ),
            ),
            Err(e) => Resolution::failed(doi, Error::Parse(alloc::format!("{e}"))),
        },
        // A 404 is doi.org's authoritative "no such DOI": reachable, definitive,
        // not "try again". Report it and drop any stale copy (`Missing`) rather
        // than grace-serving now-wrong metadata. Every other non-2xx (and any
        // 5xx/timeout the retrying fetcher already gave up on) is treated as a
        // transient reachability failure (`Failed`), grace-served if cached.
        Ok(resp) if resp.status == 404 => {
            Resolution::missing(doi, Error::NotFound(crate::csl::cite_id("doi", doi)))
        }
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

/// Fold doi.org's canonical CSL `DOI` key down to the lowercase `doi` this
/// workspace uses, lowercasing the value (DOIs are case-insensitive).
///
/// This is the *only* place we touch a doi.org field; every other field is
/// stored verbatim. Rules: an uppercase `DOI` is moved to `doi` (not left as a
/// duplicate); if a lowercase `doi` is somehow already present it wins and the
/// uppercase one is dropped; whichever value survives is lowercased when it is a
/// string. A non-string value (never valid for a DOI) is kept as-is under the
/// lowercase key.
fn normalize_doi_key(csl: &mut CslValue) {
    let Some(obj) = csl.as_object_mut() else {
        return;
    };
    let upper = obj.remove("DOI");
    // Prefer an existing lowercase `doi`; otherwise adopt the uppercase value.
    let Some(mut value) = obj.remove("doi").or(upper) else {
        return; // no DOI in either spelling — nothing to normalize.
    };
    if let Some(s) = value.as_str() {
        value = CslValue::String(s.to_ascii_lowercase());
    }
    obj.insert("doi".into(), value);
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
