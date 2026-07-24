//! The `doi` source: doi.org content negotiation.
//!
//! doi.org, asked for `application/vnd.citationstyles.csl+json`, returns
//! native CSL-JSON — so there is no per-field mapping to do, we store the
//! response essentially verbatim. One DOI per request; ~1 req/s.
//!
//! **Keys arrive canonical.** This source keeps the default
//! [`Source::normalize_key`] (trim + ASCII-lowercase), which is exactly right
//! for DOIs: the DOI Handbook declares them case-insensitive, so
//! `10.1103/PhysRevA.86.052329` and `10.1103/physreva.86.052329` are one
//! citation, and the manager folds them to one `doi:` cache id and one request
//! before this source ever sees them. Nothing here re-cases or re-trims a key —
//! including keys arriving as an arXiv chain target, which the manager
//! normalizes with *this* policy on the way in. Note this concerns the cache
//! *key* only; the CSL `DOI` **field** below is a different thing and keeps its
//! registered case. (Internal whitespace is deliberately *not* normalized away,
//! so `resolve_one`'s guard below can still reject a malformed key.)
//!
//! The one deliberate touch is the **DOI key**: this workspace emits the
//! canonical CSL-JSON spelling, uppercase `DOI`, everywhere (the arXiv source
//! also builds `DOI`). doi.org already returns `DOI`, so this is a no-op on its
//! responses; the normalization exists only to canonicalize a stray lowercase
//! `doi` up to `DOI` — see `canonicalize_doi_key`. The value is stored
//! **verbatim** (DOIs display in their registered mixed case, and CSL-JSON
//! carries them as-is). Every other field is likewise stored verbatim.

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
                canonicalize_doi_key(&mut csl);
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

/// Canonicalize the DOI key to the CSL-JSON standard uppercase `DOI`, keeping
/// its value **verbatim** (DOIs display in their registered mixed case).
///
/// This is the *only* place we touch a doi.org field; every other field is
/// stored verbatim. doi.org already returns `DOI`, so on its responses this is a
/// no-op; it exists to rename a stray lowercase `doi` up to `DOI`. Rules: if
/// both `DOI` and (a nonstandard) `doi` somehow appear, the uppercase `DOI`
/// wins and the lowercase duplicate is dropped; otherwise a lone lowercase `doi`
/// is renamed to `DOI`. The value is never re-cased.
fn canonicalize_doi_key(csl: &mut CslValue) {
    let Some(obj) = csl.as_object_mut() else {
        return;
    };
    // Fast path: no lowercase `doi` ⇒ nothing to canonicalize. This covers
    // doi.org's own responses (which spell it `DOI`) and bodies with no DOI at
    // all, and leaves the map completely untouched.
    let Some(lower) = obj.remove("doi") else {
        return;
    };
    // A lowercase `doi` was present and is now removed. Promote it to `DOI`
    // only if the standard key was absent — when both appear, uppercase wins.
    if !obj.contains_key("DOI") {
        obj.insert("DOI".into(), lower);
    }
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
