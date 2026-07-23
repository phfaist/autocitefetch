//! The `arxiv` source: the arXiv Atom API.
//!
//! Fetches `https://export.arxiv.org/api/query?id_list=…&max_results=…`
//! through [`RetrieveCtx::fetcher`] (uniform I/O — no bypass), parses the
//! returned Atom feed with a small `no_std` pull tokenizer (the [`xmlparser`]
//! crate), and maps each entry to CSL-JSON built by hand.
//!
//! Behaviour (broadly that of the JS/Python references, with fixes — the
//! entity handling below is *not* shared with them):
//! * `GET …/api/query?id_list=<comma-joined, percent-encoded ids>&max_results=<n>`
//!   for a chunk of ≤100 ids. Each requested key is **trimmed** before it is
//!   encoded into the URL (a stray space, as in `\cite{arXiv: 1211.1037}`, makes
//!   arXiv answer 400 for the *whole* chunk) and before it is matched against
//!   feed entries; the returned [`Resolution::key`] is still the caller's key,
//!   verbatim, as the manager requires.
//! * Parse each `<entry>` for `<id>`, `<title>`, `<author><name>`,
//!   `<published>`, and `<arxiv:doi>`. An entry whose `<id>` is not an
//!   `…/abs/<arxivid>` URL is an arXiv *error entry* and is skipped, so the
//!   corresponding requested key resolves to [`Outcome::Failed`].
//! * **Entity references are decoded.** `xmlparser` is a pure tokenizer: it
//!   hands back the raw source span, so `&amp;`/`&lt;`/`&#38;` would otherwise
//!   survive verbatim into titles and author names (arXiv titles routinely
//!   contain `&` and LaTeX `<`, and in a name an undecoded `&amp;` even shifts
//!   the family/given split). Text nodes are therefore decoded — the five
//!   predefined named references plus numeric/hex character references —
//!   *before* whitespace collapsing, so `&#160;` behaves like the space it is.
//!   **CDATA sections are left raw**, which is what "character data" means:
//!   they arrive as a distinct `Token::Cdata` and are appended unchanged.
//!   Anything unrecognized (`&foo;`, `&#;`, a bare `&`) is preserved verbatim
//!   rather than dropped.
//! * A **blank DOI is no DOI**: an empty or whitespace-only `<arxiv:doi>` (or
//!   an override value of `""`) is treated as absent, so it can never chain to
//!   the empty key `doi:` and throw away good arXiv metadata.
//! * Map to CSL-JSON (`type: "article-journal"`) with extension fields
//!   `arxivid` and `arxiv_version_number`.
//! * **DOI overrides.** A caller may inject, replace, or *suppress* the DOI for
//!   a given arXiv id — supplied as data ([`ArxivSource::with_override_dois`]:
//!   `Some(doi)` to inject/replace, `None` to suppress) or via a convenience
//!   JSON file ([`ArxivSource::with_override_dois_file`]). An override DOI *wins*
//!   over the feed's `<arxiv:doi>` and drives chaining like a feed DOI; a
//!   suppressing `None` keeps the arXiv metadata and skips chaining.
//! * **Version resolution.** Feed entries are grouped by *base* arxivid (the id
//!   without its `vN` suffix). A key requested *with* an explicit `vN` selects
//!   that exact version and is emitted as [`Outcome::Concrete`] preserving the
//!   version — it is never chained. A *versionless* key selects the *best*
//!   returned entry for its base id — an entry that is itself versionless wins,
//!   otherwise the one with the highest version number — and, if that entry has
//!   a DOI (after override) and chaining is enabled, is emitted as
//!   [`Outcome::Chained`] to the `doi` source with `set_properties = { arxivid }`,
//!   otherwise as concrete arXiv metadata.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use hashbrown::HashMap;

use crate::csl::CslValue;
use crate::error::Error;
use crate::fetch::Request;
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

const PREFIX: &str = "arxiv";
// 100 ids per query: the arXiv API takes the id list in the *query string* of a
// GET, and ~100 ids keeps the URL under the ~2000-character limit that proxies
// and servers commonly enforce (the JS reference documents the same bound).
const CHUNK_SIZE: usize = 100;
// arXiv asks for no more than one request every ~3 seconds.
const MIN_INTERVAL_MS: u64 = 3100;
const TTL_SECS: u64 = 10 * 24 * 60 * 60;

/// The arXiv Atom API source.
///
/// Carries the `chain_to_doi` switch plus an optional per-id DOI-override map
/// and/or file. Configure with the fluent builders, e.g.
/// `ArxivSource::new().chaining(false).with_override_dois(map)`.
pub struct ArxivSource {
    /// When true, resolved DOIs are chained to the `doi` source.
    pub chain_to_doi: bool,
    /// Inline arxivid → DOI overrides. `Some(doi)` injects/replaces the DOI;
    /// `None` suppresses it (keep arXiv metadata, don't chain). Takes
    /// precedence over file entries.
    override_dois: HashMap<String, Option<String>>,
    /// Optional URL/path of a JSON override file (fetched once per chunk).
    override_dois_file: Option<String>,
}

impl Default for ArxivSource {
    fn default() -> Self {
        ArxivSource {
            chain_to_doi: true,
            override_dois: HashMap::new(),
            override_dois_file: None,
        }
    }
}

impl ArxivSource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether resolved DOIs are chained to the `doi` source (default
    /// `true`). Builder-style.
    pub fn chaining(mut self, yes: bool) -> Self {
        self.chain_to_doi = yes;
        self
    }

    /// Attach/merge an inline arXiv-id → DOI override map, keyed by *base*
    /// arxivid (no `vN` suffix). `Some(doi)` overrides whatever `<arxiv:doi>`
    /// the feed reports (override wins) and drives chaining like a feed DOI;
    /// `None` *suppresses* the DOI — the entry keeps its arXiv metadata and is
    /// not chained, even if the feed reports a DOI. Ids absent from the map use
    /// the feed's DOI as normal.
    ///
    /// Overrides are supplied as data: the host parses whatever config format
    /// it likes and passes `(arxivid, Option<doi>)` pairs (from a `HashMap`,
    /// `Vec`, array, …). Later calls, and inline entries, win over any file.
    pub fn with_override_dois(
        mut self,
        map: impl IntoIterator<Item = (String, Option<String>)>,
    ) -> Self {
        for (k, v) in map {
            self.override_dois.insert(k, v);
        }
        self
    }

    /// Convenience: attach a URL/path to a **JSON** DOI-override file, fetched
    /// through `ctx.fetcher` and parsed as an object `{ "<arxivid>": "<doi>" }`,
    /// where a JSON `null` value means *suppress* (→ `None`). JSON is the one
    /// built-in format (it is already a core dependency); for any other format,
    /// parse it host-side and pass the data to [`Self::with_override_dois`].
    /// File entries merge with the inline map; the inline map wins. Builder-style.
    pub fn with_override_dois_file(mut self, url: impl Into<String>) -> Self {
        self.override_dois_file = Some(url.into());
        self
    }
}

impl Source for ArxivSource {
    fn prefix(&self) -> &str {
        PREFIX
    }
    fn chunk_size(&self) -> usize {
        CHUNK_SIZE
    }
    fn min_interval(&self) -> Duration {
        Duration::from_millis(MIN_INTERVAL_MS)
    }
    fn default_ttl(&self) -> Duration {
        Duration::from_secs(TTL_SECS)
    }
    fn chains_to(&self) -> &[&'static str] {
        chains_to_slice(self.chain_to_doi)
    }
    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(retrieve_impl(
            self.chain_to_doi,
            &self.override_dois,
            self.override_dois_file.as_deref(),
            keys,
            ctx,
        ))
    }
}

fn chains_to_slice(chain_to_doi: bool) -> &'static [&'static str] {
    if chain_to_doi {
        &["doi"]
    } else {
        &[]
    }
}

/// The retrieval body. `inline`/`file` describe the (optional) DOI overrides.
async fn retrieve_impl<'a>(
    chain_to_doi: bool,
    inline: &'a HashMap<String, Option<String>>,
    file: Option<&'a str>,
    keys: Vec<String>,
    ctx: &'a RetrieveCtx<'a>,
) -> Vec<Resolution> {
    // Resolve the effective override map. The file (if any) is loaded first,
    // then the inline map is layered on top so the INLINE map wins on conflicts.
    // A failed file load degrades gracefully: it fails the whole chunk like any
    // other fetch failure.
    let mut overrides: HashMap<String, Option<String>> = HashMap::new();
    if let Some(url) = file {
        match load_override_file(ctx, url).await {
            Ok(m) => overrides = m,
            Err(msg) => return fail_all(keys, msg),
        }
    }
    for (k, v) in inline {
        overrides.insert(k.clone(), v.clone());
    }

    // Build the id_list query. Ids contain `/` and `.`, so each is
    // percent-encoded; they are joined with a literal comma. Keys are trimmed
    // here (and again when matching entries): a key like `"1211.1037 "` would
    // otherwise encode to `1211.1037%20`, which arXiv rejects with a 400 —
    // failing every one of the up-to-100 keys in the chunk. Only the *request*
    // is normalized; the resolutions below still carry the caller's exact key.
    let mut url = String::from("https://export.arxiv.org/api/query?id_list=");
    for (i, k) in keys.iter().enumerate() {
        if i > 0 {
            url.push(',');
        }
        encode_query_into(&mut url, k.trim());
    }
    url.push_str("&max_results=");
    url.push_str(&keys.len().to_string());

    let resp = match ctx.fetcher.fetch(Request::get(url)).await {
        Ok(r) if r.is_success() => r,
        Ok(r) => {
            return fail_all(keys, alloc::format!("arXiv API returned status {}", r.status));
        }
        Err(e) => return fail_all(keys, alloc::format!("arXiv fetch failed: {e}")),
    };

    let text = match resp.text() {
        Ok(t) => t,
        Err(_) => return fail_all(keys, "arXiv response is not UTF-8".to_string()),
    };

    let entries = match atom::parse_feed(text) {
        Ok(e) => e,
        Err(msg) => return fail_all(keys, msg),
    };

    keys.into_iter()
        .map(|key| resolve_key(chain_to_doi, &overrides, key, &entries))
        .collect()
}

/// Fetch and parse the JSON DOI-override file: an object
/// `{ "<arxivid>": "<doi>" | null }` where a string injects a DOI and `null`
/// suppresses it. JSON only (for other formats, parse host-side). Any
/// fetch/parse/shape error is returned as a message so the caller can fail the
/// affected keys gracefully rather than panic.
async fn load_override_file(
    ctx: &RetrieveCtx<'_>,
    url: &str,
) -> Result<HashMap<String, Option<String>>, String> {
    let resp = ctx
        .fetcher
        .fetch(Request::get(url.to_string()))
        .await
        .map_err(|e| alloc::format!("arXiv DOI-override file fetch failed: {e}"))?;
    if !resp.is_success() {
        return Err(alloc::format!(
            "arXiv DOI-override file returned status {}",
            resp.status
        ));
    }
    let value: CslValue = serde_json::from_slice(&resp.body)
        .map_err(|e| alloc::format!("arXiv DOI-override file is not valid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "arXiv DOI-override file is not a JSON object".to_string())?;
    let mut map = HashMap::with_capacity(obj.len());
    for (k, v) in obj {
        match v {
            CslValue::String(s) => {
                map.insert(k.clone(), Some(s.clone()));
            }
            CslValue::Null => {
                map.insert(k.clone(), None);
            }
            _ => {
                return Err(alloc::format!(
                    "arXiv DOI-override entry `{k}` must be a string or null"
                ));
            }
        }
    }
    Ok(map)
}

/// Every key in the chunk failed identically (whole-request failure).
fn fail_all(keys: Vec<String>, msg: String) -> Vec<Resolution> {
    keys.into_iter()
        .map(|k| Resolution::failed(k, Error::Source(msg.clone())))
        .collect()
}

/// Resolve one requested key against the parsed feed entries and the effective
/// override map.
///
/// The key is trimmed for *matching* only: every `Resolution` returned here
/// carries `key` exactly as the manager supplied it, since the manager pairs
/// resolutions back to requests by that string.
fn resolve_key(
    chain_to_doi: bool,
    overrides: &HashMap<String, Option<String>>,
    key: String,
    entries: &[atom::Entry],
) -> Resolution {
    let (base, req_version) = split_version(key.trim());

    // Explicit version requested: select that exact version, emit concrete
    // metadata preserving the version — never chain (a DOI would drop the
    // version distinction). An override still applies to the `doi` field.
    if let Some(v) = req_version {
        let found = entries
            .iter()
            .find(|e| e.arxivid == base && e.version == Some(v));
        return match found {
            Some(e) => {
                let doi = effective_doi(overrides, base, e.doi.as_deref());
                Resolution::concrete(key, build_csl(e, doi))
            }
            None => {
                let msg = alloc::format!("no arXiv entry returned for `{key}`");
                Resolution::failed(key, Error::Source(msg))
            }
        };
    }

    // Versionless: select the BEST entry among all returned for this base id.
    let entry = match select_best(entries, base) {
        Some(e) => e,
        None => {
            let msg = alloc::format!("no arXiv entry returned for `{key}`");
            return Resolution::failed(key, Error::Source(msg));
        }
    };

    let doi = effective_doi(overrides, base, entry.doi.as_deref());

    // Chain to the DOI when we have one (after override) and chaining is on.
    // A `None` override — or a blank DOI — suppresses it, so chaining is
    // skipped and the arXiv metadata is kept.
    if chain_to_doi {
        if let Some(doi) = doi {
            // `base` == the trimmed key on this (versionless) path.
            let mut sp = serde_json::Map::new();
            sp.insert("arxivid".into(), CslValue::String(base.to_string()));
            return Resolution {
                key,
                outcome: Outcome::Chained {
                    prefix: "doi".to_string(),
                    key: doi.to_ascii_lowercase(),
                    set_properties: CslValue::Object(sp),
                },
            };
        }
    }

    Resolution::concrete(key, build_csl(entry, doi))
}

/// The effective DOI for a base arxivid: `Some(doi)` in the override map injects
/// it, `None` in the map suppresses it, and an absent key falls back to the
/// feed's DOI.
///
/// This is the single choke point where a **blank DOI is normalized away**. All
/// three inputs can carry one — `<arxiv:doi></arxiv:doi>` in the feed, an inline
/// override of `Some(String::new())`, or `{"<id>": ""}` in an override file —
/// and a `Some("")` here would chain to the empty key, i.e. fetch
/// `https://doi.org/`, fail, and report a failure under an empty `doi` key while
/// discarding perfectly good arXiv metadata. Blank in ⇒ `None` out, so a blank
/// *override* reads like the suppressing `null` rather than like "no override".
fn effective_doi<'a>(
    overrides: &'a HashMap<String, Option<String>>,
    base: &str,
    feed_doi: Option<&'a str>,
) -> Option<&'a str> {
    let doi = match overrides.get(base) {
        Some(Some(doi)) => Some(doi.as_str()),
        Some(None) => None,
        None => feed_doi,
    };
    doi.map(str::trim).filter(|d| !d.is_empty())
}

/// Choose the best entry for `base` among all returned entries: an entry that is
/// itself versionless (`version == None`) wins outright; otherwise the entry
/// with the highest version number. Mirrors the JS/Python `source_finalize_run`
/// reduction (which is more careful than the old first-match `.find(...)`).
fn select_best<'e>(entries: &'e [atom::Entry], base: &str) -> Option<&'e atom::Entry> {
    let mut best: Option<&atom::Entry> = None;
    for e in entries.iter().filter(|e| e.arxivid == base) {
        best = Some(match best {
            None => e,
            // A versionless best is the answer to a versionless query — keep it.
            Some(b) if b.version.is_none() => b,
            // This entry is versionless — prefer it.
            Some(_) if e.version.is_none() => e,
            // Both versioned: take the higher (ties take the later-seen).
            Some(b) if e.version >= b.version => e,
            Some(b) => b,
        });
    }
    best
}

/// Map a parsed arXiv entry to a CSL-JSON object (built by hand). `doi` is the
/// *effective* DOI (override-or-feed); when `Some`, it is written lowercased.
fn build_csl(e: &atom::Entry, doi: Option<&str>) -> CslValue {
    let mut obj = serde_json::Map::new();
    obj.insert("type".into(), CslValue::String("article-journal".to_string()));

    if let Some(title) = &e.title {
        obj.insert("title".into(), CslValue::String(title.clone()));
    }

    let mut authors = Vec::with_capacity(e.authors.len());
    for name in &e.authors {
        authors.push(build_author(name));
    }
    obj.insert("author".into(), CslValue::Array(authors));

    if let Some(issued) = build_issued(e.published.as_deref()) {
        obj.insert("issued".into(), issued);
    }

    if let Some(doi) = doi {
        obj.insert("doi".into(), CslValue::String(doi.to_ascii_lowercase()));
    }

    obj.insert("arxivid".into(), CslValue::String(e.arxivid.clone()));
    obj.insert(
        "arxiv_version_number".into(),
        match e.version {
            Some(n) => CslValue::from(n),
            None => CslValue::Null,
        },
    );

    CslValue::Object(obj)
}

/// Naive author split: the last whitespace token is `family`, the rest (if
/// any) is `given`. A single token yields `family` only.
fn build_author(name: &str) -> CslValue {
    let toks: Vec<&str> = name.split_whitespace().collect();
    let mut obj = serde_json::Map::new();
    if let Some((family, rest)) = toks.split_last() {
        obj.insert("family".into(), CslValue::String((*family).to_string()));
        if !rest.is_empty() {
            obj.insert("given".into(), CslValue::String(rest.join(" ")));
        }
    }
    CslValue::Object(obj)
}

/// Build a CSL `issued` object from a `<published>` value (`YYYY-MM-DD…`).
/// Month and day are included only if present.
fn build_issued(published: Option<&str>) -> Option<CslValue> {
    let p = published?;
    // Keep only the date portion (before any time separator).
    let date = p.find(['T', 't', ' ']).map_or(p, |i| &p[..i]);
    let mut it = date.split('-');

    let year: i64 = it.next()?.trim().parse().ok()?;
    let mut parts = Vec::with_capacity(3);
    parts.push(CslValue::from(year));
    if let Some(month) = it.next().and_then(|s| s.trim().parse::<i64>().ok()) {
        parts.push(CslValue::from(month));
        if let Some(day) = it.next().and_then(|s| s.trim().parse::<i64>().ok()) {
            parts.push(CslValue::from(day));
        }
    }

    let outer = CslValue::Array(alloc::vec![CslValue::Array(parts)]);

    let mut obj = serde_json::Map::new();
    obj.insert("date-parts".into(), outer);
    Some(CslValue::Object(obj))
}

/// Split a trailing arXiv version suffix (`vN`) off an id, equivalent to the
/// regex tail `(v(?P<versionnum>\d+))?$`, case-insensitively (`1211.1037V2` is
/// the same request as `1211.1037v2`). Returns `(base, version)`.
fn split_version(s: &str) -> (&str, Option<u32>) {
    let bytes = s.as_bytes();
    let mut i = bytes.len();
    while i > 0 && bytes[i - 1].is_ascii_digit() {
        i -= 1;
    }
    // Need at least one digit, a preceding `v`/`V`, and a non-empty base before
    // it. `| 0x20` lowercases an ASCII letter; only `v` and `V` map to `v`.
    if i < bytes.len() && i >= 2 && bytes[i - 1] | 0x20 == b'v' {
        if let Ok(ver) = s[i..].parse::<u32>() {
            return (&s[..i - 1], Some(ver));
        }
    }
    (s, None)
}

/// Percent-encode a value for use as a URL query component. Keeps the
/// unreserved set (`A-Za-z0-9-._~`) and encodes everything else — including
/// `/`, which arXiv old-style ids use (e.g. `quant-ph/0406196`).
fn encode_query_into(out: &mut String, s: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
}

/// A minimal Atom-feed reader for the arXiv API, built on the `no_std`
/// [`xmlparser`] pull tokenizer.
///
/// It is namespace-agnostic: it matches on local element names, which is
/// sufficient for the arXiv feed (`<arxiv:doi>` is the only namespaced element
/// we read, and its local name `doi` is unambiguous). Only text inside an
/// `<entry>` is captured, so feed-level `<id>`/`<title>` are ignored.
///
/// `xmlparser` is a *tokenizer*, not a parser: it hands back raw source spans
/// and never resolves references. Escaping is therefore this module's job, and
/// it hinges on one distinction the token stream already makes for us:
/// `Token::Text` is escaped source (decode it — see [`push_decoded`]) while
/// `Token::Cdata` is literal character data (append it untouched). Getting that
/// backwards would either leave `&amp;` in every title or mangle a CDATA
/// section, so the two arms in [`parse_feed`] must stay distinct.
mod atom {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    use xmlparser::{ElementEnd, Token, Tokenizer};

    /// A parsed, valid arXiv entry (invalid/error entries are dropped).
    pub struct Entry {
        pub arxivid: String,
        pub version: Option<u32>,
        pub title: Option<String>,
        pub authors: Vec<String>,
        pub published: Option<String>,
        pub doi: Option<String>,
    }

    /// Fields accumulated while an `<entry>` is open (id not yet validated).
    #[derive(Default)]
    struct Raw {
        id: Option<String>,
        title: Option<String>,
        published: Option<String>,
        doi: Option<String>,
        authors: Vec<String>,
    }

    /// Which text-bearing leaf element we are currently capturing.
    enum Cap {
        Id,
        Title,
        Published,
        Doi,
        AuthorName,
    }

    struct Parser {
        entries: Vec<Entry>,
        raw: Option<Raw>,
        in_author: bool,
        capture: Option<Cap>,
        buf: String,
    }

    impl Parser {
        fn begin(&mut self, c: Cap) {
            self.capture = Some(c);
            self.buf.clear();
        }

        fn on_open(&mut self, local: &str) {
            if local == "entry" {
                self.raw = Some(Raw::default());
                self.in_author = false;
                self.capture = None;
                return;
            }
            // Only capture fields while inside an <entry>.
            if self.raw.is_none() {
                return;
            }
            match local {
                "author" => self.in_author = true,
                "id" => self.begin(Cap::Id),
                "title" => self.begin(Cap::Title),
                "published" => self.begin(Cap::Published),
                "doi" => self.begin(Cap::Doi),
                "name" if self.in_author => self.begin(Cap::AuthorName),
                _ => {}
            }
        }

        fn on_close(&mut self, local: &str) {
            let cap_matches = match &self.capture {
                Some(Cap::Id) => local == "id",
                Some(Cap::Title) => local == "title",
                Some(Cap::Published) => local == "published",
                Some(Cap::Doi) => local == "doi",
                Some(Cap::AuthorName) => local == "name",
                None => false,
            };
            if cap_matches {
                let cap = self.capture.take().unwrap();
                let text = core::mem::take(&mut self.buf);
                if let Some(raw) = &mut self.raw {
                    match cap {
                        Cap::Id => raw.id = Some(text.trim().to_string()),
                        Cap::Title => raw.title = Some(collapse_ws(&text)),
                        Cap::Published => raw.published = Some(text.trim().to_string()),
                        // A blank `<arxiv:doi>` (empty, self-closing, or all
                        // whitespace) is *no* DOI, not the empty DOI.
                        Cap::Doi => {
                            let doi = text.trim();
                            if !doi.is_empty() {
                                raw.doi = Some(doi.to_string());
                            }
                        }
                        Cap::AuthorName => raw.authors.push(collapse_ws(&text)),
                    }
                }
            }

            if local == "author" {
                self.in_author = false;
            }
            if local == "entry" {
                if let Some(raw) = self.raw.take() {
                    if let Some(entry) = into_entry(raw) {
                        self.entries.push(entry);
                    }
                }
            }
        }

        /// Append literal character data (a CDATA section) verbatim.
        fn on_text(&mut self, text: &str) {
            if self.capture.is_some() {
                self.buf.push_str(text);
            }
        }

        /// Append an escaped text run, decoding its entity/character references.
        ///
        /// Safe to call repeatedly for one element: `xmlparser` emits a `Text`
        /// token spanning everything up to the next `<`, and a reference cannot
        /// contain a `<`, so a reference is never split across two `Text`
        /// tokens and no cross-call buffering is needed.
        fn on_text_unescaped(&mut self, text: &str) {
            if self.capture.is_none() {
                return;
            }
            // The overwhelmingly common case: nothing to decode, no scanning.
            if !text.contains('&') {
                self.buf.push_str(text);
                return;
            }
            push_decoded(&mut self.buf, text);
        }
    }

    /// Decode XML entity and character references in `s`, appending to `out`.
    ///
    /// Handles the five predefined named references (`amp lt gt quot apos`) and
    /// numeric character references (`&#NNN;`, `&#xHH;`). Anything else — an
    /// unknown name, a malformed or out-of-range number, an unterminated `&amp`,
    /// a bare `&` — is copied through **verbatim**. This is a bibliography
    /// reader, not a validator: a literal `&foo;` left in a title is a far
    /// better outcome than silently dropping text (or panicking).
    fn push_decoded(out: &mut String, s: &str) {
        let bytes = s.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            // Copy the run up to the next `&`. Both `&` and `;` are ASCII and
            // no UTF-8 continuation byte can equal them, so every index taken
            // here lands on a char boundary.
            let start = i;
            while i < bytes.len() && bytes[i] != b'&' {
                i += 1;
            }
            if i > start {
                out.push_str(&s[start..i]);
            }
            if i == bytes.len() {
                break;
            }
            // At a `&`: decode `&<body>;` if we recognize it, else emit the `&`
            // alone and resume scanning just after it (so `&amp&amp;` yields
            // `&amp&`, not a swallowed tail).
            let rest = &s[i + 1..];
            match rest
                .find(';')
                .and_then(|end| decode_reference(&rest[..end]).map(|c| (c, end)))
            {
                Some((c, end)) => {
                    out.push(c);
                    i += end + 2; // `&` + body + `;`
                }
                None => {
                    out.push('&');
                    i += 1;
                }
            }
        }
    }

    /// Decode the *body* of a reference (what sits between `&` and `;`), or
    /// `None` if it is not one we recognize.
    fn decode_reference(body: &str) -> Option<char> {
        match body {
            "amp" => return Some('&'),
            "lt" => return Some('<'),
            "gt" => return Some('>'),
            "quot" => return Some('"'),
            "apos" => return Some('\''),
            _ => {}
        }
        let num = body.strip_prefix('#')?;
        let code = match num.strip_prefix(['x', 'X']) {
            Some(hex) if !hex.is_empty() && hex.bytes().all(|b| b.is_ascii_hexdigit()) => {
                // Overflow (a 20-digit number) simply fails to parse ⇒ verbatim.
                u32::from_str_radix(hex, 16).ok()?
            }
            Some(_) => return None,
            None if !num.is_empty() && num.bytes().all(|b| b.is_ascii_digit()) => {
                num.parse::<u32>().ok()?
            }
            None => return None,
        };
        char::from_u32(code)
    }

    /// Parse an arXiv Atom feed into its valid entries. Returns `Err` only on a
    /// malformed XML document.
    pub fn parse_feed(xml: &str) -> Result<Vec<Entry>, String> {
        let mut p = Parser {
            entries: Vec::new(),
            raw: None,
            in_author: false,
            capture: None,
            buf: String::new(),
        };

        // The local name of the most recent ElementStart, awaiting its
        // ElementEnd (Open / Empty / Close).
        let mut pending: &str = "";

        for tok in Tokenizer::from(xml) {
            let tok = tok.map_err(|e| alloc::format!("arXiv Atom XML parse error: {e}"))?;
            match tok {
                Token::ElementStart { local, .. } => {
                    pending = local.as_str();
                }
                Token::ElementEnd { end, .. } => match end {
                    ElementEnd::Open => p.on_open(pending),
                    ElementEnd::Empty => {
                        // Opened and closed with no children: no text to capture.
                        p.on_open(pending);
                        p.on_close(pending);
                    }
                    ElementEnd::Close(_, local) => p.on_close(local.as_str()),
                },
                // Escaped source text vs. literal character data — see the
                // module comment above; these two must not be merged.
                Token::Text { text } => p.on_text_unescaped(text.as_str()),
                Token::Cdata { text, .. } => p.on_text(text.as_str()),
                _ => {}
            }
        }

        Ok(p.entries)
    }

    /// Validate a raw entry's `<id>` and finalize it, or drop it (error entry).
    fn into_entry(raw: Raw) -> Option<Entry> {
        let id = raw.id?;
        let (arxivid, version) = parse_arxiv_id(&id)?;
        Some(Entry {
            arxivid,
            version,
            title: raw.title,
            authors: raw.authors,
            published: raw.published,
            doi: raw.doi,
        })
    }

    /// Parse an entry `<id>` of the form
    /// `https?://arxiv.org/abs/<arxivid>[vN]` (case-insensitive scheme/host),
    /// returning `(arxivid, version)`. `None` if it is not an `…/abs/…` URL.
    fn parse_arxiv_id(id: &str) -> Option<(String, Option<u32>)> {
        // arXiv ids and URLs are ASCII, so lowercasing preserves byte offsets.
        let lower = id.to_ascii_lowercase();
        let rest = lower
            .strip_prefix("http://")
            .or_else(|| lower.strip_prefix("https://"))?;
        let after = rest.strip_prefix("arxiv.org/abs/")?;
        let start = id.len() - after.len();
        let idpart = &id[start..];
        if idpart.is_empty() {
            return None;
        }
        let (base, version) = super::split_version(idpart);
        if base.is_empty() {
            return None;
        }
        Some((base.to_string(), version))
    }

    /// Trim and collapse internal runs of whitespace/newlines to single spaces.
    fn collapse_ws(s: &str) -> String {
        let mut out = String::new();
        let mut pending_space = false;
        for c in s.chars() {
            if c.is_whitespace() {
                if !out.is_empty() {
                    pending_space = true;
                }
            } else {
                if pending_space {
                    out.push(' ');
                    pending_space = false;
                }
                out.push(c);
            }
        }
        out
    }
}
