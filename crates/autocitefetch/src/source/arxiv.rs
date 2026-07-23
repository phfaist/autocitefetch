//! The `arxiv` source: the arXiv Atom API.
//!
//! Fetches `https://export.arxiv.org/api/query?id_list=…&max_results=…`
//! through [`RetrieveCtx::fetcher`] (uniform I/O — no bypass), parses the
//! returned Atom feed with a small `no_std` pull tokenizer (the [`xmlparser`]
//! crate), and maps each entry to CSL-JSON built by hand.
//!
//! Behaviour (mirrors the JS/Python references, with fixes):
//! * `GET …/api/query?id_list=<comma-joined, percent-encoded ids>&max_results=<n>`
//!   for a chunk of ≤100 ids.
//! * Parse each `<entry>` for `<id>`, `<title>`, `<author><name>`,
//!   `<published>`, and `<arxiv:doi>`. An entry whose `<id>` is not an
//!   `…/abs/<arxivid>` URL is an arXiv *error entry* and is skipped, so the
//!   corresponding requested key resolves to [`Outcome::Failed`].
//! * Map to CSL-JSON (`type: "article-journal"`) with extension fields
//!   `arxivid` and `arxiv_version_number`.
//! * Version resolution: a key requested *with* an explicit `vN` is emitted as
//!   [`Outcome::Concrete`] preserving that exact version (never chained). A
//!   *versionless* key resolves to arXiv's returned (latest) entry; if it
//!   carries a DOI and `chain_to_doi` is set, it is emitted as
//!   [`Outcome::Chained`] to the `doi` source with `set_properties = { arxivid }`,
//!   otherwise as concrete arXiv metadata.

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::time::Duration;

use crate::csl::CslValue;
use crate::error::Error;
use crate::fetch::Request;
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

/// See module docs.
pub struct ArxivSource {
    /// When true, resolved DOIs are chained to the `doi` source.
    pub chain_to_doi: bool,
}

impl Default for ArxivSource {
    fn default() -> Self {
        ArxivSource { chain_to_doi: true }
    }
}

impl ArxivSource {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Source for ArxivSource {
    fn prefix(&self) -> &str {
        "arxiv"
    }

    fn chunk_size(&self) -> usize {
        100
    }

    fn min_interval(&self) -> Duration {
        // arXiv asks for no more than one request every ~3 seconds.
        Duration::from_millis(3100)
    }

    fn default_ttl(&self) -> Duration {
        Duration::from_secs(10 * 24 * 60 * 60)
    }

    fn chains_to(&self) -> &[&'static str] {
        if self.chain_to_doi {
            &["doi"]
        } else {
            &[]
        }
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            // Build the id_list query. Ids contain `/` and `.`, so each is
            // percent-encoded; they are joined with a literal comma.
            let mut url = String::from("https://export.arxiv.org/api/query?id_list=");
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    url.push(',');
                }
                encode_query_into(&mut url, k);
            }
            url.push_str("&max_results=");
            url.push_str(&alloc::format!("{}", keys.len()));

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
                .map(|key| resolve_key(self.chain_to_doi, key, &entries))
                .collect()
        })
    }
}

/// Every key in the chunk failed identically (whole-request failure).
fn fail_all(keys: Vec<String>, msg: String) -> Vec<Resolution> {
    keys.into_iter()
        .map(|k| Resolution::failed(k, Error::Source(msg.clone())))
        .collect()
}

/// Resolve one requested key against the parsed feed entries.
fn resolve_key(chain_to_doi: bool, key: String, entries: &[atom::Entry]) -> Resolution {
    let (base, req_version) = split_version(&key);

    // Match by base arxivid; honour an explicit requested version if present.
    let entry = entries.iter().find(|e| {
        e.arxivid == base && (req_version.is_none() || e.version == req_version)
    });

    let entry = match entry {
        Some(e) => e,
        None => {
            return Resolution::failed(
                key.clone(),
                Error::Source(alloc::format!("no arXiv entry returned for `{key}`")),
            );
        }
    };

    // Explicit version requested: emit concrete metadata, preserving the exact
    // version — never chain (a DOI would drop the version distinction).
    if req_version.is_some() {
        return Resolution::concrete(key, build_csl(entry));
    }

    // Versionless: chain to the DOI when we have one and chaining is enabled.
    if chain_to_doi {
        if let Some(doi) = &entry.doi {
            let mut sp = serde_json::Map::new();
            sp.insert("arxivid".into(), CslValue::String(key.clone()));
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

    Resolution::concrete(key, build_csl(entry))
}

/// Map a parsed arXiv entry to a CSL-JSON object (built by hand).
fn build_csl(e: &atom::Entry) -> CslValue {
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

    if let Some(doi) = &e.doi {
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
    let date = p.split(['T', 't', ' ']).next().unwrap_or(p);
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
/// regex tail `(v(?P<versionnum>\d+))?$`. Returns `(base, version)`.
fn split_version(s: &str) -> (&str, Option<u32>) {
    let bytes = s.as_bytes();
    let mut i = bytes.len();
    while i > 0 && bytes[i - 1].is_ascii_digit() {
        i -= 1;
    }
    // Need at least one digit, a preceding `v`, and a non-empty base before it.
    if i < bytes.len() && i >= 2 && bytes[i - 1] == b'v' {
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
                        Cap::Doi => raw.doi = Some(text.trim().to_string()),
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

        fn on_text(&mut self, text: &str) {
            if self.capture.is_some() {
                self.buf.push_str(text);
            }
        }
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
                Token::Text { text } => p.on_text(text.as_str()),
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
