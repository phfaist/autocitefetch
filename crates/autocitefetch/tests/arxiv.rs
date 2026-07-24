//! Integration tests for the `arxiv` source: Atom-feed parsing, CSL mapping,
//! version resolution, and DOI chaining (via a mock `Fetcher`).

use std::cell::RefCell;
use std::collections::HashMap as StdMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use autocitefetch::source::{ArxivSource, DoiSource, Source};
use autocitefetch::{
    BoxFuture, CacheRecord, CacheStore, CitationManager, Clock, FetchError, Fetcher, Request,
    Response, StoreError, Timer, Timestamp,
};

// --- a minimal, always-ready block_on (mocks never truly pend) -------------

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future did not complete (a mock unexpectedly pended)");
}

// --- mocks -----------------------------------------------------------------

struct MockFetcher {
    routes: StdMap<String, (u16, Vec<u8>)>,
    calls: RefCell<Vec<String>>,
}

impl MockFetcher {
    fn new() -> Self {
        MockFetcher {
            routes: StdMap::new(),
            calls: RefCell::new(Vec::new()),
        }
    }
    fn route(mut self, url: &str, status: u16, body: &str) -> Self {
        self.routes
            .insert(url.into(), (status, body.as_bytes().to_vec()));
        self
    }
}

impl Fetcher for MockFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        self.calls.borrow_mut().push(req.url.clone());
        let result = match self.routes.get(&req.url) {
            Some((status, body)) => Ok(Response {
                status: *status,
                headers: Default::default(),
                body: body.clone(),
            }),
            None => Err(FetchError::Status(404)),
        };
        Box::pin(async move { result })
    }
}

#[derive(Default)]
struct MemStore {
    map: RefCell<StdMap<String, CacheRecord>>,
}

impl CacheStore for MemStore {
    fn get(&self, id: &str) -> BoxFuture<'_, Result<Option<CacheRecord>, StoreError>> {
        let v = self.map.borrow().get(id).cloned();
        Box::pin(async move { Ok(v) })
    }
    fn put(&self, id: &str, record: CacheRecord) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().insert(id.into(), record);
        Box::pin(async move { Ok(()) })
    }
    fn remove(&self, id: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        self.map.borrow_mut().remove(id);
        Box::pin(async move { Ok(()) })
    }
    fn entries(&self) -> BoxFuture<'_, Result<Vec<(String, CacheRecord)>, StoreError>> {
        let all: Vec<_> = self
            .map
            .borrow()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Box::pin(async move { Ok(all) })
    }
}

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now(&self) -> Timestamp {
        Timestamp::from_millis(self.0)
    }
}

struct InstantTimer;
impl Timer for InstantTimer {
    fn sleep(&self, _dur: core::time::Duration) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

// --- canned Atom feeds -----------------------------------------------------

// A versionless request whose entry carries an `<arxiv:doi>` (mixed case, to
// exercise lowercasing). The id is `…v1`; the request is versionless.
const FEED_CHAINED: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <id>http://arxiv.org/api/query-feed-id</id>
  <entry>
    <id>http://arxiv.org/abs/1211.1037v1</id>
    <published>2012-11-05T18:30:00Z</published>
    <title>A Chained arXiv Paper</title>
    <author><name>Ada Lovelace</name></author>
    <arxiv:doi>10.1103/PhysRevLett.109.170502</arxiv:doi>
    <link href="http://arxiv.org/abs/1211.1037v1" rel="alternate" type="text/html"/>
  </entry>
</feed>"#;

// A versionless request, concrete resolution (chaining disabled). Title spans
// lines (whitespace collapse), authors exercise the family/given split
// including a single-token name. The entry carries BOTH a `<published>` (v1
// submission) and a later `<updated>` (this revision): `issued` must come from
// `<updated>`. A feed-level `<updated>` with a sentinel far-future date is
// present too and must NOT be captured (it sits outside any `<entry>`).
const FEED_CONCRETE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <id>http://arxiv.org/api/query-feed-id</id>
  <updated>2099-12-31T00:00:00Z</updated>
  <entry>
    <id>http://arxiv.org/abs/0905.2794v3</id>
    <published>2013-03-20T09:15:00Z</published>
    <updated>2013-04-25T09:15:00Z</updated>
    <title>Quantum Error Correction
      for   Beginners</title>
    <author><name>Simon J. Devitt</name></author>
    <author><name>Kae Nemoto</name></author>
    <author><name>Aristotle</name></author>
  </entry>
</feed>"#;

// --- tests -----------------------------------------------------------------

#[test]
fn arxiv_versionless_with_doi_chains_and_merges() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1211.1037&max_results=1";
    // The DOI is lowercased when chaining, so doi.org is asked for the lower form.
    let doi_url = "https://doi.org/10.1103/physrevlett.109.170502";

    let fetcher = MockFetcher::new()
        .route(arxiv_url, 200, FEED_CHAINED)
        .route(
            doi_url,
            200,
            r#"{"type":"article-journal","title":"Resolved via DOI","DOI":"10.1103/PhysRevLett.109.170502"}"#,
        );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1211.1037".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // Reading the arXiv key follows the chain to the DOI entry, rewrites the id
    // back to the requested one, and merges the chained `arxivid`.
    let item = block_on(mgr.get("arxiv", "1211.1037")).unwrap();
    assert_eq!(item["id"], "arxiv:1211.1037");
    assert_eq!(item["title"], "Resolved via DOI");
    assert_eq!(item["arxivid"], "1211.1037", "chained set_properties merged in");
    // doi.org's uppercase `DOI` is normalized on ingest to a lowercase `doi`
    // key with a lowercased value; the CSL-spec uppercase key does not survive.
    assert_eq!(item["doi"], "10.1103/physrevlett.109.170502");
    assert_eq!(item.get("DOI"), None, "uppercase DOI key must not survive");
}

#[test]
fn arxiv_concrete_parses_title_authors_and_issued() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=0905.2794&max_results=1";

    let fetcher = MockFetcher::new().route(arxiv_url, 200, FEED_CONCRETE);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().chaining(false))
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "0905.2794".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "0905.2794")).unwrap();

    assert_eq!(item["id"], "arxiv:0905.2794");
    assert_eq!(item["type"], "article-journal");
    // Internal whitespace/newlines collapse to single spaces.
    assert_eq!(item["title"], "Quantum Error Correction for Beginners");

    // Authors: naive last-token = family, rest = given; single token = family.
    let authors = item["author"].as_array().expect("author array");
    assert_eq!(authors.len(), 3);
    assert_eq!(authors[0]["family"], "Devitt");
    assert_eq!(authors[0]["given"], "Simon J.");
    assert_eq!(authors[1]["family"], "Nemoto");
    assert_eq!(authors[1]["given"], "Kae");
    assert_eq!(authors[2]["family"], "Aristotle");
    assert!(authors[2].get("given").is_none(), "single token = family only");

    // issued comes from `<updated>` (last-revision date) = 2013-04-25, NOT from
    // `<published>` (2013-03-20) and NOT from the feed-level `<updated>` (2099).
    let dp = &item["issued"]["date-parts"][0];
    assert_eq!(dp[0], 2013);
    assert_eq!(dp[1], 4);
    assert_eq!(dp[2], 25);

    // arXiv extension fields: id stripped of version, latest version captured.
    assert_eq!(item["arxivid"], "0905.2794");
    assert_eq!(item["arxiv_version_number"], 3);
}

#[test]
fn issued_falls_back_to_published_when_no_updated() {
    // An entry with no `<updated>` element: `issued` must fall back to
    // `<published>` (feedparser's `.date` does the same fallback).
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1601.00007&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1601.00007v1</id>
    <published>2016-07-08T00:00:00Z</published>
    <title>Only Published, No Updated</title>
    <author><name>Ada Lovelace</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new().chaining(false));

    let cites = vec![("arxiv".to_string(), "1601.00007".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1601.00007")).unwrap();
    let dp = &item["issued"]["date-parts"][0];
    assert_eq!(dp[0], 2016);
    assert_eq!(dp[1], 7);
    assert_eq!(dp[2], 8);
}

#[test]
fn versioned_request_uses_that_versions_updated_date() {
    // An explicitly-versioned request selects that exact entry, whose
    // `<updated>` is *that version's* date. The old behavior (using
    // `<published>`) would have reported v1's submission date for every version.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1211.1037v2&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1211.1037v2</id>
    <published>2012-11-05T18:30:00Z</published>
    <updated>2012-12-14T18:30:00Z</updated>
    <title>Revised Version</title>
    <author><name>Ada Lovelace</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![("arxiv".to_string(), "1211.1037v2".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1211.1037v2")).unwrap();
    assert_eq!(item["arxiv_version_number"], 2, "exact version, concrete");
    // issued is v2's `<updated>` (2012-12-14), NOT v1's `<published>` (2012-11-05).
    let dp = &item["issued"]["date-parts"][0];
    assert_eq!(dp[0], 2012);
    assert_eq!(dp[1], 12);
    assert_eq!(dp[2], 14);
}

#[test]
fn arxiv_unknown_id_is_reported_not_fatal() {
    // The feed returns no matching entry for the requested key.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=9999.99999&max_results=1";
    let fetcher = MockFetcher::new().route(
        arxiv_url,
        200,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <title>ArXiv Query</title>
  <entry>
    <id>http://arxiv.org/api/errors#incorrect_id_format</id>
    <title>Error</title>
  </entry>
</feed>"#,
    );

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "9999.99999".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].prefix, "arxiv");
    assert!(block_on(mgr.get("arxiv", "9999.99999")).is_err());
}

// --- entity / character references -----------------------------------------

#[test]
fn entity_references_are_decoded_in_titles_and_authors() {
    // `xmlparser` is a tokenizer and hands back raw spans, so without explicit
    // decoding these would reach CSL as the literal source text.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1234.5678,1234.5679&max_results=2";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1234.5678v1</id>
    <published>2020-01-02T00:00:00Z</published>
    <title>A &amp; B, $x &lt; y$, &#38;, &#x26;</title>
    <author><name>Ann &amp; Bob</name></author>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/1234.5679v1</id>
    <published>2020-01-02T00:00:00Z</published>
    <title>&gt;&quot;&apos; and &#8212; dash</title>
    <author><name>Zoe</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![
        ("arxiv".to_string(), "1234.5678".to_string()),
        ("arxiv".to_string(), "1234.5679".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1234.5678")).unwrap();
    assert_eq!(item["title"], "A & B, $x < y$, &, &");

    // An undecoded `&amp;` would be a whitespace-free token that shifts this
    // naive split: the family name would come out as `B` (from `&amp;`)…
    let authors = item["author"].as_array().expect("author array");
    assert_eq!(authors.len(), 1);
    assert_eq!(authors[0]["family"], "Bob");
    assert_eq!(authors[0]["given"], "Ann &");

    // The remaining predefined names, plus a multi-byte numeric reference.
    let other = block_on(mgr.get("arxiv", "1234.5679")).unwrap();
    assert_eq!(other["title"], ">\"' and \u{2014} dash");
}

#[test]
fn malformed_entity_references_are_left_verbatim() {
    // Unknown names, empty/invalid numerics and bare `&`s must survive intact
    // (and must not panic): dropping text is worse than a literal `&foo;`.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1234.5680&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1234.5680v1</id>
    <published>2020-01-02T00:00:00Z</published>
    <title>A &foo; B &#; C &amp D &#xZZ; E &#99999999999; F &</title>
    <author><name>Q &amp;</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1234.5680".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1234.5680")).unwrap();
    assert_eq!(
        item["title"],
        "A &foo; B &#; C &amp D &#xZZ; E &#99999999999; F &"
    );
    // A trailing `&amp;` still decodes even when it is the whole last token.
    let authors = item["author"].as_array().expect("author array");
    assert_eq!(authors[0]["family"], "&");
    assert_eq!(authors[0]["given"], "Q");
}

#[test]
fn cdata_content_stays_raw() {
    // CDATA is *character data* by definition: it arrives as a distinct
    // `Token::Cdata` and must NOT be run through the entity decoder.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1234.5681&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1234.5681v1</id>
    <published>2020-01-02T00:00:00Z</published>
    <title><![CDATA[raw &amp; text]]></title>
    <author><name>Zoe</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1234.5681".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1234.5681")).unwrap();
    assert_eq!(item["title"], "raw &amp; text");
}

// --- blank <arxiv:doi> ------------------------------------------------------

#[test]
fn blank_arxiv_doi_does_not_chain_to_the_empty_doi_key() {
    // All three ways a feed can report "no DOI, but with tags": an empty
    // element, a self-closing one, and a whitespace-only one. Each must yield
    // concrete arXiv metadata — never a chain to the empty key `doi:`, which
    // would fetch `https://doi.org/`, fail, and discard good metadata. No
    // doi.org route is registered, so any chain attempt fails the citation.
    let arxiv_url =
        "https://export.arxiv.org/api/query?id_list=2100.00001,2100.00002,2100.00003&max_results=3";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/2100.00001v1</id>
    <published>2021-01-02T00:00:00Z</published>
    <title>Empty Element</title>
    <author><name>Ada Lovelace</name></author>
    <arxiv:doi></arxiv:doi>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2100.00002v1</id>
    <published>2021-01-02T00:00:00Z</published>
    <title>Self Closing</title>
    <author><name>Ada Lovelace</name></author>
    <arxiv:doi/>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/2100.00003v1</id>
    <published>2021-01-02T00:00:00Z</published>
    <title>Whitespace Only</title>
    <author><name>Ada Lovelace</name></author>
    <arxiv:doi>   </arxiv:doi>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new())
        .register(DoiSource::new());

    let cites = vec![
        ("arxiv".to_string(), "2100.00001".to_string()),
        ("arxiv".to_string(), "2100.00002".to_string()),
        ("arxiv".to_string(), "2100.00003".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(
        report.is_complete(),
        "a blank DOI must not chain: {:?}",
        report.failures
    );

    for (key, title) in [
        ("2100.00001", "Empty Element"),
        ("2100.00002", "Self Closing"),
        ("2100.00003", "Whitespace Only"),
    ] {
        let item = block_on(mgr.get("arxiv", key)).unwrap();
        assert_eq!(item["title"], title, "{key}: concrete arXiv metadata");
        assert_eq!(item["arxivid"], key);
        assert!(item.get("doi").is_none(), "{key}: no empty `doi` field");
    }
}

// --- URL construction: multi-key chunks, old-style ids, key trimming --------

#[test]
fn old_style_ids_and_multi_key_chunks_build_the_expected_url() {
    // Two keys in one chunk: comma-joined, each percent-encoded (`/` → `%2F`),
    // and `max_results` = the chunk size. Routing on the exact URL *is* the
    // assertion — any other URL 404s and both keys fail.
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=math%2F0309136,hep-th%2F9901001v2&max_results=2";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/math/0309136v1</id>
    <published>2003-09-08T00:00:00Z</published>
    <title>An Old Style Preprint</title>
    <author><name>Grigori Perelman</name></author>
  </entry>
  <entry>
    <id>http://arxiv.org/abs/hep-th/9901001v2</id>
    <published>1999-01-01T00:00:00Z</published>
    <title>Another Old Style Preprint</title>
    <author><name>Ann Other</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![
        ("arxiv".to_string(), "math/0309136".to_string()),
        ("arxiv".to_string(), "hep-th/9901001v2".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    // The `/` in an old-style id is not a version separator.
    let old = block_on(mgr.get("arxiv", "math/0309136")).unwrap();
    assert_eq!(old["arxivid"], "math/0309136");
    assert_eq!(old["arxiv_version_number"], 1);
    assert_eq!(old["title"], "An Old Style Preprint");

    let versioned = block_on(mgr.get("arxiv", "hep-th/9901001v2")).unwrap();
    assert_eq!(versioned["arxivid"], "hep-th/9901001");
    assert_eq!(versioned["arxiv_version_number"], 2);
}

#[test]
fn requested_keys_are_trimmed_for_the_url_but_returned_verbatim() {
    // `\cite{arXiv: 1211.1037}` yields a key with a leading space; untrimmed it
    // encodes to `%201211.1037`, which arXiv answers with 400 — failing every
    // key in the chunk. Only the trimmed URL is routed. The stored citation is
    // still keyed by the caller's exact string (the manager matches on it).
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1211.1037&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1211.1037v1</id>
    <published>2012-11-05T18:30:00Z</published>
    <title>A Padded Request</title>
    <author><name>Ada Lovelace</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), " 1211.1037 ".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", " 1211.1037 ")).unwrap();
    assert_eq!(item["id"], "arxiv: 1211.1037 ", "key echoed verbatim");
    assert_eq!(item["title"], "A Padded Request");
    assert_eq!(item["arxivid"], "1211.1037");
}

#[test]
fn version_suffix_matching_is_case_insensitive() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1211.1037V2&max_results=1";
    let feed = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">
  <entry>
    <id>http://arxiv.org/abs/1211.1037v2</id>
    <published>2012-11-05T18:30:00Z</published>
    <title>Uppercase V Request</title>
    <author><name>Ada Lovelace</name></author>
  </entry>
</feed>"#;

    let fetcher = MockFetcher::new().route(arxiv_url, 200, feed);

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![("arxiv".to_string(), "1211.1037V2".to_string())];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert!(report.is_complete(), "failures: {:?}", report.failures);

    let item = block_on(mgr.get("arxiv", "1211.1037V2")).unwrap();
    assert_eq!(item["arxivid"], "1211.1037");
    assert_eq!(item["arxiv_version_number"], 2, "`V2` is `v2`");
}

// --- whole-request failures -------------------------------------------------

#[test]
fn http_error_status_fails_every_key_in_the_chunk() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1234.5678,1234.5679&max_results=2";
    // 400 is not retryable, so the retrying fetcher passes it straight through.
    let fetcher = MockFetcher::new().route(arxiv_url, 400, "id_list is malformed");

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![
        ("arxiv".to_string(), "1234.5678".to_string()),
        ("arxiv".to_string(), "1234.5679".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 2, "one arXiv 400 fails the whole chunk");
    for f in &report.failures {
        assert_eq!(f.prefix, "arxiv");
        assert!(
            f.message.contains("400"),
            "message should name the status: {}",
            f.message
        );
    }
}

#[test]
fn malformed_xml_fails_every_key_in_the_chunk() {
    let arxiv_url = "https://export.arxiv.org/api/query?id_list=1234.5678,1234.5679&max_results=2";
    let fetcher = MockFetcher::new().route(arxiv_url, 200, "<feed><entry><<</entry></feed>");

    let mgr = CitationManager::new(fetcher, MemStore::default(), FixedClock(0), InstantTimer)
        .register(ArxivSource::new());

    let cites = vec![
        ("arxiv".to_string(), "1234.5678".to_string()),
        ("arxiv".to_string(), "1234.5679".to_string()),
    ];
    let report = block_on(mgr.retrieve(&cites)).unwrap();
    assert_eq!(report.failures.len(), 2);
    for f in &report.failures {
        assert!(
            f.message.contains("XML parse error"),
            "message should name the parse failure: {}",
            f.message
        );
    }
}

// --- declared source constants ---------------------------------------------

#[test]
fn arxiv_source_declares_its_rate_limits_and_chaining() {
    let src = ArxivSource::new();
    assert_eq!(src.prefix(), "arxiv");
    // ~100 ids keeps the GET URL under the usual ~2000-character limit.
    assert_eq!(src.chunk_size(), 100);
    // arXiv asks for no more than one request every ~3 seconds.
    assert_eq!(src.min_interval(), core::time::Duration::from_millis(3100));
    assert_eq!(
        src.default_ttl(),
        core::time::Duration::from_secs(10 * 24 * 60 * 60)
    );
    assert_eq!(src.chains_to(), ["doi"].as_slice());

    let no_chain = ArxivSource::new().chaining(false);
    assert!(no_chain.chains_to().is_empty());
}
