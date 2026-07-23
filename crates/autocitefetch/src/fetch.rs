//! The [`Fetcher`] trait: host-provided URL retrieval.
//!
//! A fetcher performs one HTTP-style request and returns the full buffered
//! response. The host decides how (browser `fetch()`, `reqwest`, `ureq`, a
//! `file:` reader for local bibliography files, …). It is the single choke
//! point for all network/file I/O — unlike the reference implementations,
//! *every* source goes through it, including arXiv.

use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;

use hashbrown::HashMap;

use crate::BoxFuture;

/// HTTP request method. Only the two the sources need.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
}

/// A request to fetch a URL (or a `file:` path).
#[derive(Clone, Debug)]
pub struct Request {
    pub url: String,
    pub method: Method,
    /// Header names are stored lowercased (HTTP headers are case-insensitive).
    pub headers: HashMap<String, String>,
    pub body: Option<Vec<u8>>,
}

impl Request {
    /// A `GET` request for `url`.
    pub fn get(url: impl Into<String>) -> Self {
        Request {
            url: url.into(),
            method: Method::Get,
            headers: HashMap::new(),
            body: None,
        }
    }

    /// A `POST` request for `url`.
    pub fn post(url: impl Into<String>) -> Self {
        Request {
            url: url.into(),
            method: Method::Post,
            headers: HashMap::new(),
            body: None,
        }
    }

    /// Add a header (name is lowercased). Builder-style.
    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.to_ascii_lowercase(), value.into());
        self
    }

    /// Set the request body. Builder-style.
    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }
}

/// A buffered response.
#[derive(Clone, Debug)]
pub struct Response {
    pub status: u16,
    /// Header names should be lowercased by the fetcher. Nothing enforces it,
    /// so [`Response::header`] compares case-insensitively either way.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Response {
    /// Whether the status is 2xx.
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The response body decoded as UTF-8.
    pub fn text(&self) -> core::result::Result<&str, core::str::Utf8Error> {
        core::str::from_utf8(&self.body)
    }

    /// Look up a response header (case-insensitive).
    ///
    /// [`Response::headers`] only *asks* fetchers to lowercase header names;
    /// a host that stores the canonical `Retry-After` would otherwise make
    /// this return `None` and defeat the retry layer's `Retry-After` handling.
    /// So: fast path on the lowercased name, then a linear
    /// ASCII-case-insensitive scan.
    pub fn header(&self, name: &str) -> Option<&str> {
        if let Some(v) = self.headers.get(&name.to_ascii_lowercase()) {
            return Some(v.as_str());
        }
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Why a fetch failed. [`FetchError::is_retryable`] guides backoff.
#[derive(Clone, Debug)]
pub enum FetchError {
    /// Transport-level failure (DNS, connection, TLS, timeout). Retryable.
    Transport(String),
    /// A non-success HTTP status the fetcher chose to surface as an error.
    Status(u16),
    /// Anything else, host-specific.
    Other(String),
}

impl FetchError {
    /// Whether retrying (with backoff) might succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            FetchError::Transport(_) => true,
            FetchError::Status(s) => matches!(s, 429 | 500 | 502 | 503 | 504),
            FetchError::Other(_) => false,
        }
    }
}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FetchError::Transport(m) => write!(f, "transport: {m}"),
            FetchError::Status(s) => write!(f, "http status {s}"),
            FetchError::Other(m) => write!(f, "{m}"),
        }
    }
}

impl core::error::Error for FetchError {}

/// Host-provided URL retrieval. Object-safe (usable as `&dyn Fetcher`).
///
/// # Implementor contract
///
/// - **Redirects MUST be followed, preserving the request headers.** This is
///   load-bearing, not a nicety: doi.org content negotiation answers a DOI
///   with a 3xx to the registration agency, and the `accept:
///   application/vnd.citationstyles.csl+json` header has to survive the hop or
///   the agency returns a landing page instead of CSL-JSON. [`Response`]
///   cannot express "this is a redirect" ([`Response::is_success`] is 2xx
///   only) and the sources treat any 3xx as a failure, so a fetcher built on
///   an API that defaults to *not* following redirects (a browser `fetch()`
///   with `redirect: "manual"`, say) must opt back in. The bundled
///   `UreqFetcher` follows redirects and re-sends headers.
/// - A **non-success status is not an error**: return
///   `Ok(Response { status, .. })` and let the source decide.
///   [`FetchError::Status`] is only for hosts that genuinely cannot produce a
///   response body.
/// - Response header names should be lowercased (see [`Response::headers`]).
pub trait Fetcher {
    /// Perform one request and return the buffered response.
    fn fetch(&self, req: Request) -> BoxFuture<'_, core::result::Result<Response, FetchError>>;
}
