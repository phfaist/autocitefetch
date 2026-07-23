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
    /// Header names should be lowercased by the fetcher.
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
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(&name.to_ascii_lowercase()).map(String::as_str)
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
pub trait Fetcher {
    /// Perform one request and return the buffered response.
    fn fetch(&self, req: Request) -> BoxFuture<'_, core::result::Result<Response, FetchError>>;
}
