//! A blocking HTTP [`Fetcher`] for `std`/CLI hosts, backed by [`ureq`].
//!
//! This is the network backend the core crate deliberately leaves out so it
//! carries no HTTP dependency. It performs the request **synchronously** and
//! then hands the finished result back as an already-resolved future
//! (`Box::pin(async move { result })`) — the same blocking-in-a-future shape as
//! [`BlockingTimer`](crate::BlockingTimer). It **blocks the calling thread**
//! for the duration of the request, so it is meant for a blocking driver
//! (`pollster::block_on`, or the `Waker::noop()` poll loop the examples/tests
//! use), *not* for an async runtime — there, implement
//! [`Fetcher`] with `reqwest`/`hyper` instead.
//!
//! In addition to `http(s)://` URLs it resolves `file:` URLs (and bare,
//! scheme-less paths) by reading the local filesystem, returning a synthetic
//! `200` response. That is what lets
//! [`BibliographyFileSource`](autocitefetch::source::BibliographyFileSource)
//! load local `.json` bibliographies through the one `Fetcher` choke point.

use std::io::Read;
use std::time::Duration;

use autocitefetch::{BoxFuture, FetchError, Fetcher, Method, Request, Response};

/// Default `User-Agent` sent with every HTTP request. Polite APIs (crossref /
/// doi.org) ask for an identifying agent; override via
/// [`UreqFetcher::with_user_agent`].
pub const DEFAULT_USER_AGENT: &str = concat!(
    "autocitefetch-std/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/phfaist/autocitefetch)"
);

/// A blocking [`Fetcher`] over [`ureq`], with local `file:` support.
///
/// See the [module docs](self) for the blocking-in-a-future contract.
pub struct UreqFetcher {
    agent: ureq::Agent,
    user_agent: String,
}

impl UreqFetcher {
    /// A fetcher with sensible connect/read timeouts and the
    /// [`DEFAULT_USER_AGENT`].
    pub fn new() -> Self {
        Self::with_user_agent(DEFAULT_USER_AGENT)
    }

    /// A fetcher that identifies itself with `user_agent`.
    pub fn with_user_agent(user_agent: impl Into<String>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(15))
            .timeout_read(Duration::from_secs(30))
            .build();
        UreqFetcher {
            agent,
            user_agent: user_agent.into(),
        }
    }

    /// Do the actual (blocking) work for one request.
    fn fetch_blocking(&self, req: Request) -> Result<Response, FetchError> {
        let url = req.url.as_str();
        if url.starts_with("http://") || url.starts_with("https://") {
            self.fetch_http(req)
        } else {
            // `file:` URL or a bare local path.
            fetch_file(&local_path(url))
        }
    }

    fn fetch_http(&self, req: Request) -> Result<Response, FetchError> {
        let method = match req.method {
            Method::Get => "GET",
            Method::Post => "POST",
        };

        let mut request = self.agent.request(method, &req.url);
        // A default UA first, so a caller-supplied `user-agent` header (if any)
        // overrides it below.
        request = request.set("User-Agent", &self.user_agent);
        for (name, value) in &req.headers {
            request = request.set(name, value);
        }

        let result = match &req.body {
            Some(body) => request.send_bytes(body.as_slice()),
            None => request.call(),
        };

        match result {
            // 2xx.
            Ok(resp) => response_from(resp),
            // Non-2xx: surface the real status as a `Response` and let the
            // caller (the source) decide — `doi.rs`/`bibfile.rs` check
            // `is_success()` themselves. `ureq` still hands us the body here.
            Err(ureq::Error::Status(_code, resp)) => response_from(resp),
            // Hard transport failure (DNS, connect, TLS, timeout): retryable.
            Err(ureq::Error::Transport(t)) => Err(FetchError::Transport(t.to_string())),
        }
    }
}

impl Default for UreqFetcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Fetcher for UreqFetcher {
    fn fetch(&self, req: Request) -> BoxFuture<'_, Result<Response, FetchError>> {
        // Blocking-in-a-future: the request runs when this future is *polled*
        // (mirrors `BlockingTimer::sleep`), so a blocking `block_on` drives it.
        Box::pin(async move { self.fetch_blocking(req) })
    }
}

/// Convert a `ureq::Response` into our [`Response`], lowercasing header names.
fn response_from(resp: ureq::Response) -> Result<Response, FetchError> {
    let status = resp.status();

    // Collect headers *before* consuming the response for its body.
    let mut out = Response {
        status,
        headers: Default::default(), // `hashbrown::HashMap`, inferred from field
        body: Vec::new(),
    };
    for name in resp.headers_names() {
        if let Some(value) = resp.header(&name) {
            out.headers
                .insert(name.to_ascii_lowercase(), value.to_string());
        }
    }

    let mut body = Vec::new();
    resp.into_reader()
        .read_to_end(&mut body)
        .map_err(|e| FetchError::Transport(format!("reading response body: {e}")))?;
    out.body = body;
    Ok(out)
}

/// Read a local file, returning a synthetic response.
///
/// A missing file becomes a `404` [`Response`] (mirroring HTTP "not found", so
/// the source's `is_success()` check reports it uniformly); any other I/O error
/// (permissions, …) is a retryable [`FetchError::Transport`].
fn fetch_file(path: &str) -> Result<Response, FetchError> {
    match std::fs::read(path) {
        Ok(body) => Ok(Response {
            status: 200,
            headers: Default::default(),
            body,
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Response {
            status: 404,
            headers: Default::default(),
            body: Vec::new(),
        }),
        Err(e) => Err(FetchError::Transport(format!("reading `{path}`: {e}"))),
    }
}

/// Turn a `file:` URL (or a bare path) into a filesystem path.
///
/// Handles `file:///abs`, `file://host/abs`, opaque `file:rel` / `file:/abs`,
/// and scheme-less paths. Percent escapes (e.g. `%20`) are decoded.
fn local_path(url: &str) -> String {
    let raw = if let Some(rest) = url.strip_prefix("file://") {
        // Authority form: drop everything up to the first path separator.
        match rest.find('/') {
            Some(idx) => &rest[idx..],
            None => rest,
        }
    } else if let Some(rest) = url.strip_prefix("file:") {
        // Opaque form: `file:/abs` or `file:relative`.
        rest
    } else {
        // Bare, scheme-less path.
        url
    };
    percent_decode(raw)
}

/// Minimal percent-decoding (`%XX` → byte), lossy on invalid UTF-8. Leaves an
/// isolated or malformed `%` untouched.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}
