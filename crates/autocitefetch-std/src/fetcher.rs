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
//! [`Fetcher`] with `reqwest`/`hyper` instead. See
//! [`BlockingTimer`](crate::BlockingTimer) for what that costs in concurrency.
//!
//! In addition to `http(s)://` URLs it resolves `file:` URLs (and bare,
//! scheme-less paths) by reading the local filesystem, returning a synthetic
//! `200` response. That is what lets
//! [`BibliographyFileSource`](autocitefetch::source::BibliographyFileSource)
//! load local `.json` bibliographies through the one `Fetcher` choke point.
//!
//! # Error classification
//!
//! [`FetchError::Transport`] is *retryable* — the core's retry layer will
//! re-issue it up to 5 times with backoff, sleeping this thread for ~16 s in
//! total. So only genuinely transient failures are reported as `Transport`;
//! everything that will fail identically on the next attempt (a malformed URL,
//! an unknown scheme, a broken proxy config, a directory where a file was
//! expected) is reported as the non-retryable [`FetchError::Other`].

use std::io::Read;
use std::path::{Path, PathBuf};
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
/// See the [module docs](self) for the blocking-in-a-future contract and the
/// retryable/permanent error split.
pub struct UreqFetcher {
    agent: ureq::Agent,
}

impl UreqFetcher {
    /// A fetcher with sensible timeouts and the [`DEFAULT_USER_AGENT`].
    pub fn new() -> Self {
        Self::with_user_agent(DEFAULT_USER_AGENT)
    }

    /// A fetcher that identifies itself with `user_agent`.
    ///
    /// A per-request `user-agent` header still wins: ureq only emits the
    /// agent-level default when the request carries no `user-agent` of its
    /// own, and *that* check is case-insensitive — unlike its per-request
    /// header dedup, which is why the default is set here on the agent rather
    /// than on each request.
    pub fn with_user_agent(user_agent: impl Into<String>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .user_agent(&user_agent.into())
            .timeout_connect(Duration::from_secs(15))
            // `timeout_read`/`timeout_write` are per-syscall socket timeouts:
            // a server dripping one byte every 29 s would keep `read_to_end`
            // alive forever. `timeout` is the overall deadline for the whole
            // request, and it is the only thing that can stop that — there is
            // no async runtime here, so nothing else could cancel it.
            .timeout_read(Duration::from_secs(30))
            .timeout_write(Duration::from_secs(30))
            .timeout(Duration::from_secs(120))
            // Off by default in ureq 2. Without it, `HTTP(S)_PROXY` /
            // `ALL_PROXY` are ignored and every fetch on a corporate or
            // campus network fails as an opaque `ConnectionFailed`.
            .try_proxy_from_env(true)
            .build();
        UreqFetcher { agent }
    }

    /// Do the actual (blocking) work for one request.
    fn fetch_blocking(&self, req: Request) -> Result<Response, FetchError> {
        match target(&req.url)? {
            Target::Http => self.fetch_http(req),
            Target::File(path) => fetch_file(&path),
        }
    }

    /// Translate our [`Request`] into a `ureq` one (headers included, body not).
    ///
    /// The `User-Agent` default is deliberately **not** set here: ureq's
    /// per-request header dedup compares names byte-for-byte while the core
    /// lowercases them, so setting `User-Agent` here and letting a caller pass
    /// `user-agent` would put **both** on the wire (`User-Agent` is a
    /// singleton field per RFC 9110, and the caller's value would lose the
    /// lookup). It lives on the agent instead, where ureq's "does the request
    /// already carry one?" check *is* case-insensitive.
    fn build_request(&self, req: &Request) -> ureq::Request {
        let method = match req.method {
            Method::Get => "GET",
            Method::Post => "POST",
        };
        let mut request = self.agent.request(method, &req.url);
        for (name, value) in &req.headers {
            request = request.set(name, value);
        }
        request
    }

    fn fetch_http(&self, req: Request) -> Result<Response, FetchError> {
        let request = self.build_request(&req);

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
            // Everything that is not a status: split retryable from permanent.
            Err(ureq::Error::Transport(t)) => Err(transport_error(t)),
        }
    }
}

/// Classify a ureq transport failure. ureq 2 funnels *every* non-status
/// failure into `Error::Transport`, including several that can never succeed
/// on a retry; reporting those as [`FetchError::Transport`] costs 5 pointless
/// re-issues and ~16 s of `thread::sleep` before the inevitable failure.
fn transport_error(t: ureq::Transport) -> FetchError {
    use ureq::ErrorKind::*;
    let msg = t.to_string();
    match t.kind() {
        // Transient: the network may behave differently in a moment.
        Dns | ConnectionFailed | Io | ProxyConnect => FetchError::Transport(msg),
        // Permanent: bad input or bad configuration, identical every time.
        InvalidUrl | UnknownScheme | TooManyRedirects | BadStatus | BadHeader
        | InsecureRequestHttpsOnly | InvalidProxyUrl | ProxyUnauthorized => FetchError::Other(msg),
        // `HTTP` arrives as `Error::Status`, never here. Any kind a future
        // ureq 2.x adds: don't retry blindly.
        _ => FetchError::Other(msg),
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
        // A truncated body *is* worth another attempt.
        .map_err(|e| FetchError::Transport(format!("reading response body: {e}")))?;
    out.body = body;
    Ok(out)
}

/// Read a local file, returning a synthetic response.
///
/// A missing file becomes a `404` [`Response`] (mirroring HTTP "not found", so
/// the source's `is_success()` check reports it uniformly, and
/// `BibliographyFileSource` can tell "no such file" from "unreadable file").
/// Every other I/O error — permission denied, a directory where a file was
/// expected, a bad symlink — is a **non-retryable** [`FetchError::Other`]: no
/// local `read` failure fixes itself within a backoff window.
fn fetch_file(path: &Path) -> Result<Response, FetchError> {
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
        Err(e) => Err(FetchError::Other(format!(
            "reading `{}`: {e}",
            path.display()
        ))),
    }
}

/// What a request URL points at.
#[derive(Debug)]
enum Target {
    Http,
    File(PathBuf),
}

/// Classify a request URL into an HTTP request or a local path.
///
/// Accepts `http`/`https` (any case), `file:` in all its forms, and bare
/// scheme-less filesystem paths. Any *other* scheme is an error rather than a
/// path: silently reading `fs::read("HTTPS://doi.org/…")` and reporting the
/// resulting 404 as "file not found" is actively misleading.
fn target(url: &str) -> Result<Target, FetchError> {
    match url_scheme(url) {
        Some(s) if s.eq_ignore_ascii_case("http") || s.eq_ignore_ascii_case("https") => {
            Ok(Target::Http)
        }
        Some(s) if s.eq_ignore_ascii_case("file") => Ok(Target::File(file_url_path(url))),
        Some(s) => Err(FetchError::Other(format!(
            "unsupported URL scheme `{s}:` in `{url}` \
             (expected http, https, file, or a bare filesystem path)"
        ))),
        // No scheme: a bare filesystem path, taken **verbatim**. No
        // percent-decoding here — a real directory named `real%20dir` must not
        // silently resolve to `real dir`.
        None => Ok(Target::File(PathBuf::from(url))),
    }
}

/// The URL scheme, if `url` starts with one (RFC 3986 `ALPHA *(ALPHA / DIGIT /
/// "+" / "-" / ".") ":"`).
///
/// A one-character scheme is deliberately not recognised: `C:\refs.json` is a
/// Windows drive path, not a `c:` URL, and no real scheme is one letter.
fn url_scheme(url: &str) -> Option<&str> {
    let idx = url.find(':')?;
    let scheme = &url[..idx];
    if scheme.len() < 2 {
        return None;
    }
    let mut chars = scheme.chars();
    if !chars.next()?.is_ascii_alphabetic() {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')) {
        return None;
    }
    Some(scheme)
}

/// Turn a `file:` URL into a filesystem path.
///
/// Handles `file:///abs`, `file://host/abs`, and the opaque `file:/abs` /
/// `file:rel` forms. Percent escapes (`%20`, …) are decoded — and decoded to
/// *bytes*, not to a lossy `String`: carrying non-UTF-8 path bytes is the
/// whole point of percent-encoding a `file:` URL.
fn file_url_path(url: &str) -> PathBuf {
    // Caller guarantees a `file:` scheme (any case).
    let rest = &url["file:".len()..];
    let raw = if let Some(after) = rest.strip_prefix("//") {
        // Authority form. The host part is normally empty (`file:///abs`);
        // whatever it is, the path starts at the first `/`.
        match after.find('/') {
            Some(idx) => &after[idx..],
            // `file://relative` — no path component at all; be lenient and
            // treat the authority as the path.
            None => after,
        }
    } else {
        // Opaque form: `file:/abs` or `file:relative`.
        rest
    };
    path_from_bytes(strip_windows_drive_slash(percent_decode(raw)))
}

/// `file:///C:/Users/x` decodes to `/C:/Users/x`, which no Windows API
/// accepts. Drop the leading slash before a drive letter.
#[cfg(windows)]
fn strip_windows_drive_slash(p: Vec<u8>) -> Vec<u8> {
    if p.len() >= 3 && p[0] == b'/' && p[1].is_ascii_alphabetic() && p[2] == b':' {
        p[1..].to_vec()
    } else {
        p
    }
}

#[cfg(not(windows))]
fn strip_windows_drive_slash(p: Vec<u8>) -> Vec<u8> {
    p
}

/// Bytes → path. On unix this is exact (paths *are* bytes); elsewhere there is
/// no lossless conversion, so fall back to a lossy decode.
#[cfg(unix)]
fn path_from_bytes(bytes: Vec<u8>) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    PathBuf::from(std::ffi::OsString::from_vec(bytes))
}

#[cfg(not(unix))]
fn path_from_bytes(bytes: Vec<u8>) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(&bytes).into_owned())
}

/// Minimal percent-decoding (`%XX` → byte). Leaves an isolated or malformed
/// `%` untouched. Returns bytes: see [`path_from_bytes`].
fn percent_decode(s: &str) -> Vec<u8> {
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
    out
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The path a URL resolves to, or a panic if it is not a file target.
    fn path_of(url: &str) -> PathBuf {
        match target(url).expect("should be a valid target") {
            Target::File(p) => p,
            Target::Http => panic!("`{url}` unexpectedly classified as HTTP"),
        }
    }

    fn is_http(url: &str) -> bool {
        matches!(target(url), Ok(Target::Http))
    }

    // --- URL classification -------------------------------------------------

    #[test]
    fn http_and_https_are_detected_case_insensitively() {
        assert!(is_http("http://example.org/a"));
        assert!(is_http("https://doi.org/10.1/x"));
        assert!(is_http("HTTPS://doi.org/10.1/x"));
        assert!(is_http("HtTp://example.org/a"));
    }

    #[test]
    fn an_unknown_scheme_is_an_error_not_a_local_path() {
        // Regression: this used to become `fs::read("ftp://…")` → a synthetic
        // 404, which the bib source then reported as "file returned status 404".
        let err = target("ftp://example.org/refs.json").expect_err("should be rejected");
        assert!(
            matches!(err, FetchError::Other(_)),
            "an unsupported scheme is permanent, got {err:?}"
        );
        assert!(!err.is_retryable());
        assert!(format!("{err}").contains("ftp"), "message names the scheme");
    }

    // --- `file:` and bare paths ---------------------------------------------

    #[test]
    fn file_url_forms_all_resolve() {
        assert_eq!(path_of("file:///abs/refs.json"), PathBuf::from("/abs/refs.json"));
        assert_eq!(path_of("file://localhost/abs/refs.json"), PathBuf::from("/abs/refs.json"));
        assert_eq!(path_of("file:/abs/refs.json"), PathBuf::from("/abs/refs.json"));
        assert_eq!(path_of("file:rel/refs.json"), PathBuf::from("rel/refs.json"));
        assert_eq!(path_of("FILE:///abs/refs.json"), PathBuf::from("/abs/refs.json"));
    }

    #[test]
    fn bare_paths_pass_through_unchanged() {
        assert_eq!(path_of("/abs/refs.json"), PathBuf::from("/abs/refs.json"));
        assert_eq!(path_of("rel/refs.json"), PathBuf::from("rel/refs.json"));
    }

    #[test]
    fn percent_escapes_are_decoded_only_in_file_urls() {
        assert_eq!(path_of("file:///a/my%20refs.json"), PathBuf::from("/a/my refs.json"));
        // A bare path is verbatim: a directory really named `real%20dir` must
        // not silently become `real dir`.
        assert_eq!(path_of("/a/real%20dir/refs.json"), PathBuf::from("/a/real%20dir/refs.json"));
    }

    #[test]
    fn malformed_percent_escapes_are_left_alone() {
        assert_eq!(path_of("file:///a/100%.json"), PathBuf::from("/a/100%.json"));
        assert_eq!(path_of("file:///a/%zz.json"), PathBuf::from("/a/%zz.json"));
        // Truncated escape at the very end.
        assert_eq!(path_of("file:///a/x%2"), PathBuf::from("/a/x%2"));
    }

    #[test]
    #[cfg(unix)]
    fn non_utf8_escapes_survive_as_path_bytes() {
        use std::os::unix::ffi::OsStrExt;
        // The point of percent-encoding a `file:` URL is to carry bytes that
        // are not valid UTF-8; a lossy `String` conversion would turn this
        // into U+FFFD and address a different file.
        let p = path_of("file:///bad%FFbyte");
        assert_eq!(p.as_os_str().as_bytes(), b"/bad\xFFbyte");
    }

    #[test]
    #[cfg(windows)]
    fn windows_drive_letter_forms_resolve() {
        assert_eq!(
            path_of("file:///C:/Users/x/refs.json"),
            PathBuf::from("C:/Users/x/refs.json")
        );
        // A bare drive path is not a one-letter URL scheme.
        assert_eq!(path_of(r"C:\Users\x\refs.json"), PathBuf::from(r"C:\Users\x\refs.json"));
    }

    #[test]
    fn a_one_letter_scheme_is_treated_as_a_path() {
        // Same rule on every platform, so it is testable everywhere.
        assert_eq!(path_of("c:/Users/x/refs.json"), PathBuf::from("c:/Users/x/refs.json"));
    }

    #[test]
    fn percent_decode_returns_raw_bytes() {
        assert_eq!(percent_decode("a%20b"), b"a b".to_vec());
        assert_eq!(percent_decode("%FF"), vec![0xFF]);
        assert_eq!(percent_decode("plain"), b"plain".to_vec());
    }

    // --- local file taxonomy -------------------------------------------------

    #[test]
    fn a_missing_file_is_a_404_response_not_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let resp = fetch_file(&dir.path().join("nope.json")).expect("missing file is not an Err");
        assert_eq!(resp.status, 404);
        assert!(!resp.is_success());
    }

    #[test]
    fn an_existing_file_is_a_200_response() {
        let dir = tempfile::tempdir().expect("temp dir");
        let p = dir.path().join("refs.json");
        std::fs::write(&p, b"[]").expect("write");
        let resp = fetch_file(&p).expect("readable file");
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"[]");
    }

    #[test]
    fn a_directory_is_a_non_retryable_error() {
        // Pointing a bib source at a directory is an easy mistake; it used to
        // cost ~16 s of blocked-thread retries before failing anyway.
        let dir = tempfile::tempdir().expect("temp dir");
        let err = fetch_file(dir.path()).expect_err("a directory is not readable as a file");
        assert!(
            !err.is_retryable(),
            "a directory never becomes a file: {err:?}"
        );
    }

    // --- request construction -------------------------------------------------

    #[test]
    fn the_default_user_agent_is_not_written_onto_the_request() {
        // It belongs to the agent; ureq emits it at wire time only when the
        // request carries none. Writing it here too used to send *two*
        // `User-Agent` headers (ureq dedups request headers by exact name,
        // and the core lowercases them).
        let f = UreqFetcher::with_user_agent("mine/1.0");
        let built = f.build_request(&Request::get("https://example.org/"));
        assert!(
            built.all("user-agent").is_empty(),
            "the default UA must not be a request header: {:?}",
            built.all("user-agent")
        );
    }

    #[test]
    fn a_caller_supplied_user_agent_is_the_only_one() {
        let f = UreqFetcher::with_user_agent("mine/1.0");
        // `Request::header` lowercases, so this is exactly what a source sends.
        let req = Request::get("https://example.org/").header("User-Agent", "caller/2.0");
        let built = f.build_request(&req);
        assert_eq!(built.all("user-agent"), vec!["caller/2.0"]);
    }

    #[test]
    fn request_headers_reach_the_wire_request() {
        let f = UreqFetcher::new();
        let req = Request::get("https://doi.org/10.1/x")
            .header("accept", "application/vnd.citationstyles.csl+json");
        let built = f.build_request(&req);
        assert_eq!(
            built.header("Accept"),
            Some("application/vnd.citationstyles.csl+json")
        );
    }

    // --- response conversion --------------------------------------------------

    #[test]
    fn a_non_success_status_arrives_as_a_response_not_an_error() {
        // The single most load-bearing contract of this backend: sources check
        // `is_success()` themselves, so statuses must not be swallowed as
        // `FetchError::Status`.
        let raw = ureq::Response::new(429, "Too Many Requests", "slow down").expect("build");
        let resp = response_from(raw).expect("a 429 is Ok(Response), not Err");
        assert_eq!(resp.status, 429);
        assert!(!resp.is_success());
        assert_eq!(resp.text().unwrap(), "slow down");
    }

    #[test]
    fn header_names_are_lowercased_and_retry_after_is_retrievable() {
        let raw: ureq::Response =
            "HTTP/1.1 503 Service Unavailable\r\nRetry-After: 3\r\nContent-Type: text/plain\r\n\r\nnope"
                .parse()
                .expect("parse response");
        let resp = response_from(raw).expect("convert");
        assert_eq!(resp.status, 503);
        // Stored lowercased …
        assert_eq!(resp.headers.get("retry-after").map(String::as_str), Some("3"));
        // … and retrievable however the retry layer spells it.
        assert_eq!(resp.header("Retry-After"), Some("3"));
        assert_eq!(resp.header("retry-after"), Some("3"));
        assert_eq!(resp.header("content-type"), Some("text/plain"));
    }
}
