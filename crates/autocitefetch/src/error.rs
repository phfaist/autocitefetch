//! Crate error type.

use alloc::string::String;
use core::fmt;

use crate::fetch::FetchError;
use crate::store::StoreError;

/// Convenience alias for results in this crate.
pub type Result<T> = core::result::Result<T, Error>;

/// Anything that can go wrong resolving or reading a citation.
///
/// `Clone`, because a single failure often has to be attributed to every key of
/// a batch (see the bibliography source) and because failures are copied into
/// [`CiteFailure`](crate::manager::CiteFailure)-shaped reports.
#[derive(Clone, Debug)]
pub enum Error {
    /// No source is registered for the given citation prefix.
    UnknownPrefix(String),
    /// A source was registered under a prefix that cannot form an unambiguous
    /// `"prefix:key"` id: it is empty, or it contains a `':'`. Citation ids are
    /// split on the first colon, so such a prefix would collide in the cache
    /// (`("a", "b:c")` vs `("a:b", "c")`) and break the `get_by_id` round-trip.
    InvalidPrefix(String),
    /// A citation id is malformed (e.g. it has no `"prefix:key"` separator).
    InvalidId(String),
    /// A key was requested but never resolved (and no cached copy exists).
    NotFound(String),
    /// A chain of citation pointers could not be followed: the target is
    /// missing, it points at itself, or the chain is longer than
    /// `max_chain_depth`.
    Chain(String),
    /// The underlying URL fetch failed.
    Fetch(FetchError),
    /// The cache backend failed.
    Store(StoreError),
    /// A response could not be parsed into the expected shape.
    Parse(String),
    /// A source-specific failure (bad id, API error, missing bib key, …).
    Source(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownPrefix(p) => write!(f, "no source registered for prefix `{p}`"),
            Error::InvalidPrefix(p) => {
                write!(f, "invalid source prefix `{p}`: must be non-empty and contain no ':'")
            }
            Error::InvalidId(id) => {
                write!(f, "malformed citation id `{id}`: expected `prefix:key`")
            }
            Error::NotFound(id) => write!(f, "citation `{id}` not found"),
            Error::Chain(m) => write!(f, "citation chain error: {m}"),
            Error::Fetch(e) => write!(f, "fetch error: {e}"),
            Error::Store(e) => write!(f, "cache store error: {e}"),
            Error::Parse(m) => write!(f, "parse error: {m}"),
            Error::Source(m) => write!(f, "source error: {m}"),
        }
    }
}

impl core::error::Error for Error {
    /// Expose the wrapped backend error so `anyhow`/`eyre`-style reporters can
    /// print the underlying cause instead of only this crate's message.
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Error::Fetch(e) => Some(e),
            Error::Store(e) => Some(e),
            Error::UnknownPrefix(_)
            | Error::InvalidPrefix(_)
            | Error::InvalidId(_)
            | Error::NotFound(_)
            | Error::Chain(_)
            | Error::Parse(_)
            | Error::Source(_) => None,
        }
    }
}

impl From<FetchError> for Error {
    fn from(e: FetchError) -> Self {
        Error::Fetch(e)
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Error::Store(e)
    }
}
