//! Crate error type.

use alloc::string::String;
use core::fmt;

use crate::fetch::FetchError;
use crate::store::StoreError;

/// Convenience alias for results in this crate.
pub type Result<T> = core::result::Result<T, Error>;

/// Anything that can go wrong resolving or reading a citation.
#[derive(Debug)]
pub enum Error {
    /// No source is registered for the given citation prefix.
    UnknownPrefix(String),
    /// A key was requested but never resolved (and no cached copy exists).
    NotFound(String),
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
            Error::NotFound(id) => write!(f, "citation `{id}` not found"),
            Error::Fetch(e) => write!(f, "fetch error: {e}"),
            Error::Store(e) => write!(f, "cache store error: {e}"),
            Error::Parse(m) => write!(f, "parse error: {m}"),
            Error::Source(m) => write!(f, "source error: {m}"),
        }
    }
}

impl core::error::Error for Error {}

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
