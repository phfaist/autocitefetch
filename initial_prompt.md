Get me started on a new project in this empty folder.  It's a rust library (no_std with alloc) that handles automatic retrieval of bibliographic citations from multiple sources.  Retrieval of URLs and reading/writing to cache happens through generics, with user-provided implementations.

- The library is meant to be paired with document processing systems, say a latex-like document with `\cite{arXiv:1211.1037}` type commands, to automatically retrieve the bibliographic information from the relevant API (arXiv.org, doi.org, ...).

- The library stores retrieved information in a standard format, chosen to be CSL-JSON.

- The library is no_std to enable its use in a Web App (where we could use a browser-fs for caching and the browser's native fetch() for URL retrieval).

I've already written two versions of such a library:
- a JS version in $HOME/Research/projects/zoodb/zoodb/src/citationmanager
- a python version in $HOME/Research/util/flm-citations/flm_citations/ (plugs into FLM's machinery, but don't try to understand that because it's irrelevant here).

The goal is to write a single, solid Rust version that can be used in either cases.  It should replicate the structure and features of those two code bases.  (Those two follow the same structure.)  The new project can include additional features with respect to the two existing libraries.

Citation lookup goes by pair (prefix, key).  The prefix determines which
"source" to use.  A source = provider for bibliographic info, e.g., accessor for the arXiv API, doi.org, a manually-typed citation, a key in a local bibliography file, etc.

Library features:
- Easily extensible; users can add their own sources
- Advanced cache management (re-fetches if cache expires; but don't do so too aggressively as to re-download all entries at once if they all expire simultaneously; gracefully use mildly outdated entries if connection to an API server fails; etc.)

A separate crate within the same repo here provides implementations for "fetcher"s and "I/O for cache" using rust's std standard library.  Can be used by users who don't target a web app.

Suggest a library structure and go through design decisions interactively.
