//! `autocitefetch` — resolve a list of `prefix:key` citations to CSL-JSON.
//!
//! ```text
//! printf 'arxiv:1211.1037\ndoi:10.1103/PhysRev.47.777\n' | autocitefetch
//! ```
//!
//! A thin front end over [`CitationManager`] wired to the blocking
//! [`autocitefetch_std`] backends. Everything interesting — routing, chunking,
//! rate limiting, retry, chaining, the cache — lives in the libraries; this
//! crate only turns command-line flags into registered sources and a citation
//! list into a JSON array.
//!
//! Three shapes are worth knowing about when reading this file.
//!
//! **No async runtime.** Every `std` backend is *blocking-in-a-future*: it does
//! its work on the first poll and returns `Poll::Ready`. So the whole binary is
//! driven by a `Waker::noop()` poll loop ([`block_on`]) and pulls in no
//! executor — the same convention the library tests and the `resolve` example
//! follow.
//!
//! **Two-phase retrieval.** [`CitationManager::retrieve`] populates the cache
//! and returns only a failure report; [`CitationManager::get`] reads items back
//! (following arXiv → DOI chain pointers). A citation can therefore fail in two
//! places, and both are reported.
//!
//! **The cache is a directory of files, not one file.** The committable
//! `.citations.jsonl` is joined by `.citations.<writer>.log` sidecars, their
//! `.log.lock` companions, `.citations.lock` and `.citations.jsonl.tmp` — all
//! sharing the `.citations` base so a single `.citations*` gitignore line
//! covers everything but the file worth committing.

mod cli;
mod formats;
mod input;

use std::future::Future;
use std::io::Write;
use std::process::ExitCode;
use std::task::{Context, Poll, Waker};

use autocitefetch::source::{ArxivSource, BibliographyFileSource, DoiSource, ManualSource};
use autocitefetch::{CitationManager, CslValue};
use autocitefetch_std::{BlockingTimer, SingleFileCacheStore, SystemClock, UreqFetcher};
use clap::Parser;

use crate::cli::{Cli, SourceKind};

/// Program name used to prefix everything written to stderr.
const PROG: &str = "autocitefetch";

/// Every citation resolved.
const EXIT_OK: u8 = 0;
/// Some citation did not resolve; the ones that did are still written out.
const EXIT_INCOMPLETE: u8 = 1;
/// Nothing useful could be done: bad arguments, unreadable input, dead cache.
const EXIT_FATAL: u8 = 2;

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(0) => ExitCode::from(EXIT_OK),
        Ok(unresolved) => {
            warn(&format!(
                "{unresolved} citation(s) could not be resolved (see above)"
            ));
            ExitCode::from(EXIT_INCOMPLETE)
        }
        Err(msg) => {
            eprintln!("{PROG}: error: {msg}");
            ExitCode::from(EXIT_FATAL)
        }
    }
}

/// Do the whole job; return how many requested citations failed to resolve.
///
/// `Err` is reserved for the things that make the run pointless rather than
/// incomplete — unreadable input, an unopenable cache, an unwritable output. A
/// citation that cannot be fetched is *not* one of those: the report is printed
/// and the remaining items are still emitted.
fn run(cli: &Cli) -> Result<usize, String> {
    let cites = input::collect(cli)?;
    let sources = cli.enabled_sources();
    warn_about_unusable_options(cli, &cites, &sources);

    if cites.is_empty() {
        // Still valid: emit an empty array so a downstream `jq` sees
        // well-formed CSL-JSON rather than nothing at all.
        write_output(cli, &[])?;
        return Ok(0);
    }

    let manager = build_manager(cli, &sources)?;

    if cli.verbose {
        note(&format!(
            "resolving {} citation(s) via {}",
            cites.len(),
            if sources.is_empty() {
                "no sources".to_string()
            } else {
                sources
                    .iter()
                    .map(|s| s.prefix())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        ));
    }

    // Phase 1: fetch everything into the cache. Only a *store* failure is an
    // `Err`; per-citation failures come back in the report.
    let report = block_on(manager.retrieve(&cites)).map_err(|e| e.to_string())?;
    for failure in &report.failures {
        match &failure.origin {
            // A chained target that failed: name the citation the user actually
            // asked for, since `arxiv:…`'s DOI is not something they typed.
            Some((prefix, key)) if (prefix, key) != (&failure.prefix, &failure.key) => warn(
                &format!(
                    "{}:{}: {} (needed by {prefix}:{key})",
                    failure.prefix, failure.key, failure.message
                ),
            ),
            _ => warn(&format!(
                "{}:{}: {}",
                failure.prefix, failure.key, failure.message
            )),
        }
    }

    // Phase 2: read the items back, following chain pointers. `get` can fail
    // for a citation `retrieve` reported nothing about (a chain whose target
    // was dropped as authoritatively gone), so report those too.
    let mut items: Vec<CslValue> = Vec::with_capacity(cites.len());
    let mut emitted: Vec<String> = Vec::new();
    let mut unresolved = 0usize;
    for (prefix, key) in &cites {
        match block_on(manager.get(prefix, key)) {
            Ok(item) => {
                // `get` rewrites `id` to the *normalized* `prefix:key`, so two
                // spellings of one citation (`doi:10.1103/PhysRevA…` and
                // `doi:10.1103/physreva…`) collapse here rather than emitting
                // the same item twice.
                let id = item.get("id").and_then(|v| v.as_str()).unwrap_or_default();
                if id.is_empty() || !emitted.iter().any(|e| e == id) {
                    emitted.push(id.to_string());
                    items.push(item);
                }
            }
            Err(e) => {
                unresolved += 1;
                // Anything fetchable already produced a report line above; this
                // catches only what phase 2 adds, so don't second-guess whether
                // it is a duplicate — an unexplained missing item is worse.
                if !report
                    .failures
                    .iter()
                    .any(|f| &f.prefix == prefix && &f.key == key)
                {
                    warn(&format!("{prefix}:{key}: {e}"));
                }
            }
        }
    }

    if cli.verbose {
        note(&format!(
            "wrote {} item(s), {unresolved} unresolved",
            items.len()
        ));
    }
    write_output(cli, &items)?;
    Ok(unresolved)
}

/// Assemble the manager: `std` backends plus one registered source per enabled
/// [`SourceKind`].
fn build_manager(
    cli: &Cli,
    sources: &[SourceKind],
) -> Result<CitationManager<UreqFetcher, SingleFileCacheStore, SystemClock, BlockingTimer>, String> {
    // `.citations` rather than the library default `citations`: the cache lands
    // in the user's working directory, so every file it creates should be
    // hidden and cover-able by one `.citations*` ignore rule.
    let store = block_on(SingleFileCacheStore::with_base(
        &cli.cache_dir,
        &cli.cache_name,
    ))
    .map_err(|e| format!("cache in {}: {e}", cli.cache_dir.display()))?;

    let fetcher = match &cli.user_agent {
        Some(ua) => UreqFetcher::with_user_agent(ua.clone()),
        None => UreqFetcher::new(),
    };

    // A prefix is a *binding* the manager holds, not something a source
    // declares, so this is where the CLI's fixed `arxiv:`/`doi:`/`bib:`/
    // `manual:` vocabulary is decided — `SourceKind::prefix` is the whole of it.
    let mut manager = CitationManager::new(fetcher, store, SystemClock, BlockingTimer);
    for kind in sources {
        let prefix = kind.prefix();
        manager = match kind {
            SourceKind::Arxiv => manager.register(prefix, build_arxiv(cli)?),
            SourceKind::Doi => manager.register(prefix, DoiSource::new()),
            SourceKind::Bib => manager.register(
                prefix,
                BibliographyFileSource::new(cli.bib.iter().cloned())
                    .with_parser(formats::parser_for(cli.bib_format)),
            ),
            SourceKind::Manual => manager.register(prefix, ManualSource::new()),
        }
        .map_err(|e| format!("registering the `{prefix}` source: {e}"))?;
    }
    Ok(manager)
}

/// The `arxiv` source, with `--no-arxiv-chaining` and `--arxiv-doi-overrides`
/// applied. The override file is parsed *here* and passed as data, which is
/// what lets it be YAML — see [`formats`].
///
/// DOI chaining targets `SourceKind::Doi`'s prefix explicitly: the source has
/// no way to guess what the host called its DOI source, so the two ends of the
/// chain are named in one place.
fn build_arxiv(cli: &Cli) -> Result<ArxivSource, String> {
    let doi_prefix = (!cli.no_arxiv_chaining).then(|| SourceKind::Doi.prefix());
    let mut source = ArxivSource::new().chain_dois_to(doi_prefix);
    if let Some(path) = &cli.arxiv_doi_overrides {
        source = source.with_override_dois(formats::load_doi_overrides(path)?);
    }
    Ok(source)
}

/// Point out options that will quietly do nothing, and citations that cannot
/// possibly resolve, *before* spending a rate-limited fetch round on them.
fn warn_about_unusable_options(cli: &Cli, cites: &[(String, String)], sources: &[SourceKind]) {
    let enabled = |k: SourceKind| sources.contains(&k);
    let requested = input::prefixes(cites);

    if !cli.bib.is_empty() && !enabled(SourceKind::Bib) {
        warn("the `bib` source is disabled; --bib file(s) will be ignored");
    }
    if cli.bib.is_empty() && enabled(SourceKind::Bib) && requested.contains(&"bib") {
        warn("`bib:` citations were requested but no --bib file was given");
    }
    if !enabled(SourceKind::Arxiv)
        && (cli.arxiv_doi_overrides.is_some() || cli.no_arxiv_chaining)
    {
        warn("the `arxiv` source is disabled; its options will be ignored");
    }
    for prefix in requested {
        if !sources.iter().any(|s| s.prefix() == prefix) {
            warn(&format!(
                "no enabled source for prefix `{prefix}`; those citations will fail"
            ));
        }
    }
}

/// Serialize `items` as a CSL-JSON array to `--output`, or to stdout.
fn write_output(cli: &Cli, items: &[CslValue]) -> Result<(), String> {
    let mut json = if cli.compact {
        serde_json::to_string(items)
    } else {
        serde_json::to_string_pretty(items)
    }
    .map_err(|e| format!("serializing output: {e}"))?;
    json.push('\n');

    match &cli.output {
        Some(path) => {
            std::fs::write(path, json).map_err(|e| format!("{}: {e}", path.display()))
        }
        None => std::io::stdout()
            .write_all(json.as_bytes())
            .map_err(|e| format!("<stdout>: {e}")),
    }
}

/// A diagnostic that does not stop the run.
///
/// stderr, always: stdout carries the CSL-JSON and nothing else, so the tool
/// stays pipeable even when half the citations failed.
fn warn(msg: &str) {
    eprintln!("{PROG}: {msg}");
}

/// Progress chatter, emitted only under `--verbose`. Same channel as [`warn`]
/// — the distinction is which call sites are conditional, not where it goes.
fn note(msg: &str) {
    warn(msg);
}

/// Drive a future to completion on this thread, with no async runtime.
///
/// Every backend used here resolves on the first poll (`UreqFetcher` blocks the
/// thread, `BlockingTimer` sleeps it, the cache store does blocking I/O), so a
/// no-op waker suffices. The poll budget is the safety net: with `Waker::noop()`
/// nothing can wake this loop, so a future that genuinely pends would spin
/// forever — fail loudly instead of hanging.
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::thread::yield_now();
    }
    panic!("future did not complete (a backend unexpectedly pended — this driver only works with blocking backends)");
}
