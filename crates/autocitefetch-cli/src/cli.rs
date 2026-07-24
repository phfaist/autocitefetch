//! Command-line surface: the [`Cli`] struct clap derives the parser from, plus
//! the small enums its value-taking options accept.
//!
//! Everything here is declarative; the wiring lives in [`crate::run`].

use std::path::PathBuf;

use clap::{Parser, ValueEnum};

/// One of the built-in sources, as named on the command line.
///
/// The variants are exactly the built-in prefixes, so `--enable arxiv` reads
/// the same as the `arxiv:` in a citation list.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum SourceKind {
    /// `arxiv:2301.00001` — the arXiv Atom API (chains to `doi` by default).
    Arxiv,
    /// `doi:10.1103/PhysRev.47.777` — doi.org content negotiation.
    Doi,
    /// `bib:knuth1984` — the `--bib` bibliography files.
    Bib,
    /// `manual:Einstein, A. (1935)` — the key *is* the formatted citation text.
    Manual,
}

impl SourceKind {
    /// Every source, in the order they are listed in `--help`.
    pub const ALL: [SourceKind; 4] = [
        SourceKind::Arxiv,
        SourceKind::Doi,
        SourceKind::Bib,
        SourceKind::Manual,
    ];

    /// The citation prefix this source answers to.
    pub fn prefix(self) -> &'static str {
        match self {
            SourceKind::Arxiv => "arxiv",
            SourceKind::Doi => "doi",
            SourceKind::Bib => "bib",
            SourceKind::Manual => "manual",
        }
    }
}

/// How to parse a `--bib` file or the `--arxiv-doi-overrides` file.
#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum Format {
    /// Try JSON first, fall back to YAML. YAML is *almost* a JSON superset,
    /// so this really does try both rather than just running the YAML parser.
    Auto,
    Json,
    Yaml,
}

/// Resolve `prefix:key` citations to CSL-JSON.
#[derive(Parser, Debug)]
#[command(
    name = "autocitefetch",
    version,
    about = "Resolve `prefix:key` citations to CSL-JSON.",
    long_about = "\
Reads a list of citations — one `prefix:key` per line — and writes the resolved
CSL-JSON to stdout as a JSON array.

Input comes from the FILE arguments, from repeated --cite options, or, when
neither is given, from standard input. In a list file, blank lines and lines
starting with `#` are ignored and surrounding whitespace is stripped; the line
is split at its *first* colon, so a `manual:` key may itself contain colons.

Resolved entries are cached in `.citations.jsonl` in the current directory (see
--cache-dir / --cache-name), alongside the `._.citations*` sidecar, lock and temp
files the cache needs. Only `.citations.jsonl` is worth committing to version
control; gitignoring `._.citations*` covers everything else.

Exit status: 0 if every citation resolved, 1 if some did not (the rest are
still written out), 2 on a fatal error.",
    after_help = "\
Examples:
  printf 'arxiv:1211.1037\\ndoi:10.1103/PhysRev.47.777\\n' | autocitefetch
  autocitefetch cites.txt --disable manual -o refs.json
  autocitefetch --cite bib:knuth1984 --bib refs.yaml --enable bib"
)]
pub struct Cli {
    /// Files listing citations, one `prefix:key` per line (`-` reads stdin).
    ///
    /// With no FILE and no --cite, the list is read from stdin.
    #[arg(value_name = "FILE")]
    pub input: Vec<PathBuf>,

    /// Resolve this citation too, given inline. Repeatable.
    #[arg(short = 'c', long = "cite", value_name = "PREFIX:KEY")]
    pub cite: Vec<String>,

    /// Write the CSL-JSON here instead of to stdout.
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Emit the JSON array on one line instead of pretty-printed.
    #[arg(long)]
    pub compact: bool,

    /// Enable only these sources. Repeatable; default is all of them.
    #[arg(long = "enable", value_name = "SOURCE", value_enum)]
    pub enable: Vec<SourceKind>,

    /// Disable these sources. Repeatable; applied after --enable.
    ///
    /// A citation whose prefix has no enabled source is reported as a failure
    /// rather than silently skipped.
    #[arg(long = "disable", value_name = "SOURCE", value_enum)]
    pub disable: Vec<SourceKind>,

    /// Bibliography file (or URL) for the `bib` source. Repeatable; on a
    /// duplicate id the later file wins.
    ///
    /// JSON or YAML, holding either an array of CSL items (each with an `id`)
    /// or a mapping of id to item.
    #[arg(short, long = "bib", value_name = "FILE")]
    pub bib: Vec<String>,

    /// How to parse --bib files.
    #[arg(long, value_name = "FORMAT", value_enum, default_value_t = Format::Auto)]
    pub bib_format: Format,

    /// JSON or YAML mapping of arXiv id to DOI, overriding what the feed
    /// reports. A `null` value *suppresses* the DOI: the entry keeps its arXiv
    /// metadata and is not chained to doi.org.
    #[arg(long, value_name = "FILE")]
    pub arxiv_doi_overrides: Option<PathBuf>,

    /// Keep arXiv metadata instead of chaining resolved arXiv entries to
    /// doi.org.
    #[arg(long)]
    pub no_arxiv_chaining: bool,

    /// Markup format the `manual:` citation texts are written in.
    ///
    /// A manual entry is emitted as `{"_ready_formatted": {"<NAME>": "<text>"}}`,
    /// so this is what tells a downstream renderer how to read the text.
    /// Defaults to `flm`.
    #[arg(long, value_name = "NAME")]
    pub manual_format: Option<String>,

    /// Drop this top-level CSL field from every entry. Repeatable.
    ///
    /// Applied before an entry is cached, so bulky fields nobody cites —
    /// `reference` (doi.org returns the paper's entire bibliography) or
    /// `abstract` — stay out of both the output and `.citations.jsonl`.
    /// Entries already cached keep their fields until they are refetched.
    #[arg(long = "drop-field", value_name = "FIELD")]
    pub drop_field: Vec<String>,

    /// Directory holding the cache files.
    #[arg(long, value_name = "DIR", default_value = ".")]
    pub cache_dir: PathBuf,

    /// Base name of the cache files (`<NAME>.jsonl`, plus `._<NAME>.lock`, …).
    #[arg(long, value_name = "NAME", default_value = ".citations")]
    pub cache_name: String,

    /// `User-Agent` sent with every HTTP request. doi.org and arXiv ask for one
    /// that identifies you (an e-mail address is customary).
    #[arg(long, value_name = "STRING")]
    pub user_agent: Option<String>,

    /// Report progress on stderr. Repeatable.
    ///
    /// `-v` reports milestones: each retrieval pass, per-source progress, and
    /// any wait long enough to look like a hang (rate-limit pacing, retry
    /// backoff, cache compaction). `-vv` adds one line per HTTP request and per
    /// resolved citation, and stops throttling the progress counters.
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

/// Format name given to [`ManualSource`](autocitefetch::source::ManualSource)
/// when `--manual-format` is not passed: what the JS reference hard-codes.
pub const DEFAULT_MANUAL_FORMAT: &str = "flm";

impl Cli {
    /// The `--manual-format` value, or [`DEFAULT_MANUAL_FORMAT`].
    ///
    /// The flag is an `Option` rather than a clap `default_value` so that
    /// `warn_about_unusable_options` can tell "asked for" from "not mentioned"
    /// and warn when the `manual` source is disabled.
    pub fn manual_format(&self) -> &str {
        self.manual_format.as_deref().unwrap_or(DEFAULT_MANUAL_FORMAT)
    }

    /// The sources to register: all of them, narrowed by `--enable`, then by
    /// `--disable`.
    pub fn enabled_sources(&self) -> Vec<SourceKind> {
        let base: &[SourceKind] = if self.enable.is_empty() {
            &SourceKind::ALL
        } else {
            &self.enable
        };
        base.iter()
            .copied()
            // Preserve `ALL` order and drop the duplicates a repeated
            // `--enable doi --enable doi` would otherwise produce (registering
            // the same prefix twice is a silent shadowing, not an error).
            .filter(|s| !self.disable.contains(s))
            .fold(Vec::new(), |mut acc, s| {
                if !acc.contains(&s) {
                    acc.push(s);
                }
                acc
            })
    }
}
