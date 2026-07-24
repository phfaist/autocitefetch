//! Turning the command line and its citation-list files into the
//! `&[(prefix, key)]` slice [`CitationManager::retrieve`] takes.
//!
//! [`CitationManager::retrieve`]: autocitefetch::CitationManager::retrieve

use std::io::Read;
use std::path::Path;

use crate::cli::Cli;

/// Split one `prefix:key` spec.
///
/// Split at the **first** colon, and only the prefix is trimmed: a `manual:`
/// key *is* the formatted citation text, so nothing may rewrite its interior,
/// and a key that itself contains colons (`manual:Einstein, A.: On …`) must
/// survive intact. Keys are canonicalized later anyway — the manager runs each
/// through its source's `normalize_key`, which knows whether case and
/// whitespace are payload for that key space.
pub fn parse_cite(spec: &str) -> Result<(String, String), String> {
    let Some((prefix, key)) = spec.split_once(':') else {
        return Err(format!("`{spec}`: expected `prefix:key`"));
    };
    let prefix = prefix.trim();
    if prefix.is_empty() {
        return Err(format!("`{spec}`: empty citation prefix"));
    }
    if key.is_empty() {
        return Err(format!("`{spec}`: empty citation key"));
    }
    Ok((prefix.to_string(), key.to_string()))
}

/// Read the citations named on the command line, in order, without duplicates.
///
/// Sources of citations, concatenated in this order:
///
/// 1. every `FILE` argument (`-` meaning stdin),
/// 2. every `--cite` option,
/// 3. stdin, but *only* when neither of the above was given — so `--cite x`
///    alone returns immediately instead of blocking on a terminal.
pub fn collect(cli: &Cli) -> Result<Vec<(String, String)>, String> {
    let mut cites = Vec::new();

    for path in &cli.input {
        let text = read_source(path)?;
        append_list(&mut cites, &text, &path.display().to_string())?;
    }
    for spec in &cli.cite {
        cites.push(parse_cite(spec).map_err(|e| format!("--cite {e}"))?);
    }
    if cli.input.is_empty() && cli.cite.is_empty() {
        let text = read_stdin()?;
        append_list(&mut cites, &text, "<stdin>")?;
    }

    // Requesting the same citation twice is not an error (citation lists are
    // usually generated), it just must not be printed twice. Order is the
    // input's, so the output array lines up with the list the user handed in.
    let mut seen = std::collections::HashSet::new();
    cites.retain(|c| seen.insert(c.clone()));
    Ok(cites)
}

/// Parse a whole citation-list file into `out`.
///
/// Blank lines and `#` comments are skipped and each line is trimmed. A line
/// that is neither but does not parse is a hard error naming the file and line
/// number: silently skipping a typo would drop a citation from the output with
/// nothing to show for it.
fn append_list(
    out: &mut Vec<(String, String)>,
    text: &str,
    origin: &str,
) -> Result<(), String> {
    for (n, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        out.push(parse_cite(line).map_err(|e| format!("{origin}:{}: {e}", n + 1))?);
    }
    Ok(())
}

/// Read a citation-list file, or stdin for `-`.
fn read_source(path: &Path) -> Result<String, String> {
    if path.as_os_str() == "-" {
        read_stdin()
    } else {
        std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
    }
}

fn read_stdin() -> Result<String, String> {
    let mut text = String::new();
    std::io::stdin()
        .read_to_string(&mut text)
        .map_err(|e| format!("<stdin>: {e}"))?;
    Ok(text)
}

/// The distinct prefixes appearing in `cites`, for the "you asked for `bib:`
/// citations but gave no `--bib` file" style warnings.
pub fn prefixes(cites: &[(String, String)]) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    for (prefix, _) in cites {
        if !out.contains(&prefix.as_str()) {
            out.push(prefix);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_at_the_first_colon_only() {
        assert_eq!(
            parse_cite("doi:10.1103/PhysRev.47.777").unwrap(),
            ("doi".into(), "10.1103/PhysRev.47.777".into())
        );
        // A `manual:` key is the citation text: colons and interior spacing are
        // payload and must survive verbatim.
        assert_eq!(
            parse_cite("manual:Einstein, A.: On the Method").unwrap(),
            ("manual".into(), "Einstein, A.: On the Method".into())
        );
    }

    #[test]
    fn rejects_specs_that_are_not_prefix_key() {
        for bad in ["1211.1037", ":1211.1037", "arxiv:"] {
            assert!(parse_cite(bad).is_err(), "{bad} should not parse");
        }
    }

    #[test]
    fn list_skips_blanks_and_comments_and_numbers_errors() {
        let mut out = Vec::new();
        append_list(&mut out, "# a note\n\n  arxiv:1211.1037  \n", "f").unwrap();
        assert_eq!(out, vec![("arxiv".into(), "1211.1037".into())]);

        let err = append_list(&mut out, "doi:ok\nnope\n", "f").unwrap_err();
        assert!(err.starts_with("f:2:"), "{err}");
    }
}
