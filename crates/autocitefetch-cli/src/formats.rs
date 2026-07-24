//! JSON/YAML parsing for the two config-ish files this binary reads: `--bib`
//! bibliographies and the `--arxiv-doi-overrides` map.
//!
//! This is where the workspace's *host-parses-config* principle lands. The
//! `no_std` core depends on no format crate but `serde_json`, and both hooks it
//! offers for anything else are used here:
//!
//! * [`BibliographyFileSource::with_parser`] takes a bytes → [`CslValue`]
//!   function, so YAML support costs the core nothing — a bib file is still
//!   fetched through the one [`Fetcher`] choke point (including as an
//!   `http(s):` URL), only the parse step changes.
//! * arXiv DOI overrides are handed over as *data*
//!   ([`ArxivSource::with_override_dois`]) rather than as a file path, so the
//!   override file may be YAML even though the core's built-in
//!   `with_override_dois_file` convenience only knows JSON.
//!
//! [`BibliographyFileSource::with_parser`]: autocitefetch::source::BibliographyFileSource::with_parser
//! [`ArxivSource::with_override_dois`]: autocitefetch::source::ArxivSource::with_override_dois
//! [`Fetcher`]: autocitefetch::Fetcher

use std::path::Path;

use autocitefetch::CslValue;
use autocitefetch::source::BibParser;

use crate::cli::Format;

/// Parse `bytes` as JSON.
fn parse_json(bytes: &[u8]) -> Result<CslValue, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("invalid JSON: {e}"))
}

/// Parse `bytes` as YAML.
fn parse_yaml(bytes: &[u8]) -> Result<CslValue, String> {
    serde_yaml_ng::from_slice(bytes).map_err(|e| format!("invalid YAML: {e}"))
}

/// Parse `bytes` as JSON, falling back to YAML.
///
/// YAML 1.2 is very nearly a JSON superset, so running only the YAML parser
/// would usually work — but "very nearly" is not "exactly" (an integer literal
/// too large for the YAML reader's number type is valid JSON and rejected as
/// out of range), and its diagnostics for a *broken* JSON file are much worse
/// than serde_json's. So try the exact parser first.
///
/// On failure both errors are reported. With no file extension to go on —
/// `--bib` also accepts URLs — guessing which format the user *meant* and
/// hiding the other parser's complaint would be worse than showing both.
fn parse_auto(bytes: &[u8]) -> Result<CslValue, String> {
    match parse_json(bytes) {
        Ok(v) => Ok(v),
        Err(json_err) => parse_yaml(bytes)
            .map_err(|yaml_err| format!("not parseable as JSON or YAML — {json_err}; {yaml_err}")),
    }
}

/// The [`BibParser`] function pointer implementing `format`.
pub fn parser_for(format: Format) -> BibParser {
    match format {
        Format::Auto => parse_auto,
        Format::Json => parse_json,
        Format::Yaml => parse_yaml,
    }
}

/// Read and parse a local file, tagging any error with its path.
pub fn read_file(path: &Path, format: Format) -> Result<CslValue, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    parser_for(format)(&bytes).map_err(|e| format!("{}: {e}", path.display()))
}

/// Load an arXiv-id → DOI override map into the `(arxivid, Option<doi>)` pairs
/// [`ArxivSource::with_override_dois`] takes.
///
/// The file is a mapping. A string value overrides whatever DOI the arXiv feed
/// reports; a `null` value *suppresses* the DOI entirely, which is why the
/// value type is `Option<String>` and not `String`.
///
/// [`ArxivSource::with_override_dois`]: autocitefetch::source::ArxivSource::with_override_dois
pub fn load_doi_overrides(path: &Path) -> Result<Vec<(String, Option<String>)>, String> {
    let value = read_file(path, Format::Auto)?;
    let map = value.as_object().ok_or_else(|| {
        format!(
            "{}: expected a mapping of arXiv id to DOI (or to null)",
            path.display()
        )
    })?;

    let mut out = Vec::with_capacity(map.len());
    for (arxivid, doi) in map {
        let doi = match doi {
            CslValue::String(s) => Some(s.clone()),
            CslValue::Null => None,
            other => {
                return Err(format!(
                    "{}: override for `{arxivid}` must be a DOI string or null, not {}",
                    path.display(),
                    type_name(other),
                ));
            }
        };
        out.push((arxivid.clone(), doi));
    }
    Ok(out)
}

/// A human-readable name for a JSON value's type, for error messages.
fn type_name(v: &CslValue) -> &'static str {
    match v {
        CslValue::Null => "null",
        CslValue::Bool(_) => "a boolean",
        CslValue::Number(_) => "a number",
        CslValue::String(_) => "a string",
        CslValue::Array(_) => "an array",
        CslValue::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_accepts_json_and_yaml() {
        let json = br#"{"a": {"title": "T"}}"#;
        let yaml = b"a:\n  title: T\n";
        assert_eq!(parse_auto(json).unwrap(), parse_auto(yaml).unwrap());
    }

    #[test]
    fn auto_accepts_json_the_yaml_reader_rejects() {
        // Why `auto` tries JSON first instead of leaning on YAML being a JSON
        // superset: it isn't quite one. An integer literal past the YAML
        // reader's range is valid JSON and an error there.
        let json = br#"{"a": 123456789012345678901234567890}"#;
        assert!(parse_yaml(json).is_err());
        assert!(parse_auto(json).is_ok());
    }

    #[test]
    fn auto_reports_both_parsers_on_garbage() {
        let err = parse_auto(b"not: [valid").unwrap_err();
        assert!(err.contains("JSON"), "{err}");
        assert!(err.contains("YAML"), "{err}");
    }

    /// Write `contents` into a fresh scratch directory and return its path,
    /// keeping the `TempDir` alive for the caller's lifetime.
    fn scratch(name: &str, contents: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, contents).unwrap();
        (dir, path)
    }

    #[test]
    fn overrides_map_string_to_some_and_null_to_none() {
        // `~` is YAML's null: the *suppress the DOI* case, which is why the
        // value type is `Option<String>` rather than `String`.
        let (_dir, path) = scratch(
            "ovr.yaml",
            b"1211.1037: 10.1103/PhysRevA.86.052329\n0704.0001: ~\n",
        );

        let mut got = load_doi_overrides(&path).unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("0704.0001".to_string(), None),
                (
                    "1211.1037".to_string(),
                    Some("10.1103/PhysRevA.86.052329".to_string())
                ),
            ]
        );
    }

    #[test]
    fn overrides_reject_a_non_string_value() {
        let (_dir, path) = scratch("ovr.json", br#"{"1211.1037": 42}"#);
        let err = load_doi_overrides(&path).unwrap_err();
        assert!(err.contains("must be a DOI string or null"), "{err}");
    }

    #[test]
    fn overrides_reject_a_file_that_is_not_a_mapping() {
        let (_dir, path) = scratch("ovr.json", b"[1, 2]");
        let err = load_doi_overrides(&path).unwrap_err();
        assert!(err.contains("expected a mapping"), "{err}");
    }
}
