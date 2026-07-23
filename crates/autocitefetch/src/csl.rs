//! CSL-JSON representation and small helpers.
//!
//! A CSL-JSON item is modeled as a dynamic [`serde_json::Value`] (an object).
//! We keep it untyped so that data coming back verbatim from doi.org or a
//! bibliography file is preserved losslessly, and so sources can attach
//! non-standard extension fields (`arxivid`, `arxiv_version_number`, …).

use alloc::string::String;

/// A bibliographic item in CSL-JSON form (normally a JSON object).
pub type CslValue = serde_json::Value;

/// Build the canonical citation id, `"prefix:key"`.
pub fn cite_id(prefix: &str, key: &str) -> String {
    let mut s = String::with_capacity(prefix.len() + 1 + key.len());
    s.push_str(prefix);
    s.push(':');
    s.push_str(key);
    s
}

/// Set the `id` field on a CSL object (creating the object shape if needed).
pub fn set_id(item: &mut CslValue, id: &str) {
    if !item.is_object() {
        *item = CslValue::Object(serde_json::Map::new());
    }
    if let Some(obj) = item.as_object_mut() {
        obj.insert("id".into(), CslValue::String(id.into()));
    }
}

/// Read a string field, trying several key spellings in order.
///
/// doi.org emits canonical CSL casing (`DOI`, `URL`), while the arXiv mapping
/// writes lowercase (`doi`, `url`); this smooths over that divergence.
pub fn get_str<'a>(item: &'a CslValue, keys: &[&str]) -> Option<&'a str> {
    let obj = item.as_object()?;
    for k in keys {
        if let Some(v) = obj.get(*k).and_then(CslValue::as_str) {
            return Some(v);
        }
    }
    None
}

/// Shallow-merge `overrides` (a JSON object) into `target`, without clobbering
/// keys that `target` already has. Used when resolving a chained citation to
/// re-attach properties like `arxivid`.
pub fn merge_defaults(target: &mut CslValue, overrides: &CslValue) {
    let (Some(dst), Some(src)) = (target.as_object_mut(), overrides.as_object()) else {
        return;
    };
    for (k, v) in src {
        dst.entry(k.clone()).or_insert_with(|| v.clone());
    }
}
