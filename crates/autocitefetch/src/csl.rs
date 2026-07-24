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
///
/// The prefix must not itself contain a `':'` or the id is ambiguous
/// (`("a", "b:c")` and `("a:b", "c")` would both yield `"a:b:c"`, and
/// [`CitationManager::get_by_id`](crate::manager::CitationManager::get_by_id)
/// splits on the *first* colon). [`CitationManager::register`] enforces this.
///
/// [`CitationManager::register`]: crate::manager::CitationManager::register
pub fn cite_id(prefix: &str, key: &str) -> String {
    let mut s = String::with_capacity(prefix.len() + 1 + key.len());
    s.push_str(prefix);
    s.push(':');
    s.push_str(key);
    s
}

/// Set the `id` field on a CSL object, returning whether it could be set.
///
/// A non-object value is left **untouched** (and `false` is returned): this
/// used to replace the whole payload with a bare `{"id": …}`, silently
/// destroying whatever a source had returned. Callers decide what a non-object
/// payload means — the manager treats it as a source failure.
pub fn set_id(item: &mut CslValue, id: &str) -> bool {
    match item.as_object_mut() {
        Some(obj) => {
            obj.insert("id".into(), CslValue::String(id.into()));
            true
        }
        None => false,
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

/// Shallow-merge `overrides` (a JSON object) into `target`, **without**
/// clobbering keys that `target` already has — `target` wins on a collision.
/// Used along a chain to *accumulate* `set_properties` so that a property set
/// closer to the request wins over one set further away (see [`merge_over`] for
/// the complementary "overrides win" direction used against the concrete
/// target). A `null` in `overrides` is treated like any other value: inserted
/// verbatim only when the key is absent.
pub fn merge_defaults(target: &mut CslValue, overrides: &CslValue) {
    let (Some(dst), Some(src)) = (target.as_object_mut(), overrides.as_object()) else {
        return;
    };
    for (k, v) in src {
        // `entry()` would clone the key on every iteration, including the
        // common already-present case; only allocate when we actually insert.
        if !dst.contains_key(k) {
            dst.insert(k.clone(), v.clone());
        }
    }
}

/// Shallow-merge `overrides` (a JSON object) into `target`, **overwriting**
/// every colliding key — `overrides` win. This is the mirror of
/// [`merge_defaults`]: use it when a chained citation's accumulated
/// `set_properties` must take precedence over the concrete target's fields
/// (matching both reference implementations, which do
/// `{ ...target, ...set_properties }`). A `null` in `overrides` is treated like
/// any other value — inserted/overwritten verbatim, exactly as
/// [`merge_defaults`] copies it.
pub fn merge_over(target: &mut CslValue, overrides: &CslValue) {
    let (Some(dst), Some(src)) = (target.as_object_mut(), overrides.as_object()) else {
        return;
    };
    for (k, v) in src {
        dst.insert(k.clone(), v.clone());
    }
}
