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

/// Remove the given **top-level** keys from a CSL object, in place.
///
/// Used by [`CitationManager::with_dropped_csl_fields`] to strip bulky fields a
/// host has no use for — `reference` (a paper's whole reference list, routinely
/// the largest field doi.org returns), `abstract`, `relation` — *before* the
/// item is stored, so neither the in-memory view nor the persisted cache ever
/// carries them.
///
/// Only the top level is touched: a `reference` nested inside some other field
/// is left alone. A key that is not present is silently ignored, and a
/// non-object value (which is not a CSL item to begin with) is left untouched,
/// matching [`set_id`]'s tolerance.
///
/// [`CitationManager::with_dropped_csl_fields`]:
///     crate::manager::CitationManager::with_dropped_csl_fields
pub fn remove_fields(item: &mut CslValue, fields: &[String]) {
    if fields.is_empty() {
        return;
    }
    let Some(obj) = item.as_object_mut() else {
        return;
    };
    for field in fields {
        obj.remove(field.as_str());
    }
}

/// Shallow-merge `overrides` (a JSON object) into `target`, **without**
/// clobbering keys that `target` already has — `target` wins on a collision.
/// Used along a chain to *accumulate* `set_properties` so that a property set
/// closer to the request wins over one set further away (see [`merge_over`] for
/// the complementary "overrides win" direction used against the concrete
/// target).
///
/// Presence, not truthiness, is what "already has a value" means here:
/// a key present in `target` with **any** value — including an explicit JSON
/// `null` — counts as present, so its default from `overrides` is **not**
/// applied. This is intentional. In CSL-JSON one could read `"DOI": null` as an
/// *absence* and let the default win, but this code treats the `null` as a
/// deliberate, caller-chosen value and leaves it in place. (Contrast
/// [`merge_over`], which overwrites such a `null` with the override.)
///
/// The value coming from `overrides` gets no special treatment either: a `null`
/// in `overrides` is inserted verbatim, but only when the key is absent from
/// `target`, exactly like any other value.
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
/// `{ ...target, ...set_properties }`).
///
/// Every key in `overrides` is written unconditionally, so this ignores what
/// `target` already holds: an existing value in `target` — including an
/// explicit JSON `null` — is replaced by the corresponding `overrides` value.
/// This is precisely where it diverges from [`merge_defaults`], which would
/// *keep* that `null` (it counts a present-but-`null` key as already having a
/// value). A `null` on the `overrides` side is likewise written verbatim,
/// overwriting whatever was there.
pub fn merge_over(target: &mut CslValue, overrides: &CslValue) {
    let (Some(dst), Some(src)) = (target.as_object_mut(), overrides.as_object()) else {
        return;
    };
    for (k, v) in src {
        dst.insert(k.clone(), v.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// `remove_fields` is top-level-only, tolerant of absent keys, and a no-op
    /// on anything that is not an object.
    #[test]
    fn remove_fields_drops_only_top_level_keys() {
        let mut item = json!({
            "id": "doi:10.x/y",
            "reference": [{"key": "r1"}],
            "container": {"reference": "nested"},
        });
        remove_fields(
            &mut item,
            &["reference".into(), "abstract".into()], // "abstract" is absent
        );
        assert_eq!(
            item,
            json!({"id": "doi:10.x/y", "container": {"reference": "nested"}})
        );

        // Not an object: left untouched rather than mangled (cf. `set_id`).
        let mut arr = json!(["not a CSL item"]);
        remove_fields(&mut arr, &["reference".into()]);
        assert_eq!(arr, json!(["not a CSL item"]));
    }

    /// Pins the intentional "an explicit `null` in `target` blocks the default"
    /// behavior (review item #15): `merge_defaults` counts a present-but-`null`
    /// key as already having a value, so the default is *not* applied, whereas
    /// `merge_over` overwrites the `null`. Do not "fix" this into treating a
    /// `null` as an absence — it is a deliberate, caller-chosen value.
    #[test]
    fn explicit_null_in_target_blocks_default_but_not_override() {
        // merge_defaults: the `null` already present wins; default is skipped.
        let mut target = json!({ "DOI": null });
        merge_defaults(&mut target, &json!({ "DOI": "10.x/y" }));
        assert_eq!(target, json!({ "DOI": null }));

        // merge_over: the override wins, replacing the `null`.
        let mut target = json!({ "DOI": null });
        merge_over(&mut target, &json!({ "DOI": "10.x/y" }));
        assert_eq!(target, json!({ "DOI": "10.x/y" }));
    }
}
