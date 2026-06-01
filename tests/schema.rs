//! `rss schema` is fully implemented in the foundation, so these are real
//! assertions that must pass now (no stub gating).

mod common;

use common::rss;

#[test]
fn schema_fetch_is_valid_json_with_defs_and_properties() {
    let output = rss()
        .args(["schema", "--command", "fetch"])
        .output()
        .expect("spawn rss");

    assert!(
        output.status.success(),
        "`rss schema --command fetch` should exit 0"
    );

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("schema stdout should be valid JSON");

    assert!(v.get("$defs").is_some(), "schema should contain `$defs`");
    assert!(
        v.get("properties").is_some(),
        "schema should contain `properties`"
    );

    // The top-level FetchOutput contract surface agents rely on.
    let props = v["properties"]
        .as_object()
        .expect("properties is an object");
    for key in ["schema_version", "fetched_at", "feeds", "errors"] {
        assert!(
            props.contains_key(key),
            "fetch schema missing top-level `{key}`"
        );
    }

    // The Item type (referenced from feeds[].items) must be defined in `$defs`.
    assert!(
        v["$defs"].get("Item").is_some(),
        "schema `$defs` should define `Item`"
    );
}

#[test]
fn schema_discover_is_valid_json() {
    let output = rss()
        .args(["schema", "--command", "discover"])
        .output()
        .expect("spawn rss");

    assert!(
        output.status.success(),
        "`rss schema --command discover` should exit 0"
    );

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("discover schema should be valid JSON");
    assert!(v.get("properties").is_some());
    let props = v["properties"]
        .as_object()
        .expect("properties is an object");
    assert!(props.contains_key("site_url"));
    assert!(props.contains_key("feeds"));
}
