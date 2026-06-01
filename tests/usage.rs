//! Usage / argument errors. These are handled before any `fetch` work, so they
//! pass against the foundation today (no stub gating).

mod common;

use common::rss;

#[test]
fn fetch_without_urls_is_a_usage_error() {
    let output = rss().arg("fetch").output().expect("spawn rss");

    assert_eq!(
        output.status.code(),
        Some(2),
        "missing feed URLs should exit 2 (usage error)"
    );

    // stdout carries data only — a usage error must not write to it.
    assert!(
        String::from_utf8_lossy(&output.stdout).trim().is_empty(),
        "no data should be written to stdout on a usage error"
    );

    // A structured JSON error object is printed to stderr.
    let stderr = String::from_utf8_lossy(&output.stderr);
    let json_line = stderr
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with('{'))
        .unwrap_or_else(|| panic!("expected a JSON error object on stderr, got:\n{stderr}"));

    let v: serde_json::Value =
        serde_json::from_str(json_line.trim()).expect("stderr error should be JSON");
    assert_eq!(v["code"], "USAGE_ERROR", "usage errors carry a stable code");
    assert!(
        v["message"].as_str().is_some_and(|m| !m.is_empty()),
        "error object should carry a human-readable message"
    );
}
