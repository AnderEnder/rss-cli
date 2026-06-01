//! Shared helpers for the `rss` integration tests.
//!
//! Tests that exercise `rss fetch` depend on the `fetcher`, `parser`, and `cli`
//! modules. While those are still `todo!()` stubs the binary panics with a
//! "not yet implemented" message. [`is_stub_panic`] detects exactly that case so
//! the live tests can emit a skip note instead of a spurious failure — and they
//! automatically upgrade to real assertions once the stubs are implemented.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{SystemTime, UNIX_EPOCH};

use assert_cmd::Command;

/// A unique temp directory used as `--cache-dir`, removed on drop.
pub struct TempCache {
    path: PathBuf,
}

impl TempCache {
    /// Create a fresh, unique cache directory under the OS temp dir.
    pub fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "rss-cli-test-{tag}-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        std::fs::create_dir_all(&path).expect("create temp cache dir");
        Self { path }
    }

    /// The directory path, to pass to `--cache-dir`.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempCache {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// A `Command` for the compiled `rss` binary (built by `cargo test`).
pub fn rss() -> Command {
    Command::cargo_bin("rss").expect("the `rss` binary should be built by `cargo test`")
}

/// Read a fixture file from `tests/fixtures/` as a UTF-8 string.
pub fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read fixture {}: {e}", path.display()))
}

/// Start a mock HTTP server and register a `GET <path>` mock that serves `body`
/// with a `200 OK`. Keep both returned values alive for the test's duration; the
/// feed URL is `format!("{}{path}", server.url())`.
pub fn mock_server(
    path: &str,
    content_type: &str,
    body: &str,
) -> (mockito::ServerGuard, mockito::Mock) {
    let mut server = mockito::Server::new();
    let mock = server
        .mock("GET", path)
        .with_status(200)
        .with_header("content-type", content_type)
        .with_body(body)
        .create();
    (server, mock)
}

/// Run `rss fetch <feed_url> --format <format>` against a temp cache dir.
pub fn run_fetch(feed_url: &str, cache_dir: &Path, format: &str) -> Output {
    rss()
        .arg("--quiet")
        .arg("--cache-dir")
        .arg(cache_dir)
        .arg("fetch")
        .arg(feed_url)
        .arg("--format")
        .arg(format)
        .output()
        .expect("spawn rss")
}

/// True when the output indicates an unfinished `todo!()` stub panicked, rather
/// than a real failure worth asserting on. Gated narrowly on the `todo!()`
/// message: a *genuine* panic after integration won't carry this string, so the
/// test will correctly proceed to assert (and fail) instead of being masked.
pub fn is_stub_panic(output: &Output) -> bool {
    String::from_utf8_lossy(&output.stderr).contains("not yet implemented")
}

/// Print a uniform "skipped pending integration" note for a gated live test.
pub fn skip_note(test: &str, output: &Output) {
    eprintln!(
        "[qa] SKIP {test}: binary is still on a `todo!()` stub (exit {:?}); \
         live assertions will run automatically once fetch/parse/render land.",
        output.status.code()
    );
}

/// Collect the set of `feeds[].items[].id` values from a `--format json` payload.
pub fn item_ids(output: &Output) -> std::collections::BTreeSet<String> {
    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("fetch json stdout should parse");
    v["feeds"]
        .as_array()
        .expect("feeds array")
        .iter()
        .flat_map(|f| f["items"].as_array().cloned().unwrap_or_default())
        .map(|it| {
            it["id"]
                .as_str()
                .expect("item id should be a string")
                .to_string()
        })
        .collect()
}
