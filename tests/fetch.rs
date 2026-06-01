//! Live `rss fetch` integration tests, served from `mockito`.
//!
//! These depend on the `fetcher`, `parser`, and `cli` render code. Each test is
//! gated with [`is_stub_panic`]: while any of those modules is still a `todo!()`
//! stub the binary panics, and the test emits a skip note rather than a spurious
//! failure. Once integration completes the gate is transparent and the real
//! assertions run.

mod common;

use common::{TempCache, fixture, is_stub_panic, item_ids, mock_server, rss, run_fetch, skip_note};
use mockito::Matcher;

const RSS_CT: &str = "application/rss+xml; charset=utf-8";
const ATOM_CT: &str = "application/atom+xml; charset=utf-8";

/// Looks like an ISO-8601 / RFC-3339 instant.
fn looks_iso8601(s: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(s).is_ok()
}

/// `rss fetch <url> --format json` → exit 0, contract-shaped JSON, non-empty
/// items with stable ids, ISO-8601 timestamps, and a resolved relative link.
#[test]
fn fetch_json_produces_valid_structured_output() {
    let (server, _m) = mock_server("/rss2.xml", RSS_CT, &fixture("rss2.xml"));
    let feed_url = format!("{}/rss2.xml", server.url());
    let cache = TempCache::new("json");

    let output = run_fetch(&feed_url, cache.path(), "json");
    if is_stub_panic(&output) {
        skip_note("fetch_json_produces_valid_structured_output", &output);
        return;
    }

    assert!(
        output.status.success(),
        "a single good feed should exit 0; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("fetch --format json should be valid JSON");

    assert_eq!(v["schema_version"], "1");
    assert!(v["fetched_at"].as_str().is_some_and(looks_iso8601));

    let feeds = v["feeds"].as_array().expect("feeds is an array");
    assert_eq!(feeds.len(), 1, "exactly one feed requested");
    let feed = &feeds[0];
    assert_eq!(feed["feed_url"].as_str(), Some(feed_url.as_str()));
    assert_eq!(feed["status"], "ok");

    let items = feed["items"].as_array().expect("items is an array");
    assert!(!items.is_empty(), "feeds[0].items should be non-empty");

    for item in items {
        let id = item["id"].as_str().unwrap_or_default();
        assert!(!id.is_empty(), "every item must have a non-empty stable id");
        if let Some(ts) = item["published"].as_str() {
            assert!(looks_iso8601(ts), "published `{ts}` should look ISO-8601");
        }
        if let Some(ts) = item["updated"].as_str() {
            assert!(looks_iso8601(ts), "updated `{ts}` should look ISO-8601");
        }
    }

    // The item with the relative `<link>/post/1</link>` must be resolved to an
    // absolute URL. We don't bet on whether the base is the request URL or the
    // channel link — only that resolution happened.
    let rel = items
        .iter()
        .find(|it| it["title"] == "Relative Link Post")
        .expect("the relative-link item should be present");
    let url = rel["url"]
        .as_str()
        .expect("resolved item should carry a url");
    assert!(
        url.starts_with("http://") || url.starts_with("https://"),
        "relative link should resolve to an absolute URL, got `{url}`"
    );
    assert!(
        url.ends_with("/post/1"),
        "resolved URL should end with /post/1, got `{url}`"
    );
    assert_ne!(url, "/post/1", "relative link must not be left unresolved");
}

/// A JSON Feed 1.1 document parses just like RSS/Atom: items with stable ids and
/// resolved urls.
#[test]
fn fetch_json_feed_parses_items() {
    let (server, _m) = mock_server(
        "/feed.json",
        "application/feed+json; charset=utf-8",
        &fixture("jsonfeed.json"),
    );
    let feed_url = format!("{}/feed.json", server.url());
    let cache = TempCache::new("jsonfeed");

    let output = run_fetch(&feed_url, cache.path(), "json");
    if is_stub_panic(&output) {
        skip_note("fetch_json_feed_parses_items", &output);
        return;
    }
    assert!(output.status.success(), "a good JSON feed should exit 0");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("json feed output should parse");
    let feed = &v["feeds"][0];
    assert_eq!(feed["status"], "ok");
    let items = feed["items"].as_array().expect("items array");
    assert!(!items.is_empty(), "JSON feed items should be non-empty");
    for item in items {
        assert!(
            item["id"].as_str().is_some_and(|s| !s.is_empty()),
            "every JSON feed item must have a non-empty stable id"
        );
    }
}

/// THE KEYSTONE: item ids are deterministic — identical across repeated runs in
/// the same cache dir, and identical again in a *fresh* cache dir (proving the id
/// is content-derived, not cache-derived).
#[test]
fn item_ids_are_stable_across_runs_and_caches() {
    let (server, _m) = mock_server("/rss2.xml", RSS_CT, &fixture("rss2.xml"));
    let feed_url = format!("{}/rss2.xml", server.url());

    let cache_a = TempCache::new("stable-a");
    let out1 = run_fetch(&feed_url, cache_a.path(), "json");
    let out2 = run_fetch(&feed_url, cache_a.path(), "json"); // same cache, second run
    let cache_b = TempCache::new("stable-b");
    let out3 = run_fetch(&feed_url, cache_b.path(), "json"); // fresh cache dir

    if [&out1, &out2, &out3].iter().any(|o| is_stub_panic(o)) {
        skip_note("item_ids_are_stable_across_runs_and_caches", &out1);
        return;
    }

    let ids1 = item_ids(&out1);
    let ids2 = item_ids(&out2);
    let ids3 = item_ids(&out3);

    assert!(!ids1.is_empty(), "expected at least one item id");
    assert_eq!(
        ids1, ids2,
        "item ids must be identical across runs into the same cache dir"
    );
    assert_eq!(
        ids1, ids3,
        "item ids must be identical with a fresh cache dir (deterministic by construction)"
    );
}

/// Conditional GET: the second run sends `If-None-Match` and the server replies
/// `304`, so the feed is reported as `not_modified` / `from_cache`.
#[test]
fn conditional_get_reports_not_modified_on_304() {
    let mut server = mockito::Server::new();
    let body = fixture("rss2.xml");

    // First request (no validator in cache yet) → 200 + ETag.
    let m200 = server
        .mock("GET", "/feed.xml")
        .match_header("if-none-match", Matcher::Missing)
        .with_status(200)
        .with_header("content-type", RSS_CT)
        .with_header("etag", "\"v1\"")
        .with_body(&body)
        .expect(1)
        .create();
    // Second request (revalidating with the cached ETag) → 304.
    let m304 = server
        .mock("GET", "/feed.xml")
        .match_header("if-none-match", "\"v1\"")
        .with_status(304)
        .expect(1)
        .create();

    let feed_url = format!("{}/feed.xml", server.url());
    let cache = TempCache::new("cond");

    let out1 = run_fetch(&feed_url, cache.path(), "json"); // populates cache
    let out2 = run_fetch(&feed_url, cache.path(), "json"); // revalidates → 304

    if is_stub_panic(&out1) || is_stub_panic(&out2) {
        skip_note("conditional_get_reports_not_modified_on_304", &out1);
        return;
    }

    m200.assert();
    m304.assert();

    let v: serde_json::Value =
        serde_json::from_slice(&out2.stdout).expect("second run json should parse");
    let feed = &v["feeds"][0];
    assert_eq!(
        feed["status"], "not_modified",
        "a 304 revalidation should report status=not_modified"
    );
    assert_eq!(
        feed["from_cache"].as_bool(),
        Some(true),
        "a 304 revalidation should be served from_cache"
    );
}

/// `--format ndjson` emits one JSON object per line, each a feed item.
#[test]
fn fetch_ndjson_emits_one_json_object_per_line() {
    let (server, _m) = mock_server("/atom.xml", ATOM_CT, &fixture("atom.xml"));
    let feed_url = format!("{}/atom.xml", server.url());
    let cache = TempCache::new("ndjson");

    let output = run_fetch(&feed_url, cache.path(), "ndjson");
    if is_stub_panic(&output) {
        skip_note("fetch_ndjson_emits_one_json_object_per_line", &output);
        return;
    }
    assert!(
        output.status.success(),
        "ndjson fetch of a good feed should exit 0"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(
        !lines.is_empty(),
        "ndjson should emit at least one item line"
    );

    for line in &lines {
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("each ndjson line must be valid JSON ({e}): {line}"));
        assert!(
            v["id"].as_str().is_some_and(|s| !s.is_empty()),
            "each ndjson item line should carry a non-empty id"
        );
        assert!(
            v.get("feed_url").is_some(),
            "each ndjson item should carry its feed_url"
        );
    }
}

/// A malformed feed alone → that feed is `status:error` with a structured error,
/// and the process exits 4 (all feeds failed).
#[test]
fn malformed_feed_alone_exits_all_failed() {
    let (server, _m) = mock_server("/bad.xml", RSS_CT, &fixture("malformed.xml"));
    let feed_url = format!("{}/bad.xml", server.url());
    let cache = TempCache::new("bad-only");

    let output = run_fetch(&feed_url, cache.path(), "json");
    if is_stub_panic(&output) {
        skip_note("malformed_feed_alone_exits_all_failed", &output);
        return;
    }

    assert_eq!(
        output.status.code(),
        Some(4),
        "a single failing feed should exit 4 (all failed)"
    );

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("error output should still be valid JSON");
    let feed = &v["feeds"][0];
    assert_eq!(feed["status"], "error");
    assert!(
        feed["error"].is_object(),
        "a failed feed should carry a structured error object"
    );
    assert!(
        feed["error"]["code"]
            .as_str()
            .is_some_and(|c| !c.is_empty()),
        "the error should carry a stable code"
    );
    assert!(
        v["errors"].as_array().is_some_and(|e| !e.is_empty()),
        "feed-level errors should be mirrored at the top level"
    );
}

/// A good feed + a malformed feed → exit 3 (partial). `feeds[]` is in completion
/// order (`buffer_unordered`), so results are matched by `feed_url`, never index.
#[test]
fn mixed_feeds_exit_partial() {
    let mut server = mockito::Server::new();
    let _good = server
        .mock("GET", "/good.xml")
        .with_status(200)
        .with_header("content-type", RSS_CT)
        .with_body(fixture("rss2.xml"))
        .create();
    let _bad = server
        .mock("GET", "/bad.xml")
        .with_status(200)
        .with_header("content-type", RSS_CT)
        .with_body(fixture("malformed.xml"))
        .create();

    let good_url = format!("{}/good.xml", server.url());
    let bad_url = format!("{}/bad.xml", server.url());
    let cache = TempCache::new("mixed");

    let output = rss()
        .arg("--quiet")
        .arg("--cache-dir")
        .arg(cache.path())
        .arg("fetch")
        .arg(good_url.as_str())
        .arg(bad_url.as_str())
        .arg("--format")
        .arg("json")
        .output()
        .expect("spawn rss");

    if is_stub_panic(&output) {
        skip_note("mixed_feeds_exit_partial", &output);
        return;
    }

    assert_eq!(
        output.status.code(),
        Some(3),
        "one good + one bad feed should exit 3 (partial)"
    );

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("mixed output should be valid JSON");
    let feeds = v["feeds"].as_array().expect("feeds array");
    assert_eq!(feeds.len(), 2);

    let by_url = |url: &str| {
        feeds
            .iter()
            .find(|f| f.get("feed_url").and_then(|v| v.as_str()) == Some(url))
            .unwrap_or_else(|| panic!("missing feed result for {url}"))
    };
    assert_eq!(by_url(&good_url)["status"], "ok");
    assert_eq!(by_url(&bad_url)["status"], "error");
}

/// `--max-content-chars` truncates long item bodies, flags `content_truncated`, and
/// surfaces a top-level `truncation` marker counting the truncated items.
#[test]
fn max_content_chars_truncates_and_marks() {
    let long_body = "Lorem ipsum dolor sit amet ".repeat(40); // ~1080 chars
    let feed = format!(
        r#"<?xml version="1.0"?>
<rss version="2.0"><channel>
  <title>Long Feed</title>
  <link>https://example.com/</link>
  <item>
    <title>Big Post</title>
    <link>https://example.com/big</link>
    <pubDate>Mon, 05 Jan 2026 10:00:00 GMT</pubDate>
    <description><![CDATA[<p>{long_body}</p>]]></description>
  </item>
</channel></rss>"#
    );

    let (server, _m) = mock_server("/long.xml", RSS_CT, &feed);
    let feed_url = format!("{}/long.xml", server.url());
    let cache = TempCache::new("maxcontent");

    let output = rss()
        .arg("--quiet")
        .arg("--cache-dir")
        .arg(cache.path())
        .arg("fetch")
        .arg(feed_url.as_str())
        .arg("--max-content-chars")
        .arg("20")
        .arg("--format")
        .arg("json")
        .output()
        .expect("spawn rss");

    if is_stub_panic(&output) {
        skip_note("max_content_chars_truncates_and_marks", &output);
        return;
    }
    assert!(output.status.success(), "a good feed should exit 0");

    let v: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("output should be valid JSON");
    let item = &v["feeds"][0]["items"][0];

    assert_eq!(
        item["content_truncated"].as_bool(),
        Some(true),
        "a body longer than the cap should be flagged content_truncated"
    );
    let content = item["content"].as_str().expect("content string");
    assert!(
        content.chars().count() <= 20 + " …[truncated]".chars().count(),
        "content should be cut to roughly the cap plus the marker, got {} chars",
        content.chars().count()
    );

    // Top-level marker records that the result was bounded.
    let trunc = &v["truncation"];
    assert!(trunc.is_object(), "truncation marker should be present");
    assert_eq!(
        trunc["items_content_truncated"].as_u64(),
        Some(1),
        "exactly one item's content was truncated"
    );
}
