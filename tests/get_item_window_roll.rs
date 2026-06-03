use assert_cmd::Command;
use serde_json::Value;

/// End-to-end proof that `rss show` returns an item the caller already saw — by its stable
/// `id`, its raw `guid`, and its permalink `url` — served cache-first, independent of whether
/// the live feed window still contains it (the round-2 field report's `get_item` bug).
#[test]
fn get_item_survives_rolled_window_and_accepts_guid_url() {
    let mut server = mockito::Server::new();
    let body = r#"<rss version="2.0"><channel><title>x</title>
        <item><title>Post</title>
              <link>https://ex.test/p/abc/</link>
              <guid>t3_abc</guid>
              <pubDate>Mon, 02 Jun 2026 00:00:00 GMT</pubDate>
              <description>full body here</description></item>
        </channel></rss>"#;
    // The ONLY network call must be the initial `fetch`. We pin the mock to exactly one
    // hit: the three subsequent cache-first `show` calls must make zero network requests, so
    // the item survives even though the live feed window is now irrelevant. If any `show`
    // touched the network, the hit count would exceed 1 and `m1.assert()` below would fail.
    let m1 = server
        .mock("GET", "/feed.xml")
        .with_status(200)
        .with_body(body)
        .expect(1)
        .create();

    let url = format!("{}/feed.xml", server.url());
    let cache = tempfile::tempdir().unwrap();
    let cache_arg = cache.path().to_str().unwrap();

    // 1) Populate the cache.
    let out = Command::cargo_bin("rss")
        .unwrap()
        .args(["--cache-dir", cache_arg, "fetch", &url, "--format", "json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let json: Value = serde_json::from_slice(&out).unwrap();
    let item = &json["feeds"][0]["items"][0];
    let id = item["id"].as_str().unwrap().to_string();
    assert_eq!(item["guid"].as_str(), Some("t3_abc"));

    // 2) show by id — cache-first, no live dependency on the (now-irrelevant) window.
    Command::cargo_bin("rss")
        .unwrap()
        .args(["--cache-dir", cache_arg, "show", &url, "--id", &id])
        .assert()
        .success()
        .stdout(predicates::str::contains("full body here"));

    // 3) show by guid resolves the same item.
    Command::cargo_bin("rss")
        .unwrap()
        .args(["--cache-dir", cache_arg, "show", &url, "--id", "t3_abc"])
        .assert()
        .success()
        .stdout(predicates::str::contains("full body here"));

    // 4) show by permalink URL resolves the same item.
    Command::cargo_bin("rss")
        .unwrap()
        .args([
            "--cache-dir",
            cache_arg,
            "show",
            &url,
            "--id",
            "https://ex.test/p/abc/",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("full body here"));

    // Exactly one network request total (the fetch) — proves the three cache-first `show`
    // lookups never revalidated against the live feed.
    m1.assert();
}
