use std::fs;

use rss_cli::cache::{Cache, CacheMeta};
use rss_cli::config::{CachePolicy, FetchParams};
use rss_cli::fetch::HttpClient;

fn meta(url: &str) -> CacheMeta {
    CacheMeta {
        feed_url: url.into(),
        final_url: None,
        etag: Some("original".into()),
        last_modified: None,
        fetched_at: "2026-10-01T00:00:00Z".into(),
        content_type: None,
    }
}

#[test]
fn cache_clear_preserves_unrelated_files_and_directories() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    cache
        .put(&meta("https://example.com/feed"), b"body")
        .unwrap();
    for name in ["unrelated.txt", "notes.json", "notes.body"] {
        fs::write(dir.path().join(name), "keep").unwrap();
    }
    fs::create_dir(dir.path().join("nested")).unwrap();
    assert_eq!(cache.clear().unwrap(), 1);
    for name in ["unrelated.txt", "notes.json", "notes.body", "nested"] {
        assert!(dir.path().join(name).exists(), "deleted {name}");
    }
}

#[tokio::test]
async fn malformed_success_preserves_last_valid_cached_feed() {
    let mut server = mockito::Server::new_async().await;
    let url = format!("{}/feed", server.url());
    let body = b"<rss version=\"2.0\"><channel><title>Good</title><item><guid>one</guid><title>Post</title></item></channel></rss>";
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    cache.put(&meta(&url), body).unwrap();
    let _mock = server
        .mock("GET", "/feed")
        .with_status(200)
        .with_header("etag", "bad")
        .with_body("<html>temporary challenge</html>")
        .create_async()
        .await;
    let params = FetchParams::default();
    let out = rss_cli::core::fetch_feeds(std::slice::from_ref(&url), &params, &cache).await;
    assert_eq!(out.errors[0].code, "FEED_PARSE_FAILED");
    let entry = cache.get(&url).unwrap().unwrap();
    assert_eq!(entry.body, body);
    assert_eq!(entry.meta.etag.as_deref(), Some("original"));
    let item = rss_cli::core::show_item(
        &url,
        "one",
        &FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..params
        },
        &cache,
    )
    .await
    .unwrap();
    assert!(item.is_some());
}

#[tokio::test]
async fn oversized_download_does_not_replace_cached_feed() {
    let mut server = mockito::Server::new_async().await;
    let url = format!("{}/feed", server.url());
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    cache.put(&meta(&url), b"original").unwrap();
    let _mock = server
        .mock("GET", "/feed")
        .with_status(200)
        .with_body("x".repeat(16 * 1024 * 1024 + 1))
        .create_async()
        .await;
    let http = HttpClient::new("rss-test", std::time::Duration::from_secs(10)).unwrap();
    assert!(
        http.fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .is_err()
    );
    assert_eq!(cache.get(&url).unwrap().unwrap().body, b"original");
    assert!(http.get_bytes(&url).await.is_err());
    assert!(
        http.fetch(&url, &cache, CachePolicy::NoCache)
            .await
            .is_err()
    );
}

#[test]
fn concurrent_cache_writers_keep_body_and_validators_together() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    let url = "https://example.com/feed";
    let mut initial = meta(url);
    initial.etag = Some("initial".into());
    cache.put(&initial, b"initial").unwrap();
    let start = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for writer in 0..3 {
            let cache = &cache;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for iteration in 0..200 {
                    let value = format!("writer-{writer}-{iteration}");
                    let mut metadata = meta(url);
                    metadata.etag = Some(value.clone());
                    cache.put(&metadata, value.as_bytes()).unwrap();
                }
            });
        }
        start.wait();
        for _ in 0..2000 {
            let entry = cache.get(url).unwrap().unwrap();
            assert_eq!(entry.meta.etag.as_deref().unwrap().as_bytes(), entry.body);
        }
    });
}

#[tokio::test]
async fn chunked_and_gzipped_bodies_obey_the_decoded_limit() {
    let mut server = mockito::Server::new_async().await;
    let _chunked = server
        .mock("GET", "/chunked")
        .with_status(200)
        .with_chunked_body(|writer| {
            let block = [b'x'; 65536];
            for _ in 0..256 {
                writer.write_all(&block)?;
            }
            writer.write_all(b"x")
        })
        .create_async()
        .await;
    let _gzip = server
        .mock("GET", "/gzip")
        .with_status(200)
        .with_header("content-encoding", "gzip")
        .with_body(include_bytes!("fixtures/oversized.txt.gz"))
        .create_async()
        .await;
    let http = HttpClient::new("rss-test", std::time::Duration::from_secs(10)).unwrap();
    for path in ["/chunked", "/gzip"] {
        let error = http
            .get_bytes(&format!("{}{path}", server.url()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), "RESPONSE_BODY_TOO_LARGE", "{path}: {error}");
        assert_eq!(
            error.to_error_obj(None).details["limit_bytes"],
            16 * 1024 * 1024
        );
    }
}

#[test]
fn legacy_cache_pairs_remain_readable_and_upgrade_without_duplicate_listing() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    let url = "https://example.com/feed";
    let key: String = Sha256::digest(url.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    fs::write(
        dir.path().join(format!("{key}.json")),
        serde_json::to_vec(&meta(url)).unwrap(),
    )
    .unwrap();
    fs::write(dir.path().join(format!("{key}.body")), b"legacy").unwrap();
    assert_eq!(cache.get(url).unwrap().unwrap().body, b"legacy");
    assert_eq!(cache.list().unwrap()[0].size_bytes, 6);
    cache.put(&meta(url), b"new").unwrap();
    assert_eq!(cache.get(url).unwrap().unwrap().body, b"new");
    let list = cache.list().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].size_bytes, 3);
    assert_eq!(cache.clear().unwrap(), 1);
    assert!(cache.get(url).unwrap().is_none());
}

#[test]
fn oversized_direct_parser_input_is_rejected_before_parsing() {
    let body = vec![b'x'; 16 * 1024 * 1024 + 1];
    let error =
        rss_cli::parse::parse_feed(&body, "https://example.com/feed", &FetchParams::default())
            .unwrap_err();
    assert_eq!(error.code(), "RESPONSE_BODY_TOO_LARGE");
}
