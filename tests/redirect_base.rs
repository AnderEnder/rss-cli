use std::time::Duration;

use rss_cli::cache::Cache;
use rss_cli::config::{CachePolicy, FetchParams};
use rss_cli::{core, identity};

const BODY: &str = r#"<rss version="2.0"><channel><title>Redirected</title>
<item><title>Post</title><link>posts/1</link><guid isPermaLink="false">post-1</guid>
<description>Full body</description></item></channel></rss>"#;

#[tokio::test]
async fn redirected_links_use_destination_but_ids_keep_requested_namespace() {
    for policy in [CachePolicy::NoCache, CachePolicy::Revalidate] {
        let mut server = mockito::Server::new_async().await;
        let redirect = server
            .mock("GET", "/start")
            .with_status(302)
            .with_header("location", "/news/feed.xml")
            .expect(1)
            .create_async()
            .await;
        let feed = server
            .mock("GET", "/news/feed.xml")
            .with_status(200)
            .with_body(BODY)
            .expect(1)
            .create_async()
            .await;
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(Some(dir.path().into())).unwrap();
        let url = format!("{}/start", server.url());
        let expected_url = format!("{}/news/posts/1", server.url());
        let params = FetchParams {
            cache_policy: policy,
            ..Default::default()
        };
        let output = core::fetch_feeds(std::slice::from_ref(&url), &params, &cache).await;
        assert!(output.errors.is_empty());
        assert_eq!(output.feeds[0].feed_url, url);
        let item = &output.feeds[0].items[0];
        assert_eq!(item.url.as_deref(), Some(expected_url.as_str()));
        assert_eq!(
            item.id,
            identity::item_id(&url, Some(&expected_url), None, None, None).0
        );
        redirect.assert_async().await;
        feed.assert_async().await;
    }
}

#[tokio::test]
async fn cached_redirect_base_survives_every_cache_path_and_item_lookup() {
    for policy in [
        CachePolicy::CacheFirst,
        CachePolicy::MaxAge(Duration::from_secs(3600)),
        CachePolicy::Revalidate,
        CachePolicy::StaleIfError,
    ] {
        let mut server = mockito::Server::new_async().await;
        let redirect = server
            .mock("GET", "/start")
            .with_status(302)
            .with_header("location", "/news/feed.xml")
            .expect(1)
            .create_async()
            .await;
        let feed = server
            .mock("GET", "/news/feed.xml")
            .with_status(200)
            .with_header("etag", "\"v1\"")
            .with_body(BODY)
            .expect(1)
            .create_async()
            .await;
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(Some(dir.path().into())).unwrap();
        let url = format!("{}/start", server.url());
        let base = format!("{}/news/feed.xml", server.url());
        let expected_url = format!("{}/news/posts/1", server.url());
        let params = FetchParams::default();
        let http = rss_cli::fetch::HttpClient::new(&params.user_agent, params.timeout).unwrap();
        http.fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .unwrap();
        redirect.assert_async().await;
        feed.assert_async().await;
        redirect.remove_async().await;
        // A validator may be answered at the original URL without redirecting again.
        let response = match policy {
            CachePolicy::Revalidate => Some(
                server
                    .mock("GET", "/start")
                    .match_header("if-none-match", "\"v1\"")
                    .with_status(304)
                    .expect(1)
                    .create_async()
                    .await,
            ),
            CachePolicy::StaleIfError => Some(
                server
                    .mock("GET", "/start")
                    .with_status(503)
                    .expect(1)
                    .create_async()
                    .await,
            ),
            _ => None,
        };
        let raw = http.fetch(&url, &cache, policy).await.unwrap();
        assert_eq!(raw.final_url, base, "policy {policy:?}");
        assert!(raw.from_cache);
        if let Some(response) = response {
            response.assert_async().await;
        }
        let cached_params = FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..params
        };
        let output = core::fetch_feeds(std::slice::from_ref(&url), &cached_params, &cache).await;
        let item = &output.feeds[0].items[0];
        assert_eq!(item.url.as_deref(), Some(expected_url.as_str()));
        let expected_id = identity::item_id(&url, Some(&expected_url), None, None, None).0;
        assert_eq!(item.id, expected_id);
        for key in [&expected_id, &expected_url, "post-1"] {
            let found = core::show_item(&url, key, &cached_params, &cache)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(found.id, expected_id);
        }
    }
}

#[tokio::test]
async fn legacy_cache_without_redirect_metadata_uses_requested_url() {
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::open(Some(dir.path().into())).unwrap();
    let url = "https://example.test/news/feed.xml";
    let meta: rss_cli::cache::CacheMeta = serde_json::from_value(serde_json::json!({
        "feed_url": url, "etag": null, "last_modified": null,
        "fetched_at": "2026-10-20T00:00:00Z", "content_type": null
    }))
    .unwrap();
    cache.put(&meta, BODY.as_bytes()).unwrap();
    let params = FetchParams {
        cache_policy: CachePolicy::CacheFirst,
        ..Default::default()
    };
    let output = core::fetch_feeds(&[url.into()], &params, &cache).await;
    assert_eq!(
        output.feeds[0].items[0].url.as_deref(),
        Some("https://example.test/news/posts/1")
    );
}
