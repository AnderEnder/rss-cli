//! End-to-end pagination: every page must resume exactly where the previous one stopped, and
//! must not re-hit the network for feeds page 1 already fetched (ADR-0017).

use rss_cli::cache::Cache;
use rss_cli::config::{CachePolicy, FetchParams};
use rss_cli::fetch::HttpClient;

/// Mirrors the MCP server's private `MCP_DEFAULT_LIMIT`. The reference fetch below has to use
/// the *same* per-feed cap as `fetch_feed`, or it would enumerate a different item set and the
/// comparison would fail for the wrong reason.
const MCP_DEFAULT_LIMIT: usize = 25;

fn feed(n: usize, tag: &str) -> String {
    let mut items = String::new();
    for i in 0..n {
        items.push_str(&format!(
            "<item><title>{tag} post {i}</title><link>https://example.com/{tag}/{i}</link>\
             <description>body {i} {}</description></item>",
            "filler ".repeat(40)
        ));
    }
    format!(
        "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>{tag}</title>\
         <link>https://example.com/</link>{items}</channel></rss>"
    )
}

fn ids(out: &rss_cli::FetchOutput) -> Vec<String> {
    out.feeds
        .iter()
        .flat_map(|f| f.items.iter().map(|i| i.id.clone()))
        .collect()
}

#[tokio::test]
async fn paged_round_trip_covers_every_item_exactly_once() {
    let mut server = mockito::Server::new_async().await;
    // `expect(1)`: each feed is fetched from the network exactly once, on page 1. Every
    // continuation forces CachePolicy::CacheFirst, so it must be served from the body cache.
    let a = server
        .mock("GET", "/a.xml")
        .with_status(200)
        .with_body(feed(10, "a"))
        .expect(1)
        .create_async()
        .await;
    let b = server
        .mock("GET", "/b.xml")
        .with_status(200)
        .with_body(feed(10, "b"))
        .expect(1)
        .create_async()
        .await;

    let dir = tempfile::tempdir().expect("temp cache dir");
    let cache = Cache::open(Some(dir.path().to_path_buf())).unwrap();
    let http = HttpClient::new("rss-cli-test", std::time::Duration::from_secs(10)).unwrap();

    let urls = vec![
        format!("{}/a.xml", server.url()),
        format!("{}/b.xml", server.url()),
    ];

    // A budget too small for all 20 items forces continuation pages.
    let budget = 1500;
    let mut pages: Vec<rss_cli::FetchOutput> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut exhausted = false;
    for _ in 0..10 {
        let page = rss_cli::mcp::fetch_page_for_test(&http, &cache, &urls, budget, cursor).await;
        assert!(
            page.total_items > 0,
            "every page must ship at least one item"
        );
        // The page is measured *with* the truncation marker and cursor attached: reserving
        // headroom for them is what keeps the emitted response inside the caller's budget.
        let emitted = rss_cli::core::estimate_response_tokens(&page);
        assert!(
            emitted <= budget,
            "the emitted page (marker and cursor included) must fit the budget: {emitted} > {budget}"
        );
        cursor = page.truncation.as_ref().and_then(|t| t.next_cursor.clone());
        pages.push(page);
        if cursor.is_none() {
            exhausted = true;
            break;
        }
    }
    assert!(
        exhausted,
        "pagination must terminate: still handing back a cursor after {} pages",
        pages.len()
    );
    assert!(
        pages.len() >= 2,
        "a {budget}-token budget must not fit 20 items in one page"
    );
    assert!(
        pages[0]
            .truncation
            .as_ref()
            .is_some_and(|t| t.items_omitted > 0),
        "page 1 must report the items it deferred: {:?}",
        pages[0].truncation
    );

    // The unpaginated item sequence, served from the same cache so it costs no network call.
    // `fetch_feed` params: default content format, the default per-feed cap, no since/limit.
    let params = FetchParams {
        limit: Some(MCP_DEFAULT_LIMIT),
        cache_policy: CachePolicy::CacheFirst,
        ..FetchParams::default()
    };
    let reference = rss_cli::core::fetch_feeds_with(&urls, &params, &cache, &http).await;
    let expected = ids(&reference);
    assert_eq!(expected.len(), 20, "fixture sanity: 2 feeds x 10 items");

    let got: Vec<String> = pages.iter().flat_map(ids).collect();
    assert_eq!(
        got, expected,
        "the paged id sequence must equal the unpaginated one exactly — no duplicates, no \
         skipped items, same order"
    );

    // Feeds a and b were fetched on page 1; every continuation must come from the cache.
    a.assert_async().await;
    b.assert_async().await;
}
