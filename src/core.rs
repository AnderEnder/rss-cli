//! Orchestration shared by the CLI and the MCP server.
//!
//! This wiring is intentionally implemented up front against the *frozen signatures* of
//! [`crate::fetch`] and [`crate::parse`]. It compiles before those modules are filled in
//! and runs unchanged once they are, so there is no integration step for the seam itself.

use chrono::Utc;
use futures::stream::{self, StreamExt};

use crate::cache::Cache;
use crate::config::FetchParams;
use crate::error::RssError;
use crate::fetch::HttpClient;
use crate::model::{DiscoverOutput, FeedResult, FeedStatus, FetchOutput};
use crate::{discover, parse};

/// Fetch and parse many feeds concurrently, returning the full structured output.
///
/// Partial failure is the norm: a feed that errors becomes a [`FeedStatus::Error`] entry
/// (and is mirrored into [`FetchOutput::errors`]); successful feeds are unaffected.
pub async fn fetch_feeds(urls: &[String], params: &FetchParams, cache: &Cache) -> FetchOutput {
    let mut output = FetchOutput::new(now_rfc3339());

    let http = match HttpClient::new(&params.user_agent, params.timeout) {
        Ok(c) => c,
        Err(e) => {
            // Cannot build the client: every feed fails identically.
            for url in urls {
                let obj = e.to_error_obj(Some(url));
                output.errors.push(obj.clone());
                output.feeds.push(FeedResult::error(url.clone(), obj));
            }
            return output;
        }
    };

    let results: Vec<FeedResult> = stream::iter(urls.iter().cloned())
        .map(|url| {
            let http = &http;
            async move {
                match fetch_one(&url, http, params, cache).await {
                    Ok(fr) => fr,
                    Err(e) => FeedResult::error(url.clone(), e.to_error_obj(Some(&url))),
                }
            }
        })
        .buffer_unordered(params.concurrency.max(1))
        .collect()
        .await;

    for fr in results {
        if let Some(err) = &fr.error {
            output.errors.push(err.clone());
        }
        output.feeds.push(fr);
    }
    output
}

/// Fetch and parse a single feed.
pub async fn fetch_one(
    url: &str,
    http: &HttpClient,
    params: &FetchParams,
    cache: &Cache,
) -> Result<FeedResult, RssError> {
    let raw = http.fetch(url, cache, params.cache_policy).await?;
    let parsed = parse::parse_feed(&raw.body, url, params)?;
    Ok(FeedResult {
        feed_url: url.to_string(),
        status: if raw.not_modified {
            FeedStatus::NotModified
        } else {
            FeedStatus::Ok
        },
        from_cache: raw.from_cache,
        title: parsed.title,
        site_url: parsed.site_url,
        updated: parsed.updated,
        items: parsed.items,
        error: None,
    })
}

/// Discover feeds advertised on a website homepage.
pub async fn discover_feeds(site_url: &str, params: &FetchParams) -> Result<DiscoverOutput, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    discover::discover(site_url, &http).await
}

/// Fetch a feed and return the single item matching the stable `id`, if present.
///
/// Used by `rss show` and the MCP `get_item` tool. Because item ids are deterministic,
/// the lookup is stable across runs.
pub async fn show_item(
    feed_url: &str,
    id: &str,
    params: &FetchParams,
    cache: &Cache,
) -> Result<Option<crate::model::Item>, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    let fr = fetch_one(feed_url, &http, params, cache).await?;
    Ok(fr.items.into_iter().find(|it| it.id == id))
}

/// Determine the appropriate process exit code from a [`FetchOutput`].
pub fn exit_code_for(output: &FetchOutput) -> i32 {
    use crate::error::exit;
    let total = output.feeds.len();
    let failed = output
        .feeds
        .iter()
        .filter(|f| f.status == FeedStatus::Error)
        .count();
    if total == 0 || failed == 0 {
        exit::OK
    } else if failed == total {
        exit::ALL_FAILED
    } else {
        exit::PARTIAL
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
