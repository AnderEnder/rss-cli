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
use crate::model::{DiscoverOutput, FeedResult, FeedStatus, FetchOutput, TruncationInfo, Warning};
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
            // Cannot build the client: every feed fails identically (already in url order).
            for url in urls {
                let obj = e.to_error_obj(Some(url));
                output.errors.push(obj.clone());
                output.feeds.push(FeedResult::error(url.clone(), obj));
            }
            populate_totals(&mut output);
            return output;
        }
    };

    // Tag each task with its input index so we can restore request order after the
    // completion-ordered `buffer_unordered` stream — `feeds[]`/`errors[]` are then
    // deterministic within a run (an agent can address feeds by position). See ADR-0012.
    let mut results: Vec<(usize, FeedResult, Vec<Warning>)> =
        stream::iter(urls.iter().cloned().enumerate())
            .map(|(idx, url)| {
                let http = &http;
                async move {
                    match fetch_one(&url, http, params, cache).await {
                        Ok((fr, warnings)) => (idx, fr, warnings),
                        Err(e) => (
                            idx,
                            FeedResult::error(url.clone(), e.to_error_obj(Some(&url))),
                            Vec::new(),
                        ),
                    }
                }
            })
            .buffer_unordered(params.concurrency.max(1))
            .collect()
            .await;

    results.sort_by_key(|(idx, _, _)| *idx);

    for (_, fr, warnings) in results {
        if let Some(err) = &fr.error {
            output.errors.push(err.clone());
        }
        output.warnings.extend(warnings);
        output.feeds.push(fr);
    }
    populate_totals(&mut output);
    output
}

/// Fill the top-level aggregate counts from the assembled feeds. Called once both feeds and
/// per-feed counts are final (post-`limit`/`--since`/truncation).
fn populate_totals(output: &mut FetchOutput) {
    output.total_items = output.feeds.iter().map(|f| f.items.len()).sum();
    output.total_content_tokens_est = output
        .feeds
        .iter()
        .flat_map(|f| &f.items)
        .map(|i| u64::from(i.content_tokens_est))
        .sum();
}

/// Fetch and parse a single feed, returning the [`FeedResult`] plus any non-fatal
/// [`Warning`]s the parse surfaced (e.g. a content-extraction fallback). Callers aggregate
/// the warnings into [`FetchOutput::warnings`].
pub async fn fetch_one(
    url: &str,
    http: &HttpClient,
    params: &FetchParams,
    cache: &Cache,
) -> Result<(FeedResult, Vec<Warning>), RssError> {
    let raw = http.fetch(url, cache, params.cache_policy).await?;
    let parsed = parse::parse_feed(&raw.body, url, params)?;
    let item_count = parsed.items.len();
    let content_tokens_est_total = parsed
        .items
        .iter()
        .map(|i| u64::from(i.content_tokens_est))
        .sum();
    let fr = FeedResult {
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
        item_count,
        content_tokens_est_total,
        items: parsed.items,
        error: None,
    };
    Ok((fr, parsed.warnings))
}

/// Discover feeds advertised on a website homepage.
pub async fn discover_feeds(
    site_url: &str,
    params: &FetchParams,
) -> Result<DiscoverOutput, RssError> {
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
    let (fr, _warnings) = fetch_one(feed_url, &http, params, cache).await?;
    Ok(fr.items.into_iter().find(|it| it.id == id))
}

/// Total number of items across every feed in `output`.
pub fn item_count(output: &FetchOutput) -> usize {
    output.feeds.iter().map(|f| f.items.len()).sum()
}

/// Rough token estimate of the *serialized* `output` — i.e. of the payload an MCP client
/// actually receives (pretty JSON, matching [`crate::mcp`]'s emission). Uses the same
/// `ceil(chars / 4)` heuristic as per-item content estimates.
pub fn estimate_response_tokens(output: &FetchOutput) -> usize {
    let json = serde_json::to_string_pretty(output).unwrap_or_default();
    json.chars().count().div_ceil(4)
}

/// Check `output` against a token `budget`, returning the estimate on success.
///
/// On overflow, returns [`RssError::ResponseTooLarge`] carrying concrete, machine-readable
/// retry suggestions (a smaller `limit` and a `max_content_chars`) so the calling agent can
/// self-recover instead of giving up. This is the cap-and-error path; it never mutates
/// `output`.
pub fn enforce_response_budget(
    output: &FetchOutput,
    budget_tokens: usize,
) -> Result<usize, RssError> {
    let estimated = estimate_response_tokens(output);
    if estimated <= budget_tokens {
        return Ok(estimated);
    }

    let n = item_count(output).max(1);
    // Scale the item cap down by how far over budget we are, with a 10% safety margin.
    let suggested_limit = (((n as f64) * (budget_tokens as f64) / (estimated as f64)) * 0.9)
        .floor()
        .max(1.0) as usize;
    // Reserve ~30% of the budget for per-item metadata (titles, urls, ids, …); spread the
    // rest across items as content characters (~4 chars/token), with a sane floor.
    let content_budget_tokens = budget_tokens * 7 / 10;
    let suggested_max_content_chars = (content_budget_tokens.saturating_mul(4) / n).max(200);

    Err(RssError::ResponseTooLarge {
        estimated_tokens: estimated,
        budget_tokens,
        suggested_limit,
        suggested_max_content_chars,
    })
}

/// Build the [`TruncationInfo`] marker for `output`, or `None` when nothing was actually
/// cut.
///
/// The marker is emitted **only when item content was truncated** (or, in future, items
/// were omitted) — i.e. when the agent is genuinely not seeing the full data. A bare item
/// cap that dropped nothing is *not* reported here: the MCP `fetch_feed` default of 25 is
/// documented in the tool description, so a non-`null` `truncation` on an untruncated
/// response would only mislead. `applied_limit` is recorded for context when the marker
/// *is* emitted (the MCP server passes its effective limit; the CLI passes `None`).
pub fn truncation_marker(
    output: &FetchOutput,
    applied_limit: Option<usize>,
    suggestion: Option<String>,
) -> Option<TruncationInfo> {
    let items_content_truncated = output
        .feeds
        .iter()
        .flat_map(|f| &f.items)
        .filter(|i| i.content_truncated)
        .count();

    if items_content_truncated == 0 {
        return None;
    }

    Some(TruncationInfo {
        applied_limit,
        items_content_truncated,
        items_omitted: 0,
        estimated_tokens: None,
        suggestion,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentFormat, IdSource, Item};

    fn item(content_truncated: bool) -> Item {
        Item {
            id: "deadbeefdeadbeef".to_string(),
            id_source: IdSource::Link,
            feed_url: "https://example.com/feed.xml".to_string(),
            title: Some("Title".to_string()),
            url: Some("https://example.com/a".to_string()),
            authors: vec![],
            published: Some("2026-01-01T00:00:00Z".to_string()),
            updated: None,
            summary: None,
            content: Some("body".to_string()),
            content_format: ContentFormat::Markdown,
            content_tokens_est: 1,
            content_truncated,
            content_hash: Some("00112233aabbccdd".to_string()),
            categories: vec![],
            enclosures: vec![],
            guid: None,
        }
    }

    fn output_with(items: Vec<Item>) -> FetchOutput {
        let mut out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        let item_count = items.len();
        let content_tokens_est_total = items.iter().map(|i| u64::from(i.content_tokens_est)).sum();
        out.feeds.push(FeedResult {
            feed_url: "https://example.com/feed.xml".to_string(),
            status: FeedStatus::Ok,
            from_cache: false,
            title: Some("Feed".to_string()),
            site_url: None,
            updated: None,
            item_count,
            content_tokens_est_total,
            items,
            error: None,
        });
        populate_totals(&mut out);
        out
    }

    #[test]
    fn budget_ok_under_limit() {
        let out = output_with(vec![item(false)]);
        let est = enforce_response_budget(&out, 100_000).expect("under budget");
        assert!(est > 0);
    }

    #[test]
    fn budget_overflow_yields_actionable_error() {
        let out = output_with(vec![item(false), item(false), item(false)]);
        // A tiny budget forces overflow.
        let err = enforce_response_budget(&out, 1).unwrap_err();
        match err {
            RssError::ResponseTooLarge {
                budget_tokens,
                suggested_limit,
                suggested_max_content_chars,
                estimated_tokens,
            } => {
                assert_eq!(budget_tokens, 1);
                assert!(estimated_tokens > 1);
                assert!(suggested_limit >= 1);
                assert!(suggested_max_content_chars >= 200);
            }
            other => panic!("expected ResponseTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn populate_totals_sums_items_and_tokens() {
        // item() has content_tokens_est = 1.
        let out = output_with(vec![item(false), item(false), item(true)]);
        assert_eq!(out.total_items, 3);
        assert_eq!(out.total_content_tokens_est, 3);
        // Per-feed counts mirror the aggregate for a single feed.
        assert_eq!(out.feeds[0].item_count, 3);
        assert_eq!(out.feeds[0].content_tokens_est_total, 3);
    }

    #[test]
    fn marker_none_when_nothing_bounded() {
        let out = output_with(vec![item(false)]);
        assert!(truncation_marker(&out, None, None).is_none());
        // A bare item cap that dropped nothing is NOT reported as truncation, even when an
        // applied_limit is passed — only actual content truncation emits the marker.
        assert!(truncation_marker(&out, Some(25), None).is_none());
    }

    #[test]
    fn marker_reports_applied_limit_and_truncated_count() {
        let out = output_with(vec![item(true), item(false)]);
        let m = truncation_marker(&out, Some(25), Some("hint".to_string())).expect("marker");
        assert_eq!(m.applied_limit, Some(25));
        assert_eq!(m.items_content_truncated, 1);
        assert_eq!(m.items_omitted, 0);
        assert_eq!(m.suggestion.as_deref(), Some("hint"));
    }
}
