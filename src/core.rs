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
    let http = match HttpClient::new(&params.user_agent, params.timeout) {
        Ok(c) => c,
        Err(e) => {
            // Cannot build the client: every feed fails identically (already in url order).
            let mut output = FetchOutput::new(now_rfc3339());
            for url in urls {
                let obj = e.to_error_obj(Some(url));
                output.errors.push(obj.clone());
                output.feeds.push(FeedResult::error(url.clone(), obj));
            }
            populate_totals(&mut output);
            return output;
        }
    };
    fetch_feeds_with(urls, params, cache, &http).await
}

/// Fetch many feeds concurrently through a **caller-provided** [`HttpClient`]. The MCP server
/// builds the client once and shares it across tool calls so their per-host pacing coordinates
/// (ADR-0016); [`fetch_feeds`] is the thin wrapper that builds a client per call for the CLI.
pub async fn fetch_feeds_with(
    urls: &[String],
    params: &FetchParams,
    cache: &Cache,
    http: &HttpClient,
) -> FetchOutput {
    let mut output = FetchOutput::new(now_rfc3339());

    // Tag each task with its input index so we can restore request order after the
    // completion-ordered `buffer_unordered` stream — `feeds[]`/`errors[]` are then
    // deterministic within a run (an agent can address feeds by position). See ADR-0012.
    let mut results: Vec<(usize, FeedResult, Vec<Warning>)> =
        stream::iter(urls.iter().cloned().enumerate())
            .map(|(idx, url)| async move {
                match fetch_one(&url, http, params, cache).await {
                    Ok((fr, warnings)) => (idx, fr, warnings),
                    Err(e) => (
                        idx,
                        FeedResult::error(url.clone(), e.to_error_obj(Some(&url))),
                        Vec::new(),
                    ),
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

/// Recompute every derived count — per-feed `item_count`/`content_tokens_est_total` and the
/// top-level totals — from the items actually present. Called after any trim so no contract
/// field is left describing items that were shed.
fn refresh_feed_counts(output: &mut FetchOutput) {
    for feed in &mut output.feeds {
        feed.item_count = feed.items.len();
        feed.content_tokens_est_total = feed
            .items
            .iter()
            .map(|i| u64::from(i.content_tokens_est))
            .sum();
    }
    populate_totals(output);
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

/// Fetch a feed (cache-first) and return the single item whose `id`, raw `guid`, or resolved
/// `url` equals `key`, if present.
///
/// Used by `rss show` and the MCP `get_item` tool. `id` is namespaced by `feed_url` (see
/// ADR-0003); a `guid` (e.g. Reddit `t3_…`) is feed-window-independent and is the reliable
/// key across different feed URLs. The lookup is cache-first (ADR-0014): an item the caller
/// already saw survives a rolled feed window, but not a later cache-overwriting refetch.
pub async fn show_item(
    feed_url: &str,
    key: &str,
    params: &FetchParams,
    cache: &Cache,
) -> Result<Option<crate::model::Item>, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    let (fr, _warnings) = fetch_one(feed_url, &http, params, cache).await?;
    Ok(fr.items.into_iter().find(|it| {
        it.id == key || it.guid.as_deref() == Some(key) || it.url.as_deref() == Some(key)
    }))
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
    Err(too_large_error(
        estimated,
        budget_tokens,
        item_count(output),
    ))
}

/// Build the [`RssError::ResponseTooLarge`] for a payload of `estimated` tokens holding
/// `items` items, with concrete retry suggestions the calling agent can act on.
///
/// Split out of [`enforce_response_budget`] so [`paginate`] can raise the same error from the
/// *pre-trim* measurements after it has already mutated `output`.
fn too_large_error(estimated: usize, budget_tokens: usize, items: usize) -> RssError {
    let n = items.max(1);
    // Scale the item cap down by how far over budget we are, with a 10% safety margin.
    let suggested_limit = (((n as f64) * (budget_tokens as f64) / (estimated as f64)) * 0.9)
        .floor()
        .max(1.0) as usize;
    // Reserve ~30% of the budget for per-item metadata (titles, urls, ids, …); spread the
    // rest across items as content characters (~4 chars/token), with a sane floor.
    let content_budget_tokens = budget_tokens * 7 / 10;
    let suggested_max_content_chars = (content_budget_tokens.saturating_mul(4) / n).max(200);

    RssError::ResponseTooLarge {
        estimated_tokens: estimated,
        budget_tokens,
        suggested_limit,
        suggested_max_content_chars,
    }
}

/// Where a truncated page stopped, so the caller can mint a continuation cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageStop {
    /// Index into the **pre-trim** feed list — equivalently, into the request URL list —
    /// where the next page resumes. May be `>= output.feeds.len()` once trimming has dropped
    /// feeds, so do not use it to index the trimmed `output.feeds`.
    pub feed_idx: usize,
    /// Index into that feed's **pre-trim** item list where the next page resumes. Always
    /// `< feed_item_count`, so a continuation never has to treat it as "advance to the next
    /// feed".
    pub item_idx: usize,
    /// How many items the resumed feed held before shedding, for roll detection.
    pub feed_item_count: usize,
    pub items_omitted: usize,
    pub feeds_omitted: usize,
}

/// Trim `output` in place to fit `budget_tokens`, returning where the next page resumes.
///
/// This is the **fill** counterpart to [`enforce_response_budget`]'s **reject**. With a
/// batch of URLs, rejecting would discard every successful network fetch because one
/// trailing item overflowed, and the retry would re-hit every host (ADR-0017).
///
/// Returns `Ok(None)` when everything fits, `Ok(Some(stop))` when the page was trimmed, and
/// `Err(ResponseTooLarge)` only when the page cannot make progress — a *single* item that
/// does not fit on its own, or an envelope already over budget with no items to shed. Those
/// are the cases pagination cannot resolve; returning a stop for them would mint a cursor
/// identical to the request start and loop the caller forever.
pub fn paginate(
    output: &mut FetchOutput,
    budget_tokens: usize,
) -> Result<Option<PageStop>, RssError> {
    let estimated = estimate_response_tokens(output);
    if estimated <= budget_tokens {
        return Ok(None);
    }

    let original_counts: Vec<usize> = output.feeds.iter().map(|f| f.items.len()).collect();
    let original_items: usize = original_counts.iter().sum();
    // Measured before any trimming, so the suggestions describe the payload the caller asked
    // for rather than the reduced one.
    let too_large = || too_large_error(estimated, budget_tokens, original_items);

    if original_items == 0 {
        // Nothing to shed: the envelope alone (feed metadata, errors, warnings) is over
        // budget, which no amount of paging can fix.
        return Err(too_large());
    }

    // Cost of the envelope with no items at all: feed metadata, errors, warnings, totals.
    let mut skeleton = output.clone();
    for feed in &mut skeleton.feeds {
        feed.items.clear();
    }
    refresh_feed_counts(&mut skeleton);
    let base = estimate_response_tokens(&skeleton);

    // Greedily admit items in (feed, item) order until the next one would overflow.
    let mut running = base;
    let mut stop: Option<(usize, usize)> = None;
    'outer: for (fi, feed) in output.feeds.iter().enumerate() {
        for (ii, it) in feed.items.iter().enumerate() {
            let cost = serde_json::to_string_pretty(it)
                .map(|s| s.chars().count().div_ceil(4))
                .unwrap_or(0)
                + 2; // array punctuation and indentation between elements
            if running + cost > budget_tokens {
                stop = Some((fi, ii));
                break 'outer;
            }
            running += cost;
        }
    }

    let (stop_feed, stop_item) = match stop {
        // The greedy estimate says everything fits even though the real estimate did not;
        // shed the final item so we always make progress.
        None => {
            let last = output.feeds.len().saturating_sub(1);
            (last, output.feeds[last].items.len().saturating_sub(1))
        }
        Some(pos) => pos,
    };

    if stop_feed == 0 && stop_item == 0 {
        // Not even the first item fits. This is the genuine ResponseTooLarge case.
        return Err(too_large());
    }

    // Trim: truncate the stopping feed, drop every feed after it, and drop the stopping
    // feed entirely when it contributes nothing.
    output.feeds[stop_feed].items.truncate(stop_item);
    output
        .feeds
        .truncate(stop_feed + usize::from(stop_item > 0));
    refresh_feed_counts(output);

    // The per-item estimate is approximate — an item costs more nested inside the response
    // than pretty-printed on its own — so shed trailing items until the real estimate fits.
    while estimate_response_tokens(output) > budget_tokens {
        let Some(feed) = output.feeds.last_mut() else {
            break;
        };
        if feed.items.pop().is_none() {
            output.feeds.pop();
            continue;
        }
        refresh_feed_counts(output);
    }

    // A trailing feed the shed loop emptied ships as a husk claiming zero items; drop it so
    // the page only contains feeds that actually contributed. A feed that never had items
    // (an error entry) gave everything it had, so it stays.
    while let Some(last) = output.feeds.last() {
        let idx = output.feeds.len() - 1;
        if last.items.is_empty() && original_counts[idx] > 0 {
            output.feeds.pop();
        } else {
            break;
        }
    }
    refresh_feed_counts(output);

    // Resume at the first position that did not ship. Shipped positions are always a prefix
    // of the flattened (feed, item) sequence — the greedy pass admits in order, the trim
    // keeps a prefix, and shedding only pops the tail — so the first gap *is* the boundary.
    // Deriving both fields from this one scan is what keeps `feed_item_count` describing the
    // same feed as `feed_idx` even when shedding dropped back into an earlier feed.
    let resume = original_counts
        .iter()
        .enumerate()
        .find_map(|(idx, &count)| {
            let shipped = output.feeds.get(idx).map_or(0, |f| f.items.len());
            (shipped < count).then_some((idx, shipped))
        });
    let Some((feed_idx, item_idx)) = resume else {
        // Every item shipped and it still does not fit: only the envelope is left to cut.
        return Err(too_large());
    };
    if (feed_idx, item_idx) == (0, 0) {
        // Shedding left nothing to ship — a single item too large to stand on its own.
        return Err(too_large());
    }

    Ok(Some(PageStop {
        feed_idx,
        item_idx,
        feed_item_count: original_counts[feed_idx],
        items_omitted: original_items.saturating_sub(output.total_items),
        feeds_omitted: original_counts.len().saturating_sub(output.feeds.len()),
    }))
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
        feeds_omitted: 0,
        next_cursor: None,
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
    use crate::cache::{Cache, CacheMeta};
    use crate::config::CachePolicy;
    use crate::model::{ContentFormat, IdSource, Item};

    fn seed(cache: &Cache, url: &str, body: &[u8]) {
        let meta = CacheMeta {
            feed_url: url.to_string(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, body).expect("seed");
    }

    // A minimal RSS item with a distinct link and guid so we can match on each key.
    const FEED: &str = "https://t.example/r/x/.rss";
    const BODY: &str = r#"<rss version="2.0"><channel><title>x</title>
        <item><title>Post</title>
              <link>https://t.example/r/x/comments/abc/post/</link>
              <guid>t3_abc</guid>
              <pubDate>Mon, 02 Jun 2026 00:00:00 GMT</pubDate>
              <description>full body here</description></item>
        </channel></rss>"#;

    #[tokio::test]
    async fn show_item_matches_by_guid_and_url_cache_first() {
        let dir = std::env::temp_dir().join(format!("rss-core-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        seed(&cache, FEED, BODY.as_bytes());

        // CacheFirst means show_item never touches the network in this test.
        let params = FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..Default::default()
        };

        // First fetch the id the same way fetch_feed would compute it, then prove guid + url
        // resolve to the same item.
        let by_guid = show_item(FEED, "t3_abc", &params, &cache).await.unwrap();
        assert!(by_guid.is_some(), "guid lookup should resolve");
        let item = by_guid.unwrap();
        assert_eq!(item.guid.as_deref(), Some("t3_abc"));

        let by_url = show_item(
            FEED,
            "https://t.example/r/x/comments/abc/post/",
            &params,
            &cache,
        )
        .await
        .unwrap();
        assert_eq!(by_url.map(|i| i.id), Some(item.id.clone()));

        let by_id = show_item(FEED, &item.id, &params, &cache).await.unwrap();
        assert_eq!(by_id.map(|i| i.id), Some(item.id));

        std::fs::remove_dir_all(&dir).ok();
    }

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

    /// Append a second feed (same items, different url) to a single-feed fixture.
    fn with_second_feed(out: &mut FetchOutput) {
        let second = out.feeds[0].clone();
        out.feeds.push(FeedResult {
            feed_url: "https://example.com/two.xml".to_string(),
            ..second
        });
        populate_totals(out);
    }

    #[test]
    fn paginate_returns_everything_when_it_fits() {
        let mut out = output_with(vec![item(false), item(false)]);
        assert!(paginate(&mut out, 100_000).expect("fits").is_none());
        assert_eq!(out.feeds[0].items.len(), 2, "nothing should be shed");
    }

    #[test]
    fn paginate_fills_to_budget_and_reports_where_to_resume() {
        let mut out = output_with(vec![item(false), item(false), item(false), item(false)]);
        let full = estimate_response_tokens(&out);
        // A budget around half the full payload must keep some items and shed the rest.
        let stop = paginate(&mut out, full / 2)
            .expect("not an error")
            .expect("should truncate");

        let kept = out.feeds[0].items.len();
        assert!(kept >= 1, "at least one item must ship");
        assert!(kept < 4, "some items must be shed");
        assert_eq!(stop.feed_idx, 0);
        assert_eq!(
            stop.item_idx, kept,
            "resume exactly after the last shipped item"
        );
        assert_eq!(
            stop.feed_item_count, 4,
            "record the pre-shed count for roll detection"
        );
        assert_eq!(stop.items_omitted, 4 - kept);
        assert!(
            estimate_response_tokens(&out) <= full / 2,
            "the shipped page must actually fit the budget"
        );
        // Per-feed counts follow the shipped items.
        assert_eq!(out.feeds[0].item_count, kept);
        assert_eq!(out.total_items, kept);
    }

    #[test]
    fn paginate_errors_only_when_a_single_item_cannot_fit() {
        let mut out = output_with(vec![item(false)]);
        let err = paginate(&mut out, 1).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "one oversized item is the only case pagination cannot fix, got {err:?}"
        );
    }

    #[test]
    fn paginate_sheds_whole_trailing_feeds() {
        // Two feeds; the budget only admits the first feed's item. 70% of the full payload
        // sits inside the window that admits exactly one of the two items: the envelope plus
        // one item costs ~62% of the full payload, plus two items ~85%.
        let mut out = output_with(vec![item(false)]);
        with_second_feed(&mut out);
        let one_feed_budget = estimate_response_tokens(&out) * 7 / 10;

        let stop = paginate(&mut out, one_feed_budget)
            .expect("not an error")
            .expect("truncates");
        assert_eq!(out.feeds.len(), 1, "the trailing feed is dropped whole");
        assert_eq!(stop.feed_idx, 1, "resume at the dropped feed");
        assert_eq!(stop.item_idx, 0);
        assert_eq!(stop.feeds_omitted, 1);
        assert_eq!(
            stop.feed_item_count, 1,
            "the count describes the resumed feed"
        );
    }

    #[test]
    fn paginate_resume_never_skips_a_feed_the_shed_loop_emptied() {
        // Two feeds of two items. The budget is one token under the real cost of the state
        // the greedy per-item pass admits (both of feed 0's items plus one of feed 1's), so
        // the trailing-shed loop empties feed 1 after the fact. The resume position must
        // point *into* feed 1, not past it — otherwise its items are silently lost.
        let mut out = output_with(vec![item(false), item(false)]);
        with_second_feed(&mut out);

        let mut over_shipped = out.clone();
        over_shipped.feeds[1].items.truncate(1);
        refresh_feed_counts(&mut over_shipped);
        let budget = estimate_response_tokens(&over_shipped) - 1;

        let stop = paginate(&mut out, budget)
            .expect("not an error")
            .expect("truncates");
        assert_eq!(
            out.feeds.len(),
            1,
            "a trailing feed that ships nothing is dropped, not shipped as an empty husk"
        );
        assert_eq!(out.feeds[0].items.len(), 2, "feed 0 ships whole");
        assert_eq!(stop.feed_idx, 1, "resume in the emptied feed, not past it");
        assert_eq!(stop.item_idx, 0);
        assert_eq!(stop.feed_item_count, 2, "the count describes feed 1");
        assert_eq!(stop.items_omitted, 2, "both of feed 1's items are omitted");
        assert_eq!(stop.feeds_omitted, 1);
        assert!(
            estimate_response_tokens(&out) <= budget,
            "the page must fit"
        );
    }

    #[test]
    fn paginate_errors_when_shedding_leaves_nothing_to_ship() {
        // The greedy per-item estimate under-counts (an item nested in the response costs
        // more than pretty-printed on its own), so a budget just under one real item's cost
        // admits an item that the re-estimate then sheds. Shipping an empty page would mint
        // a cursor identical to the request start and loop the caller forever.
        let one_item = estimate_response_tokens(&output_with(vec![item(false)]));
        let mut out = output_with(vec![item(false), item(false)]);
        let err = paginate(&mut out, one_item - 1).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "a page that ships nothing must be an error, got {err:?}"
        );
    }

    #[test]
    fn paginate_errors_when_the_envelope_alone_is_over_budget() {
        // No items to shed: pagination cannot help, so say so instead of indexing into an
        // empty feed list.
        let mut empty = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        let err = paginate(&mut empty, 1).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "an over-budget envelope is not paginable, got {err:?}"
        );
    }

    #[test]
    fn paginate_accepts_an_output_exactly_at_budget() {
        let mut out = output_with(vec![item(false), item(false)]);
        let exact = estimate_response_tokens(&out);
        assert!(paginate(&mut out, exact).expect("fits").is_none());
        assert_eq!(out.feeds[0].items.len(), 2);
    }
}
