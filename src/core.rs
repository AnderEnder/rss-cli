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

/// One feed's outcome plus the warnings its parse raised, tagged with the feed's index into the
/// request URL list so request order survives the completion-ordered stream. `None` marks a feed
/// that was never attempted because the batch deadline had passed.
type IndexedFetch = (usize, Option<(FeedResult, Vec<Warning>)>);

/// Fetch many feeds concurrently through a **caller-provided** [`HttpClient`]. The MCP server
/// builds the client once and shares it across tool calls so their per-host pacing coordinates
/// (ADR-0016); [`fetch_feeds`] is the thin wrapper that builds a client per call for the CLI.
///
/// With [`FetchParams::deadline`] set, this is a **soft** wall-clock bound: a fetch already in
/// flight runs to completion (bounded separately by the request timeout), but no new one starts
/// once the deadline has passed. The feeds left unattempted are reported in
/// [`FetchOutput::truncation`] as `feeds_omitted` rather than fabricated as errors — they did
/// not fail, they were never tried.
pub async fn fetch_feeds_with(
    urls: &[String],
    params: &FetchParams,
    cache: &Cache,
    http: &HttpClient,
) -> FetchOutput {
    let output = FetchOutput::new(now_rfc3339());
    let stop_at = params.deadline.map(|d| tokio::time::Instant::now() + d);

    // Tag each task with its input index so we can restore request order after the
    // completion-ordered `buffer_unordered` stream — `feeds[]`/`errors[]` are then
    // deterministic within a run (an agent can address feeds by position). See ADR-0012.
    let results: Vec<IndexedFetch> = stream::iter(urls.iter().cloned().enumerate())
        .map(|(idx, url)| async move {
            // Known limitation (recorded for ADR-0017): this bounds only when a fetch may
            // *start*. `buffer_unordered` polls the first `concurrency` futures immediately, so
            // they all pass this check at t≈0 and then queue on the per-host permit, where no
            // clock reaches them — a call can outlive the deadline by up to `concurrency - 1`
            // serialized fetches (see `FetchParams::deadline`). The deeper fix is to thread the
            // deadline into the gate's permit acquire (`timeout_at` on the semaphore), which
            // touches `fetch`/`ratelimit` and is deliberately out of scope here.
            if let Some(t) = stop_at
                && tokio::time::Instant::now() >= t
            {
                return (idx, None);
            }
            match fetch_one(&url, http, params, cache).await {
                Ok((fr, warnings)) => (idx, Some((fr, warnings))),
                Err(e) => (
                    idx,
                    Some((
                        FeedResult::error(url.clone(), e.to_error_obj(Some(&url))),
                        Vec::new(),
                    )),
                ),
            }
        })
        .buffer_unordered(params.concurrency.max(1))
        .collect()
        .await;

    assemble_prefix_in_request_order(output, params.limit, results)
}

/// Fold the completion-ordered fetch results back into request order, keeping `feeds[]` a
/// contiguous **prefix** of the requested URLs and reporting whatever the deadline left
/// unattempted as `truncation.feeds_omitted`.
///
/// Pure by design — no clock, no network, no I/O — so the out-of-order case only a deadline can
/// produce (feeds 0, 2, 3 done, feed 1 never started) is a deterministic unit test instead of a
/// timing race. `output` is passed in already stamped, so `fetched_at` still marks the *start*
/// of the batch rather than the moment assembly ran.
///
/// `results` must hold exactly one entry per requested URL (which `buffer_unordered` guarantees
/// — it yields every future's output, only out of order), so its length is the request count.
fn assemble_prefix_in_request_order(
    mut output: FetchOutput,
    applied_limit: Option<usize>,
    mut results: Vec<IndexedFetch>,
) -> FetchOutput {
    let requested = results.len();
    results.sort_by_key(|(idx, _)| *idx);

    // Keep `feeds[]` a contiguous prefix of `urls`: stop at the first unattempted feed and
    // discard anything past it, even when it completed. `buffer_unordered` finishes out of
    // order, so the deadline can leave a gap (feeds 0, 2, 3 done, feed 1 never started) — and
    // every consumer that maps a feed position back to a URL, the continuation cursor above
    // all, assumes `feeds[j]` is `urls[j]`. Shipping the out-of-order survivors would save a
    // couple of fetches and mis-address every feed after the gap.
    let first_unattempted = results
        .iter()
        .position(|(_, r)| r.is_none())
        .unwrap_or(requested);

    for (_, slot) in results.into_iter().take(first_unattempted) {
        // Unreachable by construction — every slot before the first `None` is `Some` — but
        // silently keeping the prefix contiguous beats an unwrap on an invariant.
        let Some((fr, warnings)) = slot else { continue };
        if let Some(err) = &fr.error {
            output.errors.push(err.clone());
        }
        output.warnings.extend(warnings);
        output.feeds.push(fr);
    }
    populate_totals(&mut output);

    let omitted = requested - first_unattempted;
    if omitted > 0 {
        output.truncation = Some(TruncationInfo {
            applied_limit,
            items_content_truncated: 0,
            items_omitted: 0,
            feeds_omitted: omitted,
            estimated_tokens: None,
            // Left for the front-end to mint: `core` has no cursor concept (the CLI has no
            // pagination at all), and the MCP server owns the request fingerprint a cursor
            // has to carry.
            next_cursor: None,
            suggestion: Some(format!(
                "{omitted} feed(s) were not fetched before the batch deadline; request them \
                 separately, or pass truncation.next_cursor back as `cursor` if one is present"
            )),
        });
    }
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
///
/// Public so front-ends that trim a `FetchOutput` themselves keep the derived fields in one
/// place (invariant 9): the MCP server calls it after dropping the items a continuation
/// cursor already delivered.
pub fn refresh_feed_counts(output: &mut FetchOutput) {
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
    let cached_at = raw.cached_at.clone();
    // `.ok()` -> `None` here is defensive, not expected: `cached_at` is always written by
    // `now_rfc3339()` (see `cache.rs`/`fetch.rs`), the only producer, so parsing should never
    // fail in practice. If it ever did, `cached_at` would stay `Some(<string>)` while
    // `cache_age_seconds` fell back to `None` — an inconsistent pair we accept rather than
    // add error handling for a case that cannot occur.
    let cache_age_seconds = cached_at.as_deref().and_then(|ts| {
        chrono::DateTime::parse_from_rfc3339(ts)
            .ok()
            .map(|dt| (Utc::now() - dt.with_timezone(&Utc)).num_seconds().max(0) as u64)
    });
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
        cached_at,
        cache_age_seconds,
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

/// Rough token estimate of the *serialized* `output` (pretty JSON, matching [`crate::mcp`]'s
/// emission). Uses the same `ceil(chars / 4)` heuristic as per-item content estimates.
///
/// This measures the **payload only**. An MCP `CallToolResult` also carries a one-line text
/// summary next to the `structuredContent`, which is not counted here — callers budgeting
/// against a client limit must reserve for it separately (`mcp::CURSOR_HEADROOM_TOKENS` does).
pub fn estimate_response_tokens(output: &FetchOutput) -> usize {
    let json = serde_json::to_string_pretty(output).unwrap_or_default();
    json.chars().count().div_ceil(4)
}

/// Token cost of one *part* of a response — an item, a feed envelope — pretty-printed on its
/// own, plus a couple of tokens for the array punctuation it picks up once nested.
/// Deliberately approximate and low: [`paginate`]'s greedy pass adds these up and a real
/// [`estimate_response_tokens`] measurement corrects the residue afterwards.
///
/// **This must never over-estimate.** Nesting actually costs far more than the `+ 2` here
/// (indentation alone runs ~12 tokens per feed envelope and ~38 per item), and that gap is
/// load-bearing: it is what makes the greedy total a lower bound on the real payload, so a
/// greedy rejection implies a real one. Raise this constant to "improve accuracy" and a
/// greedy total can exceed the real cost — which rejects pages that would have fit, the
/// false `RESPONSE_TOO_LARGE` this accounting exists to prevent.
fn pretty_tokens<T: serde::Serialize>(value: &T) -> usize {
    serde_json::to_string_pretty(value)
        .map(|s| s.chars().count().div_ceil(4))
        .unwrap_or(0)
        + 2
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
    /// `<= feed_item_count`; the two are equal only at `0 == 0`, which means "resume at this
    /// feed, which held no items when the page was minted" (a feed the trim dropped whole
    /// because its envelope did not fit). Otherwise `item_idx < feed_item_count`, so a
    /// continuation never has to treat it as "advance to the next feed". A `0` recorded count
    /// is harmless for roll detection: comparing it against the continuation's actual count
    /// correctly reads a later non-zero count as a roll.
    pub item_idx: usize,
    /// How many items the resumed feed held before shedding, for roll detection.
    pub feed_item_count: usize,
    /// How many items the page left behind. May be `0` on a returned stop — when only a
    /// trailing feed *envelope* was deferred — so treat `Some(stop)` itself as the
    /// truncation signal, not `items_omitted > 0`.
    pub items_omitted: usize,
    /// How many feeds the page dropped **whole**, not how many it delivered incompletely:
    /// for `[f0(4 items → 2), f1(3 items)]` this is `1` (only `f1` was dropped), not `2`.
    pub feeds_omitted: usize,
}

/// Trim `output` in place to fit `budget_tokens`, returning where the next page resumes.
///
/// This is the **fill** counterpart to [`enforce_response_budget`]'s **reject**. With a
/// batch of URLs, rejecting would discard every successful network fetch because one
/// trailing item overflowed, and the retry would re-hit every host (ADR-0017).
///
/// Returns `Ok(None)` when everything fits and `Ok(Some(stop))` when the page was trimmed.
/// `Err(ResponseTooLarge)` means, exactly, that **zero items shipped**: a request that
/// carried items could not place a single one inside the budget (one oversized item, or an
/// envelope that leaves no room), or there were no items to place to begin with. That is the
/// invariant a cursor loop depends on — a stop that shipped nothing would mint a cursor no
/// further along than the request that produced it and loop the caller forever.
///
/// On `Err`, `output` **may** have been partially trimmed already (the trim, shed, and husk
/// passes run before the last checks); discard it and surface the error alone. This is
/// unlike the sibling [`enforce_response_budget`], which never mutates.
///
/// Precondition on `budget_tokens`: the page is measured *as it stands*. Anything the caller
/// attaches afterwards — a `next_cursor`, a [`TruncationInfo`] marker — is not counted, so
/// pass a budget already reduced by that headroom (order tens of tokens).
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

    // Cost of the response envelope with no feeds at all: totals, errors, warnings,
    // truncation. Feed envelopes are charged separately below, so that feeds this page never
    // reaches are never charged — folding them all in here inflated the baseline by one
    // envelope per URL and made the greedy pass reject pages that fit.
    let mut skeleton = output.clone();
    skeleton.feeds.clear();
    refresh_feed_counts(&mut skeleton);
    let base = estimate_response_tokens(&skeleton);

    // What each feed's own metadata costs once its items are stripped, in the same currency
    // as the per-item cost below. A feed that never had items is never charged here (nothing
    // admits it), so the greedy pass under-counts a page carrying error entries; that is what
    // the shed loop below is for. Do not fold these back into `base`.
    let feed_envelopes: Vec<usize> = output
        .feeds
        .iter()
        .map(|feed| {
            let mut husk = feed.clone();
            husk.items.clear();
            husk.item_count = 0;
            husk.content_tokens_est_total = 0;
            pretty_tokens(&husk)
        })
        .collect();

    // Greedily admit items in (feed, item) order until the next one would overflow, charging
    // a feed's envelope when its first item is admitted.
    let mut running = base;
    let mut stop: Option<(usize, usize)> = None;
    'outer: for (fi, feed) in output.feeds.iter().enumerate() {
        for (ii, it) in feed.items.iter().enumerate() {
            let cost = pretty_tokens(it) + if ii == 0 { feed_envelopes[fi] } else { 0 };
            if running + cost > budget_tokens {
                stop = Some((fi, ii));
                break 'outer;
            }
            running += cost;
        }
    }

    let (stop_feed, stop_item) = match stop {
        // The greedy estimate says everything fits even though the real estimate did not, so
        // stop just past the end: at the final feed's last item, which the truncate below
        // sheds to make progress. When that feed holds no items there is no item to shed and
        // the feed itself is dropped instead — the resume scan picks it back up as a target.
        None => {
            let last = output.feeds.len().saturating_sub(1);
            (last, output.feeds[last].items.len().saturating_sub(1))
        }
        Some(pos) => pos,
    };

    if stop_feed == 0 && stop_item == 0 {
        // Feed 0's envelope plus its first item do not fit. This is the genuine
        // ResponseTooLarge case, and — like the `original_items == 0` guard above — it
        // returns before `output` has been touched. Reachable only when feed 0 *has* items:
        // the greedy loop skips a leading zero-item feed entirely, so the earliest stop it
        // can report there is `(1, 0)`. A page that ships nothing anyway — because that
        // leading envelope was all that fit — exits via the `item_count` guard below.
        return Err(too_large());
    }

    // Trim: truncate the stopping feed, drop every feed after it, and drop the stopping
    // feed entirely when it contributes nothing. A trailing feed that never had items is
    // dropped here too; the resume scan below recovers it as a named resume target rather
    // than letting it vanish silently.
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
    // the page only contains feeds that actually contributed. A feed that never had items (an
    // error entry) gave everything it had, so *this* pass keeps it — but the two passes above
    // can still have dropped it: the trim drops the stopping feed whole when nothing of it
    // ships, and the shed loop pops a trailing feed ungated by `original_counts` when the page
    // is still over budget. Either way the resume scan below recovers it as a named target.
    while let Some(last) = output.feeds.last() {
        let idx = output.feeds.len() - 1;
        if last.items.is_empty() && original_counts[idx] > 0 {
            output.feeds.pop();
        } else {
            break;
        }
    }
    refresh_feed_counts(output);

    // Forward progress is measured in items, not position: a leading feed with no items makes
    // the resume scan skip past it, so a position-based guard would let a page that shipped
    // nothing mint a cursor pointing at its own start. `original_items > 0` here (the
    // no-items case returned above), so an empty page means nothing was placed at all.
    if item_count(output) == 0 {
        return Err(too_large());
    }

    // Resume at the first feed the page does not carry in full — including a feed dropped
    // whole, which a zero-item feed always is. Shipped positions are always a prefix of the
    // flattened (feed, item) sequence — the greedy pass admits in order, the trim keeps a
    // prefix, and shedding only pops the tail — so the first gap *is* the boundary. Deriving
    // both fields from this one scan is what keeps `feed_item_count` describing the same feed
    // as `feed_idx` even when shedding dropped back into an earlier feed.
    let resume = (0..original_counts.len()).find_map(|idx| match output.feeds.get(idx) {
        // Dropped entirely: resume at its first item (or, for a zero-item feed, at the feed).
        None => Some((idx, 0)),
        Some(feed) if feed.items.len() < original_counts[idx] => Some((idx, feed.items.len())),
        Some(_) => None,
    });
    let Some((feed_idx, item_idx)) = resume else {
        // Every feed is present carrying every one of its items, so there is no boundary to
        // report and only the envelope is left to cut. This arm reads the other way round
        // from the documented "zero items shipped" meaning of the error — but it is
        // unreachable in practice (the trim always removes at least an item or a feed before
        // this scan), so the documented invariant stands. If it ever is reached, "nothing was
        // deferred yet it still does not fit" is a genuine overflow.
        return Err(too_large());
    };

    Ok(Some(PageStop {
        feed_idx,
        item_idx,
        feed_item_count: original_counts[feed_idx],
        items_omitted: original_items.saturating_sub(item_count(output)),
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

    #[tokio::test]
    async fn fetch_one_reports_cache_age_for_a_cached_feed() {
        let dir = std::env::temp_dir().join(format!("rss-age-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        seed(&cache, FEED, BODY.as_bytes()); // seeded with fetched_at = 2020-01-01

        let params = FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..Default::default()
        };
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let (fr, _) = fetch_one(FEED, &http, &params, &cache).await.unwrap();

        assert_eq!(fr.cached_at.as_deref(), Some("2020-01-01T00:00:00Z"));
        assert!(
            fr.cache_age_seconds.is_some_and(|s| s > 60),
            "a 2020 entry must report a large age, got {:?}",
            fr.cache_age_seconds
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_one_clamps_a_future_fetched_at_to_zero_age() {
        // A clock-skewed cache entry stamped in the future must not wrap to a huge number
        // when `i64 -> u64` casts a negative elapsed duration; it must clamp to 0.
        let dir = std::env::temp_dir().join(format!("rss-skew-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let future = (Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
        let meta = CacheMeta {
            feed_url: FEED.to_string(),
            etag: None,
            last_modified: None,
            fetched_at: future.clone(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, BODY.as_bytes()).expect("seed");

        let params = FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..Default::default()
        };
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let (fr, _) = fetch_one(FEED, &http, &params, &cache).await.unwrap();

        assert_eq!(fr.cached_at.as_deref(), Some(future.as_str()));
        assert_eq!(
            fr.cache_age_seconds,
            Some(0),
            "a future fetched_at must clamp to 0, not wrap to a huge u64"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn expired_deadline_attempts_nothing_and_reports_every_feed() {
        let dir = std::env::temp_dir().join(format!("rss-deadline-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();

        // Unroutable URLs: a zero deadline must short-circuit before any request is attempted,
        // so this test never touches the network. Were the deadline ignored, every fetch would
        // fail to connect and land in `feeds[]` as an error entry — the assertions below would
        // see 4 feeds and no marker.
        let urls: Vec<String> = (0..4)
            .map(|i| format!("http://127.0.0.1:1/f{i}.xml"))
            .collect();
        let params = FetchParams {
            deadline: Some(std::time::Duration::ZERO),
            cache_policy: CachePolicy::NoCache,
            ..Default::default()
        };

        let out = fetch_feeds_with(&urls, &params, &cache, &http).await;
        assert!(
            out.feeds.is_empty(),
            "an expired deadline attempts no feeds"
        );
        assert!(
            out.errors.is_empty(),
            "an unattempted feed did not fail — it must not be reported as an error"
        );
        assert_eq!(
            out.truncation.as_ref().map(|t| t.feeds_omitted),
            Some(4),
            "unattempted feeds must be reported, not silently dropped"
        );
        assert!(
            out.truncation
                .as_ref()
                .and_then(|t| t.suggestion.as_deref())
                .is_some(),
            "the caller needs to be told how to get the missing feeds"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A successful single-item feed result, shaped as [`fetch_one`] would return it.
    fn ok_feed(url: &str) -> (FeedResult, Vec<Warning>) {
        let items = vec![item(false)];
        (
            FeedResult {
                feed_url: url.to_string(),
                status: FeedStatus::Ok,
                from_cache: false,
                title: Some("f".to_string()),
                site_url: None,
                updated: None,
                item_count: items.len(),
                content_tokens_est_total: 1,
                items,
                error: None,
                cached_at: None,
                cache_age_seconds: None,
            },
            Vec::new(),
        )
    }

    #[test]
    fn an_out_of_order_gap_discards_every_later_feed_and_its_errors() {
        // The branch the whole cursor contract rests on, exercised without a clock: the
        // deadline can leave feed 1 unattempted while feeds 2 and 3 (started earlier, finished
        // later) completed. `feeds[]` must stay a contiguous prefix — feeds[j] IS urls[j] for
        // every consumer, the continuation cursor above all — so the survivors past the gap are
        // discarded, and the count says so.
        let urls: Vec<String> = (0..4)
            .map(|i| format!("https://e.example/{i}.xml"))
            .collect();
        let failed = crate::model::ErrorObj::new("FEED_FETCH_FAILED", "boom");
        let results: Vec<IndexedFetch> = vec![
            (0, Some(ok_feed(&urls[0]))),
            // Never started: the deadline had passed by the time this future was polled.
            (1, None),
            (2, Some(ok_feed(&urls[2]))),
            // A *discarded* feed that also errored — the case that pins the rule below.
            (3, Some((FeedResult::error(&urls[3], failed), Vec::new()))),
        ];

        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            None,
            results,
        );

        assert_eq!(
            out.feeds.len(),
            1,
            "only the prefix before the gap ships: {:?}",
            out.feeds.iter().map(|f| &f.feed_url).collect::<Vec<_>>()
        );
        assert_eq!(out.feeds[0].feed_url, urls[0]);
        assert_eq!(
            out.total_items, 1,
            "totals describe the shipped prefix only"
        );
        assert_eq!(
            out.truncation.as_ref().map(|t| t.feeds_omitted),
            Some(3),
            "the unattempted feed AND the two survivors discarded with it are all owed"
        );
        assert!(
            out.errors.is_empty(),
            "a discarded survivor takes its errors[] entry with it — an error whose feed is \
             absent breaks the feeds[]/errors[] mirror: {:?}",
            out.errors
        );
    }

    #[test]
    fn results_arriving_out_of_order_are_restored_to_request_order() {
        // The sort is the other half of the prefix rule: `buffer_unordered` completes in
        // whatever order the network answers, but `feeds[]` is request-ordered (ADR-0012).
        let urls: Vec<String> = (0..3)
            .map(|i| format!("https://e.example/{i}.xml"))
            .collect();
        let results: Vec<IndexedFetch> = vec![
            (2, Some(ok_feed(&urls[2]))),
            (0, Some(ok_feed(&urls[0]))),
            (1, Some(ok_feed(&urls[1]))),
        ];

        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            None,
            results,
        );

        let ordered: Vec<&String> = out.feeds.iter().map(|f| &f.feed_url).collect();
        assert_eq!(ordered, urls.iter().collect::<Vec<_>>());
        assert!(
            out.truncation.is_none(),
            "nothing was omitted, so there is no marker to emit"
        );
    }

    #[tokio::test]
    async fn no_deadline_fetches_every_feed() {
        // Guards the default path: the CLI passes deadline: None and must be unaffected.
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(200)
            .with_body(
                "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>f</title>\
                 <link>https://example.com/</link></channel></rss>",
            )
            .expect_at_least(3)
            .create_async()
            .await;

        let dir = std::env::temp_dir().join(format!("rss-nodeadline-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();

        let urls: Vec<String> = (0..3)
            .map(|i| format!("{}/f{i}.xml", server.url()))
            .collect();
        let params = FetchParams {
            cache_policy: CachePolicy::NoCache,
            ..Default::default()
        };

        let out = fetch_feeds_with(&urls, &params, &cache, &http).await;
        assert_eq!(out.feeds.len(), 3);
        assert!(
            out.truncation.is_none(),
            "no deadline means no omission marker"
        );
        // `expect_at_least` is only enforced by an explicit assert: three feeds in `feeds[]`
        // could in principle be three cache hits, so check the requests actually went out.
        m.assert_async().await;

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
            cached_at: None,
            cache_age_seconds: None,
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

    /// Build a fixture holding one feed per entry in `counts`, each with that many items. A
    /// zero-item feed stands in for a feed that gave everything it had (e.g. an error entry).
    fn output_with_feeds(counts: &[usize]) -> FetchOutput {
        let mut out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        for (i, &n) in counts.iter().enumerate() {
            out.feeds.push(FeedResult {
                feed_url: format!("https://example.com/feed{i}.xml"),
                status: FeedStatus::Ok,
                from_cache: false,
                title: Some("Feed".to_string()),
                site_url: None,
                updated: None,
                item_count: 0,
                content_tokens_est_total: 0,
                items: (0..n).map(|_| item(false)).collect(),
                error: None,
                cached_at: None,
                cache_age_seconds: None,
            });
        }
        refresh_feed_counts(&mut out);
        out
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
    fn paginate_ships_a_page_that_fits_even_when_a_later_feed_is_dropped() {
        // Feed envelopes are charged per feed, when that feed's first item is admitted — not
        // all up front. Charging every feed's envelope into the baseline (including feeds the
        // trim then drops) inflated it by one envelope per URL, and the greedy pass rejected
        // budgets that demonstrably fit: with two feeds it stopped at (0, 0) and raised
        // ResponseTooLarge for any budget under `all-feeds baseline + first item`, discarding
        // every successful fetch. The budget here is the measured cost of the state the page
        // actually lands on — feed 0's envelope plus its first item — which is the smallest
        // budget that can possibly ship anything, and sits inside that old false-Err window.
        let mut target = output_with_feeds(&[2, 2]);
        target.feeds.truncate(1);
        target.feeds[0].items.truncate(1);
        refresh_feed_counts(&mut target);
        let budget = estimate_response_tokens(&target);

        let mut out = output_with_feeds(&[2, 2]);
        assert!(
            estimate_response_tokens(&out) > budget,
            "the fixture must be over budget for pagination to engage"
        );

        let stop = paginate(&mut out, budget)
            .expect("a page that fits must not be rejected")
            .expect("truncates");
        assert!(item_count(&out) >= 1, "at least one item must ship");
        assert!(
            estimate_response_tokens(&out) <= budget,
            "the shipped page must actually fit the budget"
        );
        assert_eq!(out.feeds.len(), 1, "feed 1 is deferred whole");
        assert_eq!(out.feeds[0].items.len(), 1);
        assert_eq!(stop.feed_idx, 0, "resume inside feed 0");
        assert_eq!(stop.item_idx, 1);
        assert_eq!(stop.feed_item_count, 2);
        assert_eq!(stop.items_omitted, 3);
        assert_eq!(stop.feeds_omitted, 1);
    }

    #[test]
    fn paginate_still_errors_below_the_first_feeds_envelope_and_item() {
        // The other side of the boundary above: charging envelopes incrementally must not
        // soften the genuine ResponseTooLarge. A budget that cannot hold feed 0's envelope
        // plus its first item still ships nothing, so it still errors — whether the greedy
        // pass sees it up front (envelope-only budget) or the shed loop discovers it one
        // token under the real landing cost.
        let mut envelope_only = output_with_feeds(&[2, 2]);
        envelope_only.feeds.truncate(1);
        envelope_only.feeds[0].items.clear();
        refresh_feed_counts(&mut envelope_only);
        let envelope_budget = estimate_response_tokens(&envelope_only);

        let mut landing = output_with_feeds(&[2, 2]);
        landing.feeds.truncate(1);
        landing.feeds[0].items.truncate(1);
        refresh_feed_counts(&mut landing);
        let landing_budget = estimate_response_tokens(&landing);

        for budget in [envelope_budget, landing_budget - 1] {
            let mut out = output_with_feeds(&[2, 2]);
            let err = paginate(&mut out, budget).unwrap_err();
            assert_eq!(
                err.code(),
                "RESPONSE_TOO_LARGE",
                "budget {budget} cannot ship a single item, got {err:?}"
            );
        }
    }

    #[test]
    fn paginate_sheds_whole_trailing_feeds() {
        // Two feeds of one item each; the budget only admits the first feed's item. It is the
        // measured cost of exactly the state the page lands on — feed 0's envelope and its
        // item, no trace of feed 1 — so the scenario is pinned by construction rather than by
        // a fraction of the full payload that a wider `Item` could silently move out of the
        // window. Measuring the landing state was impossible while every feed's envelope was
        // charged up front: that baseline rejected this budget outright, so the test had to
        // keep a feed envelope of slack by clearing feed 1's items instead of dropping it.
        let mut out = output_with(vec![item(false)]);
        with_second_feed(&mut out);

        let mut one_feed_shipped = out.clone();
        one_feed_shipped.feeds.truncate(1);
        refresh_feed_counts(&mut one_feed_shipped);
        let one_feed_budget = estimate_response_tokens(&one_feed_shipped);

        let stop = paginate(&mut out, one_feed_budget)
            .expect("not an error")
            .expect("truncates");
        assert_eq!(out.feeds.len(), 1, "the trailing feed is dropped whole");
        assert_eq!(
            estimate_response_tokens(&out),
            one_feed_budget,
            "the page is exactly the state the budget was measured from"
        );
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
        // Two feeds of two items. The budget is one token under the real cost of [feed 0
        // whole, one item of feed 1]. The greedy pass under-charges each item, so it believes
        // all four fit and reports no stop at all; the `None` fallback then sheds feed 1's
        // last item and the trailing-shed loop empties feed 1 entirely after the fact. The
        // resume position must point *into* feed 1, not past it — otherwise its items are
        // silently lost.
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

    // A leading zero-item feed makes the resume scan skip past it and land on feed 1 — where
    // a fresh request already starts. A position-based progress guard waves that through as
    // Ok(Some) even though the page shipped zero items, which loops a cursor caller forever.
    // Progress is items, not position. The two budgets below are separate tests on purpose:
    // they trip different amounts of the trim, and one must not mask the other.

    #[test]
    fn paginate_errors_when_a_leading_empty_feed_ships_nothing_at_all() {
        // A budget so small even the leading feed's envelope is shed: no feeds, no items.
        let mut out = output_with_feeds(&[0, 1]);
        let err = paginate(&mut out, 1).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "a page that ships nothing must be an error, got {err:?}"
        );
    }

    #[test]
    fn paginate_errors_when_a_leading_empty_feed_ships_only_its_envelope() {
        // A feed envelope ships but zero items do. The budget is the measured cost of the
        // state the trim lands on: the leading empty feed alone.
        let mut envelope_only = output_with_feeds(&[0, 1]);
        envelope_only.feeds.truncate(1);
        refresh_feed_counts(&mut envelope_only);
        let budget = estimate_response_tokens(&envelope_only);

        let mut out = output_with_feeds(&[0, 1]);
        assert!(
            estimate_response_tokens(&out) > budget,
            "the fixture must be over budget for pagination to engage"
        );
        let err = paginate(&mut out, budget).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "shipping a feed envelope with no items is not progress, got {err:?}"
        );
    }

    #[test]
    fn paginate_resumes_at_a_trailing_empty_feed_when_every_item_shipped() {
        // [feed of 4 items, feed with none]: every item fits, but the trailing feed's
        // envelope does not, so the trim drops that feed. A dropped zero-item feed is a
        // legitimate resume target — reporting the page as too large would throw away four
        // items that shipped fine. The budget is the measured cost of the target state.
        let mut target = output_with_feeds(&[4, 0]);
        target.feeds.truncate(1);
        refresh_feed_counts(&mut target);
        let budget = estimate_response_tokens(&target);

        let mut out = output_with_feeds(&[4, 0]);
        assert!(
            estimate_response_tokens(&out) > budget,
            "the trailing envelope must be what overflows"
        );

        let stop = paginate(&mut out, budget)
            .expect("not an error")
            .expect("truncates");
        assert_eq!(out.feeds.len(), 1, "the trailing feed is dropped");
        assert_eq!(out.feeds[0].items.len(), 4, "every item of feed 0 ships");
        assert_eq!(stop.feed_idx, 1, "resume at the dropped feed");
        assert_eq!(stop.item_idx, 0);
        assert_eq!(
            stop.feed_item_count, 0,
            "that feed held no items when the page was minted"
        );
        assert_eq!(
            stop.items_omitted, 0,
            "only an envelope was deferred, no items"
        );
        assert_eq!(stop.feeds_omitted, 1);
        assert!(
            estimate_response_tokens(&out) <= budget,
            "the trimmed page must actually fit"
        );
    }

    #[test]
    fn paginate_errors_when_only_empty_feed_envelopes_are_over_budget() {
        // Feeds that never held items (error entries) with an over-budget envelope: there is
        // nothing to shed, so no page boundary exists anywhere. Caught by the pre-trim
        // `original_items == 0` guard; the `resume == None` arm says the same thing.
        let mut out = output_with_feeds(&[0, 0]);
        let err = paginate(&mut out, 1).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "an envelope with no items to shed is not paginable, got {err:?}"
        );
    }

    #[test]
    fn paginate_keeps_a_trailing_feed_that_never_had_items() {
        // [2 items, none, 2 items]. One token under the real cost of [2, 0, 1]. The greedy
        // pass believes everything fits — it under-charges items, and never charges the
        // middle feed's envelope at all since no item admits it — so the `None` fallback
        // sheds feed 2's last item and the shed loop empties the rest of it. The husk pass
        // pops that husk but must KEEP the middle feed: it shipped everything it had
        // (nothing), so it is data, not a husk.
        let mut over_shipped = output_with_feeds(&[2, 0, 2]);
        over_shipped.feeds[2].items.truncate(1);
        refresh_feed_counts(&mut over_shipped);
        let budget = estimate_response_tokens(&over_shipped) - 1;

        let mut out = output_with_feeds(&[2, 0, 2]);
        let stop = paginate(&mut out, budget)
            .expect("not an error")
            .expect("truncates");
        assert_eq!(
            out.feeds.len(),
            2,
            "feed 2 is popped as a husk, feed 1 is not"
        );
        assert_eq!(
            out.feeds[1].feed_url, "https://example.com/feed1.xml",
            "the feed that never had items is retained"
        );
        assert!(out.feeds[1].items.is_empty());
        assert_eq!(out.feeds[0].items.len(), 2, "feed 0 ships whole");
        assert_eq!(stop.feed_idx, 2, "resume in the emptied feed, not past it");
        assert_eq!(stop.item_idx, 0);
        assert_eq!(stop.feed_item_count, 2, "the count describes feed 2");
        assert_eq!(stop.items_omitted, 2);
        assert_eq!(stop.feeds_omitted, 1);
        assert!(
            estimate_response_tokens(&out) <= budget,
            "the page must fit"
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
