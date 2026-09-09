//! Orchestration shared by the CLI and the MCP server.
//!
//! This wiring is intentionally implemented up front against the *frozen signatures* of
//! [`crate::fetch`] and [`crate::parse`]. It compiles before those modules are filled in
//! and runs unchanged once they are, so there is no integration step for the seam itself.

use chrono::Utc;
use futures::stream::{self, StreamExt};

use crate::cache::Cache;
use crate::config::{DedupeMode, FetchParams};
use crate::error::RssError;
use crate::fetch::HttpClient;
use crate::model::{
    AppliedFilters, DiscoverOutput, DuplicateGroup, DuplicateKeyKind, FeedResult, FeedStatus,
    FetchOutput, Item, TruncationInfo, Warning,
};
use crate::{discover, parse};

/// The absolute instant by which a fetch must have *started*, from [`FetchParams::deadline`].
/// `None` (the CLI default) is unbounded. Every `core` entry point that can reach the per-host
/// gate derives its bound here, so none is left waiting on a permit with no ceiling.
fn stop_at_for(params: &FetchParams) -> Option<tokio::time::Instant> {
    params.deadline.map(|d| tokio::time::Instant::now() + d)
}

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

/// One feed's outcome plus the warnings its parse raised and how many items `since`/`query`
/// filtered out of it, tagged with the feed's index into the request URL list so request
/// order survives the completion-ordered stream. `None` marks a feed that was never
/// attempted because the batch deadline had passed.
type IndexedFetch = (usize, Option<(FeedResult, Vec<Warning>, usize)>);

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
    let stop_at = stop_at_for(params);

    // Tag each task with its input index so we can restore request order after the
    // completion-ordered `buffer_unordered` stream — `feeds[]`/`errors[]` are then
    // deterministic within a run (an agent can address feeds by position). See ADR-0012.
    let results: Vec<IndexedFetch> = stream::iter(urls.iter().cloned().enumerate())
        .map(|(idx, url)| async move {
            // A cheap short-circuit so an already-expired batch attempts nothing at all —
            // *not* the real bound. `buffer_unordered` polls the first `concurrency` futures at
            // t≈0, so they all pass this check and then queue on the per-host permit, where
            // `HostGate::acquire_until` enforces `stop_at` for real (ADR-0016). Pinned by
            // `deadline_is_a_real_wall_clock_bound_for_a_throttled_same_host_batch`.
            if let Some(t) = stop_at
                && tokio::time::Instant::now() >= t
            {
                return (idx, None);
            }
            match fetch_one_until(&url, http, params, cache, stop_at).await {
                Ok((fr, warnings, filtered)) => (idx, Some((fr, warnings, filtered))),
                // Never reached the origin: the gate refused to start it before the deadline.
                // Report it exactly as the pre-flight check above does — unattempted, so it
                // lands in `feeds_omitted` and stays resumable, not a fabricated failure in
                // `errors[]`.
                Err(RssError::DeadlineExceeded { .. }) => (idx, None),
                Err(e) => (
                    idx,
                    Some((
                        FeedResult::error(url.clone(), e.to_error_obj(Some(&url))),
                        Vec::new(),
                        0,
                    )),
                ),
            }
        })
        .buffer_unordered(params.concurrency.max(1))
        .collect()
        .await;

    assemble_prefix_in_request_order(output, params, results)
}

/// Fold the completion-ordered fetch results back into request order, keeping `feeds[]` a
/// contiguous **prefix** of the requested URLs and reporting whatever the deadline left
/// unattempted as `truncation.feeds_omitted`.
///
/// Pure by design — no clock, no network, no I/O — so the out-of-order case only a deadline
/// produces (feeds 0, 2, 3 done, 1 never started) is a deterministic unit test, not a timing
/// race. `output` arrives already stamped, so `fetched_at` marks the batch *start*.
///
/// `results` must hold one entry per requested URL — `buffer_unordered` yields every future's
/// output, only out of order — so its length is the request count.
fn assemble_prefix_in_request_order(
    mut output: FetchOutput,
    params: &FetchParams,
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

    // Combined `since`+`query` removals across every feed that made it into this prefix —
    // the input to `applied_filters_marker` below. Not accumulated for feeds past the gap:
    // they contributed nothing to `output.feeds`, so their filter counts describe items the
    // caller never sees this call and would misrepresent what *this* batch filtered.
    let mut total_filtered = 0usize;
    for (_, slot) in results.into_iter().take(first_unattempted) {
        // Unreachable by construction — every slot before the first `None` is `Some` — but
        // silently keeping the prefix contiguous beats an unwrap on an invariant.
        let Some((fr, warnings, filtered)) = slot else {
            continue;
        };
        total_filtered += filtered;
        if let Some(err) = &fr.error {
            output.errors.push(err.clone());
        }
        output.warnings.extend(warnings);
        output.feeds.push(fr);
    }
    populate_totals(&mut output);
    output.applied_filters = applied_filters_marker(params, total_filtered);

    let omitted = requested - first_unattempted;
    if omitted > 0 {
        output.truncation = Some(TruncationInfo {
            applied_limit: params.limit,
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

/// Build the [`AppliedFilters`] marker for a batch, or `None` when neither `since` nor a
/// query that actually constrains anything was supplied.
///
/// `items_filtered_out` combines what `since` and `query` removed across the shipped prefix;
/// [`AppliedFilters::items_filtered_out`] says why they aren't split out.
///
/// The gate is [`crate::query::Query::is_empty`] on the *parsed* query — the same predicate
/// `parse_feed` uses to decide whether to filter. Anything looser (a `trim().is_empty()` on the
/// raw string) disagrees for input that is non-blank but parses to no terms (`"-"`, `""""`),
/// emitting a marker for a query that never ran. `since` alone still gates it on.
fn applied_filters_marker(
    params: &FetchParams,
    items_filtered_out: usize,
) -> Option<AppliedFilters> {
    let query_constrains = params
        .query
        .as_deref()
        .is_some_and(|q| !crate::query::Query::parse(q).is_empty());
    if params.since.is_none() && !query_constrains {
        return None;
    }
    Some(AppliedFilters {
        since: params
            .since
            .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        query: params.query.clone(),
        items_filtered_out,
    })
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

/// Group items in `output` that resolve to the same underlying entry, keyed on `guid`, then
/// resolved `url`, then `content_hash` (ADR-0018). Grouping runs over every item in the
/// batch, not only across feed boundaries — a single feed that repeats an entry produces a
/// group too, same as two feeds syndicating the same article. See [`DuplicateGroup`] for why
/// `item.id` cannot serve as the key here.
///
/// Report-only: never mutates `output`. [`drop_duplicates`] is the opt-in removal path.
///
/// Ordering is a contract (invariant 9): groups come back in first-appearance order over
/// `feeds[]` then `items[]`, and each group's `item_ids`/`feed_urls` are in that same
/// encounter order — `HashMap` iteration order must never leak into the result.
pub fn find_duplicates(output: &FetchOutput) -> Vec<DuplicateGroup> {
    use std::collections::HashMap;

    // Preserve request order: record each key's first-appearance position, and accumulate
    // one (item_id, feed_url) pair per occurrence so the two output vectors can never
    // desync -- they are unzipped from a single vector at the end, not pushed to in
    // parallel.
    let mut order: Vec<(String, DuplicateKeyKind)> = Vec::new();
    let mut occurrences: HashMap<(String, DuplicateKeyKind), Vec<(String, String)>> =
        HashMap::new();

    for feed in &output.feeds {
        for item in &feed.items {
            let Some((key, kind)) = dedup_key(item) else {
                continue;
            };
            let slot = occurrences.entry((key.clone(), kind)).or_insert_with(|| {
                order.push((key, kind));
                Vec::new()
            });
            slot.push((item.id.clone(), feed.feed_url.clone()));
        }
    }

    order
        .into_iter()
        .filter_map(|k| {
            let occ = occurrences.remove(&k)?;
            // A single occurrence is not a duplicate.
            if occ.len() <= 1 {
                return None;
            }
            let (item_ids, feed_urls) = occ.into_iter().unzip();
            Some(DuplicateGroup {
                key: k.0,
                key_kind: k.1,
                item_ids,
                feed_urls,
            })
        })
        .collect()
}

/// The dedup key for an item: `guid`, else resolved `url`, else `content_hash` — the first
/// present, non-empty field in that order. `None` when all three are absent/empty; such an
/// item is skipped by [`find_duplicates`], never grouped with other keyless items.
fn dedup_key(item: &Item) -> Option<(String, DuplicateKeyKind)> {
    if let Some(g) = item.guid.as_deref().filter(|s| !s.is_empty()) {
        return Some((g.to_string(), DuplicateKeyKind::Guid));
    }
    if let Some(u) = item.url.as_deref().filter(|s| !s.is_empty()) {
        return Some((u.to_string(), DuplicateKeyKind::Url));
    }
    item.content_hash
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|h| (h.to_string(), DuplicateKeyKind::ContentHash))
}

/// Collapse each group to its first occurrence and refresh every derived count. Opt-in only
/// (`dedupe: "drop"`); [`find_duplicates`] never mutates.
///
/// **`groups` selects which keys to collapse, not which ids to delete.** Each item's key is
/// re-derived with [`dedup_key`] and the first occurrence of a targeted key survives, because
/// `item.id` is not a usable removal handle: `identity.rs` keys on link → guid →
/// title|published while `dedup_key` prefers guid → url → content_hash, so a feed whose
/// entries share one `<link>` gives every item the same `id` across different groups. Deleting
/// "the ids after the first" would then delete an item no group named. For anything
/// `find_duplicates` produced the survivor is still its canonical `item_ids[0]`.
///
/// A stale group is harmless: an unmatched key collapses nothing, a single occurrence stays.
pub fn drop_duplicates(output: &mut FetchOutput, groups: &[DuplicateGroup]) {
    use std::collections::HashSet;

    let targets: HashSet<(&str, DuplicateKeyKind)> = groups
        .iter()
        .map(|g| (g.key.as_str(), g.key_kind))
        .collect();
    if targets.is_empty() {
        return;
    }

    // Every removal decision is made in `feeds[]`/`items[]` traversal order; the sets are
    // only ever looked up, so no hash iteration order can reach the output (invariant 9).
    let mut seen: HashSet<(String, DuplicateKeyKind)> = HashSet::new();
    for feed in &mut output.feeds {
        feed.items.retain(|item| {
            let Some((key, kind)) = dedup_key(item) else {
                return true; // keyless: never grouped, never dropped
            };
            if !targets.contains(&(key.as_str(), kind)) {
                return true; // this key was not one of the groups
            }
            seen.insert((key, kind)) // first occurrence survives, the rest go
        });
    }
    refresh_feed_counts(output);
}

/// Apply a [`DedupeMode`] to an assembled `output` — the single place the report/off/drop
/// behavior lives, so `rss fetch --dedupe` and the MCP `dedupe` argument cannot diverge
/// (invariant 6). Front-ends parse their argument with [`crate::config::parse_dedupe`] and
/// call this; they do not re-implement the match.
///
/// Under [`DedupeMode::Drop`] the groups stay on the output after the removal — they are the
/// caller's only record of what was collapsed, which is also why they are computed once rather
/// than recomputed afterwards over the survivors, where a second pass would find nothing.
///
/// Callers that bound a response must run this **first**: `duplicates[]` is a top-level field,
/// so an estimate taken before it under-counts, and `Drop` changes the item count.
pub fn apply_dedupe(output: &mut FetchOutput, mode: DedupeMode) {
    match mode {
        // Clear rather than no-op: the postcondition is that `duplicates` reflects *this*
        // mode. A bare no-op would leave a previous call's groups in place on an output that
        // was asked not to report any — no current caller does that, but nothing about the
        // signature stops one.
        DedupeMode::Off => output.duplicates.clear(),
        DedupeMode::Report => {
            let groups = find_duplicates(output);
            output.duplicates = groups;
        }
        DedupeMode::Drop => {
            let groups = find_duplicates(output);
            drop_duplicates(output, &groups);
            output.duplicates = groups;
        }
    }
}

/// Fetch and parse a single feed, returning the [`FeedResult`], any non-fatal [`Warning`]s
/// the parse surfaced (e.g. a content-extraction fallback), and how many items `since`/
/// `query` filtered out of it. Callers aggregate the warnings into [`FetchOutput::warnings`]
/// and the filtered count into [`FetchOutput::applied_filters`].
pub async fn fetch_one(
    url: &str,
    http: &HttpClient,
    params: &FetchParams,
    cache: &Cache,
) -> Result<(FeedResult, Vec<Warning>, usize), RssError> {
    // Honors `params.deadline` rather than passing `None`: a caller that set a deadline and
    // then waited on the gate with no ceiling was the whole bug this file's `stop_at_for`
    // exists to prevent.
    fetch_one_until(url, http, params, cache, stop_at_for(params)).await
}

/// [`fetch_one`] against an **explicit** absolute instant. A feed that cannot start before
/// `stop_at` returns [`RssError::DeadlineExceeded`], which [`fetch_feeds`] turns into an
/// *unattempted* feed rather than a failed one. `None` is unbounded.
///
/// A batch needs this rather than [`fetch_one`]: every feed must share the *one* instant
/// computed when the call began, or a per-feed `now + deadline` would restart the clock on
/// each feed and the batch could never expire.
pub async fn fetch_one_until(
    url: &str,
    http: &HttpClient,
    params: &FetchParams,
    cache: &Cache,
    stop_at: Option<tokio::time::Instant>,
) -> Result<(FeedResult, Vec<Warning>, usize), RssError> {
    let raw = http
        .fetch_until(url, cache, params.cache_policy, stop_at)
        .await?;
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
        // Order matters: a stale body is served with `not_modified: false` (the origin never
        // confirmed it), so `Stale` must be tested before the `304` case, not after.
        status: if raw.stale_reason.is_some() {
            FeedStatus::Stale
        } else if raw.not_modified {
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
    let mut warnings = parsed.warnings;
    // `error` stays `null` on a stale feed — the items are real, and the obvious consumer
    // idiom `if (feed.error) skip(feed)` would otherwise discard a usable feed. The refusal
    // rides here instead, and `cache_age_seconds` (already set above) carries the age, so no
    // new `FeedResult` field is needed. ADR-0019 §3.
    if let Some(reason) = &raw.stale_reason {
        let age = fr
            .cache_age_seconds
            .map_or_else(|| "unknown".to_string(), |s| format!("{s}s"));
        warnings.push(Warning {
            feed_url: Some(url.to_string()),
            code: "SERVED_STALE".to_string(),
            message: format!(
                "origin refused revalidation ({reason}); served the cached copy, {age} old"
            ),
        });
    }
    Ok((fr, warnings, parsed.items_filtered_out))
}

/// Discover feeds advertised on a website homepage, through a **caller-provided** client.
/// The MCP server passes its shared client so discovery shares the per-host gate (ADR-0016).
///
/// No filtering params apply here, but the call does traverse that gate, so it reads
/// [`FetchParams::deadline`]. Without that bound a busy host parks the call on the permit
/// queue indefinitely — `MAX_GATE_WAIT` caps a single cooldown, not the siblings ahead of you
/// — and the caller times out having received nothing.
pub async fn discover_feeds_with(
    site_url: &str,
    params: &FetchParams,
    http: &HttpClient,
) -> Result<DiscoverOutput, RssError> {
    discover::discover_until(site_url, http, stop_at_for(params)).await
}

/// Discover feeds advertised on a website homepage. Thin wrapper that builds a client for
/// the CLI, mirroring how [`fetch_feeds`] wraps [`fetch_feeds_with`].
pub async fn discover_feeds(
    site_url: &str,
    params: &FetchParams,
) -> Result<DiscoverOutput, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    discover_feeds_with(site_url, params, &http).await
}

/// Item lookup through a **caller-provided** client. See [`show_item`] for the semantics.
pub async fn show_item_with(
    feed_url: &str,
    key: &str,
    params: &FetchParams,
    cache: &Cache,
    http: &HttpClient,
) -> Result<Option<crate::model::Item>, RssError> {
    // Same bound as discovery: `CacheFirst` means a cache hit never reaches the gate, but a
    // miss does, and an unbounded wait there outlives any client's tool timeout.
    let (fr, _warnings, _filtered) =
        fetch_one_until(feed_url, http, params, cache, stop_at_for(params)).await?;
    Ok(fr.items.into_iter().find(|it| {
        it.id == key || it.guid.as_deref() == Some(key) || it.url.as_deref() == Some(key)
    }))
}

/// Fetch a feed (cache-first) and return the single item whose `id`, raw `guid`, or resolved
/// `url` equals `key`, if present. Thin wrapper that builds a client for the CLI.
///
/// `id` is namespaced by `feed_url` (ADR-0003); a `guid` (e.g. Reddit `t3_…`) is
/// feed-window-independent and is the reliable key across different feed URLs. The lookup is
/// cache-first (ADR-0014): an item the caller already saw survives a rolled feed window, but
/// not a later cache-overwriting refetch.
pub async fn show_item(
    feed_url: &str,
    key: &str,
    params: &FetchParams,
    cache: &Cache,
) -> Result<Option<crate::model::Item>, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    show_item_with(feed_url, key, params, cache, &http).await
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
/// **This must never over-estimate.** Nesting really costs far more than the `+ 2` (~12 tokens
/// per feed envelope, ~38 per item), and that gap is load-bearing: it keeps the greedy total a
/// lower bound on the real payload, so a greedy rejection implies a real one. Raising it to
/// "improve accuracy" rejects pages that would have fit — the false `RESPONSE_TOO_LARGE` this
/// accounting exists to prevent.
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
/// The **fill** counterpart to [`enforce_response_budget`]'s **reject**: with a batch,
/// rejecting would discard every successful fetch over one trailing item (ADR-0017).
///
/// `Ok(None)` = everything fit, `Ok(Some(stop))` = trimmed. `Err(ResponseTooLarge)` means
/// exactly that **zero items shipped** — the invariant a cursor loop needs, since a stop that
/// shipped nothing would mint a cursor no further along and loop forever.
///
/// On `Err`, `output` may already be partially trimmed; discard it. Unlike
/// [`enforce_response_budget`], which never mutates.
///
/// `budget_tokens` must already exclude headroom for what the caller attaches afterwards (the
/// `next_cursor` and [`TruncationInfo`]) — the page is measured as it stands.
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
    use crate::model::{ContentFormat, DuplicateGroup, DuplicateKeyKind, IdSource, Item};

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
        let (fr, _, _) = fetch_one(FEED, &http, &params, &cache).await.unwrap();

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
        let (fr, _, _) = fetch_one(FEED, &http, &params, &cache).await.unwrap();

        assert_eq!(fr.cached_at.as_deref(), Some(future.as_str()));
        assert_eq!(
            fr.cache_age_seconds,
            Some(0),
            "a future fetched_at must clamp to 0, not wrap to a huge u64"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_expired_deadline_attempts_nothing_on_any_single_url_path() {
        // `RSS_MCP_BATCH_DEADLINE_SECS=0` is a documented load-shedding value, but only the
        // batch path had a pre-flight check; the single-URL paths relied on the gate, and tokio
        // hands out an immediately-available permit *before* consulting the timer, so an
        // already-expired `stop_at` still sent. `.expect(0)` is the real assertion.
        //
        // All three entry points are exercised: covering only `discover_feeds_with` left the
        // test green when `show_item_with` or `fetch_one` stopped forwarding the deadline.
        let mut server = mockito::Server::new_async().await;
        let never = server
            .mock("GET", mockito::Matcher::Any)
            .expect(0)
            .create_async()
            .await;
        let dir = std::env::temp_dir().join(format!("rss-nodl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let expired = FetchParams {
            deadline: Some(std::time::Duration::ZERO),
            ..Default::default()
        };
        let url = format!("{}/feed.xml", server.url());

        let shed = |what: &str, e: RssError| {
            assert!(
                matches!(e, RssError::DeadlineExceeded { .. }),
                "{what}: an expired deadline must shed before the request is sent, got {e:?}"
            );
        };

        shed(
            "discover_feeds_with",
            discover_feeds_with(&server.url(), &expired, &http)
                .await
                .unwrap_err(),
        );

        // `CacheFirst` mirrors the MCP `get_item` tool. The cache dir is fresh, so this is the
        // *miss* path — the only one that reaches the gate at all (ADR-0014).
        let item_params = FetchParams {
            cache_policy: CachePolicy::CacheFirst,
            ..expired.clone()
        };
        shed(
            "show_item_with",
            show_item_with(&url, "any-id", &item_params, &cache, &http)
                .await
                .unwrap_err(),
        );

        // The public single-feed wrapper: forwarding this back to `None` would silently restore
        // the unbounded permit wait for any caller that set a deadline.
        shed(
            "fetch_one",
            fetch_one(&url, &http, &expired, &cache).await.unwrap_err(),
        );

        never.assert_async().await;
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

    /// A successful single-item feed result, shaped as [`fetch_one`] would return it. `0`
    /// items filtered out, as if `since`/`query` were never supplied.
    fn ok_feed(url: &str) -> (FeedResult, Vec<Warning>, usize) {
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
            0,
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
            (
                3,
                Some((FeedResult::error(&urls[3], failed), Vec::new(), 0)),
            ),
        ];

        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            &FetchParams::default(),
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
            &FetchParams::default(),
            results,
        );

        let ordered: Vec<&String> = out.feeds.iter().map(|f| &f.feed_url).collect();
        assert_eq!(ordered, urls.iter().collect::<Vec<_>>());
        assert!(
            out.truncation.is_none(),
            "nothing was omitted, so there is no marker to emit"
        );
    }

    #[test]
    fn applied_filters_is_none_when_neither_since_nor_query_supplied() {
        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            &FetchParams::default(),
            vec![(0, Some(ok_feed("https://e.example/0.xml")))],
        );
        assert!(
            out.applied_filters.is_none(),
            "neither filter was supplied, so the marker must stay null: {:?}",
            out.applied_filters
        );
    }

    #[test]
    fn applied_filters_gate_agrees_with_the_filter_on_queries_that_constrain_nothing() {
        // The marker must be gated by the same predicate `parse_feed` uses to decide
        // whether to filter — `Query::is_empty` on the *parsed* query. A raw
        // `trim().is_empty()` test would pass `"-"` and `"\"\""` through and emit a marker
        // announcing a query that removed nothing because it parsed to no terms at all.
        for q in ["   ", "-", "\"\""] {
            let params = FetchParams {
                query: Some(q.to_string()),
                ..Default::default()
            };
            let out = assemble_prefix_in_request_order(
                FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
                &params,
                vec![(0, Some(ok_feed("https://e.example/0.xml")))],
            );
            assert!(
                out.applied_filters.is_none(),
                "{q:?} parses to no terms, so no filter ran and the marker must stay null: {:?}",
                out.applied_filters
            );
        }
    }

    #[test]
    fn applied_filters_reports_since_removals_even_without_a_query() {
        // Defect check: `since` alone (no `query`) must still surface a non-zero
        // `items_filtered_out` — the field is not query-only. A caller that saw `since`
        // remove real items must not be told `0`, which would read as "the feed had
        // nothing new" rather than "since filtered it out".
        let params = FetchParams {
            since: Some(Utc::now()),
            ..Default::default()
        };
        let mut fr = ok_feed("https://e.example/0.xml");
        fr.2 = 3; // `since` dropped 3 items while parsing this feed.
        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            &params,
            vec![(0, Some(fr))],
        );
        let applied = out
            .applied_filters
            .expect("since was supplied, so the marker must be present");
        assert!(applied.since.is_some());
        assert!(applied.query.is_none());
        assert_eq!(
            applied.items_filtered_out, 3,
            "since's removals must be counted, not left at 0"
        );
    }

    #[test]
    fn applied_filters_reports_the_query_as_supplied() {
        let params = FetchParams {
            query: Some("rust".to_string()),
            ..Default::default()
        };
        let mut fr = ok_feed("https://e.example/0.xml");
        fr.2 = 2;
        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            &params,
            vec![(0, Some(fr))],
        );
        let applied = out.applied_filters.expect("query was supplied");
        assert!(applied.since.is_none());
        assert_eq!(applied.query.as_deref(), Some("rust"));
        assert_eq!(applied.items_filtered_out, 2);
    }

    #[test]
    fn applied_filters_sums_the_filtered_count_across_every_shipped_feed() {
        let params = FetchParams {
            query: Some("rust".to_string()),
            ..Default::default()
        };
        let mut a = ok_feed("https://e.example/a.xml");
        a.2 = 1;
        let mut b = ok_feed("https://e.example/b.xml");
        b.2 = 4;
        let out = assemble_prefix_in_request_order(
            FetchOutput::new("2026-06-01T00:00:00Z".to_string()),
            &params,
            vec![(0, Some(a)), (1, Some(b))],
        );
        assert_eq!(
            out.applied_filters
                .expect("query supplied")
                .items_filtered_out,
            5
        );
    }

    #[tokio::test]
    async fn fetch_feeds_with_reports_since_removals_end_to_end() {
        // The same defect check as above, exercised through the real network+parse path
        // rather than the pure helper, so the whole pipeline (fetch_one -> parse_feed ->
        // assemble_prefix_in_request_order) is pinned, not just one seam of it.
        let mut server = mockito::Server::new_async().await;
        let now = Utc::now();
        let body = format!(
            "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>f</title>\
             <item><title>Fresh</title><link>https://example.com/fresh</link>\
             <pubDate>{}</pubDate></item>\
             <item><title>Stale</title><link>https://example.com/stale</link>\
             <pubDate>{}</pubDate></item>\
             </channel></rss>",
            now.to_rfc2822(),
            (now - chrono::Duration::days(400)).to_rfc2822(),
        );
        let _m = server
            .mock("GET", "/f.xml")
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let dir = std::env::temp_dir().join(format!("rss-appliedfilters-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let params = FetchParams {
            since: Some(now - chrono::Duration::hours(1)),
            cache_policy: CachePolicy::NoCache,
            ..Default::default()
        };

        let urls = vec![format!("{}/f.xml", server.url())];
        let out = fetch_feeds_with(&urls, &params, &cache, &http).await;

        assert_eq!(
            out.feeds[0].items.len(),
            1,
            "only the fresh item survives since"
        );
        let applied = out
            .applied_filters
            .expect("since was supplied, so applied_filters must be present");
        assert_eq!(applied.query, None);
        assert_eq!(
            applied.items_filtered_out, 1,
            "the stale item since dropped must be counted end-to-end, not left at 0"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn deadline_is_a_real_wall_clock_bound_for_a_throttled_same_host_batch() {
        // The failure this pins: a batch of same-host feeds that all pass the *start* check at
        // t≈0, then serialize behind the cooldown the first `429` sets. Bounding only the
        // start time let the call run far past the deadline and blow the caller's tool
        // timeout, which returns **nothing** — no partial page, no per-feed statuses.
        //
        // `expired_deadline_attempts_nothing_and_reports_every_feed` cannot catch this: its
        // `Duration::ZERO` short-circuits at the pre-flight check and never reaches the gate.
        // The assertion that matters here is elapsed wall-clock, not just that omission is
        // reported.
        let mut server = mockito::Server::new_async().await;
        let m = server
            .mock("GET", mockito::Matcher::Any)
            .with_status(429) // no Retry-After: escalating default cooldown (2s, 4s, 8s…)
            .expect_at_least(1)
            .create_async()
            .await;

        let dir = std::env::temp_dir().join(format!("rss-hard-deadline-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();

        // One authority, so `per_host = 1` serializes all four behind a single permit.
        let urls: Vec<String> = (0..4)
            .map(|i| format!("{}/f{i}.xml", server.url()))
            .collect();
        let deadline = std::time::Duration::from_secs(3);
        let params = FetchParams {
            deadline: Some(deadline),
            cache_policy: CachePolicy::NoCache,
            ..Default::default()
        };

        let t0 = std::time::Instant::now();
        let out = fetch_feeds_with(&urls, &params, &cache, &http).await;
        let elapsed = t0.elapsed();

        assert!(
            elapsed < deadline,
            "the deadline must actually bound the call; took {elapsed:?} against a {deadline:?} \
             budget (unbounded, the siblings sleep out 4s + 8s + … of cooldown serially)"
        );
        let omitted = out
            .truncation
            .as_ref()
            .map(|t| t.feeds_omitted)
            .unwrap_or(0);
        assert!(
            omitted >= 3,
            "the feeds that never started must be reported as omitted, got {omitted}"
        );
        // No gap: whatever shipped is a contiguous prefix, and everything is accounted for
        // (invariant 9) — the cursor indexes into this same list.
        assert_eq!(
            out.feeds.len() + omitted,
            urls.len(),
            "every requested feed must be either shipped or reported omitted"
        );

        drop(m);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn stale_if_error_reports_stale_without_an_error_and_scores_as_success() {
        // ADR-0019 end-to-end through `core`: the shape a consumer actually sees.
        let mut server = mockito::Server::new_async().await;
        let dir = std::env::temp_dir().join(format!("rss-stale-core-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let url = format!("{}/feed.xml", server.url());

        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, BODY.as_bytes()).expect("seed");

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2) // original + the single bounded retry
            .create_async()
            .await;

        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let params = FetchParams {
            cache_policy: CachePolicy::StaleIfError,
            ..Default::default()
        };
        let out = fetch_feeds_with(std::slice::from_ref(&url), &params, &cache, &http).await;

        let feed = &out.feeds[0];
        assert_eq!(feed.status, FeedStatus::Stale);
        assert!(feed.from_cache);
        assert!(
            !feed.items.is_empty(),
            "a stale feed carries real items — that is the entire point"
        );
        // The footgun this shape exists to avoid: `if (feed.error) skip(feed)` must not
        // discard a usable feed (ADR-0019 §3).
        assert!(
            feed.error.is_none(),
            "error must stay null on a stale feed: {:?}",
            feed.error
        );
        assert!(
            feed.cache_age_seconds.is_some(),
            "the caller needs the age to enforce its own freshness floor"
        );
        let stale_warning = out
            .warnings
            .iter()
            .find(|w| w.code == "SERVED_STALE")
            .expect("the refused revalidation must be reported somewhere");
        assert_eq!(stale_warning.feed_url.as_deref(), Some(url.as_str()));
        assert!(
            stale_warning.message.contains("429"),
            "the warning must name the upstream refusal: {}",
            stale_warning.message
        );
        // Not an error => success. A stale-only batch exits 0, reachable only by opting in.
        assert_eq!(exit_code_for(&out), crate::error::exit::OK);
        assert!(out.errors.is_empty());

        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn the_default_policy_still_fails_a_throttled_feed() {
        // The regression guard for invariant 5. Had stale-serving been the default, this batch
        // would have flipped from ALL_FAILED to OK and silently broken every script that reads
        // `rss fetch`'s exit code to detect "could not get fresh data".
        let mut server = mockito::Server::new_async().await;
        let dir = std::env::temp_dir().join(format!("rss-nostale-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let url = format!("{}/feed.xml", server.url());

        // A cached copy IS available — the default policy must decline to use it anyway.
        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, BODY.as_bytes()).expect("seed");

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let http = crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
        let params = FetchParams::default(); // Revalidate
        let out = fetch_feeds_with(std::slice::from_ref(&url), &params, &cache, &http).await;

        assert_eq!(out.feeds[0].status, FeedStatus::Error);
        assert!(out.feeds[0].error.is_some());
        assert_eq!(exit_code_for(&out), crate::error::exit::ALL_FAILED);
        assert!(
            !out.warnings.iter().any(|w| w.code == "SERVED_STALE"),
            "nothing was served stale under the default policy"
        );

        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// What a `since`-scoped fetch really ships when the cached copy is older than the window
    /// — the reported digest run (8-day-old entry, `since: "24h"`, `stale-if-error`).
    ///
    /// Absent postdating a *dated* survivor is impossible (`published <= fetched_at < now -
    /// window`), so every survivor is undated and `UNDATED_ITEMS` fires. The hole is the third
    /// case: `collect_warnings` requires **all** survivors undated, so one postdated item
    /// suppresses the flag for every week-old undated item beside it. Don't treat
    /// `UNDATED_ITEMS` as a freshness signal — `cache_age_seconds` is the one that always
    /// arrives. See ADR-0022.
    #[tokio::test]
    async fn a_stale_body_older_than_the_since_window_flags_undated_items_unless_one_is_postdated()
    {
        let future = (Utc::now() + chrono::Duration::days(1)).to_rfc2822();
        let cases: [(&str, String, usize, bool); 3] = [
            // Dated: `since` empties it. Truthful, but "no news" now looks like "nothing new".
            (
                "dated",
                r#"<item><title>Old</title><guid>t3_a</guid>
                   <pubDate>Mon, 01 Jun 2026 00:00:00 GMT</pubDate></item>"#
                    .to_string(),
                0,
                false,
            ),
            // Reddit's `…/comments/.rss` shape: undated throughout, so the flag fires.
            (
                "undated",
                r#"<item><title>Comment</title><guid>t1_b</guid></item>"#.to_string(),
                1,
                true,
            ),
            // The suppression: one postdated entry makes `all(key.is_none())` false.
            (
                "postdated+undated",
                format!(
                    r#"<item><title>Postdated</title><guid>t3_p</guid>
                       <pubDate>{future}</pubDate></item>
                       <item><title>Undated</title><guid>t1_1</guid></item>"#
                ),
                2,
                false,
            ),
        ];

        for (label, items, expected_count, expect_undated_warning) in cases {
            let body =
                format!(r#"<rss version="2.0"><channel><title>t</title>{items}</channel></rss>"#);
            let mut server = mockito::Server::new_async().await;
            let dir = std::env::temp_dir().join(format!("rss-sw-{label}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let cache = Cache::open(Some(dir.clone())).unwrap();
            let url = format!("{}/feed.xml", server.url());

            let meta = crate::cache::CacheMeta {
                feed_url: url.clone(),
                etag: None,
                last_modified: None,
                fetched_at: (Utc::now() - chrono::Duration::days(8)).to_rfc3339(),
                content_type: Some("application/rss+xml".to_string()),
            };
            cache.put(&meta, body.as_bytes()).expect("seed");

            let m = server
                .mock("GET", "/feed.xml")
                .with_status(429)
                .expect(2) // original + the single bounded retry
                .create_async()
                .await;

            let http =
                crate::fetch::HttpClient::new("t", std::time::Duration::from_secs(5)).unwrap();
            let params = FetchParams {
                cache_policy: CachePolicy::StaleIfError,
                since: Some(Utc::now() - chrono::Duration::hours(24)),
                ..Default::default()
            };
            let out = fetch_feeds_with(std::slice::from_ref(&url), &params, &cache, &http).await;
            let feed = &out.feeds[0];
            let codes: Vec<&str> = out.warnings.iter().map(|w| w.code.as_str()).collect();

            assert_eq!(feed.status, FeedStatus::Stale, "{label}");
            assert_eq!(feed.item_count, expected_count, "{label}: {codes:?}");
            assert!(codes.contains(&"SERVED_STALE"), "{label}: {codes:?}");
            assert_eq!(
                codes.contains(&"UNDATED_ITEMS"),
                expect_undated_warning,
                "{label}: UNDATED_ITEMS is suppressed by any dated survivor, so it is not a \
                 freshness signal: {codes:?}"
            );
            // The age is machine-readable on the feed and always arrives, whatever the mix —
            // it is what a caller enforces its own freshness floor on (ADR-0019 §3).
            assert!(
                feed.cache_age_seconds.is_some_and(|s| s > 24 * 3600),
                "{label}: age must exceed the window: {:?}",
                feed.cache_age_seconds
            );
            // Still exit 0: a refused revalidation is not a feed error (invariant 5).
            assert_eq!(exit_code_for(&out), crate::error::exit::OK, "{label}");

            m.assert_async().await;
            std::fs::remove_dir_all(&dir).ok();
        }
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

    #[test]
    fn paginate_charges_applied_filters_against_the_budget() {
        // `applied_filters` is a top-level `FetchOutput` field, so `paginate`'s skeleton
        // (`output.clone()` with `feeds` cleared, per `estimate_response_tokens`) picks it up
        // for free via `serde` — no special-casing needed in `paginate` itself. Pin that: a
        // budget that exactly fits a bare output must reject once a real `applied_filters`
        // payload is attached and there is nothing left to shed but the sole item.
        let mut bare = output_with(vec![item(false)]);
        let budget = estimate_response_tokens(&bare);

        bare.applied_filters = Some(AppliedFilters {
            since: None,
            query: Some("a ".repeat(200)), // long enough to blow the same budget
            items_filtered_out: 3,
        });
        assert!(
            estimate_response_tokens(&bare) > budget,
            "attaching applied_filters must grow the measured payload"
        );

        let err = paginate(&mut bare, budget).unwrap_err();
        assert!(
            matches!(err, RssError::ResponseTooLarge { .. }),
            "the envelope (now including applied_filters) plus the one item must not fit, \
             got {err:?}"
        );
    }

    // --- Task 15: cross-feed duplicate reporting ---------------------------------------

    fn item_keyed(id: &str, guid: Option<&str>, url: Option<&str>) -> Item {
        Item {
            id: id.to_string(),
            guid: guid.map(|s| s.to_string()),
            url: url.map(|s| s.to_string()),
            ..item(false)
        }
    }

    /// Like [`item_keyed`] but with `content_hash: None` too, so `dedup_key` has nothing to
    /// fall back to — the "no key at all" case.
    fn item_keyless(id: &str) -> Item {
        Item {
            content_hash: None,
            ..item_keyed(id, None, None)
        }
    }

    fn two_feeds(a: Vec<Item>, b: Vec<Item>) -> FetchOutput {
        let mut out = output_with(a);
        let mut second = out.feeds[0].clone();
        second.feed_url = "https://example.com/two.xml".to_string();
        second.items = b;
        out.feeds.push(second);
        // Refresh rather than only fixing `item_count`: the cloned feed would otherwise
        // inherit feed 0's `content_tokens_est_total`, leaving the fixture internally
        // inconsistent before the code under test ever runs.
        refresh_feed_counts(&mut out);
        out
    }

    /// Build one feed per entry in `item_lists`, at `https://example.com/feedN.xml`.
    fn n_feeds(item_lists: Vec<Vec<Item>>) -> FetchOutput {
        let mut out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        for (i, items) in item_lists.into_iter().enumerate() {
            out.feeds.push(FeedResult {
                feed_url: format!("https://example.com/feed{i}.xml"),
                status: FeedStatus::Ok,
                from_cache: false,
                title: Some("Feed".to_string()),
                site_url: None,
                updated: None,
                item_count: 0,
                content_tokens_est_total: 0,
                items,
                error: None,
                cached_at: None,
                cache_age_seconds: None,
            });
        }
        refresh_feed_counts(&mut out);
        out
    }

    #[test]
    fn duplicates_match_on_guid_across_feeds() {
        // id is namespaced by feed_url (ADR-0003), so the same article syndicated through two
        // feeds has two different ids. guid is the cross-feed key.
        let out = two_feeds(
            vec![item_keyed("aaaa", Some("t3_abc"), Some("https://x/1"))],
            vec![item_keyed("bbbb", Some("t3_abc"), Some("https://x/1"))],
        );
        let groups = find_duplicates(&out);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].key, "t3_abc");
        assert_eq!(groups[0].key_kind, DuplicateKeyKind::Guid);
        assert_eq!(
            groups[0].item_ids,
            vec!["aaaa".to_string(), "bbbb".to_string()]
        );
        assert_eq!(
            groups[0].feed_urls,
            vec![
                "https://example.com/feed.xml".to_string(),
                "https://example.com/two.xml".to_string(),
            ],
            "feed_urls must be the feed each id came from, not the item's own feed_url field"
        );
    }

    #[test]
    fn duplicates_fall_back_to_url_then_content_hash() {
        let by_url = two_feeds(
            vec![item_keyed("aaaa", None, Some("https://x/1"))],
            vec![item_keyed("bbbb", None, Some("https://x/1"))],
        );
        assert_eq!(find_duplicates(&by_url)[0].key_kind, DuplicateKeyKind::Url);

        // No guid, no url: content_hash (set by item()) is the last resort.
        let by_hash = two_feeds(
            vec![item_keyed("aaaa", None, None)],
            vec![item_keyed("bbbb", None, None)],
        );
        assert_eq!(
            find_duplicates(&by_hash)[0].key_kind,
            DuplicateKeyKind::ContentHash
        );
    }

    #[test]
    fn distinct_items_produce_no_groups() {
        let out = two_feeds(
            vec![item_keyed("aaaa", Some("g1"), Some("https://x/1"))],
            vec![item_keyed("bbbb", Some("g2"), Some("https://x/2"))],
        );
        assert!(find_duplicates(&out).is_empty());
    }

    #[test]
    fn keyless_items_are_never_grouped() {
        // No guid, no url, no content_hash: dedup_key is None for both. They must not be
        // grouped with each other just because they share "no key".
        let out = two_feeds(vec![item_keyless("aaaa")], vec![item_keyless("bbbb")]);
        assert!(find_duplicates(&out).is_empty());
    }

    #[test]
    fn distinct_key_kinds_do_not_collide_on_equal_strings() {
        // item a's guid string equals item b's url string. If the dedup key were ever
        // simplified from `(String, DuplicateKeyKind)` to a bare `String`, this would
        // wrongly group them.
        let out = two_feeds(
            vec![item_keyed("aaaa", Some("https://x/1"), None)],
            vec![item_keyed("bbbb", None, Some("https://x/1"))],
        );
        assert!(
            find_duplicates(&out).is_empty(),
            "a guid and a url that share the same string must not be treated as the same key"
        );
    }

    #[test]
    fn intra_feed_duplicate_guid_forms_a_group() {
        // Two items inside the SAME feed sharing a guid (a feed that repeats an entry) are
        // grouped too -- find_duplicates groups by key over every item in the batch, not
        // only across feed boundaries. See the doc on `find_duplicates` / `DuplicateGroup`.
        let out = n_feeds(vec![vec![
            item_keyed("aaaa", Some("dup"), None),
            item_keyed("bbbb", Some("dup"), None),
        ]]);
        let groups = find_duplicates(&out);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].item_ids,
            vec!["aaaa".to_string(), "bbbb".to_string()]
        );
        assert_eq!(
            groups[0].feed_urls,
            vec![
                "https://example.com/feed0.xml".to_string(),
                "https://example.com/feed0.xml".to_string(),
            ]
        );
    }

    #[test]
    fn duplicate_group_can_span_three_feeds() {
        let out = n_feeds(vec![
            vec![item_keyed("a1", Some("shared"), None)],
            vec![item_keyed("a2", Some("shared"), None)],
            vec![item_keyed("a3", Some("shared"), None)],
        ]);
        let groups = find_duplicates(&out);
        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].item_ids,
            vec!["a1".to_string(), "a2".to_string(), "a3".to_string()]
        );
        assert_eq!(
            groups[0].feed_urls,
            vec![
                "https://example.com/feed0.xml".to_string(),
                "https://example.com/feed1.xml".to_string(),
                "https://example.com/feed2.xml".to_string(),
            ]
        );
    }

    #[test]
    fn groups_are_ordered_by_first_appearance_not_hash_iteration_order() {
        // Four distinct groups: a HashMap-iteration-order regression fails this with high
        // probability (~96%) on a random per-process hasher seed, unlike a 2-group version.
        let out = n_feeds(vec![
            vec![
                item_keyed("a1", Some("gA"), None),
                item_keyed("b1", Some("gB"), None),
                item_keyed("c1", Some("gC"), None),
                item_keyed("d1", Some("gD"), None),
            ],
            vec![
                item_keyed("a2", Some("gA"), None),
                item_keyed("b2", Some("gB"), None),
                item_keyed("c2", Some("gC"), None),
                item_keyed("d2", Some("gD"), None),
            ],
        ]);
        let groups = find_duplicates(&out);
        let keys: Vec<&str> = groups.iter().map(|g| g.key.as_str()).collect();
        assert_eq!(keys, vec!["gA", "gB", "gC", "gD"]);
    }

    #[test]
    fn find_duplicates_does_not_mutate_output() {
        let out = two_feeds(
            vec![item_keyed("aaaa", Some("t3_abc"), None)],
            vec![item_keyed("bbbb", Some("t3_abc"), None)],
        );
        let before = serde_json::to_value(&out).expect("serializable");
        let _ = find_duplicates(&out);
        let after = serde_json::to_value(&out).expect("serializable");
        assert_eq!(before, after, "find_duplicates must be read-only");
    }

    #[test]
    fn drop_duplicates_keeps_the_first_and_fixes_counts() {
        let mut out = two_feeds(
            vec![item_keyed("aaaa", Some("t3_abc"), None)],
            vec![item_keyed("bbbb", Some("t3_abc"), None)],
        );
        let groups = find_duplicates(&out);
        drop_duplicates(&mut out, &groups);

        assert_eq!(
            out.feeds[0].items.len(),
            1,
            "the canonical first copy stays"
        );
        assert_eq!(out.feeds[1].items.len(), 0, "the later copy is removed");
        assert_eq!(out.feeds[1].item_count, 0, "per-feed counts must follow");
        assert_eq!(out.total_items, 1);
        assert_eq!(
            out.total_content_tokens_est,
            out.feeds
                .iter()
                .map(|f| f.content_tokens_est_total)
                .sum::<u64>(),
            "top-level and per-feed token totals must stay consistent"
        );
    }

    #[test]
    fn drop_duplicates_with_empty_group_list_is_a_noop() {
        let mut out = two_feeds(
            vec![item_keyed("aaaa", Some("g1"), None)],
            vec![item_keyed("bbbb", Some("g2"), None)],
        );
        let before = serde_json::to_value(&out).expect("serializable");
        drop_duplicates(&mut out, &[]);
        let after = serde_json::to_value(&out).expect("serializable");
        assert_eq!(before, after, "an empty group list must change nothing");
    }

    #[test]
    fn drop_duplicates_ignores_a_stale_group_without_panicking() {
        // A group naming ids that are no longer present in `output` (e.g. computed against a
        // stale snapshot) must not panic and must not corrupt the counts of what IS present.
        let mut out = two_feeds(
            vec![item_keyed("aaaa", Some("g1"), None)],
            vec![item_keyed("bbbb", Some("g2"), None)],
        );
        let stale_group = DuplicateGroup {
            key: "ghost".to_string(),
            key_kind: DuplicateKeyKind::Guid,
            item_ids: vec!["zzzz".to_string(), "yyyy".to_string()],
            feed_urls: vec![
                "https://example.com/gone1.xml".to_string(),
                "https://example.com/gone2.xml".to_string(),
            ],
        };
        drop_duplicates(&mut out, &[stale_group]);

        assert_eq!(
            out.total_items, 2,
            "nothing present should have been removed"
        );
        assert_eq!(out.feeds[0].items.len(), 1);
        assert_eq!(out.feeds[1].items.len(), 1);
    }

    #[test]
    fn drop_duplicates_same_id_intra_feed_keeps_one_copy() {
        // An intra-feed duplicate commonly shares an id too: ADR-0003 keys `id` on
        // feed_url + (link -> guid -> title|published), so a feed repeating the same entry
        // (same link/guid) produces two items with the SAME id, not two different ones.
        let out = n_feeds(vec![vec![
            item_keyed("same", Some("dup"), None),
            item_keyed("same", Some("dup"), None),
        ]]);
        let groups = find_duplicates(&out);
        assert_eq!(
            groups[0].item_ids,
            vec!["same".to_string(), "same".to_string()]
        );
        let mut out = out;
        drop_duplicates(&mut out, &groups);
        assert_eq!(out.feeds[0].items.len(), 1, "exactly one copy survives");
        assert_eq!(out.total_items, 1);
    }

    #[test]
    fn drop_duplicates_three_same_id_copies_keeps_exactly_one() {
        // Same as the 2-copy case but with three repeats, so a fix that special-cased
        // "exactly 2" (e.g. "keep first, remove second") rather than counting the real
        // removal budget would still fail this one.
        let out = n_feeds(vec![vec![
            item_keyed("same", Some("dup"), None),
            item_keyed("same", Some("dup"), None),
            item_keyed("same", Some("dup"), None),
        ]]);
        let groups = find_duplicates(&out);
        assert_eq!(groups[0].item_ids.len(), 3);
        let mut out = out;
        drop_duplicates(&mut out, &groups);
        assert_eq!(out.feeds[0].items.len(), 1, "exactly one copy survives");
        assert_eq!(out.total_items, 1);
    }

    #[test]
    fn drop_duplicates_removes_by_key_not_by_id_when_ids_collide() {
        // `identity.rs` derives `id` from link -> guid -> title|published; `dedup_key`
        // prefers guid -> url -> content_hash. The two preferences DISAGREE, so "same id"
        // and "same duplicate group" are independent properties: a feed whose entries all
        // link to the same page gives every one of its items the same id, while their guids
        // put them in different groups (or in none).
        //
        // Removal must therefore follow the group key, not the id. Budgeting N removals per
        // id lets the first item carrying that id absorb the budget — deleting an item no
        // group ever named, and leaving the real duplicate in place. Counts stay internally
        // consistent either way, so only an identity assertion catches it.
        let out = n_feeds(vec![
            vec![item_keyed("bbbb", Some("shared"), None)],
            vec![
                item_keyed("xxxx", Some("unrelated"), None),
                item_keyed("xxxx", Some("shared"), None),
            ],
        ]);
        let groups = find_duplicates(&out);
        assert_eq!(groups.len(), 1, "only the shared guid groups");
        assert_eq!(groups[0].key, "shared");

        let mut out = out;
        drop_duplicates(&mut out, &groups);
        assert_eq!(out.feeds[0].items.len(), 1, "the canonical copy stays");
        assert_eq!(
            out.feeds[1]
                .items
                .iter()
                .map(|i| i.guid.as_deref().unwrap_or(""))
                .collect::<Vec<_>>(),
            vec!["unrelated"],
            "the duplicate must go and the item no group named must stay — not the reverse"
        );
        assert_eq!(out.total_items, 2);
    }

    // --- Task 16: the shared `dedupe` mode both front-ends apply -----------------------

    /// Two feeds carrying the same entry, plus one item unique to the second feed.
    fn overlapping_feeds() -> FetchOutput {
        n_feeds(vec![
            vec![item_keyed("aaaa", Some("shared"), None)],
            vec![
                item_keyed("bbbb", Some("shared"), None),
                item_keyed("cccc", Some("only-here"), None),
            ],
        ])
    }

    #[test]
    fn apply_dedupe_report_groups_without_removing_anything() {
        let mut out = overlapping_feeds();
        apply_dedupe(&mut out, DedupeMode::Report);
        assert_eq!(out.duplicates.len(), 1, "the shared guid groups");
        assert_eq!(out.duplicates[0].key, "shared");
        assert_eq!(out.total_items, 3, "reporting must not remove items");
        assert_eq!(out.feeds[0].item_count, 1);
        assert_eq!(out.feeds[1].item_count, 2, "per-feed counts stay untouched");
    }

    #[test]
    fn apply_dedupe_off_skips_detection_entirely() {
        let mut out = overlapping_feeds();
        apply_dedupe(&mut out, DedupeMode::Off);
        assert!(
            out.duplicates.is_empty(),
            "off must not even report: {:?}",
            out.duplicates
        );
        assert_eq!(out.total_items, 3);

        // And `duplicates` reflects the mode it was last given, not whatever was there
        // before: `off` after a `report` must not leave the earlier groups behind.
        apply_dedupe(&mut out, DedupeMode::Report);
        assert_eq!(out.duplicates.len(), 1);
        apply_dedupe(&mut out, DedupeMode::Off);
        assert!(
            out.duplicates.is_empty(),
            "off must clear a previous report's groups: {:?}",
            out.duplicates
        );
        assert_eq!(out.total_items, 3, "and still remove nothing");
    }

    #[test]
    fn apply_dedupe_drop_removes_later_copies_and_keeps_the_audit_trail() {
        let mut out = overlapping_feeds();
        apply_dedupe(&mut out, DedupeMode::Drop);
        assert_eq!(out.total_items, 2, "the later copy goes");
        assert_eq!(out.feeds[0].item_count, 1, "the canonical copy stays");
        assert_eq!(
            out.feeds[1]
                .items
                .iter()
                .map(|i| i.id.as_str())
                .collect::<Vec<_>>(),
            vec!["cccc"],
            "only the duplicate is removed from the later feed"
        );
        // The groups stay on the output *after* removal: they are the audit trail naming
        // which items are gone, so a caller can still see what was collapsed.
        assert_eq!(out.duplicates.len(), 1);
        assert_eq!(
            out.duplicates[0].item_ids,
            vec!["aaaa".to_string(), "bbbb".to_string()],
            "the group must still name the removed copy, not only the survivor"
        );
    }

    #[test]
    fn apply_dedupe_drop_keeps_derived_counts_consistent() {
        let mut out = overlapping_feeds();
        apply_dedupe(&mut out, DedupeMode::Drop);
        let expected: usize = out.feeds.iter().map(|f| f.items.len()).sum();
        assert_eq!(out.total_items, expected);
        for feed in &out.feeds {
            assert_eq!(feed.item_count, feed.items.len());
            let tokens: u64 = feed
                .items
                .iter()
                .map(|i| u64::from(i.content_tokens_est))
                .sum();
            assert_eq!(feed.content_tokens_est_total, tokens);
        }
        assert_eq!(
            out.total_content_tokens_est,
            out.feeds
                .iter()
                .map(|f| f.content_tokens_est_total)
                .sum::<u64>(),
        );
    }

    #[test]
    fn fresh_fetch_output_serializes_duplicates_as_empty_array_not_omitted() {
        let out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        let v = serde_json::to_value(&out).expect("serializable");
        assert_eq!(
            v.get("duplicates"),
            Some(&serde_json::json!([])),
            "duplicates must serialize as [] and never be omitted (invariant 2)"
        );
    }
}
