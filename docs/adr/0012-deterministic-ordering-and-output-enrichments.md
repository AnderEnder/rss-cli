# 12. Deterministic ordering and self-describing output enrichments

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

A round of "what else would make the output more AI-friendly?" surfaced several gaps in the
`fetch` contract. None is a bug per se, but each makes an agent's life harder:

1. **Non-deterministic feed order.** `core::fetch_feeds` collected results from
   `buffer_unordered` in *completion* order, so `feeds[]`/`errors[]` came back in whatever
   order the network happened to finish. Two runs over the same URLs could interleave
   differently — an agent couldn't address a feed by position, and diffs were noisy.
   [ADR-0002](0002-data-on-request-not-a-subscription-manager.md) loosely implied "same
   inputs → same output," which the code did not actually honor for ordering.
2. **No aggregate counts.** To budget a response an agent had to walk every feed and sum
   `content_tokens_est` itself.
3. **No cheap change-detection.** The stable `id` says *which* item; nothing said whether
   its *body* changed between fetches.
4. **Silent fallbacks.** When HTML→Markdown conversion failed we degraded to a tag strip
   ([ADR-0009](0009-html-to-markdown-htmd-html2text.md)) and said nothing; feeds whose items
   are entirely undated were ordered by feed order with no signal that "newest-first" was
   best-effort.
5. **Lossy NDJSON.** `--format ndjson` emitted only items; feed-level errors went to stderr,
   so a consumer piping just stdout silently lost failures.

## Decision

Five **additive** enrichments to the `fetch` output. All are new fields or an opt-in stream
mode, so `SCHEMA_VERSION` stays `"1"` ([ADR-0006](0006-ai-facing-output-contract.md)).

- **Deterministic ordering *within a run*.** `fetch_feeds` tags each concurrent task with
  its input index and sorts by it before assembling output, so `feeds[]` (and the mirrored
  `errors[]`) are always in request order. An agent can address feeds by position and diff
  two runs cleanly **modulo** the fields that legitimately vary.
  - **This is ordering determinism, not byte-reproducibility.** Output is *never* byte-equal
    across runs by design: `fetched_at` is `now()`, and `status`/`from_cache` flip
    `200`→`304` on revalidation ([ADR-0005](0005-conditional-get-always-revalidate-default.md)).
    We claim only that the *order and identity* of results are stable, not their timestamps.
- **Aggregate counts.** Top-level `total_items` and `total_content_tokens_est` (`u64`), plus
  per-feed `item_count` and `content_tokens_est_total`, computed in `core` **after**
  `--since`/`limit`/truncation — i.e. they describe what the agent actually receives, which
  is the number to budget against.
- **`content_hash` per item.** A 16-hex SHA-256 prefix of the **pre-truncation** extracted
  content (so the hash is independent of `max_content_chars`), `null` when `content` is
  `null`. Cheap change-detection without diffing text; `sha2` is already a dependency.
- **Non-fatal `warnings: []`.** A new top-level array of `{feed_url, code, message}`,
  distinct from `errors` (which mean a feed failed). Kept **deliberately rare** so it stays
  meaningful:
  - `CONTENT_EXTRACTION_FALLBACK` — one aggregated warning per feed when the Markdown/Text
    converter errored and we fell back to a tag strip. This required threading a `fell_back`
    signal out of `content::extract` (now returns `(String, bool)`).
  - `UNDATED_ITEMS` — emitted **only when every returned item is undated** (so ordering is
    genuinely unreliable). We do *not* warn per-undated-item: that fires on a large fraction
    of real feeds and would train agents to ignore the array.
- **Self-contained NDJSON (opt-in).** `--ndjson-records` emits one tagged record per line
  (`{"type":"item"…}`, `{"type":"error"…}`, and a final `{"type":"summary", total_items,
  total_content_tokens_est, warnings, truncation, …}`), so a stdout-only consumer keeps the
  errors and totals. The **default** ndjson stream stays bare `Item` lines for
  back-compatibility.

## Consequences

- `feeds[]`/`errors[]` are positionally stable within a run; the `mixed_feeds` integration
  test now asserts order, not just membership.
- Agents can budget from `total_content_tokens_est` before reading any body, and detect a
  changed body via `content_hash` without re-reading it.
- The `warnings` channel is low-volume by construction; an empty `[]` is the common case.
- `content::extract`'s signature change rippled through `entry_to_item` → `parse_feed` →
  `ParsedFeed` → `core`; `fetch_one` now returns `(FeedResult, Vec<Warning>)` and the caller
  aggregates warnings to the top level (mirroring how errors already work).
- The opt-in NDJSON mode means existing `jq`-style item pipelines are unaffected unless they
  ask for records.

## Alternatives considered

- **Claim byte-reproducible output** (the framing an earlier draft used for ordering).
  Rejected as false: timestamps and cache status vary by design. Overclaiming would repeat
  the imprecision this ADR corrects in ADR-0002.
- **Tagged NDJSON as the default.** Rejected — it breaks every consumer that assumes one
  `Item` per line. Made opt-in instead.
- **Per-undated-item warnings** (and warning on *any* undated item). Rejected as noise; only
  the all-undated case is a real ordering caveat.
- **Per-feed `warnings` mirrored like `error`.** Considered (it mirrors the errors pattern)
  but deferred to keep the contract surface smaller; each `Warning` already carries
  `feed_url`, so an agent can group top-level warnings by feed.
