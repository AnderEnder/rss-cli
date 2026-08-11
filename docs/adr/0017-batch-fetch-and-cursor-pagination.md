# 17. Batch fetch and stateless cursor pagination for `fetch_feed`

- **Status:** Accepted
- **Date:** 2026-08-10

## Context

An agent assembling a rate-limited, multi-source daily digest reported seven gaps in the
`rss mcp` tool surface. Verification against the code showed **four of the seven already
existed** and the report was mostly a **discovery** problem, not an engineering one:
`RATE_LIMITED` with `retry_after` already existed, conditional GET was already the default,
and per-host throttling already existed — but only for `fetch_feed`; `get_item` and
`discover_feeds` each built their own `HttpClient` and bypassed the shared client and the
gate entirely, a real gate-bypass bug rather than a documentation gap, fixed by routing both
through the server's shared client. Cache visibility/control was half built (`from_cache`
existed, cache age and an MCP `cache_policy` argument did not). Only two gaps were genuinely
absent: cross-feed dedup and keyword filtering (Phase 3 — not built as of this ADR).

The one gap that was real *and* structural was batch fetch. `core::fetch_feeds_with` already
accepted `&[String]` and fetched concurrently with per-host pacing ([ADR-0016](0016-per-host-request-gate.md))
— but `FetchFeedArgs` exposed only a single `url`, so a caller wanting several feeds had to
issue several tool calls and pace them itself, defeating the point of the server-side gate.
ADR-0016 anticipated this and deliberately deferred it:

> **Batch `fetch_feed` (`urls: [..]`) so the server paces internally.** A strong agent-UX win
> … but it adds contract surface, needs a per-feed response-budget rework, and does not help
> the CLI. Deferred to a possible follow-up ADR; the gate fixes both front-ends first.

This ADR is that follow-up. Wiring the batch through exposed the response-budget rework
ADR-0016 predicted would be needed: the existing budget check
(`core::enforce_response_budget`) was all-or-nothing — reject the whole `FetchOutput` if it
overflowed. With a single URL that discards one feed's data on retry. With a batch of `N`
URLs it discards up to `N` **successful** network fetches over one trailing item, and a naive
retry re-hits every host the batch just paced its way through. That forced the second half of
this ADR: pagination.

## Decision

Five coordinated changes, all landing in `core.rs`/`mcp.rs`/the new `cursor.rs` — invariant
#6's shared-core rule, not a claim that the CLI (which has no MCP response budget to page
against, per [ADR-0011](0011-bounded-mcp-responses.md)) uses any of it.

### 1. Batch `urls[]`, keep `url`

`FetchFeedArgs` gains `urls: Option<Vec<String>>` (lenient-deserialized like every other MCP
array/scalar argument — see the gotchas below) alongside the existing `url: Option<String>`.
Exactly one of the two must be supplied; both or neither is a usage error. `url` is **not**
renamed or deprecated: every existing client configuration passes `url`, and renaming it
would break all of them for no behavioral gain. `resolve_urls` is the one place that
reconciles the two into a single ordered `Vec<String>`, capped at `MCP_MAX_URLS = 50` to
bound one call's blast radius — a caller wanting more pages via `truncation.next_cursor`.

### 2. Fill, don't reject

`core::paginate` replaces the reject-only budget check on the `fetch_feed` path. The
previous check, `core::enforce_response_budget`, is left in place (not deleted) but is no
longer called from either tool in production: `get_item_inner` builds its own single-item
`ResponseTooLarge` guard inline rather than through it. Given a `FetchOutput` and a token
budget, `paginate` greedily fills feeds and items in (feed, item) order — the same order the
response ships in — until the next item would overflow, then trims to that boundary and
reports a `core::PageStop` describing exactly where the next page resumes.

Two properties the outline didn't anticipate turned out to be load-bearing:

- **The per-part estimate must never over-estimate.** `core::pretty_tokens` prices an item
  or a feed envelope by pretty-printing it *on its own* plus a flat `+ 2`, deliberately
  ignoring that nesting the same value inside `FetchOutput` costs more (indentation alone
  runs roughly 12 tokens per feed envelope and 38 per item). That gap is what makes the
  greedy running total a **lower bound** on the real serialized cost, which is what makes a
  greedy rejection imply a real one. Raising the constant to "improve accuracy" would let
  the greedy total exceed the real cost on some inputs, rejecting pages that would in fact
  have fit — reintroducing a narrower version of the all-or-nothing bug this decision fixes.
  Pinned by `paginate_ships_a_page_that_fits_even_when_a_later_feed_is_dropped` and
  `paginate_still_errors_below_the_first_feeds_envelope_and_item`.
- **Forward progress is measured in items shipped, not cursor position.** A position-based
  "did we advance" guard is satisfied by a page that skips past a leading zero-item feed
  (an error entry, or a feed the trim dropped whole) without shipping anything at all — the
  resulting cursor would point at the very position the *next* request already starts from,
  looping a client forever. `paginate` instead asserts `item_count(output) > 0` before
  returning a stop, and returns `Err(ResponseTooLarge)` otherwise. Combined with the
  never-over-estimate property, `Err` from `paginate` means exactly one thing: **no
  non-empty prefix of this output fits the budget** — one oversized item, or an over-budget
  envelope with nothing left to shed.

### 3. A stateless cursor is the pagination store

`src/cursor.rs` defines an opaque, base64url-encoded `Cursor { v, fp, f, i, n, s }`. The
server keeps **no server-side result store** — nothing to evict, nothing to leak across
requests or processes. Instead, a continuation call forces `CachePolicy::CacheFirst`
(overriding whatever `cache_policy` the caller passed), so the **existing HTTP body cache**
([ADR-0004](0004-file-based-atomic-cache.md)) is the pagination store: a feed already
fetched on page 1 costs no network call on page 2. Positions (`f`: an index into the
request's URL list; `i`/`n`: an index and item-count into that feed's item list, for
roll detection) are stable across pages because item ids are deterministic by construction
([ADR-0003](0003-deterministic-content-hash-item-ids.md)) and a cache hit returns
byte-identical bytes.

A corollary the outline underestimated: `paginate` measures the page **before** the
`next_cursor` / `TruncationInfo` marker exists to attach to it, so a page filled exactly to
`max_response_tokens` would ship over budget once the marker (and the result's summary text)
land on top. `mcp.rs`'s `CURSOR_HEADROOM_TOKENS` reserves for this — computed once, from a
`worst_case_marker()` (every numeric field at its max, a full-length cursor, the widest
combined suggestion text) rather than hardcoded, so the reserve tracks the marker's actual
shape as the code around it changes. The plan's original estimate of ~40–60 tokens was off
by close to an order of magnitude: the measured reserve is roughly 230 tokens for today's
constants (the dominant costs are the suggestion strings and the encoded cursor, not the
numeric fields). Pinned by `cursor_headroom_covers_the_marker_it_reserves_for`, which fails
loudly if the reserve ever under-covers the real marker.

### 4. Fingerprint raw arguments; carry the resolved `since` in the cursor

`cursor::fingerprint` hashes the URL list plus a tuple of scalar arguments **as the client
sent them** — not as `core` resolved them — with one deliberate exception. `since: "2h"`
resolves to `Utc::now() - 2h`, a different instant on every call; fingerprinting the
*resolved* value would make a page-2 cursor mismatch every time (the fingerprint would never
match a fresh resolution), and even a matching fingerprint would filter page 2 against a
later cutoff than page 1, silently skipping items in between. The fix: fingerprint the raw
`since` string, and carry the **resolved** cutoff as the cursor's `s` field (epoch seconds).
A continuation reuses that exact instant instead of re-resolving.

Two positions in the fingerprinted tuple were reserved for arguments that did not exist yet
at the time this ADR was written: Phase 3's `query` and `dedupe`. Both contributed the fixed
bytes `""` and `"report"` respectively — every outstanding cursor depended on those exact
bytes, not on any real filtering behavior. As of the `query` argument landing, the `query`
position carries the caller's actual value instead of the `""` placeholder; `dedupe` remains
reserved (fixed `"report"`) until it lands too. This is exactly the predicted transition:
populating a placeholder position with a real value changes every fingerprint's output, so
outstanding cursors minted before that change no longer match and are rejected as "does not
match this request" — the safe failure mode (a caller retries without a cursor) rather than a
silent mismatch.

### 5. The batch deadline omits feeds; no new `FeedStatus` variant

`FetchParams::deadline` bounds a batch's wall-clock budget (the MCP server sets it from
`MCP_BATCH_DEADLINE_SECS`/`RSS_MCP_BATCH_DEADLINE_SECS`; the CLI leaves it `None`). A feed
whose fetch had not **started** by the deadline is left out of `feeds[]` entirely and counted
in `truncation.feeds_omitted` — it did not fail, so it does not get a `FeedResult::error`
entry or an `errors[]` entry. This reuses the same `truncation`/`next_cursor` vocabulary
pagination already established, rather than adding a new `FeedStatus::Deferred` (or similar)
variant: a new enum variant is **wire-visible** to any consumer that matches `FeedStatus`
exhaustively (every existing client breaks or must add a case), while an additive field on an
already-optional `TruncationInfo` is not.

A tradeoff this forces: `fetch_feeds_with` fans out through `buffer_unordered`, which
completes **out of order** — feeds 0, 2, 3 can finish while feed 1, admitted at the same time,
is still queued behind the deadline check (or, degenerately, was never polled before the
deadline passed). `assemble_prefix_in_request_order` keeps only the contiguous prefix up to
the first unattempted feed and **discards every later result, including ones that
succeeded**, because the cursor's `f` indexes the original request URL list — a gap would
shift every position after it by one, and a cursor minted from a shifted position silently
resumes at the wrong feed. Throwing away already-completed work is the price of keeping
positions meaningful; the alternative (shipping the out-of-order survivors) saves a handful of
fetches on one page at the cost of corrupting every cursor that follows.

## Known limitations

- **The batch deadline bounds when a fetch may *start*, not permit contention.**
  `buffer_unordered(params.concurrency)` (default 8) admits that many futures at once; all of
  them pass the deadline check at t≈0 and then queue on the per-host permit
  (`HOST_MAX_CONCURRENCY = 1`), where no clock reaches them. The real bound on a batch is
  therefore the deadline **plus up to `concurrency - 1` already-admitted fetches**, each
  serialized behind the host gate and each able to additionally sit out a cooldown escalating
  toward `HOST_MAX_COOLDOWN` (bounded, in turn, by the per-request `timeout`). This is a
  distinct gap from what `MAX_GATE_WAIT` covers ([ADR-0016](0016-per-host-request-gate.md)):
  that bounds one *sibling's* pacing wait once it holds a permit, not the wait *for* a permit
  in the first place. The deeper fix — threading the batch deadline into `HostGate`'s permit
  acquire and `timeout_at`-ing the semaphore — was deliberately deferred; it touches
  `fetch.rs`/`ratelimit.rs` and is a larger, riskier change than this ADR's scope. Anchored in
  code at `FetchParams::deadline`'s doc comment and the `stop_at` check in
  `core::fetch_feeds_with`, both of which point back here.
- **The `errors[]` orphan repair lives in `mcp.rs`, not in `core::paginate`.** `paginate`
  trims `output.feeds` but has no opinion on `output.errors` — it returns a `FetchOutput`
  whose `errors[]` can still name a feed that `feeds[]` no longer contains (a feed dropped
  whole because none of it fit). `fetch_feed_with_deadline` repairs this itself, retaining
  only errors whose `feed_url` is still present in the trimmed `feeds[]`. Nothing is lost —
  the next page reports the same feed again — but the fix is scoped to this one caller. The
  next code path that calls `core::paginate` directly will inherit the same orphaning unless
  it repeats the retain-filter itself; `output.warnings[]` has the identical shape of gap
  (a `Warning` also carries an optional `feed_url`) and today is filtered **nowhere** at all,
  including in `mcp.rs`. Fixing either belongs in `core::paginate` itself, not in this ADR.

## Consequences

- These are additive contract changes only — new optional fields, no removed or renamed
  ones — so `SCHEMA_VERSION` stays `"1"` ([ADR-0006](0006-ai-facing-output-contract.md)).
- `HOST_MAX_CONCURRENCY = 1` ([ADR-0016](0016-per-host-request-gate.md)) still serializes
  same-host requests, so a large single-host batch does not get faster just because it is
  now one call — it pages across multiple `fetch_feed` calls via `next_cursor` instead of one
  slow call, which is the intended tradeoff (a usable partial result now beats a complete one
  that might outlive the caller's tool-call timeout).
- `RESPONSE_TOO_LARGE` on `fetch_feed` **narrows**: it no longer fires whenever the whole
  batch doesn't fit, only when not even one item can be placed in the budget (an oversized
  single item, or an envelope with nothing to shed). `get_item`'s single-item
  `RESPONSE_TOO_LARGE` guard is unrelated and unchanged — there is exactly one item, so "not
  even one item fits" and "the batch doesn't fit" coincide there.
- `truncation.items_omitted`/`feeds_omitted` describe **this page only**, not a running
  total across a paged sequence — a client summing them across pages double-counts (or, more
  precisely, sums two independently-scoped snapshots, which is not a meaningful total).
  `SERVER_INSTRUCTIONS` and the tool description both call this out.
- A batch deadline that expires mid-fan-out throws away any already-completed work past the
  first unattempted feed, in exchange for `feeds[]` staying a provably contiguous prefix of
  the request — the property the cursor's `f` field depends on.

## Alternatives considered

- **Keep rejecting over-budget batches outright.** The status quo before this ADR, and
  exactly the problem ADR-0016 flagged batching would make worse: with `N` URLs, rejection
  discards `N` successful fetches instead of one, and a retry re-hits every host the gate
  just finished pacing through. Rejected.
- **A server-side pagination result store** (cache the trimmed remainder under a token,
  serve it back on the next call). Rejected: it needs an eviction policy (size or TTL) that a
  stateless server does not otherwise have, it does not survive a server restart between
  pages, and the existing body cache already serves the same purpose for free once a
  continuation is forced to `CacheFirst`.
- **Fingerprint the resolved `since` cutoff instead of the raw string.** Rejected — see
  Decision 4; it makes every relative-window cursor mismatch on the very next call.
- **A new `FeedStatus` variant for a deadline-omitted feed** (e.g. `Deferred`). Rejected: it
  is wire-visible to any client that matches `FeedStatus` exhaustively, forcing every
  existing consumer to add a case for something that isn't a fetch outcome at all — the feed
  was never attempted. Folding it into the already-optional, already-additive
  `TruncationInfo.feeds_omitted` needed no consumer change.
- **Scale `limit` down by the number of URLs in a batch** (so a 10-URL call with `limit=25`
  caps at 2–3 items per feed instead of 25). Rejected: `limit` is documented as per-feed, and
  a caller who fetches the same single URL alone vs. inside a 10-URL batch would silently get
  different amounts of data for the same feed. The response budget, not the item cap, is
  what should shrink under batch pressure — which is exactly what pagination does.
