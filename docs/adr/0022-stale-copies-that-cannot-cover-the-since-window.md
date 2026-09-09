# 22. A stale copy that cannot cover the `since` window

- **Status:** Proposed — **not implemented.** Reverses a decision ADR-0019 took deliberately;
  needs a call on that before code lands.
- **Date:** 2026-09-09

## Context

A field report from the same digest task that motivated
[ADR-0019](0019-stale-if-error-cache-policy.md): a run with `since: "24h"` and
`cache_policy: "stale-if-error"` hit Reddit's limiter and got back a feed with
`status: "stale"` and `cache_age_seconds: 690896` — a cache entry **eight days old** answering
a **24-hour** question.

ADR-0019 §Consequences declined a freshness ceiling on purpose: "An agent that wants a hard
freshness floor can enforce it client-side on [`cache_age_seconds`]; the server does not guess
a staleness ceiling on the caller's behalf." That reasoning still holds for a *bare*
`stale-if-error` fetch. It is weaker when the caller has already stated its window.

### What actually ships today

Pinned by
`core::tests::a_stale_body_older_than_the_since_window_flags_undated_items_unless_one_is_postdated`:

| Cached body | `item_count` | Warnings | Exit |
|---|---|---|---|
| dated items | **0** (contingent — see below) | `SERVED_STALE` | 0 |
| undated items (Reddit `…/comments/.rss`) | 1 | `SERVED_STALE`, `UNDATED_ITEMS` | 0 |
| one postdated item + undated items | 2 | `SERVED_STALE` **only** | 0 |

Two facts follow, and the first corrects the obvious reading of the report:

1. **Absent postdating, week-old items always arrive flagged — and postdating is exactly how
   that breaks.** A *dated* survivor of a window wider than the cache age is impossible:
   it would need `published >= now - window`, but `published <= fetched_at < now - window`. So
   every survivor is undated, and `collect_warnings` — which runs on *survivors*, after `since`
   and after `limit` — fires `UNDATED_ITEMS`. That is the whole guarantee, and it rests on the
   `all(key.is_none())` predicate: **one postdated item makes it false and suppresses the flag
   for every week-old undated item beside it** (row 3). Postdating is therefore not a footnote
   but the mechanism by which the guarantee fails — and it is also what makes row 1's `0`
   contingent rather than general. `cache_age_seconds` is the only signal that arrives in all
   three rows; `UNDATED_ITEMS` must not be read as a freshness signal.
2. **The real defect is the *dated* case, which the report did not name.** `item_count: 0`,
   `error: null`, `status: "stale"`, exit `0`. Nothing *false* is stated, but the response
   conflates two situations a digest must distinguish: *"this feed published nothing in your
   window"* and *"we have a blind spot spanning your entire window and could not reach the
   origin."* The caller can tell them apart only by comparing `cache_age_seconds` against a
   window it must re-derive itself — an inference available in principle and not made in
   practice.

### The framing that makes this decidable

Not *age* but **coverage**. `cache_age_seconds` measures time since the last *confirmation*
with the origin, not the age of the bytes — `FeedResult::cached_at`'s own doc comment is
explicit, and a feed that `304`s hourly holds `cache_age_seconds ≈ 1h` however old its newest
item is. A ceiling built on "the data is too old" would therefore be wrong in both directions.

The sound predicate is: **is the unconfirmed gap wider than the window the caller asked
about?** When `cache_age_seconds > since_window`, any item published inside the requested
window may be missing, and by construction no dated item we hold can fall inside it. That
predicate is correct on a cleanly-revalidating feed too — a `304` ten minutes ago means full
coverage of a 24h window, and `item_count: 0` is then a truthful "nothing new."

## Proposal

**When the caller passes `since` and `cache_age_seconds > (now - resolved_since)`, do not paper
over the refusal.** Propagate the original `RssError` instead of downgrading it to a warning.

Deliberately *not* "return `RATE_LIMITED`" as a blanket rule: `fetch::is_origin_refusal` admits
`Http`/`RateLimited`/`Network`, so the honest code is whatever the origin earned — per
[ADR-0021](0021-origin-429-is-rate-limited.md) that is `RATE_LIMITED` for a `429` and
`FEED_FETCH_FAILED` for a `403`, which is the right asymmetry for the same reason ADR-0021
gives.

No `since`, no window, no rule: a bare `stale-if-error` fetch keeps ADR-0019's behaviour
exactly, which is its motivating case (a 40-minute-old subreddit beats no subreddit).

## Consequences if accepted

- **An exit-code flip, reachable only by opt-in.** A fully-stale-and-uncovered batch goes from
  exit `0` to exit `4`. This is ADR-0019's own argument for making `stale-if-error` opt-in, now
  cutting the other way — there it protected callers who never asked; here the caller *did*
  ask, and asked for a window the answer cannot satisfy.
- **`since` grows a second meaning:** a filter over items *and* a coverage requirement on the
  cache. That is new surface on an existing parameter, and the cheaper-to-explain alternative
  is `max_stale` below.
- **A partial regression of ADR-0019's availability win** for exactly the callers who scope by
  time — which is most digest callers. Worth stating plainly: this trades availability for
  honesty, and reasonable people would not all take that trade.
- It does **not** touch `UNDATED_ITEMS`, and undated feeds lose the most: today they get real
  items, and after this they get an error whenever the origin refuses and the cache is cold
  enough. This is in tension with the `since`-retains-undated rule
  (`parse.rs`, pinned by `since_retains_undated_items_so_a_comment_feed_is_not_emptied`),
  whose whole point is not emptying a comment feed. An error is at least loud where that
  gotcha's concern was silence — but the tension is real and should be decided, not glossed.

## Alternatives considered

- **`max_stale` parameter; caller sets its own tolerance.** Strictly more expressive, needs no
  reinterpretation of `since`, and keeps ADR-0019's "the server does not guess" stance intact.
  Rejected as the *primary* fix only because it does nothing for a caller that does not know to
  pass it — and the failure here was an agent not making an inference it had the data for.
  **Should probably ship alongside rather than instead.**
- **Add the age to the `SERVED_STALE` warning as a structured field.** Requested in the report.
  Rejected as redundant rather than missing: the age is machine-readable on
  `FeedResult::cache_age_seconds` (ADR-0019 §3 chose that over a new field), so it is available
  on the feed — **one join away**, since `warnings[]` is a top-level list keyed by `feed_url`.
  A caller that branches on `feed.status == "stale" && feed.cache_age_seconds > tolerance`
  never needs the join or any text parsing. Adding `Warning.details` would be a schema change
  buying a shortcut, not a capability.
- **Do nothing.** Defensible: `status`, `cache_age_seconds` and `SERVED_STALE` always arrive,
  and the fix costs availability. Two things argue against. Fact 2 — an agent reading
  `item_count: 0, error: null` concludes "no news," and that conclusion is wrong in a way the
  server could have prevented. And row 3 — the one case that ships genuinely week-old items
  loses `UNDATED_ITEMS` to the `all()` predicate, so "the caller can always see it" holds only
  for a caller already comparing age against its own window, which is the inference that did
  not happen.
- **Emit a distinct warning (`STALE_EXCEEDS_WINDOW`) and keep exit 0.** Additive, no
  availability loss, no exit-code change. Weaker for the same reason ADR-0021 rejected
  `retryable: true` in `details` — agents branch on status and error first, and a fifth warning
  code is a fifth thing to not read. Cheapest option if the exit-code flip is judged too
  expensive.
