# 20. Per-feed caller watermarks (`since_last_seen`)

- **Status:** Proposed — **not implemented.** Requires a decision on the ADR-0002 boundary
  before any code lands.
- **Date:** 2026-09-03

## Context

A digest agent's job is "everything new since last time." Nothing in the current surface
expresses that, so every caller reconstructs it from two approximations:

1. **A time window** (`since: "24h"`), which is wrong at both edges. Too narrow and a late
   run drops items; too wide and it re-delivers. It also keys on `published`, which Reddit
   comment feeds leave `null` and many feeds backdate.
2. **A client-side seen-store.** The digest task keeps `.seen-posts.json` and marks items as
   delivered. **Three separate bugs have now occurred in that mechanism** — over-marking 52
   stories, then 14, then 2 — each one silently suppressing content that was never shown to
   anyone.

That bug density is the actual argument. The mechanism is not incidental complexity in the
caller; it is a distributed-systems problem (exactly-once delivery over a rolling window) that
we hand to a model to maintain by hand in a JSON file. `item.id` is deterministic
([ADR-0003](0003-deterministic-content-hash-item-ids.md)) and the cache already sits next to
the data, so the server is where this is cheap and correct.

### Why this is not simply allowed

It is squarely against three standing commitments, and this ADR exists to name that rather
than route around it:

- **[ADR-0002](0002-data-on-request-not-a-subscription-manager.md)** — "data-on-request, not a
  subscription manager … no subscriptions, no read/unread state."
- **Invariant 11** — "MCP pagination is stateless and cache-backed. **Don't add a server-side
  store.**"
- **v1 non-goals** — "persisted read/unread/star state," explicitly requiring a new ADR.

If accepted, this **supersedes the read-state clause of ADR-0002** and narrows invariant 11
from "no server-side state" to "no server-side state *on the pagination path*." Both documents
would need amending in the same change; leaving them contradicted is not an option.

## Proposal

### 1. A `since_last_seen` mode on `fetch_feed`, keyed by an explicit caller-supplied token

The server stores, per `(caller_token, feed_url)`, a watermark. The token is **supplied by the
caller**, never inferred from transport identity — an MCP stdio session has no stable identity
worth keying on, and inferring one would make behaviour depend on process lifetime.

A watermark is a **set of recently-delivered `item.id`s** with a horizon, not a timestamp.
Timestamps are what already fails: they cannot express "this feed backdated an item" or handle
`published: null`. Bounded at N ids or M days per feed, whichever is smaller.

### 2. The advance is explicit, not implicit

Reading items must **not** mark them seen. A response returns a `delivery_token`; a subsequent
`ack_delivery` call advances the watermark. This is the single most important detail: an
implicit advance re-creates the exact bug class it is meant to remove — the digest run that
fetched 52 stories, crashed before publishing, and had them marked delivered anyway.

### 3. It stays off the pagination path

`since_last_seen` and `cursor` are mutually exclusive, joining the invariant-12
`no_cursor_reason` list. A continuation that consulted a mutable watermark would not be
stateless, and the whole page-fingerprint design (ADR-0017) assumes a caller's arguments
resolve identically across a paged sequence.

## Consequences if accepted

- **A stateful store enters the server** — schema, migration, concurrent-access, and
  garbage-collection concerns that the file cache
  ([ADR-0004](0004-file-based-atomic-cache.md)) deliberately avoided by being pure content
  cache. Two MCP servers against one cache dir now contend on mutable state, not just on
  atomic body writes.
- **`rss-cli` becomes a feed reader by degrees.** The honest reading of ADR-0002 is that read
  state is the *first* subscription feature; scheduled refresh and unread counts are the
  natural next asks, and each will cite this ADR as precedent.
- **The `.seen-posts.json` bug class moves rather than vanishes.** It becomes our bug class,
  in Rust, with tests — which is the point — but "the caller can no longer get this wrong" is
  only true if decision 2 holds. Under an implicit advance we would have adopted the bug.
- **A stale cache is no longer harmless.** Today wiping the cache costs a refetch. With
  watermarks it costs correctness.

## Alternatives considered

- **Do nothing; keep `.seen-posts.json`.** The status quo. Its bug count is the case against
  it, but note that none of those three bugs lost *data* — they suppressed already-fetched
  items, and every one was caught. The mechanism is buggy, not catastrophic.
- **A local corpus the caller appends to** (poll every 2–3h into `worklog/feeds/`, digest reads
  the warm corpus). Keeps all state in the caller, needs **no** server change, and additionally
  fixes the rate-limit burst that motivated
  [ADR-0019](0019-stale-if-error-cache-policy.md) — ingest time becomes the window, so
  "since the last digest" is trivially correct without any watermark. **This is the cheaper
  answer to the same problem and should be tried before this ADR is accepted.**
- **Return `content_hash` and let the caller dedupe.** Already available
  ([ADR-0018](0018-duplicate-reporting-and-keyword-filtering.md)). It solves *duplicate
  detection* but not *delivery tracking* — the caller still has to remember what it showed.
- **Resource subscriptions** (`feed://{url}` + `notifications/resources/updated`). Subsumes
  watermarks *and* the polling task, but is a strictly larger version of the same ADR-0002
  violation — the server would hold both subscription and delivery state. If the boundary is
  going to move, it should move once, deliberately, with that endgame in view rather than
  arriving there by accretion.
