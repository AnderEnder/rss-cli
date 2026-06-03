# 14. get_item cache-first read + multi-key (id/guid/url) lookup

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

`get_item` (and its CLI twin `rss show`) retrieves a single item by re-fetching the live
feed and scanning it for a match. Two problems surfaced in the round-2 MCP field report
(2026-06-03), both rooted in that re-fetch-and-match design.

First, the lookup re-fetched the feed under the default revalidate policy and matched on
`id` **only**. Feeds with a sliding window — Reddit listings are the canonical case — drop
older entries as new ones arrive. So an agent that saw an item in one `fetch_feed` call and
then asked for its full body a moment later could get `NOT_FOUND`: the item it had just seen
had already rolled out of the live window, even though it was still sitting in our cache from
that very fetch.

Second, the `id` itself confused callers. Item ids are deterministic content hashes
**namespaced by `feed_url`** by construction ([ADR-0003](0003-deterministic-content-hash-item-ids.md)):
the same underlying post fetched from two different feed URLs (e.g. Reddit's `?t=day` vs
`?t=week`) yields two different ids. A caller who fetched a post one way and tried to
`get_item` it via the id it remembered from the other way got nothing. This is working as
designed — the id is feed-URL-local on purpose — but it reads as "ids are unstable" to a
caller who does not know the namespacing rule.

## Decision

Three coordinated changes, all landing in the shared core so `rss show` and the MCP
`get_item` tool stay in lock-step (invariant #6):

(a) **A new `CachePolicy::CacheFirst`.** When a cache entry exists for the feed, serve its
cached body directly — *regardless of age* — with no network call; only on a cache miss does
it fetch (then behave like a normal revalidating GET). Both front-ends use `CacheFirst` for
item lookup. This is what lets an item the caller just saw survive a rolled feed window: the
read comes from the cache, not the (now-different) live feed.

(b) **`core::show_item` matches on `id` OR raw `guid` OR resolved `url`.** The lookup key may
be the stable `id`, the item's raw `guid` (e.g. Reddit's `t3_…` / `t1_…`), or its
resolved permalink `url`. Whichever a caller happens to be holding now resolves the item.

(c) **`item_id` is unchanged.** The hash and its byte layout are frozen — invariant #4, and
the `identity.rs` known-answer test stays pinned at `1b9107de952289cb` / `a86aced5664c7742`.
We do **not** chase cross-feed-url stability by changing the hash. Instead, callers who need
a key that is stable across feed URLs key on the `guid`: a Reddit `t3_…` identifies the
post independently of which feed window or feed URL surfaced it, so it is the portable key
the id deliberately is not.

A cache-first read is an explicit exception to the always-revalidate default of
[ADR-0005](0005-conditional-get-always-revalidate-default.md) — which is precisely why it
gets its own ADR before the code lands. All of this is additive; no serialized struct
changes, so `SCHEMA_VERSION` stays `"1"`.

## Consequences

- The reporter's exact failure — "fetch a feed, then ask for an item's full body shortly
  after" — now succeeds even when the live window has already rolled, because the body is
  served from the cache that the prior fetch populated.
- It does **not** survive a *later* refetch of the *same* `feed_url` that overwrote the cache
  with a rolled window. The cache holds one body per feed URL; once that body is replaced,
  the earlier item is gone. We call this the **feed-window constraint** and document it
  plainly rather than over-promise. Only a per-item body store would close this "much later"
  gap, and we deliberately did not build one.
- The lookup still requires the caller to pass the **`feed_url` that contains the item**. The
  cache is keyed by feed URL, and guid/url matching searches *that* feed's cached body — it
  is not a global item index.
- There is no per-item store and no new on-disk format; `CacheFirst` reuses the existing
  feed-body cache ([ADR-0004](0004-file-based-atomic-cache.md)).
- Additive only: no struct change, so `SCHEMA_VERSION` stays `"1"`.

## Alternatives considered

- **A per-item body cache.** This is the only design that would close the "much later" gap —
  surviving a refetch that overwrote the feed body. Rejected for v1: it adds a second cache
  surface with its own eviction policy and key space, for a case the cache-first read already
  covers in the common "shortly after" path. It can be added later without a contract change
  if the gap proves painful in practice.
- **Reusing `CachePolicy::MaxAge`.** Rejected. `MaxAge` serves the cache only while the entry
  is within its freshness window and revalidates once it is older — which re-introduces the
  very eviction we are trying to avoid. An item that has aged past the window would trigger a
  live refetch and could vanish from the result, defeating the purpose. `CacheFirst` ignores
  age entirely, which is the whole point.
- **A guid-based `item_id`.** Rejected on two grounds. It violates invariant #4 (the id byte
  layout is frozen), and it reintroduces the id churn that [ADR-0003](0003-deterministic-content-hash-item-ids.md)
  exists to prevent: roughly 41% of feeds regenerate their guids, so a guid-derived id would
  be unstable on a large fraction of real feeds. We keep the deterministic content-hash id
  and let callers key on `guid` *as a lookup key* when they need feed-window independence —
  without making the id itself depend on the guid.
