# 3. Deterministic content-hash item ids (the keystone)

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The single most important AI-friendliness property is this: **an agent that references
item X today must still resolve to X tomorrow.** Without a stable identifier, an agent
cannot say "summarize item `abc123`" or "I already processed these ids" across
invocations.

The natural candidate is the feed-provided `<guid>` / Atom `<id>`. In the wild it is
unreliable: a large fraction of feeds (research put it around ~41%) **regenerate their
GUIDs on every fetch** (timestamp-based ids, session tokens in URLs, CMS quirks). So a
guid-first identity scheme would produce *the most* id churn for *exactly* the worst-
behaved feeds — the opposite of the goal. We also cannot lean on the cache for identity:
[ADR-0002](0002-data-on-request-not-a-subscription-manager.md) makes operations cache-
independent, so a fresh cache (or a different machine) must yield the same ids.

## Decision

Each item gets a **deterministic content-hash id**, stable by construction and independent
of both the cache and guid stability. Implemented in [`src/identity.rs`](../../src/identity.rs):

```text
key = first present & non-empty of:  link  →  guid  →  (title + "|" + published)
id  = lowercase_hex( sha256( feed_url + "\n" + key ) )[..16]   # first 8 bytes → 16 hex chars
```

- `id_source` records which field supplied the key: `link` | `guid` | `hash`
  (`hash` is the title/published fallback, including the degenerate all-absent case where
  `key` collapses to the empty string).
- The raw feed `guid` is still emitted on every item for reference — it is simply **not**
  trusted as the identity basis unless it is the only key available.
- The id is **namespaced by `feed_url`**: the same article fetched via a mirror, or via an
  `http` vs `https` URL, yields a *different* id. This is the correct trade-off — identity
  is scoped to "this item as delivered by this feed," which is what `rss show <feed> --id`
  resolves against.
- The construction is pinned by a known-answer test (`item_id` of feed + `…/a` →
  `1b9107de952289cb`) so the exact byte layout (single `\n` separator, no trailing
  newline, hex-string truncation, lowercase) can never drift silently.

## Consequences

- Ids are identical across runs, machines, and a cold cache — by construction, with no
  state required. This is what makes `rss show … --id <id>` and the MCP `get_item` tool
  reliable.
- Feeds with stable links (the common case) get `id_source: "link"`; the churny-guid feeds
  that motivated this still get a stable id from their link rather than their bad guid.
- A feed that *edits* an item's link (changes its slug) will produce a new id for that
  item — we accept this as rare and prefer it to trusting unstable guids. A cache-assisted
  old→new id map was considered as later hardening and deliberately left out of v1.
- Truncation to 64 bits is for ergonomics (short, copy-pasteable ids). Collision risk is
  negligible at feed scale; ids are namespaced per feed, which shrinks the space each id
  must be unique within to a single feed's items.

## Alternatives considered

- **guid-first identity.** Rejected: reintroduces id churn on the ~41% of feeds that
  regenerate guids, defeating the entire purpose.
- **Cache-assigned sequential / random ids.** Rejected: violates cache-independence and
  cross-machine stability ([ADR-0002](0002-data-on-request-not-a-subscription-manager.md));
  a cold cache would renumber everything.
- **Full 256-bit / 32-hex id.** Rejected for ergonomics; 64 bits is ample given per-feed
  namespacing.
