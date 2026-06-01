# 2. Data-on-request, not a subscription manager

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The obvious shape for an RSS tool is a feed *reader*: a subscription list you `add`/
`list`/`remove`, persisted per-feed read/unread/star state, and a background sync. That is
what most RSS CLIs are. But the primary consumer here is an **AI agent** (humans
secondary), and an agent already has its own notion of which feeds matter and what it has
already seen — it carries that state in its own context or store. A tool that *also* keeps
hidden, mutable, per-machine state (a subscription DB, a read cursor) becomes
non-idempotent and non-portable: the same command produces different output depending on
invisible local history, which is exactly what makes a tool hard for an agent to reason
about.

## Decision

`rss-cli` is **data-on-request**, not a subscription manager:

- No `add` / `list` / `remove` of feeds; no stored subscription list.
- No persisted read / unread / starred state.
- The caller supplies feed URLs on every invocation (positional args, `--input`,
  `--opml`, or stdin). The tool fetches, parses, and emits — then forgets.
- The **only** local state is an HTTP cache (see
  [ADR-0004](0004-file-based-atomic-cache.md)), and it is a pure performance/politeness
  optimization: deleting it changes latency and bandwidth, never the *data* a command
  produces.

Operations are therefore **idempotent and deterministic**: same inputs → same output,
regardless of what ran before or on which machine.

## Consequences

- Trivial to script and to call as an MCP tool; no setup, no migration, no state to
  corrupt.
- Output is reproducible and cache-independent — a property the stable-id design
  ([ADR-0003](0003-deterministic-content-hash-item-ids.md)) depends on.
- Users who want a persistent reading list must keep the feed list themselves (an OPML
  file works well with `--opml`). That is a deliberate non-goal, not an oversight.
- Cross-invocation conveniences that *would* need state (e.g. "only show me items since I
  last checked") are out of scope for v1; an agent implements them on top using the stable
  ids and `--since`.

## Alternatives considered

- **Full subscription manager with a feed DB and read state.** Rejected: re-introduces
  hidden mutable state, breaks idempotency/portability, and duplicates state the calling
  agent already owns. It is also a much larger surface for v1.
