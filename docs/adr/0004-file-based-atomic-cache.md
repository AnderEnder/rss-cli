# 4. File-based atomic cache, not an embedded DB

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The tool needs a small local cache for two jobs: (a) storing HTTP validators (`ETag`,
`Last-Modified`) and bodies so refetches can be conditional GETs
([ADR-0005](0005-conditional-get-always-revalidate-default.md)), and (b) holding the last
body so `rss show` / the MCP `get_item` can resolve an item without a forced network hit.
It must be lightweight (the project's defining adjective), have no C/build-time
dependency, and ideally be inspectable by a curious human or agent.

## Decision

A **file-based cache**, implemented in [`src/cache.rs`](../../src/cache.rs). Each feed is
two files under the OS cache dir (via the `directories` crate, overridable with
`--cache-dir`), keyed by `sha256(feed_url)`:

- `<hash>.json` — `CacheMeta` (feed_url, etag, last_modified, fetched_at, content_type).
- `<hash>.body` — the raw response bytes.

Writes are **atomic**: write to a uniquely-named temp file in the same directory, then
`rename` over the target (atomic within a filesystem), so a crash or concurrent run can
never observe a torn entry. There is no index file to keep consistent; `cache list` simply
scans `*.json`.

## Consequences

- Zero non-Rust dependencies; nothing to migrate; trivial to reason about and to delete
  (`rss cache clear`, or just `rm` the dir).
- Human/agent-inspectable: the metadata is plain JSON and the body is the literal feed.
- The cache is explicitly **not** responsible for id stability — that is by construction
  ([ADR-0003](0003-deterministic-content-hash-item-ids.md)). Its only jobs are conditional
  GET and `show`/`get_item` lookups.
- No global lock: two concurrent processes hitting the same feed may both write, but the
  atomic rename means each write is all-or-nothing and last-writer-wins is harmless (both
  bodies are valid). This is acceptable for a CLI; it is not a transactional store.

## Alternatives considered

- **`redb` (pure-Rust embedded KV) or `sqlite`.** Rejected for v1: transactional integrity
  is overkill for a best-effort HTTP cache, and both add weight / a less inspectable store.
  `redb` is noted as the upgrade path if a future feature genuinely needs transactions.
- **In-memory only (no persistence).** Rejected: defeats conditional GET across
  invocations, which is the main politeness/bandwidth win.
