# 5. Conditional GET with an always-revalidate default

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

Given a cache ([ADR-0004](0004-file-based-atomic-cache.md)), we must choose a default
freshness policy. A TTL ("serve from cache for N minutes without checking the network")
is fast but **unpredictable for an agent**: whether you get fresh data depends on the
invisible age of a local file. For a tool whose value is predictability, surprising
staleness is worse than a cheap network round-trip.

## Decision

The default policy is **always revalidate** via a conditional GET. Implemented in
[`src/fetch.rs`](../../src/fetch.rs) as the `CachePolicy` enum:

- **`Revalidate` (default):** every fetch sends `If-None-Match` (from the cached `ETag`)
  and `If-Modified-Since` (from the cached `Last-Modified`). A `304 Not Modified` serves
  the cached body with `status: "not_modified"`, `from_cache: true`, and refreshes only
  `fetched_at`. A `200` stores the new body + validators and returns fresh content.
- **`MaxAge(d)` (opt-in, `--max-age`):** if a cache entry is younger than `d`, return it
  with **no network call at all**; otherwise fall through to revalidate. This is the
  explicit "trade freshness for speed" knob.
- **`NoCache` (`--no-cache`):** a plain GET that neither reads nor writes the cache.
- `--refresh` forces revalidation even when `--max-age` would otherwise serve from cache.

So back-to-back default calls *do* touch the network — but with a single cheap conditional
request that usually returns an empty `304`.

## Consequences

- **Predictable by default:** an agent gets current data every call, while still saving
  bandwidth (the body only transfers on real change) and being polite to origin servers.
- The speed-over-freshness path (`--max-age`) exists but is opt-in and obvious in the
  output (`from_cache: true`, `not_modified: true`), so staleness is never silent.
- A server that returns `304` with no cache entry present is treated as an error (it
  should never happen) rather than silently returning an empty body.
- `status: "not_modified"` is surfaced as a first-class outcome in the output contract
  ([ADR-0006](0006-ai-facing-output-contract.md)) so callers can cheaply detect "nothing
  changed."

## Alternatives considered

- **TTL-by-default (serve from cache for N minutes).** Rejected as the default for
  unpredictable staleness; kept as the opt-in `--max-age` behavior.
- **Honoring `Cache-Control: max-age` from the response.** Out of scope for v1; the
  conditional-GET default already covers the common case, and an explicit `--max-age`
  gives the caller direct control without parsing/obeying origin cache directives.
