# 19. Serving stale on a refused revalidation (`stale-if-error`)

- **Status:** Accepted
- **Date:** 2026-09-03

## Context

A field report from an agent assembling a daily digest over ~15 Reddit subreddit feeds hit
Reddit's rate limiter and lost **8 of 15 feeds** to `FEED_FETCH_FAILED` / `RATE_LIMITED` in a
single call. The cache held a good copy of every one of them. We refused to hand it over,
because the [ADR-0005](0005-conditional-get-always-revalidate-default.md) state machine treats
a failed revalidation as a failed fetch: no body, `FeedStatus::Error`, items dropped.

That is the wrong trade for a caller who would obviously rather have a 40-minute-old subreddit
than no subreddit. It is also a trade the caller could not make for itself — the alternative,
retrying the failed URLs with `cache_policy: "cache-first"`, needs a second round-trip, and on
a relative window (`since: "24h"`) it silently recovers only the items that predate the
previous run's fetch.

HTTP already has a name for exactly this: `stale-if-error`
([RFC 5861](https://www.rfc-editor.org/rfc/rfc5861#section-4)) — serve a cached copy when the
origin errors. We adopt the name deliberately rather than inventing one, and specifically
**not** `stale-while-revalidate`, which is a different behaviour (serve stale *proactively*
while refreshing in the background) that we are not implementing.

## Decision

### 1. A new opt-in `CachePolicy::StaleIfError`, spelled `stale-if-error`

Serving stale is **opt-in, not the default.** The default stays `Revalidate` and behaves
exactly as before. This is the load-bearing half of the decision, and it is what keeps the
exit-code contract (invariant 5) intact for every existing caller — see Consequences.

### 2. A new `FeedStatus::Stale`, and `SCHEMA_VERSION` goes to `"2"`

`Stale` means: *the origin refused or failed this revalidation, and these items come from the
cached body instead.* It is a third success-ish outcome alongside `Ok` and `NotModified`.

The bump is **conservative**. `Stale` is unreachable unless the caller passes
`cache_policy: "stale-if-error"`, so no existing consumer can encounter the new variant. We
bump anyway because `status` is a closed enum in the generated schema, and a consumer that
validates strictly or matches exhaustively is entitled to treat a new member as breaking.
Under invariant 1 the honest move on any doubt is to bump.

### 3. `error` stays `null` on a stale feed; the reason goes in `warnings[]`

A stale feed carries **real items**, so overloading `error` would be a footgun: the obvious
consumer idiom `if (feed.error) skip(feed)` would discard a perfectly usable feed. `error`
therefore stays `null`, and the refused revalidation is reported as an additive
`Warning { feed_url, code: "SERVED_STALE", message }` naming the upstream status.

This needs **no new `FeedResult` field**. Staleness *age* is already expressible — `cached_at`
and `cache_age_seconds` exist for precisely this judgement (ADR-0012) — so only the *reason*
was missing, and `warnings[]` is already the channel for non-fatal degradation.

### 4. Only a *refused* revalidation goes stale, not a parse failure

`StaleIfError` covers the transport/status half: a `429`, a `403`, a `5xx`, a connect or read
timeout — the cases where our copy is still the best available truth. A body that arrived and
failed to **parse** stays `FeedStatus::Error`: the origin answered, and a feed that now emits
malformed XML is a real problem the caller should see rather than paper over with an old copy.

## Consequences

- **Exit codes are unchanged for every existing caller.** `exit_code_for` counts only
  `FeedStatus::Error`, so a stale feed scores as success and a stale-only batch exits `0`, not
  `4`. That flip is reachable *only* by opting in — which is the point of decision 1. Had we
  made stale-on-error the default, `rss fetch` against a rate-limited feed would have gone from
  exit `4` to exit `0` and silently broken any script using the exit code to detect "could not
  get fresh data." Invariant 5's wording ("they key on feed **errors**") still holds exactly;
  what changed is that a refused revalidation is no longer necessarily an error.
- **A stale feed is deliberately distinguishable three ways** — `status: "stale"`, a
  `SERVED_STALE` warning, and a non-null `cache_age_seconds`. An agent that wants a hard
  freshness floor can enforce it client-side on the last of those; the server does not guess a
  staleness ceiling on the caller's behalf.
- **`from_cache: true` is now ambiguous on its own** between `NotModified` (origin confirmed
  our copy is current) and `Stale` (origin never answered). It always needed `status` to be
  read alongside it; this makes that sharper.
- **A stale page is safe to paginate.** Its success path *is* `Revalidate`, so it writes the
  body to the cache exactly as the default does; only the refusal path skips the write, and
  that path is serving an already-cached body anyway. Either way the snapshot a continuation
  reads is present, so `StaleIfError` does **not** join the `no_cursor_reason` list in
  invariant 12. (Contrast `no-cache`, which is barred precisely because it writes *nothing* —
  "writes nothing" is the disqualifying property, and `stale-if-error` does not have it.)
- **The cache is now load-bearing for availability, not just for cost.** A cache wipe degrades
  a rate-limited batch from "stale but usable" back to "8 of 15 missing."

## Alternatives considered

- **Make it the default.** Rejected on the exit-code regression above: it changes what a
  successful `rss fetch` means for callers who never asked for it. Opt-in costs the digest task
  one argument.
- **Keep `status: "error"` and add a `stale_body_available` flag.** Non-breaking, needs no
  version bump — but it makes the useful case the *awkward* one: the items are right there in
  `items[]` while `status` and `error` both say the feed failed. Every consumer would need
  special-casing to use them, which is most of the work of a new status with none of the
  clarity.
- **Put the 429 in `error` alongside the items.** Rejected: see decision 3. It breaks
  `if (feed.error) skip(feed)`.
- **Serve stale automatically on the retry after a `403`/`429`.** That would fold this into
  [ADR-0015](0015-bounded-retry-on-transient-429-403.md)'s bounded retry and make it
  unconditional — the default-behaviour problem again, with no way to decline it.
