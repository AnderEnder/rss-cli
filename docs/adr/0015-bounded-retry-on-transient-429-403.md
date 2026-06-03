# 15. Bounded single retry on transient 429/403 (honor Retry-After)

- **Status:** Accepted
- **Date:** 2026-06-03

## Context

The round-2 MCP field report (2026-06-03) showed Reddit returning intermittent `403`
responses mid-batch — its rate limiter, not a genuine authorization failure. The same URL,
fetched again a moment later, succeeds. With a single-shot fetch there is no way for a
caller to tell a transient rate-limit blip from a permanent failure: both surface as the
same error, and a batch fetch loses items to what is really just back-pressure. `429` (the
explicit "too many requests" status) has the same shape and the same fix.

## Decision

Retry **exactly once** on a `403` or `429` response. Before the retry, wait
`min(Retry-After, 5s)` when the server sent a `Retry-After` header, or a `500 ms` base delay
when it did not. If the second attempt still fails, the resulting `FEED_FETCH_FAILED` error
carries the `http_status` and — when the server sent one — the raw `retry_after` value in
its `details`.

The retry is deliberately **bounded to a single attempt**. One retry is enough to absorb a
transient blip while staying polite: it never turns into a loop that hammers a struggling
provider, and it never masks a persistent outage behind escalating waits — a real failure
still surfaces promptly, now annotated with the status the caller needs to reason about it.

## Consequences

- Transient rate-limit blips self-heal: a `403`/`429` that clears on the next call no longer
  costs the caller an item in a batch fetch.
- Latency is added only when a request actually hits a `403`/`429` — at most one bounded wait
  (sub-second without a `Retry-After` header, capped at 5 s with one).
- The error envelope gains an additive `retry_after` detail alongside the existing
  `http_status`. This needs no schema bump: an error's `details` is a free-form
  `serde_json::Value`, not part of the generated output contract, so `SCHEMA_VERSION` stays
  `"1"`.

## Alternatives considered

- **Exponential, multi-attempt backoff.** Rejected. It is unbounded in spirit, impolite to a
  provider that is already signalling back-pressure, and it would mask a persistent outage
  behind a growing wait — exactly the opposite of surfacing a real failure promptly. A single
  bounded retry captures essentially all the benefit (transient blips clear on the very next
  call) at none of the cost.
- **No retry at all.** Rejected. The field report shows that for Reddit this materially hurts
  batch fetches — items are lost to back-pressure that a single retry would have ridden out.
- **Honoring the HTTP-date form of `Retry-After`.** Deferred. `Retry-After` may be either
  delta-seconds or an HTTP-date; Reddit sends delta-seconds. We parse only the delta-seconds
  form and ignore the date form rather than risk computing a wrong sleep from a misparsed or
  clock-skewed date. The raw header value is still surfaced in the error `details` regardless
  of form, so nothing is silently dropped.
