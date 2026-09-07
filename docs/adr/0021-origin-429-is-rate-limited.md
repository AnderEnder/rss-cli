# 21. An origin `429` is `RATE_LIMITED`, not `FEED_FETCH_FAILED`

- **Status:** Accepted
- **Date:** 2026-09-08

## Context

[ADR-0015](0015-bounded-retry-on-transient-429-403.md) established the bounded single retry on
a transient `403`/`429` and specified that when the second attempt also fails, the result is a
`FEED_FETCH_FAILED` error carrying `http_status` and the raw `retry_after`.
[ADR-0016](0016-per-host-request-gate.md) later added a **distinct `RATE_LIMITED` code** for
the gate's own fail-fast, expressly "so an agent can branch on it and pace instead of
hand-rolling delays."

Together those two decisions put the codes on the wrong cases. The **common** event — the
origin itself answering `429` — got the opaque code, while the **rare** one — our own pacing
ceiling, which ADR-0016 notes is "rare at default settings" — got the paceable one.

A digest agent assembling a daily roundup over Reddit feeds is the failure that surfaced it.
`FEED_FETCH_FAILED` reads as "this source is broken," so the agent dropped throttled
subreddits from the digest permanently rather than retrying them later. Nothing in the
response distinguished "wait 40 seconds" from "this feed is dead."

Two details made it worse:

- **The pacing hint was empty exactly when it mattered.** ADR-0016's own probe found the
  provider sends **no `Retry-After` header at all**, so `details.retry_after` — the raw header
  — is `null` on precisely the hosts that throttle hardest. The agent was told to back off with
  no indication of how long.
- **The information already existed.** The gate computes a cooldown for every throttle,
  honoring `Retry-After` where a server sends one and otherwise escalating
  `base · 2^(n-1)` toward `HOST_MAX_COOLDOWN` (ADR-0016). It was simply never surfaced.

## Decision

**1. Status `429` yields `RATE_LIMITED`.** `RssError::Http { status: 429, .. }` maps to the
same paceable code the gate's fail-fast uses. One code now means "wait, then retry," whichever
side refused.

**2. Every other status keeps `FEED_FETCH_FAILED` — notably `403`.** ADR-0015 retries both
`403` and `429`, but they are not the same thing on failure: a `403` is a *block*, not a
*window*, and waiting it out is the wrong response. The asymmetry is deliberate and looks like
an inconsistency worth "simplifying"; it is not.

**3. The wait comes from the gate, not from re-parsing headers.**
`HostGate::cooldown_remaining` reports the learned window. It already honors the server's
`Retry-After` where one was sent and otherwise supplies a bounded escalation, so it is the best
available estimate and introduces no per-provider constant to drift. It reports *pacing* only,
not permit contention — it is not an end-to-end time-to-send.

**4. Both pacing keys are emitted, so one code means one shape.** The origin variant carries
`details.retry_after_seconds` *and* `details.retry_after_ms`, matching the gate's own
`RATE_LIMITED`; a client must never have to know which side refused in order to find the wait.
An origin `429` additionally keeps `details.http_status` and the raw `details.retry_after`.

**5. This supersedes one clause of ADR-0015, and nothing else.** Specifically the final
sentence of its Decision, naming `FEED_FETCH_FAILED` as the persistent-failure code — and only
for `429`. ADR-0015's retry policy itself (exactly one attempt, waiting
`min(Retry-After, 5 s)`) is unchanged and still in force.

**6. Runtime and error-code only.** No serialized struct changed. Which `code()` a status
yields is behaviour, not schema, so `SCHEMA_VERSION` stays `"2"`.

### A related record correction

ADR-0017's first *Known limitation* — "the batch deadline bounds when a fetch may *start*, not
permit contention," with the `timeout_at`-the-semaphore fix "deliberately deferred" — was
**closed** by the batch-deadline work in `6af13c7`, which threaded the deadline into
`HostGate::acquire_until`. The change accompanying this ADR extends that bound to the
single-URL tool paths (`discover_feeds`, `get_item`) and to `fetch_one`. Treat that limitation
as closed rather than outstanding; it is not a gap awaiting work.

## Consequences

- An agent has exactly one code to branch on for throttling, and it arrives with a number.
  Sources degrade to "retry later" instead of vanishing from a digest.
- **This is a behaviour change to a documented contract.** A caller that matched
  `FEED_FETCH_FAILED` to detect throttling must now also handle `RATE_LIMITED`. Error codes are
  stable strings by design, so this is the kind of change that needs recording — hence this ADR.
- Every documentation surface has to agree, and one already drifted: the shipped
  `SERVER_INSTRUCTIONS` still described an origin `429` as `FEED_FETCH_FAILED` after the code
  changed, teaching clients to branch on a code the server no longer sends. That is worse than
  documenting nothing, and no test caught it. It is now pinned by
  `mcp::tests::the_shipped_guidance_names_the_code_a_429_actually_gets`, which derives the
  expected code from `RssError::code()` rather than restating it.
- `RssError::Http` gained a `retry_after_hint` field. That is source-breaking for downstream
  code constructing or exhaustively matching the variant — accepted as an ordinary minor change
  at `0.x`, consistent with ADR-0016 having shipped the `RATE_LIMITED` variant itself as
  "additive in `error.rs`."
- A non-`429` failure that happens to arrive while the host is in cooldown also carries the
  hint. This is intentional and truthful — the gate really will hold the next send that long —
  but it means the hint's presence is a function of gate state, not of the status.

## Alternatives considered

- **Keep `FEED_FETCH_FAILED` and add `retryable: true` to `details`.** A smaller change that
  breaks no matcher. Rejected: agents branch on the code first, and the whole failure here was
  a *missed signal* — burying the distinction one level deeper in a free-form object invites
  exactly the same miss.
- **Route an origin `429` through the existing `RateLimited` variant.** Avoids adding a field.
  Rejected on three counts: it loses `details.http_status`, which both the README and the
  shipped MCP guidance promise for an origin `429`; restoring it means adding a field to
  `RateLimited` instead, which is the same enum change moved sideways; and it drops `"429"`
  from the error's `Display`, which the `SERVED_STALE` warning quotes verbatim (ADR-0019).
- **Parse `x-ratelimit-reset` and add a per-reddit.com default.** Rejected: the gate already
  computes a better number from data it observes directly, and a per-provider constant is one
  more thing to fall out of date silently.
- **Include `403` in the reclassification.** Rejected — see Decision 2. A block is not a
  window, and telling an agent to wait out a `403` produces a polite infinite loop.
