# 23. Cooldown escalation should decay, not reset

- **Status:** Proposed — **not implemented.**
- **Date:** 2026-09-09

## Context

A field report observed `details.retry_after_ms` values of `16000`, `4000`, `4000` and `60000`
in a single run against one host, and asked whether the learned cooldown was converging on the
wrong number under concurrent pressure — a `4000` handed out against a host in a 60s-class
limit burns the caller's retry.

Two mechanisms were ruled out by reading the code, and both are worth recording because they
are the natural first guesses:

- **It is not per-feed.** `ratelimit::authority_of` keys the gate on `host:port`, so every
  subreddit shares one `www.reddit.com:443` slot. There is one number per host, never one per
  feed.
- **A cache hit is not feeding the learner.** `note_success` is reachable *only* from inside
  `HttpClient::gated_send`. A `CacheFirst` or within-`max-age` hit never enters it, so it
  cannot report headroom the origin never granted.

**Nor is the value a decaying remainder.** `cooldown_remaining` returns
`next_allowed_ms - now_ms()`, which invites reading `4000` as "16s window, 12s elapsed." The
arithmetic rules that out: `cooldown_for(n, None) = base · 2^(n-1)` capped at `max_cooldown`
yields exactly `{2000, 4000, 8000, 16000, 32000, 60000}`, and all three observed values land on
that ladder. A decayed remainder would read `3847`, not `4000`. These are fresh cooldowns read
within a millisecond of `note_throttled` writing them — so the counter genuinely stood at
`n = 4`, then `n = 2`, then `n >= 6`.

### The actual cause

`note_success` **hard-stores `0`** into `consecutive_throttles`, while `warm_until_ms` still
holds up to `warm_window` (120 s) of "this host was recently throttled." With `per_host = 1`
and ~15 same-host feeds, any interleaved non-throttled response wipes the learned depth, so the
ladder sawtooths: a `16000` is followed by a `4000` toward a host whose real limit has not
moved. The gate is discarding information it is still separately tracking.

The reported conclusion was right and the reported mechanism was not, which matters because it
changes the fix: nothing is wrong with the escalation curve or with per-host keying.

## Proposal

**Decay the counter instead of resetting it:** `saturating_sub(1)` in place of `store(0)`, so
one success steps the ladder down one rung rather than to the floor. `warm_until_ms` already
encodes "recently throttled" and is left to expire on its own, so the two signals stay
consistent instead of contradicting each other.

Two smaller points in the same function, worth deciding together:

- `is_retryable` is `403`/`429` only, so `note_success` currently also fires on a `404` or a
  `500`. Truthful under its own doc comment ("a non-throttled response") but it means an
  origin *outage* resets throttle learning.
- `cooldown_remaining` is documented as pacing-only, "not an end-to-end time-to-send"
  ([ADR-0021](0021-origin-429-is-rate-limited.md) §3). That is correct and should stay. The
  gap is that a client cannot tell the shipped number is the *gate's* window rather than an
  estimate of the origin's limit — worth a wording pass on the shipped MCP guidance, not a
  behaviour change.

## Consequences if accepted

- A host under sustained concurrent pressure holds its learned depth, so `retry_after_ms`
  stops understating. A caller's single bounded retry lands after the window instead of inside
  it.
- **Slower recovery.** A host that genuinely recovers takes `n` successes to walk back to the
  base cooldown rather than one, so the first post-recovery throttle waits longer than it
  needs to. This is the real cost and it is the mirror of the current bug; `warm_window` bounds
  how long it can matter.
- No serialized struct and no error code changes — runtime only, so `SCHEMA_VERSION` stays
  `"2"`. Same classification as ADR-0021 §6.
- `ratelimit::tests::note_success_resets_escalation_counter` asserts the current hard reset and
  would need to change with it — it pins the behaviour this ADR proposes to alter, which is
  what a pin is for.

## Alternatives considered

- **Do nothing.** The number is truthful about `next_allowed_ms` at emission, and ADR-0021
  already disclaims it as an end-to-end estimate. Rejected because "truthful but reliably too
  small" is the failure ADR-0021 set out to fix — an agent given a number it can act on should
  not have to discount it.
- **Suppress the reset only while the host is warm.** Equivalent in the common case and closer
  to the intent, but it makes recovery depend on two pieces of state instead of one, and
  `saturating_sub(1)` already degrades gracefully.
- **Report an end-to-end time-to-send** (pacing + permit queue). Rejected: ADR-0021 §3 keeps
  those apart deliberately, and the queue depth is not knowable without holding the slot-map
  lock across the estimate — which the gate must never do.
- **Parse provider-specific rate-limit headers.** Already rejected by ADR-0021; the gate's
  observed number needs no per-provider constant to drift.
