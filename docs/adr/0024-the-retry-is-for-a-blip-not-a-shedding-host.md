# 24. The single retry is for a blip, not for a host that is already shedding

- **Status:** Accepted. Narrows ADR-0015's "exactly once" — see Consequences.
- **Date:** 2026-09-11

## Context

[ADR-0015](0015-bounded-retry-on-transient-429-403.md) retries a `403`/`429` once, waiting
`min(Retry-After, 5s)` or a `500 ms` base delay when the origin sent no header. ADR-0016 then
added a gate that *learns* a per-host cooldown, and
[ADR-0023](0023-cooldown-escalation-decays-rather-than-resets.md) made that cooldown escalate
across consecutive throttles. The two never got reconciled: `gated_send` notes the throttle —
recording "this host needs 2–16 s" — and then re-asks it 500 ms later.

Measured 2026-09-11, three subreddit `.rss` feeds in one batch against a host that sends no
`Retry-After` (the ADR-0016 provider):

- five requests for three feeds, two of them refused on arrival and with no chance of
  succeeding;
- reported `details.retry_after_seconds` of **4 s** and **16 s** where a single climb each
  would have said 2 s and 4 s — every doomed retry calls `note_throttled` again, so the ladder
  advances twice per feed;
- raising the batch deadline, the gate wait *and* the max cooldown together changed nothing:
  the loss was never the deadline (`truncation` stayed `null`), it was the origin refusing.

Separately measured: this host refuses roughly one request in three even at 30 s spacing, so no
in-call wait we would accept makes a retry likely to land.

## Decision

Spend the retry only when there is reason to believe the refusal was transient:

1. The origin sent `Retry-After` → honor it, clamped to `RETRY_MAX_DELAY`, as before. The
   origin stated when to come back; that is better evidence than anything we infer.
2. Else, if the host was already **warm** — it threw a throttle inside ADR-0023's warm window
   — **do not retry.** Hand the refusal back, carrying the gate's learned cooldown as the
   pacing hint.
3. Else → the `500 ms` base delay, exactly as ADR-0015 specified.

Warmth, not `cooldown_remaining`, is the predicate: a caller that waited a cooldown out has
consumed it, so `next_allowed` says nothing about whether the host is still on a streak.
`HostGate::is_warm` exists for this and is read *before* `note_throttled`, which would
otherwise mark the host warm by itself.

## Consequences

- One fewer request per subsequent same-host feed, against exactly the hosts that are asking
  for fewer requests.
- The escalation ladder advances once per feed instead of twice, so `retry_after_seconds`
  describes what one refusal taught us rather than two.
- A feed that would have been rescued by the second attempt now fails — but only on a host
  already throttling inside the warm window, where the measurements put that rescue at
  roughly one in three, paid for by every sibling's gate wait.
- ADR-0015's guarantee narrows: "retries exactly once" becomes "retries exactly once, unless
  the host is already shedding". The cold-host blip case it was written for is untouched, and
  its test still passes unchanged.
- Pinned by `fetch::tests::a_second_throttle_on_a_warm_host_does_not_spend_the_retry`, which
  asserts the request *count* (the resulting error looks identical either way), and by
  `fetch::tests::retry_delay_skips_a_warm_host_but_always_honors_retry_after`.

## Alternatives considered

- **Wait the learned cooldown instead of 500 ms, still inside the retry.** Rejected: the
  in-flight retry holds the host permit, so stretching it blocks every sibling behind one
  request — the merge of two deliberately separate bounds that ADR-0016 warns against.
- **Drop the retry for any headerless `403`/`429`.** Rejected: it breaks the case ADR-0015 was
  written for. The field report's Reddit `403`s cleared on the very next call, and that host is
  *cold* the first time it refuses.
- **Raise the base cooldown above `RETRY_MAX_DELAY` so the existing bound skips the retry.**
  Rejected as a side effect masquerading as a decision: it changes every sibling's wait to
  express something about one retry.
- **Keep retrying and raise the batch deadline.** Rejected on measurement — bounds we control
  were not what was binding.
