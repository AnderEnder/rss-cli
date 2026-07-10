# 16. Per-host request gate: shared client + adaptive cooldown for concurrent fetches

- **Status:** Proposed
- **Date:** 2026-07-10

## Context

Field reports show that some feed providers rate-limit **per-domain** and aggressively: a
caller pulling several feeds from the same host gets `HTTP 429`, while feeds from *different*
hosts fetched at the same time are unaffected. Callers work around it by serializing requests
with manual delays, but on heavy days even sequential requests get throttled, and coverage
suffers (feeds dropped, follow-up item fetches skipped).

The root cause is structural, not a bug in one call:

- The MCP `fetch_feed` tool takes a **single** URL and builds a **fresh** `HttpClient` per
  call (`fetch_feed_inner` → `core::fetch_feeds` → `HttpClient::new`). `RssServer` shares
  only the `Cache` across tool calls — no HTTP client, no rate-limiter state.
- So the 429 burst comes from the **client firing several concurrent `fetch_feed` tool
  calls**, each independently hitting the same host. There is **no shared state across
  concurrent tool calls** to coordinate pacing. Nothing local to one call can fix it.
- The CLI `rss fetch` of many URLs has the same gap: `buffer_unordered(8)` fans out with no
  per-host limit.
- The existing mitigation ([ADR-0015](0015-bounded-retry-on-transient-429-403.md)) is a
  single bounded retry *within one request*. It cannot coordinate across concurrent
  requests, and its wait is capped at 5 s.

**Empirical probe (2026-07-10, against a provider known to throttle aggressively):**

- The provider sent **no `Retry-After` header** on its 429s — so ADR-0015's Retry-After
  honoring is moot for such hosts; a cooldown must supply its own default.
- **Even serial, back-to-back requests got 429'd after the first**, and several seconds of
  spacing still tripped it: roughly **one success per ~20–30 s per client IP**. This
  confirms the field reports of "even sequential requests were throttled": per-host
  *concurrency=1 alone is not sufficient*; genuine inter-request pacing is needed.
- No client-side design can exceed a provider's IP-level limit. The achievable win is (a)
  stop *self-inflicted* bursts, (b) make throttling **graceful** (a structured, paceable
  signal instead of an opaque 429/timeout), and (c) raise the provider's ceiling where
  cheaply possible (a descriptive User-Agent — generic/placeholder UAs are commonly throttled
  into a smaller shared bucket).

## Decision

Introduce a process-wide **per-host request gate** in a new `src/ratelimit.rs`, owned by a
**reused** `HttpClient` that the long-lived `rss mcp` server builds **once** and shares
across all tool calls. This is complementary to ADR-0015, not a reversal: **ADR-0015 is a
per-request retry *reaction*; ADR-0016 is cross-request *scheduling*.** The two waits are one
budget, never stacked (see below).

**Gate mechanism.** Keyed on authority (`host:port`; derived with the already-present `url`
crate). Each host slot holds a `tokio::sync::Semaphore` (concurrency cap) and atomic
epoch-millisecond deadlines (`next_allowed`, `warm_until`) plus a consecutive-throttle
counter. A request:

1. Acquires the per-host permit (`acquire_owned`, so a cancelled/timed-out call releases it
   via RAII — a cancel can never wedge a host).
2. Computes `wait = max(next_allowed − now, 0)`. If `wait ≤ MAX_GATE_WAIT`, it sleeps `wait`
   then sends; if `wait > MAX_GATE_WAIT`, it **fails fast** with a structured
   `RATE_LIMITED` error carrying `retry_after` rather than blocking unboundedly.
3. While a host is **warm** (recently throttled), reserves `next_allowed = now + STICKY_SPACING`
   for the following sibling — proactive spacing that costs the happy path **nothing**
   (a host that has never 429'd is never warm).
4. On a `403`/`429` response, sets the sibling cooldown lock-free:
   `next_allowed = max(next_allowed, now + cooldown)` and marks the host warm.

**Cooldown value.** When the server sent a `Retry-After` (delta-seconds *or* HTTP-date), the
cooldown is that value capped at `HOST_MAX_COOLDOWN` (honored as-is below that — no lower
floor, since sticky spacing already prevents immediately re-hammering the host). When it sent
none, the cooldown **escalates** with consecutive throttles on that host
(`HOST_BASE_COOLDOWN · 2^(n−1)`, capped at `HOST_MAX_COOLDOWN`) and resets on the next
success. Escalation lets the gate *learn* a headerless provider's window in a few tries
instead of re-probing at a fixed, too-short interval — chosen deliberately because the probe
showed a real window an order of magnitude larger than any safe fixed default, and because
the "server absorbs the wait" behavior below makes a longer learned cooldown acceptable.

The gate wraps **only the real network send** — cache hits and `304` short-circuits return
before it and gain zero latency. The current request keeps ADR-0015's single retry unchanged
and holds its permit through it, so it never waits on the cooldown *it* just set (no
double-wait); the retry budget stays exactly two attempts.

**Behavior on a long throttle — the server absorbs it.** A slow-but-complete fetch is
preferred over a dropped feed, so a gated call **blocks** up to `MAX_GATE_WAIT` (default 60 s)
waiting the cooldown out, and only sheds to a `RATE_LIMITED` error past that hard bound.
`MAX_GATE_WAIT` is env-tunable (`RSS_MAX_GATE_WAIT_SECS`) so a client with a tighter tool-call
timeout can lower it.

**Constants (all env-tunable; runtime-only, so `SCHEMA_VERSION` stays `"1"`):**

| Constant | Default | Env | Role |
|---|---|---|---|
| `HOST_MAX_CONCURRENCY` | `1` | `RSS_HOST_CONCURRENCY` | serialize same-host requests |
| `HOST_BASE_COOLDOWN` | `2 s` | — | first headerless cooldown; escalation base |
| `HOST_MAX_COOLDOWN` | `60 s` | `RSS_MAX_COOLDOWN_SECS` | cap on any single cooldown |
| `STICKY_SPACING` | `1 s` | — | inter-request spacing once a host is warm |
| `WARM_WINDOW` | `120 s` | — | how long sticky spacing stays active after a throttle |
| `MAX_GATE_WAIT` | `60 s` | `RSS_MAX_GATE_WAIT_SECS` | max block before fail-fast |
| `RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` | `500 ms` / `5 s` | — | **unchanged** ADR-0015 in-flight retry |

Note the two caps are deliberately **distinct**: the in-flight `RETRY_MAX_DELAY` stays 5 s
(the retrying request holds its permit, so a 60 s value there would block siblings), while
the new `HOST_MAX_COOLDOWN`/`MAX_GATE_WAIT` govern the sibling-facing wait.

**Wiring (additive, honors invariant #6).** `core::fetch_feeds` splits into a thin wrapper
(builds the client, keeps today's signature and every call site) plus a new
`fetch_feeds_with(urls, params, cache, &HttpClient)`. `RssServer` gains an `http: HttpClient`
field built once and passes `&self.http` into `fetch_feed_inner`. All pacing lives in
`fetch`/`ratelimit` — the shared path both front-ends traverse — never in `mcp.rs`.

**Paired changes shipped with the gate:**

- **Descriptive User-Agent.** `config.rs` ships a UA with an empty placeholder path; set it to
  the real repository URL (already in `Cargo.toml`). Cheapest lever; independently helps the
  "sequential still throttled" symptom for providers that penalize placeholder UAs.
- **Honor the HTTP-date form of `Retry-After`** (deferred by ADR-0015). Now that a bounded
  cooldown consumes it, any parsed value — delta-seconds or HTTP-date — is capped at
  `HOST_MAX_COOLDOWN` (and a past/negative date parses to `None`), so a skewed date can never
  produce a wrong sleep. The raw header is still surfaced in the error details.
- **A distinct `RATE_LIMITED` error code** (additive in `error.rs`) carrying `retry_after`
  in the free-form `details`, so an agent can branch on it and pace instead of hand-rolling
  delays.
- `Cargo.toml`: add `"sync"` to tokio's feature list (`Semaphore` needs it).

## Consequences

- The concurrent-MCP burst is fixed for both front-ends from one place; the CLI batch path
  benefits for free. Concurrent same-host tool calls serialize through the shared gate.
- Distinct hosts stay fully parallel — a cooldown on one host never slows another
  (per-host isolation).
- The happy path (any host that hasn't 429'd) gains **zero** latency: no permit contention,
  no cooldown, no spacing. Existing happy-path and ADR-0015 retry tests are unaffected —
  each hits a unique loopback authority (mockito ports) and the pinned retry tests have no
  siblings, so attempt counts and sub-second timings are preserved.
- On a genuinely saturated provider window the gate does not manufacture capacity: a
  heavy-day batch either serializes slowly (up to `MAX_GATE_WAIT` per feed) or sheds to a
  structured `RATE_LIMITED` the agent can reason about. Reduced coverage on the worst days
  remains possible — but it degrades *gracefully* and the agent is told how long to wait.
- Reusing one `HttpClient` restores keep-alive/TLS-session reuse to the host, removing
  per-call connection churn that itself invites throttling.
- **Risk to guard in review/impl:** an implementer must not conflate the two caps (raising
  `RETRY_MAX_DELAY` to 60 s would reintroduce 60 s of in-flight blocking that the per-call
  ceiling exists to prevent), and must never hold the slot-map lock across an `.await`.
  Both are called out in the CLAUDE.md gotchas.
- With `MAX_GATE_WAIT` at 60 s, a single gated call can block longer than some MCP clients'
  tool-call timeout. This is the deliberate "server absorbs it" trade; the env var exists to
  dial it back per client.

## Alternatives considered

- **Proactive token bucket / AIMD rate limiter.** Rejected for v1. A static req/interval
  rate is a guess at an unknown, variable provider limit (simultaneously too slow and too
  aggressive), and AIMD's learned state is useless in the short-lived `rss fetch` process.
  The reactive gate honors the server's own signal where it sends one and, via bounded
  escalation, converges where it does not.
- **Process-global `static` gate keyed on authority.** A principled fallback (politeness is
  arguably process-wide) and a smaller diff, but it fights the codebase's explicit-params
  ethos, is awkward to reset across in-process tests, and does **not** also fix the per-call
  client rebuild. The `Arc<HostGate>`-in-reused-client design is strictly better here.
- **Fixed (non-escalating) headerless cooldown.** Simpler, but the probe showed a provider
  window far larger than any safe fixed default, so a fixed value re-probes and re-429s
  repeatedly. Bounded escalation converges in a few tries at a modest first-hit cost.
- **Fail-fast-only (return `RATE_LIMITED` immediately, never block).** Rejected: it just
  re-hands the throttling to the agent, which already drops coverage that way. We absorb the
  burst in the server and shed only past the bound.
- **Batch `fetch_feed` (`urls: [..]`) so the server paces internally.** A strong agent-UX
  win (removes the client's incentive to fire N concurrent single-URL calls) but it adds
  contract surface, needs a per-feed response-budget rework, and does not help the CLI.
  Deferred to a possible follow-up ADR; the gate fixes both front-ends first.
- **Per-host concurrency = 2.** Rejected in favor of 1: because gates are per-host, `1` only
  serializes multiple same-host feeds (exactly the goal) and leaves distinct hosts parallel.
  Tunable via `RSS_HOST_CONCURRENCY`.
- **eTLD+1 authority keying** (collapsing subdomains of one provider). Deferred; needs a
  public-suffix dependency and callers typically hit one subdomain consistently. Revisit if
  it recurs.
