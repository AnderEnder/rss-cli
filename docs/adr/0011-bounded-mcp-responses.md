# 11. Bounded MCP responses

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The MCP server serializes a whole `FetchOutput` and returns it as a single tool result.
For a large feed — a hot Reddit post's comment `.rss`, say — that result can exceed the
client's tool-result size limit, which surfaces to the agent as an opaque
*tool-result-too-large* failure. The agent gets nothing and no hint, even though a smaller
request would have worked. Three concrete gaps were reported:

1. No graceful handling of large feeds (the server never bounds its own output).
2. The error carried no remedy — no machine-readable code, no "retry with `limit=N`".
3. `limit` caps item *count*, not *size*, so there was no way to fetch many items while
   skipping giant bodies.

The server cannot intercept the client's size limit, so the fix must be **proactive**: the
server bounds what it emits and, when it can't, fails with a *structured, actionable* error.

## Decision

The MCP server **bounds its responses** (the "caps + actionable error" approach; the
self-recovery path the reporter explicitly endorsed). Implemented across `model.rs`,
`config.rs`, `error.rs`, `core.rs`, and `mcp.rs`:

- **Default item cap.** `fetch_feed` applies `limit = 25` (`MCP_DEFAULT_LIMIT`) when the
  caller passes none — bounding the common "too many items" blow-up in a single pass.
- **Response token budget.** After building the output, the server estimates serialized
  tokens once (`ceil(chars/4)`) and compares against `max_response_tokens` (default
  `MCP_DEFAULT_MAX_RESPONSE_TOKENS`, overridable per call). Over budget → it returns a
  structured **`RESPONSE_TOO_LARGE`** error instead of an oversized payload.
- **Actionable error.** `RESPONSE_TOO_LARGE` carries `estimated_tokens`, `budget_tokens`,
  `suggested_limit`, and `suggested_max_content_chars` in `details`, so the agent retries
  successfully rather than giving up. *All* MCP tool errors were upgraded from plain strings
  to structured `ErrorObj` JSON (stable `code` + `details`), consistent with the CLI
  ([ADR-0006](0006-ai-facing-output-contract.md)).
- **Per-item size knob.** A new `max_content_chars` (on `FetchParams`, exposed as the CLI
  `--max-content-chars` and an MCP arg) truncates each item's *extracted* body on a char
  boundary, appends an ellipsis marker, recomputes `content_tokens_est`, and flags
  `content_truncated: true`. This is issue 3's "fetch many items, skip giant bodies."
- **Truncation marker.** `FetchOutput.truncation` (a `TruncationInfo`, `null` when nothing
  was bounded) records the applied cap and how many items were content-truncated, so the
  agent knows it is not seeing an unbounded result.
- **`get_item` is the full-content escape hatch** — but a single giant item can itself
  exceed budget, so it too returns `RESPONSE_TOO_LARGE` (with a suggested
  `max_content_chars`) rather than tripping the client limit.

We deliberately do **not** truncate content by default — that would degrade normal feeds
(most articles exceed any small cap). The count-cap handles "many items"; the budget-error
handles "few huge items"; full content stays the default.

These are additive contract changes, so `SCHEMA_VERSION` stays `"1"`
([ADR-0006](0006-ai-facing-output-contract.md): adding fields is backward-compatible).

## Consequences

- Agents get a usable result or a self-recoverable error — never an opaque size failure.
- Every `fetch_feed` response is bounded (≤ 25 items by default) and self-describing about
  it via `truncation`. Callers who want more pass `limit`/`max_response_tokens` explicitly.
- The token budget uses a rough `ceil(chars/4)` estimate on the *pretty* JSON (what the
  client receives); it is intentionally conservative. The server cannot know the client's
  exact limit, so the default is headroom, not a guarantee — hence the override.
- The CLI defaults are unchanged (no item cap, full content) — bounding is an MCP concern;
  `--max-content-chars` is opt-in there.

## Alternatives considered

- **Do nothing / rely on the client.** Rejected — that is the reported bug.
- **Auto-truncate to fit (iteratively shrink content, then drop items, return success).**
  Considered as a "Tier 2." Deferred: it is the most test-heavy, highest-risk piece, and
  its only marginal value over caps + error is saving one retry round-trip. The static caps
  plus an informative error solve the reported case at far lower complexity. Can be added
  later without a contract change (the `TruncationInfo` fields `items_omitted` /
  `estimated_tokens` already exist for it).
- **Default per-item `max_content_chars`.** Rejected as a default: degrades normal feeds;
  kept as an opt-in knob.
