# 13. Structured MCP tool results

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The MCP tools originally returned their result as a single block of pretty-printed JSON
**text** (`CallToolResult` with one text `Content`). MCP (rmcp 1.7) supports richer results
that AI clients use directly:

- **`structuredContent`** — a typed JSON object the client parses without re-parsing text.
- **`outputSchema`** on the tool definition — lets the client know the shape in advance and
  validate the structured result.
- **tool annotations** — `readOnlyHint` / `idempotentHint` / `openWorldHint`, which let a
  client reason about a tool's safety and caching.

Adopting these is a clear AI-friendliness win, but it collides with
[ADR-0011](0011-bounded-mcp-responses.md): the MCP server bounds its response size, and the
naive "spec back-compat" advice — emit the full payload as *both* `structuredContent` and
text — would put the whole `FetchOutput` on the wire **twice**, roughly doubling tokens.
Worse, `enforce_response_budget` estimates once over the single `FetchOutput`, so it would
under-count the real wire payload by ~2× and could pass a result that still trips the
client's limit.

## Decision

Adopt structured results for the **data tools** (`fetch_feed`, `get_item`,
`discover_feeds`), implemented in `mcp.rs`:

- **`structuredContent` is primary; text is a short summary, not a copy.** A
  `structured_result` helper puts the typed value in `structured_content` and only a terse
  one-line summary (e.g. `Fetched 2 item(s) (~30 content tokens) from "Hacker News". Full
  data in structuredContent.`) in the text `content`. We deliberately do **not** duplicate
  the full payload as text. This keeps the budget honest: `enforce_response_budget` estimates
  over the *pretty* `FetchOutput`, while the wire carries the *compact* `structuredContent`
  plus a negligible summary — so the estimate is a slight **over**-count of the real payload
  (conservative, never an under-count), exactly as ADR-0011 intends.
  `CallToolResult` is `#[non_exhaustive]`, so we set its public `structured_content` field on
  an owned `success(...)` result rather than constructing a struct literal.
- **`outputSchema` is declared on the success shape** via the macro
  (`output_schema = rmcp::handler::server::tool::schema_for_type::<FetchOutput>()`, and
  likewise `Item` / `DiscoverOutput`). The schema is generated from the model structs, so it
  cannot drift (consistent with invariant 1).
- **Errors stay text-only.** A `RESPONSE_TOO_LARGE` (or any) error keeps the structured
  `ErrorObj` JSON in the text `content` with `isError: true` and sets **no**
  `structuredContent`. Putting an `ErrorObj` under a `FetchOutput` `outputSchema` would
  violate the declared schema for a conformance-checking client; an error result simply has
  no structured body.
- **Annotations on every tool.** All four are `readOnlyHint = true`, `idempotentHint = true`.
  `openWorldHint = true` for the three that hit the network; `get_schema` is `false` (purely
  local). `get_schema` keeps returning its schema as text (it *is* a schema, is not
  budget-checked, and doubling it would be pure waste).

## Consequences

- AI clients get a typed `structuredContent` object matching an advertised `outputSchema`,
  while non-structured clients still see a useful one-line summary.
- The response budget remains accurate because the payload is not duplicated; a live stdio
  roundtrip confirmed the text summary (~90 chars) is a fraction of the structured payload
  (~1.7 kB), not a second copy.
- The `decode` test helper now prefers `structuredContent` (falling back to text for error
  results), and a new test asserts success carries `structuredContent` with a non-duplicating
  summary while errors carry none. Another test introspects `tool_router().list_all()` to
  assert the advertised annotations and `outputSchema`.

## Alternatives considered

- **Emit full payload as both structured and text (spec's back-compat SHOULD).** Rejected:
  ~2× tokens and a budget that under-counts by half — directly at odds with ADR-0011.
- **Account for the duplication in the budget estimate instead.** Possible, but it spends
  the client's token budget to send the same data twice for no benefit to a structured-aware
  agent. Minimal text is strictly better.
- **`structuredContent` on `get_schema` too.** Rejected — it returns a (large) schema, is not
  budget-checked, and the value is meant to be read as-is; structuring it adds cost without
  benefit.
