# 6. A stable, AI-facing output contract

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

The product *is* its output contract: an agent consuming `rss-cli` depends on field names,
shapes, timestamp formats, where data vs. logs go, and how to tell success from partial
failure. Ad-hoc or drifting output is the failure mode that makes a tool unusable for
automation. Several related sub-decisions are really one decision — "the AI-facing
contract" — and are recorded together here.

## Decision

The contract is defined by the serialized types in [`src/model.rs`](../../src/model.rs)
and enforced along these axes:

1. **Schema is generated, never hand-written.** The output structs derive
   `schemars::JsonSchema`; `rss schema --command fetch|discover` emits the authoritative
   JSON Schema *from the structs*. Prose examples (README, this ADR) are illustrative only
   and may not be edited into a separate "schema" — the generated one is the source of
   truth. `SCHEMA_VERSION` (currently `"1"`) is bumped on any breaking change.

2. **Predictable shape: optional fields are `null`, never omitted.** Every documented
   field is always present in `--format json`; absent values serialize as `null`. A
   consumer never has to distinguish "missing key" from "null value."

3. **stdout is data; stderr is everything else.** All machine-readable output goes to
   stdout; all logs, diagnostics, and `tracing` output go to stderr. Piping stdout to `jq`
   is always safe.

4. **Partial failure is normal and structured.** Fetching N feeds where M fail still emits
   the N−M successes plus structured `ErrorObj`s (each with a stable
   `SCREAMING_SNAKE_CASE` `code`, a message, and a `details` object). Errors appear both
   on the failing `FeedResult` and mirrored in the top-level `errors` array for a quick
   scan.

5. **Deterministic exit codes** (in [`src/error.rs`](../../src/error.rs)): `0` all ok ·
   `1` unexpected internal error · `2` usage/argument error · `3` partial failure · `4`
   all feeds failed.

6. **Three formats:** `json` (one `FetchOutput` document, default), `ndjson` (one `Item`
   per line — each carries its own `feed_url` — for streaming/`jq`), and `text` (the only
   human-oriented, color-using mode; respects `NO_COLOR`).

7. **Normalized timestamps:** all times are RFC-3339 / ISO-8601 UTC.

## Consequences

- The schema cannot lie: it is derived from the same structs that produce the output, so
  it never drifts. Hand-editing the schema is a defect.
- Adding a field is backward-compatible; renaming/removing/retyping one is a breaking
  change that requires bumping `SCHEMA_VERSION`.
- Agents can self-describe the contract (`rss schema`) and the tools surface it too (MCP
  `get_schema`).
- `null`-not-omitted costs a little verbosity in exchange for a shape consumers can rely
  on unconditionally.

## Alternatives considered

- **Hand-maintained schema doc.** Rejected: guaranteed to drift from the structs.
- **Omitting absent optional fields** (serde default). Rejected: forces every consumer to
  handle "key may be missing," a common source of agent/script bugs.
- **Failing the whole run on any feed error.** Rejected: partial results are valuable and
  expected when fetching many feeds; hence exit code `3` and per-feed errors.
