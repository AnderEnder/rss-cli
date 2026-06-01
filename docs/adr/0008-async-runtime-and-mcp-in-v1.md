# 8. Async runtime (tokio) and a native MCP server in v1

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

Two coupled decisions about how the tool runs and how agents reach it.

First, concurrency: fetching many feeds is I/O-bound, so issuing requests in parallel is
the single biggest latency win. That argues for either an async runtime or a thread pool
around a blocking client.

Second, reach: the project's whole point is being callable by AI agents. A CLI is callable
by agents that can spawn processes and parse stdout, but the **Model Context Protocol** is
the emerging native way for agents to call tools directly. MCP was initially scoped to v2
and then explicitly pulled into v1.

These interact: the chosen MCP SDK, `rmcp`, is built on `tokio`. Picking a blocking HTTP
stack would force a second execution model alongside it.

## Decision

- **`tokio` (multi-thread runtime) is mandatory.** Feeds are fetched concurrently with
  bounded parallelism via `futures::stream::buffer_unordered`, capped by `--concurrency`
  (default 8). See [`src/core.rs`](../../src/core.rs).
- **A native MCP server ships in v1**: `rss mcp` runs over stdio using `rmcp`, exposing
  `fetch_feed`, `discover_feeds`, `get_item`, and `get_schema`
  ([`src/mcp.rs`](../../src/mcp.rs)).
- **The CLI and the MCP server share one core.** Both dispatch into the same functions in
  `core.rs` and return the same `model.rs` types, so the two front-ends cannot diverge in
  behavior or output shape.

## Consequences

- Many-feed fetches are fast and politely bounded; one runtime serves both the CLI and the
  MCP server.
- Agents can call `rss-cli` either as a subprocess (parse stdout) or as MCP tools, getting
  identical structured results either way.
- `tokio` + `rmcp` add binary size and compile time — accepted as the cost of in-v1 MCP
  and concurrency. (Binary size is partly clawed back by
  [ADR-0010](0010-release-profile-tuning.md).)

## Alternatives considered

- **Blocking client (`ureq`) + a thread pool.** Rejected: `rmcp` requires `tokio`, so a
  blocking stack would mean maintaining two concurrency models in one binary.
- **Defer MCP to v2** (the original plan). Reversed by explicit request; building it in v1
  while the shared core was fresh was cheaper than retrofitting later.
