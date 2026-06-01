# Architecture Decision Records

This directory records the **significant, hard-to-reverse decisions** behind `rss-cli`
and — more importantly — *why* they were made and what was rejected. An ADR captures the
context and the trade-off at the moment of the decision; it is not updated as the code
evolves (a superseding ADR is written instead).

- **ADRs** answer *why* (this directory) — backward-looking rationale.
- **[CLAUDE.md](../../CLAUDE.md)** answers *how to work in the repo* — forward-looking
  operational guidance for contributors and coding agents.
- **[README.md](../../README.md)** is the user-facing manual (commands, flags, schema).

These three intentionally do **not** restate each other; each links to the others.

## Format

Each ADR is a single file: `NNNN-kebab-title.md`, using a lightweight
[Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
template — **Status · Context · Decision · Consequences** (plus *Alternatives considered*
where a real fork existed). Statuses: `Proposed`, `Accepted`, `Superseded by NNNN`,
`Deprecated`.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-data-on-request-not-a-subscription-manager.md) | Data-on-request, not a subscription manager | Accepted |
| [0003](0003-deterministic-content-hash-item-ids.md) | Deterministic content-hash item ids (the keystone) | Accepted |
| [0004](0004-file-based-atomic-cache.md) | File-based atomic cache, not an embedded DB | Accepted |
| [0005](0005-conditional-get-always-revalidate-default.md) | Conditional GET with always-revalidate default | Accepted |
| [0006](0006-ai-facing-output-contract.md) | A stable, AI-facing output contract | Accepted |
| [0007](0007-no-builtin-llm-summarization.md) | No built-in LLM summarization | Accepted |
| [0008](0008-async-runtime-and-mcp-in-v1.md) | Async runtime (tokio) and a native MCP server in v1 | Accepted |
| [0009](0009-html-to-markdown-htmd-html2text.md) | HTML→Markdown via `htmd` + `html2text` | Accepted |
| [0010](0010-release-profile-tuning.md) | Tuned release profile (LTO, single codegen unit, abort, strip) | Accepted |
| [0011](0011-bounded-mcp-responses.md) | Bounded MCP responses (default cap + budget + `RESPONSE_TOO_LARGE`) | Accepted |
| [0012](0012-deterministic-ordering-and-output-enrichments.md) | Deterministic ordering + output enrichments (aggregates, `content_hash`, `warnings`, NDJSON records) | Accepted |
| [0013](0013-structured-mcp-tool-results.md) | Structured MCP tool results (`structuredContent` + `outputSchema` + annotations) | Accepted |
