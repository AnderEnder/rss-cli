# 1. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

`rss-cli` was built greenfield in a single intensive session using a research swarm and a
team of parallel agents. Several load-bearing decisions were made quickly — stable item
ids, the cache design, the output contract — and the *reasoning* behind them (especially
the alternatives that were rejected and why) is the kind of knowledge that evaporates if
it lives only in a chat transcript or a person's head. A future contributor (human or
agent) who doesn't know *why* guid-first identity was rejected is liable to "simplify" it
straight back into the bug it was designed to avoid.

## Decision

We keep **Architecture Decision Records** in [`docs/adr/`](./), one Markdown file per
decision, in the Nygard format (Status · Context · Decision · Consequences, plus
*Alternatives considered* where relevant). An ADR is immutable once Accepted; we change
direction by writing a new ADR that supersedes the old one rather than editing history.

## Consequences

- The *why* is durable and reviewable in the repo, decoupled from the code's *what*.
- A small ongoing cost: a genuinely significant decision should land with an ADR.
- ADRs are explicitly **not** kept in sync with the code line-by-line — they are a record
  of intent at a point in time. Operational truth that must track the code lives in
  [CLAUDE.md](../../CLAUDE.md) and the source itself (e.g. the schema is generated, never
  hand-written).
