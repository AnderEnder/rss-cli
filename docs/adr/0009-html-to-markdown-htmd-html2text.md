# 9. HTML→Markdown via `htmd` + `html2text`

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

Feed bodies are HTML. For agent consumption, clean **Markdown** is the ideal default
(compact, structure-preserving, LLM-native), with plain **text** as an alternative and raw
**html** / **none** as escape hatches ([ADR-0007](0007-no-builtin-llm-summarization.md)).
We needed crates to do the conversion.

The research swarm initially recommended a content-extraction crate named **`readdown`**.
On verification against crates.io, **`readdown` does not exist** (a hallucinated
dependency). This is a reminder that swarm-proposed crate names are verified live before
adoption.

## Decision

- **HTML → Markdown:** [`htmd`](https://crates.io/crates/htmd) (`htmd::convert`), the
  default for `--content markdown`.
- **HTML → plain text:** [`html2text`](https://crates.io/crates/html2text) for
  `--content text`.
- `--content html` passes the body through; `--content none` omits it (`content: null`).
- A `strip_tags` fallback guards against conversion failure.
- Implemented in [`src/content.rs`](../../src/content.rs), which also computes
  `content_tokens_est`.

## Consequences

- Markdown-by-default output is ready for an LLM with no further cleaning.
- Two focused, pure-Rust crates instead of one (non-existent) all-in-one; each does one job
  well.
- **Process note:** crate names suggested by research/LLM agents are confirmed on
  crates.io before they enter `Cargo.toml`. (`readdown` and the `reqwest` `rustls` feature
  rename were both caught this way — see [CLAUDE.md](../../CLAUDE.md) gotchas.)

## Alternatives considered

- **`readdown`.** Rejected — does not exist.
- **A full HTML5 DOM stack (`scraper`/`html5ever`) for body conversion.** Not needed;
  `htmd`/`html2text` handle feed-body HTML directly. (A lightweight parser, `tl`, is used
  only for `<link rel=alternate>` discovery, not body conversion.)
