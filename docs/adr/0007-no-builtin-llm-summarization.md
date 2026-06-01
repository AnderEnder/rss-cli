# 7. No built-in LLM summarization

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

A tempting feature for an "AI-friendly" RSS tool is to summarize items itself (`rss fetch
--summarize`). But the primary consumer *is* an LLM agent, which is already far better
positioned to summarize — it knows the user's intent, the desired length and tone, and the
broader context. Baking summarization into the CLI would mean choosing/configuring a model
provider, holding API keys, incurring per-item latency and cost, and producing output the
calling agent would often redo anyway.

## Decision

`rss-cli` does **no** LLM summarization. Instead it makes content cheap for an agent to
summarize:

- Item bodies are cleaned to **Markdown by default** (`--content markdown`, via `htmd`;
  also `text`, `html`, or `none`) — see
  [ADR-0009](0009-html-to-markdown-htmd-html2text.md).
- Each item carries `content_tokens_est`, a rough token estimate, so an agent can budget
  context before deciding what to pull or summarize.
- `--limit` and `--since` let the caller bound how much content comes back.

## Consequences

- No API keys, no model config, no provider coupling, no per-item cost/latency in the
  tool. It stays a fast, deterministic data tool.
- The agent owns summarization, with full control over prompt, length, and model — and can
  use `content_tokens_est` to plan.
- Users who specifically want the *CLI* to emit summaries are not served; that is a
  deliberate non-goal. It could be added later as an explicit, opt-in, provider-configured
  feature without disturbing the core.

## Alternatives considered

- **Built-in `--summarize` calling a hosted model.** Rejected for v1: provider coupling,
  secrets handling, cost/latency, and redundancy with the calling agent's own capability.
