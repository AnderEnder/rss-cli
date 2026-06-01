# CLAUDE.md

Operational guidance for working in this repo (Claude Code and other contributors).

- **What it does / how to use it:** [README.md](./README.md) (user-facing).
- **Why it's built this way:** [docs/adr/](./docs/adr/) (decision records). Read these
  before changing anything load-bearing — several designs look "simplifiable" until you
  know what they're avoiding.
- This file is the **how to work here** layer: commands, invariants, and the gotchas that
  already bit during the build.

## What this is

`rss-cli` is a lightweight, cache-backed, AI-friendly RSS/Atom/JSON-Feed CLI (binary name
`rss`) that also runs as an MCP server (`rss mcp`). It is **data-on-request**, not a feed
reader: no subscriptions, no read/unread state — see
[ADR-0002](./docs/adr/0002-data-on-request-not-a-subscription-manager.md). Rust **edition
2024**.

## Build, test, and gates

These all pass on `feat/rss-cli-v1`. Run them before declaring any change done — `-D
warnings` is enforced, and CI ([.github/workflows/ci.yml](./.github/workflows/ci.yml))
runs the same gates in the **dev** profile.

```sh
cargo fmt --all                              # format (CI uses --check)
cargo clippy --all-targets -- -D warnings    # lint; warnings are errors
cargo build                                  # dev build
cargo test                                   # unit + integration tests
cargo build --release                        # ~1–2 min: lto=fat + codegen-units=1 (ADR-0010)
```

Quick smoke checks:

```sh
cargo run -- fetch https://news.ycombinator.com/rss --format json | jq '.feeds[0].items[0]'
cargo run -- schema --command fetch          # authoritative JSON Schema (generated)
cargo run -- discover https://news.ycombinator.com
```

## Architecture map

A single core powers both the CLI and the MCP server, so the two front-ends cannot diverge
(see [ADR-0008](./docs/adr/0008-async-runtime-and-mcp-in-v1.md)).

| File | Responsibility |
|------|----------------|
| `src/model.rs` | **The output contract.** Serialized types + `schemars` derives. The AI-facing API — treat field names as stable ([ADR-0006](./docs/adr/0006-ai-facing-output-contract.md)). |
| `src/error.rs` | `RssError`, stable `code()` strings, and the `exit` code constants. |
| `src/config.rs` | `FetchParams` + `CachePolicy` (the *runtime* params, not serialized). |
| `src/core.rs` | Orchestration: concurrent fetch (`buffer_unordered`), `fetch_one`, `discover_feeds`, `show_item`, `exit_code_for`. **CLI and MCP both call into here.** |
| `src/fetch.rs` | `HttpClient`: reqwest + conditional GET, the `CachePolicy` state machine ([ADR-0005](./docs/adr/0005-conditional-get-always-revalidate-default.md)). |
| `src/cache.rs` | Atomic file cache (`<hash>.json` + `<hash>.body`) ([ADR-0004](./docs/adr/0004-file-based-atomic-cache.md)). |
| `src/parse.rs` | `feed-rs` → `model` types; date normalize to UTC; relative→absolute URL resolution; `--since`/`--limit`; newest-first sort. |
| `src/identity.rs` | **The keystone:** deterministic stable item ids ([ADR-0003](./docs/adr/0003-deterministic-content-hash-item-ids.md)). Pinned by a known-answer test. |
| `src/content.rs` | HTML → markdown/text/html/none + `content_tokens_est` ([ADR-0009](./docs/adr/0009-html-to-markdown-htmd-html2text.md)). |
| `src/discover.rs` | `<link rel=alternate>` autodiscovery via `tl`. |
| `src/output.rs` | `json`/`ndjson`/`text` rendering; `schema_for` (schema emission). |
| `src/mcp.rs` | `rmcp` stdio server; tools delegate to `core`. |
| `src/cli.rs` / `src/main.rs` | clap surface; dispatch; exit-code mapping; stderr `tracing`. |
| `tests/` | Integration tests (`assert_cmd`, `mockito`, `insta`) + `fixtures/`. |

## Invariants — do not break these

1. **The schema is generated, never hand-written.** It comes from the `model.rs` structs
   via `schemars`. Do not write a schema file by hand; update the structs and let `rss
   schema` emit it. Any breaking change to the structs requires bumping `SCHEMA_VERSION`
   in `model.rs`.
2. **Optional fields serialize as `null`, never omitted.** Don't add `#[serde(skip_serializing_if)]`
   to contract fields — consumers rely on a fixed shape ([ADR-0006](./docs/adr/0006-ai-facing-output-contract.md)).
3. **stdout is data only; stderr is logs/diagnostics.** Never print logs or progress to
   stdout. `tracing` is wired to stderr.
4. **Item ids are deterministic *by construction*, not cache-dependent.** Don't make
   identity read from the cache or trust raw guids. The known-answer test in
   `identity.rs` (`1b9107de952289cb`, `a86aced5664c7742`) locks the exact byte layout — if
   you change `item_id`, you are changing the public id contract.
5. **Exit codes are a contract:** `0` ok · `1` unexpected · `2` usage · `3` partial · `4`
   all-failed. Defined in `error.rs::exit`; mapped in `main.rs`.
6. **CLI and MCP share `core.rs`.** Add behavior in the core and expose it from both
   front-ends; don't fork logic into `mcp.rs` or `cli.rs`.
7. **MCP responses are size-bounded.** `fetch_feed` defaults to `limit=25` and rejects
   over-budget results with a structured `RESPONSE_TOO_LARGE` error (carrying suggested
   `limit`/`max_content_chars`); every MCP tool error is structured `ErrorObj` JSON, never
   a bare string. Don't return an unbounded `FetchOutput` or a plain-text tool error. See
   [ADR-0011](docs/adr/0011-bounded-mcp-responses.md).
8. **MCP data tools return structured content, not duplicated text.** `fetch_feed` /
   `get_item` / `discover_feeds` put the payload in `structuredContent` (matching the tool's
   generated `outputSchema`) with only a one-line summary in text — never the full payload as
   text too (that doubles tokens and breaks the budget). Errors stay text-only `ErrorObj`
   with no `structuredContent`. See [ADR-0013](docs/adr/0013-structured-mcp-tool-results.md).
9. **`feeds[]`/`errors[]` are in request order** (deterministic *within a run* — not
   byte-reproducible, since `fetched_at`/`status`/`from_cache` vary). `total_items`,
   `total_content_tokens_est`, per-feed counts, `content_hash`, and `warnings` are **additive**
   contract fields computed in `core` (so CLI and MCP stay in sync); `warnings` is kept rare
   on purpose. See [ADR-0012](docs/adr/0012-deterministic-ordering-and-output-enrichments.md).

## Gotchas (these already bit — don't relearn them)

- **`reqwest` 0.13 TLS feature is `rustls`, not `rustls-tls`.** The 0.12-era
  `rustls-tls` name does not resolve in 0.13. Current features:
  `["rustls", "gzip", "charset", "http2"]`.
- **`sha2` 0.11 `finalize()` output has no `LowerHex` impl.** Hex it manually:
  `digest.iter().map(|b| format!("{b:02x}")).collect()` (see `cache.rs` / `identity.rs`).
- **Edition 2024** — let-chains (`if let Some(x) = a && cond`) are used (e.g. `fetch.rs`,
  `cache.rs`); fine on this edition. Keep clippy happy (`is_none_or`, no
  `collapsible_if`).
- **Verify crate names on crates.io before adding them.** The research swarm hallucinated
  a `readdown` crate that does not exist ([ADR-0009](./docs/adr/0009-html-to-markdown-htmd-html2text.md));
  we use `htmd` + `html2text`. Confirm any AI-suggested dependency is real.
- **Parallel edits collide on the shared crate.** This was built by multiple agents owning
  disjoint files over frozen `model.rs`/`error.rs` interfaces; a typo in one module breaks
  the whole-crate build for everyone. If you fan out work, freeze the shared types first
  and keep ownership by file.
- **MCP clients stringify numeric tool args.** Many MCP clients serialize *every* tool
  argument as a JSON string, so a bare `Option<usize>` field rejects `"25"` with
  `invalid type: string "25", expected usize` — which makes `limit` / `max_content_chars` /
  `max_response_tokens` unusable from those clients. `mcp.rs` deserializes those fields with
  `de_lenient_opt_usize` (accepts number **or** numeric string; advertises `integer` in the
  schema regardless). Don't "simplify" them back to `Option<usize>` — that reintroduces the
  bug. The `fetch_args_coerce_stringified_integers` test pins it.

## Non-goals (v1)

Subscription management (add/list/remove), persisted read/unread/star state, full-text
search, scheduled/daemon refresh, and built-in LLM summarization
([ADR-0007](./docs/adr/0007-no-builtin-llm-summarization.md)). Don't add these without a
new ADR — they were deliberately excluded.

## Repo state

- Branch: `feat/rss-cli-v1`. The project is intentionally **local-only** (no remote, no
  PR) per the owner's choice.
