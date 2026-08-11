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
| `src/model.rs` | **The output contract.** Serialized types + `schemars` derives. The AI-facing API — treat field names as stable ([ADR-0006](./docs/adr/0006-ai-facing-output-contract.md)). Includes the additive `FetchOutput.applied_filters` and `FetchOutput.duplicates` ([ADR-0018](./docs/adr/0018-duplicate-reporting-and-keyword-filtering.md)). |
| `src/error.rs` | `RssError`, stable `code()` strings, and the `exit` code constants. |
| `src/config.rs` | `FetchParams` + `CachePolicy` + `DedupeMode` (the *runtime* params, not serialized); `parse_since`/`parse_duration`/`parse_cache_policy`/`parse_dedupe` (shared by the CLI and MCP); `FetchParams::deadline` bounds a batch's wall-clock budget ([ADR-0017](./docs/adr/0017-batch-fetch-and-cursor-pagination.md)). |
| `src/core.rs` | Orchestration: concurrent fetch (`buffer_unordered`) via `fetch_feeds`/`fetch_feeds_with`, `fetch_one`, `discover_feeds`/`discover_feeds_with`, `show_item`/`show_item_with`, `exit_code_for`; `paginate`/`PageStop` fill a `FetchOutput` to a token budget for MCP cursor pagination ([ADR-0017](./docs/adr/0017-batch-fetch-and-cursor-pagination.md)); `find_duplicates`/`drop_duplicates`/`apply_dedupe` implement the `dedupe` modes ([ADR-0018](./docs/adr/0018-duplicate-reporting-and-keyword-filtering.md)). **CLI and MCP both call into here.** |
| `src/fetch.rs` | `HttpClient`: reqwest + conditional GET, the `CachePolicy` state machine ([ADR-0005](./docs/adr/0005-conditional-get-always-revalidate-default.md)); routes every send through the per-host gate ([ADR-0016](./docs/adr/0016-per-host-request-gate.md)). |
| `src/ratelimit.rs` | `HostGate`: shared per-host (authority) concurrency cap + adaptive cooldown for concurrent fetches ([ADR-0016](./docs/adr/0016-per-host-request-gate.md)). Lives inside a *reused* `HttpClient`. |
| `src/cache.rs` | Atomic file cache (`<hash>.json` + `<hash>.body`) ([ADR-0004](./docs/adr/0004-file-based-atomic-cache.md)). |
| `src/parse.rs` | `feed-rs` → `model` types; date normalize to UTC; relative→absolute URL resolution; filtering in the order `--since` → `--query` → sort → `--limit`; newest-first sort. |
| `src/query.rs` | Local keyword matching for `--query` / MCP `query`: AND-ed substring terms, `"quoted phrases"`, `-negation` ([ADR-0018](./docs/adr/0018-duplicate-reporting-and-keyword-filtering.md)). |
| `src/identity.rs` | **The keystone:** deterministic stable item ids ([ADR-0003](./docs/adr/0003-deterministic-content-hash-item-ids.md)). Pinned by a known-answer test. |
| `src/content.rs` | HTML → markdown/text/html/none + `content_tokens_est` ([ADR-0009](./docs/adr/0009-html-to-markdown-htmd-html2text.md)). |
| `src/discover.rs` | `<link rel=alternate>` autodiscovery via `tl`. |
| `src/output.rs` | `json`/`ndjson`/`text` rendering; `schema_for` (schema emission). |
| `src/cursor.rs` | Opaque stateless continuation cursors for paginated `fetch_feed` responses ([ADR-0017](docs/adr/0017-batch-fetch-and-cursor-pagination.md)). |
| `src/mcp.rs` | `rmcp` stdio server; tools delegate to `core`; batches `urls[]`, mints/consumes continuation cursors, and enforces the response-token budget and batch deadline ([ADR-0017](./docs/adr/0017-batch-fetch-and-cursor-pagination.md)). |
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
7. **MCP responses are size-bounded.** `fetch_feed` defaults to `limit=25` and *fills* the
   response up to `max_response_tokens` rather than rejecting outright — an over-budget batch
   ships whatever fits plus `truncation.next_cursor`, and the caller pages the rest
   ([ADR-0017](docs/adr/0017-batch-fetch-and-cursor-pagination.md)). `RESPONSE_TOO_LARGE` now
   fires only when not even one item can be placed in the budget (one oversized item, or an
   envelope with nothing left to shed) — `get_item`'s single-item guard is unchanged, since
   there is only ever one item to place there. Every MCP tool error is structured `ErrorObj`
   JSON, never a bare string. Don't return an unbounded `FetchOutput` or a plain-text tool
   error. See [ADR-0011](docs/adr/0011-bounded-mcp-responses.md).
8. **MCP data tools return structured content, not duplicated text.** `fetch_feed` /
   `get_item` / `discover_feeds` put the payload in `structuredContent` (matching the tool's
   generated `outputSchema`) with only a one-line summary in text — never the full payload as
   text too (that doubles tokens and breaks the budget). Errors stay text-only `ErrorObj`
   with no `structuredContent`. See [ADR-0013](docs/adr/0013-structured-mcp-tool-results.md).
9. **`feeds[]`/`errors[]` are in request order** (deterministic *within a run* — not
   byte-reproducible, since `fetched_at`/`status`/`from_cache` vary). `total_items`,
   `total_content_tokens_est`, per-feed counts, `content_hash`, `applied_filters`,
   `duplicates`, and `warnings` are **additive**
   contract fields computed in `core` (so CLI and MCP stay in sync); `warnings` is kept rare
   on purpose. `feeds[]` is also a contiguous **prefix** of the request URL list — a batch
   deadline may legitimately shorten it, but must never leave a gap, because the MCP cursor's
   position indexes into that same list ([ADR-0017](docs/adr/0017-batch-fetch-and-cursor-pagination.md)).
   See [ADR-0012](docs/adr/0012-deterministic-ordering-and-output-enrichments.md).
10. **Item lookup is multi-key and cache-first.** `core::show_item` (`rss show` / MCP
    `get_item`) matches an item by `id` **or** `guid` **or** resolved `url`, and reads
    cache-first by default so an item the caller already saw survives a rolled feed window
    ([ADR-0014](docs/adr/0014-get-item-cache-first-multi-key-lookup.md)); `rss show --refresh`
    opts back into a live revalidate. The transient `403`/`429` single retry surfaces
    `retry_after` in the error `details`
    ([ADR-0015](docs/adr/0015-bounded-retry-on-transient-429-403.md)). Both are **additive** —
    `SCHEMA_VERSION` stays `"1"`.
11. **MCP pagination is stateless and cache-backed.** A continuation `cursor` carries the
    position, a fingerprint of the *raw* request arguments, and the *resolved* `since`
    cutoff; the call forces `CachePolicy::CacheFirst` so the existing body cache is the
    pagination store. Don't add a server-side result store, and don't fingerprint the
    resolved `since` — a relative window resolves differently on every call, so every
    continuation would mismatch ([ADR-0017](docs/adr/0017-batch-fetch-and-cursor-pagination.md)).
    Because the cache *is* the store, a request that refuses to write it cannot be paged:
    `cache_policy: "no-cache"` ships a bounded page with `next_cursor: null` and a suggestion
    rather than a token the next page would redeem against some earlier call's snapshot.
    `mcp.rs`'s `no_cursor_reason` is the one place that decision lives.
12. **Cross-feed dedup reports by default; it does not remove.** `duplicates[]` groups items
    by `guid` → `url` → `content_hash` (never by `id`, which is namespaced by `feed_url` per
    ADR-0003) and leaves `feeds[]`, request order, and per-feed `item_count` untouched. Only
    the opt-in `dedupe: "drop"` removes copies — and `drop` cannot be paged **in either
    direction**, because a continuation page cannot see canonical copies from earlier pages and
    would ship the same article twice: it is rejected when passed *with* a `cursor`, and an
    over-budget `drop` page also withholds one (`no_cursor_reason` in `mcp.rs`). Withholding is
    load-bearing, not belt-and-braces — a first `drop` page is legal, so it reaches the
    fingerprint stamped `"drop"`, and a token minted from it can never be redeemed by anything.
    Both front-ends parse the mode with
    `config::parse_dedupe` and apply it through the one shared `core::apply_dedupe`
    (invariant 6); grouping runs **before** `core::paginate` (the budget has to measure the
    `duplicates[]` that ships), so on a paged response `duplicates[]` describes that page's
    fetch only ([ADR-0018](docs/adr/0018-duplicate-reporting-and-keyword-filtering.md)).

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
- **`CachePolicy::CacheFirst` exists specifically for item lookup** (`rss show` / MCP
  `get_item`); do not "simplify" it back to `Revalidate` — that reintroduces the rolled-window
  `NOT_FOUND` the policy was added to fix ([ADR-0014](docs/adr/0014-get-item-cache-first-multi-key-lookup.md)).
  Pinned by `tests/get_item_window_roll.rs` and
  `fetch::tests::cache_first_serves_stale_cache_without_network`.
- **The 403/429 retry is single and bounded** (one retry, capped wait); do not turn it into an
  unbounded loop — that would be impolite and could mask a persistent outage
  ([ADR-0015](docs/adr/0015-bounded-retry-on-transient-429-403.md)). Pinned by
  `fetch::tests::retries_once_on_403_then_succeeds` /
  `fetch::tests::persistent_403_surfaces_status_in_error`.
- **The MCP server reuses ONE `HttpClient` across tool calls** — do not "simplify" it back to
  a per-call `HttpClient::new`. The shared client is what lets concurrent `fetch_feed` calls
  coordinate their per-host pacing (and share the connection pool); a fresh client per call
  reintroduces the concurrent-call 429 burst ([ADR-0016](docs/adr/0016-per-host-request-gate.md)).
  Pinned by `mcp::tests::concurrent_fetches_share_one_client_and_gate`.
- **The rate-limit gate has TWO distinct caps — don't merge them.** `RETRY_MAX_DELAY` (5 s)
  bounds the ADR-0015 *in-flight* retry (which holds its host permit); `HOST_MAX_COOLDOWN` /
  `MAX_GATE_WAIT` (60 s) bound the *sibling-facing* gate wait (ADR-0016). Raising
  `RETRY_MAX_DELAY` to match would block siblings for 60 s behind a retrying request — the very
  thing the split avoids.
- **Never hold the `HostGate` slot-map lock across an `.await`.** `slot_for` locks only to
  insert-and-clone the `Arc<HostSlot>`, then drops the guard; all waiting uses the per-slot
  semaphore + atomic deadlines. Holding the `std::sync::Mutex` across a sleep would stall the
  runtime. (Also: `tokio` needs the `"sync"` feature for `Semaphore`.)
- **MCP clients stringify array arguments too.** `urls` accepts a JSON array, a stringified
  JSON array, or a delimited string via `de_lenient_url_list` — the same liberality
  `de_lenient_opt_usize` gives numbers. Pinned by `urls_arg_accepts_every_stringified_form`.
- **`fetch_feed`'s `limit` is per feed, not per batch.** A 10-URL call assembles up to
  10 x limit candidates and lets the response budget page them. Don't "fix" this by scaling
  the cap down by feed count — that silently returns fewer items than the same single-URL
  call would.
- **`core::pretty_tokens` must never over-estimate.** It deliberately under-counts (a flat
  `+ 2` regardless of real nesting cost) so `paginate`'s greedy running total stays a
  provable *lower bound* on the real serialized payload — a greedy rejection then implies a
  real one. Raising the constant to "improve accuracy" lets the greedy total exceed the real
  cost and rejects pages that would actually have fit
  ([ADR-0017](docs/adr/0017-batch-fetch-and-cursor-pagination.md)). Pinned by
  `paginate_ships_a_page_that_fits_even_when_a_later_feed_is_dropped` and
  `paginate_still_errors_below_the_first_feeds_envelope_and_item`.
- **`query` is applied before `limit`, on purpose.** So `limit` means "N matching items", not
  "N items, some of which match". Moving the filter after the limit silently returns fewer
  results than asked for. The order in `parse::parse_feed` is `since` → `query` → sort →
  `limit`. Pinned by `parse::tests::query_filters_before_limit`.
- **`item.id` and the dedup key disagree — never collapse duplicates by id.** `identity.rs`
  derives `id` from link → guid → title|published; `core::dedup_key` prefers guid → url →
  content_hash. A feed whose entries all carry the same `<link>` gives every one of its items
  the *same* `id` while their guids put them in *different* groups. So `drop_duplicates` takes
  the groups as a set of **keys to collapse** and re-derives each surviving item's key — it
  does not delete `item_ids`. "Simplifying" it back to id-set membership (or a per-id removal
  budget) deletes an item no group named and leaves the real duplicate in place; every derived
  count is refreshed afterwards, so the output stays internally consistent and no count
  assertion catches it. Pinned by
  `core::tests::drop_duplicates_removes_by_key_not_by_id_when_ids_collide`.

## Non-goals (v1)

Subscription management (add/list/remove), persisted read/unread/star state, full-text
search, scheduled/daemon refresh, and built-in LLM summarization
([ADR-0007](./docs/adr/0007-no-builtin-llm-summarization.md)). Don't add these without a
new ADR — they were deliberately excluded.

## Repo state

- Default branch is `main`; `origin` is `git@github.com:AnderEnder/rss-cli.git`. (An earlier
  version of this section said the project was local-only with no remote and named
  `feat/rss-cli-v1` as the branch — both stale.)
- **Feature work merges into local `main`; publishing is a separate, explicit decision.**
  Don't `git push` or open a PR unless the owner asks for it in that turn. Dependabot PRs
  arrive on their own; the branches under `origin/dependabot/*` are not ours to manage.
