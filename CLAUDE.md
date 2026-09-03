# CLAUDE.md

How to work in this repo. **What it does:** [README.md](./README.md). **Why it's built this
way:** [docs/adr/](./docs/adr/) — read the relevant ADR before changing anything load-bearing;
several designs look simplifiable until you know what they avoid.

`rss-cli` is a cache-backed, AI-friendly RSS/Atom/JSON-Feed CLI (binary `rss`) that also runs
as an MCP server (`rss mcp`). **Data-on-request, not a feed reader** — no subscriptions, no
read/unread state ([ADR-0002](./docs/adr/0002-data-on-request-not-a-subscription-manager.md)).
Rust **edition 2024**.

## House rule: be concise

Applies to this file, ADRs, doc comments, code comments, commit messages, and PR text.

- Say it once, in the one place it belongs. Cross-reference instead of repeating.
- A comment earns its place by saying what the code cannot: **why**, not what. One or two
  lines. If it needs a paragraph, the reasoning belongs in an ADR and the comment links there.
- Prefer naming the trap and its pin (`don't X — reintroduces Y; pinned by <test>`) over
  narrating the history.
- `model.rs` doc comments become the generated `outputSchema` **shipped to every MCP client on
  every session** — they cost tokens on the wire. Keep them tight.

## Build, test, gates

Run before declaring any change done. `-D warnings` is enforced; CI
([.github/workflows/ci.yml](./.github/workflows/ci.yml)) runs the same gates in the dev profile.

```sh
cargo fmt --all                              # CI uses --check
cargo clippy --all-targets -- -D warnings
cargo test                                   # unit + integration
cargo build --release                        # ~12 min: lto=fat + codegen-units=1 (ADR-0010)
```

Smoke checks:

```sh
cargo run -- fetch https://news.ycombinator.com/rss --format json | jq '.feeds[0].items[0]'
cargo run -- schema --command fetch          # authoritative JSON Schema (generated)
cargo run -- discover https://news.ycombinator.com
```

## Architecture map

One core powers both front-ends, so they cannot diverge
([ADR-0008](./docs/adr/0008-async-runtime-and-mcp-in-v1.md)).

| File | Responsibility |
|------|----------------|
| `src/model.rs` | **The output contract.** Serialized types + `schemars`. Field names are stable ([ADR-0006](./docs/adr/0006-ai-facing-output-contract.md)). |
| `src/error.rs` | `RssError`, stable `code()` strings, `exit` code constants. |
| `src/config.rs` | Runtime params (not serialized): `FetchParams`, `CachePolicy`, `DedupeMode`, and the `parse_*` functions both front-ends share. |
| `src/core.rs` | Orchestration: `fetch_feeds*`, `fetch_one`, `discover_feeds*`, `show_item*`, `exit_code_for`, `paginate`/`PageStop`, `find_duplicates`/`drop_duplicates`/`apply_dedupe`. **CLI and MCP both call in here.** |
| `src/fetch.rs` | `HttpClient`: reqwest + conditional GET, the `CachePolicy` state machine ([ADR-0005](./docs/adr/0005-conditional-get-always-revalidate-default.md)); every send goes through the per-host gate. |
| `src/ratelimit.rs` | `HostGate`: per-authority concurrency cap + adaptive cooldown ([ADR-0016](./docs/adr/0016-per-host-request-gate.md)). Lives inside a *reused* `HttpClient`. |
| `src/cache.rs` | Atomic file cache, `<hash>.json` + `<hash>.body` ([ADR-0004](./docs/adr/0004-file-based-atomic-cache.md)). |
| `src/parse.rs` | `feed-rs` → `model`; dates to UTC; relative→absolute URLs; filter order `since` → `query` → sort → `limit`. |
| `src/query.rs` | Keyword matching: AND-ed substring terms, `"quoted phrases"`, `-negation` ([ADR-0018](./docs/adr/0018-duplicate-reporting-and-keyword-filtering.md)). |
| `src/identity.rs` | **The keystone:** deterministic item ids ([ADR-0003](./docs/adr/0003-deterministic-content-hash-item-ids.md)). Known-answer test. |
| `src/content.rs` | HTML → markdown/text/html/none + `content_tokens_est` ([ADR-0009](./docs/adr/0009-html-to-markdown-htmd-html2text.md)). |
| `src/cursor.rs` | Opaque stateless continuation cursors ([ADR-0017](./docs/adr/0017-batch-fetch-and-cursor-pagination.md)). |
| `src/discover.rs` | `<link rel=alternate>` autodiscovery via `tl`. |
| `src/output.rs` | `json`/`ndjson`/`text` rendering; `schema_for`. |
| `src/mcp.rs` | `rmcp` stdio server; tools delegate to `core`; batches `urls[]`, mints/consumes cursors, enforces the token budget and batch deadline. |
| `src/cli.rs` / `src/main.rs` | clap surface; dispatch; exit-code mapping; stderr `tracing`. |
| `tests/` | Integration (`assert_cmd`, `mockito`, `insta`) + `fixtures/`. |

## Invariants — do not break these

1. **The schema is generated, never hand-written.** Update the `model.rs` structs and let
   `rss schema` emit it. A breaking change to them requires bumping `SCHEMA_VERSION`.
2. **Optional contract fields serialize as `null`, never omitted.** No
   `#[serde(skip_serializing_if)]` — consumers rely on a fixed shape (ADR-0006).
3. **stdout is data only; stderr is logs.** `tracing` is wired to stderr.
4. **Item ids are deterministic by construction, not cache-dependent.** Don't read identity
   from the cache or trust raw guids. The known-answer test (`1b9107de952289cb`,
   `a86aced5664c7742`) locks the byte layout — changing `item_id` changes a public contract.
5. **Exit codes are a contract:** `0` ok · `1` unexpected · `2` usage · `3` partial · `4`
   all-failed. Defined in `error.rs::exit`, mapped in `main.rs`. They key on feed **errors**,
   never on item counts (a feed emptied by `--dedupe drop` is still exit 0).
6. **CLI and MCP share `core.rs`.** Add behavior to the core; don't fork it into a front-end.
7. **MCP responses are size-bounded and *fill* rather than reject.** An over-budget batch ships
   what fits plus `truncation.next_cursor`. `RESPONSE_TOO_LARGE` fires only when not even one
   item fits. Every tool error is structured `ErrorObj` JSON, never a bare string.
   ([ADR-0011](./docs/adr/0011-bounded-mcp-responses.md),
   [ADR-0017](./docs/adr/0017-batch-fetch-and-cursor-pagination.md))
8. **MCP data tools return `structuredContent` plus a one-line text summary** — never the
   payload as text too, which doubles tokens. Errors are text-only `ErrorObj`, no
   `structuredContent`. ([ADR-0013](./docs/adr/0013-structured-mcp-tool-results.md))
9. **`feeds[]`/`errors[]` follow request order, and `feeds[]` is a contiguous *prefix* of the
   request URL list.** A batch deadline may shorten it but must never leave a gap — the cursor
   indexes into that same list. Aggregates (`total_items`, per-feed counts, `applied_filters`,
   `duplicates`, `warnings`) are additive and computed in `core`, so both front-ends agree.
   ([ADR-0012](./docs/adr/0012-deterministic-ordering-and-output-enrichments.md))
10. **Item lookup is multi-key and cache-first.** `core::show_item` matches on `id` **or**
    `guid` **or** resolved `url`, reading cache-first so a rolled window can't lose an item the
    caller already saw; `--refresh` opts back into a live revalidate.
    ([ADR-0014](./docs/adr/0014-get-item-cache-first-multi-key-lookup.md))
11. **MCP pagination is stateless and cache-backed.** The cursor carries the position, a
    fingerprint of the *raw* arguments, and the *resolved* `since`; the call forces
    `CacheFirst`, so the body cache is the store. Don't add a server-side store, and don't
    fingerprint the resolved `since` — a relative window resolves differently every call, so
    every continuation would mismatch. (ADR-0017)
12. **Two request shapes cannot be paged**, and both ship `next_cursor: null` with a
    suggestion. `mcp.rs`'s `no_cursor_reason` is the single place this lives.
    - `dedupe: "drop"` — a continuation can't see canonical copies from earlier pages. Rejected
      *with* a cursor, and withheld on mint too: a first `drop` page is legal, so it reaches
      the fingerprint stamped `"drop"`, and nothing could ever redeem that token.
    - `cache_policy: "no-cache"` — it writes nothing, so the next page would resume against
      some earlier call's snapshot.
13. **Dedup reports by default; only `drop` removes.** `duplicates[]` groups on `guid` → `url`
    → `content_hash` and leaves `feeds[]`, order, and `item_count` untouched. Grouping runs
    *before* `paginate` (the budget must measure what ships), so on a paged response
    `duplicates[]` describes that page's fetch only.
    ([ADR-0018](./docs/adr/0018-duplicate-reporting-and-keyword-filtering.md))

## Gotchas (these already bit)

- **`reqwest` 0.13 TLS feature is `rustls`, not `rustls-tls`.** Features:
  `["rustls", "gzip", "charset", "http2"]`.
- **`sha2` 0.11 `finalize()` has no `LowerHex`.** Hex manually:
  `digest.iter().map(|b| format!("{b:02x}")).collect()`.
- **Edition 2024 let-chains** are used. Keep clippy happy (`is_none_or`, no `collapsible_if`).
- **Verify crate names on crates.io.** An AI-suggested `readdown` crate did not exist; we use
  `htmd` + `html2text`.
- **Parallel edits collide on the shared crate.** If you fan work out, freeze the shared types
  first and keep ownership by file — one typo breaks everyone's build.
- **MCP clients stringify *every* argument.** Numbers go through `de_lenient_opt_usize`, lists
  through `de_lenient_url_list` (JSON array, stringified array, or delimited string). Don't
  "simplify" either back to a plain type. Pinned by `fetch_args_coerce_stringified_integers`
  and `urls_arg_accepts_every_stringified_form`.
- **`CachePolicy::CacheFirst` exists for item lookup.** Reverting it to `Revalidate`
  reintroduces the rolled-window `NOT_FOUND` (ADR-0014). Pinned by
  `tests/get_item_window_roll.rs`.
- **The 403/429 retry is single and bounded.** An unbounded loop is impolite and masks outages
  ([ADR-0015](./docs/adr/0015-bounded-retry-on-transient-429-403.md)). Pinned by
  `fetch::tests::retries_once_on_403_then_succeeds`.
- **The MCP server reuses ONE `HttpClient`.** A per-call client reintroduces the concurrent-call
  429 burst (ADR-0016). Pinned by `mcp::tests::concurrent_fetches_share_one_client_and_gate`.
- **The rate limiter has three separate bounds — don't merge them.** `RETRY_MAX_DELAY` (5 s)
  bounds one *in-flight* retry holding its host permit; `HOST_MAX_COOLDOWN`/`MAX_GATE_WAIT`
  (60 s) bound a *sibling's* gate wait; `FetchParams::deadline` bounds when a fetch may
  **start**. Raising the first to match the second blocks siblings behind a retrying request.
- **`FetchParams::deadline` is enforced *at the gate*, not just before it.**
  `HostGate::acquire_until` bounds both the permit wait and the cooldown sleep by it, so a
  same-host batch sheds what it cannot start instead of serializing behind escalating
  cooldowns. Don't revert it to a pre-flight-only check: with `per_host = 1` all
  `concurrency` feeds pass a start-time check at t≈0, and the call then runs **~82 s against
  a 3 s deadline** — past any client tool timeout, which returns *nothing at all*, not even
  the `feeds_omitted` envelope. This is a fourth, tighter ceiling
  (`min(MAX_GATE_WAIT, deadline)`), *not* a merge of the three above. Pinned by
  `core::tests::deadline_is_a_real_wall_clock_bound_for_a_throttled_same_host_batch`
  (asserts elapsed wall-clock — omission counts alone pass either way).
- **Never hold the `HostGate` slot-map lock across an `.await`.** `slot_for` locks only to
  insert-and-clone the `Arc<HostSlot>`; all waiting uses the per-slot semaphore.
- **`fetch_feed`'s `limit` is per feed, not per batch.** Scaling it down by feed count would
  silently return fewer items than the same single-URL call.
- **`core::pretty_tokens` must never over-estimate.** Its flat `+ 2` deliberately under-counts
  so `paginate`'s running total is a provable *lower bound* on the real payload — then a greedy
  rejection implies a real one. "Improving accuracy" rejects pages that would have fit
  (ADR-0017). Pinned by `paginate_ships_a_page_that_fits_even_when_a_later_feed_is_dropped`.
- **`query` runs before `limit`, on purpose,** so `limit` means "N matching items". Pinned by
  `parse::tests::query_filters_before_limit`.
- **`item.id` and the dedup key disagree — never collapse duplicates by id.** `identity.rs`
  keys on link → guid → title|published; `core::dedup_key` on guid → url → content_hash. A feed
  whose entries share one `<link>` gives every item the same `id` while their guids put them in
  different groups, so id-based removal deletes an item no group named and leaves the real
  duplicate — with every count refreshed afterwards, so nothing catches it. `drop_duplicates`
  therefore re-derives each item's key. Pinned by
  `core::tests::drop_duplicates_removes_by_key_not_by_id_when_ids_collide`.

## Non-goals (v1)

Subscription management, persisted read/unread/star state, full-text search, scheduled refresh,
built-in LLM summarization ([ADR-0007](./docs/adr/0007-no-builtin-llm-summarization.md)). Don't
add these without a new ADR.

## Repo state

- Default branch `main`; `origin` is `git@github.com:AnderEnder/rss-cli.git`.
- **Feature work merges into local `main`; publishing is a separate, explicit decision.** Don't
  push or open a PR unless asked in that turn. `origin/dependabot/*` is not ours to manage.
