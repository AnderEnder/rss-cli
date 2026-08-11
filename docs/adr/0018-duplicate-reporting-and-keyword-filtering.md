# 18. Cross-feed duplicate reporting and local keyword filtering

- **Status:** Accepted
- **Date:** 2026-08-11

## Context

[ADR-0017](0017-batch-fetch-and-cursor-pagination.md) closed the structural gap in the MCP
surface (batch fetch, pagination, cache control) and noted that of the seven gaps the field
report raised, only two were **genuinely absent**: cross-feed duplicate detection and keyword
filtering. This ADR is those two.

Both come from the same use case — an agent assembling a digest over several overlapping
sources — and both are small enough that neither alone would justify a record. They share one
here because they share a shape: each adds an additive top-level field and an argument on both
front-ends, and each has a decision inside it that looks arbitrary until you know what it is
avoiding.

**Duplicates.** A digest over Hacker News, Lobsters, and a handful of aggregators sees the
same article two or three times. The caller cannot collapse them itself using what we return:
`item.id` is a content hash **namespaced by `feed_url`** by construction
([ADR-0003](0003-deterministic-content-hash-item-ids.md)), so the same article delivered by two
feeds has two different ids and is un-groupable by id — the same namespacing that
[ADR-0014](0014-get-item-cache-first-multi-key-lookup.md) documented as confusing for
`get_item`, surfacing here as a second, distinct symptom.

**Filtering.** Some providers offer a server-side query — `hnrss.org` takes `?q=`, Reddit has
`search.rss` — but that syntax is provider-specific and does not generalize to arXiv, GitHub
release feeds, or a plain blog. An agent wanting "items about Rust from these nine sources"
had to fetch every item in full and discard most of them, paying the whole content-extraction
and response-budget cost for the ones it never wanted.

## Decision

Five decisions, all landing in shared code (`core.rs`, `parse.rs`, the new `query.rs`,
`config.rs`) so the CLI and the MCP server cannot drift apart — invariant 6.

### 1. Dedup keys on `guid`, then `url`, then `content_hash` — never on `id`

`core::dedup_key` returns the first present, non-empty of those three, tagged with a
`DuplicateKeyKind` so the caller knows which one matched and can judge how much to trust it.
`item.id` is not a candidate: it is namespaced by `feed_url`, so keying on it would group
nothing across feeds, which is the entire case this exists for.

The order is by decreasing reliability *as a within-batch identity signal*:

- **`guid`** — two feeds syndicating one entry usually carry the same guid. Note this is a
  different property from guid *stability across fetches*, which `identity.rs` documents as
  poor (roughly 41% of feeds regenerate guids) and which is exactly why `id` is a content hash
  rather than the guid. A guid is a good key for "are these two items, fetched right now, the
  same entry?" and a bad key for "is this the same item I saw last week."
- **`url`** — the resolved permalink, when there is no guid.
- **`content_hash`** — **lossy, and knowingly so.** `Item::content_hash` is SHA-256 over the
  extracted *body text* alone: no title, no url, no date. Two genuinely different items that
  share body text — an empty body, a shared boilerplate stub, a "read more on our site"
  placeholder — hash identically and are reported as a false-positive group. We report the
  ambiguity via `key_kind: content_hash` rather than adding heuristics (a title-similarity
  check, a minimum body length) to filter it: a heuristic would trade a visible false positive
  for an invisible false negative, and this kind is only reachable when both `guid` and `url`
  are absent, which is rare. Callers that need precision treat a `content_hash` group as a hint
  to verify, not a certainty.

An item with none of the three has no key and is skipped entirely — never grouped with other
keyless items, which would make "has no identity" itself an identity.

Grouping is deliberately not restricted to items in *different* feeds: the key is computed over
every item in the batch, so a single feed that repeats an entry produces a group too. That
repetition is worth reporting on its own, and special-casing it away would cost a
feed-boundary check for no gain.

### 2. Report by default; `drop` is opt-in

`dedupe` accepts `report` (default) | `off` | `drop`, parsed by
`config::parse_dedupe` into a `DedupeMode` and applied by the single shared
`core::apply_dedupe`. Under `report`, `duplicates[]` is populated and **nothing else changes**:
`feeds[]`, request order, and every per-feed `item_count` are exactly what they would have been
without the argument, so invariant 9 holds unchanged and no existing consumer sees a difference
it did not ask for. `drop` collapses each group to its canonical (first-in-request-order) copy
and refreshes every derived count.

Defaulting to `report` is the load-bearing half. Removing items is visible in three places a
consumer may be relying on — per-feed `item_count`, the top-level totals, and the item sequence
itself — and no caller asked for that when they upgraded. Reporting delivers the whole
capability (the caller can collapse the groups itself, with its own tie-breaking rules) at zero
contract cost.

`off` exists for callers that do not want the field computed at all. It is cheap either way —
`find_duplicates` is a single `HashMap` pass, O(items) — so `off` is about the response payload,
not CPU.

### 3. `drop_duplicates` collapses by re-derived key, not by `item.id`

`drop_duplicates` takes the groups as a list of **keys to collapse**, then re-derives each
surviving item's key with `dedup_key` and keeps the first occurrence of each targeted key. It
deliberately does **not** work from the `item_ids` the groups carry.

This is not defensive coding; an id-based removal is actively wrong. `identity.rs` derives `id`
from link → guid → title|published, while `dedup_key` prefers guid → url → content_hash. **The
two preferences disagree.** A feed whose entries all carry the same `<link>` gives every one of
its items the same `id`, while their differing guids place them in different groups (or in
none at all). Removing "the ids after the first" then deletes an item no group ever named and
leaves the real duplicate in place — and because every derived count is refreshed afterwards,
the output stays *internally consistent*, so no count assertion catches it. Only an identity
assertion does. Pinned by
`core::tests::drop_duplicates_removes_by_key_not_by_id_when_ids_collide`.

Re-deriving cannot make that mistake, and for any group `find_duplicates` actually produced,
the survivor is exactly the `item_ids[0]` it designated canonical. A stale group is harmless:
a key with no occurrence matches nothing, and one with a single occurrence keeps it.

### 4. Keyword filtering is mechanical, local, and applied before `limit`

`query.rs` implements the whole grammar: whitespace-separated terms all AND-ed,
`"quoted phrases"` matching as a unit, `-term` negating, case-insensitive substring matching
over each item's `title`, `summary`, and `content`. No regex, no ranking, no stemming, no
field-spanning phrases — each field is matched on its own, never concatenated.

This does **not** conflict with [ADR-0007](0007-no-builtin-llm-summarization.md), which rejects
built-in *LLM summarization*. That rejection is about not embedding a model, a prompt, and an
API key in a fetch tool. Substring matching is deterministic, offline, has no model behind it,
and returns the same answer on every run — it is the same category of thing as `--since`.

Filtering runs in `parse::parse_feed` in the order **`since` → `query` → sort → `limit`**, so
`limit` means "N matching items", not "N items, some of which match". A caller asking for 10
items about Rust gets 10 if the feed window holds 10, rather than 10 candidates of which 2 match.
Pinned by `parse::tests::query_filters_before_limit`.

The cost is real and bounded: content extraction runs on items the query then discards, because
the query matches the item **as it will be returned** — post-extraction and post-truncation. That
ordering is itself a decision. Matching pre-extraction HTML would search markup (`<a href>`
targets, class names) and give false positives no user could explain; matching post-truncation
means the searchable surface shrinks with `max_content_chars` and vanishes under
`--content none`. We chose "what you search is what you get" and documented the interaction on
`FetchParams::query` rather than adding a second, invisible extraction pass. The waste is capped
by the feed window, which the origin already bounds.

`applied_filters` reports the resolved `since`, the query as supplied, and the combined number
of items removed — `null` when neither filter ran (and when a query parses to no terms at all,
like `" "` or `"-"`, which constrains nothing). Without it, "the feed had nothing new" and "my
query was too narrow" are the same empty response.

### 5. `dedupe: "drop"` is rejected together with `cursor`

Deduping is a **whole-result** operation, but a continuation page fetches only
`urls[start_feed..]` ([ADR-0017](0017-batch-fetch-and-cursor-pagination.md) §3). A duplicate
whose canonical copy lived in an earlier feed is invisible to page 2, so the surviving copy
becomes canonical there and ships — putting the same article on both pages, which is the exact
outcome the caller asked to prevent. `fetch_feed` returns a `USAGE_ERROR` naming the
interaction; that is cheaper and more honest than silently degrading, and the message points at
the workaround (`report`, plus the caller's own collapse).

The rejection is symmetric: `drop` may not *redeem* a cursor, and a `drop` page does not *mint*
one either. A first `drop` page is a perfectly legal call, so it reaches the fingerprint and
could otherwise hand back a continuation stamped `drop` — a token no later call can redeem
(as `drop` it hits the guard, as anything else it fails the fingerprint check), and one whose
`i`/`n` index the post-removal item list while a continuation would refetch without the
removal. So an over-budget `drop` page ships with `next_cursor: null` and a `suggestion` naming
the two ways forward: page under `report` and collapse the duplicates caller-side, or keep
`drop` and shrink the batch.

`report` pages without any such problem, because it removes nothing. `dedupe` occupies a slot in
the cursor fingerprint because a cursor minted under `off` must not be served under `report` —
the two produce different responses for the same position. Because the fingerprint hashes the
**canonical** spelling and the default is `"report"`, this slot is byte-identical to the fixed
placeholder ADR-0017 reserved for it — so, unlike `query`, populating it invalidated no
outstanding cursor.

## Consequences

- `SCHEMA_VERSION` stays `"1"`. `duplicates[]` and `applied_filters` are additive fields on
  `FetchOutput`; `dedupe` and `query` are arguments, which are not part of the output contract
  at all ([ADR-0006](0006-ai-facing-output-contract.md)).
- `dedupe: "drop"` is the only thing here that changes `item_count`, and only when asked for.
  Everything else is observationally identical to the previous release for a caller that passes
  neither argument — except that `duplicates[]`, always present as `[]`, may now be non-empty.
- **`drop` runs after `limit`, so a feed can return fewer items than its cap.** `limit` is a
  per-feed cap applied in `parse_feed`; deduping is a cross-batch step over the assembled
  result, which has no per-feed meaning to top back up. `--limit 25 --dedupe drop` therefore
  means "up to 25 per feed, minus whatever an earlier feed already delivered", not "25 distinct
  items per feed". Widening the fetch to compensate would fetch more than the caller asked for
  on the guess that some of it will be dropped.
- **`duplicates[]` on a paged response describes that page's fetch only.** Grouping runs before
  `core::paginate`, because `duplicates[]` is part of the payload the budget measures — computing
  it after would let a page ship over budget. The consequence is that a `report` group can name
  an item the page budget then trimmed off. That item ships on the next page **without** its
  group: a continuation fetches only `urls[start_feed..]` and drains the items already
  delivered, so the copy it would be grouped with is not in view. Each group is therefore
  reported by exactly one page — the page that fetched both copies — and a caller reconciling a
  paged batch must keep the groups from every page, not just the last. Recomputing after
  `paginate` would not fix this (it would lose the group altogether) and would break `drop`,
  whose groups are deliberately an audit trail of items that are already gone. Like
  `truncation.items_omitted` and `applied_filters.items_filtered_out`, these are per-page
  snapshots and must not be summed across pages.
- **`duplicates[]` is charged against the MCP response budget**, since it is part of the
  measured payload. A group costs on the order of a short item, so a heavily overlapping batch
  (many URLs, each item syndicated several times) pages sooner than the same batch would with
  `dedupe: "off"` — and in the pathological case where the groups alone exceed
  `max_response_tokens`, `paginate` reports `RESPONSE_TOO_LARGE` ("an envelope with nothing
  left to shed"). Both recoveries the error already suggests work: a smaller `limit` yields
  fewer items and therefore fewer groups, and `dedupe: "off"` removes the field's cost
  entirely.
- **`off` is indistinguishable from "report found nothing"** — both leave `duplicates: []`, and
  there is no `Option` marker the way `applied_filters` gives `query` one. Accepted rather than
  fixed: making `duplicates` nullable would change an existing field's type (a breaking change
  requiring a `SCHEMA_VERSION` bump) to disambiguate a state the caller itself chose by passing
  `off`.
- Filtering shrinks responses in the common case, which interacts *well* with the response
  budget: fewer matching items means fewer pages. It does not shrink the *fetch* — every item in
  the window is still downloaded, parsed, and extracted before being discarded.
- A `content_hash`-keyed group may be a false positive. This is reported, never silently
  filtered, and is only reachable when both `guid` and `url` are absent.

## Alternatives considered

- **Dedup on `item.id`.** Rejected: `id` is namespaced by `feed_url`
  ([ADR-0003](0003-deterministic-content-hash-item-ids.md)), so it groups nothing across feeds
  — the only case that matters. Changing `id` to be feed-URL-independent was already rejected by
  [ADR-0014](0014-get-item-cache-first-multi-key-lookup.md) on separate grounds (invariant 4,
  plus guid instability across fetches).
- **Remove duplicates by default.** Rejected: it silently changes `feeds[]`, per-feed
  `item_count`, and the item sequence for every existing caller, to solve a problem only some of
  them have. Reporting gives the same information at no contract cost, and `drop` is one
  argument away.
- **Remove by `item_ids` set membership** (or a per-id removal budget). Rejected, and this is
  the subtle one — see Decision 3. `id` and `dedup_key` disagree about what makes two items the
  same, so an id-driven removal can delete an item no group named while leaving the real
  duplicate in place, with every count staying internally consistent.
- **Silently degrade `drop` on a continuation page** (dedupe within the page only). Rejected:
  it produces the exact failure the caller used `drop` to avoid — the same article on two pages
  — and does so invisibly. A `USAGE_ERROR` costs one retry and explains itself.
- **A cross-request duplicate memory** (remember keys already shipped, so paged and repeated
  calls stay deduped). Rejected for the same reason ADR-0017 rejected a server-side pagination
  store: it needs an eviction policy a stateless server does not have, does not survive a
  restart, and leaks state across unrelated callers.
- **Proxying provider-specific search syntax** (pass `q=` through to feeds that support it).
  Rejected: it works on a handful of hosts, silently does nothing on the rest, and the caller
  cannot tell which case it is in. A local filter behaves identically everywhere.
- **Regex, ranking, or stemming in `query`.** Rejected for v1. Regex invites catastrophic
  backtracking on caller-supplied input; ranking implies a scoring model to explain and tune;
  stemming is language-specific. The grammar we shipped is the subset that is obvious in every
  language and needs no documentation beyond one sentence.
- **Filter after `limit`.** Rejected: `--limit 10 --query rust` would return "at most 10 of the
  newest items, of which some match" — usually far fewer than 10 results, with no way for the
  caller to ask for more without guessing a larger limit.
