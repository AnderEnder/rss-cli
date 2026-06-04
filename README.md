# rss-cli

[![build status](https://github.com/AnderEnder/rss-cli/workflows/Build/badge.svg)](https://github.com/AnderEnder/rss-cli/actions)
[![release status](https://github.com/AnderEnder/rss-cli/workflows/Release/badge.svg)](https://github.com/AnderEnder/rss-cli/actions)
[![crates.io](https://img.shields.io/crates/v/rss-cli.svg)](https://crates.io/crates/rss-cli)

A lightweight, cache-backed, **AI-friendly RSS / Atom / JSON Feed CLI**.

`rss` fetches and parses feeds on demand and emits clean, predictable **structured
JSON** designed for agents and scripts to consume directly. It is intentionally
*not* a feed reader: there is no subscription database, no background sync, and no
"unread" state. You ask for feeds, you get data — on request.

Key properties:

- **Data on request.** Every command reads inputs you pass and prints results to
  stdout. All logs and diagnostics go to stderr, so stdout is always clean,
  machine-parseable data.
- **Stable item ids.** Each item gets a deterministic id derived from its content
  and feed URL — identical across runs and machines when fetched via the same feed
  URL — so an agent can reference an item today and resolve the same item tomorrow
  (see [Stable item ids](#stable-item-ids)).
- **Cache-backed & polite.** A small file cache stores HTTP validators so repeat
  fetches use conditional GETs (`If-None-Match` / `If-Modified-Since`) and a `304`
  serves the cached body — saving bandwidth and being kind to servers.
- **Self-describing.** `rss schema` emits the JSON Schema of the output so an agent
  can learn the contract without guessing.
- **MCP-native.** `rss mcp` runs the same operations as a Model Context Protocol
  server over stdio, so AI clients can call them as tools.

---

## Install / build

**From crates.io** (recommended) — needs a stable Rust toolchain (edition 2024):

```sh
cargo install rss-cli      # builds and installs the `rss` binary onto your PATH
```

**Prebuilt binaries** — no toolchain required. Download the tarball for your platform from
the [latest release](https://github.com/AnderEnder/rss-cli/releases/latest), then put the
extracted `rss` binary on your `PATH`.

**From source:**

```sh
# build a release binary at target/release/rss
cargo build --release

# …or install onto your PATH from a local checkout
cargo install --path .
```

Then run the binary as `rss` (or `./target/release/rss`).

---

## Commands

Global flags (valid on every subcommand):

| Flag | Description |
|------|-------------|
| `--cache-dir <DIR>` | Override the cache directory. |
| `-q`, `--quiet` | Suppress all non-data output on stderr. |
| `-v`, `--verbose` | Increase log verbosity (repeatable: `-v` info, `-vv` debug, `-vvv` trace). |
| `--no-color` | Disable ANSI color in text output (also respects `NO_COLOR`). |

### `rss fetch` — fetch & parse feeds

```sh
# One feed as pretty JSON (the default format)
rss fetch https://example.com/feed.xml

# Several feeds, newest 5 items each, as newline-delimited JSON
rss fetch https://a.com/feed https://b.com/atom --limit 5 --format ndjson

# Read URLs from stdin, an OPML export, or a text file
cat urls.txt | rss fetch -
rss fetch --opml subscriptions.opml
rss fetch --input urls.txt

# Only items from the last 2 days, body as plain text
rss fetch https://example.com/feed.xml --since 2d --content text
```

Notable flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--format <json\|ndjson\|text>` | `json` | Output format. `json` is the full document; `ndjson` is one item per line; `text` is a human summary. |
| `--ndjson-records` | — | With `--format ndjson`, emit tagged `{type:item\|error\|summary}` records (keeps errors + totals in the stream) instead of bare items. |
| `--content <markdown\|text\|html\|none>` | `markdown` | How to render item bodies. `none` omits the body. |
| `--limit <N>` | — | Max items per feed (newest first). |
| `--max-content-chars <N>` | — | Truncate each item body to at most N characters (flagged `content_truncated`). Fetch many items while skipping giant bodies. |
| `--since <WHEN>` | — | Only items at/after a duration (`2h`, `7d`) or ISO date (`2026-06-01`). |
| `--concurrency <N>` | `8` | Max feeds fetched in parallel. |
| `--timeout <SECS>` | `30` | Per-request timeout. |
| `--no-cache` | — | Bypass the cache entirely (no read, no write). |
| `--max-age <DUR>` | — | Serve from cache without revalidating if the entry is younger than `DUR`. |
| `--refresh` | — | Force revalidation, ignoring `--max-age`. |
| `--user-agent <STRING>` | (tool default) | Override the `User-Agent` header. |

Inputs (positional URLs, `-` for stdin, `--input`, `--opml`) are merged and
de-duplicated while preserving order.

### `rss discover` — find feeds on a website

Scans a homepage's `<link rel="alternate">` tags for advertised feeds.

```sh
rss discover https://example.com
```

### `rss show` — show a single item (by id, guid, or URL)

```sh
rss show https://example.com/feed.xml --id 1a2b3c4d5e6f7a8b
```

The `--id` value may be the item's stable **id**, its raw **guid**, or its
permalink **URL** — `show` matches any of the three. Because ids are deterministic,
an id captured from an earlier `fetch` resolves to the same item later. The `id` is
namespaced by feed URL, so if you fetch the same post from two different feed URLs
(e.g. Reddit `?t=day` vs `?t=week`) the ids differ — use the **guid** (e.g. Reddit
`t3_…`) as the portable key across feed URLs.

Retrieval is **cache-first**: `show` serves the matching item from the cached feed
body without revalidating, so an item from a prior `fetch` survives a rolled feed
window — but **not** a later refetch that overwrote the cache with a newer window.
Pass `--refresh` to bypass the cache-first read and fetch the live feed instead
(which may miss items that have since rolled out of the feed window). See
[ADR-0014](docs/adr/0014-get-item-cache-first-multi-key-lookup.md).

### `rss schema` — emit the output JSON Schema

```sh
rss schema --command fetch      # schema for `rss fetch` output (default)
rss schema --command discover   # schema for `rss discover` output
```

The schema is generated from the output types themselves, so it is always an
accurate description of what the commands emit.

### `rss cache` — inspect / clear the cache

```sh
rss cache path     # print the cache directory
rss cache list     # list cached feeds (JSON)
rss cache clear    # remove all cache entries
```

By default the cache lives in the OS cache directory (e.g.
`~/Library/Caches/...` on macOS, `$XDG_CACHE_HOME/...` on Linux); override it with
`--cache-dir`.

### `rss mcp` — run as an MCP server

See [MCP server](#mcp-server) below.

---

## Output schema

`rss fetch --format json` prints a single `FetchOutput` document. Fields are a
**stable contract**: optional fields are always present, serialized as `null`
rather than omitted, so the shape is predictable across every item. The
authoritative schema is `rss schema --command fetch`.

```jsonc
{
  "schema_version": "1",
  "fetched_at": "2026-06-01T12:00:00Z",   // when this invocation ran (RFC-3339 UTC)
  "total_items": 1,                        // items across all feeds (after limit/--since)
  "total_content_tokens_est": 7,           // sum of items' content_tokens_est (budget against this)
  "feeds": [
    {
      "feed_url": "https://example.com/feed.xml",
      "status": "ok",                       // "ok" | "not_modified" | "error"
      "from_cache": false,                  // true when served from a cached body
      "title": "Example Feed",
      "site_url": "https://example.com/",
      "updated": "2026-06-01T12:00:00Z",
      "item_count": 1,                      // == items.length (explicit budgeting count)
      "content_tokens_est_total": 7,        // sum of this feed's items' content_tokens_est
      "items": [
        {
          "id": "1a2b3c4d5e6f7a8b",         // stable, deterministic id
          "id_source": "link",              // "link" | "guid" | "hash"
          "feed_url": "https://example.com/feed.xml",
          "title": "First Post",
          "url": "https://example.com/posts/first",   // resolved absolute permalink
          "authors": ["Alice"],
          "published": "2026-06-01T09:00:00Z",
          "updated": null,
          "summary": "A short summary.",
          "content": "The **first** post body.",       // in the requested format
          "content_format": "markdown",
          "content_tokens_est": 7,           // rough token estimate (reflects truncation)
          "content_truncated": false,        // true when the body was cut to a cap
          "content_hash": "5777e294c43119b6", // 16-hex SHA-256 of full body; null if no content
          "categories": ["news"],
          "enclosures": [],
          "guid": "urn:example:first"        // raw feed guid/id (may not be stable)
        }
      ],
      "error": null                          // populated when status == "error"
    }
  ],
  "errors": [],                              // feed-level errors mirrored here
  "warnings": [],                            // non-fatal data-quality notes (see below)
  "truncation": null                         // non-null when the result was size-bounded
}
```

`feeds[]` (and the mirrored `errors[]`) are emitted in **request order** — deterministic
within a run, so you can address a feed by position. (Output is not byte-identical across
runs: `fetched_at` and the cache status fields vary by design.) `content_hash` is a stable
hash of the full (pre-truncation) body, so an agent can tell whether an item's content
changed between fetches without diffing text.

**Warnings** are non-fatal notes, distinct from `errors` (which mean a feed failed). They are
kept rare so they stay meaningful — e.g. `CONTENT_EXTRACTION_FALLBACK` (HTML→Markdown
conversion failed and fell back to a tag strip) or `UNDATED_ITEMS` (every returned item lacks
a date, so newest-first ordering is best-effort). Each carries `{feed_url, code, message}`.

With `--format ndjson`, each line of stdout is a single `Item` object (each carries its own
`feed_url`); feed-level errors are reported on stderr. Pass `--ndjson-records` to instead emit
one **tagged** record per line — `{"type":"item", …}`, `{"type":"error", …}`, and a final
`{"type":"summary", total_items, total_content_tokens_est, warnings, truncation, …}` — so a
consumer reading only stdout keeps the errors and totals.

Errors are structured objects:

```jsonc
{
  "feed_url": "https://example.com/feed.xml",
  "code": "FEED_PARSE_FAILED",   // stable, machine-readable code
  "message": "feed parse error: …",
  "details": {}                   // extra context, e.g. {"http_status": 500}
}
```

---

## Freshness policy

The default policy is **always revalidate**. On every fetch `rss` sends a
conditional GET using the cached validators (`If-None-Match` from the stored
`ETag`, `If-Modified-Since` from `Last-Modified`):

- a `304 Not Modified` serves the cached body (`status: "not_modified"`,
  `from_cache: true`);
- a `200 OK` stores the new body and validators and returns the fresh content.

To trade freshness for speed, pass `--max-age <DUR>`: if the cached entry is
younger than `DUR` it is served directly **without any network call**. `--refresh`
forces revalidation even within `--max-age`, and `--no-cache` ignores the cache
entirely (no read, no write).

The cache exists only for conditional GETs and for resolving `show` lookups — it is
**not** what makes item ids stable.

---

## Provider notes (Reddit)

Reddit's feeds have a few quirks worth knowing when consuming them:

- **Comment feeds return one more item than `--limit`.** A subreddit comment `.rss`
  appends the original post (OP) to the comment listing, so a request for `N`
  comments can come back with `N + 1` items (the OP being the extra one).
- **Comment items have `published: null`.** They populate only `updated`, not
  `published`, so when time-filtering (e.g. `--since`) fall back to `updated` for
  these items.
- **`search.rss` is best-effort.** Results from Reddit's search feed are sparse and
  noisy by nature (Reddit-side), so treat a thin or empty result as expected rather
  than an error.
- **Transient `403`/`429` are retried once automatically.** Reddit intermittently
  rate-limits mid-batch; `rss` retries a `403`/`429` exactly once (honoring
  `Retry-After`, capped) before giving up. On persistent failure the
  `FEED_FETCH_FAILED` error surfaces `http_status` and (when sent) `retry_after` in
  its `details`. See
  [ADR-0015](docs/adr/0015-bounded-retry-on-transient-429-403.md).

---

## Stable item ids

GUIDs are unreliable in the wild (a large fraction of feeds regenerate them on
every fetch), so `rss` never trusts them for identity. Instead each item's `id` is
a **deterministic content hash**, computed the same way on every run and every
machine:

```text
key = first present of:  link  →  guid  →  (title + "|" + published)
id  = lowercase_hex( sha256( feed_url + "\n" + key ) )[..16]
```

`id_source` records which field supplied the key (`link`, `guid`, or `hash` for the
degenerate fallback). Because the id is derived purely from feed content — never
from the cache — the same item produces the same id across runs, machines, and
fresh caches, as long as the feed is fetched via the same `feed_url` (the id is
namespaced by it, so a mirror or an `http`/`https` variant produces different ids).
This is what lets an agent capture an id now and resolve it later via
`rss show … --id <id>`.

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | OK — all requested feeds succeeded. |
| `1` | Unexpected internal error. |
| `2` | Usage / argument error. |
| `3` | Partial — some feeds succeeded, some failed. |
| `4` | All requested feeds failed. |

A usage error (code `2`) prints a JSON error object to stderr and writes nothing to
stdout.

---

## MCP server

`rss mcp` runs the same core operations as a [Model Context Protocol](https://modelcontextprotocol.io)
server over **stdio**, so AI clients can call them as tools. stdout is the MCP
transport; all logging goes to stderr.

Exposed tools:

| Tool | Arguments | Returns |
|------|-----------|---------|
| `fetch_feed` | `url`; optional `content_format`, `limit` (default 25), `max_content_chars`, `max_response_tokens` | a `FetchOutput` for the feed |
| `discover_feeds` | `site_url` | discovered feeds |
| `get_item` | `feed_url`, `id`; optional `max_content_chars` | a single item, resolved cache-first by its `id`, raw `guid`, or permalink URL |
| `get_schema` | `command` | the JSON Schema for that command's output |

**Results are structured.** The data tools (`fetch_feed`, `get_item`, `discover_feeds`)
return their payload as MCP `structuredContent` matching an advertised `outputSchema`, with a
one-line text summary alongside (the full payload is *not* duplicated as text, which would
double the response). Every tool advertises annotations (`readOnlyHint`, `idempotentHint`,
and `openWorldHint` for the network-touching tools). See
[ADR-0013](docs/adr/0013-structured-mcp-tool-results.md).

**Responses are size-bounded.** AI clients reject oversized tool results, so `fetch_feed`
caps items (default 25) and checks an estimated-token budget (`max_response_tokens`). If a
result would overflow, the tool returns a structured **`RESPONSE_TOO_LARGE`** error whose
`details` include `suggested_limit` and `suggested_max_content_chars` — so the agent can
retry and self-recover instead of failing. Use `max_content_chars` to fetch many items
while truncating long bodies (each truncated item is flagged `content_truncated`), and
`get_item` to pull the full body of a specific item. All tool errors are structured JSON
matching the `ErrorObj` contract (a stable `code` plus `details`). See
[ADR-0011](docs/adr/0011-bounded-mcp-responses.md).

### Connecting a client

Most MCP clients share the same `mcpServers` JSON shape — only the file location
differs — so the snippet below is reusable. The command is always `rss mcp` with no
extra arguments. (Two clients use a different top-level key: VS Code and Zed, noted
below.)

**Claude Desktop** — edit `claude_desktop_config.json` (macOS:
`~/Library/Application Support/Claude/`, Windows: `%APPDATA%\Claude\`):

```json
{
  "mcpServers": {
    "rss": { "command": "rss", "args": ["mcp"] }
  }
}
```

**Claude Code** — add it from the CLI (no file editing):

```sh
claude mcp add rss -- rss mcp
# share it with a repo instead (writes .mcp.json):
claude mcp add --scope project rss -- rss mcp
```

**Cursor** — `~/.cursor/mcp.json` (global) or `.cursor/mcp.json` (per project). Same
`mcpServers` shape as the Claude Desktop snippet above.

**Windsurf** — `~/.codeium/windsurf/mcp_config.json`. Same `mcpServers` shape.

**VS Code** (Copilot agent mode) — `.vscode/mcp.json`, which uses a `servers` key
(not `mcpServers`):

```json
{
  "servers": {
    "rss": { "command": "rss", "args": ["mcp"] }
  }
}
```

**Zed** — `settings.json`, under a `context_servers` key:

```json
{
  "context_servers": {
    "rss": { "command": "rss", "args": ["mcp"], "env": {} }
  }
}
```

> **`PATH` note.** If `rss` isn't on the client's `PATH`, use the absolute path to the
> binary as `command` (for example `/Users/you/.cargo/bin/rss`, or the
> `target/release/rss` you built). GUI apps in particular often don't inherit your
> shell's `PATH`.

---

## Design & contributing

- **Why it's built this way** — the significant decisions (stable ids, the cache,
  the output contract, MCP-in-v1, the release profile) are recorded as
  [Architecture Decision Records](docs/adr/). Read these before changing anything
  load-bearing.
- **Working in the repo** — [CLAUDE.md](CLAUDE.md) has the build/test/lint gates,
  the module map, the invariants that must hold, and the gotchas that already bit.

---

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual
licensed as above, without any additional terms or conditions.
