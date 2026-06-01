# rss

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

Requires a stable Rust toolchain (edition 2024).

```sh
# Build a release binary at target/release/rss
cargo build --release

# …or install it onto your PATH
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
| `--content <markdown\|text\|html\|none>` | `markdown` | How to render item bodies. `none` omits the body. |
| `--limit <N>` | — | Max items per feed (newest first). |
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

### `rss show` — show a single item by its stable id

```sh
rss show https://example.com/feed.xml --id 1a2b3c4d5e6f7a8b
```

Because ids are deterministic, an id captured from an earlier `fetch` resolves to
the same item later.

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
  "feeds": [
    {
      "feed_url": "https://example.com/feed.xml",
      "status": "ok",                       // "ok" | "not_modified" | "error"
      "from_cache": false,                  // true when served from a cached body
      "title": "Example Feed",
      "site_url": "https://example.com/",
      "updated": "2026-06-01T12:00:00Z",
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
          "content_tokens_est": 7,           // rough token estimate for budgeting
          "categories": ["news"],
          "enclosures": [],
          "guid": "urn:example:first"        // raw feed guid/id (may not be stable)
        }
      ],
      "error": null                          // populated when status == "error"
    }
  ],
  "errors": []                               // feed-level errors mirrored here
}
```

With `--format ndjson`, each line of stdout is a single `Item` object (each carries
its own `feed_url`); feed-level errors are reported on stderr.

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
| `fetch_feed` | `url`, optional `content_format`, `limit`, `since` | a feed result (or full `FetchOutput` for multiple urls) |
| `discover_feeds` | `site_url` | discovered feeds |
| `get_item` | `feed_url`, `id` | a single item, resolved by its stable id |
| `get_schema` | `command` | the JSON Schema for that command's output |

Example MCP client configuration (e.g. Claude Desktop's `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "rss": {
      "command": "rss",
      "args": ["mcp"]
    }
  }
}
```

If `rss` is not on the client's `PATH`, use the absolute path to the binary (for
example `/Users/you/.cargo/bin/rss` or the `target/release/rss` you built).
