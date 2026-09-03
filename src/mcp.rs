//! Model Context Protocol server (stdio transport). **Owner: `mcp` agent.**
//!
//! Runs `rss mcp`: exposes the same core operations as MCP tools so agents can call the
//! tool directly. Implemented with `rmcp` 2.x (`#[tool]` / `#[tool_router]` macros,
//! `serve(stdio())`).
//!
//! ## Requirements
//! - Expose tools that delegate to [`crate::core`] (do **not** reimplement fetch/parse):
//!   - `fetch_feed { url, content_format?, limit?, since? }` → a single feed's `FeedResult`
//!     (or the full `FetchOutput` for multiple urls).
//!   - `discover_feeds { site_url }` → `DiscoverOutput`.
//!   - `get_item { feed_url, id }` → one `Item` (fetch + find by stable id).
//!   - `get_schema { command }` → the JSON Schema from [`crate::output::schema_for`].
//! - Tool results are JSON (serialize the model types).
//! - **All logging/diagnostics must go to stderr** — stdout is the MCP transport.
//! - Build the [`Cache`](crate::cache::Cache) once and share it across tool calls.

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

use crate::cache::Cache;
use crate::config::{CachePolicy, FetchParams};
use crate::core;
use crate::error::RssError;
use crate::fetch::HttpClient;
use crate::model::{ContentFormat, ErrorObj};
use crate::output;

/// Human-readable guidance surfaced to MCP clients during `initialize`.
const SERVER_INSTRUCTIONS: &str = "\
AI-friendly RSS/Atom tools. Every tool returns JSON matching the rss-cli output \
contract (use get_schema for authoritative shapes). fetch_feed retrieves and parses \
feeds; discover_feeds finds feeds advertised on a website; get_item returns a single \
item by its stable id, guid, or permalink; get_schema returns the JSON Schema for \
'fetch' or 'discover'.\n\
\n\
BATCHING AND PACING. Pass several feeds at once via fetch_feed's `urls` array (max 50) \
rather than one call per feed with your own delays -- the server serializes same-host \
requests and applies an adaptive cooldown honoring the origin's Retry-After, so it \
paces on your behalf; do NOT hand-roll sleeps. A throttled feed gets its own \
feeds[].error, not a failed call -- check each feed's status, not just the call's. \
FEED_FETCH_FAILED (an origin 429/403, the common case) carries details.retry_after, the \
origin's raw header, null if absent -- then back off yourself rather than assume none \
is needed. RATE_LIMITED (the server's own pacing ceiling, rare at default settings) \
carries retry_after_seconds/retry_after_ms instead, always populated; see fetch_feed's \
own description for exact fields. get_item/discover_feeds report the same codes as an \
actual tool failure, because each targets one feed or site. A batch that cannot finish \
inside the wall-clock deadline returns what it completed plus truncation.feeds_omitted \
-- follow truncation.next_cursor when present, or request the omitted feeds separately.\n\
\n\
CACHING. cache_policy controls the network call: revalidate (default) re-checks with \
whatever validators the cache holds (If-None-Match / If-Modified-Since) -- a 304 comes \
back as 'not_modified' with from_cache true, but a cold cache or an origin that sends \
no validators still gets a full fetch; no-cache always refetches in full and writes \
nothing, so it CANNOT BE PAGED (a bounded no-cache page comes back with next_cursor \
null -- use the default revalidate if the batch may need paging); cache-first \
serves any cached copy with no network call at all; max-age:<duration> (e.g. \
max-age:15m) does the same but only when the cache is younger than that duration. A \
continuation page (see `cursor`) forces cache-first for the whole page, so a feed \
already cached comes back not_modified/from_cache true whether or not it actually \
changed -- indistinguishable from the result alone. `since` (a duration like 2h/7d, or \
an ISO-8601 date) filters items before `limit`. Judge staleness via \
cached_at/cache_age_seconds -- a feed revalidated every call holds cache_age_seconds \
near its poll interval, not the body's true age.\n\
\n\
FILTERING AND DUPLICATES. `query` keyword-filters items (AND-ed terms, \"quoted \
phrases\", -term excludes) before `limit`, and applied_filters reports what it removed. \
`dedupe` handles one entry arriving from several feeds: report (the default) groups the \
copies in duplicates[] and removes nothing, off skips detection, drop also removes the \
later copies and so lowers each feed's item_count. drop cannot be paged in either \
direction -- it is rejected together with `cursor`, and an over-budget drop page returns \
next_cursor null -- because a continuation page cannot see canonical copies from earlier \
pages. Page under report and collapse the duplicates on your side. duplicates[] covers \
what THAT page fetched, so across pages a group may appear on several of them or on \
none -- merge by key/key_kind, never append or sum.\n\
\n\
SIZE LIMITS AND PAGING. Responses are size-bounded. fetch_feed caps items per feed \
(default 25) and fills up to max_response_tokens; whatever did not fit is reported in \
truncation (items_omitted, feeds_omitted -- both PER PAGE, not cumulative: do not sum \
them across pages) with truncation.next_cursor. Pass that token back as `cursor` WITH \
THE SAME ARGUMENTS for the next page: a feed already fetched costs nothing, but one the \
batch deadline never reached still needs a live fetch. A single item too large on its \
own still returns RESPONSE_TOO_LARGE with suggested_limit / \
suggested_max_content_chars.";

/// Default item cap `fetch_feed` applies when the caller passes no `limit`. Bounds the common
/// "too many items" blow-up (e.g. a hot post's comment feed) without the caller opting in.
const MCP_DEFAULT_LIMIT: usize = 25;

/// Default response budget (estimated tokens) when the caller passes no `max_response_tokens`.
/// Conservative headroom under typical MCP client tool-result limits; the `ceil(chars/4)`
/// estimate (over *pretty* JSON) errs toward over-counting. Overridable per call.
///
/// Calibrated down from an initial 20k after a field report: a full-content 25-item feed
/// (~50–60 KB) slipped under the 20k budget yet still tripped the client's own tool-result
/// limit, which then dumped the payload to a temp file. 10k (~40 KB) keeps the structured
/// `RESPONSE_TOO_LARGE` self-recovery path (ADR-0011) firing *before* the client truncates,
/// while staying generous for ordinary feeds. This is a heuristic, not a measured universal
/// client limit — callers with a different limit pass `max_response_tokens` explicitly.
const MCP_DEFAULT_MAX_RESPONSE_TOKENS: usize = 10_000;

/// Wall-clock budget for one batched `fetch_feed` call. Chosen to land under a typical MCP
/// client tool timeout, in the same spirit as the `MCP_DEFAULT_MAX_RESPONSE_TOKENS`
/// recalibration above: a heuristic, not a measured universal limit — hence the override.
///
/// It exists because same-host feeds serialize (`HOST_MAX_CONCURRENCY = 1`) and the gate's
/// `MAX_GATE_WAIT` bounds only the pacing sleep, not the wait for a host permit. Without a
/// wall-clock bound a large same-host batch can outlive the client's tool timeout and return
/// *nothing* — strictly worse than returning the feeds that did finish plus a cursor.
const MCP_BATCH_DEADLINE_SECS: u64 = 25;

/// Environment override for [`MCP_BATCH_DEADLINE_SECS`], in whole seconds.
const BATCH_DEADLINE_ENV: &str = "RSS_MCP_BATCH_DEADLINE_SECS";

/// Resolve the batch deadline from a raw override string. Split from [`batch_deadline`] so the
/// policy is testable without mutating process-global environment state.
///
/// Absent, blank or unparseable input falls back to the default: a typo must not silently
/// remove the bound. `0` is a *valid* value meaning "already expired" — to run without a
/// deadline, unset the variable rather than zeroing it.
///
/// Zero is for tests and load-shedding, not for paginating: page 1 attempts nothing, so with
/// `feeds[]` empty there is no fetched prefix to resume past and the caller gets
/// `feeds_omitted > 0` with `next_cursor: null`.
fn parse_batch_deadline(raw: Option<&str>) -> std::time::Duration {
    raw.and_then(|s| s.trim().parse::<u64>().ok()).map_or_else(
        || std::time::Duration::from_secs(MCP_BATCH_DEADLINE_SECS),
        std::time::Duration::from_secs,
    )
}

/// The batch deadline in force for this process.
fn batch_deadline() -> std::time::Duration {
    parse_batch_deadline(std::env::var(BATCH_DEADLINE_ENV).ok().as_deref())
}

/// Suggestion attached to a page that was bounded by `max_response_tokens`. Pulled out as a
/// constant because [`CURSOR_HEADROOM_TOKENS`] measures it — the reserve has to track the
/// text, not a number someone remembered to update.
const PAGINATION_SUGGESTION: &str = "response was bounded by max_response_tokens; pass \
     truncation.next_cursor back as `cursor` with the same arguments for the next page -- \
     a feed already fetched costs nothing, but a feed the batch deadline never reached \
     still needs a live fetch";

/// Suggestion attached when per-item content was capped by `max_content_chars`. A page can be
/// both content-truncated *and* paginated, so this is combined with [`PAGINATION_SUGGESTION`]
/// rather than replaced by it — see [`combined_suggestion`].
const CONTENT_TRUNCATION_SUGGESTION: &str =
    "content was truncated; call get_item for the full body of a specific item";

/// The summary-line clause for a content-truncated page. A `const` because it is emitted in the
/// `CallToolResult`'s text alongside the payload, and [`CURSOR_HEADROOM_TOKENS`] reserves for it.
const CONTENT_TRUNCATED_NOTE: &str = " Content truncated (see structuredContent.truncation).";

/// The summary-line clause for a paginated page. See [`CONTENT_TRUNCATED_NOTE`].
///
/// "items or feeds" because both bounds mint this cursor: the response budget defers items,
/// while the batch deadline defers whole feeds it never attempted — a page bounded only by the
/// deadline has every item of every feed it shipped.
const MORE_ITEMS_NOTE: &str = " More items or feeds remain: pass truncation.next_cursor back \
     as `cursor` for the next page.";

/// Suggestion for an over-budget page that gets no cursor because `dedupe='drop'` cannot be
/// paged (ADR-0018). Kept shorter than [`PAGINATION_SUGGESTION`] on purpose: it is emitted in
/// place of that clause, on a marker that also carries no cursor, so
/// [`CURSOR_HEADROOM_TOKENS`]'s reserve — sized from [`worst_case_marker`], which has both a
/// full cursor and the longer clause — still covers it. Pinned by
/// `the_drop_clause_fits_inside_the_reserved_headroom`.
const DROP_NOT_PAGEABLE_SUGGESTION: &str = "dedupe='drop' cannot be paged, so this over-budget page has no cursor; re-run with \
     dedupe='report' and page from there (collapsing duplicates yourself), or keep 'drop' and \
     shrink the batch";

/// Suggestion for a bounded page that gets no cursor because `cache_policy='no-cache'` cannot
/// be paged: paging reads the body cache, and `no-cache` refuses to write it. Length-bounded
/// for the same reason as [`DROP_NOT_PAGEABLE_SUGGESTION`].
const NO_CACHE_NOT_PAGEABLE_SUGGESTION: &str = "cache_policy='no-cache' cannot be paged (paging reads the body cache, which no-cache does \
     not write), so this bounded page has no cursor; re-run with the default revalidate, which \
     refetches and persists, or shrink the batch";

/// Append `addition` to whatever suggestion the marker already carries, so a page that is both
/// content-truncated and bounded keeps both hints instead of losing the earlier one.
fn join_suggestion(existing: Option<&str>, addition: &str) -> String {
    match existing {
        Some(s) if !s.is_empty() => format!("{s}; {addition}"),
        _ => addition.to_string(),
    }
}

/// Join whatever suggestion the content-truncation marker already carries with the pagination
/// advice, so a page that is both content-truncated and bounded keeps both hints.
fn combined_suggestion(existing: Option<&str>) -> String {
    join_suggestion(existing, PAGINATION_SUGGESTION)
}

/// The widest [`crate::model::TruncationInfo`] a paginated response can carry: every numeric
/// field at its maximum, a full-length cursor, and the widest suggestion text (both clauses).
fn worst_case_marker() -> crate::model::TruncationInfo {
    crate::model::TruncationInfo {
        applied_limit: Some(usize::MAX),
        items_content_truncated: usize::MAX,
        items_omitted: usize::MAX,
        feeds_omitted: usize::MAX,
        next_cursor: Some(
            crate::cursor::Cursor {
                v: u8::MAX,
                fp: "f".repeat(16),
                f: usize::MAX,
                i: usize::MAX,
                n: usize::MAX,
                s: Some(i64::MIN),
            }
            .encode(),
        ),
        estimated_tokens: Some(usize::MAX),
        suggestion: Some(combined_suggestion(Some(CONTENT_TRUNCATION_SUGGESTION))),
    }
}

/// Tokens to hold back from `max_response_tokens` before calling [`core::paginate`].
///
/// `paginate` measures the page *as it stands*: the `truncation` marker and `next_cursor`
/// attached afterwards are not counted, nor is the summary clause [`truncation_note`] adds to
/// the result's *text* — so a page filled exactly to the budget would ship over it. Measured
/// rather than hardcoded: the dominant terms are the suggestion texts, the encoded cursor and
/// the notes, all of which move when the code does. Computed once — the inputs are constants.
/// Pinned by `cursor_headroom_covers_the_marker_it_reserves_for`.
static CURSOR_HEADROOM_TOKENS: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    // `+ 8` for the two extra spaces of indentation each of the marker's lines picks up once
    // nested inside `FetchOutput`. The `"truncation":` key itself needs no reserve: it was
    // already measured (as `null`) in the page `paginate` saw.
    let marker = serde_json::to_string_pretty(&worst_case_marker())
        .map(|s| s.chars().count().div_ceil(4))
        .unwrap_or(0)
        + 8;
    // The emitted result also carries the summary text, which `estimate_response_tokens` never
    // sees. Only the truncation clauses are reserved here: they exist exactly on the pages that
    // sit at the ceiling.
    let note =
        (CONTENT_TRUNCATED_NOTE.chars().count() + MORE_ITEMS_NOTE.chars().count()).div_ceil(4);
    marker + note
});

/// Deserialize an optional `usize` that may arrive as a JSON **number** *or* a JSON
/// **string** (`25` or `"25"`); `null`/absent both map to `None`. Many MCP clients serialize
/// every tool-call argument as a string, so a plain `Option<usize>` rejects `"25"` with
/// `invalid type: string "25", expected usize` and makes `limit` / `max_content_chars` /
/// `max_response_tokens` unusable from those clients. We still advertise `integer` in the
/// tool schema (schemars reads the field *type*, not this attribute) but accept either form —
/// liberal in what we accept, strict in what we advertise.
///
/// Do **not** "simplify" the annotated fields back to a bare `Option<usize>`: that
/// reintroduces the string-rejection bug for every client that stringifies arguments.
fn de_lenient_opt_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => {
            if let Some(u) = n.as_u64() {
                usize::try_from(u)
                    .map(Some)
                    .map_err(|_| Error::custom(format!("integer {u} is out of range")))
            } else if let Some(f) = n
                .as_f64()
                .filter(|f| f.is_finite() && *f >= 0.0 && f.fract() == 0.0)
            {
                Ok(Some(f as usize))
            } else {
                Err(Error::custom(format!(
                    "expected a non-negative integer, got {n}"
                )))
            }
        }
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            trimmed.parse::<usize>().map(Some).map_err(|_| {
                Error::custom(format!(
                    "expected a non-negative integer (or its string form), got {s:?}"
                ))
            })
        }
        Some(other) => Err(Error::custom(format!(
            "expected an integer or numeric string, got {other}"
        ))),
    }
}

/// Maximum URLs accepted by a single `fetch_feed` call. Bounds the blast radius of one
/// request; larger sets should page via `truncation.next_cursor`.
const MCP_MAX_URLS: usize = 50;

/// Deserialize an optional list of URLs that may arrive as a JSON **array**, a JSON
/// **string containing an array**, a newline-separated **string**, a comma-separated
/// **string** (only when every resulting piece is itself an absolute `http(s)` URL — a
/// bare comma-separated list would otherwise clash with commas inside a query string), or
/// a single bare URL string. Clients that stringify every tool argument (see
/// `de_lenient_opt_usize`) do the same to arrays, so a bare `Option<Vec<String>>` rejects
/// `"[\"a\"]"`.
///
/// Empty entries are dropped; an empty result maps to `None` ("not supplied"). The tool
/// schema still advertises `array of string` — schemars reads the field *type*.
fn de_lenient_url_list<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    fn clean(items: Vec<String>) -> Option<Vec<String>> {
        let out: Vec<String> = items
            .into_iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        (!out.is_empty()).then_some(out)
    }

    fn strings_from_json_array<E>(items: Vec<serde_json::Value>) -> Result<Vec<String>, E>
    where
        E: serde::de::Error,
    {
        items
            .into_iter()
            .map(|v| match v {
                serde_json::Value::String(s) => Ok(s),
                other => Err(E::custom(format!(
                    "expected a string URL in the list, got {other}"
                ))),
            })
            .collect()
    }

    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Array(items)) => Ok(clean(strings_from_json_array(items)?)),
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            // A stringified JSON array from a client that serializes everything as text.
            if trimmed.starts_with('[')
                && let Ok(serde_json::Value::Array(items)) =
                    serde_json::from_str::<serde_json::Value>(trimmed)
            {
                return Ok(clean(strings_from_json_array(items)?));
            }
            // Otherwise: newline-separated, comma-separated, or a single bare URL.
            // Newlines can never appear inside a URL, so splitting on them is always safe.
            let lines: Vec<String> = trimmed.split('\n').map(|s| s.to_string()).collect();
            // Commas *can* appear inside a URL's query string (`?ids=1,2,3`), so only treat
            // a comma as a separator when every resulting piece is itself an absolute
            // http(s) URL. Otherwise the comma belongs to the URL and splitting would
            // silently fetch a truncated one.
            let comma_split: Vec<String> = lines
                .iter()
                .flat_map(|line| line.split(',').map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect();
            let all_absolute = comma_split.iter().all(|s| {
                let lower = s.to_ascii_lowercase();
                lower.starts_with("http://") || lower.starts_with("https://")
            });
            if all_absolute && !comma_split.is_empty() {
                Ok(clean(comma_split))
            } else {
                Ok(clean(lines))
            }
        }
        Some(other) => Err(Error::custom(format!(
            "expected an array of URLs or a delimited string, got {other}"
        ))),
    }
}

/// Resolve the effective URL list from the mutually exclusive `url` / `urls` arguments.
///
/// `url` is retained alongside `urls` for backward compatibility: renaming it would break
/// every existing client configuration.
fn resolve_urls(args: &FetchFeedArgs) -> Result<Vec<String>, RssError> {
    let missing = || {
        RssError::Usage(
            "missing required argument: pass 'url' (one feed) or 'urls' (many)".to_string(),
        )
    };
    let list = match (&args.url, &args.urls) {
        (Some(_), Some(_)) => {
            return Err(RssError::Usage(
                "pass either 'url' (one feed) or 'urls' (many), not both".to_string(),
            ));
        }
        (Some(u), None) => {
            let trimmed = u.trim();
            if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![trimmed.to_string()]
            }
        }
        (None, Some(list)) => list.clone(),
        (None, None) => return Err(missing()),
    };
    // Guarded here, not borrowed from the deserializer: callers index `list[0]`.
    if list.is_empty() {
        return Err(missing());
    }
    if list.len() > MCP_MAX_URLS {
        return Err(RssError::Usage(format!(
            "too many feeds: {} exceeds the per-call cap of {MCP_MAX_URLS}; split the request",
            list.len()
        )));
    }
    Ok(list)
}

// === Tool argument structs (deserialized from MCP `arguments`; schema'd for clients) ===

/// Arguments for the `fetch_feed` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct FetchFeedArgs {
    /// A single RSS/Atom feed URL. Mutually exclusive with `urls`; supply exactly one.
    #[serde(default)]
    url: Option<String>,
    /// Feed URLs to fetch in one call (max 50). The server paces requests per host, so
    /// prefer one batched call over several calls with your own delays between them.
    #[serde(default, deserialize_with = "de_lenient_url_list")]
    urls: Option<Vec<String>>,
    /// Content extraction format. Defaults to `markdown`.
    #[serde(default)]
    #[schemars(extend("enum" = ["markdown", "text", "html", "none", null]))]
    content_format: Option<String>,
    /// Only include items published at or after this time: a duration (`2h`, `7d`) or an
    /// ISO-8601 date/datetime (`2026-06-01`). Applied before `limit`.
    #[serde(default)]
    since: Option<String>,
    /// Keyword filter over each item's title, summary, and content. Space-separated terms
    /// are AND-ed, `"quoted phrases"` match as a unit, `-term` excludes. Applied before
    /// `limit`, so `limit` means "N matching items".
    //
    // A plain `Option<String>` needs no `de_lenient_*` helper, unlike the numeric fields:
    // clients that stringify every argument already send a string for this one. Kept as a
    // non-doc comment deliberately — it is crate-internal reasoning, and a doc comment here
    // ships in the generated `inputSchema` to every client on every session.
    #[serde(default)]
    query: Option<String>,
    /// Cross-feed duplicate handling: `report` (the default — group repeated entries in
    /// `duplicates[]` and change nothing else), `off` (skip detection), or `drop` (also remove
    /// the later copies, which lowers each feed's `item_count`). `drop` cannot be combined
    /// with `cursor`.
    //
    // Plain `Option<String>` for the same reason as `query`; see the note there.
    #[serde(default)]
    #[schemars(extend("enum" = ["report", "off", "drop", null]))]
    dedupe: Option<String>,
    /// Maximum number of items to return (most recent first). Omit to use the default cap
    /// of 25; pass a larger number to fetch more (subject to the response budget).
    #[serde(default, deserialize_with = "de_lenient_opt_usize")]
    limit: Option<usize>,
    /// Maximum characters of extracted content per item; longer bodies are truncated on a
    /// char boundary and flagged `content_truncated`. Omit to keep full content.
    #[serde(default, deserialize_with = "de_lenient_opt_usize")]
    max_content_chars: Option<usize>,
    /// Soft cap on response size in estimated tokens. If the result would exceed it, the
    /// tool returns a RESPONSE_TOO_LARGE error with suggested `limit`/`max_content_chars`
    /// instead of an oversized payload. Omit to use the default budget.
    #[serde(default, deserialize_with = "de_lenient_opt_usize")]
    max_response_tokens: Option<usize>,
    /// Cache behavior: `revalidate` (default — re-checks with cached validators, cheap when
    /// unchanged, but a cold cache or a validator-less origin gets a full fetch), `no-cache`
    /// (always refetch), `cache-first` (serve any cached copy without a network call), or
    /// `max-age:<duration>` (serve cache younger than e.g. `15m`). Ignored on a continuation
    /// call (see `cursor`), which is always served cache-first.
    //
    // A `pattern`, not an `enum`: `max-age:<duration>` is open-ended, so a closed member list
    // would advertise it as invalid. `config::parse_cache_policy` stays deliberately more
    // lenient than this (blank, mixed case, surrounding whitespace) — advertise the canonical
    // grammar, accept sloppy input.
    //
    // Kept to plain ECMA-262: JSON Schema `pattern` has no inline-flag syntax, so an `(?i)`
    // prefix is not case-insensitivity here — it is a literal that makes the regex match
    // nothing in a strict validator.
    #[serde(default)]
    #[schemars(extend("pattern" = r"^(revalidate|no-cache|cache-first|max-age:\S+)$"))]
    cache_policy: Option<String>,
    /// Opaque continuation token from a prior response's `truncation.next_cursor`. Pass it
    /// back with the SAME arguments to get the next page. Continuation pages resume
    /// cache-first: a feed already fetched in a prior page costs nothing, but a feed the
    /// batch deadline never reached still needs a live fetch.
    #[serde(default)]
    cursor: Option<String>,
}

/// Arguments for the `discover_feeds` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct DiscoverFeedsArgs {
    /// The website URL to scan for advertised feeds.
    site_url: String,
}

/// Arguments for the `get_item` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct GetItemArgs {
    /// The feed URL that contains the item.
    feed_url: String,
    /// The item key: its stable `id`, raw `guid` (e.g. Reddit `t3_…`/`t1_…`), or permalink
    /// URL. A guid is the reliable key across different feed URLs, since `id` is namespaced
    /// by `feed_url`. Served cache-first: an item from a prior `fetch_feed` survives a rolled
    /// feed window, but not a later refetch that overwrote the cache.
    id: String,
    /// Maximum characters of extracted content; a longer body is truncated and flagged.
    /// Omit for full content (use this if a single large item is rejected as too large).
    #[serde(default, deserialize_with = "de_lenient_opt_usize")]
    max_content_chars: Option<usize>,
}

/// Arguments for the `get_schema` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct GetSchemaArgs {
    /// Which command's output schema to return.
    #[schemars(extend("enum" = ["fetch", "discover"]))]
    command: String,
}

/// MCP server state: the shared, cheaply-cloneable HTTP cache and HTTP client plus the
/// generated tool router. Built once in [`serve_stdio`] and shared across all tool calls —
/// sharing the client is what lets concurrent `fetch_feed` calls coordinate their per-host
/// pacing (ADR-0016); a fresh client per call could not.
#[derive(Clone)]
struct RssServer {
    cache: Cache,
    http: HttpClient,
    tool_router: ToolRouter<Self>,
}

impl RssServer {
    fn new(cache: Cache, http: HttpClient) -> Self {
        Self {
            cache,
            http,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl RssServer {
    /// Fetch and parse RSS/Atom feeds. Returns the full `FetchOutput` (one entry per URL in
    /// `feeds`), so feed-level errors (including a rate-limit or HTTP failure on one feed of
    /// a batch) surface as a `FeedStatus::Error` entry in `feeds[]`/`errors[]` rather than a
    /// tool failure — args are `{ url | urls[], content_format?, since?, query?, dedupe?,
    /// limit?, max_content_chars?, max_response_tokens?, cache_policy?, cursor? }`.
    #[tool(
        description = "Fetch and parse RSS/Atom feeds. Pass one `url` OR a `urls` array (max \
        50) -- batch rather than looping, because the server paces per host for you. Returns \
        FetchOutput as structured content plus a one-line summary; schema: get_schema \
        command=fetch. content_format is markdown|text|html|none. `limit` caps items PER \
        FEED, newest first (DEFAULT 25); `since` accepts a duration (2h, 7d) or an ISO-8601 \
        date and, like `query`, is applied before limit. `query` keyword-filters each item's \
        title/summary/content: space-separated terms AND, \"quoted phrases\" match as a \
        unit, `-term` excludes. Either filter's removals are reported in \
        structuredContent.applied_filters (null when neither was supplied). `dedupe` groups \
        the same entry arriving from several feeds: report (DEFAULT) lists the copies in \
        structuredContent.duplicates and removes nothing; off skips detection; drop also \
        removes the later copies, lowering each feed's item_count. drop CANNOT BE PAGED at \
        all: it is REJECTED with a `cursor`, and an over-budget drop page comes back with \
        truncation.next_cursor null (a continuation page cannot see canonical copies from \
        earlier pages). Page under report and collapse the duplicates yourself. \
        max_content_chars truncates each body (flagged \
        content_truncated). `cache_policy` is revalidate (default; re-checks with cached \
        validators, cheap when unchanged, but a cold cache or validator-less origin is a \
        full fetch) | no-cache (writes nothing, so it CANNOT BE PAGED -- a bounded page comes \
        back with next_cursor null) | cache-first (serves any cached copy, regardless of age) | \
        max-age:<duration> (serves a copy younger than that duration); every result carries \
        from_cache, cached_at, and cache_age_seconds so you can judge staleness (cached_at and \
        cache_age_seconds are null when not served from cache; a cleanly revalidating feed \
        holds cache_age_seconds near its poll interval, not the body's true age). Over-budget \
        results are PAGED, not rejected: check truncation.next_cursor and pass it back as \
        `cursor` with identical arguments -- a feed already fetched costs nothing, but one \
        the batch deadline never reached still needs a live fetch. \
        truncation.items_omitted/feeds_omitted describe only this page, not a running total. \
        A lone oversized item still returns RESPONSE_TOO_LARGE with \
        suggested_max_content_chars (a tool-level failure). If the host's pacing ceiling is \
        hit for one feed, THAT FEED gets a RATE_LIMITED entry in feeds[].error with \
        details.retry_after_seconds/retry_after_ms -- the call itself still succeeds, so \
        check per-feed status, not just whether the call errored; wait that long and retry \
        that feed. \
        Provider notes: \
        some feeds (e.g. Reddit comment .rss) populate only updated, not published, and \
        append the original post to a comment listing (so a comment feed can return one more \
        item than limit); search.rss results are best-effort and may be sparse.",
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = true),
        output_schema = rmcp::handler::server::tool::schema_for_type::<crate::model::FetchOutput>()
    )]
    async fn fetch_feed(
        &self,
        Parameters(args): Parameters<FetchFeedArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(fetch_feed_inner(&self.http, &self.cache, args).await)
    }

    /// Discover feeds advertised on a website's homepage.
    #[tool(
        description = "Discover RSS/Atom/JSON feeds advertised on a website. Returns the \
        DiscoverOutput as structured content (schema: get_schema command=discover).",
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = true),
        output_schema = rmcp::handler::server::tool::schema_for_type::<crate::model::DiscoverOutput>()
    )]
    async fn discover_feeds(
        &self,
        Parameters(args): Parameters<DiscoverFeedsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match core::discover_feeds_with(&args.site_url, &FetchParams::default(), &self.http).await {
            Ok(out) => {
                let summary = format!(
                    "Discovered {} feed(s) at {}.",
                    out.feeds.len(),
                    args.site_url
                );
                Ok(structured_result(&out, summary))
            }
            Err(e) => Ok(tool_error_obj(&e, Some(&args.site_url))),
        }
    }

    /// Fetch a feed and return the single item whose stable id matches `id`.
    #[tool(
        description = "Fetch a feed and return the single Item matching a stable id (from \
        fetch_feed) as structured content, or an error if the id is not present. \
        max_content_chars truncates the body; a single oversized item (e.g. a hot comment \
        thread) returns RESPONSE_TOO_LARGE with a suggested_max_content_chars to retry with. \
        Served cache-first, so an item you already saw survives a rolled feed window, but not \
        a later refetch that overwrote the cache.",
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = true),
        output_schema = rmcp::handler::server::tool::schema_for_type::<crate::model::Item>()
    )]
    async fn get_item(
        &self,
        Parameters(args): Parameters<GetItemArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(get_item_inner(&self.http, &self.cache, args).await)
    }

    /// Return the authoritative JSON Schema for a command's output.
    #[tool(
        description = "Return the authoritative JSON Schema for a command's output. \
        command is 'fetch' or 'discover'.",
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = false)
    )]
    async fn get_schema(
        &self,
        Parameters(args): Parameters<GetSchemaArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let schema = output::schema_for(&args.command);
        if schema.is_null() {
            return Ok(tool_error_code(
                "USAGE_ERROR",
                format!(
                    "unknown command '{}' (expected 'fetch' or 'discover')",
                    args.command
                ),
                None,
            ));
        }
        Ok(json_result(&schema))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for RssServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(SERVER_INSTRUCTIONS)
    }
}

/// Core of the `fetch_feed` tool, free of the `#[tool]` macro plumbing so it is directly
/// unit-testable. Applies the default item cap, the per-item content cap, and the response
/// budget, attaching a [`crate::model::TruncationInfo`] marker when content was truncated.
///
/// A thin wrapper over [`fetch_feed_with_deadline`] with the process-wide batch deadline: the
/// deadline is the one input a tool argument cannot carry, so injecting it is what makes the
/// deadline-bounded paths testable without mutating process-global environment state.
async fn fetch_feed_inner(http: &HttpClient, cache: &Cache, args: FetchFeedArgs) -> CallToolResult {
    fetch_feed_with_deadline(http, cache, args, batch_deadline()).await
}

/// [`fetch_feed_inner`] with the batch deadline supplied by the caller. See that function.
async fn fetch_feed_with_deadline(
    http: &HttpClient,
    cache: &Cache,
    args: FetchFeedArgs,
    deadline: std::time::Duration,
) -> CallToolResult {
    let urls = match resolve_urls(&args) {
        Ok(u) => u,
        Err(e) => return tool_error_obj(&e, None),
    };
    let primary = urls[0].clone();

    let mut params = FetchParams::default();
    if let Some(cf) = args.content_format.as_deref() {
        match parse_content_format(cf) {
            Some(fmt) => params.content_format = fmt,
            None => {
                return tool_error_code(
                    "USAGE_ERROR",
                    format!("invalid content_format '{cf}' (expected markdown|text|html|none)"),
                    Some(&primary),
                );
            }
        }
    }
    if let Some(raw) = args.since.as_deref() {
        match crate::config::parse_since(raw) {
            Ok(dt) => params.since = Some(dt),
            Err(e) => return tool_error_obj(&e, Some(&primary)),
        }
    }
    if let Some(raw) = args.cache_policy.as_deref() {
        match crate::config::parse_cache_policy(raw) {
            Ok(p) => params.cache_policy = p,
            Err(e) => return tool_error_obj(&e, Some(&primary)),
        }
    }
    // Parsed here, with the other scalar arguments, so an unknown mode is rejected before a
    // single network request goes out — and via the shared `parse_dedupe`, so the CLI cannot
    // grow a second grammar (invariant 6).
    let dedupe = match crate::config::parse_dedupe(args.dedupe.as_deref().unwrap_or("")) {
        Ok(mode) => mode,
        Err(e) => return tool_error_obj(&e, Some(&primary)),
    };
    // Deduping is a whole-result operation, but a continuation page fetches only
    // `urls[start_feed..]`: a duplicate whose canonical copy lived in an earlier feed is
    // invisible here, so the survivor would become canonical and ship — putting the same
    // article on two pages. Rejecting is cheaper and more honest than degrading silently.
    // `report` pages fine, since it removes nothing.
    //
    // Ahead of `resume_from_cursor` below so the mode is judged before the token is: a
    // malformed cursor would otherwise mask this with its own USAGE_ERROR.
    if dedupe == crate::config::DedupeMode::Drop
        && args.cursor.as_deref().is_some_and(|c| !c.trim().is_empty())
    {
        return tool_error_code(
            "USAGE_ERROR",
            "dedupe='drop' cannot be combined with a cursor: a continuation page cannot see \
             canonical copies from earlier pages, so the same item could ship twice. Use \
             dedupe='report' (the default) when paging and drop duplicates on your side.",
            Some(&primary),
        );
    }
    // Apply a default item cap so a single huge feed doesn't blow the response budget.
    params.limit = Some(args.limit.unwrap_or(MCP_DEFAULT_LIMIT));
    params.max_content_chars = args.max_content_chars;
    params.query = args.query.clone();
    // Bound the batch in wall-clock time as well as in tokens, so a slow same-host run returns
    // the feeds that finished plus a cursor instead of timing out with nothing.
    params.deadline = Some(deadline);

    // Every argument that changes which items a continuation walks belongs here. Values are
    // *normalized* (defaulted limit, canonical format and dedupe spelling) so a client echoing
    // a response's resolved values back on page 2 isn't told its cursor is foreign.
    //
    // `since` is the deliberate exception and stays RAW: `"2h"` resolves anew every call, so a
    // resolved fingerprint would never match. The resolved cutoff rides in the cursor's `s`.
    //
    // `drop` does reach here — only a *continuation* under `drop` is rejected above — which is
    // why `may_mint_cursor` withholds the token no later call could validate.
    let canonical_format = match params.content_format {
        ContentFormat::Markdown => "markdown",
        ContentFormat::Text => "text",
        ContentFormat::Html => "html",
        ContentFormat::None => "none",
    };
    let fp = crate::cursor::fingerprint(
        &urls,
        &[
            canonical_format,
            args.since.as_deref().unwrap_or(""),
            &params.limit.map(|n| n.to_string()).unwrap_or_default(),
            &params
                .max_content_chars
                .map(|n| n.to_string())
                .unwrap_or_default(),
            args.query.as_deref().unwrap_or(""),
            dedupe.as_str(),
        ],
    );

    let resume = match resume_from_cursor(
        args.cursor.as_deref(),
        &fp,
        urls.len(),
        &primary,
        &mut params,
    ) {
        Ok(r) => r,
        Err(result) => return result,
    };

    // `Cursor.f` always indexes the ORIGINAL request URL list; a continuation fetches
    // `urls[f..]`, so this page's feed 0 is `urls[f]`.
    let start_feed = resume.as_ref().map_or(0, |c| c.f);
    let requested = &urls[start_feed..];
    let mut out = core::fetch_feeds_with(requested, &params, cache, http).await;
    // Cursor arithmetic rests on `feeds[]` being a prefix of `requested` (invariant 9), so
    // `feeds[j] == requested[j]` and `PageStop::feed_idx + start_feed` indexes `urls`. Shorter
    // is fine — the deadline drops trailing feeds — but a *gap* would shift every later
    // position. `debug_assert` reds the dev build; the boolean is the release belt, since no
    // cursor is merely lossy while a cursor at the wrong feed is wrong.
    let mismatch = first_position_mismatch(&out.feeds, requested);
    let positions_line_up = mismatch.is_none();
    debug_assert!(
        positions_line_up,
        "core must return feeds[] as a prefix of the requested urls for cursor positions to \
         line up; first mismatch (index, feeds[i].feed_url, urls[i]): {mismatch:?} ({} feeds \
         for {} urls)",
        out.feeds.len(),
        requested.len()
    );

    // The two unpageable request shapes (invariant 12). Both get a bounded page plus advice
    // rather than a token nothing could redeem: a `drop` cursor is rejected by the guard above
    // and its `i`/`n` index the post-removal list anyway, and a `no-cache` cursor would resume
    // against whatever snapshot an earlier call left in the cache.
    let no_cursor_reason: Option<&'static str> = if dedupe == crate::config::DedupeMode::Drop {
        Some(DROP_NOT_PAGEABLE_SUGGESTION)
    } else if params.cache_policy == CachePolicy::NoCache {
        Some(NO_CACHE_NOT_PAGEABLE_SUGGESTION)
    } else {
        None
    };
    let may_mint_cursor = positions_line_up && no_cursor_reason.is_none();

    // Skip the items already delivered from the resumed feed, warning if the window rolled.
    // `skipped` is what we *actually* dropped (a rolled window can hold fewer items than the
    // cursor's `i`), and it is the offset that makes the next cursor absolute again.
    let mut skipped = 0usize;
    if let Some(c) = &resume
        && let Some(feed) = out.feeds.first_mut()
    {
        // Only a *successful* fetch can say the window moved: a feed that errored has zero
        // items for a reason already in `feeds[].error`, and calling that a roll misdirects.
        //
        // `c.i > 0` likewise — resuming at a feed's head drains nothing, so no count can make
        // this page skip or repeat. It also keeps the deadline's `i: 0, n: 0` cursor quiet,
        // where the zeros mean "unattempted", not "empty window". The cost is that a genuine
        // roll goes unreported on a feed dropped whole at `i: 0, n: 10`; accepted, since the
        // warning only claims "resuming here may skip or repeat", which at `i == 0` it cannot.
        // Pinned by `a_cursor_that_measured_a_window_but_resumes_at_zero_stays_quiet`.
        if feed.error.is_none() && c.i > 0 && feed.items.len() != c.n {
            out.warnings.push(crate::model::Warning {
                feed_url: Some(feed.feed_url.clone()),
                code: "CACHE_WINDOW_ROLLED".to_string(),
                message: format!(
                    "feed now has {} item(s) but had {} when the cursor was minted; resuming \
                     at index {} may skip or repeat items",
                    feed.items.len(),
                    c.n,
                    c.i
                ),
            });
        }
        skipped = c.i.min(feed.items.len());
        feed.items.drain(..skipped);
        // Per-feed counts *and* the top-level totals now describe items that are gone.
        core::refresh_feed_counts(&mut out);
    }

    // Before `paginate` (whose skeleton measures `duplicates[]`, so grouping later would ship
    // the page over budget) and after the resume-skip. The consequence is that groups describe
    // what *this page* fetched — see ADR-0018 and `FetchOutput::duplicates` for how a caller
    // reconciles them across pages. Moving it after `paginate` would also break `drop`, whose
    // groups are an audit trail of items already gone.
    core::apply_dedupe(&mut out, dedupe);

    // A continuation with nothing left to ship is *finished*, not over budget. It happens when
    // the cached window rolled shorter than the cursor's `i`: the drain empties the resumed
    // feed, and `paginate` would answer `RESPONSE_TOO_LARGE` — discarding the
    // CACHE_WINDOW_ROLLED warning that explains the emptiness and advising a budget change that
    // cannot conjure items that are gone from the window. An empty final page terminates the
    // caller's loop honestly instead. No `errors[]` orphan check is needed: nothing was trimmed
    // here, so every error still has its feed. This is the one page that may exceed
    // `max_response_tokens` — there is no item left to drop.
    if resume.is_some() && core::item_count(&out) == 0 {
        // "Finished" as far as the *roll* goes — but the deadline may have left feeds
        // unattempted behind the drained one, and those are data the caller hasn't seen. Only
        // reachable with a deadline; without one every later feed would be here with items.
        //
        // Non-empty `feeds[]` means the resumed feed *was* fetched (the drain emptied it), so
        // resuming past it is right. Empty means the deadline attempted nothing, and the
        // caller's own cursor is still the place to resume — minting here would re-deliver
        // page 1.
        //
        // `may_mint_cursor` for consistency with the other two mint sites; on this path
        // (`resume.is_some()`) both unpageable shapes are already excluded, so it changes
        // nothing today.
        if may_mint_cursor
            && !out.feeds.is_empty()
            && let Some(m) = out.truncation.as_mut()
            && m.feeds_omitted > 0
        {
            m.next_cursor = Some(continuation(
                &fp,
                &params,
                start_feed + out.feeds.len(),
                0,
                0,
            ));
        }
        // Stamped here too: every other page that ships a marker reports its size, and a caller
        // that reads `estimated_tokens` should not have to special-case this one.
        stamp_estimated_tokens(&mut out);
        let summary = fetch_summary(&out, &primary);
        return structured_result(&out, summary);
    }

    // Fill the page to the budget rather than rejecting the whole batch: with `urls[]`,
    // rejecting would discard every successful fetch because one trailing item overflowed
    // (ADR-0017). Reserve headroom for the marker + cursor `paginate` cannot measure.
    let budget = args
        .max_response_tokens
        .unwrap_or(MCP_DEFAULT_MAX_RESPONSE_TOKENS);
    // Taken *before* paginate: `estimate_response_tokens` serializes `truncation`, so leaving
    // the deadline's marker in place would inflate the payload estimate and shed items that
    // would have fit. It is folded back into the page's own marker further down.
    //
    // Deliberately *after* the empty-continuation return above, which reports the deadline's
    // omissions — and cursors them — from core's marker in place. Only pages that reach the
    // budget need the marker lifted out of the payload being measured.
    let deadline_marker = out.truncation.take();
    let stop = match core::paginate(&mut out, budget.saturating_sub(*CURSOR_HEADROOM_TOKENS)) {
        Ok(stop) => stop,
        // `paginate` mutates `out` on its error paths, so surface the error alone — never a
        // half-trimmed page. On a continuation this does *not* mean the batch failed: the
        // pages already delivered are still valid, so say so.
        Err(e) => {
            let mut obj = e.to_error_obj(Some(&primary));
            if resume.is_some() {
                obj.message.push_str(
                    " (this is a continuation page: the pages you already received are still \
                     valid — retry this same cursor with a larger max_response_tokens or a \
                     smaller max_content_chars)",
                );
                if let Some(details) = obj.details.as_object_mut() {
                    details.insert("continuation".to_string(), serde_json::Value::Bool(true));
                }
            }
            return error_result(obj);
        }
    };

    // `paginate` trims `feeds` but not `errors`/`warnings`, so a feed it dropped whole would
    // leave an entry in each with no matching `feeds[]` entry — breaking the documented mirror
    // for errors, and reporting a data-quality note about a feed the page does not carry.
    // Dropping them loses nothing: `paginate` resumes at the first feed this page does not
    // carry in full — which a dropped feed always is — so the next page reports it again.
    let present: std::collections::HashSet<String> =
        out.feeds.iter().map(|f| f.feed_url.clone()).collect();
    out.errors
        .retain(|e| e.feed_url.as_ref().is_none_or(|u| present.contains(u)));
    out.warnings
        .retain(|w| w.feed_url.as_ref().is_none_or(|u| present.contains(u)));

    // A marker is emitted only when the agent is genuinely not seeing everything: content was
    // truncated, or (below) the page was bounded and there is more to fetch.
    let mut marker = core::truncation_marker(
        &out,
        params.limit,
        Some(CONTENT_TRUNCATION_SUGGESTION.to_string()),
    );
    // Borrow rather than move: Task 8 inspects `stop` again to decide whether the deadline
    // (not the budget) is what truncated the page.
    if let Some(stop) = stop.as_ref() {
        let m = marker.get_or_insert(crate::model::TruncationInfo {
            applied_limit: params.limit,
            items_content_truncated: 0,
            items_omitted: 0,
            feeds_omitted: 0,
            estimated_tokens: None,
            next_cursor: None,
            suggestion: None,
        });
        m.items_omitted = stop.items_omitted;
        m.feeds_omitted = stop.feeds_omitted;
        // Only mint a cursor while feed positions provably line up with `urls` (see above): a
        // truncated page without a continuation is lossy, one pointing at the wrong feed is
        // silently wrong data. `may_mint_cursor` additionally withholds one under
        // `dedupe='drop'`, which cannot be paged at all.
        if may_mint_cursor {
            // Positions in `stop` are relative to the fetched slice; the cursor is absolute.
            let absolute_feed = start_feed + stop.feed_idx;
            // Items this call already skipped in the resumed feed, so the next cursor's `i`/`n`
            // stay absolute within that feed's item list rather than relative to this page. Only
            // the resumed feed (this page's feed 0) carries such an offset.
            let already = if absolute_feed == start_feed {
                skipped
            } else {
                0
            };
            m.next_cursor = Some(continuation(
                &fp,
                &params,
                absolute_feed,
                already + stop.item_idx,
                already + stop.feed_item_count,
            ));
            // Combine, never replace: a page can be both content-truncated and paginated, and
            // dropping the get_item hint would lose the only route to the full body.
            m.suggestion = Some(combined_suggestion(m.suggestion.as_deref()));
        } else if let Some(reason) = no_cursor_reason {
            // No cursor is coming, so say what to do instead — a truncated page whose marker
            // offers neither a continuation nor advice reads as an unexplained short answer.
            // Appended, not assigned, for the same reason the branch above combines: this page
            // may also be content-truncated, and overwriting would drop the get_item hint that
            // is the only route to a full body.
            m.suggestion = Some(join_suggestion(m.suggestion.as_deref(), reason));
        }
    }

    // After the block above, never before: that one *assigns* `feeds_omitted` from the budget's
    // `PageStop`, so merging first would have the assignment wipe the deadline's count.
    merge_deadline_omissions(&mut marker, deadline_marker);

    // The deadline, not the budget, is what bounded this page: resume at the first feed it
    // never attempted, which is exactly where the delivered prefix ends. Gated on `stop` being
    // absent because the budget's cursor points *earlier* in the same list and already covers
    // these feeds; gated on `may_mint_cursor` for the same reasons as the cursor above.
    if stop.is_none()
        && may_mint_cursor
        && let Some(m) = marker.as_mut()
        && m.feeds_omitted > 0
    {
        // `i: 0` because an unattempted feed has delivered nothing; `n: 0` because this call
        // never saw its window (see the roll-detection gate above, which reads `i` not `n`).
        m.next_cursor = Some(continuation(
            &fp,
            &params,
            start_feed + out.feeds.len(),
            0,
            0,
        ));
    }

    // The same page, when the request shape forbids a cursor. The budget branch above attaches
    // its own advice, but a page bounded only by the *deadline* never enters it — `stop` is
    // `None` — and would otherwise ship feeds_omitted > 0, no cursor, and no word on why.
    if stop.is_none()
        && let Some(reason) = no_cursor_reason
        && let Some(m) = marker.as_mut()
        && m.feeds_omitted > 0
    {
        m.suggestion = Some(join_suggestion(m.suggestion.as_deref(), reason));
    }

    out.truncation = marker;
    stamp_estimated_tokens(&mut out);

    let summary = fetch_summary(&out, &primary);
    structured_result(&out, summary)
}

/// Record the response size on the truncation marker, if there is one.
///
/// Measured with the marker already attached, so `estimated_tokens` describes the payload the
/// client actually receives (bar the digits of the number itself). Shared by both return paths
/// — the ordinary one and the drained-continuation early return — so a marker never ships
/// without its size.
fn stamp_estimated_tokens(out: &mut crate::model::FetchOutput) {
    if out.truncation.is_some() {
        let estimated = core::estimate_response_tokens(out);
        if let Some(m) = out.truncation.as_mut() {
            m.estimated_tokens = Some(estimated);
        }
    }
}

/// The first position at which `feeds[]` stops being a contiguous **prefix** of `requested`,
/// as `(index, feeds[index].feed_url, requested[index])`, or `None` when it is a genuine
/// prefix — including the legitimately shorter list the batch deadline produces.
///
/// A pure function rather than an inline `all(...)` so the failure modes are unit-testable: the
/// gap case (a missing feed with delivered feeds after it, which would shift every later cursor
/// position by one) and the overrun case (more feeds than URLs) are both unreachable from a
/// real fetch, and would otherwise be compiled but never run.
fn first_position_mismatch(
    feeds: &[crate::model::FeedResult],
    requested: &[String],
) -> Option<(usize, String, String)> {
    if feeds.len() > requested.len() {
        // More feeds than were asked for: there is no URL to compare the extras against, so
        // name the first one and say so.
        let idx = requested.len();
        return Some((
            idx,
            feeds[idx].feed_url.clone(),
            "<no url requested at this position>".to_string(),
        ));
    }
    feeds
        .iter()
        .zip(requested)
        .enumerate()
        .find(|(_, (feed, url))| &&feed.feed_url != url)
        .map(|(idx, (feed, url))| (idx, feed.feed_url.clone(), url.clone()))
}

/// Encode a continuation cursor for this request, resuming at item `i` of feed `f` (an index
/// into the **original** URL list) in a feed that held `n` items when the page was minted.
///
/// Shared by both mint sites — the response budget's and the batch deadline's — so a cursor
/// always carries the same fingerprint and the same resolved `since` cutoff.
fn continuation(fp: &str, params: &FetchParams, f: usize, i: usize, n: usize) -> String {
    crate::cursor::Cursor {
        v: crate::cursor::CURSOR_VERSION,
        fp: fp.to_string(),
        f,
        i,
        n,
        s: params.since.map(|dt| dt.timestamp()),
    }
    .encode()
}

/// Fold the batch deadline's omission marker into the page's own.
///
/// The two counts are **disjoint sets**, so they add: the deadline omits feeds that were never
/// fetched, the budget omits feeds that were fetched but did not fit. `max` would under-report,
/// and a plain assignment either way round would lose one of them. The budget's suggestion wins
/// when it has one — it already names the cursor that recovers both — and the deadline's fills
/// in otherwise, which is the common case ([`core::truncation_marker`] yields `None` unless item
/// *content* was truncated).
fn merge_deadline_omissions(
    marker: &mut Option<crate::model::TruncationInfo>,
    deadline: Option<crate::model::TruncationInfo>,
) {
    let Some(d) = deadline else { return };
    match marker.as_mut() {
        Some(m) => {
            m.feeds_omitted += d.feeds_omitted;
            if m.suggestion.is_none() {
                m.suggestion = d.suggestion;
            }
        }
        None => *marker = Some(d),
    }
}

/// Validate the caller's `cursor` argument against this request and apply the state it records
/// to `params`, returning the decoded position — or `None` when no cursor was supplied. `Err`
/// carries a ready-to-return tool error.
///
/// Split out of [`fetch_feed_inner`] so that function reads as one sequence of batch steps
/// while the continuation rules — and the reasons behind each rejection — live together here.
fn resume_from_cursor(
    token: Option<&str>,
    fp: &str,
    url_count: usize,
    primary: &str,
    params: &mut FetchParams,
) -> Result<Option<crate::cursor::Cursor>, CallToolResult> {
    // A blank cursor is not a malformed one: clients that fill every advertised optional
    // property with an empty string mean "no cursor", the same way `url: ""` means "not
    // supplied" (see `resolve_urls`). Rejecting it would fail a plain page-1 request.
    let Some(token) = token.filter(|t| !t.trim().is_empty()) else {
        return Ok(None);
    };
    let c = crate::cursor::Cursor::decode(token).map_err(|e| tool_error_obj(&e, Some(primary)))?;
    if c.fp != fp {
        return Err(tool_error_code(
            "USAGE_ERROR",
            "cursor does not match this request; pass the cursor back with the same \
             urls/content_format/since/limit/max_content_chars/query/dedupe, or drop it to \
             start over",
            Some(primary),
        ));
    }
    if c.f >= url_count {
        return Err(tool_error_code(
            "USAGE_ERROR",
            "cursor points past the end of this request's feed list",
            Some(primary),
        ));
    }
    // Reuse the page-1 cutoff so every page filters against one instant. An `s` that is
    // not a representable instant would otherwise *widen* the window — dropping the cutoff
    // entirely and returning items page 1 filtered out — so reject it instead.
    if let Some(secs) = c.s {
        match chrono::DateTime::from_timestamp(secs, 0) {
            Some(dt) => params.since = Some(dt),
            None => {
                return Err(tool_error_code(
                    "USAGE_ERROR",
                    "invalid cursor: the recorded `since` cutoff is not a valid instant",
                    Some(primary),
                ));
            }
        }
    }
    // A continuation is served from cache: feeds already fetched cost nothing, and feeds
    // never reached fall through to the network on a cache miss (see `CachePolicy`). This
    // deliberately overrides whatever `cache_policy` the caller passed — the body cache is
    // the pagination store.
    params.cache_policy = CachePolicy::CacheFirst;
    Ok(Some(c))
}

/// A one-line, human/agent-readable summary of a `fetch_feed` result. Kept terse on purpose:
/// it is the *unstructured* `content` companion to the full `structured_content` payload, so
/// we don't duplicate the whole FetchOutput as text (which would ~double the response and
/// undercut the token budget — see ADR-0011/ADR-0013).
fn fetch_summary(out: &crate::model::FetchOutput, url: &str) -> String {
    match out.feeds.len() {
        0 => format!("fetch_feed: no result for {url}."),
        1 => {
            let feed = &out.feeds[0];
            if let Some(err) = &feed.error {
                return format!(
                    "fetch_feed: {url} returned an error ({}); see structuredContent.",
                    err.code
                );
            }
            let title = feed.title.as_deref().unwrap_or(url);
            let mut s = format!(
                "Fetched {} item(s) (~{} content tokens) from \"{title}\".",
                out.total_items, out.total_content_tokens_est
            );
            s.push_str(&truncation_note(out));
            s.push_str(" Full data in structuredContent.");
            s
        }
        n => {
            let failed = out.errors.len();
            let mut s = format!(
                "Fetched {} item(s) (~{} content tokens) from {n} feed(s)",
                out.total_items, out.total_content_tokens_est
            );
            if failed > 0 {
                s.push_str(&format!(
                    "; {failed} feed(s) failed (see structuredContent.errors)"
                ));
            }
            s.push('.');
            s.push_str(&truncation_note(out));
            s.push_str(" Full data in structuredContent.");
            s
        }
    }
}

/// How this response was bounded, as a sentence or two for the summary line. Kept separate
/// from [`fetch_summary`]'s two branches so both report pagination identically — an agent that
/// reads only the text must still learn that a `next_cursor` is waiting.
fn truncation_note(out: &crate::model::FetchOutput) -> String {
    let Some(t) = &out.truncation else {
        return String::new();
    };
    let mut s = String::new();
    if t.items_content_truncated > 0 {
        s.push_str(CONTENT_TRUNCATED_NOTE);
    }
    if t.next_cursor.is_some() {
        s.push_str(MORE_ITEMS_NOTE);
    }
    s
}

/// Core of the `get_item` tool, free of the `#[tool]` macro plumbing. Guards the
/// full-content escape hatch: a single item that still exceeds the budget yields a
/// `RESPONSE_TOO_LARGE` error rather than tripping the client limit.
async fn get_item_inner(http: &HttpClient, cache: &Cache, args: GetItemArgs) -> CallToolResult {
    let params = FetchParams {
        max_content_chars: args.max_content_chars,
        cache_policy: CachePolicy::CacheFirst,
        ..FetchParams::default()
    };
    match core::show_item_with(&args.feed_url, &args.id, &params, cache, http).await {
        Ok(Some(item)) => {
            let estimated = serde_json::to_string_pretty(&item)
                .map(|s| s.chars().count().div_ceil(4))
                .unwrap_or(0);
            if estimated > MCP_DEFAULT_MAX_RESPONSE_TOKENS {
                let suggested = (MCP_DEFAULT_MAX_RESPONSE_TOKENS * 7 / 10)
                    .saturating_mul(4)
                    .max(200);
                let err = RssError::ResponseTooLarge {
                    estimated_tokens: estimated,
                    budget_tokens: MCP_DEFAULT_MAX_RESPONSE_TOKENS,
                    suggested_limit: 1,
                    suggested_max_content_chars: suggested,
                };
                return tool_error_obj(&err, Some(&args.feed_url));
            }
            let summary = format!(
                "Item {}: \"{}\".",
                item.id,
                item.title.as_deref().unwrap_or("(untitled)")
            );
            structured_result(&item, summary)
        }
        Ok(None) => tool_error_code(
            "NOT_FOUND",
            format!("item '{}' not found in {}", args.id, args.feed_url),
            Some(&args.feed_url),
        ),
        Err(e) => tool_error_obj(&e, Some(&args.feed_url)),
    }
}

/// Serialize a value as pretty JSON and wrap it in a successful tool result.
///
/// Serialization of our own model types cannot realistically fail, but if it ever does we
/// surface it as a tool error rather than panicking the server.
fn json_result<T: Serialize>(value: &T) -> CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(json) => CallToolResult::success(vec![ContentBlock::text(json)]),
        Err(e) => tool_error_code(
            "INTERNAL_ERROR",
            format!("failed to serialize result: {e}"),
            None,
        ),
    }
}

/// Wrap `value` as the tool's **structured** result: the typed data goes in
/// `structured_content` (machine-readable, matching the tool's `output_schema`) and only a
/// short `summary` line goes in the unstructured `content`. We deliberately do *not* also
/// serialize the full value as text — that would double the payload and undercut the
/// response budget (see ADR-0013). `CallToolResult` is `#[non_exhaustive]`, so we mutate the
/// public `structured_content` field on an owned `success` result rather than struct-literal.
fn structured_result<T: Serialize>(value: &T, summary: impl Into<String>) -> CallToolResult {
    match serde_json::to_value(value) {
        Ok(json) => {
            let mut result = CallToolResult::success(vec![ContentBlock::text(summary.into())]);
            result.structured_content = Some(json);
            result
        }
        Err(e) => tool_error_code(
            "INTERNAL_ERROR",
            format!("failed to serialize result: {e}"),
            None,
        ),
    }
}

/// Build a tool-level error result (`is_error: true`) from an [`RssError`], serializing the
/// structured [`ErrorObj`] (stable `code` + machine-readable `details`, e.g. the
/// `suggested_*` fields on `RESPONSE_TOO_LARGE`) as JSON so the agent can parse and recover.
fn tool_error_obj(err: &RssError, feed_url: Option<&str>) -> CallToolResult {
    error_result(err.to_error_obj(feed_url))
}

/// Build a structured tool error from an explicit code + message, for argument/validation
/// failures that don't correspond to an [`RssError`] variant.
fn tool_error_code(
    code: &str,
    message: impl Into<String>,
    feed_url: Option<&str>,
) -> CallToolResult {
    let mut obj = ErrorObj::new(code, message);
    if let Some(u) = feed_url {
        obj.feed_url = Some(u.to_string());
    }
    error_result(obj)
}

/// Serialize an [`ErrorObj`] as JSON and wrap it in a failed tool result.
fn error_result(obj: ErrorObj) -> CallToolResult {
    let json = serde_json::to_string_pretty(&obj).unwrap_or_else(|_| {
        format!(
            "{{\"code\":\"{}\",\"message\":\"{}\"}}",
            obj.code, obj.message
        )
    });
    CallToolResult::error(vec![ContentBlock::text(json)])
}

/// Parse a user-supplied content-format string into a [`ContentFormat`]. Case-insensitive.
fn parse_content_format(s: &str) -> Option<ContentFormat> {
    match s.trim().to_ascii_lowercase().as_str() {
        "markdown" => Some(ContentFormat::Markdown),
        "text" => Some(ContentFormat::Text),
        "html" => Some(ContentFormat::Html),
        "none" => Some(ContentFormat::None),
        _ => None,
    }
}

/// Run the MCP server over stdio until the client disconnects. **Owner: `mcp` agent.**
pub async fn serve_stdio(cache: Cache) -> Result<(), RssError> {
    // One shared HTTP client (and its per-host gate + connection pool) for every tool call.
    let params = FetchParams::default();
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    let server = RssServer::new(cache, http);
    tracing::info!("starting MCP server on stdio");

    let service = server
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| RssError::Other(format!("failed to start MCP server: {e}")))?;

    let quit_reason = service
        .waiting()
        .await
        .map_err(|e| RssError::Other(format!("MCP server error: {e}")))?;
    tracing::info!(?quit_reason, "MCP server stopped");
    Ok(())
}

/// Test-only seam: run one `fetch_feed` page and return the model rather than the wire
/// `CallToolResult`, so integration tests assert on typed data. Not part of the MCP surface.
#[doc(hidden)]
pub async fn fetch_page_for_test(
    http: &HttpClient,
    cache: &Cache,
    urls: &[String],
    max_response_tokens: usize,
    cursor: Option<String>,
) -> crate::model::FetchOutput {
    let args = FetchFeedArgs {
        url: None,
        urls: Some(urls.to_vec()),
        content_format: None,
        limit: None,
        max_content_chars: None,
        max_response_tokens: Some(max_response_tokens),
        since: None,
        query: None,
        dedupe: None,
        cache_policy: None,
        cursor,
    };
    let result = fetch_feed_inner(http, cache, args).await;
    let wire = serde_json::to_value(&result).expect("serialize result");
    serde_json::from_value(wire["structuredContent"].clone())
        .expect("structuredContent should be a FetchOutput")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode a `CallToolResult` into `(is_error, inner_payload_json)`. We serialize the
    /// whole result and read the MCP wire fields. Success results now carry the typed data in
    /// `structuredContent` (with only a summary in `content`); error results carry the
    /// `ErrorObj` JSON as text. Prefer `structuredContent` when present, else parse the text.
    fn decode(result: &CallToolResult) -> (bool, serde_json::Value) {
        let v = serde_json::to_value(result).expect("serialize CallToolResult");
        let is_error = v
            .get("isError")
            .or_else(|| v.get("is_error"))
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        if let Some(sc) = v.get("structuredContent")
            && !sc.is_null()
        {
            return (is_error, sc.clone());
        }
        let text = v["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("expected text content, got: {v}"));
        let payload = serde_json::from_str(text)
            .unwrap_or_else(|e| panic!("tool text should be JSON ({e}): {text}"));
        (is_error, payload)
    }

    /// Pull the unstructured `content[0].text` summary out of a result, if any.
    fn summary_text(result: &CallToolResult) -> Option<String> {
        let v = serde_json::to_value(result).ok()?;
        v["content"][0]["text"].as_str().map(|s| s.to_string())
    }

    /// A fresh HTTP client for a test (its own per-host gate, so tests never couple).
    fn test_http() -> HttpClient {
        HttpClient::new("rss-cli-test", std::time::Duration::from_secs(10)).expect("build client")
    }

    fn temp_cache(tag: &str) -> (Cache, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "rss-mcp-test-{}-{tag}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create temp cache");
        (Cache::open(Some(dir.clone())).expect("open cache"), dir)
    }

    fn feed_with_items(n: usize) -> String {
        let mut items = String::new();
        for i in 0..n {
            items.push_str(&format!(
                "<item><title>Post {i}</title><link>https://example.com/{i}</link>\
                 <description>body number {i}</description></item>"
            ));
        }
        format!(
            "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Feed</title>\
             <link>https://example.com/</link>{items}</channel></rss>"
        )
    }

    /// A feed whose items straddle a `since` cutoff: `recent` items dated within the hour, then
    /// `stale` items over a year old. [`feed_with_items`] emits no dates at all, and undated
    /// items are deliberately *retained* by the `since` filter (see `parse.rs`), so a cutoff
    /// test built on it cannot tell an applied cutoff from a dropped one.
    fn feed_with_dated_items(recent: usize, stale: usize) -> String {
        let now = chrono::Utc::now();
        let mut items = String::new();
        let mut push = |label: &str, i: usize, when: chrono::DateTime<chrono::Utc>| {
            items.push_str(&format!(
                "<item><title>{label} {i}</title>\
                 <link>https://example.com/{label}/{i}</link>\
                 <pubDate>{}</pubDate><description>body number {i}</description></item>",
                when.to_rfc2822()
            ));
        };
        for i in 0..recent {
            push("Fresh", i, now - chrono::Duration::minutes(i as i64 + 1));
        }
        for i in 0..stale {
            push("Stale", i, now - chrono::Duration::days(400 + i as i64));
        }
        format!(
            "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Feed</title>\
             <link>https://example.com/</link>{items}</channel></rss>"
        )
    }

    /// Every item title in a page, for asserting on which side of a `since` cutoff shipped.
    fn titles(out: &crate::model::FetchOutput) -> Vec<String> {
        out.feeds
            .iter()
            .flat_map(|f| &f.items)
            .filter_map(|i| i.title.clone())
            .collect()
    }

    /// `fetch_feed` arguments for a multi-URL batch.
    fn batch_args(urls: Vec<String>) -> FetchFeedArgs {
        let mut args = fetch_args(String::new());
        args.url = None;
        args.urls = Some(urls);
        args
    }

    /// Decode a page that must have succeeded into the typed model.
    fn output_of(result: &CallToolResult) -> crate::model::FetchOutput {
        let (is_error, payload) = decode(result);
        assert!(!is_error, "expected a successful page, got: {payload}");
        serde_json::from_value(payload).expect("structuredContent should be a FetchOutput")
    }

    /// The `truncation.next_cursor` of a successful page, if it has one.
    fn cursor_of(result: &CallToolResult) -> Option<String> {
        output_of(result).truncation.and_then(|t| t.next_cursor)
    }

    /// Estimated size of the whole (unpaginated) response for `args`. Tests derive their
    /// budgets from this rather than hardcoding a fraction, so they cannot drift into the
    /// "not even one item fits" window if the fixture changes size.
    async fn full_response_tokens(http: &HttpClient, cache: &Cache, args: FetchFeedArgs) -> usize {
        core::estimate_response_tokens(&output_of(&fetch_feed_inner(http, cache, args).await))
    }

    fn fetch_args(url: String) -> FetchFeedArgs {
        FetchFeedArgs {
            url: Some(url),
            urls: None,
            content_format: None,
            since: None,
            query: None,
            dedupe: None,
            limit: None,
            max_content_chars: None,
            max_response_tokens: None,
            cache_policy: None,
            cursor: None,
        }
    }

    #[tokio::test]
    async fn fetch_feed_applies_default_item_cap() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(30))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("cap");

        let result = fetch_feed_inner(
            &test_http(),
            &cache,
            fetch_args(format!("{}/feed.xml", server.url())),
        )
        .await;
        let (is_error, payload) = decode(&result);

        assert!(!is_error, "a normal feed should succeed: {payload}");
        let items = payload["feeds"][0]["items"].as_array().expect("items");
        assert_eq!(
            items.len(),
            MCP_DEFAULT_LIMIT,
            "fetch_feed should cap to the default {MCP_DEFAULT_LIMIT} items when no limit is passed"
        );
        // Nothing was content-truncated, so the marker stays null (cap is documented, not noise).
        assert!(
            payload["truncation"].is_null(),
            "untruncated result → truncation null"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_fetches_a_batch_in_request_order() {
        let mut server = mockito::Server::new_async().await;
        let _a = server
            .mock("GET", "/a.xml")
            .with_status(200)
            .with_body(feed_with_items(2))
            .create_async()
            .await;
        let _b = server
            .mock("GET", "/b.xml")
            .with_status(200)
            .with_body(feed_with_items(3))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("batch");

        let mut args = fetch_args(String::new());
        args.url = None;
        args.urls = Some(vec![
            format!("{}/a.xml", server.url()),
            format!("{}/b.xml", server.url()),
        ]);

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(!is_error, "batch fetch should succeed: {payload}");
        let feeds = payload["feeds"].as_array().expect("feeds");
        assert_eq!(feeds.len(), 2);
        // Request order is a contract (invariant 9), not completion order.
        assert!(feeds[0]["feed_url"].as_str().unwrap().ends_with("/a.xml"));
        assert!(feeds[1]["feed_url"].as_str().unwrap().ends_with("/b.xml"));
        assert_eq!(payload["total_items"], 5);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn batch_keeps_good_feeds_when_one_fails() {
        let mut server = mockito::Server::new_async().await;
        let _a = server
            .mock("GET", "/a.xml")
            .with_status(200)
            .with_body(feed_with_items(2))
            .create_async()
            .await;
        let _bad = server
            .mock("GET", "/bad.xml")
            .with_status(404)
            .create_async()
            .await;
        let (cache, dir) = temp_cache("batch-partial");

        let mut args = fetch_args(String::new());
        args.url = None;
        args.urls = Some(vec![
            format!("{}/a.xml", server.url()),
            format!("{}/bad.xml", server.url()),
        ]);

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(
            !is_error,
            "one bad feed must not fail the whole call: {payload}"
        );
        assert_eq!(payload["feeds"][0]["status"], "ok");
        assert_eq!(payload["feeds"][1]["status"], "error");
        assert_eq!(payload["errors"].as_array().unwrap().len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn concurrent_fetches_share_one_client_and_gate() {
        // Pins the reported bug: two concurrent fetch_feed calls go through ONE shared
        // HttpClient (hence one per-host gate), the way RssServer wires them — not a fresh
        // client per call. Both succeed here; the gate's serialize/cooldown behavior is
        // unit-tested in `ratelimit`.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(2))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("shared-client");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let (r1, r2) = tokio::join!(
            fetch_feed_inner(&http, &cache, fetch_args(url.clone())),
            fetch_feed_inner(&http, &cache, fetch_args(url.clone())),
        );
        for r in [&r1, &r2] {
            let (is_error, payload) = decode(r);
            assert!(
                !is_error,
                "shared-client concurrent fetch should succeed: {payload}"
            );
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_item_and_discover_use_the_shared_client() {
        // ADR-0016: every tool must share one HttpClient so their per-host pacing coordinates.
        // Before this fix, core::show_item and core::discover_feeds each built their own.
        let mut server = mockito::Server::new_async().await;
        let _f = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(3))
            .create_async()
            .await;
        let _s = server
            .mock("GET", "/site")
            .with_status(200)
            .with_header("content-type", "text/html")
            .with_body(
                "<html><head><link rel=\"alternate\" type=\"application/rss+xml\" \
                 href=\"/feed.xml\"></head></html>",
            )
            .create_async()
            .await;
        let (cache, dir) = temp_cache("shared-all");
        let http = test_http();

        let args = GetItemArgs {
            feed_url: format!("{}/feed.xml", server.url()),
            id: "0000000000000000".to_string(),
            max_content_chars: None,
        };
        // Signature proves the client is threaded through; NOT_FOUND is the expected outcome.
        let (is_error, payload) = decode(&get_item_inner(&http, &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "NOT_FOUND");

        let out = core::discover_feeds_with(
            &format!("{}/site", server.url()),
            &FetchParams::default(),
            &http,
        )
        .await
        .expect("discover should succeed");
        assert_eq!(out.feeds.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_item_and_discover_serialize_through_one_gate_on_the_same_host() {
        // The assertions above don't prove the gate is *shared* — a `*_with` that accepts
        // `http` and quietly builds its own `HttpClient` inside would still pass them.
        //
        // Formulated as a hold-and-timeout race, like
        // `ratelimit::tests::cap_one_serializes_same_authority`, not a wall-clock threshold:
        // `/feed.xml` is held open while `/site` answers instantly. Sharing the gate means
        // `discover` blocks in `HostGate::acquire` with no timer to lose to, so scheduling
        // delay cannot red this on correct code; a fresh client skips the permit entirely and
        // returns in ~10ms against the 300ms window.
        //
        // Assumes the default env (unset `RSS_HOST_CONCURRENCY`/`RSS_MAX_GATE_WAIT_SECS`): a
        // cap above ADR-0016's 1 hands `discover_fut` a second permit and reds this.
        // The 100ms drive step below just lets `get_item_inner` take the permit first.
        let (base, hold) = feed_hold_server(feed_with_items(1), site_with_alternate_link());
        let (cache, dir) = temp_cache("shared-gate-probe");
        let http = test_http();

        let get_item_args = GetItemArgs {
            feed_url: format!("{base}/feed.xml"),
            id: "0000000000000000".to_string(),
            max_content_chars: None,
        };
        let site_url = format!("{base}/site");
        let discover_params = FetchParams::default();

        let item_fut = get_item_inner(&http, &cache, get_item_args);
        tokio::pin!(item_fut);
        let discover_fut = core::discover_feeds_with(&site_url, &discover_params, &http);
        tokio::pin!(discover_fut);

        // Drive the first call forward until it's blocked on the held socket read — this is
        // what establishes "in flight, holding the one permit" below. It can never finish
        // inside this window (the server won't answer `/feed.xml` until `hold.release()`), so
        // the timeout always elapses here; we only care about the side effect of polling it.
        let _ = tokio::time::timeout(std::time::Duration::from_millis(100), &mut item_fut).await;

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(300), &mut discover_fut)
                .await
                .is_err(),
            "discover_feeds_with must not complete while get_item_inner's /feed.xml request is \
             still held and holding the shared gate's one permit (ADR-0016) — completing here \
             means it went through its own gate instead of the shared client's"
        );

        hold.release();

        let (is_error, payload) = decode(&item_fut.await);
        assert!(is_error, "expected NOT_FOUND, got {payload}");
        assert_eq!(payload["code"], "NOT_FOUND");
        assert_eq!(
            discover_fut
                .await
                .expect("discover should succeed")
                .feeds
                .len(),
            1,
            "site body advertises one alternate feed"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn tools_advertise_annotations_and_output_schema() {
        let tools = RssServer::tool_router().list_all();

        let fetch = tools
            .iter()
            .find(|t| t.name == "fetch_feed")
            .expect("fetch_feed tool registered");
        let ann = fetch.annotations.as_ref().expect("fetch_feed annotations");
        assert_eq!(ann.read_only_hint, Some(true));
        assert_eq!(ann.idempotent_hint, Some(true));
        assert_eq!(ann.open_world_hint, Some(true), "fetch hits the network");
        assert!(
            fetch.output_schema.is_some(),
            "fetch_feed should advertise its FetchOutput output_schema"
        );
        // The continuation token is useless to a client that cannot see it in the input schema.
        assert!(
            fetch
                .input_schema
                .get("properties")
                .is_some_and(|p| p.as_object().is_some_and(|p| p.contains_key("cursor"))),
            "fetch_feed must advertise the `cursor` argument: {:?}",
            fetch.input_schema
        );

        // get_schema is local-only (not open-world) and has no fixed output shape.
        let get_schema = tools
            .iter()
            .find(|t| t.name == "get_schema")
            .expect("get_schema tool registered");
        assert_eq!(
            get_schema
                .annotations
                .as_ref()
                .and_then(|a| a.open_world_hint),
            Some(false),
            "get_schema does not touch the network"
        );
        assert!(get_schema.output_schema.is_none());
    }

    #[test]
    fn enumerated_args_advertise_their_values_and_the_parsers_accept_every_one() {
        // The prose used to be the *only* place the legal values lived, so a client whose
        // tool-listing truncates long descriptions could not discover them. These constraints
        // put them in the schema itself — and this test pins the half that actually matters:
        // everything advertised must round-trip through the shared parser. A schema that
        // promises a value `config` rejects is worse than no schema.
        let tools = RssServer::tool_router().list_all();
        let props = |tool: &str| {
            tools
                .iter()
                .find(|t| t.name == tool)
                .unwrap_or_else(|| panic!("{tool} registered"))
                .input_schema
                .get("properties")
                .and_then(|p| p.as_object())
                .cloned()
                .unwrap_or_else(|| panic!("{tool} has properties"))
        };
        let fetch = props("fetch_feed");

        let members = |schema: &serde_json::Value| -> Vec<String> {
            schema
                .get("enum")
                .and_then(|e| e.as_array())
                .unwrap_or_else(|| panic!("expected an enum in {schema}"))
                .iter()
                .filter(|v| !v.is_null()) // `null` is the "argument omitted" member
                .map(|v| v.as_str().expect("enum members are strings").to_string())
                .collect()
        };

        let formats = members(&fetch["content_format"]);
        assert_eq!(formats, ["markdown", "text", "html", "none"]);
        for f in &formats {
            assert!(
                parse_content_format(f).is_some(),
                "content_format `{f}` is advertised but rejected by the parser"
            );
        }

        let modes = members(&fetch["dedupe"]);
        assert_eq!(modes, ["report", "off", "drop"]);
        for m in &modes {
            assert!(
                crate::config::parse_dedupe(m).is_ok(),
                "dedupe `{m}` is advertised but rejected by the parser"
            );
        }

        let commands = members(&props("get_schema")["command"]);
        assert_eq!(commands, ["fetch", "discover"]);
        for c in &commands {
            assert!(
                !crate::output::schema_for(c).is_null(),
                "get_schema command `{c}` is advertised but has no schema"
            );
        }

        // `cache_policy` is a `pattern`, not an `enum`, because `max-age:<duration>` is
        // open-ended. Assert the literal so a rewrite has to come back through this test —
        // JSON Schema `pattern` is ECMA-262, which has no inline-flag syntax, so an `(?i)`
        // would be a literal that matches nothing rather than a case-insensitivity switch.
        let pattern = fetch["cache_policy"]["pattern"]
            .as_str()
            .expect("cache_policy advertises a pattern");
        assert_eq!(
            pattern, r"^(revalidate|no-cache|cache-first|max-age:\S+)$",
            "keep this plain ECMA-262"
        );
        assert!(
            !pattern.contains("(?"),
            "no inline flags in a JSON Schema pattern"
        );
        for p in ["revalidate", "no-cache", "cache-first", "max-age:15m"] {
            assert!(
                crate::config::parse_cache_policy(p).is_ok(),
                "cache_policy `{p}` matches the advertised pattern but the parser rejects it"
            );
        }
    }

    #[test]
    fn arg_descriptions_carry_no_crate_internal_reasoning() {
        // Every `inputSchema` description ships to every client on every session, so notes
        // about Rust types are pure token cost to the reader who cannot act on them (the same
        // rule CLAUDE.md states for `model.rs`). Two of these leaked `Option<String>` rationale.
        let tools = RssServer::tool_router().list_all();
        for t in &tools {
            let Some(props) = t.input_schema.get("properties").and_then(|p| p.as_object()) else {
                continue;
            };
            for (name, schema) in props {
                let Some(desc) = schema.get("description").and_then(|d| d.as_str()) else {
                    continue;
                };
                for leak in ["Option<", "Vec<", "usize", "&str", "serde", "de_lenient"] {
                    assert!(
                        !desc.contains(leak),
                        "{}.{name} description leaks the crate-internal token `{leak}`: \
                         put it in a `//` comment instead — a doc comment here is billed to \
                         every client, every session.\n{desc}",
                        t.name
                    );
                }
            }
        }
    }

    #[test]
    fn tool_surface_documents_its_own_pacing_and_cache_behavior() {
        // The original field report listed conditional GET, per-host pacing, and RATE_LIMITED
        // as "missing" -- all three shipped, but nothing on the tool surface said so. This test
        // exists so that documentation cannot silently drift back out of sync.
        //
        // Read the *served* value, not the bare `SERVER_INSTRUCTIONS` const: a prior version of
        // this test read the const directly, so deleting `.with_instructions(SERVER_INSTRUCTIONS)`
        // in `get_info()` (i.e. shipping no instructions at all over the protocol) still passed
        // every assertion below -- the exact failure this test exists to catch.
        let (cache, dir) = temp_cache("tool-surface-docs");
        let server = RssServer::new(cache, test_http());
        let served = server
            .get_info()
            .instructions
            .expect("get_info() must advertise instructions to MCP clients");
        assert_eq!(
            served, SERVER_INSTRUCTIONS,
            "get_info().instructions must be exactly SERVER_INSTRUCTIONS -- the const alone is \
             not what a client sees"
        );
        std::fs::remove_dir_all(&dir).ok();

        let tools = RssServer::tool_router().list_all();
        let fetch = tools
            .iter()
            .find(|t| t.name == "fetch_feed")
            .expect("fetch_feed registered");
        let desc = fetch.description.as_deref().unwrap_or_default().to_string();
        let haystack = format!("{served}\n{desc}");

        for needle in [
            "RATE_LIMITED",
            "retry_after_seconds",
            "not_modified",
            "cache_age_seconds",
            "next_cursor",
            "cache_policy",
            "urls",
            "since",
        ] {
            assert!(
                haystack.contains(needle),
                "the tool surface must document '{needle}' -- an undiscoverable feature is \
                 indistinguishable from a missing one"
            );
        }

        // Every advertised argument must be named in the docs, so a new one ships documented
        // or reds here.
        //
        // Weaker than it looks: it is a substring check, and `limit`/`cursor`/
        // `max_content_chars`/`url` each also occur inside a longer name
        // (`suggested_limit`, `next_cursor`, …). All four do have genuine mentions, verified
        // by hand, but the loop can't tell — green here is not proof of a real explanation.
        let props = fetch
            .input_schema
            .get("properties")
            .and_then(|p| p.as_object())
            .expect("fetch_feed input_schema should have properties");
        for name in props.keys() {
            assert!(
                haystack.contains(name.as_str()),
                "fetch_feed argument '{name}' is not named anywhere in SERVER_INSTRUCTIONS or \
                 the fetch_feed description -- an agent cannot discover what it cannot guess"
            );
        }
    }

    #[tokio::test]
    async fn fetch_feed_success_uses_structured_content_with_summary_text() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(3))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("structured");

        let result = fetch_feed_inner(
            &test_http(),
            &cache,
            fetch_args(format!("{}/feed.xml", server.url())),
        )
        .await;
        let wire = serde_json::to_value(&result).expect("serialize result");

        // The typed FetchOutput lives in structuredContent (matching the tool output_schema).
        assert!(
            wire.get("structuredContent").is_some_and(|v| !v.is_null()),
            "success result should carry structuredContent: {wire}"
        );
        assert_eq!(wire["structuredContent"]["total_items"], 3);

        // The unstructured content is only a short summary, NOT a second full copy of the
        // payload (that would double the response and undercut the token budget).
        let summary = summary_text(&result).expect("a text summary");
        assert!(
            summary.contains("item(s)"),
            "summary should be terse: {summary}"
        );
        assert!(
            !summary.contains("\"feeds\""),
            "text content must not duplicate the full FetchOutput JSON: {summary}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn error_result_has_no_structured_content() {
        // An over-budget result is an error: it must stay text-only (the ErrorObj), with no
        // structuredContent, so it never violates the success output_schema (ADR-0013).
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("err-nostruct");

        let mut args = fetch_args(format!("{}/feed.xml", server.url()));
        args.max_response_tokens = Some(1);
        let result = fetch_feed_inner(&test_http(), &cache, args).await;
        let wire = serde_json::to_value(&result).expect("serialize result");

        assert_eq!(wire["isError"], true);
        assert!(
            wire.get("structuredContent").is_none_or(|v| v.is_null()),
            "error results must not carry structuredContent: {wire}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_over_budget_returns_structured_response_too_large() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("budget");

        let mut args = fetch_args(format!("{}/feed.xml", server.url()));
        args.max_response_tokens = Some(1); // force overflow

        let result = fetch_feed_inner(&test_http(), &cache, args).await;
        let (is_error, payload) = decode(&result);

        assert!(is_error, "an over-budget result must be an error");
        // The error payload is a structured ErrorObj the agent can parse to self-recover.
        assert_eq!(payload["code"], "RESPONSE_TOO_LARGE");
        assert!(
            payload["details"]["suggested_max_content_chars"]
                .as_u64()
                .is_some_and(|n| n >= 200),
            "must suggest a max_content_chars to retry with: {payload}"
        );
        assert!(
            payload["details"]["suggested_limit"].as_u64().is_some(),
            "must suggest a limit to retry with: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_content_cap_truncates_and_marks() {
        let long = "word ".repeat(200); // ~1000 chars
        let feed = format!(
            "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Feed</title>\
             <link>https://example.com/</link><item><title>Big</title>\
             <link>https://example.com/big</link><description><![CDATA[<p>{long}</p>]]></description>\
             </item></channel></rss>"
        );
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed)
            .create_async()
            .await;
        let (cache, dir) = temp_cache("trunc");

        let mut args = fetch_args(format!("{}/feed.xml", server.url()));
        args.max_content_chars = Some(15);

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(
            !is_error,
            "truncated-but-fitting result should succeed: {payload}"
        );
        assert_eq!(payload["feeds"][0]["items"][0]["content_truncated"], true);
        assert_eq!(
            payload["truncation"]["items_content_truncated"].as_u64(),
            Some(1)
        );
        assert_eq!(
            payload["truncation"]["applied_limit"].as_u64(),
            Some(MCP_DEFAULT_LIMIT as u64)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_rejects_bad_content_format() {
        let (cache, dir) = temp_cache("badfmt");
        let mut args = fetch_args("https://example.com/feed.xml".to_string());
        args.content_format = Some("yaml".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_rejects_bad_cache_policy() {
        let (cache, dir) = temp_cache("badcachepolicy");
        let mut args = fetch_args("https://example.com/feed.xml".to_string());
        args.cache_policy = Some("aggressive".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");
        let message = payload["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("no-cache"),
            "error should list valid forms: {message}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_rejects_bad_since() {
        let (cache, dir) = temp_cache("badsince");
        let mut args = fetch_args("https://example.com/feed.xml".to_string());
        args.since = Some("not-a-date".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A feed whose items are distinguishable by keyword, for `query` tests.
    fn feed_with_keyword_items() -> String {
        "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Feed</title>\
         <link>https://example.com/</link>\
         <item><title>Rust news</title><link>https://example.com/1</link></item>\
         <item><title>Python news</title><link>https://example.com/2</link></item>\
         <item><title>More rust</title><link>https://example.com/3</link></item>\
         </channel></rss>"
            .to_string()
    }

    #[tokio::test]
    async fn fetch_feed_query_argument_filters_items_before_limit() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_keyword_items())
            .create_async()
            .await;
        let (cache, dir) = temp_cache("query-filter");

        let mut args = fetch_args(format!("{}/feed.xml", server.url()));
        args.query = Some("rust".to_string());
        args.limit = Some(2);
        let out = output_of(&fetch_feed_inner(&test_http(), &cache, args).await);

        let titles: Vec<&str> = out.feeds[0]
            .items
            .iter()
            .filter_map(|i| i.title.as_deref())
            .collect();
        assert_eq!(
            titles.len(),
            2,
            "both rust items must ship, not 2 of the newest regardless of match: {titles:?}"
        );
        assert!(
            titles.iter().all(|t| t.to_lowercase().contains("rust")),
            "every shipped item must match the query: {titles:?}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_reports_applied_filters_for_a_query() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_keyword_items())
            .create_async()
            .await;
        let (cache, dir) = temp_cache("query-applied-filters");

        let mut args = fetch_args(format!("{}/feed.xml", server.url()));
        args.query = Some("rust".to_string());
        let out = output_of(&fetch_feed_inner(&test_http(), &cache, args).await);

        let applied = out
            .applied_filters
            .expect("query was supplied, so applied_filters must be present");
        assert_eq!(applied.query.as_deref(), Some("rust"));
        assert!(applied.since.is_none());
        assert_eq!(
            applied.items_filtered_out, 1,
            "the one non-matching item must be counted"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cursor_from_a_different_query_is_rejected() {
        // Same shape as `cursor_from_a_different_request_is_rejected`, but for `query`: a
        // cursor minted under one keyword filter must not validate a continuation call under
        // a different one, since the two filter different item sequences (defect check for
        // the fingerprint placeholder that used to sit here — see cursor.rs's `fingerprint`).
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-fp-query").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        args.query = Some("post".to_string());
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        let mut args = batch_args(urls);
        args.max_response_tokens = Some(full / 2);
        args.query = Some("something else".to_string()); // changes the request shape
        args.cursor = Some(cursor);
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);

        assert!(is_error, "a mismatched cursor must not be honored");
        assert_eq!(payload["code"], "USAGE_ERROR");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|m| m.contains("does not match")),
            "the error must say the cursor belongs to another request: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn fetch_feed_cache_first_avoids_network_on_warm_cache() {
        // Discriminates the wiring from a no-op: the default policy (Revalidate) would send
        // a conditional GET here and hit the mock; `cache-first` must serve the cached body
        // without any network call at all. Mirrors
        // `fetch::tests::cache_first_serves_stale_cache_without_network`.
        let mut server = mockito::Server::new_async().await;
        let (cache, dir) = temp_cache("cachepolicy-cachefirst");
        let url = format!("{}/feed.xml", server.url());

        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache
            .put(&meta, feed_with_items(1).as_bytes())
            .expect("seed cache");

        // A conditional GET (what Revalidate would send) would match and count as a hit;
        // `cache-first` must never send it.
        let mock = server
            .mock("GET", "/feed.xml")
            .match_header("if-none-match", "\"v1\"")
            .with_status(304)
            .expect(0)
            .create_async()
            .await;

        let mut args = fetch_args(url);
        args.cache_policy = Some("  Cache-First ".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(!is_error, "cache-first must not error: {payload}");
        mock.assert_async().await; // expect(0): fails if the network was hit.

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_item_missing_id_is_structured_not_found() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(3))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("getitem");

        let args = GetItemArgs {
            feed_url: format!("{}/feed.xml", server.url()),
            id: "0000000000000000".to_string(),
            max_content_chars: None,
        };
        let (is_error, payload) = decode(&get_item_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "NOT_FOUND");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fetch_args_coerce_stringified_integers() {
        // Many MCP clients serialize numeric tool arguments as JSON strings; the tool must
        // accept "25" exactly as it accepts 25 (see `de_lenient_opt_usize`). Before this fix
        // every such call failed with `invalid type: string "25", expected usize`.
        let args: FetchFeedArgs = serde_json::from_str(
            r#"{"url":"https://e.com/f","limit":"25","max_content_chars":"500","max_response_tokens":"8000"}"#,
        )
        .expect("stringified integers should deserialize");
        assert_eq!(args.limit, Some(25));
        assert_eq!(args.max_content_chars, Some(500));
        assert_eq!(args.max_response_tokens, Some(8000));
    }

    #[test]
    fn fetch_args_accept_native_integers() {
        let args: FetchFeedArgs =
            serde_json::from_str(r#"{"url":"https://e.com/f","limit":25,"max_content_chars":500}"#)
                .expect("native integers should still deserialize");
        assert_eq!(args.limit, Some(25));
        assert_eq!(args.max_content_chars, Some(500));
        assert_eq!(args.max_response_tokens, None);
    }

    #[test]
    fn fetch_args_absent_null_and_empty_string_are_none() {
        let absent: FetchFeedArgs =
            serde_json::from_str(r#"{"url":"https://e.com/f"}"#).expect("absent fields ok");
        assert_eq!(absent.limit, None);
        assert_eq!(absent.max_content_chars, None);

        let null: FetchFeedArgs = serde_json::from_str(
            r#"{"url":"https://e.com/f","limit":null,"max_content_chars":null}"#,
        )
        .expect("explicit null ok");
        assert_eq!(null.limit, None);
        assert_eq!(null.max_content_chars, None);

        // An empty string is treated as "unset" rather than a parse error — some clients send
        // "" for a cleared optional field.
        let empty: FetchFeedArgs = serde_json::from_str(r#"{"url":"https://e.com/f","limit":""}"#)
            .expect("empty string ok");
        assert_eq!(empty.limit, None);
    }

    #[test]
    fn fetch_args_reject_non_numeric_and_negative() {
        assert!(
            serde_json::from_str::<FetchFeedArgs>(r#"{"url":"u","limit":"twenty"}"#).is_err(),
            "a non-numeric string must not silently parse"
        );
        assert!(
            serde_json::from_str::<FetchFeedArgs>(r#"{"url":"u","limit":-5}"#).is_err(),
            "a negative number is not a valid usize"
        );
        assert!(
            serde_json::from_str::<FetchFeedArgs>(r#"{"url":"u","limit":"-5"}"#).is_err(),
            "a negative numeric string is not a valid usize"
        );
    }

    #[test]
    fn get_item_args_coerce_stringified_max_content_chars() {
        let args: GetItemArgs = serde_json::from_str(
            r#"{"feed_url":"https://e.com/f","id":"abc","max_content_chars":"1000"}"#,
        )
        .expect("stringified max_content_chars should deserialize");
        assert_eq!(args.max_content_chars, Some(1000));
    }

    #[test]
    fn urls_arg_accepts_every_stringified_form() {
        // Clients stringify every argument, so `urls` must survive the same abuse `limit` does.
        let cases = [
            r#"{"urls":["https://a/f","https://b/f"]}"#,
            r#"{"urls":"[\"https://a/f\", \"https://b/f\"]"}"#,
            r#"{"urls":"https://a/f\nhttps://b/f"}"#,
            r#"{"urls":"https://a/f, https://b/f"}"#,
        ];
        for raw in cases {
            let args: FetchFeedArgs =
                serde_json::from_str(raw).unwrap_or_else(|e| panic!("{raw} should parse: {e}"));
            assert_eq!(
                args.urls.as_deref(),
                Some(["https://a/f".to_string(), "https://b/f".to_string()].as_slice()),
                "failed for {raw}"
            );
        }

        // A single bare URL string is a one-element list.
        let one: FetchFeedArgs = serde_json::from_str(r#"{"urls":"https://a/f"}"#).unwrap();
        assert_eq!(
            one.urls.as_deref(),
            Some(["https://a/f".to_string()].as_slice())
        );

        // A comma inside a URL's query string is part of the URL, not a separator.
        let commas_in_query: FetchFeedArgs =
            serde_json::from_str(r#"{"urls":"https://a/f?ids=1,2,3"}"#).unwrap();
        assert_eq!(
            commas_in_query.urls.as_deref(),
            Some(["https://a/f?ids=1,2,3".to_string()].as_slice()),
            "a comma inside a query string must not split the URL"
        );

        // Newlines still separate even when a line contains an in-URL comma.
        let mixed: FetchFeedArgs =
            serde_json::from_str(r#"{"urls":"https://a/f?ids=1,2\nhttps://b/f"}"#).unwrap();
        assert_eq!(
            mixed.urls.as_deref(),
            Some(["https://a/f?ids=1,2".to_string(), "https://b/f".to_string()].as_slice()),
            "newlines separate; the in-URL comma is preserved"
        );

        // Absent, null, and empty all mean "not supplied".
        for raw in [
            r#"{"url":"u"}"#,
            r#"{"urls":null}"#,
            r#"{"urls":""}"#,
            r#"{"urls":[]}"#,
        ] {
            let args: FetchFeedArgs = serde_json::from_str(raw).unwrap();
            assert_eq!(args.urls, None, "failed for {raw}");
        }

        // A non-string element in the array is a hard error, not a silent skip.
        assert!(
            serde_json::from_str::<FetchFeedArgs>(r#"{"urls":[1,2]}"#).is_err(),
            "non-string array elements must be rejected"
        );
    }

    /// Serve two feeds and return `(http, cache, dir, urls, full_tokens)` — the shared setup
    /// for the pagination tests. `full_tokens` is the size of the *unpaginated* response, so
    /// each test can ask for a budget that provably forces a second page.
    async fn paging_fixture(
        server: &mut mockito::ServerGuard,
        tag: &str,
    ) -> (HttpClient, Cache, std::path::PathBuf, Vec<String>, usize) {
        server
            .mock("GET", "/a.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        server
            .mock("GET", "/b.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache(tag);
        let http = test_http();
        let urls = vec![
            format!("{}/a.xml", server.url()),
            format!("{}/b.xml", server.url()),
        ];
        let full = full_response_tokens(&http, &cache, batch_args(urls.clone())).await;
        (http, cache, dir, urls, full)
    }

    #[tokio::test]
    async fn drop_never_mints_a_cursor_it_could_not_redeem() {
        // The drop+cursor guard covers *redemption*; this covers minting. A first `drop` page
        // carries no cursor, so the guard does not fire and the request reaches the
        // fingerprint — stamped `drop`. A continuation token bound to it is unredeemable by
        // construction: passed back as `drop` it hits the guard, passed back as anything else
        // it fails the fingerprint check. Its `i`/`n` would also be wrong, indexing the
        // post-removal item list while a continuation refetches without the removal. So an
        // over-budget `drop` page must ship with `next_cursor: null` and say what to do.
        //
        // One URL on purpose: its items are distinct, so `drop` removes nothing and the page
        // overflows for exactly the same reason the `report` control does. Two copies of one
        // feed would let `drop` shed half the batch and fit, testing nothing.
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/one.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("dedupe-drop-no-cursor");
        let http = test_http();
        let url = format!("{}/one.xml", server.url());
        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;

        // Control: the identical over-budget page under the default `report` does page.
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            payload["truncation"]["next_cursor"].is_string(),
            "the control must actually be over budget and pageable: {payload}"
        );

        let mut args = fetch_args(url);
        args.max_response_tokens = Some(full / 2);
        args.dedupe = Some("drop".to_string());
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            !is_error,
            "the page is fine; only the cursor is withheld: {payload}"
        );
        assert!(
            payload["truncation"]["next_cursor"].is_null(),
            "drop must not mint a token no later call could redeem: {payload}"
        );
        assert!(
            payload["truncation"]["suggestion"]
                .as_str()
                .unwrap_or_default()
                .contains("dedupe='report'"),
            "a truncated page with no cursor must name the way forward: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_content_truncated_drop_page_keeps_the_get_item_hint() {
        // The cursorless-`drop` advice is *appended*, never assigned: a page can be both
        // content-truncated and over budget, and overwriting the marker's suggestion would
        // drop the get_item hint — the only route to a full body — exactly the loss the
        // paginated branch's `combined_suggestion` exists to prevent.
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/one.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("dedupe-drop-both-hints");
        let http = test_http();
        let url = format!("{}/one.xml", server.url());
        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;

        let mut args = fetch_args(url);
        args.max_response_tokens = Some(full / 2);
        args.max_content_chars = Some(4); // forces content truncation too
        args.dedupe = Some("drop".to_string());
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);

        assert!(
            payload["truncation"]["items_content_truncated"]
                .as_u64()
                .unwrap_or(0)
                > 0,
            "the fixture must actually content-truncate, or this proves nothing: {payload}"
        );
        let s = payload["truncation"]["suggestion"]
            .as_str()
            .unwrap_or_default();
        assert!(
            s.contains("get_item") && s.contains("dedupe='report'"),
            "both hints must survive: {s}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_cursorless_clauses_fit_inside_the_reserved_headroom() {
        // `CURSOR_HEADROOM_TOKENS` is measured from `worst_case_marker`, which carries a full
        // cursor and `PAGINATION_SUGGESTION`. A cursorless page swaps that clause for one of
        // these and carries no cursor, so it stays inside the reserve as long as the clause is
        // no longer — not automatic, and it would silently under-reserve if someone expanded
        // one of them.
        for (name, clause) in [
            ("drop", DROP_NOT_PAGEABLE_SUGGESTION),
            ("no-cache", NO_CACHE_NOT_PAGEABLE_SUGGESTION),
        ] {
            assert!(
                clause.len() <= PAGINATION_SUGGESTION.len(),
                "the cursorless-{name} clause ({} chars) must not exceed the pagination clause \
                 it replaces ({} chars), or the headroom reserve stops covering it",
                clause.len(),
                PAGINATION_SUGGESTION.len()
            );
        }
    }

    #[tokio::test]
    async fn no_cache_never_mints_a_cursor_it_could_not_honor() {
        // Pagination is cache-backed: a continuation forces `CacheFirst` and walks whatever the
        // body cache holds. `no-cache` fetches fresh bytes and does not write them, so a cursor
        // minted here would be redeemed against some *earlier* call's cached snapshot — the
        // cursor's `i` drained off a different item list, silently skipping and repeating. The
        // default `revalidate` refetches AND persists, so it pages correctly; `no-cache` gets a
        // bounded page with no cursor and advice instead.
        let mut server = mockito::Server::new_async().await;
        server
            .mock("GET", "/one.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("no-cache-no-cursor");
        let http = test_http();
        let url = format!("{}/one.xml", server.url());
        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;

        // Control: the identical over-budget page under the default policy does page.
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            payload["truncation"]["next_cursor"].is_string(),
            "the control must actually be over budget and pageable: {payload}"
        );

        let mut args = fetch_args(url);
        args.max_response_tokens = Some(full / 2);
        args.cache_policy = Some("no-cache".to_string());
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            !is_error,
            "the page is fine; only the cursor is withheld: {payload}"
        );
        assert!(
            payload["truncation"]["next_cursor"].is_null(),
            "no-cache must not mint a cursor the next page cannot honor: {payload}"
        );
        assert!(
            payload["truncation"]["suggestion"]
                .as_str()
                .unwrap_or_default()
                .contains("no-cache"),
            "a bounded page with no cursor must name the way forward: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cursor_headroom_covers_the_marker_it_reserves_for() {
        // The reserve must be an upper bound on everything the emitted result gains after
        // `paginate` measured it — the truncation marker *and* the summary clause in the
        // result's text — measured the same way the budget is. If it under-reserves, a page
        // filled exactly to `budget - headroom` ships over the caller's budget.
        let mut out = crate::model::FetchOutput::new("2026-01-01T00:00:00Z".to_string());
        let bare = core::estimate_response_tokens(&out);
        out.truncation = Some(worst_case_marker());
        let with_marker = core::estimate_response_tokens(&out);
        // The worst-case marker is both content-truncated and paginated, so the real
        // `truncation_note` emits both clauses — the widest note a summary can carry.
        let note = truncation_note(&out);
        assert!(
            note.contains("Content truncated") && note.contains("next_cursor"),
            "the worst-case note must carry both clauses: {note}"
        );

        let actual = (with_marker - bare) + note.chars().count().div_ceil(4);
        let reserved = *CURSOR_HEADROOM_TOKENS;
        assert!(
            actual <= reserved,
            "the marker plus its summary note costs {actual} tokens but only {reserved} are \
             reserved"
        );
        // ...and not wildly generous either: a bloated reserve silently shrinks every page.
        assert!(
            reserved <= actual + 30,
            "reserve {reserved} is far above the real cost {actual}"
        );
    }

    #[tokio::test]
    async fn over_budget_batch_returns_a_page_plus_a_cursor() {
        // The headline behavior change: an over-budget batch used to throw away every
        // successful fetch with RESPONSE_TOO_LARGE. It now ships what fits and hands back a
        // continuation token (ADR-0017).
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-first").await;

        let budget = full / 2;
        let mut args = batch_args(urls);
        args.max_response_tokens = Some(budget);
        let result = fetch_feed_inner(&http, &cache, args).await;
        let page = output_of(&result);

        assert!(
            page.total_items > 0,
            "an over-budget batch must still ship the items that fit"
        );
        let t = page
            .truncation
            .as_ref()
            .expect("a bounded page must carry a truncation marker");
        assert!(
            t.next_cursor.is_some(),
            "a bounded page must hand back a cursor: {t:?}"
        );
        assert!(
            t.items_omitted > 0,
            "it must report what it deferred: {t:?}"
        );
        assert_eq!(t.applied_limit, Some(MCP_DEFAULT_LIMIT));
        assert!(
            t.suggestion
                .as_deref()
                .is_some_and(|s| s.contains("next_cursor")),
            "the suggestion must tell the agent how to page: {:?}",
            t.suggestion
        );
        // Headroom check, end to end: the *emitted* result carries the marker, the cursor and
        // the summary text — none of which `paginate` measured — and must still fit the
        // caller's budget.
        let summary = summary_text(&result).expect("a text summary");
        let emitted = core::estimate_response_tokens(&page) + summary.chars().count().div_ceil(4);
        assert!(
            emitted <= budget,
            "the emitted result (payload + summary text) must fit the budget: {emitted} > {budget}"
        );
        assert!(
            summary.contains("next_cursor"),
            "the one-line summary must point at the cursor: {summary}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_page_that_is_both_truncated_and_paginated_keeps_both_hints() {
        // A page can be content-capped *and* bounded by the budget. Overwriting the suggestion
        // with the pagination advice would drop the only pointer to the full body (`get_item`),
        // so the two are combined.
        let long = "word ".repeat(200);
        let items: String = (0..10)
            .map(|i| {
                format!(
                    "<item><title>Big {i}</title><link>https://example.com/big/{i}</link>\
                     <description><![CDATA[<p>{long}</p>]]></description></item>"
                )
            })
            .collect();
        let feed = format!(
            "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel><title>Feed</title>\
             <link>https://example.com/</link>{items}</channel></rss>"
        );
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed)
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-both-hints");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let mut probe = fetch_args(url.clone());
        probe.max_content_chars = Some(120);
        let full = full_response_tokens(&http, &cache, probe).await;

        let budget = full / 2;
        let mut args = fetch_args(url);
        args.max_content_chars = Some(120);
        args.max_response_tokens = Some(budget);
        let result = fetch_feed_inner(&http, &cache, args).await;
        let page = output_of(&result);

        // This is the widest page the reserve has to cover: the longest suggestion text (both
        // clauses combined) and *both* summary clauses. Measure the whole emitted result.
        let summary = summary_text(&result).expect("a text summary");
        assert!(
            summary.contains("Content truncated") && summary.contains("next_cursor"),
            "the summary must carry both clauses: {summary}"
        );
        let emitted = core::estimate_response_tokens(&page) + summary.chars().count().div_ceil(4);
        assert!(
            emitted <= budget,
            "the widest emitted result must still fit the budget: {emitted} > {budget}"
        );

        let t = page
            .truncation
            .expect("a capped, bounded page carries a marker");
        assert!(
            t.items_content_truncated > 0,
            "content must be capped: {t:?}"
        );
        assert!(
            t.next_cursor.is_some(),
            "and the page must be bounded: {t:?}"
        );
        let suggestion = t.suggestion.unwrap_or_default();
        assert!(
            suggestion.contains("get_item") && suggestion.contains("next_cursor"),
            "both hints must survive: {suggestion}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_shed_error_feed_leaves_no_orphan_in_errors() {
        // `paginate` trims `feeds` but not `errors`. A failed feed dropped whole must not
        // linger in `errors[]` with no matching `feeds[]` entry (the two are documented as
        // mirrors) — it belongs to the next page, which resumes at exactly that feed.
        let mut server = mockito::Server::new_async().await;
        let _a = server
            .mock("GET", "/a.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let _bad = server
            .mock("GET", "/bad.xml")
            .with_status(404)
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-orphan-error");
        let http = test_http();
        let urls = vec![
            format!("{}/a.xml", server.url()),
            format!("{}/bad.xml", server.url()),
        ];

        let full = full_response_tokens(&http, &cache, batch_args(urls.clone())).await;
        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let result = fetch_feed_inner(&http, &cache, args).await;
        let page1 = output_of(&result);

        assert_eq!(
            page1.feeds.len(),
            1,
            "the budget must have shed the trailing feed: {:?}",
            page1.feeds.iter().map(|f| &f.feed_url).collect::<Vec<_>>()
        );
        assert!(
            page1.errors.is_empty(),
            "an error whose feed was shed must not be reported without it: {:?}",
            page1.errors
        );
        let cursor = page1
            .truncation
            .and_then(|t| t.next_cursor)
            .expect("page 1 must hand back a cursor");

        // The next page reaches the failed feed and reports it there, so nothing is lost.
        let mut args = batch_args(urls);
        args.cursor = Some(cursor);
        let page2 = output_of(&fetch_feed_inner(&http, &cache, args).await);
        assert_eq!(
            page2.errors.len(),
            1,
            "the failed feed must surface on the page that reaches it: {page2:?}"
        );
        assert!(
            page2
                .feeds
                .iter()
                .any(|f| f.status == crate::model::FeedStatus::Error),
            "and it must be present in feeds[] alongside its error"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cursor_from_a_different_request_is_rejected() {
        // Resuming at a position computed under different arguments would return wrong data,
        // so a fingerprint mismatch is a hard usage error, not a silent restart.
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-fp").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        let mut args = batch_args(urls);
        args.max_response_tokens = Some(full / 2);
        args.content_format = Some("text".to_string()); // changes the request shape
        args.cursor = Some(cursor);
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);

        assert!(is_error, "a mismatched cursor must not be honored");
        assert_eq!(payload["code"], "USAGE_ERROR");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|m| m.contains("does not match")),
            "the error must say the cursor belongs to another request: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cursor_past_the_end_of_the_url_list_is_rejected() {
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-oob").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        // Keep the (matching) fingerprint, move the feed position out of range.
        let mut c = crate::cursor::Cursor::decode(&cursor).expect("decode page 1 cursor");
        c.f = urls.len() + 5;

        let mut args = batch_args(urls);
        args.max_response_tokens = Some(full / 2);
        args.cursor = Some(c.encode());
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);

        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|m| m.contains("past the end")),
            "the error must name the out-of-range position: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_continuation_feed_is_not_reported_as_a_rolled_window() {
        // A feed that *errors* on the continuation also has zero items, but the reason is
        // already in feeds[].error — labelling it CACHE_WINDOW_ROLLED would misdirect.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-roll-vs-error");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        // Evict the cache, then make the refetch fail: the continuation's CacheFirst misses
        // and falls through to a network call that no longer has a mock behind it.
        cache.clear().expect("evict the cache");
        server.reset();

        let mut args = fetch_args(url);
        args.cursor = Some(cursor);
        let page2 = output_of(&fetch_feed_inner(&http, &cache, args).await);

        assert_eq!(page2.errors.len(), 1, "the failed refetch must be reported");
        assert!(
            !page2
                .warnings
                .iter()
                .any(|w| w.code == "CACHE_WINDOW_ROLLED"),
            "a fetch failure is not a rolled window: {:?}",
            page2.warnings
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn garbage_cursor_is_a_usage_error() {
        // The token is validated rather than silently ignored.
        let (cache, dir) = temp_cache("page-garbage");
        let mut args = fetch_args("https://example.invalid/feed.xml".to_string());
        args.cursor = Some("not-a-real-cursor!!".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|m| m.contains("cursor")),
            "the error must name the cursor: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn continuation_that_cannot_fit_an_item_says_earlier_pages_shipped() {
        // `paginate` can still raise RESPONSE_TOO_LARGE on page 2 (an item that fits no
        // budget). The agent must not read that as "the whole batch failed" — pages already
        // delivered are still good and the cursor is still valid.
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-cont-err").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        let mut args = batch_args(urls);
        args.max_response_tokens = Some(1); // no item can fit
        args.cursor = Some(cursor);
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);

        assert!(is_error);
        assert_eq!(payload["code"], "RESPONSE_TOO_LARGE");
        assert_eq!(
            payload["details"]["continuation"], true,
            "the error must be machine-readably marked as a continuation: {payload}"
        );
        assert!(
            payload["message"]
                .as_str()
                .is_some_and(|m| m.contains("continuation")),
            "the message must say earlier pages already shipped: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn continuation_warns_when_the_cached_window_rolled() {
        // Another caller can overwrite the cached body between pages (a `revalidate` fetch).
        // The item count then no longer matches what the cursor recorded: we resume anyway and
        // warn, because `warnings` is the additive "you should know, but here is your data"
        // channel (invariant 9).
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-roll");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");
        let c = crate::cursor::Cursor::decode(&cursor).expect("decode cursor");
        assert_eq!(c.f, 0, "a single-feed request resumes in feed 0");
        assert_eq!(
            c.n, 10,
            "the cursor records the window it was minted against"
        );

        // Roll the window: the same feed, now two items shorter.
        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache
            .put(&meta, feed_with_items(8).as_bytes())
            .expect("overwrite the cached body");

        // Page 2 gets the full budget (`max_response_tokens` is deliberately outside the
        // fingerprint), so everything the rolled window still holds ships in one go and the
        // resumed count is exact.
        let mut args = fetch_args(url);
        args.cursor = Some(cursor);
        let page2 = output_of(&fetch_feed_inner(&http, &cache, args).await);

        assert!(
            page2
                .warnings
                .iter()
                .any(|w| w.code == "CACHE_WINDOW_ROLLED"),
            "a shifted window must be warned about: {:?}",
            page2.warnings
        );
        assert_eq!(
            page2.total_items,
            8usize.saturating_sub(c.i),
            "the continuation resumes at the recorded index in the new, shorter window"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_cursor_resuming_at_index_zero_does_not_claim_the_window_rolled() {
        // `i == 0` drains nothing, so a differing item count cannot skip or repeat anything —
        // and `n` is then a placeholder, not a measurement. The batch deadline mints exactly
        // such a cursor for a feed it never attempted (`i: 0, n: 0`); warning on it would fire
        // CACHE_WINDOW_ROLLED on every continuation page of a deadline-bounded batch.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("resume-at-zero");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        // Take a real page-1 cursor for its fingerprint, then rewind it to the head of the
        // feed — the shape the deadline mints for a feed no page has touched.
        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");
        let mut c = crate::cursor::Cursor::decode(&cursor).expect("decode cursor");
        c.i = 0;
        c.n = 0;

        let mut args = fetch_args(url);
        args.cursor = Some(c.encode());
        let page = output_of(&fetch_feed_inner(&http, &cache, args).await);

        assert!(
            !page
                .warnings
                .iter()
                .any(|w| w.code == "CACHE_WINDOW_ROLLED"),
            "resuming at index 0 skips nothing, so there is no roll to report: {:?}",
            page.warnings
        );
        assert_eq!(
            page.total_items, 10,
            "the whole feed still ships: {:?}",
            page.truncation
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn batch_deadline_override_parses_and_falls_back() {
        let default = std::time::Duration::from_secs(MCP_BATCH_DEADLINE_SECS);
        assert_eq!(
            parse_batch_deadline(Some("5")),
            std::time::Duration::from_secs(5)
        );
        assert_eq!(
            parse_batch_deadline(Some(" 7 ")),
            std::time::Duration::from_secs(7),
            "trimmed, like the rate-limit overrides"
        );
        // `0` means "already expired", not "disabled": an operator turning the bound off has to
        // remove the variable, not zero it.
        assert_eq!(parse_batch_deadline(Some("0")), std::time::Duration::ZERO);
        assert_eq!(parse_batch_deadline(None), default, "absent falls back");
        assert_eq!(parse_batch_deadline(Some("")), default);
        assert_eq!(
            parse_batch_deadline(Some("soon")),
            default,
            "garbage falls back to the bound rather than removing it"
        );
    }

    /// A `TruncationInfo` carrying only the fields the merge tests care about.
    fn marker_with(feeds_omitted: usize, suggestion: Option<&str>) -> crate::model::TruncationInfo {
        crate::model::TruncationInfo {
            applied_limit: None,
            items_content_truncated: 0,
            items_omitted: 0,
            feeds_omitted,
            next_cursor: None,
            estimated_tokens: None,
            suggestion: suggestion.map(str::to_string),
        }
    }

    #[test]
    fn deadline_and_budget_feed_omissions_are_summed_not_overwritten() {
        // The two sets are disjoint: the deadline omits feeds that were never fetched, the
        // budget omits feeds that were fetched but did not fit. Reporting either count alone
        // understates how much the caller still has to retrieve.
        let mut marker = Some(crate::model::TruncationInfo {
            items_omitted: 12,
            ..marker_with(6, Some("budget advice"))
        });
        merge_deadline_omissions(&mut marker, Some(marker_with(40, Some("deadline advice"))));

        let m = marker.expect("the merged marker survives");
        assert_eq!(m.feeds_omitted, 46, "6 shed by the budget + 40 never tried");
        assert_eq!(
            m.items_omitted, 12,
            "the deadline omits whole feeds, never items"
        );
        assert_eq!(
            m.suggestion.as_deref(),
            Some("budget advice"),
            "the budget's advice already names the cursor that recovers both"
        );
    }

    #[test]
    fn a_deadline_marker_survives_when_the_page_has_none() {
        // `core::truncation_marker` returns None unless item *content* was truncated — the
        // common case — so a merge requiring both markers to be Some would silently drop every
        // ordinary deadline report.
        let mut marker = None;
        merge_deadline_omissions(&mut marker, Some(marker_with(3, Some("deadline advice"))));
        let m = marker.expect("the deadline's marker becomes the page's marker");
        assert_eq!(m.feeds_omitted, 3);
        assert_eq!(m.suggestion.as_deref(), Some("deadline advice"));

        // No deadline marker leaves an existing one exactly as it was.
        let mut marker = Some(marker_with(2, Some("budget advice")));
        merge_deadline_omissions(&mut marker, None);
        assert_eq!(marker.map(|m| m.feeds_omitted), Some(2));
    }

    /// A minimal HTTP/1.1 server that answers **every** request with `body` after `delay`, on
    /// its own OS threads. Returns its base URL; the listener lives as long as the test process.
    ///
    /// mockito answers instantly, and `core`'s deadline is a wall clock, so this is the only way
    /// to make the deadline expire *between* one feed and the next without either mutating the
    /// clock (`tokio::time::pause` races with real socket I/O) or the environment. `delay` must
    /// stay comfortably above the deadline a test pairs it with — the margin is what keeps the
    /// test deterministic.
    fn slow_feed_server(delay: std::time::Duration, body: String) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let body = body.clone();
                std::thread::spawn(move || {
                    use std::io::{BufRead, BufReader, Write};
                    // Drain the request head first: answering mid-write can trip the client.
                    if let Ok(peer) = stream.try_clone() {
                        let mut reader = BufReader::new(peer);
                        let mut line = String::new();
                        while reader.read_line(&mut line).unwrap_or(0) > 0 {
                            if line == "\r\n" {
                                break;
                            }
                            line.clear();
                        }
                    }
                    std::thread::sleep(delay);
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/rss+xml\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(body.as_bytes());
                    let _ = stream.flush();
                });
            }
        });
        base
    }

    /// Sends `()` to release the `/feed.xml` hold in [`feed_hold_server`]. A raw `Sender`
    /// would work too; the wrapper just makes the call site (`hold.release()`) read as intent
    /// rather than a bare channel send.
    struct HoldRelease(std::sync::mpsc::Sender<()>);

    impl HoldRelease {
        fn release(&self) {
            let _ = self.0.send(());
        }
    }

    /// A minimal HTTP/1.1 server that answers `/site` with `site_body` **immediately**, but
    /// blocks `/feed.xml`'s response until [`HoldRelease::release`] is called — no timer
    /// involved, on either path.
    ///
    /// This is the network-level analogue of `ratelimit::tests::cap_one_serializes_same_authority`'s
    /// "hold a permit open, then race a contender against a short timeout" shape: a caller can
    /// prove a second same-authority request is stuck *before* it ever reaches the socket
    /// (blocked in `HostGate::acquire`, because the first request's held response is still
    /// occupying the one permit) by racing it against a timeout while never releasing the
    /// hold — no wall-clock margin to tune, because the "blocked" side has no clock in it at
    /// all.
    fn feed_hold_server(feed_body: String, site_body: String) -> (String, HoldRelease) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = std::sync::Arc::new(std::sync::Mutex::new(Some(rx)));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let feed_body = feed_body.clone();
                let site_body = site_body.clone();
                let rx = rx.clone();
                std::thread::spawn(move || {
                    use std::io::{BufRead, BufReader, Write};
                    let mut path = String::new();
                    if let Ok(peer) = stream.try_clone() {
                        let mut reader = BufReader::new(peer);
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) > 0 {
                            path = line.split_whitespace().nth(1).unwrap_or("").to_string();
                        }
                        line.clear();
                        while reader.read_line(&mut line).unwrap_or(0) > 0 {
                            if line == "\r\n" {
                                break;
                            }
                            line.clear();
                        }
                    }
                    let (content_type, body) = if path == "/feed.xml" {
                        // Block until released. Only one request ever hits this path in these
                        // tests, so taking the single `Receiver` once is enough.
                        if let Some(rx) = rx.lock().unwrap_or_else(|e| e.into_inner()).take() {
                            let _ = rx.recv();
                        }
                        ("application/rss+xml", feed_body)
                    } else {
                        ("text/html", site_body)
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes());
                    let _ = stream.write_all(body.as_bytes());
                    let _ = stream.flush();
                });
            }
        });
        (base, HoldRelease(tx))
    }

    /// A homepage advertising exactly one alternate feed at `/feed.xml`, for
    /// [`feed_hold_server`]'s immediate-answer `/site` path.
    fn site_with_alternate_link() -> String {
        "<html><head><link rel=\"alternate\" type=\"application/rss+xml\" \
         href=\"/feed.xml\"></head></html>"
            .to_string()
    }

    #[tokio::test]
    async fn an_expired_deadline_ships_nothing_and_cursors_the_whole_batch() {
        // The deadline's OWN mint, the `stop.is_none()` one: nothing was fetched, so `paginate`
        // reports no stop and the marker `merge_deadline_omissions` carried over is what gets
        // the cursor — at feed 0, because the delivered prefix is empty. Unroutable URLs prove
        // it never reaches the network: an expired deadline attempts nothing.
        let (cache, dir) = temp_cache("deadline-expired");
        let urls: Vec<String> = (0..3)
            .map(|i| format!("http://127.0.0.1:1/f{i}.xml"))
            .collect();

        // Default budget on purpose: with zero items a small `max_response_tokens` would send
        // `paginate` down its `original_items == 0` branch and answer RESPONSE_TOO_LARGE.
        let page = output_of(
            &fetch_feed_with_deadline(
                &test_http(),
                &cache,
                batch_args(urls.clone()),
                std::time::Duration::ZERO,
            )
            .await,
        );

        assert!(page.feeds.is_empty(), "nothing was attempted: {page:?}");
        assert!(
            page.errors.is_empty(),
            "an unattempted feed did not fail: {:?}",
            page.errors
        );
        let t = page
            .truncation
            .as_ref()
            .expect("every omitted feed must be reported");
        assert_eq!(t.feeds_omitted, urls.len());
        assert!(
            t.estimated_tokens.is_some(),
            "a shipped marker always reports its size"
        );
        let c = crate::cursor::Cursor::decode(
            t.next_cursor
                .as_deref()
                .expect("the caller needs a cursor for the feeds it never got"),
        )
        .expect("decode the minted cursor");
        assert_eq!(
            (c.f, c.i, c.n),
            (0, 0, 0),
            "resume at the first feed, which is every feed: nothing was delivered"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The delay a slow-server test serves each response after, and the deadline it pairs with.
    /// The gap between them is the determinism margin: only the fetch holding the per-host
    /// permit can finish inside `DEADLINE`, and every sibling queued behind it is shed at the
    /// gate once the deadline passes (`HostGate::acquire_until`), so a page under this pair
    /// reliably ships a short prefix and reports the rest omitted.
    const SERVER_DELAY: std::time::Duration = std::time::Duration::from_millis(80);
    const DEADLINE: std::time::Duration = std::time::Duration::from_millis(25);

    /// A deadline worth several `SERVER_DELAY`s, for the one test that needs *more than one*
    /// feed delivered before a budget trims it. Since the deadline became a real bound, feeds
    /// on one host are delivered serially at roughly `SERVER_DELAY` apart, so the count is
    /// governed by how many fit — not by how many were admitted at t≈0.
    const COMPOSED_DEADLINE: std::time::Duration = std::time::Duration::from_millis(300);

    #[tokio::test]
    async fn a_page_bounded_by_both_the_deadline_and_the_budget_keeps_both_counts() {
        // The full composition on one page: core's deadline marker is lifted out BEFORE
        // `paginate` measures the payload, the budget's `PageStop` then assigns its own counts,
        // and `merge_deadline_omissions` adds them — with the BUDGET minting the cursor, because
        // it points earlier in the same list than the deadline's would.
        let base = slow_feed_server(SERVER_DELAY, feed_with_items(5));
        let urls: Vec<String> = (0..10).map(|i| format!("{base}/f{i}.xml")).collect();
        let (cache, dir) = temp_cache("deadline-and-budget");
        let http = test_http();

        // Measure the same deadline-bounded page under a generous budget first, so the budget
        // below is a fraction of the page actually being trimmed — not of the whole batch.
        let generous = output_of(
            &fetch_feed_with_deadline(&http, &cache, batch_args(urls.clone()), COMPOSED_DEADLINE)
                .await,
        );
        let delivered = generous.feeds.len();
        // A range, not an equality: delivery is timing-derived, and delivering *fewer* would
        // only mean the deadline bit harder — never a defect. Two conditions matter, and both
        // are about what this test is for: >1 so the budget below has a page to trim, and
        // <10 so the deadline genuinely omitted feeds for the counts to compose.
        //
        // Do NOT lower this back to `DEADLINE` — the deadline is now enforced at the per-host
        // permit, so a 25 ms budget against an 80 ms server delivers exactly one feed and the
        // budget half of this test stops exercising anything. Pinned by
        // `core::tests::deadline_is_a_real_wall_clock_bound_for_a_throttled_same_host_batch`.
        assert!(
            delivered > 1 && delivered < urls.len(),
            "need a multi-feed page that the deadline still trimmed; expected 2..{}, got \
             {delivered}",
            urls.len()
        );
        let full = core::estimate_response_tokens(&generous);

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let page =
            output_of(&fetch_feed_with_deadline(&http, &cache, args, COMPOSED_DEADLINE).await);

        assert!(
            page.total_items > 0,
            "a doubly-bounded page still ships what fits: {page:?}"
        );
        let t = page
            .truncation
            .as_ref()
            .expect("a doubly-bounded page must carry a marker");
        assert_eq!(
            t.feeds_omitted,
            urls.len() - page.feeds.len(),
            "the two sets are disjoint — never tried plus fetched-but-dropped — so every \
             requested feed is either delivered or counted: {t:?}"
        );
        let c = crate::cursor::Cursor::decode(
            t.next_cursor
                .as_deref()
                .expect("a trimmed page needs a cursor"),
        )
        .expect("decode the minted cursor");
        assert!(
            c.f < delivered,
            "the BUDGET minted this: its cursor points inside the fetched prefix, while the \
             deadline's would point just past it (feed {delivered})"
        );
        assert!(
            c.n > 0,
            "the budget measured the window it stopped in; the deadline's mint records n: 0"
        );
        assert!(
            t.suggestion
                .as_deref()
                .is_some_and(|s| s.contains("max_response_tokens")),
            "the budget's advice wins — it already names the cursor that recovers both: {:?}",
            t.suggestion
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_continuation_drained_by_a_roll_still_cursors_the_deadlines_feeds() {
        // The early-return mint. The resumed feed drains to nothing, so the page is "finished"
        // as far as the ROLL goes — but the deadline left feeds behind it that the caller has
        // never seen, and ending the loop there would silently lose them.
        //
        // The feeds behind the resumed one carry no items, which is what makes the whole page
        // itemless (the early-return condition) while `feeds[]` is non-empty (the mint's gate).
        let base = slow_feed_server(SERVER_DELAY, feed_with_items(0));
        let urls: Vec<String> = (0..10).map(|i| format!("{base}/f{i}.xml")).collect();
        let (cache, dir) = temp_cache("deadline-drained-continuation");
        let http = test_http();

        // A continuation is served cache-first, so seeding feed 0's window is what the resumed
        // feed returns.
        let meta = crate::cache::CacheMeta {
            feed_url: urls[0].clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache
            .put(&meta, feed_with_items(10).as_bytes())
            .expect("seed the resumed feed's window");

        // Take a real cursor for its fingerprint from an expired-deadline page (which fetches
        // nothing), then point it past the end of feed 0's window so the drain empties it.
        let page1 = output_of(
            &fetch_feed_with_deadline(
                &http,
                &cache,
                batch_args(urls.clone()),
                std::time::Duration::ZERO,
            )
            .await,
        );
        let mut c = crate::cursor::Cursor::decode(
            page1
                .truncation
                .and_then(|t| t.next_cursor)
                .as_deref()
                .expect("the expired page hands back a cursor"),
        )
        .expect("decode cursor");
        c.i = 10;
        c.n = 10;

        let mut args = batch_args(urls.clone());
        args.cursor = Some(c.encode());
        let page = output_of(&fetch_feed_with_deadline(&http, &cache, args, DEADLINE).await);

        assert_eq!(
            page.total_items, 0,
            "the drained feed and the itemless ones behind it leave nothing to ship: {page:?}"
        );
        assert!(
            !page.feeds.is_empty() && page.feeds.len() < urls.len(),
            "the resumed feed WAS fetched and the deadline stopped later ones: {} feeds",
            page.feeds.len()
        );
        let t = page
            .truncation
            .as_ref()
            .expect("the feeds the deadline never tried must be reported");
        assert_eq!(
            t.feeds_omitted,
            urls.len() - page.feeds.len(),
            "every requested feed is either delivered or counted: {t:?}"
        );
        assert!(
            t.estimated_tokens.is_some(),
            "this page ships a marker like any other, so it reports its size too"
        );
        let next = crate::cursor::Cursor::decode(
            t.next_cursor
                .as_deref()
                .expect("a roll the caller did not cause must not end the loop early"),
        )
        .expect("decode the minted cursor");
        assert_eq!(
            (next.f, next.i, next.n),
            (page.feeds.len(), 0, 0),
            "resume at the first feed the deadline never attempted — the end of the delivered \
             prefix — with no measurement of its window"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `FeedResult` that only has to carry a `feed_url`, for the position checks.
    fn placeholder_feed(url: &str) -> crate::model::FeedResult {
        crate::model::FeedResult::error(url, crate::model::ErrorObj::new("X", "x"))
    }

    #[test]
    fn a_short_feed_list_is_a_prefix_but_a_gap_or_an_overrun_is_not() {
        // The guard the cursor mints are gated on. The deadline makes `feeds[]` legitimately
        // SHORTER than the request, so length alone cannot be the test; what must never pass is
        // a gap, which shifts every later position by one and would point a cursor at the wrong
        // feed. Neither the gap nor the overrun is reachable from a real fetch, so this is the
        // only place they execute.
        let urls: Vec<String> = (0..4)
            .map(|i| format!("https://e.example/{i}.xml"))
            .collect();

        // Vacuous, short, and exact: all genuine prefixes.
        assert_eq!(
            first_position_mismatch(&[], &urls),
            None,
            "empty is a prefix"
        );
        let short = vec![placeholder_feed(&urls[0]), placeholder_feed(&urls[1])];
        assert_eq!(
            first_position_mismatch(&short, &urls),
            None,
            "the shape a deadline-bounded page has: shorter, but still a prefix"
        );
        let exact: Vec<_> = urls.iter().map(|u| placeholder_feed(u)).collect();
        assert_eq!(first_position_mismatch(&exact, &urls), None);

        // A gap: feed 1 was never attempted but 2 and 3 completed and shipped anyway. Note it
        // has the same length as a legitimate two-feed trim — only the pairwise check sees it.
        let gapped = vec![
            placeholder_feed(&urls[0]),
            placeholder_feed(&urls[2]),
            placeholder_feed(&urls[3]),
        ];
        let (idx, got, want) =
            first_position_mismatch(&gapped, &urls).expect("a gap is not a prefix");
        assert_eq!(
            (idx, got.as_str(), want.as_str()),
            (1, urls[2].as_str(), urls[1].as_str()),
            "the report names the first mismatching index and BOTH urls, which the counts \
             cannot distinguish from a deadline trim"
        );

        // More feeds than urls: the extra has nothing to be compared against.
        let mut over = exact.clone();
        over.push(placeholder_feed(&urls[0]));
        let (idx, _, want) =
            first_position_mismatch(&over, &urls).expect("an overrun is not a prefix");
        assert_eq!(idx, urls.len());
        assert!(
            want.contains("no url requested"),
            "explains the overrun: {want}"
        );
    }

    #[tokio::test]
    async fn a_cursor_that_measured_a_window_but_resumes_at_zero_stays_quiet() {
        // The case the `c.i > 0` gate deliberately gives up, pinned so the narrowing stays a
        // decision rather than an accident: a BUDGET cursor can carry `i: 0, n: 10` — a feed the
        // page dropped whole, whose window really was measured — and a genuine window change on
        // that feed is then not reported. Accepted because at `i == 0` nothing is drained: the
        // next page re-delivers the feed from its head whatever its window now holds, so the
        // warning's claim ("may skip or repeat items") would be false.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("measured-window-at-zero");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");
        let mut c = crate::cursor::Cursor::decode(&cursor).expect("decode cursor");
        // A measured window (`n`) with nothing yet drained from it (`i`) — unlike the deadline's
        // `i: 0, n: 0`, which records no measurement at all.
        c.i = 0;
        c.n = 10;

        // Roll the window shorter than the cursor recorded. A continuation reads the cache, so
        // this is what the next page sees.
        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache
            .put(&meta, feed_with_items(8).as_bytes())
            .expect("shrink the cached body");

        let mut args = fetch_args(url);
        args.cursor = Some(c.encode());
        let page = output_of(&fetch_feed_inner(&http, &cache, args).await);

        assert!(
            !page
                .warnings
                .iter()
                .any(|w| w.code == "CACHE_WINDOW_ROLLED"),
            "a cursor that drained nothing cannot skip or repeat, so the roll is not reported \
             even though the window really did change: {:?}",
            page.warnings
        );
        assert_eq!(
            page.total_items, 8,
            "the whole (shorter) window still ships from its head: {:?}",
            page.truncation
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_continuation_whose_window_rolled_past_the_cursor_ends_cleanly() {
        // The cached window can shrink to fewer items than the cursor already delivered, which
        // leaves the resumed feed empty. No budget change can produce the items — they are gone
        // from the window — so the honest answer is a successful empty page still carrying the
        // CACHE_WINDOW_ROLLED warning that explains it. A RESPONSE_TOO_LARGE here would throw
        // that warning away and tell the agent to retry with a smaller max_content_chars, which
        // can never help.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(10))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-roll-empty");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        let full = full_response_tokens(&http, &cache, fetch_args(url.clone())).await;
        let mut args = fetch_args(url.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");
        let c = crate::cursor::Cursor::decode(&cursor).expect("decode cursor");
        assert!(
            c.i > 0,
            "page 1 must have delivered items for the roll to swallow"
        );

        // Roll the window back to exactly the items page 1 already delivered: the drain empties
        // the feed and this page has nothing left to ship.
        let meta = crate::cache::CacheMeta {
            feed_url: url.clone(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache
            .put(&meta, feed_with_items(c.i).as_bytes())
            .expect("shrink the cached body");

        let mut args = fetch_args(url);
        args.cursor = Some(cursor);
        // The knob that used to force the error: with zero items left, `paginate` takes its
        // `original_items == 0` branch for any budget the bare envelope exceeds. The empty page
        // deliberately ignores the budget — there is nothing left to trim.
        args.max_response_tokens = Some(1);
        let page = output_of(&fetch_feed_inner(&http, &cache, args).await);

        assert_eq!(
            page.total_items, 0,
            "the rolled window has nothing left to ship: {page:?}"
        );
        assert!(
            page.warnings
                .iter()
                .any(|w| w.code == "CACHE_WINDOW_ROLLED"),
            "the warning explaining the empty page must survive: {:?}",
            page.warnings
        );
        assert!(
            page.truncation
                .as_ref()
                .and_then(|t| t.next_cursor.as_ref())
                .is_none(),
            "a page that can never ship an item must not hand back another cursor: {:?}",
            page.truncation
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_cursor_survives_defaults_echoed_back_but_not_a_real_change() {
        // Clients commonly echo a response's resolved values back on the next call. `limit: 25`
        // sent explicitly is the same request as `limit` omitted, and `parse_content_format` is
        // case-insensitive, so neither may invalidate the cursor mid-pagination. A genuinely
        // different limit still must.
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-fp-norm").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        // The same request, spelled out: the effective limit and a differently-cased format.
        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        args.limit = Some(MCP_DEFAULT_LIMIT);
        args.content_format = Some("Markdown".to_string());
        args.cursor = Some(cursor.clone());
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            !is_error,
            "echoing the resolved defaults back must not invalidate the cursor: {payload}"
        );

        // A different limit changes which items exist, so it must still be rejected.
        let mut args = batch_args(urls);
        args.max_response_tokens = Some(full / 2);
        args.limit = Some(3);
        args.cursor = Some(cursor);
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            is_error,
            "a different limit is a different request: {payload}"
        );
        assert_eq!(payload["code"], "USAGE_ERROR");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn continuation_reuses_the_page_one_since_cutoff() {
        // `since: "7d"` resolves to a new instant on every call. The fingerprint covers the
        // RAW string (so the continuation matches at all) and the resolved cutoff rides in the
        // cursor's `s` field (so every page filters against one instant).
        let mut server = mockito::Server::new_async().await;
        for path in ["/a.xml", "/b.xml"] {
            server
                .mock("GET", path)
                .with_status(200)
                .with_body(feed_with_dated_items(8, 4))
                .create_async()
                .await;
        }
        let (cache, dir) = temp_cache("page-since");
        let http = test_http();
        let urls = vec![
            format!("{}/a.xml", server.url()),
            format!("{}/b.xml", server.url()),
        ];
        // `FetchFeedArgs` is not `Clone`; rebuild the same request for each call.
        let args_for = |cursor: Option<String>, budget: Option<usize>| {
            let mut a = batch_args(urls.clone());
            a.since = Some("7d".to_string());
            a.cursor = cursor;
            a.max_response_tokens = budget;
            a
        };

        // The cutoff is real: the 4 year-old items per feed are filtered out before paging.
        let whole = output_of(&fetch_feed_inner(&http, &cache, args_for(None, None)).await);
        assert_eq!(whole.total_items, 16, "8 fresh items from each of 2 feeds");
        assert!(
            titles(&whole).iter().all(|t| t.starts_with("Fresh")),
            "the 7d cutoff must drop the year-old items: {:?}",
            titles(&whole)
        );
        let full = core::estimate_response_tokens(&whole);

        let cursor =
            cursor_of(&fetch_feed_inner(&http, &cache, args_for(None, Some(full / 2))).await)
                .expect("page 1 must hand back a cursor");
        let c1 = crate::cursor::Cursor::decode(&cursor).expect("decode cursor");
        let cutoff =
            c1.s.expect("a resolved `since` cutoff must ride in the cursor");
        let expected = (chrono::Utc::now() - chrono::Duration::days(7)).timestamp();
        assert!(
            (cutoff - expected).abs() < 60,
            "the cursor must carry the resolved cutoff ({cutoff} vs ~{expected})"
        );

        // Size the continuation, then ask for half of it: page 2 must truncate too, so the
        // page-3 cursor is unconditional — a missing one fails the test rather than skipping it.
        let rest = core::estimate_response_tokens(&output_of(
            &fetch_feed_inner(&http, &cache, args_for(Some(cursor.clone()), None)).await,
        ));
        let page2 = output_of(
            &fetch_feed_inner(
                &http,
                &cache,
                // Same raw `since` string, later resolved instant.
                args_for(Some(cursor.clone()), Some(rest / 2)),
            )
            .await,
        );
        assert!(page2.total_items > 0, "the continuation must be accepted");
        assert!(
            titles(&page2).iter().all(|t| t.starts_with("Fresh")),
            "the continuation must apply the cutoff too: {:?}",
            titles(&page2)
        );
        let next = page2
            .truncation
            .and_then(|t| t.next_cursor)
            .expect("a page bounded to half the remainder must hand back a cursor");
        let c2 = crate::cursor::Cursor::decode(&next).expect("decode page 2 cursor");
        assert_eq!(
            c2.s,
            Some(cutoff),
            "every page must carry page 1's cutoff, not a freshly resolved one"
        );

        // Sharp pin: two `7d` resolutions seconds apart are numerically identical, so equality
        // above cannot by itself prove the cursor's instant is what filters. Forge a cutoff far
        // from any fresh resolution — the fingerprint does not cover `s` — and watch the
        // year-old items reappear. A re-resolving implementation would still filter them out.
        let mut forged = c1;
        forged.s = Some((chrono::Utc::now() - chrono::Duration::days(500)).timestamp());
        let widened = output_of(
            &fetch_feed_inner(&http, &cache, args_for(Some(forged.encode()), None)).await,
        );
        assert!(
            titles(&widened).iter().any(|t| t.starts_with("Stale")),
            "the page must filter against the cursor's instant, not a freshly resolved one: {:?}",
            titles(&widened)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_blank_cursor_is_a_page_one_request() {
        // An empty string decodes as valid base64 and then fails JSON parsing, so without
        // normalizing it a client that fills every advertised optional property with "" gets a
        // hard USAGE_ERROR on what is plainly a page-1 request. `url: ""` already means "not
        // supplied" here; a blank cursor means the same.
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body(feed_with_items(3))
            .create_async()
            .await;
        let (cache, dir) = temp_cache("page-blank-cursor");
        let http = test_http();
        let url = format!("{}/feed.xml", server.url());

        for blank in ["", "   "] {
            let mut args = fetch_args(url.clone());
            args.cursor = Some(blank.to_string());
            let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
            assert!(
                !is_error,
                "a blank cursor ({blank:?}) must read as 'no cursor': {payload}"
            );
            assert_eq!(payload["total_items"], 3, "and return page 1 in full");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    // --- Task 16: the `dedupe` argument --------------------------------------------------

    /// Serve the same body at `/a.xml` and `/b.xml`, so every item is syndicated twice.
    ///
    /// [`feed_with_items`] emits no `<guid>`, so `feed-rs` synthesizes each entry's id from
    /// its link + title — deterministic and identical at both URLs, which is what makes the
    /// two copies group under `DuplicateKeyKind::Guid`.
    async fn overlapping_feeds(
        server: &mut mockito::ServerGuard,
        items: usize,
        tag: &str,
    ) -> (HttpClient, Cache, std::path::PathBuf, Vec<String>) {
        for path in ["/a.xml", "/b.xml"] {
            server
                .mock("GET", path)
                .with_status(200)
                .with_body(feed_with_items(items))
                .create_async()
                .await;
        }
        let (cache, dir) = temp_cache(tag);
        let urls = vec![
            format!("{}/a.xml", server.url()),
            format!("{}/b.xml", server.url()),
        ];
        (test_http(), cache, dir, urls)
    }

    #[tokio::test]
    async fn dedupe_reports_by_default_and_drops_on_request() {
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls) = overlapping_feeds(&mut server, 2, "dedupe").await;

        // Default: report, do not remove.
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, batch_args(urls.clone())).await);
        assert_eq!(payload["total_items"], 4, "reporting must not remove items");
        assert_eq!(
            payload["duplicates"].as_array().map(|a| a.len()),
            Some(2),
            "both syndicated entries should be grouped: {payload}"
        );
        // `feed_with_items` emits no `<guid>`, so this depends on feed-rs synthesizing
        // `entry.id` from link+title — an internal of that crate, not a contract of ours.
        // Accept `url` too: without the synthesized id the identical `<link>` at both URLs
        // still groups these, one rung down the key ladder. `tests/fetch.rs` pins `guid`
        // against a fixture with real `<guid>`s, which is where that belongs.
        let kind = payload["duplicates"][0]["key_kind"].as_str().unwrap_or("");
        assert!(
            kind == "guid" || kind == "url",
            "identical entries at two URLs must group on guid or url, got {kind:?}: {payload}"
        );
        assert_eq!(
            payload["feeds"][1]["item_count"], 2,
            "report must leave per-feed counts untouched (invariant 9)"
        );

        // off: no detection at all.
        let mut args = batch_args(urls.clone());
        args.dedupe = Some("off".to_string());
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert_eq!(payload["total_items"], 4);
        assert_eq!(
            payload["duplicates"].as_array().map(|a| a.len()),
            Some(0),
            "off must skip detection: {payload}"
        );

        // drop: keep the canonical first copy only. The blank `cursor` is deliberate — clients
        // that fill every advertised optional string with `""` mean "no cursor" (see
        // `resume_from_cursor`), so the drop+cursor guard must read it the same way rather than
        // rejecting a plain page-1 request.
        let mut args = batch_args(urls);
        args.dedupe = Some("drop".to_string());
        args.cursor = Some("  ".to_string());
        let (_, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert_eq!(
            payload["total_items"], 2,
            "drop must remove the later copies"
        );
        assert_eq!(payload["feeds"][0]["item_count"], 2);
        assert_eq!(payload["feeds"][1]["item_count"], 0);
        assert_eq!(
            payload["duplicates"].as_array().map(|a| a.len()),
            Some(2),
            "the groups stay as the audit trail of what was removed: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dedupe_rejects_an_unknown_mode() {
        // The brief's version of this test only round-tripped the string through serde and
        // asserted nothing about validation. Call the tool instead: an unknown mode must be a
        // structured USAGE_ERROR that names every mode the caller could have meant.
        let (cache, dir) = temp_cache("dedupe-bad-mode");
        let mut args = fetch_args("https://example.com/f.xml".to_string());
        args.dedupe = Some("maybe".to_string());

        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error, "an unknown dedupe mode must fail: {payload}");
        assert_eq!(payload["code"], "USAGE_ERROR");
        let message = payload["message"].as_str().unwrap_or_default();
        for mode in ["report", "off", "drop"] {
            assert!(
                message.contains(mode),
                "the message must name '{mode}' so an agent can self-correct: {payload}"
            );
        }
        assert!(
            message.contains("maybe"),
            "the message must echo the rejected value: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dedupe_drop_is_rejected_with_a_cursor() {
        // Deduping is a whole-result operation, but a continuation page only fetches
        // `urls[start_feed..]`: a duplicate whose canonical copy lived in an earlier feed is
        // invisible there, so the survivor becomes canonical and the same article ships twice.
        //
        // The cursor here is a *well-formed* token on purpose. A garbage one would fail
        // `Cursor::decode` with its own USAGE_ERROR whose message also contains "cursor", so
        // this test would pass with the guard deleted. The assertions below name `dedupe`,
        // which only this guard's message does — the fingerprint-mismatch message says "drop
        // it to start over", so asserting on "drop" alone would not discriminate either.
        let (cache, dir) = temp_cache("dedupe-cursor");
        let token = crate::cursor::Cursor {
            v: crate::cursor::CURSOR_VERSION,
            fp: "0123456789abcdef".to_string(),
            f: 0,
            i: 1,
            n: 3,
            s: None,
        }
        .encode();

        let mut args = fetch_args("https://example.com/f.xml".to_string());
        args.dedupe = Some("drop".to_string());
        args.cursor = Some(token.clone());
        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        assert_eq!(payload["code"], "USAGE_ERROR");
        let message = payload["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("dedupe") && message.contains("cursor"),
            "the message must explain the interaction: {payload}"
        );

        // Control: the identical cursor without `drop` fails for a different reason (this one
        // does not match the request), proving the assertion above is the guard's doing and not
        // any cursor rejection.
        let mut args = fetch_args("https://example.com/f.xml".to_string());
        args.cursor = Some(token);
        let (is_error, payload) = decode(&fetch_feed_inner(&test_http(), &cache, args).await);
        assert!(is_error);
        // Discriminate on a phrase unique to the guard, not on the word "dedupe": the
        // fingerprint-mismatch message legitimately lists `dedupe` among the arguments a
        // continuation must keep identical.
        assert!(
            !payload["message"]
                .as_str()
                .unwrap_or_default()
                .contains("cannot be combined"),
            "only the drop+cursor guard may report the drop/cursor interaction: {payload}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_cursor_is_bound_to_the_dedupe_mode_that_minted_it() {
        // `dedupe` occupies the last slot of the cursor fingerprint. `report` (the default)
        // and `off` produce different item *reporting*, so a continuation must not cross
        // between them — while the default and an explicit `report` are the same request and
        // must stay interchangeable, exactly as `limit`/`content_format` do.
        let mut server = mockito::Server::new_async().await;
        let (http, cache, dir, urls, full) = paging_fixture(&mut server, "page-fp-dedupe").await;

        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        let cursor = cursor_of(&fetch_feed_inner(&http, &cache, args).await)
            .expect("page 1 must hand back a cursor");

        // Spelling out the default is the same request.
        let mut args = batch_args(urls.clone());
        args.max_response_tokens = Some(full / 2);
        args.dedupe = Some("report".to_string());
        args.cursor = Some(cursor.clone());
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            !is_error,
            "an explicit `report` is the default spelled out: {payload}"
        );

        // A different mode is a different request.
        let mut args = batch_args(urls);
        args.max_response_tokens = Some(full / 2);
        args.dedupe = Some("off".to_string());
        args.cursor = Some(cursor);
        let (is_error, payload) = decode(&fetch_feed_inner(&http, &cache, args).await);
        assert!(
            is_error,
            "a different dedupe mode must not resume: {payload}"
        );
        assert_eq!(payload["code"], "USAGE_ERROR");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn resolve_urls_requires_exactly_one_of_url_or_urls() {
        let mut args = fetch_args("https://a/f".to_string());
        assert_eq!(
            resolve_urls(&args).unwrap(),
            vec!["https://a/f".to_string()]
        );

        // urls alone.
        args.url = None;
        args.urls = Some(vec!["https://a/f".into(), "https://b/f".into()]);
        assert_eq!(resolve_urls(&args).unwrap().len(), 2);

        // Both is ambiguous.
        args.url = Some("https://a/f".into());
        assert!(
            resolve_urls(&args).is_err(),
            "both url and urls must be rejected"
        );

        // Neither is a usage error.
        args.url = None;
        args.urls = None;
        assert!(
            resolve_urls(&args).is_err(),
            "neither url nor urls must be rejected"
        );

        // A whitespace-only `url` is the same as not supplying one.
        args.url = Some("   ".into());
        args.urls = None;
        assert!(
            resolve_urls(&args).is_err(),
            "a whitespace-only url must be rejected"
        );

        // Over the cap.
        args.url = None;
        args.urls = Some(
            (0..MCP_MAX_URLS + 1)
                .map(|i| format!("https://a/{i}"))
                .collect(),
        );
        let err = resolve_urls(&args).unwrap_err();
        assert!(
            err.to_string().contains(&MCP_MAX_URLS.to_string()),
            "the error should name the cap: {err}"
        );
    }
}
