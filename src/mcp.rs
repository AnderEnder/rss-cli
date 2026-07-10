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
AI-friendly RSS/Atom tools. All tools return JSON text matching the rss-cli output \
contract (use get_schema for the authoritative shapes). fetch_feed retrieves and parses a \
feed; discover_feeds finds feeds advertised on a website; get_item returns a single item by \
its stable id; get_schema returns the JSON Schema for the 'fetch' or 'discover' output. \
Responses are size-bounded: fetch_feed caps items (default 25) and rejects oversized results \
with a RESPONSE_TOO_LARGE error carrying suggested limit/max_content_chars to retry with.";

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

// === Tool argument structs (deserialized from MCP `arguments`; schema'd for clients) ===

/// Arguments for the `fetch_feed` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct FetchFeedArgs {
    /// The RSS/Atom feed URL to fetch and parse.
    url: String,
    /// Content extraction format: `markdown` (default), `text`, `html`, or `none`.
    #[serde(default)]
    content_format: Option<String>,
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
    /// Which command's output schema to return: `fetch` or `discover`.
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
    /// Fetch and parse a single RSS/Atom feed. Returns the full `FetchOutput` (one entry in
    /// `feeds`), so feed-level errors surface as a `FeedStatus::Error` entry rather than a
    /// tool failure.
    ///
    /// Note: the frozen module doc sketches `{ ..., since? }` returning a single `FeedResult`;
    /// the assigned task spec supersedes that — args are `{ url, content_format?, limit? }` and
    /// we return the whole `FetchOutput`.
    #[tool(
        description = "Fetch and parse an RSS/Atom feed by URL. Returns the FetchOutput as \
        structured content (and a one-line text summary); schema: get_schema command=fetch. \
        content_format is one of markdown|text|html|none; limit caps items, newest first \
        (DEFAULT 25 when omitted). max_content_chars truncates each item body (flagged \
        content_truncated). The response is size-bounded by max_response_tokens; if it would \
        overflow, the tool returns a RESPONSE_TOO_LARGE error whose details include \
        suggested_limit and suggested_max_content_chars to retry with. Provider notes: some \
        feeds (e.g. Reddit comment .rss) populate only updated, not published, and append the \
        original post to a comment listing (so a comment feed can return one more item than \
        limit); search.rss results are best-effort and may be sparse.",
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
        match core::discover_feeds(&args.site_url, &FetchParams::default()).await {
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
        thread) returns RESPONSE_TOO_LARGE with a suggested_max_content_chars to retry with.",
        annotations(read_only_hint = true, idempotent_hint = true, open_world_hint = true),
        output_schema = rmcp::handler::server::tool::schema_for_type::<crate::model::Item>()
    )]
    async fn get_item(
        &self,
        Parameters(args): Parameters<GetItemArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        Ok(get_item_inner(&self.cache, args).await)
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
async fn fetch_feed_inner(http: &HttpClient, cache: &Cache, args: FetchFeedArgs) -> CallToolResult {
    let mut params = FetchParams::default();
    if let Some(cf) = args.content_format.as_deref() {
        match parse_content_format(cf) {
            Some(fmt) => params.content_format = fmt,
            None => {
                return tool_error_code(
                    "USAGE_ERROR",
                    format!("invalid content_format '{cf}' (expected markdown|text|html|none)"),
                    Some(&args.url),
                );
            }
        }
    }
    // Apply a default item cap so a single huge feed doesn't blow the response budget.
    params.limit = Some(args.limit.unwrap_or(MCP_DEFAULT_LIMIT));
    params.max_content_chars = args.max_content_chars;

    let mut out =
        core::fetch_feeds_with(std::slice::from_ref(&args.url), &params, cache, http).await;

    // Reject oversized results with an actionable error rather than letting the client trip
    // its own tool-result-too-large limit on an opaque failure.
    let budget = args
        .max_response_tokens
        .unwrap_or(MCP_DEFAULT_MAX_RESPONSE_TOKENS);
    match core::enforce_response_budget(&out, budget) {
        Ok(estimated) => {
            // A marker is emitted only when content was actually truncated; the default cap
            // is documented in the tool description, so an untruncated response stays clean.
            if let Some(mut marker) = core::truncation_marker(
                &out,
                params.limit,
                Some(
                    "content was truncated; call get_item for the full body of a specific item"
                        .to_string(),
                ),
            ) {
                marker.estimated_tokens = Some(estimated);
                out.truncation = Some(marker);
            }
            let summary = fetch_summary(&out, &args.url);
            structured_result(&out, summary)
        }
        Err(e) => tool_error_obj(&e, Some(&args.url)),
    }
}

/// A one-line, human/agent-readable summary of a `fetch_feed` result. Kept terse on purpose:
/// it is the *unstructured* `content` companion to the full `structured_content` payload, so
/// we don't duplicate the whole FetchOutput as text (which would ~double the response and
/// undercut the token budget — see ADR-0011/ADR-0013).
fn fetch_summary(out: &crate::model::FetchOutput, url: &str) -> String {
    match out.feeds.first() {
        Some(feed) if feed.error.is_some() => {
            let code = feed
                .error
                .as_ref()
                .map(|e| e.code.as_str())
                .unwrap_or("ERROR");
            format!("fetch_feed: {url} returned an error ({code}); see structuredContent.")
        }
        Some(feed) => {
            let title = feed.title.as_deref().unwrap_or(url);
            let mut s = format!(
                "Fetched {} item(s) (~{} content tokens) from \"{title}\".",
                out.total_items, out.total_content_tokens_est
            );
            if out.truncation.is_some() {
                s.push_str(" Content truncated (see structuredContent.truncation).");
            }
            s.push_str(" Full data in structuredContent.");
            s
        }
        None => format!("fetch_feed: no result for {url}."),
    }
}

/// Core of the `get_item` tool, free of the `#[tool]` macro plumbing. Guards the
/// full-content escape hatch: a single item that still exceeds the budget yields a
/// `RESPONSE_TOO_LARGE` error rather than tripping the client limit.
async fn get_item_inner(cache: &Cache, args: GetItemArgs) -> CallToolResult {
    let params = FetchParams {
        max_content_chars: args.max_content_chars,
        cache_policy: CachePolicy::CacheFirst,
        ..FetchParams::default()
    };
    match core::show_item(&args.feed_url, &args.id, &params, cache).await {
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

    fn fetch_args(url: String) -> FetchFeedArgs {
        FetchFeedArgs {
            url,
            content_format: None,
            limit: None,
            max_content_chars: None,
            max_response_tokens: None,
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
        let (is_error, payload) = decode(&get_item_inner(&cache, args).await);
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
}
