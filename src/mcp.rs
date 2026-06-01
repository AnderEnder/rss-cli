//! Model Context Protocol server (stdio transport). **Owner: `mcp` agent.**
//!
//! Runs `rss mcp`: exposes the same core operations as MCP tools so agents can call the
//! tool directly. Implement with `rmcp` 1.7 (`#[tool]` / `#[tool_router]` macros,
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
use rmcp::model::{CallToolResult, Content, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::cache::Cache;
use crate::config::FetchParams;
use crate::core;
use crate::error::RssError;
use crate::model::ContentFormat;
use crate::output;

/// Human-readable guidance surfaced to MCP clients during `initialize`.
const SERVER_INSTRUCTIONS: &str = "\
AI-friendly RSS/Atom tools. All tools return JSON text matching the rss-cli output \
contract (use get_schema for the authoritative shapes). fetch_feed retrieves and parses a \
feed; discover_feeds finds feeds advertised on a website; get_item returns a single item by \
its stable id; get_schema returns the JSON Schema for the 'fetch' or 'discover' output.";

// === Tool argument structs (deserialized from MCP `arguments`; schema'd for clients) ===

/// Arguments for the `fetch_feed` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct FetchFeedArgs {
    /// The RSS/Atom feed URL to fetch and parse.
    url: String,
    /// Content extraction format: `markdown` (default), `text`, `html`, or `none`.
    #[serde(default)]
    content_format: Option<String>,
    /// Maximum number of items to return (most recent first); omit for all.
    #[serde(default)]
    limit: Option<usize>,
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
    /// The stable item id, as returned by `fetch_feed`.
    id: String,
}

/// Arguments for the `get_schema` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct GetSchemaArgs {
    /// Which command's output schema to return: `fetch` or `discover`.
    command: String,
}

/// MCP server state: the shared, cheaply-cloneable HTTP cache plus the generated
/// tool router. Built once in [`serve_stdio`] and shared across all tool calls.
#[derive(Clone)]
struct RssServer {
    cache: Cache,
    tool_router: ToolRouter<Self>,
}

impl RssServer {
    fn new(cache: Cache) -> Self {
        Self {
            cache,
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
        description = "Fetch and parse an RSS/Atom feed by URL. Returns the FetchOutput JSON \
        (schema: get_schema command=fetch). content_format is one of markdown|text|html|none; \
        limit caps items (most recent first)."
    )]
    async fn fetch_feed(
        &self,
        Parameters(args): Parameters<FetchFeedArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let mut params = FetchParams::default();
        if let Some(cf) = args.content_format.as_deref() {
            match parse_content_format(cf) {
                Some(fmt) => params.content_format = fmt,
                None => {
                    return Ok(tool_error(format!(
                        "invalid content_format '{cf}' (expected markdown|text|html|none)"
                    )));
                }
            }
        }
        params.limit = args.limit;

        let out = core::fetch_feeds(std::slice::from_ref(&args.url), &params, &self.cache).await;
        Ok(json_result(&out))
    }

    /// Discover feeds advertised on a website's homepage.
    #[tool(
        description = "Discover RSS/Atom/JSON feeds advertised on a website. Returns the \
        DiscoverOutput JSON (schema: get_schema command=discover)."
    )]
    async fn discover_feeds(
        &self,
        Parameters(args): Parameters<DiscoverFeedsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match core::discover_feeds(&args.site_url, &FetchParams::default()).await {
            Ok(out) => Ok(json_result(&out)),
            Err(e) => Ok(tool_error(e.to_string())),
        }
    }

    /// Fetch a feed and return the single item whose stable id matches `id`.
    #[tool(
        description = "Fetch a feed and return the single Item matching a stable id (from \
        fetch_feed). Returns the Item JSON, or an error if the id is not present."
    )]
    async fn get_item(
        &self,
        Parameters(args): Parameters<GetItemArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match core::show_item(
            &args.feed_url,
            &args.id,
            &FetchParams::default(),
            &self.cache,
        )
        .await
        {
            Ok(Some(item)) => Ok(json_result(&item)),
            Ok(None) => Ok(tool_error(format!(
                "item '{}' not found in {}",
                args.id, args.feed_url
            ))),
            Err(e) => Ok(tool_error(e.to_string())),
        }
    }

    /// Return the authoritative JSON Schema for a command's output.
    #[tool(
        description = "Return the authoritative JSON Schema for a command's output. \
        command is 'fetch' or 'discover'."
    )]
    async fn get_schema(
        &self,
        Parameters(args): Parameters<GetSchemaArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let schema = output::schema_for(&args.command);
        if schema.is_null() {
            return Ok(tool_error(format!(
                "unknown command '{}' (expected 'fetch' or 'discover')",
                args.command
            )));
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

/// Serialize a value as pretty JSON and wrap it in a successful tool result.
///
/// Serialization of our own model types cannot realistically fail, but if it ever does we
/// surface it as a tool error rather than panicking the server.
fn json_result<T: Serialize>(value: &T) -> CallToolResult {
    match serde_json::to_string_pretty(value) {
        Ok(json) => CallToolResult::success(vec![Content::text(json)]),
        Err(e) => tool_error(format!("failed to serialize result: {e}")),
    }
}

/// Build a tool-level error result (`is_error: true`) carrying a plain-text message, so the
/// calling agent sees the failure without it being a transport/protocol error.
fn tool_error(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![Content::text(message.into())])
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
    let server = RssServer::new(cache);
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
