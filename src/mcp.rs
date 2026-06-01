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

use crate::cache::Cache;
use crate::error::RssError;

/// Run the MCP server over stdio until the client disconnects. **Owner: `mcp` agent.**
pub async fn serve_stdio(cache: Cache) -> Result<(), RssError> {
    let _ = cache;
    todo!("mcp: implement rmcp stdio server exposing core operations as tools")
}
