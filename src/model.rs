//! Serialized output types — **the AI-facing API contract**.
//!
//! Field names here are a stable contract: agents depend on them. Optional fields are
//! serialized as `null` (never omitted) so the shape is predictable across every item.
//! The authoritative schema is produced from these structs via [`crate::output::schema_for`]
//! (`schemars`); the docs in the plan are only illustrative.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Output schema version. Bump on any breaking change to these structs.
pub const SCHEMA_VERSION: &str = "1";

/// Top-level result of `rss fetch`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FetchOutput {
    pub schema_version: String,
    /// RFC-3339 / ISO-8601 UTC timestamp of when this invocation ran.
    pub fetched_at: String,
    /// Total number of items returned across every feed (after `limit`/`--since`). Lets an
    /// agent budget before walking the `feeds` array.
    pub total_items: usize,
    /// Sum of every item's `content_tokens_est` (reflects truncation). The number to budget
    /// a response against.
    pub total_content_tokens_est: u64,
    pub feeds: Vec<FeedResult>,
    /// Feed-level errors mirrored here for quick scanning (also present per-feed).
    pub errors: Vec<ErrorObj>,
    /// Non-fatal data-quality warnings (e.g. a content converter fell back to a tag strip,
    /// or a feed's items are entirely undated). Empty `[]` normally; each carries its
    /// `feed_url`. Distinct from `errors`, which mean a feed failed outright.
    pub warnings: Vec<Warning>,
    /// Present (non-`null`) when this result was bounded — an item cap was applied, item
    /// bodies were truncated, or items were omitted to fit a size budget. `null` otherwise.
    /// Primarily populated by the MCP server, which bounds responses (see the `rss mcp`
    /// docs); the CLI populates it only when `--max-content-chars` truncates content.
    pub truncation: Option<TruncationInfo>,
}

impl FetchOutput {
    pub fn new(fetched_at: String) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            fetched_at,
            total_items: 0,
            total_content_tokens_est: 0,
            feeds: Vec::new(),
            errors: Vec::new(),
            warnings: Vec::new(),
            truncation: None,
        }
    }
}

/// A non-fatal data-quality note about a feed (the feed still parsed and produced items).
/// Surfaces silent fallbacks an agent should know about — e.g. lower-fidelity content
/// extraction, or items it cannot order by time.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Warning {
    /// The feed this warning pertains to, if applicable.
    pub feed_url: Option<String>,
    /// Stable, machine-readable code (e.g. `CONTENT_EXTRACTION_FALLBACK`, `UNDATED_ITEMS`).
    pub code: String,
    pub message: String,
}

/// Describes how a [`FetchOutput`] was bounded. A summary so an agent can tell at a glance
/// that it is not seeing the full, untruncated result and how to adjust.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TruncationInfo {
    /// The item cap actually applied (e.g. the MCP default of 25), or `null` if none.
    pub applied_limit: Option<usize>,
    /// Number of items whose `content` was truncated (e.g. by `max_content_chars`).
    pub items_content_truncated: usize,
    /// Number of items dropped entirely to fit a response budget. `0` unless the server
    /// shed items (a Tier-2 behavior; always `0` in the cap-and-error path).
    pub items_omitted: usize,
    /// Rough token estimate of the (possibly reduced) serialized response, if computed.
    pub estimated_tokens: Option<usize>,
    /// Human/agent-facing hint on how to adjust the request (e.g. which knob to pass).
    pub suggestion: Option<String>,
}

/// Outcome of fetching a single feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeedStatus {
    /// Fetched and parsed fresh content.
    Ok,
    /// Server returned `304 Not Modified`; served from cache.
    NotModified,
    /// The feed failed to fetch or parse; see `error`.
    Error,
}

/// Per-feed result.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FeedResult {
    pub feed_url: String,
    pub status: FeedStatus,
    pub from_cache: bool,
    pub title: Option<String>,
    pub site_url: Option<String>,
    /// Feed-level last-updated timestamp (RFC-3339 UTC), if the feed provides one.
    pub updated: Option<String>,
    /// Number of items returned for this feed (equals `items.len()`; surfaced as an explicit
    /// budgeting count).
    pub item_count: usize,
    /// Sum of this feed's items' `content_tokens_est` (reflects truncation).
    pub content_tokens_est_total: u64,
    pub items: Vec<Item>,
    pub error: Option<ErrorObj>,
}

impl FeedResult {
    /// Construct an error result for a feed that failed before producing items.
    pub fn error(feed_url: impl Into<String>, error: ErrorObj) -> Self {
        Self {
            feed_url: feed_url.into(),
            status: FeedStatus::Error,
            from_cache: false,
            title: None,
            site_url: None,
            updated: None,
            item_count: 0,
            content_tokens_est_total: 0,
            items: Vec::new(),
            error: Some(error),
        }
    }
}

/// Which feed field the stable [`Item::id`] was derived from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IdSource {
    Link,
    Guid,
    Hash,
}

/// The format of [`Item::content`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContentFormat {
    #[default]
    Markdown,
    Text,
    Html,
    /// Content extraction disabled (`content` will be `null`).
    None,
}

/// A single feed item / entry.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Item {
    /// Stable, deterministic identifier (see [`crate::identity`]). Stable across runs.
    pub id: String,
    pub id_source: IdSource,
    pub feed_url: String,
    pub title: Option<String>,
    /// Resolved, absolute permalink for the item.
    pub url: Option<String>,
    pub authors: Vec<String>,
    /// RFC-3339 UTC publication timestamp.
    pub published: Option<String>,
    /// RFC-3339 UTC last-updated timestamp.
    pub updated: Option<String>,
    pub summary: Option<String>,
    /// Item body in the requested `content_format` (or `null` when `--content none`).
    pub content: Option<String>,
    pub content_format: ContentFormat,
    /// Rough token estimate for `content` (for agent budgeting). Reflects the truncated
    /// content when `content_truncated` is `true`.
    pub content_tokens_est: u32,
    /// `true` when `content` was cut short (e.g. by `max_content_chars` or a response
    /// budget). The body ends with an ellipsis marker; fetch the item via `get_item` /
    /// `rss show` without a cap for the full text.
    pub content_truncated: bool,
    /// 16-hex SHA-256 of the *full, pre-truncation* extracted content in the requested
    /// `content_format`. Stable across runs, so an agent can detect when an item's body
    /// changed without diffing text. `null` when `content` is `null` (`--content none`).
    pub content_hash: Option<String>,
    pub categories: Vec<String>,
    pub enclosures: Vec<Enclosure>,
    /// The raw feed-provided guid/id, for reference (not necessarily stable).
    pub guid: Option<String>,
}

/// A media attachment (podcast audio, image, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Enclosure {
    pub url: String,
    pub mime: Option<String>,
    pub length: Option<u64>,
}

/// A structured, machine-readable error. Emitted to stdout under `--format json` and
/// always carried in [`FeedResult::error`] / [`FetchOutput::errors`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ErrorObj {
    /// The feed this error pertains to, if applicable.
    pub feed_url: Option<String>,
    /// Stable error code enum value (e.g. `FEED_FETCH_FAILED`). See [`crate::error`].
    pub code: String,
    pub message: String,
    /// Free-form extra context (HTTP status, etc.). `{}` when empty.
    #[serde(default)]
    pub details: serde_json::Value,
}

impl ErrorObj {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            feed_url: None,
            code: code.into(),
            message: message.into(),
            details: serde_json::Value::Object(Default::default()),
        }
    }

    pub fn with_feed(mut self, feed_url: impl Into<String>) -> Self {
        self.feed_url = Some(feed_url.into());
        self
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = details;
        self
    }
}

/// Top-level result of `rss discover`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscoverOutput {
    pub schema_version: String,
    pub site_url: String,
    pub feeds: Vec<DiscoveredFeed>,
}

impl DiscoverOutput {
    pub fn new(site_url: impl Into<String>, feeds: Vec<DiscoveredFeed>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            site_url: site_url.into(),
            feeds,
        }
    }
}

/// A feed discovered on a website's homepage.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DiscoveredFeed {
    pub url: String,
    /// `"rss" | "atom" | "json" | "unknown"`.
    pub feed_type: String,
    pub title: Option<String>,
}
