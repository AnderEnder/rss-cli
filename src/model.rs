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
    /// What filtering ran (`since` and/or `query`) and what it removed, combined across
    /// every feed in this batch. `null` when neither was supplied.
    pub applied_filters: Option<AppliedFilters>,
    /// Groups of items in this batch that resolve to the same underlying entry — most
    /// often the same article syndicated through two feeds, but also a single feed that
    /// repeats an entry (see [`DuplicateGroup`]). Empty `[]` when nothing matched. Reporting
    /// only: nothing is removed unless a caller opts into `dedupe: "drop"`
    /// ([`crate::core::apply_dedupe`]), so request order and per-feed `item_count` stay intact
    /// by default. Empty `[]` also when `dedupe: "off"` skipped detection — that is
    /// indistinguishable from "detection ran and found nothing", by design (ADR-0018).
    ///
    /// **Per response, not cumulative** (like `applied_filters`), and each group is reported
    /// exactly once. Grouping runs over the batch *before* an MCP page is trimmed to its token
    /// budget, because the groups are part of the payload being measured. Under `report` a
    /// group can therefore name an item the page budget then omitted: that item ships on the
    /// next page, but *without* its group — a continuation only fetches the feeds from its
    /// resume point onward and skips the items already delivered, so the copy it would be
    /// grouped with is no longer in view. Keep the groups from every page if you are
    /// reconciling a paged batch. Under `dedupe: "drop"` the groups are deliberately an audit
    /// trail of items that are already gone, so they name ids no longer present in `feeds[]` at
    /// all — and such a response never carries a `next_cursor`, since `drop` cannot be paged.
    pub duplicates: Vec<DuplicateGroup>,
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
            applied_filters: None,
            duplicates: Vec::new(),
        }
    }
}

/// Which field matched when grouping duplicate items (see [`DuplicateGroup`]).
///
/// Checked in this order — `Guid`, then `Url`, then `ContentHash` — because each is
/// progressively less reliable as a cross-feed identity signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DuplicateKeyKind {
    /// The feed-supplied guid — the most reliable key *within one batch*, since two feeds
    /// syndicating the same entry usually carry the same guid. (That is a different property
    /// from guid *stability across fetches*, which [`crate::identity`] documents as poor and
    /// which is exactly why `id` is a content hash rather than the guid.)
    Guid,
    /// The resolved item permalink, used when no guid is available.
    Url,
    /// The content hash, used only when neither a guid nor a url is available.
    ///
    /// **Lossy.** [`Item::content_hash`] is a hash of the *body text* alone — it carries no
    /// title, url, or date. Two genuinely different items that happen to share body text
    /// (an empty body, a shared boilerplate stub, a "read more on our site" placeholder)
    /// hash identically and are reported as a false-positive duplicate. This kind is only
    /// reached when both `guid` and `url` are absent/empty, so it is rare in practice, but
    /// callers that care about precision should treat a `ContentHash`-kind group as a hint
    /// to verify, not a certainty — this crate does not add heuristics to filter these out;
    /// it reports the ambiguity and lets the caller decide.
    ContentHash,
}

/// A group of items that share a [`DuplicateKeyKind`] key.
///
/// Despite the name, grouping is **not** restricted to items in *different* feeds: the key
/// is computed over every item in the batch, so two items sharing a guid inside the same
/// feed (a feed that repeats an entry) form a group too — that repetition is worth
/// reporting on its own. The common case remains the same article syndicated through two
/// feeds, which is exactly the case `item.id` cannot catch: `id` is namespaced by
/// `feed_url` (ADR-0003), so the same article delivered by two feeds gets two different
/// ids and can never be grouped by `id`.
///
/// `item_ids` and `feed_urls` are index-parallel: `item_ids[i]` came from `feed_urls[i]`.
/// [`crate::core::find_duplicates`] builds both from a single vector of `(item_id,
/// feed_url)` pairs and unzips it at the end, so it never desyncs them. The fields are
/// public and the type is `Deserialize`, so that is a producer-side guarantee, not one the
/// type can enforce on a hand-built value.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DuplicateGroup {
    /// The shared key value (a guid, a url, or a content hash, per `key_kind`).
    pub key: String,
    pub key_kind: DuplicateKeyKind,
    /// Ids of the items sharing the key, in request order. The first is canonical — the
    /// copy [`crate::core::drop_duplicates`] keeps.
    ///
    /// Index-parallel with `feed_urls`: `item_ids[i]` and `feed_urls[i]` describe the same
    /// occurrence.
    pub item_ids: Vec<String>,
    /// The feed each id in `item_ids` came from, index-parallel with it.
    pub feed_urls: Vec<String>,
}

/// What filtering was applied to a fetch and what it removed. Present (non-`null`) only
/// when `since` and/or a `query` that actually constrains something was supplied, so an
/// empty (or shorter-than-expected) result is diagnosable: "the feed had nothing new" and
/// "my filter was too narrow" look identical without it. A `query` that parses to no terms
/// at all (`" "`, `"-"`) filters nothing and does not produce this marker.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AppliedFilters {
    /// The `since` cutoff as an RFC-3339 UTC instant, or `null` if `since` was not supplied.
    pub since: Option<String>,
    /// The keyword query as supplied, or `null` if `query` was not supplied.
    pub query: Option<String>,
    /// Items removed by `since` and/or `query`, combined across every feed in this batch
    /// (no per-filter breakdown). `0` means the filter(s) that ran removed nothing — not
    /// proof that neither ran; check `since`/`query` above for that. Items dropped by
    /// `limit` are *not* counted here — that cap is reported by `truncation.applied_limit`.
    ///
    /// **Per response, not cumulative.** Under MCP cursor pagination each page reports what
    /// the feeds *that page fetched* filtered out, so the counts are not disjoint across
    /// pages (a partially-delivered feed is re-fetched and re-counted on the next page) and
    /// must not be summed.
    pub items_filtered_out: usize,
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
    /// Number of items dropped entirely to fit a response budget. Non-zero when the server
    /// filled the page to the budget and shed the rest (see `next_cursor`); `0` in the
    /// cap-and-error path, which rejects an oversized response rather than trimming it.
    /// Describes only THIS page, not a running total — a client that sums it across every
    /// page it fetches gets the wrong answer.
    pub items_omitted: usize,
    /// Number of whole feeds not included in this page — shed to fit the budget or not
    /// reached before the batch deadline. `0` when every requested feed is present.
    /// Per-page like `items_omitted`: do not sum it across pages.
    pub feeds_omitted: usize,
    /// Opaque token to pass back as `cursor` to retrieve the next page. `null` when this
    /// response is complete. Continuation pages resume cache-first: a feed already fetched
    /// costs nothing, but a feed the batch deadline never reached still needs a live fetch.
    pub next_cursor: Option<String>,
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
    /// RFC-3339 time this cache entry was last written — the original fetch, or the most
    /// recent successful revalidation preceding this one, whichever is newer. `null` when
    /// fetched fresh this call. A feed that revalidates cleanly on every call (repeated
    /// `304`s) will show this drifting forward with the polling interval, not staying
    /// pinned to when the body was first fetched from origin — it reflects "last confirmed
    /// with origin," not "age of these bytes." Pair with `cache_age_seconds` to judge
    /// staleness — `from_cache: true` alone cannot distinguish 5 minutes from 5 days.
    pub cached_at: Option<String>,
    /// Age in seconds since `cached_at` was written. `null` when not served from cache.
    /// Same caveat as `cached_at`: for a feed revalidated every N minutes, this hovers
    /// around N minutes indefinitely rather than growing toward the body's true age.
    pub cache_age_seconds: Option<u64>,
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
            cached_at: None,
            cache_age_seconds: None,
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
    /// RFC-3339 UTC publication timestamp. **May be `null` even for a normal item** — some
    /// feeds (e.g. Reddit comment `.rss`) populate only `updated`. A consumer that
    /// time-filters items should fall back to `updated` when this is `null`; rss-cli's own
    /// `--since` and newest-first ordering already key on `published` then `updated`.
    pub published: Option<String>,
    /// RFC-3339 UTC last-updated timestamp. The reliable timestamp for feeds that omit
    /// `published` (see `published`).
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
