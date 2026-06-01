//! `feed-rs` → [`crate::model`] conversion. **Owner: `parser` agent.**
//!
//! Frozen public interface — [`crate::core`] depends on these exact signatures.
//!
//! ## Requirements
//! - Parse `body` with `feed_rs::parser::parse` (auto-detects RSS/Atom/JSON Feed). Be
//!   lenient: malformed feeds should yield [`RssError::Parse`], never a panic.
//! - Normalize all dates to RFC-3339 UTC strings (`feed-rs` yields `chrono::DateTime`).
//! - Resolve relative item links to absolute URLs against `feed_url` (use the `url` crate).
//! - For each item, compute the stable id via [`crate::identity::item_id`] and the content
//!   via [`crate::content`], honoring `params.content_format`.
//! - Apply `params.since` (drop older items) and `params.limit` (keep newest N) — sort by
//!   `published` (fallback `updated`) descending before limiting.
//! - Populate `title`, `site_url` (the feed's `<link>`/`homepage`), and `updated`.

use crate::config::FetchParams;
use crate::error::RssError;
use crate::model::Item;

/// Parsed feed metadata plus its items, ready to drop into a [`crate::model::FeedResult`].
#[derive(Debug, Clone)]
pub struct ParsedFeed {
    pub title: Option<String>,
    pub site_url: Option<String>,
    /// Feed-level updated timestamp, RFC-3339 UTC.
    pub updated: Option<String>,
    pub items: Vec<Item>,
}

/// Parse raw feed bytes into a [`ParsedFeed`]. `feed_url` is the (post-redirect) URL the
/// bytes came from, used both for relative-link resolution and as the id namespace.
pub fn parse_feed(body: &[u8], feed_url: &str, params: &FetchParams) -> Result<ParsedFeed, RssError> {
    let _ = (body, feed_url, params);
    todo!("parser: implement feed-rs -> model conversion")
}
