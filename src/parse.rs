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

use chrono::{DateTime, SecondsFormat, Utc};
use feed_rs::model::{Entry, Link};
use url::Url;

use crate::config::FetchParams;
use crate::content;
use crate::error::RssError;
use crate::identity;
use crate::model::{ContentFormat, Enclosure, Item};

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
pub fn parse_feed(
    body: &[u8],
    feed_url: &str,
    params: &FetchParams,
) -> Result<ParsedFeed, RssError> {
    let feed = feed_rs::parser::parse(body).map_err(|e| RssError::Parse(e.to_string()))?;

    let title = feed.title.map(|t| t.content);
    let updated = feed.updated.map(rfc3339);
    let site_url = pick_link_href(&feed.links).map(|s| s.to_string());

    // Base URL for resolving relative item links. If `feed_url` itself is unparseable we
    // simply keep raw hrefs rather than failing the whole parse.
    let base = Url::parse(feed_url).ok();

    // Build (item, sort-key) pairs so we can filter/sort by the underlying instant before
    // discarding it. The sort key is `published` falling back to `updated`.
    let mut rows: Vec<(Item, Option<DateTime<Utc>>)> = Vec::with_capacity(feed.entries.len());
    for entry in feed.entries {
        let sort_key = entry.published.or(entry.updated);
        let item = entry_to_item(entry, feed_url, base.as_ref(), params);
        rows.push((item, sort_key));
    }

    // `--since`: drop items whose known instant is older than the cutoff. Items with no
    // date are retained (we cannot prove they are older).
    if let Some(since) = params.since {
        rows.retain(|(_, key)| match key {
            Some(dt) => *dt >= since,
            None => true,
        });
    }

    // Sort newest-first. Reversing the key sorts dated items descending and places undated
    // items (`None`, the largest under `Reverse`) last. `sort_by_key` is stable, so the
    // original feed order is preserved within ties.
    rows.sort_by_key(|row| std::cmp::Reverse(row.1));

    // `--limit`: keep the newest N after sorting.
    if let Some(limit) = params.limit {
        rows.truncate(limit);
    }

    let items = rows.into_iter().map(|(item, _)| item).collect();

    Ok(ParsedFeed {
        title,
        site_url,
        updated,
        items,
    })
}

/// Convert a single `feed-rs` [`Entry`] into our serialized [`Item`].
fn entry_to_item(entry: Entry, feed_url: &str, base: Option<&Url>, params: &FetchParams) -> Item {
    // Collect attachments while `entry` is still fully owned (borrows `media`/`links`).
    let enclosures = collect_enclosures(&entry);

    // Resolve the item permalink to an absolute URL against the feed URL; keep the raw
    // href if resolution is impossible.
    let raw_link = pick_link_href(&entry.links);
    let url = raw_link.map(|href| resolve(base, href));

    // The raw feed-provided guid/id (kept for reference; not necessarily stable).
    let guid = if entry.id.is_empty() {
        None
    } else {
        Some(entry.id.clone())
    };

    let title = entry.title.map(|t| t.content);
    let published = entry.published.map(rfc3339);
    let updated = entry.updated.map(rfc3339);
    let authors = entry.authors.into_iter().map(|p| p.name).collect();
    let categories = entry.categories.into_iter().map(|c| c.term).collect();
    let summary = entry.summary.map(|t| t.content);

    // Content body: prefer the entry's <content>, fall back to the summary HTML when the
    // content element is absent/empty. Skipped entirely when extraction is disabled.
    let format = params.content_format;
    let content = if format == ContentFormat::None {
        None
    } else {
        let content_html = entry.content.and_then(|c| c.body);
        let source = content_html
            .as_deref()
            .filter(|h| !h.trim().is_empty())
            .or_else(|| summary.as_deref().filter(|s| !s.trim().is_empty()));
        source.map(|html| content::extract(html, format))
    };
    let content_tokens_est = content
        .as_deref()
        .map(content::estimate_tokens)
        .unwrap_or(0);

    let (id, id_source) = identity::item_id(
        feed_url,
        url.as_deref(),
        guid.as_deref(),
        title.as_deref(),
        published.as_deref(),
    );

    Item {
        id,
        id_source,
        feed_url: feed_url.to_string(),
        title,
        url,
        authors,
        published,
        updated,
        summary,
        content,
        content_format: format,
        content_tokens_est,
        categories,
        enclosures,
        guid,
    }
}

/// Gather media attachments from both `media:content` blocks and `rel="enclosure"` links.
/// Entries without a URL are skipped.
fn collect_enclosures(entry: &Entry) -> Vec<Enclosure> {
    let mut enclosures = Vec::new();

    // RSS Media spec: <media:content> / <media:group>.
    for media in &entry.media {
        for content in &media.content {
            if let Some(url) = &content.url {
                enclosures.push(Enclosure {
                    url: url.to_string(),
                    mime: content.content_type.as_ref().map(|m| m.to_string()),
                    length: content.size,
                });
            }
        }
    }

    // RSS 2.0 / Atom: <enclosure> surfaces as a link with rel="enclosure".
    for link in &entry.links {
        if link.rel.as_deref() == Some("enclosure") && !link.href.is_empty() {
            enclosures.push(Enclosure {
                url: link.href.clone(),
                mime: link.media_type.clone(),
                length: link.length,
            });
        }
    }

    enclosures
}

/// Choose the best href from a set of links: the `rel="alternate"` entry if present,
/// otherwise the first link. Returns `None` for an empty set.
fn pick_link_href(links: &[Link]) -> Option<&str> {
    links
        .iter()
        .find(|l| l.rel.as_deref() == Some("alternate"))
        .or_else(|| links.first())
        .map(|l| l.href.as_str())
}

/// Resolve `href` to an absolute URL against `base`; keep `href` verbatim on failure
/// (e.g. when there is no base or the join is invalid).
fn resolve(base: Option<&Url>, href: &str) -> String {
    base.and_then(|b| b.join(href).ok())
        .map(|u| u.to_string())
        .unwrap_or_else(|| href.to_string())
}

/// Normalize a `chrono` timestamp to an RFC-3339 UTC string with second precision and a
/// trailing `Z` (e.g. `2026-01-02T03:04:05Z`).
fn rfc3339(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::IdSource;
    use chrono::TimeZone;

    const FEED_URL: &str = "https://example.com/feed.xml";

    fn params() -> FetchParams {
        FetchParams::default()
    }

    const RSS: &str = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Example Blog</title>
    <link>https://example.com/</link>
    <item>
      <title>First Post</title>
      <link>/posts/first</link>
      <guid>tag:example.com,2026:1</guid>
      <pubDate>Mon, 05 Jan 2026 10:00:00 GMT</pubDate>
      <description><![CDATA[<p>Hello <b>world</b></p>]]></description>
      <enclosure url="https://example.com/audio.mp3" type="audio/mpeg" length="1234"/>
    </item>
    <item>
      <title>Second Post</title>
      <link>https://example.com/posts/second</link>
      <pubDate>Tue, 06 Jan 2026 10:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>"#;

    const ATOM: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Example</title>
  <updated>2026-02-01T12:00:00Z</updated>
  <link rel="alternate" href="https://atom.example.com/"/>
  <entry>
    <title>Atom Entry</title>
    <id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
    <link rel="alternate" href="/articles/atom"/>
    <updated>2026-02-01T12:00:00Z</updated>
    <published>2026-02-01T11:00:00Z</published>
    <content type="html">&lt;p&gt;Atom body&lt;/p&gt;</content>
  </entry>
</feed>"#;

    #[test]
    fn parses_rss_metadata_and_items() {
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.title.as_deref(), Some("Example Blog"));
        assert_eq!(parsed.site_url.as_deref(), Some("https://example.com/"));
        assert_eq!(parsed.items.len(), 2);
    }

    #[test]
    fn rss_relative_link_resolved_to_absolute() {
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        // Newest-first: "Second Post" (Jan 6) precedes "First Post" (Jan 5).
        let first = parsed
            .items
            .iter()
            .find(|i| i.title.as_deref() == Some("First Post"))
            .unwrap();
        assert_eq!(
            first.url.as_deref(),
            Some("https://example.com/posts/first")
        );
    }

    #[test]
    fn rss_items_sorted_newest_first() {
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items[0].title.as_deref(), Some("Second Post"));
        assert_eq!(parsed.items[1].title.as_deref(), Some("First Post"));
    }

    const MEDIA_RSS: &str = r#"<?xml version="1.0"?>
<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
  <channel>
    <title>Media Feed</title>
    <item>
      <title>Podcast Episode</title>
      <link>https://example.com/ep1</link>
      <media:content url="https://cdn.example.com/ep1.mp3" type="audio/mpeg" fileSize="5000"/>
    </item>
  </channel>
</rss>"#;

    #[test]
    fn media_content_maps_to_enclosure() {
        let parsed = parse_feed(MEDIA_RSS.as_bytes(), FEED_URL, &params()).unwrap();
        let item = &parsed.items[0];
        assert_eq!(item.enclosures.len(), 1);
        let enc = &item.enclosures[0];
        assert_eq!(enc.url, "https://cdn.example.com/ep1.mp3");
        assert_eq!(enc.mime.as_deref(), Some("audio/mpeg"));
        assert_eq!(enc.length, Some(5000));
    }

    #[test]
    fn rss_enclosure_and_stable_id_present() {
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        let first = parsed
            .items
            .iter()
            .find(|i| i.title.as_deref() == Some("First Post"))
            .unwrap();

        // Stable id present, 16 lowercase hex chars, derived from the resolved link.
        assert_eq!(first.id.len(), 16);
        assert_eq!(first.id_source, IdSource::Link);

        // Enclosure mapped from the RSS <enclosure> element.
        assert_eq!(first.enclosures.len(), 1);
        assert_eq!(first.enclosures[0].url, "https://example.com/audio.mp3");
        assert_eq!(first.enclosures[0].mime.as_deref(), Some("audio/mpeg"));
        assert_eq!(first.enclosures[0].length, Some(1234));
    }

    #[test]
    fn id_is_deterministic_across_parses() {
        let a = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        let b = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(a.items[0].id, b.items[0].id);
        assert_eq!(a.items[1].id, b.items[1].id);
    }

    #[test]
    fn parses_atom_and_resolves_relative_link() {
        let parsed = parse_feed(ATOM.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.title.as_deref(), Some("Atom Example"));
        assert_eq!(
            parsed.site_url.as_deref(),
            Some("https://atom.example.com/")
        );
        assert_eq!(parsed.updated.as_deref(), Some("2026-02-01T12:00:00Z"));
        assert_eq!(parsed.items.len(), 1);

        let entry = &parsed.items[0];
        assert_eq!(entry.title.as_deref(), Some("Atom Entry"));
        // Relative entry link resolved against the feed URL.
        assert_eq!(
            entry.url.as_deref(),
            Some("https://example.com/articles/atom")
        );
        assert_eq!(entry.published.as_deref(), Some("2026-02-01T11:00:00Z"));
        // Markdown content extracted from the inline HTML body.
        assert!(entry.content.as_deref().unwrap().contains("Atom body"));
        assert!(entry.content_tokens_est > 0);
        assert_eq!(
            entry.guid.as_deref(),
            Some("urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a")
        );
    }

    #[test]
    fn since_drops_older_items() {
        let mut p = params();
        // Cutoff between the two posts (Jan 5 vs Jan 6).
        p.since = Some(Utc.with_ymd_and_hms(2026, 1, 6, 0, 0, 0).unwrap());
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &p).unwrap();
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].title.as_deref(), Some("Second Post"));
    }

    #[test]
    fn limit_keeps_newest_n() {
        let mut p = params();
        p.limit = Some(1);
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &p).unwrap();
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].title.as_deref(), Some("Second Post"));
    }

    #[test]
    fn content_none_leaves_content_null() {
        let mut p = params();
        p.content_format = ContentFormat::None;
        let parsed = parse_feed(ATOM.as_bytes(), FEED_URL, &p).unwrap();
        assert!(parsed.items[0].content.is_none());
        assert_eq!(parsed.items[0].content_tokens_est, 0);
    }

    #[test]
    fn garbage_input_is_parse_error_not_panic() {
        let err = parse_feed(b"not a feed at all", FEED_URL, &params()).unwrap_err();
        assert!(matches!(err, RssError::Parse(_)));
    }
}
