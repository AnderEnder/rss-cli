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
//! - Apply `params.since` (drop older items), `params.query` (keyword filter, see
//!   [`crate::query`]), and `params.limit` (keep newest N) — in that order, so `limit` means
//!   "N matching items" — then sort by `published` (fallback `updated`) descending.
//! - Populate `title`, `site_url` (the feed's `<link>`/`homepage`), and `updated`.

mod rss_enclosures;

use chrono::{DateTime, SecondsFormat, Utc};
use feed_rs::model::{Entry, Link};
use url::Url;

use crate::config::FetchParams;
use crate::content;
use crate::error::RssError;
use crate::identity;
use crate::model::{ContentFormat, Enclosure, Item, Warning};

/// Parsed feed metadata plus its items, ready to drop into a [`crate::model::FeedResult`].
#[derive(Debug, Clone)]
pub struct ParsedFeed {
    pub title: Option<String>,
    pub site_url: Option<String>,
    /// Feed-level updated timestamp, RFC-3339 UTC.
    pub updated: Option<String>,
    pub items: Vec<Item>,
    /// Non-fatal data-quality warnings about this feed (already carry `feed_url`).
    pub warnings: Vec<Warning>,
    /// How many items `params.since` and `params.query` removed, combined. `0` when neither
    /// filter changed the result (including when neither was supplied).
    pub items_filtered_out: usize,
}

/// Parse raw feed bytes using `feed_url` for both identity and relative-link resolution.
pub fn parse_feed(
    body: &[u8],
    feed_url: &str,
    params: &FetchParams,
) -> Result<ParsedFeed, RssError> {
    parse_feed_with_base(body, feed_url, feed_url, params)
}

/// Keep identity scoped to the requested feed URL while resolving links after redirects.
pub(crate) fn parse_feed_with_base(
    body: &[u8],
    feed_url: &str,
    resolution_base: &str,
    params: &FetchParams,
) -> Result<ParsedFeed, RssError> {
    let feed = feed_rs::parser::parse(body).map_err(|e| RssError::Parse(e.to_string()))?;

    let title = feed.title.map(|t| t.content);
    let updated = feed.updated.map(rfc3339);
    let site_url = pick_link_href(&feed.links).map(|s| s.to_string());

    // Base URL for resolving relative item links. If the base is unparseable we
    // simply keep raw hrefs rather than failing the whole parse.
    let base = Url::parse(resolution_base).ok();

    // Build (item, sort-key, content_fell_back) triples so we can filter/sort by the
    // underlying instant before discarding it. The sort key is `published` falling back to
    // `updated`; `fell_back` records a lower-fidelity content extraction for warnings.
    let mut rows: Vec<(Item, Option<DateTime<Utc>>, bool)> = Vec::with_capacity(feed.entries.len());
    // feed-rs merges RSS media into MediaRSS, discarding distinct enclosure content.
    let recovered = if feed.feed_type == feed_rs::model::FeedType::RSS2 {
        rss_enclosures::read(body).filter(|items| items.len() == feed.entries.len())
    } else {
        None
    };
    for (index, entry) in feed.entries.into_iter().enumerate() {
        let sort_key = entry.published.or(entry.updated);
        let (mut item, fell_back) = entry_to_item(entry, feed_url, base.as_ref(), params);
        if let Some(enclosures) = recovered.as_ref().and_then(|items| items.get(index)) {
            for enclosure in enclosures {
                if let Some(existing) = item.enclosures.iter_mut().find(|e| e.url == enclosure.url)
                {
                    existing.mime = existing.mime.take().or_else(|| enclosure.mime.clone());
                    existing.length = existing.length.or(enclosure.length);
                } else {
                    item.enclosures.push(enclosure.clone());
                }
            }
        }
        rows.push((item, sort_key, fell_back));
    }

    // `--since`: drop items whose known instant is older than the cutoff. Items with no
    // date are retained (we cannot prove they are older). Counted into
    // `items_filtered_out` alongside the keyword filter below — see that field's docs for
    // why the two are not split out.
    let before_since = rows.len();
    if let Some(since) = params.since {
        rows.retain(|(_, key, _)| match key {
            Some(dt) => *dt >= since,
            None => true,
        });
    }
    let mut items_filtered_out = before_since - rows.len();

    // `--query`: keyword filter over title/summary/content (see `crate::query`). Applied
    // before the sort and before `--limit`, so `limit` means "N matching items" rather than
    // "N items, some of which match".
    if let Some(raw) = params.query.as_deref() {
        let q = crate::query::Query::parse(raw);
        if !q.is_empty() {
            let before = rows.len();
            rows.retain(|(item, _, _)| q.matches_item(item));
            items_filtered_out += before - rows.len();
        }
    }

    // Sort newest-first. Reversing the key sorts dated items descending and places undated
    // items (`None`, the largest under `Reverse`) last. `sort_by_key` is stable, so the
    // original feed order is preserved within ties.
    rows.sort_by_key(|row| std::cmp::Reverse(row.1));

    // `--limit`: keep the newest N after sorting.
    if let Some(limit) = params.limit {
        rows.truncate(limit);
    }

    // Derive non-fatal warnings from the *surviving* rows (so a dropped item never warns).
    let warnings = collect_warnings(feed_url, &rows);

    let items = rows.into_iter().map(|(item, _, _)| item).collect();

    Ok(ParsedFeed {
        title,
        site_url,
        updated,
        items,
        warnings,
        items_filtered_out,
    })
}

/// Build the non-fatal [`Warning`]s for a feed from its returned rows. Kept deliberately
/// rare so the channel stays meaningful: a single aggregated warning when content
/// extraction degraded, and an ordering caveat only when *every* returned item is undated.
fn collect_warnings(feed_url: &str, rows: &[(Item, Option<DateTime<Utc>>, bool)]) -> Vec<Warning> {
    let mut warnings = Vec::new();

    let fell_back = rows.iter().filter(|(_, _, fb)| *fb).count();
    if fell_back > 0 {
        warnings.push(Warning {
            feed_url: Some(feed_url.to_string()),
            code: "CONTENT_EXTRACTION_FALLBACK".to_string(),
            message: format!(
                "HTML conversion failed for {fell_back} item(s); fell back to a plain tag \
                 strip (content may be lower fidelity)"
            ),
        });
    }

    if !rows.is_empty() && rows.iter().all(|(_, key, _)| key.is_none()) {
        warnings.push(Warning {
            feed_url: Some(feed_url.to_string()),
            code: "UNDATED_ITEMS".to_string(),
            message: format!(
                "all {} returned item(s) lack a published/updated date; newest-first ordering \
                 falls back to the feed's original order",
                rows.len()
            ),
        });
    }

    warnings
}

/// Convert a single `feed-rs` [`Entry`] into our serialized [`Item`], returning
/// `(item, content_fell_back)` — the flag is `true` when content extraction degraded to a
/// tag strip (see [`content::extract`]) so the caller can warn.
fn entry_to_item(
    entry: Entry,
    feed_url: &str,
    base: Option<&Url>,
    params: &FetchParams,
) -> (Item, bool) {
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
    let authors = entry
        .authors
        .into_iter()
        .filter_map(|p| p.name.or(p.email))
        .collect();
    let categories = entry.categories.into_iter().map(|c| c.term).collect();
    let summary = entry.summary.map(|t| t.content);

    // Content body: prefer the entry's <content>, fall back to the summary HTML when the
    // content element is absent/empty. Skipped entirely when extraction is disabled.
    let format = params.content_format;
    let mut content_truncated = false;
    let mut content_fell_back = false;
    // SHA-256 of the *pre-truncation* extracted body, so the hash reflects the real content
    // regardless of `max_content_chars`. `None` when there is no content.
    let mut content_hash = None;
    let content = if format == ContentFormat::None {
        None
    } else {
        let content_html = entry.content.and_then(|c| c.body);
        let source = content_html
            .as_deref()
            .filter(|h| !h.trim().is_empty())
            .or_else(|| summary.as_deref().filter(|s| !s.trim().is_empty()));
        source.map(|html| {
            let (extracted, fell_back) = content::extract(html, format);
            content_fell_back = fell_back;
            content_hash = Some(content::content_hash(&extracted));
            // Apply the per-item character cap to the *extracted* body (not source HTML).
            match params.max_content_chars {
                Some(max) => {
                    let (capped, cut) = content::truncate_to_chars(&extracted, max);
                    content_truncated = cut;
                    capped
                }
                None => extracted,
            }
        })
    };
    // Estimate tokens from the final (possibly truncated) content, so agents budget on what
    // they actually receive.
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

    let item = Item {
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
        content_truncated,
        content_hash,
        categories,
        enclosures,
        guid,
    };
    (item, content_fell_back)
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
/// otherwise the first non-comment link. Comment targets are never permalinks.
fn pick_link_href(links: &[Link]) -> Option<&str> {
    let mut eligible = links.iter().filter(|link| link.target.is_none());
    eligible
        .clone()
        .find(|link| link.rel.as_deref() == Some("alternate"))
        .or_else(|| eligible.next())
        .map(|link| link.href.as_str())
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
    fn authors_use_names_and_preserve_email_only_contacts() {
        for (author, expected) in [
            ("alice@example.com", "alice@example.com"),
            ("Alice &lt;alice@example.com&gt;", "Alice"),
            ("Alice", "Alice"),
        ] {
            let body = format!(
                "<rss version=\"2.0\"><channel><title>Authors</title><item>\
                 <title>Post</title><author>{author}</author></item></channel></rss>"
            );
            let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
            assert_eq!(parsed.items[0].authors, [expected], "author: {author}");
        }
    }

    #[test]
    fn comments_never_replace_the_article_permalink() {
        let article = "<link>https://example.com/post</link>";
        let comments = "<comments>https://example.com/comments</comments>";
        let comment_feed = "<wfw:commentRss>https://example.com/comments.xml</wfw:commentRss>";
        let expected_id = identity::item_id(
            FEED_URL,
            Some("https://example.com/post"),
            Some("post"),
            Some("Post"),
            None,
        )
        .0;
        for links in [
            format!("{comments}{article}"),
            format!("{article}{comments}"),
            format!("{comment_feed}{article}{comments}"),
        ] {
            let body = format!(
                "<rss version=\"2.0\" xmlns:wfw=\"http://wellformedweb.org/CommentAPI/\">\
                 <channel><title>Comments</title><item><title>Post</title>\
                 <guid>post</guid>{links}</item></channel></rss>"
            );
            let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
            assert_eq!(
                parsed.items[0].url.as_deref(),
                Some("https://example.com/post")
            );
            assert_eq!(parsed.items[0].id, expected_id);
        }
    }

    #[test]
    fn comments_without_an_article_link_are_not_a_permalink() {
        let body = b"<rss version=\"2.0\"><channel><title>Comments</title><item>\
                     <title>Post</title><comments>https://example.com/comments</comments>\
                     </item></channel></rss>";
        let parsed = parse_feed(body, FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items[0].url, None);
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
    fn mixed_rss_and_media_rss_preserves_distinct_attachments() {
        let body = include_str!("../tests/fixtures/mixed_attachments.rss");
        let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
        let enclosures = &parsed.items[0].enclosures;
        assert_eq!(
            enclosures.len(),
            2,
            "both distinct attachments must survive"
        );
        let audio = enclosures
            .iter()
            .find(|e| e.url.ends_with("audio.mp3"))
            .unwrap();
        assert_eq!(audio.mime.as_deref(), Some("audio/mpeg"));
        assert_eq!(audio.length, Some(1234));
        let media = enclosures
            .iter()
            .find(|e| e.url.ends_with("video.mp4"))
            .unwrap();
        assert_eq!(media.mime.as_deref(), Some("video/mp4"));
        assert_eq!(media.length, Some(5000));
    }

    #[test]
    fn matching_rss_and_media_rss_url_fills_metadata_without_duplicates() {
        let body = MEDIA_RSS.replace(
            "<media:content url=\"https://cdn.example.com/ep1.mp3\" type=\"audio/mpeg\" fileSize=\"5000\"/>",
            "<enclosure url=\"https://cdn.example.com/ep1.mp3\" type=\"audio/mpeg\" length=\"1234\"/><media:content url=\"https://cdn.example.com/ep1.mp3\"/>",
        );
        let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items[0].enclosures.len(), 1);
        assert_eq!(
            parsed.items[0].enclosures[0].mime.as_deref(),
            Some("audio/mpeg")
        );
        assert_eq!(parsed.items[0].enclosures[0].length, Some(1234));
    }

    #[test]
    fn mixed_attachments_survive_unescaped_ampersands_in_attributes() {
        let body = include_str!("../tests/fixtures/mixed_attachments.rss");
        for malformed in [
            body.replace("</item>", "<description><a href=\"https://example.com/?a=1&b=2\">link</a></description></item>"),
            body.replace("audio.mp3", "audio.mp3?a=1&b=2"),
        ] {
            let parsed = parse_feed(malformed.as_bytes(), FEED_URL, &params()).unwrap();
            assert_eq!(parsed.items[0].enclosures.len(), 2);
            assert!(parsed.items[0].enclosures.iter().any(|e| e.mime.as_deref() == Some("audio/mpeg")));
        }
    }

    #[test]
    fn mixed_rss_enclosure_keeps_upstream_mime_validation() {
        let body = include_str!("../tests/fixtures/mixed_attachments.rss")
            .replace("audio/mpeg", "invalid mime");
        let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
        let audio = parsed.items[0]
            .enclosures
            .iter()
            .find(|e| e.url.ends_with("audio.mp3"))
            .unwrap();
        assert_eq!(audio.mime, None);
        assert_eq!(audio.length, Some(1234));
    }

    #[test]
    fn single_rss_enclosure_keeps_upstream_mime_validation() {
        let body = RSS.replace("audio/mpeg", "invalid mime");
        let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
        let audio = parsed
            .items
            .iter()
            .find_map(|item| item.enclosures.first())
            .unwrap();
        assert_eq!(audio.mime, None);
        assert_eq!(audio.length, Some(1234));
    }

    #[test]
    fn recovered_enclosures_stay_with_their_direct_rss_item() {
        let body = r#"<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:other="urn:other"
            xml:base="https://cdn.example.com/">
          <channel><title>Attachments</title>
            <item/>
            <item xml:base="episodes/">
              <title>Mixed</title>
              <media:content url="video.mp4" type="video/mp4"/>
              <enclosure xml:base="../audio/" url="episode.mp3?a=1&amp;b=2" type="audio/mpeg"/>
              <other:enclosure url="https://invalid.example.com/foreign"/>
              <other:item><enclosure url="https://invalid.example.com/nested"/></other:item>
              <description><![CDATA[<enclosure url="https://invalid.example.com/content"/>]]></description>
            </item>
            <item><title>Last</title><enclosure url="last.mp3" length="77"/></item>
          </channel></rss>"#;
        let parsed = parse_feed(body.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items.len(), 3);
        assert!(parsed.items[0].enclosures.is_empty());
        assert_eq!(parsed.items[1].enclosures.len(), 2);
        let audio = parsed.items[1]
            .enclosures
            .iter()
            .find(|e| e.mime.as_deref() == Some("audio/mpeg"))
            .unwrap();
        assert_eq!(
            audio.url,
            "https://cdn.example.com/audio/episode.mp3?a=1&b=2"
        );
        assert_eq!(audio.length, None);
        assert_eq!(parsed.items[2].enclosures.len(), 1);
        assert_eq!(
            parsed.items[2].enclosures[0].url,
            "https://cdn.example.com/last.mp3"
        );
        assert_eq!(parsed.items[2].enclosures[0].length, Some(77));
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
    fn since_drops_are_counted_in_items_filtered_out() {
        // `since` alone (no `query`) must still populate `items_filtered_out` — the field
        // describes everything `since`/`query` removed, not just the query's share. A caller
        // relying on this count to explain an empty/short result must not see `0` while
        // `since` quietly dropped items.
        let mut p = params();
        p.since = Some(Utc.with_ymd_and_hms(2026, 1, 6, 0, 0, 0).unwrap());
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &p).unwrap();
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(
            parsed.items_filtered_out, 1,
            "the item since dropped must be counted, not silently left at 0"
        );
    }

    #[test]
    fn query_filters_before_limit() {
        // Three items; only two mention "rust". With limit=2 the result must be the two
        // matching items, not "the newest two items, some of which match".
        let body = r#"<rss version="2.0"><channel><title>f</title>
            <item><title>Rust news</title><link>https://e.com/1</link>
                  <pubDate>Wed, 03 Jun 2026 00:00:00 GMT</pubDate></item>
            <item><title>Python news</title><link>https://e.com/2</link>
                  <pubDate>Tue, 02 Jun 2026 00:00:00 GMT</pubDate></item>
            <item><title>More rust</title><link>https://e.com/3</link>
                  <pubDate>Mon, 01 Jun 2026 00:00:00 GMT</pubDate></item>
            </channel></rss>"#;

        let p = FetchParams {
            query: Some("rust".to_string()),
            limit: Some(2),
            ..Default::default()
        };
        let parsed = parse_feed(body.as_bytes(), "https://e.com/f", &p).expect("parse");

        assert_eq!(parsed.items.len(), 2);
        assert!(
            parsed.items.iter().all(|i| i
                .title
                .as_deref()
                .unwrap()
                .to_lowercase()
                .contains("rust"))
        );
        assert_eq!(
            parsed.items_filtered_out, 1,
            "the non-matching item must be counted"
        );
    }

    #[test]
    fn query_negation_excludes_items() {
        let body = r#"<rss version="2.0"><channel><title>f</title>
            <item><title>Rust and python</title><link>https://e.com/1</link></item>
            <item><title>Rust alone</title><link>https://e.com/2</link></item>
            </channel></rss>"#;
        let p = FetchParams {
            query: Some("rust -python".to_string()),
            ..Default::default()
        };
        let parsed = parse_feed(body.as_bytes(), "https://e.com/f", &p).expect("parse");
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].title.as_deref(), Some("Rust alone"));
    }

    #[test]
    fn since_and_query_filtering_combine_in_items_filtered_out() {
        // Both filters active: the count must reflect the total removed by *either*, since
        // `items_filtered_out` has no per-filter breakdown.
        let body = r#"<rss version="2.0"><channel><title>f</title>
            <item><title>Rust news</title><link>https://e.com/1</link>
                  <pubDate>Wed, 03 Jun 2026 00:00:00 GMT</pubDate></item>
            <item><title>Python news</title><link>https://e.com/2</link>
                  <pubDate>Tue, 02 Jun 2026 00:00:00 GMT</pubDate></item>
            <item><title>Old rust</title><link>https://e.com/3</link>
                  <pubDate>Mon, 01 Jan 2020 00:00:00 GMT</pubDate></item>
            </channel></rss>"#;
        let p = FetchParams {
            since: Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
            query: Some("rust".to_string()),
            ..Default::default()
        };
        let parsed = parse_feed(body.as_bytes(), "https://e.com/f", &p).expect("parse");
        // `since` drops "Old rust" (2020), `query` drops "Python news" — one survivor.
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.items[0].title.as_deref(), Some("Rust news"));
        assert_eq!(parsed.items_filtered_out, 2, "since's one plus query's one");
    }

    #[test]
    fn no_query_leaves_items_filtered_out_at_zero() {
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items_filtered_out, 0);
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
    fn max_content_chars_truncates_and_flags() {
        let mut p = params();
        p.max_content_chars = Some(4);
        let parsed = parse_feed(ATOM.as_bytes(), FEED_URL, &p).unwrap();
        let item = &parsed.items[0];
        assert!(
            item.content_truncated,
            "content should be flagged truncated"
        );
        let content = item.content.as_deref().unwrap();
        assert!(content.ends_with(content::TRUNCATION_MARKER));
        // Token estimate reflects the truncated body, not the original.
        assert_eq!(item.content_tokens_est, content::estimate_tokens(content));
    }

    #[test]
    fn no_cap_leaves_content_untruncated() {
        let parsed = parse_feed(ATOM.as_bytes(), FEED_URL, &params()).unwrap();
        assert!(!parsed.items[0].content_truncated);
    }

    #[test]
    fn content_none_leaves_content_null() {
        let mut p = params();
        p.content_format = ContentFormat::None;
        let parsed = parse_feed(ATOM.as_bytes(), FEED_URL, &p).unwrap();
        assert!(parsed.items[0].content.is_none());
        assert_eq!(parsed.items[0].content_tokens_est, 0);
        // No content → no hash.
        assert!(parsed.items[0].content_hash.is_none());
    }

    #[test]
    fn content_hash_present_stable_and_truncation_independent() {
        let full = parse_feed(ATOM.as_bytes(), FEED_URL, &params()).unwrap();
        let hash = full.items[0]
            .content_hash
            .clone()
            .expect("content present → hash present");
        assert_eq!(hash.len(), 16);

        // Re-parsing yields the same hash (deterministic).
        let again = parse_feed(ATOM.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(again.items[0].content_hash.as_deref(), Some(hash.as_str()));

        // The hash is over the *pre-truncation* body, so a cap does not change it.
        let mut p = params();
        p.max_content_chars = Some(4);
        let capped = parse_feed(ATOM.as_bytes(), FEED_URL, &p).unwrap();
        assert!(capped.items[0].content_truncated);
        assert_eq!(
            capped.items[0].content_hash.as_deref(),
            Some(hash.as_str()),
            "truncating the served body must not change the content hash"
        );
    }

    const UNDATED_RSS: &str = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Undated Feed</title>
    <link>https://example.com/</link>
    <item><title>A</title><link>https://example.com/a</link></item>
    <item><title>B</title><link>https://example.com/b</link></item>
  </channel>
</rss>"#;

    #[test]
    fn all_undated_items_emit_single_warning() {
        let parsed = parse_feed(UNDATED_RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items.len(), 2);
        let undated: Vec<_> = parsed
            .warnings
            .iter()
            .filter(|w| w.code == "UNDATED_ITEMS")
            .collect();
        assert_eq!(undated.len(), 1, "exactly one aggregated undated warning");
        assert_eq!(undated[0].feed_url.as_deref(), Some(FEED_URL));
    }

    #[test]
    fn since_retains_undated_items_so_a_comment_feed_is_not_emptied() {
        // Load-bearing for comment feeds. Reddit's `.../comments/.rss` carries no
        // `published` on any entry, so if the `since` filter dropped undated items, every
        // `since`-scoped fetch of a comment feed would return ZERO items — silently, since
        // an empty feed is not an error. The filter keeps them because it cannot prove they
        // are older than the cutoff.
        //
        // Don't "tighten" the `None => true` arm in the `since` retain: that is the whole
        // reason `since` is safe to pass to an undated feed.
        let mut p = params();
        p.since = Some(Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap());

        let parsed = parse_feed(UNDATED_RSS.as_bytes(), FEED_URL, &p).unwrap();
        assert_eq!(
            parsed.items.len(),
            2,
            "an undated item survives even a cutoff far in the future"
        );
        assert_eq!(
            parsed.items_filtered_out, 0,
            "nothing was filtered, so nothing should be counted as filtered"
        );
    }

    #[test]
    fn one_dated_item_suppresses_the_undated_warning_for_every_item_beside_it() {
        // `collect_warnings` needs *all* survivors undated, so `UNDATED_ITEMS` is not a
        // per-item flag and must not be read as a freshness signal (ADR-0022).
        let mixed = r#"<rss version="2.0"><channel><title>t</title>
            <item><title>Dated</title><guid>d</guid>
                  <pubDate>Mon, 01 Jun 2026 00:00:00 GMT</pubDate></item>
            <item><title>Undated</title><guid>u</guid></item>
            </channel></rss>"#;

        let parsed = parse_feed(mixed.as_bytes(), FEED_URL, &params()).unwrap();
        assert_eq!(parsed.items.len(), 2);
        assert!(
            parsed.warnings.iter().all(|w| w.code != "UNDATED_ITEMS"),
            "one dated survivor suppresses the flag for the undated item beside it"
        );
    }

    #[test]
    fn dated_feed_has_no_undated_warning() {
        // RSS has pubDates, so ordering is reliable and no warning should fire.
        let parsed = parse_feed(RSS.as_bytes(), FEED_URL, &params()).unwrap();
        assert!(parsed.warnings.iter().all(|w| w.code != "UNDATED_ITEMS"));
    }

    #[test]
    fn garbage_input_is_parse_error_not_panic() {
        let err = parse_feed(b"not a feed at all", FEED_URL, &params()).unwrap_err();
        assert!(matches!(err, RssError::Parse(_)));
    }
}
