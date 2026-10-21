//! Feed autodiscovery from a website homepage. **Owner: `cli` agent.**
//!
//! ## Requirements
//! - GET the `site_url` HTML via [`HttpClient::get_bytes_until`], bounded by the caller's
//!   deadline ([`HttpClient::get_bytes`] is the unbounded wrapper).
//! - Parse the `<head>` for `<link rel="alternate" type="application/rss+xml">` and
//!   `type="application/atom+xml"` (also accept `application/json` / `feed+json`). Use the
//!   lightweight `tl` HTML parser — do not pull a heavyweight DOM stack.
//! - Resolve each `href` to an absolute URL against `site_url` (the `url` crate).
//! - Map the `type` to `feed_type` (`"rss" | "atom" | "json"`), carry the `title` attr.
//! - If the page *is itself* a feed (content sniffing optional), or if no `<link>` tags are
//!   found, return an empty list rather than erroring (the CLI decides exit behavior).

use std::collections::HashSet;

use url::Url;

use crate::error::RssError;
use crate::fetch::HttpClient;
use crate::model::{DiscoverOutput, DiscoveredFeed};

/// Discover feeds advertised on `site_url`. **Owner: `cli` agent.** Unbounded; see
/// [`discover_until`].
pub async fn discover(site_url: &str, http: &HttpClient) -> Result<DiscoverOutput, RssError> {
    discover_until(site_url, http, None).await
}

/// [`discover`] bounded by an absolute instant, mirroring `fetch`/`fetch_until`. The homepage
/// GET traverses the per-host gate, where an unbounded permit wait is what let a busy host park
/// the call past the caller's timeout. `None` is the unbounded (CLI) behaviour.
pub async fn discover_until(
    site_url: &str,
    http: &HttpClient,
    stop_at: Option<tokio::time::Instant>,
) -> Result<DiscoverOutput, RssError> {
    let (bytes, final_url) = http.get_bytes_until(site_url, stop_at).await?;
    let html = String::from_utf8_lossy(&bytes);
    // Resolve relative hrefs against the post-redirect URL when available.
    let feeds = extract_feeds(&html, &final_url);
    Ok(DiscoverOutput::new(site_url, feeds))
}

/// Extract `<link rel="alternate">` feed references from an HTML document, resolving each
/// `href` to an absolute URL against `base_url`. Factored out so it can be unit-tested on
/// inline HTML without the network. De-dup is order-preserving on the resolved URL.
fn extract_feeds(html: &str, base_url: &str) -> Vec<DiscoveredFeed> {
    let dom = match tl::parse(html, tl::ParserOptions::default()) {
        Ok(dom) => dom,
        // A malformed homepage should not error the whole command; treat it as no feeds.
        Err(_) => return Vec::new(),
    };
    let document_base = Url::parse(base_url).ok();
    let base = dom
        .nodes()
        .iter()
        .filter_map(|n| n.as_tag())
        .filter(|t| t.name().as_bytes().eq_ignore_ascii_case(b"base"))
        .find_map(|t| attr(t.attributes(), "href"))
        .and_then(|href| document_base.as_ref()?.join(href.trim()).ok())
        .or(document_base);

    let mut feeds: Vec<DiscoveredFeed> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();

    for node in dom.nodes() {
        let Some(tag) = node.as_tag() else { continue };
        if !tag.name().as_bytes().eq_ignore_ascii_case(b"link") {
            continue;
        }
        let attrs = tag.attributes();
        let rel = attr(attrs, "rel");
        // Only consider `<link rel="alternate">` (rel may carry multiple space-separated tokens).
        if !rel.as_deref().is_some_and(|r| {
            r.split_ascii_whitespace()
                .any(|token| token.eq_ignore_ascii_case("alternate"))
        }) {
            continue;
        }

        let href = match attr(attrs, "href") {
            Some(h) if !h.trim().is_empty() => h,
            _ => continue,
        };
        let ty = attr(attrs, "type");
        let feed_type = match classify(ty.as_deref(), &href) {
            Some(t) => t,
            None => continue,
        };

        // Resolve to an absolute URL; skip entries we cannot resolve deterministically.
        let resolved = match &base {
            Some(b) => match b.join(href.trim()) {
                Ok(u) => u.to_string(),
                Err(_) => continue,
            },
            None => href.trim().to_string(),
        };

        if !seen.insert(resolved.clone()) {
            continue;
        }
        feeds.push(DiscoveredFeed {
            url: resolved,
            feed_type: feed_type.to_string(),
            title: attr(attrs, "title"),
        });
    }

    feeds
}

/// Decode HTML entities once before interpreting attribute values.
fn attr(attrs: &tl::Attributes<'_>, key: &str) -> Option<String> {
    attrs
        .get(key)
        .flatten()
        .map(|b| html_escape::decode_html_entities(&b.as_utf8_str()).into_owned())
}

/// Map a `<link>`'s `type` (and `href` as a fallback) to a feed type, or `None` to reject.
fn classify(ty: Option<&str>, href: &str) -> Option<&'static str> {
    if let Some(ty) = ty {
        // Strip any `; charset=…` parameter and normalize case.
        let mime = ty
            .split(';')
            .next()
            .unwrap_or(ty)
            .trim()
            .to_ascii_lowercase();
        return match mime.as_str() {
            "application/rss+xml" => Some("rss"),
            "application/atom+xml" => Some("atom"),
            "application/json" | "application/feed+json" => Some("json"),
            _ => None,
        };
    }
    // No `type`: fall back to a coarse extension heuristic on the href.
    let lower = href
        .split(['?', '#'])
        .next()
        .unwrap_or(href)
        .to_ascii_lowercase();
    if lower.ends_with(".rss") || lower.ends_with(".atom") || lower.ends_with(".xml") {
        Some("unknown")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_attribute_entities_before_resolving_feed_urls() {
        let html = r#"<link rel="altern&#97;te" type="application/rss&#43;xml"
            href="/feed?a=1&amp;b=2&#38;c=3&#x26;d=%26amp%3B" title="News &amp; &#x1F4F0;">"#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert_eq!(feeds.len(), 1);
        assert_eq!(
            feeds[0].url,
            "https://example.com/feed?a=1&b=2&c=3&d=%26amp%3B"
        );
        assert_eq!(feeds[0].title.as_deref(), Some("News & 📰"));
    }

    #[test]
    fn attribute_entities_are_decoded_only_once() {
        let html = r#"<link rel="alternate" type="application/rss+xml"
            href="/feed?q=&amp;amp;" title="&amp;lt;news&amp;gt;">"#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert_eq!(feeds[0].url, "https://example.com/feed?q=&amp;");
        assert_eq!(feeds[0].title.as_deref(), Some("&lt;news&gt;"));
    }

    #[test]
    fn extracts_rss_and_atom_links() {
        let html = r#"
            <html><head>
              <link rel="alternate" type="application/rss+xml" title="RSS Feed" href="/feed.rss">
              <link rel="alternate" type="application/atom+xml" title="Atom Feed" href="https://other.example/atom.xml">
            </head><body></body></html>
        "#;
        let feeds = extract_feeds(html, "https://example.com/blog/");

        assert_eq!(feeds.len(), 2);

        assert_eq!(feeds[0].feed_type, "rss");
        assert_eq!(feeds[0].title.as_deref(), Some("RSS Feed"));
        // Relative href resolved against the base URL.
        assert_eq!(feeds[0].url, "https://example.com/feed.rss");

        assert_eq!(feeds[1].feed_type, "atom");
        // Absolute href left intact.
        assert_eq!(feeds[1].url, "https://other.example/atom.xml");
    }

    #[test]
    fn accepts_json_and_strips_charset() {
        let html = r#"<link rel="alternate" type="application/feed+json; charset=utf-8" href="/feed.json">"#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].feed_type, "json");
        assert_eq!(feeds[0].url, "https://example.com/feed.json");
    }

    #[test]
    fn rejects_non_feed_links() {
        // stylesheet, html alternate, and a bare anchor must all be ignored.
        let html = r#"
            <link rel="stylesheet" href="/style.css">
            <link rel="alternate" type="text/html" href="/index.html">
            <link rel="alternate" hreflang="fr" href="/fr/">
        "#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert!(feeds.is_empty(), "expected no feeds, got {feeds:?}");
    }

    #[test]
    fn extension_heuristic_when_type_missing() {
        let html = r#"<link rel="alternate" href="/posts.atom">"#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].feed_type, "unknown");
    }

    #[test]
    fn dedup_is_order_preserving() {
        let html = r#"
            <link rel="alternate" type="application/rss+xml" href="/feed.rss">
            <link rel="alternate" type="application/rss+xml" href="/feed.rss">
            <link rel="alternate" type="application/atom+xml" href="/feed.atom">
        "#;
        let feeds = extract_feeds(html, "https://example.com/");
        assert_eq!(feeds.len(), 2);
        assert_eq!(feeds[0].url, "https://example.com/feed.rss");
        assert_eq!(feeds[1].url, "https://example.com/feed.atom");
    }
}

#[cfg(test)]
mod audit_tests {
    use super::*;
    #[test]
    fn discovery_honors_first_base_and_exact_rel_tokens() {
        let html = r#"<base href="../blog/"><base href="https://wrong.example/"><link rel="notalternate" type="application/rss+xml" href="wrong.xml"><link rel="ALTERNATE stylesheet" type="application/rss+xml" href="feed.xml">"#;
        let feeds = extract_feeds(html, "https://example.com/path/page");
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].url, "https://example.com/blog/feed.xml");
        let feeds = extract_feeds(
            r#"<base href="https://cdn.example/blog/"><link rel="alternate" type="application/rss+xml" href="feed.xml">"#,
            "https://example.com/",
        );
        assert_eq!(feeds[0].url, "https://cdn.example/blog/feed.xml");
    }
}
