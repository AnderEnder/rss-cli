//! HTML → markdown/text extraction + token estimation. **Owner: `parser` agent.**
//!
//! ## Requirements
//! - [`extract`] converts feed item HTML into the requested [`ContentFormat`]:
//!   - `Markdown`: convert with `htmd` (HTML → Markdown).
//!   - `Text`: render to plain text with `html2text`.
//!   - `Html`: return the HTML as-is (feed-rs already emits sanitized content).
//!   - `None`: callers should not call `extract`; return an empty string defensively.
//! - On any converter error, fall back to a naive tag strip rather than panicking.
//! - [`estimate_tokens`] returns a cheap, dependency-free token estimate
//!   (`ceil(chars / 4)`), used so agents can budget context.

use crate::model::ContentFormat;

/// Wrap width passed to `html2text`'s plain-text renderer. Wide enough to avoid hard
/// wrapping prose mid-sentence while still bounding pathological tables.
const TEXT_WRAP_WIDTH: usize = 80;

/// Convert `html` to the requested format. **Owner: `parser` agent.**
pub fn extract(html: &str, format: ContentFormat) -> String {
    match format {
        ContentFormat::Markdown => htmd::convert(html).unwrap_or_else(|_| strip_tags(html)),
        ContentFormat::Text => html2text::from_read(html.as_bytes(), TEXT_WRAP_WIDTH)
            .unwrap_or_else(|_| strip_tags(html)),
        ContentFormat::Html => html.to_string(),
        ContentFormat::None => String::new(),
    }
}

/// Rough token estimate for already-extracted text. **Owner: `parser` agent.**
///
/// `ceil(chars / 4)` — i.e. `(n + 3) / 4`, expressed via `div_ceil`.
pub fn estimate_tokens(text: &str) -> u32 {
    text.chars().count().div_ceil(4) as u32
}

/// Marker appended to truncated content. The boolean flag is authoritative; this is a
/// human/agent-visible hint that the body was cut.
pub const TRUNCATION_MARKER: &str = " …[truncated]";

/// Truncate `text` to at most `max_chars` *characters* (not bytes), appending
/// [`TRUNCATION_MARKER`] when anything was cut. Returns `(text, was_truncated)`.
///
/// Counts and slices by Unicode scalar values, so it never panics on a multi-byte boundary
/// the way `&text[..n]` would. Note: when applied to rendered Markdown this may cut through
/// markup (mid-link, mid-emphasis) — accepted as the simple, predictable behavior; callers
/// wanting intact markup should fetch the item without a cap.
pub fn truncate_to_chars(text: &str, max_chars: usize) -> (String, bool) {
    // Avoid counting all chars on long strings unless needed: peek at char index `max_chars`.
    if text.char_indices().nth(max_chars).is_none() {
        return (text.to_string(), false); // `text` has <= max_chars characters.
    }
    let kept: String = text.chars().take(max_chars).collect();
    (format!("{kept}{TRUNCATION_MARKER}"), true)
}

/// Last-resort tag stripper used when a converter errors out. Drops everything between
/// `<` and `>` and collapses runs of whitespace, so we always return *something* readable.
fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_extraction_keeps_text() {
        let md = extract("<p>Hi <a href=x>link</a></p>", ContentFormat::Markdown);
        assert!(!md.is_empty());
        assert!(md.contains("Hi"), "markdown should retain text: {md:?}");
    }

    #[test]
    fn text_extraction_keeps_text() {
        let text = extract("<p>Hi <a href=x>link</a></p>", ContentFormat::Text);
        assert!(text.contains("Hi"), "text should retain content: {text:?}");
    }

    #[test]
    fn html_format_is_passthrough() {
        let html = "<p>raw <b>html</b></p>";
        assert_eq!(extract(html, ContentFormat::Html), html);
    }

    #[test]
    fn none_format_is_empty() {
        assert_eq!(extract("<p>anything</p>", ContentFormat::None), "");
    }

    #[test]
    fn token_estimate_is_ceil_div_four() {
        // 8 chars -> 2 tokens; 9 chars -> ceil(9/4) = 3.
        assert_eq!(estimate_tokens("abcdefgh"), 2);
        assert_eq!(estimate_tokens("abcdefghi"), 3);
        assert_eq!(estimate_tokens(""), 0);
        // Counts Unicode scalar values, not bytes.
        assert_eq!(estimate_tokens("héllo"), 2); // 5 chars -> ceil(5/4) = 2
    }

    #[test]
    fn strip_tags_fallback_is_clean() {
        assert_eq!(strip_tags("<p>Hi  <b>there</b></p>"), "Hi there");
    }

    #[test]
    fn truncate_under_limit_is_untouched() {
        let (out, cut) = truncate_to_chars("hello", 10);
        assert_eq!(out, "hello");
        assert!(!cut);
        // Exactly at the limit is not truncated.
        let (out, cut) = truncate_to_chars("hello", 5);
        assert_eq!(out, "hello");
        assert!(!cut);
    }

    #[test]
    fn truncate_over_limit_appends_marker() {
        let (out, cut) = truncate_to_chars("hello world", 5);
        assert!(cut);
        assert!(out.starts_with("hello"));
        assert!(out.ends_with(TRUNCATION_MARKER));
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        // Multi-byte characters: slicing by byte index would panic; by char it must not.
        let s = "héllo wörld"; // 'é' and 'ö' are 2 bytes each
        let (out, cut) = truncate_to_chars(s, 4);
        assert!(cut);
        assert!(out.starts_with("héll"));
        // 4 kept chars + the marker.
        assert_eq!(out.chars().count(), 4 + TRUNCATION_MARKER.chars().count());
    }

    #[test]
    fn truncate_to_zero_is_just_marker() {
        let (out, cut) = truncate_to_chars("anything", 0);
        assert!(cut);
        assert_eq!(out, TRUNCATION_MARKER);
    }
}
