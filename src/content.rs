//! HTML → markdown/text extraction + token estimation. **Owner: `parser` agent.**
//!
//! ## Requirements
//! - [`extract`] converts feed item HTML into the requested [`ContentFormat`]:
//!   - `Markdown`: convert with `htmd` (HTML → Markdown).
//!   - `Text`: render to plain text with `html2text`.
//!   - `Html`: return the HTML as-is (feed-rs already emits sanitized content).
//!   - `None`: callers should not call `extract`; return an empty string defensively.
//! - On any converter error, fall back to a naive tag strip rather than panicking, and
//!   report the fallback so callers can surface a `CONTENT_EXTRACTION_FALLBACK` warning.
//! - [`estimate_tokens`] returns a cheap, dependency-free token estimate
//!   (`ceil(chars / 4)`), used so agents can budget context.

use sha2::{Digest, Sha256};

use crate::model::ContentFormat;

/// Wrap width passed to `html2text`'s plain-text renderer. Wide enough to avoid hard
/// wrapping prose mid-sentence while still bounding pathological tables.
const TEXT_WRAP_WIDTH: usize = 80;

/// Convert `html` to the requested format. **Owner: `parser` agent.**
///
/// Returns `(content, fell_back)`. `fell_back` is `true` when the Markdown/Text converter
/// errored and we degraded to a naive tag strip (lower fidelity) — the caller turns that
/// into a non-fatal warning. It is always `false` for `Html`/`None` (no conversion).
pub fn extract(html: &str, format: ContentFormat) -> (String, bool) {
    match format {
        ContentFormat::Markdown => match htmd::convert(html) {
            Ok(md) => (md, false),
            Err(_) => (strip_tags(html), true),
        },
        ContentFormat::Text => match html2text::from_read(html.as_bytes(), TEXT_WRAP_WIDTH) {
            Ok(text) => (text, false),
            Err(_) => (strip_tags(html), true),
        },
        ContentFormat::Html => (html.to_string(), false),
        ContentFormat::None => (String::new(), false),
    }
}

/// Stable 16-hex (64-bit) SHA-256 prefix of already-extracted content, for cheap
/// change-detection across runs. Mirrors the item-id construction (lowercase hex, first 8
/// bytes); 64 bits is ample to flag "this body changed".
pub fn content_hash(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut hex = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
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
        let (md, fell_back) = extract("<p>Hi <a href=x>link</a></p>", ContentFormat::Markdown);
        assert!(!md.is_empty());
        assert!(md.contains("Hi"), "markdown should retain text: {md:?}");
        assert!(!fell_back, "a well-formed fragment should not fall back");
    }

    #[test]
    fn text_extraction_keeps_text() {
        let (text, fell_back) = extract("<p>Hi <a href=x>link</a></p>", ContentFormat::Text);
        assert!(text.contains("Hi"), "text should retain content: {text:?}");
        assert!(!fell_back);
    }

    #[test]
    fn html_format_is_passthrough() {
        let html = "<p>raw <b>html</b></p>";
        assert_eq!(
            extract(html, ContentFormat::Html),
            (html.to_string(), false)
        );
    }

    #[test]
    fn none_format_is_empty() {
        assert_eq!(
            extract("<p>anything</p>", ContentFormat::None),
            (String::new(), false)
        );
    }

    #[test]
    fn content_hash_is_stable_16_hex_and_change_sensitive() {
        let a = content_hash("the body");
        assert_eq!(a, content_hash("the body"), "hash is deterministic");
        assert_eq!(a.len(), 16);
        assert!(
            a.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_ne!(
            a,
            content_hash("the body changed"),
            "different content → different hash"
        );
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
