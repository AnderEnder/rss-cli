//! HTML → markdown/text extraction + token estimation. **Owner: `parser` agent.**
//!
//! ## Requirements
//! - [`extract`] converts feed item HTML into the requested [`ContentFormat`]:
//!   - `Markdown` / `Text`: use `html2text` (markdown-ish vs plain text rendering).
//!   - `Html`: return the HTML (optionally sanitized; sanitization is out of scope for v1
//!     unless trivial — emitting feed-rs's already-sanitized content is acceptable).
//!   - `None`: callers should not call `extract`; `content` stays `null`.
//! - [`estimate_tokens`] returns a cheap, dependency-free token estimate (a reasonable
//!   heuristic is `ceil(chars / 4)`), used so agents can budget context.

use crate::model::ContentFormat;

/// Convert `html` to the requested format. **Owner: `parser` agent.**
pub fn extract(html: &str, format: ContentFormat) -> String {
    let _ = (html, format);
    todo!("parser: implement html2text extraction")
}

/// Rough token estimate for already-extracted text. **Owner: `parser` agent.**
pub fn estimate_tokens(text: &str) -> u32 {
    let _ = text;
    todo!("parser: implement token estimate (~chars/4)")
}
