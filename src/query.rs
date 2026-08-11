//! Local keyword filtering for feed items (ADR-0018).
//!
//! Deliberately small: exact, case-insensitive substring matching with AND semantics,
//! quoted phrases, and `-` negation. No regex, no ranking, no stemming — a caller that
//! needs those should post-process. This is mechanical and deterministic, so it does not
//! conflict with ADR-0007's rejection of built-in LLM summarization.

use crate::model::Item;

/// A parsed keyword query: every `required` term must appear and no `excluded` term may.
///
/// A few semantics are real design choices, spelled out here rather than left implicit:
///
/// - **Substring, not word-boundary.** Matching is `str::contains` on a lowercased
///   haystack, so a query for `"ai"` matches an item whose only relevant word is "said".
///   Callers that need whole-word matching must post-process; this module stays
///   mechanical on purpose (see the module docs).
/// - **Case-insensitive via `str::to_lowercase()`** — full Unicode lowercasing (`ÜBER`
///   matches `über`), but not full Unicode case *folding* (`STRASSE` will not match
///   `straße`). Good enough for keyword search, not a collation-aware matcher.
/// - **An all-exclusions query is not the same as an empty one.** `-spam` with no
///   required terms has an empty `required` list (vacuously satisfied) and a non-empty
///   `excluded` list, so [`Query::is_empty`] is `false` and every item that lacks "spam"
///   matches. Only a query with *no* terms at all — required and excluded both empty —
///   is `is_empty()`.
/// - **[`Query::matches_item`] searches `title`, `summary`, and `content` only** — not
///   `authors`, `url`, `categories`, or `guid`. It sees the item as it will be *returned*,
///   after content rendering and truncation, so the searchable surface shrinks with the
///   content settings: under `--content none` the body is gone entirely, and under
///   `--max-content-chars N` a term appearing only past character N will not match. The
///   same query can therefore match fewer items under a narrower content setting.
/// - **Each field is matched on its own; fields are never concatenated**, so a quoted
///   phrase cannot span a field boundary — a title ending in "breaking" followed by a
///   summary starting with "news" does not satisfy `"breaking news"`. There is no
///   separator character for a term to straddle, so this holds for *every* term,
///   including one that itself contains a newline. Within a field the text is matched
///   verbatim, so a phrase can't span a line break *inside* `content` either —
///   `"new async"` will not match a body containing `"the new\nasync runtime"`. A phrase
///   matches contiguously, within one field, or not at all. Required terms are still
///   ANDed *across* fields: one term in the title and another in the content is a match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Query {
    required: Vec<String>,
    excluded: Vec<String>,
}

impl Query {
    /// Parse a query string. Whitespace separates terms; `"quoted phrases"` match as a
    /// unit; a leading `-` negates. Terms are lowercased once here so matching is cheap.
    /// A `-` with nothing attached to it (end of input, or followed by whitespace) is not
    /// a term and does not negate whatever comes next.
    pub fn parse(s: &str) -> Self {
        let mut q = Query::default();
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if c.is_whitespace() {
                continue;
            }
            let negated = c == '-';
            // A `-` with nothing attached to it is not a term: end of input drops it,
            // and whitespace right after it means it wasn't fused to a word either, so
            // it must not negate the next token.
            let first = if negated {
                match chars.next() {
                    None => break,
                    Some(f) if f.is_whitespace() => continue,
                    Some(f) => f,
                }
            } else {
                c
            };

            let mut term = String::new();
            if first == '"' {
                for c in chars.by_ref() {
                    if c == '"' {
                        break;
                    }
                    term.push(c);
                }
            } else {
                term.push(first);
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() {
                        break;
                    }
                    term.push(c);
                    chars.next();
                }
            }

            let term = term.trim().to_lowercase();
            if term.is_empty() {
                continue;
            }
            if negated {
                q.excluded.push(term);
            } else {
                q.required.push(term);
            }
        }
        q
    }

    /// True when the query constrains nothing, so every item passes. An all-exclusions
    /// query (e.g. `-spam`) is *not* empty — see the type docs.
    pub fn is_empty(&self) -> bool {
        self.required.is_empty() && self.excluded.is_empty()
    }

    /// Match against an arbitrary haystack (already-joined text). Lowercases the whole
    /// haystack exactly once regardless of how many terms are checked against it, so the
    /// cost is O(haystack length), not O(haystack length * term count) — the caller
    /// should call this once per item, not once per term.
    pub fn matches_text(&self, text: &str) -> bool {
        if self.is_empty() {
            return true;
        }
        let hay = text.to_lowercase();
        self.required.iter().all(|t| hay.contains(t))
            && !self.excluded.iter().any(|t| hay.contains(t))
    }

    /// Match against an item's `title`, `summary`, and `content` (see the type docs for
    /// why those three fields and not the others). Each present field is lowercased once
    /// and searched on its own — no concatenation, so no synthetic separator exists for a
    /// term to match across. Cost is O(total item text) regardless of how many terms the
    /// query has; call this once per item, never once per term.
    pub fn matches_item(&self, item: &Item) -> bool {
        if self.is_empty() {
            return true;
        }
        let fields: Vec<String> = [
            item.title.as_deref(),
            item.summary.as_deref(),
            item.content.as_deref(),
        ]
        .into_iter()
        .flatten()
        .map(str::to_lowercase)
        .collect();
        let in_some_field = |term: &str| fields.iter().any(|f| f.contains(term));
        self.required.iter().all(|t| in_some_field(t))
            && !self.excluded.iter().any(|t| in_some_field(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentFormat, IdSource};

    fn haystack_matches(q: &str, text: &str) -> bool {
        Query::parse(q).matches_text(text)
    }

    fn make_item(title: Option<&str>, summary: Option<&str>, content: Option<&str>) -> Item {
        Item {
            id: "id".into(),
            id_source: IdSource::Hash,
            feed_url: "https://example.com/feed".into(),
            title: title.map(str::to_string),
            url: None,
            authors: Vec::new(),
            published: None,
            updated: None,
            summary: summary.map(str::to_string),
            content: content.map(str::to_string),
            content_format: ContentFormat::Markdown,
            content_tokens_est: 0,
            content_truncated: false,
            content_hash: None,
            categories: Vec::new(),
            enclosures: Vec::new(),
            guid: None,
        }
    }

    #[test]
    fn terms_are_anded_and_case_insensitive() {
        assert!(haystack_matches("rust async", "Rust and ASYNC runtimes"));
        assert!(
            !haystack_matches("rust async", "Rust runtimes"),
            "a missing term fails the AND"
        );
        assert!(haystack_matches("RUST", "rust"));
    }

    #[test]
    fn quoted_phrases_match_as_a_unit() {
        assert!(haystack_matches(
            "\"async runtime\"",
            "a new async runtime lands"
        ));
        assert!(
            !haystack_matches("\"async runtime\"", "async and runtime, separately"),
            "a phrase must match contiguously"
        );
    }

    #[test]
    fn leading_dash_negates() {
        assert!(haystack_matches("rust -python", "rust is nice"));
        assert!(!haystack_matches("rust -python", "rust and python"));
        assert!(
            haystack_matches("-python", "rust only"),
            "negation alone is a valid query"
        );
    }

    #[test]
    fn empty_query_matches_everything() {
        assert!(Query::parse("").is_empty());
        assert!(Query::parse("   ").is_empty());
        assert!(haystack_matches("", "anything at all"));
    }

    #[test]
    fn dash_followed_by_space_is_not_negation() {
        // A bare `-` with nothing attached to it is not a term, and must not fuse onto
        // whatever word comes after it as a negation either.
        assert!(
            haystack_matches("rust - python", "rust and python"),
            "the lone dash must be discarded, not treated as negating 'python'"
        );
    }

    #[test]
    fn dash_alone_and_empty_phrase_produce_no_terms() {
        assert!(
            Query::parse("-").is_empty(),
            "a bare trailing dash contributes no term"
        );
        assert!(
            Query::parse("\"\"").is_empty(),
            "an empty quoted phrase contributes no term"
        );
    }

    #[test]
    fn non_ascii_terms_are_case_folded_via_unicode_lowercasing() {
        assert!(haystack_matches("ÜBER", "the über cool feature"));
        assert!(haystack_matches("über", "ÜBER MODE"));
    }

    #[test]
    fn punctuation_is_literal_not_a_word_boundary() {
        assert!(
            !haystack_matches("rust,", "rust and go"),
            "the comma is part of the term; it is not stripped"
        );
        assert!(haystack_matches("rust,", "the rust, we love"));
    }

    #[test]
    fn mixed_required_excluded_and_phrase() {
        assert!(haystack_matches(
            "\"breaking news\" -sports",
            "breaking news: markets rally"
        ));
        assert!(!haystack_matches(
            "\"breaking news\" -sports",
            "breaking news: sports rally"
        ));
    }

    #[test]
    fn matches_item_searches_title_summary_and_content() {
        let item = make_item(
            Some("Rust 2.0"),
            Some("a big async release"),
            Some("full changelog here"),
        );
        assert!(Query::parse("rust").matches_item(&item));
        assert!(Query::parse("async").matches_item(&item));
        assert!(Query::parse("changelog").matches_item(&item));
        assert!(!Query::parse("python").matches_item(&item));
    }

    #[test]
    fn matches_item_does_not_let_a_phrase_span_a_field_boundary() {
        let item = make_item(Some("breaking"), Some("news today"), None);
        assert!(
            !Query::parse("\"breaking news\"").matches_item(&item),
            "title and summary are different fields; a phrase must not match across them"
        );
        assert!(
            Query::parse("breaking news").matches_item(&item),
            "an AND across fields is still fine"
        );
    }

    #[test]
    fn empty_and_all_exclusion_queries_agree_on_the_item_path() {
        let item = make_item(Some("Rust 2.0"), Some("async release"), None);
        assert!(
            Query::parse("").matches_item(&item),
            "is_empty() skipping and matches_item matching must agree"
        );
        assert!(Query::parse("   ").matches_item(&item));
        assert!(
            !Query::parse("-async").is_empty(),
            "an all-exclusions query is not empty"
        );
        assert!(!Query::parse("-async").matches_item(&item));
        assert!(
            Query::parse("-python").matches_item(&item),
            "an item lacking every excluded term matches"
        );
    }

    #[test]
    fn a_phrase_containing_a_newline_still_cannot_span_a_field_boundary() {
        // A quoted phrase can legitimately carry a literal newline (an MCP caller sends
        // `"foo\nbar"` in JSON). Matching each field separately is what makes the
        // no-cross-field-match guarantee hold for those terms too, rather than resting
        // on an assumption that terms never contain the join character.
        let q = Query::parse("\"foo\nbar\"");
        assert!(
            !q.matches_item(&make_item(Some("foo"), Some("bar"), None)),
            "the newline is in the query, not in either field"
        );
        assert!(
            q.matches_item(&make_item(Some("unrelated"), None, Some("foo\nbar"))),
            "but it still matches when the newline is genuinely inside one field"
        );
    }

    #[test]
    fn matches_item_searches_only_the_fields_that_are_present() {
        let item = make_item(Some("title only"), None, None);
        assert!(Query::parse("title").matches_item(&item));
        assert!(!Query::parse("missing").matches_item(&item));
    }
}
