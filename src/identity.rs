//! Deterministic, cache-independent stable item IDs — **the keystone.** Owner: `parser`.
//!
//! GUIDs are unreliable in the wild (~41% of feeds regenerate them every fetch), so the id
//! is a deterministic content hash that is identical across runs and machines *by
//! construction*, never relying on the cache or on guid stability:
//!
//! ```text
//! key = first present of: link -> guid -> (title + "|" + published)
//! id  = lowercase_hex(sha256(feed_url + "\n" + key))[..16]
//! ```
//!
//! `id_source` records which field supplied `key`. If nothing is present, fall back to a
//! hash of `feed_url` alone with `IdSource::Hash` (degenerate but stable for that feed).

use sha2::{Digest, Sha256};

use crate::model::IdSource;

/// Compute the stable id and its source for an item. **Owner: `parser` agent.**
pub fn item_id(
    feed_url: &str,
    link: Option<&str>,
    guid: Option<&str>,
    title: Option<&str>,
    published: Option<&str>,
) -> (String, IdSource) {
    // Pick the first present & non-empty field, recording where the key came from.
    let (key, source) = match non_empty(link) {
        Some(l) => (l.to_string(), IdSource::Link),
        None => match non_empty(guid) {
            Some(g) => (g.to_string(), IdSource::Guid),
            // Neither link nor guid: derive the key from the (title + "|" + published)
            // pair. When both are absent the composite collapses to the empty key, per the
            // spec's "if none present, key = \"\"" — a stable hash of the feed namespace
            // alone. A present title or published still disambiguates such items.
            None => {
                let composite = format!("{}|{}", title.unwrap_or(""), published.unwrap_or(""));
                let key = if composite == "|" {
                    String::new()
                } else {
                    composite
                };
                (key, IdSource::Hash)
            }
        },
    };

    // id = lowercase hex of sha256(feed_url + "\n" + key), truncated to 16 hex chars.
    let mut hasher = Sha256::new();
    hasher.update(feed_url.as_bytes());
    hasher.update(b"\n");
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        hex.push_str(&format!("{byte:02x}"));
    }

    (hex, source)
}

/// `Some(s)` only when `s` is present and not empty.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FEED: &str = "https://example.com/feed";

    #[test]
    fn known_answer_pins_byte_construction() {
        // Anchor test: expected hex computed independently via
        //   printf 'https://example.com/feed\nhttps://example.com/a' | shasum -a 256
        // This pins the exact construction: single '\n' separator, no trailing newline,
        // hex-string (not byte) truncation to 16 chars, lowercase.
        let (id, source) = item_id(FEED, Some("https://example.com/a"), None, None, None);
        assert_eq!(id, "1b9107de952289cb");
        assert_eq!(source, IdSource::Link);
    }

    #[test]
    fn deterministic_across_calls() {
        let a = item_id(
            FEED,
            Some("https://example.com/a"),
            Some("guid-1"),
            None,
            None,
        );
        let b = item_id(
            FEED,
            Some("https://example.com/a"),
            Some("guid-1"),
            None,
            None,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn link_preferred_over_guid() {
        let (with_link, src) = item_id(
            FEED,
            Some("https://example.com/a"),
            Some("guid"),
            None,
            None,
        );
        let (link_only, _) = item_id(FEED, Some("https://example.com/a"), None, None, None);
        // The id ignores the guid entirely when a link is present.
        assert_eq!(with_link, link_only);
        assert_eq!(src, IdSource::Link);

        // Falling back to guid yields a different id and records the source.
        let (guid_based, guid_src) = item_id(FEED, None, Some("guid"), None, None);
        assert_ne!(with_link, guid_based);
        assert_eq!(guid_src, IdSource::Guid);
    }

    #[test]
    fn hash_fallback_when_no_link_or_guid() {
        let (_, src) = item_id(
            FEED,
            None,
            None,
            Some("A Title"),
            Some("2026-01-01T00:00:00Z"),
        );
        assert_eq!(src, IdSource::Hash);

        // Truly-all-absent collapses to the empty key, per spec ("if none present, key = ''").
        // Known answer: printf 'https://example.com/feed\n' | shasum -a 256 -> a86aced5664c7742
        let (empty_a, src_a) = item_id(FEED, Some(""), Some(""), None, None);
        let (empty_b, _) = item_id(FEED, None, None, None, None);
        assert_eq!(empty_a, empty_b);
        assert_eq!(empty_a, "a86aced5664c7742");
        assert_eq!(src_a, IdSource::Hash);

        // A present published timestamp still disambiguates an otherwise title-less item.
        let (with_pub, _) = item_id(FEED, None, None, None, Some("2026-01-01T00:00:00Z"));
        assert_ne!(with_pub, empty_a);
    }

    #[test]
    fn different_feed_url_gives_different_id() {
        let (a, _) = item_id(FEED, Some("https://example.com/a"), None, None, None);
        let (b, _) = item_id(
            "https://other.com/feed",
            Some("https://example.com/a"),
            None,
            None,
            None,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn id_is_16_lowercase_hex_chars() {
        let (id, _) = item_id(FEED, Some("https://example.com/a"), None, None, None);
        assert_eq!(id.len(), 16);
        assert!(
            id.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }
}
