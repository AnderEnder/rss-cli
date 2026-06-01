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

use crate::model::IdSource;

/// Compute the stable id and its source for an item. **Owner: `parser` agent.**
pub fn item_id(
    feed_url: &str,
    link: Option<&str>,
    guid: Option<&str>,
    title: Option<&str>,
    published: Option<&str>,
) -> (String, IdSource) {
    let _ = (feed_url, link, guid, title, published);
    todo!("parser: implement deterministic stable id (see module docs)")
}
