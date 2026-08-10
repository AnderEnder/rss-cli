//! Opaque continuation cursors for paginated `fetch_feed` responses (ADR-0017).
//!
//! Pagination is **stateless**: the server keeps no result store, so there is no eviction
//! policy and nothing to leak. A cursor records *where to resume* plus a fingerprint of the
//! request it was minted for; the already-cached feed bodies act as the pagination store,
//! because a continuation call fetches with [`crate::config::CachePolicy::CacheFirst`].
//!
//! Item positions are stable across pages because item ids are deterministic by
//! construction (ADR-0003) and a cache hit returns byte-identical bytes.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::RssError;

/// Wire version of the cursor payload. Bump only on an incompatible field change.
pub const CURSOR_VERSION: u8 = 1;

/// A decoded continuation position. Field names are single letters to keep the encoded
/// token short — it travels in every paginated response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cursor {
    /// Payload version; see [`CURSOR_VERSION`].
    pub v: u8,
    /// Fingerprint of the request this cursor was minted for; see [`fingerprint`].
    pub fp: String,
    /// Index into the **original** request URL list where the next page resumes.
    pub f: usize,
    /// Index into feed `f`'s post-filter item list where the next page resumes.
    pub i: usize,
    /// How many items feed `f` held when this cursor was minted, for roll detection.
    pub n: usize,
    /// The resolved `since` cutoff as epoch seconds, so every page filters against the
    /// *same* instant. Without this a relative window like `2h` would slide between pages
    /// and silently skip items.
    pub s: Option<i64>,
}

impl Cursor {
    /// Encode as an opaque URL-safe, unpadded base64 token.
    pub fn encode(&self) -> String {
        // Serializing our own plain-data struct cannot fail; fall back to an empty payload
        // rather than panicking a live server.
        let json = serde_json::to_vec(self).unwrap_or_default();
        URL_SAFE_NO_PAD.encode(json)
    }

    /// Decode a token produced by [`Cursor::encode`].
    pub fn decode(token: &str) -> Result<Self, RssError> {
        let bytes = URL_SAFE_NO_PAD
            .decode(token.trim())
            .map_err(|_| RssError::Usage("invalid cursor: not a valid token".to_string()))?;
        let cursor: Cursor = serde_json::from_slice(&bytes)
            .map_err(|_| RssError::Usage("invalid cursor: malformed payload".to_string()))?;
        if cursor.v != CURSOR_VERSION {
            return Err(RssError::Usage(format!(
                "unsupported cursor version {} (this server emits version {CURSOR_VERSION}); \
                 re-request without a cursor",
                cursor.v
            )));
        }
        Ok(cursor)
    }
}

/// Fingerprint the request shape a cursor belongs to: the first 16 hex chars of a SHA-256
/// over the URL list (in order) followed by `parts`.
///
/// `parts` carries the **raw argument strings as the client sent them** — in order:
/// `content_format`, `since`, `limit`, `max_content_chars`, `query`, `dedupe`. Raw, not
/// resolved: `since: "2h"` resolves to a different instant on every call, so fingerprinting
/// the resolved value would make every continuation mismatch. The resolved cutoff travels
/// in [`Cursor::s`] instead.
///
/// `max_response_tokens` is deliberately **excluded** — a caller may legitimately request a
/// smaller page 2.
pub fn fingerprint(urls: &[String], parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for url in urls {
        hasher.update(url.as_bytes());
        hasher.update(b"\n");
    }
    hasher.update(b"\x1e"); // record separator between the url list and the scalar parts
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\x1f"); // unit separator
    }
    // sha2 0.11's finalize() output has no LowerHex impl — hex it by hand.
    hasher
        .finalize()
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Cursor {
        Cursor {
            v: CURSOR_VERSION,
            fp: "0123456789abcdef".into(),
            f: 2,
            i: 7,
            n: 25,
            s: Some(1_780_000_000),
        }
    }

    #[test]
    fn cursor_round_trips_exactly() {
        let c = sample();
        let decoded = Cursor::decode(&c.encode()).expect("round-trip");
        assert_eq!(decoded.v, c.v);
        assert_eq!(decoded.fp, c.fp);
        assert_eq!(decoded.f, c.f);
        assert_eq!(decoded.i, c.i);
        assert_eq!(decoded.n, c.n);
        assert_eq!(decoded.s, c.s);
    }

    #[test]
    fn cursor_encoding_is_url_safe_and_unpadded() {
        let token = sample().encode();
        assert!(
            token
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "token must be URL-safe with no padding: {token}"
        );
    }

    #[test]
    fn cursor_rejects_garbage_and_future_versions() {
        assert!(Cursor::decode("not base64!!").is_err());
        assert!(Cursor::decode("").is_err());
        let mut future = sample();
        future.v = 99;
        let err = Cursor::decode(&future.encode()).unwrap_err().to_string();
        assert!(
            err.contains("version"),
            "should name the version problem: {err}"
        );
    }

    #[test]
    fn fingerprint_is_stable_and_order_sensitive() {
        let a = vec!["https://a/f".to_string(), "https://b/f".to_string()];
        let b = vec!["https://b/f".to_string(), "https://a/f".to_string()];
        assert_eq!(
            fingerprint(&a, &["markdown", "2h", "25", "", "", "report"]),
            fingerprint(&a, &["markdown", "2h", "25", "", "", "report"]),
            "same inputs must fingerprint identically"
        );
        assert_ne!(
            fingerprint(&a, &["markdown", "2h", "25", "", "", "report"]),
            fingerprint(&b, &["markdown", "2h", "25", "", "", "report"]),
            "url order changes item positions, so it must change the fingerprint"
        );
        assert_ne!(
            fingerprint(&a, &["markdown", "2h", "25", "", "", "report"]),
            fingerprint(&a, &["markdown", "7d", "25", "", "", "report"]),
            "a different since window changes which items exist"
        );
        assert_eq!(
            fingerprint(&a, &["markdown", "2h", "25", "", "", "report"]).len(),
            16
        );
    }
}
