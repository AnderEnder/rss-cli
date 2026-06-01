//! HTTP client with conditional GET. **Owner: `fetcher` agent.**
//!
//! Frozen public interface — [`crate::core`] depends on these exact signatures. Implement
//! the bodies; do not change the signatures without coordinating with the team lead.
//!
//! ## Requirements
//! - Build a `reqwest::Client` with gzip, a sane redirect policy, the given timeout, and
//!   the provided User-Agent.
//! - Honor [`CachePolicy`]:
//!   - `NoCache`: plain GET, never read or write the cache.
//!   - `MaxAge(d)`: if a cache entry exists and is younger than `d`, return it without any
//!     network call (`from_cache = true`, `not_modified = true`). Otherwise revalidate.
//!   - `Revalidate` (default): send `If-None-Match` (etag) / `If-Modified-Since`
//!     (last_modified) from the cache entry if present. On `304`, return the cached body
//!     with `not_modified = true`, `from_cache = true`. On `200`, store the new body +
//!     validators (`ETag`, `Last-Modified`) in the cache and return it.
//! - On a non-success, non-304 status, return [`RssError::Http`].
//! - `final_url` is the URL after following redirects (used to resolve relative links).

use std::time::Duration;

use crate::cache::Cache;
use crate::config::CachePolicy;
use crate::error::RssError;

/// Raw bytes of a fetched (or cached) feed, plus the metadata `parse`/`core` need.
#[derive(Debug, Clone)]
pub struct RawFeed {
    pub body: Vec<u8>,
    /// URL after redirects (use to resolve relative item links).
    pub final_url: String,
    pub content_type: Option<String>,
    pub status: u16,
    /// True when served from cache via a `304` (or a fresh `MaxAge` hit).
    pub not_modified: bool,
    /// True when the returned body came from the cache rather than a fresh `200` body.
    pub from_cache: bool,
}

/// Reusable HTTP client.
#[derive(Clone)]
pub struct HttpClient {
    #[allow(dead_code)]
    inner: reqwest::Client,
}

impl HttpClient {
    /// Build a client with the given User-Agent and per-request timeout.
    pub fn new(user_agent: &str, timeout: Duration) -> Result<Self, RssError> {
        let inner = reqwest::Client::builder()
            .user_agent(user_agent)
            .timeout(timeout)
            .gzip(true)
            .build()
            .map_err(|e| RssError::Network(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Fetch `url`, applying the cache `policy`. See module docs for the contract.
    pub async fn fetch(
        &self,
        url: &str,
        cache: &Cache,
        policy: CachePolicy,
    ) -> Result<RawFeed, RssError> {
        let _ = (url, cache, policy);
        todo!("fetcher: implement conditional GET + cache integration")
    }

    /// Plain GET returning the raw body (used by `discover` for the homepage HTML).
    pub async fn get_bytes(&self, url: &str) -> Result<(Vec<u8>, String), RssError> {
        let _ = url;
        todo!("fetcher: implement plain GET returning (body, final_url)")
    }
}
