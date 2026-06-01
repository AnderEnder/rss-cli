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

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use reqwest::header::{
    CONTENT_TYPE, ETAG, HeaderMap, HeaderName, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
};

use crate::cache::{Cache, CacheMeta};
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
        match policy {
            // Never touch the cache: a plain GET returning whatever the server sends.
            CachePolicy::NoCache => {
                let resp = self
                    .inner
                    .get(url)
                    .send()
                    .await
                    .map_err(|e| RssError::Network(e.to_string()))?;
                let status = resp.status();
                let final_url = resp.url().to_string();
                if !status.is_success() {
                    return Err(RssError::Http {
                        status: status.as_u16(),
                        url: url.to_string(),
                    });
                }
                let content_type = header_string(resp.headers(), &CONTENT_TYPE);
                let body = resp
                    .bytes()
                    .await
                    .map_err(|e| RssError::Network(e.to_string()))?
                    .to_vec();
                Ok(RawFeed {
                    body,
                    final_url,
                    content_type,
                    status: status.as_u16(),
                    not_modified: false,
                    from_cache: false,
                })
            }

            // Serve a still-fresh cache entry without any network round-trip.
            CachePolicy::MaxAge(max_age) => {
                if let Some(entry) = cache.get(url)?
                    && is_fresh(&entry.meta.fetched_at, max_age)
                {
                    return Ok(RawFeed {
                        body: entry.body,
                        final_url: url.to_string(),
                        content_type: entry.meta.content_type,
                        status: 200,
                        not_modified: true,
                        from_cache: true,
                    });
                }
                self.revalidate(url, cache).await
            }

            // Default: conditional GET, letting a `304` reuse the cached body.
            CachePolicy::Revalidate => self.revalidate(url, cache).await,
        }
    }

    /// Conditional GET: attach validators from any cache entry, reuse the cached body on a
    /// `304`, and otherwise store and return the fresh `200` response.
    async fn revalidate(&self, url: &str, cache: &Cache) -> Result<RawFeed, RssError> {
        let cached = cache.get(url)?;

        let mut req = self.inner.get(url);
        if let Some(entry) = &cached {
            if let Some(etag) = &entry.meta.etag {
                req = req.header(IF_NONE_MATCH, etag.as_str());
            }
            if let Some(last_modified) = &entry.meta.last_modified {
                req = req.header(IF_MODIFIED_SINCE, last_modified.as_str());
            }
        }

        let resp = req
            .send()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?;
        let status = resp.status();
        let final_url = resp.url().to_string();

        // `304 Not Modified`: the cached body still stands; refresh only `fetched_at`.
        if status == StatusCode::NOT_MODIFIED {
            let entry = cached.ok_or_else(|| {
                RssError::Network(format!(
                    "server returned 304 but no cache entry exists for {url}"
                ))
            })?;
            let meta = CacheMeta {
                feed_url: url.to_string(),
                etag: entry.meta.etag.clone(),
                last_modified: entry.meta.last_modified.clone(),
                fetched_at: now_rfc3339(),
                content_type: entry.meta.content_type.clone(),
            };
            cache.put(&meta, &entry.body)?;
            return Ok(RawFeed {
                body: entry.body,
                final_url,
                content_type: entry.meta.content_type,
                status: StatusCode::NOT_MODIFIED.as_u16(),
                not_modified: true,
                from_cache: true,
            });
        }

        if !status.is_success() {
            return Err(RssError::Http {
                status: status.as_u16(),
                url: url.to_string(),
            });
        }

        // `200 OK`: capture validators, store the new body, and return it.
        let content_type = header_string(resp.headers(), &CONTENT_TYPE);
        let etag = header_string(resp.headers(), &ETAG);
        let last_modified = header_string(resp.headers(), &LAST_MODIFIED);
        let body = resp
            .bytes()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?
            .to_vec();

        let meta = CacheMeta {
            feed_url: url.to_string(),
            etag,
            last_modified,
            fetched_at: now_rfc3339(),
            content_type: content_type.clone(),
        };
        cache.put(&meta, &body)?;

        Ok(RawFeed {
            body,
            final_url,
            content_type,
            status: status.as_u16(),
            not_modified: false,
            from_cache: false,
        })
    }

    /// Plain GET returning the raw body (used by `discover` for the homepage HTML).
    pub async fn get_bytes(&self, url: &str) -> Result<(Vec<u8>, String), RssError> {
        let resp = self
            .inner
            .get(url)
            .send()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?;
        let status = resp.status();
        let final_url = resp.url().to_string();
        if !status.is_success() {
            return Err(RssError::Http {
                status: status.as_u16(),
                url: url.to_string(),
            });
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?
            .to_vec();
        Ok((body, final_url))
    }
}

/// `true` if a cache entry written at `fetched_at` (RFC-3339) is younger than `max_age`.
fn is_fresh(fetched_at: &str, max_age: Duration) -> bool {
    let Ok(fetched) = DateTime::parse_from_rfc3339(fetched_at) else {
        return false;
    };
    let Ok(max_age) = chrono::Duration::from_std(max_age) else {
        // An absurdly large window — treat any existing entry as fresh.
        return true;
    };
    Utc::now().signed_duration_since(fetched) < max_age
}

/// Current UTC time as an RFC-3339 string (seconds precision, `Z` suffix).
fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Extract a response header as an owned `String`, if present and valid UTF-8.
fn header_string(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    /// Build a client with a short timeout for tests.
    fn client() -> HttpClient {
        HttpClient::new("rss-cli-test", Duration::from_secs(10)).expect("build client")
    }

    /// A per-test temp cache dir (tag keeps parallel tests from colliding).
    fn temp_cache_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rss-fetch-test-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp cache dir");
        dir
    }

    #[tokio::test]
    async fn fetch_200_stores_body_and_validators() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_header("etag", "\"v1\"")
            .with_header("content-type", "application/rss+xml")
            .with_body("<rss>fresh</rss>")
            .create_async()
            .await;

        let dir = temp_cache_dir("store");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let raw = client()
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .expect("fetch ok");

        assert_eq!(raw.status, 200);
        assert!(!raw.from_cache);
        assert!(!raw.not_modified);
        assert_eq!(raw.body, b"<rss>fresh</rss>".to_vec());
        assert_eq!(raw.content_type.as_deref(), Some("application/rss+xml"));

        // The body and validators must have landed in the cache.
        let entry = cache.get(&url).expect("cache get").expect("entry present");
        assert_eq!(entry.body, b"<rss>fresh</rss>".to_vec());
        assert_eq!(entry.meta.etag.as_deref(), Some("\"v1\""));

        mock.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn revalidate_304_reuses_cached_body() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("revalidate");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // Seed the cache with a validator and a deliberately stale timestamp so we can
        // confirm the `304` refreshes `fetched_at`.
        let seeded = "2020-01-01T00:00:00Z".to_string();
        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: seeded.clone(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, b"<rss>cached</rss>").expect("seed cache");

        let mock = server
            .mock("GET", "/feed.xml")
            .match_header("if-none-match", "\"v1\"")
            .with_status(304)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .expect("fetch ok");

        assert_eq!(raw.status, 304);
        assert!(raw.from_cache);
        assert!(raw.not_modified);
        assert_eq!(raw.body, b"<rss>cached</rss>".to_vec());
        assert_eq!(raw.content_type.as_deref(), Some("application/rss+xml"));

        // The conditional GET fired and `fetched_at` was refreshed (validators kept).
        mock.assert_async().await;
        let entry = cache.get(&url).expect("cache get").expect("entry present");
        assert_ne!(entry.meta.fetched_at, seeded);
        assert_eq!(entry.meta.etag.as_deref(), Some("\"v1\""));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn maxage_serves_cache_without_network() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("maxage");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: now_rfc3339(),
            content_type: Some("application/atom+xml".to_string()),
        };
        cache
            .put(&meta, b"<feed>cached</feed>")
            .expect("seed cache");

        // A network hit would be a bug: this mock returns a *different* body and must
        // never be matched.
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<feed>network</feed>")
            .expect(0)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::MaxAge(Duration::from_secs(3600)))
            .await
            .expect("fetch ok");

        assert_eq!(raw.status, 200);
        assert!(raw.from_cache);
        assert!(raw.not_modified);
        // The cached body (not the mock's) proves no network round-trip happened.
        assert_eq!(raw.body, b"<feed>cached</feed>".to_vec());
        assert_eq!(raw.content_type.as_deref(), Some("application/atom+xml"));

        mock.assert_async().await; // expect(0): fails if the network was hit.
        std::fs::remove_dir_all(&dir).ok();
    }
}
