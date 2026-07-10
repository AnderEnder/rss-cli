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
//!   - `CacheFirst`: if a cache entry exists, return it without any network call regardless
//!     of age (`from_cache = true`, `not_modified = true`); only a cache miss falls through
//!     to a conditional GET. Used by item lookup so a rolled feed window cannot evict an item
//!     the caller already saw (ADR-0014).
//!   - `Revalidate` (default): send `If-None-Match` (etag) / `If-Modified-Since`
//!     (last_modified) from the cache entry if present. On `304`, return the cached body
//!     with `not_modified = true`, `from_cache = true`. On `200`, store the new body +
//!     validators (`ETag`, `Last-Modified`) in the cache and return it.
//! - On a non-success, non-304 status, return [`RssError::Http`].
//! - `final_url` is the URL after following redirects (used to resolve relative links).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use reqwest::header::{
    CONTENT_TYPE, ETAG, HeaderMap, HeaderName, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED,
};
use tokio::time::sleep;

use crate::cache::{Cache, CacheMeta};
use crate::config::CachePolicy;
use crate::error::RssError;
use crate::ratelimit::HostGate;

/// Raw bytes of a fetched (or cached) feed, plus the metadata `parse`/`core` need.
#[derive(Debug, Clone)]
pub struct RawFeed {
    pub body: Vec<u8>,
    /// URL after redirects (use to resolve relative item links).
    pub final_url: String,
    pub content_type: Option<String>,
    pub status: u16,
    /// True when served from cache via a `304` (or a `MaxAge`/`CacheFirst` hit).
    pub not_modified: bool,
    /// True when the returned body came from the cache rather than a fresh `200` body.
    pub from_cache: bool,
}

/// Reusable HTTP client.
///
/// Reuse it: cloning is cheap (both fields are `Arc`-backed) and a clone **shares the same
/// per-host gate and connection pool**. The MCP server builds one and shares it across tool
/// calls so concurrent calls coordinate their pacing (ADR-0016); the CLI builds one per run.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    /// Shared per-host request gate (ADR-0016).
    gate: Arc<HostGate>,
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
        Ok(Self {
            inner,
            gate: Arc::new(HostGate::from_env()),
        })
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
                let resp = self.gated_send(url, || self.inner.get(url)).await?;
                let status = resp.status();
                let final_url = resp.url().to_string();
                if !status.is_success() {
                    return Err(RssError::Http {
                        status: status.as_u16(),
                        url: url.to_string(),
                        retry_after: retry_after_raw(resp.headers()),
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

            // Serve the cached body if present, regardless of age — never revalidate.
            // Only a cache miss falls through to a normal conditional GET. See ADR-0014.
            CachePolicy::CacheFirst => {
                if let Some(entry) = cache.get(url)? {
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

        let build = || {
            let mut req = self.inner.get(url);
            if let Some(entry) = &cached {
                if let Some(etag) = &entry.meta.etag {
                    req = req.header(IF_NONE_MATCH, etag.as_str());
                }
                if let Some(last_modified) = &entry.meta.last_modified {
                    req = req.header(IF_MODIFIED_SINCE, last_modified.as_str());
                }
            }
            req
        };

        let resp = self.gated_send(url, build).await?;
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
                retry_after: retry_after_raw(resp.headers()),
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
        let resp = self.gated_send(url, || self.inner.get(url)).await?;
        let status = resp.status();
        let final_url = resp.url().to_string();
        if !status.is_success() {
            return Err(RssError::Http {
                status: status.as_u16(),
                url: url.to_string(),
                retry_after: retry_after_raw(resp.headers()),
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

/// Statuses we retry exactly once — transient provider rate-limiting.
fn is_retryable(status: StatusCode) -> bool {
    status == StatusCode::FORBIDDEN || status == StatusCode::TOO_MANY_REQUESTS
}

/// Parse the delta-seconds form of `Retry-After`, bounded to `max`. The HTTP-date form is
/// intentionally ignored (Reddit sends delta-seconds) rather than risk a wrong sleep.
fn retry_after(headers: &HeaderMap, max: Duration) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs).min(max))
}

/// Raw `Retry-After` header value, for surfacing in the error detail.
fn retry_after_raw(headers: &HeaderMap) -> Option<String> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

impl HttpClient {
    /// Gate-aware send: acquire the per-host permit (waiting out any active cooldown / sticky
    /// spacing), then send with the single bounded ADR-0015 retry on a transient `403`/`429`.
    /// `build` is called again for the retry so headers/validators are re-attached cleanly.
    ///
    /// The permit is held across the retry, so a request never blocks on the cooldown it
    /// itself just set (the two waits are one budget — ADR-0016). Acquiring may instead return
    /// [`RssError::RateLimited`] when a sibling's cooldown would make this request wait past
    /// the gate ceiling.
    async fn gated_send<F>(&self, url: &str, build: F) -> Result<reqwest::Response, RssError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        // The gate keys on the *request* URL's host. reqwest follows redirects internally, so
        // a cross-host redirect's throttle is attributed to the origin host, not the final
        // one — acceptable (callers hit one host consistently) and unavoidable pre-send.
        let _permit = self.gate.acquire(url).await?;

        let resp = build()
            .send()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?;
        if !is_retryable(resp.status()) {
            self.gate.note_success(url);
            return Ok(resp);
        }

        // Transient 403/429: extend the sibling cooldown, then spend the single retry.
        self.gate
            .note_throttled(url, retry_after_duration(resp.headers()));
        let wait = retry_after(resp.headers(), RETRY_MAX_DELAY).unwrap_or(RETRY_BASE_DELAY);
        sleep(wait).await;
        let resp = build()
            .send()
            .await
            .map_err(|e| RssError::Network(e.to_string()))?;
        if is_retryable(resp.status()) {
            self.gate
                .note_throttled(url, retry_after_duration(resp.headers()));
        } else {
            self.gate.note_success(url);
        }
        Ok(resp)
    }
}

/// Parse `Retry-After` into a `Duration` from now, accepting **both** the delta-seconds and
/// the HTTP-date forms. ADR-0015 deferred the date form; ADR-0016 consumes it (the gate clamps
/// the result), so a skewed or already-past date is harmless. `None` when absent, unparseable,
/// or already in the past.
fn retry_after_duration(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let raw = raw.trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    // HTTP-date form (RFC 1123, an RFC 2822 date).
    let when = DateTime::parse_from_rfc2822(raw).ok()?;
    (when.with_timezone(&Utc) - Utc::now()).to_std().ok()
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

    fn headers_with_retry_after(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(reqwest::header::RETRY_AFTER, value.parse().unwrap());
        h
    }

    #[test]
    fn retry_after_parses_delta_seconds_and_caps_to_max() {
        // The in-flight ADR-0015 retry wait: delta-seconds, clamped to `max`.
        assert_eq!(
            retry_after(&headers_with_retry_after("2"), RETRY_MAX_DELAY),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            retry_after(&headers_with_retry_after("30"), RETRY_MAX_DELAY),
            Some(RETRY_MAX_DELAY),
            "a value above the cap must clamp to RETRY_MAX_DELAY"
        );
        assert_eq!(retry_after(&HeaderMap::new(), RETRY_MAX_DELAY), None);
    }

    #[test]
    fn retry_after_duration_parses_delta_seconds_and_http_date() {
        // Delta-seconds (uncapped here — the gate clamps).
        assert_eq!(
            retry_after_duration(&headers_with_retry_after("45")),
            Some(Duration::from_secs(45))
        );
        // A future HTTP-date yields a positive duration...
        let future =
            retry_after_duration(&headers_with_retry_after("Wed, 21 Oct 2099 07:28:00 GMT"))
                .expect("a future HTTP-date should parse to a positive duration");
        assert!(future > Duration::from_secs(0));
        // ...a past date yields None (never a wrong/negative sleep)...
        assert_eq!(
            retry_after_duration(&headers_with_retry_after("Wed, 21 Oct 2015 07:28:00 GMT")),
            None
        );
        // ...and garbage / absent yield None.
        assert_eq!(
            retry_after_duration(&headers_with_retry_after("soon")),
            None
        );
        assert_eq!(retry_after_duration(&HeaderMap::new()), None);
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

    #[tokio::test]
    async fn cache_first_serves_stale_cache_without_network() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("cachefirst");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // Deliberately STALE timestamp: CacheFirst must ignore age entirely.
        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, b"<rss>cached</rss>").expect("seed cache");

        // Any network hit is a bug.
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>network</rss>")
            .expect(0)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::CacheFirst)
            .await
            .expect("fetch ok");

        assert!(raw.from_cache);
        assert!(raw.not_modified);
        assert_eq!(raw.body, b"<rss>cached</rss>".to_vec());

        mock.assert_async().await; // expect(0): fails if the network was hit.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cache_first_fetches_on_miss() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>fresh</rss>")
            .create_async()
            .await;
        let dir = temp_cache_dir("cachefirst-miss");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let raw = client()
            .fetch(&url, &cache, CachePolicy::CacheFirst)
            .await
            .expect("fetch ok");

        assert!(!raw.from_cache);
        assert_eq!(raw.body, b"<rss>fresh</rss>".to_vec());
        mock.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn retries_once_on_403_then_succeeds() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("retry-ok");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let m403 = server
            .mock("GET", "/feed.xml")
            .with_status(403)
            .expect(1)
            .create_async()
            .await;
        let m200 = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>ok</rss>")
            .expect(1)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::NoCache)
            .await
            .expect("retry should succeed");
        assert_eq!(raw.body, b"<rss>ok</rss>".to_vec());

        m403.assert_async().await;
        m200.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn persistent_403_surfaces_status_in_error() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("retry-fail");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // Two 403s (original + one retry), then assert we still error out.
        let m = server
            .mock("GET", "/feed.xml")
            .with_status(403)
            .expect(2)
            .create_async()
            .await;

        let err = client()
            .fetch(&url, &cache, CachePolicy::NoCache)
            .await
            .unwrap_err();
        match err {
            RssError::Http { status, .. } => assert_eq!(status, 403),
            other => panic!("expected Http error, got {other:?}"),
        }
        m.assert_async().await; // exactly 2 attempts: one retry, no more.
        std::fs::remove_dir_all(&dir).ok();
    }
}
