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
//!   - `StaleIfError`: like `Revalidate`, but if the origin *refuses* the revalidation
//!     (`429`/`403`/`5xx`/transport error) and a cached body exists, return it with
//!     `from_cache = true`, `not_modified = false`, and `stale_reason = Some(..)`. A cache
//!     miss, or any error that is not an origin refusal, propagates unchanged (ADR-0019).
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
    /// RFC-3339 time the served body was written to cache, when it came from cache.
    /// `None` for a body fetched fresh from the network this call.
    pub cached_at: Option<String>,
    /// Why this body is stale: the origin refused or failed the revalidation and
    /// [`CachePolicy::StaleIfError`] fell back to the cache. `None` on every other path.
    /// `core` turns it into [`crate::model::FeedStatus::Stale`] plus a `SERVED_STALE`
    /// warning (ADR-0019).
    pub stale_reason: Option<String>,
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
        self.fetch_until(url, cache, policy, None, None).await
    }

    /// [`fetch`](Self::fetch) bounded by an absolute batch deadline: a fetch that cannot even
    /// *start* before `stop_at` yields [`RssError::DeadlineExceeded`] from the per-host gate
    /// rather than queueing behind a cooldown that outlives the caller's timeout. See
    /// [`HostGate::acquire_until`]. `None` is the unbounded (CLI) behaviour.
    pub async fn fetch_until(
        &self,
        url: &str,
        cache: &Cache,
        policy: CachePolicy,
        stop_at: Option<tokio::time::Instant>,
        coverage_floor: Option<DateTime<Utc>>,
    ) -> Result<RawFeed, RssError> {
        match policy {
            // Never touch the cache: a plain GET returning whatever the server sends.
            CachePolicy::NoCache => {
                let resp = self
                    .gated_send(url, stop_at, || self.inner.get(url))
                    .await?;
                let status = resp.status();
                let final_url = resp.url().to_string();
                if !status.is_success() {
                    return Err(self.http_error(url, &resp));
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
                    cached_at: None,
                    stale_reason: None,
                })
            }

            // Serve a still-fresh cache entry without any network round-trip.
            CachePolicy::MaxAge(max_age) => {
                if let Some(entry) = cache.get(url)?
                    && is_fresh(&entry.meta.fetched_at, max_age)
                {
                    let cached_at = entry.meta.fetched_at.clone();
                    return Ok(RawFeed {
                        body: entry.body,
                        final_url: url.to_string(),
                        content_type: entry.meta.content_type,
                        status: 200,
                        not_modified: true,
                        from_cache: true,
                        cached_at: Some(cached_at),
                        stale_reason: None,
                    });
                }
                self.revalidate(url, cache, stop_at).await
            }

            // Serve the cached body if present, regardless of age — never revalidate.
            // Only a cache miss falls through to a normal conditional GET. See ADR-0014.
            CachePolicy::CacheFirst => {
                if let Some(entry) = cache.get(url)? {
                    let cached_at = entry.meta.fetched_at.clone();
                    return Ok(RawFeed {
                        body: entry.body,
                        final_url: url.to_string(),
                        content_type: entry.meta.content_type,
                        status: 200,
                        not_modified: true,
                        from_cache: true,
                        cached_at: Some(cached_at),
                        stale_reason: None,
                    });
                }
                self.revalidate(url, cache, stop_at).await
            }

            // Revalidate, but let a *refused* revalidation fall back to the cached body
            // instead of failing the feed (ADR-0019). Opt-in — the only path that can set
            // `stale_reason`.
            CachePolicy::StaleIfError => match self.revalidate(url, cache, stop_at).await {
                Ok(raw) => Ok(raw),
                // Only an *origin refusal* may be papered over with an old copy; every other
                // error propagates untouched. See `is_origin_refusal` for why each exclusion
                // is there.
                Err(e) if is_origin_refusal(&e) => {
                    // `.ok().flatten()` deliberately, not `?`: if the cache read itself fails
                    // the fallback simply did not work, and the caller should see the origin's
                    // `429` — not a `CACHE_ERROR` that masks what actually happened.
                    match cache.get(url).ok().flatten() {
                        // Serving a copy from before the window reports a guaranteed-empty
                        // answer as success and hides the refusal (ADR-0022).
                        Some(entry) if !covers_window(&entry.meta.fetched_at, coverage_floor) => {
                            Err(e)
                        }
                        Some(entry) => Ok(RawFeed {
                            body: entry.body,
                            final_url: url.to_string(),
                            content_type: entry.meta.content_type,
                            status: 200,
                            // Not a `304`: the origin never confirmed this body is current,
                            // which is exactly what separates stale from `NotModified`.
                            not_modified: false,
                            from_cache: true,
                            cached_at: Some(entry.meta.fetched_at),
                            stale_reason: Some(e.to_string()),
                        }),
                        // No usable cached copy, so there is no stale body to serve and the
                        // refusal stands rather than becoming an empty success.
                        None => Err(e),
                    }
                }
                Err(e) => Err(e),
            },

            // Default: conditional GET, letting a `304` reuse the cached body.
            CachePolicy::Revalidate => self.revalidate(url, cache, stop_at).await,
        }
    }

    /// Conditional GET: attach validators from any cache entry, reuse the cached body on a
    /// `304`, and otherwise store and return the fresh `200` response. `stop_at` bounds only
    /// the wait to *start* the request (see [`HostGate::acquire_until`]); a cache hit that
    /// short-circuits before here never consults it.
    async fn revalidate(
        &self,
        url: &str,
        cache: &Cache,
        stop_at: Option<tokio::time::Instant>,
    ) -> Result<RawFeed, RssError> {
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

        let resp = self.gated_send(url, stop_at, build).await?;
        let status = resp.status();
        let final_url = resp.url().to_string();

        // `304 Not Modified`: the cached body still stands; refresh only `fetched_at`.
        if status == StatusCode::NOT_MODIFIED {
            let entry = cached.ok_or_else(|| {
                RssError::Network(format!(
                    "server returned 304 but no cache entry exists for {url}"
                ))
            })?;
            // The body served here is the one written at `entry.meta.fetched_at` — report
            // that (pre-refresh) time, not the `now()` this call is about to stamp on the
            // entry for the *next* lookup.
            let cached_at = entry.meta.fetched_at.clone();
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
                cached_at: Some(cached_at),
                stale_reason: None,
            });
        }

        if !status.is_success() {
            return Err(self.http_error(url, &resp));
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
            cached_at: None,
            stale_reason: None,
        })
    }

    /// Classify a non-success response — the single place any status becomes an error.
    ///
    /// The gate's learned cooldown rides along whenever one is active, whatever the status: it
    /// is a true statement about when this host will next be sent to. It carries the most
    /// weight on a `429` (the status that becomes `RATE_LIMITED`), because the provider
    /// ADR-0016 probed sends no `Retry-After` at all, leaving the hint as the only pacing
    /// signal the caller gets.
    fn http_error(&self, url: &str, resp: &reqwest::Response) -> RssError {
        RssError::Http {
            status: resp.status().as_u16(),
            url: url.to_string(),
            retry_after: retry_after_raw(resp.headers()),
            retry_after_hint: self.gate.cooldown_remaining(url),
        }
    }

    /// Plain GET returning the raw body (used by `discover` for the homepage HTML).
    /// Unbounded — see [`get_bytes_until`](Self::get_bytes_until) to bound the wait to start.
    pub async fn get_bytes(&self, url: &str) -> Result<(Vec<u8>, String), RssError> {
        self.get_bytes_until(url, None).await
    }

    /// [`get_bytes`](Self::get_bytes) bounded by an absolute instant, mirroring
    /// [`fetch`](Self::fetch)/[`fetch_until`](Self::fetch_until).
    ///
    /// `None` is what turned a `discover` call on a busy host into a multi-minute hang:
    /// `MAX_GATE_WAIT` bounds one cooldown, never the queue of siblings behind it.
    pub async fn get_bytes_until(
        &self,
        url: &str,
        stop_at: Option<tokio::time::Instant>,
    ) -> Result<(Vec<u8>, String), RssError> {
        let resp = self
            .gated_send(url, stop_at, || self.inner.get(url))
            .await?;
        let status = resp.status();
        let final_url = resp.url().to_string();
        if !status.is_success() {
            return Err(self.http_error(url, &resp));
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

/// Whether a copy written at `fetched_at` can cover a window opening at `floor` (the
/// caller's *resolved* `since`). `None` floor = no window stated, so anything covers.
/// An unreadable stamp does not cover — serve only when provably covering
/// ([ADR-0022](../docs/adr/0022-stale-copies-that-cannot-cover-the-since-window.md)).
fn covers_window(fetched_at: &str, floor: Option<DateTime<Utc>>) -> bool {
    let Some(floor) = floor else { return true };
    DateTime::parse_from_rfc3339(fetched_at).is_ok_and(|dt| dt.with_timezone(&Utc) >= floor)
}

/// Whether an error means *the origin refused or failed to answer* — the only class
/// [`CachePolicy::StaleIfError`] may paper over with a cached body (ADR-0019).
///
/// The exclusions are the point of this function:
/// - **`Parse`** — the origin *did* answer; a feed that now emits malformed XML is a real
///   problem the caller should see, not something to hide behind an old copy (ADR-0019 §4).
///   (It also cannot reach here: parsing happens in `core`, after this layer.)
/// - **`DeadlineExceeded`** — the fetch never *started*, so the feed is unattempted, not
///   failed. Serving stale here would convert an omission the caller can resume into a
///   silently stale page, and would corrupt `feeds_omitted` (invariant 9).
/// - **`Cache`/`Io`** — the cache itself is the thing that is broken; reading it again is not
///   a recovery.
/// - **`Usage`/`InvalidUrl`/`ResponseTooLarge`/`NotFound`/`Other`** — caller or internal
///   errors, not origin behaviour.
fn is_origin_refusal(e: &RssError) -> bool {
    matches!(
        e,
        RssError::Http { .. } | RssError::RateLimited { .. } | RssError::Network(_)
    )
}

impl HttpClient {
    /// Gate-aware send: acquire the per-host permit (waiting out any active cooldown / sticky
    /// spacing), then send with the single bounded ADR-0015 retry on a transient `403`/`429`.
    /// `build` is called again for the retry so headers/validators are re-attached cleanly.
    ///
    /// The permit is held across the retry, so a request never blocks on the cooldown it
    /// itself just set (the two waits are one budget — ADR-0016). Acquiring may instead return
    /// [`RssError::RateLimited`] when a sibling's cooldown would make this request wait past
    /// the gate ceiling.
    async fn gated_send<F>(
        &self,
        url: &str,
        stop_at: Option<tokio::time::Instant>,
        build: F,
    ) -> Result<reqwest::Response, RssError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        // The gate keys on the *request* URL's host. reqwest follows redirects internally, so
        // a cross-host redirect's throttle is attributed to the origin host, not the final
        // one — acceptable (callers hit one host consistently) and unavoidable pre-send.
        let _permit = self.gate.acquire_until(url, stop_at).await?;

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
        // The served body is the one stored at the pre-refresh time, not the `now()` this
        // call is about to stamp on the entry for the *next* lookup.
        assert_eq!(
            raw.cached_at.as_deref(),
            Some(seeded.as_str()),
            "a 304 must report the timestamp the entry carried before this call refreshed it"
        );

        // The conditional GET fired and `fetched_at` was refreshed (validators kept).
        mock.assert_async().await;
        let entry = cache.get(&url).expect("cache get").expect("entry present");
        assert_ne!(entry.meta.fetched_at, seeded);
        assert_eq!(entry.meta.etag.as_deref(), Some("\"v1\""));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Pins the documented drift: across successive `304`s, `cached_at` reports the time of
    /// the *previous* revalidation, not the original origin fetch. A second 304 must report
    /// what the first one wrote, not the value seeded before either ran.
    #[tokio::test]
    async fn revalidate_304_twice_reports_previous_revalidation_time() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("revalidate-twice");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let original_fetch = "2020-01-01T00:00:00Z".to_string();
        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: original_fetch.clone(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, b"<rss>cached</rss>").expect("seed cache");

        // The etag survives both 304 branches (`entry.meta.etag.clone()`), so one mock with
        // `expect(2)` matches both conditional GETs.
        let mock = server
            .mock("GET", "/feed.xml")
            .match_header("if-none-match", "\"v1\"")
            .with_status(304)
            .expect(2)
            .create_async()
            .await;

        let http = client();

        let first = http
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .expect("first revalidate ok");
        assert_eq!(first.cached_at.as_deref(), Some(original_fetch.as_str()));
        let after_first = cache
            .get(&url)
            .expect("cache get")
            .expect("entry present")
            .meta
            .fetched_at;
        assert_ne!(after_first, original_fetch);

        let second = http
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .expect("second revalidate ok");
        assert_eq!(
            second.cached_at.as_deref(),
            Some(after_first.as_str()),
            "a second successive 304 must report the previous revalidation's timestamp"
        );
        assert_ne!(
            second.cached_at.as_deref(),
            Some(original_fetch.as_str()),
            "cached_at is NOT stable across repeated revalidations — it drifts forward"
        );

        mock.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn maxage_serves_cache_without_network() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("maxage");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let seeded = now_rfc3339();
        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: seeded.clone(),
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
        assert_eq!(
            raw.cached_at.as_deref(),
            Some(seeded.as_str()),
            "a MaxAge hit must report the entry's write time"
        );

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
    async fn cache_hit_reports_when_the_body_was_fetched() {
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_header("etag", "\"v1\"")
            .with_body("<rss version=\"2.0\"><channel><title>t</title></channel></rss>")
            .create_async()
            .await;

        let dir = std::env::temp_dir().join(format!("rss-cachedat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        let client = HttpClient::new("t", Duration::from_secs(5)).unwrap();
        let url = format!("{}/feed.xml", server.url());

        // A fresh 200 body did not come from cache.
        let fresh = client
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .unwrap();
        assert!(
            fresh.cached_at.is_none(),
            "a fresh body has no cache timestamp"
        );

        // A CacheFirst hit reports the entry's write time.
        let hit = client
            .fetch(&url, &cache, CachePolicy::CacheFirst)
            .await
            .unwrap();
        assert!(hit.from_cache);
        assert!(
            hit.cached_at.is_some(),
            "a cache hit must report when it was stored"
        );

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
        assert!(
            raw.cached_at.is_none(),
            "NoCache never reads or writes the cache, so it has no cache timestamp"
        );

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

    /// Seed a cached body so a `StaleIfError` test has something stale to fall back to.
    fn seed(cache: &Cache, url: &str, body: &[u8]) {
        let meta = CacheMeta {
            feed_url: url.to_string(),
            etag: Some("\"seeded\"".to_string()),
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, body).expect("seed cache");
    }

    /// Seed with an explicit `fetched_at`, for the coverage tests below.
    fn seed_at(cache: &Cache, url: &str, body: &[u8], fetched_at: DateTime<Utc>) {
        let meta = CacheMeta {
            feed_url: url.to_string(),
            etag: Some("\"seeded\"".to_string()),
            last_modified: None,
            fetched_at: fetched_at.to_rfc3339(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, body).expect("seed cache");
    }

    /// The reported digest run: 8 days cached, 24h asked for. ADR-0022.
    #[test]
    fn covers_window_is_a_coverage_test_not_a_freshness_test() {
        let floor = Utc::now() - chrono::Duration::hours(24);
        let at = |d: chrono::Duration| (Utc::now() - d).to_rfc3339();

        assert!(
            covers_window(&at(chrono::Duration::days(3650)), None),
            "no window, any age"
        );
        assert!(
            covers_window(&at(chrono::Duration::hours(1)), Some(floor)),
            "inside"
        );
        assert!(
            !covers_window(&at(chrono::Duration::days(8)), Some(floor)),
            "before it opened"
        );
        // An unreadable stamp proves nothing. Defensive: `now_rfc3339()` is the only producer.
        assert!(!covers_window("not a timestamp", Some(floor)));
        assert!(
            covers_window("not a timestamp", None),
            "with no window there is nothing to prove coverage of"
        );
    }

    #[tokio::test]
    async fn stale_if_error_declines_a_copy_that_cannot_cover_the_window() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-uncovered");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());
        // The reported digest run: 8 days cached, 24h asked for.
        seed_at(
            &cache,
            &url,
            b"<rss>cached</rss>",
            Utc::now() - chrono::Duration::days(8),
        );

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let err = client()
            .fetch_until(
                &url,
                &cache,
                CachePolicy::StaleIfError,
                None,
                Some(Utc::now() - chrono::Duration::hours(24)),
            )
            .await
            .expect_err("a copy older than the window must not be served as a stale success");

        // The origin's own error, not a substitute: a 429 is `RATE_LIMITED` (ADR-0021).
        match err {
            RssError::Http { status, .. } => assert_eq!(status, 429),
            other => panic!("expected the origin's Http error, got {other:?}"),
        }
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The other side: a copy inside the window still covers it, so ADR-0019 holds for a
    /// caller polling faster than it scopes.
    #[tokio::test]
    async fn stale_if_error_still_serves_a_copy_that_covers_the_window() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-covered");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());
        seed_at(
            &cache,
            &url,
            b"<rss>cached</rss>",
            Utc::now() - chrono::Duration::hours(1),
        );

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let raw = client()
            .fetch_until(
                &url,
                &cache,
                CachePolicy::StaleIfError,
                None,
                Some(Utc::now() - chrono::Duration::hours(24)),
            )
            .await
            .expect("an hour-old copy covers a 24h window");

        assert!(raw.from_cache);
        assert!(raw.stale_reason.is_some());
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// No window, no rule — a bare `stale-if-error` fetch keeps ADR-0019 exactly.
    #[tokio::test]
    async fn stale_if_error_without_a_window_serves_any_age() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-nowindow");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());
        seed_at(
            &cache,
            &url,
            b"<rss>cached</rss>",
            Utc::now() - chrono::Duration::days(8),
        );

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let raw = client()
            .fetch_until(&url, &cache, CachePolicy::StaleIfError, None, None)
            .await
            .expect("with no window stated, any cached copy is still the best available truth");

        assert!(raw.from_cache);
        assert!(raw.stale_reason.is_some());
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn stale_if_error_serves_the_cached_body_when_the_origin_refuses() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-429");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());
        seed(&cache, &url, b"<rss>cached</rss>");

        // 429 twice (original + the single ADR-0015 retry), so the revalidation truly fails.
        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::StaleIfError)
            .await
            .expect("a refused revalidation with a cached copy must not fail the feed");

        assert_eq!(raw.body, b"<rss>cached</rss>");
        assert!(raw.from_cache);
        assert!(
            !raw.not_modified,
            "the origin never confirmed this body — that is what separates stale from a 304"
        );
        assert!(
            raw.stale_reason
                .as_deref()
                .is_some_and(|r| r.contains("429")),
            "the staleness reason must name the upstream refusal: {:?}",
            raw.stale_reason
        );
        assert_eq!(raw.cached_at.as_deref(), Some("2020-01-01T00:00:00Z"));
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_throttled_429_is_paceable_and_carries_the_gates_learned_window() {
        // The digest-agent bug: a Reddit-style `429` (no `Retry-After` header) surfaced as an
        // opaque `FEED_FETCH_FAILED` with no wait hint, so the caller could not tell "wait 40s"
        // from "this feed is dead" and dropped the source. The gate already knows the window.
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("throttle-code");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // No `Retry-After` anywhere — exactly the provider ADR-0016 probed.
        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        let err = client()
            .fetch(&url, &cache, CachePolicy::Revalidate)
            .await
            .unwrap_err();

        assert_eq!(err.code(), "RATE_LIMITED", "got {err:?}");
        match &err {
            RssError::Http {
                status,
                retry_after,
                retry_after_hint,
                ..
            } => {
                assert_eq!(*status, 429);
                assert_eq!(*retry_after, None, "this provider sends no header");
                assert!(
                    retry_after_hint.is_some_and(|d| d > Duration::from_secs(0)),
                    "the gate's escalated cooldown must ride along, else the agent has nothing \
                     to pace on: {retry_after_hint:?}"
                );
            }
            other => panic!("expected Http, got {other:?}"),
        }
        assert!(
            err.to_error_obj(Some(&url)).details["retry_after_seconds"].is_u64(),
            "the wait must reach the wire as a number"
        );
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn get_bytes_sheds_a_cooldown_that_outlives_the_deadline() {
        // `discover` used to call `get_bytes` with `stop_at: None`, so a warm host's cooldown
        // was slept out in full and the caller got nothing back before its tool timeout.
        // `MAX_GATE_WAIT` does not save it: 30s is under that ceiling, so the old code slept.
        let mut server = mockito::Server::new_async().await;
        let url = format!("{}/index.html", server.url());
        let http = client();
        http.gate
            .note_throttled(&url, Some(Duration::from_secs(30)));

        // Proves the request is never sent: the deadline is enforced *at the gate*.
        let never = server
            .mock("GET", "/index.html")
            .expect(0)
            .create_async()
            .await;

        let start = tokio::time::Instant::now();
        let err = http
            .get_bytes_until(&url, Some(start + Duration::from_millis(200)))
            .await
            .unwrap_err();
        let elapsed = start.elapsed();

        assert!(
            matches!(err, RssError::DeadlineExceeded { .. }),
            "a cooldown past the deadline must shed as unattempted, got {err:?}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the deadline must bound the wait, not the 30s cooldown; took {elapsed:?}"
        );
        never.assert_async().await;
    }

    #[tokio::test]
    async fn stale_if_error_still_fails_when_nothing_is_cached() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-nocache");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(429)
            .expect(2)
            .create_async()
            .await;

        // No cached copy: there is no stale body to serve, so the refusal must stand rather
        // than become a silently empty success.
        let err = client()
            .fetch(&url, &cache, CachePolicy::StaleIfError)
            .await
            .unwrap_err();
        match err {
            RssError::Http { status, .. } => assert_eq!(status, 429),
            other => panic!("expected the original Http error, got {other:?}"),
        }
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn stale_if_error_is_a_plain_revalidate_when_the_origin_answers() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("stale-happy");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());
        seed(&cache, &url, b"<rss>cached</rss>");

        let m = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>fresh</rss>")
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::StaleIfError)
            .await
            .expect("a 200 must behave exactly like Revalidate");

        assert_eq!(raw.body, b"<rss>fresh</rss>", "the fresh body wins");
        assert!(
            raw.stale_reason.is_none(),
            "nothing was refused, so nothing is stale"
        );
        assert!(!raw.from_cache);
        m.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn only_origin_refusals_may_be_papered_over_with_a_stale_copy() {
        // The exclusions are the whole safety argument for `StaleIfError` (ADR-0019 §4).
        assert!(is_origin_refusal(&RssError::Http {
            status: 429,
            url: "u".into(),
            retry_after: None,
            retry_after_hint: None
        }));
        assert!(is_origin_refusal(&RssError::RateLimited {
            url: "u".into(),
            retry_after: Duration::from_secs(1)
        }));
        assert!(is_origin_refusal(&RssError::Network("reset".into())));

        // A body that arrived and failed to parse is a real problem, not a staleness case.
        assert!(!is_origin_refusal(&RssError::Parse("bad xml".into())));
        // Never started => unattempted, and must stay resumable via `feeds_omitted`. Serving
        // stale here would convert an omission into a silently stale page (invariant 9).
        assert!(!is_origin_refusal(&RssError::DeadlineExceeded {
            url: "u".into()
        }));
        assert!(!is_origin_refusal(&RssError::Cache("disk".into())));
        assert!(!is_origin_refusal(&RssError::Usage("nope".into())));
    }
}
