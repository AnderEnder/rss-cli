//! Error type, stable error codes, and process exit codes.

use std::time::Duration;

use crate::model::ErrorObj;

/// Process exit codes (stable contract for scripts and agents).
pub mod exit {
    /// All feeds succeeded.
    pub const OK: i32 = 0;
    /// Unexpected internal error.
    pub const UNEXPECTED: i32 = 1;
    /// Usage / argument error.
    pub const USAGE: i32 = 2;
    /// Some feeds succeeded, some failed.
    pub const PARTIAL: i32 = 3;
    /// Every requested feed failed.
    pub const ALL_FAILED: i32 = 4;
}

/// Library error type. Carries a stable [`code`](RssError::code) used in [`ErrorObj`].
#[derive(Debug, thiserror::Error)]
pub enum RssError {
    #[error("usage error: {0}")]
    Usage(String),

    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("HTTP status {status}: {url}")]
    Http {
        status: u16,
        url: String,
        /// Raw `Retry-After` header value when the server sent one (delta-seconds form).
        retry_after: Option<String>,
        /// The per-host gate's learned cooldown, when one is active. ADR-0016's probe found a
        /// provider that throttles with **no** `Retry-After` at all, so on those hosts this is
        /// the only thing that makes a `429` paceable rather than a guess.
        retry_after_hint: Option<Duration>,
    },

    #[error("rate limited: {url} (retry after ~{}s)", retry_after.as_secs())]
    RateLimited {
        url: String,
        /// How long the caller should wait before retrying, per the per-host gate (ADR-0016).
        retry_after: Duration,
    },

    /// The batch deadline passed before this fetch could **start** — it never reached the
    /// origin. On the batch path it stays internal: `core::fetch_feeds` maps it to an
    /// unattempted feed (`truncation.feeds_omitted`), never into `errors[]`. The single-URL
    /// `discover_feeds` / `get_item` tools have no such envelope to defer into, so there it
    /// surfaces as a `BATCH_DEADLINE_EXCEEDED` tool error — still "retry this", never "this
    /// feed is broken". Unreachable when `FetchParams::deadline` is `None`.
    #[error("batch deadline passed before {url} was attempted")]
    DeadlineExceeded { url: String },

    #[error("feed parse error: {0}")]
    Parse(String),

    #[error("no feeds discovered at {0}")]
    NotFound(String),

    #[error("cache error: {0}")]
    Cache(String),

    #[error(
        "response too large: ~{estimated_tokens} tokens exceeds the {budget_tokens}-token \
         budget; retry with limit={suggested_limit} or max_content_chars={suggested_max_content_chars}"
    )]
    ResponseTooLarge {
        estimated_tokens: usize,
        budget_tokens: usize,
        suggested_limit: usize,
        suggested_max_content_chars: usize,
    },

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

impl RssError {
    /// Stable, machine-readable error code (`SCREAMING_SNAKE_CASE`).
    pub fn code(&self) -> &'static str {
        match self {
            RssError::Usage(_) => "USAGE_ERROR",
            RssError::InvalidUrl(_) => "INVALID_URL",
            RssError::Network(_) => "NETWORK_ERROR",
            // A `429` is a window, not a dead feed. It shares the gate's paceable code so an
            // agent branches on one signal and waits, instead of reading an opaque
            // `FEED_FETCH_FAILED` as "drop this source" (ADR-0016). Every other status stays a
            // plain fetch failure — notably `403`, which is a block and must not be waited out.
            RssError::Http { status: 429, .. } => "RATE_LIMITED",
            RssError::Http { .. } => "FEED_FETCH_FAILED",
            RssError::RateLimited { .. } => "RATE_LIMITED",
            RssError::DeadlineExceeded { .. } => "BATCH_DEADLINE_EXCEEDED",
            RssError::Parse(_) => "FEED_PARSE_FAILED",
            RssError::NotFound(_) => "NOT_FOUND",
            RssError::Cache(_) => "CACHE_ERROR",
            RssError::ResponseTooLarge { .. } => "RESPONSE_TOO_LARGE",
            RssError::Io(_) => "IO_ERROR",
            RssError::Other(_) => "INTERNAL_ERROR",
        }
    }

    /// Convert into the serialized [`ErrorObj`], attaching any structured details.
    pub fn to_error_obj(&self, feed_url: Option<&str>) -> ErrorObj {
        let mut obj = ErrorObj::new(self.code(), self.to_string());
        if let Some(u) = feed_url {
            obj.feed_url = Some(u.to_string());
        }
        match self {
            RssError::Http {
                status,
                retry_after,
                retry_after_hint,
                ..
            } => {
                obj.details = serde_json::json!({
                    "http_status": status,
                    "retry_after": retry_after,
                    // Both pacing keys, exactly as the gate's own `RATE_LIMITED` emits them: a
                    // `429` now shares that code, so a client must never have to know which
                    // side refused to find the wait. `null` when the host is not in cooldown.
                    "retry_after_seconds": retry_after_hint.map(|d| d.as_secs()),
                    "retry_after_ms": retry_after_hint
                        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
                });
            }
            RssError::RateLimited { retry_after, .. } => {
                // Machine-readable pacing hint so the agent can wait, not give up.
                obj.details = serde_json::json!({
                    "retry_after_seconds": retry_after.as_secs(),
                    "retry_after_ms": u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX),
                });
            }
            RssError::ResponseTooLarge {
                estimated_tokens,
                budget_tokens,
                suggested_limit,
                suggested_max_content_chars,
            } => {
                // Machine-readable remediation so the agent can retry without giving up.
                obj.details = serde_json::json!({
                    "estimated_tokens": estimated_tokens,
                    "budget_tokens": budget_tokens,
                    "suggested_limit": suggested_limit,
                    "suggested_max_content_chars": suggested_max_content_chars,
                });
            }
            _ => {}
        }
        obj
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_error_surfaces_retry_after_when_present() {
        let e = RssError::Http {
            status: 503,
            url: "https://x".into(),
            retry_after: Some("2".into()),
            retry_after_hint: None,
        };
        let obj = e.to_error_obj(None);
        assert_eq!(obj.code, "FEED_FETCH_FAILED");
        assert_eq!(obj.details["http_status"], 503);
        assert_eq!(obj.details["retry_after"], "2");
    }

    #[test]
    fn a_429_is_paceable_and_every_other_status_is_a_plain_failure() {
        // The bug this pins: a Reddit-style `429` (no `Retry-After` header at all) arrived as
        // an opaque `FEED_FETCH_FAILED`, indistinguishable from a dead feed, so a digest agent
        // dropped the source instead of waiting. The gate's learned cooldown rides along as
        // `retry_after_seconds` — the same key the gate's own `RATE_LIMITED` uses.
        let throttled = RssError::Http {
            status: 429,
            url: "https://x/feed".into(),
            retry_after: None,
            retry_after_hint: Some(Duration::from_secs(48)),
        };
        let obj = throttled.to_error_obj(Some("https://x/feed"));
        assert_eq!(obj.code, "RATE_LIMITED");
        // Both variants of `RATE_LIMITED` must present the same pacing shape, since the MCP
        // guidance promises these two keys for the code, not for one of its two sources.
        assert_eq!(obj.details["retry_after_seconds"], 48);
        assert_eq!(obj.details["retry_after_ms"], 48_000);
        // Display keeps naming the status, which is what a `SERVED_STALE` warning quotes.
        assert!(obj.message.contains("429"), "message was {:?}", obj.message);

        // `403` is a block, not a window: waiting it out is wrong, so it must not share the
        // paceable code even though the gate retries both.
        for status in [403, 404, 500] {
            let e = RssError::Http {
                status,
                url: "https://x/feed".into(),
                retry_after: None,
                retry_after_hint: None,
            };
            assert_eq!(e.code(), "FEED_FETCH_FAILED", "status {status}");
            assert_eq!(
                e.to_error_obj(None).details["retry_after_seconds"],
                serde_json::Value::Null
            );
        }
    }

    #[test]
    fn rate_limited_surfaces_code_and_retry_after_details() {
        // Pins the AI-facing contract for the gate's fail-fast error (ADR-0016): stable code
        // plus machine-readable pacing hints in the free-form details.
        let e = RssError::RateLimited {
            url: "https://x/feed".into(),
            retry_after: Duration::from_secs(12),
        };
        let obj = e.to_error_obj(Some("https://x/feed"));
        assert_eq!(obj.code, "RATE_LIMITED");
        assert_eq!(obj.feed_url.as_deref(), Some("https://x/feed"));
        assert_eq!(obj.details["retry_after_seconds"], 12);
        assert_eq!(obj.details["retry_after_ms"], 12_000);
    }
}
