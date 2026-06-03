//! Error type, stable error codes, and process exit codes.

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
    },

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
            RssError::Http { .. } => "FEED_FETCH_FAILED",
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
                ..
            } => {
                obj.details = serde_json::json!({
                    "http_status": status,
                    "retry_after": retry_after,
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
            status: 429,
            url: "https://x".into(),
            retry_after: Some("2".into()),
        };
        let obj = e.to_error_obj(None);
        assert_eq!(obj.code, "FEED_FETCH_FAILED");
        assert_eq!(obj.details["http_status"], 429);
        assert_eq!(obj.details["retry_after"], "2");
    }
}
