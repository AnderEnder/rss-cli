//! Runtime parameters and policies — the *non-serialized* counterpart to [`crate::model`].

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::model::ContentFormat;

/// Default User-Agent. Polite, identifies the tool, points at the project.
pub const DEFAULT_USER_AGENT: &str = concat!(
    "rss-cli/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/)"
);

/// How the cache should be consulted for a fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CachePolicy {
    /// Default. Always revalidate with a conditional GET (`If-None-Match` /
    /// `If-Modified-Since`); a `304` serves the cached body.
    #[default]
    Revalidate,
    /// Serve directly from cache without hitting the network if the cached entry is
    /// younger than this duration; otherwise behave like [`CachePolicy::Revalidate`].
    MaxAge(Duration),
    /// Ignore the cache entirely (do not read or write it).
    NoCache,
}

/// Parameters shared by the CLI and the MCP server for a fetch operation.
#[derive(Debug, Clone)]
pub struct FetchParams {
    pub content_format: ContentFormat,
    /// Maximum items per feed (most recent first), or `None` for all.
    pub limit: Option<usize>,
    /// Only include items published at or after this instant.
    pub since: Option<DateTime<Utc>>,
    /// Maximum number of feeds fetched concurrently.
    pub concurrency: usize,
    pub timeout: Duration,
    pub user_agent: String,
    pub cache_policy: CachePolicy,
}

impl Default for FetchParams {
    fn default() -> Self {
        Self {
            content_format: ContentFormat::Markdown,
            limit: None,
            since: None,
            concurrency: 8,
            timeout: Duration::from_secs(30),
            user_agent: DEFAULT_USER_AGENT.to_string(),
            cache_policy: CachePolicy::Revalidate,
        }
    }
}
