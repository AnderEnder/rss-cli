//! Feed autodiscovery from a website homepage. **Owner: `cli` agent.**
//!
//! ## Requirements
//! - GET the `site_url` HTML via [`HttpClient::get_bytes`].
//! - Parse the `<head>` for `<link rel="alternate" type="application/rss+xml">` and
//!   `type="application/atom+xml"` (also accept `application/json` / `feed+json`). Use the
//!   lightweight `tl` HTML parser — do not pull a heavyweight DOM stack.
//! - Resolve each `href` to an absolute URL against `site_url` (the `url` crate).
//! - Map the `type` to `feed_type` (`"rss" | "atom" | "json"`), carry the `title` attr.
//! - If the page *is itself* a feed (content sniffing optional), or if no `<link>` tags are
//!   found, return an empty list rather than erroring (the CLI decides exit behavior).

use crate::error::RssError;
use crate::fetch::HttpClient;
use crate::model::DiscoverOutput;

/// Discover feeds advertised on `site_url`. **Owner: `cli` agent.**
pub async fn discover(site_url: &str, http: &HttpClient) -> Result<DiscoverOutput, RssError> {
    let _ = (site_url, http);
    todo!("cli: implement <link rel=alternate> autodiscovery with `tl`")
}
