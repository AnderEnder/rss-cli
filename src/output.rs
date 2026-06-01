//! Rendering of results to json / ndjson / text, plus JSON Schema emission.
//!
//! [`OutputFormat`] and [`schema_for`] are foundation (frozen + implemented). The
//! `render_*` functions are **owned by the `cli` agent**.

use crate::model::{DiscoverOutput, FeedStatus, FetchOutput};

// --- Tiny ANSI styling helpers (no-ops when `color` is false) ---------------

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

fn bold(s: &str, color: bool) -> String {
    if color {
        format!("{BOLD}{s}{RESET}")
    } else {
        s.to_string()
    }
}

fn dim(s: &str, color: bool) -> String {
    if color {
        format!("{DIM}{s}{RESET}")
    } else {
        s.to_string()
    }
}

/// Stable, lowercase label for a feed status (mirrors the JSON `snake_case` form).
fn status_label(status: FeedStatus) -> &'static str {
    match status {
        FeedStatus::Ok => "ok",
        FeedStatus::NotModified => "not_modified",
        FeedStatus::Error => "error",
    }
}

/// Output format selected via `--format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// One pretty-printed JSON object (the full [`FetchOutput`] / [`DiscoverOutput`]).
    #[default]
    Json,
    /// Newline-delimited JSON: one `Item` per line (each carries `feed_url`).
    Ndjson,
    /// Human-readable text (the only format that may use color).
    Text,
}

/// The authoritative JSON Schema for a command's output (`rss schema --command <cmd>`).
///
/// `command` is `"fetch"` or `"discover"`. Returns the schema as a JSON value derived from
/// the `#[derive(JsonSchema)]` model types — this is the source of truth for the contract.
pub fn schema_for(command: &str) -> serde_json::Value {
    match command {
        "fetch" => serde_json::to_value(schemars::schema_for!(FetchOutput))
            .unwrap_or(serde_json::Value::Null),
        "discover" => serde_json::to_value(schemars::schema_for!(DiscoverOutput))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

/// Render a [`FetchOutput`] in the given format. **Owner: `cli` agent.**
///
/// - `Json`: `serde_json::to_string_pretty(out)`.
/// - `Ndjson`: one line per `Item` across all feeds; feed-level errors go to stderr (the
///   caller handles that) — this function returns only the stdout payload. When
///   `ndjson_records` is set, emit self-contained tagged records instead (see
///   [`render_fetch_ndjson_records`]).
/// - `Text`: a compact human summary; use `color` to decide on ANSI styling.
pub fn render_fetch(
    out: &FetchOutput,
    format: OutputFormat,
    color: bool,
    ndjson_records: bool,
) -> String {
    match format {
        OutputFormat::Json => serde_json::to_string_pretty(out).unwrap_or_default(),
        OutputFormat::Ndjson if ndjson_records => render_fetch_ndjson_records(out),
        OutputFormat::Ndjson => out
            .feeds
            .iter()
            .flat_map(|f| f.items.iter())
            .map(|item| serde_json::to_string(item).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n"),
        OutputFormat::Text => render_fetch_text(out, color),
    }
}

/// Self-contained NDJSON: every line is a record tagged with a `type` discriminator
/// (`"item"`, `"error"`, or a final `"summary"`), so a consumer reading only stdout never
/// loses feed-level errors or the aggregate counts (which the bare-item stream drops to
/// stderr). Opt-in via `--ndjson-records`; the default stream stays pure `Item` lines for
/// back-compatibility. See ADR-0012.
fn render_fetch_ndjson_records(out: &FetchOutput) -> String {
    let mut lines: Vec<String> = Vec::new();

    let tagged = |value: serde_json::Value, kind: &str| -> String {
        let mut value = value;
        if let Some(obj) = value.as_object_mut() {
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(kind.to_string()),
            );
        }
        value.to_string()
    };

    for item in out.feeds.iter().flat_map(|f| f.items.iter()) {
        if let Ok(v) = serde_json::to_value(item) {
            lines.push(tagged(v, "item"));
        }
    }
    for err in &out.errors {
        if let Ok(v) = serde_json::to_value(err) {
            lines.push(tagged(v, "error"));
        }
    }
    // A trailing summary record so the stream is self-describing about totals and bounding.
    let summary = serde_json::json!({
        "type": "summary",
        "schema_version": out.schema_version,
        "fetched_at": out.fetched_at,
        "feeds": out.feeds.len(),
        "errors": out.errors.len(),
        "total_items": out.total_items,
        "total_content_tokens_est": out.total_content_tokens_est,
        "warnings": out.warnings,
        "truncation": out.truncation,
    });
    lines.push(summary.to_string());

    lines.join("\n")
}

/// Concise, deterministic human summary of a [`FetchOutput`].
fn render_fetch_text(out: &FetchOutput, color: bool) -> String {
    let mut lines: Vec<String> = Vec::new();

    for feed in &out.feeds {
        // Header: title (or feed_url) · status · item count.
        let head = feed.title.as_deref().unwrap_or(&feed.feed_url);
        let cache = if feed.from_cache { " (cached)" } else { "" };
        lines.push(format!(
            "{}  [{}]{}  {} item(s)",
            bold(head, color),
            status_label(feed.status),
            cache,
            feed.items.len(),
        ));

        for item in &feed.items {
            let published = item.published.as_deref().unwrap_or("-");
            let title = item.title.as_deref().unwrap_or("-");
            let url = item.url.as_deref().unwrap_or("-");
            lines.push(format!(
                "  - {}  {}  {}",
                dim(published, color),
                title,
                dim(url, color),
            ));
        }

        if let Some(err) = &feed.error {
            lines.push(format!("  ! {}: {}", err.code, err.message));
        }
    }

    // Mirror feed-level errors as a trailing summary for quick scanning.
    if !out.errors.is_empty() {
        lines.push(bold(&format!("{} error(s):", out.errors.len()), color));
        for err in &out.errors {
            match &err.feed_url {
                Some(url) => lines.push(format!("  ! [{}] {}: {}", err.code, url, err.message)),
                None => lines.push(format!("  ! [{}] {}", err.code, err.message)),
            }
        }
    }

    lines.join("\n")
}

/// Render a [`DiscoverOutput`] in the given format. **Owner: `cli` agent.**
pub fn render_discover(out: &DiscoverOutput, format: OutputFormat, color: bool) -> String {
    match format {
        OutputFormat::Json => serde_json::to_string_pretty(out).unwrap_or_default(),
        OutputFormat::Ndjson => out
            .feeds
            .iter()
            .map(|feed| serde_json::to_string(feed).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n"),
        OutputFormat::Text => render_discover_text(out, color),
    }
}

/// Concise, deterministic human summary of a [`DiscoverOutput`].
fn render_discover_text(out: &DiscoverOutput, color: bool) -> String {
    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "{}  {} feed(s)",
        bold(&out.site_url, color),
        out.feeds.len(),
    ));
    for feed in &out.feeds {
        let title = feed.title.as_deref().unwrap_or("-");
        lines.push(format!(
            "  {}  [{}]  {}",
            feed.url,
            feed.feed_type,
            dim(title, color),
        ));
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContentFormat, DiscoveredFeed, FeedResult, FeedStatus, IdSource, Item};

    fn sample_item(id: &str, title: &str, url: &str) -> Item {
        Item {
            id: id.to_string(),
            id_source: IdSource::Link,
            feed_url: "https://example.com/feed.xml".to_string(),
            title: Some(title.to_string()),
            url: Some(url.to_string()),
            authors: vec!["Ada".to_string()],
            published: Some("2026-01-02T03:04:05Z".to_string()),
            updated: None,
            summary: Some("a summary".to_string()),
            content: Some("body".to_string()),
            content_format: ContentFormat::Markdown,
            content_tokens_est: 1,
            content_truncated: false,
            content_hash: Some("00112233aabbccdd".to_string()),
            categories: vec![],
            enclosures: vec![],
            guid: None,
        }
    }

    fn sample_output() -> FetchOutput {
        let mut out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        let items = vec![
            sample_item("1", "First Post", "https://example.com/1"),
            sample_item("2", "Second Post", "https://example.com/2"),
        ];
        let content_tokens_est_total = items.iter().map(|i| u64::from(i.content_tokens_est)).sum();
        out.feeds.push(FeedResult {
            feed_url: "https://example.com/feed.xml".to_string(),
            status: FeedStatus::Ok,
            from_cache: false,
            title: Some("Example Feed".to_string()),
            site_url: Some("https://example.com".to_string()),
            updated: None,
            item_count: items.len(),
            content_tokens_est_total,
            items,
            error: None,
        });
        out.total_items = out.feeds.iter().map(|f| f.items.len()).sum();
        out.total_content_tokens_est = out
            .feeds
            .iter()
            .flat_map(|f| &f.items)
            .map(|i| u64::from(i.content_tokens_est))
            .sum();
        out
    }

    #[test]
    fn fetch_json_roundtrips() {
        let out = sample_output();
        let json = render_fetch(&out, OutputFormat::Json, false, false);
        let parsed: FetchOutput = serde_json::from_str(&json).expect("valid json roundtrip");
        assert_eq!(parsed.feeds.len(), 1);
        assert_eq!(parsed.feeds[0].items.len(), 2);
        assert_eq!(parsed.total_items, 2);
    }

    #[test]
    fn fetch_ndjson_one_line_per_item() {
        let out = sample_output();
        let ndjson = render_fetch(&out, OutputFormat::Ndjson, false, false);
        assert_eq!(ndjson.lines().count(), 2);
        // Each line is independently parseable as an Item.
        for line in ndjson.lines() {
            let _: Item = serde_json::from_str(line).expect("each line is an Item");
        }
    }

    #[test]
    fn fetch_ndjson_records_are_tagged_and_self_contained() {
        use crate::model::ErrorObj;
        let mut out = sample_output();
        out.errors
            .push(ErrorObj::new("FEED_FETCH_FAILED", "boom").with_feed("https://bad.example/x"));

        let ndjson = render_fetch(&out, OutputFormat::Ndjson, false, true);
        let records: Vec<serde_json::Value> = ndjson
            .lines()
            .map(|l| serde_json::from_str(l).expect("each record line is JSON"))
            .collect();

        // 2 items + 1 error + 1 summary.
        assert_eq!(records.len(), 4);
        assert_eq!(records[0]["type"], "item");
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[2]["type"], "error");
        assert_eq!(records[2]["code"], "FEED_FETCH_FAILED");
        let summary = records.last().unwrap();
        assert_eq!(summary["type"], "summary");
        assert_eq!(summary["total_items"], 2);
        assert_eq!(summary["errors"], 1);
    }

    #[test]
    fn fetch_text_contains_titles() {
        let out = sample_output();
        let text = render_fetch(&out, OutputFormat::Text, false, false);
        assert!(text.contains("Example Feed"));
        assert!(text.contains("First Post"));
        assert!(text.contains("Second Post"));
        assert!(text.contains("2 item(s)"));
    }

    #[test]
    fn fetch_text_color_wraps_but_preserves_titles() {
        let out = sample_output();
        let text = render_fetch(&out, OutputFormat::Text, true, false);
        // The bare title is still present even with ANSI wrapping.
        assert!(text.contains("Example Feed"));
        assert!(text.contains(BOLD));
    }

    #[test]
    fn fetch_text_renders_errors() {
        use crate::model::ErrorObj;
        let mut out = FetchOutput::new("2026-06-01T00:00:00Z".to_string());
        let err = ErrorObj::new("FEED_FETCH_FAILED", "boom").with_feed("https://bad.example/x");
        out.feeds
            .push(FeedResult::error("https://bad.example/x", err.clone()));
        out.errors.push(err);

        let text = render_fetch(&out, OutputFormat::Text, false, false);
        assert!(text.contains("[error]")); // status label for FeedStatus::Error
        assert!(text.contains("FEED_FETCH_FAILED"));
        assert!(text.contains("boom"));
        assert!(text.contains("1 error(s):"));
    }

    #[test]
    fn discover_renders_all_formats() {
        let out = DiscoverOutput::new(
            "https://example.com",
            vec![
                DiscoveredFeed {
                    url: "https://example.com/feed.rss".to_string(),
                    feed_type: "rss".to_string(),
                    title: Some("RSS".to_string()),
                },
                DiscoveredFeed {
                    url: "https://example.com/atom.xml".to_string(),
                    feed_type: "atom".to_string(),
                    title: None,
                },
            ],
        );

        let json = render_discover(&out, OutputFormat::Json, false);
        let parsed: DiscoverOutput = serde_json::from_str(&json).expect("valid json roundtrip");
        assert_eq!(parsed.feeds.len(), 2);

        let ndjson = render_discover(&out, OutputFormat::Ndjson, false);
        assert_eq!(ndjson.lines().count(), 2);

        let text = render_discover(&out, OutputFormat::Text, false);
        assert!(text.contains("https://example.com/feed.rss"));
        assert!(text.contains("[atom]"));
    }
}
