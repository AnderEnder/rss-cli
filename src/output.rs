//! Rendering of results to json / ndjson / text, plus JSON Schema emission.
//!
//! [`OutputFormat`] and [`schema_for`] are foundation (frozen + implemented). The
//! `render_*` functions are **owned by the `cli` agent**.

use crate::model::{DiscoverOutput, FetchOutput};

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
///   caller handles that) — this function returns only the stdout payload.
/// - `Text`: a compact human summary; use `color` to decide on ANSI styling.
pub fn render_fetch(out: &FetchOutput, format: OutputFormat, color: bool) -> String {
    let _ = (out, format, color);
    todo!("cli: implement fetch rendering (json/ndjson/text)")
}

/// Render a [`DiscoverOutput`] in the given format. **Owner: `cli` agent.**
pub fn render_discover(out: &DiscoverOutput, format: OutputFormat, color: bool) -> String {
    let _ = (out, format, color);
    todo!("cli: implement discover rendering (json/ndjson/text)")
}
