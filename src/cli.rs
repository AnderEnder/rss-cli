//! Command-line surface (frozen) + mechanical arg→params conversion.
//!
//! The clap structs here define the stable command surface. Rendering and dispatch
//! behavior is wired in [`crate::main`] / [`crate::output`] (owned by the `cli` agent),
//! but the flags, defaults, and the arg→[`FetchParams`] mapping below are foundation.

use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::config::{CachePolicy, DEFAULT_USER_AGENT, FetchParams};
use crate::error::RssError;
use crate::model::ContentFormat;
use crate::output::OutputFormat;

/// An AI-friendly RSS/Atom feed CLI.
#[derive(Debug, Parser)]
#[command(name = "rss", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Override the cache directory.
    #[arg(long, global = true, value_name = "DIR")]
    pub cache_dir: Option<PathBuf>,

    /// Suppress all non-data output on stderr.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Increase logging verbosity (repeatable). Logs go to stderr.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Disable ANSI color in text output (also respects the `NO_COLOR` env var).
    #[arg(long, global = true)]
    pub no_color: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Fetch and parse one or more feeds, emitting structured items.
    Fetch(FetchArgs),
    /// Discover feeds advertised on a website homepage.
    Discover(DiscoverArgs),
    /// Show the full content of a single item by its stable id.
    Show(ShowArgs),
    /// Emit the JSON Schema of a command's output.
    Schema(SchemaArgs),
    /// Inspect or clear the local cache.
    Cache(CacheArgs),
    /// Run as a Model Context Protocol server (stdio transport).
    Mcp,
}

/// Output format, as selected on the command line.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum FormatArg {
    #[default]
    Json,
    Ndjson,
    Text,
}

impl From<FormatArg> for OutputFormat {
    fn from(f: FormatArg) -> Self {
        match f {
            FormatArg::Json => OutputFormat::Json,
            FormatArg::Ndjson => OutputFormat::Ndjson,
            FormatArg::Text => OutputFormat::Text,
        }
    }
}

/// Content extraction format, as selected on the command line.
#[derive(Debug, Clone, Copy, Default, ValueEnum)]
pub enum ContentArg {
    #[default]
    Markdown,
    Text,
    Html,
    None,
}

impl From<ContentArg> for ContentFormat {
    fn from(c: ContentArg) -> Self {
        match c {
            ContentArg::Markdown => ContentFormat::Markdown,
            ContentArg::Text => ContentFormat::Text,
            ContentArg::Html => ContentFormat::Html,
            ContentArg::None => ContentFormat::None,
        }
    }
}

#[derive(Debug, Args)]
pub struct FetchArgs {
    /// Feed URLs to fetch. Use `-` to read URLs from stdin (one per line).
    #[arg(value_name = "URL")]
    pub urls: Vec<String>,

    /// Read feed URLs from an OPML file (uses each outline's `xmlUrl`).
    #[arg(long, value_name = "FILE")]
    pub opml: Option<PathBuf>,

    /// Read feed URLs from a text file (one per line; `#` comments allowed).
    #[arg(long, value_name = "FILE")]
    pub input: Option<PathBuf>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = FormatArg::Json)]
    pub format: FormatArg,

    /// Content extraction format for item bodies.
    #[arg(long, value_enum, default_value_t = ContentArg::Markdown)]
    pub content: ContentArg,

    /// Maximum items per feed (newest first).
    #[arg(long, value_name = "N")]
    pub limit: Option<usize>,

    /// Truncate each item body to at most this many characters (flagged `content_truncated`
    /// in the output). Useful to fetch many items while skipping giant bodies.
    #[arg(long, value_name = "N")]
    pub max_content_chars: Option<usize>,

    /// Only include items at/after this time: a duration (e.g. `2h`, `7d`) or an
    /// ISO-8601 date/datetime.
    #[arg(long, value_name = "WHEN")]
    pub since: Option<String>,

    /// Max feeds fetched concurrently.
    #[arg(long, default_value_t = 8, value_name = "N")]
    pub concurrency: usize,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 30, value_name = "SECS")]
    pub timeout: u64,

    /// Bypass the cache entirely (no read, no write).
    #[arg(long)]
    pub no_cache: bool,

    /// Serve from cache without revalidating if the entry is younger than this
    /// duration (e.g. `15m`, `1h`).
    #[arg(long, value_name = "DUR", conflicts_with_all = ["no_cache", "refresh"])]
    pub max_age: Option<String>,

    /// Force revalidation, ignoring `--max-age`.
    #[arg(long, conflicts_with = "no_cache")]
    pub refresh: bool,

    /// Override the User-Agent header.
    #[arg(long, value_name = "STRING")]
    pub user_agent: Option<String>,
}

impl FetchArgs {
    /// Resolve the effective cache policy from the cache-related flags.
    pub fn cache_policy(&self) -> Result<CachePolicy, RssError> {
        if self.no_cache {
            Ok(CachePolicy::NoCache)
        } else if let Some(ma) = &self.max_age {
            Ok(CachePolicy::MaxAge(parse_duration(ma)?))
        } else {
            // `--refresh` and the default both revalidate.
            Ok(CachePolicy::Revalidate)
        }
    }

    /// Build the [`FetchParams`] this invocation should use.
    pub fn to_params(&self) -> Result<FetchParams, RssError> {
        Ok(FetchParams {
            content_format: self.content.into(),
            limit: self.limit,
            max_content_chars: self.max_content_chars,
            since: self.since.as_deref().map(parse_since).transpose()?,
            concurrency: self.concurrency.max(1),
            timeout: Duration::from_secs(self.timeout),
            user_agent: self
                .user_agent
                .clone()
                .unwrap_or_else(|| DEFAULT_USER_AGENT.to_string()),
            cache_policy: self.cache_policy()?,
        })
    }

    /// Gather feed URLs from all input sources (positional args, `--opml`, `--input`,
    /// and stdin via `-`), de-duplicated while preserving order.
    pub fn collect_urls(&self) -> Result<Vec<String>, RssError> {
        let mut urls: Vec<String> = Vec::new();
        let push = |u: String, urls: &mut Vec<String>| {
            let u = u.trim().to_string();
            if !u.is_empty() && !u.starts_with('#') && !urls.contains(&u) {
                urls.push(u);
            }
        };

        for u in &self.urls {
            if u == "-" {
                let mut buf = String::new();
                std::io::stdin().read_to_string(&mut buf)?;
                for line in buf.lines() {
                    push(line.to_string(), &mut urls);
                }
            } else {
                push(u.clone(), &mut urls);
            }
        }

        if let Some(path) = &self.input {
            let text = std::fs::read_to_string(path)?;
            for line in text.lines() {
                push(line.to_string(), &mut urls);
            }
        }

        if let Some(path) = &self.opml {
            let text = std::fs::read_to_string(path)?;
            let doc = opml::OPML::from_str(&text)
                .map_err(|e| RssError::Usage(format!("invalid OPML: {e}")))?;
            collect_opml_urls(&doc.body.outlines, &mut |u| push(u, &mut urls));
        }

        if urls.is_empty() {
            return Err(RssError::Usage(
                "no feed URLs provided (pass URLs, --input, --opml, or `-` for stdin)".into(),
            ));
        }
        Ok(urls)
    }
}

fn collect_opml_urls(outlines: &[opml::Outline], push: &mut impl FnMut(String)) {
    for o in outlines {
        if let Some(url) = &o.xml_url {
            push(url.clone());
        }
        collect_opml_urls(&o.outlines, push);
    }
}

#[derive(Debug, Args)]
pub struct DiscoverArgs {
    /// Website homepage URL to scan for feed `<link>` tags.
    #[arg(value_name = "SITE_URL")]
    pub site_url: String,

    /// Output format.
    #[arg(long, value_enum, default_value_t = FormatArg::Json)]
    pub format: FormatArg,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 30, value_name = "SECS")]
    pub timeout: u64,

    /// Override the User-Agent header.
    #[arg(long, value_name = "STRING")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Args)]
pub struct ShowArgs {
    /// Feed URL containing the item.
    #[arg(value_name = "FEED_URL")]
    pub feed_url: String,

    /// Stable item id (from a prior `fetch`).
    #[arg(long, value_name = "ITEM_ID")]
    pub id: String,

    /// Content extraction format.
    #[arg(long, value_enum, default_value_t = ContentArg::Markdown)]
    pub content: ContentArg,

    /// Truncate the item body to at most this many characters (flagged `content_truncated`).
    #[arg(long, value_name = "N")]
    pub max_content_chars: Option<usize>,

    /// Output format.
    #[arg(long, value_enum, default_value_t = FormatArg::Json)]
    pub format: FormatArg,
}

#[derive(Debug, Args)]
pub struct SchemaArgs {
    /// Which command's output schema to emit.
    #[arg(long, default_value = "fetch", value_parser = ["fetch", "discover"])]
    pub command: String,
}

#[derive(Debug, Args)]
pub struct CacheArgs {
    #[command(subcommand)]
    pub action: CacheAction,
}

#[derive(Debug, Subcommand)]
pub enum CacheAction {
    /// Print the cache directory path.
    Path,
    /// List cached feeds.
    List {
        /// Output format.
        #[arg(long, value_enum, default_value_t = FormatArg::Json)]
        format: FormatArg,
    },
    /// Remove all cache entries.
    Clear,
}

/// Parse a `--since` value: a relative duration (`2h`, `7d`) or an ISO-8601 instant.
pub fn parse_since(s: &str) -> Result<DateTime<Utc>, RssError> {
    let s = s.trim();
    // Try a relative duration first.
    if let Ok(d) = parse_duration(s) {
        let d = chrono::Duration::from_std(d)
            .map_err(|e| RssError::Usage(format!("duration too large: {e}")))?;
        return Ok(Utc::now() - d);
    }
    // Full RFC-3339 datetime.
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    // Bare date (assume midnight UTC).
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(dt) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(DateTime::from_naive_utc_and_offset(dt, Utc));
    }
    Err(RssError::Usage(format!(
        "invalid --since value '{s}' (use e.g. '2h', '7d', or '2026-06-01')"
    )))
}

/// Parse a simple duration like `30s`, `15m`, `2h`, `7d`, `1w`.
pub fn parse_duration(s: &str) -> Result<Duration, RssError> {
    let s = s.trim();
    let (num, unit) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit())
            .ok_or_else(|| RssError::Usage(format!("invalid duration '{s}'")))?,
    );
    let n: u64 = num
        .parse()
        .map_err(|_| RssError::Usage(format!("invalid duration '{s}'")))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        "w" => n * 604800,
        other => return Err(RssError::Usage(format!("unknown duration unit '{other}'"))),
    };
    Ok(Duration::from_secs(secs))
}
