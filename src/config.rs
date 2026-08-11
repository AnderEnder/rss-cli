//! Runtime parameters and policies — the *non-serialized* counterpart to [`crate::model`].

use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::error::RssError;
use crate::model::ContentFormat;

/// Default User-Agent. Polite, identifies the tool, points at the project.
pub const DEFAULT_USER_AGENT: &str = concat!(
    "rss-cli/",
    env!("CARGO_PKG_VERSION"),
    " (+https://github.com/AnderEnder/rss-cli)"
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
    /// Serve the cached entry **without any network call, regardless of age**, if one
    /// exists; only on a cache miss does it fetch (then behave like [`CachePolicy::Revalidate`]).
    /// Used by item lookup (`rss show` / MCP `get_item`) so a rolled feed window cannot evict
    /// an item the caller already saw. Exception to the always-revalidate default — see
    /// ADR-0014.
    CacheFirst,
    /// Ignore the cache entirely (do not read or write it).
    NoCache,
}

/// How cross-feed duplicates are handled for a fetch (ADR-0018).
///
/// Not a field of [`FetchParams`]: duplicate detection runs over the *assembled*
/// [`crate::model::FetchOutput`], after every feed has been fetched and parsed, so it is a
/// post-processing step ([`crate::core::apply_dedupe`]) rather than a per-feed parameter.
/// Both front-ends parse their argument with [`parse_dedupe`] and hand the result to that
/// one function, so `rss fetch --dedupe` and the MCP `dedupe` argument cannot diverge
/// (invariant 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DedupeMode {
    /// Default. Group duplicates into [`crate::model::FetchOutput::duplicates`] and change
    /// nothing else: `feeds[]`, request order, and per-feed `item_count` are untouched.
    #[default]
    Report,
    /// Skip detection entirely; `duplicates` stays `[]`. Indistinguishable in the output
    /// from a `Report` run that found nothing — deliberately, see ADR-0018.
    Off,
    /// Report *and* remove: keep the first (canonical) copy of each group and drop the rest,
    /// which changes per-feed `item_count` and the top-level totals. Opt-in only.
    Drop,
}

impl DedupeMode {
    /// The canonical argument spelling, and the one form callers may rely on.
    ///
    /// Load-bearing beyond display: the MCP cursor fingerprint hashes this string, so these
    /// exact bytes are part of every continuation token (ADR-0017 §4). `Report` must stay
    /// `"report"` — that is the placeholder value outstanding cursors were minted against.
    pub fn as_str(self) -> &'static str {
        match self {
            DedupeMode::Report => "report",
            DedupeMode::Off => "off",
            DedupeMode::Drop => "drop",
        }
    }
}

/// Parameters shared by the CLI and the MCP server for a fetch operation.
#[derive(Debug, Clone)]
pub struct FetchParams {
    pub content_format: ContentFormat,
    /// Maximum items per feed (most recent first), or `None` for all.
    pub limit: Option<usize>,
    /// Maximum characters of extracted `content` per item; longer bodies are truncated on a
    /// char boundary and flagged `content_truncated`. `None` keeps full content.
    pub max_content_chars: Option<usize>,
    /// Only include items published at or after this instant.
    pub since: Option<DateTime<Utc>>,
    /// Keyword filter applied to each item's title, summary, and content (see
    /// [`crate::query::Query`]). Space-separated terms are AND-ed; `"quoted phrases"` match
    /// as a unit; a leading `-term` excludes. Applied in [`crate::parse::parse_feed`] after
    /// `since` and before `limit`, so `limit` means "N matching items", not "N items, some
    /// of which match".
    ///
    /// It matches the item as it will be *returned*, so `content_format` and
    /// `max_content_chars` narrow what is searchable: a term that only appears past the
    /// truncation point, or anywhere in the body under `--content none`, will not match.
    pub query: Option<String>,
    /// Maximum number of feeds fetched concurrently.
    pub concurrency: usize,
    pub timeout: Duration,
    pub user_agent: String,
    pub cache_policy: CachePolicy,
    /// Wall-clock budget for a whole batch. Feeds not *started* by the deadline are left
    /// unattempted and reported via `truncation.feeds_omitted`, so a large same-host batch
    /// returns a usable partial page instead of outliving the caller's timeout. `None`
    /// (the CLI default) means no deadline.
    ///
    /// A third, independent bound — deliberately *not* folded into either rate-limit cap:
    /// `RETRY_MAX_DELAY` bounds one in-flight retry, `MAX_GATE_WAIT` bounds a sibling's pacing
    /// wait, and this bounds when a fetch may **start**.
    ///
    /// What it therefore does *not* bound is a fetch already admitted. The first `concurrency`
    /// futures are polled at once, all pass the check at t≈0, and then queue on the per-host
    /// permit, where no clock reaches them. So the real bound on a batch is the deadline **plus
    /// up to `concurrency - 1` already-admitted fetches** draining serially behind
    /// `HOST_MAX_CONCURRENCY`, each of which can additionally sit out a cooldown escalating
    /// toward `HOST_MAX_COOLDOWN` (and is itself capped by `timeout`). Permit contention is
    /// exactly what `MAX_GATE_WAIT` does not cover — and this does not cover it either; the
    /// deeper fix (a deadline threaded into the gate's permit acquire) is recorded as a known
    /// limitation in ADR-0017.
    pub deadline: Option<Duration>,
}

impl Default for FetchParams {
    fn default() -> Self {
        Self {
            content_format: ContentFormat::Markdown,
            limit: None,
            max_content_chars: None,
            since: None,
            query: None,
            concurrency: 8,
            timeout: Duration::from_secs(30),
            user_agent: DEFAULT_USER_AGENT.to_string(),
            cache_policy: CachePolicy::Revalidate,
            deadline: None,
        }
    }
}

/// Parse a `--since` / `since` value: a relative duration (`2h`, `7d`) or an ISO-8601
/// instant. Shared by the CLI and the MCP server so both front-ends accept the same forms.
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
        "invalid since value '{s}' (use e.g. '2h', '7d', or '2026-06-01')"
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

/// Parse an MCP `cache_policy` argument into a [`CachePolicy`].
///
/// A single string rather than a `no_cache: bool` + `max_age: String` pair: clients that
/// stringify integers stringify booleans too, so `"false"` would fail to deserialize as a
/// bare `Option<bool>` exactly as `"25"` failed as a bare `Option<usize>`.
pub fn parse_cache_policy(s: &str) -> Result<CachePolicy, RssError> {
    let s = s.trim().to_ascii_lowercase();
    match s.as_str() {
        "" | "revalidate" => Ok(CachePolicy::Revalidate),
        "no-cache" => Ok(CachePolicy::NoCache),
        "cache-first" => Ok(CachePolicy::CacheFirst),
        other => match other.strip_prefix("max-age:") {
            Some(dur) if !dur.trim().is_empty() => Ok(CachePolicy::MaxAge(parse_duration(dur)?)),
            _ => Err(RssError::Usage(format!(
                "invalid cache_policy '{s}' (expected revalidate | no-cache | cache-first | \
                 max-age:<duration>, e.g. max-age:15m)"
            ))),
        },
    }
}

/// Parse a `--dedupe` / `dedupe` argument into a [`DedupeMode`].
///
/// Shared by both front-ends for the same reason [`parse_cache_policy`] is: the grammar and
/// its error message are part of the tool surface, and two copies would drift. Blank input is
/// the default rather than an error, so a client that fills every advertised optional string
/// with `""` still gets a working call (the same liberality `parse_cache_policy` grants).
pub fn parse_dedupe(s: &str) -> Result<DedupeMode, RssError> {
    let s = s.trim().to_ascii_lowercase();
    match s.as_str() {
        "" | "report" => Ok(DedupeMode::Report),
        "off" => Ok(DedupeMode::Off),
        "drop" => Ok(DedupeMode::Drop),
        other => Err(RssError::Usage(format!(
            "invalid dedupe '{other}' (expected report | off | drop)"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_handles_every_unit() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604_800));
        assert_eq!(parse_duration("1w").unwrap(), Duration::from_secs(604_800));
        assert!(parse_duration("2y").is_err(), "unknown unit must error");
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn parse_cache_policy_covers_the_documented_grammar() {
        assert_eq!(
            parse_cache_policy("revalidate").unwrap(),
            CachePolicy::Revalidate
        );
        assert_eq!(
            parse_cache_policy("no-cache").unwrap(),
            CachePolicy::NoCache
        );
        assert_eq!(
            parse_cache_policy("cache-first").unwrap(),
            CachePolicy::CacheFirst
        );
        assert_eq!(
            parse_cache_policy("max-age:15m").unwrap(),
            CachePolicy::MaxAge(Duration::from_secs(900))
        );
        // Case- and whitespace-insensitive, since clients vary.
        assert_eq!(
            parse_cache_policy("  No-Cache ").unwrap(),
            CachePolicy::NoCache
        );

        // An unknown form must name the accepted ones, so an agent can self-correct.
        let err = parse_cache_policy("aggressive").unwrap_err().to_string();
        assert!(
            err.contains("no-cache"),
            "error should list valid forms: {err}"
        );
        assert!(parse_cache_policy("max-age:").is_err());
        assert!(parse_cache_policy("max-age:2y").is_err());
    }

    #[test]
    fn parse_dedupe_covers_the_documented_grammar() {
        assert_eq!(parse_dedupe("report").unwrap(), DedupeMode::Report);
        assert_eq!(parse_dedupe("off").unwrap(), DedupeMode::Off);
        assert_eq!(parse_dedupe("drop").unwrap(), DedupeMode::Drop);
        // Absent / blank means the default, exactly as `parse_cache_policy` treats `""`.
        assert_eq!(parse_dedupe("").unwrap(), DedupeMode::Report);
        assert_eq!(parse_dedupe("   ").unwrap(), DedupeMode::Report);
        assert_eq!(
            parse_dedupe(&String::default()).unwrap(),
            DedupeMode::default()
        );
        // Case- and whitespace-insensitive, since clients vary.
        assert_eq!(parse_dedupe("  DROP ").unwrap(), DedupeMode::Drop);

        // An unknown mode must name every accepted one, so an agent can self-correct.
        let err = parse_dedupe("maybe").unwrap_err().to_string();
        for mode in ["report", "off", "drop"] {
            assert!(err.contains(mode), "error should list '{mode}': {err}");
        }
        assert!(
            err.contains("maybe"),
            "error should echo what was rejected: {err}"
        );
    }

    #[test]
    fn dedupe_mode_as_str_round_trips_through_parse_dedupe() {
        // The MCP cursor fingerprint hashes `as_str()`, so a drift between the two spellings
        // would mint cursors that no later call can reproduce.
        for mode in [DedupeMode::Report, DedupeMode::Off, DedupeMode::Drop] {
            assert_eq!(parse_dedupe(mode.as_str()).unwrap(), mode);
        }
        // Pinned literally: outstanding cursors were minted against these exact bytes.
        assert_eq!(DedupeMode::default().as_str(), "report");
    }

    #[test]
    fn parse_since_accepts_duration_rfc3339_and_bare_date() {
        // A relative duration resolves to a past instant.
        assert!(parse_since("2h").unwrap() < Utc::now());
        // RFC-3339 round-trips exactly.
        let dt = parse_since("2026-06-01T12:00:00Z").unwrap();
        assert_eq!(
            dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "2026-06-01T12:00:00Z"
        );
        // A bare date is midnight UTC.
        let d = parse_since("2026-06-01").unwrap();
        assert_eq!(
            d.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "2026-06-01T00:00:00Z"
        );
        assert!(parse_since("not-a-date").is_err());
    }
}
