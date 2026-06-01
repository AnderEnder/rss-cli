//! `rss` binary entry point: parse args, set up logging, dispatch, map exit codes.
//!
//! stdout carries data only; all logs/diagnostics go to stderr.

use std::process::ExitCode;

use clap::Parser;

use rss_cli::cache::Cache;
use rss_cli::cli::{CacheAction, Cli, Command};
use rss_cli::config::FetchParams;
use rss_cli::core;
use rss_cli::error::{RssError, exit};
use rss_cli::model::ErrorObj;
use rss_cli::output;

#[tokio::main]
async fn main() -> ExitCode {
    let code = run().await;
    // Map our i32 exit codes onto process::ExitCode.
    ExitCode::from(code as u8)
}

async fn run() -> i32 {
    let cli = Cli::parse();
    init_tracing(cli.quiet, cli.verbose);
    let color = !cli.no_color && std::env::var_os("NO_COLOR").is_none();

    match &cli.command {
        Command::Fetch(args) => {
            let params = match args.to_params() {
                Ok(p) => p,
                Err(e) => return fail(&e),
            };
            let urls = match args.collect_urls() {
                Ok(u) => u,
                Err(e) => return fail(&e),
            };
            let cache = match Cache::open(cli.cache_dir.clone()) {
                Ok(c) => c,
                Err(e) => return fail(&e),
            };
            let mut out = core::fetch_feeds(&urls, &params, &cache).await;
            // Surface per-item content truncation (from --max-content-chars) at the top level.
            out.truncation = core::truncation_marker(&out, None, None);
            println!("{}", output::render_fetch(&out, args.format.into(), color));
            core::exit_code_for(&out)
        }

        Command::Discover(args) => {
            let params = FetchParams {
                user_agent: args
                    .user_agent
                    .clone()
                    .unwrap_or_else(|| rss_cli::config::DEFAULT_USER_AGENT.to_string()),
                timeout: std::time::Duration::from_secs(args.timeout),
                ..FetchParams::default()
            };
            match core::discover_feeds(&args.site_url, &params).await {
                Ok(out) => {
                    println!(
                        "{}",
                        output::render_discover(&out, args.format.into(), color)
                    );
                    exit::OK
                }
                Err(e) => fail(&e),
            }
        }

        Command::Show(args) => {
            let params = FetchParams {
                content_format: args.content.into(),
                max_content_chars: args.max_content_chars,
                ..FetchParams::default()
            };
            let cache = match Cache::open(cli.cache_dir.clone()) {
                Ok(c) => c,
                Err(e) => return fail(&e),
            };
            match core::show_item(&args.feed_url, &args.id, &params, &cache).await {
                Ok(Some(item)) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&item).unwrap_or_default()
                    );
                    exit::OK
                }
                Ok(None) => fail(&RssError::NotFound(format!(
                    "item '{}' not found in {}",
                    args.id, args.feed_url
                ))),
                Err(e) => fail(&e),
            }
        }

        Command::Schema(args) => {
            let schema = output::schema_for(&args.command);
            println!(
                "{}",
                serde_json::to_string_pretty(&schema).unwrap_or_default()
            );
            exit::OK
        }

        Command::Cache(args) => {
            let cache = match Cache::open(cli.cache_dir.clone()) {
                Ok(c) => c,
                Err(e) => return fail(&e),
            };
            match &args.action {
                CacheAction::Path => {
                    println!("{}", cache.dir().display());
                    exit::OK
                }
                CacheAction::List { format } => match cache.list() {
                    Ok(items) => {
                        let _ = format; // text rendering can be added by the cli agent
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&items).unwrap_or_default()
                        );
                        exit::OK
                    }
                    Err(e) => fail(&e),
                },
                CacheAction::Clear => match cache.clear() {
                    Ok(n) => {
                        tracing::info!("cleared {n} cached feed(s)");
                        exit::OK
                    }
                    Err(e) => fail(&e),
                },
            }
        }

        Command::Mcp => {
            let cache = match Cache::open(cli.cache_dir.clone()) {
                Ok(c) => c,
                Err(e) => return fail(&e),
            };
            match rss_cli::mcp::serve_stdio(cache).await {
                Ok(()) => exit::OK,
                Err(e) => fail(&e),
            }
        }
    }
}

/// Print a structured error to stderr and return the matching exit code.
fn fail(e: &RssError) -> i32 {
    let obj: ErrorObj = e.to_error_obj(None);
    eprintln!(
        "{}",
        serde_json::to_string(&obj).unwrap_or_else(|_| e.to_string())
    );
    match e {
        RssError::Usage(_) | RssError::InvalidUrl(_) => exit::USAGE,
        _ => exit::UNEXPECTED,
    }
}

/// Initialize stderr logging. Verbosity: `-v` = info, `-vv` = debug, `-vvv` = trace.
fn init_tracing(quiet: bool, verbose: u8) {
    use tracing_subscriber::{EnvFilter, fmt};

    if quiet {
        return;
    }
    let level = match verbose {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    let _ = fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .try_init();
}
