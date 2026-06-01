//! `rss-cli` — an AI-friendly RSS/Atom feed CLI library.
//!
//! The crate is split into small modules with **frozen public interfaces** so that
//! independent implementation work can proceed in parallel without colliding:
//!
//! - [`model`]   — serialized output types (the AI-facing API contract).
//! - [`config`]  — runtime parameters that are *not* serialized (params, policies).
//! - [`error`]   — error type, stable error codes, and process exit codes.
//! - [`cache`]   — atomic file-based HTTP cache (conditional GET + `show`/`get_item`).
//! - [`fetch`]   — HTTP client with conditional GET.
//! - [`parse`]   — `feed-rs` → [`model`] conversion, date/URL normalization.
//! - [`identity`]— deterministic, cache-independent stable item IDs (the keystone).
//! - [`content`] — HTML → markdown/text extraction + token estimation.
//! - [`discover`]— feed autodiscovery from a website URL.
//! - [`output`]  — json/ndjson/text rendering + JSON Schema emission.
//! - [`core`]    — orchestration that the CLI and the MCP server both call.
//! - [`mcp`]     — Model Context Protocol server (stdio transport).

pub mod cache;
pub mod cli;
pub mod config;
pub mod content;
pub mod core;
pub mod discover;
pub mod error;
pub mod fetch;
pub mod identity;
pub mod mcp;
pub mod model;
pub mod output;
pub mod parse;

pub use error::{RssError, exit};
pub use model::{
    DiscoverOutput, DiscoveredFeed, Enclosure, ErrorObj, FeedResult, FeedStatus, FetchOutput,
    IdSource, Item, SCHEMA_VERSION,
};
