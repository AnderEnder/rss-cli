//! Atomic, file-based HTTP cache.
//!
//! Lightweight by design: no database engine, no C dependency. Each feed is stored as a
//! pair of files under the cache dir, keyed by `sha256(feed_url)`:
//!
//! - `<hash>.json` — [`CacheMeta`] (validators + timestamps + content type).
//! - `<hash>.body` — the raw response body bytes.
//!
//! Both are written atomically (temp file + rename) so a crash can never leave a torn
//! entry. The cache exists for (a) conditional GET and (b) resolving `show` / `get_item`
//! lookups — it is **not** responsible for item-ID stability (IDs are deterministic; see
//! [`crate::identity`]).

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::RssError;

type Result<T> = std::result::Result<T, RssError>;

/// Cached HTTP metadata for a feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    pub feed_url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// RFC-3339 UTC time the entry was written.
    pub fetched_at: String,
    pub content_type: Option<String>,
}

/// A full cache entry: metadata plus the raw body bytes.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub meta: CacheMeta,
    pub body: Vec<u8>,
}

/// Summary of a cache entry for `rss cache list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheListItem {
    pub feed_url: String,
    pub fetched_at: String,
    pub size_bytes: u64,
    pub etag: Option<String>,
}

/// Handle to the on-disk cache directory.
#[derive(Debug, Clone)]
pub struct Cache {
    root: PathBuf,
}

impl Cache {
    /// Open (creating if needed) the cache. If `dir` is `None`, use the OS cache dir
    /// (`~/Library/Caches/...`, `$XDG_CACHE_HOME/...`, etc.) via the `directories` crate.
    pub fn open(dir: Option<PathBuf>) -> Result<Self> {
        let root = match dir {
            Some(d) => d,
            None => directories::ProjectDirs::from("io", "rss-cli", "rss-cli")
                .map(|p| p.cache_dir().to_path_buf())
                .ok_or_else(|| RssError::Cache("could not determine OS cache dir".into()))?,
        };
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// The cache directory path.
    pub fn dir(&self) -> &Path {
        &self.root
    }

    fn key(feed_url: &str) -> String {
        let mut h = Sha256::new();
        h.update(feed_url.as_bytes());
        h.finalize().iter().map(|b| format!("{b:02x}")).collect()
    }

    fn meta_path(&self, feed_url: &str) -> PathBuf {
        self.root.join(format!("{}.json", Self::key(feed_url)))
    }

    fn body_path(&self, feed_url: &str) -> PathBuf {
        self.root.join(format!("{}.body", Self::key(feed_url)))
    }

    /// Read a cache entry for `feed_url`, if present and intact.
    pub fn get(&self, feed_url: &str) -> Result<Option<CacheEntry>> {
        let meta_path = self.meta_path(feed_url);
        let body_path = self.body_path(feed_url);
        if !meta_path.exists() || !body_path.exists() {
            return Ok(None);
        }
        let meta_bytes = fs::read(&meta_path)?;
        let meta: CacheMeta = serde_json::from_slice(&meta_bytes)
            .map_err(|e| RssError::Cache(format!("corrupt cache metadata: {e}")))?;
        let body = fs::read(&body_path)?;
        Ok(Some(CacheEntry { meta, body }))
    }

    /// Atomically write a cache entry.
    pub fn put(&self, meta: &CacheMeta, body: &[u8]) -> Result<()> {
        atomic_write(&self.body_path(&meta.feed_url), body)?;
        let meta_bytes = serde_json::to_vec_pretty(meta)
            .map_err(|e| RssError::Cache(format!("serialize cache metadata: {e}")))?;
        atomic_write(&self.meta_path(&meta.feed_url), &meta_bytes)?;
        Ok(())
    }

    /// Remove all cache entries, returning the number of feeds removed.
    pub fn clear(&self) -> Result<usize> {
        let mut removed = 0;
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "json") {
                removed += 1;
            }
            if path.is_file() {
                fs::remove_file(&path)?;
            }
        }
        Ok(removed)
    }

    /// List cached feeds.
    pub fn list(&self) -> Result<Vec<CacheListItem>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if !path.extension().is_some_and(|e| e == "json") {
                continue;
            }
            let meta_bytes = match fs::read(&path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            let Ok(meta) = serde_json::from_slice::<CacheMeta>(&meta_bytes) else {
                continue;
            };
            let size = path
                .with_extension("body")
                .metadata()
                .map(|m| m.len())
                .unwrap_or(0);
            out.push(CacheListItem {
                feed_url: meta.feed_url,
                fetched_at: meta.fetched_at,
                size_bytes: size,
                etag: meta.etag,
            });
        }
        Ok(out)
    }
}

/// Write `bytes` to `path` atomically (temp file in the same dir, then rename).
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let tmp = path.with_extension(format!(
        "tmp.{}",
        std::process::id() as u64 ^ rand_suffix()
    ));
    fs::write(&tmp, bytes)?;
    // Rename is atomic within the same filesystem; temp lives in the same dir.
    match fs::rename(&tmp, path) {
        Ok(()) => Ok(()),
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            let _ = dir; // keep `dir` referenced for clarity even if rename is enough.
            Err(RssError::Io(e))
        }
    }
}

/// Cheap, dependency-free per-call suffix to avoid temp-file collisions.
fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
