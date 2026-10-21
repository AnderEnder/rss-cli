//! Atomic, file-based HTTP cache.
//!
//! Each `<sha256(feed_url)>.entry` stores one compact JSON metadata line followed by the
//! raw body. One rename publishes both together; legacy `.json`/`.body` pairs remain readable.
//! The cache supports conditional GET and item lookup, never item identity.

use std::collections::HashSet;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::RssError;

type Result<T> = std::result::Result<T, RssError>;
const MAX_META_BYTES: usize = 64 * 1024;

/// Cached HTTP metadata for a feed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    pub feed_url: String,
    /// Resolution base for the cached body after redirects.
    #[serde(default)]
    pub final_url: Option<String>,
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

    fn entry_path(&self, feed_url: &str) -> PathBuf {
        self.root.join(format!("{}.entry", Self::key(feed_url)))
    }

    /// Read a complete snapshot, falling back to the legacy pair on a miss.
    pub fn get(&self, feed_url: &str) -> Result<Option<CacheEntry>> {
        let path = self.entry_path(feed_url);
        let entry = match read_bounded(&path, crate::config::MAX_BODY_BYTES + MAX_META_BYTES) {
            Ok(bytes) => decode_entry(&bytes)?,
            Err(RssError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                let meta = match read_bounded(&self.meta_path(feed_url), MAX_META_BYTES) {
                    Ok(bytes) => serde_json::from_slice::<CacheMeta>(&bytes)
                        .map_err(|e| RssError::Cache(format!("corrupt cache metadata: {e}")))?,
                    Err(RssError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                };
                let body =
                    match read_bounded(&self.body_path(feed_url), crate::config::MAX_BODY_BYTES) {
                        Ok(body) => body,
                        Err(RssError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                            return Ok(None);
                        }
                        Err(e) => return Err(e),
                    };
                CacheEntry { meta, body }
            }
            Err(e) => return Err(e),
        };
        if entry.meta.feed_url != feed_url {
            return Err(RssError::Cache(
                "cache metadata URL does not match its key".into(),
            ));
        }
        Ok(Some(entry))
    }

    /// Atomically publish metadata and body in one record.
    pub fn put(&self, meta: &CacheMeta, body: &[u8]) -> Result<()> {
        if body.len() > crate::config::MAX_BODY_BYTES {
            return Err(RssError::BodyTooLarge {
                limit_bytes: crate::config::MAX_BODY_BYTES,
            });
        }
        let mut bytes = serde_json::to_vec(meta)
            .map_err(|e| RssError::Cache(format!("serialize cache metadata: {e}")))?;
        if bytes.len() >= MAX_META_BYTES {
            return Err(RssError::Cache("cache metadata exceeds 64 KiB".into()));
        }
        bytes.push(b'\n');
        bytes.extend_from_slice(body);
        atomic_write(&self.entry_path(&meta.feed_url), &bytes)
    }

    /// Remove only recognized cache records, returning the number of feed keys removed.
    pub fn clear(&self) -> Result<usize> {
        let mut removed = HashSet::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if owned_path(&path) && entry.file_type()?.is_file() {
                match fs::remove_file(&path) {
                    Ok(()) => {
                        removed.insert(path.file_stem().unwrap().to_owned());
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(removed.len())
    }

    /// List each feed once, preferring the atomic record over legacy metadata.
    pub fn list(&self) -> Result<Vec<CacheListItem>> {
        let mut urls = HashSet::new();
        for entry in fs::read_dir(&self.root)? {
            let path = entry?.path();
            if !owned_path(&path) {
                continue;
            }
            let metadata = match path.extension().and_then(|e| e.to_str()) {
                Some("entry") => {
                    read_bounded(&path, crate::config::MAX_BODY_BYTES + MAX_META_BYTES)
                        .ok()
                        .and_then(|bytes| decode_entry(&bytes).ok())
                        .map(|entry| entry.meta)
                }
                Some("json") => read_bounded(&path, MAX_META_BYTES)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<CacheMeta>(&bytes).ok()),
                _ => None,
            };
            if let Some(meta) = metadata
                && path.file_stem().and_then(|s| s.to_str())
                    == Some(Self::key(&meta.feed_url).as_str())
            {
                urls.insert(meta.feed_url);
            }
        }
        let mut out = Vec::new();
        for url in urls {
            if let Ok(Some(entry)) = self.get(&url) {
                out.push(CacheListItem {
                    feed_url: entry.meta.feed_url,
                    fetched_at: entry.meta.fetched_at,
                    size_bytes: entry.body.len() as u64,
                    etag: entry.meta.etag,
                });
            }
        }
        out.sort_by(|a, b| a.feed_url.cmp(&b.feed_url));
        Ok(out)
    }
}

fn owned_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("entry" | "json" | "body")
    ) && path.file_stem().and_then(|s| s.to_str()).is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(RssError::BodyTooLarge {
            limit_bytes: crate::config::MAX_BODY_BYTES,
        });
    }
    Ok(bytes)
}

fn decode_entry(bytes: &[u8]) -> Result<CacheEntry> {
    let split = bytes
        .iter()
        .position(|b| *b == b'\n')
        .ok_or_else(|| RssError::Cache("incomplete cache record".into()))?;
    let body = &bytes[split + 1..];
    if split >= MAX_META_BYTES || body.len() > crate::config::MAX_BODY_BYTES {
        return Err(RssError::BodyTooLarge {
            limit_bytes: crate::config::MAX_BODY_BYTES,
        });
    }
    let meta = serde_json::from_slice(&bytes[..split])
        .map_err(|e| RssError::Cache(format!("corrupt cache metadata: {e}")))?;
    Ok(CacheEntry {
        meta,
        body: body.to_vec(),
    })
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let (tmp, mut file) = loop {
        let tmp = path.with_extension(format!(
            "entry.tmp.{}.{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    };
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result.map_err(RssError::Io)
}
