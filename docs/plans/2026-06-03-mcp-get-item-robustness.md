# MCP `get_item` Robustness + 403 Retry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use `claude-superskills:executing-plans` to implement this plan task-by-task.

**Goal:** Make `get_item` reliably return the full body of an item the caller already
saw, add lookup by `guid`/permalink-URL (not just the volatile per-`feed_url` `id`), add a
single bounded retry on transient Reddit `403`/`429`, and document the feed-window /
id-namespacing constraints — all driven by the round-2 MCP field report (2026-06-03).

**Architecture:** All behavior lands in `core.rs` / `fetch.rs` / `config.rs` and is exposed
unchanged from **both** front-ends (`rss show` and the MCP `get_item` tool) — invariant #6,
no logic forked into `mcp.rs`/`cli.rs`. The retrieval fix is a new **cache-first** read
policy (serve the cached feed body without revalidating, so a rolled-window live refetch
can't evict the item the caller saw) plus **multi-key matching** (`id` OR `guid` OR `url`).
`item_id` itself stays byte-frozen (invariant #4) — the fix is purely additive lookup keys
and a cache-read policy.

**Tech Stack:** Rust edition 2024, `tokio`, `reqwest` 0.13 (`rustls`), `feed-rs`, `serde`,
`schemars`, `rmcp` 1.7; tests with `assert_cmd` + `mockito` + `insta`. Gates: `cargo fmt
--all`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, `cargo build --release`.

---

## Scope decisions (locked with the owner, 2026-06-03)

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | **In-architecture `get_item` fix:** cache-first read policy + match by `id`/`guid`/`url`. | Solves the reporter's "fetch full body shortly after" case using the existing feed-body cache. **No** per-item body store. |
| 2 | **No default `max_content_chars`** on the MCP path. | ADR-0011 stands; `RESPONSE_TOO_LARGE` self-recovery (already shipped) handles the naive-call case. The reporter called this "optional". |
| 3 | **Single bounded retry** on `403`/`429`, honoring `Retry-After` (capped). | Reddit rate-limits intermittently; one polite retry, error surfaces `http_status` + `retry_after`. |

### What this fix does NOT do (state plainly; do not over-promise)
- It still requires the caller to pass the **`feed_url` that contains the item** — the cache
  is keyed by `feed_url`, and guid/url matching searches *that* feed's cached body.
- It does **not** survive a *later* refetch of the same `feed_url` that overwrote the cache
  with a rolled window. Only a per-item body store would close that "much later" gap, and we
  deliberately did not build one (decision #1). Document this as the feed-window constraint.
- `item_id` is **namespaced by `feed_url` by construction** and stays that way (the
  `identity.rs` known-answer test `1b9107de952289cb` / `a86aced5664c7742` is frozen). The
  reporter's "id not stable" observation was the *same post fetched from two different feed
  URLs* (`?t=day` vs `?t=week`) — working as designed. The cure is guid-lookup + docs, not
  changing the hash.

### Out of scope (no code; doc-only or declined)
- Populating `published` from `updated` — **declined**, corrupts the contract; documented in
  round 1 (`model.rs`).
- `limit+1` on Reddit comment feeds (OP appended) — **doc-only**.
- `search.rss` sparseness/noise — **doc-only** (Reddit-side, best-effort).

### Invariants to respect (from `CLAUDE.md`)
- #1 schema is generated from `model.rs`, never hand-written; breaking struct change → bump
  `SCHEMA_VERSION`. **This plan is additive → `SCHEMA_VERSION` stays `"1"`.**
- #2 optional fields serialize as `null`, never omitted.
- #4 `item_id` byte layout is frozen — **do not touch `identity.rs::item_id`**.
- #6 CLI and MCP share `core.rs`.
- #8 MCP data tools return `structuredContent`, errors stay text-only `ErrorObj`.

---

## Task 1: ADR-0014 — `get_item` retrieval semantics (cache-first + multi-key lookup)

A cache-first read is an **exception to ADR-0005** (always-revalidate default), so per
`CLAUDE.md` it requires its own ADR before the code lands.

**Files:**
- Create: `docs/adr/0014-get-item-cache-first-multi-key-lookup.md`
- Modify: `docs/adr/README.md` (add row to the index table)

**Step 1: Write the ADR** using the repo's Nygard template (Status · Context · Decision ·
Consequences · Alternatives considered). Content must capture:
- **Context:** `get_item`/`rss show` re-fetched the live feed and matched by `id` only; a
  rolled feed window (Reddit) → `NOT_FOUND`, and the per-`feed_url` `id` confused callers who
  fetched the same post from two feed URLs.
- **Decision:** (a) new `CachePolicy::CacheFirst` — serve the cached body if present,
  regardless of age, fetching only on a cache miss; both front-ends use it for item lookup.
  (b) `core::show_item` matches `id` **or** `guid` **or** resolved `url`. (c) `item_id` is
  unchanged (invariant #4); cross-feed-url stability is achieved by letting callers key on
  `guid` (e.g. Reddit `t3_…`), which is feed-window-independent.
- **Consequences:** survives the "shortly after" rolled-window case; does **not** survive a
  later cache-overwriting refetch (the feed-window constraint — documented); no per-item store.
- **Alternatives considered & rejected:** per-item body cache (more surface, new eviction
  policy — declined for v1); `MaxAge` reuse (rejected — it still revalidates once stale,
  re-introducing the very eviction we are avoiding); guid-based `item_id` (rejected —
  invariant #4, and ~41% of feeds regenerate guids, per ADR-0003).

**Step 2: Add the index row** in `docs/adr/README.md`:

```markdown
| [0014](0014-get-item-cache-first-multi-key-lookup.md) | get_item cache-first read + multi-key (id/guid/url) lookup | Accepted |
```

**Step 3: Commit**

```bash
git add docs/adr/0014-get-item-cache-first-multi-key-lookup.md docs/adr/README.md
git commit -m "docs(adr): 0014 get_item cache-first read + multi-key lookup"
```

---

## Task 2: ADR-0015 — bounded single retry on transient 429/403

A retry-on-error path is a small but real change to fetch semantics; give it a focused ADR
(matches the repo's one-decision-per-ADR norm).

**Files:**
- Create: `docs/adr/0015-bounded-retry-on-transient-429-403.md`
- Modify: `docs/adr/README.md`

**Step 1: Write the ADR.** Capture:
- **Context:** Reddit returns intermittent `403` (rate-limit) mid-batch, succeeding on a
  later call; callers cannot distinguish transient from permanent.
- **Decision:** retry **once** on `403`/`429`, waiting `min(Retry-After, 5s)` or a 500 ms
  base when no header; on persistent failure the `FEED_FETCH_FAILED` error carries
  `http_status` and (when present) `retry_after` in `details`. Bounded — one retry only, so
  it stays polite and never masks a persistent outage.
- **Consequences:** transient blips self-heal; latency added only on a real rate-limit; the
  error envelope gains an additive `retry_after` detail (no schema bump — `details` is
  free-form `serde_json::Value`).
- **Alternatives:** exponential multi-retry (rejected — unbounded, impolite); no retry
  (rejected — the field report shows it materially hurts batch fetches); honoring HTTP-date
  `Retry-After` form (deferred — Reddit sends delta-seconds; parse only that, ignore the date
  form rather than mis-sleep).

**Step 2: Add the index row:**

```markdown
| [0015](0015-bounded-retry-on-transient-429-403.md) | Bounded single retry on transient 429/403 (honor Retry-After) | Accepted |
```

**Step 3: Commit**

```bash
git add docs/adr/0015-bounded-retry-on-transient-429-403.md docs/adr/README.md
git commit -m "docs(adr): 0015 bounded retry on transient 429/403"
```

---

## Task 3: Add `CachePolicy::CacheFirst` variant (compiles, delegates as placeholder)

Add the variant and a temporary arm that delegates to `revalidate`, so the crate compiles
before we write the behavior test. (TDD: this keeps the build green; Task 4 adds the failing
behavior test against this placeholder.)

**Files:**
- Modify: `src/config.rs:18-28` (the `CachePolicy` enum)
- Modify: `src/fetch.rs:72-124` (the `match policy` in `fetch`)

**Step 1: Add the variant** to `src/config.rs`, after `MaxAge`:

```rust
    /// Serve the cached entry **without any network call, regardless of age**, if one
    /// exists; only on a cache miss does it fetch (then behave like [`CachePolicy::Revalidate`]).
    /// Used by item lookup (`rss show` / MCP `get_item`) so a rolled feed window cannot evict
    /// an item the caller already saw. Exception to the always-revalidate default — see
    /// ADR-0014.
    CacheFirst,
```

**Step 2: Add a placeholder arm** in `src/fetch.rs::fetch` (`match policy { … }`), so it
compiles:

```rust
            // Implemented in Task 5; placeholder so the crate builds.
            CachePolicy::CacheFirst => self.revalidate(url, cache).await,
```

**Step 3: Verify it builds**

Run: `cargo build`
Expected: builds clean, no warnings.

**Step 4: Commit**

```bash
git add src/config.rs src/fetch.rs
git commit -m "feat(cache): add CacheFirst policy variant (placeholder arm)"
```

---

## Task 4: Failing test for `CacheFirst` (serve stale cache, never hit network)

**Files:**
- Test: `src/fetch.rs` (add to the existing `#[cfg(test)] mod tests`, near
  `maxage_serves_cache_without_network` ~line 360)

**Step 1: Write the failing test.** Mirror the `MaxAge` test but seed a **stale**
`fetched_at` and assert the network is never touched (`.expect(0)`):

```rust
    #[tokio::test]
    async fn cache_first_serves_stale_cache_without_network() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("cachefirst");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // Deliberately STALE timestamp: CacheFirst must ignore age entirely.
        let meta = CacheMeta {
            feed_url: url.clone(),
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, b"<rss>cached</rss>").expect("seed cache");

        // Any network hit is a bug.
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>network</rss>")
            .expect(0)
            .create_async()
            .await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::CacheFirst)
            .await
            .expect("fetch ok");

        assert!(raw.from_cache);
        assert!(raw.not_modified);
        assert_eq!(raw.body, b"<rss>cached</rss>".to_vec());

        mock.assert_async().await; // expect(0): fails if the network was hit.
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn cache_first_fetches_on_miss() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/feed.xml")
            .with_status(200)
            .with_body("<rss>fresh</rss>")
            .create_async()
            .await;
        let dir = temp_cache_dir("cachefirst-miss");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let raw = client()
            .fetch(&url, &cache, CachePolicy::CacheFirst)
            .await
            .expect("fetch ok");

        assert!(!raw.from_cache);
        assert_eq!(raw.body, b"<rss>fresh</rss>".to_vec());
        mock.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }
```

**Step 2: Run to verify it fails**

Run: `cargo test --lib fetch::tests::cache_first_serves_stale_cache_without_network`
Expected: FAIL — the placeholder arm revalidates, so the `expect(0)` mock is hit (panic:
"expected 0 requests, got 1") and/or the body is `network`.

---

## Task 5: Implement the real `CacheFirst` arm

**Files:**
- Modify: `src/fetch.rs` (replace the placeholder arm from Task 3)

**Step 1: Replace the placeholder** with the real behavior:

```rust
            // Serve the cached body if present, regardless of age — never revalidate.
            // Only a cache miss falls through to a normal conditional GET. See ADR-0014.
            CachePolicy::CacheFirst => {
                if let Some(entry) = cache.get(url)? {
                    return Ok(RawFeed {
                        body: entry.body,
                        final_url: url.to_string(),
                        content_type: entry.meta.content_type,
                        status: 200,
                        not_modified: true,
                        from_cache: true,
                    });
                }
                self.revalidate(url, cache).await
            }
```

**Step 2: Run the tests**

Run: `cargo test --lib fetch::tests::cache_first`
Expected: PASS (both `cache_first_serves_stale_cache_without_network` and
`cache_first_fetches_on_miss`).

**Step 3: Update the `fetch` module-doc contract** (`src/fetch.rs:9-16`) to mention
`CacheFirst` alongside the other policies (one bullet, matching the existing style).

**Step 4: Commit**

```bash
git add src/fetch.rs
git commit -m "feat(cache): implement CacheFirst (serve cached body, fetch only on miss)"
```

---

## Task 6: Multi-key lookup in `core::show_item` (id OR guid OR url) + cache-first

**Files:**
- Modify: `src/core.rs:134-143` (`show_item`)

**Step 1: Write the failing test** in `src/core.rs` (add a `#[cfg(test)] mod tests` block if
none exists, else extend it). Drive it through the cache so no network is needed:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{Cache, CacheMeta};
    use crate::config::{CachePolicy, FetchParams};

    fn seed(cache: &Cache, url: &str, body: &[u8]) {
        let meta = CacheMeta {
            feed_url: url.to_string(),
            etag: None,
            last_modified: None,
            fetched_at: "2020-01-01T00:00:00Z".to_string(),
            content_type: Some("application/rss+xml".to_string()),
        };
        cache.put(&meta, body).expect("seed");
    }

    // A minimal RSS item with a distinct link and guid so we can match on each key.
    const FEED: &str = "https://t.example/r/x/.rss";
    const BODY: &str = r#"<rss version="2.0"><channel><title>x</title>
        <item><title>Post</title>
              <link>https://t.example/r/x/comments/abc/post/</link>
              <guid>t3_abc</guid>
              <pubDate>Mon, 02 Jun 2026 00:00:00 GMT</pubDate>
              <description>full body here</description></item>
        </channel></rss>"#;

    #[tokio::test]
    async fn show_item_matches_by_guid_and_url_cache_first() {
        let dir = std::env::temp_dir().join(format!("rss-core-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cache = Cache::open(Some(dir.clone())).unwrap();
        seed(&cache, FEED, BODY.as_bytes());

        // CacheFirst means show_item never touches the network in this test.
        let params = FetchParams { cache_policy: CachePolicy::CacheFirst, ..Default::default() };

        // First fetch the id the same way fetch_feed would compute it, then prove guid + url
        // resolve to the same item.
        let by_guid = show_item(FEED, "t3_abc", &params, &cache).await.unwrap();
        assert!(by_guid.is_some(), "guid lookup should resolve");
        let item = by_guid.unwrap();
        assert_eq!(item.guid.as_deref(), Some("t3_abc"));

        let by_url = show_item(FEED, "https://t.example/r/x/comments/abc/post/", &params, &cache)
            .await.unwrap();
        assert_eq!(by_url.map(|i| i.id), Some(item.id.clone()));

        let by_id = show_item(FEED, &item.id, &params, &cache).await.unwrap();
        assert_eq!(by_id.map(|i| i.id), Some(item.id));

        std::fs::remove_dir_all(&dir).ok();
    }
}
```

**Step 2: Run to verify it fails**

Run: `cargo test --lib core::tests::show_item_matches_by_guid_and_url_cache_first`
Expected: FAIL — current `show_item` matches only `it.id == id`, so guid/url lookups return
`None`.

**Step 3: Implement multi-key matching.** Rename the param `id` → `key` and broaden the
predicate. Replace `show_item` body:

```rust
/// Fetch a feed (cache-first) and return the single item whose `id`, raw `guid`, or resolved
/// `url` equals `key`, if present.
///
/// Used by `rss show` and the MCP `get_item` tool. `id` is namespaced by `feed_url` (see
/// ADR-0003); a `guid` (e.g. Reddit `t3_…`) is feed-window-independent and is the reliable
/// key across different feed URLs. The lookup is cache-first (ADR-0014): an item the caller
/// already saw survives a rolled feed window, but not a later cache-overwriting refetch.
pub async fn show_item(
    feed_url: &str,
    key: &str,
    params: &FetchParams,
    cache: &Cache,
) -> Result<Option<crate::model::Item>, RssError> {
    let http = HttpClient::new(&params.user_agent, params.timeout)?;
    let (fr, _warnings) = fetch_one(feed_url, &http, params, cache).await?;
    Ok(fr.items.into_iter().find(|it| {
        it.id == key
            || it.guid.as_deref() == Some(key)
            || it.url.as_deref() == Some(key)
    }))
}
```

**Step 4: Run to verify it passes**

Run: `cargo test --lib core::tests::show_item_matches_by_guid_and_url_cache_first`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/core.rs
git commit -m "feat(core): show_item matches id/guid/url, cache-first (ADR-0014)"
```

---

## Task 7: Front-ends pass `CacheFirst` and accept any key (CLI `rss show` + MCP `get_item`)

Wire both front-ends to the new behavior. No logic here — just default the policy to
`CacheFirst` and update help/arg docs to say the key may be id/guid/url.

**Files:**
- Modify: `src/main.rs:74-98` (the `Command::Show` handler — set `cache_policy`)
- Modify: `src/cli.rs:272-293` (`ShowArgs` — `id` help text; add optional `--refresh`)
- Modify: `src/mcp.rs` (`get_item_inner` ~line 363 — set `cache_policy`; `GetItemArgs.id`
  doc ~line 141)

**Step 1 (CLI handler):** in `src/main.rs`, the `Command::Show` arm, set the policy (and
honor an optional `--refresh` for a live read):

```rust
        Command::Show(args) => {
            let params = FetchParams {
                content_format: args.content.into(),
                max_content_chars: args.max_content_chars,
                cache_policy: if args.refresh {
                    CachePolicy::Revalidate
                } else {
                    CachePolicy::CacheFirst
                },
                ..FetchParams::default()
            };
```

(Leave the rest of the arm unchanged; ensure `CachePolicy` is imported in `main.rs`.)

**Step 2 (CLI args):** in `src/cli.rs` `ShowArgs`, update the `id` doc and add `--refresh`:

```rust
    /// Stable item id, raw guid, or item permalink URL (from a prior `fetch`). A guid is the
    /// reliable key across different feed URLs (the id is namespaced by feed URL).
    #[arg(long, value_name = "ITEM_KEY")]
    pub id: String,

    /// Bypass the cache-first read and revalidate the live feed (may miss items that have
    /// rolled out of the feed window).
    #[arg(long)]
    pub refresh: bool,
```

**Step 3 (MCP):** in `src/mcp.rs`, set the policy in `get_item_inner`:

```rust
    let params = FetchParams {
        max_content_chars: args.max_content_chars,
        cache_policy: CachePolicy::CacheFirst,
        ..FetchParams::default()
    };
```

and update the `GetItemArgs.id` doc (~line 141):

```rust
    /// The item key: its stable `id`, raw `guid` (e.g. Reddit `t3_…`/`t1_…`), or permalink
    /// URL. A guid is the reliable key across different feed URLs, since `id` is namespaced
    /// by `feed_url`. Served cache-first: an item from a prior `fetch_feed` survives a rolled
    /// feed window, but not a later refetch that overwrote the cache.
    id: String,
```

(Confirm `CachePolicy` is imported in `mcp.rs`; add to the `use crate::config::…` line if not.)

**Step 4: Verify build + existing tests still green**

Run: `cargo build && cargo test --lib`
Expected: builds clean; all existing tests pass (the `NOT_FOUND` message in
`get_item_missing_id_is_structured_not_found` still holds — a genuinely-absent key returns
`None` → `NOT_FOUND`).

**Step 5: Commit**

```bash
git add src/main.rs src/cli.rs src/mcp.rs
git commit -m "feat(cli,mcp): get_item/show accept id|guid|url, default cache-first"
```

---

## Task 8: End-to-end window-roll + multi-key integration test (the crown jewel)

Prove the reporter's exact failure is fixed, through the real CLI binary, offline via
`mockito`. This is the test that demonstrates the bug is gone.

**Files:**
- Create: `tests/get_item_window_roll.rs`

**Step 1: Write the test.** Fetch a feed (item present) into a shared cache dir, then have
the server stop serving that item; assert `rss show` (cache-first) still returns it, and that
`guid`/`url` keys resolve.

```rust
use assert_cmd::Command;
use serde_json::Value;

#[test]
fn get_item_survives_rolled_window_and_accepts_guid_url() {
    let mut server = mockito::Server::new();
    let body = r#"<rss version="2.0"><channel><title>x</title>
        <item><title>Post</title>
              <link>https://ex.test/p/abc/</link>
              <guid>t3_abc</guid>
              <pubDate>Mon, 02 Jun 2026 00:00:00 GMT</pubDate>
              <description>full body here</description></item>
        </channel></rss>"#;
    // First GET (the fetch) returns the item; the mock is consumed after one hit.
    let _m1 = server.mock("GET", "/feed.xml").with_status(200)
        .with_body(body).expect_at_least(1).create();

    let url = format!("{}/feed.xml", server.url());
    let cache = tempfile::tempdir().unwrap();
    let cache_arg = cache.path().to_str().unwrap();

    // 1) Populate the cache.
    let out = Command::cargo_bin("rss").unwrap()
        .args(["--cache-dir", cache_arg, "fetch", &url, "--format", "json"])
        .assert().success().get_output().stdout.clone();
    let json: Value = serde_json::from_slice(&out).unwrap();
    let item = &json["feeds"][0]["items"][0];
    let id = item["id"].as_str().unwrap().to_string();
    assert_eq!(item["guid"].as_str(), Some("t3_abc"));

    // 2) show by id — cache-first, no live dependency on the (now-irrelevant) window.
    Command::cargo_bin("rss").unwrap()
        .args(["--cache-dir", cache_arg, "show", &url, "--id", &id])
        .assert().success()
        .stdout(predicates::str::contains("full body here"));

    // 3) show by guid resolves the same item.
    Command::cargo_bin("rss").unwrap()
        .args(["--cache-dir", cache_arg, "show", &url, "--id", "t3_abc"])
        .assert().success()
        .stdout(predicates::str::contains("full body here"));

    // 4) show by permalink URL resolves the same item.
    Command::cargo_bin("rss").unwrap()
        .args(["--cache-dir", cache_arg, "show", &url, "--id", "https://ex.test/p/abc/"])
        .assert().success()
        .stdout(predicates::str::contains("full body here"));
}
```

**Step 2: Ensure dev-deps exist.** Check `Cargo.toml [dev-dependencies]` has `tempfile` and
`predicates`; add `tempfile` if missing (`predicates`/`assert_cmd`/`mockito` are already
used by the suite).

Run: `cargo test --test get_item_window_roll`
Expected: PASS — all four CLI invocations succeed; the body is returned even though the feed
window is irrelevant to the cache-first read.

**Step 3: Commit**

```bash
git add tests/get_item_window_roll.rs Cargo.toml
git commit -m "test: get_item survives rolled window; accepts id/guid/url (e2e)"
```

---

## Task 9: Add `retry_after` to `RssError::Http` and `to_error_obj`

Prepare the error type so the retry (Task 11) can surface `Retry-After`. Additive only —
`details` is free-form, no schema bump.

**Files:**
- Modify: `src/error.rs:32` (the `Http` variant) and `:85-87` (`to_error_obj`)

**Step 1: Write the failing test** in `src/error.rs` tests (or add a small `#[cfg(test)]`):

```rust
    #[test]
    fn http_error_surfaces_retry_after_when_present() {
        let e = RssError::Http {
            status: 429,
            url: "https://x".into(),
            retry_after: Some("2".into()),
        };
        let obj = e.to_error_obj(None);
        assert_eq!(obj.code, "FEED_FETCH_FAILED");
        assert_eq!(obj.details["http_status"], 429);
        assert_eq!(obj.details["retry_after"], "2");
    }
```

**Step 2: Run to verify it fails**

Run: `cargo test --lib error::`
Expected: FAIL to compile — `Http` has no `retry_after` field yet.

**Step 3: Implement.** Add the field:

```rust
    Http {
        status: u16,
        url: String,
        /// Raw `Retry-After` header value when the server sent one (delta-seconds form).
        retry_after: Option<String>,
    },
```

and in `to_error_obj`:

```rust
            RssError::Http { status, retry_after, .. } => {
                obj.details = serde_json::json!({
                    "http_status": status,
                    "retry_after": retry_after,
                });
            }
```

**Step 4: Fix all construction sites.** Add `retry_after: None` to every existing
`RssError::Http { … }` in `src/fetch.rs` (the `NoCache` arm ~line 84, `revalidate` ~line 175,
`get_bytes` ~line 221). The retry path (Task 11) will set it to `Some(..)`.

**Step 5: Run**

Run: `cargo test --lib error:: && cargo build`
Expected: the new test PASSES; the crate builds.

**Step 6: Commit**

```bash
git add src/error.rs src/fetch.rs
git commit -m "feat(error): Http error carries retry_after detail"
```

---

## Task 10: Failing test for the 403→200 single retry

**Files:**
- Test: `src/fetch.rs` `#[cfg(test)] mod tests`

**Step 1: Write the failing tests.** mockito serves `403` once, then `200`; the fetch should
retry and succeed. A second test asserts a persistent `403` surfaces the error with
`http_status`/`retry_after`. Keep them fast: no `Retry-After` header → the 500 ms base delay
applies once (sub-second).

```rust
    #[tokio::test]
    async fn retries_once_on_403_then_succeeds() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("retry-ok");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        let m403 = server.mock("GET", "/feed.xml")
            .with_status(403).expect(1).create_async().await;
        let m200 = server.mock("GET", "/feed.xml")
            .with_status(200).with_body("<rss>ok</rss>").expect(1).create_async().await;

        let raw = client()
            .fetch(&url, &cache, CachePolicy::NoCache)
            .await
            .expect("retry should succeed");
        assert_eq!(raw.body, b"<rss>ok</rss>".to_vec());

        m403.assert_async().await;
        m200.assert_async().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn persistent_403_surfaces_status_in_error() {
        let mut server = mockito::Server::new_async().await;
        let dir = temp_cache_dir("retry-fail");
        let cache = Cache::open(Some(dir.clone())).expect("open cache");
        let url = format!("{}/feed.xml", server.url());

        // Two 403s (original + one retry), then assert we still error out.
        let m = server.mock("GET", "/feed.xml").with_status(403)
            .expect(2).create_async().await;

        let err = client().fetch(&url, &cache, CachePolicy::NoCache).await.unwrap_err();
        match err {
            RssError::Http { status, .. } => assert_eq!(status, 403),
            other => panic!("expected Http error, got {other:?}"),
        }
        m.assert_async().await; // exactly 2 attempts: one retry, no more.
        std::fs::remove_dir_all(&dir).ok();
    }
```

**Step 2: Run to verify they fail**

Run: `cargo test --lib fetch::tests::retries_once_on_403`
Expected: FAIL — no retry today; the first `403` errors immediately (only 1 request → the
`m200.expect(1)` and `m403.expect(1)` assertions fail / the success `.expect()` fails).

---

## Task 11: Implement the bounded single retry on 403/429

**Files:**
- Modify: `src/fetch.rs` (add `send_with_retry` + helpers; use it in `NoCache`, `revalidate`,
  and `get_bytes`)

**Step 1: Add helpers** (module-level, near `is_fresh`):

```rust
use tokio::time::sleep;

/// Statuses we retry exactly once — transient provider rate-limiting.
fn is_retryable(status: StatusCode) -> bool {
    status == StatusCode::FORBIDDEN || status == StatusCode::TOO_MANY_REQUESTS
}

/// Parse the delta-seconds form of `Retry-After`, bounded to `max`. The HTTP-date form is
/// intentionally ignored (Reddit sends delta-seconds) rather than risk a wrong sleep.
fn retry_after(headers: &HeaderMap, max: Duration) -> Option<Duration> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    let secs: u64 = raw.trim().parse().ok()?;
    Some(Duration::from_secs(secs).min(max))
}

/// Raw `Retry-After` header value, for surfacing in the error detail.
fn retry_after_raw(headers: &HeaderMap) -> Option<String> {
    headers.get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

const RETRY_BASE_DELAY: Duration = Duration::from_millis(500);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// Send a freshly-built request, retrying once on a transient 403/429. `build` is called
/// again for the retry so headers/validators are re-attached cleanly.
async fn send_with_retry<F>(build: F) -> Result<reqwest::Response, RssError>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    let resp = build().send().await.map_err(|e| RssError::Network(e.to_string()))?;
    if !is_retryable(resp.status()) {
        return Ok(resp);
    }
    let wait = retry_after(resp.headers(), RETRY_MAX_DELAY).unwrap_or(RETRY_BASE_DELAY);
    sleep(wait).await;
    build().send().await.map_err(|e| RssError::Network(e.to_string()))
}
```

**Step 2: Use it in `revalidate`.** Replace the inline `req`/`req.send()` (lines ~132-145)
with a `build` closure + `send_with_retry`:

```rust
        let cached = cache.get(url)?;
        let build = || {
            let mut req = self.inner.get(url);
            if let Some(entry) = &cached {
                if let Some(etag) = &entry.meta.etag {
                    req = req.header(IF_NONE_MATCH, etag.as_str());
                }
                if let Some(last_modified) = &entry.meta.last_modified {
                    req = req.header(IF_MODIFIED_SINCE, last_modified.as_str());
                }
            }
            req
        };
        let resp = send_with_retry(build).await?;
        let status = resp.status();
        let final_url = resp.url().to_string();
```

And update the non-success branch (~line 174) to capture `retry_after`:

```rust
        if !status.is_success() {
            return Err(RssError::Http {
                status: status.as_u16(),
                url: url.to_string(),
                retry_after: retry_after_raw(resp.headers()),
            });
        }
```

**Step 3: Use it in the `NoCache` arm** (lines ~75-88) — replace `self.inner.get(url).send()`
with `send_with_retry(|| self.inner.get(url))`, and set `retry_after` on its error branch the
same way.

**Step 4: Use it in `get_bytes`** (lines ~212-225) — same `send_with_retry(|| self.inner.get(url))`
substitution and `retry_after` on the error branch (keeps discovery resilient too).

**Step 5: Run the retry + full fetch tests**

Run: `cargo test --lib fetch::`
Expected: PASS — `retries_once_on_403_then_succeeds`, `persistent_403_surfaces_status_in_error`,
and all pre-existing fetch tests (`fetch_200_…`, `revalidate_304_…`, `maxage_…`, the new
`cache_first_…`).

**Step 6: Commit**

```bash
git add src/fetch.rs
git commit -m "feat(fetch): single bounded retry on transient 403/429 (ADR-0015)"
```

---

## Task 12: Documentation — schema notes + README provider notes

Address the doc-only items (`limit+1`, `published: null`, `search.rss`) and document the new
id-namespacing / feed-window / retry behavior where agents will see it.

**Files:**
- Modify: `src/mcp.rs` — the server `instructions`/tool descriptions (the `fetch_feed`
  description string ~line 36 and `GetItemArgs` already done in Task 7).
- Modify: `README.md` — `rss show` / MCP section + a "Provider notes (Reddit)" subsection.
- Modify: `src/model.rs` — `Item` doc only if a schema-visible note is warranted (the
  `published`/`updated` note already exists from round 1; do **not** re-add).

**Step 1: MCP `fetch_feed` description** — append a provider-quirks sentence so the agent sees
it in `tools/list` (keep it terse; it flows into the tool schema the client reads):

```
… Provider notes: some feeds (e.g. Reddit comment .rss) populate only `updated`, not
`published`, and append the original post to a comment listing (so a comment feed can return
one more item than `limit`). `search.rss` results are best-effort and may be sparse.
```

**Step 2: README** — under the item-retrieval docs, add:
- `get_item`/`rss show` accept **id, guid, or permalink URL**; `id` is namespaced by feed URL,
  so a `guid` (Reddit `t3_…`) is the portable key.
- Cache-first retrieval + the feed-window constraint (survives "shortly after", not a later
  cache-overwriting refetch); `--refresh` forces a live read.
- A "Provider notes (Reddit)" subsection covering `limit+1`, `published: null` on comments,
  `search.rss` sparseness, and the bounded 403/429 retry.

**Step 3: Verify schema unchanged in shape**

Run: `cargo run -- schema --command fetch | jq '.properties.feeds' >/dev/null && echo OK`
Expected: `OK` (schema still emits; `SCHEMA_VERSION` still `"1"` — no struct change).

**Step 4: Commit**

```bash
git add src/mcp.rs README.md
git commit -m "docs: get_item key options, feed-window caveat, Reddit provider notes"
```

---

## Task 13: Update `CLAUDE.md` (invariants + gotchas) and final gate run

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add to "Gotchas"**:
- `CachePolicy::CacheFirst` exists specifically for item lookup; do not "simplify" `get_item`
  back to `Revalidate` (re-introduces the rolled-window `NOT_FOUND`). Pinned by
  `get_item_window_roll` + `cache_first_serves_stale_cache_without_network`.
- The 403/429 retry is **single and bounded**; do not turn it into an unbounded loop.

**Step 2: Add to "Invariants"** a note that `show_item` matches `id`/`guid`/`url` and is
cache-first by default (cross-references ADR-0014), and that this is additive
(`SCHEMA_VERSION` unchanged).

**Step 3: Run the full gate suite**

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release
```

Expected: fmt clean; clippy zero warnings; **all** tests pass (the prior 66 + the new
`cache_first_*`, `show_item_*`, `retries_once_*`, `persistent_403_*`, `http_error_surfaces_*`,
and the `get_item_window_roll` e2e); release builds.

**Step 4: Live MCP smoke check** (optional, mirrors `CLAUDE.md`): start `./target/release/rss
mcp`, call `fetch_feed` then `get_item` with the returned `guid` and confirm a structured
`Item` comes back; confirm a bogus key returns a structured `NOT_FOUND`.

**Step 5: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(claude): CacheFirst/get_item + retry invariants and gotchas"
```

---

## Done criteria

- [ ] `get_item`/`rss show` return an item the caller already saw even after the feed window
      rolls (cache-first), proven by `tests/get_item_window_roll.rs`.
- [ ] `get_item`/`rss show` resolve by `id`, `guid`, **or** permalink `url`.
- [ ] Transient `403`/`429` triggers exactly one bounded retry honoring `Retry-After`;
      persistent failure surfaces `http_status` + `retry_after`.
- [ ] ADR-0014 and ADR-0015 written and indexed; `CLAUDE.md` updated.
- [ ] `limit+1`, `published: null`, `search.rss` documented for agents (schema/tool text +
      README).
- [ ] `SCHEMA_VERSION` unchanged (`"1"`) — all changes additive.
- [ ] All gates green: `cargo fmt --all` · `cargo clippy --all-targets -- -D warnings` ·
      `cargo test` · `cargo build --release`.
- [ ] **No push / no PR** — repo is intentionally local-only (owner's standing constraint).

## Notes for the executing engineer

- **Frozen:** `identity.rs::item_id` (invariant #4 — known-answer test). Never edit it; the
  whole point is that `id` is deterministic. Cross-feed-url portability comes from guid
  lookup, not from changing the hash.
- **Both front-ends share `core.rs`** (invariant #6). The matching + policy live in
  `core::show_item`; `main.rs`/`mcp.rs` only choose the `CachePolicy`.
- **MCP errors stay text-only `ErrorObj`** (invariant #8). A `NOT_FOUND` for a genuinely-absent
  key is still text-only with no `structuredContent`.
- **Keep tests fast:** the retry tests rely on absent `Retry-After` so only the 500 ms base
  delay applies once; don't add `Retry-After: <large>` to a test unless you also cap it.
- Commit after each task (frequent commits). **Do not push.**
