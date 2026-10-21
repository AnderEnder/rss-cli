# 25. Preserve feed snapshots and source text

- **Status:** Accepted
- **Date:** 2026-10-21
- **Supersedes:** the paired-file layout in ADR-0004, count-only cursor checks in ADR-0017,
  and presentation-dependent keyword matching in ADR-0018.

## Context

Concurrent cache writes can mix one body's bytes with another body's validators. A malformed
HTTP 200 can replace a usable feed, and a same-count window replacement evades pagination's
roll warning. Treating plain feed text as HTML loses literal tags; filtering rendered bodies
makes query results depend on output format and truncation.

## Decision

- Publish one `<sha256(feed_url)>.entry` record: compact JSON metadata, newline, raw body.
  Write a unique file with exclusive creation, sync it, then rename. Read legacy `.json` /
  `.body` pairs when no atomic record exists. Listing prefers the atomic record; clearing
  removes only recognized cache filenames, counting each feed key once.
- Cache a fresh HTTP body only after `feed-rs` accepts it. Invalid content still fails parsing
  for the caller; the prior body and validators remain available to cache-first lookup.
- New cursors carry a digest of the full filtered, ordered item list for their resume feed,
  captured before draining or budgeting. A changed digest at a nonzero offset returns
  `CURSOR_EXPIRED`: restart and reconcile previously delivered ids. A feed resumed at its head
  needs no guard because no items from it were delivered. Legacy cursors remain readable with
  their count-based `CACHE_WINDOW_ROLLED` warning. The response schema stays unchanged.
- Honor source MIME types. Plain text remains literal in text output and is escaped for HTML
  and Markdown; HTML/XHTML continues through the converters. Search full plain source text
  with collapsed whitespace, after `since` and before output conversion, body caps, or `limit`.
- Reject decoded HTTP bodies over 16 MiB with `RESPONSE_BODY_TOO_LARGE` and `limit_bytes`.
  Enforce the bound while reading chunks, including decompression, and at cache/parser entry
  points. Cache metadata is capped separately at 64 KiB. This does not impose a hard CPU or
  whole-process memory ceiling.

## Consequences

Existing cache data remains readable; new writes need no global lock or database. Older
binaries cannot see new atomic records, so using versions concurrently may require a refetch.
Legacy files are retained until cache clearing. Valid responses are parsed once for cache
admission and again for output. This small extra cost preserves the raw HTTP client interface.
The existing stale-if-error policy still refuses to hide parse failures.

New cursors detect same-count replacements without retaining historical snapshots. Restarting
may re-deliver earlier items, which callers reconcile by id. Legacy cursors cannot gain a
snapshot guarantee retroactively. Stable item-id construction and output field shapes stay
unchanged; corrected source rendering can change `content_hash`.
