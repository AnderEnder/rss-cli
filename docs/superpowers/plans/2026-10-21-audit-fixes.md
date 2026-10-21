# Audit fixes implementation plan

**Goal:** Fix the seven reproduced bugs and three engineering gaps from the repository audit.

**Architecture:** Keep the shared core and public output shapes. Use checked duration arithmetic, source-aware content rendering, presentation-independent query matching, atomic cache records with legacy reads, validated cache replacement, and snapshot-aware cursors. Bound decoded HTTP bodies before parsing.

**Tech stack:** Rust, feed-rs, reqwest, tokio, mockito, assert_cmd.

Work in the clean existing checkout; execute inline. Feature proposals are out of scope.

- [x] Input validation (`src/config.rs`): regress overflowing durations/date subtraction, then use checked arithmetic; verify configuration tests.
- [x] Content and queries (`src/parse.rs`, `src/content.rs`, `src/query.rs`): regress JSON/Atom plain text and query selection under none/capped/formatted content; preserve source MIME and filter full plain content before rendering; verify parser tests.
- [x] Discovery (`src/discover.rs`): regress absolute/relative HTML bases and rel tokens; resolve against the first valid href base and require exact rel tokens; verify discovery tests.
- [x] CLI rendering (`src/output.rs`, `src/main.rs`, `tests/usage.rs`): regress show/cache text and NDJSON through the binary; route formats through shared renderers.
- [x] Cache (`src/cache.rs`): regress unrelated-file preservation and concurrent body/validator consistency; atomically publish metadata plus raw body in one .entry record, read legacy pairs, and recognize owned filenames only.
- [x] Fetch (`src/fetch.rs`, `src/config.rs`, `tests/fetch.rs`): regress invalid replacement and decoded oversized/chunked bodies; keep valid cached bytes, enforce a 16 MiB decoded body ceiling on fetch/discovery/cache-backed parsing.
- [x] Pagination (`src/cursor.rs`, `src/mcp.rs`): regress same-count replacement between pages; carry the filtered feed snapshot digest and return CURSOR_EXPIRED on mismatch; retain legacy cursor handling.
- [x] Documentation: record cache/query/cursor decisions and resource limits; update contradictory docs.
- [x] Verification: format check, clippy with warnings denied, complete suite, release build, final code review.

## Execution evidence

The audit already reproduced the seven bugs against e9268a7; new automated regressions must fail before each implementation. Publishing was deferred during implementation; the user subsequently requested PRs.


Regression evidence: duration overflow, source text/query presentation, discovery base/rel,
cache clearing/formats/replacement, concurrent cache writers, and snapshot replacement were
observed failing before their fixes. All 297 tests now pass. Gzip/chunked limit tests and
legacy cache compatibility pass. Independent review found no blocking issues; its Markdown
entity finding was reproduced and fixed with named/numeric entity regressions.

Ruling: continue legacy cursors with count warnings; only newly minted snapshots can enforce
expiry. Resuming at offset zero remains valid because that feed has delivered no items.
Ruling: preserve the raw HTTP client return type; validate cache admission and retain core's
parse failure rather than caching malformed bytes. This adds one bounded parser pass.

Final gates: fmt, clippy (-D warnings), all 297 tests, and full release build passed.
Optimized-binary smoke checks confirmed structured usage errors and literal text preservation.
Publishing: split the verified changes into four stacked PRs for cache/input bounds, source text/query matching, CLI/discovery, and pagination/documentation. Each intermediate commit is verified before publishing.
