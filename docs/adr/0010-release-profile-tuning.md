# 10. Tuned release profile (LTO, single codegen unit, abort, strip)

- **Status:** Accepted
- **Date:** 2026-06-01

## Context

A CLI binary is shipped and run many times; build time matters far less than the artifact.
`tokio` + `rmcp` + `reqwest` pull in a lot of code, so the default release profile produces
a larger binary than necessary. The question was which optimization flags are worth
enabling — with the honest caveat that this tool is **I/O-bound** (wall-clock is dominated
by network fetches), so CPU optimizations mostly buy **binary size** and **startup/parse**
overhead, not throughput.

## Decision

Set in [`Cargo.toml`](../../Cargo.toml) `[profile.release]`:

| Flag | Value | Why |
|------|-------|-----|
| `opt-level` | `3` | Full optimization (also the default; explicit for clarity). |
| `lto` | `"fat"` | Whole-program link-time optimization across all crates. |
| `codegen-units` | `1` | Maximize cross-function optimization (slower compile, faster/smaller code). |
| `panic` | `"abort"` | No unwinding tables → smaller, faster binary; a CLI can just abort. |
| `strip` | `true` | Strip symbols/debuginfo from the shipped binary. |

Verified: release build succeeds and runs; the stripped binary is ~7.9 MB.

## Consequences

- Smaller binary and slightly faster startup/parse. **Throughput is unchanged** — the real
  runtime levers are concurrency (`--concurrency`) and conditional-GET caching
  ([ADR-0005](0005-conditional-get-always-revalidate-default.md)), not codegen.
- Release builds are slower (~1–2 min) due to `lto = "fat"` + `codegen-units = 1`. This is
  the release profile only; **CI and `cargo test` use the dev profile** and are unaffected.
- `panic = "abort"` means no stack unwinding on panic. Acceptable: all real error paths use
  `Result`/`RssError`, never panics for control flow. (`cargo test` runs under dev, so test
  harness unwinding is unaffected.)

## Alternatives considered

- **`opt-level = "z"`/`"s"` (size-optimized).** Not chosen for the default; a viable switch
  if binary size becomes the priority over speed.
- **Keep `panic = "unwind"`.** Would preserve unwinding at the cost of size/speed; not
  worth it for a CLI that has no recover-from-panic requirement.
- **musl static build.** Out of scope here; a portable-binary concern, orthogonal to these
  flags.
