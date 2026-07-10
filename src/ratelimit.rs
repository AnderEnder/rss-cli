//! Per-host request gate: shared, adaptive rate limiting for concurrent fetches.
//!
//! See [ADR-0016](../docs/adr/0016-per-host-request-gate.md). The gate lives inside a
//! *reused* [`crate::fetch::HttpClient`] so concurrent MCP tool calls (and a CLI batch)
//! coordinate their pacing toward a single host — preventing the self-inflicted `429` burst
//! that a fresh-client-per-call fundamentally cannot. It is **reactive, not proactive**: it
//! honors a server `Retry-After` where present and, for hosts that send none, applies a
//! bounded escalating cooldown learned from consecutive throttles.
//!
//! This is complementary to the per-request retry of
//! [ADR-0015](../docs/adr/0015-bounded-retry-on-transient-429-403.md): that reacts *within*
//! one request; this schedules *across* concurrent requests. The two waits are one budget —
//! a request holds its host permit through its own retry, so it never waits on the cooldown
//! it just set.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::RssError;

/// Max concurrent in-flight requests to a single authority (`host:port`). `1` serializes the
/// many feeds that share a host while distinct hosts stay fully parallel (each has its own
/// gate). Override with `RSS_HOST_CONCURRENCY`.
const HOST_MAX_CONCURRENCY: usize = 1;

/// First cooldown applied when a host `429`/`403`s **without** a `Retry-After` header; also
/// the base for bounded escalation on consecutive throttles.
const HOST_BASE_COOLDOWN: Duration = Duration::from_secs(2);

/// Hard ceiling on any single cooldown (an honored `Retry-After` or an escalated default).
/// Override with `RSS_MAX_COOLDOWN_SECS`.
const HOST_MAX_COOLDOWN: Duration = Duration::from_secs(60);

/// Inter-request spacing enforced while a host is "warm" (recently throttled). Costs the
/// happy path nothing: a host that has never throttled is never warm.
const STICKY_SPACING: Duration = Duration::from_secs(1);

/// How long a host stays "warm" (sticky spacing active) after its most recent throttle.
const WARM_WINDOW: Duration = Duration::from_secs(120);

/// Max **cooldown/spacing** wait a request will absorb before failing fast with
/// [`RssError::RateLimited`] (ADR-0016). This bounds the pacing sleep against `next_allowed`,
/// **not** the time spent contending for the per-host permit — under `cap = 1` a sibling can
/// still block on `acquire_owned` for as long as the in-flight holder runs, which is
/// separately bounded by the reqwest request timeout. Override with `RSS_MAX_GATE_WAIT_SECS`.
const MAX_GATE_WAIT: Duration = Duration::from_secs(60);

/// Current wall-clock as epoch milliseconds (matches the crate's chrono-based clock).
fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

/// Derive the gate key from a URL: lowercased `host:port` (with the scheme's default port
/// filled in), e.g. `example.com:443`. A URL that fails to parse becomes its own bucket.
fn authority_of(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(u) => {
            let host = u.host_str().unwrap_or_default().to_ascii_lowercase();
            let port = u.port_or_known_default().unwrap_or(0);
            format!("{host}:{port}")
        }
        Err(_) => url.to_string(),
    }
}

/// Per-authority state. All timing fields are epoch-ms in atomics so they are read/updated
/// lock-free; the only lock in the gate is the brief one around the slot map.
struct HostSlot {
    /// Concurrency cap for this host. `Arc` so `acquire_owned` yields a `'static` permit.
    sem: Arc<Semaphore>,
    /// Earliest epoch-ms at which the next request to this host may be *sent*.
    next_allowed_ms: AtomicI64,
    /// Epoch-ms until which the host is considered recently-throttled (sticky spacing on).
    warm_until_ms: AtomicI64,
    /// Consecutive throttles with no intervening success — drives cooldown escalation.
    consecutive_throttles: AtomicU32,
}

impl HostSlot {
    fn new(per_host: usize) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(per_host.max(1))),
            next_allowed_ms: AtomicI64::new(0),
            warm_until_ms: AtomicI64::new(0),
            consecutive_throttles: AtomicU32::new(0),
        }
    }
}

/// The shared per-host gate. Cheap to clone-share via `Arc` (as it is inside `HttpClient`).
pub struct HostGate {
    slots: Mutex<HashMap<String, Arc<HostSlot>>>,
    per_host: usize,
    base_cooldown: Duration,
    max_cooldown: Duration,
    sticky_spacing: Duration,
    warm_window: Duration,
    max_gate_wait: Duration,
}

impl Default for HostGate {
    fn default() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            per_host: HOST_MAX_CONCURRENCY,
            base_cooldown: HOST_BASE_COOLDOWN,
            max_cooldown: HOST_MAX_COOLDOWN,
            sticky_spacing: STICKY_SPACING,
            warm_window: WARM_WINDOW,
            max_gate_wait: MAX_GATE_WAIT,
        }
    }
}

impl HostGate {
    /// Build a gate, letting environment variables override the tunable defaults (ADR-0016).
    pub fn from_env() -> Self {
        let mut gate = Self::default();
        if let Some(n) = env_parse::<usize>("RSS_HOST_CONCURRENCY") {
            gate.per_host = n.max(1);
        }
        if let Some(secs) = env_parse::<u64>("RSS_MAX_COOLDOWN_SECS") {
            gate.max_cooldown = Duration::from_secs(secs);
        }
        if let Some(secs) = env_parse::<u64>("RSS_MAX_GATE_WAIT_SECS") {
            gate.max_gate_wait = Duration::from_secs(secs);
        }
        gate
    }

    /// Get (or create) the slot for an authority. Holds the map lock only for the O(1)
    /// insert-and-clone — **never across an `.await`** (the one non-negotiable rule).
    fn slot_for(&self, authority: &str) -> Arc<HostSlot> {
        let mut slots = self.slots.lock().expect("host-gate mutex poisoned");
        slots
            .entry(authority.to_string())
            .or_insert_with(|| Arc::new(HostSlot::new(self.per_host)))
            .clone()
    }

    /// Resolve the slot for a URL (derive its authority, get-or-create). Convenience shared by
    /// every public entry point.
    fn slot_for_url(&self, url: &str) -> Arc<HostSlot> {
        self.slot_for(&authority_of(url))
    }

    /// Acquire the right to send a request to `url`'s host, waiting out any active cooldown /
    /// spacing. Returns the permit guard (released on drop, including on cancellation) or a
    /// [`RssError::RateLimited`] if the required wait exceeds `max_gate_wait`.
    pub async fn acquire(&self, url: &str) -> Result<OwnedSemaphorePermit, RssError> {
        let slot = self.slot_for_url(url);

        // Concurrency cap. `acquire_owned` yields a permit carrying no borrow, so the guard is
        // `Send` and RAII-released even if the caller's future is dropped mid-flight.
        let permit = slot
            .sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RssError::Other("rate-limiter semaphore closed".to_string()))?;

        // `next_allowed - now` is a non-negative millisecond count; the conversion is total.
        let now = now_ms();
        let target = slot.next_allowed_ms.load(Ordering::Relaxed);
        let wait = Duration::from_millis(u64::try_from((target - now).max(0)).unwrap_or(0));
        if wait > self.max_gate_wait {
            // Beyond the block ceiling: shed to a paceable error rather than hang. The permit
            // drops here (RAII), freeing the host for the next caller.
            return Err(RssError::RateLimited {
                url: url.to_string(),
                retry_after: wait,
            });
        }
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }

        // Sticky spacing: while the host is warm, reserve a gap before the *next* sibling so a
        // serialized batch is paced, not just serialized. No effect on a cold host.
        let now = now_ms();
        if slot.warm_until_ms.load(Ordering::Relaxed) > now {
            let spaced = now + ms(self.sticky_spacing);
            slot.next_allowed_ms.fetch_max(spaced, Ordering::Relaxed);
        }

        Ok(permit)
    }

    /// The cooldown to apply on a throttle. Honors the server's `retry_after` (capped above
    /// only — no lower floor, since sticky spacing already prevents re-hammering); when the
    /// server sent none, escalates `base · 2^(n-1)` with `n` = consecutive throttles, capped
    /// at `max_cooldown`. Pure (no clock, no state) so it is directly unit-tested.
    fn cooldown_for(&self, n: u32, retry_after: Option<Duration>) -> Duration {
        match retry_after {
            Some(d) => d.min(self.max_cooldown),
            None => {
                // `min(30)` only guards `1u32 << shift` from panicking (shift ≥ 32); the real
                // ceiling is `max_cooldown` via the `.min` below (saturating_mul handles the
                // Duration side).
                let shift = n.saturating_sub(1).min(30);
                self.base_cooldown
                    .saturating_mul(1u32 << shift)
                    .min(self.max_cooldown)
            }
        }
    }

    /// Record a `403`/`429` from `url`'s host: extend the sibling cooldown and mark the host
    /// warm. `retry_after` is the server's honored value (any form) when present; otherwise a
    /// bounded escalation of the base cooldown is used.
    pub fn note_throttled(&self, url: &str, retry_after: Option<Duration>) {
        let slot = self.slot_for_url(url);
        let n = slot.consecutive_throttles.fetch_add(1, Ordering::Relaxed) + 1;
        let cooldown = self.cooldown_for(n, retry_after);

        let now = now_ms();
        slot.next_allowed_ms
            .fetch_max(now + ms(cooldown), Ordering::Relaxed);
        slot.warm_until_ms
            .fetch_max(now + ms(self.warm_window), Ordering::Relaxed);
    }

    /// Record a non-throttled response from `url`'s host: reset the escalation counter so the
    /// next throttle starts from the base cooldown again. `warm_until` is left to decay on its
    /// own, so a host that just recovered keeps light spacing briefly.
    pub fn note_success(&self, url: &str) {
        self.slot_for_url(url)
            .consecutive_throttles
            .store(0, Ordering::Relaxed);
    }
}

/// Milliseconds of a `Duration` as `i64` (saturating — durations here are always small).
fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Parse an environment variable into `T`, or `None` if unset/blank/unparseable.
fn env_parse<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate() -> HostGate {
        HostGate::default()
    }

    #[test]
    fn authority_normalizes_host_and_port() {
        assert_eq!(
            authority_of("https://Example.com/feed.xml"),
            "example.com:443"
        );
        assert_eq!(authority_of("http://example.com/feed"), "example.com:80");
        assert_eq!(
            authority_of("https://example.com:8443/x"),
            "example.com:8443"
        );
        // Different hosts are different buckets; same host different path is the same bucket.
        assert_ne!(
            authority_of("https://a.example.com/x"),
            authority_of("https://b.example.com/x")
        );
        assert_eq!(
            authority_of("https://example.com/x"),
            authority_of("https://example.com/y")
        );
    }

    #[tokio::test]
    async fn cold_host_acquires_without_waiting() {
        let g = gate();
        // No prior throttle: acquire is immediate and sets no cooldown.
        let t0 = now_ms();
        let _p = g.acquire("https://example.com/feed").await.unwrap();
        assert!(now_ms() - t0 < 200, "cold acquire must not sleep");
    }

    #[tokio::test]
    async fn cap_one_serializes_same_authority() {
        let g = gate();
        let p1 = g.acquire("https://example.com/a").await.unwrap();
        // With permits=1 the second acquire cannot complete while p1 is held.
        let fut = g.acquire("https://example.com/b");
        tokio::pin!(fut);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut fut)
                .await
                .is_err(),
            "second same-host acquire should block while the first permit is held"
        );
        drop(p1);
        // Once released, it proceeds.
        assert!(fut.await.is_ok());
    }

    #[tokio::test]
    async fn distinct_authorities_do_not_block_each_other() {
        let g = gate();
        let _a = g.acquire("https://a.example.com/x").await.unwrap();
        // Different host → different slot → different semaphore → no contention.
        let b = tokio::time::timeout(
            Duration::from_millis(100),
            g.acquire("https://b.example.com/x"),
        )
        .await;
        assert!(
            b.is_ok(),
            "a distinct host must not be gated behind another"
        );
    }

    #[tokio::test]
    async fn throttle_makes_sibling_wait_but_not_a_different_host() {
        let g = gate();
        // Simulate a throttle with an explicit short Retry-After so the test stays fast.
        let t_throttled = now_ms();
        g.note_throttled(
            "https://slow.example.com/x",
            Some(Duration::from_millis(300)),
        );

        // A different host is unaffected (generous upper bound — a cold acquire is a no-op,
        // but keep slack so a loaded CI runner can't flake this).
        let t0 = now_ms();
        let _other = g.acquire("https://fast.example.com/y").await.unwrap();
        assert!(now_ms() - t0 < 300, "unrelated host must not wait");

        // The throttled host's next request waits out (roughly) the cooldown. Measure from
        // before the throttle was recorded so scheduler jitter in the intervening cold acquire
        // cannot eat into the lower bound and flake the test.
        let _same = g.acquire("https://slow.example.com/z").await.unwrap();
        let waited = now_ms() - t_throttled;
        assert!(
            (250..3000).contains(&waited),
            "throttled host should wait ~the cooldown, waited {waited}ms"
        );
    }

    #[test]
    fn cooldown_for_escalates_doubling_and_caps() {
        let g = HostGate {
            base_cooldown: Duration::from_secs(2),
            max_cooldown: Duration::from_secs(60),
            ..HostGate::default()
        };
        // Headerless: base · 2^(n-1), i.e. 2s, 4s, 8s, 16s, 32s, then clamped at 60s.
        assert_eq!(g.cooldown_for(1, None), Duration::from_secs(2));
        assert_eq!(g.cooldown_for(2, None), Duration::from_secs(4));
        assert_eq!(g.cooldown_for(3, None), Duration::from_secs(8));
        assert_eq!(g.cooldown_for(4, None), Duration::from_secs(16));
        assert_eq!(g.cooldown_for(5, None), Duration::from_secs(32));
        assert_eq!(g.cooldown_for(6, None), Duration::from_secs(60)); // 64s clamped to 60
        assert_eq!(g.cooldown_for(7, None), Duration::from_secs(60));
        // Large n must not panic on the shift and stays clamped (regression guard for the
        // shift-cap: only the max_cooldown ceiling should bind).
        assert_eq!(g.cooldown_for(1000, None), Duration::from_secs(60));
        // Honored Retry-After is capped above only (no lower floor) — sticky spacing is the floor.
        assert_eq!(
            g.cooldown_for(1, Some(Duration::from_millis(300))),
            Duration::from_millis(300)
        );
        assert_eq!(
            g.cooldown_for(9, Some(Duration::from_secs(600))),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn note_success_resets_escalation_counter() {
        let g = gate();
        let url = "https://esc.example.com/x";
        g.note_throttled(url, None);
        g.note_throttled(url, None);
        assert_eq!(
            g.slot_for("esc.example.com:443")
                .consecutive_throttles
                .load(Ordering::Relaxed),
            2
        );
        g.note_success(url);
        assert_eq!(
            g.slot_for("esc.example.com:443")
                .consecutive_throttles
                .load(Ordering::Relaxed),
            0,
            "a success must reset the escalation counter so the next throttle starts from base"
        );
    }

    #[tokio::test]
    async fn warm_host_spaces_the_next_sibling() {
        // A warm host must not just serialize — it must leave a gap before the NEXT sibling
        // (sticky spacing). Use a tiny cooldown + spacing so the test stays fast.
        let g = HostGate {
            sticky_spacing: Duration::from_millis(300),
            ..HostGate::default()
        };
        let url = "https://warm.example.com/x";
        // Mark warm with a near-zero cooldown so the first acquire drains it immediately but
        // the host stays warm (warm_until is far in the future).
        g.note_throttled(url, Some(Duration::from_millis(1)));
        let _first = g.acquire(url).await.unwrap(); // drains the ~1ms cooldown, reserves +spacing
        // The next sibling must wait ~sticky_spacing even though the cooldown is long gone.
        let t = now_ms();
        drop(_first);
        let _second = g.acquire(url).await.unwrap();
        let waited = now_ms() - t;
        assert!(
            (200..2000).contains(&waited),
            "a warm host should space the next sibling by ~sticky_spacing, waited {waited}ms"
        );
    }

    #[tokio::test]
    async fn zero_host_concurrency_is_floored_and_does_not_deadlock() {
        // A per_host of 0 would make Semaphore::new(0) block every acquire forever. HostSlot::new
        // floors permits to >=1; pin that guard (from_env applies the same floor to its env
        // input). We avoid mutating process env in a parallel test — it races concurrent getenv.
        let g = HostGate {
            per_host: 0,
            ..HostGate::default()
        };
        let acquired = tokio::time::timeout(
            Duration::from_millis(500),
            g.acquire("https://z.example.com/x"),
        )
        .await;
        assert!(
            acquired.is_ok(),
            "per_host must be floored to 1 so acquire never deadlocks"
        );
    }

    #[tokio::test]
    async fn wait_beyond_ceiling_fails_fast_with_rate_limited() {
        let g = HostGate {
            max_gate_wait: Duration::from_millis(100),
            max_cooldown: Duration::from_secs(60),
            ..HostGate::default()
        };
        let url = "https://busy.example.com/x";
        // A long honored Retry-After pushes next_allowed far beyond the 100ms ceiling.
        g.note_throttled(url, Some(Duration::from_secs(30)));

        let err = g.acquire(url).await.unwrap_err();
        match err {
            RssError::RateLimited { retry_after, .. } => {
                assert!(
                    retry_after >= Duration::from_secs(1),
                    "should report a real wait"
                );
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancelled_acquire_releases_the_permit() {
        let g = gate();
        let p1 = g.acquire("https://cx.example.com/a").await.unwrap();
        // A second acquire that we cancel (drop the future) must not leak the (not-yet-held)
        // permit; and once p1 drops the host is usable again.
        {
            let fut = g.acquire("https://cx.example.com/b");
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(50), &mut fut).await;
            // `fut` dropped here (cancelled while waiting on the semaphore).
        }
        drop(p1);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(200),
                g.acquire("https://cx.example.com/c")
            )
            .await
            .is_ok(),
            "host must be acquirable after a cancelled waiter and a released permit"
        );
    }
}
