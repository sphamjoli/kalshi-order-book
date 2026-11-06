//! Exchange-independent connection rules: URL safety and retry delays.

use rand::RngExt;
use std::time::Duration;
use url::Url;

/// Returns whether a URL protects its traffic. Plain schemes are allowed only
/// on loopback, for local test servers.
pub fn is_secure(url: &Url) -> bool {
    matches!(url.scheme(), "https" | "wss")
        || matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"))
}

/// Adds up to half of `delay` at random, so sockets that failed together do
/// not all retry together.
pub fn with_jitter(delay: Duration) -> Duration {
    delay + delay.mul_f64(rand::rng().random_range(0.0..0.5))
}

/// Returns a random delay of up to `delay`, which spreads a crowd of retries
/// the most widely.
pub fn full_jitter(delay: Duration) -> Duration {
    delay.mul_f64(rand::rng().random_range(0.0..1.0))
}

/// Returns a random delay between `floor` and `delay`. Retries spread widely,
/// and the floor stops a failing peer being retried at once.
pub fn spread(delay: Duration, floor: Duration) -> Duration {
    floor + full_jitter(delay.saturating_sub(floor))
}

/// Returns the attempt number after a connection that lasted `uptime`. Only a
/// connection that lasted `stable_after` counts as recovery, so one that drops
/// right after its acknowledgement keeps backing off.
pub fn next_attempt(attempt: u32, uptime: Duration, stable_after: Duration) -> u32 {
    if uptime >= stable_after {
        0
    } else {
        attempt.saturating_add(1)
    }
}

/// Returns the delay before retry `attempt`, doubling from `base` up to `max`.
pub fn backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    base.saturating_mul(2u32.saturating_pow(attempt)).min(max)
}
