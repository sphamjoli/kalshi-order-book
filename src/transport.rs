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

/// Returns the delay before retry `attempt`, doubling from `base` up to `max`.
pub fn backoff(attempt: u32, base: Duration, max: Duration) -> Duration {
    base.saturating_mul(2u32.saturating_pow(attempt)).min(max)
}
