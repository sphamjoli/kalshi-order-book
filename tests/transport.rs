use engineering_challenge::transport::backoff;
use std::time::Duration;

const BASE: Duration = Duration::from_secs(1);
const MAX: Duration = Duration::from_secs(60);

#[test]
fn delay_doubles_from_the_base() {
    assert_eq!(backoff(0, BASE, MAX), BASE);
    assert_eq!(backoff(1, BASE, MAX), Duration::from_secs(2));
    assert_eq!(backoff(3, BASE, MAX), Duration::from_secs(8));
}

#[test]
fn delay_stops_at_the_cap_even_for_huge_attempts() {
    assert_eq!(backoff(6, BASE, MAX), MAX);
    assert_eq!(backoff(u32::MAX, BASE, MAX), MAX);
}
