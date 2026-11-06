use engineering_challenge::transport::{backoff, full_jitter, next_attempt, spread};
use std::time::Duration;

const BASE: Duration = Duration::from_secs(1);
const MAX: Duration = Duration::from_secs(60);

#[test]
fn delay_doubles_up_to_the_cap() {
    assert_eq!(backoff(0, BASE, MAX), BASE);
    assert_eq!(backoff(1, BASE, MAX), Duration::from_secs(2));
    assert_eq!(backoff(3, BASE, MAX), Duration::from_secs(8));
    assert_eq!(backoff(6, BASE, MAX), MAX);
    assert_eq!(backoff(u32::MAX, BASE, MAX), MAX);
}

#[test]
fn full_jitter_stays_within_the_delay() {
    let delay = Duration::from_secs(1);
    assert!((0..100).all(|_| full_jitter(delay) < delay));
}

#[test]
fn spread_stays_between_the_floor_and_the_delay() {
    let (floor, delay) = (Duration::from_millis(100), Duration::from_secs(1));
    assert!((0..100).all(|_| (floor..=delay).contains(&spread(delay, floor))));
    assert_eq!(spread(floor, delay), delay, "the floor wins");
}

#[test]
fn only_a_lasting_connection_resets_the_attempts() {
    assert_eq!(next_attempt(3, Duration::from_secs(1), MAX), 4);
    assert_eq!(next_attempt(3, MAX, MAX), 0);
}
