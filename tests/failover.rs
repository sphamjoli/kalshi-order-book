//! Failover between feeds, driven through real collectors.

use engineering_challenge::Side;
use engineering_challenge::collector::{BookRecord, Collector, CollectorOutput};
use engineering_challenge::domain::{Level, MarketEvent, Price, Quantity, QuantityDelta};
use engineering_challenge::failover::{Failover, FeedId};
use std::time::{Duration, Instant};

const PRIMARY: FeedId = FeedId(0);
const BACKUP: FeedId = FeedId(1);

fn collector() -> Collector {
    Collector::new(["M".to_owned()], Duration::from_secs(5))
}

fn snapshot_event(quantity: i64) -> MarketEvent {
    MarketEvent::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: 1,
        exchange_clock: None,
        bids: vec![Level {
            price: Price::try_new(400_000).unwrap(),
            quantity: Quantity::try_new(quantity).unwrap(),
        }],
        asks: vec![],
    }
}

fn delta_event(time: i64) -> MarketEvent {
    MarketEvent::Delta {
        symbol: "M".to_owned(),
        exchange_time_ms: time,
        exchange_clock: None,
        side: Side::Buy,
        price: Price::try_new(400_000).unwrap(),
        delta: QuantityDelta::new(1_000_000),
    }
}

/// Applies `event` and returns what would be recorded.
fn feed(
    failover: &mut Failover,
    id: FeedId,
    collector: &mut Collector,
    event: MarketEvent,
) -> Vec<BookRecord> {
    let outputs = collector.handle(event, Instant::now());
    outputs
        .iter()
        .filter_map(|output| match output {
            CollectorOutput::Record(record) => failover.route(id, collector, record),
            CollectorOutput::Resync(_) => None,
        })
        .collect()
}

fn released() -> Vec<String> {
    vec!["M".to_owned()]
}

fn is_snapshot(records: &[BookRecord]) -> bool {
    matches!(records, [BookRecord::Snapshot { .. }])
}

fn is_update(records: &[BookRecord]) -> bool {
    matches!(records, [BookRecord::Update { .. }])
}

/// Two live feeds, with the primary owning the market.
fn running() -> (Failover, Collector, Collector) {
    let (mut failover, mut primary, mut backup) = (Failover::default(), collector(), collector());
    assert!(is_snapshot(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        snapshot_event(1_000_000)
    )));
    assert!(
        feed(
            &mut failover,
            BACKUP,
            &mut backup,
            snapshot_event(1_000_000)
        )
        .is_empty()
    );
    (failover, primary, backup)
}

#[test]
fn only_the_owner_is_recorded() {
    let (mut failover, mut primary, mut backup) = running();
    assert!(is_update(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        delta_event(2)
    )));
    assert!(feed(&mut failover, BACKUP, &mut backup, delta_event(2)).is_empty());
}

#[test]
fn a_switch_records_a_snapshot_then_deltas() {
    let (mut failover, _primary, mut backup) = running();
    failover.release(PRIMARY, &released());
    let first = feed(&mut failover, BACKUP, &mut backup, delta_event(2));
    let [BookRecord::Snapshot { bids, .. }] = first.as_slice() else {
        panic!("a switch must start with a snapshot, got {first:?}");
    };
    // The snapshot already holds the delta that triggered the takeover.
    assert_eq!(bids[0].quantity.raw(), 2_000_000);
    assert!(is_update(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        delta_event(3)
    )));
}

#[test]
fn a_recovered_feed_stays_on_standby() {
    let (mut failover, mut primary, mut backup) = running();
    failover.release(PRIMARY, &released());
    feed(&mut failover, BACKUP, &mut backup, delta_event(2));
    assert!(
        feed(
            &mut failover,
            PRIMARY,
            &mut primary,
            snapshot_event(5_000_000)
        )
        .is_empty()
    );
}

#[test]
fn a_standby_loss_leaves_the_owner_in_place() {
    let (mut failover, mut primary, _backup) = running();
    failover.release(BACKUP, &released());
    assert!(is_update(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        delta_event(2)
    )));
}

fn clocked_snapshot(clock: u64) -> MarketEvent {
    MarketEvent::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: 1,
        exchange_clock: Some(clock),
        bids: vec![],
        asks: vec![],
    }
}

fn clocked_delta(clock: u64) -> MarketEvent {
    MarketEvent::Delta {
        symbol: "M".to_owned(),
        exchange_time_ms: 2,
        exchange_clock: Some(clock),
        side: Side::Buy,
        price: Price::try_new(400_000).unwrap(),
        delta: QuantityDelta::new(1_000_000),
    }
}

/// Two live feeds with clocks, with the primary owning the market.
fn clocked() -> (Failover, Collector, Collector) {
    let (mut failover, mut primary, mut backup) = (Failover::default(), collector(), collector());
    feed(&mut failover, PRIMARY, &mut primary, clocked_snapshot(10));
    feed(&mut failover, BACKUP, &mut backup, clocked_snapshot(10));
    (failover, primary, backup)
}

#[test]
fn the_first_copy_of_a_change_is_recorded_from_either_feed() {
    let (mut failover, mut primary, mut backup) = clocked();
    assert!(is_update(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        clocked_delta(11)
    )));
    assert!(feed(&mut failover, PRIMARY, &mut primary, clocked_delta(11)).is_empty());
    assert!(is_update(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        clocked_delta(12)
    )));
    assert!(feed(&mut failover, BACKUP, &mut backup, clocked_delta(12)).is_empty());
}

#[test]
fn changes_sharing_a_clock_are_each_recorded_once() {
    let (mut failover, mut primary, mut backup) = clocked();
    for _ in 0..2 {
        assert!(is_update(&feed(
            &mut failover,
            PRIMARY,
            &mut primary,
            clocked_delta(11)
        )));
    }
    for _ in 0..2 {
        assert!(feed(&mut failover, BACKUP, &mut backup, clocked_delta(11)).is_empty());
    }
    assert!(is_update(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        clocked_delta(12)
    )));
}

#[test]
fn a_stalled_owner_loses_nothing() {
    let (mut failover, _stalled, mut backup) = clocked();
    for clock in 11..15 {
        assert!(is_update(&feed(
            &mut failover,
            BACKUP,
            &mut backup,
            clocked_delta(clock)
        )));
    }
}

#[test]
fn a_standby_snapshot_ahead_of_the_record_catches_it_up() {
    let (mut failover, _primary, mut backup) = clocked();
    assert!(is_snapshot(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        clocked_snapshot(20)
    )));
}

#[test]
fn a_standby_snapshot_level_with_the_record_is_not_repeated() {
    let (mut failover, primary, backup) = clocked();
    let record = |collector: &Collector| collector.snapshot("M").unwrap();
    assert!(
        failover
            .route(PRIMARY, &primary, &record(&primary))
            .is_some()
    );
    assert!(failover.route(BACKUP, &backup, &record(&backup)).is_none());
}

#[test]
fn a_clock_that_falls_restarts_the_market() {
    let (mut failover, mut primary, _backup) = clocked();
    feed(&mut failover, PRIMARY, &mut primary, clocked_delta(11));
    assert!(is_snapshot(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        clocked_delta(3)
    )));
    assert!(is_update(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        clocked_delta(4)
    )));
}

#[test]
fn a_switch_with_clocks_records_a_snapshot_then_deltas() {
    let (mut failover, _primary, mut backup) = clocked();
    failover.release(PRIMARY, &released());
    assert!(is_snapshot(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        clocked_delta(11)
    )));
    assert!(is_update(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        clocked_delta(12)
    )));
}

#[test]
fn a_late_copy_of_a_change_inside_a_snapshot_is_dropped() {
    let (mut failover, _primary, mut backup) = clocked();
    assert!(feed(&mut failover, BACKUP, &mut backup, clocked_delta(10)).is_empty());
}

#[test]
fn a_standby_minute_snapshot_after_a_delta_is_not_recorded() {
    let (mut failover, mut primary, mut backup) = clocked();
    feed(&mut failover, PRIMARY, &mut primary, clocked_delta(11));
    feed(&mut failover, BACKUP, &mut backup, clocked_delta(11));
    let snapshot = backup.snapshot("M").unwrap();
    assert!(failover.route(BACKUP, &backup, &snapshot).is_none());
}

#[test]
fn a_stalled_owner_cannot_cost_a_market_its_minute_snapshot() {
    let (mut failover, mut primary, mut backup) = clocked();
    feed(&mut failover, BACKUP, &mut backup, clocked_delta(11));
    let stale = primary.snapshot("M").unwrap();
    assert!(failover.route(PRIMARY, &primary, &stale).is_none());
    let current = backup.snapshot("M").unwrap();
    assert!(failover.route(BACKUP, &backup, &current).is_some());
    // The standby now owns the market, so its next snapshot also passes.
    assert!(failover.route(BACKUP, &backup, &current).is_some());
    // The old owner is now a standby, so its snapshot is not recorded.
    feed(&mut failover, PRIMARY, &mut primary, clocked_delta(11));
    let caught_up = primary.snapshot("M").unwrap();
    assert!(failover.route(PRIMARY, &primary, &caught_up).is_none());
}

/// Leaves the backup behind the record with no owner, after the primary saw
/// clock 11 and gave the market up.
fn owner_lost_with_backup_behind(failover: &mut Failover) -> (Collector, Collector) {
    let (mut primary, mut backup) = (collector(), collector());
    feed(failover, PRIMARY, &mut primary, clocked_snapshot(10));
    feed(failover, BACKUP, &mut backup, clocked_snapshot(10));
    feed(failover, PRIMARY, &mut primary, clocked_delta(11));
    failover.release(PRIMARY, &released());
    (primary, backup)
}

#[test]
fn a_takeover_waits_until_the_feed_reaches_the_record() {
    let mut failover = Failover::default();
    let (_primary, mut backup) = owner_lost_with_backup_behind(&mut failover);
    let behind = feed(&mut failover, BACKUP, &mut backup, clocked_delta(10));
    assert!(behind.is_empty(), "the book must not move back in time");
    let level = feed(&mut failover, BACKUP, &mut backup, clocked_delta(11));
    assert!(is_snapshot(&level));
}

#[test]
fn a_takeover_stops_waiting_for_a_clock_that_was_reset() {
    let mut failover = Failover::with_takeover_wait(Duration::ZERO);
    let (_primary, mut backup) = owner_lost_with_backup_behind(&mut failover);
    let records = feed(&mut failover, BACKUP, &mut backup, clocked_delta(10));
    assert!(is_snapshot(&records));
}
