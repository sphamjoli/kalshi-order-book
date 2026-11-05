//! Failover between feeds, driven through real collectors.

use engineering_challenge::Side;
use engineering_challenge::collector::{Collector, CollectorOutput};
use engineering_challenge::domain::{Level, MarketEvent, Price, Quantity, QuantityDelta};
use engineering_challenge::failover::Failover;
use std::time::{Duration, Instant};

const PRIMARY: usize = 0;
const BACKUP: usize = 1;

fn collector() -> Collector {
    Collector::new(["M".to_owned()], Duration::from_secs(5))
}

fn snapshot_event(quantity: i64) -> MarketEvent {
    MarketEvent::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: 1,
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
        side: Side::Buy,
        price: Price::try_new(400_000).unwrap(),
        delta: QuantityDelta::new(1_000_000),
    }
}

/// Applies `event` and returns what would be recorded.
fn feed(
    failover: &mut Failover,
    id: usize,
    collector: &mut Collector,
    event: MarketEvent,
) -> Vec<CollectorOutput> {
    let outputs = collector.handle(event, Instant::now());
    outputs
        .iter()
        .filter_map(|output| failover.route(id, collector, output))
        .collect()
}

fn released() -> CollectorOutput {
    CollectorOutput::Released(vec!["M".to_owned()])
}

fn is_snapshot(records: &[CollectorOutput]) -> bool {
    matches!(records, [CollectorOutput::Snapshot { .. }])
}

fn is_update(records: &[CollectorOutput]) -> bool {
    matches!(records, [CollectorOutput::Update { .. }])
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
    let (mut failover, primary, mut backup) = running();
    assert!(failover.route(PRIMARY, &primary, &released()).is_none());
    let first = feed(&mut failover, BACKUP, &mut backup, delta_event(2));
    let [CollectorOutput::Snapshot { bids, .. }] = first.as_slice() else {
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
fn a_resync_gives_up_the_market() {
    let (mut failover, mut primary, mut backup) = running();
    let lost = feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        MarketEvent::ContinuityLost,
    );
    assert!(lost.is_empty());
    assert!(is_snapshot(&feed(
        &mut failover,
        BACKUP,
        &mut backup,
        delta_event(2)
    )));
}

#[test]
fn a_recovered_feed_stays_on_standby() {
    let (mut failover, mut primary, mut backup) = running();
    failover.route(PRIMARY, &primary, &released());
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
    let (mut failover, mut primary, backup) = running();
    failover.route(BACKUP, &backup, &released());
    assert!(is_update(&feed(
        &mut failover,
        PRIMARY,
        &mut primary,
        delta_event(2)
    )));
}
