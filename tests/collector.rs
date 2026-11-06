//! Book lifecycle rules, driven with normalised events.

use engineering_challenge::Side;
use engineering_challenge::collector::{BookRecord, Collector, CollectorOutput};
use engineering_challenge::domain::{Level, MarketEvent, Price, Quantity, QuantityDelta};
use std::time::{Duration, Instant};

const GRACE: Duration = Duration::from_secs(5);

fn collector() -> Collector {
    Collector::new(["A".to_owned(), "A".to_owned(), "B".to_owned()], GRACE)
}

fn level(price: i64) -> Level {
    Level {
        price: Price::try_new(price).unwrap(),
        quantity: Quantity::try_new(5).unwrap(),
    }
}

fn snapshot(symbol: &str, bids: Vec<Level>, asks: Vec<Level>) -> MarketEvent {
    MarketEvent::Snapshot {
        symbol: symbol.to_owned(),
        exchange_time_ms: 1,
        exchange_clock: None,
        bids,
        asks,
    }
}

fn delta(symbol: &str, side: Side, price: i64, change: i64) -> MarketEvent {
    MarketEvent::Delta {
        symbol: symbol.to_owned(),
        exchange_time_ms: 1,
        exchange_clock: None,
        side,
        price: Price::try_new(price).unwrap(),
        delta: QuantityDelta::new(change),
    }
}

fn resync(symbols: &[&str]) -> Vec<CollectorOutput> {
    vec![CollectorOutput::Resync(
        symbols.iter().map(|s| s.to_string()).collect(),
    )]
}

#[test]
fn a_delta_before_the_first_snapshot_is_dropped() {
    let now = Instant::now();
    assert!(
        collector()
            .handle(delta("A", Side::Buy, 40, 1), now)
            .is_empty()
    );
}

#[test]
fn a_contradicting_delta_resyncs_only_that_market() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    let _ = collector.handle(snapshot("B", vec![level(40)], vec![]), now);
    let outputs = collector.handle(delta("A", Side::Buy, 40, -9), now);
    assert_eq!(outputs, resync(&["A"]));
    let live: Vec<_> = collector.live_books().map(|(symbol, _)| symbol).collect();
    assert_eq!(live, ["B"]);
}

#[test]
fn deltas_during_a_resync_are_dropped_until_the_snapshot() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    let _ = collector.handle(MarketEvent::ContinuityLost, now);
    assert!(
        collector
            .handle(delta("A", Side::Buy, 40, 1), now)
            .is_empty()
    );
    let _ = collector.handle(snapshot("A", vec![level(41)], vec![]), now);
    assert_eq!(collector.live_books().count(), 1);
}

#[test]
fn a_rejected_snapshot_leaves_the_market_unpublished() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    let outputs = collector.handle(snapshot("A", vec![level(41), level(41)], vec![]), now);
    assert_eq!(outputs, resync(&["A"]));
    assert_eq!(collector.live_books().count(), 0);
}

#[test]
fn a_snapshot_for_an_unknown_market_is_ignored() {
    let now = Instant::now();
    let mut collector = collector();
    assert!(
        collector
            .handle(snapshot("Z", vec![], vec![]), now)
            .is_empty()
    );
    assert_eq!(collector.live_books().count(), 0);
}

#[test]
fn a_book_crossed_past_the_grace_period_is_resynced() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![level(45)]), now);
    let _ = collector.handle(delta("A", Side::Sell, 39, 1), now);
    assert!(
        collector
            .tick(now + GRACE - Duration::from_millis(1))
            .is_empty()
    );
    assert_eq!(collector.tick(now + GRACE), resync(&["A"]));
    assert_eq!(collector.live_books().count(), 0);
}

#[test]
fn a_crossing_that_clears_is_forgotten() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![level(45)]), now);
    let _ = collector.handle(delta("A", Side::Sell, 39, 1), now);
    let _ = collector.handle(delta("A", Side::Sell, 39, -1), now + Duration::from_secs(1));
    assert!(collector.tick(now + GRACE * 2).is_empty());
}

#[test]
fn the_grace_period_runs_from_the_first_crossing() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![level(45)]), now);
    let _ = collector.handle(delta("A", Side::Sell, 39, 1), now);
    let _ = collector.handle(delta("A", Side::Sell, 39, 1), now + Duration::from_secs(4));
    assert_eq!(collector.tick(now + GRACE), resync(&["A"]));
}

#[test]
fn a_crossed_snapshot_starts_the_grace_period() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(45)], vec![level(40)]), now);
    assert_eq!(collector.tick(now + GRACE), resync(&["A"]));
}

#[test]
fn a_delta_for_an_unknown_market_invalidates_live_books() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    let outputs = collector.handle(delta("Z", Side::Buy, 40, 1), now);
    assert_eq!(outputs, resync(&["A"]));
}

#[test]
fn repeated_continuity_loss_requests_each_market_once() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    assert_eq!(
        collector.handle(MarketEvent::ContinuityLost, now),
        resync(&["A"])
    );
    assert!(
        collector
            .handle(MarketEvent::ContinuityLost, now)
            .is_empty()
    );
}

#[test]
fn snapshots_cover_quiet_live_books_and_skip_the_rest() {
    let now = Instant::now();
    let mut collector = collector();
    let _ = collector.handle(snapshot("A", vec![level(40)], vec![]), now);
    let _ = collector.handle(delta("A", Side::Buy, 40, 1), now);
    let snapshots = collector.snapshots();
    let [CollectorOutput::Record(BookRecord::Snapshot { symbol, bids, .. })] = &snapshots[..]
    else {
        panic!("expected only A, got {snapshots:?}");
    };
    assert_eq!(symbol, "A");
    assert_eq!(bids[0].quantity.raw(), 6);
}

#[test]
fn a_book_past_the_output_limit_is_resynced() {
    let mut collector = collector();
    let now = Instant::now();
    let _ = collector.handle(snapshot("A", vec![], vec![]), now);
    for price in 1..=65_535 {
        let outputs = collector.handle(delta("A", Side::Buy, price, 5), now);
        assert!(matches!(
            outputs[..],
            [CollectorOutput::Record(BookRecord::Update { .. })]
        ));
    }
    let outputs = collector.handle(delta("A", Side::Buy, 65_536, 5), now);
    assert_eq!(outputs, [CollectorOutput::Resync(vec!["A".to_owned()])]);
}

#[test]
fn a_snapshot_carries_the_clock_of_the_latest_delta() {
    let mut collector = Collector::new(["A".to_owned()], Duration::from_secs(5));
    let snapshot = MarketEvent::Snapshot {
        symbol: "A".to_owned(),
        exchange_time_ms: 1,
        exchange_clock: Some(5),
        bids: vec![],
        asks: vec![],
    };
    let delta = MarketEvent::Delta {
        symbol: "A".to_owned(),
        exchange_time_ms: 2,
        exchange_clock: Some(9),
        side: Side::Buy,
        price: Price::try_new(400_000).unwrap(),
        delta: QuantityDelta::new(1_000_000),
    };
    let _ = collector.handle(snapshot, Instant::now());
    assert_eq!(collector.snapshot("A").unwrap().exchange_clock(), Some(5));
    let _ = collector.handle(delta, Instant::now());
    assert_eq!(collector.snapshot("A").unwrap().exchange_clock(), Some(9));
}

#[test]
fn a_snapshot_past_the_output_limit_is_not_published() {
    let mut collector = collector();
    let levels = (1..=65_536).map(level).collect();
    let outputs = collector.handle(snapshot("A", levels, vec![]), Instant::now());
    assert_eq!(outputs, resync(&["A"]));
    assert_eq!(collector.live_books().count(), 0);
}
