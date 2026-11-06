//! One feed's route to the writer queue.

use chrono::Utc;
use engineering_challenge::collector::{Collector, CollectorOutput};
use engineering_challenge::domain::MarketEvent;
use engineering_challenge::failover::{Failover, FeedId};
use engineering_challenge::output::OutputRecord;
use engineering_challenge::provider::FeedHandler;
use engineering_challenge::router::FeedRouter;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, channel};
use tokio_util::sync::CancellationToken;

fn live_collector() -> (Collector, Vec<CollectorOutput>) {
    let mut collector = Collector::new(["M".to_owned()], Duration::from_secs(5));
    let snapshot = MarketEvent::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: 1,
        exchange_clock: None,
        bids: vec![],
        asks: vec![],
    };
    let outputs = collector.handle(snapshot, Instant::now());
    (collector, outputs)
}

fn routers() -> (
    FeedRouter,
    FeedRouter,
    Receiver<OutputRecord>,
    CancellationToken,
) {
    let (sender, receiver) = channel(8);
    let failover = Arc::new(Mutex::new(Failover::default()));
    let shutdown = CancellationToken::new();
    let router = |id| {
        FeedRouter::new(
            FeedId(id),
            failover.clone(),
            sender.clone(),
            shutdown.clone(),
        )
    };
    (router(0), router(1), receiver, shutdown)
}

#[tokio::test]
async fn only_the_owner_reaches_the_queue() {
    let (mut first, mut second, mut records, _shutdown) = routers();
    let (collector, outputs) = live_collector();
    first.on_output(&collector, &outputs[0], Utc::now());
    second.on_output(&collector, &outputs[0], Utc::now());
    assert!(records.try_recv().is_ok());
    assert!(records.try_recv().is_err());
}

#[tokio::test]
async fn a_stopped_writer_stops_the_run() {
    let (mut first, _second, records, shutdown) = routers();
    drop(records);
    let (collector, outputs) = live_collector();
    first.on_output(&collector, &outputs[0], Utc::now());
    assert!(shutdown.is_cancelled());
}

#[tokio::test]
async fn a_release_or_a_resync_hands_the_market_to_the_other_feed() {
    let resync = CollectorOutput::Resync(vec!["M".to_owned()]);
    for use_resync in [false, true] {
        let (mut first, mut second, mut records, _shutdown) = routers();
        let (collector, outputs) = live_collector();
        first.on_output(&collector, &outputs[0], Utc::now());
        if use_resync {
            first.on_output(&collector, &resync, Utc::now());
        } else {
            first.on_released(&["M".to_owned()]);
        }
        second.on_output(&collector, &outputs[0], Utc::now());
        assert!(records.try_recv().is_ok());
        assert!(records.try_recv().is_ok(), "the second feed must take over");
    }
}
