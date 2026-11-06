//! A second provider, built only from the public contracts, runs through the
//! real tracker and manager.

use chrono::{DateTime, Utc};
use engineering_challenge::collector::{Collector, CollectorOutput};
use engineering_challenge::discovery::MarketTracker;
use engineering_challenge::manager::SocketManager;
use engineering_challenge::provider::{
    FeedHandler, MarketChanges, MarketDirectory, MarketStream, StreamCommand, UpcomingMarket,
};
use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{sleep, timeout};

const POLL: Duration = Duration::from_millis(10);
const SETTLE: Duration = Duration::from_millis(100);

struct FakeDirectory;

impl MarketDirectory for FakeDirectory {
    type Error = Infallible;

    async fn scan_open(
        &self,
        mut on_page: impl FnMut(Vec<String>) -> ControlFlow<()>,
    ) -> Result<(), Infallible> {
        let _ = on_page(vec!["A".to_owned(), "B".to_owned(), "C".to_owned()]);
        Ok(())
    }

    async fn changes_since(&self, _: i64) -> Result<MarketChanges, Infallible> {
        Ok(MarketChanges {
            opened: vec!["D".to_owned()],
            closed: vec!["A".to_owned()],
            upcoming: vec![UpcomingMarket {
                symbol: "SOON".to_owned(),
                opens_at: 0,
            }],
        })
    }
}

/// Records what each socket was asked to follow.
#[derive(Clone, Default)]
struct FakeStream {
    markets: Vec<String>,
    started: Arc<Mutex<Vec<Vec<String>>>>,
}

impl MarketStream for FakeStream {
    const CONNECTION_LIMIT: usize = 6;

    fn for_markets(&self, markets: Vec<String>) -> Self {
        Self {
            markets,
            started: self.started.clone(),
        }
    }

    async fn run<H: FeedHandler + Send>(
        &self,
        mut commands: UnboundedReceiver<StreamCommand>,
        _handler: H,
    ) {
        let starts = {
            let mut started = self.started.lock().unwrap();
            started.push(self.markets.clone());
            started.len()
        };
        assert!(
            starts > 1 || !self.markets.contains(&"BOOM".to_owned()),
            "the first start of this market fails"
        );
        while commands.recv().await.is_some() {}
    }
}

#[tokio::test]
async fn a_new_provider_needs_only_the_contracts() {
    let stream = FakeStream::default();
    let started = stream.started.clone();
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(stream, 2, vec![handler, handler]);
    let tracker = MarketTracker::new(&FakeDirectory, POLL, Duration::from_secs(3600));
    let _ = timeout(SETTLE, tracker.run(&mut manager, None)).await;

    let mut placed: Vec<_> = manager.placed().into_iter().collect();
    placed.sort();
    assert_eq!(placed, ["B", "C", "D", "SOON"]);
    assert_eq!(manager.socket_count(), 4);
    sleep(POLL).await;
    let markets: usize = started.lock().unwrap().iter().map(Vec::len).sum();
    assert_eq!(markets, 6, "both feeds start every first-scan market");
    manager.close().await;
}

#[tokio::test]
async fn a_limited_run_stays_within_its_limit() {
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(FakeStream::default(), 10, vec![handler]);
    let tracker = MarketTracker::new(&FakeDirectory, POLL, Duration::from_secs(3600));
    let _ = timeout(SETTLE, tracker.run(&mut manager, Some(3))).await;
    assert_eq!(manager.market_count(), 3);
    manager.close().await;
}

#[tokio::test]
async fn sockets_never_pass_the_provider_limit() {
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(FakeStream::default(), 1, vec![handler, handler]);
    manager.add((0..20).map(|market| market.to_string()).collect());
    assert_eq!(manager.socket_count(), FakeStream::CONNECTION_LIMIT);
    assert_eq!(manager.market_count(), 20, "no market is left out");
    manager.close().await;
}

/// Remembers which markets its stream gave up.
#[derive(Clone, Default)]
struct ReleaseLog(Arc<Mutex<Vec<String>>>);

impl FeedHandler for ReleaseLog {
    fn on_output(&mut self, _: &Collector, _: &CollectorOutput, _: DateTime<Utc>) {}

    fn on_released(&mut self, symbols: &[String]) {
        self.0.lock().unwrap().extend_from_slice(symbols);
    }
}

#[tokio::test]
async fn a_panicked_socket_gives_up_its_markets_and_restarts_with_them() {
    let stream = FakeStream::default();
    let started = stream.started.clone();
    let log = ReleaseLog::default();
    let mut manager = SocketManager::new(stream, 10, vec![log.clone()]);
    manager.add(vec!["BOOM".to_owned()]);
    let tracker = MarketTracker::new(&FakeDirectory, POLL, Duration::from_secs(3600));
    // A limit of zero leaves only the polling loop, which finds the panic.
    let _ = timeout(SETTLE, tracker.run(&mut manager, Some(0))).await;

    assert_eq!(*started.lock().unwrap(), [["BOOM"], ["BOOM"]]);
    assert_eq!(*log.0.lock().unwrap(), ["BOOM"]);
    manager.close().await;
}

#[tokio::test]
async fn placement_balances_sockets_and_reuses_freed_room() {
    let stream = FakeStream::default();
    let started = stream.started.clone();
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(stream, 2, vec![handler]);
    let markets = |names: &[&str]| names.iter().map(ToString::to_string).collect::<Vec<_>>();

    manager.add(markets(&["A", "B", "C"]));
    assert_eq!(manager.socket_count(), 2);
    manager.add(markets(&["A"]));
    assert_eq!(manager.market_count(), 3, "adding twice changes nothing");
    manager.remove(markets(&["A", "B", "C"]));
    assert_eq!(manager.market_count(), 0);
    manager.add(markets(&["D", "E", "F", "G"]));
    assert_eq!(manager.socket_count(), 2, "freed room is used first");
    assert_eq!(manager.market_count(), 4);

    sleep(POLL).await;
    let mut sizes: Vec<_> = started.lock().unwrap().iter().map(Vec::len).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, [1, 2]);
    manager.close().await;
}

/// Hangs on its second scan, and places `BOOM` once that scan has begun.
/// Its first listing of upcoming markets fails.
#[derive(Default)]
struct FlakyDirectory {
    scans: AtomicUsize,
    listings: AtomicUsize,
}

impl MarketDirectory for FlakyDirectory {
    type Error = std::io::Error;

    async fn scan_open(
        &self,
        _: impl FnMut(Vec<String>) -> ControlFlow<()>,
    ) -> Result<(), Self::Error> {
        if self.scans.fetch_add(1, Ordering::SeqCst) > 0 {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn changes_since(&self, _: i64) -> Result<MarketChanges, Self::Error> {
        let rescanning = self.scans.load(Ordering::SeqCst) > 1;
        Ok(MarketChanges {
            opened: rescanning.then(|| "BOOM".to_owned()).into_iter().collect(),
            closed: vec![],
            upcoming: vec![],
        })
    }

    async fn upcoming_markets(&self) -> Result<Vec<UpcomingMarket>, Self::Error> {
        if self.listings.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(std::io::Error::other("listing failed"));
        }
        Ok(vec![UpcomingMarket {
            symbol: "SOON".to_owned(),
            opens_at: 0,
        }])
    }
}

#[tokio::test]
async fn a_socket_that_fails_during_a_rescan_restarts_without_waiting_for_it() {
    let stream = FakeStream::default();
    let started = stream.started.clone();
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(stream, 10, vec![handler]);
    let directory = FlakyDirectory::default();
    let tracker = MarketTracker::new(&directory, POLL, Duration::ZERO);
    let _ = timeout(SETTLE, tracker.run(&mut manager, None)).await;
    let starts = started.lock().unwrap().clone();
    let boom = starts
        .iter()
        .filter(|markets| markets.contains(&"BOOM".to_owned()));
    assert!(boom.count() >= 2, "{starts:?}");
    manager.close().await;
}

#[tokio::test]
async fn a_failed_upcoming_listing_is_tried_again_on_a_limited_run() {
    let handler = |_: &Collector, _: &CollectorOutput, _| {};
    let mut manager = SocketManager::new(FakeStream::default(), 10, vec![handler]);
    let directory = FlakyDirectory::default();
    let tracker = MarketTracker::new(&directory, POLL, Duration::from_secs(3600));
    let _ = timeout(SETTLE, tracker.run(&mut manager, Some(1))).await;
    assert!(manager.placed().contains("SOON"));
    manager.close().await;
}
