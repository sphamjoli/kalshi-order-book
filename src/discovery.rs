//! Market tracking: finds markets and updates the socket manager as they open
//! and close. Short time-filtered polls report changes, and a periodic full
//! scan finds what they miss. Removal needs a closed listing or a complete
//! scan, so a failed scan removes nothing.

use crate::manager::SocketManager;
use crate::provider::{
    MarketChanges, MarketDirectory, MarketStream, StreamHandler, UpcomingMarket,
};
use crate::transport;
use chrono::Utc;
use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

/// Overlap between polls, so a market on the boundary is not missed. Adding
/// or removing a market twice changes nothing.
const POLL_OVERLAP_SECONDS: i64 = 60;
/// A rescan that finds fewer than 1/`MIN_RESCAN_FRACTION` of the placed markets
/// removes nothing.
const MIN_RESCAN_FRACTION: usize = 2;
const MAX_SCAN_RETRY_DELAY: Duration = Duration::from_secs(60);

/// The state carried between polls.
struct PollState {
    since: i64,
    /// Listed markets waiting to open, by opening time.
    upcoming: BTreeMap<i64, Vec<String>>,
    /// The most markets to place, if limited.
    max_markets: Option<usize>,
}

/// Updates the placed markets from the exchange's listings.
pub struct MarketTracker<'a, D> {
    directory: &'a D,
    poll_interval: Duration,
    rescan_interval: Duration,
}

impl<'a, D: MarketDirectory> MarketTracker<'a, D> {
    /// Creates a tracker that polls every `poll_interval` and compares every
    /// placed market with a full scan every `rescan_interval`.
    pub fn new(directory: &'a D, poll_interval: Duration, rescan_interval: Duration) -> Self {
        Self {
            directory,
            poll_interval,
            rescan_interval,
        }
    }

    /// Places markets as the first scan finds them, because a full scan takes
    /// too long to wait for, then follows changes until dropped. A failed scan
    /// starts again, and markets already placed keep running. `max_markets`
    /// holds for the whole run. A limited run never rescans, because a rescan
    /// places every open market regardless of the limit.
    pub async fn run<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &mut SocketManager<S, H>,
        max_markets: Option<usize>,
    ) {
        let mut state = PollState {
            since: Utc::now().timestamp() - POLL_OVERLAP_SECONDS,
            upcoming: BTreeMap::new(),
            max_markets,
        };
        let mut attempt = 0;
        while let Err(error) = self.first_scan(manager, max_markets).await {
            let delay = transport::backoff(attempt, self.poll_interval, MAX_SCAN_RETRY_DELAY);
            let delay = transport::spread(delay, self.poll_interval);
            warn!(%error, "First scan failed; starting again");
            attempt += 1;
            sleep(delay).await;
        }
        info!(
            markets = manager.market_count(),
            sockets = manager.socket_count(),
            "First scan finished"
        );
        self.refresh_upcoming(&mut state).await;
        let mut next_rescan = Instant::now() + self.rescan_interval;
        loop {
            sleep(self.poll_interval).await;
            manager.restart_failed();
            self.poll(manager, &mut state).await;
            if max_markets.is_none() && Instant::now() >= next_rescan {
                self.rescan(manager, &mut state).await;
                self.refresh_upcoming(&mut state).await;
                next_rescan = Instant::now() + self.rescan_interval;
            }
        }
    }

    async fn poll<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &mut SocketManager<S, H>,
        state: &mut PollState,
    ) {
        let polled_at = Utc::now().timestamp() - POLL_OVERLAP_SECONDS;
        match self.directory.changes_since(state.since).await {
            Ok(changes) => {
                state.since = polled_at;
                state.apply(manager, changes);
            }
            Err(error) => warn!(%error, "Market poll failed; will retry"),
        }
        let due = state.take_due(Utc::now().timestamp());
        manager.add(state.within_limit(manager, due));
    }

    /// A failed refresh keeps what is already scheduled. The refresh after the
    /// next rescan fills any gap, and a limited run does not rescan.
    async fn refresh_upcoming(&self, state: &mut PollState) {
        match self.directory.upcoming_markets().await {
            Ok(markets) => state.schedule(markets),
            Err(error) => warn!(%error, "Could not list upcoming markets"),
        }
    }

    /// Finds what the polls missed. Polling continues during the scan, and
    /// only markets placed before it began can be removed, because a later one
    /// may be missing from pages already fetched. The result of a failed scan
    /// is discarded.
    async fn rescan<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &mut SocketManager<S, H>,
        state: &mut PollState,
    ) {
        let placed_before = manager.placed();
        let scan = self.directory.open_markets();
        tokio::pin!(scan);
        let result = loop {
            tokio::select! {
                result = &mut scan => break result,
                () = sleep(self.poll_interval) => self.poll(manager, state).await,
            }
        };
        match result {
            Ok(markets) => {
                self.reconcile(manager, &placed_before, markets.into_iter().collect());
            }
            Err(error) => warn!(%error, "Rescan failed; will try again later"),
        }
    }

    /// Markets that polls removed during the scan stay removed, though an
    /// early page may still list them. A scan below the `MIN_RESCAN_FRACTION`
    /// threshold is treated as an exchange fault and removes nothing.
    fn reconcile<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &mut SocketManager<S, H>,
        placed_before: &HashSet<String>,
        open: HashSet<String>,
    ) {
        let placed_now = manager.placed();
        let removed_since = placed_before.difference(&placed_now);
        let removed_since: HashSet<&String> = removed_since.collect();
        if open.len() < placed_before.len() / MIN_RESCAN_FRACTION {
            warn!(
                open = open.len(),
                placed = placed_before.len(),
                "Rescan found too few markets; removing none"
            );
        } else {
            manager.remove(
                placed_now
                    .difference(&open)
                    .filter(|symbol| placed_before.contains(*symbol))
                    .cloned()
                    .collect(),
            );
        }
        manager.add(
            open.iter()
                .filter(|symbol| !removed_since.contains(symbol))
                .cloned()
                .collect(),
        );
    }

    /// Adding is idempotent, so a repeat scan only places what is missing.
    async fn first_scan<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &mut SocketManager<S, H>,
        max_markets: Option<usize>,
    ) -> Result<(), D::Error> {
        let limit = max_markets.unwrap_or(usize::MAX);
        // A retried scan counts what the failed one placed against the limit.
        let mut total = manager.market_count().min(limit);
        self.directory
            .scan_open(|mut page| {
                page.truncate(limit - total);
                total += page.len();
                manager.add(page);
                if total == limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .await
    }
}

impl PollState {
    fn apply<S: MarketStream, H: StreamHandler>(
        &mut self,
        manager: &mut SocketManager<S, H>,
        changes: MarketChanges,
    ) {
        self.schedule(changes.upcoming);
        manager.remove(changes.closed);
        manager.add(self.within_limit(manager, changes.opened));
    }

    fn schedule(&mut self, markets: Vec<UpcomingMarket>) {
        for market in markets {
            let due = self.upcoming.entry(market.opens_at).or_default();
            if !due.contains(&market.symbol) {
                due.push(market.symbol);
            }
        }
    }

    /// Takes the markets whose opening time has been reached.
    fn take_due(&mut self, now: i64) -> Vec<String> {
        let later = self.upcoming.split_off(&(now + 1));
        let due = std::mem::replace(&mut self.upcoming, later);
        due.into_values().flatten().collect()
    }

    fn within_limit<S: MarketStream, H: StreamHandler>(
        &self,
        manager: &SocketManager<S, H>,
        mut symbols: Vec<String>,
    ) -> Vec<String> {
        if let Some(limit) = self.max_markets {
            let placed = manager.placed();
            symbols.retain(|symbol| !placed.contains(symbol));
            symbols.sort_unstable();
            symbols.dedup();
            symbols.truncate(limit.saturating_sub(placed.len()));
        }
        symbols
    }
}
