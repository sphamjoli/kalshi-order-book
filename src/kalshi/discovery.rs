//! Market tracking: finds markets and keeps the socket manager in step as they
//! open and close, using short time-filtered polls, with a periodic full scan
//! to catch anything they miss. Removal needs a closed or settled listing or a
//! complete scan, so a failed scan removes nothing.

use crate::kalshi::manager::{SocketManager, StreamHandler};
use crate::kalshi::rest::{KalshiClient, Market, MarketQuery, RestError};
use crate::transport;
use chrono::Utc;
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tracing::{info, warn};

/// Overlap between polls, so a market on the boundary is not missed. Adding
/// or removing a market twice changes nothing.
const POLL_OVERLAP_SECONDS: i64 = 60;
/// A rescan below one over this share of the placed markets is not believed.
const MIN_RESCAN_FRACTION: usize = 2;
const MAX_SCAN_RETRY_DELAY: Duration = Duration::from_secs(60);

/// Keeps the placed markets in step with the exchange.
pub struct MarketTracker<'a> {
    client: &'a KalshiClient,
    poll_interval: Duration,
    rescan_interval: Duration,
}

impl<'a> MarketTracker<'a> {
    /// Creates a tracker that polls every `poll_interval` and compares every
    /// placed market with a full scan every `rescan_interval`.
    pub fn new(
        client: &'a KalshiClient,
        poll_interval: Duration,
        rescan_interval: Duration,
    ) -> Self {
        Self {
            client,
            poll_interval,
            rescan_interval,
        }
    }

    /// Places markets as the first scan finds them, because a full scan takes
    /// far too long to wait for, then follows changes until dropped. A failed
    /// scan starts again, and markets already placed keep running.
    /// `max_markets` limits the first scan, and a limited run never rescans,
    /// because a rescan would place every market.
    pub async fn run<H: StreamHandler>(
        &self,
        manager: &mut SocketManager<H>,
        max_markets: Option<usize>,
    ) {
        let mut since = Utc::now().timestamp() - POLL_OVERLAP_SECONDS;
        let mut attempt = 0;
        while let Err(error) = self.first_scan(manager, max_markets).await {
            let delay = transport::backoff(attempt, self.poll_interval, MAX_SCAN_RETRY_DELAY);
            warn!(%error, "First scan failed; starting again");
            attempt += 1;
            sleep(delay).await;
        }
        info!(
            markets = manager.market_count(),
            sockets = manager.socket_count(),
            "First scan finished"
        );
        let mut next_rescan = Instant::now() + self.rescan_interval;
        loop {
            sleep(self.poll_interval).await;
            self.poll(manager, &mut since).await;
            if max_markets.is_none() && Instant::now() >= next_rescan {
                self.rescan(manager, &mut since).await;
                next_rescan = Instant::now() + self.rescan_interval;
            }
        }
    }

    async fn poll<H: StreamHandler>(&self, manager: &mut SocketManager<H>, since: &mut i64) {
        let polled_at = Utc::now().timestamp() - POLL_OVERLAP_SECONDS;
        match self.apply_changes(manager, *since).await {
            Ok(()) => *since = polled_at,
            Err(error) => warn!(%error, "Market poll failed; will retry"),
        }
    }

    /// Catches anything the polls missed. Polling continues during the scan,
    /// and only markets placed before it began can be removed, because a
    /// later one may be missing from pages already fetched. A failed scan
    /// changes nothing.
    async fn rescan<H: StreamHandler>(&self, manager: &mut SocketManager<H>, since: &mut i64) {
        let placed_before = manager.placed();
        let scan = self.client.markets(MarketQuery::Open);
        tokio::pin!(scan);
        let result = loop {
            tokio::select! {
                result = &mut scan => break result,
                () = sleep(self.poll_interval) => self.poll(manager, since).await,
            }
        };
        match result {
            Ok(markets) => {
                let open: HashSet<String> = tickers(markets).into_iter().collect();
                self.reconcile(manager, &placed_before, open);
            }
            Err(error) => warn!(%error, "Rescan failed; will try again later"),
        }
    }

    /// Markets that polls removed during the scan stay removed, though an
    /// early page may still list them. A scan far smaller than what is placed
    /// looks like an exchange fault, so it removes nothing.
    fn reconcile<H: StreamHandler>(
        &self,
        manager: &mut SocketManager<H>,
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
    async fn first_scan<H: StreamHandler>(
        &self,
        manager: &mut SocketManager<H>,
        max_markets: Option<usize>,
    ) -> Result<(), RestError> {
        let limit = max_markets.unwrap_or(usize::MAX);
        // A retried scan continues from what the failed one placed.
        let mut total = manager.market_count().min(limit);
        self.client
            .scan_markets(MarketQuery::Open, |page| {
                let page = page.into_iter().take(limit - total);
                let tickers: Vec<_> = page.map(|market| market.ticker).collect();
                total += tickers.len();
                manager.add(tickers);
                if total == limit {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .await
    }

    /// Fetches both lists before changing anything, so a failed poll can be
    /// repeated from the same time.
    async fn apply_changes<H: StreamHandler>(
        &self,
        manager: &mut SocketManager<H>,
        since: i64,
    ) -> Result<(), RestError> {
        let created = self
            .client
            .markets(MarketQuery::CreatedSince(since))
            .await?;
        let closed = self.client.markets(MarketQuery::ClosedSince(since)).await?;
        let settled = self
            .client
            .markets(MarketQuery::SettledSince(since))
            .await?;
        manager.add(tickers(created));
        manager.remove(tickers(closed));
        manager.remove(tickers(settled));
        Ok(())
    }
}

fn tickers(markets: Vec<Market>) -> Vec<String> {
    markets.into_iter().map(|market| market.ticker).collect()
}
