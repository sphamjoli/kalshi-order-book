//! Kalshi's market list as a [`MarketDirectory`].

use crate::kalshi::rest::{KalshiClient, Market, MarketQuery, RestError};
use crate::provider::{MarketChanges, MarketDirectory, UpcomingMarket};
use chrono::DateTime;
use std::ops::ControlFlow;

impl MarketDirectory for KalshiClient {
    type Error = RestError;

    async fn scan_open(
        &self,
        mut on_page: impl FnMut(Vec<String>) -> ControlFlow<()>,
    ) -> Result<(), RestError> {
        self.scan_markets(MarketQuery::Open, |page| on_page(tickers(page)))
            .await
    }

    /// Fetches every list before returning, so a failed poll changes nothing
    /// and can be repeated from the same time.
    async fn changes_since(&self, since: i64) -> Result<MarketChanges, RestError> {
        let opened = self.markets(MarketQuery::CreatedSince(since)).await?;
        let mut closed = self.markets(MarketQuery::ClosedSince(since)).await?;
        closed.extend(self.markets(MarketQuery::SettledSince(since)).await?);
        let upcoming = self.markets(MarketQuery::UnopenedSince(since)).await?;
        Ok(MarketChanges {
            opened: tickers(opened),
            closed: tickers(closed),
            upcoming: upcoming_markets(upcoming),
        })
    }

    async fn upcoming_markets(&self) -> Result<Vec<UpcomingMarket>, RestError> {
        Ok(upcoming_markets(self.markets(MarketQuery::Unopened).await?))
    }
}

fn tickers(markets: Vec<Market>) -> Vec<String> {
    markets.into_iter().map(|market| market.ticker).collect()
}

/// A market without a valid open time cannot be scheduled. The periodic scan finds
/// it once it is open.
fn upcoming_markets(markets: Vec<Market>) -> Vec<UpcomingMarket> {
    markets
        .into_iter()
        .filter_map(|market| {
            Some(UpcomingMarket {
                opens_at: DateTime::parse_from_rfc3339(&market.open_time?)
                    .ok()?
                    .timestamp(),
                symbol: market.ticker,
            })
        })
        .collect()
}
