//! The contracts a market data provider implements. Everything outside a
//! provider's own module depends on these, so adding a provider means
//! implementing them and choosing it in the application, with no change to
//! placement, tracking, books, failover or output.

use crate::collector::{Collector, CollectorOutput};
use chrono::{DateTime, Utc};
use std::future::Future;
use std::ops::ControlFlow;
use tokio::sync::mpsc::UnboundedReceiver;

/// A change to the markets of a running stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamCommand {
    Add(Vec<String>),
    Remove(Vec<String>),
}

/// What changed on the exchange since a given time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketChanges {
    pub opened: Vec<String>,
    /// Markets that stopped trading, for any reason.
    pub closed: Vec<String>,
    /// New markets that have not opened yet.
    pub upcoming: Vec<UpcomingMarket>,
}

/// A market listed before it opens. Exchanges may list a market long before
/// trading starts, so its creation says nothing about when to follow it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpcomingMarket {
    pub symbol: String,
    /// When trading starts, in Unix seconds.
    pub opens_at: i64,
}

/// Receives what one stream produces.
pub trait FeedHandler {
    /// Sees one collector output, the state that produced it, and its UTC
    /// receipt time.
    fn on_output(
        &mut self,
        collector: &Collector,
        output: &CollectorOutput,
        received_at: DateTime<Utc>,
    );

    /// The stream no longer feeds `symbols`.
    fn on_released(&mut self, _symbols: &[String]) {}
}

impl<T: FnMut(&Collector, &CollectorOutput, DateTime<Utc>)> FeedHandler for T {
    fn on_output(
        &mut self,
        collector: &Collector,
        output: &CollectorOutput,
        received_at: DateTime<Utc>,
    ) {
        self(collector, output, received_at);
    }
}

/// A handler that each socket task can own a copy of.
pub trait StreamHandler: FeedHandler + Clone + Send + 'static {}

impl<T: FeedHandler + Clone + Send + 'static> StreamHandler for T {}

/// One provider connection that feeds books for a set of markets.
pub trait MarketStream: Clone + Send + Sync + 'static {
    /// The connections the provider allows one account.
    const CONNECTION_LIMIT: usize;

    /// Returns the same stream for a different set of markets.
    #[must_use]
    fn for_markets(&self, symbols: Vec<String>) -> Self;

    /// Runs until `commands` closes, reconnecting as needed.
    fn run<H: FeedHandler + Send>(
        &self,
        commands: UnboundedReceiver<StreamCommand>,
        handler: H,
    ) -> impl Future<Output = ()> + Send;
}

/// The provider's list of markets.
pub trait MarketDirectory {
    type Error: std::error::Error;

    /// Passes each page of open markets to `on_page` as it arrives, because a
    /// full scan can take too long to wait for.
    fn scan_open(
        &self,
        on_page: impl FnMut(Vec<String>) -> ControlFlow<()>,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Returns what opened and closed at or after `since`, in Unix seconds.
    fn changes_since(&self, since: i64)
    -> impl Future<Output = Result<MarketChanges, Self::Error>>;

    /// Returns every market listed but not yet open. A provider that lists
    /// markets only once they are open has none.
    fn upcoming_markets(&self) -> impl Future<Output = Result<Vec<UpcomingMarket>, Self::Error>> {
        async { Ok(Vec::new()) }
    }

    /// Returns every open market.
    fn open_markets(&self) -> impl Future<Output = Result<Vec<String>, Self::Error>> {
        async {
            let mut markets = Vec::new();
            self.scan_open(|page| {
                markets.extend(page);
                ControlFlow::Continue(())
            })
            .await?;
            Ok(markets)
        }
    }
}
