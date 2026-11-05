//! Collector: the book lifecycle of the markets on one stream. Owns all
//! mutable book state and decides when a book can no longer be believed. Knows
//! nothing of sockets or wire formats, and takes the time as a value, so every
//! transition is deterministic.
//!
//! Invariants:
//!
//! - No delta is applied to a book that is not live.
//! - For each market, output starts with a snapshot.
//! - A book with uncertain continuity is never published.

use crate::Side;
use crate::domain::{Book, Level, LevelChange, MarketEvent, Price, QuantityDelta};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tracing::warn;

/// The most levels a side may hold, because the output format counts them in
/// 16 bits and a record it cannot encode would stop the whole run.
const MAX_BOOK_DEPTH: usize = 65_535;

/// Holds the markets of one stream, because a lost message on a stream
/// affects exactly the books that stream feeds.
#[derive(Debug)]
pub struct Collector {
    markets: BTreeMap<String, MarketState>,
    crossed_grace: Duration,
}

/// Where one market is in its lifecycle.
#[derive(Debug)]
enum MarketState {
    /// Subscribed, or awaiting a replacement: no book can be trusted.
    AwaitingSnapshot,
    Live {
        book: Book,
        /// The exchange time of the state this book describes.
        exchange_time_ms: i64,
        /// When the book last became crossed, if it still is.
        crossed_since: Option<Instant>,
    },
    /// Removed from the stream. Kept so that frames already in flight are not
    /// mistaken for corruption. A new connection starts without these.
    Retired,
}

/// What the collector asks the rest of the program to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorOutput {
    /// Record the whole book of a market.
    Snapshot {
        symbol: String,
        exchange_time_ms: i64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// Record one absolute level change.
    Update {
        symbol: String,
        exchange_time_ms: i64,
        change: LevelChange,
    },
    /// Request replacement snapshots for these markets.
    Resync(Vec<String>),
    /// This stream's connection ended, so it no longer feeds these markets.
    Released(Vec<String>),
}

impl Collector {
    /// Starts every market waiting for its first snapshot. A book may stay
    /// crossed for `crossed_grace` before it is treated as drifted.
    pub fn new(symbols: impl IntoIterator<Item = String>, crossed_grace: Duration) -> Self {
        Self {
            markets: symbols
                .into_iter()
                .map(|symbol| (symbol, MarketState::AwaitingSnapshot))
                .collect(),
            crossed_grace,
        }
    }

    /// Starts following `symbols`, each waiting for its first snapshot.
    pub fn add(&mut self, symbols: &[String]) {
        for symbol in symbols {
            self.markets
                .insert(symbol.clone(), MarketState::AwaitingSnapshot);
        }
    }

    /// Stops following `symbols`.
    pub fn remove(&mut self, symbols: &[String]) {
        for symbol in symbols {
            if let Some(state) = self.markets.get_mut(symbol) {
                *state = MarketState::Retired;
            }
        }
    }

    /// Applies one event. `now` is its monotonic receipt time.
    #[must_use = "a dropped resync leaves markets unpublished"]
    pub fn handle(&mut self, event: MarketEvent, now: Instant) -> Vec<CollectorOutput> {
        match event {
            MarketEvent::Snapshot {
                symbol,
                exchange_time_ms,
                bids,
                asks,
            } => self.on_snapshot(symbol, exchange_time_ms, bids, asks, now),
            MarketEvent::Delta {
                symbol,
                exchange_time_ms,
                side,
                price,
                delta,
            } => self.on_delta(symbol, exchange_time_ms, side, price, delta, now),
            MarketEvent::ContinuityLost => self.invalidate_live(),
        }
    }

    /// Resyncs books that have stayed crossed past the grace period. Runs on a
    /// timer, because a drifted market may never send another delta.
    #[must_use = "a dropped resync leaves markets unpublished"]
    pub fn tick(&mut self, now: Instant) -> Vec<CollectorOutput> {
        let drifted: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, state)| match state {
                MarketState::Live {
                    crossed_since: Some(since),
                    ..
                } => now.duration_since(*since) >= self.crossed_grace,
                _ => false,
            })
            .map(|(symbol, _)| symbol.clone())
            .collect();
        if drifted.is_empty() {
            return Vec::new();
        }
        warn!(markets = drifted.len(), "Books stayed crossed");
        self.invalidate(drifted)
    }

    /// Returns a snapshot of every live book, so a quiet market is still
    /// recorded. A book that cannot be trusted is left out.
    pub fn snapshots(&self) -> Vec<CollectorOutput> {
        self.markets
            .keys()
            .filter_map(|symbol| self.snapshot(symbol))
            .collect()
    }

    /// Returns a snapshot of one book, if it is live.
    pub fn snapshot(&self, symbol: &str) -> Option<CollectorOutput> {
        match self.markets.get(symbol)? {
            MarketState::Live {
                book,
                exchange_time_ms,
                ..
            } => Some(CollectorOutput::Snapshot {
                symbol: symbol.to_owned(),
                exchange_time_ms: *exchange_time_ms,
                bids: book.bids().collect(),
                asks: book.asks().collect(),
            }),
            _ => None,
        }
    }

    /// Returns the markets still waiting for a snapshot.
    pub fn awaiting(&self) -> Vec<String> {
        self.markets
            .iter()
            .filter(|(_, state)| matches!(state, MarketState::AwaitingSnapshot))
            .map(|(symbol, _)| symbol.clone())
            .collect()
    }

    /// Returns the books that can be published.
    pub fn live_books(&self) -> impl Iterator<Item = (&str, &Book)> {
        self.markets
            .iter()
            .filter_map(|(symbol, state)| match state {
                MarketState::Live { book, .. } => Some((symbol.as_str(), book)),
                _ => None,
            })
    }

    fn on_delta(
        &mut self,
        symbol: String,
        exchange_time_ms: i64,
        side: Side,
        price: Price,
        delta: QuantityDelta,
        now: Instant,
    ) -> Vec<CollectorOutput> {
        let Some(state) = self.markets.get_mut(&symbol) else {
            // A corrupted ticker may hide a change to a known market.
            warn!(%symbol, "Delta for a market not on this stream");
            return self.invalidate_live();
        };
        let MarketState::Live {
            book,
            exchange_time_ms: latest,
            crossed_since,
        } = state
        else {
            return Vec::new();
        };
        match book.apply(side, price, delta) {
            Ok(_) if book.depth() > MAX_BOOK_DEPTH => {
                warn!(%symbol, "Book grew past the output limit");
                self.invalidate(vec![symbol])
            }
            Ok(change) => {
                update_crossing(book, crossed_since, now);
                *latest = exchange_time_ms;
                vec![CollectorOutput::Update {
                    symbol,
                    exchange_time_ms,
                    change,
                }]
            }
            Err(error) => {
                warn!(%symbol, %error, "Delta contradicts the book");
                self.invalidate(vec![symbol])
            }
        }
    }

    fn on_snapshot(
        &mut self,
        symbol: String,
        exchange_time_ms: i64,
        bids: Vec<Level>,
        asks: Vec<Level>,
        now: Instant,
    ) -> Vec<CollectorOutput> {
        let Some(state) = self.markets.get_mut(&symbol) else {
            warn!(%symbol, "Snapshot for a market not on this stream");
            return Vec::new();
        };
        if matches!(state, MarketState::Retired) {
            return Vec::new();
        }
        match Book::from_levels(bids, asks) {
            Ok(book) => {
                let output = CollectorOutput::Snapshot {
                    symbol,
                    exchange_time_ms,
                    bids: book.bids().collect(),
                    asks: book.asks().collect(),
                };
                let crossed_since = book.is_crossed().then_some(now);
                *state = MarketState::Live {
                    book,
                    exchange_time_ms,
                    crossed_since,
                };
                vec![output]
            }
            Err(error) => {
                warn!(%symbol, %error, "Snapshot rejected");
                self.invalidate(vec![symbol])
            }
        }
    }

    /// Markets already awaiting a snapshot have a request outstanding, so
    /// repeated gaps do not ask for them again.
    fn invalidate_live(&mut self) -> Vec<CollectorOutput> {
        let live: Vec<String> = self
            .live_books()
            .map(|(symbol, _)| symbol.to_owned())
            .collect();
        if live.is_empty() {
            return Vec::new();
        }
        self.invalidate(live)
    }

    fn invalidate(&mut self, symbols: Vec<String>) -> Vec<CollectorOutput> {
        for symbol in &symbols {
            self.markets
                .insert(symbol.clone(), MarketState::AwaitingSnapshot);
        }
        vec![CollectorOutput::Resync(symbols)]
    }
}

/// Keeps the first crossing time while a book stays crossed.
fn update_crossing(book: &Book, crossed_since: &mut Option<Instant>, now: Instant) {
    *crossed_since = book.is_crossed().then(|| crossed_since.unwrap_or(now));
}
