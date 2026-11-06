//! Collector: the book lifecycle of the markets on one stream. Owns all
//! mutable book state and decides when a book can no longer be trusted. Has no
//! dependency on sockets or wire formats and takes the time as a value, so
//! every transition is deterministic.
//!
//! Invariants:
//!
//! - No delta is applied to a book that is not live.
//! - For each market, the first record is a snapshot.
//! - A book whose continuity is known to be lost is not published until a
//!   snapshot replaces it.

use crate::Side;
use crate::domain::{Book, Level, LevelChange, MarketEvent, Price, QuantityDelta};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tracing::warn;

/// The most levels a delta may leave on a side, because the output format
/// counts them in 16 bits and a record it cannot encode would stop the run.
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
    /// Subscribed or awaiting a replacement snapshot. No book is held.
    AwaitingSnapshot,
    Live {
        book: Book,
        /// The exchange position of the state this book describes.
        stamp: Stamp,
        /// When the book last became crossed, if it still is.
        crossed_since: Option<Instant>,
    },
    /// Removed from the stream. Kept so that frames already in flight are not
    /// mistaken for corruption. A new connection starts without these.
    Retired,
}

/// The exchange time and clock of a change.
#[derive(Debug, Clone, Copy)]
struct Stamp {
    time_ms: i64,
    clock: Option<u64>,
}

/// A book state or change to record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookRecord {
    /// The whole book of a market.
    Snapshot {
        symbol: String,
        exchange_time_ms: i64,
        exchange_clock: Option<u64>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// One absolute level change.
    Update {
        symbol: String,
        exchange_time_ms: i64,
        exchange_clock: Option<u64>,
        change: LevelChange,
    },
}

/// What the collector asks the rest of the program to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorOutput {
    Record(BookRecord),
    /// Request replacement snapshots for these markets.
    Resync(Vec<String>),
}

impl BookRecord {
    /// Returns the market this record describes.
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot { symbol, .. } | Self::Update { symbol, .. } => symbol,
        }
    }

    /// Returns the exchange clock of this record, if the provider has one.
    pub fn exchange_clock(&self) -> Option<u64> {
        match self {
            Self::Snapshot { exchange_clock, .. } | Self::Update { exchange_clock, .. } => {
                *exchange_clock
            }
        }
    }

    /// Returns the exchange time of this record.
    pub fn exchange_time_ms(&self) -> i64 {
        match self {
            Self::Snapshot {
                exchange_time_ms, ..
            }
            | Self::Update {
                exchange_time_ms, ..
            } => *exchange_time_ms,
        }
    }
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
                exchange_clock,
                bids,
                asks,
            } => {
                let stamp = Stamp {
                    time_ms: exchange_time_ms,
                    clock: exchange_clock,
                };
                self.on_snapshot(symbol, stamp, bids, asks, now)
            }
            MarketEvent::Delta {
                symbol,
                exchange_time_ms,
                exchange_clock,
                side,
                price,
                delta,
            } => {
                let stamp = Stamp {
                    time_ms: exchange_time_ms,
                    clock: exchange_clock,
                };
                self.on_delta(symbol, stamp, side, price, delta, now)
            }
            MarketEvent::ContinuityLost => self.invalidate_live(),
        }
    }

    /// Resyncs books that have stayed crossed for at least the grace period.
    /// Runs on a timer, because a drifted market may never send another delta.
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
    /// recorded.
    pub fn snapshots(&self) -> Vec<CollectorOutput> {
        self.markets
            .keys()
            .filter_map(|symbol| self.snapshot(symbol))
            .map(CollectorOutput::Record)
            .collect()
    }

    /// Returns a snapshot of one book, if it is live.
    pub fn snapshot(&self, symbol: &str) -> Option<BookRecord> {
        match self.markets.get(symbol)? {
            MarketState::Live { book, stamp, .. } => Some(BookRecord::Snapshot {
                symbol: symbol.to_owned(),
                exchange_time_ms: stamp.time_ms,
                exchange_clock: stamp.clock,
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
        stamp: Stamp,
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
            stamp: latest,
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
                *latest = stamp;
                vec![CollectorOutput::Record(BookRecord::Update {
                    symbol,
                    exchange_time_ms: stamp.time_ms,
                    exchange_clock: stamp.clock,
                    change,
                })]
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
        stamp: Stamp,
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
                let output = CollectorOutput::Record(BookRecord::Snapshot {
                    symbol,
                    exchange_time_ms: stamp.time_ms,
                    exchange_clock: stamp.clock,
                    bids: book.bids().collect(),
                    asks: book.asks().collect(),
                });
                let crossed_since = book.is_crossed().then_some(now);
                *state = MarketState::Live {
                    book,
                    stamp,
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
