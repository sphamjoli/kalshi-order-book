//! Failover: merges the feeds of each market into one record. A market can run
//! on several independent feeds, each with its own book, and the exchange clock
//! identifies matching changes. The first feed to deliver a change is recorded
//! and later copies are discarded, so a slow or stalled feed does not delay the
//! record while another keeps up. One feed owns each market and supplies its
//! minute snapshots. An owner that loses the market gives it up, and the next
//! feed with a live book takes over with a full snapshot, so deltas never
//! continue from a book the record has not seen.
//!
//! A provider without a clock has no way to match changes, so only the owner
//! of a market is recorded.

use crate::collector::{BookRecord, Collector};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// How long a feed behind the record waits before it may take a market over.
const TAKEOVER_WAIT: Duration = Duration::from_secs(5);

/// Identifies one feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FeedId(pub usize);

/// A place in one market's changes. Several changes can share a clock, and
/// every feed delivers those in the same order, so the ordinal tells them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Position {
    clock: u64,
    ordinal: u32,
}

/// The merge of each market. A market with no entry has no live feed.
#[derive(Debug)]
pub struct Failover {
    markets: HashMap<String, MarketMerge>,
    takeover_wait: Duration,
}

/// How far the record and each feed have got in one market.
#[derive(Debug, Default)]
struct MarketMerge {
    owner: Option<FeedId>,
    recorded: Option<Position>,
    feeds: HashMap<FeedId, Option<Position>>,
    /// When a feed behind the record first tried to take the market over.
    waiting_since: Option<Instant>,
}

impl Default for Failover {
    fn default() -> Self {
        Self::with_takeover_wait(TAKEOVER_WAIT)
    }
}

impl Failover {
    /// Creates a merge in which a feed behind the record waits up to `wait`
    /// before it may take a market over.
    pub fn with_takeover_wait(wait: Duration) -> Self {
        Self {
            markets: HashMap::new(),
            takeover_wait: wait,
        }
    }

    /// Returns what should be recorded for `record` from `feed`. `collector`
    /// is the state that produced it, so a takeover records the book as it
    /// stands after this record, and the record itself is then redundant.
    pub fn route(
        &mut self,
        feed: FeedId,
        collector: &Collector,
        record: &BookRecord,
    ) -> Option<BookRecord> {
        let symbol = record.symbol();
        let merge = self.markets.entry(symbol.to_owned()).or_default();
        let is_snapshot = matches!(record, BookRecord::Snapshot { .. });
        let (position, clock_fell) = merge.advance(feed, record.exchange_clock(), is_snapshot);
        if clock_fell {
            warn!(symbol, "Exchange clock fell; starting the market again");
            merge.owner = None;
            merge.recorded = None;
        }
        if merge.owner.is_none() {
            if merge.must_wait(position, self.takeover_wait) {
                return None;
            }
            merge.waiting_since = None;
            debug!(symbol, feed = feed.0, "Feed took over a market");
            merge.owner = Some(feed);
            merge.recorded = position;
            return collector.snapshot(symbol);
        }
        let wanted = if is_snapshot {
            merge.wants_snapshot(feed, position)
        } else {
            merge.wants_update(feed, position)
        };
        if wanted {
            merge.recorded = position.max(merge.recorded);
        }
        wanted.then(|| record.clone())
    }

    /// Removes `feed` from `symbols`. An owner gives up the market,
    /// and a market with no feed left is forgotten, so closed markets do not
    /// build up.
    pub fn release(&mut self, feed: FeedId, symbols: &[String]) {
        for symbol in symbols {
            let Some(merge) = self.markets.get_mut(symbol) else {
                continue;
            };
            merge.feeds.remove(&feed);
            if merge.owner == Some(feed) {
                merge.owner = None;
            }
            if merge.feeds.is_empty() {
                self.markets.remove(symbol);
            }
        }
    }
}

impl MarketMerge {
    /// A takeover behind the record would move a consumer's book back in time
    /// and repeat recorded changes, so the feed first catches up. The wait is
    /// limited because a clock reset can leave every feed behind for good.
    fn must_wait(&mut self, position: Option<Position>, wait: Duration) -> bool {
        let behind = matches!(
            (position, self.recorded),
            (Some(position), Some(recorded)) if position.clock < recorded.clock
        );
        behind
            && self
                .waiting_since
                .get_or_insert_with(Instant::now)
                .elapsed()
                < wait
    }

    fn wants_update(&self, feed: FeedId, position: Option<Position>) -> bool {
        match (position, self.recorded) {
            (Some(position), Some(recorded)) => position > recorded,
            _ => self.owner == Some(feed),
        }
    }

    /// The owner supplies the minute snapshots. A standby replaces it when the
    /// standby is ahead of the record, or level with the record while the owner
    /// is behind, so a stalled owner does not stop a market's snapshots.
    fn wants_snapshot(&mut self, feed: FeedId, position: Option<Position>) -> bool {
        let is_owner = self.owner == Some(feed);
        let (Some(position), Some(recorded)) = (position, self.recorded) else {
            return is_owner;
        };
        if is_owner {
            return position.clock >= recorded.clock;
        }
        let replaces_owner = position.clock > recorded.clock
            || (position.clock == recorded.clock && self.owner_is_behind(recorded));
        if replaces_owner {
            self.owner = Some(feed);
        }
        replaces_owner
    }

    fn owner_is_behind(&self, recorded: Position) -> bool {
        self.owner
            .and_then(|owner| self.feeds.get(&owner).copied().flatten())
            .is_some_and(|owner| owner.clock < recorded.clock)
    }

    /// Moves `feed` to `clock`, and returns its new position and whether its
    /// own clock went backwards, as exchange maintenance can cause. A snapshot
    /// covers every change at its clock, so it sits after all of them.
    fn advance(
        &mut self,
        feed: FeedId,
        clock: Option<u64>,
        is_snapshot: bool,
    ) -> (Option<Position>, bool) {
        let last = self.feeds.entry(feed).or_default();
        let Some(clock) = clock else {
            return (None, false);
        };
        let ordinal = match *last {
            _ if is_snapshot => u32::MAX,
            Some(last) if last.clock == clock => last.ordinal.saturating_add(1),
            _ => 0,
        };
        let fell = last.is_some_and(|last| clock < last.clock);
        let position = Position { clock, ordinal };
        *last = Some(position);
        (Some(position), fell)
    }
}
