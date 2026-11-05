//! Failover: chooses which feed's output reaches the record for each market.
//! Every market runs on several independent feeds, each with its own book. One
//! feed owns a market at a time. A feed that loses a market gives up ownership,
//! and the next feed with a live book takes over with a full snapshot, so
//! deltas never continue from a book the record has not seen.

use crate::collector::{Collector, CollectorOutput};
use std::collections::HashMap;
use tracing::debug;

/// Identifies one feed.
pub type FeedId = usize;

/// The owner of each market. A market with no entry is free to be claimed.
#[derive(Debug, Default)]
pub struct Failover {
    owners: HashMap<String, FeedId>,
}

impl Failover {
    /// Returns what should be recorded for `output` from `feed`. `collector`
    /// is the state that produced it, so a takeover records the book as it
    /// stands after this output, and the output itself is then redundant.
    pub fn route(
        &mut self,
        feed: FeedId,
        collector: &Collector,
        output: &CollectorOutput,
    ) -> Option<CollectorOutput> {
        match output {
            CollectorOutput::Snapshot { symbol, .. } => {
                self.claim(feed, symbol);
                self.is_owner(feed, symbol).then(|| output.clone())
            }
            CollectorOutput::Update { symbol, .. } => {
                if self.claim(feed, symbol) {
                    collector.snapshot(symbol)
                } else {
                    self.is_owner(feed, symbol).then(|| output.clone())
                }
            }
            CollectorOutput::Resync(symbols) | CollectorOutput::Released(symbols) => {
                self.release(feed, symbols);
                None
            }
        }
    }

    /// Returns whether `feed` has just become the owner.
    fn claim(&mut self, feed: FeedId, symbol: &str) -> bool {
        if self.owners.contains_key(symbol) {
            return false;
        }
        debug!(symbol, feed, "Feed took over a market");
        self.owners.insert(symbol.to_owned(), feed);
        true
    }

    fn is_owner(&self, feed: FeedId, symbol: &str) -> bool {
        self.owners.get(symbol) == Some(&feed)
    }

    fn release(&mut self, feed: FeedId, symbols: &[String]) {
        for symbol in symbols {
            if self.is_owner(feed, symbol) {
                self.owners.remove(symbol);
            }
        }
    }
}
