//! Decides what to do about markets still waiting for replacement snapshots.
//! Pure, so every path can be tested without a socket.

use std::collections::HashSet;

/// Retries with no new snapshots before the connection is rebuilt. A sequence
/// that jumped would otherwise discard every replacement snapshot.
const MAX_STALLED_RESYNCS: u32 = 3;

/// What one review of the waiting markets calls for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResyncDecision {
    /// Nothing is waiting.
    Idle,
    /// Ask again for the waiting markets.
    Retry,
    /// Ask again, and report that requests are going unanswered.
    RetryOverdue,
    /// Replace the connection.
    Rebuild,
}

/// Tracks progress between reviews.
#[derive(Debug, Default)]
pub struct ResyncWatch {
    last_awaiting: HashSet<String>,
    stalled: u32,
}

impl ResyncWatch {
    /// Reviews the markets in `awaiting`. Progress is a market leaving the
    /// set, because its size also changes when markets are added. A stall
    /// rebuilds the connection only when `has_live_books` is false, so one
    /// stuck market does not cost its healthy neighbours.
    pub fn review(&mut self, awaiting: &[String], has_live_books: bool) -> ResyncDecision {
        let current: HashSet<String> = awaiting.iter().cloned().collect();
        let progressed = self
            .last_awaiting
            .iter()
            .any(|symbol| !current.contains(symbol));
        self.stalled = if awaiting.is_empty() || progressed {
            0
        } else {
            self.stalled + 1
        };
        self.last_awaiting = current;
        match (self.stalled >= MAX_STALLED_RESYNCS, has_live_books) {
            (true, false) => ResyncDecision::Rebuild,
            (true, true) => ResyncDecision::RetryOverdue,
            (false, _) if awaiting.is_empty() => ResyncDecision::Idle,
            (false, _) => ResyncDecision::Retry,
        }
    }
}
