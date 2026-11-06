//! Carries one feed's output to the record: asks failover what should be
//! recorded, then queues it for the writer.

use crate::collector::{Collector, CollectorOutput};
use crate::failover::{Failover, FeedId};
use crate::output::OutputRecord;
use crate::provider::FeedHandler;
use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::block_in_place;
use tokio_util::sync::CancellationToken;

/// The route from one feed to the writer. Needs a multi-threaded runtime,
/// because a full queue blocks the calling task.
#[derive(Clone)]
pub struct FeedRouter {
    feed: FeedId,
    failover: Arc<Mutex<Failover>>,
    records: Sender<OutputRecord>,
    shutdown: CancellationToken,
}

impl FeedRouter {
    /// Creates the route for `feed`. `shutdown` is cancelled if the writer
    /// stops, because the run cannot continue without it.
    pub fn new(
        feed: FeedId,
        failover: Arc<Mutex<Failover>>,
        records: Sender<OutputRecord>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            feed,
            failover,
            records,
            shutdown,
        }
    }

    fn failover(&self) -> std::sync::MutexGuard<'_, Failover> {
        self.failover
            .lock()
            .expect("a panic while routing leaves ownership unknown")
    }

    /// Blocks only when the queue is full, which applies back-pressure. A
    /// closed queue means the writer failed.
    fn send(&self, record: OutputRecord) {
        let sent = match self.records.try_send(record) {
            Err(TrySendError::Full(record)) => {
                block_in_place(|| self.records.blocking_send(record)).is_ok()
            }
            other => other.is_ok(),
        };
        if !sent {
            self.shutdown.cancel();
        }
    }
}

impl FeedHandler for FeedRouter {
    fn on_output(
        &mut self,
        collector: &Collector,
        output: &CollectorOutput,
        received_at: DateTime<Utc>,
    ) {
        match output {
            CollectorOutput::Record(record) => {
                // Held across the send, so the queue keeps the routing order.
                let mut failover = self.failover();
                if let Some(output) = failover.route(self.feed, collector, record) {
                    self.send(OutputRecord {
                        output,
                        received_at,
                    });
                }
            }
            CollectorOutput::Resync(symbols) => self.on_released(symbols),
        }
    }

    fn on_released(&mut self, symbols: &[String]) {
        let feed = self.feed;
        self.failover().release(feed, symbols);
    }
}
