//! The state of one Kalshi connection, which reads frames and sends commands.
//! Sequence numbers are valid only within a connection, so this state is
//! discarded with its socket.

use crate::collector::{Collector, CollectorOutput};
use crate::domain::MarketEvent;
use crate::kalshi::normalise::Normaliser;
use crate::kalshi::stream::StreamError;
use crate::kalshi::wire::{add_markets_command, delete_markets_command, get_snapshot_command};
use crate::provider::{FeedHandler, StreamCommand};
use chrono::{DateTime, Utc};
use futures_util::SinkExt;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tracing::warn;

pub(super) type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// When a frame arrived, taken once before any parsing.
#[derive(Debug, Clone, Copy)]
pub(super) struct Receipt {
    pub(super) monotonic: Instant,
    pub(super) utc: DateTime<Utc>,
}

/// The state of one connection.
pub(super) struct Session {
    pub(super) socket: Socket,
    pub(super) normaliser: Normaliser,
    pub(super) collector: Collector,
    pub(super) next_command_id: u64,
    pub(super) io_timeout: Duration,
}

impl Session {
    /// A change needs the subscription ID, so commands wait for the
    /// acknowledgement. A socket with no markets has none to wait for.
    pub(super) fn can_accept_commands(&self, tickers: &[String]) -> bool {
        self.normaliser.subscription().is_some() || tickers.is_empty()
    }

    /// Changes the set first, so that if the command cannot be sent, the next
    /// connection subscribes to the right markets.
    pub(super) async fn apply(
        &mut self,
        command: StreamCommand,
        tickers: &mut Vec<String>,
        handler: &mut impl FeedHandler,
    ) -> Result<(), StreamError> {
        let id = self.next_id();
        match command {
            StreamCommand::Add(added) => {
                tickers.extend(added.iter().cloned());
                self.collector.add(&added);
                let sid = self.subscription()?;
                self.send(add_markets_command(id, sid, &added).to_string())
                    .await
            }
            StreamCommand::Remove(removed) => {
                let gone: HashSet<&String> = removed.iter().collect();
                tickers.retain(|ticker| !gone.contains(ticker));
                self.collector.remove(&removed);
                handler.on_released(&removed);
                let sid = self.subscription()?;
                self.send(delete_markets_command(id, sid, &removed).to_string())
                    .await
            }
        }
    }

    /// Without a subscription there is nothing to change, so the connection
    /// is replaced instead.
    fn subscription(&self) -> Result<u64, StreamError> {
        self.normaliser
            .subscription()
            .ok_or(StreamError::NoSubscription)
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_command_id;
        self.next_command_id += 1;
        id
    }

    /// Sends a Close frame. The connection is ending, so a failure is only
    /// logged.
    pub(super) async fn close(&mut self) {
        match timeout(self.io_timeout, self.socket.close(None)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "Close frame failed"),
            Err(_) => warn!("Close frame timed out"),
        }
    }

    /// An unusable frame may have carried a book change, so it counts as lost
    /// continuity.
    pub(super) async fn on_text(
        &mut self,
        text: &str,
        received: Receipt,
        handler: &mut impl FeedHandler,
    ) -> Result<(), StreamError> {
        let event = self.normaliser.normalise(text).unwrap_or_else(|error| {
            warn!(%error, "Frame rejected");
            Some(MarketEvent::ContinuityLost)
        });
        let Some(event) = event else {
            return Ok(());
        };
        let outputs = self.collector.handle(event, received.monotonic);
        self.publish(outputs, received.utc, handler).await
    }

    pub(super) async fn publish(
        &mut self,
        outputs: Vec<CollectorOutput>,
        received_at: DateTime<Utc>,
        handler: &mut impl FeedHandler,
    ) -> Result<(), StreamError> {
        for output in outputs {
            handler.on_output(&self.collector, &output, received_at);
            if let CollectorOutput::Resync(symbols) = &output {
                self.request_snapshots(symbols).await?;
            }
        }
        Ok(())
    }

    pub(super) async fn request_snapshots(
        &mut self,
        symbols: &[String],
    ) -> Result<(), StreamError> {
        let sid = self.subscription()?;
        let id = self.next_id();
        self.send(get_snapshot_command(id, sid, symbols).to_string())
            .await
    }

    /// A failed send ends the connection, so a lost command cannot go unseen.
    /// The timeout is required because the silence branch of the stream's
    /// select cannot fire while this call is pending.
    pub(super) async fn send(&mut self, command: String) -> Result<(), StreamError> {
        timeout(
            self.io_timeout,
            self.socket.send(Message::Text(command.into())),
        )
        .await
        .map_err(|_| StreamError::TimedOut)?
        .map_err(Box::new)?;
        Ok(())
    }
}
