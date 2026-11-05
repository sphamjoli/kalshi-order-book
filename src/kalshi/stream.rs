//! Kalshi WebSocket session: connects, subscribes, and drives the normaliser
//! and collector. Sequence numbers are not comparable across connections, so
//! every connection starts with fresh state and fresh snapshots. It never gives
//! up on a live market: a lost connection only slows down, up to a capped
//! delay. Dropping the command sender closes it cleanly.
//!
//! Handshake signing: <https://docs.kalshi.com/getting_started/quick_start_websockets>.

use crate::collector::{Collector, CollectorOutput};
use crate::domain::MarketEvent;
use crate::kalshi::auth::{RequestSigner, auth_header_map};
use crate::kalshi::normalise::Normaliser;
use crate::kalshi::wire::{
    add_markets_command, delete_markets_command, get_snapshot_command, subscribe_command,
};
use crate::transport;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::InvalidHeaderValue;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{MissedTickBehavior, interval, interval_at, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{info, trace, warn};
use url::Url;

/// Timing used outside tests. Kalshi pings every 10 seconds, so 30 seconds of
/// silence means three missed pings.
pub const DEFAULT_TIMING: StreamTiming = StreamTiming {
    silence_limit: Duration::from_secs(30),
    resync_retry: Duration::from_secs(30),
    crossed_check: Duration::from_secs(1),
    crossed_grace: Duration::from_secs(5),
    snapshot_interval: Duration::from_secs(60),
    recycle_interval: Duration::from_secs(6 * 60 * 60),
    reconnect_base: Duration::from_secs(1),
    reconnect_max: Duration::from_secs(60),
};

/// Retries with no new snapshots before the connection is rebuilt. A sequence
/// that jumped would otherwise discard every replacement snapshot.
const MAX_STALLED_RESYNCS: u32 = 3;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Reasons a connection ends or cannot start.
#[derive(Debug, Error)]
pub enum StreamError {
    #[error("URL is invalid: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("URL must use wss unless the host is loopback")]
    InsecureUrl,
    #[error("signed header is not a valid header value")]
    InvalidHeader(#[from] InvalidHeaderValue),
    #[error("socket failed: {0}")]
    Socket(#[from] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("server closed the connection")]
    Closed,
    #[error("no frame within the silence limit")]
    Silent,
    #[error("no subscription is announced on this connection")]
    NoSubscription,
    #[error("snapshot requests are going unanswered")]
    SnapshotsStalled,
}

/// The durations that govern one stream.
#[derive(Debug, Clone, Copy)]
pub struct StreamTiming {
    /// How long a connection may stay silent. Pings count as traffic.
    pub silence_limit: Duration,
    /// How often unanswered snapshot requests are sent again.
    pub resync_retry: Duration,
    /// How often crossed books are checked.
    pub crossed_check: Duration,
    /// How long a book may stay crossed before it is resynced.
    pub crossed_grace: Duration,
    /// How often every live book is recorded in full.
    pub snapshot_interval: Duration,
    /// How long a connection lives before it is replaced, which guards
    /// against bugs that build up on either side of it. Jitter is added so
    /// sockets do not all reconnect together.
    pub recycle_interval: Duration,
    /// The first delay before reconnecting. It doubles on each failure.
    pub reconnect_base: Duration,
    /// The longest reconnect delay, and how long a connection must last to
    /// count as recovered.
    pub reconnect_max: Duration,
}

/// A subscription to the books of a set of markets.
#[derive(Clone)]
pub struct KalshiStream {
    url: Url,
    signer: Arc<dyn RequestSigner>,
    tickers: Vec<String>,
    timing: StreamTiming,
}

/// A change to the markets of a running stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamCommand {
    Add(Vec<String>),
    Remove(Vec<String>),
}

/// Why a connection ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    Stopped,
    Recycled,
}

/// When a frame arrived, taken once before any parsing.
#[derive(Debug, Clone, Copy)]
struct Receipt {
    monotonic: Instant,
    utc: DateTime<Utc>,
}

/// The state of one connection.
struct Session {
    socket: Socket,
    normaliser: Normaliser,
    collector: Collector,
    next_command_id: u64,
    io_timeout: Duration,
}

/// Receives each collector output.
pub trait OutputHandler: FnMut(&Collector, &CollectorOutput, DateTime<Utc>) {}

impl<T: FnMut(&Collector, &CollectorOutput, DateTime<Utc>)> OutputHandler for T {}

impl KalshiStream {
    /// Creates a stream. Nothing connects until [`Self::run`].
    ///
    /// # Errors
    /// Returns a [`StreamError`] when the URL is invalid or insecure.
    pub fn try_new(
        url: &str,
        signer: Arc<dyn RequestSigner>,
        tickers: Vec<String>,
        timing: StreamTiming,
    ) -> Result<Self, StreamError> {
        let url = Url::parse(url)?;
        if !transport::is_secure(&url) {
            return Err(StreamError::InsecureUrl);
        }
        Ok(Self {
            url,
            signer,
            tickers,
            timing,
        })
    }

    /// Returns the same stream for a different set of markets.
    pub fn for_tickers(&self, tickers: Vec<String>) -> Self {
        Self {
            tickers,
            ..self.clone()
        }
    }

    /// Runs forever, reconnecting as needed. `on_output` sees each collector
    /// output with the state that produced it and its UTC receipt time.
    pub async fn run(
        &self,
        mut commands: UnboundedReceiver<StreamCommand>,
        mut on_output: impl OutputHandler,
    ) {
        let mut tickers = self.tickers.clone();
        let mut attempt = 0;
        loop {
            let started = Instant::now();
            let session = self
                .run_session(&mut tickers, &mut commands, &mut on_output)
                .await;
            if session.is_err() {
                let released = CollectorOutput::Released(tickers.clone());
                let no_books = Collector::new([], self.timing.crossed_grace);
                on_output(&no_books, &released, Utc::now());
            }
            let error = match session {
                Ok(SessionEnd::Stopped) => return,
                Ok(SessionEnd::Recycled) => {
                    info!("Recycled the connection");
                    attempt = 0;
                    continue;
                }
                Err(error) => error,
            };
            // Only a connection that lasted counts as recovery. One that drops
            // right after its acknowledgement must still back off.
            let stable = started.elapsed() >= self.timing.reconnect_max;
            attempt = if stable { 0 } else { attempt + 1 };
            let delay = transport::backoff(
                attempt,
                self.timing.reconnect_base,
                self.timing.reconnect_max,
            );
            warn!(
                %error,
                attempt,
                markets = tickers.len(),
                uptime_secs = started.elapsed().as_secs(),
                "Connection lost; reconnecting"
            );
            sleep(transport::with_jitter(delay)).await;
        }
    }

    async fn run_session(
        &self,
        tickers: &mut Vec<String>,
        commands: &mut UnboundedReceiver<StreamCommand>,
        on_output: &mut impl OutputHandler,
    ) -> Result<SessionEnd, StreamError> {
        let mut session = Session {
            socket: self.connect().await?,
            normaliser: Normaliser::default(),
            collector: Collector::new(tickers.iter().cloned(), self.timing.crossed_grace),
            next_command_id: 2,
            io_timeout: self.timing.silence_limit,
        };
        if !tickers.is_empty() {
            session
                .send(subscribe_command(1, tickers).to_string())
                .await?;
            info!(markets = tickers.len(), "Subscribed");
        }

        let mut crossed_check = interval(self.timing.crossed_check);
        crossed_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let retry_start = tokio::time::Instant::now() + self.timing.resync_retry;
        let mut resync_retry = interval_at(retry_start, self.timing.resync_retry);
        resync_retry.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let snapshot_start = tokio::time::Instant::now() + self.timing.snapshot_interval;
        let mut snapshot_timer = interval_at(snapshot_start, self.timing.snapshot_interval);
        snapshot_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let recycle = sleep(transport::with_jitter(self.timing.recycle_interval));
        tokio::pin!(recycle);
        let mut last_frame = tokio::time::Instant::now();
        let mut last_awaiting = HashSet::new();
        let mut stalled_resyncs = 0;
        loop {
            tokio::select! {
                // Checked in order, with silence last, so a task that was held
                // up by a slow writer reads its waiting frames before it can
                // judge the connection silent.
                biased;
                () = &mut recycle => {
                    // Before closing, because closing can take a while.
                    let released = CollectorOutput::Released(tickers.clone());
                    on_output(&session.collector, &released, Utc::now());
                    session.close().await;
                    return Ok(SessionEnd::Recycled);
                }
                command = commands.recv(), if session.can_accept_commands(tickers) => {
                    let Some(command) = command else {
                        session.close().await;
                        return Ok(SessionEnd::Stopped);
                    };
                    session.apply(command, tickers, on_output).await?;
                }
                _ = crossed_check.tick() => {
                    let outputs = session.collector.tick(Instant::now());
                    session.publish(outputs, Utc::now(), on_output).await?;
                }
                _ = snapshot_timer.tick() => {
                    let outputs = session.collector.snapshots();
                    session.publish(outputs, Utc::now(), on_output).await?;
                }
                _ = resync_retry.tick() => {
                    let awaiting = session.collector.awaiting();
                    let current: HashSet<String> = awaiting.iter().cloned().collect();
                    // Progress is a market leaving the set. Its size would
                    // also change when markets are added.
                    let progressed = last_awaiting.iter().any(|symbol| !current.contains(symbol));
                    stalled_resyncs = if awaiting.is_empty() || progressed {
                        0
                    } else {
                        stalled_resyncs + 1
                    };
                    last_awaiting = current;
                    if stalled_resyncs >= MAX_STALLED_RESYNCS {
                        // One stuck market must not cost its healthy neighbours.
                        if session.collector.live_books().next().is_none() {
                            return Err(StreamError::SnapshotsStalled);
                        }
                        warn!(markets = awaiting.len(), "Snapshots are overdue");
                    }
                    if !awaiting.is_empty() {
                        session.request_snapshots(&awaiting).await?;
                    }
                }
                frame = session.socket.next() => {
                    let received = Receipt { monotonic: Instant::now(), utc: Utc::now() };
                    last_frame = tokio::time::Instant::now();
                    match frame.ok_or(StreamError::Closed)?.map_err(Box::new)? {
                        Message::Text(text) => {
                            trace!(frame = %text, "Received");
                            session.on_text(&text, received, on_output).await?;
                        }
                        Message::Close(_) => return Err(StreamError::Closed),
                        _ => {}
                    }
                }
                () = sleep_until(last_frame + self.timing.silence_limit) => {
                    return Err(StreamError::Silent);
                }
            }
        }
    }

    /// Signs the handshake path. Each connection signs again, because a
    /// signature carries its own timestamp.
    async fn connect(&self) -> Result<Socket, StreamError> {
        let signed = self
            .signer
            .sign(Utc::now().timestamp_millis(), "GET", self.url.path());
        let mut request = self.url.as_str().into_client_request().map_err(Box::new)?;
        request.headers_mut().extend(auth_header_map(&signed)?);
        let (socket, _) = timeout(self.timing.silence_limit, connect_async(request))
            .await
            .map_err(|_| StreamError::Silent)?
            .map_err(Box::new)?;
        Ok(socket)
    }
}

impl Session {
    /// A change needs the subscription ID, so commands wait for the
    /// acknowledgement. A socket with no markets has none to wait for.
    fn can_accept_commands(&self, tickers: &[String]) -> bool {
        self.normaliser.subscription().is_some() || tickers.is_empty()
    }

    /// Changes the set first, so that if the command cannot be sent, the next
    /// connection subscribes to the right markets anyway.
    async fn apply(
        &mut self,
        command: StreamCommand,
        tickers: &mut Vec<String>,
        on_output: &mut impl OutputHandler,
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
                let released = CollectorOutput::Released(removed.clone());
                on_output(&self.collector, &released, Utc::now());
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

    /// Sends a Close frame. The connection is ending either way, so a failure
    /// here changes nothing.
    async fn close(&mut self) {
        match timeout(self.io_timeout, self.socket.close(None)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "Close frame failed"),
            Err(_) => warn!("Close frame timed out"),
        }
    }

    /// An unusable frame may have carried a book change, so it counts as lost
    /// continuity rather than as nothing.
    async fn on_text(
        &mut self,
        text: &str,
        received: Receipt,
        on_output: &mut impl OutputHandler,
    ) -> Result<(), StreamError> {
        let event = self.normaliser.normalise(text).unwrap_or_else(|error| {
            warn!(%error, "Frame rejected");
            Some(MarketEvent::ContinuityLost)
        });
        let Some(event) = event else {
            return Ok(());
        };
        let outputs = self.collector.handle(event, received.monotonic);
        self.publish(outputs, received.utc, on_output).await
    }

    async fn publish(
        &mut self,
        outputs: Vec<CollectorOutput>,
        received_at: DateTime<Utc>,
        on_output: &mut impl OutputHandler,
    ) -> Result<(), StreamError> {
        for output in outputs {
            on_output(&self.collector, &output, received_at);
            if let CollectorOutput::Resync(symbols) = &output {
                self.request_snapshots(symbols).await?;
            }
        }
        Ok(())
    }

    async fn request_snapshots(&mut self, symbols: &[String]) -> Result<(), StreamError> {
        let sid = self.subscription()?;
        let id = self.next_id();
        self.send(get_snapshot_command(id, sid, symbols).to_string())
            .await
    }

    /// A failed send ends the connection, so a lost command cannot go unseen.
    /// The timeout matters because this runs outside the watchdog's select.
    async fn send(&mut self, command: String) -> Result<(), StreamError> {
        timeout(
            self.io_timeout,
            self.socket.send(Message::Text(command.into())),
        )
        .await
        .map_err(|_| StreamError::Silent)?
        .map_err(Box::new)?;
        Ok(())
    }
}
