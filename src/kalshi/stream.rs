//! Kalshi WebSocket stream: connects, subscribes, and drives the normaliser
//! and collector. Sequence numbers are not comparable across connections, so
//! every connection starts with fresh state and fresh snapshots. Reconnection
//! continues until the command channel closes, with a delay that grows to a
//! cap. The stream reads that channel only on a connection that can accept
//! commands, and then sends a Close frame and stops.
//!
//! Handshake signing: <https://docs.kalshi.com/getting_started/quick_start_websockets>.

use crate::collector::Collector;
use crate::kalshi::auth::{RequestSigner, auth_header_map};
use crate::kalshi::normalise::Normaliser;
use crate::kalshi::session::{Receipt, Session, Socket};
use crate::kalshi::wire::subscribe_command;
use crate::provider::{FeedHandler, MarketStream, StreamCommand};
use crate::resync::{ResyncDecision, ResyncWatch};
use crate::transport;
use chrono::Utc;
use futures_util::StreamExt;
use reqwest::header::InvalidHeaderValue;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{MissedTickBehavior, interval, interval_at, sleep, sleep_until, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{info, trace, warn};
use url::Url;

/// Default timing. Kalshi pings every 10 seconds, so 30 seconds of
/// silence means three missed pings.
/// How often a stream checks that its commands are still open.
const CLOSED_CHECK: Duration = Duration::from_millis(100);
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

/// The connections Kalshi allows a Basic account.
const CONNECTION_LIMIT: usize = 200;

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
    #[error("a send or handshake passed the time limit")]
    TimedOut,
    #[error("no subscription is announced on this connection")]
    NoSubscription,
    #[error("snapshot requests are going unanswered")]
    SnapshotsStalled,
}

/// The durations that govern one stream.
#[derive(Debug, Clone, Copy)]
pub struct StreamTiming {
    /// How long a connection may stay silent. Pings count as traffic. Also
    /// bounds the handshake, each send and the Close frame.
    pub silence_limit: Duration,
    /// How often unanswered snapshot requests are sent again.
    pub resync_retry: Duration,
    /// How often crossed books are checked.
    pub crossed_check: Duration,
    /// How long a book may stay crossed before it is resynced.
    pub crossed_grace: Duration,
    /// How often every live book is recorded in full.
    pub snapshot_interval: Duration,
    /// How long a connection lives before it is replaced, which limits faults
    /// that accumulate over its lifetime on either side. Jitter is added so
    /// sockets do not all reconnect together.
    pub recycle_interval: Duration,
    /// The shortest reconnect delay. The upper bound from which each delay is
    /// drawn doubles on each failure.
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

/// Why a connection ended without an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    Stopped,
    Recycled,
}

/// Waits for the sender to go. The receiver offers only this check, and it
/// must not consume commands that a later connection will apply.
async fn until_closed(commands: &UnboundedReceiver<StreamCommand>) {
    while !commands.is_closed() {
        sleep(CLOSED_CHECK).await;
    }
}

impl MarketStream for KalshiStream {
    const CONNECTION_LIMIT: usize = CONNECTION_LIMIT;

    fn for_markets(&self, tickers: Vec<String>) -> Self {
        Self {
            tickers,
            ..self.clone()
        }
    }

    /// Runs until `commands` closes, reconnecting as needed.
    async fn run<H: FeedHandler + Send>(
        &self,
        mut commands: UnboundedReceiver<StreamCommand>,
        mut handler: H,
    ) {
        let mut tickers = self.tickers.clone();
        let mut attempt = 0;
        loop {
            let started = Instant::now();
            let session = self
                .run_session(&mut tickers, &mut commands, &mut handler)
                .await;
            if session.is_err() {
                handler.on_released(&tickers);
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
            attempt =
                transport::next_attempt(attempt, started.elapsed(), self.timing.reconnect_max);
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
            tokio::select! {
                () = sleep(transport::spread(delay, self.timing.reconnect_base)) => {}
                () = until_closed(&commands) => return,
            }
        }
    }
}

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

    async fn run_session(
        &self,
        tickers: &mut Vec<String>,
        commands: &mut UnboundedReceiver<StreamCommand>,
        handler: &mut impl FeedHandler,
    ) -> Result<SessionEnd, StreamError> {
        let socket = tokio::select! {
            socket = self.connect() => socket?,
            () = until_closed(commands) => return Ok(SessionEnd::Stopped),
        };
        let mut session = Session {
            socket,
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
        // A random start spreads sockets across the interval, so they do not
        // all stop reading to write their books at once.
        let snapshot_start =
            tokio::time::Instant::now() + transport::full_jitter(self.timing.snapshot_interval);
        let mut snapshot_timer = interval_at(snapshot_start, self.timing.snapshot_interval);
        snapshot_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let recycle = sleep(transport::with_jitter(self.timing.recycle_interval));
        tokio::pin!(recycle);
        let mut closed_check = interval(CLOSED_CHECK);
        closed_check.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_frame = tokio::time::Instant::now();
        let mut resyncs = ResyncWatch::default();
        loop {
            tokio::select! {
                // Checked in order, with silence last, so a task that was held
                // up by a slow writer reads its waiting frames before it can
                // judge the connection silent.
                biased;
                () = &mut recycle => {
                    // Before closing, because closing can take a while.
                    handler.on_released(tickers);
                    session.close().await;
                    return Ok(SessionEnd::Recycled);
                }
                command = commands.recv(), if session.can_accept_commands(tickers) => {
                    let Some(command) = command else {
                        session.close().await;
                        return Ok(SessionEnd::Stopped);
                    };
                    session.apply(command, tickers, handler).await?;
                }
                // The command branch is disabled until the subscription is
                // acknowledged, so it cannot be the only way to see a close.
                _ = closed_check.tick() => {
                    if commands.is_closed() {
                        session.close().await;
                        return Ok(SessionEnd::Stopped);
                    }
                }
                _ = crossed_check.tick() => {
                    let outputs = session.collector.tick(Instant::now());
                    session.publish(outputs, Utc::now(), handler).await?;
                }
                _ = snapshot_timer.tick() => {
                    let outputs = session.collector.snapshots();
                    session.publish(outputs, Utc::now(), handler).await?;
                }
                _ = resync_retry.tick() => {
                    let awaiting = session.collector.awaiting();
                    let has_live_books = session.collector.live_books().next().is_some();
                    match resyncs.review(&awaiting, has_live_books) {
                        ResyncDecision::Idle => {}
                        ResyncDecision::Rebuild => return Err(StreamError::SnapshotsStalled),
                        ResyncDecision::RetryOverdue => {
                            warn!(markets = awaiting.len(), "Snapshots are overdue");
                            session.request_snapshots(&awaiting).await?;
                        }
                        ResyncDecision::Retry => session.request_snapshots(&awaiting).await?,
                    }
                }
                frame = session.socket.next() => {
                    let received = Receipt { monotonic: Instant::now(), utc: Utc::now() };
                    last_frame = tokio::time::Instant::now();
                    match frame.ok_or(StreamError::Closed)?.map_err(Box::new)? {
                        Message::Text(text) => {
                            trace!(frame = %text, "Received");
                            session.on_text(&text, received, handler).await?;
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
            .map_err(|_| StreamError::TimedOut)?
            .map_err(Box::new)?;
        Ok(socket)
    }
}
