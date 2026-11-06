//! The application: builds its dependencies once and runs one command until it
//! finishes or a shutdown is requested.

use crate::collector::{Collector, CollectorOutput};
use crate::discovery::MarketTracker;
use crate::failover::{Failover, FeedId};
use crate::kalshi::auth::{DefaultRequestSigner, RequestSigner};
use crate::kalshi::rest::{KalshiClient, MarketQuery};
use crate::kalshi::stream::{DEFAULT_TIMING, KalshiStream};
use crate::manager::SocketManager;
use crate::output;
use crate::provider::{MarketDirectory, MarketStream};
use crate::router::FeedRouter;
use crate::types::{AppError, CliCommand, CollectOptions, Config};
use chrono::{DateTime, Utc};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::task::spawn_blocking;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Records held for the writer. A full queue blocks the sending feed. The
/// capacity is a record count, so its memory use depends on book sizes.
const OUTPUT_QUEUE_CAPACITY: usize = 65_536;
/// How often new and closed markets are looked up.
const MARKET_POLL_INTERVAL: Duration = Duration::from_secs(30);
/// How often every placed market is compared with a full scan.
const MARKET_RESCAN_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// How long the writer gets to finish. A reader that stopped reading must not
/// hold the process open forever.
const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const SAMPLE_SIZE: usize = 5;

/// The provider clients and shutdown token of one run.
pub struct App {
    kalshi: KalshiClient,
    stream: KalshiStream,
    shutdown: CancellationToken,
}

impl App {
    /// Loads the signing key and builds the Kalshi client and stream.
    ///
    /// # Errors
    /// Returns an [`AppError`] when the key cannot be read or used, or the
    /// client or stream cannot be built.
    pub fn new(config: &Config) -> Result<Self, AppError> {
        let pem = std::fs::read_to_string(&config.kalshi_private_key_path)?;
        let signer: Arc<dyn RequestSigner> = Arc::new(DefaultRequestSigner::from_pem(
            config.kalshi_api_key_id.clone(),
            &pem,
        )?);
        Ok(Self {
            kalshi: KalshiClient::try_new(&config.kalshi_rest_url, signer.clone())?,
            stream: KalshiStream::try_new(&config.kalshi_ws_url, signer, vec![], DEFAULT_TIMING)?,
            shutdown: CancellationToken::new(),
        })
    }

    /// Returns the token that stops this app. Long-running tasks watch a clone
    /// of it, so one request reaches all of them.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Runs `command` until it finishes, a stop signal arrives, or the token is
    /// cancelled. A requested stop is a success.
    ///
    /// # Errors
    /// Returns the command's error, or [`AppError::Io`] when the signal
    /// listener cannot be installed.
    pub async fn run(&self, command: CliCommand) -> Result<(), AppError> {
        // Collection watches for shutdown itself, so it can drain its output.
        if matches!(command, CliCommand::Collect(_)) {
            return self.execute(command).await;
        }
        tokio::select! {
            result = self.execute(command) => result,
            stopped = self.shutdown_requested() => {
                info!("Shutting down");
                Ok(stopped?)
            }
        }
    }

    async fn execute(&self, command: CliCommand) -> Result<(), AppError> {
        match command {
            CliCommand::Check => self.check().await,
            CliCommand::Discover => self.discover().await,
            CliCommand::Watch { ticker } => {
                let (_commands, receiver) = unbounded_channel();
                let stream = self.stream.for_markets(vec![ticker]);
                stream.run(receiver, log_top_of_book).await;
                Ok(())
            }
            CliCommand::Collect(options) => {
                self.collect(&self.kalshi, self.stream.clone(), options)
                    .await
            }
            CliCommand::Verify { path } => verify(&path),
        }
    }

    /// On shutdown, closes every socket and then waits up to
    /// `WRITER_DRAIN_TIMEOUT` for the writer to drain, so that queued records
    /// reach the file and it ends on a whole chunk.
    async fn collect<D: MarketDirectory, S: MarketStream>(
        &self,
        directory: &D,
        stream: S,
        options: CollectOptions,
    ) -> Result<(), AppError> {
        let (sender, receiver) = channel(OUTPUT_QUEUE_CAPACITY);
        let path = options.output;
        let writer = spawn_blocking(move || output::write_records(receiver, &path));
        let failover = Arc::new(Mutex::new(Failover::default()));
        let handlers = (0..options.feeds)
            .map(|feed| {
                let feed = FeedId(feed);
                FeedRouter::new(
                    feed,
                    failover.clone(),
                    sender.clone(),
                    self.shutdown.clone(),
                )
            })
            .collect();
        drop(sender);
        let mut manager = SocketManager::new(stream, options.markets_per_socket, handlers);
        let tracker = MarketTracker::new(directory, MARKET_POLL_INTERVAL, MARKET_RESCAN_INTERVAL);
        let fed = tokio::select! {
            () = tracker.run(&mut manager, options.max_markets) => Ok(()),
            stopped = self.shutdown_requested() => stopped.map_err(AppError::from),
        };
        info!("Closing sockets and draining output");
        manager.close().await;
        timeout(WRITER_DRAIN_TIMEOUT, writer)
            .await
            .map_err(|_| AppError::WriterStalled)???;
        fed
    }

    /// Turns SIGINT or SIGTERM into a cancelled token, so every watcher sees
    /// one signal.
    async fn shutdown_requested(&self) -> std::io::Result<()> {
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                self.shutdown.cancel();
                signal
            }
            _ = terminate.recv() => {
                self.shutdown.cancel();
                Ok(())
            }
            () = self.shutdown.cancelled() => Ok(()),
        }
    }

    async fn check(&self) -> Result<(), AppError> {
        let limits = self.kalshi.account_limits().await?;
        info!(?limits, "Kalshi accepted the signature");
        Ok(())
    }

    async fn discover(&self) -> Result<(), AppError> {
        let markets = self.kalshi.markets(MarketQuery::Open).await?;
        let sample: Vec<_> = markets.iter().take(SAMPLE_SIZE).collect();
        info!(count = markets.len(), ?sample, "Discovery finished");
        Ok(())
    }
}

/// Reports what a file of records holds, after decoding every field.
///
/// # Errors
/// Returns an [`AppError`] when the file cannot be read or decoded.
pub fn verify(path: &Path) -> Result<(), AppError> {
    let summary = output::summarise(path)?;
    info!(?summary, "Records decoded");
    Ok(())
}

fn log_top_of_book(collector: &Collector, output: &CollectorOutput, _: DateTime<Utc>) {
    if let CollectorOutput::Resync(symbols) = output {
        info!(markets = symbols.len(), "Requesting replacement snapshots");
    }
    for (symbol, book) in collector.live_books() {
        let bid = book.bids().next().map(|level| level.price.raw());
        let ask = book.asks().next().map(|level| level.price.raw());
        info!(symbol, ?bid, ?ask, "Top of book");
    }
}
