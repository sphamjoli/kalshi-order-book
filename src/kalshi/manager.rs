//! Socket manager: decides which socket holds each market and keeps the
//! sockets running. Sockets do the I/O; this module only places markets.

use crate::kalshi::stream::{KalshiStream, OutputHandler, StreamCommand};
use futures_util::future::join_all;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{info, warn};

/// The connections Kalshi allows a Basic account.
const CONNECTION_LIMIT: usize = 200;
/// How long sockets get to send a Close frame before they are aborted.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// A handler that each socket task can own a copy of.
pub trait StreamHandler: OutputHandler + Clone + Send + 'static {}

impl<T: OutputHandler + Clone + Send + 'static> StreamHandler for T {}

/// Runs every market on each of several independent feeds, so losing a socket
/// never leaves a market without a live copy. Feeds share no sockets.
pub struct SocketManager<H> {
    feeds: Vec<Feed<H>>,
}

/// One complete set of sockets. Each market sits on exactly one of them.
struct Feed<H> {
    template: KalshiStream,
    capacity: usize,
    handler: H,
    sockets: Vec<SocketHandle>,
    placement: HashMap<String, usize>,
    tasks: JoinSet<()>,
}

/// One running socket.
struct SocketHandle {
    commands: UnboundedSender<StreamCommand>,
    load: usize,
}

impl<H: StreamHandler> SocketManager<H> {
    /// Creates a manager with one feed per handler and no sockets yet.
    /// `capacity` is the most markets one socket may hold.
    pub fn new(template: KalshiStream, capacity: usize, handlers: Vec<H>) -> Self {
        let feeds = handlers
            .into_iter()
            .map(|handler| Feed {
                template: template.clone(),
                capacity,
                handler,
                sockets: Vec::new(),
                placement: HashMap::new(),
                tasks: JoinSet::new(),
            })
            .collect();
        Self { feeds }
    }

    /// Returns how many sockets are running across all feeds.
    pub fn socket_count(&self) -> usize {
        self.feeds.iter().map(|feed| feed.sockets.len()).sum()
    }

    /// Returns how many markets are placed. Every feed holds the same set.
    pub fn market_count(&self) -> usize {
        self.feeds.first().map_or(0, |feed| feed.placement.len())
    }

    /// Returns the markets currently placed.
    pub fn placed(&self) -> HashSet<String> {
        self.feeds.first().map_or_else(HashSet::new, |feed| {
            feed.placement.keys().cloned().collect()
        })
    }

    /// Places every market not yet placed on every feed. Adding a placed
    /// market changes nothing, so callers may overlap.
    pub fn add(&mut self, symbols: Vec<String>) {
        for feed in &mut self.feeds {
            feed.add(symbols.clone());
        }
        if self.socket_count() > CONNECTION_LIMIT {
            warn!(
                sockets = self.socket_count(),
                limit = CONNECTION_LIMIT,
                "More sockets than a Basic account allows"
            );
        }
    }

    /// Removes every placed market from every feed.
    pub fn remove(&mut self, symbols: Vec<String>) {
        for feed in &mut self.feeds {
            feed.remove(symbols.clone());
        }
    }

    /// Closes every socket. A socket still reconnecting is aborted after the
    /// timeout, because it has nothing to close.
    pub async fn close(self) {
        let closing = self.feeds.into_iter().map(Feed::close);
        if timeout(CLOSE_TIMEOUT, join_all(closing)).await.is_err() {
            warn!("Some sockets did not close in time");
        }
    }
}

impl<H: StreamHandler> Feed<H> {
    /// Places every market not yet placed on the least-loaded socket with
    /// room.
    fn add(&mut self, mut symbols: Vec<String>) {
        symbols.retain(|symbol| !self.placement.contains_key(symbol));
        symbols.sort_unstable();
        symbols.dedup();
        if symbols.is_empty() {
            return;
        }
        info!(markets = symbols.len(), "Placing markets");
        while !symbols.is_empty() {
            match self.least_loaded() {
                Some(index) => {
                    let room = self.capacity - self.sockets[index].load;
                    let batch = take_batch(&mut symbols, room);
                    self.record(index, &batch);
                    self.send(index, StreamCommand::Add(batch));
                }
                None => {
                    let batch = take_batch(&mut symbols, self.capacity);
                    self.spawn_socket(batch);
                }
            }
        }
    }

    /// Removes every placed market from its socket.
    fn remove(&mut self, symbols: Vec<String>) {
        let mut by_socket: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in symbols {
            if let Some(index) = self.placement.remove(&symbol) {
                by_socket.entry(index).or_default().push(symbol);
            }
        }
        for (index, batch) in by_socket {
            info!(markets = batch.len(), socket = index, "Removing markets");
            self.sockets[index].load -= batch.len();
            self.send(index, StreamCommand::Remove(batch));
        }
    }

    async fn close(mut self) {
        self.sockets.clear();
        self.tasks.join_all().await;
    }

    fn least_loaded(&self) -> Option<usize> {
        self.sockets
            .iter()
            .enumerate()
            .filter(|(_, socket)| socket.load < self.capacity)
            .min_by_key(|(_, socket)| socket.load)
            .map(|(index, _)| index)
    }

    fn record(&mut self, index: usize, batch: &[String]) {
        self.sockets[index].load += batch.len();
        for symbol in batch {
            self.placement.insert(symbol.clone(), index);
        }
    }

    fn send(&self, index: usize, command: StreamCommand) {
        if self.sockets[index].commands.send(command).is_err() {
            warn!(socket = index, "Socket task has stopped");
        }
    }

    fn spawn_socket(&mut self, batch: Vec<String>) {
        let (commands, receiver) = unbounded_channel();
        let index = self.sockets.len();
        self.sockets.push(SocketHandle { commands, load: 0 });
        self.record(index, &batch);
        let stream = self.template.for_tickers(batch);
        let handler = self.handler.clone();
        self.tasks
            .spawn(async move { stream.run(receiver, handler).await });
    }
}

fn take_batch(symbols: &mut Vec<String>, size: usize) -> Vec<String> {
    symbols.split_off(symbols.len().saturating_sub(size))
}
