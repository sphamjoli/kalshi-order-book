//! Stream behaviour against a local WebSocket server.

use chrono::{DateTime, Utc};
use engineering_challenge::collector::{BookRecord, Collector, CollectorOutput};
use engineering_challenge::kalshi::auth::{
    AuthHeaders, KEY_HEADER, RequestSigner, SIGNATURE_HEADER, TIMESTAMP_HEADER,
};
use engineering_challenge::kalshi::stream::{KalshiStream, StreamError, StreamTiming};
use engineering_challenge::provider::{FeedHandler, MarketStream, StreamCommand};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

const TIMING: StreamTiming = StreamTiming {
    silence_limit: Duration::from_secs(5),
    resync_retry: Duration::from_millis(100),
    crossed_check: Duration::from_millis(50),
    crossed_grace: Duration::from_secs(5),
    snapshot_interval: Duration::from_secs(3600),
    recycle_interval: Duration::from_secs(3600),
    reconnect_base: Duration::from_millis(1),
    reconnect_max: Duration::from_millis(5),
};
const WAIT: Duration = Duration::from_secs(5);

struct EchoSigner;

impl RequestSigner for EchoSigner {
    fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> AuthHeaders {
        AuthHeaders {
            key_id: "test-key".to_owned(),
            timestamp_ms: timestamp_ms.to_string(),
            signature: format!("{method}{path}"),
        }
    }
}

struct Server {
    listener: TcpListener,
    url: String,
}

impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/trade-api/ws/v2", listener.local_addr().unwrap());
        Self { listener, url }
    }

    /// Accepts one connection and returns it with its signature header.
    #[allow(
        clippy::result_large_err,
        reason = "tungstenite fixes this callback signature"
    )]
    async fn accept(&self) -> (WebSocketStream<TcpStream>, String) {
        let (tcp, _) = timeout(WAIT, self.listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut signature = String::new();
        let socket = accept_hdr_async(tcp, |request: &Request, response: Response| {
            let headers = request.headers();
            assert!(headers.contains_key(KEY_HEADER), "key header is required");
            assert!(
                headers.contains_key(TIMESTAMP_HEADER),
                "timestamp is required"
            );
            signature = headers[SIGNATURE_HEADER].to_str().unwrap().to_owned();
            Ok(response)
        })
        .await
        .unwrap();
        (socket, signature)
    }
}

/// What a stream told its handler.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Output(CollectorOutput),
    Released(Vec<String>),
}

/// Passes everything a stream reports to a channel.
struct Forward(UnboundedSender<Seen>);

impl FeedHandler for Forward {
    fn on_output(&mut self, _: &Collector, output: &CollectorOutput, _: DateTime<Utc>) {
        self.0.send(Seen::Output(output.clone())).unwrap();
    }

    fn on_released(&mut self, symbols: &[String]) {
        self.0.send(Seen::Released(symbols.to_vec())).unwrap();
    }
}

fn ignore(_: &Collector, _: &CollectorOutput, _: DateTime<Utc>) {}

fn start(url: &str) -> UnboundedReceiver<Seen> {
    start_with(url, TIMING)
}

/// Starts a stream for market `M` and returns its outputs as a channel.
fn start_with(url: &str, timing: StreamTiming) -> UnboundedReceiver<Seen> {
    let stream =
        KalshiStream::try_new(url, Arc::new(EchoSigner), vec!["M".to_owned()], timing).unwrap();
    let (sender, receiver) = unbounded_channel();
    let (commands, command_receiver) = unbounded_channel();
    tokio::spawn(async move {
        let _commands = commands;
        stream.run(command_receiver, Forward(sender)).await;
    });
    receiver
}

async fn send(socket: &mut WebSocketStream<TcpStream>, frame: Value) {
    socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
}

async fn next_command(socket: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        let frame = timeout(WAIT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = frame {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

fn ack() -> Value {
    json!({ "id": 1, "type": "subscribed", "msg": { "channel": "orderbook_delta", "sid": 7 } })
}

fn snapshot(seq: u64) -> Value {
    json!({
        "type": "orderbook_snapshot", "sid": 7, "seq": seq, "sending_ts_ms": 1,
        "msg": { "market_ticker": "M", "yes_dollars_fp": [["0.40", "1.00"]] },
    })
}

fn delta(seq: u64) -> Value {
    json!({
        "type": "orderbook_delta", "sid": 7, "seq": seq, "sending_ts_ms": 1,
        "msg": {
            "market_ticker": "M", "price_dollars": "0.40", "delta_fp": "1.00",
            "side": "yes", "ts_ms": 2,
        },
    })
}

async fn next_output(outputs: &mut UnboundedReceiver<Seen>) -> Seen {
    timeout(WAIT, outputs.recv()).await.unwrap().unwrap()
}

#[tokio::test]
async fn signs_the_handshake_subscribes_and_follows_the_book() {
    let server = Server::start().await;
    let mut outputs = start(&server.url);
    let (mut socket, signature) = server.accept().await;
    assert_eq!(signature, "GET/trade-api/ws/v2");
    let command = next_command(&mut socket).await;
    assert_eq!(command["cmd"], "subscribe");
    assert_eq!(command["params"]["market_tickers"], json!(["M"]));

    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    send(&mut socket, delta(2)).await;
    assert!(matches!(
        next_output(&mut outputs).await,
        Seen::Output(CollectorOutput::Record(BookRecord::Snapshot { .. }))
    ));
    let Seen::Output(CollectorOutput::Record(BookRecord::Update { change, .. })) =
        next_output(&mut outputs).await
    else {
        panic!("expected update");
    };
    assert_eq!(change.quantity.raw(), 2_000_000);
}

#[tokio::test]
async fn a_gap_requests_a_replacement_snapshot() {
    let server = Server::start().await;
    let mut outputs = start(&server.url);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    send(&mut socket, delta(9)).await;
    next_output(&mut outputs).await;
    assert_eq!(
        next_output(&mut outputs).await,
        Seen::Output(CollectorOutput::Resync(vec!["M".to_owned()]))
    );
    let command = next_command(&mut socket).await;
    assert_eq!(command["params"]["action"], "get_snapshot");
    assert_eq!(command["params"]["sids"], json!([7]));
}

#[tokio::test]
async fn a_malformed_frame_counts_as_lost_continuity() {
    let server = Server::start().await;
    let mut outputs = start(&server.url);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    socket.send(Message::Text("not json".into())).await.unwrap();
    next_output(&mut outputs).await;
    assert!(matches!(
        next_output(&mut outputs).await,
        Seen::Output(CollectorOutput::Resync(_))
    ));
}

#[tokio::test]
async fn an_unanswered_snapshot_request_is_sent_again() {
    let server = Server::start().await;
    let _outputs = start(&server.url);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    for _ in 0..2 {
        let command = next_command(&mut socket).await;
        assert_eq!(command["params"]["action"], "get_snapshot");
    }
}

#[tokio::test]
async fn a_lost_connection_reconnects_and_starts_from_a_new_snapshot() {
    let server = Server::start().await;
    let mut outputs = start(&server.url);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(50)).await;
    next_output(&mut outputs).await;
    drop(socket);

    let (mut socket, _) = server.accept().await;
    assert_eq!(next_command(&mut socket).await["cmd"], "subscribe");
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    let released = Seen::Released(vec!["M".to_owned()]);
    assert_eq!(next_output(&mut outputs).await, released);
    assert!(matches!(
        next_output(&mut outputs).await,
        Seen::Output(CollectorOutput::Record(BookRecord::Snapshot { .. }))
    ));
}

#[test]
fn plain_ws_is_only_allowed_on_loopback() {
    let stream = KalshiStream::try_new("ws://example.com/ws", Arc::new(EchoSigner), vec![], TIMING);
    assert!(matches!(stream, Err(StreamError::InsecureUrl)));
}

#[tokio::test]
async fn every_live_book_is_snapshotted_on_the_timer() {
    let server = Server::start().await;
    let timing = StreamTiming {
        snapshot_interval: Duration::from_millis(20),
        ..TIMING
    };
    let mut outputs = start_with(&server.url, timing);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    for _ in 0..3 {
        assert!(matches!(
            next_output(&mut outputs).await,
            Seen::Output(CollectorOutput::Record(BookRecord::Snapshot { .. }))
        ));
    }
}

#[tokio::test]
async fn a_whole_socket_is_recycled_on_the_timer() {
    let server = Server::start().await;
    let timing = StreamTiming {
        recycle_interval: Duration::from_millis(50),
        ..TIMING
    };
    let mut outputs = start_with(&server.url, timing);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    let released = Seen::Released(vec!["M".to_owned()]);
    assert_eq!(next_output(&mut outputs).await, released);
    let (mut socket, _) = server.accept().await;
    assert_eq!(next_command(&mut socket).await["cmd"], "subscribe");
}

fn stream(url: &str, tickers: Vec<String>) -> KalshiStream {
    KalshiStream::try_new(url, Arc::new(EchoSigner), tickers, TIMING).unwrap()
}

#[tokio::test]
async fn markets_are_added_and_removed_on_a_running_socket() {
    let server = Server::start().await;
    let (commands, receiver) = unbounded_channel();
    let (sender, mut outputs) = unbounded_channel();
    let stream = stream(&server.url, vec!["M".to_owned()]);
    tokio::spawn(async move {
        stream.run(receiver, Forward(sender)).await;
    });
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    next_output(&mut outputs).await;

    commands
        .send(StreamCommand::Add(vec!["N".to_owned()]))
        .unwrap();
    let added = next_command(&mut socket).await;
    assert_eq!(added["params"]["action"], "add_markets");
    assert_eq!(added["params"]["market_tickers"], json!(["N"]));

    commands
        .send(StreamCommand::Remove(vec!["M".to_owned()]))
        .unwrap();
    let removed = next_command(&mut socket).await;
    assert_eq!(removed["params"]["action"], "delete_markets");
    let released = Seen::Released(vec!["M".to_owned()]);
    assert_eq!(next_output(&mut outputs).await, released);

    // A delta already in flight for a removed market is not corruption.
    send(&mut socket, delta(2)).await;
    send(&mut socket, json!({ "type": "ok", "sid": 7, "seq": 3 })).await;
    let silent = timeout(Duration::from_millis(100), outputs.recv()).await;
    assert!(silent.is_err(), "expected no output, got {silent:?}");
}

#[tokio::test]
async fn dropping_the_commands_closes_the_socket_cleanly() {
    let server = Server::start().await;
    let (commands, receiver) = unbounded_channel::<StreamCommand>();
    let stream = stream(&server.url, vec!["M".to_owned()]);
    let task = tokio::spawn(async move { stream.run(receiver, ignore).await });
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    drop(commands);
    let frame = timeout(WAIT, socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(frame, Message::Close(_)));
    timeout(WAIT, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn a_command_waits_for_the_acknowledgement() {
    let server = Server::start().await;
    let (commands, receiver) = unbounded_channel();
    let stream = stream(&server.url, vec!["M".to_owned()]);
    tokio::spawn(async move { stream.run(receiver, ignore).await });
    commands
        .send(StreamCommand::Add(vec!["N".to_owned()]))
        .unwrap();
    let (mut socket, _) = server.accept().await;
    assert_eq!(next_command(&mut socket).await["cmd"], "subscribe");
    send(&mut socket, ack()).await;
    let added = next_command(&mut socket).await;
    assert_eq!(added["params"]["action"], "add_markets");
}

#[tokio::test]
async fn unanswered_snapshots_rebuild_the_connection() {
    let server = Server::start().await;
    let _outputs = start(&server.url);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    tokio::spawn(async move { while socket.next().await.is_some() {} });
    let (mut socket, _) = server.accept().await;
    assert_eq!(next_command(&mut socket).await["cmd"], "subscribe");
}

#[tokio::test]
async fn a_peer_that_never_upgrades_is_retried() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let timing = StreamTiming {
        silence_limit: Duration::from_millis(50),
        ..TIMING
    };
    let _outputs = start_with(&url, timing);
    let first = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let second = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    drop((first, second));
}

#[tokio::test]
async fn a_connection_that_drops_after_its_acknowledgement_backs_off() {
    let server = Server::start().await;
    let timing = StreamTiming {
        reconnect_base: Duration::from_millis(50),
        reconnect_max: Duration::from_secs(1),
        ..TIMING
    };
    let _outputs = start_with(&server.url, timing);
    let (mut socket, _) = server.accept().await;
    let started = std::time::Instant::now();
    for _ in 0..3 {
        next_command(&mut socket).await;
        send(&mut socket, ack()).await;
        drop(socket);
        (socket, _) = server.accept().await;
    }
    // Three reconnects, each waiting at least the base delay.
    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[tokio::test]
async fn the_first_minute_snapshot_comes_within_one_interval() {
    let interval = Duration::from_millis(300);
    let server = Server::start().await;
    let timing = StreamTiming {
        snapshot_interval: interval,
        ..TIMING
    };
    let mut outputs = start_with(&server.url, timing);
    let (mut socket, _) = server.accept().await;
    let connected = std::time::Instant::now();
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    send(&mut socket, snapshot(1)).await;
    next_output(&mut outputs).await;
    next_output(&mut outputs).await;
    assert!(connected.elapsed() <= interval + interval / 2);
}

/// Runs `stream`, drops its commands once `connected` is done, and expects it
/// to stop.
async fn stops_when_commands_close(stream: KalshiStream, connected: impl Future<Output = ()>) {
    let (commands, receiver) = unbounded_channel::<StreamCommand>();
    let task = tokio::spawn(async move { stream.run(receiver, ignore).await });
    connected.await;
    drop(commands);
    timeout(WAIT, task).await.unwrap().unwrap();
}

fn slow_timing() -> StreamTiming {
    StreamTiming {
        reconnect_base: Duration::from_secs(3600),
        reconnect_max: Duration::from_secs(3600),
        silence_limit: Duration::from_secs(3600),
        resync_retry: Duration::from_secs(3600),
        ..TIMING
    }
}

#[tokio::test]
async fn a_stream_stops_while_waiting_to_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    drop(listener);
    let stream = KalshiStream::try_new(&url, Arc::new(EchoSigner), vec![], slow_timing()).unwrap();
    stops_when_commands_close(stream, sleep(Duration::from_millis(50))).await;
}

#[tokio::test]
async fn a_stream_stops_during_a_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/ws", listener.local_addr().unwrap());
    let stream = KalshiStream::try_new(&url, Arc::new(EchoSigner), vec![], slow_timing()).unwrap();
    let accepted = async {
        let _ = Box::leak(Box::new(listener.accept().await.unwrap()));
    };
    stops_when_commands_close(stream, accepted).await;
}

#[tokio::test]
async fn a_stream_stops_before_its_subscription_is_acknowledged() {
    let server = Server::start().await;
    let tickers = vec!["M".to_owned()];
    let stream =
        KalshiStream::try_new(&server.url, Arc::new(EchoSigner), tickers, slow_timing()).unwrap();
    let subscribed = async {
        let (mut socket, _) = server.accept().await;
        next_command(&mut socket).await;
        Box::leak(Box::new(socket));
    };
    stops_when_commands_close(stream, subscribed).await;
}
