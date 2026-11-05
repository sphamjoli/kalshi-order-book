//! Stream behaviour against a local WebSocket server; never touches the network.

use engineering_challenge::collector::CollectorOutput;
use engineering_challenge::kalshi::auth::{
    AuthHeaders, KEY_HEADER, RequestSigner, SIGNATURE_HEADER, TIMESTAMP_HEADER,
};
use engineering_challenge::kalshi::manager::SocketManager;
use engineering_challenge::kalshi::stream::{
    KalshiStream, StreamCommand, StreamError, StreamTiming,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::time::timeout;
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

fn start(url: &str) -> UnboundedReceiver<CollectorOutput> {
    start_with(url, TIMING)
}

/// Starts a stream for market `M` and returns its outputs as a channel.
fn start_with(url: &str, timing: StreamTiming) -> UnboundedReceiver<CollectorOutput> {
    let stream =
        KalshiStream::try_new(url, Arc::new(EchoSigner), vec!["M".to_owned()], timing).unwrap();
    let (sender, receiver) = unbounded_channel();
    let (commands, command_receiver) = unbounded_channel();
    tokio::spawn(async move {
        let _commands = commands;
        stream
            .run(command_receiver, |_, output, _| {
                sender.send(output.clone()).unwrap()
            })
            .await;
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

async fn next_output(outputs: &mut UnboundedReceiver<CollectorOutput>) -> CollectorOutput {
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
        CollectorOutput::Snapshot { .. }
    ));
    let CollectorOutput::Update { change, .. } = next_output(&mut outputs).await else {
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
        CollectorOutput::Resync(vec!["M".to_owned()])
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
        CollectorOutput::Resync(_)
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
    let released = CollectorOutput::Released(vec!["M".to_owned()]);
    assert_eq!(next_output(&mut outputs).await, released);
    assert!(matches!(
        next_output(&mut outputs).await,
        CollectorOutput::Snapshot { .. }
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
            CollectorOutput::Snapshot { .. }
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
    let _outputs = start_with(&server.url, timing);
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
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
        stream
            .run(receiver, |_, output, _| {
                sender.send(output.clone()).unwrap()
            })
            .await;
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
    let released = CollectorOutput::Released(vec!["M".to_owned()]);
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
    let task = tokio::spawn(async move { stream.run(receiver, |_, _, _| {}).await });
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
async fn the_manager_balances_markets_and_opens_sockets_when_full() {
    let server = Server::start().await;
    let template = stream(&server.url, vec![]);
    let mut manager = SocketManager::new(template, 2, vec![|_: &_, _: &_, _| {}]);
    let tickers = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();

    manager.add(tickers(&["A", "B", "C"]));
    assert_eq!(manager.socket_count(), 2);
    let (mut first, _) = server.accept().await;
    let (mut second, _) = server.accept().await;
    let mut subscribed = [
        next_command(&mut first).await["params"]["market_tickers"].clone(),
        next_command(&mut second).await["params"]["market_tickers"].clone(),
    ];
    subscribed.sort_by_key(|markets| markets.as_array().unwrap().len());
    assert_eq!(subscribed[0].as_array().unwrap().len(), 1);
    assert_eq!(subscribed[1].as_array().unwrap().len(), 2);

    manager.add(tickers(&["A"]));
    assert_eq!(manager.market_count(), 3);
    manager.remove(tickers(&["A", "B", "C"]));
    assert_eq!(manager.market_count(), 0);
    manager.add(tickers(&["D", "E", "F", "G"]));
    assert_eq!(manager.socket_count(), 2);
    assert_eq!(manager.market_count(), 4);
    timeout(WAIT, manager.close()).await.unwrap();
}

#[tokio::test]
async fn a_command_waits_for_the_acknowledgement() {
    let server = Server::start().await;
    let (commands, receiver) = unbounded_channel();
    let stream = stream(&server.url, vec!["M".to_owned()]);
    tokio::spawn(async move { stream.run(receiver, |_, _, _| {}).await });
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
    // A delay that never grew would total at most 225 milliseconds.
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test]
async fn adding_markets_during_recovery_is_not_a_stall() {
    let server = Server::start().await;
    let (commands, receiver) = unbounded_channel();
    let tickers: Vec<String> = (0..6).map(|index| format!("M{index}")).collect();
    let stream = stream(&server.url, tickers.clone());
    tokio::spawn(async move { stream.run(receiver, |_, _, _| {}).await });
    let (mut socket, _) = server.accept().await;
    next_command(&mut socket).await;
    send(&mut socket, ack()).await;
    for (index, ticker) in tickers.iter().enumerate() {
        // The set grows on every round even though one market is answered.
        let added = vec![format!("N{index}a"), format!("N{index}b")];
        commands.send(StreamCommand::Add(added)).unwrap();
        let frame = json!({
            "type": "orderbook_snapshot", "sid": 7, "seq": index + 1, "sending_ts_ms": 1,
            "msg": { "market_ticker": ticker, "yes_dollars_fp": [["0.40", "1.00"]] },
        });
        send(&mut socket, frame).await;
        tokio::time::sleep(TIMING.resync_retry + Duration::from_millis(10)).await;
    }
    let rebuilt = timeout(Duration::from_millis(50), server.listener.accept()).await;
    assert!(rebuilt.is_err(), "progress must not rebuild the connection");
}
