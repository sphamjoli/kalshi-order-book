//! Market tracking against a local mock server. Sockets point at an address
//! that refuses connections, because only placement is under test.

use engineering_challenge::discovery::MarketTracker;
use engineering_challenge::kalshi::auth::{AuthHeaders, RequestSigner};
use engineering_challenge::kalshi::rest::{KalshiClient, RetryPolicy};
use engineering_challenge::kalshi::stream::{DEFAULT_TIMING, KalshiStream};
use engineering_challenge::manager::SocketManager;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use wiremock::matchers::{path, query_param, query_param_contains, query_param_is_missing};
use wiremock::{Mock, MockBuilder, MockServer, ResponseTemplate};

const POLL: Duration = Duration::from_millis(10);
const NO_RESCAN: Duration = Duration::from_secs(3600);
const RESCAN: Duration = Duration::from_millis(30);
const SETTLE: Duration = Duration::from_millis(200);
const SLOW_SCAN: Duration = Duration::from_millis(300);
const RETRY: RetryPolicy = RetryPolicy {
    attempts: 1,
    base: Duration::from_millis(1),
    max: Duration::from_millis(1),
};

struct NoSigner;

impl RequestSigner for NoSigner {
    fn sign(&self, timestamp_ms: i64, _: &str, _: &str) -> AuthHeaders {
        AuthHeaders {
            key_id: "k".to_owned(),
            timestamp_ms: timestamp_ms.to_string(),
            signature: "s".to_owned(),
        }
    }
}

fn page(tickers: &[&str]) -> ResponseTemplate {
    let markets: Vec<_> = tickers
        .iter()
        .map(|ticker| json!({ "ticker": ticker, "status": "active" }))
        .collect();
    ResponseTemplate::new(200).set_body_json(json!({ "markets": markets, "cursor": "" }))
}

fn first_scan() -> MockBuilder {
    Mock::given(path("/markets"))
        .and(query_param("status", "open"))
        .and(query_param_is_missing("min_created_ts"))
}

fn created() -> MockBuilder {
    Mock::given(path("/markets")).and(query_param_contains("min_created_ts", ""))
}

fn closed() -> MockBuilder {
    Mock::given(path("/markets")).and(query_param("status", "closed"))
}

fn settled() -> MockBuilder {
    Mock::given(path("/markets")).and(query_param("status", "settled"))
}

fn client(server: &MockServer) -> KalshiClient {
    KalshiClient::try_new(&server.uri(), Arc::new(NoSigner))
        .unwrap()
        .with_retry(RETRY)
}

/// Runs a tracker for a moment and returns how many markets stayed placed.
async fn placed_after_polling(server: &MockServer, max_markets: Option<usize>) -> usize {
    placed_with_rescan(server, max_markets, NO_RESCAN).await
}

/// Lists no settled markets unless the test already mounted some.
async fn placed_with_rescan(
    server: &MockServer,
    max_markets: Option<usize>,
    rescan: Duration,
) -> usize {
    settled().respond_with(page(&[])).mount(server).await;
    let stream = KalshiStream::try_new(
        "ws://127.0.0.1:1/ws",
        Arc::new(NoSigner),
        vec![],
        DEFAULT_TIMING,
    )
    .unwrap();
    let mut manager = SocketManager::new(stream, 10, vec![|_: &_, _: &_, _| {}]);
    let client = client(server);
    let tracker = MarketTracker::new(&client, POLL, rescan);
    let _ = timeout(SETTLE, tracker.run(&mut manager, max_markets)).await;
    let placed = manager.market_count();
    manager.close().await;
    placed
}

async fn scans_started(server: &MockServer) -> usize {
    let requests = server.received_requests().await.unwrap_or_default();
    requests
        .iter()
        .filter(|request| {
            let query = request.url.query().unwrap_or_default();
            query.contains("status=open") && !query.contains("min_created_ts")
        })
        .count()
}

/// Stops as the third scan starts, which is when the slow second scan has
/// been applied. The third has not, so timing cannot change what it removes.
async fn placed_after_one_rescan(server: &MockServer) -> usize {
    settled().respond_with(page(&[])).mount(server).await;
    let stream = KalshiStream::try_new(
        "ws://127.0.0.1:1/ws",
        Arc::new(NoSigner),
        vec![],
        DEFAULT_TIMING,
    )
    .unwrap();
    let mut manager = SocketManager::new(stream, 10, vec![|_: &_, _: &_, _| {}]);
    let client = client(server);
    let tracker = MarketTracker::new(&client, POLL, RESCAN);
    let third_scan = async {
        while scans_started(server).await < 3 {
            sleep(POLL).await;
        }
    };
    tokio::select! {
        _ = tracker.run(&mut manager, None) => panic!("the tracker stopped"),
        () = third_scan => {}
    }
    let placed = manager.market_count();
    manager.close().await;
    placed
}

#[tokio::test]
async fn new_markets_are_added_and_closed_ones_removed() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .mount(&server)
        .await;
    created().respond_with(page(&["C"])).mount(&server).await;
    closed().respond_with(page(&["A"])).mount(&server).await;
    assert_eq!(placed_after_polling(&server, None).await, 2);
}

#[tokio::test]
async fn a_failed_poll_removes_nothing() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .mount(&server)
        .await;
    created().respond_with(page(&["C"])).mount(&server).await;
    closed()
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    assert_eq!(placed_after_polling(&server, None).await, 2);
}

#[tokio::test]
async fn a_failed_first_scan_starts_again() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan().respond_with(page(&["A"])).mount(&server).await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_after_polling(&server, None).await, 1);
}

#[tokio::test]
async fn settled_markets_are_removed() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    settled().respond_with(page(&["A"])).mount(&server).await;
    assert_eq!(placed_after_polling(&server, None).await, 1);
}

#[tokio::test]
async fn a_rescan_removes_what_polls_missed_and_adds_the_rest() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan()
        .respond_with(page(&["B", "C", "D"]))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_with_rescan(&server, None, RESCAN).await, 3);
}

#[tokio::test]
async fn a_failed_rescan_removes_nothing() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan()
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_with_rescan(&server, None, RESCAN).await, 2);
}

#[tokio::test]
async fn a_market_placed_during_a_rescan_survives_it() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A"]))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan()
        .respond_with(page(&["A"]).set_delay(SLOW_SCAN))
        .mount(&server)
        .await;
    created().respond_with(page(&["N"])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_after_one_rescan(&server).await, 2);
}

#[tokio::test]
async fn a_limited_run_never_rescans() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B", "C"]))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_with_rescan(&server, Some(2), RESCAN).await, 2);
}

#[tokio::test]
async fn an_empty_rescan_removes_nothing() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan().respond_with(page(&[])).mount(&server).await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_with_rescan(&server, None, RESCAN).await, 2);
}

#[tokio::test]
async fn a_market_closed_during_a_rescan_stays_removed() {
    let server = MockServer::start().await;
    first_scan()
        .respond_with(page(&["A", "B"]))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_scan()
        .respond_with(page(&["A", "B"]).set_delay(SLOW_SCAN))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&["A"])).mount(&server).await;
    assert_eq!(placed_after_one_rescan(&server).await, 1);
}

#[tokio::test]
async fn a_listed_market_is_followed_once_its_opening_time_passes() {
    let server = MockServer::start().await;
    first_scan().respond_with(page(&["A"])).mount(&server).await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    let listed = json!({ "cursor": "", "markets": [
        { "ticker": "PAST", "status": "unopened", "open_time": "2020-01-01T00:00:00Z" },
        { "ticker": "FUTURE", "status": "unopened", "open_time": "2999-01-01T00:00:00Z" },
        { "ticker": "UNKNOWN", "status": "unopened" },
        { "ticker": "GARBLED", "status": "unopened", "open_time": "soon" },
    ]});
    Mock::given(path("/markets"))
        .and(query_param("status", "unopened"))
        .and(query_param_is_missing("min_created_ts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(listed))
        .mount(&server)
        .await;
    assert_eq!(placed_after_polling(&server, None).await, 2);
}

#[tokio::test]
async fn a_retried_limited_scan_still_reaches_its_limit() {
    let server = MockServer::start().await;
    let next_page = || first_scan().and(query_param("cursor", "next"));
    next_page()
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    next_page()
        .respond_with(page(&["B", "C"]))
        .mount(&server)
        .await;
    first_scan()
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "markets": [{ "ticker": "A", "status": "active" }], "cursor": "next",
        })))
        .mount(&server)
        .await;
    created().respond_with(page(&[])).mount(&server).await;
    closed().respond_with(page(&[])).mount(&server).await;
    assert_eq!(placed_after_polling(&server, Some(2)).await, 2);
}
