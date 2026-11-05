//! REST client behaviour against a local mock server; never touches the network.

use engineering_challenge::kalshi::auth::{
    AuthHeaders, KEY_HEADER, RequestSigner, SIGNATURE_HEADER, TIMESTAMP_HEADER, auth_header_map,
};
use engineering_challenge::kalshi::rest::{
    KalshiClient, Market, MarketQuery, RestError, RetryPolicy,
};
use serde_json::json;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockBuilder, MockServer, Request, Respond, ResponseTemplate};

/// Exposes the signed message as the signature, so tests can read it.
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

async fn server(response: ResponseTemplate) -> (MockServer, KalshiClient) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/trade-api/v2/account/limits"))
        .respond_with(response)
        .mount(&server)
        .await;
    let base_url = format!("{}/trade-api/v2", server.uri());
    let client = KalshiClient::try_new(&base_url, Arc::new(EchoSigner)).unwrap();
    (server, client)
}

fn limits_body() -> serde_json::Value {
    json!({
        "usage_tier": "basic",
        "read": { "refill_rate": 200, "bucket_capacity": 600 },
        "write": { "refill_rate": 100, "bucket_capacity": 100 },
    })
}

#[tokio::test]
async fn signs_the_full_path_and_sends_every_header() {
    let (server, client) = server(ResponseTemplate::new(200).set_body_json(limits_body())).await;
    let limits = client.account_limits().await.unwrap();
    assert_eq!(limits.usage_tier, "basic");
    assert_eq!(limits.read.refill_rate, 200);

    let requests = server.received_requests().await.unwrap();
    let headers = &requests[0].headers;
    assert_eq!(
        headers.get(SIGNATURE_HEADER).unwrap(),
        "GET/trade-api/v2/account/limits"
    );
    assert_eq!(headers.get(KEY_HEADER).unwrap(), "test-key");
    assert!(headers.contains_key(TIMESTAMP_HEADER));
}

#[tokio::test]
async fn trailing_slash_on_the_base_url_changes_nothing() {
    let (server, _) = server(ResponseTemplate::new(200).set_body_json(limits_body())).await;
    let base_url = format!("{}/trade-api/v2/", server.uri());
    let client = KalshiClient::try_new(&base_url, Arc::new(EchoSigner)).unwrap();
    client.account_limits().await.unwrap();
}

#[tokio::test]
async fn refused_credentials_are_a_distinct_error() {
    for status in [401, 403] {
        let (_server, client) = server(ResponseTemplate::new(status)).await;
        let error = client.account_limits().await.unwrap_err();
        assert!(matches!(error, RestError::Unauthorized));
    }
}

#[tokio::test]
async fn other_failures_report_only_the_status() {
    let (_server, client) =
        server(ResponseTemplate::new(404).set_body_string("private body")).await;
    let error = client.account_limits().await.unwrap_err();
    assert!(matches!(error, RestError::Status(404)));
    assert!(!error.to_string().contains("private body"));
}

#[tokio::test]
async fn a_malformed_body_is_an_error() {
    let (_server, client) = server(ResponseTemplate::new(200).set_body_string("{}")).await;
    assert!(matches!(
        client.account_limits().await.unwrap_err(),
        RestError::Decode(_)
    ));
}

const FAST_RETRY: RetryPolicy = RetryPolicy {
    attempts: 3,
    base: Duration::from_millis(1),
    max: Duration::from_millis(1),
};

async fn markets_server() -> (MockServer, KalshiClient) {
    let server = MockServer::start().await;
    let base_url = format!("{}/trade-api/v2", server.uri());
    let client = KalshiClient::try_new(&base_url, Arc::new(EchoSigner))
        .unwrap()
        .with_retry(FAST_RETRY);
    (server, client)
}

fn page(tickers: &[&str], cursor: &str) -> ResponseTemplate {
    let markets: Vec<_> = tickers
        .iter()
        .map(|ticker| json!({ "ticker": ticker, "status": "active" }))
        .collect();
    ResponseTemplate::new(200).set_body_json(json!({ "markets": markets, "cursor": cursor }))
}

fn first_page() -> MockBuilder {
    Mock::given(path("/trade-api/v2/markets"))
        .and(query_param("status", "open"))
        .and(query_param("mve_filter", "exclude"))
        .and(query_param("limit", "1000"))
        .and(query_param_is_missing("cursor"))
}

fn page_after(cursor: &str) -> MockBuilder {
    Mock::given(path("/trade-api/v2/markets")).and(query_param("cursor", cursor))
}

fn tickers(markets: &[Market]) -> Vec<&str> {
    markets.iter().map(|m| m.ticker.as_str()).collect()
}

#[tokio::test]
async fn discovery_follows_cursors_until_the_end() {
    let (server, client) = markets_server().await;
    first_page()
        .respond_with(page(&["A"], "c1"))
        .mount(&server)
        .await;
    page_after("c1")
        .respond_with(page(&["B"], ""))
        .mount(&server)
        .await;
    let markets = client.markets(MarketQuery::Open).await.unwrap();
    assert_eq!(tickers(&markets), ["A", "B"]);
}

#[tokio::test]
async fn a_response_without_a_cursor_is_not_the_end_of_a_scan() {
    for body in [
        json!({ "markets": [] }),
        json!({ "markets": [], "cursor": null }),
    ] {
        let (server, client) = markets_server().await;
        first_page()
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        assert!(matches!(
            client.markets(MarketQuery::Open).await.unwrap_err(),
            RestError::Decode(_)
        ));
    }
}

#[tokio::test]
async fn a_page_above_the_limit_is_rejected() {
    let (server, client) = markets_server().await;
    let tickers = vec!["A"; 1001];
    first_page()
        .respond_with(page(&tickers, ""))
        .mount(&server)
        .await;
    assert!(matches!(
        client.markets(MarketQuery::Open).await.unwrap_err(),
        RestError::OversizedPage
    ));
}

/// Answers every page with a new cursor, so only the page bound can stop it.
struct EndlessPages(AtomicUsize);

impl Respond for EndlessPages {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let cursor = self.0.fetch_add(1, Ordering::Relaxed).to_string();
        page(&[], &cursor)
    }
}

#[tokio::test]
async fn an_endless_scan_stops_at_the_page_bound() {
    let (server, client) = markets_server().await;
    Mock::given(path("/trade-api/v2/markets"))
        .respond_with(EndlessPages(AtomicUsize::new(0)))
        .mount(&server)
        .await;
    assert!(matches!(
        client.markets(MarketQuery::Open).await.unwrap_err(),
        RestError::TooManyPages
    ));
}

#[tokio::test]
async fn a_redirect_is_not_followed() {
    let (server, client) = markets_server().await;
    let other = MockServer::start().await;
    first_page()
        .respond_with(ResponseTemplate::new(302).insert_header("location", other.uri()))
        .mount(&server)
        .await;
    assert!(matches!(
        client.markets(MarketQuery::Open).await.unwrap_err(),
        RestError::Status(302)
    ));
    assert!(other.received_requests().await.unwrap().is_empty());
}

#[test]
fn plain_http_is_only_allowed_on_loopback() {
    let insecure = KalshiClient::try_new("http://example.com/v2", Arc::new(EchoSigner));
    assert!(matches!(insecure, Err(RestError::InsecureBaseUrl)));
    assert!(KalshiClient::try_new("https://example.com/v2", Arc::new(EchoSigner)).is_ok());
    assert!(KalshiClient::try_new("http://127.0.0.1:1/v2", Arc::new(EchoSigner)).is_ok());
}

#[tokio::test]
async fn a_failed_later_page_returns_no_partial_list() {
    let (server, client) = markets_server().await;
    first_page()
        .respond_with(page(&["A"], "c1"))
        .mount(&server)
        .await;
    page_after("c1")
        .respond_with(ResponseTemplate::new(500))
        .expect(3)
        .mount(&server)
        .await;
    let error = client.markets(MarketQuery::Open).await.unwrap_err();
    assert!(matches!(error, RestError::Status(500)));
}

#[tokio::test]
async fn a_repeated_cursor_stops_discovery() {
    let (server, client) = markets_server().await;
    first_page()
        .respond_with(page(&["A"], "c1"))
        .mount(&server)
        .await;
    page_after("c1")
        .respond_with(page(&["B"], "c1"))
        .mount(&server)
        .await;
    let error = client.markets(MarketQuery::Open).await.unwrap_err();
    assert!(matches!(error, RestError::RepeatedCursor));
}

#[tokio::test]
async fn change_queries_ask_for_the_right_markets() {
    let (server, client) = markets_server().await;
    Mock::given(path("/trade-api/v2/markets"))
        .and(query_param("status", "open"))
        .and(query_param("min_created_ts", "5"))
        .respond_with(page(&["NEW"], ""))
        .mount(&server)
        .await;
    Mock::given(path("/trade-api/v2/markets"))
        .and(query_param("status", "closed"))
        .and(query_param("min_close_ts", "6"))
        .respond_with(page(&["OLD"], ""))
        .mount(&server)
        .await;
    let mut seen = Vec::new();
    for query in [MarketQuery::CreatedSince(5), MarketQuery::ClosedSince(6)] {
        client
            .scan_markets(query, |page| {
                seen.extend(page.into_iter().map(|m| m.ticker));
                ControlFlow::Continue(())
            })
            .await
            .unwrap();
    }
    assert_eq!(seen, ["NEW", "OLD"]);
}

#[tokio::test]
async fn a_rate_limited_page_is_retried() {
    let (server, client) = markets_server().await;
    first_page()
        .respond_with(ResponseTemplate::new(429))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    first_page()
        .respond_with(page(&["A"], ""))
        .mount(&server)
        .await;
    let markets = client.markets(MarketQuery::Open).await.unwrap();
    assert_eq!(tickers(&markets), ["A"]);
}

#[tokio::test]
async fn refused_credentials_are_not_retried() {
    let (server, client) = markets_server().await;
    first_page()
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let error = client.markets(MarketQuery::Open).await.unwrap_err();
    assert!(matches!(error, RestError::Unauthorized));
}

#[test]
fn signature_header_is_marked_sensitive() {
    let headers = auth_header_map(&EchoSigner.sign(1, "GET", "/a")).unwrap();
    assert!(headers.get(SIGNATURE_HEADER).unwrap().is_sensitive());
}

#[test]
fn an_invalid_base_url_is_rejected() {
    let result = KalshiClient::try_new("not a url", Arc::new(EchoSigner));
    assert!(matches!(result, Err(RestError::InvalidBaseUrl(_))));
}

#[tokio::test]
async fn an_oversized_body_is_rejected() {
    let body = vec![b' '; (32 << 20) + 1];
    let (_server, client) = server(ResponseTemplate::new(200).set_body_bytes(body)).await;
    let error = client.account_limits().await.unwrap_err();
    assert!(matches!(error, RestError::OversizedBody));
}
