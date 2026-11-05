//! Kalshi WebSocket message shapes, exactly as sent. Decimal fields stay as
//! strings here; [`super::normalise`] owns their conversion.
//!
//! Format: <https://docs.kalshi.com/websockets/orderbook-updates>, read
//! November 2026.

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Value, json};

/// Message type of a full book.
pub const SNAPSHOT_TYPE: &str = "orderbook_snapshot";
/// Message type of a signed level change.
pub const DELTA_TYPE: &str = "orderbook_delta";

/// Message type that announces a subscription.
pub const SUBSCRIBED_TYPE: &str = "subscribed";
/// Message type of a successful command reply.
pub const OK_TYPE: &str = "ok";
/// A rejected command. It says nothing about any book.
pub const ERROR_TYPE: &str = "error";

/// The fields shared by every server message. The body stays unparsed, so an
/// unknown type can still take part in sequence tracking.
#[derive(Debug, Deserialize)]
pub struct Envelope<'a> {
    #[serde(rename = "type")]
    pub kind: &'a str,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub sending_ts_ms: Option<i64>,
    #[serde(borrow)]
    pub msg: Option<&'a RawValue>,
}

/// The body of a `subscribed` acknowledgement.
#[derive(Debug, Deserialize)]
pub struct SubscribedBody {
    pub sid: u64,
}

/// One `[price, count]` pair.
#[derive(Debug, Deserialize)]
pub struct WireLevel {
    pub price: String,
    pub count: String,
}

/// The body of an `orderbook_snapshot`. Kalshi omits a side that is empty.
#[derive(Debug, Deserialize)]
pub struct SnapshotBody {
    pub market_ticker: String,
    #[serde(default)]
    pub yes_dollars_fp: Vec<WireLevel>,
    #[serde(default)]
    pub no_dollars_fp: Vec<WireLevel>,
}

/// The body of an `orderbook_delta`.
#[derive(Debug, Deserialize)]
pub struct DeltaBody {
    pub market_ticker: String,
    pub price_dollars: String,
    pub delta_fp: String,
    pub side: ContractSide,
    pub ts_ms: i64,
}

/// The binary contract a level belongs to. Both sides are bids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContractSide {
    Yes,
    No,
}

/// Builds the command that subscribes to real-time books for `tickers`.
pub fn subscribe_command(id: u64, tickers: &[String]) -> Value {
    json!({
        "id": id,
        "cmd": "subscribe",
        "params": { "channels": ["orderbook_delta"], "market_tickers": tickers },
    })
}

/// Builds the command that requests replacement snapshots on subscription
/// `sid`.
pub fn get_snapshot_command(id: u64, sid: u64, tickers: &[String]) -> Value {
    update_subscription(id, sid, tickers, "get_snapshot")
}

/// Builds the command that adds markets to subscription `sid`.
pub fn add_markets_command(id: u64, sid: u64, tickers: &[String]) -> Value {
    update_subscription(id, sid, tickers, "add_markets")
}

/// Builds the command that removes markets from subscription `sid`.
pub fn delete_markets_command(id: u64, sid: u64, tickers: &[String]) -> Value {
    update_subscription(id, sid, tickers, "delete_markets")
}

fn update_subscription(id: u64, sid: u64, tickers: &[String], action: &str) -> Value {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sids": [sid], "market_tickers": tickers, "action": action },
    })
}
