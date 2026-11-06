//! Behavioural traces through the normaliser and collector.

use engineering_challenge::collector::{BookRecord, Collector, CollectorOutput};
use engineering_challenge::domain::{Level, MarketEvent, Price, Quantity};
use engineering_challenge::kalshi::normalise::Normaliser;
use serde_json::json;
use std::time::{Duration, Instant};

const GRACE: Duration = Duration::from_secs(5);

/// Feeds frames through the normaliser and collector, as one stream would.
struct Stream {
    normaliser: Normaliser,
    collector: Collector,
}

impl Stream {
    fn new(tickers: &[&str]) -> Self {
        let mut normaliser = Normaliser::default();
        let ack = json!({ "id": 1, "type": "subscribed", "msg": { "channel": "c", "sid": 1 } });
        normaliser.normalise(&ack.to_string()).unwrap();
        Self {
            normaliser,
            collector: Collector::new(tickers.iter().map(|t| t.to_string()), GRACE),
        }
    }

    fn feed(&mut self, frame: String) -> Vec<CollectorOutput> {
        let event: Option<MarketEvent> = self.normaliser.normalise(&frame).unwrap();
        event.map_or_else(Vec::new, |event| {
            self.collector.handle(event, Instant::now())
        })
    }
}

fn snapshot(ticker: &str, seq: u64, yes: &[[&str; 2]], no: &[[&str; 2]]) -> String {
    json!({
        "type": "orderbook_snapshot", "sid": 1, "seq": seq, "sending_ts_ms": 1,
        "msg": { "market_ticker": ticker, "yes_dollars_fp": yes, "no_dollars_fp": no },
    })
    .to_string()
}

fn delta(ticker: &str, seq: u64, price: &str, change: &str) -> String {
    json!({
        "type": "orderbook_delta", "sid": 1, "seq": seq, "sending_ts_ms": 1,
        "msg": {
            "market_ticker": ticker, "price_dollars": price, "delta_fp": change,
            "side": "yes", "ts_ms": 2,
        },
    })
    .to_string()
}

fn level(price: i64, quantity: i64) -> Level {
    Level {
        price: Price::try_new(price).unwrap(),
        quantity: Quantity::try_new(quantity).unwrap(),
    }
}

// Trace: A snapshot seq 1; B snapshot seq 2; A delta seq 3 on same SID
// Expected: No false gap for A; independent market books
#[test]
fn interleaved_symbols_on_one_sid_have_no_false_gap() {
    let mut stream = Stream::new(&["A", "B"]);
    stream.feed(snapshot("A", 1, &[["0.40", "1.00"]], &[]));
    stream.feed(snapshot("B", 2, &[["0.50", "1.00"]], &[]));
    let outputs = stream.feed(delta("A", 3, "0.40", "2.00"));
    assert!(
        matches!(&outputs[..], [CollectorOutput::Record(BookRecord::Update { symbol, .. })] if symbol == "A")
    );
    assert_eq!(stream.collector.live_books().count(), 2);
}

// Trace: Snapshot has bids only after old book had asks
// Expected: Old asks disappear
#[test]
fn bids_only_snapshot_clears_old_asks() {
    let mut stream = Stream::new(&["A"]);
    stream.feed(snapshot("A", 1, &[["0.40", "1.00"]], &[["0.50", "1.00"]]));
    let outputs = stream.feed(snapshot("A", 2, &[["0.41", "1.00"]], &[]));
    let [CollectorOutput::Record(BookRecord::Snapshot { bids, asks, .. })] = &outputs[..] else {
        panic!("expected snapshot, got {outputs:?}");
    };
    assert_eq!(bids, &[level(410_000, 1_000_000)]);
    assert!(asks.is_empty());
}

// Trace: One delta in a subscription is missing
// Expected: Every dependent replica becomes invalid; no authoritative periodic snapshot from it
#[test]
fn missing_delta_invalidates_every_dependent_replica() {
    let mut stream = Stream::new(&["A", "B"]);
    stream.feed(snapshot("A", 1, &[["0.40", "1.00"]], &[]));
    stream.feed(snapshot("B", 2, &[["0.50", "1.00"]], &[]));
    let outputs = stream.feed(delta("A", 9, "0.40", "1.00"));
    assert_eq!(
        outputs,
        [CollectorOutput::Resync(vec![
            "A".to_owned(),
            "B".to_owned()
        ])]
    );
    assert_eq!(stream.collector.live_books().count(), 0);
}
