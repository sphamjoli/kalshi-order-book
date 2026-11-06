//! Frame normalisation on hand-written frames shaped like Kalshi's documented
//! examples.

use engineering_challenge::Side;
use engineering_challenge::domain::{Level, MarketEvent, Price, Quantity, QuantityDelta};
use engineering_challenge::kalshi::normalise::{NormaliseError, Normaliser};
use engineering_challenge::kalshi::wire::{get_snapshot_command, subscribe_command};
use serde_json::json;

fn level(price: i64, quantity: i64) -> Level {
    Level {
        price: Price::try_new(price).unwrap(),
        quantity: Quantity::try_new(quantity).unwrap(),
    }
}

/// A normaliser that has seen subscription 2 announced.
fn subscribed() -> Normaliser {
    let mut normaliser = Normaliser::default();
    let ack = json!({ "id": 1, "type": "subscribed", "msg": { "channel": "c", "sid": 2 } });
    normaliser.normalise(&ack.to_string()).unwrap();
    normaliser
}

fn delta_frame(seq: u64, side: &str, price: &str, delta: &str) -> String {
    json!({
        "type": "orderbook_delta", "sid": 2, "seq": seq, "sending_ts_ms": 5,
        "msg": {
            "market_ticker": "M", "market_id": "x", "clock": 1, "price_dollars": price,
            "delta_fp": delta, "side": side, "ts": "2022-11-22T20:44:01Z", "ts_ms": 7,
        },
    })
    .to_string()
}

fn delta(frame: &str) -> (Side, i64, QuantityDelta) {
    match subscribed().normalise(frame).unwrap() {
        Some(MarketEvent::Delta {
            side, price, delta, ..
        }) => (side, price.raw(), delta),
        other => panic!("expected delta, got {other:?}"),
    }
}

fn is_invalid(frame: &str) -> bool {
    subscribed().normalise(frame).is_err()
}

#[test]
fn snapshot_maps_yes_to_bids_and_mirrors_no_to_asks() {
    let frame = json!({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2, "sending_ts_ms": 9,
        "msg": {
            "market_ticker": "M", "market_id": "x", "clock": 1,
            "yes_dollars_fp": [["0.0800", "300.00"]],
            "no_dollars_fp": [["0.55", "7.00"]],
        },
    });
    let event = subscribed().normalise(&frame.to_string()).unwrap().unwrap();
    assert_eq!(
        event,
        MarketEvent::Snapshot {
            symbol: "M".to_owned(),
            exchange_time_ms: 9,
            exchange_clock: Some(1),
            bids: vec![level(80_000, 300_000_000)],
            asks: vec![level(450_000, 7_000_000)],
        }
    );
}

#[test]
fn an_omitted_side_is_empty() {
    let frame = json!({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2, "sending_ts_ms": 9,
        "msg": { "market_ticker": "M", "yes_dollars_fp": [["0.08", "1.00"]] },
    });
    let event = subscribed().normalise(&frame.to_string()).unwrap();
    assert!(matches!(event, Some(MarketEvent::Snapshot { asks, .. }) if asks.is_empty()));
}

#[test]
fn yes_delta_is_a_bid_change() {
    let (side, price, change) = delta(&delta_frame(3, "yes", "0.9600", "-54.00"));
    assert_eq!(side, Side::Buy);
    assert_eq!(price, 960_000);
    assert_eq!(change, QuantityDelta::new(-54_000_000));
}

#[test]
fn no_delta_mirrors_the_price_and_keeps_its_sign() {
    let (side, price, change) = delta(&delta_frame(3, "no", "0.55", "-1.50"));
    assert_eq!(side, Side::Sell);
    assert_eq!(price, 450_000);
    assert_eq!(change, QuantityDelta::new(-1_500_000));
}

#[test]
fn prices_at_both_ends_are_valid() {
    assert_eq!(delta(&delta_frame(3, "yes", "1.0000", "1.00")).1, 1_000_000);
    assert_eq!(delta(&delta_frame(3, "yes", "0.0000", "1.00")).1, 0);
}

#[test]
fn six_decimal_places_are_exact_and_a_seventh_is_rejected() {
    assert_eq!(delta(&delta_frame(3, "yes", "0.123456", "1.00")).1, 123_456);
    assert!(is_invalid(&delta_frame(3, "yes", "0.1234567", "1.00")));
}

#[test]
fn bad_numbers_are_rejected() {
    for price in ["1.000001", "-0.01", "abc", ""] {
        assert!(is_invalid(&delta_frame(3, "yes", price, "1.00")), "{price}");
    }
    for text in ["1_0.00", "0_.5", "+1.00", ".5", "5.", "1e2", " 1.0", "1.0 "] {
        assert!(is_invalid(&delta_frame(3, "yes", "0.5", text)), "{text}");
    }
    assert!(is_invalid(&delta_frame(
        3,
        "yes",
        "0.50",
        "99999999999999999.00"
    )));
}

#[test]
fn a_price_above_one_reports_its_range() {
    let error = subscribed()
        .normalise(&delta_frame(3, "no", "1.50", "1.00"))
        .unwrap_err();
    assert!(matches!(error, NormaliseError::PriceOutOfRange(1_500_000)));
}

#[test]
fn an_unknown_side_is_rejected() {
    assert!(is_invalid(&delta_frame(3, "maybe", "0.50", "1.00")));
}

#[test]
fn contiguous_frames_have_no_gap() {
    let mut normaliser = subscribed();
    for seq in 3..6 {
        let event = normaliser.normalise(&delta_frame(seq, "yes", "0.5", "1.00"));
        assert!(matches!(event, Ok(Some(MarketEvent::Delta { .. }))));
    }
}

#[test]
fn a_jump_a_repeat_or_a_step_back_loses_continuity() {
    for next in [5, 3, 2] {
        let mut normaliser = subscribed();
        normaliser
            .normalise(&delta_frame(3, "yes", "0.5", "1.00"))
            .unwrap();
        let event = normaliser
            .normalise(&delta_frame(next, "yes", "0.5", "1.00"))
            .unwrap();
        assert_eq!(event, Some(MarketEvent::ContinuityLost), "{next}");
    }

    // The mark never falls, so a replay cannot look contiguous.
    let mut normaliser = subscribed();
    let mut feed = |seq| {
        normaliser
            .normalise(&delta_frame(seq, "yes", "0.5", "1.00"))
            .unwrap()
    };
    feed(10);
    assert_eq!(feed(3), Some(MarketEvent::ContinuityLost));
    assert_eq!(feed(4), Some(MarketEvent::ContinuityLost));
}

#[test]
fn a_command_reply_uses_up_its_seq() {
    let mut normaliser = subscribed();
    normaliser
        .normalise(&delta_frame(3, "yes", "0.5", "1.00"))
        .unwrap();
    let reply = json!({ "type": "ok", "sid": 2, "seq": 4 }).to_string();
    assert_eq!(normaliser.normalise(&reply).unwrap(), None);
    let next = normaliser.normalise(&delta_frame(5, "yes", "0.5", "1.00"));
    assert!(matches!(next, Ok(Some(MarketEvent::Delta { .. }))));
}

#[test]
fn an_unknown_type_loses_continuity() {
    for kind in ["orderbook_deltb", "orderbook_reset", "unsubscribed"] {
        let frame = json!({ "type": kind, "sid": 2, "seq": 4, "msg": {} }).to_string();
        let event = subscribed().normalise(&frame).unwrap();
        assert_eq!(event, Some(MarketEvent::ContinuityLost), "{kind}");
    }
}

#[test]
fn a_book_frame_needs_a_position() {
    let frame = delta_frame(3, "yes", "0.5", "1.00");
    let mut value: serde_json::Value = serde_json::from_str(&frame).unwrap();
    for field in ["sid", "seq"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(is_invalid(&missing.to_string()), "{field}");
    }
    value["seq"] = serde_json::Value::Null;
    assert!(is_invalid(&value.to_string()));
}

#[test]
fn a_book_frame_on_an_unannounced_subscription_is_rejected() {
    let error = Normaliser::default()
        .normalise(&delta_frame(3, "yes", "0.5", "1.00"))
        .unwrap_err();
    assert!(matches!(error, NormaliseError::UnknownSubscription(2)));
}

#[test]
fn an_unknown_frame_still_counts_its_seq() {
    let mut normaliser = subscribed();
    normaliser
        .normalise(&delta_frame(3, "yes", "0.5", "1.00"))
        .unwrap();
    let unknown = json!({ "type": "new_thing", "sid": 2, "seq": 4, "msg": {} }).to_string();
    normaliser.normalise(&unknown).unwrap();
    let next = normaliser.normalise(&delta_frame(5, "yes", "0.5", "1.00"));
    assert!(matches!(next, Ok(Some(MarketEvent::Delta { .. }))));
}

#[test]
fn a_time_the_output_cannot_hold_is_rejected() {
    for ms in [-1, i64::MAX, 9_000_000_000_000_000] {
        let frame =
            delta_frame(3, "yes", "0.5", "1.00").replace("\"ts_ms\":7", &format!("\"ts_ms\":{ms}"));
        let error = subscribed().normalise(&frame).unwrap_err();
        assert!(matches!(error, NormaliseError::InvalidTime(_)), "{ms}");
    }
}

#[test]
fn a_side_too_large_for_a_record_is_rejected() {
    let levels = vec![["0.50", "1.00"]; 65_536];
    let frame = json!({
        "type": "orderbook_snapshot", "sid": 2, "seq": 2, "sending_ts_ms": 9,
        "msg": { "market_ticker": "M", "yes_dollars_fp": levels },
    });
    let error = subscribed().normalise(&frame.to_string()).unwrap_err();
    assert!(matches!(error, NormaliseError::TooManyLevels(65_536)));
}

#[test]
fn an_acknowledgement_has_no_book_meaning() {
    let ack = json!({ "id": 1, "type": "subscribed", "msg": { "channel": "c", "sid": 1 } });
    let event = subscribed().normalise(&ack.to_string()).unwrap();
    assert_eq!(event, None);
}

#[test]
fn commands_match_the_documented_shape() {
    let tickers = ["A".to_owned()];
    assert_eq!(
        subscribe_command(1, &tickers),
        json!({
            "id": 1, "cmd": "subscribe",
            "params": { "channels": ["orderbook_delta"], "market_tickers": ["A"] },
        })
    );
    assert_eq!(
        get_snapshot_command(2, 456, &tickers),
        json!({
            "id": 2, "cmd": "update_subscription",
            "params": { "sids": [456], "market_tickers": ["A"], "action": "get_snapshot" },
        })
    );
}

#[test]
fn a_rejected_command_leaves_the_books_alone() {
    let frame = json!({ "type": "error", "id": 9, "msg": { "code": 8 } }).to_string();
    assert_eq!(subscribed().normalise(&frame).unwrap(), None);
}

#[test]
fn a_missing_time_falls_back_and_never_costs_the_book() {
    let frame = delta_frame(3, "yes", "0.5", "1.00").replace(",\"ts_ms\":7", "");
    let event = subscribed().normalise(&frame).unwrap();
    let Some(MarketEvent::Delta {
        exchange_time_ms, ..
    }) = event
    else {
        panic!("expected a delta, got {event:?}");
    };
    assert_eq!(exchange_time_ms, 5, "the sending time stands in");

    let frame = frame.replace("\"sending_ts_ms\":5,", "");
    let event = subscribed().normalise(&frame).unwrap();
    assert!(matches!(event, Some(MarketEvent::Delta { .. })));
}

#[test]
fn a_delta_carries_the_exchange_clock() {
    let frame = delta_frame(3, "yes", "0.5", "1.00").replace("\"clock\":1", "\"clock\":42");
    let event = subscribed().normalise(&frame).unwrap();
    assert!(
        matches!(
            event,
            Some(MarketEvent::Delta {
                exchange_clock: Some(42),
                ..
            })
        ),
        "{event:?}"
    );
}
