//! SBE output: records must decode back to what was written.

use chrono::DateTime;
use engineering_challenge::Side;
use engineering_challenge::collector::BookRecord;
use engineering_challenge::domain::{Level, LevelChange, Price, Quantity};
use engineering_challenge::output::{
    OutputError, OutputRecord, SbeOutput, Summary, summarise, write_records,
};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use std::path::Path;
use tempfile::NamedTempFile;
use tokio::sync::mpsc::channel;

fn level(price: i64, quantity: i64) -> Level {
    Level {
        price: Price::try_new(price).unwrap(),
        quantity: Quantity::try_new(quantity).unwrap(),
    }
}

fn record(output: BookRecord) -> OutputRecord {
    OutputRecord {
        output,
        received_at: DateTime::from_timestamp_micros(1_700_000_000_000_123).unwrap(),
    }
}

fn snapshot() -> OutputRecord {
    record(BookRecord::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: 1_700_000_000_000,
        exchange_clock: None,
        bids: vec![level(400_000, 2_000_000), level(300_000, 1_000_000)],
        asks: vec![level(600_000, 3_500_000)],
    })
}

fn update() -> OutputRecord {
    record(BookRecord::Update {
        symbol: "M".to_owned(),
        exchange_time_ms: 1_700_000_000_001,
        exchange_clock: None,
        change: LevelChange {
            side: Side::Sell,
            price: Price::try_new(600_000).unwrap(),
            quantity: Quantity::ZERO,
        },
    })
}

fn encoded(records: &[OutputRecord]) -> NamedTempFile {
    let file = NamedTempFile::new().unwrap();
    let mut output = SbeOutput::create(file.path()).unwrap();
    for record in records {
        output.write(record).unwrap();
    }
    output.finish().unwrap();
    file
}

fn reader(file: &NamedTempFile) -> AlignedSbeReader<OrderbookSchema> {
    AlignedSbeReader::open(file.path()).unwrap()
}

#[test]
fn a_snapshot_keeps_every_field() {
    let file = encoded(&[snapshot()]);
    let mut reader = reader(&file);
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected snapshot");
    };
    let decoded = SnapshotDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(decoded.symbol, "M");
    assert_eq!(decoded.received_at, snapshot().received_at);
    assert_eq!(
        decoded.exchange_timestamp.timestamp_millis(),
        1_700_000_000_000
    );
    assert_eq!(decoded.bids[0].price_level, 400_000);
    assert_eq!(decoded.bids[1].quantity, 1_000_000);
    assert_eq!(decoded.asks[0].quantity, 3_500_000);
}

#[test]
fn an_update_keeps_every_field_and_zero_means_removed() {
    let file = encoded(&[update()]);
    let mut reader = reader(&file);
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected update");
    };
    let decoded = PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap();
    assert_eq!(decoded.side, Side::Sell);
    assert_eq!(decoded.price_level, 600_000);
    assert_eq!(decoded.quantity, 0);
    assert_eq!(decoded.backtest_timestamp, update().received_at);
}

#[test]
fn the_supplied_reader_walks_every_record() {
    let file = encoded(&[snapshot(), update(), update()]);
    let expected = Summary {
        snapshots: 1,
        updates: 2,
        symbols: 1,
    };
    assert_eq!(summarise(file.path()).unwrap(), expected);
}

#[test]
fn an_out_of_range_time_is_rejected() {
    let bad = record(BookRecord::Snapshot {
        symbol: "M".to_owned(),
        exchange_time_ms: i64::MAX,
        exchange_clock: None,
        bids: vec![],
        asks: vec![],
    });
    let file = NamedTempFile::new().unwrap();
    let error = SbeOutput::create(file.path()).unwrap().write(&bad);
    assert!(matches!(error, Err(OutputError::InvalidTime(_))));
}

#[test]
fn every_queued_record_is_written_after_the_senders_leave() {
    let (sender, receiver) = channel(2);
    sender.try_send(snapshot()).unwrap();
    sender.try_send(update()).unwrap();
    drop(sender);
    let file = NamedTempFile::new().unwrap();
    write_records(receiver, file.path()).unwrap();
    let expected = Summary {
        snapshots: 1,
        updates: 1,
        symbols: 1,
    };
    assert_eq!(summarise(file.path()).unwrap(), expected);
}

#[test]
fn an_unwritable_path_closes_the_queue() {
    let (sender, receiver) = channel(1);
    assert!(write_records(receiver, Path::new("/no/such/dir/out")).is_err());
    assert!(sender.is_closed());
}
