//! Writes a snapshot and an update with the supplied SBE helpers, then reads
//! them back unchanged.

use chrono::DateTime;
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[test]
fn messages_survive_a_write_and_read() {
    let timestamp = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
    let symbol = "EXAMPLE".to_owned();
    let snapshot = SnapshotDomain {
        received_at: timestamp,
        exchange_timestamp: timestamp,
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 400_000,
            quantity: 2_000_000,
        }],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 600_000,
            quantity: 3_500_000,
        }],
        symbol: symbol.clone(),
    };
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: timestamp,
        exchange_timestamp: timestamp,
        price_level: 400_000,
        quantity: 1_250_000,
        side: Side::Buy,
        symbol,
    };

    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE_64_MEBIBYTES).unwrap();
    writer.write_msg(&snapshot).unwrap();
    writer.write_msg(&update).unwrap();
    writer.flush().unwrap();
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path()).unwrap();
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected snapshot");
    };
    assert_eq!(SnapshotDomain::try_from_decoder(decoded).unwrap(), snapshot);
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg().unwrap() else {
        panic!("expected price level update");
    };
    assert_eq!(
        PriceLevelUpdateDomain::try_from_decoder(decoded).unwrap(),
        update
    );
    assert!(reader.read_msg().unwrap().is_none());
}
