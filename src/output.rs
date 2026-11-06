//! Output: maps collector results to SBE records and is the single ordered
//! writer, using the supplied aligned helpers so the supplied reader can open
//! the result. Records reach the path a chunk at a time. Diagnostics go to
//! stderr. Per-market order is guaranteed; global time order across markets is
//! not promised.

use crate::collector::BookRecord;
use crate::util::dtos::SbeEncode;
use crate::util::dtos::challenge_orderbook::sbe_rt::DecodeError;
use crate::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use crate::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};
use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

/// The chunk size the supplied reader expects.
const CHUNK_SIZE: usize = 64 << 20;

/// Reasons output cannot be written or read. A broken pipe is one of them:
/// a reader that left means the run did not record what it claims.
#[derive(Debug, Error)]
pub enum OutputError {
    /// The supplied SBE helpers report `anyhow::Error`, including for I/O.
    #[error("SBE output failed: {0}")]
    Encode(#[from] anyhow::Error),
    #[error("record cannot be decoded: {0}")]
    Decode(#[from] DecodeError),
    #[error("record has an unknown type")]
    UnknownRecord,
    #[error("exchange time is out of range: {0}")]
    InvalidTime(i64),
}

/// A collector result with its local receipt time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputRecord {
    pub output: BookRecord,
    pub received_at: DateTime<Utc>,
}

/// Counts of what a file holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    pub snapshots: u64,
    pub updates: u64,
    /// Distinct markets with at least one record, which is what coverage is.
    pub symbols: usize,
}

/// Encodes records through the supplied writer.
pub struct SbeOutput {
    writer: AlignedSbeWriter,
}

impl SbeOutput {
    /// Creates a writer over `path`, clearing any existing file.
    ///
    /// # Errors
    /// Returns [`OutputError::Encode`] when the path cannot be opened.
    pub fn create(path: &Path) -> Result<Self, OutputError> {
        Ok(Self {
            writer: AlignedSbeWriter::create(path, CHUNK_SIZE)?,
        })
    }

    /// Writes one record.
    ///
    /// # Errors
    /// Returns an [`OutputError`] when encoding or the sink fails.
    pub fn write(&mut self, record: &OutputRecord) -> Result<(), OutputError> {
        match &record.output {
            BookRecord::Snapshot {
                symbol,
                exchange_time_ms,
                bids,
                asks,
                ..
            } => self.encode(&SnapshotDomain {
                received_at: record.received_at,
                exchange_timestamp: exchange_time(*exchange_time_ms)?,
                bids: bids
                    .iter()
                    .map(|level| SnapshotBidsEntryDomain {
                        price_level: level.price.raw(),
                        quantity: level.quantity.raw(),
                    })
                    .collect(),
                asks: asks
                    .iter()
                    .map(|level| SnapshotAsksEntryDomain {
                        price_level: level.price.raw(),
                        quantity: level.quantity.raw(),
                    })
                    .collect(),
                symbol: symbol.clone(),
            }),
            BookRecord::Update {
                symbol,
                exchange_time_ms,
                change,
                ..
            } => self.encode(&PriceLevelUpdateDomain {
                backtest_timestamp: record.received_at,
                exchange_timestamp: exchange_time(*exchange_time_ms)?,
                price_level: change.price.raw(),
                quantity: change.quantity.raw(),
                side: change.side,
                symbol: symbol.clone(),
            }),
        }
    }

    /// Writes the final chunk. Without this the buffered records are lost.
    ///
    /// # Errors
    /// Returns [`OutputError::Encode`] when the write fails.
    pub fn finish(mut self) -> Result<(), OutputError> {
        Ok(self.writer.flush()?)
    }

    fn encode<T: SbeEncode>(&mut self, message: &T) -> Result<(), OutputError> {
        Ok(self.writer.write_msg(message)?)
    }
}

/// Writes queued records until every sender has gone, then finishes the file.
/// Blocks, so run it off the async threads.
///
/// # Errors
/// Returns the first [`OutputError`]. Dropping the receiver then tells the
/// senders that the writer has stopped.
pub fn write_records(mut records: Receiver<OutputRecord>, path: &Path) -> Result<(), OutputError> {
    let mut output = SbeOutput::create(path)?;
    let mut written = Ok(());
    while written.is_ok()
        && let Some(record) = records.blocking_recv()
    {
        written = output.write(&record);
    }
    // Keep what was accepted, even when a later record failed.
    let finished = output.finish();
    written.and(finished)
}

/// Reads a whole file with the supplied reader and decodes every field.
///
/// # Errors
/// Returns an [`OutputError`] when the file cannot be opened, or a record
/// cannot be decoded or is of an unknown type.
pub fn summarise(path: &Path) -> Result<Summary, OutputError> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut summary = Summary::default();
    let mut symbols = HashSet::new();
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                symbols.insert(SnapshotDomain::try_from_decoder(decoder)?.symbol);
                summary.snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                symbols.insert(PriceLevelUpdateDomain::try_from_decoder(decoder)?.symbol);
                summary.updates += 1;
            }
            _ => return Err(OutputError::UnknownRecord),
        }
    }
    summary.symbols = symbols.len();
    Ok(summary)
}

fn exchange_time(ms: i64) -> Result<DateTime<Utc>, OutputError> {
    DateTime::from_timestamp_millis(ms).ok_or(OutputError::InvalidTime(ms))
}
