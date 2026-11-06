//! Turns Kalshi frames into [`MarketEvent`]s: exact decimal scaling, NO-side
//! mirroring and sequence tracking. Pure apart from the sequence state.
//!
//! A NO bid at `p` is an offer to sell YES at `1 - p`, with the same quantity
//! and the same delta sign:
//! <https://docs.kalshi.com/getting_started/orderbook_responses>.

use crate::Side;
use crate::domain::{BookError, Level, MarketEvent, Price, Quantity, QuantityDelta};
use crate::kalshi::wire::{
    ContractSide, DELTA_TYPE, DeltaBody, ERROR_TYPE, Envelope, OK_TYPE, SNAPSHOT_TYPE,
    SUBSCRIBED_TYPE, SnapshotBody, SubscribedBody, WireLevel,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use thiserror::Error;
use tracing::warn;

/// Fixed-point value of one unit, and the highest price of a binary contract.
const SCALE: i64 = 1_000_000;
const MAX_DECIMAL_PLACES: usize = 6;
/// The most levels one side of an SBE snapshot can hold.
const MAX_LEVELS_PER_SIDE: usize = 65_535;

/// Reasons a frame cannot be normalised.
#[derive(Debug, Error)]
pub enum NormaliseError {
    #[error("malformed message: {0}")]
    Json(#[from] serde_json::Error),
    #[error("not an exact fixed-point value: {0}")]
    InvalidDecimal(String),
    #[error("price is above one dollar: {0}")]
    PriceOutOfRange(i64),
    #[error("book message on unannounced subscription {0}")]
    UnknownSubscription(u64),
    #[error("exchange time cannot be recorded: {0}")]
    InvalidTime(i64),
    #[error("a book side has {0} levels, more than a record can hold")]
    TooManyLevels(usize),
    #[error("{0} message has no {1}")]
    MissingField(&'static str, &'static str),
    #[error(transparent)]
    Book(#[from] BookError),
}

/// Normalises the frames of one connection. `seq` is not comparable across
/// connections, so each connection needs a new value.
#[derive(Debug, Default)]
pub struct Normaliser {
    /// Highest `seq` per announced subscription; `None` before the first.
    last_seq: HashMap<u64, Option<u64>>,
}

impl Normaliser {
    /// Returns the announced subscription. A connection has one orderbook
    /// subscription, however many markets it carries.
    pub fn subscription(&self) -> Option<u64> {
        self.last_seq.keys().next().copied()
    }

    /// Normalises one text frame. Returns `None` for a frame with no book
    /// meaning. Fails closed: a frame after a gap is discarded, and a type
    /// this code does not know could have changed a book, so both yield
    /// [`MarketEvent::ContinuityLost`] and the replacement snapshot supersedes
    /// them.
    ///
    /// # Errors
    /// Returns a [`NormaliseError`] when the frame cannot be trusted,
    /// including a book message without a position on an announced
    /// subscription.
    pub fn normalise(&mut self, frame: &str) -> Result<Option<MarketEvent>, NormaliseError> {
        let envelope: Envelope = serde_json::from_str(frame)?;
        match envelope.kind {
            SUBSCRIBED_TYPE => {
                let body: SubscribedBody = parse_body(&envelope, SUBSCRIBED_TYPE)?;
                self.last_seq.entry(body.sid).or_default();
                Ok(None)
            }
            OK_TYPE => match (envelope.sid, envelope.seq) {
                (Some(sid), Some(seq)) if self.last_seq.contains_key(&sid) => Ok(self
                    .advance(sid, seq)?
                    .then_some(MarketEvent::ContinuityLost)),
                _ => Ok(None),
            },
            ERROR_TYPE => {
                warn!("Kalshi rejected a command");
                match (envelope.sid, envelope.seq) {
                    (Some(sid), Some(seq)) if self.last_seq.contains_key(&sid) => Ok(self
                        .advance(sid, seq)?
                        .then_some(MarketEvent::ContinuityLost)),
                    _ => Ok(None),
                }
            }
            SNAPSHOT_TYPE => self.book_event(&envelope, SNAPSHOT_TYPE, snapshot_event),
            DELTA_TYPE => self.book_event(&envelope, DELTA_TYPE, delta_event),
            _ => {
                if let (Some(sid), Some(seq)) = (envelope.sid, envelope.seq)
                    && self.last_seq.contains_key(&sid)
                {
                    self.advance(sid, seq)?;
                }
                Ok(Some(MarketEvent::ContinuityLost))
            }
        }
    }

    fn book_event(
        &mut self,
        envelope: &Envelope,
        kind: &'static str,
        build: fn(&Envelope) -> Result<MarketEvent, NormaliseError>,
    ) -> Result<Option<MarketEvent>, NormaliseError> {
        let sid = envelope
            .sid
            .ok_or(NormaliseError::MissingField(kind, "sid"))?;
        let seq = envelope
            .seq
            .ok_or(NormaliseError::MissingField(kind, "seq"))?;
        if self.advance(sid, seq)? {
            return Ok(Some(MarketEvent::ContinuityLost));
        }
        build(envelope).map(Some)
    }

    /// Records a position and returns whether it breaks the sequence. The
    /// first frame sets the baseline, and a repeat or backward step counts as
    /// a gap. The mark never falls, so a replay cannot look contiguous.
    fn advance(&mut self, sid: u64, seq: u64) -> Result<bool, NormaliseError> {
        let last = self
            .last_seq
            .get_mut(&sid)
            .ok_or(NormaliseError::UnknownSubscription(sid))?;
        let gap = last.is_some_and(|last| last.checked_add(1) != Some(seq));
        *last = (*last).max(Some(seq));
        Ok(gap)
    }
}

fn snapshot_event(envelope: &Envelope) -> Result<MarketEvent, NormaliseError> {
    let body: SnapshotBody = parse_body(envelope, SNAPSHOT_TYPE)?;
    let exchange_time_ms = exchange_time(envelope.sending_ts_ms)?;
    Ok(MarketEvent::Snapshot {
        symbol: body.market_ticker,
        exchange_time_ms,
        exchange_clock: body.clock,
        bids: levels(&body.yes_dollars_fp, ContractSide::Yes)?,
        asks: levels(&body.no_dollars_fp, ContractSide::No)?,
    })
}

fn delta_event(envelope: &Envelope) -> Result<MarketEvent, NormaliseError> {
    let body: DeltaBody = parse_body(envelope, DELTA_TYPE)?;
    let (side, price) = to_book_side(body.side, parse_price(&body.price_dollars)?)?;
    Ok(MarketEvent::Delta {
        symbol: body.market_ticker,
        exchange_time_ms: exchange_time(body.ts_ms.or(envelope.sending_ts_ms))?,
        exchange_clock: body.clock,
        side,
        price,
        delta: QuantityDelta::new(scale(&body.delta_fp)?),
    })
}

fn parse_body<'a, T: serde::Deserialize<'a>>(
    envelope: &Envelope<'a>,
    kind: &'static str,
) -> Result<T, NormaliseError> {
    let body = envelope
        .msg
        .ok_or(NormaliseError::MissingField(kind, "msg"))?;
    Ok(serde_json::from_str(body.get())?)
}

/// Kalshi documents its timestamps as optional, and a missing one says nothing
/// about the book, so the local time stands in rather than costing a resync.
fn exchange_time(ms: Option<i64>) -> Result<i64, NormaliseError> {
    checked_time(ms.unwrap_or_else(|| Utc::now().timestamp_millis()))
}

/// Rejects a time the output format cannot hold, so one bad frame costs a
/// resync rather than the whole run.
fn checked_time(ms: i64) -> Result<i64, NormaliseError> {
    if ms < 0 || DateTime::from_timestamp_millis(ms).is_none() {
        return Err(NormaliseError::InvalidTime(ms));
    }
    Ok(ms)
}

fn levels(wire: &[WireLevel], contract: ContractSide) -> Result<Vec<Level>, NormaliseError> {
    if wire.len() > MAX_LEVELS_PER_SIDE {
        return Err(NormaliseError::TooManyLevels(wire.len()));
    }
    wire.iter()
        .map(|level| {
            let (_, price) = to_book_side(contract, parse_price(&level.price)?)?;
            Ok(Level {
                price,
                quantity: Quantity::try_new(scale(&level.count)?)?,
            })
        })
        .collect()
}

/// Maps a contract bid onto the YES book.
fn to_book_side(contract: ContractSide, price: Price) -> Result<(Side, Price), BookError> {
    match contract {
        ContractSide::Yes => Ok((Side::Buy, price)),
        ContractSide::No => Ok((Side::Sell, Price::try_new(SCALE - price.raw())?)),
    }
}

fn parse_price(text: &str) -> Result<Price, NormaliseError> {
    let raw = scale(text)?;
    if raw > SCALE {
        return Err(NormaliseError::PriceOutOfRange(raw));
    }
    Ok(Price::try_new(raw)?)
}

/// Converts decimal text to fixed point, rejecting anything inexact.
fn scale(text: &str) -> Result<i64, NormaliseError> {
    Some(text)
        .filter(|text| is_canonical(text))
        .and_then(|text| Decimal::from_str_exact(text).ok())
        .and_then(|value| value.checked_mul(Decimal::from(SCALE)))
        .and_then(|scaled| scaled.to_i64())
        .ok_or_else(|| NormaliseError::InvalidDecimal(text.to_owned()))
}

/// Accepts `-?digits(.digits)?` with at most six decimal places. The decimal
/// library alone also accepts forms such as `1_0.00`, where one corrupted byte
/// would change a value silently.
fn is_canonical(text: &str) -> bool {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, "0"));
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    digits(whole) && digits(fraction) && fraction.len() <= MAX_DECIMAL_PLACES
}
