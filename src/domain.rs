//! Domain units and order books: exact fixed-point values and deterministic
//! book transitions. Owns no I/O. Owns the validated representation of price
//! and quantity once data has crossed the wire boundary.
//!
//! Invariants:
//!
//! - A book is built whole from one snapshot; replacing it replaces the value.
//! - Each transition is atomic: a rejected input changes nothing.
//! - Output uses absolute quantities; zero deletes a level; stored
//!   quantities stay positive.

use crate::Side;
use std::collections::BTreeMap;
use thiserror::Error;

/// Reasons a value or book transition is rejected. Each rejection leaves
/// existing book state unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BookError {
    #[error("price cannot be negative: {0}")]
    NegativePrice(i64),
    #[error("quantity cannot be negative: {0}")]
    NegativeQuantity(i64),
    #[error("quantity overflowed")]
    QuantityOverflow,
    #[error("price level appears twice in one snapshot: {0}")]
    DuplicateLevel(i64),
}

/// A price in fixed point, where 1_000_000 is one unit of currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(i64);

/// An absolute quantity in fixed point, where 1_000_000 is one contract.
/// Zero is valid here because output uses it to report a removed level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quantity(i64);

/// A signed change to a level, in the same fixed-point scale as [`Quantity`].
/// The compiler rejects a delta where an absolute quantity is required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuantityDelta(i64);

/// One price level of a full book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: Price,
    pub quantity: Quantity,
}

/// The absolute result of one accepted change. Quantity zero means the level
/// is absent after the change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelChange {
    pub side: Side,
    pub price: Price,
    pub quantity: Quantity,
}

/// The replicated state of one market, independent of any exchange. There is no
/// empty constructor: a book exists only once a full snapshot has built it,
/// so a change cannot be applied before the first snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<Price, Quantity>,
    asks: BTreeMap<Price, Quantity>,
}

/// One normalised input from any exchange. A provider adapter produces these,
/// so nothing past the adapter depends on an exchange's wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    /// The whole book of one market.
    Snapshot {
        symbol: String,
        exchange_time_ms: i64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// A signed change to one level.
    Delta {
        symbol: String,
        exchange_time_ms: i64,
        side: Side,
        price: Price,
        delta: QuantityDelta,
    },
    /// Messages were lost, so every book fed by this stream is unreliable.
    ContinuityLost,
}

impl Price {
    /// Creates a price from a fixed-point value. Exchange-specific upper bounds
    /// belong to the adapter that knows them.
    ///
    /// # Errors
    /// Returns [`BookError::NegativePrice`] when `raw` is below zero.
    pub fn try_new(raw: i64) -> Result<Self, BookError> {
        if raw < 0 {
            return Err(BookError::NegativePrice(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the fixed-point value.
    pub fn raw(self) -> i64 {
        self.0
    }
}

impl Quantity {
    /// No contracts. As output, this reports a removed level.
    pub const ZERO: Self = Self(0);

    /// Creates a quantity from a fixed-point value.
    ///
    /// # Errors
    /// Returns [`BookError::NegativeQuantity`] when `raw` is below zero.
    pub fn try_new(raw: i64) -> Result<Self, BookError> {
        if raw < 0 {
            return Err(BookError::NegativeQuantity(raw));
        }
        Ok(Self(raw))
    }

    /// Returns the fixed-point value.
    pub fn raw(self) -> i64 {
        self.0
    }

    fn checked_apply(self, delta: QuantityDelta) -> Result<Self, BookError> {
        let next = self
            .0
            .checked_add(delta.0)
            .ok_or(BookError::QuantityOverflow)?;
        Self::try_new(next)
    }
}

impl QuantityDelta {
    /// Creates a change from a signed fixed-point value. All `i64` values are
    /// valid; [`Book::apply`] rejects a change that the level cannot absorb.
    pub fn new(raw: i64) -> Self {
        Self(raw)
    }
}

impl Book {
    /// Builds a whole book from a snapshot. Replacing a book means replacing
    /// the value, so a rejected snapshot leaves the previous book untouched.
    /// Zero-quantity levels are treated as absent. O(L log L) for L levels.
    ///
    /// # Errors
    /// Returns [`BookError::DuplicateLevel`] when a side repeats a price.
    pub fn from_levels(
        bids: impl IntoIterator<Item = Level>,
        asks: impl IntoIterator<Item = Level>,
    ) -> Result<Self, BookError> {
        Ok(Self {
            bids: collect_side(bids)?,
            asks: collect_side(asks)?,
        })
    }

    /// Applies a signed change and returns the resulting absolute quantity.
    /// A missing level counts as zero. O(log L) for L levels on the side.
    ///
    /// # Errors
    /// Returns an error and leaves the book unchanged when the result would
    /// be negative or overflow.
    pub fn apply(
        &mut self,
        side: Side,
        price: Price,
        delta: QuantityDelta,
    ) -> Result<LevelChange, BookError> {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let current = levels.get(&price).copied().unwrap_or(Quantity::ZERO);
        let quantity = current.checked_apply(delta)?;
        if quantity == Quantity::ZERO {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        Ok(LevelChange {
            side,
            price,
            quantity,
        })
    }

    /// Returns bids from the highest price to the lowest. Allocates nothing.
    pub fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids.iter().rev().map(to_level)
    }

    /// Returns asks from the lowest price to the highest. Allocates nothing.
    pub fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks.iter().map(to_level)
    }

    /// Returns the level count of the deeper side. O(1).
    pub fn depth(&self) -> usize {
        self.bids.len().max(self.asks.len())
    }

    /// Returns whether the best bid is at or above the best ask. Crossing is
    /// not assumed impossible, so this is a fact for the caller to judge, not
    /// an error. O(log L) for L levels.
    pub fn is_crossed(&self) -> bool {
        match (self.bids.last_key_value(), self.asks.first_key_value()) {
            (Some((bid, _)), Some((ask, _))) => bid >= ask,
            _ => false,
        }
    }
}

fn to_level((price, quantity): (&Price, &Quantity)) -> Level {
    Level {
        price: *price,
        quantity: *quantity,
    }
}

fn collect_side(
    levels: impl IntoIterator<Item = Level>,
) -> Result<BTreeMap<Price, Quantity>, BookError> {
    let mut side = BTreeMap::new();
    for level in levels {
        if side.insert(level.price, level.quantity).is_some() {
            return Err(BookError::DuplicateLevel(level.price.raw()));
        }
    }
    side.retain(|_, quantity| *quantity != Quantity::ZERO);
    Ok(side)
}
