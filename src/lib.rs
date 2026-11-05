//! A market-data collector: discovers markets, keeps an order book for each,
//! and records snapshots and price-level updates as SBE.

pub mod app;
pub mod collector;
pub mod domain;
pub mod failover;
pub mod kalshi;
pub mod output;
mod side;
pub mod transport;
pub mod types;
pub mod util;
pub use side::{InvalidSide, Side};
