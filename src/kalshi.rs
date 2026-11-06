//! Kalshi provider: implements the contracts in [`crate::provider`] with wire
//! parsing, normalisation into domain values, and authentication. Owns knowledge of the exchange protocol; nothing
//! outside this module should depend on Kalshi's message shapes.
//!
//! Rules:
//!
//! - Unknown types with a valid envelope still participate in sequence
//!   tracking; unknown book-changing semantics require invalidation.
//! - A partial/failed scan cannot delete symbols absent from the
//!   incomplete result.

pub mod auth;
pub mod directory;
pub mod normalise;
pub mod rest;
mod session;
pub mod stream;
pub mod wire;
