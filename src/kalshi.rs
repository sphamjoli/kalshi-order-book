//! Kalshi provider: implements the contracts in [`crate::provider`] with wire
//! parsing, normalisation into domain values, and authentication. Owns
//! knowledge of the exchange protocol. Code outside this module should not
//! depend on Kalshi's message shapes.
//!
//! Rules:
//!
//! - Unknown types with a valid envelope still take part in sequence tracking,
//!   and each one invalidates the stream's books.
//! - A partial or failed scan cannot remove symbols absent from the incomplete
//!   result.

pub mod auth;
pub mod directory;
pub mod normalise;
pub mod rest;
mod session;
pub mod stream;
pub mod wire;
