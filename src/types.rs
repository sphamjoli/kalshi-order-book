//! Configuration, command-line arguments and process-level errors.

use crate::kalshi::auth::AuthError;
use crate::kalshi::rest::RestError;
use crate::kalshi::stream::StreamError;
use crate::output::OutputError;
use clap::{Args, Parser, Subcommand};
use derive_from_env::FromEnv;
use std::path::PathBuf;
use thiserror::Error;
use tokio::task::JoinError;

/// Settings read from the environment. Holds the key path, not key material.
/// The URLs have no default, so an unset URL fails instead of selecting another
/// environment.
#[derive(Clone, Debug, PartialEq, FromEnv)]
pub struct Config {
    pub kalshi_api_key_id: String,
    pub kalshi_private_key_path: String,
    pub kalshi_rest_url: String,
    pub kalshi_ws_url: String,
}

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "engineering-challenge",
    version,
    about = "Records Kalshi order books as FIX SBE",
    after_help = "Kalshi credentials and URLs come from the environment or a .env file. \
                  RUST_LOG sets log detail. Logs go to stderr."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: CliCommand,
}

/// How one recording runs.
#[derive(Debug, Clone, PartialEq, Eq, Args)]
pub struct CollectOptions {
    /// The most markets to place during the run.
    #[arg(long)]
    pub max_markets: Option<usize>,
    /// The most markets per socket, exceeded only at the provider's socket limit.
    #[arg(long, default_value_t = 2000, value_parser = parse_positive)]
    pub markets_per_socket: usize,
    /// Independent copies of every market. With two or more, a lost socket
    /// fails over to a live copy. Each copy adds its own sockets and traffic.
    #[arg(long, default_value_t = 2, value_parser = parse_positive)]
    pub feeds: usize,
    /// Where records go. The supplied writer needs a path, so stdout is
    /// addressed by its device name.
    #[arg(long, default_value = "/dev/stdout")]
    pub output: PathBuf,
}

/// What one run of the program does.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum CliCommand {
    /// Proves the configured key can make an authenticated call.
    Check,
    /// Reports how many open markets a full scan finds.
    Discover,
    /// Follows one market's book until stopped.
    Watch { ticker: String },
    /// Records open markets until stopped.
    Collect(CollectOptions),
    /// Decodes a recorded file and reports what it holds.
    Verify { path: PathBuf },
}

/// Failures that end the process. Recoverable failures stay as module errors.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("Authentication error: {0}")]
    Auth(#[from] AuthError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Kalshi REST error: {0}")]
    Rest(#[from] RestError),
    #[error("Kalshi stream error: {0}")]
    Stream(#[from] StreamError),
    #[error("Output error: {0}")]
    Output(#[from] OutputError),
    #[error("Output writer stopped unexpectedly: {0}")]
    Writer(#[from] JoinError),
    #[error("Output writer did not finish in time; buffered records may be lost")]
    WriterStalled,
}

/// A socket capacity or feed count of zero could hold no market.
fn parse_positive(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) | Err(_) => Err("must be a whole number of at least 1".to_owned()),
        Ok(capacity) => Ok(capacity),
    }
}
