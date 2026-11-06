use clap::Parser;
use engineering_challenge::app::{self, App};
use engineering_challenge::types::{Cli, CliCommand, Config};
use tracing_subscriber::EnvFilter;

const DEFAULT_LOG: &str = "info";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    dotenvy::from_path(".env").ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG))
                // Its trace level prints the signed handshake headers.
                .add_directive("tungstenite=info".parse()?),
        )
        .with_writer(std::io::stderr)
        .init();
    // Decoding a file needs no credentials.
    if let CliCommand::Verify { path } = &cli.command {
        return Ok(app::verify(path)?);
    }
    let config = Config::from_env()?;
    if let Err(error) = App::new(&config)?.run(cli.command).await {
        // A stalled writer thread would keep a normal return from finishing.
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
    Ok(())
}
