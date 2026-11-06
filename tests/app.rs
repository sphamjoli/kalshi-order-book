//! Command parsing and shutdown behaviour of the application.

use clap::Parser;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use engineering_challenge::app::App;
use engineering_challenge::types::{AppError, Cli, CliCommand, CollectOptions, Config};
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;
use wiremock::{Mock, MockServer, ResponseTemplate};

fn key_file() -> NamedTempFile {
    let pem = SigningKey::from_bytes(&[7; 32])
        .to_pkcs8_pem(LineEnding::LF)
        .unwrap();
    let mut file = NamedTempFile::new().unwrap();
    file.write_all(pem.as_bytes()).unwrap();
    file
}

fn config(key: &NamedTempFile, url: String) -> Config {
    Config {
        kalshi_api_key_id: "test-key".to_owned(),
        kalshi_private_key_path: key.path().to_str().unwrap().to_owned(),
        kalshi_rest_url: url,
        kalshi_ws_url: "ws://127.0.0.1:1".to_owned(),
    }
}

#[test]
fn known_commands_parse_and_others_are_rejected() {
    let parse = |word| Cli::try_parse_from(["app", word]).map(|cli| cli.command);
    assert_eq!(parse("check").unwrap(), CliCommand::Check);
    assert_eq!(parse("discover").unwrap(), CliCommand::Discover);
    assert!(parse("nope").is_err());
    let empty_socket = ["app", "collect", "--markets-per-socket", "0"];
    assert!(Cli::try_parse_from(empty_socket).is_err());
    assert!(Cli::try_parse_from(["app", "collect", "--feeds", "0"]).is_err());
    let collect = Cli::try_parse_from(["app", "collect", "--max-markets", "3"]).unwrap();
    assert!(matches!(
        collect.command,
        CliCommand::Collect(CollectOptions {
            max_markets: Some(3),
            markets_per_socket: 2000,
            feeds: 2,
            ..
        })
    ));
    let watch = Cli::try_parse_from(["app", "watch", "M"]).unwrap();
    assert_eq!(
        watch.command,
        CliCommand::Watch {
            ticker: "M".to_owned()
        }
    );
}

#[test]
fn a_missing_key_file_is_an_io_error() {
    let key = key_file();
    let mut config = config(&key, "http://localhost".to_owned());
    config.kalshi_private_key_path = "/no/such/key".to_owned();
    assert!(matches!(App::new(&config), Err(AppError::Io(_))));
}

#[tokio::test]
async fn a_shutdown_stops_a_running_command_cleanly() {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(20)))
        .mount(&server)
        .await;
    let key = key_file();
    let app = App::new(&config(&key, server.uri())).unwrap();
    let token = app.shutdown_token();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        token.cancel();
    });
    let stopped = tokio::time::timeout(Duration::from_secs(5), app.run(CliCommand::Discover));
    stopped.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_command_error_is_returned() {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;
    let key = key_file();
    let app = App::new(&config(&key, server.uri())).unwrap();
    assert!(matches!(
        app.run(CliCommand::Check).await,
        Err(AppError::Rest(_))
    ));
}
