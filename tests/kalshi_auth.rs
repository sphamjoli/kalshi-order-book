//! Request signing through the public signer API.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, SigningKey};
use engineering_challenge::kalshi::auth::{
    AuthError, AuthHeaders, DefaultRequestSigner, RequestSigner,
};

fn key() -> SigningKey {
    SigningKey::from_bytes(&[7; 32])
}

fn signer() -> DefaultRequestSigner {
    let pem = key().to_pkcs8_pem(LineEnding::LF).unwrap();
    DefaultRequestSigner::from_pem("test-key".to_owned(), &pem).unwrap()
}

fn decode(headers: &AuthHeaders) -> Signature {
    let bytes: [u8; 64] = STANDARD
        .decode(&headers.signature)
        .unwrap()
        .try_into()
        .unwrap();
    Signature::from_bytes(&bytes)
}

#[test]
fn signature_verifies_over_timestamp_method_path() {
    let headers = signer().sign(1_700_000_000_000, "GET", "/trade-api/v2/markets");
    key()
        .verifying_key()
        .verify_strict(b"1700000000000GET/trade-api/v2/markets", &decode(&headers))
        .unwrap();
    assert_eq!(headers.timestamp_ms, "1700000000000");
    assert_eq!(headers.key_id, "test-key");
}

#[test]
fn query_string_is_not_signed() {
    let signer = signer();
    assert_eq!(
        signer.sign(1, "GET", "/markets?limit=5"),
        signer.sign(1, "GET", "/markets")
    );
}

#[test]
fn invalid_pem_is_rejected() {
    assert_eq!(
        DefaultRequestSigner::from_pem("id".to_owned(), "not a key").unwrap_err(),
        AuthError::InvalidKey
    );
}

#[test]
fn debug_output_redacts_the_key() {
    let output = format!("{:?}", signer());
    assert!(output.contains("<redacted>"));
    assert!(!output.contains("SigningKey"));
}

#[test]
fn debug_output_redacts_the_headers() {
    let headers = signer().sign(1, "GET", "/a");
    let output = format!("{headers:?}");
    assert!(!output.contains(&headers.signature));
    assert!(!output.contains(&headers.key_id));
}
