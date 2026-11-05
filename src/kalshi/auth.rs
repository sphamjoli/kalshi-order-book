//! Kalshi request signing. Owns the private key; callers only see the headers
//! produced for one request. Signatures are per request, so nothing here is a
//! session: sign again for every HTTP call and every WebSocket handshake.
//!
//! Scheme: <https://docs.kalshi.com/getting_started/api_keys>, read November
//! 2026. A WebSocket handshake signs `GET` and `/trade-api/ws/v2`, per
//! <https://docs.kalshi.com/getting_started/quick_start_websockets>.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::header::{HeaderMap, HeaderValue, InvalidHeaderValue};
use thiserror::Error;

/// Header carrying the API key ID.
pub const KEY_HEADER: &str = "KALSHI-ACCESS-KEY";
/// Header carrying the signing time in Unix milliseconds.
pub const TIMESTAMP_HEADER: &str = "KALSHI-ACCESS-TIMESTAMP";
/// Header carrying the base64-encoded signature.
pub const SIGNATURE_HEADER: &str = "KALSHI-ACCESS-SIGNATURE";

/// Reasons a signer cannot be created.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    /// Carries no source error, so the message contains no key material.
    #[error("private key is not a PKCS#8 Ed25519 PEM")]
    InvalidKey,
}

/// Produces the authentication headers for one exchange request.
pub trait RequestSigner: Send + Sync {
    /// Signs one request. The caller supplies the time and owns the clock.
    fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> AuthHeaders;
}

/// Holds an Ed25519 key. Kalshi selects the verification algorithm from the
/// registered public key, so an RSA PEM is rejected rather than misused.
pub struct DefaultRequestSigner {
    key_id: String,
    key: SigningKey,
}

/// Header values for exactly one request.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthHeaders {
    /// Value for [`KEY_HEADER`].
    pub key_id: String,
    /// Value for [`TIMESTAMP_HEADER`].
    pub timestamp_ms: String,
    /// Value for [`SIGNATURE_HEADER`].
    pub signature: String,
}

impl DefaultRequestSigner {
    /// Creates a signer from a key ID and PEM contents, not a file path.
    ///
    /// # Errors
    /// Returns [`AuthError::InvalidKey`] when the PEM cannot be used.
    pub fn from_pem(key_id: String, pem: &str) -> Result<Self, AuthError> {
        let key = SigningKey::from_pkcs8_pem(pem).map_err(|_| AuthError::InvalidKey)?;
        Ok(Self { key_id, key })
    }
}

impl RequestSigner for DefaultRequestSigner {
    /// Signs `timestamp + method + path`, excluding any query string.
    fn sign(&self, timestamp_ms: i64, method: &str, path: &str) -> AuthHeaders {
        let path = path.split('?').next().unwrap_or(path);
        let message = format!("{timestamp_ms}{method}{path}");
        AuthHeaders {
            key_id: self.key_id.clone(),
            timestamp_ms: timestamp_ms.to_string(),
            signature: STANDARD.encode(self.key.sign(message.as_bytes()).to_bytes()),
        }
    }
}

impl std::fmt::Debug for DefaultRequestSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultRequestSigner")
            .field("key_id", &self.key_id)
            .field("key", &"<redacted>")
            .finish()
    }
}

impl std::fmt::Debug for AuthHeaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthHeaders")
            .field("timestamp_ms", &self.timestamp_ms)
            .field("key_id", &"<redacted>")
            .field("signature", &"<redacted>")
            .finish()
    }
}

/// Builds the request headers, marking the signature as sensitive so that
/// HTTP and WebSocket clients keep it out of their debug output.
///
/// # Errors
/// Returns an error when a value is not a valid header value.
pub fn auth_header_map(signed: &AuthHeaders) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    let mut signature = HeaderValue::from_str(&signed.signature)?;
    signature.set_sensitive(true);
    headers.insert(KEY_HEADER, HeaderValue::from_str(&signed.key_id)?);
    headers.insert(
        TIMESTAMP_HEADER,
        HeaderValue::from_str(&signed.timestamp_ms)?,
    );
    headers.insert(SIGNATURE_HEADER, signature);
    Ok(headers)
}
