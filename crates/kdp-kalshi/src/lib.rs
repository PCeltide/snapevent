//! Kalshi API clients (REST + WebSocket).
//!
//! This crate owns all wire-level interaction with Kalshi: HTTP for the REST
//! trade API and public market metadata ([`rest`]), and WebSocket for the live
//! L2 feed ([`ws`]). Wire payloads decode into the shared [`kdp_core`] types.
//!
//! Authentication (RSA-PSS-SHA256 request signing, see [`auth`]) is required
//! for private endpoints and is deferred to a later phase. The Phase 1 smoke
//! path ([`rest::list_markets`]) uses only the public, unauthenticated
//! `/markets` endpoint, so no credentials are needed to exercise it.

pub mod auth;
pub mod ratelimit;
pub mod rest;
pub mod ws;

use thiserror::Error;

/// Default Kalshi REST API base URL (production).
pub const KALSHI_API_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";

/// Errors that can arise while talking to Kalshi.
#[derive(Debug, Error)]
pub enum KalshiError {
    /// The HTTP request itself failed: DNS, TLS, timeout, or a non-success
    /// status code surfaced via `error_for_status`.
    #[error("kalshi http request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// The HTTP call succeeded but the body could not be decoded as expected.
    /// (Reserved for hand-rolled decoding paths; reqwest decode failures map to
    /// [`KalshiError::Http`].)
    #[error("failed to decode kalshi response: {0}")]
    Decode(String),

    /// A required credential environment variable was not set.
    #[error("missing credential environment variable {0}")]
    MissingCredential(&'static str),

    /// The private key file could not be read.
    #[error("reading private key file {path}: {source}")]
    KeyFile {
        /// Path we tried to read.
        path: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The private key PEM could not be parsed as PKCS#8 or PKCS#1.
    #[error("invalid RSA private key PEM (tried PKCS#8 then PKCS#1): {0}")]
    KeyParse(String),

    /// Producing an RSA-PSS signature failed.
    #[error("RSA-PSS signing failed: {0}")]
    Signing(String),

    /// A WebSocket transport or handshake error.
    #[error("websocket error: {0}")]
    WebSocket(String),
}

/// Convenience result alias for this crate.
pub type Result<T> = std::result::Result<T, KalshiError>;
