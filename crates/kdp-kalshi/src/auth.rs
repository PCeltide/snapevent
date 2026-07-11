//! RSA-PSS-SHA256 request signing for Kalshi's authenticated endpoints.
//!
//! Kalshi authenticates **every** request — including the WebSocket upgrade for
//! public market-data channels — with an RSA-PSS signature over the string
//! `timestamp_ms + HTTP_METHOD + request_path`, sent in three headers
//! ([`HEADER_KEY`], [`HEADER_SIGNATURE`], [`HEADER_TIMESTAMP`]). The signature is
//! RSA-PSS with SHA-256, MGF1-SHA256, and a salt length equal to the digest
//! length (32 bytes), base64-encoded — reimplemented from scratch against
//! Kalshi's spec (not copied from the proprietary reference client).
//!
//! [`KalshiCredentials`] loads the API key id and an RSA private key PEM (PKCS#8
//! or PKCS#1) and is shared by the REST and WebSocket clients. Key material is
//! never logged: signing is `#[instrument(skip(self))]` and the `Debug` impl
//! redacts the key.

/// Environment variable holding the Kalshi API key id (a UUID).
pub const ENV_API_KEY_ID: &str = "KALSHI_API_KEY_ID";

/// Environment variable holding the filesystem path to the RSA private key PEM.
pub const ENV_PRIVATE_KEY_PATH: &str = "KDP_KALSHI_PRIVATE_KEY_PATH";

/// Default private key path when [`ENV_PRIVATE_KEY_PATH`] is unset: a
/// repo-root `kalshi_private_key.pem` (git-ignored). Override via the
/// environment for portability; the CLI loads `.env` so this rarely applies.
pub const DEFAULT_PRIVATE_KEY_PATH: &str = "kalshi_private_key.pem";

/// Header carrying the API key id.
pub const HEADER_KEY: &str = "KALSHI-ACCESS-KEY";
/// Header carrying the base64 RSA-PSS-SHA256 signature.
pub const HEADER_SIGNATURE: &str = "KALSHI-ACCESS-SIGNATURE";
/// Header carrying the signing timestamp (unix milliseconds, as a string).
pub const HEADER_TIMESTAMP: &str = "KALSHI-ACCESS-TIMESTAMP";

/// PSS salt length in bytes: equal to the SHA-256 digest length, matching
/// Kalshi's `salt_length = DIGEST_LENGTH` parameter.
const SALT_LEN: usize = 32;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use sha2::Sha256;
use tracing::instrument;

use crate::KalshiError;

/// Kalshi API credentials: the key id plus a prepared RSA-PSS-SHA256 signer.
///
/// Construct with [`from_pem`](Self::from_pem) (or [`from_env`](Self::from_env))
/// and share across the REST and WebSocket clients. The signing key is built
/// once with the digest-length salt; signing is randomized per call. Key
/// material is never logged (signing spans `skip(self)`; `Debug` is redacted).
pub struct KalshiCredentials {
    key_id: String,
    signing_key: SigningKey<Sha256>,
}

impl KalshiCredentials {
    /// Build credentials from a key id and an RSA private key PEM, trying PKCS#8
    /// (`BEGIN PRIVATE KEY`) then PKCS#1 (`BEGIN RSA PRIVATE KEY`).
    #[instrument(skip(pem, key_id))]
    pub fn from_pem(key_id: impl Into<String>, pem: &str) -> Result<Self, KalshiError> {
        let key_id = key_id.into();
        let private_key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .map_err(|e| KalshiError::KeyParse(e.to_string()))?;
        let signing_key = SigningKey::<Sha256>::new_with_salt_len(private_key, SALT_LEN);
        Ok(Self {
            key_id,
            signing_key,
        })
    }

    /// Build credentials from the environment: [`ENV_API_KEY_ID`] for the key id
    /// and the PEM read from the path in [`ENV_PRIVATE_KEY_PATH`] (falling back
    /// to [`DEFAULT_PRIVATE_KEY_PATH`]). The CLI loads `.env` before calling this.
    #[instrument]
    pub fn from_env() -> Result<Self, KalshiError> {
        let key_id = std::env::var(ENV_API_KEY_ID)
            .map_err(|_| KalshiError::MissingCredential(ENV_API_KEY_ID))?;
        let path = std::env::var(ENV_PRIVATE_KEY_PATH)
            .unwrap_or_else(|_| DEFAULT_PRIVATE_KEY_PATH.to_string());
        let pem = std::fs::read_to_string(&path).map_err(|source| KalshiError::KeyFile {
            path: path.clone(),
            source,
        })?;
        Self::from_pem(key_id, &pem)
    }

    /// The API key id (non-secret; sent in [`HEADER_KEY`]).
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Sign `timestamp_ms + method + path` and return the base64 signature.
    ///
    /// Uses `try_sign_with_rng` (never the panicking `sign_with_rng`) so a
    /// signing failure surfaces as [`KalshiError::Signing`].
    #[instrument(skip(self))]
    pub fn sign(&self, ts_ms: i64, method: &str, path: &str) -> Result<String, KalshiError> {
        let message = format!("{ts_ms}{method}{path}");
        let mut rng = rand::thread_rng();
        let signature = self
            .signing_key
            .try_sign_with_rng(&mut rng, message.as_bytes())
            .map_err(|e| KalshiError::Signing(e.to_string()))?;
        Ok(STANDARD.encode(signature.to_bytes()))
    }

    /// The three Kalshi auth headers for `method`/`path`, signed at the current
    /// wall-clock time (unix milliseconds).
    #[instrument(skip(self))]
    pub fn signed_headers(
        &self,
        method: &str,
        path: &str,
    ) -> Result<[(&'static str, String); 3], KalshiError> {
        let ts_ms = chrono::Utc::now().timestamp_millis();
        self.signed_headers_at(ts_ms, method, path)
    }

    /// The three Kalshi auth headers for `method`/`path` at a caller-supplied
    /// timestamp (deterministic; the basis of [`signed_headers`](Self::signed_headers)).
    #[instrument(skip(self))]
    pub fn signed_headers_at(
        &self,
        ts_ms: i64,
        method: &str,
        path: &str,
    ) -> Result<[(&'static str, String); 3], KalshiError> {
        let signature = self.sign(ts_ms, method, path)?;
        Ok([
            (HEADER_KEY, self.key_id.clone()),
            (HEADER_SIGNATURE, signature),
            (HEADER_TIMESTAMP, ts_ms.to_string()),
        ])
    }
}

impl std::fmt::Debug for KalshiCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalshiCredentials")
            .field("key_id", &self.key_id)
            .field("signing_key", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use rsa::pkcs1::DecodeRsaPrivateKey;
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::pss::{Signature, VerifyingKey};
    use rsa::signature::Verifier;
    use rsa::RsaPrivateKey;
    use sha2::Sha256;

    use crate::KalshiError;

    // Throwaway 2048-bit RSA test key (generated with openssl for this test only;
    // NOT a credential). Same key in PKCS#1 and PKCS#8 form to exercise both
    // loaders — the real Kalshi key is PKCS#1 ("BEGIN RSA PRIVATE KEY").
    const TEST_KEY_PKCS1: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEArPXeruTqzeG+G8wEi4IXQ3io5m8C5i1niz5rnP4T9jk+rr61
sQSUHrhm+bjWDgZkB9S7b1YfOKlHLSaWZlDVEkV4ih9628kHiQTuUg8dEJ+qV4L+
KyT3NtD5SLX8zwAPVbRDe6Fs524UxpS/90sxAYwugbo7VaOIKDousLtF2l5o43uI
cceJg25coEpLWdBhd9gicOB6LOd0aL6M8hnjO+gpaY8Y2Tu6D5+joKoNylNx0Fs7
/d/0alPG+/YPuk9T93oYwi+lgDj7wYaGycfLoda2kSXxpzDEbm7Cf3qSz3z6MUXQ
FZ7ZBaurK4bbGWbuthOpLYEz6WdMPPczT/a7QQIDAQABAoIBABJ754jsWPWYtERB
45MfMNTv7lOgtdVwhm94mRqQLVpJLYbnmwDoets6R3L/oGH4/TQNtYbv/r/Y7gzd
YgGxjhvORdQJCjcL1FzTJJRWHiaP0a4EMfKIGE1ePDiGORbMLFdd6j+qSuUEEX9U
F/HZ3MphYfpQ3eod4xKy08OPt/wz9nOE8MZjzg5jn508jcSlybKnD2xmuxwTen+7
oDQsfinLbiQJUeD8isuCZFUwaST+70R9Sfi8LnS22y+zfy6UpYSt4EX0+F+KQZGp
tpRj4LZK/XgXycqOUhFC8Ab4hN3HveBSLGIv2kUfjT4lIwIy16QF8IIn/qXBY9BM
FeqoB+MCgYEA7CVNgmD4mi9Fujc0Poqwk+Ag7OVGMxp7wvomAtD1lYVbUOvFvrU8
Z5ZwunwI4Qtasf3zTsRGwQ5xglx03MthoCAjArt9jYci/0qupeobn9JZrrl9durB
9WryoBpC0zjFQisiyDZT1h59pWLGqckge7YHXO3vrYI2gdqfx7KTiuMCgYEAu4CY
OBrKQI6iaVKCbk8HGygral6e2J1TM257TP4Llq1u6yPPgg5FOofCq2Lyapgp0ndE
4BJa2u7EOGgd03bbeM+YWz4LPI+CLKRW52syaiYW7taVJ3DrbRoAGUkF0zrzjcro
2Hnr1UHezvMqvx7sjpCbHw+pYDpp6vstxj9TBosCgYEA1Tx9+CxucHQdZ6CvyYXd
Gzr5IFGMiVrxxMeziTl9ea35HmI4pxPq3rNHSe306pohJLbnfQnZxjyvnQK1+Caj
Gj/KvY3mOuV7YcHjYSi8Fx6QIymWNMqZqG4RdycfjrIl1bEz8Ey2eZQA61X9hJV8
gpmFnpGwqyH47Fspit8jQfcCgYBTmsVEzv07x923JKkv0mESxNiG92XQpGXC2xJz
hBtatj5s7mzKSt6neH1euiHpUavkQnYdi1GjqS8pD5OtBKRbvATtOj78Y+jhSu3N
BklWd2FmYZvkGD+BSESfAaZtRy3uHXmxfLuhPVvB3z9CNOG5t9TTBsK5O5KayiDg
8r9sfQKBgQDJARE6C88qhrO0rc8jxwmAtrN+MLw3nVhZh5oy9KV2jOExx62px1qZ
/h6d6cbtl83OAvdkwMK6yFvGymfMNRuOLfM5xYPkVayupCr4pdmzNSEqRvhLQtY/
TvCNFuTNKMwyk3JGcGxd1RRFL2ImsBUgnZts7/4YAww+ZOUnZnhH0Q==
-----END RSA PRIVATE KEY-----
";

    const TEST_KEY_PKCS8: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCs9d6u5OrN4b4b
zASLghdDeKjmbwLmLWeLPmuc/hP2OT6uvrWxBJQeuGb5uNYOBmQH1LtvVh84qUct
JpZmUNUSRXiKH3rbyQeJBO5SDx0Qn6pXgv4rJPc20PlItfzPAA9VtEN7oWznbhTG
lL/3SzEBjC6BujtVo4goOi6wu0XaXmjje4hxx4mDblygSktZ0GF32CJw4Hos53Ro
vozyGeM76ClpjxjZO7oPn6Ogqg3KU3HQWzv93/RqU8b79g+6T1P3ehjCL6WAOPvB
hobJx8uh1raRJfGnMMRubsJ/epLPfPoxRdAVntkFq6srhtsZZu62E6ktgTPpZ0w8
9zNP9rtBAgMBAAECggEAEnvniOxY9Zi0REHjkx8w1O/uU6C11XCGb3iZGpAtWkkt
huebAOh62zpHcv+gYfj9NA21hu/+v9juDN1iAbGOG85F1AkKNwvUXNMklFYeJo/R
rgQx8ogYTV48OIY5FswsV13qP6pK5QQRf1QX8dncymFh+lDd6h3jErLTw4+3/DP2
c4TwxmPODmOfnTyNxKXJsqcPbGa7HBN6f7ugNCx+KctuJAlR4PyKy4JkVTBpJP7v
RH1J+LwudLbbL7N/LpSlhK3gRfT4X4pBkam2lGPgtkr9eBfJyo5SEULwBviE3ce9
4FIsYi/aRR+NPiUjAjLXpAXwgif+pcFj0EwV6qgH4wKBgQDsJU2CYPiaL0W6NzQ+
irCT4CDs5UYzGnvC+iYC0PWVhVtQ68W+tTxnlnC6fAjhC1qx/fNOxEbBDnGCXHTc
y2GgICMCu32NhyL/Sq6l6huf0lmuuX126sH1avKgGkLTOMVCKyLINlPWHn2lYsap
ySB7tgdc7e+tgjaB2p/HspOK4wKBgQC7gJg4GspAjqJpUoJuTwcbKCtqXp7YnVMz
bntM/guWrW7rI8+CDkU6h8KrYvJqmCnSd0TgElra7sQ4aB3Tdtt4z5hbPgs8j4Is
pFbnazJqJhbu1pUncOttGgAZSQXTOvONyujYeevVQd7O8yq/HuyOkJsfD6lgOmnq
+y3GP1MGiwKBgQDVPH34LG5wdB1noK/Jhd0bOvkgUYyJWvHEx7OJOX15rfkeYjin
E+res0dJ7fTqmiEktud9CdnGPK+dArX4JqMaP8q9jeY65XthweNhKLwXHpAjKZY0
ypmobhF3Jx+OsiXVsTPwTLZ5lADrVf2ElXyCmYWekbCrIfjsWymK3yNB9wKBgFOa
xUTO/TvH3bckqS/SYRLE2Ib3ZdCkZcLbEnOEG1q2PmzubMpK3qd4fV66IelRq+RC
dh2LUaOpLykPk60EpFu8BO06Pvxj6OFK7c0GSVZ3YWZhm+QYP4FIRJ8Bpm1HLe4d
ebF8u6E9W8HfP0I04bm31NMGwrk7kprKIODyv2x9AoGBAMkBEToLzyqGs7StzyPH
CYC2s34wvDedWFmHmjL0pXaM4THHranHWpn+Hp3pxu2Xzc4C92TAwrrIW8bKZ8w1
G44t8znFg+RVrK6kKvil2bM1ISpG+EtC1j9O8I0W5M0ozDKTckZwbF3VFEUvYiaw
FSCdm2zv/hgDDD5k5SdmeEfR
-----END PRIVATE KEY-----
";

    // Independent RSA-PSS-SHA256 (MGF1-SHA256, salt_len = digest = 32) signature
    // produced by `openssl dgst` over OPENSSL_MSG. If our verifying key accepts
    // it, our PSS parameters match an independent implementation — and therefore
    // Kalshi's server.
    const OPENSSL_MSG_TS: i64 = 1_700_000_000_000;
    const OPENSSL_MSG_METHOD: &str = "GET";
    const OPENSSL_MSG_PATH: &str = "/trade-api/ws/v2";
    const OPENSSL_SIG_B64: &str = "LbD/a/eXe3FrJdaOmS/ktMXAbjSJwjKvC0spqAP+YAQBmqLKWGXCGIzeFObAH1GSDq5w3ZQv9xebiMOIuXk4y4CNEd8GbriWrBvEUMu7K8CYoHuHOfyfR93d+rxLlqYC6/3n5iQ0WBVyX2vnNJbeCDJRwiXRVuQh+z8ejsmWrbg3Qf+EDz31wbpTMe84x/OR2092r1C0F3YW1XOoOLLkVthZcPTkEgEVNCwIikSaKf13WUqCa1kUuqLjVP++CEeye4WyIDgvUgdNXtYH16VnIzfQNeV1v+iAfRoS5lZ1dt4YzGKM08RKs/t2QNLnjnFzoe4kuKCpQPFRmrXYi/LA2g==";

    /// Build a salt-len-32 PSS verifying key directly from a test PEM (test-only;
    /// production only ever signs — Kalshi verifies).
    fn verifying_key(pem: &str) -> VerifyingKey<Sha256> {
        let key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .expect("load test key");
        VerifyingKey::<Sha256>::new_with_salt_len(key.to_public_key(), 32)
    }

    #[test]
    fn loads_pkcs8_and_signs_verifiably() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("load pkcs8");
        let sig_b64 = creds.sign(123, "GET", "/x").expect("sign");
        let bytes = STANDARD.decode(&sig_b64).expect("b64 decode");
        let signature = Signature::try_from(bytes.as_slice()).expect("parse sig");
        verifying_key(TEST_KEY_PKCS8)
            .verify(b"123GET/x", &signature)
            .expect("our own signature must verify");
    }

    #[test]
    fn loads_pkcs1_key_our_real_format() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS1).expect("load pkcs1");
        let sig_b64 = creds.sign(1, "GET", "/y").expect("sign");
        assert!(!sig_b64.is_empty());
    }

    #[test]
    fn verifies_independent_openssl_signature() {
        // The load-bearing cross-implementation check for the #1 risk.
        let msg = format!("{OPENSSL_MSG_TS}{OPENSSL_MSG_METHOD}{OPENSSL_MSG_PATH}");
        let bytes = STANDARD.decode(OPENSSL_SIG_B64).expect("b64 decode");
        let signature = Signature::try_from(bytes.as_slice()).expect("parse sig");
        verifying_key(TEST_KEY_PKCS1)
            .verify(msg.as_bytes(), &signature)
            .expect("openssl signature must verify with our params");
    }

    #[test]
    fn rejects_tampered_message() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("load");
        let sig_b64 = creds.sign(100, "GET", "/path").expect("sign");
        let bytes = STANDARD.decode(&sig_b64).expect("b64");
        let signature = Signature::try_from(bytes.as_slice()).expect("parse sig");
        assert!(
            verifying_key(TEST_KEY_PKCS8)
                .verify(b"100GET/PATH", &signature)
                .is_err(),
            "a tampered message must not verify"
        );
    }

    #[test]
    fn signatures_are_randomized() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("load");
        let a = creds.sign(1, "GET", "/x").expect("sign a");
        let b = creds.sign(1, "GET", "/x").expect("sign b");
        assert_ne!(a, b, "PSS signatures must be randomized");
    }

    #[test]
    fn signed_headers_have_expected_names_and_values() {
        let creds = KalshiCredentials::from_pem("my-key-id", TEST_KEY_PKCS8).expect("load");
        let headers = creds
            .signed_headers_at(1_700_000_000_000, "GET", "/trade-api/ws/v2")
            .expect("headers");
        assert_eq!(headers[0], (HEADER_KEY, "my-key-id".to_string()));
        assert_eq!(headers[1].0, HEADER_SIGNATURE);
        assert!(!headers[1].1.is_empty(), "signature present");
        assert_eq!(headers[2], (HEADER_TIMESTAMP, "1700000000000".to_string()));
    }

    #[test]
    fn rejects_invalid_pem() {
        assert!(matches!(
            KalshiCredentials::from_pem("kid", "not a valid pem"),
            Err(KalshiError::KeyParse(_))
        ));
    }

    #[test]
    fn debug_redacts_key_material() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("load");
        let dbg = format!("{creds:?}");
        assert!(dbg.contains("redacted"), "Debug must redact the key: {dbg}");
        assert!(!dbg.contains("BEGIN"), "Debug must not leak PEM");
    }
}
