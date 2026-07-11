//! Build the authenticated WebSocket upgrade request.
//!
//! Kalshi requires the RSA-PSS auth headers on the WS upgrade even for public
//! channels (see [`crate::auth`]). This turns the base [`KALSHI_WS_URL`] into a
//! client request carrying `KALSHI-ACCESS-{KEY,SIGNATURE,TIMESTAMP}` signed over
//! `ts_ms + "GET" + WS_PATH`.

use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};
use tracing::instrument;

use crate::auth::KalshiCredentials;
use crate::KalshiError;

/// Build the authenticated upgrade request for `url`.
///
/// The RSA-PSS signature is computed over `GET` + the URL's path, fresh each call
/// (current timestamp), so build the request immediately before connecting to
/// avoid timestamp skew. `url` is a parameter (not hard-coded) so a mock server
/// can be targeted in tests; production passes [`crate::ws::KALSHI_WS_URL`].
#[instrument(skip(creds))]
pub fn authenticated_request(creds: &KalshiCredentials, url: &str) -> Result<Request, KalshiError> {
    let mut request = url
        .into_client_request()
        .map_err(|e| KalshiError::WebSocket(format!("building upgrade request: {e}")))?;

    // Sign over the actual request path (derived from the URL), so a mock URL with
    // a different path still produces a self-consistent signature.
    let path = request.uri().path().to_string();
    for (name, value) in creds.signed_headers("GET", &path)? {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| KalshiError::WebSocket(format!("invalid header name {name}: {e}")))?;
        let header_value = HeaderValue::from_str(&value)
            .map_err(|e| KalshiError::WebSocket(format!("invalid header value: {e}")))?;
        request.headers_mut().insert(header_name, header_value);
    }

    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;

    // A throwaway PKCS#8 test key (not a credential) — same one used in auth tests.
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

    #[test]
    fn request_carries_the_three_auth_headers() {
        let creds = KalshiCredentials::from_pem("test-key-id", TEST_KEY_PKCS8).expect("creds");
        let request = authenticated_request(&creds, crate::ws::KALSHI_WS_URL).expect("request");
        let headers = request.headers();
        assert_eq!(
            headers
                .get("KALSHI-ACCESS-KEY")
                .and_then(|v| v.to_str().ok()),
            Some("test-key-id")
        );
        assert!(headers.contains_key("KALSHI-ACCESS-SIGNATURE"));
        assert!(headers.contains_key("KALSHI-ACCESS-TIMESTAMP"));
    }

    #[test]
    fn request_targets_the_given_ws_url() {
        let creds = KalshiCredentials::from_pem("kid", TEST_KEY_PKCS8).expect("creds");
        let request = authenticated_request(&creds, crate::ws::KALSHI_WS_URL).expect("request");
        assert_eq!(request.uri().host(), Some("api.elections.kalshi.com"));
        assert_eq!(request.uri().path(), crate::ws::WS_PATH);
    }
}
