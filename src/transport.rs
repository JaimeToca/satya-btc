use std::fmt;
use std::fs;
use std::time::Duration;

use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::jsonrpc::{Request, Response, Transport};
use bitcoincore_rpc::Client;
use crate::config::{RpcAuth, RpcConfig};

/// Build a Bitcoin Core RPC client through a timeout-capable transport.
///
/// Uses a small custom [`Transport`] (see [`HeaderTransport`]) backed by `minreq` rather than
/// the `jsonrpc` crate's own `minreq_http` transport, because `minreq_http::Builder` only
/// exposes `basic_auth`/`cookie_auth` and has no way to attach arbitrary headers (verified
/// against the vendored `jsonrpc 0.18.0` source: `jsonrpc::http::minreq_http::Builder` has no
/// header-setting method). `minreq` itself (with rustls-based HTTPS support enabled via the
/// `minreq/https` feature) works for both `http://` and `https://` RPC URLs -- needed for
/// HTTPS providers such as GetBlock, whose auth token lives in the URL path rather than in
/// basic auth, and now also for providers that authenticate via a custom header (e.g.
/// `X-Api-Key: abc123`).
pub(crate) fn build_client(cfg: &RpcConfig) -> anyhow::Result<Client> {
    let user_pass = match &cfg.auth {
        Some(RpcAuth::Cookie(path)) => {
            let contents = fs::read_to_string(path)?;
            let line = contents.lines().next().ok_or_else(|| {
                anyhow::anyhow!("cookie file {} is empty", path.display())
            })?;
            let colon = line
                .find(':')
                .ok_or_else(|| anyhow::anyhow!("cookie file {} is malformed", path.display()))?;
            Some((line[..colon].to_string(), line[colon + 1..].to_string()))
        }
        Some(RpcAuth::UserPass(user, pass)) => Some((user.clone(), pass.clone())),
        // No cookie/user/pass configured: assume the URL itself carries the credential
        // (e.g. a provider API key in the path), so skip basic auth entirely.
        None => None,
    };

    let transport = HeaderTransport {
        url: cfg.url.clone(),
        timeout: cfg.timeout,
        basic_auth: user_pass.map(|(user, pass)| basic_auth_header(&user, &pass)),
        headers: cfg.headers.clone(),
    };
    let jsonrpc_client = jsonrpc::client::Client::with_transport(transport);
    Ok(Client::from_jsonrpc(jsonrpc_client))
}

/// Builds the value of an HTTP `Authorization: Basic ...` header from a user/pass pair,
/// matching the encoding `jsonrpc::minreq_http::Builder::basic_auth` uses internally.
fn basic_auth_header(user: &str, pass: &str) -> String {
    let mut s = user.to_string();
    s.push(':');
    s.push_str(pass);
    format!("Basic {}", base64::encode(s.as_bytes()))
}

/// A minimal [`jsonrpc::Transport`] backed directly by `minreq`, supporting arbitrary extra
/// HTTP headers in addition to (or instead of) HTTP basic auth. Mirrors the request/response
/// handling of `jsonrpc::http::minreq_http::MinreqHttpTransport`, whose behavior (timeout,
/// JSON body, error mapping) this preserves so `is_reconnectable` in this module keeps working
/// unchanged: any failure here is wrapped as `jsonrpc::Error::Transport(_)`, same as upstream.
#[derive(Clone, Debug)]
struct HeaderTransport {
    url: String,
    timeout: Duration,
    /// Pre-built `Authorization` header value (e.g. `"Basic <base64>"`), if basic auth is in
    /// use.
    basic_auth: Option<String>,
    /// Additional `Name: Value` headers to send with every request.
    headers: Vec<(String, String)>,
}

impl HeaderTransport {
    fn request<R>(&self, body: impl serde::Serialize) -> Result<R, HeaderTransportError>
    where
        R: for<'a> serde::de::Deserialize<'a>,
    {
        let mut req = minreq::Request::new(minreq::Method::Post, &self.url)
            .with_timeout(self.timeout.as_secs());
        if let Some(auth) = &self.basic_auth {
            req = req.with_header("Authorization", auth);
        }
        for (name, value) in &self.headers {
            req = req.with_header(name, value);
        }
        let req = req.with_json(&body)?;

        // Send the request and parse the response. If the response is an error that does not
        // contain valid JSON in its body, return the raw HTTP error so callers can match
        // against it -- mirrors `minreq_http::MinreqHttpTransport::request`.
        let resp = req.send()?;
        match resp.json() {
            Ok(json) => Ok(json),
            Err(minreq_err) => {
                if resp.status_code != 200 {
                    let raw = resp.as_str().unwrap_or("");
                    let mut chars = raw.chars();
                    let mut body: String = chars.by_ref().take(MAX_ERR_BODY_LEN).collect();
                    if chars.next().is_some() {
                        body.push('…');
                    }
                    Err(HeaderTransportError::Http {
                        status_code: resp.status_code,
                        body,
                    })
                } else {
                    Err(HeaderTransportError::Minreq(minreq_err))
                }
            }
        }
    }
}

impl Transport for HeaderTransport {
    fn send_request(&self, req: Request) -> Result<Response, jsonrpc::Error> {
        Ok(self.request(req)?)
    }

    fn send_batch(&self, reqs: &[Request]) -> Result<Vec<Response>, jsonrpc::Error> {
        Ok(self.request(reqs)?)
    }

    fn fmt_target(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.url)
    }
}

/// Maximum number of characters of a provider HTTP error body we keep. Bounds
/// the transient allocation at the source, so a multi-MB (or malicious)
/// provider error response body is never materialized in full.
const MAX_ERR_BODY_LEN: usize = 512;

/// Error type for [`HeaderTransport`], analogous to `jsonrpc::http::minreq_http::Error`.
#[derive(Debug)]
enum HeaderTransportError {
    Json(serde_json::Error),
    Minreq(minreq::Error),
    Http { status_code: i32, body: String },
}

impl fmt::Display for HeaderTransportError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HeaderTransportError::Json(e) => write!(f, "parsing JSON failed: {e}"),
            HeaderTransportError::Minreq(e) => write!(f, "minreq: {e}"),
            HeaderTransportError::Http { status_code, body } => {
                write!(f, "http (status: {status_code}, body: {body})")
            }
        }
    }
}

impl std::error::Error for HeaderTransportError {}

impl From<serde_json::Error> for HeaderTransportError {
    fn from(e: serde_json::Error) -> Self {
        HeaderTransportError::Json(e)
    }
}

impl From<minreq::Error> for HeaderTransportError {
    fn from(e: minreq::Error) -> Self {
        HeaderTransportError::Minreq(e)
    }
}

/// Maps our transport error into `jsonrpc::Error`, matching how upstream's `minreq_http`
/// converts its own error type: JSON errors become `Error::Json`, everything else becomes
/// `Error::Transport(_)` so `is_reconnectable` (which matches on that variant) keeps working.
impl From<HeaderTransportError> for jsonrpc::Error {
    fn from(e: HeaderTransportError) -> jsonrpc::Error {
        match e {
            HeaderTransportError::Json(e) => jsonrpc::Error::Json(e),
            e => jsonrpc::Error::Transport(Box::new(e)),
        }
    }
}
