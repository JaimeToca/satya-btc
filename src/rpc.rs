use std::fmt;
use std::fs;
use std::time::Duration;

use bitcoincore_rpc::bitcoin::{Network, Txid};
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::jsonrpc::{Request, Response, Transport};
use bitcoincore_rpc::{json::GetMempoolEntryResult, Client, RpcApi};
use crate::config::{RpcAuth, RpcConfig};
use crate::mempool::MempoolTx;

pub struct Rpc {
    client: Client,
    cfg: RpcConfig,
}

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`).
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

/// Combined mempool status from a single `getmempoolinfo` call.
pub struct MempoolInfo {
    pub loaded: bool,
    pub min_fee_sat_vb: f64,
}

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        let client = build_client(cfg)?;
        Ok(Self { client, cfg: cfg.clone() })
    }

    pub fn network(&mut self) -> anyhow::Result<Network> {
        self.with_reconnect(|c| Ok(c.get_blockchain_info()?.chain))
    }

    pub fn tip_height(&mut self) -> anyhow::Result<u64> {
        self.with_reconnect(|c| Ok(c.get_blockchain_info()?.blocks))
    }

    /// Combined mempool status from a single `getmempoolinfo` call.
    pub fn mempool_info(&mut self) -> anyhow::Result<MempoolInfo> {
        self.with_reconnect(|c| {
            let info = c.get_mempool_info()?;
            // Older nodes don't report `loaded` at all; treat that as loaded.
            let loaded = info.loaded.unwrap_or(true);
            let min_fee_sat_vb = info.mempool_min_fee.to_sat() as f64 / 1000.0;
            Ok(MempoolInfo { loaded, min_fee_sat_vb })
        })
    }

    pub fn raw_mempool_txids(&mut self) -> anyhow::Result<Vec<Txid>> {
        self.with_reconnect(|c| Ok(c.get_raw_mempool()?))
    }

    pub fn raw_mempool_verbose(&mut self) -> anyhow::Result<Vec<(Txid, MempoolTx)>> {
        self.with_reconnect(|c| {
            let entries = c.get_raw_mempool_verbose()?;
            Ok(entries
                .into_iter()
                .map(|(txid, entry)| {
                    let tx = entry_to_tx(&entry);
                    (txid, tx)
                })
                .collect())
        })
    }

    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub fn mempool_entry(&mut self, txid: &Txid) -> anyhow::Result<Option<MempoolTx>> {
        self.with_reconnect(|c| match c.get_mempool_entry(txid) {
            Ok(entry) => Ok(Some(entry_to_tx(&entry))),
            Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Rpc(rpc_err)))
                if rpc_err.code == RPC_INVALID_ADDRESS_OR_KEY =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        })
    }

    /// Runs `f` against the current client. On an auth or transport-level error, rebuilds the
    /// client once (re-reading the cookie file, in case it rotated) and retries `f` a single
    /// time before giving up.
    fn with_reconnect<T>(
        &mut self,
        f: impl Fn(&Client) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match f(&self.client) {
            Ok(v) => Ok(v),
            Err(e) if is_reconnectable(&e) => {
                self.client = build_client(&self.cfg)?;
                f(&self.client)
            }
            Err(e) => Err(e),
        }
    }
}

/// Whether an error looks like an auth failure (e.g. rotated cookie) or a transport-level
/// problem, either of which warrants rebuilding the client and retrying once.
fn is_reconnectable(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<bitcoincore_rpc::Error>(),
        Some(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Transport(_)))
            | Some(bitcoincore_rpc::Error::Io(_))
    )
}

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
fn build_client(cfg: &RpcConfig) -> anyhow::Result<Client> {
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
                    Err(HeaderTransportError::Http {
                        status_code: resp.status_code,
                        body: resp.as_str().unwrap_or("").to_string(),
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

/// Convert a raw `getmempoolentry`/`getrawmempool(true)` result into our `MempoolTx`.
fn entry_to_tx(entry: &GetMempoolEntryResult) -> MempoolTx {
    let vsize = entry.vsize as u32;
    let weight = entry.weight.map(|w| w as u32).unwrap_or(vsize * 4);
    MempoolTx {
        vsize,
        weight,
        fee: entry.fees.base,
        depends: entry.depends.clone(),
    }
}
