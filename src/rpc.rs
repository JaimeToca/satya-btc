use crate::config::{RpcAuth, RpcConfig};
use bitcoin::{Amount, Network, Txid};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fs;

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`). `getmempoolentry` returns this when the tx
/// vanished between listing and fetch.
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

/// Maximum number of characters of a provider HTTP error body we keep. Bounds
/// the transient allocation at the source, so a multi-MB (or malicious)
/// provider error response body is never materialized in full in an error.
const MAX_ERR_BODY_LEN: usize = 512;

/// Resolved basic-auth credentials, or none (when the URL itself carries the
/// credential, e.g. a provider API key in the path).
#[derive(Clone)]
enum Auth {
    Basic { user: String, pass: String },
    None,
}

/// A cheaply-cloneable, async JSON-RPC handle to a Bitcoin Core RPC endpoint.
///
/// The underlying `reqwest::Client` is internally `Arc`'d and connection-pooled
/// (keep-alive), so `Rpc` is cheap to clone and share across concurrent fetches
/// without rebuilding a connection each time — and dropping an in-flight request
/// future truly cancels it.
#[derive(Clone)]
pub struct Rpc {
    http: reqwest::Client,
    url: String,
    auth: Auth,
    /// Additional `Name: Value` HTTP headers sent with every request (e.g. a
    /// provider API key header). Applied per request.
    headers: Vec<(String, String)>,
    /// Kept so `reconnect` can rebuild the client and re-read a rotated cookie.
    cfg: RpcConfig,
}

/// Errors from an RPC call. `Http`/`Decode` come `From` reqwest/serde_json so
/// `call` can use `?`; `Auth` and `Rpc` are surfaced explicitly.
#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("http transport error: {0}")]
    Http(#[from] reqwest::Error),
    /// HTTP 401/403 — e.g. a rotated cookie. Reconnectable.
    #[error("rpc authentication failed")]
    Auth,
    /// A non-null JSON-RPC `error` object in the response body.
    #[error("rpc error (code {code}): {message}")]
    Rpc { code: i32, message: String },
    #[error("response decode error: {0}")]
    Decode(#[from] serde_json::Error),
}

/// `getblockchaininfo` — only the fields we use.
#[derive(Deserialize)]
struct BlockchainInfo {
    /// Core reports the chain as "main"/"test"/"signet"/… which the default
    /// `Network` derive does NOT accept (it expects variant names). Map through
    /// `as_core_arg`, which uses Core's exact chain strings.
    #[serde(with = "bitcoin::network::as_core_arg")]
    chain: Network,
    blocks: u64,
}

/// `getmempoolinfo` — only the fields we use.
#[derive(Deserialize)]
pub struct MempoolInfo {
    /// Present on nodes that report load progress; absent on older nodes.
    pub loaded: Option<bool>,
    /// Minimum mempool-acceptance fee rate, denominated by Core in BTC/kvB.
    /// Parsed to exact integer sats via `as_btc` (no float rounding).
    #[serde(with = "bitcoin::amount::serde::as_btc")]
    pub mempoolminfee: Amount,
}

/// One `getmempoolentry` / `getrawmempool true` entry — only the fields we use.
#[derive(Deserialize)]
pub struct MempoolEntry {
    pub vsize: u64,
    pub weight: u64,
    pub depends: Vec<Txid>,
    pub fees: MempoolEntryFees,
    pub ancestorsize: u64,
    pub descendantsize: u64,
}

/// The `fees` sub-object of a mempool entry. Core reports each as BTC decimals;
/// `as_btc` parses to exact integer sats.
#[derive(Deserialize)]
pub struct MempoolEntryFees {
    #[serde(with = "bitcoin::amount::serde::as_btc")]
    pub base: Amount,
    #[serde(with = "bitcoin::amount::serde::as_btc")]
    pub ancestor: Amount,
    #[serde(with = "bitcoin::amount::serde::as_btc")]
    pub descendant: Amount,
}

/// The JSON-RPC response envelope: `result` XOR `error`. `result` stays untyped
/// here so the envelope is parsed once (checking `error`) before deserializing
/// the payload into the caller's `T` — keeping `call<T>` the single parse path.
#[derive(Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: serde_json::Value,
    error: Option<RpcResponseError>,
}

#[derive(Deserialize)]
struct RpcResponseError {
    code: i32,
    message: String,
}

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(cfg.timeout).build()?;
        Ok(Self {
            http,
            url: cfg.url.clone(),
            auth: resolve_auth(cfg)?,
            headers: cfg.headers.clone(),
            cfg: cfg.clone(),
        })
    }

    /// The single request/parse path — every typed method funnels through here.
    ///
    /// Builds the JSON-RPC request, POSTs it with auth + custom headers (the
    /// per-request timeout is baked into the `reqwest::Client`), maps HTTP
    /// 401/403 to `Auth` and other non-success statuses to an error, then parses
    /// the `{result, error}` envelope: a non-null `error` becomes `Rpc`,
    /// otherwise `result` is deserialized into `T`.
    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        let body = serde_json::json!({
            "jsonrpc": "1.0",
            "id": 0,
            "method": method,
            "params": params,
        });

        let mut req = self.http.post(&self.url).json(&body);
        if let Auth::Basic { user, pass } = &self.auth {
            req = req.basic_auth(user, Some(pass));
        }
        for (name, value) in &self.headers {
            req = req.header(name, value);
        }

        let resp = req.send().await?;
        let status = resp.status();
        // Auth failures never carry a useful JSON-RPC body; classify by status.
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(RpcError::Auth);
        }

        // Bitcoin Core returns method-level RPC errors (e.g. `getmempoolentry`
        // for a vanished tx, code -5) as HTTP 500 with a valid JSON-RPC error
        // BODY — so we must parse the `{result, error}` envelope regardless of
        // the HTTP status, and only treat the status as the error when the body
        // isn't a parseable envelope (a genuine gateway/transport failure).
        let bytes = resp.bytes().await?;
        match serde_json::from_slice::<RpcResponse>(&bytes) {
            Ok(parsed) => {
                if let Some(err) = parsed.error {
                    return Err(RpcError::Rpc {
                        code: err.code,
                        message: err.message,
                    });
                }
                // No error → deserialize `result` into the caller's type. A
                // missing/null result surfaces as a `Decode` error, not a panic.
                Ok(serde_json::from_value(parsed.result)?)
            }
            // Body isn't a JSON-RPC envelope. On a non-success status this is a
            // real HTTP failure (surface it, with a bounded body snippet); on a
            // 2xx it's a genuine decode error.
            Err(decode_err) => {
                if !status.is_success() {
                    let body = truncate(&String::from_utf8_lossy(&bytes), MAX_ERR_BODY_LEN);
                    Err(RpcError::Rpc {
                        code: status.as_u16() as i32,
                        message: format!("http {status}: {body}"),
                    })
                } else {
                    Err(RpcError::Decode(decode_err))
                }
            }
        }
    }

    pub async fn network(&self) -> Result<Network, RpcError> {
        Ok(self
            .call::<BlockchainInfo>("getblockchaininfo", serde_json::json!([]))
            .await?
            .chain)
    }

    pub async fn tip_height(&self) -> Result<u64, RpcError> {
        Ok(self
            .call::<BlockchainInfo>("getblockchaininfo", serde_json::json!([]))
            .await?
            .blocks)
    }

    pub async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        self.call("getmempoolinfo", serde_json::json!([])).await
    }

    pub async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        self.call("getrawmempool", serde_json::json!([false])).await
    }

    pub async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        let map: std::collections::HashMap<Txid, MempoolEntry> =
            self.call("getrawmempool", serde_json::json!([true])).await?;
        Ok(map.into_iter().collect())
    }

    /// Fetch a single mempool entry. `Ok(None)` if the tx disappeared between
    /// listing and fetch (the node returns `RPC_INVALID_ADDRESS_OR_KEY`, -5).
    pub async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        match self
            .call("getmempoolentry", serde_json::json!([txid.to_string()]))
            .await
        {
            Ok(entry) => Ok(Some(entry)),
            Err(RpcError::Rpc {
                code: RPC_INVALID_ADDRESS_OR_KEY,
                ..
            }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Rebuild the underlying client (re-reading the cookie file, in case it
    /// rotated). Called at the loop level after a reconnectable (auth/transport)
    /// error.
    pub fn reconnect(&mut self) -> anyhow::Result<()> {
        let http = reqwest::Client::builder()
            .timeout(self.cfg.timeout)
            .build()?;
        self.http = http;
        self.auth = resolve_auth(&self.cfg)?;
        Ok(())
    }
}

/// Resolve basic-auth credentials from config: cookie file (read + parse
/// `user:password` now), user/pass (direct), or none (URL carries credential).
///
/// The cookie is read at connect/reconnect time so a rotated cookie is picked
/// up on reconnect.
fn resolve_auth(cfg: &RpcConfig) -> anyhow::Result<Auth> {
    match &cfg.auth {
        Some(RpcAuth::Cookie(path)) => {
            let contents = fs::read_to_string(path)?;
            let line = contents
                .lines()
                .next()
                .ok_or_else(|| anyhow::anyhow!("cookie file {} is empty", path.display()))?;
            let colon = line
                .find(':')
                .ok_or_else(|| anyhow::anyhow!("cookie file {} is malformed", path.display()))?;
            Ok(Auth::Basic {
                user: line[..colon].to_string(),
                pass: line[colon + 1..].to_string(),
            })
        }
        Some(RpcAuth::UserPass(user, pass)) => Ok(Auth::Basic {
            user: user.clone(),
            pass: pass.clone(),
        }),
        None => Ok(Auth::None),
    }
}

/// Truncate `s` to at most `max` chars (char boundary safe), appending `…` when
/// truncated. Bounds error-body allocation.
fn truncate(s: &str, max: usize) -> String {
    let mut chars = s.chars();
    let mut out: String = chars.by_ref().take(max).collect();
    if chars.next().is_some() {
        out.push('…');
    }
    out
}

/// Whether an error looks like an auth failure (e.g. rotated cookie) or a
/// connect/timeout transport problem, either of which warrants rebuilding the
/// client and retrying.
pub fn is_reconnectable(err: &RpcError) -> bool {
    match err {
        RpcError::Auth => true,
        RpcError::Http(e) => e.is_connect() || e.is_timeout(),
        _ => false,
    }
}
