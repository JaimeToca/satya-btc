use crate::config::{RpcAuth, RpcConfig};
use bitcoin::{Amount, Network, Txid};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::fs;

/// Per-call response-body caps. `resp.bytes()` would materialize the whole body
/// with no bound, so an untrusted provider could OOM us; instead `call` streams
/// the body and enforces one of these limits. The tiers reflect expected sizes:
/// small control responses, a bare-txid list, and the (much larger) verbose
/// mempool dump.
const MAX_BODY_CONTROL: usize = 16 * 1024 * 1024; // 16 MiB
const MAX_BODY_TXIDS: usize = 64 * 1024 * 1024; // 64 MiB
const MAX_BODY_VERBOSE: usize = 512 * 1024 * 1024; // 512 MiB

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`). `getmempoolentry` returns this when the tx
/// vanished between listing and fetch.
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

/// Maximum number of characters of a provider HTTP error body we keep in an
/// error string. This bounds only the error *message*; the body itself is
/// already bounded during the streaming read by the per-call `max_body` cap
/// (see `call`), so this is a further truncation for log/error ergonomics.
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
    /// Kept so `reconnect` can rebuild the client and re-read a rotated cookie.
    cfg: RpcConfig,
}

/// Errors from an RPC call. `Decode` comes `From` serde_json so `call` can use
/// `?`; `Http`, `Auth`, and `Rpc` are surfaced explicitly. `Http` is built via
/// `reqwest::Error::without_url()` so a path-embedded provider token (e.g.
/// `https://host/<KEY>`, which reqwest does NOT redact) never reaches the logs.
#[derive(thiserror::Error, Debug)]
pub enum RpcError {
    #[error("http transport error: {0}")]
    Http(#[source] reqwest::Error),
    /// HTTP 401/403 — e.g. a rotated cookie. Reconnectable.
    #[error("rpc authentication failed")]
    Auth,
    /// A non-2xx HTTP status whose body is NOT a parseable JSON-RPC envelope —
    /// e.g. a gateway/WAF 5xx or 4xx. Kept distinct from `Rpc` so JSON-RPC
    /// method codes (negative, like -5) never collide with HTTP statuses.
    #[error("http {status}: {body}")]
    HttpStatus { status: u16, body: String },
    /// A non-null JSON-RPC `error` object in the response body.
    #[error("rpc error (code {code}): {message}")]
    Rpc { code: i32, message: String },
    /// The response body exceeded the per-call cap. NOT reconnectable —
    /// rebuilding the client won't shrink an abusive body.
    #[error("response body exceeded {limit} bytes")]
    BodyTooLarge { limit: usize },
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
    /// Parsed via `as_btc` to integer sats — exact for every real sat value
    /// (≤ 21M BTC fits f64's mantissa); identical to the prior bitcoincore-rpc path.
    #[serde(with = "bitcoin::amount::serde::as_btc")]
    pub mempoolminfee: Amount,
}

/// One `getmempoolentry` / `getrawmempool true` entry — only the fields we use.
#[derive(Deserialize)]
pub struct MempoolEntry {
    /// Older Core / alt stacks emit this as `size`; accept both.
    #[serde(alias = "size")]
    pub vsize: u64,
    /// Added in Core v0.19; absent on older nodes. Optional so a single missing
    /// `weight` doesn't fail the WHOLE verbose-mempool HashMap decode (which
    /// would trigger a bulk-resync death spiral). The `From` impl falls back to
    /// `vsize * 4`.
    pub weight: Option<u64>,
    pub depends: Vec<Txid>,
    pub fees: MempoolEntryFees,
    pub ancestorsize: u64,
    pub descendantsize: u64,
}

/// The `fees` sub-object of a mempool entry. Core reports each as BTC decimals;
/// `as_btc` parses BTC decimals to integer sats (exact; same as the old path).
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
    #[serde(default)]
    error: Option<RpcResponseError>,
}

#[derive(Deserialize)]
struct RpcResponseError {
    code: i32,
    message: String,
}

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        // Disable redirects: an untrusted RPC provider must not be able to
        // redirect us (and our credentials) to another host.
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            http,
            url: cfg.url.clone(),
            auth: resolve_auth(cfg)?,
            cfg: cfg.clone(),
        })
    }

    /// The single request/parse path — every typed method funnels through here.
    ///
    /// Builds the JSON-RPC request, POSTs it with auth (the
    /// per-request timeout is baked into the `reqwest::Client`), maps HTTP
    /// 401/403 to `Auth` and other non-success statuses to `HttpStatus`, then
    /// parses the `{result, error}` envelope: a non-null `error` becomes `Rpc`,
    /// otherwise `result` is deserialized into `T`.
    ///
    /// `max_body` bounds the response body: the body is read as a bounded
    /// stream (never `resp.bytes()`, which would materialize the whole thing),
    /// so an untrusted provider can't OOM us.
    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
        max_body: usize,
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

        let resp = req
            .send()
            .await
            .map_err(|e| RpcError::Http(e.without_url()))?;
        let status = resp.status();
        // Auth failures never carry a useful JSON-RPC body; classify by status.
        // Read a bounded snippet of the body first so a WAF/rate-limit 403 is
        // diagnosable (the body read is itself capped by MAX_BODY_CONTROL).
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            let snippet = match read_body_bounded(resp, MAX_BODY_CONTROL).await {
                Ok(bytes) => truncate(&String::from_utf8_lossy(&bytes), MAX_ERR_BODY_LEN),
                Err(_) => String::new(),
            };
            tracing::warn!(status = %status, body = %snippet, "rpc auth failure");
            return Err(RpcError::Auth);
        }

        // Bitcoin Core returns method-level RPC errors (e.g. `getmempoolentry`
        // for a vanished tx, code -5) as HTTP 500 with a valid JSON-RPC error
        // BODY — so we must parse the `{result, error}` envelope regardless of
        // the HTTP status, and only treat the status as the error when the body
        // isn't a parseable envelope (a genuine gateway/transport failure).
        let bytes = read_body_bounded(resp, max_body).await?;
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
            // real HTTP failure (surface it as `HttpStatus`, with a bounded body
            // snippet); on a 2xx it's a genuine decode error.
            Err(decode_err) => {
                if !status.is_success() {
                    Err(RpcError::HttpStatus {
                        status: status.as_u16(),
                        body: truncate(&String::from_utf8_lossy(&bytes), MAX_ERR_BODY_LEN),
                    })
                } else {
                    Err(RpcError::Decode(decode_err))
                }
            }
        }
    }

    pub async fn network(&self) -> Result<Network, RpcError> {
        Ok(self
            .call::<BlockchainInfo>("getblockchaininfo", serde_json::json!([]), MAX_BODY_CONTROL)
            .await?
            .chain)
    }

    pub async fn tip_height(&self) -> Result<u64, RpcError> {
        Ok(self
            .call::<BlockchainInfo>("getblockchaininfo", serde_json::json!([]), MAX_BODY_CONTROL)
            .await?
            .blocks)
    }

    pub async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        self.call("getmempoolinfo", serde_json::json!([]), MAX_BODY_CONTROL)
            .await
    }

    pub async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        self.call("getrawmempool", serde_json::json!([false]), MAX_BODY_TXIDS)
            .await
    }

    pub async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        let map: std::collections::HashMap<Txid, MempoolEntry> = self
            .call("getrawmempool", serde_json::json!([true]), MAX_BODY_VERBOSE)
            .await?;
        Ok(map.into_iter().collect())
    }

    /// Fetch a single mempool entry. `Ok(None)` if the tx disappeared between
    /// listing and fetch (the node returns `RPC_INVALID_ADDRESS_OR_KEY`, -5).
    pub async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        match self
            .call(
                "getmempoolentry",
                serde_json::json!([txid.to_string()]),
                MAX_BODY_CONTROL,
            )
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
        // Build both the new auth and the new client into locals first, and only
        // assign to `self` once BOTH succeed — so a cookie-read failure can't
        // leave us half-updated (fresh client, stale auth).
        let auth = resolve_auth(&self.cfg)?;
        let http = reqwest::Client::builder()
            .timeout(self.cfg.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        self.http = http;
        self.auth = auth;
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

/// Read a response body as a bounded stream, enforcing `max_body` both from the
/// declared `Content-Length` (preflight) and during streaming (Content-Length
/// can lie), so an untrusted provider can't OOM us. Returns `BodyTooLarge` if
/// the body exceeds the cap, before allocating past it.
async fn read_body_bounded(resp: reqwest::Response, max_body: usize) -> Result<Vec<u8>, RpcError> {
    // Preflight: reject an oversize declared length before reading a byte.
    if let Some(len) = resp.content_length() {
        if len > max_body as u64 {
            return Err(RpcError::BodyTooLarge { limit: max_body });
        }
    }
    let mut resp = resp;
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| RpcError::Http(e.without_url()))?
    {
        // Enforce during streaming too, before extending past the cap.
        if buf.len() + chunk.len() > max_body {
            return Err(RpcError::BodyTooLarge { limit: max_body });
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
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

/// Whether an error warrants rebuilding the (possibly-stale keep-alive) client
/// and retrying: any transport error (`Http`), an auth failure (e.g. rotated
/// cookie), or an HTTP status (e.g. a gateway 5xx). A real JSON-RPC method
/// error (`Rpc`, including -5), a `Decode` error, and `BodyTooLarge` are NOT
/// reconnectable — rebuilding the pool wouldn't change the outcome.
pub fn is_reconnectable(err: &RpcError) -> bool {
    matches!(
        err,
        RpcError::Auth | RpcError::Http(_) | RpcError::HttpStatus { .. }
    )
}

/// The RPC surface the sync loop consumes. Implemented by the real reqwest
/// `Rpc` (production) and by the simulation `MockNode` / `SimulatedRpc` (tests).
/// Native async fn in trait (stable since 1.75) — no `async_trait` macro.
pub trait MempoolRpc {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError>;
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError>;
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError>;
    async fn tip_height(&self) -> Result<u64, RpcError>;
    fn reconnect(&mut self) -> anyhow::Result<()>;
}

impl MempoolRpc for Rpc {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        Rpc::mempool_info(self).await
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        Rpc::raw_mempool_txids(self).await
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        Rpc::raw_mempool_verbose(self).await
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        Rpc::mempool_entry(self, txid).await
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        Rpc::tip_height(self).await
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        Rpc::reconnect(self)
    }
}
