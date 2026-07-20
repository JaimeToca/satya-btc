use std::fs;

use bitcoincore_rpc::bitcoin::{Network, Txid};
use bitcoincore_rpc::jsonrpc;
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
fn build_client(cfg: &RpcConfig) -> anyhow::Result<Client> {
    let (user, pass) = match &cfg.auth {
        RpcAuth::Cookie(path) => {
            let contents = fs::read_to_string(path)?;
            let line = contents.lines().next().ok_or_else(|| {
                anyhow::anyhow!("cookie file {} is empty", path.display())
            })?;
            let colon = line
                .find(':')
                .ok_or_else(|| anyhow::anyhow!("cookie file {} is malformed", path.display()))?;
            (line[..colon].to_string(), line[colon + 1..].to_string())
        }
        RpcAuth::UserPass(user, pass) => (user.clone(), pass.clone()),
    };

    let transport = jsonrpc::simple_http::Builder::new()
        .url(&cfg.url)?
        .auth(user, Some(pass))
        .timeout(cfg.timeout)
        .build();
    let jsonrpc_client = jsonrpc::client::Client::with_transport(transport);
    Ok(Client::from_jsonrpc(jsonrpc_client))
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
