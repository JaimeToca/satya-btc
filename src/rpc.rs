use bitcoincore_rpc::bitcoin::{Network, Txid};
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::{json::GetMempoolEntryResult, Auth, Client, RpcApi};
use crate::config::{RpcAuth, RpcConfig};
use crate::mempool::MempoolTx;

pub struct Rpc {
    client: bitcoincore_rpc::Client,
}

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`).
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        let auth = match &cfg.auth {
            RpcAuth::Cookie(path) => Auth::CookieFile(path.clone()),
            RpcAuth::UserPass(user, pass) => Auth::UserPass(user.clone(), pass.clone()),
        };
        let client = Client::new(&cfg.url, auth)?;
        Ok(Self { client })
    }

    pub fn network(&self) -> anyhow::Result<Network> {
        Ok(self.client.get_blockchain_info()?.chain)
    }

    pub fn tip_height(&self) -> anyhow::Result<u64> {
        Ok(self.client.get_blockchain_info()?.blocks)
    }

    pub fn mempool_loaded(&self) -> anyhow::Result<bool> {
        // Older nodes don't report `loaded` at all; treat that as loaded.
        Ok(self.client.get_mempool_info()?.loaded.unwrap_or(true))
    }

    pub fn mempool_min_fee_sat_vb(&self) -> anyhow::Result<f64> {
        let info = self.client.get_mempool_info()?;
        Ok(info.mempool_min_fee.to_sat() as f64 / 1000.0)
    }

    pub fn raw_mempool_txids(&self) -> anyhow::Result<Vec<Txid>> {
        Ok(self.client.get_raw_mempool()?)
    }

    pub fn raw_mempool_verbose(&self) -> anyhow::Result<Vec<(Txid, MempoolTx)>> {
        let entries = self.client.get_raw_mempool_verbose()?;
        Ok(entries
            .into_iter()
            .map(|(txid, entry)| {
                let tx = entry_to_tx(&entry);
                (txid, tx)
            })
            .collect())
    }

    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub fn mempool_entry(&self, txid: &Txid) -> anyhow::Result<Option<MempoolTx>> {
        match self.client.get_mempool_entry(txid) {
            Ok(entry) => Ok(Some(entry_to_tx(&entry))),
            Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Rpc(rpc_err)))
                if rpc_err.code == RPC_INVALID_ADDRESS_OR_KEY =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
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
