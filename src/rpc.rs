use bitcoincore_rpc::bitcoin::{Network, Txid};
use crate::config::RpcConfig;
use crate::mempool::MempoolTx;

pub struct Rpc {
    client: bitcoincore_rpc::Client,
}

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> { unimplemented!() } // T3
    pub fn network(&self) -> anyhow::Result<Network> { unimplemented!() }        // T3
    pub fn tip_height(&self) -> anyhow::Result<u64> { unimplemented!() }         // T3
    pub fn mempool_loaded(&self) -> anyhow::Result<bool> { unimplemented!() }    // T3
    pub fn mempool_min_fee_sat_vb(&self) -> anyhow::Result<f64> { unimplemented!() } // T3
    pub fn raw_mempool_txids(&self) -> anyhow::Result<Vec<Txid>> { unimplemented!() } // T3
    pub fn raw_mempool_verbose(&self) -> anyhow::Result<Vec<(Txid, MempoolTx)>> { unimplemented!() } // T3
    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub fn mempool_entry(&self, txid: &Txid) -> anyhow::Result<Option<MempoolTx>> { unimplemented!() } // T3
}
