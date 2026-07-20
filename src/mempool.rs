use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use bitcoincore_rpc::bitcoin::{Amount, Network, Txid};

#[derive(Debug, Clone)]
pub struct MempoolTx {
    pub vsize: u32,
    pub weight: u32,
    pub fee: Amount,
    pub depends: Vec<Txid>,
}

#[derive(Debug)]
pub struct MempoolState {
    pub txs: HashMap<Txid, MempoolTx>,
    pub mempool_min_fee_sat_vb: f64,
    pub tip_height: u64,
    pub network: Network,
    pub caught_up: bool,
}

impl MempoolState {
    pub fn new(network: Network) -> Self {
        Self { txs: HashMap::new(), mempool_min_fee_sat_vb: 0.0, tip_height: 0, network, caught_up: false }
    }
}

pub type SharedState = Arc<RwLock<MempoolState>>;

/// Result of diffing the node's current txid set against our cache.
pub struct Diff {
    pub new: Vec<Txid>,   // present at node, absent from cache -> fetch details
    pub gone: Vec<Txid>,  // present in cache, absent at node -> remove
}

pub fn compute_diff(cache: &HashMap<Txid, MempoolTx>, node_txids: &HashSet<Txid>) -> Diff {
    unimplemented!() // T4
}

/// Insert freshly-fetched txs and remove departed ones.
pub fn apply(state: &mut MempoolState, gone: &[Txid], fetched: Vec<(Txid, MempoolTx)>) {
    unimplemented!() // T4
}
