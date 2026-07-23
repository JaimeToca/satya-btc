use crate::rpc::{MempoolEntry, MempoolInfo};
use bitcoin::{Amount, Network, Txid};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

// The cache stores full per-tx package data, but no consumer reads these fields
// yet — the GBT fee estimator (`/fees`) that will read them is not built. The
// fields are populated and carried deliberately as its data substrate (and are
// exercised by the simulation harness), so silence the write-only `dead_code`
// lint here rather than dropping data we're about to need.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MempoolTx {
    pub vsize: u32,
    pub weight: u32,
    pub fee: Amount,
    pub depends: Vec<Txid>,
    // NOTE: the `ancestor_*`/`descendant_*` package fields below are a SNAPSHOT
    // taken at the moment this tx was fetched from the node via
    // `getmempoolentry`. They are NOT refreshed as related txs later enter or
    // leave the mempool, so once a tx is cached its package totals can drift
    // from the node's live view (e.g. a new child raises the real descendant
    // fee/size but this cached copy stays put). Phase-3 GBT must recompute /
    // refresh package data (ancestor/descendant fee and size) from a fresh
    // source rather than trust these cached values as live.
    pub ancestor_fee: Amount,
    pub ancestor_vsize: u32,
    pub descendant_fee: Amount,
    pub descendant_vsize: u32,
}

impl From<&MempoolEntry> for MempoolTx {
    /// Convert a raw `getmempoolentry`/`getrawmempool(true)` entry into our `MempoolTx`.
    fn from(entry: &MempoolEntry) -> Self {
        // Saturating u32 casts: these fields cross an untrusted RPC boundary, so
        // clamp rather than silently wrap on an absurd value.
        let vsize = u32::try_from(entry.vsize).unwrap_or(u32::MAX);
        MempoolTx {
            vsize,
            // `weight` was added in Core v0.19; when absent, fall back to the
            // pre-segwit-style `vsize * 4` so a missing field doesn't fail the
            // whole verbose-mempool decode.
            weight: entry
                .weight
                .map(|w| u32::try_from(w).unwrap_or(u32::MAX))
                .unwrap_or_else(|| vsize.saturating_mul(4)),
            fee: entry.fees.base,
            depends: entry.depends.clone(),
            ancestor_fee: entry.fees.ancestor,
            ancestor_vsize: u32::try_from(entry.ancestorsize).unwrap_or(u32::MAX),
            descendant_fee: entry.fees.descendant,
            descendant_vsize: u32::try_from(entry.descendantsize).unwrap_or(u32::MAX),
        }
    }
}

/// Minimum mempool-acceptance fee rate, in sat/vB.
///
/// `getmempoolinfo`'s `mempoolminfee` is denominated in BTC/kvB; the `as_btc`
/// serde parse already gave us exact sats, and dividing by 1000 converts kvB to
/// vB, giving sat/vB.
pub fn min_fee_sat_vb(info: &MempoolInfo) -> f64 {
    info.mempoolminfee.to_sat() as f64 / 1000.0
}

#[derive(Debug)]
pub struct MempoolState {
    pub txs: HashMap<Txid, MempoolTx>,
    pub mempool_min_fee_sat_vb: f64,
    pub tip_height: u64,
    pub network: Network,
    pub caught_up: bool,
    pub last_sync_ok: Option<SystemTime>,
}

impl MempoolState {
    pub fn new(network: Network) -> Self {
        Self {
            txs: HashMap::new(),
            mempool_min_fee_sat_vb: 0.0,
            tip_height: 0,
            network,
            caught_up: false,
            last_sync_ok: None,
        }
    }
}

pub type SharedState = Arc<RwLock<MempoolState>>;

/// Acquire the write lock, tolerating a poisoned lock (recovering the inner
/// state rather than propagating the panic that poisoned it).
pub fn write_state(state: &SharedState) -> RwLockWriteGuard<'_, MempoolState> {
    state.write().unwrap_or_else(|p| p.into_inner())
}

/// Acquire the read lock, tolerating a poisoned lock.
pub fn read_state(state: &SharedState) -> RwLockReadGuard<'_, MempoolState> {
    state.read().unwrap_or_else(|p| p.into_inner())
}

/// Result of diffing the node's current txid set against our cache.
pub struct Diff {
    pub new: Vec<Txid>,  // present at node, absent from cache -> fetch details
    pub gone: Vec<Txid>, // present in cache, absent at node -> remove
}

pub fn compute_diff(cache_keys: &HashSet<Txid>, node_txids: &HashSet<Txid>) -> Diff {
    let new = node_txids.difference(cache_keys).copied().collect();
    let gone = cache_keys.difference(node_txids).copied().collect();
    Diff { new, gone }
}
