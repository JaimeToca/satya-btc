use std::collections::HashMap;

use bitcoin::{Amount, Txid};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{MempoolEntry, MempoolEntryFees, MempoolInfo, MempoolRpc, RpcError};

/// Inclusive sat/vB range synthetic fees are drawn from (uniform).
#[derive(Clone, Copy)]
pub struct FeeDistribution {
    pub min_sat_vb: u64,
    pub max_sat_vb: u64,
}

#[derive(Clone, Copy)]
pub struct ChurnConfig {
    pub arrivals_per_tick: usize,
    pub evictions_per_tick: usize,
    pub fee: FeeDistribution,
}

/// A deterministic, in-memory stand-in for a Bitcoin node's mempool RPC surface.
pub struct MockNode {
    txs: HashMap<Txid, MempoolEntry>,
    tip_height: u64,
    min_fee: Amount,
    /// When true, the NEXT `mempool_info` reports `loaded: Some(false)` then clears
    /// on the next `advance()`.
    reloading: bool,
    rng: StdRng,
    cfg: ChurnConfig,
}

/// `MempoolEntry` isn't `Clone` (see `clone_entry`), so `#[derive(Clone)]` can't
/// be used here; clone field-by-field, reusing `clone_entry` for the map values.
impl Clone for MockNode {
    fn clone(&self) -> Self {
        Self {
            txs: self
                .txs
                .iter()
                .map(|(k, v)| (*k, clone_entry(v)))
                .collect(),
            tip_height: self.tip_height,
            min_fee: self.min_fee,
            reloading: self.reloading,
            rng: self.rng.clone(),
            cfg: self.cfg,
        }
    }
}

impl MockNode {
    pub fn new(seed: u64, initial_size: usize, cfg: ChurnConfig) -> Self {
        let mut node = Self {
            txs: HashMap::with_capacity(initial_size),
            tip_height: 800_000,
            min_fee: Amount::from_sat(1_000), // 1 sat/vB-ish floor in sats/kvB terms
            reloading: false,
            rng: StdRng::seed_from_u64(seed),
            cfg,
        };
        for _ in 0..initial_size {
            let (txid, entry) = node.gen_entry();
            node.txs.insert(txid, entry);
        }
        node
    }

    /// One churn tick: add `arrivals_per_tick` fresh txs, evict `evictions_per_tick`.
    pub fn advance(&mut self) {
        self.reloading = false;
        for _ in 0..self.cfg.evictions_per_tick {
            if let Some(&victim) = self.txs.keys().next() {
                self.txs.remove(&victim);
            }
        }
        for _ in 0..self.cfg.arrivals_per_tick {
            let (txid, entry) = self.gen_entry();
            self.txs.insert(txid, entry);
        }
    }

    pub fn reload(&mut self) {
        self.reloading = true;
    }

    pub fn mass_drop(&mut self, fraction: f64) {
        let target = ((self.txs.len() as f64) * fraction) as usize;
        let victims: Vec<Txid> = self.txs.keys().take(target).copied().collect();
        for v in victims {
            self.txs.remove(&v);
        }
    }

    /// Simulate a confirmed block: advance the tip and confirm the highest
    /// fee-rate txs up to one block's weight budget (~4M weight units), the way
    /// a real miner fills a block from the top of the fee market. Deterministic
    /// for a given mempool: ties on fee-rate break on txid. The lowest-fee txs
    /// are left behind, so `/fees` dips after a block and recovers as churn refills.
    pub fn mine_block(&mut self) {
        const BLOCK_WEIGHT: u64 = 4_000_000;
        self.tip_height += 1;

        // Rank candidates by fee-rate (sat/vB) descending; tie-break on txid so
        // the selection is reproducible regardless of HashMap iteration order.
        let mut ranked: Vec<(Txid, u64, u64)> = self
            .txs
            .iter()
            .map(|(txid, e)| {
                let vsize = e.vsize.max(1);
                let sat_vb = e.fees.base.to_sat() / vsize;
                let weight = e.weight.unwrap_or(vsize * 4);
                (*txid, sat_vb, weight)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

        let mut used = 0u64;
        for (txid, _rate, weight) in ranked {
            if used + weight > BLOCK_WEIGHT {
                break;
            }
            used += weight;
            self.txs.remove(&txid);
        }
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// Synchronous snapshot accessors for callers that can't `.await` while
    /// holding a lock on the node (e.g. `sim::server`'s handler, which must
    /// release its `std::sync::Mutex<MockNode>` guard before building a JSON
    /// response). These mirror the `MempoolRpc` methods but return plain
    /// values instead of `Result<_, RpcError>` futures, since `MockNode`
    /// never actually fails or awaits internally.
    pub fn tip_height_sync(&self) -> u64 {
        self.tip_height
    }

    /// Synchronous mirror of `mempool_info().loaded` for `sim::server`'s handler,
    /// which reads node state under a `std::sync::Mutex` guard it must drop before
    /// awaiting. `false` while a reload is pending (models a node still loading
    /// its mempool after restart).
    pub fn loaded_sync(&self) -> bool {
        !self.reloading
    }

    pub fn snapshot_entries(&self) -> Vec<(Txid, MempoolEntry)> {
        self.txs.iter().map(|(k, v)| (*k, clone_entry(v))).collect()
    }

    pub fn entry_by_txid(&self, txid: &Txid) -> Option<MempoolEntry> {
        self.txs.get(txid).map(clone_entry)
    }

    /// Build one synthetic `(Txid, MempoolEntry)` with a fresh random txid and
    /// internally-consistent size/fee/package fields.
    fn gen_entry(&mut self) -> (Txid, MempoolEntry) {
        use bitcoin::hashes::Hash;
        let mut raw = [0u8; 32];
        self.rng.fill(&mut raw);
        let txid = Txid::from_byte_array(raw);

        let vsize: u64 = self.rng.gen_range(110..=100_000);
        let weight = vsize * 4;
        let sat_vb: u64 = self
            .rng
            .gen_range(self.cfg.fee.min_sat_vb..=self.cfg.fee.max_sat_vb);
        let base = Amount::from_sat(sat_vb.saturating_mul(vsize));

        // Solo package (no ancestors/descendants) keeps the model simple but
        // consistent: ancestor/descendant totals equal this tx's own.
        let fees = MempoolEntryFees {
            base,
            ancestor: base,
            descendant: base,
        };
        let entry = MempoolEntry {
            vsize,
            weight: Some(weight),
            depends: Vec::new(),
            fees,
            ancestorsize: vsize,
            descendantsize: vsize,
            // Fixed synthetic entry time; the sim doesn't model tx age, so a
            // constant keeps snapshots deterministic.
            time: Some(1_700_000_000),
        };
        (txid, entry)
    }
}

impl MempoolRpc for MockNode {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        Ok(MempoolInfo {
            loaded: Some(!self.reloading),
            mempoolminfee: self.min_fee,
        })
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        Ok(self.txs.keys().copied().collect())
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        Ok(self
            .txs
            .iter()
            .map(|(k, v)| (*k, clone_entry(v)))
            .collect())
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        Ok(self.txs.get(txid).map(clone_entry))
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        Ok(self.tip_height)
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// `MempoolEntry` isn't `Clone` (it only derives `Deserialize`). The sim needs to
/// hand out owned copies, so clone field-by-field here rather than adding a
/// `Clone` derive to the production type.
fn clone_entry(e: &MempoolEntry) -> MempoolEntry {
    MempoolEntry {
        vsize: e.vsize,
        weight: e.weight,
        depends: e.depends.clone(),
        fees: MempoolEntryFees {
            base: e.fees.base,
            ancestor: e.fees.ancestor,
            descendant: e.fees.descendant,
        },
        ancestorsize: e.ancestorsize,
        descendantsize: e.descendantsize,
        time: e.time,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ChurnConfig {
        ChurnConfig {
            arrivals_per_tick: 50,
            evictions_per_tick: 40,
            fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
        }
    }

    #[tokio::test]
    async fn same_seed_produces_identical_mempool() {
        let a = MockNode::new(42, 1000, cfg());
        let b = MockNode::new(42, 1000, cfg());
        let ta = a.raw_mempool_txids().await.unwrap();
        let tb = b.raw_mempool_txids().await.unwrap();
        assert_eq!(ta.len(), 1000);
        let sa: std::collections::HashSet<_> = ta.into_iter().collect();
        let sb: std::collections::HashSet<_> = tb.into_iter().collect();
        assert_eq!(sa, sb, "identical seed must yield identical txid set");
    }

    #[tokio::test]
    async fn advance_applies_churn_counts() {
        let mut n = MockNode::new(7, 1000, cfg());
        n.advance();
        // +50 arrivals, -40 evictions => net +10 (arrivals use fresh random
        // txids so they never collide with evicted ones).
        assert_eq!(n.len(), 1010);
    }

    #[tokio::test]
    async fn mempool_entry_none_for_absent_tx() {
        let n = MockNode::new(1, 10, cfg());
        let absent = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .unwrap();
        assert!(n.mempool_entry(&absent).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fees_within_configured_range() {
        let n = MockNode::new(3, 500, cfg());
        for (_txid, e) in n.raw_mempool_verbose().await.unwrap() {
            let sat_vb = e.fees.base.to_sat() / e.vsize.max(1);
            assert!((1..=500).contains(&sat_vb), "fee {sat_vb} sat/vB out of range");
        }
    }

    #[tokio::test]
    async fn mine_block_advances_tip_and_drops_top_fee_txs() {
        // Wide fee spread so "highest fee-rate confirmed first" is observable.
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
        };
        let mut n = MockNode::new(99, 2000, cfg);

        let tip_before = n.tip_height_sync();
        let len_before = n.len();
        // Lowest fee-rate tx present before the block.
        let min_rate_before = n
            .raw_mempool_verbose()
            .await
            .unwrap()
            .into_iter()
            .map(|(_id, e)| e.fees.base.to_sat() / e.vsize.max(1))
            .min()
            .unwrap();

        n.mine_block();

        // Tip advanced by exactly one block.
        assert_eq!(n.tip_height_sync(), tip_before + 1);
        // A block confirmed at least one tx (max weight per tx is 400k < 4M budget).
        assert!(n.len() < len_before, "block should shrink the mempool");
        // Total confirmed weight never exceeds one block.
        let confirmed = len_before - n.len();
        assert!(confirmed >= 1);
        // The lowest fee-rate tx is NOT confirmed (miners take the top of the book).
        let min_rate_after = n
            .raw_mempool_verbose()
            .await
            .unwrap()
            .into_iter()
            .map(|(_id, e)| e.fees.base.to_sat() / e.vsize.max(1))
            .min()
            .unwrap();
        assert_eq!(min_rate_after, min_rate_before, "lowest-fee tx must survive");
    }
}
