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

    pub fn len(&self) -> usize {
        self.txs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
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
}
