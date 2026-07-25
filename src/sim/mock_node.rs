use std::collections::{HashMap, HashSet};

use bitcoin::{Amount, Txid};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{MempoolEntry, MempoolEntryFees, MempoolInfo, MempoolRpc, RpcError};

/// Inclusive sat/vB range synthetic fees are drawn from (uniform).
#[derive(Clone, Copy)]
pub struct FeeDistribution {
    pub min_sat_vb: u64,
    pub max_sat_vb: u64,
    /// Power-law skew exponent for the fee-rate draw. 1.0 = uniform; > 1.0 biases
    /// draws toward the floor (the realistic "wall at the relay floor").
    pub skew: f64,
}

impl FeeDistribution {
    /// Uniform fee-rates over [min, max] (skew = 1.0) — the pre-skew behaviour.
    pub fn uniform(min_sat_vb: u64, max_sat_vb: u64) -> Self {
        Self {
            min_sat_vb,
            max_sat_vb,
            skew: 1.0,
        }
    }
    /// Power-law-skewed fee-rates over [min, max]. `skew > 1.0` piles draws near the floor.
    pub fn skewed(min_sat_vb: u64, max_sat_vb: u64, skew: f64) -> Self {
        Self {
            min_sat_vb,
            max_sat_vb,
            skew,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ChurnConfig {
    pub arrivals_per_tick: usize,
    pub evictions_per_tick: usize,
    pub fee: FeeDistribution,
    /// Fraction of arrivals that attach as a CPFP child of an existing tx.
    pub cpfp_fraction: f64,
    /// Max linear chain length (root + descendants). 1 disables chaining.
    pub max_chain: usize,
}

/// A deterministic, in-memory stand-in for a Bitcoin node's mempool RPC surface.
pub struct MockNode {
    txs: HashMap<Txid, MempoolEntry>,
    /// Reverse index: parent txid -> its single child (linear chains).
    children: HashMap<Txid, Txid>,
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
            txs: self.txs.iter().map(|(k, v)| (*k, clone_entry(v))).collect(),
            children: self.children.clone(),
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
            children: HashMap::new(),
            tip_height: 800_000,
            min_fee: Amount::from_sat(1_000), // 1 sat/vB-ish floor in sats/kvB terms
            reloading: false,
            rng: StdRng::seed_from_u64(seed),
            cfg,
        };
        for _ in 0..initial_size {
            node.add_arrival();
        }
        node
    }

    /// One churn tick: add `arrivals_per_tick` fresh txs, evict `evictions_per_tick`.
    pub fn advance(&mut self) {
        self.reloading = false;
        for _ in 0..self.cfg.evictions_per_tick {
            let Some(start) = self.txs.keys().next().copied() else {
                break;
            };
            // Descend the linear chain to its leaf so we never orphan a child.
            let mut leaf = start;
            while let Some(child) = self.children.get(&leaf).copied() {
                leaf = child;
            }
            self.remove_leaf(leaf);
        }
        for _ in 0..self.cfg.arrivals_per_tick {
            self.add_arrival();
        }
    }

    pub fn reload(&mut self) {
        self.reloading = true;
    }

    pub fn mass_drop(&mut self, fraction: f64) {
        let target = ((self.txs.len() as f64) * fraction) as usize;
        for _ in 0..target {
            let Some(start) = self.txs.keys().next().copied() else {
                break;
            };
            // Descend the linear chain to its leaf so we never orphan a child.
            let mut leaf = start;
            while let Some(child) = self.children.get(&leaf).copied() {
                leaf = child;
            }
            self.remove_leaf(leaf);
        }
    }

    /// Ancestor-package effective rate (sat/vB): min(own_rate, package_rate), using
    /// the entry's ancestor aggregates as the package totals — mirrors gbt.rs.
    fn effective_rate(&self, t: &Txid) -> u64 {
        let e = &self.txs[t];
        let own = own_rate_sat_vb(e);
        let pkg = e.fees.ancestor.to_sat() / e.ancestorsize.max(1);
        own.min(pkg)
    }

    /// Confirm every tx in `set` (assumed ancestor-closed), then re-root any
    /// survivor whose parent was confirmed: clear its depends and reset ancestor
    /// aggregates to its own. Descendant aggregates of confirmed ancestors need no
    /// repair — they leave with them. Chains are linear, so after re-rooting we
    /// also walk DOWN each new root's surviving `children` chain, recomputing
    /// every descendant's ancestor aggregates from its (now-correct) parent —
    /// otherwise a surviving grandchild would keep counting a confirmed
    /// great-ancestor that no longer exists.
    fn confirm_set(&mut self, set: &HashSet<Txid>) {
        let reroot: Vec<Txid> = self
            .txs
            .keys()
            .filter(|t| !set.contains(*t))
            .filter(|t| {
                self.txs[*t]
                    .depends
                    .first()
                    .is_some_and(|p| set.contains(p))
            })
            .copied()
            .collect();
        for t in &reroot {
            if let Some(e) = self.txs.get_mut(t) {
                e.depends.clear();
                e.ancestorsize = e.vsize;
                e.fees.ancestor = e.fees.base;
            }
        }
        for t in set {
            self.txs.remove(t);
            self.children.remove(t);
        }
        // Repair stale ancestor aggregates on every descendant below each new
        // root: walk the linear children chain, recomputing each child from its
        // already-correct parent.
        for root in reroot {
            let mut parent = root;
            while let Some(child) = self.children.get(&parent).copied() {
                let (parent_ancestorsize, parent_fees_ancestor) = {
                    let p = &self.txs[&parent];
                    (p.ancestorsize, p.fees.ancestor)
                };
                if let Some(c) = self.txs.get_mut(&child) {
                    c.ancestorsize = c.vsize + parent_ancestorsize;
                    c.fees.ancestor = c.fees.base + parent_fees_ancestor;
                }
                parent = child;
            }
        }
    }

    /// Simulate a confirmed block: advance the tip and confirm the highest
    /// effective-rate ancestor packages up to one block's weight budget, mirroring
    /// how a real miner fills a block. Confirms whole ancestor sets (never a child
    /// without its parent); the tip advances by one.
    pub fn mine_block(&mut self) {
        const BLOCK_WEIGHT: u64 = 4_000_000;
        self.tip_height += 1;

        let uids: Vec<Txid> = self.txs.keys().copied().collect();
        let mut ranked: Vec<(u64, Txid)> =
            uids.iter().map(|t| (self.effective_rate(t), *t)).collect();
        // Descending effective rate; tie-break on txid for reproducibility.
        ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));

        let mut used = 0u64;
        let mut confirmed: HashSet<Txid> = HashSet::new();
        for (_score, t) in ranked {
            if confirmed.contains(&t) {
                continue;
            }
            // Package = this tx + its not-yet-confirmed ancestors.
            let mut pkg = vec![t];
            pkg.extend(
                self.ancestors_of(&t)
                    .into_iter()
                    .filter(|a| !confirmed.contains(a)),
            );
            let pkg_weight: u64 = pkg
                .iter()
                .map(|x| {
                    let e = &self.txs[x];
                    e.weight.unwrap_or(e.vsize * 4)
                })
                .sum();
            if used + pkg_weight > BLOCK_WEIGHT {
                continue;
            }
            used += pkg_weight;
            for x in pkg {
                confirmed.insert(x);
            }
        }

        self.confirm_set(&confirmed);
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

    /// A fresh standalone entry paying `sat_vb`; depends empty, ancestor/descendant
    /// aggregates equal its own (a solo package).
    fn gen_standalone_entry(&mut self, sat_vb: u64) -> (Txid, MempoolEntry) {
        use bitcoin::hashes::Hash;
        let mut raw = [0u8; 32];
        self.rng.fill(&mut raw);
        let txid = Txid::from_byte_array(raw);
        let vsize: u64 = self.rng.gen_range(110..=100_000);
        let weight = vsize * 4;
        let base = Amount::from_sat(sat_vb.saturating_mul(vsize));
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
            time: Some(1_700_000_000),
        };
        (txid, entry)
    }

    /// In-mempool ancestors of `txid`, nearest parent first (linear chain).
    fn ancestors_of(&self, txid: &Txid) -> Vec<Txid> {
        let mut out = Vec::new();
        let mut cur = *txid;
        while let Some(entry) = self.txs.get(&cur) {
            match entry.depends.first() {
                Some(p) if self.txs.contains_key(p) => {
                    out.push(*p);
                    cur = *p;
                }
                _ => break,
            }
        }
        out
    }

    /// Test/inspection accessor: the recorded child of `parent`, if any.
    pub fn child_of(&self, parent: &Txid) -> Option<Txid> {
        self.children.get(parent).copied()
    }

    /// An eligible parent to extend: a childless tx whose chain is shorter than
    /// `max_chain`. Low-fee-biased (samples a few candidates, keeps the cheapest)
    /// so CPFP children visibly lift cheap parents. Returns None if none exist.
    fn pick_parent(&mut self) -> Option<Txid> {
        if self.cfg.max_chain < 2 {
            return None;
        }
        // O(n) per arrival by design: sim mempools are 10^2-10^4 txs (not
        // production-scale); do not hoist into a hot path.
        let keys: Vec<Txid> = self.txs.keys().copied().collect();
        let eligible: Vec<Txid> = keys
            .into_iter()
            .filter(|t| {
                !self.children.contains_key(t)
                    && self.ancestors_of(t).len() + 1 < self.cfg.max_chain
            })
            .collect();
        if eligible.is_empty() {
            return None;
        }
        let samples = eligible.len().min(8);
        let mut best: Option<(u64, Txid)> = None;
        for _ in 0..samples {
            let idx = self.rng.gen_range(0..eligible.len());
            let t = eligible[idx];
            let r = own_rate_sat_vb(&self.txs[&t]);
            match best {
                Some((br, bt)) if (br, bt) <= (r, t) => {}
                _ => best = Some((r, t)),
            }
        }
        best.map(|(_, t)| t)
    }

    /// Adjust the descendant aggregates of `from` and every ancestor above it by one
    /// tx of `vsize`/`base`. `add == true` bumps (a child attached); `false` subtracts
    /// (a leaf removed), saturating/checked to avoid underflow if aggregates ever drift.
    fn adjust_descendant_totals(&mut self, from: Txid, vsize: u64, base: Amount, add: bool) {
        let mut chain = vec![from];
        chain.extend(self.ancestors_of(&from));
        for a in chain {
            if let Some(e) = self.txs.get_mut(&a) {
                if add {
                    e.descendantsize += vsize;
                    e.fees.descendant += base;
                } else {
                    e.descendantsize = e.descendantsize.saturating_sub(vsize);
                    e.fees.descendant = e.fees.descendant.checked_sub(base).unwrap_or(Amount::ZERO);
                }
            }
        }
    }

    /// Attach a fresh high-fee child to `parent`, extending a linear CPFP chain and
    /// keeping all ancestor/descendant aggregates truthful.
    fn attach_child(&mut self, parent: Txid) {
        let child_rate = self.cfg.fee.max_sat_vb; // high fee -> package lift
        let (txid, mut entry) = self.gen_standalone_entry(child_rate);
        entry.depends = vec![parent];
        {
            let p = &self.txs[&parent];
            entry.ancestorsize = entry.vsize + p.ancestorsize;
            entry.fees.ancestor = entry.fees.base + p.fees.ancestor;
        }
        let child_vsize = entry.vsize;
        let child_base = entry.fees.base;
        self.txs.insert(txid, entry);
        self.children.insert(parent, txid);
        // Bump descendant aggregates on the parent AND every ancestor above it.
        self.adjust_descendant_totals(parent, child_vsize, child_base, true);
    }

    /// Remove a leaf tx (one with no in-mempool child), keeping the children index
    /// and every ancestor's descendant aggregates consistent. No-op if `leaf` is
    /// absent or still has a child.
    fn remove_leaf(&mut self, leaf: Txid) {
        if self.children.contains_key(&leaf) {
            return; // not a leaf
        }
        let Some(entry) = self.txs.remove(&leaf) else {
            return;
        };
        let vsize = entry.vsize;
        let base = entry.fees.base;
        if let Some(parent) = entry.depends.first().copied() {
            self.children.remove(&parent);
            // Subtract this leaf from the parent AND every ancestor above it.
            self.adjust_descendant_totals(parent, vsize, base, false);
        }
    }

    /// One arrival: attach a CPFP child with probability `cpfp_fraction` when an
    /// eligible parent exists; otherwise insert a standalone tx.
    fn add_arrival(&mut self) {
        let attach = self.cfg.cpfp_fraction > 0.0 && self.rng.gen::<f64>() < self.cfg.cpfp_fraction;
        if attach {
            if let Some(parent) = self.pick_parent() {
                self.attach_child(parent);
                return;
            }
        }
        let sat_vb = self.sample_fee_rate();
        let (txid, entry) = self.gen_standalone_entry(sat_vb);
        self.txs.insert(txid, entry);
    }

    /// Draw a standalone tx's fee-rate (sat/vB) from a bounded power-law over
    /// `[min, max]`: `rate = min + (max - min) * u^skew`. `skew == 1.0` is uniform;
    /// `skew > 1.0` piles draws near the floor. All randomness from the seeded rng.
    fn sample_fee_rate(&mut self) -> u64 {
        let FeeDistribution {
            min_sat_vb: min,
            max_sat_vb: max,
            skew,
        } = self.cfg.fee;
        if max <= min {
            return min;
        }
        let u: f64 = self.rng.gen(); // [0, 1)
        let span = (max - min) as f64;
        let rate = min + (span * u.powf(skew)).round() as u64;
        rate.min(max)
    }
}

/// Own fee-rate in sat/vB (integer), guarding zero vsize.
fn own_rate_sat_vb(e: &MempoolEntry) -> u64 {
    e.fees.base.to_sat() / e.vsize.max(1)
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
        Ok(self.txs.iter().map(|(k, v)| (*k, clone_entry(v))).collect())
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
            fee: FeeDistribution::uniform(1, 500),
            cpfp_fraction: 0.0,
            max_chain: 1,
        }
    }

    fn cpfp_cfg() -> ChurnConfig {
        ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 1.0, // every arrival that can attach, does
            max_chain: 3,
        }
    }

    #[tokio::test]
    async fn packages_have_consistent_aggregates_and_present_parents() {
        let node = MockNode::new(2024, 300, cpfp_cfg());
        let mut saw_package = false;
        for (txid, e) in node.raw_mempool_verbose().await.unwrap() {
            if let Some(parent) = e.depends.first().copied() {
                saw_package = true;
                // Invariant: parent is present in the mempool.
                let p = node
                    .entry_by_txid(&parent)
                    .expect("child's depends parent must be present");
                // Child ancestor aggregates = own + parent's ancestor totals.
                assert_eq!(e.ancestorsize, e.vsize + p.ancestorsize);
                assert_eq!(e.fees.ancestor, e.fees.base + p.fees.ancestor);
                // The parent must record this child.
                assert_eq!(node.child_of(&parent), Some(txid));
            }
        }
        assert!(
            saw_package,
            "cpfp_fraction=1.0 must produce at least one package"
        );
    }

    #[tokio::test]
    async fn parent_descendant_aggregates_match_child_subtree() {
        let node = MockNode::new(7, 300, cpfp_cfg());
        for (txid, p) in node.raw_mempool_verbose().await.unwrap() {
            if let Some(child) = node.child_of(&txid) {
                let c = node.entry_by_txid(&child).unwrap();
                // Linear chain: parent's descendant totals = own + child's subtree.
                assert_eq!(p.descendantsize, p.vsize + c.descendantsize);
                assert_eq!(p.fees.descendant, p.fees.base + c.fees.descendant);
            }
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
            assert!(
                (1..=500).contains(&sat_vb),
                "fee {sat_vb} sat/vB out of range"
            );
        }
    }

    #[tokio::test]
    async fn churn_eviction_never_orphans_a_child() {
        let cfg = ChurnConfig {
            arrivals_per_tick: 40,
            evictions_per_tick: 40,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 0.5,
            max_chain: 3,
        };
        let mut node = MockNode::new(99, 500, cfg);
        for _ in 0..20 {
            node.advance();
            // Invariant after every tick: every child's parent is still present.
            for (_txid, e) in node.raw_mempool_verbose().await.unwrap() {
                if let Some(parent) = e.depends.first() {
                    assert!(
                        node.entry_by_txid(parent).is_some(),
                        "eviction orphaned a child (parent gone)"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn mass_drop_never_orphans_and_shrinks() {
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 0.5,
            max_chain: 3,
        };
        let mut node = MockNode::new(42, 400, cfg);
        let before = node.len();
        node.mass_drop(0.5);
        assert!(node.len() < before, "mass_drop must shrink the mempool");
        for (_txid, e) in node.raw_mempool_verbose().await.unwrap() {
            if let Some(parent) = e.depends.first() {
                assert!(
                    node.entry_by_txid(parent).is_some(),
                    "mass_drop orphaned a child"
                );
            }
        }
    }

    #[tokio::test]
    async fn mine_block_advances_tip_and_drops_top_fee_txs() {
        // Wide fee spread so "highest fee-rate confirmed first" is observable.
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 500),
            cpfp_fraction: 0.0,
            max_chain: 1,
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
        assert_eq!(
            min_rate_after, min_rate_before,
            "lowest-fee tx must survive"
        );
    }

    #[tokio::test]
    async fn confirm_set_recomputes_grandchild_ancestor_aggregates() {
        // Find a length-3 linear chain a -> b -> c (a's depends empty, b depends
        // on a, c depends on b) so confirming only `a` leaves `b` re-rooted and
        // `c` a surviving grandchild whose stale aggregates must be repaired.
        let mut node = MockNode::new(11, 400, cpfp_cfg());
        let mut found: Option<(Txid, Txid, Txid)> = None;
        for (a, ea) in node.snapshot_entries() {
            if !ea.depends.is_empty() {
                continue;
            }
            let Some(b) = node.child_of(&a) else { continue };
            let Some(c) = node.child_of(&b) else { continue };
            found = Some((a, b, c));
            break;
        }
        let (a, b, c) = found.expect(
            "expected at least one length-3 chain a->b->c with cpfp_fraction=1.0, max_chain=3",
        );

        let b_before = node.entry_by_txid(&b).unwrap();
        let c_before = node.entry_by_txid(&c).unwrap();
        let a_entry = node.entry_by_txid(&a).unwrap();

        // Sanity: before confirming, c's aggregates DO include a's contribution.
        assert_eq!(
            c_before.ancestorsize,
            c_before.vsize + b_before.vsize + a_entry.vsize
        );

        node.confirm_set(&HashSet::from([a]));

        // a is gone.
        assert!(node.entry_by_txid(&a).is_none());

        // b was re-rooted: depends cleared, ancestor aggregates equal its own.
        let b_after = node.entry_by_txid(&b).expect("b must survive");
        assert!(
            b_after.depends.is_empty(),
            "b must be re-rooted (depends cleared)"
        );
        assert_eq!(b_after.ancestorsize, b_after.vsize);
        assert_eq!(b_after.fees.ancestor, b_after.fees.base);

        // c survives with b still as its parent, but its ancestor aggregates must
        // be recomputed to drop a's now-confirmed contribution.
        let c_after = node.entry_by_txid(&c).expect("c must survive");
        assert_eq!(c_after.depends, vec![b]);
        assert_eq!(
            c_after.ancestorsize,
            c_after.vsize + b_after.ancestorsize,
            "c.ancestorsize must no longer count the confirmed root a"
        );
        assert_eq!(
            c_after.fees.ancestor,
            c_after.fees.base + b_after.fees.ancestor,
            "c.fees.ancestor must no longer count the confirmed root a"
        );
    }

    #[tokio::test]
    async fn aggregates_stay_truthful_across_churn_and_mine_cycles() {
        let cfg = ChurnConfig {
            arrivals_per_tick: 60,
            evictions_per_tick: 20,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 0.6,
            max_chain: 3,
        };
        let mut node = MockNode::new(2024, 400, cfg);

        // Assert the truthful-aggregate invariant over EVERY survivor: each
        // child's ancestor totals equal its own plus its (present) parent's;
        // each root's equal its own.
        fn assert_invariant(node: &MockNode) {
            for (txid, e) in node.snapshot_entries() {
                if let Some(parent) = e.depends.first().copied() {
                    let p = node
                        .entry_by_txid(&parent)
                        .unwrap_or_else(|| panic!("{txid:?}'s parent {parent:?} must be present"));
                    assert_eq!(
                        e.ancestorsize,
                        e.vsize + p.ancestorsize,
                        "ancestorsize drift for {txid:?}"
                    );
                    assert_eq!(
                        e.fees.ancestor,
                        e.fees.base + p.fees.ancestor,
                        "fees.ancestor drift for {txid:?}"
                    );
                } else {
                    assert_eq!(
                        e.ancestorsize, e.vsize,
                        "root ancestorsize drift for {txid:?}"
                    );
                    assert_eq!(
                        e.fees.ancestor, e.fees.base,
                        "root fees.ancestor drift for {txid:?}"
                    );
                }
            }
        }

        let mut forced_partial_confirms = 0usize;
        for _ in 0..15 {
            node.advance();
            // Deterministically drive the partial-confirmation path: when a
            // length->=3 chain exists, confirm ONLY its root, leaving a surviving
            // grandchild whose ancestor aggregates must be recomputed. This is the
            // exact drift scenario; mine_block reaches it only rarely under this
            // seed, so force it directly to give the guard teeth. Assert the
            // invariant IMMEDIATELY after the forced confirm — before the ensuing
            // mine_block/eviction can mask a stale grandchild by removing it.
            let root_with_grandchild = node.snapshot_entries().into_iter().find_map(|(a, ea)| {
                if !ea.depends.is_empty() {
                    return None;
                }
                let b = node.child_of(&a)?;
                node.child_of(&b)?; // grandchild must exist
                Some(a)
            });
            if let Some(a) = root_with_grandchild {
                node.confirm_set(&HashSet::from([a]));
                forced_partial_confirms += 1;
                assert_invariant(&node);
            }
            node.mine_block();
            assert_invariant(&node);
        }
        assert!(
            forced_partial_confirms > 0,
            "test must exercise the partial-confirm path at least once"
        );
        assert_invariant(&node);
    }

    #[test]
    fn skewed_fees_pile_near_the_floor() {
        // skew = 3 over [1, 1000]: most draws land in the bottom quarter (<=250).
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::skewed(1, 1000, 3.0),
            cpfp_fraction: 0.0,
            max_chain: 1,
        };
        let mut node = MockNode::new(2024, 1, cfg);
        let n = 5000;
        let mut in_bottom_quarter = 0u32;
        for _ in 0..n {
            let r = node.sample_fee_rate();
            assert!((1..=1000).contains(&r), "sample {r} out of [1,1000]");
            if r <= 250 {
                in_bottom_quarter += 1;
            }
        }
        let frac = f64::from(in_bottom_quarter) / f64::from(n);
        assert!(
            frac > 0.55,
            "skewed draw should pile low: {frac} in bottom quarter (uniform ~0.25)"
        );
    }

    #[test]
    fn uniform_skew_spreads_evenly() {
        // skew = 1.0 (uniform) over [1, 1000]: ~25% in the bottom quarter.
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 1000),
            cpfp_fraction: 0.0,
            max_chain: 1,
        };
        let mut node = MockNode::new(7, 1, cfg);
        let n = 5000;
        let mut in_bottom_quarter = 0u32;
        for _ in 0..n {
            let r = node.sample_fee_rate();
            assert!((1..=1000).contains(&r), "sample {r} out of [1,1000]");
            if r <= 250 {
                in_bottom_quarter += 1;
            }
        }
        let frac = f64::from(in_bottom_quarter) / f64::from(n);
        assert!(
            (0.15..0.35).contains(&frac),
            "uniform draw should be ~0.25 in bottom quarter, got {frac}"
        );
    }

    #[tokio::test]
    async fn mine_block_confirms_whole_packages_and_never_orphans() {
        let cfg = ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 1.0,
            max_chain: 3,
        };
        let mut node = MockNode::new(5, 300, cfg);
        let tip_before = node.tip_height_sync();
        node.mine_block();
        assert_eq!(node.tip_height_sync(), tip_before + 1);
        // No survivor may reference a parent that is gone (a confirmed parent must
        // have been re-rooted: its child's depends cleared).
        for (_txid, e) in node.raw_mempool_verbose().await.unwrap() {
            if let Some(parent) = e.depends.first() {
                assert!(
                    node.entry_by_txid(parent).is_some(),
                    "mine_block left a survivor whose parent was confirmed"
                );
            }
        }
    }
}
