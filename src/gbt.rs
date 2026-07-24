//! Pure fee-projection core — the *selection* half of Bitcoin Core's block
//! assembly (`getblocktemplate`). No async, no locks, no I/O; unit-testable in
//! isolation like `sync::decision`.
//!
//! Independent implementation of the public algorithm in Bitcoin Core's
//! `BlockAssembler` (`src/node/miner.cpp`, `addPackageTxs`), MIT-licensed:
//! rank transactions by ancestor-package fee rate, greedily pack the best
//! package into each projected block, and lift a package's members to their
//! shared (CPFP) effective rate as ancestors get included.

use std::collections::{BinaryHeap, HashMap, HashSet};

/// Consensus block weight limit (weight units).
pub const MAX_BLOCK_WEIGHT: u32 = 4_000_000;
/// Weight held back for the coinbase / block overhead, so a projected block
/// never packs the full 4M and overstates how much fee-paying data fits.
pub const BLOCK_RESERVED_WEIGHT: u32 = 4_000;
/// Number of projected blocks to build. The last is unbounded (the
/// "everything else" block), so tiers past the horizon still have a floor.
pub const MAX_BLOCKS: usize = 8;
/// Consecutive non-fitting packages before we declare a bounded block full and
/// move on. Mirrors Core's "try smaller options for a while" behaviour.
const FAILURE_LIMIT: u32 = 1000;

/// One transaction handed to the projector. `uid` must be its index in the
/// input `Vec` (dense `0..n`); `order` is a deterministic tie-breaker.
#[derive(Debug, Clone)]
pub struct GbtTx {
    pub uid: u32,
    pub order: u32,
    pub fee: u64,          // sats
    pub weight: u32,       // weight units
    pub parents: Vec<u32>, // direct in-mempool parent uids
}

/// Result of a projection run.
#[derive(Debug, Clone)]
pub struct Projection {
    /// One inner Vec per projected block, each a list of uids. Block 0 = next block.
    pub blocks: Vec<Vec<u32>>,
    /// CPFP-adjusted effective fee rate (sat/vB) for every included uid.
    pub effective_rates: HashMap<u32, f64>,
}

/// Internal per-tx working state, indexed by `uid`.
struct Audit {
    order: u32,
    fee: u64,
    weight: u32,
    ancestors: Vec<u32>, // full transitive closure of in-set ancestors
    children: Vec<u32>,  // direct children (reverse of `parents`)
}

/// sat/vB from sats and weight units: `4 * fee / weight`.
fn rate(fee: u64, weight: u64) -> f64 {
    if weight == 0 {
        0.0
    } else {
        4.0 * fee as f64 / weight as f64
    }
}

/// Current score of a tx: `min(own_rate, package_rate)` over its not-yet-used
/// ancestors. Scores only ever need recomputing as ancestors get `used`.
fn score_of(uid: u32, audits: &[Audit], used: &[bool]) -> f64 {
    let a = &audits[uid as usize];
    let own = rate(a.fee, a.weight as u64);
    let mut pf = a.fee;
    let mut pw = a.weight as u64;
    for &an in &a.ancestors {
        if !used[an as usize] {
            pf += audits[an as usize].fee;
            pw += audits[an as usize].weight as u64;
        }
    }
    own.min(rate(pf, pw))
}

/// Max-heap entry. Higher score = higher priority; ties broken deterministically
/// by smaller `order` then smaller `uid`. Scores are finite (weights are > 0 for
/// real txs, and `rate` guards zero), so `partial_cmp` never returns `None`.
struct HeapItem {
    score: f64,
    order: u32,
    uid: u32,
}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score && self.order == other.order && self.uid == other.uid
    }
}
impl Eq for HeapItem {}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary: higher score first.
        self.score
            .partial_cmp(&other.score)
            .expect("scores are finite")
            // Tie-break: smaller order/uid = higher priority, so reverse them.
            .then_with(|| other.order.cmp(&self.order))
            .then_with(|| other.uid.cmp(&self.uid))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Project the mempool into fee-ordered blocks using the default limits.
pub fn project(txs: Vec<GbtTx>) -> Projection {
    project_with(txs, MAX_BLOCKS, MAX_BLOCK_WEIGHT, BLOCK_RESERVED_WEIGHT)
}

fn project_with(
    txs: Vec<GbtTx>,
    max_blocks: usize,
    max_block_weight: u32,
    reserved: u32,
) -> Projection {
    let n = txs.len();
    if n == 0 {
        return Projection {
            blocks: Vec::new(),
            effective_rates: HashMap::new(),
        };
    }

    // Build audits indexed by uid (dense 0..n).
    let mut audits: Vec<Audit> = Vec::with_capacity(n);
    for tx in &txs {
        debug_assert_eq!(tx.uid as usize, audits.len(), "uids must be dense 0..n");
        audits.push(Audit {
            order: tx.order,
            fee: tx.fee,
            weight: tx.weight,
            ancestors: Vec::new(),
            children: Vec::new(),
        });
    }

    // Direct children (reverse edges).
    for tx in &txs {
        for &p in &tx.parents {
            audits[p as usize].children.push(tx.uid);
        }
    }

    // Transitive ancestor closures (memoized DFS; depth bounded by policy limits).
    let mut memo: Vec<Option<Vec<u32>>> = vec![None; n];
    for uid in 0..n as u32 {
        let anc = build_ancestors(uid, &txs, &mut memo);
        audits[uid as usize].ancestors = anc;
    }

    let mut used = vec![false; n];
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(n);
    for uid in 0..n as u32 {
        heap.push(HeapItem {
            score: score_of(uid, &audits, &used),
            order: audits[uid as usize].order,
            uid,
        });
    }

    let mut blocks: Vec<Vec<u32>> = Vec::new();
    let mut effective_rates: HashMap<u32, f64> = HashMap::new();
    let mut cur_block: Vec<u32> = Vec::new();
    let mut block_weight: u32 = reserved;
    let mut overflow: Vec<u32> = Vec::new();
    let mut failures: u32 = 0;

    while let Some(item) = heap.pop() {
        let uid = item.uid;
        if used[uid as usize] {
            continue;
        }
        // Lazy refresh: an entry can be stale (its score changed as ancestors
        // got used). Recompute; if it differs, re-push the corrected entry and
        // move on rather than trusting a stale ordering.
        let cur = score_of(uid, &audits, &used);
        if (cur - item.score).abs() > 1e-9 {
            heap.push(HeapItem {
                score: cur,
                order: audits[uid as usize].order,
                uid,
            });
            continue;
        }

        // Build the package: not-yet-used ancestors, ordered roots-first, then uid.
        let mut pkg: Vec<u32> = audits[uid as usize]
            .ancestors
            .iter()
            .copied()
            .filter(|&a| !used[a as usize])
            .collect();
        pkg.sort_by(|&a, &b| {
            audits[a as usize]
                .ancestors
                .len()
                .cmp(&audits[b as usize].ancestors.len())
                .then(audits[a as usize].order.cmp(&audits[b as usize].order))
                .then(a.cmp(&b))
        });
        pkg.push(uid);

        let pkg_weight: u32 = pkg.iter().map(|&m| audits[m as usize].weight).sum();
        let pkg_fee: u64 = pkg.iter().map(|&m| audits[m as usize].fee).sum();

        let bounded = blocks.len() < max_blocks - 1;
        if bounded && block_weight.saturating_add(pkg_weight) > max_block_weight {
            // Doesn't fit this block; hold it and try smaller packages.
            overflow.push(uid);
            failures += 1;
        } else {
            let cluster_rate = rate(pkg_fee, pkg_weight as u64);
            for &m in &pkg {
                used[m as usize] = true;
                cur_block.push(m);
                block_weight += audits[m as usize].weight;
                effective_rates.insert(m, cluster_rate);
            }
            refresh_descendants(&pkg, &audits, &used, &mut heap);
            failures = 0;
        }

        // Finalize the current bounded block when it's effectively full or the
        // heap is drained; the final (unbounded) block is pushed after the loop.
        let bounded = blocks.len() < max_blocks - 1;
        if bounded && (failures > FAILURE_LIMIT || heap.is_empty()) && !cur_block.is_empty() {
            blocks.push(std::mem::take(&mut cur_block));
            block_weight = reserved;
            failures = 0;
            for &o in &overflow {
                heap.push(HeapItem {
                    score: score_of(o, &audits, &used),
                    order: audits[o as usize].order,
                    uid: o,
                });
            }
            overflow.clear();
        }
    }

    if !cur_block.is_empty() {
        blocks.push(cur_block);
    }

    Projection {
        blocks,
        effective_rates,
    }
}

/// Memoized transitive ancestor closure for `uid`.
fn build_ancestors(uid: u32, txs: &[GbtTx], memo: &mut Vec<Option<Vec<u32>>>) -> Vec<u32> {
    if let Some(a) = &memo[uid as usize] {
        return a.clone();
    }
    let mut set: HashSet<u32> = HashSet::new();
    for &p in &txs[uid as usize].parents {
        set.insert(p);
        for a in build_ancestors(p, txs, memo) {
            set.insert(a);
        }
    }
    let v: Vec<u32> = set.into_iter().collect();
    memo[uid as usize] = Some(v.clone());
    v
}

/// After a package is used, re-push refreshed scores for its whole descendant
/// closure: including an ancestor raises a descendant's effective rate, so its
/// stale (lower) heap entry must be superseded by a corrected one.
fn refresh_descendants(
    pkg: &[u32],
    audits: &[Audit],
    used: &[bool],
    heap: &mut BinaryHeap<HeapItem>,
) {
    let mut seen: HashSet<u32> = HashSet::new();
    let mut stack: Vec<u32> = Vec::new();
    for &m in pkg {
        for &c in &audits[m as usize].children {
            if seen.insert(c) {
                stack.push(c);
            }
        }
    }
    while let Some(d) = stack.pop() {
        if !used[d as usize] {
            heap.push(HeapItem {
                score: score_of(d, audits, used),
                order: audits[d as usize].order,
                uid: d,
            });
        }
        for &c in &audits[d as usize].children {
            if seen.insert(c) {
                stack.push(c);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(uid: u32, fee: u64, weight: u32, parents: &[u32]) -> GbtTx {
        GbtTx {
            uid,
            order: uid,
            fee,
            weight,
            parents: parents.to_vec(),
        }
    }

    // Boundary rate of a block = the cheapest effective rate among its txs.
    fn boundary(proj: &Projection, block: usize) -> f64 {
        proj.blocks[block]
            .iter()
            .map(|uid| proj.effective_rates[uid])
            .fold(f64::INFINITY, f64::min)
    }

    #[test]
    fn independent_txs_rank_by_descending_feerate() {
        // Three independent txs, distinct fee rates; all fit one block.
        // 1000 wu = 250 vB each. Rates: 40, 20, 4 sat/vB.
        let txs = vec![
            tx(0, 10_000, 1000, &[]), // 40 sat/vB
            tx(1, 5_000, 1000, &[]),  // 20 sat/vB
            tx(2, 1_000, 1000, &[]),  // 4  sat/vB
        ];
        let proj = project(txs);
        assert_eq!(proj.blocks[0], vec![0, 1, 2], "packed best-rate first");
        assert!((proj.effective_rates[&0] - 40.0).abs() < 1e-6);
        assert!((proj.effective_rates[&2] - 4.0).abs() < 1e-6);
    }

    #[test]
    fn cpfp_parent_pulled_up_by_child() {
        // Parent pays 1 sat/vB alone; high-fee child lifts the package.
        // parent: fee 250, weight 1000 (250 vB) -> 1 sat/vB own.
        // child:  fee 25_000, weight 1000 (250 vB) -> 100 sat/vB own, depends on parent.
        // package rate = (250+25000) / (250+250) = 25250/500 = 50.5 sat/vB.
        let txs = vec![tx(0, 250, 1000, &[]), tx(1, 25_000, 1000, &[0])];
        let proj = project(txs);
        // Both selected, parent (0) before child (1).
        assert_eq!(proj.blocks[0], vec![0, 1]);
        // Parent's effective rate is lifted to the package rate, not its lonely 1 sat/vB.
        assert!(proj.effective_rates[&0] > 40.0, "parent lifted by CPFP");
        assert!(
            (proj.effective_rates[&0] - proj.effective_rates[&1]).abs() < 1e-6,
            "package members share the cluster rate"
        );
    }

    #[test]
    fn overflow_spills_to_next_block() {
        // Two txs that can't share a block: each ~ full block weight.
        // With MAX_BLOCKS >= 2, the lower-rate one lands in block 1.
        let almost_full = MAX_BLOCK_WEIGHT - BLOCK_RESERVED_WEIGHT - 10;
        let txs = vec![
            tx(0, 10_000, almost_full, &[]), // higher rate
            tx(1, 1_000, almost_full, &[]),  // lower rate
        ];
        let proj = project(txs);
        assert_eq!(proj.blocks[0], vec![0]);
        assert_eq!(proj.blocks[1], vec![1]);
        assert!(
            boundary(&proj, 0) > boundary(&proj, 1),
            "block 0 boundary is higher"
        );
    }

    #[test]
    fn empty_input_yields_no_blocks() {
        let proj = project(vec![]);
        assert!(proj.blocks.is_empty());
        assert!(proj.effective_rates.is_empty());
    }
}
