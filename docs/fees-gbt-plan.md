# /fees GBT Fee Estimation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `/fees` endpoint that serves CPFP-aware recommended fee rates, computed from a Bitcoin Core-style ancestor-package block projection over the live in-memory mempool.

**Architecture:** A pure, unit-testable projection module (`src/gbt.rs`) implements Core's block-assembly *selection* algorithm. An adapter/orchestration module (`src/fees.rs`) snapshots `MempoolState.txs`, runs the projection off the async executor (`spawn_blocking`), extracts fee tiers, and caches a `FeeEstimate` on shared state. The sync loop triggers a throttled recompute after each healthy tick; `/fees` serves the cached estimate, gated on `caught_up`.

**Tech Stack:** Rust, tokio (`spawn_blocking`), axum, `std::collections::BinaryHeap` (no new dependencies).

## Global Constraints

- **Algorithm provenance:** independent implementation of Bitcoin Core's `BlockAssembler` (`src/node/miner.cpp`, `addPackageTxs`), MIT-licensed. Cite Bitcoin Core only. Do NOT reference any third-party explorer project in code, comments, docs, or commit messages.
- **No new crate dependencies.** Use `std` only for the algorithm (`BinaryHeap`, `HashMap`, `HashSet`).
- **Fee rate unit:** sat/vB, where `vsize = weight / 4`, i.e. `sat/vB = 4 * fee_sats / weight`.
- **Sigops:** approximated (not modelled). **Accelerations:** not modelled.
- **Match existing style:** pure-core module mirrors `src/sync/decision.rs` (named `const`s with rationale comments, `#[cfg(test)] mod tests`). Config knobs follow the clap/env pattern in `src/config.rs`.
- **Package/DAG scale:** Bitcoin Core's default policy caps ancestor/descendant counts at 25, so ancestor closures are small; recursive closure building is safe.

---

### Task 1: Pure projection core (`src/gbt.rs`)

**Files:**
- Create: `src/gbt.rs`
- Modify: `src/main.rs:1-8` (add `mod gbt;`)

**Interfaces:**
- Produces:
  - `pub struct GbtTx { pub uid: u32, pub order: u32, pub fee: u64, pub weight: u32, pub parents: Vec<u32> }`
  - `pub struct Projection { pub blocks: Vec<Vec<u32>>, pub effective_rates: std::collections::HashMap<u32, f64> }`
  - `pub fn project(txs: Vec<GbtTx>) -> Projection`
  - `pub const MAX_BLOCK_WEIGHT: u32`, `pub const BLOCK_RESERVED_WEIGHT: u32`, `pub const MAX_BLOCKS: usize`
- Requires: `uid`s are dense `0..txs.len()` and each `GbtTx` sits at index `uid` (guaranteed by the Task 2 adapter).

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `src/gbt.rs` (module body written in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn tx(uid: u32, fee: u64, weight: u32, parents: &[u32]) -> GbtTx {
        GbtTx { uid, order: uid, fee, weight, parents: parents.to_vec() }
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
        let txs = vec![
            tx(0, 250, 1000, &[]),
            tx(1, 25_000, 1000, &[0]),
        ];
        let proj = project(txs);
        // Both selected, parent (0) before child (1).
        assert_eq!(proj.blocks[0], vec![0, 1]);
        // Parent's effective rate is lifted to the package rate, not its lonely 1 sat/vB.
        assert!(proj.effective_rates[&0] > 40.0, "parent lifted by CPFP");
        assert!((proj.effective_rates[&0] - proj.effective_rates[&1]).abs() < 1e-6,
            "package members share the cluster rate");
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
        assert!(boundary(&proj, 0) > boundary(&proj, 1), "block 0 boundary is higher");
    }

    #[test]
    fn empty_input_yields_no_blocks() {
        let proj = project(vec![]);
        assert!(proj.blocks.is_empty());
        assert!(proj.effective_rates.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib gbt`
Expected: FAIL to compile — `GbtTx`, `Projection`, `project` not defined.

- [ ] **Step 3: Write the module**

Write the top of `src/gbt.rs` (above the `tests` module from Step 1):

```rust
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
    pub fee: u64,    // sats
    pub weight: u32, // weight units
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
        return Projection { blocks: Vec::new(), effective_rates: HashMap::new() };
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
        heap.push(HeapItem { score: score_of(uid, &audits, &used), order: audits[uid as usize].order, uid });
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
            heap.push(HeapItem { score: cur, order: audits[uid as usize].order, uid });
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
                heap.push(HeapItem { score: score_of(o, &audits, &used), order: audits[o as usize].order, uid: o });
            }
            overflow.clear();
        }
    }

    if !cur_block.is_empty() {
        blocks.push(cur_block);
    }

    Projection { blocks, effective_rates }
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
fn refresh_descendants(pkg: &[u32], audits: &[Audit], used: &[bool], heap: &mut BinaryHeap<HeapItem>) {
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
            heap.push(HeapItem { score: score_of(d, audits, used), order: audits[d as usize].order, uid: d });
        }
        for &c in &audits[d as usize].children {
            if seen.insert(c) {
                stack.push(c);
            }
        }
    }
}
```

Then add `mod gbt;` to `src/main.rs` after `mod config;` (keep alphabetical-ish grouping):

```rust
mod config;
mod gbt;
mod http;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib gbt`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint and commit**

Run: `cargo clippy --all-targets`
Expected: no warnings in `gbt.rs`.

```bash
git add src/gbt.rs src/main.rs
git commit -m "feat(fees): pure Core-style GBT block-projection core"
```

---

### Task 2: Fee-tier adapter and estimate (`src/fees.rs`)

**Files:**
- Create: `src/fees.rs`
- Modify: `src/main.rs` (add `mod fees;`)

**Interfaces:**
- Consumes: `crate::gbt::{GbtTx, Projection, project}` (Task 1).
- Produces:
  - `pub struct FeeEstimate { pub fastest_fee: f64, pub half_hour_fee: f64, pub hour_fee: f64, pub economy_fee: f64, pub minimum_fee: f64, pub as_of: u64 }` (derives `Debug, Clone, Serialize`).
  - `pub fn compute_estimate(snapshot: &[(bitcoin::Txid, u64, u32, Vec<bitcoin::Txid>)], min_fee_sat_vb: f64) -> FeeEstimate`
  - `pub fn tiers_from_projection(proj: &Projection, min_fee_sat_vb: f64, as_of: u64) -> FeeEstimate`
- Snapshot tuple = `(txid, fee_sats, weight, depends)`.

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `src/fees.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::gbt::Projection;
    use std::collections::HashMap;

    #[test]
    fn empty_projection_falls_back_to_minimum() {
        let proj = Projection { blocks: vec![], effective_rates: HashMap::new() };
        let est = tiers_from_projection(&proj, 3.0, 42);
        assert_eq!(est.fastest_fee, 3.0);
        assert_eq!(est.economy_fee, 3.0);
        assert_eq!(est.minimum_fee, 3.0);
        assert_eq!(est.as_of, 42);
    }

    #[test]
    fn tiers_read_off_block_boundaries_and_floor_at_minimum() {
        // Three projected blocks with descending boundary rates 50, 20, 5.
        let mut rates = HashMap::new();
        rates.insert(0u32, 50.0);
        rates.insert(1u32, 20.0);
        rates.insert(2u32, 5.0);
        let proj = Projection { blocks: vec![vec![0], vec![1], vec![2]], effective_rates: rates };
        // min_fee 8 -> the 5 sat/vB economy boundary is floored up to 8.
        let est = tiers_from_projection(&proj, 8.0, 0);
        assert_eq!(est.fastest_fee, 50.0);   // block 0
        // half_hour clamps to last available block (index 2) since < 3 blocks after 0..
        assert_eq!(est.hour_fee, 5.0_f64.max(8.0)); // floored at minimum
        assert_eq!(est.economy_fee, 8.0);    // 5 floored to min 8
        assert_eq!(est.minimum_fee, 8.0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib fees`
Expected: FAIL to compile — `tiers_from_projection` not defined.

- [ ] **Step 3: Write the module**

Write the top of `src/fees.rs`:

```rust
//! Turns the pure `gbt` projection into cached, servable fee tiers.
//!
//! Responsibilities: adapt a mempool snapshot into `gbt::GbtTx` inputs, run the
//! projection, and read recommended fee tiers off the projected-block
//! boundaries. Kept separate from `gbt` (the algorithm) so tier policy can move
//! without touching the packing core.

use crate::gbt::{self, GbtTx, Projection};
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Recommended fee tiers (sat/vB) plus the unix second they were computed.
#[derive(Debug, Clone, Serialize)]
pub struct FeeEstimate {
    pub fastest_fee: f64,   // block 0 boundary (~next block)
    pub half_hour_fee: f64, // ~block 2 boundary (~3 blocks)
    pub hour_fee: f64,      // ~block 5 boundary (~6 blocks)
    pub economy_fee: f64,   // last projected block boundary, floored at minimum
    pub minimum_fee: f64,   // relay floor (mempoolminfee)
    pub as_of: u64,
}

/// Block index used for each time tier (0-based). ~10-min blocks.
const HALF_HOUR_BLOCK: usize = 2;
const HOUR_BLOCK: usize = 5;

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Cheapest effective rate among a projected block's txs (its bottom boundary).
fn block_boundary(proj: &Projection, block: usize) -> Option<f64> {
    proj.blocks.get(block).map(|b| {
        b.iter()
            .map(|uid| proj.effective_rates.get(uid).copied().unwrap_or(0.0))
            .fold(f64::INFINITY, f64::min)
    })
}

/// Read fee tiers off the projected-block boundaries, flooring every tier at the
/// relay minimum. With fewer projected blocks than a tier's index, that tier
/// clamps to the last block's boundary.
pub fn tiers_from_projection(proj: &Projection, min_fee_sat_vb: f64, as_of: u64) -> FeeEstimate {
    let floor = min_fee_sat_vb;
    if proj.blocks.is_empty() {
        return FeeEstimate {
            fastest_fee: floor,
            half_hour_fee: floor,
            hour_fee: floor,
            economy_fee: floor,
            minimum_fee: floor,
            as_of,
        };
    }
    let last = proj.blocks.len() - 1;
    let at = |idx: usize| -> f64 {
        let idx = idx.min(last);
        block_boundary(proj, idx).unwrap_or(floor).max(floor)
    };
    FeeEstimate {
        fastest_fee: at(0),
        half_hour_fee: at(HALF_HOUR_BLOCK),
        hour_fee: at(HOUR_BLOCK),
        economy_fee: block_boundary(proj, last).unwrap_or(floor).max(floor),
        minimum_fee: floor,
        as_of,
    }
}

/// Map a mempool snapshot to `gbt` inputs. Assigns dense uids and resolves each
/// tx's `depends` to parent uids (dropping any parent not in the set — the same
/// in-mempool-only ancestor semantics Core uses).
pub fn snapshot_to_gbt(snapshot: &[(Txid, u64, u32, Vec<Txid>)]) -> Vec<GbtTx> {
    let mut uid_of: HashMap<Txid, u32> = HashMap::with_capacity(snapshot.len());
    for (i, (txid, _, _, _)) in snapshot.iter().enumerate() {
        uid_of.insert(*txid, i as u32);
    }
    snapshot
        .iter()
        .enumerate()
        .map(|(i, (txid, fee, weight, depends))| {
            let parents = depends.iter().filter_map(|d| uid_of.get(d).copied()).collect();
            GbtTx {
                uid: i as u32,
                // Deterministic tie-breaker from the leading txid bytes.
                order: u32::from_be_bytes(txid.to_byte_array()[0..4].try_into().unwrap()),
                fee: *fee,
                weight: *weight,
                parents,
            }
        })
        .collect()
}

/// Full pipeline: snapshot -> projection -> tiers, stamped with the current time.
pub fn compute_estimate(
    snapshot: &[(Txid, u64, u32, Vec<Txid>)],
    min_fee_sat_vb: f64,
) -> FeeEstimate {
    let gbt_txs = snapshot_to_gbt(snapshot);
    let proj = gbt::project(gbt_txs);
    tiers_from_projection(&proj, min_fee_sat_vb, now_unix())
}
```

Add `mod fees;` to `src/main.rs` after `mod config;`/`mod gbt;`:

```rust
mod config;
mod fees;
mod gbt;
mod http;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib fees`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/fees.rs src/main.rs
git commit -m "feat(fees): snapshot adapter and projected-block fee tiers"
```

---

### Task 3: Cache the estimate on shared state (`src/mempool.rs`)

**Files:**
- Modify: `src/mempool.rs:78-99` (`MempoolState` struct + `new`)

**Interfaces:**
- Consumes: `crate::fees::FeeEstimate` (Task 2).
- Produces: `MempoolState.fee_estimate: Option<FeeEstimate>`, default `None`.

- [ ] **Step 1: Write the failing test**

Add to `src/mempool.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn new_state_has_no_fee_estimate() {
        let s = MempoolState::new(Network::Bitcoin);
        assert!(s.fee_estimate.is_none());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib mempool::tests::new_state_has_no_fee_estimate`
Expected: FAIL to compile — no field `fee_estimate`.

- [ ] **Step 3: Add the field**

In `src/mempool.rs`, add to the `MempoolState` struct (after `last_sync_ok`):

```rust
    pub last_sync_ok: Option<SystemTime>,
    /// Most recent computed fee estimate, or `None` before the first recompute.
    pub fee_estimate: Option<crate::fees::FeeEstimate>,
```

And initialize it in `MempoolState::new` (after `last_sync_ok: None,`):

```rust
            last_sync_ok: None,
            fee_estimate: None,
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib mempool::tests::new_state_has_no_fee_estimate`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/mempool.rs
git commit -m "feat(fees): hold latest FeeEstimate on MempoolState"
```

---

### Task 4: Config knob and sync wiring (`src/config.rs`, `src/sync/mod.rs`, `src/main.rs`)

**Files:**
- Modify: `src/config.rs:22-38` (add `fee_recompute_min_interval`), `:40-69` (CLI arg), `:81-99` (build)
- Modify: `src/sync/mod.rs:113-121` (`SyncConfig` field)
- Modify: `src/main.rs` (populate `SyncConfig`)

**Interfaces:**
- Produces: `Config.fee_recompute_min_interval: Duration`; `SyncConfig.fee_recompute_min_interval: Duration`.

- [ ] **Step 1: Write the failing test**

Add a test module to `src/config.rs` (file currently has none):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fee_recompute_interval_floored_at_poll() {
        // A tiny requested interval is floored at the poll interval.
        let d = fee_recompute_interval(Some(100), 2000);
        assert_eq!(d, Duration::from_millis(2000));
        // A larger requested interval is honoured.
        let d = fee_recompute_interval(Some(9000), 2000);
        assert_eq!(d, Duration::from_millis(9000));
        // Default (None) is 5000, still floored at poll.
        let d = fee_recompute_interval(None, 2000);
        assert_eq!(d, Duration::from_millis(5000));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib config`
Expected: FAIL to compile — `fee_recompute_interval` not defined.

- [ ] **Step 3: Add the knob and helper**

In `src/config.rs`, add the field to `Config` (after `tick_budget`):

```rust
    pub tick_budget: Duration,
    /// Minimum time between fee-estimate recomputes. Default 5000ms, floored at
    /// `poll_interval` so it never runs more often than the mempool refreshes.
    pub fee_recompute_min_interval: Duration,
```

Add the CLI arg to `Cli` (after `tick_budget_ms`):

```rust
    #[arg(long, env = "TICK_BUDGET_MS")]
    tick_budget_ms: Option<u64>,
    /// Minimum ms between fee-estimate recomputes (default 5000, floored at POLL_INTERVAL_MS).
    #[arg(long, env = "FEE_RECOMPUTE_MIN_INTERVAL_MS")]
    fee_recompute_min_interval_ms: Option<u64>,
```

Add a free helper (above `impl Config`) so it's unit-testable:

```rust
/// Resolve the fee-recompute interval: default 5000ms, floored at the poll
/// interval so recomputes never outpace the mempool refresh.
fn fee_recompute_interval(requested_ms: Option<u64>, poll_ms: u64) -> Duration {
    Duration::from_millis(requested_ms.unwrap_or(5000).max(poll_ms))
}
```

Populate it in `Config::from_env` (inside the returned `Config { .. }`, after `tick_budget: ...,`):

```rust
            fee_recompute_min_interval: fee_recompute_interval(
                cli.fee_recompute_min_interval_ms,
                cli.poll_interval_ms,
            ),
```

- [ ] **Step 4: Add the `SyncConfig` field**

In `src/sync/mod.rs`, add to `SyncConfig` (after `tick_budget`):

```rust
    pub tick_budget: Duration,
    /// Minimum time between fee-estimate recomputes (see `config`).
    pub fee_recompute_min_interval: Duration,
```

In `src/main.rs`, add to the `sync::SyncConfig { .. }` literal (after `tick_budget: cfg.tick_budget,`):

```rust
        tick_budget: cfg.tick_budget,
        fee_recompute_min_interval: cfg.fee_recompute_min_interval,
```

- [ ] **Step 5: Run test and full build to verify**

Run: `cargo test --lib config` then `cargo build`
Expected: config test PASS; build succeeds (any `sim` server building `SyncConfig` also needs the field — see note below).

> If `cargo build --features simulation` fails on a second `SyncConfig` literal in `src/sim/`, add `fee_recompute_min_interval: <same as tick or a default>` there too. Grep: `grep -rn "SyncConfig {" src`.

- [ ] **Step 6: Commit**

```bash
git add src/config.rs src/sync/mod.rs src/main.rs
git commit -m "feat(fees): FEE_RECOMPUTE_MIN_INTERVAL_MS config knob"
```

---

### Task 5: Throttled recompute in the sync loop (`src/sync/mod.rs`)

**Files:**
- Modify: `src/sync/mod.rs` (`run`, `steady_tick` signatures + recompute helper)

**Interfaces:**
- Consumes: `crate::fees::compute_estimate` (Task 2), `SyncConfig.fee_recompute_min_interval` (Task 4).
- Produces: writes `MempoolState.fee_estimate` after healthy ticks and successful bulk resyncs.

- [ ] **Step 1: Add the recompute helper**

Add near the other free helpers in `src/sync/mod.rs`:

```rust
/// Recompute the fee estimate if at least `min_interval` has passed since the
/// last recompute. Snapshots the tx fields the projector needs under a read
/// lock, runs the CPU-bound projection on the blocking pool, then stores the
/// result. `last` is advanced only when a recompute actually runs.
async fn maybe_recompute_fees(
    state: &SharedState,
    last: &mut Instant,
    min_interval: Duration,
) {
    if last.elapsed() < min_interval {
        return;
    }
    let (snapshot, min_fee) = {
        let g = read_state(state);
        let snapshot: Vec<(Txid, u64, u32, Vec<Txid>)> = g
            .txs
            .iter()
            .map(|(id, tx)| (*id, tx.fee.to_sat(), tx.weight, tx.depends.clone()))
            .collect();
        (snapshot, g.mempool_min_fee_sat_vb)
    };
    match tokio::task::spawn_blocking(move || {
        crate::fees::compute_estimate(&snapshot, min_fee)
    })
    .await
    {
        Ok(estimate) => {
            let mut g = write_state(state);
            g.fee_estimate = Some(estimate);
        }
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "fee recompute task failed");
        }
    }
    *last = Instant::now();
}
```

- [ ] **Step 2: Thread the timer through `run` and `steady_tick`**

In `run`, after `let mut last_bulk_resync = ...;` add:

```rust
    // Force a fee recompute on the first steady tick by backdating the timer.
    let mut last_fee_recompute = Instant::now()
        .checked_sub(cfg.fee_recompute_min_interval)
        .unwrap_or_else(Instant::now);
```

Change the `steady_tick(...)` call to pass it:

```rust
        steady_tick(
            &mut rpc,
            &state,
            &cfg,
            &mut caught_up_prev,
            &mut last_bulk_resync,
            &mut last_fee_recompute,
        )
        .await;
```

Update `steady_tick`'s signature (add the parameter):

```rust
async fn steady_tick<R: MempoolRpc + Clone + Send + Sync + 'static>(
    rpc: &mut R,
    state: &SharedState,
    cfg: &SyncConfig,
    caught_up_prev: &mut bool,
    last_bulk_resync: &mut Instant,
    last_fee_recompute: &mut Instant,
) {
```

- [ ] **Step 3: Trigger recompute after healthy applies**

In `steady_tick`, in the desync `BulkResync` arm, after the `Some(count) => apply_bulk_success(...)` line, recompute before the `return`. Replace the `match bulk_resync(...)` block with:

```rust
                match bulk_resync(rpc, state).await {
                    Some(count) => {
                        apply_bulk_success(state, caught_up_prev, count);
                        maybe_recompute_fees(state, last_fee_recompute, cfg.fee_recompute_min_interval).await;
                    }
                    None => set_synced(state, caught_up_prev, false, "resync_failed", 0),
                }
```

At the very end of `steady_tick`, after the final apply `{ ... }` block closes, add:

```rust
    // Refresh the fee estimate from the just-applied mempool (throttled).
    maybe_recompute_fees(state, last_fee_recompute, cfg.fee_recompute_min_interval).await;
}
```

> Note: this runs on the normal (non-early-return) path. Ticks that returned early on an RPC error or cooldown intentionally skip the recompute; the next healthy tick refreshes it.

- [ ] **Step 4: Verify the build and existing tests**

Run: `cargo test --features simulation`
Expected: PASS (existing suite unchanged; new modules compile).

- [ ] **Step 5: Commit**

```bash
git add src/sync/mod.rs
git commit -m "feat(fees): throttled fee recompute after healthy sync ticks"
```

---

### Task 6: `/fees` HTTP route (`src/http.rs`)

**Files:**
- Modify: `src/http.rs` (add route + handler)

**Interfaces:**
- Consumes: `MempoolState.fee_estimate` (Task 3), `MempoolState.caught_up`.
- Produces: `GET /fees` → `200` `FeeEstimate` JSON when caught up and an estimate exists, else `503`.

- [ ] **Step 1: Add the handler and route**

In `src/http.rs`, extend the imports:

```rust
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
```

Register the route in `router` (chain onto the existing `.route("/health", ...)`):

```rust
        .route("/health", get(health))
        .route("/fees", get(fees))
```

Add the handler (after `health`):

```rust
/// Serve the cached fee estimate — but only when the sync layer vouches for the
/// mempool (`caught_up`). Returns `503` before the first estimate or whenever the
/// view is known to be behind, so we never serve a number we can't stand behind.
async fn fees(State(state): State<SharedState>) -> impl IntoResponse {
    let s = read_state(&state);
    match &s.fee_estimate {
        Some(est) if s.caught_up => Json(est.clone()).into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
```

- [ ] **Step 2: Verify the build**

Run: `cargo build`
Expected: succeeds.

- [ ] **Step 3: Manual smoke check (optional, needs a node)**

Run: `just run` in one shell, then in another:
`curl -s localhost:8080/fees | jq`
Expected: `503` until the first sync completes, then a JSON object with `fastest_fee`, `half_hour_fee`, `hour_fee`, `economy_fee`, `minimum_fee`, `as_of`.

- [ ] **Step 4: Commit**

```bash
git add src/http.rs
git commit -m "feat(fees): GET /fees route gated on caught_up"
```

---

### Task 7: End-to-end sim test (`src/sync/sim_tests.rs`)

**Files:**
- Modify: `src/sync/sim_tests.rs` (add one test)

**Interfaces:**
- Consumes: existing sim harness (`MockNode`, the sync entry used by other sim tests), `MempoolState.fee_estimate`.

- [ ] **Step 1: Inspect the existing sim tests for the harness entry points**

Run: `grep -n "fn \|MockNode::new\|SyncConfig\|steady_tick\|run(" src/sync/sim_tests.rs | head -40`
Expected: shows how existing tests build a `MockNode`, a `SyncConfig`, and drive ticks. Mirror that exact setup in the new test (reuse the same helper if one exists).

- [ ] **Step 2: Write the test**

Add to `src/sync/sim_tests.rs` (adapt `build_sync_cfg()` / tick-driving to match the file's existing helpers found in Step 1):

```rust
    #[tokio::test]
    async fn fee_estimate_is_populated_and_monotone() {
        // Build a populated mock mempool and drive enough ticks for one healthy
        // apply + fee recompute. (Mirror the existing tests' harness setup.)
        let cfg = ChurnConfig {
            arrivals_per_tick: 50,
            evictions_per_tick: 40,
            fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
        };
        let node = MockNode::new(99, 5_000, cfg);
        let (state, _handles) = run_sim_until_caught_up(node).await; // helper per Step 1

        let g = read_state(&state);
        let est = g.fee_estimate.clone().expect("estimate computed after catch-up");
        // Tiers descend by confirmation speed and stay finite/non-negative.
        assert!(est.fastest_fee.is_finite() && est.fastest_fee >= est.minimum_fee);
        assert!(est.fastest_fee >= est.half_hour_fee - 1e-9);
        assert!(est.half_hour_fee >= est.hour_fee - 1e-9);
        assert!(est.hour_fee >= est.minimum_fee - 1e-9);
        assert!(est.economy_fee >= est.minimum_fee - 1e-9);
    }
```

> If no `run_sim_until_caught_up` helper exists, drive `steady_tick` in a loop until `read_state(&state).caught_up` is true (bounded iterations), exactly as the neighbouring tests do, then assert on `fee_estimate`. Keep the monotonicity assertions unchanged.

- [ ] **Step 3: Run the test**

Run: `cargo test --features simulation fee_estimate_is_populated_and_monotone`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/sync/sim_tests.rs
git commit -m "test(fees): sim test asserts /fees estimate is populated and monotone"
```

---

### Task 8: Document the algorithm and endpoint in the README

**Files:**
- Modify: `README.md` — the "Why the sync layer feeds this cleanly" subsection (~`:419-436`) and the `/health fields` area (~`:647`).

**Interfaces:** none (docs only). Must stay consistent with the implemented behaviour: **stateless recompute** (not incremental), **`/fees` gated on `caught_up`**, tiers off block boundaries 0/2/5 + last.

- [ ] **Step 1: Correct the "runs incrementally" claim**

The README currently states the estimator "runs incrementally … re-derives it against each mempool delta." The implementation is a **stateless recompute from a fresh snapshot**. Replace the second bullet under "Why the sync layer feeds this cleanly" (the `**It runs incrementally.**` bullet) with:

```markdown
- **It recomputes from a fresh snapshot.** Rather than maintain a second,
  long-lived copy of the mempool, the estimator rebuilds its working set from the
  live cache on each run and re-derives the projection from scratch — so there is
  no separate structure to keep in sync and no stale package data to carry
  forward. The recompute runs off the async path (on a blocking thread) and is
  throttled (`FEE_RECOMPUTE_MIN_INTERVAL_MS`, default 5s), so a fast-churning
  mempool can't spin it. Fed by the fresh sync loop and ZMQ block-push, the fee
  number still tracks reality with minimal latency.
```

Leave the first bullet (`**MempoolTx captures the package data.**`) and the closing "gated on `caught_up`" sentence as-is — both already match the implementation.

- [ ] **Step 2: Verify the algorithm section is accurate**

Read `README.md:310-436`. Confirm each of these still matches the code and fix any drift inline:
- 4,000,000 WU block limit with a coinbase reserve → `MAX_BLOCK_WEIGHT` / `BLOCK_RESERVED_WEIGHT` in `src/gbt.rs`. ✓
- Ranking key = ancestor/package fee rate, `min(own, package)` → `score_of`. ✓
- Greedy: take best package, include not-yet-used ancestors, then re-score descendants → `project_with` + `refresh_descendants`. ✓
- Tier table: next=block 1, ~30 min=block 3, ~1 hour=block 6 (1-based) = indices 0/2/5 (0-based) in `src/fees.rs`; economy=last block; minimum=`mempoolminfee`. ✓
- Sigops / descendant limits / RBF explicitly not modelled. ✓

- [ ] **Step 3: Document the `/fees` endpoint fields**

After the `### `/health` fields` subsection, add a sibling subsection:

```markdown
### `/fees` fields

`GET /fees` returns the cached fee estimate, in **sat/vB**. It is gated on
`caught_up`: before the first successful sync, or whenever `/health` reports the
mempool is out of sync, it returns `503` rather than a number it can't vouch for.

| Field           | Meaning                                                       |
|-----------------|---------------------------------------------------------------|
| `fastest_fee`   | boundary rate of projected block 1 (~next block)              |
| `half_hour_fee` | boundary rate of projected block ~3 (~30 min)                 |
| `hour_fee`      | boundary rate of projected block ~6 (~1 hour)                 |
| `economy_fee`   | boundary rate of the last projected block, floored at minimum |
| `minimum_fee`   | mempool min relay fee (`mempoolminfee`)                       |
| `as_of`         | unix seconds when the estimate was computed                   |
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: reconcile fee-algorithm README with implementation; document /fees"
```

---

## Self-Review

**Spec coverage** (against `docs/fees-gbt-design.md`):
- Goal / `/fees` endpoint → Tasks 6, 8. ✓
- Provenance (Core MIT, no third-party ref) → Global Constraints + Task 1 header. ✓
- `src/gbt.rs` pure algorithm (score, greedy pack, CPFP descendant re-score) → Task 1. ✓
- `src/fees.rs` adapter + tiers + `compute_estimate` → Task 2. ✓
- `MempoolState.fee_estimate` → Task 3. ✓
- Throttled `spawn_blocking` recompute after apply + bulk → Task 5. ✓
- `FEE_RECOMPUTE_MIN_INTERVAL_MS` config, floored at poll → Task 4. ✓
- `/fees` gated on `caught_up`, `503` otherwise → Task 6. ✓
- Sigops approximated, accelerations excluded → Global Constraints (no code models them). ✓
- Tests: gbt unit (order/CPFP/boundary/empty), fees tiers (mapping/floor/empty), sim monotone → Tasks 1, 2, 7. ✓
- README algorithm + endpoint docs → Task 8. ✓

**Design-doc deviation (intentional, noted):** the design's testing section proposed extending `sim/mock_node.rs` to emit CPFP chains. CPFP correctness is instead covered directly at the `gbt.rs` unit level (`cpfp_parent_pulled_up_by_child`), and the sim test asserts populated + monotone tiers without mock-node surgery. This keeps the sim change minimal; update the design doc's testing note to match.

**Placeholder scan:** no TBD/TODO; every code step contains complete code. Task 7 leaves the harness-wiring call (`run_sim_until_caught_up`) to be matched against the existing sim tests, with an explicit fallback described — this is adaptation to existing code, not a placeholder.

**Type consistency:** `GbtTx`/`Projection`/`project` (Task 1) are consumed with matching signatures in Task 2; `FeeEstimate` fields are identical across Tasks 2, 3, 6, 8; `compute_estimate(&[(Txid,u64,u32,Vec<Txid>)], f64)` matches the snapshot built in Task 5; `fee_recompute_min_interval` name is consistent across Tasks 4–5.
