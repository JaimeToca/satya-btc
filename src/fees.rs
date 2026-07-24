//! Turns the pure `gbt` projection into cached, servable fee tiers.
//!
//! Responsibilities: adapt a mempool snapshot into `gbt::GbtTx` inputs, run the
//! projection, and read recommended fee tiers off a weight histogram of the
//! projection's CPFP-effective rates. Kept separate from `gbt` (the algorithm)
//! so tier policy can move without touching the packing core.

use crate::gbt::{self, GbtTx};
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use serde::Serialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Recommended fee tiers (sat/vB) plus the unix second they were computed.
#[derive(Debug, Clone, Serialize)]
pub struct FeeEstimate {
    pub next_block: f64,      // depth 1 (~next block)
    pub within_3_blocks: f64, // depth 3 (~30 min)
    pub within_6_blocks: f64, // depth 6 (~1 hour)
    pub horizon: f64,         // deepest projected block, floored at relay_floor
    pub relay_floor: f64,     // mempool min relay fee (mempoolminfee)
    pub computed_at: u64,     // unix seconds the estimate was computed
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Block weight used to convert cumulative weight into a projected-block depth.
/// Matches the per-block tx-weight budget the projection actually packs
/// against (consensus weight limit minus the coinbase reserve), rather than
/// the full consensus limit, removing a ~0.1% optimistic bias in tier depth.
const TIER_BLOCK_WEIGHT: u64 = (gbt::MAX_BLOCK_WEIGHT - gbt::BLOCK_RESERVED_WEIGHT) as u64;

// Projected-block depth (1-based) each time tier corresponds to (~10-min blocks).
const FASTEST_DEPTH: u64 = 1;
const HALF_HOUR_DEPTH: u64 = 3;
const HOUR_DEPTH: u64 = 6;
const ECONOMY_DEPTH: u64 = gbt::MAX_BLOCKS as u64;

/// Fee to confirm within `depth_blocks` blocks: over txs sorted by effective rate
/// (highest first), the rate at which cumulative weight first reaches
/// `depth_blocks * TIER_BLOCK_WEIGHT`. If the mempool holds less weight than that,
/// anything at the relay floor confirms, so the tier is the floor. Non-increasing
/// in `depth_blocks` by construction.
fn tier_at_depth(sorted_desc: &[(f64, u32)], depth_blocks: u64, floor: f64) -> f64 {
    let threshold = depth_blocks.saturating_mul(TIER_BLOCK_WEIGHT);
    let mut cum: u64 = 0;
    for &(rate, weight) in sorted_desc {
        cum += weight as u64;
        if cum >= threshold {
            return rate.max(floor);
        }
    }
    floor
}

/// Recommended tiers from (effective_rate_sat_vb, weight) pairs for every tx.
/// Tiers are read off a weight histogram of CPFP-effective rates: to confirm
/// within N blocks you must outbid everything below the top N block-weights of
/// rate-sorted mempool. Monotone (fastest >= half_hour >= hour >= economy),
/// each floored at the relay minimum.
pub fn recommended_tiers(
    mut rate_weights: Vec<(f64, u32)>,
    min_fee_sat_vb: f64,
    computed_at: u64,
) -> FeeEstimate {
    let floor = min_fee_sat_vb;
    rate_weights.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    FeeEstimate {
        next_block: tier_at_depth(&rate_weights, FASTEST_DEPTH, floor),
        within_3_blocks: tier_at_depth(&rate_weights, HALF_HOUR_DEPTH, floor),
        within_6_blocks: tier_at_depth(&rate_weights, HOUR_DEPTH, floor),
        horizon: tier_at_depth(&rate_weights, ECONOMY_DEPTH, floor),
        relay_floor: floor,
        computed_at,
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
            let parents = depends
                .iter()
                .filter_map(|d| uid_of.get(d).copied())
                .collect();
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

/// Full pipeline: snapshot -> projection -> histogram tiers, stamped with now.
pub fn compute_estimate(
    snapshot: &[(Txid, u64, u32, Vec<Txid>)],
    min_fee_sat_vb: f64,
) -> FeeEstimate {
    let gbt_txs = snapshot_to_gbt(snapshot);
    let proj = gbt::project(gbt_txs);
    // Join each tx's CPFP-effective rate (from the projection) with its weight.
    // uid == position in `snapshot`.
    let rate_weights: Vec<(f64, u32)> = snapshot
        .iter()
        .enumerate()
        .filter_map(|(uid, (_txid, _fee, weight, _depends))| {
            proj.effective_rates
                .get(&(uid as u32))
                .map(|&rate| (rate, *weight))
        })
        .collect();
    recommended_tiers(rate_weights, min_fee_sat_vb, now_unix())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_mempool_all_tiers_at_minimum() {
        let est = recommended_tiers(vec![], 3.0, 42);
        assert_eq!(est.next_block, 3.0);
        assert_eq!(est.within_3_blocks, 3.0);
        assert_eq!(est.within_6_blocks, 3.0);
        assert_eq!(est.horizon, 3.0);
        assert_eq!(est.relay_floor, 3.0);
        assert_eq!(est.computed_at, 42);
    }

    #[test]
    fn tiers_descend_with_block_depth() {
        let bw = crate::gbt::MAX_BLOCK_WEIGHT;
        // One block-weight at each of 100/50/20/5 sat/vB (4 blocks of backlog),
        // deliberately unsorted on input.
        let rw = vec![(5.0, bw), (100.0, bw), (20.0, bw), (50.0, bw)];
        let est = recommended_tiers(rw, 1.0, 0);
        assert_eq!(est.next_block, 100.0); // depth 1 -> top block
        assert_eq!(est.within_3_blocks, 20.0); // depth 3 -> 3rd-highest block
        assert_eq!(est.within_6_blocks, 1.0); // depth 6 -> only 4 blocks exist -> floor
        assert_eq!(est.horizon, 1.0); // depth 8 -> floor
        assert!(est.next_block >= est.within_3_blocks);
        assert!(est.within_3_blocks >= est.within_6_blocks);
        assert!(est.within_6_blocks >= est.horizon);
    }

    #[test]
    fn tiers_floored_at_minimum() {
        let bw = crate::gbt::MAX_BLOCK_WEIGHT;
        let est = recommended_tiers(vec![(2.0, bw)], 5.0, 0);
        assert_eq!(est.next_block, 5.0); // 2 floored up to relay minimum 5
        assert_eq!(est.relay_floor, 5.0);
    }

    #[test]
    fn partial_block_returns_marginal_rate() {
        let bw = crate::gbt::MAX_BLOCK_WEIGHT;
        // Half a block at 100, half at 40 = exactly one block; the depth-1
        // threshold is only reached at the 40 tx, so that's the marginal rate.
        let rw = vec![(100.0, bw / 2), (40.0, bw / 2)];
        let est = recommended_tiers(rw, 1.0, 0);
        assert_eq!(est.next_block, 40.0);
        assert_eq!(est.within_3_blocks, 1.0); // only 1 block of weight -> deeper tiers floor
    }
}
