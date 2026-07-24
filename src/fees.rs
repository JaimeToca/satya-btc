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

/// Full pipeline: snapshot -> projection -> tiers, stamped with the current time.
///
/// No consumer calls this yet — the `/fees` HTTP handler that will is a later
/// task. Kept and silenced here (same convention as `mempool::MempoolTx`)
/// rather than dropped, since it's the deliberate integration point this
/// module exists to provide.
#[allow(dead_code)]
pub fn compute_estimate(
    snapshot: &[(Txid, u64, u32, Vec<Txid>)],
    min_fee_sat_vb: f64,
) -> FeeEstimate {
    let gbt_txs = snapshot_to_gbt(snapshot);
    let proj = gbt::project(gbt_txs);
    tiers_from_projection(&proj, min_fee_sat_vb, now_unix())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gbt::Projection;
    use std::collections::HashMap;

    #[test]
    fn empty_projection_falls_back_to_minimum() {
        let proj = Projection {
            blocks: vec![],
            effective_rates: HashMap::new(),
        };
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
        let proj = Projection {
            blocks: vec![vec![0], vec![1], vec![2]],
            effective_rates: rates,
        };
        // min_fee 8 -> the 5 sat/vB economy boundary is floored up to 8.
        let est = tiers_from_projection(&proj, 8.0, 0);
        assert_eq!(est.fastest_fee, 50.0); // block 0
                                           // half_hour clamps to last available block (index 2) since < 3 blocks after 0..
        assert_eq!(est.hour_fee, 5.0_f64.max(8.0)); // floored at minimum
        assert_eq!(est.economy_fee, 8.0); // 5 floored to min 8
        assert_eq!(est.minimum_fee, 8.0);
    }
}
