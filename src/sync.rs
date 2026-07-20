use crate::mempool::{apply, compute_diff, SharedState};
use crate::rpc::Rpc;
use std::collections::HashSet;
use std::thread::sleep;
use std::time::Duration;

/// If the node's mempool txid count drops below this fraction of our cached
/// tx count (while the cache is large), treat it as a likely node restart /
/// mempool flush rather than genuine mass eviction, and re-sync from scratch
/// instead of blindly evicting everything we no longer see.
const MASS_DROP_RATIO: f64 = 0.2;

/// Cache size below which we don't bother applying the mass-drop guard —
/// small mempools can legitimately shrink by more than `MASS_DROP_RATIO`
/// during normal operation (e.g. a handful of txs all confirming at once).
const MASS_DROP_MIN_CACHE_SIZE: usize = 100;

/// Blocking loop; call on a dedicated std::thread. Never returns under normal operation.
pub fn run(rpc: Rpc, state: SharedState, poll_interval: Duration) {
    // --- Startup: wait for the node's mempool to finish loading. ---
    loop {
        match rpc.mempool_loaded() {
            Ok(true) => break,
            Ok(false) => {
                tracing::info!("waiting for node mempool to finish loading");
            }
            Err(e) => {
                tracing::warn!(error = %e, "error checking mempool_loaded during startup");
            }
        }
        sleep(poll_interval);
    }

    // --- Initial bulk load. ---
    bulk_load(&rpc, &state, poll_interval);

    // --- Steady-state loop. ---
    loop {
        sleep(poll_interval);

        let node_txids: HashSet<_> = match rpc.raw_mempool_txids() {
            Ok(txids) => txids.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %e, "raw_mempool_txids failed");
                continue;
            }
        };

        let loaded = match rpc.mempool_loaded() {
            Ok(loaded) => loaded,
            Err(e) => {
                tracing::warn!(error = %e, "mempool_loaded check failed");
                continue;
            }
        };

        // Single read-lock snapshot: cache size/caught_up for the mass-drop
        // check, and a clone of the cache for diffing — all taken together so
        // we don't reacquire the lock (and risk it changing under us) between
        // checks.
        let (cache_len, was_caught_up, cache_snapshot) = match state.read() {
            Ok(guard) => (guard.txs.len(), guard.caught_up, guard.txs.clone()),
            Err(poisoned) => {
                let guard = poisoned.into_inner();
                (guard.txs.len(), guard.caught_up, guard.txs.clone())
            }
        };

        let mass_drop = was_caught_up
            && cache_len >= MASS_DROP_MIN_CACHE_SIZE
            && (node_txids.len() as f64) < (cache_len as f64) * MASS_DROP_RATIO;

        if !loaded || mass_drop {
            match state.write() {
                Ok(mut guard) => guard.caught_up = false,
                Err(poisoned) => poisoned.into_inner().caught_up = false,
            }
            tracing::warn!(
                loaded,
                mass_drop,
                node_txid_count = node_txids.len(),
                cache_len,
                "mempool desync detected (node reload or mass drop); skipping eviction this tick"
            );
            continue;
        }

        let diff = compute_diff(&cache_snapshot, &node_txids);

        let mut fetched = Vec::with_capacity(diff.new.len());
        let mut fetch_err = false;
        for txid in &diff.new {
            match rpc.mempool_entry(txid) {
                Ok(Some(tx)) => fetched.push((*txid, tx)),
                Ok(None) => {
                    // Vanished between listing and fetch; nothing to add.
                }
                Err(e) => {
                    tracing::warn!(error = %e, %txid, "mempool_entry fetch failed");
                    fetch_err = true;
                    break;
                }
            }
        }
        if fetch_err {
            continue;
        }

        let min_fee = match rpc.mempool_min_fee_sat_vb() {
            Ok(fee) => fee,
            Err(e) => {
                tracing::warn!(error = %e, "mempool_min_fee_sat_vb failed");
                continue;
            }
        };
        let tip_height = match rpc.tip_height() {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "tip_height failed");
                continue;
            }
        };

        match state.write() {
            Ok(mut guard) => {
                apply(&mut guard, &diff.gone, fetched);
                guard.mempool_min_fee_sat_vb = min_fee;
                guard.tip_height = tip_height;
                guard.caught_up = true;
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                apply(&mut guard, &diff.gone, fetched);
                guard.mempool_min_fee_sat_vb = min_fee;
                guard.tip_height = tip_height;
                guard.caught_up = true;
            }
        }
    }
}

/// Fetch the full mempool from the node and replace the in-memory cache wholesale.
/// Retries indefinitely (on `poll_interval`) until it succeeds, since the caller
/// depends on a fully-populated cache before entering steady state.
fn bulk_load(rpc: &Rpc, state: &SharedState, poll_interval: Duration) {
    loop {
        let loaded = match rpc.raw_mempool_verbose() {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(error = %e, "raw_mempool_verbose failed during bulk load");
                sleep(poll_interval);
                continue;
            }
        };
        let min_fee = match rpc.mempool_min_fee_sat_vb() {
            Ok(fee) => fee,
            Err(e) => {
                tracing::warn!(error = %e, "mempool_min_fee_sat_vb failed during bulk load");
                sleep(poll_interval);
                continue;
            }
        };
        let tip_height = match rpc.tip_height() {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(error = %e, "tip_height failed during bulk load");
                sleep(poll_interval);
                continue;
            }
        };

        let count = loaded.len();
        match state.write() {
            Ok(mut guard) => {
                guard.txs = loaded.into_iter().collect();
                guard.mempool_min_fee_sat_vb = min_fee;
                guard.tip_height = tip_height;
                guard.caught_up = true;
            }
            Err(poisoned) => {
                let mut guard = poisoned.into_inner();
                guard.txs = loaded.into_iter().collect();
                guard.mempool_min_fee_sat_vb = min_fee;
                guard.tip_height = tip_height;
                guard.caught_up = true;
            }
        }
        tracing::info!(count, "mempool bulk load complete");
        return;
    }
}
