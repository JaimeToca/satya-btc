use crate::mempool::{compute_diff, read_state, write_state, SharedState};
use crate::rpc::Rpc;
use std::collections::HashSet;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

/// Cache size below which we skip the mass-drop resync check — small mempools
/// can legitimately shrink by more than 80% during normal operation (e.g. a few
/// txs all confirming in one block), which would otherwise trigger a needless
/// full resync.
const MASS_DROP_MIN_CACHE_SIZE: usize = 100;

/// Blocking loop; call on a dedicated std::thread. Never returns under normal operation.
pub fn run(mut rpc: Rpc, state: SharedState, poll_interval: Duration) {
    // --- Startup: wait for the node's mempool to finish loading, then do an
    // initial full load before entering steady state. ---
    loop {
        match rpc.mempool_info() {
            Ok(info) if info.loaded => break,
            Ok(_) => {
                tracing::info!("waiting for node mempool to finish loading");
            }
            Err(e) => {
                tracing::warn!(error = %e, "error checking mempool_info during startup");
            }
        }
        sleep(poll_interval);
    }

    loop {
        if bulk_resync(&mut rpc, &state) {
            break;
        }
        tracing::warn!("initial bulk resync failed; retrying");
        sleep(poll_interval);
    }

    // --- Steady-state loop. ---
    loop {
        sleep(poll_interval);

        let info = match rpc.mempool_info() {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!(error = %e, "mempool_info failed");
                mark_stale(&state);
                continue;
            }
        };

        let node_txids: HashSet<_> = match rpc.raw_mempool_txids() {
            Ok(txids) => txids.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %e, "raw_mempool_txids failed");
                mark_stale(&state);
                continue;
            }
        };

        // Snapshot keys only — never clone tx values just to diff.
        let (cache_len, cache_keys) = {
            let g = read_state(&state);
            (g.txs.len(), g.txs.keys().copied().collect::<HashSet<_>>())
        };

        let mass_drop = cache_len >= MASS_DROP_MIN_CACHE_SIZE
            && node_txids.len().saturating_mul(5) < cache_len;

        if !info.loaded || mass_drop {
            tracing::warn!(
                loaded = info.loaded,
                mass_drop,
                node_txid_count = node_txids.len(),
                cache_len,
                "mempool desync detected (node reload or mass drop); resyncing from scratch"
            );
            if !bulk_resync(&mut rpc, &state) {
                mark_stale(&state);
            }
            continue;
        }

        let diff = compute_diff(&cache_keys, &node_txids);

        // Apply removals immediately: the node's txid list is authoritative,
        // so departed txs shouldn't wait on the (possibly-failing) fetch of
        // newly-seen ones below.
        {
            let mut g = write_state(&state);
            for txid in &diff.gone {
                g.txs.remove(txid);
            }
        }

        // Fetch adds best-effort: a single failed fetch doesn't abort the
        // batch or discard txs already fetched this tick.
        let mut fetched = Vec::with_capacity(diff.new.len());
        for txid in &diff.new {
            match rpc.mempool_entry(txid) {
                Ok(Some(tx)) => fetched.push((*txid, tx)),
                Ok(None) => {
                    // Vanished between listing and fetch; nothing to add.
                }
                Err(e) => {
                    tracing::warn!(error = %e, %txid, "mempool_entry fetch failed; will retry next tick");
                }
            }
        }

        let tip_height = rpc.tip_height().ok();

        {
            let mut g = write_state(&state);
            for (txid, tx) in fetched {
                g.txs.insert(txid, tx);
            }
            g.mempool_min_fee_sat_vb = info.min_fee_sat_vb;
            if let Some(h) = tip_height {
                g.tip_height = h;
            }
            g.caught_up = true;
            g.last_sync_ok = Some(SystemTime::now());
        }
    }
}

/// Fetch the full mempool from the node and replace the in-memory cache
/// wholesale. Returns `true` on success (state updated, `caught_up = true`,
/// `last_sync_ok` refreshed) or `false` on failure (state left untouched by
/// this call; caller decides how to mark staleness).
fn bulk_resync(rpc: &mut Rpc, state: &SharedState) -> bool {
    let entries = match rpc.raw_mempool_verbose() {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %e, "raw_mempool_verbose failed during bulk resync");
            return false;
        }
    };
    let info = match rpc.mempool_info() {
        Ok(info) => info,
        Err(e) => {
            tracing::warn!(error = %e, "mempool_info failed during bulk resync");
            return false;
        }
    };
    let tip_height = match rpc.tip_height() {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "tip_height failed during bulk resync");
            return false;
        }
    };

    let count = entries.len();
    {
        let mut g = write_state(state);
        g.txs = entries.into_iter().collect();
        g.mempool_min_fee_sat_vb = info.min_fee_sat_vb;
        g.tip_height = tip_height;
        g.caught_up = true;
        g.last_sync_ok = Some(SystemTime::now());
    }
    tracing::info!(count, "mempool bulk resync complete");
    true
}

/// Mark the cache stale (not caught up) after a failed poll. Does not touch
/// `last_sync_ok`, which reflects the last time a sync actually succeeded.
fn mark_stale(state: &SharedState) {
    write_state(state).caught_up = false;
}
