use crate::mempool::{compute_diff, read_state, write_state, SharedState};
use crate::rpc::Rpc;
use std::collections::HashSet;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime};

/// Cache size below which we skip the mass-drop resync check — small mempools
/// can legitimately shrink by more than 80% during normal operation (e.g. a few
/// txs all confirming in one block), which would otherwise trigger a needless
/// full resync.
const MASS_DROP_MIN_CACHE_SIZE: usize = 100;

/// Maximum number of newly-seen txids we'll fetch full details for in a
/// single tick. Bounds per-tick RPC volume so an unbounded (or malicious)
/// mempool can't force one tick to issue hundreds of thousands of sequential
/// RPCs. Any remainder stays in the node's txid set and reappears in
/// `diff.new` on the next tick, so nothing is permanently lost — it's just
/// spread across ticks.
const MAX_NEW_FETCH_PER_TICK: usize = 2000;

/// Minimum time between `bulk_resync` calls triggered by desync detection
/// (node reload / mass drop). Without this, a provider or node that flaps
/// `loaded=false` or oscillates mempool size can force a full verbose
/// mempool download on nearly every tick.
const RESYNC_COOLDOWN: Duration = Duration::from_secs(60);

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

    let mut last_bulk_resync: Option<Instant>;
    loop {
        if bulk_resync(&mut rpc, &state) {
            last_bulk_resync = Some(Instant::now());
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
            let cooling_down = last_bulk_resync
                .is_some_and(|t| t.elapsed() < RESYNC_COOLDOWN);
            if cooling_down {
                tracing::warn!(
                    loaded = info.loaded,
                    mass_drop,
                    node_txid_count = node_txids.len(),
                    cache_len,
                    "mempool desync detected but bulk resync is in cooldown; waiting it out"
                );
                mark_stale(&state);
                continue;
            }
            tracing::warn!(
                loaded = info.loaded,
                mass_drop,
                node_txid_count = node_txids.len(),
                cache_len,
                "mempool desync detected (node reload or mass drop); resyncing from scratch"
            );
            last_bulk_resync = Some(Instant::now());
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

        // Fetch adds best-effort and bounded: a single failed fetch doesn't
        // abort the batch or discard txs already fetched this tick, and we
        // only fetch up to MAX_NEW_FETCH_PER_TICK per tick. Anything left
        // over stays absent from the cache, so it's still in `diff.new` (and
        // gets fetched) next tick — nothing is permanently lost.
        let mut fetched = Vec::with_capacity(diff.new.len().min(MAX_NEW_FETCH_PER_TICK));
        let mut fetch_errors = 0usize;
        for txid in diff.new.iter().take(MAX_NEW_FETCH_PER_TICK) {
            match rpc.mempool_entry(txid) {
                Ok(Some(tx)) => fetched.push((*txid, tx)),
                Ok(None) => {
                    // Vanished between listing and fetch; nothing to add.
                }
                Err(e) => {
                    fetch_errors += 1;
                    tracing::warn!(error = %e, %txid, "mempool_entry fetch failed; will retry next tick");
                }
            }
        }

        let tip_height = rpc.tip_height().ok();

        // Only promote to "caught up" when this tick fully resolved the
        // node's new-txid list (didn't hit the per-tick cap, and every fetch
        // either succeeded or the tx had already vanished). Otherwise the
        // cache is known to be behind the node, so `/health` should say so.
        let backlog = diff.new.len() > MAX_NEW_FETCH_PER_TICK || fetch_errors > 0;

        {
            let mut g = write_state(&state);
            for (txid, tx) in fetched {
                g.txs.insert(txid, tx);
            }
            g.mempool_min_fee_sat_vb = info.min_fee_sat_vb;
            if let Some(h) = tip_height {
                g.tip_height = h;
            }
            if backlog {
                g.caught_up = false;
            } else {
                g.caught_up = true;
                g.last_sync_ok = Some(SystemTime::now());
            }
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
