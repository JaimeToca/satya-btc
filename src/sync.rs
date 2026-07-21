use crate::mempool::{self, compute_diff, read_state, write_state, MempoolState, MempoolTx, SharedState};
use crate::rpc::Rpc;
use std::collections::HashSet;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime};

/// Maximum length of a formatted error string included in a log line, so a
/// huge (or secret-bearing) provider error body can't blow up the logs.
const MAX_ERR_LOG_LEN: usize = 200;

/// Format an error and truncate it to `MAX_ERR_LOG_LEN` chars, so oversized
/// error bodies (e.g. from a misbehaving RPC provider) don't get logged in
/// full.
fn short_err(e: &anyhow::Error) -> String {
    let full = format!("{e}");
    if full.chars().count() > MAX_ERR_LOG_LEN {
        let mut s: String = full.chars().take(MAX_ERR_LOG_LEN).collect();
        s.push('…');
        s
    } else {
        full
    }
}

/// Set `state.caught_up` (state-write semantics unchanged from the previous
/// direct assignments) and, if this changes the value relative to `prev`, log
/// the transition once. `prev` is updated to match. This turns what used to
/// be scattered per-tick logging into a single operator-facing signal: a
/// 10-minute outage now produces one "mempool out of sync" warn instead of
/// one per tick.
///
/// `mempool_size` is only meaningful (and only logged) on a false->true
/// transition; pass a value already in hand rather than taking a fresh lock
/// just to read the cache length.
fn set_synced_locked(g: &mut MempoolState, prev: &mut bool, synced: bool, reason: &str, mempool_size: usize) {
    g.caught_up = synced;
    if synced == *prev {
        return;
    }
    if synced {
        tracing::info!(mempool_size, "mempool in sync");
    } else {
        tracing::warn!(reason, "mempool out of sync");
    }
    *prev = synced;
}

/// Like `set_synced_locked`, but acquires the write lock itself. For call
/// sites that aren't already inside a write-lock block.
fn set_synced(state: &SharedState, prev: &mut bool, synced: bool, reason: &str, mempool_size: usize) {
    let mut g = write_state(state);
    set_synced_locked(&mut g, prev, synced, reason, mempool_size);
}

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
            // Older nodes don't report `loaded` at all; treat that as loaded.
            Ok(info) if info.loaded.unwrap_or(true) => break,
            Ok(_) => {
                tracing::info!("waiting for node mempool to finish loading");
            }
            Err(e) => {
                tracing::debug!(error = %short_err(&e), "error checking mempool_info during startup");
            }
        }
        sleep(poll_interval);
    }

    let mut last_bulk_resync: Option<Instant>;
    loop {
        if let Some(count) = bulk_resync(&mut rpc, &state) {
            last_bulk_resync = Some(Instant::now());
            tracing::info!(mempool_size = count, "mempool in sync");
            break;
        }
        tracing::warn!("initial bulk resync failed; retrying");
        sleep(poll_interval);
    }

    // Tracks the previously-logged `caught_up` value so we only emit a
    // transition log on the edge (see `set_synced`), not every tick. The
    // initial bulk resync above set `caught_up = true` in state (and we just
    // logged the corresponding "mempool in sync" line directly), so start
    // this tracker in sync too.
    let mut caught_up_prev = true;

    // --- Steady-state loop. ---
    loop {
        sleep(poll_interval);

        let info = match rpc.mempool_info() {
            Ok(info) => info,
            Err(e) => {
                tracing::debug!(error = %short_err(&e), "mempool_info failed");
                set_synced(&state, &mut caught_up_prev, false, "rpc_error:mempool_info", 0);
                continue;
            }
        };
        // Older nodes don't report `loaded` at all; treat that as loaded.
        let loaded = info.loaded.unwrap_or(true);
        let min_fee_sat_vb = mempool::min_fee_sat_vb(&info);

        let node_txids: HashSet<_> = match rpc.raw_mempool_txids() {
            Ok(txids) => txids.into_iter().collect(),
            Err(e) => {
                tracing::debug!(error = %short_err(&e), "raw_mempool_txids failed");
                set_synced(&state, &mut caught_up_prev, false, "rpc_error:raw_mempool_txids", 0);
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

        if !loaded || mass_drop {
            let reason = if !loaded { "node_not_loaded" } else { "mass_drop" };
            let cooling_down = last_bulk_resync
                .is_some_and(|t| t.elapsed() < RESYNC_COOLDOWN);
            if cooling_down {
                tracing::debug!(
                    loaded,
                    mass_drop,
                    node_txid_count = node_txids.len(),
                    cache_len,
                    "mempool desync detected but bulk resync is in cooldown; waiting it out"
                );
                set_synced(&state, &mut caught_up_prev, false, reason, 0);
                continue;
            }
            tracing::debug!(
                loaded,
                mass_drop,
                node_txid_count = node_txids.len(),
                cache_len,
                "mempool desync detected (node reload or mass drop); resyncing from scratch"
            );
            last_bulk_resync = Some(Instant::now());
            match bulk_resync(&mut rpc, &state) {
                Some(count) => {
                    caught_up_prev = true;
                    tracing::info!(mempool_size = count, "mempool in sync");
                }
                None => set_synced(&state, &mut caught_up_prev, false, "resync_failed", 0),
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
            match rpc.mempool_entry(txid).map(|opt| opt.map(|e| MempoolTx::from(&e))) {
                Ok(Some(tx)) => fetched.push((*txid, tx)),
                Ok(None) => {
                    // Vanished between listing and fetch; nothing to add.
                }
                Err(e) => {
                    fetch_errors += 1;
                    tracing::debug!(error = %short_err(&e), %txid, "mempool_entry fetch failed; will retry next tick");
                }
            }
        }

        let tip_height = rpc.tip_height().ok();

        // Only promote to "caught up" when this tick fully resolved the
        // node's new-txid list (didn't hit the per-tick cap, and every fetch
        // either succeeded or the tx had already vanished). Otherwise the
        // cache is known to be behind the node, so `/health` should say so.
        let backlog = diff.new.len() > MAX_NEW_FETCH_PER_TICK || fetch_errors > 0;

        tracing::debug!(
            new = diff.new.len(),
            gone = diff.gone.len(),
            fetched = fetched.len(),
            backlog,
            "sync tick"
        );

        // Cache size after this tick's removals/inserts, computed from
        // counts already in hand rather than re-reading the cache under lock
        // just for a log field.
        let mempool_size = cache_len - diff.gone.len() + fetched.len();

        {
            let mut g = write_state(&state);
            for (txid, tx) in fetched {
                g.txs.insert(txid, tx);
            }
            g.mempool_min_fee_sat_vb = min_fee_sat_vb;
            if let Some(h) = tip_height {
                g.tip_height = h;
            }
            if backlog {
                set_synced_locked(&mut g, &mut caught_up_prev, false, "backlog", 0);
            } else {
                set_synced_locked(&mut g, &mut caught_up_prev, true, "", mempool_size);
                g.last_sync_ok = Some(SystemTime::now());
            }
        }
    }
}

/// Fetch the full mempool from the node and replace the in-memory cache
/// wholesale. Returns `Some(count)` on success (state updated, `caught_up =
/// true`, `last_sync_ok` refreshed), where `count` is the number of txs
/// loaded, or `None` on failure (state left untouched by this call; caller
/// decides how to mark staleness).
fn bulk_resync(rpc: &mut Rpc, state: &SharedState) -> Option<usize> {
    let entries = match rpc.raw_mempool_verbose() {
        Ok(entries) => entries,
        Err(e) => {
            tracing::debug!(error = %short_err(&e), "raw_mempool_verbose failed during bulk resync");
            return None;
        }
    };
    let info = match rpc.mempool_info() {
        Ok(info) => info,
        Err(e) => {
            tracing::debug!(error = %short_err(&e), "mempool_info failed during bulk resync");
            return None;
        }
    };
    let tip_height = match rpc.tip_height() {
        Ok(h) => h,
        Err(e) => {
            tracing::debug!(error = %short_err(&e), "tip_height failed during bulk resync");
            return None;
        }
    };

    let count = entries.len();
    {
        let mut g = write_state(state);
        g.txs = entries
            .into_iter()
            .map(|(txid, entry)| (txid, MempoolTx::from(&entry)))
            .collect();
        g.mempool_min_fee_sat_vb = mempool::min_fee_sat_vb(&info);
        g.tip_height = tip_height;
        g.caught_up = true;
        g.last_sync_ok = Some(SystemTime::now());
    }
    tracing::info!(count, "mempool bulk resync complete");
    Some(count)
}
