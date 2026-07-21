use crate::mempool::{
    self, compute_diff, read_state, write_state, MempoolState, MempoolTx, SharedState,
};
use crate::rpc::{self, Rpc};
use bitcoincore_rpc::bitcoin::Txid;
use futures::stream::{self, StreamExt};
use futures::FutureExt;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::Semaphore;

/// Maximum length of a formatted error string included in a log line, so a
/// huge (or secret-bearing) provider error body can't blow up the logs.
const MAX_ERR_LOG_LEN: usize = 200;

/// Format an error (including its full `anyhow` source/cause chain, via the
/// alternate `{:#}` form) and truncate it to `MAX_ERR_LOG_LEN` chars in a
/// single pass, so oversized error bodies (e.g. from a misbehaving RPC
/// provider) don't get logged in full.
fn short_err(e: &anyhow::Error) -> String {
    let full = format!("{e:#}");
    let mut chars = full.chars();
    let mut s: String = chars.by_ref().take(MAX_ERR_LOG_LEN).collect();
    if chars.next().is_some() {
        s.push('…');
    }
    s
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
fn set_synced_locked(
    g: &mut MempoolState,
    prev: &mut bool,
    synced: bool,
    reason: &str,
    mempool_size: usize,
) {
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
fn set_synced(
    state: &SharedState,
    prev: &mut bool,
    synced: bool,
    reason: &str,
    mempool_size: usize,
) {
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

/// Logging/timing knobs for the sync loop, grouped so `run`'s signature stays
/// readable as they accumulate.
#[derive(Debug, Clone, Copy)]
pub struct SyncConfig {
    pub poll_interval: Duration,
    /// Max concurrent `getmempoolentry` calls per tick (via `buffer_unordered`).
    pub fetch_concurrency: usize,
    /// Max fetch time per tick before bailing and marking the state stale
    /// (`caught_up = false`); the remainder is retried on the next tick.
    pub tick_budget: Duration,
}

/// Async sync loop; spawn on the tokio runtime. Never returns under normal
/// operation. `wake_rx` lets a future ZMQ block event (Task 5) trigger an
/// immediate steady-state tick instead of waiting out the poll interval.
pub async fn run(
    mut rpc: Rpc,
    state: SharedState,
    cfg: SyncConfig,
    mut wake_rx: tokio::sync::mpsc::Receiver<()>,
) {
    let poll_interval = cfg.poll_interval;
    // --- Startup: wait for the node's mempool to finish loading, then do an
    // initial full load before entering steady state. ---
    loop {
        match rpc.mempool_info().await {
            // Older nodes don't report `loaded` at all; treat that as loaded.
            Ok(info) if info.loaded.unwrap_or(true) => break,
            Ok(_) => {
                tracing::info!("waiting for node mempool to finish loading");
            }
            Err(e) => {
                tracing::warn!(error = %short_err(&e), "error checking mempool_info during startup");
                // A node restart rotates the cookie; rebuild the client before
                // the retry sleep so we don't spin on 401 forever at startup.
                if rpc::is_reconnectable(&e) {
                    if let Err(re) = rpc.reconnect() {
                        tracing::warn!(error = %short_err(&re), "rpc reconnect failed");
                    }
                }
            }
        }
        tokio::time::sleep(poll_interval).await;
    }

    // Tracks the previously-logged `caught_up` value so we only emit a
    // transition log on the edge (see `set_synced`), not every tick. Starts
    // `false` so the initial bulk resync below logs the false->true edge via
    // `set_synced` instead of unconditionally.
    let mut caught_up_prev = false;

    let mut last_bulk_resync: Option<Instant>;
    loop {
        if let Some(count) = bulk_resync(&mut rpc, &state).await {
            last_bulk_resync = Some(Instant::now());
            set_synced(&state, &mut caught_up_prev, true, "", count);
            break;
        }
        tracing::warn!("initial bulk resync failed; retrying");
        tokio::time::sleep(poll_interval).await;
    }

    // --- Steady-state loop. ---
    // Shared across ALL ticks: caps concurrent in-flight blocking
    // `getmempoolentry` calls at `fetch_concurrency` globally, not just per
    // tick. Each fetch acquires an owned permit that's moved into its
    // `spawn_blocking` closure and released only when that blocking RPC
    // completes — so work orphaned by a budget-bail `break` (whose futures are
    // dropped) still holds its permit until the underlying RPC finishes,
    // protecting bitcoind's `rpcworkqueue` from a pile-up across ticks.
    let fetch_semaphore = Arc::new(Semaphore::new(cfg.fetch_concurrency));
    loop {
        // Interruptible wait: either the poll interval elapses, or a ZMQ block
        // event wakes us early (wire-up in Task 5).
        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = wake_rx.recv() => {}
        }

        let info = match rpc.mempool_info().await {
            Ok(info) => info,
            Err(e) => {
                tracing::warn!(error = %short_err(&e), "mempool_info failed");
                if rpc::is_reconnectable(&e) {
                    if let Err(re) = rpc.reconnect() {
                        tracing::warn!(error = %short_err(&re), "rpc reconnect failed");
                    }
                }
                set_synced(
                    &state,
                    &mut caught_up_prev,
                    false,
                    "rpc_error:mempool_info",
                    0,
                );
                continue;
            }
        };
        // Older nodes don't report `loaded` at all; treat that as loaded.
        let loaded = info.loaded.unwrap_or(true);
        let min_fee_sat_vb = mempool::min_fee_sat_vb(&info);

        let node_txids: HashSet<_> = match rpc.raw_mempool_txids().await {
            Ok(txids) => txids.into_iter().collect(),
            Err(e) => {
                tracing::warn!(error = %short_err(&e), "raw_mempool_txids failed");
                if rpc::is_reconnectable(&e) {
                    if let Err(re) = rpc.reconnect() {
                        tracing::warn!(error = %short_err(&re), "rpc reconnect failed");
                    }
                }
                set_synced(
                    &state,
                    &mut caught_up_prev,
                    false,
                    "rpc_error:raw_mempool_txids",
                    0,
                );
                continue;
            }
        };

        // Snapshot keys only — never clone tx values just to diff.
        let (cache_len, cache_keys) = {
            let g = read_state(&state);
            (g.txs.len(), g.txs.keys().copied().collect::<HashSet<_>>())
        };

        let mass_drop =
            cache_len >= MASS_DROP_MIN_CACHE_SIZE && node_txids.len().saturating_mul(5) < cache_len;

        if !loaded || mass_drop {
            let reason = if !loaded {
                "node_not_loaded"
            } else {
                "mass_drop"
            };
            let cooling_down = last_bulk_resync.is_some_and(|t| t.elapsed() < RESYNC_COOLDOWN);
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
            match bulk_resync(&mut rpc, &state).await {
                Some(count) => set_synced(&state, &mut caught_up_prev, true, "", count),
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

        // Fetch adds best-effort, bounded, and concurrent: a single failed
        // fetch doesn't abort the batch or discard txs already fetched this
        // tick; we fetch up to MAX_NEW_FETCH_PER_TICK per tick with at most
        // `cfg.fetch_concurrency` calls in flight (via `buffer_unordered`); and
        // we stop consuming once this tick's fetch has run past `cfg.tick_budget`.
        // Anything left over (capped, dropped in-flight on budget bail, or
        // errored) stays absent from the cache, so it's still in `diff.new`
        // (and gets fetched) next tick — nothing is permanently lost.
        let candidates: Vec<Txid> = diff.new.iter().take(MAX_NEW_FETCH_PER_TICK).copied().collect();
        let to_fetch = candidates.len();
        let fetch_start = Instant::now();
        let mut fetched = Vec::with_capacity(to_fetch);
        let mut fetch_errors = 0usize;
        // Count of candidates whose fetch fully resolved this tick — incremented
        // for EVERY processed result including `Ok(None)` (vanished) and `Err`.
        // `budget_exceeded` is computed from this after the loop: a tick is only
        // "stale" if candidates remain unresolved when we stopped, so a tick
        // that resolved every candidate but happened to cross the wall-clock
        // budget on the last item is NOT marked stale.
        let mut resolved = 0usize;
        // Per-result processing, shared between the main consume loop and the
        // budget-bail drain below so the Ok(Some)/Ok(None)/Err handling can't
        // drift between the two.
        let process = |txid: Txid,
                           res: anyhow::Result<Option<MempoolTx>>,
                           fetched: &mut Vec<(Txid, MempoolTx)>,
                           fetch_errors: &mut usize,
                           resolved: &mut usize| {
            *resolved += 1;
            match res {
                Ok(Some(tx)) => fetched.push((txid, tx)),
                Ok(None) => {
                    // Vanished between listing and fetch; nothing to add.
                }
                Err(e) => {
                    *fetch_errors += 1;
                    tracing::debug!(error = %short_err(&e), %txid, "mempool_entry fetch failed; will retry next tick");
                }
            }
        };
        let mut results = stream::iter(candidates.into_iter())
            .map(|txid| {
                let rpc = rpc.clone();
                let sem = fetch_semaphore.clone();
                async move {
                    // Acquire a permit (bounds concurrency globally across
                    // ticks). It's moved into the blocking closure inside
                    // `mempool_entry_with_permit` and released only when the RPC
                    // completes — so a budget-bail that drops this future still
                    // holds the permit until the orphaned RPC finishes.
                    let permit = match sem.clone().acquire_owned().await {
                        Ok(p) => p,
                        // The semaphore is never closed for the process lifetime;
                        // treat a closed semaphore as a fetch error rather than
                        // panicking.
                        Err(e) => return (txid, Err(anyhow::Error::new(e))),
                    };
                    (
                        txid,
                        rpc.mempool_entry_with_permit(&txid, permit)
                            .await
                            .map(|opt| opt.map(|e| MempoolTx::from(&e))),
                    )
                }
            })
            .buffer_unordered(cfg.fetch_concurrency);
        while let Some((txid, res)) = results.next().await {
            process(txid, res, &mut fetched, &mut fetch_errors, &mut resolved);
            // Time-based bail: stop consuming once this tick's fetch has run
            // past budget. Before stopping, drain any results that are ALREADY
            // ready without awaiting new work (`now_or_never`), so completed
            // fetches this tick aren't thrown away just because we crossed the
            // budget. In-flight (not-yet-ready) futures are dropped when we
            // `break`; their permits are still held inside their spawn_blocking
            // closures until those RPCs finish, and the txids reappear in next
            // tick's `diff.new`.
            if fetch_start.elapsed() > cfg.tick_budget {
                while let Some(Some((txid, res))) = results.next().now_or_never() {
                    process(txid, res, &mut fetched, &mut fetch_errors, &mut resolved);
                }
                break;
            }
        }
        // Stale only if candidates remain unresolved after the (drained) stop —
        // NOT merely because wall-clock time crossed the budget.
        let budget_exceeded = resolved < to_fetch;

        let tip_height = rpc.tip_height().await.ok();

        // Only promote to "caught up" when this tick fully resolved the
        // node's new-txid list (didn't hit the per-tick cap, didn't bail on
        // the time budget, and every fetch either succeeded or the tx had
        // already vanished). Otherwise the cache is known to be behind the
        // node, so `/health` should say so.
        let backlog =
            diff.new.len() > MAX_NEW_FETCH_PER_TICK || fetch_errors > 0 || budget_exceeded;

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
async fn bulk_resync(rpc: &mut Rpc, state: &SharedState) -> Option<usize> {
    // On a reconnectable (auth/transport) error, rebuild the client before
    // bailing so the NEXT resync attempt uses a fresh connection / re-read
    // cookie. A node restart empties the mempool (triggering a mass-drop
    // resync) AND rotates the cookie, so without this both the initial
    // bulk-load loop and steady-state resyncs would wedge on 401 forever.
    let reconnect_if_needed = |rpc: &mut Rpc, e: &anyhow::Error| {
        if rpc::is_reconnectable(e) {
            if let Err(re) = rpc.reconnect() {
                tracing::warn!(error = %short_err(&re), "rpc reconnect failed");
            }
        }
    };
    let entries = match rpc.raw_mempool_verbose().await {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "raw_mempool_verbose failed during bulk resync");
            reconnect_if_needed(rpc, &e);
            return None;
        }
    };
    let info = match rpc.mempool_info().await {
        Ok(info) => info,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "mempool_info failed during bulk resync");
            reconnect_if_needed(rpc, &e);
            return None;
        }
    };
    let tip_height = match rpc.tip_height().await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "tip_height failed during bulk resync");
            reconnect_if_needed(rpc, &e);
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
