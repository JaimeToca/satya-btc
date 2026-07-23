mod decision;

use crate::mempool::{
    self, compute_diff, read_state, write_state, MempoolState, MempoolTx, SharedState,
};
use crate::rpc::{self, Rpc, RpcError};
use bitcoin::Txid;
use decision::{
    decide_desync, is_backlog, projected_mempool_size, resync_cooling_down, DesyncAction,
};
use futures::stream::{self, StreamExt};
use futures::FutureExt;
use std::collections::HashSet;
use std::time::{Duration, Instant, SystemTime};

/// Maximum length of a formatted error string included in a log line, so a
/// huge (or secret-bearing) provider error body can't blow up the logs.
const MAX_ERR_LOG_LEN: usize = 200;

/// Format an error (for `anyhow::Error`, the alternate `{:#}` form includes the
/// full source/cause chain; for other `Display` types it's just their message)
/// and truncate it to `MAX_ERR_LOG_LEN` chars in a single pass, so oversized
/// error bodies (e.g. from a misbehaving RPC provider) don't get logged in full.
fn short_err<E: std::fmt::Display>(e: &E) -> String {
    let full = format!("{e:#}");
    let mut chars = full.chars();
    let mut s: String = chars.by_ref().take(MAX_ERR_LOG_LEN).collect();
    if chars.next().is_some() {
        s.push('…');
    }
    s
}

/// On a reconnectable (auth/transport) error, rebuild the client so the NEXT
/// call uses a fresh connection / re-read cookie. A node restart empties the
/// mempool (triggering a mass-drop resync) AND rotates the cookie, so without
/// this the startup loop, steady-state ticks, and bulk resyncs would all wedge
/// on 401 forever. Non-reconnectable errors are left alone.
fn reconnect_on_error(rpc: &mut Rpc, e: &RpcError) {
    if rpc::is_reconnectable(e) {
        if let Err(re) = rpc.reconnect() {
            tracing::warn!(error = %short_err(&re), "rpc reconnect failed");
        }
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

/// Mark a successful bulk resync's freshness flags together under one write
/// lock: set `caught_up = true` (with edge logging via `set_synced_locked`) AND
/// refresh `last_sync_ok`, in the same critical section. `bulk_resync` writes
/// only the mempool DATA; this caller-owned step owns BOTH freshness flags, so a
/// `/health` reader can never observe the full mempool with `last_sync_ok` set
/// but `caught_up` still false.
fn apply_bulk_success(state: &SharedState, caught_up_prev: &mut bool, count: usize) {
    let mut g = write_state(state);
    set_synced_locked(&mut g, caught_up_prev, true, "", count);
    g.last_sync_ok = Some(SystemTime::now());
}

/// Maximum number of newly-seen txids we'll fetch full details for in a
/// single tick. Bounds per-tick RPC volume so an unbounded (or malicious)
/// mempool can't force one tick to issue hundreds of thousands of sequential
/// RPCs. Any remainder stays in the node's txid set and reappears in
/// `diff.new` on the next tick, so nothing is permanently lost — it's just
/// spread across ticks.
const MAX_NEW_FETCH_PER_TICK: usize = 2000;

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

/// Result of fetching (a capped slice of) this tick's new txids.
struct FetchBatchResult {
    /// Successfully fetched `(txid, tx)` pairs to insert into the cache.
    fetched: Vec<(Txid, MempoolTx)>,
    /// How many fetches returned an error (retried next tick).
    fetch_errors: usize,
    /// True if we stopped before resolving every attempted candidate (budget
    /// bail with in-flight work dropped).
    incomplete: bool,
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
    wait_until_mempool_loaded(&mut rpc, poll_interval).await;

    // Tracks the previously-logged `caught_up` value so we only emit a
    // transition log on the edge (see `set_synced`), not every tick. Starts
    // `false` so the initial bulk resync below logs the false->true edge via
    // `set_synced` instead of unconditionally.
    let mut caught_up_prev = false;

    let mut last_bulk_resync =
        initial_bulk_load(&mut rpc, &state, poll_interval, &mut caught_up_prev).await;

    // --- Steady-state loop. ---
    // Per-tick fetch concurrency is bounded by `buffer_unordered` in
    // `fetch_new_entries`. Because the RPC is now async over reqwest, dropping
    // the fetch stream on a budget-bail truly cancels in-flight requests (no
    // orphaned work), so no cross-tick semaphore is needed to protect
    // bitcoind's `rpcworkqueue`.
    loop {
        // Interruptible wait: either the poll interval elapses, or a ZMQ block
        // event wakes us early (wire-up in Task 5).
        tokio::select! {
            _ = tokio::time::sleep(poll_interval) => {}
            _ = wake_rx.recv() => {}
        }

        steady_tick(
            &mut rpc,
            &state,
            &cfg,
            &mut caught_up_prev,
            &mut last_bulk_resync,
        )
        .await;
    }
}

/// Startup gate: poll `mempool_info` until the node reports its mempool has
/// finished loading, then return. Older nodes don't report `loaded`; treat that
/// as loaded.
async fn wait_until_mempool_loaded(rpc: &mut Rpc, poll: Duration) {
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
                reconnect_on_error(rpc, &e);
            }
        }
        tokio::time::sleep(poll).await;
    }
}

/// Cold-start: retry `bulk_resync` until one succeeds, then return the
/// `last_bulk_resync` instant (stamped AFTER the successful bulk, so the first
/// cooldown window starts from load completion). Only returns after a success.
async fn initial_bulk_load(
    rpc: &mut Rpc,
    state: &SharedState,
    poll: Duration,
    caught_up_prev: &mut bool,
) -> Instant {
    loop {
        if let Some(count) = bulk_resync(rpc, state).await {
            // Stamp AFTER the successful bulk (not before): the initial-load
            // cooldown window is measured from when the load actually landed.
            let last_bulk_resync = Instant::now();
            apply_bulk_success(state, caught_up_prev, count);
            return last_bulk_resync;
        }
        tracing::warn!("initial bulk resync failed; retrying");
        tokio::time::sleep(poll).await;
    }
}

/// One steady-state tick: read node state, react to desync, else diff/fetch and
/// update the cache. `caught_up_prev` and `last_bulk_resync` are threaded by
/// `&mut` and mutated in place across ticks.
async fn steady_tick(
    rpc: &mut Rpc,
    state: &SharedState,
    cfg: &SyncConfig,
    caught_up_prev: &mut bool,
    last_bulk_resync: &mut Instant,
) {
    let info = match rpc.mempool_info().await {
        Ok(info) => info,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "mempool_info failed");
            reconnect_on_error(rpc, &e);
            set_synced(state, caught_up_prev, false, "rpc_error:mempool_info", 0);
            return;
        }
    };
    // Older nodes don't report `loaded` at all; treat that as loaded.
    let loaded = info.loaded.unwrap_or(true);
    let min_fee_sat_vb = mempool::min_fee_sat_vb(&info);

    let node_txids: HashSet<_> = match rpc.raw_mempool_txids().await {
        Ok(txids) => txids.into_iter().collect(),
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "raw_mempool_txids failed");
            reconnect_on_error(rpc, &e);
            set_synced(
                state,
                caught_up_prev,
                false,
                "rpc_error:raw_mempool_txids",
                0,
            );
            return;
        }
    };

    // Snapshot keys only — never clone tx values just to diff.
    let (cache_len, cache_keys) = {
        let g = read_state(state);
        (g.txs.len(), g.txs.keys().copied().collect::<HashSet<_>>())
    };

    // Evaluate cooldown AT the decision site (with a fresh `Instant::now()`),
    // not frozen at tick start — the RPC calls above may have taken real time.
    let cooling_down = resync_cooling_down(Some(*last_bulk_resync), Instant::now());
    if let Some(action) = decide_desync(loaded, cache_len, node_txids.len(), cooling_down) {
        match action {
            DesyncAction::WaitCooldown { reason } => {
                tracing::debug!(
                    loaded,
                    node_txid_count = node_txids.len(),
                    cache_len,
                    "mempool desync detected but bulk resync is in cooldown; waiting it out"
                );
                set_synced(state, caught_up_prev, false, reason, 0);
            }
            // `reason` (node_not_loaded / mass_drop) isn't logged on this arm:
            // the resync's own debug line and the fixed ""/"resync_failed"
            // state reasons below match the previous inline behavior.
            DesyncAction::BulkResync { reason: _ } => {
                tracing::debug!(
                    loaded,
                    node_txid_count = node_txids.len(),
                    cache_len,
                    "mempool desync detected (node reload or mass drop); resyncing from scratch"
                );
                // Stamp BEFORE the bulk (unlike initial_bulk_load): a failed
                // steady bulk still starts the 60s cooldown, so a flapping node
                // can't force a full download every tick.
                *last_bulk_resync = Instant::now();
                match bulk_resync(rpc, state).await {
                    Some(count) => apply_bulk_success(state, caught_up_prev, count),
                    None => set_synced(state, caught_up_prev, false, "resync_failed", 0),
                }
            }
        }
        return;
    }

    let diff = compute_diff(&cache_keys, &node_txids);

    // Apply removals immediately: the node's txid list is authoritative, so
    // departed txs shouldn't wait on the (possibly-failing) fetch of
    // newly-seen ones below.
    {
        let mut g = write_state(state);
        for txid in &diff.gone {
            g.txs.remove(txid);
        }
    }

    let res = fetch_new_entries(
        rpc,
        &diff.new,
        cfg.fetch_concurrency,
        cfg.tick_budget,
        MAX_NEW_FETCH_PER_TICK,
    )
    .await;

    let tip_height = rpc.tip_height().await.ok();

    // Only promote to "caught up" when this tick fully resolved the node's
    // new-txid list (didn't hit the per-tick cap, didn't bail on the time
    // budget, and every fetch either succeeded or the tx had already vanished).
    // Otherwise the cache is known to be behind the node, so `/health` should
    // say so. NOTE: `diff.new.len()` here is UNCAPPED.
    let backlog = is_backlog(
        diff.new.len(),
        MAX_NEW_FETCH_PER_TICK,
        res.fetch_errors,
        res.incomplete,
    );

    // Cache size after this tick's removals/inserts, computed from counts
    // already in hand rather than re-reading the cache under lock just for a
    // log field.
    let mempool_size = projected_mempool_size(cache_len, diff.gone.len(), res.fetched.len());

    // Per-tick building summary. `debug` so `info` prod runs stay quiet, but
    // `RUST_LOG=satya::sync=debug` shows the mempool churn live: how many txids
    // the node added/removed this tick, how many we fetched, the running size,
    // and whether we're keeping up.
    tracing::debug!(
        new = diff.new.len(),
        gone = diff.gone.len(),
        fetched = res.fetched.len(),
        size = mempool_size,
        backlog,
        "mempool tick"
    );

    {
        let mut g = write_state(state);
        for (txid, tx) in res.fetched {
            g.txs.insert(txid, tx);
        }
        g.mempool_min_fee_sat_vb = min_fee_sat_vb;
        if let Some(h) = tip_height {
            g.tip_height = h;
        }
        if backlog {
            set_synced_locked(&mut g, caught_up_prev, false, "backlog", 0);
        } else {
            set_synced_locked(&mut g, caught_up_prev, true, "", mempool_size);
            g.last_sync_ok = Some(SystemTime::now());
        }
    }
}

/// Fetch full details for (a capped slice of) this tick's new txids, best-effort,
/// bounded, and concurrent. A single failed fetch doesn't abort the batch or
/// discard txs already fetched this tick; we fetch up to `max_per_tick` with at
/// most `concurrency` calls in flight (via `buffer_unordered`); and we stop
/// consuming once this tick's fetch has run past `tick_budget`. Anything left
/// over (capped, dropped in-flight on budget bail, or errored) stays absent from
/// the cache, so it's still in `diff.new` (and gets fetched) next tick — nothing
/// is permanently lost.
async fn fetch_new_entries(
    rpc: &Rpc,
    new_txids: &[Txid],
    concurrency: usize,
    tick_budget: Duration,
    max_per_tick: usize,
) -> FetchBatchResult {
    let candidates: Vec<Txid> = new_txids.iter().take(max_per_tick).copied().collect();
    let attempted = candidates.len();
    let fetch_start = Instant::now();
    let mut fetched = Vec::with_capacity(attempted);
    let mut fetch_errors = 0usize;
    // Count of candidates whose fetch fully resolved this tick — incremented for
    // EVERY processed result including `Ok(None)` (vanished) and `Err`.
    // `incomplete` is computed from this after the loop: a tick is only "stale"
    // if candidates remain unresolved when we stopped, so a tick that resolved
    // every candidate but happened to cross the wall-clock budget on the last
    // item is NOT marked stale.
    let mut resolved = 0usize;
    // Per-result processing, shared between the main consume loop and the
    // budget-bail drain below so the Ok(Some)/Ok(None)/Err handling can't drift
    // between the two.
    let process = |txid: Txid,
                   res: Result<Option<MempoolTx>, RpcError>,
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
            async move {
                (
                    txid,
                    rpc.mempool_entry(&txid)
                        .await
                        .map(|opt| opt.map(|e| MempoolTx::from(&e))),
                )
            }
        })
        .buffer_unordered(concurrency);
    while let Some((txid, res)) = results.next().await {
        process(txid, res, &mut fetched, &mut fetch_errors, &mut resolved);
        // Time-based bail: stop consuming once this tick's fetch has run past
        // budget. Before stopping, drain any results that are ALREADY ready
        // without awaiting new work (`now_or_never`), so completed fetches this
        // tick aren't thrown away just because we crossed the budget. In-flight
        // (not-yet-ready) futures are dropped when we `break`; dropping a
        // reqwest request future cancels it, so no orphaned work is left
        // running, and those txids simply reappear in next tick's `diff.new`.
        if fetch_start.elapsed() > tick_budget {
            while let Some(Some((txid, res))) = results.next().now_or_never() {
                process(txid, res, &mut fetched, &mut fetch_errors, &mut resolved);
            }
            break;
        }
    }
    // Stale only if candidates remain unresolved after the (drained) stop — NOT
    // merely because wall-clock time crossed the budget.
    let incomplete = resolved < attempted;

    FetchBatchResult {
        fetched,
        fetch_errors,
        incomplete,
    }
}

/// Fetch the full mempool from the node and replace the in-memory cache's DATA
/// (`txs`, `mempool_min_fee_sat_vb`, `tip_height`) wholesale. Returns
/// `Some(count)` on success — where `count` is the number of txs loaded — or
/// `None` on failure (state left untouched by this call).
///
/// This does NOT set the freshness flags (`caught_up`, `last_sync_ok`): the
/// caller owns both (via `apply_bulk_success`) so they're set together under one
/// lock and `/health` never sees a full mempool with `caught_up` still false.
async fn bulk_resync(rpc: &mut Rpc, state: &SharedState) -> Option<usize> {
    let entries = match rpc.raw_mempool_verbose().await {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "raw_mempool_verbose failed during bulk resync");
            reconnect_on_error(rpc, &e);
            return None;
        }
    };
    let info = match rpc.mempool_info().await {
        Ok(info) => info,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "mempool_info failed during bulk resync");
            reconnect_on_error(rpc, &e);
            return None;
        }
    };
    let tip_height = match rpc.tip_height().await {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %short_err(&e), "tip_height failed during bulk resync");
            reconnect_on_error(rpc, &e);
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
    }
    tracing::info!(count, "mempool bulk resync complete");
    Some(count)
}
