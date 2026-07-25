//! End-to-end sync-loop scenarios against the simulated node. Fast and
//! deterministic — no network, no wall-clock dependence except where a scenario
//! explicitly needs the resync cooldown.
use std::time::Duration;

use super::*; // brings initial_bulk_load, steady_tick, SyncConfig, read_state, etc.
use crate::sim::{ChurnConfig, FeeDistribution, MockNode, NetworkProfile, SimulatedRpc};
use bitcoin::Network;

fn churn(arrivals: usize, evictions: usize) -> ChurnConfig {
    ChurnConfig {
        arrivals_per_tick: arrivals,
        evictions_per_tick: evictions,
        fee: FeeDistribution::uniform(1, 500),
        cpfp_fraction: 0.0,
        max_chain: 1,
    }
}

fn cfg() -> SyncConfig {
    SyncConfig {
        poll_interval: Duration::from_millis(10),
        fetch_concurrency: 5,
        tick_budget: Duration::from_secs(30), // generous: budget bail isn't under test here
        fee_recompute_min_interval: Duration::from_millis(2000),
    }
}

/// Construct an empty shared state the way `run` expects it: `MempoolState` has
/// no `Default` impl, so build via `MempoolState::new(network)` and wrap in the
/// real `Arc<RwLock<_>>` alias.
fn empty_state() -> SharedState {
    std::sync::Arc::new(std::sync::RwLock::new(MempoolState::new(Network::Bitcoin)))
}

#[tokio::test]
async fn cold_bulk_load_builds_full_mempool() {
    let node = MockNode::new(1, 5_000, churn(0, 0));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;

    let g = read_state(&state);
    assert_eq!(g.txs.len(), 5_000);
    assert!(g.caught_up);
    assert!(g.last_sync_ok.is_some());
}

#[tokio::test]
async fn steady_churn_local_stays_caught_up() {
    let node = MockNode::new(2, 2_000, churn(30, 30));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;

    // Advance the node and run several steady ticks; a fast local profile keeps up.
    for _ in 0..5 {
        rpc.inner_mut().advance();
        steady_tick(
            &mut rpc,
            &state,
            &cfg(),
            &mut caught_up_prev,
            &mut last_bulk,
            &mut std::time::Instant::now(),
        )
        .await;
        assert!(
            read_state(&state).caught_up,
            "local profile must stay caught up"
        );
        assert_eq!(
            read_state(&state).txs.len(),
            rpc.inner_mut().len(),
            "a fast local profile must fully track the node's size every tick, not just report caught_up"
        );
    }
}

#[tokio::test]
async fn rate_limited_remote_falls_behind() {
    // Heavy churn + a throttled (20 req/sec) profile => per-tx catch-up can't
    // keep up => backlog. Latency is kept at 1ms (not the 150ms
    // `getblock_remote` preset) so the test stays fast: the backlog here is
    // caused by the RATE LIMIT (429s => fetch_errors > 0 => is_backlog), not by
    // latency, so there is no need to pay the realistic latency cost in a test.
    // The initial mempool size only matters for the bulk load's own cost (the
    // backlog condition itself is driven by per-tick churn vs. the 20/sec rate
    // limit, not total size), so it's shrunk from 20,000 to 3,000 to cut the
    // dominant cost (the verbose bulk-load clone) — confirmed still reliably
    // triggers `caught_up == false` below.
    let node = MockNode::new(3, 3_000, churn(600, 600));
    let profile = NetworkProfile {
        latency: Duration::from_millis(1),
        req_per_sec: Some(20),
        body_cap: None,
        drop_rate: 0.0,
    };
    let mut rpc = SimulatedRpc::new(node, profile);
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;
    assert!(
        read_state(&state).caught_up,
        "bulk verbose load succeeds even remote"
    );

    rpc.inner_mut().advance();
    steady_tick(
        &mut rpc,
        &state,
        &cfg(),
        &mut caught_up_prev,
        &mut last_bulk,
        &mut std::time::Instant::now(),
    )
    .await;
    assert!(
        !read_state(&state).caught_up,
        "throttled per-tx catch-up must report backlog (caught_up=false)"
    );

    // Prove the backlog is genuine partial progress caused by 429s, not the
    // alternate cause `new_count > MAX_NEW_FETCH_PER_TICK` (2000): this tick's
    // uncapped `diff.new` is only 600 (churn(600, 600)'s arrivals_per_tick),
    // well under the 2000 cap, so that branch of `is_backlog` can't be what
    // tripped `caught_up = false` here — it must be `fetch_errors > 0` from the
    // 20/sec rate limit. Removals (`diff.gone`) are applied unconditionally
    // (no RPC involved), so the cache lost all 600 evicted txids this tick
    // while the throttled fetch pass could only land a handful of the 600
    // new arrivals within the budget; the node's total size is restored to
    // 3,000 by `advance()` (600 evicted + 600 arrived), so the cache must be
    // strictly behind the node (some fetches succeeded, but nowhere near all
    // of them) while still holding the bulk of its pre-tick contents.
    let cache_len = read_state(&state).txs.len();
    let node_len = rpc.inner_mut().len();
    assert!(cache_len > 0, "cache must not be emptied by the rate limit");
    assert!(
        cache_len < node_len,
        "cache ({cache_len}) must be strictly behind the node ({node_len}): the 429s dropped \
         some fetches, not all, proving partial progress rather than a total stall"
    );
}

#[tokio::test]
async fn mass_drop_via_bulk_load_shrinks_cache() {
    // `initial_bulk_load` always bulk-loads regardless of desync logic, so this
    // only proves the bulk-load path faithfully reflects whatever the node
    // currently holds — it does NOT exercise `steady_tick`'s desync-DETECTION
    // branch (see `steady_tick_detects_mass_drop_and_defers_under_cooldown` /
    // `steady_tick_mass_drop_resyncs_when_cooldown_expired` for that).
    let mut node = MockNode::new(4, 10_000, churn(0, 0));
    node.mass_drop(0.9);
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;
    assert_eq!(
        read_state(&state).txs.len(),
        1_000,
        "cache matches post-drop node"
    );
}

#[tokio::test]
async fn steady_tick_detects_mass_drop_and_defers_under_cooldown() {
    // Distinguishes `steady_tick`'s desync-DETECTION branch (`decide_desync` ->
    // `DesyncAction::WaitCooldown`) from the normal per-tx diff path, using the
    // real constants in `decision.rs`: MASS_DROP_MIN_CACHE_SIZE = 100,
    // MASS_DROP_INVERSE = 5 (mass drop when `node * 5 < cache`), RESYNC_COOLDOWN
    // = 60s.
    let node = MockNode::new(5, 5_000, churn(0, 0));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;
    assert_eq!(read_state(&state).txs.len(), 5_000);
    assert!(read_state(&state).caught_up);

    // mass_drop(0.9) on 5000 txs removes (5000 * 0.9) as usize = 4500, leaving
    // exactly 500. node*5 = 2500 < cache_len (5000) => is_mass_drop is true;
    // cache_len (5000) >= MASS_DROP_MIN_CACHE_SIZE (100).
    rpc.inner_mut().mass_drop(0.9);
    assert_eq!(rpc.inner_mut().len(), 500);

    // Cooldown ACTIVE: last_bulk_resync is "now", well within RESYNC_COOLDOWN
    // (60s), so `decide_desync` must route to `WaitCooldown`, NOT `BulkResync`.
    let mut last_bulk_resync = std::time::Instant::now();
    steady_tick(
        &mut rpc,
        &state,
        &cfg(),
        &mut caught_up_prev,
        &mut last_bulk_resync,
        &mut std::time::Instant::now(),
    )
    .await;

    // The distinguishing assertion: the NORMAL per-tx diff path would have
    // removed the ~4500 departed txids and shrunk the cache to ~500. The cache
    // staying at 5000 proves `decide_desync` took the mass-drop ->
    // `WaitCooldown` branch (desync detected, resync deferred by cooldown)
    // instead of falling through to the normal diff/fetch path. A plain "cache
    // shrank" assertion could NOT prove this — the point is that it did NOT.
    assert_eq!(
        read_state(&state).txs.len(),
        5_000,
        "cache must be untouched while the mass-drop resync is cooling down"
    );
    assert!(
        !read_state(&state).caught_up,
        "mass drop under cooldown must report out of sync"
    );
}

#[tokio::test]
async fn fee_estimate_is_populated_and_monotone() {
    let node = MockNode::new(99, 5_000, churn(50, 40));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();
    // Backdate the fee-recompute timer so the throttle (2s in `cfg()`) lets the
    // recompute run on this tick instead of skipping it.
    let mut last_fee = std::time::Instant::now() - Duration::from_secs(10);

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;
    rpc.inner_mut().advance();
    steady_tick(
        &mut rpc,
        &state,
        &cfg(),
        &mut caught_up_prev,
        &mut last_bulk,
        &mut last_fee,
    )
    .await;

    let g = read_state(&state);
    assert!(
        g.caught_up,
        "local profile should be caught up after a healthy tick"
    );
    let est = g
        .fee_estimate
        .clone()
        .expect("fee estimate should be populated after a healthy tick with the recompute due");
    // Tiers finite, floored at the relay minimum, and monotone non-increasing.
    assert!(est.next_block.is_finite());
    assert!(est.next_block >= est.relay_floor - 1e-9);
    assert!(est.next_block >= est.within_3_blocks - 1e-9);
    assert!(est.within_3_blocks >= est.within_6_blocks - 1e-9);
    assert!(est.within_6_blocks >= est.horizon - 1e-9);
    assert!(est.horizon >= est.relay_floor - 1e-9);
}

#[tokio::test]
async fn steady_tick_mass_drop_resyncs_when_cooldown_expired() {
    // Companion to `steady_tick_detects_mass_drop_and_defers_under_cooldown`:
    // once the cooldown has expired, the SAME mass-drop condition must route to
    // `DesyncAction::BulkResync` and actually reload from the node, shrinking
    // the cache to match.
    let node = MockNode::new(6, 5_000, churn(0, 0));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(
        &mut rpc,
        &state,
        Duration::from_millis(10),
        &mut caught_up_prev,
    )
    .await;
    assert_eq!(read_state(&state).txs.len(), 5_000);

    rpc.inner_mut().mass_drop(0.9);
    assert_eq!(rpc.inner_mut().len(), 500);

    // Cooldown EXPIRED: last_bulk_resync is 61s in the past, past RESYNC_COOLDOWN
    // (60s), so `decide_desync` must route to `BulkResync` and reload now.
    let mut last_bulk_resync = std::time::Instant::now() - Duration::from_secs(61);
    steady_tick(
        &mut rpc,
        &state,
        &cfg(),
        &mut caught_up_prev,
        &mut last_bulk_resync,
        &mut std::time::Instant::now(),
    )
    .await;

    assert_eq!(
        read_state(&state).txs.len(),
        500,
        "bulk resync must fire once the cooldown has expired, reloading from the node"
    );
    assert!(
        read_state(&state).caught_up,
        "a successful bulk resync must report caught up"
    );
}

#[tokio::test]
async fn mocknode_packages_lift_a_parent_through_the_estimator() {
    use crate::rpc::MempoolRpc;
    use crate::sim::{ChurnConfig, FeeDistribution, MockNode};
    // Dense packages: every eligible arrival attaches as a high-fee child.
    let node = MockNode::new(
        2024,
        400,
        ChurnConfig {
            arrivals_per_tick: 0,
            evictions_per_tick: 0,
            fee: FeeDistribution::uniform(1, 100),
            cpfp_fraction: 1.0,
            max_chain: 3,
        },
    );
    // Build the snapshot the estimator consumes: (txid, fee_sats, weight, depends).
    let snap: Vec<(bitcoin::Txid, u64, u32, Vec<bitcoin::Txid>)> = node
        .raw_mempool_verbose()
        .await
        .unwrap()
        .into_iter()
        .map(|(txid, e)| {
            (
                txid,
                e.fees.base.to_sat(),
                e.weight.unwrap_or(e.vsize * 4) as u32,
                e.depends.clone(),
            )
        })
        .collect();

    let gbt_txs = crate::fees::snapshot_to_gbt(&snap);
    let proj = crate::gbt::project(gbt_txs);

    // Some in-set parent (a tx another tx depends on) must be lifted above its
    // own solo rate by its CPFP child.
    let lifted = snap.iter().enumerate().any(|(i, (t, fee, weight, _))| {
        let is_parent = snap.iter().any(|(_, _, _, d)| d.contains(t));
        if !is_parent {
            return false;
        }
        let own = 4.0 * *fee as f64 / *weight as f64;
        proj.effective_rates
            .get(&(i as u32))
            .is_some_and(|&eff| eff > own + 1e-9)
    });
    assert!(
        lifted,
        "MockNode CPFP packages must lift at least one parent through the estimator"
    );
}
