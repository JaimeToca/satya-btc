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
        fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
    }
}

fn cfg() -> SyncConfig {
    SyncConfig {
        poll_interval: Duration::from_millis(10),
        fetch_concurrency: 5,
        tick_budget: Duration::from_secs(30), // generous: budget bail isn't under test here
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

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;

    let g = read_state(&state);
    assert_eq!(g.txs.len(), 5_000);
    assert!(g.caught_up);
    assert!(g.last_sync_ok.is_some());
}

#[tokio::test]
async fn steady_churn_local_stays_caught_up() {
    let node = MockNode::new(2, 2_000, churn(30, 30));
    let mut rpc = SimulatedRpc::new(node.clone(), NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;

    // Advance the node and run several steady ticks; a fast local profile keeps up.
    for _ in 0..5 {
        rpc.inner_mut().advance();
        steady_tick(&mut rpc, &state, &cfg(), &mut caught_up_prev, &mut last_bulk).await;
        assert!(read_state(&state).caught_up, "local profile must stay caught up");
    }
}

#[tokio::test]
async fn rate_limited_remote_falls_behind() {
    // Heavy churn + a throttled (20 req/sec) profile => per-tx catch-up can't
    // keep up => backlog. Latency is kept at 1ms (not the 150ms
    // `getblock_remote` preset) so the test stays fast: the backlog here is
    // caused by the RATE LIMIT (429s => fetch_errors > 0 => is_backlog), not by
    // latency, so there is no need to pay the realistic latency cost in a test.
    let node = MockNode::new(3, 20_000, churn(600, 600));
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

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;
    assert!(read_state(&state).caught_up, "bulk verbose load succeeds even remote");

    rpc.inner_mut().advance();
    steady_tick(&mut rpc, &state, &cfg(), &mut caught_up_prev, &mut last_bulk).await;
    assert!(
        !read_state(&state).caught_up,
        "throttled per-tx catch-up must report backlog (caught_up=false)"
    );
}

#[tokio::test]
async fn mass_drop_triggers_resync() {
    let mut node = MockNode::new(4, 10_000, churn(0, 0));
    node.mass_drop(0.9);
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;
    assert_eq!(read_state(&state).txs.len(), 1_000, "cache matches post-drop node");
}
