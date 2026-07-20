use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::time::UNIX_EPOCH;
use crate::mempool::{read_state, SharedState};

#[derive(Serialize)]
struct Health {
    caught_up: bool,
    mempool_size: usize,
    tip_height: u64,
    mempool_min_fee_sat_vb: f64,
    network: String,
    last_sync_ok: Option<u64>,
}

pub fn router(state: SharedState) -> Router {
    Router::new().route("/health", get(health)).with_state(state)
}

async fn health(State(state): State<SharedState>) -> Json<Health> {
    let s = read_state(&state);
    let last_sync_ok = s
        .last_sync_ok
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs());
    Json(Health {
        caught_up: s.caught_up,
        mempool_size: s.txs.len(),
        tip_height: s.tip_height,
        mempool_min_fee_sat_vb: s.mempool_min_fee_sat_vb,
        network: s.network.to_string(),
        last_sync_ok,
    })
}
