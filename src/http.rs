use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use crate::mempool::SharedState;

#[derive(Serialize)]
struct Health {
    caught_up: bool,
    mempool_size: usize,
    tip_height: u64,
    mempool_min_fee_sat_vb: f64,
    network: String,
}

pub fn router(state: SharedState) -> Router {
    Router::new().route("/health", get(health)).with_state(state)
}

async fn health(State(state): State<SharedState>) -> Json<Health> {
    let s = state.read().unwrap();
    Json(Health {
        caught_up: s.caught_up,
        mempool_size: s.txs.len(),
        tip_height: s.tip_height,
        mempool_min_fee_sat_vb: s.mempool_min_fee_sat_vb,
        network: s.network.to_string(),
    })
}
