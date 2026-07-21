use crate::mempool::{read_state, SharedState};
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::time::UNIX_EPOCH;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tower_http::LatencyUnit;
use tracing::Level;

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
    Router::new()
        .route("/health", get(health))
        // Access log: one INFO line per request with method, path, status, and
        // latency. Emitted through the same tracing subscriber as everything
        // else, so it obeys RUST_LOG. To quiet the frequent /health poll in
        // prod, filter the target, e.g. `RUST_LOG=info,tower_http::trace=warn`.
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
        )
        .with_state(state)
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
