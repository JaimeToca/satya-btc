use crate::mempool::{read_state, SharedState};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
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
    age_secs: Option<u64>,
}

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/fees", get(fees))
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
    let age_secs = s
        .last_sync_ok
        .and_then(|t| SystemTime::now().duration_since(t).ok())
        .map(|d| d.as_secs());
    Json(Health {
        caught_up: s.caught_up,
        mempool_size: s.txs.len(),
        tip_height: s.tip_height,
        mempool_min_fee_sat_vb: s.mempool_min_fee_sat_vb,
        network: s.network.to_string(),
        last_sync_ok,
        age_secs,
    })
}

/// Serve the cached fee estimate — but only when the sync layer vouches for the
/// mempool (`caught_up`). Returns `503` before the first estimate or whenever the
/// view is known to be behind, so we never serve a number we can't stand behind.
async fn fees(State(state): State<SharedState>) -> impl IntoResponse {
    let s = read_state(&state);
    match &s.fee_estimate {
        Some(est) if s.caught_up => Json(est.clone()).into_response(),
        _ => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}
