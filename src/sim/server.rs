//! Serves a [`MockNode`] over a real HTTP JSON-RPC endpoint (axum), with a
//! [`NetworkProfile`] applied, so the REAL reqwest [`crate::rpc::Rpc`] client
//! is exercised end-to-end (body streaming, timeouts, 429 classification)
//! entirely offline. Reached via the feature-gated `sim-serve` entrypoint
//! (`run_cli`, invoked from `main.rs` before the normal indexer boots).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::sim::network::{check_rate_limit, Limiter};
use crate::sim::{ChurnConfig, FeeDistribution, MockNode, NetworkProfile};

/// Shared server state: the churning node behind a plain (non-async) mutex —
/// every handler access is a brief lock-snapshot-unlock, never held across an
/// `.await` — plus the rate limiter (same discipline) and the profile
/// (immutable after startup, so no lock needed for it).
struct ServerState {
    node: Mutex<MockNode>,
    limiter: Mutex<Limiter>,
    profile: NetworkProfile,
}

#[derive(Deserialize)]
struct JsonRpcRequest {
    method: String,
    #[serde(default)]
    params: Vec<Value>,
}

/// Bind a `MockNode` behind an HTTP JSON-RPC endpoint (Core-shaped), gated by
/// `profile`'s rate limit. `port = 0` picks an ephemeral free port. Spawns the
/// axum server task AND a background churn timer (advances the node every 2s)
/// on the current tokio runtime, then returns the bound address.
///
/// Bind failures (e.g. the requested port is already in use) are propagated as
/// `Err` rather than panicking, so a caller (the `sim-serve` CLI, or a test
/// that races a port) can report a clean error instead of leaving a
/// half-started, unreachable server task behind.
pub async fn spawn(
    node: MockNode,
    profile: NetworkProfile,
    port: u16,
    block_secs: u64,
    reload_every: u32,
) -> anyhow::Result<SocketAddr> {
    use anyhow::Context;

    let state = Arc::new(ServerState {
        node: Mutex::new(node),
        limiter: Mutex::new(Limiter::new()),
        profile,
    });

    let app = Router::new()
        .route("/", post(handle_rpc))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("sim server failed to bind 127.0.0.1:{port}"))?;
    let addr = listener
        .local_addr()
        .context("sim server failed to read bound local_addr")?;

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "sim server exited with error");
        }
    });

    // Keep the mempool alive: advance churn on a fixed tick so a client
    // watching the live server sees arrivals/evictions over time. On a
    // configurable cadence, also mine a block (confirm top-fee txs, advance
    // the tip) and — every `reload_every` blocks — simulate a node restart
    // (mass-drop + loaded:false) so the indexer's resync path is exercised live.
    tokio::spawn(async move {
        const CHURN_SECS: u64 = 2;
        let tpb = ticks_per_block(block_secs, CHURN_SECS);
        let mut interval = tokio::time::interval(Duration::from_secs(CHURN_SECS));
        interval.tick().await; // consume the immediate first tick so the first advance() is a full period in
        let mut tick: u64 = 0;
        let mut blocks: u32 = 0;
        loop {
            interval.tick().await;
            tick += 1;
            let mut n = state.node.lock().unwrap();
            n.advance();
            if tpb != 0 && tick.is_multiple_of(tpb) {
                n.mine_block();
                blocks += 1;
                if reload_every != 0 && blocks.is_multiple_of(reload_every) {
                    // Node-restart disruption: drop most of the mempool and
                    // report loaded:false for the next poll.
                    n.mass_drop(0.8);
                    n.reload();
                }
            }
        }
    });

    Ok(addr)
}

/// How many `churn_secs`-length churn ticks elapse between simulated blocks.
/// `block_secs == 0` disables mining (returns 0). Otherwise the result is
/// floored at 1 so a sub-tick block interval mines every tick rather than never.
fn ticks_per_block(block_secs: u64, churn_secs: u64) -> u64 {
    if block_secs == 0 {
        0
    } else {
        (block_secs / churn_secs.max(1)).max(1)
    }
}

async fn handle_rpc(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<JsonRpcRequest>,
) -> Response {
    // Rate-limit gate first, mirroring `SimulatedRpc::gate`: lock briefly,
    // check/book the window, release — never held across an `.await`.
    if let Some(limit) = state.profile.req_per_sec {
        let within_budget = {
            let mut limiter = state.limiter.lock().unwrap();
            check_rate_limit(&mut limiter, limit, Instant::now())
        };
        if !within_budget {
            // Empty body (not a JSON-RPC envelope) so the real client's `call`
            // classifies this as `RpcError::HttpStatus { status: 429, .. }`
            // rather than trying to parse a `{result,error}` envelope.
            return (StatusCode::TOO_MANY_REQUESTS, "").into_response();
        }
    }

    if state.profile.latency > Duration::ZERO {
        tokio::time::sleep(state.profile.latency).await;
    }

    dispatch(&state, &req.method, &req.params).await
}

async fn dispatch(state: &ServerState, method: &str, params: &[Value]) -> Response {
    match method {
        "getblockchaininfo" => {
            let tip_height = {
                let n = state.node.lock().unwrap();
                n.tip_height_sync()
            };
            ok_response(json!({ "chain": "main", "blocks": tip_height }))
        }
        "getmempoolinfo" => {
            let loaded = {
                let n = state.node.lock().unwrap();
                n.loaded_sync()
            };
            ok_response(json!({
                "loaded": loaded,
                "mempoolminfee": 0.00001,
            }))
        }
        "getrawmempool" => {
            let verbose = params.first().and_then(Value::as_bool).unwrap_or(false);
            let entries = {
                let n = state.node.lock().unwrap();
                n.snapshot_entries()
            };
            if verbose {
                let map: serde_json::Map<String, Value> = entries
                    .into_iter()
                    .map(|(txid, entry)| (txid.to_string(), verbose_entry_json(&entry)))
                    .collect();
                ok_response(Value::Object(map))
            } else {
                let txids: Vec<String> = entries
                    .into_iter()
                    .map(|(txid, _)| txid.to_string())
                    .collect();
                ok_response(json!(txids))
            }
        }
        "getmempoolentry" => {
            let txid_str = params.first().and_then(Value::as_str).unwrap_or_default();
            let entry = match txid_str.parse::<bitcoin::Txid>() {
                Ok(txid) => {
                    let n = state.node.lock().unwrap();
                    n.entry_by_txid(&txid)
                }
                Err(_) => None,
            };
            match entry {
                Some(e) => ok_response(verbose_entry_json(&e)),
                None => Json(json!({
                    "result": null,
                    "error": { "code": -5, "message": "Transaction not in mempool" },
                    "id": 0,
                }))
                .into_response(),
            }
        }
        other => Json(json!({
            "result": null,
            "error": { "code": -32601, "message": format!("Method not found: {other}") },
            "id": 0,
        }))
        .into_response(),
    }
}

fn ok_response(result: Value) -> Response {
    Json(json!({ "result": result, "error": null, "id": 0 })).into_response()
}

fn verbose_entry_json(e: &crate::rpc::MempoolEntry) -> Value {
    json!({
        "vsize": e.vsize,
        "weight": e.weight,
        "depends": e.depends.iter().map(|d| d.to_string()).collect::<Vec<_>>(),
        "fees": {
            "base": e.fees.base.to_btc(),
            "ancestor": e.fees.ancestor.to_btc(),
            "descendant": e.fees.descendant.to_btc(),
        },
        "ancestorsize": e.ancestorsize,
        "descendantsize": e.descendantsize,
    })
}

/// Small `clap::Parser` scoped to the `sim-serve` entrypoint, parsed manually
/// from `main.rs` (the production CLI in `config.rs` is env-var driven and is
/// left untouched; see `main.rs` for the `sim-serve` dispatch guard).
#[derive(clap::Parser)]
#[command(name = "sim-serve")]
struct SimServeArgs {
    #[arg(long, default_value_t = 18443)]
    port: u16,
    #[arg(long, default_value_t = 20_000)]
    size: usize,
    #[arg(long, default_value_t = 600)]
    arrivals: usize,
    #[arg(long, default_value_t = 600)]
    evictions: usize,
    /// remote = getblock_remote profile; anything else = local_node.
    #[arg(long, default_value = "remote")]
    profile: String,
    /// Seconds between simulated blocks (confirm top-fee txs, advance tip).
    /// 0 = never mine (mempool churns only).
    #[arg(long, default_value_t = 30)]
    block_secs: u64,
    /// Simulate a node restart (mass-drop + loaded:false) every N blocks.
    /// 0 = never.
    #[arg(long, default_value_t = 0)]
    reload_every: u32,
    /// Fraction of arrivals that attach as a CPFP child (0 = no packages).
    #[arg(long, default_value_t = 0.15)]
    cpfp_fraction: f64,
    /// Max linear chain length (1 = no chaining).
    #[arg(long, default_value_t = 3)]
    max_chain: usize,
}

/// Entry point for `main.rs`'s `sim-serve` guard: parses the sim flags,
/// builds the `MockNode` + `NetworkProfile`, spawns the server, logs the
/// bound address, and blocks forever (the churn timer inside `spawn` keeps
/// the mempool alive).
pub async fn run_cli() -> anyhow::Result<()> {
    use clap::Parser;
    // `env::args()` includes the `sim-serve` token itself as argv[1]; clap
    // expects argv[0] to be the program name, so splice a placeholder in.
    let rest = std::env::args().skip(2);
    let args = std::iter::once("sim-serve".to_string()).chain(rest);
    let args = SimServeArgs::parse_from(args);

    let cpfp_fraction = if args.cpfp_fraction.is_nan() {
        tracing::warn!(
            value = args.cpfp_fraction,
            "cpfp_fraction is NaN; clamping to 0.0"
        );
        0.0
    } else {
        let clamped = args.cpfp_fraction.clamp(0.0, 1.0);
        if clamped != args.cpfp_fraction {
            tracing::warn!(
                value = args.cpfp_fraction,
                clamped,
                "cpfp_fraction out of [0.0, 1.0]; clamping"
            );
        }
        clamped
    };
    let max_chain = args.max_chain.max(1);
    if max_chain != args.max_chain {
        tracing::warn!(
            value = args.max_chain,
            max_chain,
            "max_chain below 1; flooring"
        );
    }

    let churn = ChurnConfig {
        arrivals_per_tick: args.arrivals,
        evictions_per_tick: args.evictions,
        fee: FeeDistribution {
            min_sat_vb: 1,
            max_sat_vb: 500,
        },
        cpfp_fraction,
        max_chain,
    };
    let node = MockNode::new(0, args.size, churn);
    let profile = if args.profile == "remote" {
        NetworkProfile::getblock_remote()
    } else {
        NetworkProfile::local_node()
    };

    let addr = spawn(node, profile, args.port, args.block_secs, args.reload_every).await?;
    tracing::info!(%addr, "sim node serving; point BTC_RPC_URL at it");
    std::future::pending::<anyhow::Result<()>>().await
}

#[cfg(test)]
mod cadence_tests {
    use super::ticks_per_block;

    #[test]
    fn ticks_per_block_maps_interval_to_churn_ticks() {
        // 0 disables mining entirely.
        assert_eq!(ticks_per_block(0, 2), 0);
        // 30s blocks over 2s churn ticks => mine every 15 ticks.
        assert_eq!(ticks_per_block(30, 2), 15);
        // Sub-tick intervals floor at 1 (mine every tick) rather than never.
        assert_eq!(ticks_per_block(1, 2), 1);
        assert_eq!(ticks_per_block(2, 2), 1);
    }
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod server_tests;
