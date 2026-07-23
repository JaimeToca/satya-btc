mod config;
mod http;
mod mempool;
mod rpc;
#[cfg(feature = "simulation")]
mod sim;
mod sync;
mod zmq;

use mempool::MempoolState;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = config::Config::from_env()?;
    let mut rpc = rpc::Rpc::connect(&cfg.rpc)?;
    // Bounded startup retry for the initial `network()` probe: a node restart /
    // cookie-not-yet-readable race at startup shouldn't crash-loop the process.
    // On a reconnectable (auth/transport) error, rebuild the client and retry a
    // few times; give up (propagate the error) after the bound so we can't hang
    // forever.
    let network = {
        const MAX_ATTEMPTS: u32 = 5;
        let mut attempt = 1;
        loop {
            match rpc.network().await {
                Ok(n) => break n,
                Err(e) if rpc::is_reconnectable(&e) && attempt < MAX_ATTEMPTS => {
                    tracing::warn!(
                        attempt,
                        max_attempts = MAX_ATTEMPTS,
                        error = %e,
                        "initial network() probe failed with a reconnectable error; reconnecting and retrying"
                    );
                    if let Err(re) = rpc.reconnect() {
                        tracing::warn!(error = %re, "rpc reconnect failed");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    };
    let state: mempool::SharedState = Arc::new(RwLock::new(MempoolState::new(network)));

    // Channel for the optional ZMQ block listener (Task 5) to wake the sync
    // loop for an immediate tick. Capacity 1 so `try_send` in the listener
    // debounces redundant block events.
    let (wake_tx, wake_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Opt-in: only if BTC_ZMQ_BLOCK is set do we subscribe to the node's
    // zmqpubhashblock publisher for immediate-on-block ticks. Unset = polling
    // only, unchanged behavior.
    if let Some(ep) = cfg.zmq_block.clone() {
        tracing::info!(endpoint = %ep, "starting zmq block listener");
        tokio::spawn(zmq::spawn_block_listener(ep, wake_tx.clone()));
    }

    // Keep a sender alive for the process lifetime regardless of whether the
    // listener runs: if the last `wake_tx` were dropped, `wake_rx.recv()` would
    // resolve immediately on a closed channel and spin the steady-state loop.
    let _wake_tx = wake_tx;

    // Sync loop as a tokio task (async RPC over reqwest).
    let sync_state = state.clone();
    let sync_cfg = sync::SyncConfig {
        poll_interval: cfg.poll_interval,
        fetch_concurrency: cfg.fetch_concurrency,
        tick_budget: cfg.tick_budget,
    };
    let sync_handle = tokio::spawn(sync::run(rpc, sync_state, sync_cfg, wake_rx));

    // sync::run only returns via panic (it's an infinite loop). Supervise the
    // task: if it ever ends, the process is silently frozen but still reporting
    // `/health`, which is worse than a clean exit. Log and terminate so a
    // process supervisor (systemd/docker) restarts us.
    tokio::spawn(async move {
        let _ = sync_handle.await;
        tracing::error!("sync task exited unexpectedly; shutting down");
        std::process::exit(1);
    });

    let listener = tokio::net::TcpListener::bind(cfg.http_bind).await?;
    tracing::info!("listening on http://{}", cfg.http_bind);
    axum::serve(listener, http::router(state)).await?;
    Ok(())
}
