mod config;
mod rpc;
mod mempool;
mod sync;
mod http;

use std::sync::{Arc, RwLock};
use mempool::MempoolState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "info".into())).init();

    let cfg = config::Config::from_env()?;
    let mut rpc = rpc::Rpc::connect(&cfg.rpc)?;
    let network = rpc.network()?;
    let state: mempool::SharedState = Arc::new(RwLock::new(MempoolState::new(network)));

    // Sync loop on its own OS thread (blocking RPC client).
    let sync_state = state.clone();
    let poll = cfg.poll_interval;
    let sync_handle = std::thread::spawn(move || sync::run(rpc, sync_state, poll));

    // sync::run only returns via panic (it's an infinite loop). Supervise the
    // thread off-runtime: if it ever ends, the process is silently frozen but
    // still reporting `/health`, which is worse than a clean exit. Log and
    // terminate so a process supervisor (systemd/docker) restarts us.
    tokio::spawn(async move {
        let _ = tokio::task::spawn_blocking(move || sync_handle.join()).await;
        tracing::error!("sync thread exited unexpectedly; shutting down");
        std::process::exit(1);
    });

    let listener = tokio::net::TcpListener::bind(cfg.http_bind).await?;
    tracing::info!("listening on http://{}", cfg.http_bind);
    axum::serve(listener, http::router(state)).await?;
    Ok(())
}
