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
    std::thread::spawn(move || sync::run(rpc, sync_state, poll));

    let listener = tokio::net::TcpListener::bind(cfg.http_bind).await?;
    tracing::info!("listening on http://{}", cfg.http_bind);
    axum::serve(listener, http::router(state)).await?;
    Ok(())
}
