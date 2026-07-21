use crate::config::RpcConfig;
use bitcoincore_rpc::bitcoin::{Network, Txid};
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::{
    json::{GetMempoolEntryResult, GetMempoolInfoResult},
    Client, RpcApi,
};
use std::sync::Arc;

/// A cheaply-cloneable, async-callable handle to a Bitcoin Core RPC client.
///
/// The underlying `bitcoincore_rpc::Client` is blocking, so each typed method
/// runs its blocking call on the tokio blocking pool via `spawn_blocking`. The
/// `Client` is held behind an `Arc` so it can be cloned into those `'static`
/// closures and shared across concurrent fetches (Task 4) without rebuilding a
/// connection each time.
#[derive(Clone)]
pub struct Rpc {
    client: Arc<Client>,
    cfg: RpcConfig,
}

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`).
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        Ok(Self {
            client: Arc::new(crate::transport::build_client(cfg)?),
            cfg: cfg.clone(),
        })
    }

    pub async fn network(&self) -> anyhow::Result<Network> {
        let client = self.client.clone();
        spawn_blocking(move || Ok(client.get_blockchain_info()?.chain)).await
    }

    pub async fn tip_height(&self) -> anyhow::Result<u64> {
        let client = self.client.clone();
        spawn_blocking(move || Ok(client.get_blockchain_info()?.blocks)).await
    }

    /// Raw mempool status from a single `getmempoolinfo` call.
    pub async fn mempool_info(&self) -> anyhow::Result<GetMempoolInfoResult> {
        let client = self.client.clone();
        spawn_blocking(move || Ok(client.get_mempool_info()?)).await
    }

    pub async fn raw_mempool_txids(&self) -> anyhow::Result<Vec<Txid>> {
        let client = self.client.clone();
        spawn_blocking(move || Ok(client.get_raw_mempool()?)).await
    }

    pub async fn raw_mempool_verbose(&self) -> anyhow::Result<Vec<(Txid, GetMempoolEntryResult)>> {
        let client = self.client.clone();
        spawn_blocking(move || Ok(client.get_raw_mempool_verbose()?.into_iter().collect())).await
    }

    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub async fn mempool_entry(
        &self,
        txid: &Txid,
    ) -> anyhow::Result<Option<GetMempoolEntryResult>> {
        let client = self.client.clone();
        let txid = *txid;
        spawn_blocking(move || match client.get_mempool_entry(&txid) {
            Ok(entry) => Ok(Some(entry)),
            Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Rpc(rpc_err)))
                if rpc_err.code == RPC_INVALID_ADDRESS_OR_KEY =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        })
        .await
    }

    /// Rebuild the underlying client (re-reading the cookie file, in case it
    /// rotated), replacing the shared `Arc`. Called at the loop level after a
    /// reconnectable (auth/transport) error.
    ///
    /// Semantic shift vs. the previous in-call `with_reconnect`: reconnect used
    /// to happen inline ("retry immediately in-call"). Now it happens at the
    /// loop level ("reconnect, then retry on the next tick"). This is
    /// acceptable because `minreq` opens a fresh connection per call, so
    /// transient transport errors self-heal on their own; and the loop already
    /// degrades (`caught_up=false`) and retries on the next tick regardless.
    pub fn reconnect(&mut self) -> anyhow::Result<()> {
        self.client = Arc::new(crate::transport::build_client(&self.cfg)?);
        Ok(())
    }
}

/// Run a blocking RPC closure on the tokio blocking pool and unwrap the
/// `JoinError` as a bug (`spawn_blocking` only fails to join if the closure
/// panicked, which is not something callers can recover from).
async fn spawn_blocking<T, F>(f: F) -> anyhow::Result<T>
where
    F: FnOnce() -> anyhow::Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .expect("rpc blocking task panicked")
}

/// Whether an error looks like an auth failure (e.g. rotated cookie) or a transport-level
/// problem, either of which warrants rebuilding the client and retrying.
pub fn is_reconnectable(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<bitcoincore_rpc::Error>(),
        Some(bitcoincore_rpc::Error::JsonRpc(
            jsonrpc::error::Error::Transport(_)
        )) | Some(bitcoincore_rpc::Error::Io(_))
    )
}
