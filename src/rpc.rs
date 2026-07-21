use bitcoincore_rpc::bitcoin::{Network, Txid};
use bitcoincore_rpc::jsonrpc;
use bitcoincore_rpc::{
    json::{GetMempoolEntryResult, GetMempoolInfoResult},
    Client, RpcApi,
};
use crate::config::RpcConfig;

pub struct Rpc {
    client: Client,
    cfg: RpcConfig,
}

/// Bitcoin Core's "transaction not in mempool" JSON-RPC error code
/// (`RPC_INVALID_ADDRESS_OR_KEY`).
const RPC_INVALID_ADDRESS_OR_KEY: i32 = -5;

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> {
        let client = crate::transport::build_client(cfg)?;
        Ok(Self { client, cfg: cfg.clone() })
    }

    pub fn network(&mut self) -> anyhow::Result<Network> {
        self.with_reconnect(|c| Ok(c.get_blockchain_info()?.chain))
    }

    pub fn tip_height(&mut self) -> anyhow::Result<u64> {
        self.with_reconnect(|c| Ok(c.get_blockchain_info()?.blocks))
    }

    /// Raw mempool status from a single `getmempoolinfo` call.
    pub fn mempool_info(&mut self) -> anyhow::Result<GetMempoolInfoResult> {
        self.with_reconnect(|c| Ok(c.get_mempool_info()?))
    }

    pub fn raw_mempool_txids(&mut self) -> anyhow::Result<Vec<Txid>> {
        self.with_reconnect(|c| Ok(c.get_raw_mempool()?))
    }

    pub fn raw_mempool_verbose(&mut self) -> anyhow::Result<Vec<(Txid, GetMempoolEntryResult)>> {
        self.with_reconnect(|c| Ok(c.get_raw_mempool_verbose()?.into_iter().collect()))
    }

    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub fn mempool_entry(
        &mut self,
        txid: &Txid,
    ) -> anyhow::Result<Option<GetMempoolEntryResult>> {
        self.with_reconnect(|c| match c.get_mempool_entry(txid) {
            Ok(entry) => Ok(Some(entry)),
            Err(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Rpc(rpc_err)))
                if rpc_err.code == RPC_INVALID_ADDRESS_OR_KEY =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        })
    }

    /// Runs `f` against the current client. On an auth or transport-level error, rebuilds the
    /// client once (re-reading the cookie file, in case it rotated) and retries `f` a single
    /// time before giving up.
    fn with_reconnect<T>(
        &mut self,
        f: impl Fn(&Client) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        match f(&self.client) {
            Ok(v) => Ok(v),
            Err(e) if is_reconnectable(&e) => {
                self.client = crate::transport::build_client(&self.cfg)?;
                f(&self.client)
            }
            Err(e) => Err(e),
        }
    }
}

/// Whether an error looks like an auth failure (e.g. rotated cookie) or a transport-level
/// problem, either of which warrants rebuilding the client and retrying once.
fn is_reconnectable(err: &anyhow::Error) -> bool {
    matches!(
        err.downcast_ref::<bitcoincore_rpc::Error>(),
        Some(bitcoincore_rpc::Error::JsonRpc(jsonrpc::error::Error::Transport(_)))
            | Some(bitcoincore_rpc::Error::Io(_))
    )
}
