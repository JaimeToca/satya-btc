use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

#[derive(Debug, Clone)]
pub enum RpcAuth {
    Cookie(PathBuf),
    UserPass(String, String),
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub url: String, // e.g. http://127.0.0.1:8332 or an HTTPS provider URL
    /// `None` when the URL itself carries the credential (e.g. a provider API key in the
    /// path, like `https://go.getblock.io/<KEY>`), so no separate basic auth is needed.
    pub auth: Option<RpcAuth>,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc: RpcConfig,
    pub http_bind: SocketAddr,
    pub poll_interval: Duration,
    /// Max concurrent getmempoolentry calls per tick. Bounded by node
    /// rpcthreads (default 4) / rpcworkqueue (default 16).
    pub fetch_concurrency: usize,
    /// Node zmqpubhashblock endpoint (e.g. tcp://127.0.0.1:28332) for
    /// immediate recompute on new blocks. Unset = polling only.
    pub zmq_block: Option<String>,
    /// Max fetch time per tick before bailing and marking stale. Default:
    /// 2 x POLL_INTERVAL_MS. Floored at `poll_interval` so a pathologically
    /// tiny value (e.g. `TICK_BUDGET_MS=0`) can't force a permanent backlog by
    /// bailing after the very first result.
    pub tick_budget: Duration,
}

#[derive(Parser)]
#[command(name = "btc-indexer")]
struct Cli {
    #[arg(long, env = "BTC_RPC_URL", default_value = "http://127.0.0.1:8332")]
    rpc_url: String,
    #[arg(long, env = "BTC_RPC_COOKIE_FILE")]
    rpc_cookie_file: Option<PathBuf>,
    #[arg(long, env = "BTC_RPC_USER")]
    rpc_user: Option<String>,
    #[arg(long, env = "BTC_RPC_PASS")]
    rpc_pass: Option<String>,
    #[arg(long, env = "RPC_TIMEOUT_SECS", default_value_t = 30)]
    rpc_timeout_secs: u64,
    #[arg(long, env = "HTTP_BIND", default_value = "127.0.0.1:8080")]
    http_bind: SocketAddr,
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 2000)]
    poll_interval_ms: u64,
    /// Max concurrent getmempoolentry calls per tick. Bounded by node
    /// rpcthreads (default 4) / rpcworkqueue (default 16).
    #[arg(long, env = "FETCH_CONCURRENCY", default_value_t = 10)]
    fetch_concurrency: usize,
    /// Node zmqpubhashblock endpoint (e.g. tcp://127.0.0.1:28332) for
    /// immediate recompute on new blocks. Unset = polling only.
    #[arg(long, env = "BTC_ZMQ_BLOCK")]
    zmq_block: Option<String>,
    /// Max fetch time per tick before bailing and marking stale. Default:
    /// 2 x POLL_INTERVAL_MS.
    #[arg(long, env = "TICK_BUDGET_MS")]
    tick_budget_ms: Option<u64>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let cli = Cli::parse();
        let auth = match (cli.rpc_cookie_file, cli.rpc_user, cli.rpc_pass) {
            (Some(path), _, _) => Some(RpcAuth::Cookie(path)),
            (None, Some(u), Some(p)) => Some(RpcAuth::UserPass(u, p)),
            // Neither cookie nor user/pass provided: assume the RPC URL itself carries the
            // credential (e.g. a provider API key embedded in the path), so no basic auth.
            _ => None,
        };
        Ok(Config {
            rpc: RpcConfig {
                url: cli.rpc_url,
                auth,
                timeout: Duration::from_secs(cli.rpc_timeout_secs),
            },
            http_bind: cli.http_bind,
            poll_interval: Duration::from_millis(cli.poll_interval_ms),
            fetch_concurrency: cli.fetch_concurrency.max(1),
            zmq_block: cli.zmq_block,
            tick_budget: Duration::from_millis(
                cli.tick_budget_ms
                    .unwrap_or(cli.poll_interval_ms.saturating_mul(2))
                    // Floor at the poll interval so a pathologically tiny
                    // TICK_BUDGET_MS (e.g. 0) can't bail after the first result
                    // and wedge the sync in permanent backlog.
                    .max(cli.poll_interval_ms),
            ),
        })
    }
}

