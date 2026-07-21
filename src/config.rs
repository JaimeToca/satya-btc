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
    /// Additional `Name: Value` HTTP headers sent with every RPC request (e.g. a provider API
    /// key header like `X-Api-Key: abc123`). Additive with any auth mode, or with none at all.
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc: RpcConfig,
    pub http_bind: SocketAddr,
    pub poll_interval: Duration,
    /// When true, the sync loop logs one line per tick at INFO (adds/removes/
    /// size/tip). When false (default), it stays quiet in steady state and
    /// only logs sync-state transitions, the heartbeat, and errors.
    pub sync_log_verbose: bool,
    /// How often to emit a steady-state liveness "heartbeat" log at INFO, even
    /// when nothing changed. `Duration::ZERO` disables the heartbeat.
    pub heartbeat: Duration,
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
    /// Custom HTTP header(s) sent with every RPC request, as `Name: Value` (repeatable).
    #[arg(long = "rpc-header", env = "BTC_RPC_HEADERS", value_delimiter = ',')]
    rpc_headers: Vec<String>,
    #[arg(long, env = "HTTP_BIND", default_value = "127.0.0.1:8080")]
    http_bind: SocketAddr,
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 2000)]
    poll_interval_ms: u64,
    /// Log one INFO line per sync tick (adds/removes/size/tip). Accepts
    /// true/false/1/0/yes/no. Off by default; useful when validating a new node.
    #[arg(
        long,
        env = "SYNC_LOG_VERBOSE",
        action = clap::ArgAction::Set,
        default_value_t = false,
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    sync_log_verbose: bool,
    /// Seconds between steady-state liveness heartbeat logs. `0` disables it.
    #[arg(long, env = "HEARTBEAT_SECS", default_value_t = 30)]
    heartbeat_secs: u64,
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
        let headers = cli
            .rpc_headers
            .iter()
            .map(|entry| parse_header(entry))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Config {
            rpc: RpcConfig {
                url: cli.rpc_url,
                auth,
                timeout: Duration::from_secs(cli.rpc_timeout_secs),
                headers,
            },
            http_bind: cli.http_bind,
            poll_interval: Duration::from_millis(cli.poll_interval_ms),
            sync_log_verbose: cli.sync_log_verbose,
            heartbeat: Duration::from_secs(cli.heartbeat_secs),
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

/// Parses a single `--rpc-header`/`BTC_RPC_HEADERS` entry of the form `Name: Value`, splitting
/// on the first `:` and trimming whitespace from both sides.
fn parse_header(entry: &str) -> anyhow::Result<(String, String)> {
    let colon = entry.find(':').ok_or_else(|| {
        anyhow::anyhow!("malformed --rpc-header {entry:?}: expected \"Name: Value\"")
    })?;
    let name = entry[..colon].trim().to_string();
    let value = entry[colon + 1..].trim().to_string();
    if name.is_empty() {
        anyhow::bail!("malformed --rpc-header {entry:?}: header name is empty");
    }
    Ok((name, value))
}
