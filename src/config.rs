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
    pub url: String,        // e.g. http://127.0.0.1:8332
    pub auth: RpcAuth,
    pub timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc: RpcConfig,
    pub http_bind: SocketAddr,
    pub poll_interval: Duration,
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
    #[arg(long, env = "HTTP_BIND", default_value = "127.0.0.1:8080")]
    http_bind: SocketAddr,
    #[arg(long, env = "POLL_INTERVAL_MS", default_value_t = 2000)]
    poll_interval_ms: u64,
    #[arg(long, env = "RPC_TIMEOUT_SECS", default_value_t = 30)]
    rpc_timeout_secs: u64,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let cli = Cli::parse();
        let auth = match (cli.rpc_cookie_file, cli.rpc_user, cli.rpc_pass) {
            (Some(path), _, _) => RpcAuth::Cookie(path),
            (None, Some(u), Some(p)) => RpcAuth::UserPass(u, p),
            _ => anyhow::bail!("provide BTC_RPC_COOKIE_FILE or both BTC_RPC_USER and BTC_RPC_PASS"),
        };
        Ok(Config {
            rpc: RpcConfig { url: cli.rpc_url, auth, timeout: Duration::from_secs(cli.rpc_timeout_secs) },
            http_bind: cli.http_bind,
            poll_interval: Duration::from_millis(cli.poll_interval_ms),
        })
    }
}
