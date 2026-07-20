use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

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

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        unimplemented!() // T2
    }
}
