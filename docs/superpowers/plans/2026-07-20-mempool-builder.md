# Mempool Builder (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A lightweight, Bitcoin-Core-RPC-only Rust service that keeps an accurate in-memory mempool current via a ~2s diff-sync poll, exposed through shared state and a `/health` endpoint.

**Architecture:** A blocking sync loop (on its own OS thread) polls Bitcoin Core over RPC, diffs the txid set against an in-memory cache, fetches details only for new txs, and writes to `Arc<RwLock<MempoolState>>`. axum (on tokio) serves read-only handlers from the same shared state.

**Tech Stack:** Rust, tokio, axum, `bitcoincore-rpc` (+ `bitcoin`), serde, tracing, clap.

## Global Constraints

- **Bitcoin Core RPC only.** No database, Redis, esplora, or electrum. No ZMQ this phase.
- **No tests this phase** (unit/integration handled later). Each task verifies with `cargo clippy`/`cargo check`; integration verified by a manual run at the end. Do NOT write `#[test]` code.
- **Money never via `f64`.** Fees come from Core as BTC decimals; use `rust-bitcoin`'s `Amount`. Convert `mempool_min_fee` (an `Amount` representing BTC/kvB) to sat/vB as `amount.to_sat() as f64 / 1000.0`.
- **Network is inferred from the node** (`get_blockchain_info().chain`), never configured.
- **Conservative Rust:** no macros/codegen. Reuse `bitcoincore-rpc`; do not hand-roll an RPC client.
- **Config from env + CLI**, single static binary, third-party deployable.
- **`caught_up`** starts false; the future `/fees` returns 503 until true. Phase 1 only reports it.
- Verify exact `bitcoincore-rpc` result field names against the pinned crate version — the API drifts between releases; adjust field access to match `cargo doc`.

## File Structure

```
btc-indexer/
  Cargo.toml           # dependencies (T1)
  src/
    main.rs            # wire config -> state -> sync thread -> axum (T1 skeleton, T7 final)
    config.rs          # Config, RpcConfig, from_env() (T2)
    rpc.rs             # Rpc wrapper over bitcoincore-rpc -> our types (T3)
    mempool.rs         # MempoolTx, MempoolState, compute_diff/apply (T1 types, T4 logic)
    sync.rs            # blocking poll loop: startup + steady-state + restart guard (T5)
    http.rs            # axum router + /health (T6)
```

## Execution Waves (for parallel dispatch)

- **Wave 0:** T1 (foundation — defines all interfaces, everything compiles).
- **Wave 1 (parallel, distinct files):** T2 `config.rs`, T3 `rpc.rs`, T4 `mempool.rs` logic.
- **Wave 2 (parallel, distinct files):** T5 `sync.rs`, T6 `http.rs`.
- **Wave 3:** T7 `main.rs` final wiring + manual run.

After T1, each task owns exactly one file, so Wave 1/2 tasks can run in parallel agents without edit conflicts.

---

### Task 1: Project foundation & interfaces

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/main.rs`
- Create: `src/config.rs`, `src/rpc.rs`, `src/mempool.rs`, `src/sync.rs`, `src/http.rs`

**Interfaces:**
- Produces (consumed by all later tasks): the type/function signatures below. Bodies are stubbed with `unimplemented!()` where noted so the crate compiles.

- [ ] **Step 1: Set dependencies in `Cargo.toml`**

```toml
[package]
name = "btc-indexer"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["rt-multi-thread", "macros", "net", "signal"] }
axum = "0.7"
bitcoincore-rpc = "0.19"
bitcoin = "0.32"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
clap = { version = "4", features = ["derive", "env"] }
anyhow = "1"
```

(Confirm `bitcoin` version matches the one `bitcoincore-rpc 0.19` re-exports; prefer `bitcoincore_rpc::bitcoin` re-export to avoid a version mismatch.)

- [ ] **Step 2: Define the mempool types in `src/mempool.rs`**

```rust
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use bitcoincore_rpc::bitcoin::{Amount, Network, Txid};

#[derive(Debug, Clone)]
pub struct MempoolTx {
    pub vsize: u32,
    pub weight: u32,
    pub fee: Amount,
    pub depends: Vec<Txid>,
}

#[derive(Debug)]
pub struct MempoolState {
    pub txs: HashMap<Txid, MempoolTx>,
    pub mempool_min_fee_sat_vb: f64,
    pub tip_height: u64,
    pub network: Network,
    pub caught_up: bool,
}

impl MempoolState {
    pub fn new(network: Network) -> Self {
        Self { txs: HashMap::new(), mempool_min_fee_sat_vb: 0.0, tip_height: 0, network, caught_up: false }
    }
}

pub type SharedState = Arc<RwLock<MempoolState>>;

/// Result of diffing the node's current txid set against our cache.
pub struct Diff {
    pub new: Vec<Txid>,   // present at node, absent from cache -> fetch details
    pub gone: Vec<Txid>,  // present in cache, absent at node -> remove
}

pub fn compute_diff(cache: &HashMap<Txid, MempoolTx>, node_txids: &HashSet<Txid>) -> Diff {
    unimplemented!() // T4
}

/// Insert freshly-fetched txs and remove departed ones.
pub fn apply(state: &mut MempoolState, gone: &[Txid], fetched: Vec<(Txid, MempoolTx)>) {
    unimplemented!() // T4
}
```

- [ ] **Step 3: Define the config interface in `src/config.rs`**

```rust
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
```

- [ ] **Step 4: Define the RPC wrapper interface in `src/rpc.rs`**

```rust
use bitcoincore_rpc::bitcoin::{Network, Txid};
use crate::config::RpcConfig;
use crate::mempool::MempoolTx;

pub struct Rpc {
    client: bitcoincore_rpc::Client,
}

impl Rpc {
    pub fn connect(cfg: &RpcConfig) -> anyhow::Result<Self> { unimplemented!() } // T3
    pub fn network(&self) -> anyhow::Result<Network> { unimplemented!() }        // T3
    pub fn tip_height(&self) -> anyhow::Result<u64> { unimplemented!() }         // T3
    pub fn mempool_loaded(&self) -> anyhow::Result<bool> { unimplemented!() }    // T3
    pub fn mempool_min_fee_sat_vb(&self) -> anyhow::Result<f64> { unimplemented!() } // T3
    pub fn raw_mempool_txids(&self) -> anyhow::Result<Vec<Txid>> { unimplemented!() } // T3
    pub fn raw_mempool_verbose(&self) -> anyhow::Result<Vec<(Txid, MempoolTx)>> { unimplemented!() } // T3
    /// `Ok(None)` if the tx disappeared between listing and fetch.
    pub fn mempool_entry(&self, txid: &Txid) -> anyhow::Result<Option<MempoolTx>> { unimplemented!() } // T3
}
```

- [ ] **Step 5: Define sync + http interfaces (stubs)**

`src/sync.rs`:
```rust
use crate::mempool::SharedState;
use crate::rpc::Rpc;
use std::time::Duration;

/// Blocking loop; call on a dedicated std::thread. Never returns under normal operation.
pub fn run(rpc: Rpc, state: SharedState, poll_interval: Duration) { unimplemented!() } // T5
```

`src/http.rs`:
```rust
use crate::mempool::SharedState;

pub fn router(state: SharedState) -> axum::Router { unimplemented!() } // T6
```

- [ ] **Step 6: Skeleton `src/main.rs` (compiles; real wiring in T7)**

```rust
mod config;
mod rpc;
mod mempool;
mod sync;
mod http;

fn main() {
    // Final wiring added in Task 7.
}
```

- [ ] **Step 7: Verify it compiles**

Run: `cargo clippy --all-targets`
Expected: compiles with no errors (dead-code / unused warnings are acceptable at this stage).

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock src/
git commit -m "feat: project skeleton and module interfaces for mempool builder"
```

---

### Task 2: Configuration (`config.rs`)  — Wave 1

**Files:**
- Modify: `src/config.rs`

**Interfaces:**
- Consumes: the `Config`/`RpcConfig`/`RpcAuth` structs from Task 1.
- Produces: a working `Config::from_env() -> anyhow::Result<Config>`.

- [ ] **Step 1: Implement `from_env` with clap deriving from env vars**

Use `clap` with `env` so the same fields work as flags or env vars. Defaults: `http_bind = 127.0.0.1:8080`, `poll_interval = 2000ms`, `timeout = 30s`. Auth resolves to `Cookie` if `BTC_RPC_COOKIE_FILE` is set, else `UserPass` from `BTC_RPC_USER`/`BTC_RPC_PASS`; error if neither is usable.

```rust
use clap::Parser;

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
```

- [ ] **Step 2: Verify**

Run: `cargo clippy` — no errors.

- [ ] **Step 3: Commit**

```bash
git add src/config.rs && git commit -m "feat: env/CLI configuration"
```

---

### Task 3: RPC wrapper (`rpc.rs`) — Wave 1

**Files:**
- Modify: `src/rpc.rs`

**Interfaces:**
- Consumes: `RpcConfig`/`RpcAuth` (T1), `MempoolTx` (T1).
- Produces: the working `Rpc` methods declared in Task 1 Step 4.

- [ ] **Step 1: Implement `connect` and the typed methods**

Map `bitcoincore-rpc` results into our types. Key conversions:
- `connect`: build `bitcoincore_rpc::Auth` from `RpcAuth` (`Auth::CookieFile(path)` / `Auth::UserPass(u,p)`), then `Client::new(&cfg.url, auth)`.
- `network`: `self.client.get_blockchain_info()?.chain` (a `bitcoin::Network`).
- `tip_height`: `get_blockchain_info()?.blocks`.
- `mempool_loaded`: `get_mempool_info()?.loaded` (`Option<bool>` → treat `None` as `true` for older nodes).
- `mempool_min_fee_sat_vb`: `get_mempool_info()?.mempool_min_fee.to_sat() as f64 / 1000.0`.
- `raw_mempool_txids`: `get_raw_mempool()?`.
- `raw_mempool_verbose`: `get_raw_mempool_verbose()?` → for each `(txid, entry)` build `MempoolTx { vsize: entry.vsize as u32, weight: entry.weight? as u32 (or entry.vsize*4 if weight absent), fee: entry.fees.base, depends: entry.depends }`.
- `mempool_entry`: `get_mempool_entry(txid)`; map the "tx not in mempool" RPC error (code -5 / -8) to `Ok(None)`, propagate other errors. Reuse the same entry→`MempoolTx` conversion (extract a private `fn entry_to_tx(txid, entry) -> MempoolTx`).

Confirm the exact `GetMempoolEntryResult` / `GetMempoolInfoResult` field names against `cargo doc -p bitcoincore-rpc --open` before finalizing; adjust `.vsize` / `.weight` / `.fees.base` / `.depends` access to match.

- [ ] **Step 2: Verify**

Run: `cargo clippy` — no errors.

- [ ] **Step 3: Commit**

```bash
git add src/rpc.rs && git commit -m "feat: typed Bitcoin Core RPC wrapper"
```

---

### Task 4: Diff-sync logic (`mempool.rs`) — Wave 1

**Files:**
- Modify: `src/mempool.rs`

**Interfaces:**
- Consumes: `MempoolTx`, `MempoolState`, `Diff` (T1).
- Produces: working `compute_diff` and `apply` (signatures from Task 1 Step 2).

- [ ] **Step 1: Implement `compute_diff`**

```rust
pub fn compute_diff(cache: &HashMap<Txid, MempoolTx>, node_txids: &HashSet<Txid>) -> Diff {
    let new = node_txids.iter().filter(|t| !cache.contains_key(*t)).copied().collect();
    let gone = cache.keys().filter(|t| !node_txids.contains(*t)).copied().collect();
    Diff { new, gone }
}
```

- [ ] **Step 2: Implement `apply`**

```rust
pub fn apply(state: &mut MempoolState, gone: &[Txid], fetched: Vec<(Txid, MempoolTx)>) {
    for txid in gone {
        state.txs.remove(txid);
    }
    for (txid, tx) in fetched {
        state.txs.insert(txid, tx);
    }
}
```

- [ ] **Step 3: Verify**

Run: `cargo clippy` — no errors.

- [ ] **Step 4: Commit**

```bash
git add src/mempool.rs && git commit -m "feat: mempool diff and apply logic"
```

---

### Task 5: Sync loop (`sync.rs`) — Wave 2

**Files:**
- Modify: `src/sync.rs`

**Interfaces:**
- Consumes: `Rpc` (T3), `SharedState`/`compute_diff`/`apply` (T1/T4).
- Produces: `pub fn run(rpc: Rpc, state: SharedState, poll_interval: Duration)`.

- [ ] **Step 1: Implement startup + steady-state loop**

Behavior:
1. **Startup:** poll `rpc.mempool_loaded()` until true (sleep `poll_interval` between tries, log at info). Then bulk-load via `rpc.raw_mempool_verbose()`, take the write lock, replace `state.txs`, set `network`/`tip_height`/`mempool_min_fee_sat_vb`, and set `caught_up = true`. Log the loaded count.
2. **Steady state loop** (every `poll_interval`):
   - `let node = rpc.raw_mempool_txids()?` → `HashSet<Txid>`.
   - **Restart guard:** if `!rpc.mempool_loaded()?` OR (`state.caught_up` and `node.len()` dropped below ~20% of `state.txs.len()` while `state.txs.len()` is large) → set `caught_up = false`, skip eviction this tick, log a warning, and `continue` (re-bulk-load on the next healthy tick). Keep the threshold a named `const MASS_DROP_RATIO: f64 = 0.2;`.
   - Otherwise compute the diff (read-lock to snapshot the current key set, or clone keys), fetch details for `diff.new` via `rpc.mempool_entry` (skip `Ok(None)`), then take the write lock once and call `apply(&mut state, &diff.gone, fetched)`, refresh `mempool_min_fee_sat_vb` and `tip_height`, ensure `caught_up = true`.
   - On any RPC error: log, sleep `poll_interval`, continue (do not crash the loop).
3. Sleep `poll_interval` at the end of each iteration.

Hold the `RwLock` write guard only around `apply` + field updates — never across RPC calls.

- [ ] **Step 2: Verify**

Run: `cargo clippy` — no errors.

- [ ] **Step 3: Commit**

```bash
git add src/sync.rs && git commit -m "feat: mempool sync loop with startup and restart guard"
```

---

### Task 6: HTTP `/health` (`http.rs`) — Wave 2

**Files:**
- Modify: `src/http.rs`

**Interfaces:**
- Consumes: `SharedState` (T1).
- Produces: `pub fn router(state: SharedState) -> axum::Router` serving `GET /health`.

- [ ] **Step 1: Implement router + handler**

```rust
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use crate::mempool::SharedState;

#[derive(Serialize)]
struct Health {
    caught_up: bool,
    mempool_size: usize,
    tip_height: u64,
    mempool_min_fee_sat_vb: f64,
    network: String,
}

pub fn router(state: SharedState) -> Router {
    Router::new().route("/health", get(health)).with_state(state)
}

async fn health(State(state): State<SharedState>) -> Json<Health> {
    let s = state.read().unwrap();
    Json(Health {
        caught_up: s.caught_up,
        mempool_size: s.txs.len(),
        tip_height: s.tip_height,
        mempool_min_fee_sat_vb: s.mempool_min_fee_sat_vb,
        network: s.network.to_string(),
    })
}
```

- [ ] **Step 2: Verify**

Run: `cargo clippy` — no errors.

- [ ] **Step 3: Commit**

```bash
git add src/http.rs && git commit -m "feat: /health endpoint"
```

---

### Task 7: Wire-up & manual run (`main.rs`) — Wave 3

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Implement `main`**

```rust
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
    let rpc = rpc::Rpc::connect(&cfg.rpc)?;
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
```

- [ ] **Step 2: Verify build**

Run: `cargo clippy --all-targets` — no errors.

- [ ] **Step 3: Manual run against a node**

Point at any reachable Core node (regtest/signet/testnet is fine):
```bash
BTC_RPC_URL=http://127.0.0.1:18443 \
BTC_RPC_COOKIE_FILE=/path/to/.cookie \
cargo run
# in another shell:
curl -s localhost:8080/health | jq
```
Expected: JSON with `caught_up` flipping to `true` shortly after start and `mempool_size` matching `bitcoin-cli getmempoolinfo` `size` (±churn).

- [ ] **Step 4: Commit**

```bash
git add src/main.rs && git commit -m "feat: wire config, sync thread, and http server"
```

---

## Self-Review

**Spec coverage:** RPC connection (T3) · diff-sync with cold-load + steady-state + restart guard (T5) · shared state (T1) · `/health` (T6) · config with inferred network (T2/T7) · money via `Amount`, min-fee→sat/vB (T3) · `caught_up` gate field (T1/T5/T6) · `bitcoincore-rpc` reuse (T3) · sync-thread + axum-tokio split (T7). All spec sections map to a task.

**Placeholder scan:** The `unimplemented!()` bodies in T1 are intentional compiling stubs, each filled by a named later task (T2–T6); no task ships them. No "TBD"/"add error handling" hand-waving — error behavior is specified per task.

**Type consistency:** `MempoolTx { vsize, weight, fee, depends }`, `MempoolState` fields, `compute_diff`/`apply`/`Rpc` method names are used identically across T1–T7.
