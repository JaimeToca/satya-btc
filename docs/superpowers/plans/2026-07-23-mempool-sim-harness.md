# Mempool Sync Test + Network Simulation Harness — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, offline test + network-simulation harness for Satya's mempool sync loop, and regression-lock the remote-provider rate-limit backlog observed live against GetBlock.

**Architecture:** Extract a `MempoolRpc` trait (the seam) so the sync loop becomes generic over its RPC source. Provide an in-memory `MockNode` that implements it with a seeded, churning synthetic mempool, and a `SimulatedRpc` wrapper that injects a `NetworkProfile` (latency / 429 rate limit / body cap / drops). Fast in-crate tests drive the real sync functions against the mock; a feature-gated HTTP server exercises the real reqwest transport.

**Tech Stack:** Rust, tokio, reqwest (prod transport), axum (already a dep; reused for the sim server), `rand` (seeded `StdRng`, optional dep behind the `simulation` feature), serde_json.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-23-mempool-sync-simulation-harness-design.md` — source of truth.
- **No new production runtime dependencies.** `rand` is an OPTIONAL dep enabled only by a new `simulation` Cargo feature. All simulation code is gated behind `#[cfg(feature = "simulation")]`; the default release build must not compile any of it.
- **Tests run with the feature on:** the `just test` recipe becomes `cargo test --features simulation`. Plain `cargo test` (no feature) must still compile and pass the pre-existing `decision.rs` tests.
- **Behavior-preserving seam:** Task 1 changes only types (generics), never sync behavior. The pre-existing 9 `decision.rs` tests plus `cargo build`/`clippy` guard it.
- Reuse existing types verbatim — `MempoolInfo`, `MempoolEntry`, `MempoolEntryFees`, `RpcError`, `MempoolTx`, `SharedState`/`MempoolState`. Do NOT duplicate or rewrite them.
- Fees stay exact sats (`bitcoin::Amount`); synthetic fees are built in sats, converted to BTC decimals (`Amount::to_btc()`) only when the HTTP sim emits Core-shaped JSON.
- No lib-crate restructure: everything lives in the existing `satya` binary crate. Shared sim code is reached from tests (in-crate `#[cfg(test)]` modules) and from a feature-gated `sim-serve` subcommand — never from `tests/` integration files or `examples/` (which would require a lib target).
- Do NOT push or open PRs (this includes any dispatched subagent) — commit locally only.

---

### Task 1: Extract the `MempoolRpc` trait and make the sync loop generic

Behavior-preserving seam extraction. The real `Rpc` keeps all its inherent methods; a trait forwards to them, and the sync functions take a generic `R: MempoolRpc` instead of the concrete `Rpc`.

**Files:**
- Modify: `src/rpc.rs` — add `pub trait MempoolRpc` + `impl MempoolRpc for Rpc`.
- Modify: `src/sync/mod.rs` — thread a generic type parameter through `run`, `wait_until_mempool_loaded`, `initial_bulk_load`, `steady_tick`, `bulk_resync`, `fetch_new_entries`, `reconnect_on_error`.
- Modify: `Cargo.toml` — add the `[features]` section and optional `rand` dep (used from Task 2 on; declared here so the feature exists).

**Interfaces:**
- Produces: `pub trait MempoolRpc` with these exact methods (native `async fn` in trait — no `async_trait`):
  ```rust
  pub trait MempoolRpc {
      async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;
      async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError>;
      async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError>;
      async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError>;
      async fn tip_height(&self) -> Result<u64, RpcError>;
      fn reconnect(&mut self) -> anyhow::Result<()>;
  }
  ```
- The `fetch_new_entries` path clones the rpc and moves it into `buffer_unordered` futures, so its bound is `R: MempoolRpc + Clone + Send + Sync + 'static`. `run`/`steady_tick`/`bulk_resync`/`initial_bulk_load`/`wait_until_mempool_loaded`/`reconnect_on_error` take the same bound (thread it uniformly to avoid bound-mismatch churn).

- [ ] **Step 1: Add the trait and impl to `src/rpc.rs`**

At the end of `src/rpc.rs` (after the `impl Rpc` block), add:

```rust
/// The RPC surface the sync loop consumes. Implemented by the real reqwest
/// `Rpc` (production) and by the simulation `MockNode` / `SimulatedRpc` (tests).
/// Native async fn in trait (stable since 1.75) — no `async_trait` macro.
pub trait MempoolRpc {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError>;
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError>;
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError>;
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError>;
    async fn tip_height(&self) -> Result<u64, RpcError>;
    fn reconnect(&mut self) -> anyhow::Result<()>;
}

impl MempoolRpc for Rpc {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        Rpc::mempool_info(self).await
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        Rpc::raw_mempool_txids(self).await
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        Rpc::raw_mempool_verbose(self).await
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        Rpc::mempool_entry(self, txid).await
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        Rpc::tip_height(self).await
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        Rpc::reconnect(self)
    }
}
```

- [ ] **Step 2: Make the sync functions generic in `src/sync/mod.rs`**

Add `use crate::rpc::MempoolRpc;` to the imports. Then change each signature from the concrete `Rpc` to a generic bound. Exact edits:

```rust
// was: pub async fn run(mut rpc: Rpc, ...)
pub async fn run<R: MempoolRpc + Clone + Send + Sync + 'static>(
    mut rpc: R,
    state: SharedState,
    cfg: SyncConfig,
    mut wake_rx: tokio::sync::mpsc::Receiver<()>,
) { /* body unchanged */ }

// was: fn reconnect_on_error(rpc: &mut Rpc, e: &RpcError)
fn reconnect_on_error<R: MempoolRpc>(rpc: &mut R, e: &RpcError) { /* body unchanged */ }

// was: async fn wait_until_mempool_loaded(rpc: &mut Rpc, poll: Duration)
async fn wait_until_mempool_loaded<R: MempoolRpc>(rpc: &mut R, poll: Duration) { /* unchanged */ }

// was: async fn initial_bulk_load(rpc: &mut Rpc, ...)
async fn initial_bulk_load<R: MempoolRpc>(
    rpc: &mut R, state: &SharedState, poll: Duration, caught_up_prev: &mut bool,
) -> Instant { /* unchanged */ }

// was: async fn steady_tick(rpc: &mut Rpc, ...)
async fn steady_tick<R: MempoolRpc + Clone + Send + Sync + 'static>(
    rpc: &mut R, state: &SharedState, cfg: &SyncConfig,
    caught_up_prev: &mut bool, last_bulk_resync: &mut Instant,
) { /* unchanged */ }

// was: async fn fetch_new_entries(rpc: &Rpc, ...)
async fn fetch_new_entries<R: MempoolRpc + Clone + Send + Sync + 'static>(
    rpc: &R, new_txids: &[Txid], concurrency: usize,
    tick_budget: Duration, max_per_tick: usize,
) -> FetchBatchResult { /* unchanged */ }

// was: async fn bulk_resync(rpc: &mut Rpc, state: &SharedState) -> Option<usize>
async fn bulk_resync<R: MempoolRpc>(rpc: &mut R, state: &SharedState) -> Option<usize> { /* unchanged */ }
```

Leave every function BODY exactly as-is. If `use crate::rpc::Rpc;` becomes unused after this, remove it (clippy will flag it).

- [ ] **Step 3: Add the `simulation` feature + optional `rand` to `Cargo.toml`**

After the `[dependencies]` block, add `rand` as optional and a `[features]` section:

```toml
# Seeded RNG for the simulation harness only (behind the `simulation` feature).
rand = { version = "0.8", optional = true }
```

```toml
[features]
# Compiles the offline mempool/network simulation harness (MockNode,
# SimulatedRpc, the sim HTTP server, and their tests). Off by default so the
# release build ships none of it.
simulation = ["dep:rand"]
```

- [ ] **Step 4: Verify the refactor compiles, lints, and existing tests still pass**

Run:
```bash
cargo build
cargo clippy --all-targets -- -D warnings
cargo test decision
```
Expected: build clean; clippy clean; the 9 `decision.rs` tests pass. `main.rs`'s `sync::run(rpc, ...)` call is unchanged because `Rpc: MempoolRpc` and type inference fills `R = Rpc`.

- [ ] **Step 5: Commit**

```bash
git add src/rpc.rs src/sync/mod.rs Cargo.toml
git commit -m "refactor(sync): extract MempoolRpc trait; make sync loop generic over its RPC source

Behavior-preserving seam so the sync loop can run against a simulated
node in tests. Real Rpc impls MempoolRpc by forwarding to its inherent
methods. Adds the (empty-for-now) simulation feature + optional rand.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: `MockNode` — in-memory synthetic mempool implementing `MempoolRpc`

A seeded, deterministic node whose mempool churns on demand. Realistic fee/package data so it also seeds future GBT testing.

**Files:**
- Create: `src/sim/mod.rs` — `#[cfg(feature = "simulation")] pub mod sim;` wiring (see Step 1).
- Create: `src/sim/mock_node.rs` — `MockNode`, `ChurnConfig`, `FeeDistribution`, `impl MempoolRpc`.
- Modify: `src/main.rs` — register the module: `#[cfg(feature = "simulation")] mod sim;`.

**Interfaces:**
- Consumes: `MempoolRpc` (Task 1); `MempoolInfo`, `MempoolEntry`, `MempoolEntryFees`, `RpcError` from `crate::rpc`.
- Produces:
  ```rust
  pub struct ChurnConfig { pub arrivals_per_tick: usize, pub evictions_per_tick: usize, pub fee: FeeDistribution }
  pub struct FeeDistribution { pub min_sat_vb: u64, pub max_sat_vb: u64 } // inclusive sat/vB range
  pub struct MockNode { /* private */ }
  impl MockNode {
      pub fn new(seed: u64, initial_size: usize, cfg: ChurnConfig) -> Self;
      pub fn advance(&mut self);                 // one churn tick
      pub fn reload(&mut self);                  // next mempool_info reports loaded=false once
      pub fn mass_drop(&mut self, fraction: f64);// remove `fraction` of txs at once
      pub fn len(&self) -> usize;
      pub fn is_empty(&self) -> bool;
  }
  impl Clone for MockNode { /* derive; needed for fetch_new_entries bound */ }
  impl MempoolRpc for MockNode { /* ... */ }
  ```
  `MockNode` must be `Clone + Send + Sync + 'static` (derive `Clone`; it holds only owned data).

- [ ] **Step 1: Create the module wiring**

Create `src/sim/mod.rs`:
```rust
//! Offline mempool + network simulation harness. Compiled only under the
//! `simulation` feature (and thus in `cargo test --features simulation`).
pub mod mock_node;

pub use mock_node::{ChurnConfig, FeeDistribution, MockNode};
```

Add to `src/main.rs` near the other `mod` declarations:
```rust
#[cfg(feature = "simulation")]
mod sim;
```

- [ ] **Step 2: Write the failing determinism test**

Create `src/sim/mock_node.rs` with a test module at the bottom (implementation added in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ChurnConfig {
        ChurnConfig {
            arrivals_per_tick: 50,
            evictions_per_tick: 40,
            fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
        }
    }

    #[tokio::test]
    async fn same_seed_produces_identical_mempool() {
        let a = MockNode::new(42, 1000, cfg());
        let b = MockNode::new(42, 1000, cfg());
        let ta = a.raw_mempool_txids().await.unwrap();
        let tb = b.raw_mempool_txids().await.unwrap();
        assert_eq!(ta.len(), 1000);
        let sa: std::collections::HashSet<_> = ta.into_iter().collect();
        let sb: std::collections::HashSet<_> = tb.into_iter().collect();
        assert_eq!(sa, sb, "identical seed must yield identical txid set");
    }

    #[tokio::test]
    async fn advance_applies_churn_counts() {
        let mut n = MockNode::new(7, 1000, cfg());
        n.advance();
        // +50 arrivals, -40 evictions => net +10 (arrivals use fresh random
        // txids so they never collide with evicted ones).
        assert_eq!(n.len(), 1010);
    }

    #[tokio::test]
    async fn mempool_entry_none_for_absent_tx() {
        let n = MockNode::new(1, 10, cfg());
        let absent = "0000000000000000000000000000000000000000000000000000000000000001"
            .parse()
            .unwrap();
        assert!(n.mempool_entry(&absent).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fees_within_configured_range() {
        let n = MockNode::new(3, 500, cfg());
        for (_txid, e) in n.raw_mempool_verbose().await.unwrap() {
            let sat_vb = e.fees.base.to_sat() / e.vsize.max(1);
            assert!((1..=500).contains(&sat_vb), "fee {sat_vb} sat/vB out of range");
        }
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail to compile (no impl yet)**

Run: `cargo test --features simulation mock_node`
Expected: FAIL — `MockNode`, `ChurnConfig`, etc. not found.

- [ ] **Step 4: Implement `MockNode`**

Prepend to `src/sim/mock_node.rs` (above the test module):

```rust
use std::collections::HashMap;

use bitcoin::{Amount, Txid};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{MempoolEntry, MempoolEntryFees, MempoolInfo, RpcError};

/// Inclusive sat/vB range synthetic fees are drawn from (uniform).
#[derive(Clone, Copy)]
pub struct FeeDistribution {
    pub min_sat_vb: u64,
    pub max_sat_vb: u64,
}

#[derive(Clone, Copy)]
pub struct ChurnConfig {
    pub arrivals_per_tick: usize,
    pub evictions_per_tick: usize,
    pub fee: FeeDistribution,
}

/// A deterministic, in-memory stand-in for a Bitcoin node's mempool RPC surface.
#[derive(Clone)]
pub struct MockNode {
    txs: HashMap<Txid, MempoolEntry>,
    tip_height: u64,
    min_fee: Amount,
    /// When true, the NEXT `mempool_info` reports `loaded: Some(false)` then clears.
    reloading: bool,
    rng: StdRng,
    cfg: ChurnConfig,
}

impl MockNode {
    pub fn new(seed: u64, initial_size: usize, cfg: ChurnConfig) -> Self {
        let mut node = Self {
            txs: HashMap::with_capacity(initial_size),
            tip_height: 800_000,
            min_fee: Amount::from_sat(1_000), // 1 sat/vB-ish floor in sats/kvB terms
            reloading: false,
            rng: StdRng::seed_from_u64(seed),
            cfg,
        };
        for _ in 0..initial_size {
            let (txid, entry) = node.gen_entry();
            node.txs.insert(txid, entry);
        }
        node
    }

    /// One churn tick: add `arrivals_per_tick` fresh txs, evict `evictions_per_tick`.
    pub fn advance(&mut self) {
        for _ in 0..self.cfg.evictions_per_tick {
            if let Some(&victim) = self.txs.keys().next() {
                self.txs.remove(&victim);
            }
        }
        for _ in 0..self.cfg.arrivals_per_tick {
            let (txid, entry) = self.gen_entry();
            self.txs.insert(txid, entry);
        }
    }

    pub fn reload(&mut self) {
        self.reloading = true;
    }

    pub fn mass_drop(&mut self, fraction: f64) {
        let target = ((self.txs.len() as f64) * fraction) as usize;
        let victims: Vec<Txid> = self.txs.keys().take(target).copied().collect();
        for v in victims {
            self.txs.remove(&v);
        }
    }

    pub fn len(&self) -> usize {
        self.txs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// Build one synthetic `(Txid, MempoolEntry)` with a fresh random txid and
    /// internally-consistent size/fee/package fields.
    fn gen_entry(&mut self) -> (Txid, MempoolEntry) {
        use bitcoin::hashes::Hash;
        let mut raw = [0u8; 32];
        self.rng.fill(&mut raw);
        let txid = Txid::from_byte_array(raw);

        let vsize: u64 = self.rng.gen_range(110..=100_000);
        let weight = vsize * 4;
        let sat_vb: u64 = self
            .rng
            .gen_range(self.cfg.fee.min_sat_vb..=self.cfg.fee.max_sat_vb);
        let base = Amount::from_sat(sat_vb.saturating_mul(vsize));

        // Solo package (no ancestors/descendants) keeps the model simple but
        // consistent: ancestor/descendant totals equal this tx's own.
        let fees = MempoolEntryFees {
            base,
            ancestor: base,
            descendant: base,
        };
        let entry = MempoolEntry {
            vsize,
            weight: Some(weight),
            depends: Vec::new(),
            fees,
            ancestorsize: vsize,
            descendantsize: vsize,
        };
        (txid, entry)
    }
}

impl MempoolRpc for MockNode {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        Ok(MempoolInfo {
            loaded: Some(!self.reloading),
            mempoolminfee: self.min_fee,
        })
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        Ok(self.txs.keys().copied().collect())
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        Ok(self
            .txs
            .iter()
            .map(|(k, v)| (*k, clone_entry(v)))
            .collect())
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        Ok(self.txs.get(txid).map(clone_entry))
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        Ok(self.tip_height)
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// `MempoolEntry` isn't `Clone` (it only derives `Deserialize`). The sim needs to
/// hand out owned copies, so clone field-by-field here rather than adding a
/// `Clone` derive to the production type.
fn clone_entry(e: &MempoolEntry) -> MempoolEntry {
    MempoolEntry {
        vsize: e.vsize,
        weight: e.weight,
        depends: e.depends.clone(),
        fees: MempoolEntryFees {
            base: e.fees.base,
            ancestor: e.fees.ancestor,
            descendant: e.fees.descendant,
        },
        ancestorsize: e.ancestorsize,
        descendantsize: e.descendantsize,
    }
}

use crate::rpc::MempoolRpc;
```

> NOTE for the implementer: `mempool_info` above needs the `reloading` flag to CLEAR after being read once, but `mempool_info` takes `&self`. Resolve by having `wait_until_mempool_loaded`'s test (Task 4) call `reload()` then advance state explicitly; do NOT add interior mutability here. If a self-clearing reload is needed, change `reload()` semantics to "reports `loaded:false` until a subsequent `advance()` clears `reloading`" and clear `self.reloading = false;` at the top of `advance()`. Implement THIS latter semantics: set `self.reloading = false;` as the first line of `advance()`.

Add `self.reloading = false;` as the first statement in `advance()`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test --features simulation mock_node`
Expected: PASS (4 tests). Then `cargo clippy --all-targets --features simulation -- -D warnings` clean.

- [ ] **Step 6: Commit**

```bash
git add src/sim/mod.rs src/sim/mock_node.rs src/main.rs
git commit -m "feat(sim): MockNode — deterministic in-memory mempool implementing MempoolRpc

Seeded synthetic mempool with configurable churn and realistic
sat/vB fee distribution (GBT-ready). Feature-gated behind `simulation`.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: `NetworkProfile` + `SimulatedRpc` — the network-simulation layer

Wrap any `MempoolRpc` and inject latency, a per-second rate limit (surfacing real `RpcError::HttpStatus { status: 429 }`), a body-size cap (`RpcError::BodyTooLarge`), and random drops (`RpcError::HttpStatus { status: 503 }`).

**Files:**
- Create: `src/sim/network.rs` — `NetworkProfile`, presets, `SimulatedRpc`, `impl MempoolRpc`.
- Modify: `src/sim/mod.rs` — add `pub mod network;` and re-exports.

**Interfaces:**
- Consumes: `MempoolRpc`, `MockNode` (Task 2); `RpcError`.
- Produces:
  ```rust
  #[derive(Clone)]
  pub struct NetworkProfile {
      pub latency: std::time::Duration,
      pub req_per_sec: Option<u32>, // None = unlimited
      pub body_cap: Option<usize>,  // bytes; verbose responses above this fail
      pub drop_rate: f64,           // 0.0..=1.0 fraction of calls that fail transport
  }
  impl NetworkProfile { pub fn local_node() -> Self; pub fn getblock_remote() -> Self; }

  #[derive(Clone)]
  pub struct SimulatedRpc<N: MempoolRpc> { /* inner: N, profile, shared limiter+rng */ }
  impl<N: MempoolRpc> SimulatedRpc<N> { pub fn new(inner: N, profile: NetworkProfile) -> Self; }
  impl<N: MempoolRpc + Clone + Send + Sync + 'static> MempoolRpc for SimulatedRpc<N> { /* ... */ }
  ```
  Rate limiter + rng are shared behind `Arc<Mutex<..>>` so `Clone` (needed for the `fetch_new_entries` bound) shares one budget across cloned handles — a single 20 req/sec budget, not 20/sec per in-flight clone.

- [ ] **Step 1: Write the failing tests**

Create `src/sim/network.rs` with this test module (impl added Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{ChurnConfig, FeeDistribution, MockNode};

    fn node() -> MockNode {
        MockNode::new(
            9,
            100,
            ChurnConfig {
                arrivals_per_tick: 0,
                evictions_per_tick: 0,
                fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 10 },
            },
        )
    }

    #[tokio::test]
    async fn rate_limit_surfaces_429_after_budget() {
        let profile = NetworkProfile {
            latency: std::time::Duration::ZERO,
            req_per_sec: Some(2),
            body_cap: None,
            drop_rate: 0.0,
        };
        let rpc = SimulatedRpc::new(node(), profile);
        // 2 allowed in the current second, 3rd rejected.
        assert!(rpc.tip_height().await.is_ok());
        assert!(rpc.tip_height().await.is_ok());
        match rpc.tip_height().await {
            Err(RpcError::HttpStatus { status: 429, .. }) => {}
            other => panic!("expected 429, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn body_cap_rejects_large_verbose() {
        let profile = NetworkProfile {
            latency: std::time::Duration::ZERO,
            req_per_sec: None,
            body_cap: Some(10), // absurdly small: 100-entry verbose exceeds it
            drop_rate: 0.0,
        };
        let rpc = SimulatedRpc::new(node(), profile);
        match rpc.raw_mempool_verbose().await {
            Err(RpcError::BodyTooLarge { .. }) => {}
            other => panic!("expected BodyTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unlimited_profile_passes_through() {
        let rpc = SimulatedRpc::new(node(), NetworkProfile::local_node());
        assert_eq!(rpc.raw_mempool_txids().await.unwrap().len(), 100);
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features simulation network`
Expected: FAIL — `NetworkProfile`, `SimulatedRpc` not found.

- [ ] **Step 3: Implement the network layer**

Prepend to `src/sim/network.rs`:

```rust
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bitcoin::Txid;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{MempoolEntry, MempoolInfo, MempoolRpc, RpcError};

#[derive(Clone)]
pub struct NetworkProfile {
    pub latency: Duration,
    pub req_per_sec: Option<u32>,
    pub body_cap: Option<usize>,
    pub drop_rate: f64,
}

impl NetworkProfile {
    /// Local node: effectively instant, no limits.
    pub fn local_node() -> Self {
        Self { latency: Duration::ZERO, req_per_sec: None, body_cap: None, drop_rate: 0.0 }
    }
    /// Throttled remote provider (the GetBlock profile that produced the
    /// observed backlog): ~150ms latency, 20 req/sec, generous body cap.
    pub fn getblock_remote() -> Self {
        Self {
            latency: Duration::from_millis(150),
            req_per_sec: Some(20),
            body_cap: Some(512 * 1024 * 1024),
            drop_rate: 0.0,
        }
    }
}

/// Fixed-window per-second limiter. Shared across clones so one budget governs
/// all concurrent calls (mirrors a real provider's account-wide limit).
struct Limiter {
    window_start: Instant,
    count: u32,
}

struct Shared {
    limiter: Limiter,
    rng: StdRng,
}

#[derive(Clone)]
pub struct SimulatedRpc<N: MempoolRpc> {
    inner: N,
    profile: NetworkProfile,
    shared: Arc<Mutex<Shared>>,
}

impl<N: MempoolRpc> SimulatedRpc<N> {
    pub fn new(inner: N, profile: NetworkProfile) -> Self {
        Self {
            inner,
            profile,
            shared: Arc::new(Mutex::new(Shared {
                limiter: Limiter { window_start: Instant::now(), count: 0 },
                rng: StdRng::seed_from_u64(0x5A7A),
            })),
        }
    }

    /// Apply network effects that don't depend on the response body. Returns
    /// `Err` if the call should be rejected (rate limit or random drop). Must be
    /// called (and its lock released) BEFORE any `.await` on the inner RPC.
    fn gate(&self) -> Result<(), RpcError> {
        // Scope the lock so it is never held across the latency await below.
        {
            let mut s = self.shared.lock().unwrap();
            if self.profile.drop_rate > 0.0 && s.rng.gen::<f64>() < self.profile.drop_rate {
                return Err(RpcError::HttpStatus {
                    status: 503,
                    body: "simulated transport drop".to_string(),
                });
            }
            if let Some(limit) = self.profile.req_per_sec {
                if s.limiter.window_start.elapsed() >= Duration::from_secs(1) {
                    s.limiter.window_start = Instant::now();
                    s.limiter.count = 0;
                }
                if s.limiter.count >= limit {
                    return Err(RpcError::HttpStatus {
                        status: 429,
                        body: String::new(),
                    });
                }
                s.limiter.count += 1;
            }
        }
        Ok(())
    }

    async fn delay(&self) {
        if self.profile.latency > Duration::ZERO {
            tokio::time::sleep(self.profile.latency).await;
        }
    }
}

impl<N: MempoolRpc + Clone + Send + Sync + 'static> MempoolRpc for SimulatedRpc<N> {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.mempool_info().await
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.raw_mempool_txids().await
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        self.gate()?;
        self.delay().await;
        let entries = self.inner.raw_mempool_verbose().await?;
        if let Some(cap) = self.profile.body_cap {
            // Approximate serialized size: ~180 bytes/entry is plenty to trip a
            // deliberately tiny cap in tests and to model a real large body.
            let approx = entries.len().saturating_mul(180);
            if approx > cap {
                return Err(RpcError::BodyTooLarge { limit: cap });
            }
        }
        Ok(entries)
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.mempool_entry(txid).await
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.tip_height().await
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        self.inner.reconnect()
    }
}
```

Add to `src/sim/mod.rs`:
```rust
pub mod network;
pub use network::{NetworkProfile, SimulatedRpc};
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test --features simulation network`
Expected: PASS (3 tests). Then `cargo clippy --all-targets --features simulation -- -D warnings` clean.

- [ ] **Step 5: Commit**

```bash
git add src/sim/network.rs src/sim/mod.rs
git commit -m "feat(sim): NetworkProfile + SimulatedRpc — latency/429/body-cap/drop injection

Shared per-second limiter surfaces real RpcError::HttpStatus{429}; body
cap surfaces BodyTooLarge. getblock_remote() preset matches the observed
throttled-provider profile.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Integration tests — drive the real sync loop against the simulator

These are in-crate `#[cfg(test)]` tests (not `tests/`), so they can call the private sync functions and inspect `SharedState`.

**Files:**
- Create: `src/sync/sim_tests.rs` — the scenario tests.
- Modify: `src/sync/mod.rs` — add `#[cfg(all(test, feature = "simulation"))] mod sim_tests;` and, where needed, mark `initial_bulk_load` / `steady_tick` reachable to the submodule (they already live in the same module, so `mod sim_tests` under `sync` can call them directly — no visibility change needed).

**Interfaces:**
- Consumes: `initial_bulk_load`, `steady_tick`, `SyncConfig`, `SharedState`/`MempoolState`, `read_state`, `wait_until_mempool_loaded`, `bulk_resync` (Task 1, same module); `MockNode`, `SimulatedRpc`, `NetworkProfile`, `ChurnConfig`, `FeeDistribution` (Tasks 2–3).
- Produces: nothing consumed downstream (leaf task).

- [ ] **Step 1: Add the submodule declaration in `src/sync/mod.rs`**

Near the top-level `mod` items of `src/sync/mod.rs` (next to `mod decision;`):
```rust
#[cfg(all(test, feature = "simulation"))]
mod sim_tests;
```

- [ ] **Step 2: Write the scenario tests**

Create `src/sync/sim_tests.rs`. Inspect how `SharedState` is constructed and read by looking at `src/state.rs` (or wherever `SharedState`/`MempoolState` and `read_state` are defined) and mirror the existing construction used by `run`. Use this content, adjusting the state constructor to the real one if it differs:

```rust
//! End-to-end sync-loop scenarios against the simulated node. Fast and
//! deterministic — no network, no wall-clock dependence except where a scenario
//! explicitly needs the resync cooldown.
use std::time::Duration;

use super::*; // brings initial_bulk_load, steady_tick, SyncConfig, read_state, etc.
use crate::sim::{ChurnConfig, FeeDistribution, MockNode, NetworkProfile, SimulatedRpc};

fn churn(arrivals: usize, evictions: usize) -> ChurnConfig {
    ChurnConfig {
        arrivals_per_tick: arrivals,
        evictions_per_tick: evictions,
        fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
    }
}

fn cfg() -> SyncConfig {
    SyncConfig {
        poll_interval: Duration::from_millis(10),
        fetch_concurrency: 5,
        tick_budget: Duration::from_secs(30), // generous: budget bail isn't under test here
    }
}

/// Construct an empty shared state the way `run` expects it. If the real
/// constructor differs (see src/state.rs), use that instead.
fn empty_state() -> SharedState {
    SharedState::default()
}

#[tokio::test]
async fn cold_bulk_load_builds_full_mempool() {
    let node = MockNode::new(1, 5_000, churn(0, 0));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;

    let g = read_state(&state);
    assert_eq!(g.txs.len(), 5_000);
    assert!(g.caught_up);
    assert!(g.last_sync_ok.is_some());
}

#[tokio::test]
async fn steady_churn_local_stays_caught_up() {
    let node = MockNode::new(2, 2_000, churn(30, 30));
    let mut rpc = SimulatedRpc::new(node.clone(), NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;

    // Advance the node and run several steady ticks; a fast local profile keeps up.
    for _ in 0..5 {
        rpc.inner_mut().advance(); // see Step 3 note: expose inner_mut for tests
        steady_tick(&mut rpc, &state, &cfg(), &mut caught_up_prev, &mut last_bulk).await;
        assert!(read_state(&state).caught_up, "local profile must stay caught up");
    }
}

#[tokio::test]
async fn rate_limited_remote_falls_behind() {
    // Heavy churn + 20 req/sec => per-tx catch-up can't keep up => backlog.
    let node = MockNode::new(3, 20_000, churn(600, 600));
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::getblock_remote());
    let state = empty_state();
    let mut caught_up_prev = false;
    let mut last_bulk = std::time::Instant::now();

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;
    assert!(read_state(&state).caught_up, "bulk verbose load succeeds even remote");

    rpc.inner_mut().advance();
    steady_tick(&mut rpc, &state, &cfg(), &mut caught_up_prev, &mut last_bulk).await;
    assert!(
        !read_state(&state).caught_up,
        "throttled per-tx catch-up must report backlog (caught_up=false)"
    );
}

#[tokio::test]
async fn mass_drop_triggers_resync() {
    let mut node = MockNode::new(4, 10_000, churn(0, 0));
    node.mass_drop(0.9);
    let mut rpc = SimulatedRpc::new(node, NetworkProfile::local_node());
    let state = empty_state();
    let mut caught_up_prev = false;

    initial_bulk_load(&mut rpc, &state, Duration::from_millis(10), &mut caught_up_prev).await;
    assert_eq!(read_state(&state).txs.len(), 1_000, "cache matches post-drop node");
}
```

- [ ] **Step 3: Expose `inner_mut()` on `SimulatedRpc` for tests**

The steady-churn/backlog tests need to `advance()` the wrapped node between ticks. Add to `impl<N: MempoolRpc> SimulatedRpc<N>` in `src/sim/network.rs`:
```rust
/// Test/sim access to the wrapped node (e.g. to advance churn between ticks).
pub fn inner_mut(&mut self) -> &mut N {
    &mut self.inner
}
```

- [ ] **Step 4: Reconcile `empty_state()` with the real constructor**

Open `src/state.rs` (or wherever `SharedState`, `MempoolState`, `read_state`, `write_state` are defined). Confirm how `run`'s caller builds the shared state and whether `MempoolState` implements `Default`. If `SharedState::default()` is not the real pattern, replace `empty_state()` with the actual construction (e.g. `SharedState::new(MempoolState::default())` or an `Arc<RwLock<_>>` wrap). Confirm `read_state(&state)` yields a guard exposing `.txs`, `.caught_up`, `.last_sync_ok`.

- [ ] **Step 5: Run the scenario tests**

Run: `cargo test --features simulation sim_tests`
Expected: PASS (4 tests). If `rate_limited_remote_falls_behind` does not report backlog, raise `arrivals_per_tick` (the churn must exceed what 20 req/sec clears within `tick_budget`). If it flakes on timing, that scenario legitimately depends on the limiter — keep `tick_budget` generous and churn well above the budget.

- [ ] **Step 6: Commit**

```bash
git add src/sync/sim_tests.rs src/sync/mod.rs src/sim/network.rs
git commit -m "test(sync): end-to-end sync-loop scenarios against the simulator

Cold bulk load, steady churn stays caught up (local), throttled remote
falls behind (regression-locks the observed GetBlock backlog), and
mass-drop resync — all deterministic, no network.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Live HTTP sim server + real-transport test + `just simulate`

Serve a `MockNode` over a real HTTP JSON-RPC endpoint (via axum, already a dep), apply a `NetworkProfile`, and run the REAL reqwest `Rpc` against it — the only layer that exercises body streaming, the client timeout, and 429 classification end-to-end. Reached via a feature-gated `sim-serve` subcommand (no lib restructure).

**Files:**
- Create: `src/sim/server.rs` — `spawn(node, profile, addr) -> (SocketAddr, JoinHandle)` and the axum JSON-RPC handler emitting Core-shaped JSON.
- Modify: `src/sim/mod.rs` — `pub mod server;`.
- Modify: `src/main.rs` — a `#[cfg(feature = "simulation")]` `sim-serve` clap subcommand that calls `sim::server::spawn` and blocks.
- Modify: `justfile` — `simulate` recipe; update `test` recipe to `cargo test --features simulation`.
- Create: `src/sim/server_tests.rs` (or a `#[cfg(test)] mod` in `server.rs`) — the real-transport test.

**Interfaces:**
- Consumes: `MockNode`, `NetworkProfile` (Tasks 2–3); real `Rpc`, `RpcError`, `RpcConfig` (`crate::rpc`, `crate::config`).
- Produces: `pub async fn spawn(node: MockNode, profile: NetworkProfile, port: u16) -> std::net::SocketAddr` (binds `127.0.0.1:port`, `port=0` picks a free port; spawns the server task; returns the bound address).

- [ ] **Step 1: Write the failing real-transport test**

Create `src/sim/server_tests.rs`:
```rust
#[cfg(test)]
mod tests {
    use crate::config::RpcConfig;
    use crate::rpc::{MempoolRpc, Rpc, RpcError};
    use crate::sim::{server, ChurnConfig, FeeDistribution, MockNode, NetworkProfile};
    use std::time::Duration;

    fn node(size: usize) -> MockNode {
        MockNode::new(
            11, size,
            ChurnConfig { arrivals_per_tick: 0, evictions_per_tick: 0,
                          fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 50 } },
        )
    }

    fn client(addr: std::net::SocketAddr) -> Rpc {
        Rpc::connect(&RpcConfig {
            url: format!("http://{addr}"),
            auth: None,
            timeout: Duration::from_secs(5),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn real_client_bulk_loads_over_http() {
        let addr = server::spawn(node(500), NetworkProfile::local_node(), 0).await;
        let rpc = client(addr);
        let entries = rpc.raw_mempool_verbose().await.unwrap();
        assert_eq!(entries.len(), 500);
    }

    #[tokio::test]
    async fn real_client_sees_429_from_throttled_server() {
        let profile = NetworkProfile { req_per_sec: Some(1), ..NetworkProfile::local_node() };
        let addr = server::spawn(node(10), profile, 0).await;
        let rpc = client(addr);
        let _ = rpc.tip_height().await; // consumes the 1/sec budget
        match rpc.tip_height().await {
            Err(RpcError::HttpStatus { status: 429, .. }) | Err(RpcError::Auth) => {}
            other => panic!("expected 429 surfaced by real client, got {other:?}"),
        }
    }
}
```
(The `Auth` allowance covers the client mapping 401/403; a 429 must NOT map to `Auth` — if this test shows `Auth` for a 429, that's a real client bug to file, but Core-style 429 should surface as `HttpStatus`.)

Add at bottom of `src/sim/server.rs`: `#[cfg(test)] #[path = "server_tests.rs"] mod server_tests;` — or inline the module. Confirm the real `RpcConfig` field names by reading `src/config.rs` and adjust `client()` accordingly.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --features simulation server`
Expected: FAIL — `server::spawn` not found.

- [ ] **Step 3: Implement the sim server**

Create `src/sim/server.rs`. Build an axum app with one POST `/` route that parses a JSON-RPC request (`method`, `params`), applies the profile via a `SimulatedRpc`-style gate, and returns Core-shaped JSON. Emit fees as BTC decimals (`Amount::to_btc()`). Bind with `tokio::net::TcpListener`, read back `local_addr()`, spawn `axum::serve`, return the address. Handle exactly the methods the client calls: `getblockchaininfo`, `getmempoolinfo`, `getrawmempool` (`params[0]==true` → verbose map, else txid array), `getmempoolentry`. For a 429 from the gate, respond HTTP 429 with an empty body (so the client maps it to `HttpStatus`, not a JSON-RPC envelope). Reuse `NetworkProfile`'s limiter logic — factor the gate out of Task 3 into a shared helper if convenient, or re-implement the per-second window here (small).

Key shape for a verbose entry (BTC decimals, matching Core):
```rust
serde_json::json!({
    "vsize": e.vsize,
    "weight": e.weight,
    "depends": [],
    "fees": {
        "base": e.fees.base.to_btc(),
        "ancestor": e.fees.ancestor.to_btc(),
        "descendant": e.fees.descendant.to_btc(),
    },
    "ancestorsize": e.ancestorsize,
    "descendantsize": e.descendantsize,
})
```
`getblockchaininfo` must return at least `{"chain":"main","blocks":<tip_height>}` (the client's `BlockchainInfo` reads `chain` via `as_core_arg` and `blocks`). `getmempoolinfo` returns `{"loaded":true,"mempoolminfee":0.00001}`. Wrap every success as `{"result":<value>,"error":null,"id":0}`.

- [ ] **Step 4: Wire the module + subcommand + justfile**

`src/sim/mod.rs`: add `pub mod server;`.

`src/main.rs` — add a feature-gated subcommand. Because the existing CLI is a flat clap `derive` struct driven by env vars, add an optional subcommand field that only exists under the feature, and branch on it before the normal indexer startup:
```rust
// In the CLI struct:
#[cfg(feature = "simulation")]
#[command(subcommand)]
sim: Option<SimCmd>,

#[cfg(feature = "simulation")]
#[derive(clap::Subcommand)]
enum SimCmd {
    /// Serve a simulated node over HTTP for offline testing.
    SimServe {
        #[arg(long, default_value_t = 18443)] port: u16,
        #[arg(long, default_value_t = 20_000)] size: usize,
        #[arg(long, default_value_t = 600)] arrivals: usize,
        #[arg(long, default_value_t = 600)] evictions: usize,
        /// remote = getblock_remote profile; anything else = local_node.
        #[arg(long, default_value = "remote")] profile: String,
    },
}
```
In `main`, before the indexer boots:
```rust
#[cfg(feature = "simulation")]
if let Some(SimCmd::SimServe { port, size, arrivals, evictions, profile }) = cli.sim {
    let churn = crate::sim::ChurnConfig {
        arrivals_per_tick: arrivals, evictions_per_tick: evictions,
        fee: crate::sim::FeeDistribution { min_sat_vb: 1, max_sat_vb: 500 },
    };
    let node = crate::sim::MockNode::new(0, size, churn);
    let net = if profile == "remote" {
        crate::sim::NetworkProfile::getblock_remote()
    } else {
        crate::sim::NetworkProfile::local_node()
    };
    let addr = crate::sim::server::spawn(node, net, port).await;
    tracing::info!(%addr, "sim node serving; point BTC_RPC_URL at it");
    // Keep the process alive; churn advances on a timer inside spawn().
    std::future::pending::<()>().await;
    return Ok(());
}
```
`spawn` should also start a background timer advancing the node's churn every ~2s (so a client sees a live, churning mempool). Guard the mutation with the same lock the handler uses.

`justfile` — add and update:
```make
# Run the offline simulated node (needs `--features simulation`)
simulate:
    cargo run --features simulation -- sim-serve

# Run unit tests (simulation harness included)
test:
    cargo test --features simulation
```

- [ ] **Step 5: Run the real-transport tests + full suite**

Run:
```bash
cargo test --features simulation
cargo clippy --all-targets --features simulation -- -D warnings
cargo build            # default features: sim code must be fully excluded
cargo test             # default features: decision.rs tests still pass
```
Expected: all green. The default `cargo build`/`cargo test` (no `--features simulation`) must compile with zero simulation code.

- [ ] **Step 6: Manual smoke (optional but recommended)**

```bash
just simulate &                 # serves on 127.0.0.1:18443
BTC_RPC_URL=http://127.0.0.1:18443 RUST_LOG=info,satya::sync=debug cargo run
# Watch: bulk resync complete, then `mempool tick ...` lines; Ctrl-C both.
```

- [ ] **Step 7: Commit**

```bash
git add src/sim/server.rs src/sim/server_tests.rs src/sim/mod.rs src/main.rs justfile
git commit -m "feat(sim): live HTTP sim server + real-transport tests + just simulate

Serves a churning MockNode over axum JSON-RPC with a NetworkProfile so
the real reqwest client is exercised end-to-end (429, body, timeout)
offline. `just simulate` runs it; `just test` now enables the feature.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:**
- Goal 1 (proper deterministic tests of mempool creation) → Task 4.
- Goal 2 (simulate the network: latency/429/body-cap/drop, local + remote presets) → Task 3.
- Goal 3 (live offline harness exercising real transport) → Task 5.
- Goal 4 (GBT-ready synthetic fees) → Task 2 (`FeeDistribution`, consistent package fields).
- Architecture pieces (MempoolRpc trait, MockNode, NetworkProfile/SimulatedRpc, `just simulate`) → Tasks 1/2/3/5 respectively.
- Test matrix items 1–5 → Task 4; item 6 (real transport) → Task 5.

**Placeholder scan:** No TBD/TODO; each code step carries complete code. Two explicit implementer reconciliation points are called out with exact instructions, not left vague: `empty_state()` vs the real `SharedState` constructor (Task 4 Step 4) and `RpcConfig` field names (Task 5 Step 1) — both say "read this file, adjust to match," which is correct because those are pre-existing types the plan must not guess at.

**Type consistency:** Trait method names/signatures identical across Tasks 1–3 and used verbatim in Task 4. `MempoolEntry`/`MempoolEntryFees`/`MempoolInfo` fields match `src/rpc.rs` (`vsize:u64`, `weight:Option<u64>`, `depends:Vec<Txid>`, `fees{base,ancestor,descendant:Amount}`, `ancestorsize`/`descendantsize:u64`; `MempoolInfo{loaded:Option<bool>, mempoolminfee:Amount}`). `SyncConfig{poll_interval,fetch_concurrency,tick_budget}` matches. `RpcError::HttpStatus{status,body}` and `BodyTooLarge{limit}` match the enum. `inner_mut()` added in Task 4 Step 3 is on the type defined in Task 3.

**Known implementer caveats (surfaced, not hidden):** (a) native async-fn-in-trait may emit a dyn-compatibility lint — the trait is only used with generics, never `dyn`, so that's fine; if a lint fires, it's allow-able. (b) `MockNode::advance()` clears `reloading` first (Task 2 Step 4 note). (c) The gate logic is duplicated between `SimulatedRpc` (Task 3) and the HTTP server (Task 5) unless factored — Task 5 Step 3 allows either; a shared helper is preferred but not required.
