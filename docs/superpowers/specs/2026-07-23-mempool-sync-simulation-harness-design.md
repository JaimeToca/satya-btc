# Mempool Sync Test + Network Simulation Harness — Design

**Status:** Approved (2026-07-23)

## Why

Satya's mempool sync loop has only pure-logic unit tests (`sync/decision.rs`).
The loop that actually talks to a node — bulk load, per-tick diff/fetch, desync
recovery — is untested, and the transport (`rpc.rs`: 429 handling, body-size
caps, timeouts) is untested. We also have no reliable Bitcoin RPC provider: a
live test against GetBlock showed the per-tx `getmempoolentry` catch-up model
collapses under a remote rate limit (5,349 × HTTP 429; `caught_up` stuck false),
while the single `getrawmempool verbose` bulk load worked perfectly. We need to
reproduce and regression-lock that behavior **offline and deterministically**,
and simulate an unreliable network without any provider.

## Goals

1. **Proper tests of mempool creation** — deterministic, fast, assert internal
   cache state through the real sync loop.
2. **Simulate the network** — latency, rate limiting (429), body-size caps, and
   drops, with presets matching a local node and a throttled remote provider.
3. **A live offline harness** — run the real binary against a simulated node so
   the real reqwest transport is exercised (no provider required).
4. **GBT-ready** — synthetic transactions carry realistic fee/ancestor-package
   data so this harness also seeds testing of the future GBT fee estimator.

Non-goals: modeling Bitcoin consensus, real transaction validity, P2P relay, or
block templates. This simulates the *RPC surface* the sync loop consumes.

## Architecture

Four pieces, layered:

```
sync::run<R: MempoolRpc>(rpc: R, ...)        <- production loop, now generic
        |
   +----+-----------------------------+
   |                                   |
  Rpc (reqwest)  -> prod        SimulatedRpc<MockNode>  -> tests + `just simulate`
                                        |
                                  NetworkProfile { latency, req_per_sec(429),
                                                   body_cap, drop_rate }
                                        |
                                  MockNode (in-memory synthetic mempool)
```

### 1. `MempoolRpc` trait (the seam)

Extract the seven methods the sync loop calls on `Rpc` into a trait. Native
`async fn` in traits (stable since 1.75) — no `async_trait` macro.

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

The real `Rpc` gets a `impl MempoolRpc for Rpc` that forwards to its existing
inherent methods (keep the inherent methods; the trait forwards, so nothing else
in the codebase changes call style). The sync functions in `sync/mod.rs`
(`wait_until_mempool_loaded`, `initial_bulk_load`, `steady_tick`, `bulk_resync`,
`fetch_new_entries`, `reconnect_on_error`, `run`) change their `&mut Rpc` / `&Rpc`
parameters to a generic `<R: MempoolRpc>` bound. Behavior is unchanged for
production — this is a mechanical seam extraction.

**Concurrency note:** `fetch_new_entries` clones the rpc (`rpc.clone()`) and uses
`buffer_unordered` for concurrent `mempool_entry` calls. The trait bound for that
path therefore also needs `R: Clone + Send + Sync + 'static` (matching what `Rpc`
already provides). Capture the exact existing bounds when extracting.

### 2. `MockNode` — the simulated node

In-memory, deterministic, seeded. Lives under `#[cfg(test)]` plus a small
non-test module gated behind a `simulation` feature (so `just simulate` can build
it into a dev binary without shipping it in release).

```rust
pub struct MockNode {
    txs: HashMap<Txid, MempoolEntry>,   // the synthetic mempool
    tip_height: u64,
    loaded: bool,                       // getmempoolinfo.loaded
    min_fee: Amount,
    rng: StdRng,                        // seeded -> deterministic
    cfg: ChurnConfig,
}

pub struct ChurnConfig {
    pub arrivals_per_tick: usize,       // new txs added on each advance()
    pub evictions_per_tick: usize,      // txs removed on each advance()
    pub fee_dist: FeeDistribution,      // realistic sat/vB spread (GBT-ready)
}
```

- `MockNode::new(seed, initial_size, cfg)` — builds an initial mempool of
  `initial_size` synthetic entries.
- `advance()` — one simulated wall-tick: insert `arrivals_per_tick` new txs
  (fresh random Txids, realistic `vsize` 110–100_000, `weight ≈ vsize*4`, base
  fee drawn from `fee_dist`, mostly empty `depends`, ancestor/descendant fields
  consistent with a small package), evict `evictions_per_tick` existing txs.
  Deterministic given the seed.
- `reload()` — flips `loaded=false` then `true` on next poll (simulate node
  restart / mempool reload) for the desync-recovery test.
- `mass_drop(fraction)` — remove a large fraction at once (simulate a mined
  block clearing the mempool) for the mass-drop desync test.

`MockNode` implements `MempoolRpc` directly (no network): `raw_mempool_verbose`
returns all entries, `mempool_entry` looks one up (`None` if absent → exercises
the `-5 → Ok(None)` path), etc. Tests can call `advance()` between sync ticks and
assert the cache exactly.

**Realistic fee data (GBT-ready):** `FeeDistribution` produces a plausible
sat/vB spread (e.g. a log-normal-ish mix skewed toward 1–20 sat/vB with a long
tail) and consistent ancestor/descendant fee+size, so a future GBT estimator can
be tested against a known synthetic distribution. This is the one piece we invest
in now to avoid rebuilding the generator later.

### 3. `NetworkProfile` + `SimulatedRpc` (the network layer)

`SimulatedRpc<N: MempoolRpc>` wraps any `MempoolRpc` (a `MockNode`) and applies a
`NetworkProfile` before delegating:

```rust
pub struct NetworkProfile {
    pub latency: Duration,          // added per call
    pub req_per_sec: Option<u32>,   // None = unlimited; Some(n) -> 429 beyond n/sec
    pub body_cap: Option<usize>,    // simulate provider truncating/streaming caps
    pub drop_rate: f64,             // fraction of calls that fail as transport error
}
```

Presets:
- `NetworkProfile::local_node()` — `latency ≈ 0`, `req_per_sec: None`,
  no caps, `drop_rate: 0.0`.
- `NetworkProfile::getblock_remote()` — `latency ≈ 150ms`, `req_per_sec:
  Some(20)`, a large `body_cap`, small `drop_rate` — the profile that produced
  the observed backlog.

Rate limiting maps to `RpcError::HttpStatus { status: 429, .. }` (the same error
the real client surfaces), so the sync loop's reaction is tested against the real
error type. `SimulatedRpc` implements `MempoolRpc`.

### 4. `just simulate` — live HTTP harness

A small binary (`src/bin/simulate.rs`, behind the `simulation` feature) that
serves a `MockNode` over a real hyper JSON-RPC endpoint with a chosen
`NetworkProfile`, advancing churn on a timer. Run the **real** `satya` binary
against `BTC_RPC_URL=http://127.0.0.1:<port>` to exercise the real reqwest
transport (429 classification, body streaming, timeout) fully offline. A
`just simulate` recipe wires it up.

## Test Matrix

Integration tests in `tests/sync_simulation.rs` drive the **real** `sync` loop
against `MockNode` / `SimulatedRpc`:

1. **Cold bulk load** — `initial_bulk_load` over a 24k-entry MockNode →
   `caught_up:true`, cache size == node size, `last_sync_ok` set.
2. **Steady churn, local profile** — modest churn on `local_node` → stays
   `caught_up:true` tick after tick; cache tracks node (new added, gone removed).
3. **Rate-limited remote → backlog** — `getblock_remote` profile with churn
   exceeding the 20/sec budget → `caught_up:false`, `new` backlog persists.
   Regression-locks the observed GetBlock behavior.
4. **Node reload recovery** — `reload()` (`loaded:false`) → loop waits, then
   bulk-resyncs and returns to `caught_up:true`.
5. **Mass drop recovery** — `mass_drop(0.9)` → triggers bulk resync (cooldown
   honored), cache shrinks to match.
6. **Transport (real HTTP via `just simulate` binary, in a test)** — 429 is
   classified reconnectable; an oversized body hits `BodyTooLarge`; a slow body
   trips the client timeout. No panics.

Fast tests (1–5) use `MockNode`/`SimulatedRpc` directly — no HTTP, deterministic,
sub-second. Test 6 uses the real transport against the sim server.

## Dependencies

Dev/sim additions (dev-dependencies, or gated behind the `simulation` feature for
the binary): `rand` (seeded `StdRng`), `hyper`/`axum` for the sim server (axum is
already a runtime dep — reuse it), `tokio` test macros (already present). No new
production runtime dependencies.

## Risks / Mitigations

- **Generic-izing sync touches production signatures.** Mitigation: pure
  mechanical seam extraction, behavior-preserving; the existing 9 decision-logic
  tests plus the new bulk-load test guard it. Keep `Rpc`'s inherent methods so
  `main.rs`/others are untouched except the type parameter threading.
- **Timing-based tests (rate limit, timeout) can be flaky.** Mitigation: keep
  fast tests logical/deterministic (budget expressed as call counts, not
  wall-clock where avoidable); confine wall-clock behavior to test 6 with
  generous margins.
- **Feature-gating drift.** Mitigation: CI builds `--features simulation` and the
  default build; `just simulate` documents the flag.

## Out of Scope

Real consensus, tx validity, P2P, block templates, and the GBT estimator itself
(only its *test substrate* — realistic synthetic fee data — is in scope here).
