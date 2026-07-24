# Fee estimation via GBT projection — design

## Goal

Serve recommended fee rates at a new `/fees` endpoint, computed from a
CPFP-package-aware projection of which mempool transactions would land in the
next several blocks. This is the "Phase-3 GBT" anticipated by the comment in
`src/mempool.rs` (the one warning that the cached `ancestor_*`/`descendant_*`
package fields drift and must be recomputed from a fresh source).

Scope is **fee estimates only** — four/five summary numbers. Not projected-block
visualization, not a mining template, not per-transaction streaming.

## Provenance & licensing

The transaction-selection algorithm is **Bitcoin Core's `BlockAssembler`**
(`src/node/miner.cpp`, `addPackageTxs`), which is **MIT-licensed**. This is an
independent implementation of Core's public algorithm; no other third-party
code is used.

## Available inputs (and deliberate exclusions)

From `getmempoolentry` / verbose `getrawmempool`, each `MempoolTx` already carries
everything the algorithm needs:

- `fee` (base), `vsize`, `weight`
- `depends: Vec<Txid>` — the in-mempool parent graph, straight from Core.

Two inputs we deliberately **exclude**:

- **Sigops** — computing them requires fetching and parsing every raw transaction.
  We approximate `sigops = 0`, i.e. `sigop_adjusted_vsize ≈ vsize`. The 80k-sigop
  block cap essentially never binds before the size cap on modern mainnet, and the
  per-tx adjustment only perturbs a minority of sigops-heavy txs — rarely enough to
  move a fee-tier boundary. Small, rare, non-systematic gap; closable later by
  adding a sigops source.
- **Accelerations** — out-of-band, paid transaction-priority arrangements offered
  by external services and mining pools. That data is private to those services
  and invisible to a Bitcoin node, and it reflects only specific pools. Excluding
  it keeps our estimate a neutral, network-wide, on-chain number.

## Approach: stateless recompute + cached result

Each recompute rebuilds its working set fresh from the current mempool snapshot —
**no persistent GBT state, no drift, no uid bookkeeping.** We serve only summary
numbers, so there is no need to maintain long-lived per-transaction state across
ticks.

The heavy kernel (relatives graph + scoring + packing) is a full recompute per
run; a stateless design gives up no meaningful speed for a summary endpoint. If
input marshalling ever shows up as a bottleneck at mainnet scale, the sync loop
already produces `diff.new` / `diff.gone`, so an incremental input path can be
added later behind the same `/fees` contract.

## Module structure

Two new modules, mirroring the existing `sync::decision` "pure core" pattern.

### `src/gbt.rs` — pure algorithm (no async, no locks, no I/O)

Unit-testable in isolation like `decision.rs`.

Input, one per transaction:

```rust
pub struct GbtTx {
    pub uid: u32,        // dense 0..n index for this run
    pub order: u32,      // deterministic tie-breaker (leading bytes of txid)
    pub fee: u64,        // sats
    pub weight: u32,     // weight units
    pub parents: Vec<u32>, // parent uids (from `depends`)
}
```

Output:

```rust
pub struct Projection {
    pub blocks: Vec<Vec<u32>>,      // uid lists, block 0 = next block
    pub effective_rates: Vec<(u32, f64)>, // CPFP-adjusted rate per dirty uid
}
```

Algorithm (Core's `addPackageTxs`):

1. Build ancestor sets and ancestor fee/weight totals from `parents`.
2. `score = min(own_fee_rate, ancestor_package_fee_rate)`.
3. Sort by score (descending), `order` as tie-breaker.
4. Greedily pack packages (ancestors first) into blocks up to
   `MAX_BLOCK_WEIGHT = 4_000_000` WU (with the same reserved-weight margin Core
   uses); overflow packages spill to later blocks; final block is unbounded.
5. When a package is mined, re-score descendants (their effective rate drops) via
   a priority queue — this yields the CPFP effective rates.

Constants live here as named `const`s (block weight, reserved weight, max blocks)
with the same rationale-comment style as `decision.rs`.

### `src/fees.rs` — adapter + tier extraction + orchestration

- **Snapshot adapter:** `MempoolState.txs` (`HashMap<Txid, MempoolTx>`) → `Vec<GbtTx>`.
  Build a throwaway `HashMap<Txid, u32>` uid map for this run; map each tx's
  `depends` to parent uids (dropping any parent not in the set — same semantics as
  Core's in-mempool-only ancestors). `order` = first 4 bytes of the txid.
- **Tier extraction:** the projection's per-tx CPFP-effective rates + each tx's
  weight + `mempool_min_fee_sat_vb` → `FeeEstimate`.
- **Orchestration:** throttled recompute driver (below).

```rust
#[derive(Debug, Clone, Serialize)]
pub struct FeeEstimate {
    pub next_block: f64,    // depth 1 block (~next block)
    pub within_3_blocks: f64,  // depth 3 blocks (~30 min)
    pub within_6_blocks: f64,       // depth 6 blocks (~1 hour)
    pub horizon: f64,    // depth MAX_BLOCKS (projection horizon), floored at relay_floor
    pub relay_floor: f64,    // mempool_min_fee_sat_vb (relay floor)
    pub computed_at: u64,          // unix seconds this estimate was computed
}
```

Tier rule — **weight histogram of effective rates.** Reading a tier off a single
projected block's *minimum* rate is NOT monotone: greedy assembly fills the tail
of early blocks with small low-rate gap-filler txs, so block 0's minimum can dip
below a later block's. Instead, tiers are read off a weight histogram: take
`(effective_rate, weight)` for every tx, sort by rate descending, and the fee to
confirm within N blocks is the rate at which cumulative weight first reaches
`N × MAX_BLOCK_WEIGHT`. This is **monotone by construction**
(`fastest ≥ half_hour ≥ hour ≥ economy`) and robust to gap-filler outliers. If the
mempool holds less than N blocks of weight, anything at the relay floor confirms,
so that tier is the floor. Depths (1 / 3 / 6 / `MAX_BLOCKS`) are named constants;
`relay_floor` = `mempool_min_fee_sat_vb`. All tunable without touching `gbt.rs`.

## Integration with the sync loop

`MempoolState` gains one field:

```rust
pub fee_estimate: Option<FeeEstimate>,
```

Recompute is triggered from `steady_tick` **after** the apply critical section
(the `write_state` block near `src/sync/mod.rs:366` that inserts new txs and sets
min-fee/tip) and after `bulk_resync`. Mechanics:

- Runs on `tokio::task::spawn_blocking` (CPU-bound; must not stall the async sync
  loop), reading a cloned snapshot of the txs it needs.
- **Throttled:** recompute at most once per `FEE_RECOMPUTE_MIN_INTERVAL`
  (new config knob, default ~5s, floored at `poll_interval`). Skip if a recompute
  is already in flight.
- On completion, writes `fee_estimate` under `write_state`.

`caught_up` gating: `/fees` is gated on `caught_up`, so it never serves a number
computed from a mempool it can't vouch for. `computed_at` is still included so callers
can see the estimate's age. This matches the README's stated contract.

## HTTP surface

New route `GET /fees` in `src/http.rs`, alongside `/health`:

- `200` with the `FeeEstimate` JSON when an estimate exists **and** the state is
  `caught_up`.
- `503` otherwise — before the first estimate is computed, or whenever the sync
  layer reports the mempool is out of sync.

`FeeEstimate` derives `Serialize`. No change to `/health`.

## Configuration

One new knob in `src/config.rs`, following the existing clap/env pattern:

- `FEE_RECOMPUTE_MIN_INTERVAL_MS` (default 5000, floored at `poll_interval`).

Block count and weight are compile-time constants in `gbt.rs` (not configurable);
`relay_floor` reuses the existing `mempool_min_fee_sat_vb`.

## Testing

- **`gbt.rs` unit tests** (pure, deterministic), covering:
  - Independent txs pack in descending fee-rate order.
  - **CPFP:** a low-fee parent with a high-fee child are selected together, and the
    parent's effective rate is lifted to the package rate.
  - Block boundary: packing stops at `MAX_BLOCK_WEIGHT`; overflow spills to the
    next block; final block unbounded.
  - Deterministic tie-break via `order`.
- **`fees.rs` unit tests:** `Projection` → `FeeEstimate` tier mapping, including the
  `relay_floor` floor and the empty-mempool case.
- **Simulation harness:** add a sim test that drives the sync loop against
  `MockNode` and asserts the produced `FeeEstimate` is populated and monotone
  (`fastest ≥ half_hour ≥ hour ≥ minimum`) and stays finite under churn. CPFP
  correctness is covered at the `gbt.rs` unit level, so the mock node needs no
  changes.

## Non-goals

- No projected-block visualization or per-tx streaming.
- No mining block template / coinbase construction.
- No raw-transaction fetch (hence no exact sigops).
- No acceleration modelling.
- No on-disk persistence of the estimate.

## Design decisions (summary)

- **Algorithm:** independent implementation of Core's `BlockAssembler` (MIT).
- **Parent graph:** taken from Core's `depends` (no raw-tx parsing).
- **Sigops:** approximated (`0`) — minor, rare, closable-later gap.
- **Accelerations:** excluded — keeps a neutral, network-wide estimate.
- **State:** stateless recompute per run — no drift, no uid bookkeeping.
- **Runtime:** native Rust in the same binary.
- **Output:** cached `FeeEstimate` served at `/fees`.
