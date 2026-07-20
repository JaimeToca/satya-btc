# Phase 1 — Mempool Builder Design

- **Date:** 2026-07-20
- **Status:** Approved for implementation planning
- **Scope:** Phase 1 of a lightweight Bitcoin fee-estimation backend

---

## 1. Why this backend exists

Some Bitcoin node and wallet setups surface **inaccurate fee estimates** — they lean on
`estimatesmartfee` or coarse heuristics that lag the real state of the mempool. The goal of
this project is a **lightweight, self-hostable backend** that derives accurate recommended
fees from a faithful, live simulation of what a miner would actually build next (a GBT-style
block assembler), rather than from the node's built-in estimator.

That end goal — **only a fees endpoint** — is reached in later phases. **Phase 1 builds the
foundation it all rests on: a faithful in-memory mempool kept continuously in sync with a
Bitcoin Core node.** No fees, no block assembly yet — just the accurate, current mempool and
the shared state the next phases will read.

## 2. Scope

**In scope (Phase 1):**
- Bitcoin Core RPC connection (reusing `bitcoincore-rpc`).
- A diff-sync poll loop that keeps an in-memory mempool current (~1–2s cadence).
- Shared, concurrently-readable mempool state.
- A `/health` status endpoint — the seam the future `/fees` endpoint plugs into.

**Deferred (explicitly out of Phase 1):**
- The GBT-style block assembler.
- The `/fees` endpoint and fee math.
- Sigops (needed by the assembler, not by the builder).
- Disk persistence / warm-restart snapshot.
- ZMQ / sub-second liveness.
- All tests (unit + integration) — handled in a later phase.

**Non-goals (for the whole project):**
- Not an explorer; no block history, analytics, or charts.
- No database, Redis, esplora, or electrum. **Bitcoin Core RPC only.**
- Single static binary, deployable by third parties with minimal configuration.

## 3. Verified assumptions

Verified against a live **Bitcoin Core v29** node.

- `getrawmempool true` returns per tx: `vsize`, `weight`, `fees.base` (BTC), `depends`
  (unconfirmed parents), `spentby`, ancestor/descendant counts & sizes. **No sigops.**
- `getrawmempool false` (txid list) is far cheaper (~15 MB at 300k txs vs ~100–150 MB verbose)
  and is the right primitive for a per-poll "what changed" diff.
- `getmempoolinfo` exposes `loaded` (a clean startup-readiness gate) and `mempoolminfee` in
  **BTC/kvB** → `sat/vB = value × 1e8 / 1000`.
- Fees arrive as **BTC floating-point**; they must be handled as integer sats. `bitcoincore-rpc`
  deserializes them into `rust-bitcoin`'s `Amount`, which does this correctly — so we never
  touch `f64` for money.
- Sigops (future GBT need) are absent from every mempool RPC. Sourcing them (via
  `getblocktemplate` or approximation) is a **GBT-phase decision**, not Phase 1.

## 4. Architecture & data flow

```
Bitcoin Core ──RPC──► [ sync loop ] ──writes──► Arc<RwLock<MempoolState>> ──reads──► [ axum /health ]
   (poll 1–2s)         diff + fetch deltas       single writer, many readers        (+ future /fees)
```

- The **sync loop** runs on its own OS thread using the blocking `bitcoincore-rpc` client.
- **axum** runs on tokio and serves read-only handlers.
- They share `Arc<RwLock<MempoolState>>`: the loop is the sole writer (brief write locks per
  poll); handlers are readers. No async plumbing in the RPC layer.

## 5. Components

| Module    | Responsibility                                                        |
|-----------|-----------------------------------------------------------------------|
| `config`  | Parse configuration from env + CLI; hold the validated `Config`.      |
| `rpc`     | Thin typed wrapper over `bitcoincore-rpc` for the ~4 methods we use.  |
| `mempool` | `MempoolTx` model, `MempoolState`, and the diff-sync apply logic.     |
| `sync`    | The poll loop: drives `rpc`, computes the diff, writes to state.      |
| `http`    | axum router + `/health` handler.                                      |
| `main`    | Wire config → state → sync thread + http server.                     |

Each module has one clear purpose and a small interface; the diff-sync logic in `mempool` is
independently reasoned about and does not depend on `http`.

## 6. Data model

```
struct MempoolTx {
    vsize:   u32,
    weight:  u32,
    fee:     Amount,        // rust-bitcoin Amount (sats), parsed from Core's BTC decimal
    depends: Vec<Txid>,     // unconfirmed parents — the CPFP graph, for free
}

struct MempoolState {
    txs:            HashMap<Txid, MempoolTx>,
    mempool_min_fee: FeeRate,   // stored as sat/vB
    tip_height:     u64,
    network:        Network,    // inferred from the node
    caught_up:      bool,       // false until first full sync completes
}
```

Nothing more per tx — sigops and effective/CPFP feerates belong to the GBT phase.

## 7. The sync loop

**Startup**
1. Connect to Core; read `getblockchaininfo` for `chain` (→ `network`) and tip.
2. Gate on `getmempoolinfo.loaded == true` (wait/retry until the node's mempool is ready).
3. **Bulk cold-load** via one `getrawmempool true`, populate `txs`.
4. Set `caught_up = true` once the cache matches the node's reported size.

**Steady state** (every `poll_interval`, default ~1–2s)
1. `getrawmempool false` → current txid set (cheap).
2. Diff against `txs`:
   - **new** = in node, not in cache → fetch details with `get_mempool_entry` (per-tx),
   - **gone** = in cache, not in node → mark for removal.
3. Take the write lock briefly; insert new, remove gone; refresh `mempool_min_fee` and
   `tip_height` from `getmempoolinfo` / tip.

**Restart / clear protection**
- If the node restarts, its mempool briefly looks empty and a naive diff would delete
  everything. Guard on **real signals** — `getmempoolinfo.loaded` + RPC health, plus a
  "don't evict on a sudden mass drop" check — rather than mempool.space's `>20000 && ratio<=0.80`
  heuristic. While guarded: freeze eviction and set `caught_up = false` until the node is healthy.

**`caught_up` semantics**
- Reported by `/health`; the future `/fees` returns **503 until `caught_up`**, so we never serve
  fees derived from a partial mempool.

## 8. Configuration (deployer-first)

**Required:** Bitcoin Core RPC endpoint (host/port or URL) + auth — **cookie-file path or
user:pass**.

**Optional (sane defaults):** HTTP bind address, poll interval, RPC timeout, log level.

**Inferred, not configured:** network (from `getblockchaininfo.chain`) — one fewer thing to
misconfigure.

**Source:** environment variables (container-friendly) with a thin CLI for `--help`. No config-file
format unless a deployer need emerges.

## 9. HTTP surface (Phase 1)

```
GET /health → 200
{
  "caught_up": true,
  "mempool_size": 152340,
  "tip_height": 850000,
  "mempool_min_fee_sat_vb": 1.0,
  "network": "bitcoin"
}
```

Single read-only endpoint. It both verifies the builder works and is the exact shape the future
`/fees` handler will sit beside.

## 10. Dependencies (conservative)

`tokio`, `axum`, `bitcoincore-rpc` (+ `bitcoin`), `serde`, `tracing`, and a small arg parser
(`clap`). Nothing beyond what each responsibility requires. No macro/codegen machinery — there is
no structural repetition to justify it.

## 11. What later phases inherit

- **GBT assembler** reads `MempoolState` directly — `fee`, `vsize`, `weight`, `depends` are
  already present and typed.
- **`/fees`** inherits the `caught_up` 503-gate and `mempool_min_fee` already in sat/vB.
- **Sigops** sourcing is the first open decision of the GBT phase (`getblocktemplate` vs
  approximating `adjusted_vsize = vsize`).

## 12. Deferred items (revisit later, with triggers)

- **Warm-restart snapshot** — add only if the startup 503 window bothers deployers.
- **Batched `get_mempool_entry`** — add only if per-poll deltas grow large enough to matter.
- **Sigops** — decided at the start of the GBT phase.
