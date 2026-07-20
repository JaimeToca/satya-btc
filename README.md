# btc-indexer

A lightweight, self-hostable backend that keeps an accurate in-memory view of the
Bitcoin mempool, synced directly from your own Bitcoin Core node.

This is **Phase 1** of a small, focused project whose end goal is a single thing done
well: **accurate fee recommendations**. Phase 1 builds — and ships on its own — the
foundation everything else stands on: a faithful, always-current mempool.

---

## Why this exists

Wallets and explorers need to answer one deceptively hard question: *"what fee rate
should I pay to get confirmed in roughly N blocks?"*

Many setups answer it with a node's built-in estimator (`estimatesmartfee`) or coarse
heuristics. Those are convenient but can be **inaccurate or laggy** — they smooth over
short-term mempool dynamics and don't reflect what a miner would actually assemble
*right now*. When the mempool moves quickly, that gap is exactly when a good estimate
matters most, and a stale-low number leaves a transaction stuck.

The accurate approach is to **simulate the next few blocks from the live mempool** —
ordering transactions by their real, CPFP-aware effective fee rate, the way a miner
does — and read the fee tiers off that simulation. That's what mempool.space does, but
its backend is a large, multi-service system (an explorer, analytics, mining
dashboards, a database, Redis, multiple node backends).

**btc-indexer is the lightweight version of just the fee-estimation core.** No database,
no Redis, no explorer — one static binary that talks only to your Bitcoin Core node.

## What Phase 1 delivers (and what's next)

Phase 1 is intentionally scoped to the **mempool builder**:

- Connects to a Bitcoin Core node over RPC.
- Keeps an in-memory mempool continuously in sync (~2s diff-based poll).
- Exposes the state through a read-only `/health` endpoint.

**Not yet implemented** (on the roadmap):

- The GBT-style block assembler (CPFP-aware projected blocks).
- The `/fees` endpoint (recommended fee tiers).

The `/fees` endpoint is the whole point of the project; Phase 1 is the accurate mempool
it will be computed from. The data model already carries what the assembler needs
(`fee`, `vsize`, `weight`, and unconfirmed-parent links), and the `caught_up` flag is in
place to gate `/fees` (returning 503 until the mempool is fully synced).

## How it works

```
 Bitcoin Core ──RPC──►  sync loop  ──writes──►  Arc<RwLock<MempoolState>>  ──reads──►  axum /health
   (poll ~2s)           (own OS thread)          (single writer, many readers)         (tokio)
```

A single writer, many readers. The sync loop is the only thing that mutates state; HTTP
handlers only read. This keeps the design small and the concurrency obvious.

### Components

| Module      | Responsibility                                                        |
|-------------|-----------------------------------------------------------------------|
| `config`    | Parse configuration from environment variables / CLI flags.           |
| `rpc`       | Typed wrapper over `bitcoincore-rpc` for the handful of calls we use.  |
| `mempool`   | The `MempoolTx` / `MempoolState` model and the diff/apply logic.       |
| `sync`      | The poll loop: cold-load, steady-state diff, restart guard.            |
| `http`      | The axum router and `/health` handler.                                 |
| `main`      | Wire config → shared state → sync thread → HTTP server.                |

### The sync loop

**Cold start.** Wait until the node reports its mempool is loaded (`getmempoolinfo.loaded`),
then bulk-load the whole mempool once (`getrawmempool true`), and mark the state
`caught_up`.

**Steady state (every poll).** Fetch the cheap txid list (`getrawmempool false`), diff it
against the cache, and fetch details (`getmempoolentry`) **only for newly-seen
transactions** — departed transactions are simply dropped. This avoids re-downloading the
entire mempool (which can be 100+ MB) every couple of seconds; only the small per-poll
delta costs a detail fetch.

**Restart guard.** If the node restarts, its mempool briefly looks empty, and a naive diff
would delete the whole cache. The loop guards against this using real signals — the node's
`loaded` flag plus a "don't evict on a sudden mass drop" check — freezing eviction and
clearing `caught_up` until the node looks healthy again, rather than emitting a spurious
"mempool cleared".

**Concurrency.** The loop is blocking and runs on its own OS thread, while axum runs on the
async runtime; they share the state through an `RwLock`. A lock guard is never held across
an RPC call, so a slow node never blocks `/health` readers.

### A note on correctness details

- **Fees are handled as integer satoshis**, never as floating point — Bitcoin Core reports
  fees as BTC decimals, which `bitcoincore-rpc` deserializes into `rust-bitcoin`'s `Amount`.
  The only floating-point value is the *fee rate* (`mempool_min_fee_sat_vb`), where it's
  appropriate.
- **The network is inferred from the node** (`getblockchaininfo.chain`), so there's no
  network flag to misconfigure.

## Requirements

- A running **Bitcoin Core** node with the JSON-RPC interface enabled (verified against
  Bitcoin Core v29). No other services are required.
- **Rust** (stable) to build.

## Configuration

All configuration is via environment variables (equivalent `--kebab-case` CLI flags also
work). Only the RPC connection is required; everything else has a default.

| Variable                | Required            | Default                 | Meaning                                          |
|-------------------------|---------------------|-------------------------|--------------------------------------------------|
| `BTC_RPC_URL`           | no                  | `http://127.0.0.1:8332` | Bitcoin Core JSON-RPC URL.                        |
| `BTC_RPC_COOKIE_FILE`   | one auth method\*   | —                       | Path to the node's `.cookie` file.               |
| `BTC_RPC_USER`          | one auth method\*   | —                       | RPC username (used with `BTC_RPC_PASS`).          |
| `BTC_RPC_PASS`          | one auth method\*   | —                       | RPC password (used with `BTC_RPC_USER`).          |
| `HTTP_BIND`             | no                  | `127.0.0.1:8080`        | Address the HTTP server binds to.                |
| `POLL_INTERVAL_MS`      | no                  | `2000`                  | Mempool poll interval, in milliseconds.          |
| `RPC_TIMEOUT_SECS`      | no                  | `30`                    | Timeout for each Bitcoin Core RPC call, seconds. |

\* **Authentication:** provide **either** `BTC_RPC_COOKIE_FILE` **or** both `BTC_RPC_USER`
and `BTC_RPC_PASS`. If a cookie file is given it takes precedence; if neither is usable the
process exits with a clear error. The cookie file is the usual choice for a local node —
Bitcoin Core writes it to its data directory (e.g. `~/.bitcoin/.cookie` on mainnet).

## Running

```bash
# Build
cargo build --release

# Run against a local node using cookie auth
BTC_RPC_URL=http://127.0.0.1:8332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
./target/release/btc-indexer
```

On start it logs the address it's listening on and begins syncing. Check it:

```bash
curl -s http://127.0.0.1:8080/health | jq
```

```json
{
  "caught_up": true,
  "mempool_size": 152340,
  "tip_height": 850000,
  "mempool_min_fee_sat_vb": 1.0,
  "network": "bitcoin",
  "last_sync_ok": 1721557200
}
```

### `/health` fields

| Field                    | Type          | Meaning                                                                 |
|--------------------------|---------------|-------------------------------------------------------------------------|
| `caught_up`              | bool          | `true` only after a recent successful sync; `false` before the first sync, while resyncing, or during any node/RPC outage. |
| `mempool_size`           | number        | Transactions currently held in the in-memory mempool.                   |
| `tip_height`             | number        | The node's chain tip height as last seen.                               |
| `mempool_min_fee_sat_vb` | number        | The node's current mempool min fee, in sat/vB (the eviction floor).     |
| `network`                | string        | Network inferred from the node (`bitcoin`, `testnet`, `signet`, `regtest`).|
| `last_sync_ok`           | number \| null | Unix seconds of the last successful sync, or `null` if never synced — lets you tell "never synced" from "synced N seconds ago" even when `caught_up` is `false`. |

Logging verbosity follows the standard `RUST_LOG` environment variable (defaults to `info`).

## Roadmap

- **GBT-style block assembler** — order the mempool into projected blocks by CPFP-aware
  effective fee rate.
- **`/fees` endpoint** — recommended fee tiers derived from those projected blocks, gated on
  `caught_up`.

### Hardening (Phase 1.1)

A staff code-review pass hardened the failure paths: `/health` now reports `caught_up: false`
during node/RPC outages (it no longer over-claims readiness), RPC calls have a timeout and the
client reconnects if the node's cookie rotates, a single failed transaction fetch no longer
stalls the sync, and the sync thread is supervised (a panic exits the process for a supervisor
to restart, rather than silently freezing). Remaining items (release profile, `lib.rs` split,
minor allocation trims) are tracked for the test phase.

## License

See [LICENSE](LICENSE) if present, otherwise this is currently unlicensed / private.
