# btc-indexer

A lightweight, self-hostable Rust backend that keeps an accurate, always-current
in-memory view of the Bitcoin mempool — synced directly from your own Bitcoin Core
node — as the foundation for **accurate fee recommendations**. One static binary, no
database, no Redis, no explorer.

**Status:** the mempool builder (Phase 1) is shipped and hardened. A fee-latency phase
(Phase 2 — concurrent fetch, ZMQ block-push, freshness signals) is in progress. The fee
estimator itself (Phase 3 — a GBT-style assembler and a `/fees` endpoint) is the end
goal. See the [Roadmap](#5-roadmap).

---

## 1. Project description

btc-indexer connects to a Bitcoin Core node over JSON-RPC and maintains a faithful,
continuously-updated copy of the node's mempool in memory. It exposes that state over a
small read-only HTTP API. It is deliberately minimal: a single process that talks only
to your node, with no external dependencies to operate.

The project is built in focused phases, each shipping working software on its own:

- **Phase 1 — mempool builder (done):** RPC connection, diff-based mempool sync, shared
  state, and a `/health` endpoint.
- **Phase 2 — fee latency (in progress):** drive mempool freshness toward mempool.space
  parity — bounded concurrent fetch, ZMQ block-push for instant post-block recompute, a
  time-based freshness guard, and package (ancestor/descendant) data capture.
- **Phase 3 — fees (planned):** a GBT-style block assembler and a `/fees` endpoint.

The `/fees` endpoint is the whole point; everything before it exists to feed it a fresh,
complete, honestly-aged mempool.

## 2. Problem to solve

Wallets and explorers must answer one deceptively hard question: *"what fee rate should I
pay to confirm in roughly N blocks?"*

Many setups answer it with a node's built-in estimator (`estimatesmartfee`) or coarse
heuristics. Those are convenient but can be **inaccurate or laggy** — they smooth over
short-term mempool dynamics and don't reflect what a miner would actually assemble *right
now*. When the mempool moves quickly — exactly when a good estimate matters most — a
stale-low number leaves a transaction stuck.

The accurate approach is to **simulate the next few blocks from the live mempool**,
ordering transactions by their real, CPFP-aware *effective* fee rate the way a miner
does, and read the fee tiers off that simulation. That is what mempool.space does — but
its backend is a large, multi-service system (explorer, analytics, mining dashboards, a
database, Redis, multiple node backends).

**btc-indexer is the lightweight version of just the fee-estimation core.** And because a
fee estimate is only ever as good as the **freshness and completeness** of the mempool it
is computed from, keeping that view current — even at a block boundary, even under load —
is a first-class concern of this project, not an afterthought.

## 3. Architecture and Design

```
 Bitcoin Core ──RPC──►  sync task  ──writes──►  Arc<RwLock<MempoolState>>  ──reads──►  axum /health
   (poll ~2s)          (supervised)            (single writer, many readers)          (tokio)
```

**Single writer, many readers.** The sync loop is the only thing that mutates state; HTTP
handlers only read. This keeps the design small and the concurrency obvious. The sync
loop runs as a dedicated supervised background task — if it ever dies, the process exits
so a supervisor (systemd/Docker) restarts it, rather than silently freezing while still
serving `/health`.

### Components

| Module      | Responsibility                                                        |
|-------------|-----------------------------------------------------------------------|
| `config`    | Parse configuration from environment variables / CLI flags.           |
| `transport` | The custom JSON-RPC HTTP transport (auth, custom headers, timeout).   |
| `rpc`       | Typed wrapper over `bitcoincore-rpc` for the handful of calls we use.  |
| `mempool`   | The `MempoolTx` / `MempoolState` model and the diff/apply logic.       |
| `sync`      | The poll loop: cold-load, steady-state diff, restart guard.            |
| `http`      | The axum router and `/health` handler.                                 |
| `main`      | Wire config → shared state → sync task → HTTP server.                  |

### The sync loop

**Cold start.** Wait until the node reports its mempool is loaded
(`getmempoolinfo.loaded`), then bulk-load the whole mempool once (`getrawmempool true`)
and mark the state `caught_up`.

**Steady state (every poll).** Fetch the cheap txid list (`getrawmempool false`), diff it
against the cache, and fetch details (`getmempoolentry`) **only for newly-seen
transactions** — departed transactions are simply dropped. This avoids re-downloading the
entire mempool (100+ MB) every couple of seconds; only the small per-poll delta costs a
detail fetch. New-fetch volume per tick is capped so an unbounded mempool can't force one
tick to issue unbounded RPCs.

**Restart guard.** If the node restarts, its mempool briefly looks empty and a naive diff
would delete the whole cache. The loop guards against this using real signals — the node's
`loaded` flag plus a "don't evict on a sudden mass drop" check with a resync cooldown —
freezing eviction and clearing `caught_up` until the node looks healthy again, rather than
emitting a spurious "mempool cleared".

**Honesty.** `caught_up` is only `true` when a tick fully resolved the node's new-txid
list; any RPC error, backlog, or outage flips it `false`, and `last_sync_ok` records the
last good sync so a consumer can tell "never synced" from "synced N seconds ago".

### Freshness & latency — Phase 2 (in progress)

Phase 1's steady-state fetch is **sequential** (one `getmempoolentry` per new tx). Against
a local node that is sub-millisecond; against latency, or during a post-block refill burst
even locally, a tick can run long and the mempool view silently lags — precisely the data a
fee estimate sits on. Phase 2 closes that gap:

- **Bounded concurrent fetch** (`FETCH_CONCURRENCY`, default 10) — fetch new-tx details in
  parallel instead of one at a time.
- **ZMQ block-push** (`BTC_ZMQ_BLOCK`) — subscribe to the node's `zmqpubhashblock` so a new
  block triggers an *immediate* recompute instead of waiting for the next poll. A block is
  when fees swing most, so this is the highest-leverage freshness win. Polling remains the
  baseline; ZMQ is an opt-in accelerator.
- **Time-based freshness guard** (`TICK_BUDGET_MS`) — if a tick's fetch overruns its budget,
  stop and mark `caught_up=false` rather than let a slow tick masquerade as fresh.
- **Package data** — capture ancestor/descendant fee+size (already returned by
  `getmempoolentry`) so the Phase-3 assembler can rank by *effective* (CPFP-aware) fee rate.

These knobs are present in the configuration but are being wired in over Phase 2; see the
[Roadmap](#5-roadmap) for current status. ZMQ is a raw socket on the node, so this path
assumes a **local node** — which is the intended deployment anyway.

### Correctness notes

- **Fees are integer satoshis**, never floating point — Bitcoin Core reports fees as BTC
  decimals, which `bitcoincore-rpc` deserializes into `rust-bitcoin`'s `Amount`. The only
  floating-point value is the *fee rate* (`mempool_min_fee_sat_vb`), where it's appropriate.
- **The network is inferred from the node** (`getblockchaininfo.chain`) — no network flag to
  misconfigure.
- **No lock is held across an RPC call or `.await`**, so a slow node never blocks `/health`
  readers.

### Known limitations

- The RPC transport opens a fresh connection per call (no HTTP keep-alive). Concurrency
  still massively beats sequential fetching; connection pooling would require an async RPC
  client and is out of scope.
- Phase 2 does block-push only; a fully event-driven ingest (`zmqpubsequence` +
  `zmqpubrawtx`) is a later ceiling.

## 4. Setup

### Requirements

- A running **Bitcoin Core** node with JSON-RPC enabled (verified against Bitcoin Core
  v29). No other services required.
- **Rust** (stable) to build. Optionally [`just`](https://github.com/casey/just) as a task
  runner.

### Node setup

You need a **full, tip-synced** node — but not a heavy one:

- **No `txindex` needed.** The mempool RPCs read the in-memory mempool; they don't touch a
  transaction index. Skip `txindex=1`.
- **Pruning is fine.** The mempool is independent of block storage, so a pruned node
  (`prune=10000`, ~10 GB) serves every RPC we use. (Initial Block Download still validates
  the whole chain once.)
- **Must not be `blocksonly`.** `blocksonly=1` disables mempool relay → a near-empty
  mempool → useless fees. Leave it off (the default).
- **Must be caught up to the chain tip** — a still-syncing node has an unrepresentative
  mempool.

A minimal `bitcoin.conf`:

```ini
server=1              # enable JSON-RPC
prune=10000           # ~10 GB; mempool RPCs are unaffected by pruning
maxmempool=300        # MB; larger = fuller mempool view = better fees
# txindex NOT set     # not needed
# blocksonly MUST stay off (default)

# --- for Phase 2 (optional, when using concurrency / ZMQ) ---
rpcthreads=10         # match FETCH_CONCURRENCY so parallel fetches aren't serialized
# rpcworkqueue=16     # keep FETCH_CONCURRENCY <= this (default 16)
# zmqpubhashblock=tcp://127.0.0.1:28332   # enables immediate recompute on new blocks
```

### Build & run

```bash
# Build
cargo build --release            # or: just release

# Run against a local node using cookie auth
BTC_RPC_URL=http://127.0.0.1:8332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
./target/release/btc-indexer

# ...or against a hosted HTTPS provider whose key is in the URL (no auth needed)
BTC_RPC_URL=https://go.getblock.io/<YOUR_KEY> ./target/release/btc-indexer
```

Or copy `.env.example` to `.env` (gitignored) and run `just run`, which loads it
automatically. Check it:

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

### Configuration

All configuration is via environment variables (equivalent `--kebab-case` CLI flags also
work). Only the RPC connection is required; everything else has a default.

| Variable                | Default                 | Meaning                                          |
|-------------------------|-------------------------|--------------------------------------------------|
| `BTC_RPC_URL`           | `http://127.0.0.1:8332` | Bitcoin Core JSON-RPC URL (`http://` local or `https://` provider). |
| `BTC_RPC_COOKIE_FILE`   | —                       | Path to the node's `.cookie` file.               |
| `BTC_RPC_USER` / `_PASS`| —                       | RPC username / password (used together).         |
| `BTC_RPC_HEADERS`       | —                       | Extra request headers, `Name: Value`, comma-separated (or repeat `--rpc-header`). For API-key providers. |
| `HTTP_BIND`             | `127.0.0.1:8080`        | Address the HTTP server binds to.                |
| `POLL_INTERVAL_MS`      | `2000`                  | Mempool poll interval, in milliseconds.          |
| `RPC_TIMEOUT_SECS`      | `30`                    | Timeout for each Bitcoin Core RPC call, seconds. |
| `SYNC_LOG_VERBOSE`      | `false`                 | Log one INFO line per sync tick. Accepts `true/false/1/0/yes/no`. |
| `HEARTBEAT_SECS`        | `30`                    | Seconds between steady-state liveness heartbeat logs; `0` disables. |
| `FETCH_CONCURRENCY` ⏳   | `10`                    | *(Phase 2)* Max concurrent `getmempoolentry` calls per tick. Bound by node `rpcthreads`/`rpcworkqueue`. |
| `BTC_ZMQ_BLOCK` ⏳       | —                       | *(Phase 2)* Node `zmqpubhashblock` endpoint for immediate recompute on new blocks. Unset = polling only. |
| `TICK_BUDGET_MS` ⏳      | `2 × POLL_INTERVAL_MS`  | *(Phase 2)* Max fetch time per tick before bailing and marking stale. |

⏳ = accepted by the binary now; behavior is being wired in over Phase 2.

**Authentication:** provide `BTC_RPC_COOKIE_FILE`, **or** both `BTC_RPC_USER` and
`BTC_RPC_PASS`, **or neither** — omit auth when the endpoint carries its credential in the
URL (hosted providers like GetBlock, e.g. `https://go.getblock.io/<KEY>`). A cookie file
takes precedence; it's the usual choice for a local node (Bitcoin Core writes it to its
data directory, e.g. `~/.bitcoin/.cookie` on mainnet). For API-key-header providers, set
`BTC_RPC_HEADERS="X-Api-Key: your_key"` (additive with any auth mode). Because the env var
is comma-separated, avoid header *values* containing a comma (use repeated `--rpc-header`
flags instead).

### `/health` fields

| Field                    | Type          | Meaning                                                                 |
|--------------------------|---------------|-------------------------------------------------------------------------|
| `caught_up`              | bool          | `true` only after a recent successful sync; `false` before the first sync, while resyncing, or during any node/RPC outage. |
| `mempool_size`           | number        | Transactions currently held in the in-memory mempool.                   |
| `tip_height`             | number        | The node's chain tip height as last seen.                               |
| `mempool_min_fee_sat_vb` | number        | The node's current mempool min fee, in sat/vB (the eviction floor).     |
| `network`                | string        | Network inferred from the node (`bitcoin`, `testnet`, `signet`, `regtest`). |
| `last_sync_ok`           | number \| null | Unix seconds of the last successful sync, or `null` if never synced.    |

### Logging

Logging is structured (via `tracing`) and follows `RUST_LOG` (defaults to `info`). By
design the sync loop is quiet in steady state — it logs sync-state transitions, errors,
and a periodic heartbeat, not one line per tick — so `info` stays readable in production.

| I want to see…                        | Set                                          |
|---------------------------------------|----------------------------------------------|
| One line per sync tick                | `SYNC_LOG_VERBOSE=true`                       |
| A liveness heartbeat every N seconds  | `HEARTBEAT_SECS=N` (default 30; `0` off)      |
| Every RPC call to the node            | `RUST_LOG=info,bitcoincore_rpc=debug`         |
| RPC calls **and** full responses      | `RUST_LOG=info,bitcoincore_rpc=trace` (loud)  |
| HTTP request access log               | on at `info` by default (method/path/status/latency) |
| …but silence the frequent `/health`   | `RUST_LOG=info,tower_http::trace=warn`        |

HTTP requests are logged via [`tower-http`](https://docs.rs/tower-http)'s `TraceLayer`, so
each request emits an INFO line with method, path, status, and latency.

## 5. Roadmap

| Phase | Scope | Status |
|-------|-------|--------|
| **1** | Mempool builder — RPC, diff-sync, shared state, `/health` | ✅ done |
| **1.1** | Hardening — honest `caught_up`, RPC timeout + cookie-rotation reconnect, resilient fetch, supervised sync task | ✅ done |
| **2** | Fee latency — package-data capture, config knobs, tokio migration, bounded concurrent fetch + time-budget bail, ZMQ block-push, freshness signal | ⏳ in progress |
| **3** | Fees — GBT-style CPFP-aware block assembler; `/fees` endpoint with recommended tiers, gated on `caught_up` | ⏳ planned |

**Beyond:** an optional Esplora/electrs adapter, WebSocket push to clients, connection
keep-alive, and the automated test suite (the codebase is deliberately test-free through
these phases; tests land as their own phase).

## License

See [LICENSE](LICENSE) if present, otherwise this is currently unlicensed / private.
