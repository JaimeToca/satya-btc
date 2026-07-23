# Satya

> **Satya** (Sanskrit: truth) - the true, live fee your own node sees.

A lightweight, self-hostable Rust backend that keeps an accurate, always-current
in-memory view of the Bitcoin mempool — synced directly from your own Bitcoin Core
node — as the foundation for **accurate fee recommendations**. One static binary, no
database, no Redis, no explorer.

**Status:** the mempool builder (Phase 1) is shipped and hardened. A fee-latency phase
(Phase 2 — concurrent fetch, ZMQ block-push, freshness signals) is complete. The fee
estimator itself (Phase 3 — a GBT-style assembler and a `/fees` endpoint) is the end
goal. See the [Roadmap](#5-roadmap).

---

## 1. Project description

Satya connects to a Bitcoin Core node over JSON-RPC and maintains a faithful,
continuously-updated copy of the node's mempool in memory. It exposes that state over a
small read-only HTTP API. It is deliberately minimal: a single process that talks only
to your node, with no external dependencies to operate.

The project is built in focused phases, each shipping working software on its own:

- **Phase 1 — mempool builder (done):** RPC connection, diff-based mempool sync, shared
  state, and a `/health` endpoint.
- **Phase 2 — fee latency (done):** drive mempool freshness toward mempool.space
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

**Satya is the lightweight version of just the fee-estimation core.** And because a
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
| `rpc`       | Async JSON-RPC client over `reqwest` (auth, timeout, the calls we use).|
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

### Freshness & latency

Phase 1's steady-state fetch was **sequential** (one `getmempoolentry` per new tx). Against
a local node that is sub-millisecond; against latency, or during a post-block refill burst
even locally, a tick can run long and the mempool view silently lags — precisely the data a
fee estimate sits on. Phase 2 closes that gap:

- **Bounded concurrent fetch** (`FETCH_CONCURRENCY`, default 10) — fetches new-tx details in
  parallel instead of one at a time.
- **ZMQ block-push** (`BTC_ZMQ_BLOCK`) — subscribes to the node's `zmqpubhashblock` so a new
  block triggers an *immediate* recompute instead of waiting for the next poll. A block is
  when fees swing most, so this is the highest-leverage freshness win. Polling remains the
  baseline; ZMQ is an opt-in accelerator.
- **Time-based freshness guard** (`TICK_BUDGET_MS`) — if a tick's fetch overruns its budget,
  it stops and marks `caught_up=false` rather than let a slow tick masquerade as fresh.
- **Package data** — captures ancestor/descendant fee+size (already returned by
  `getmempoolentry`) so the Phase-3 assembler can rank by *effective* (CPFP-aware) fee rate.

ZMQ is a raw socket on the node, so this path assumes a **local node** — which is the
intended deployment anyway.

### Correctness notes

- **Fees are integer satoshis**, never floating point — Bitcoin Core reports fees as BTC
  decimals, which we deserialize (via serde's `as_btc`) straight into the `bitcoin`
  crate's `Amount`. The only floating-point value is the *fee rate*
  (`mempool_min_fee_sat_vb`), where it's appropriate.
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

## 4. Deployment

Satya is a single static binary that talks to **one** thing: a Bitcoin Core JSON-RPC
endpoint. That endpoint is the only dependency you must supply — we can't bundle a node.
This section covers how to get each dependency, then four concrete paths from "kick the
tyres in 60 seconds" to "run it in prod on your own low-disk mainnet node".

### 4.1 Dependencies (how to get each)

| Dependency | Required? | How to get it |
|------------|-----------|---------------|
| **Rust** (stable) | to build from source | Install via [rustup](https://rustup.rs): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh`. Builds on latest stable; developed against 1.93. Not needed if you deploy the Docker image. |
| **A Bitcoin RPC endpoint** | **yes** — the one hard dependency | Either **run your own node** ([Bitcoin Core download](https://bitcoincore.org/en/download/), verified against v29) — regtest/signet/mainnet, see the paths below — **or** point at a **remote provider** (GetBlock, QuickNode, …) with the key in the URL. |
| [**`just`**](https://github.com/casey/just) | optional | `cargo install just`. Task runner for the `just <recipe>` shortcuts below. Every recipe is a thin wrapper — you can always run the underlying `cargo`/`curl`/`bitcoin-cli` command by hand. |
| [**Docker**](https://docs.docker.com/get-docker/) | optional | Only for the container path (`just docker` / `docker compose up`). |
| [**`jq`**](https://jqlang.github.io/jq/) | optional | Pretty-prints `/health` in `just health`. Any JSON tool (or none) works. |

You need **exactly one** RPC endpoint. Pick the path that matches your goal:

| Path | Endpoint | Disk | Real fees? | Use it for |
|------|----------|------|-----------|------------|
| **A — regtest** | local `bitcoind -regtest` | ~0 | ✗ (you mint blocks) | fastest smoke test of the mechanics |
| **B — signet** | local `bitcoind -signet` | a few GB | ✗ (test-net levels) | realistic relay on a real (test) mempool |
| **C — mainnet node** | your own node | ~15 GB+ | ✓ | **production** |
| **D — remote provider** | `https://provider/<key>` | 0 | ✓ but degraded | zero-infra look, works today |

### Path A — Fastest test (regtest, zero disk)

Regtest is a private chain you mine yourself. No download, no sync, disposable. It proves
the sync loop / `/health` plumbing works, but the fees are meaningless (you decide when
blocks happen), so this is **mechanics only, not real fees**.

```bash
just regtest-up          # starts bitcoind -regtest, a `dev` wallet, and 101 mature blocks
```

<details><summary>…or the manual equivalent</summary>

```bash
bitcoind -regtest -daemon
bitcoin-cli -regtest createwallet dev
bitcoin-cli -regtest generatetoaddress 101 "$(bitcoin-cli -regtest getnewaddress)"
```
</details>

Point Satya at it (regtest RPC is on **18443**; cookie lives under the `regtest/`
subdir) and create some mempool activity:

```bash
BTC_RPC_URL=http://127.0.0.1:18443 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/regtest/.cookie" \
./target/release/satya &          # or: just run  (reads .env — its defaults target regtest)

just regtest-tx                    # sends one tx into the mempool
just health                        # expect mempool_size >= 1, caught_up: true
```

Tear it down with `just regtest-down`.

### Path B — Realistic test (signet)

Signet is a stable, low-volume public test network with **real relay** and a real
(if small) mempool. It syncs in minutes and a few GB. This exercises Satya against
genuine mempool dynamics — but signet fee levels are **not mainnet fee levels**, so treat
the numbers as a functional check, not a production estimate.

`bitcoin.conf`:

```ini
signet=1
server=1
# blocksonly MUST stay off (default) so the mempool relays
```

Then start `bitcoind`, wait for it to reach the tip, and run Satya against the signet RPC
port (**38332**), cookie under `signet/`:

```bash
BTC_RPC_URL=http://127.0.0.1:38332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/signet/.cookie" \
./target/release/satya
```

### Path C — Production (your own mainnet node, low disk)

This is the real deployment: fees computed from **your** node's live mainnet mempool.
You need a **full, tip-synced, non-`blocksonly`** node — but not a heavy one. A minimal
`bitcoin.conf`:

```ini
server=1              # enable JSON-RPC
prune=550             # MB; smallest allowed. Bump higher if you like — mempool RPCs
                      # are unaffected by pruning. (~11 GB chainstate is unprunable.)
maxmempool=300        # MB; larger = fuller mempool view = truer fees (see §4.6)
# txindex NOT set     # not needed — mempool RPCs never touch a tx index
# blocksonly MUST stay off (default) — it disables mempool relay -> useless fees
rpcthreads=10         # >= FETCH_CONCURRENCY so parallel fetches aren't serialized
# rpcworkqueue=16     # keep FETCH_CONCURRENCY <= this (default 16)
# zmqpubhashblock=tcp://0.0.0.0:28332   # optional: immediate recompute on new blocks
```

**No `txindex`. Pruning is fine.** The mempool is held in memory and is independent of
block storage, so every RPC Satya uses works on a pruned node. **Must not be
`blocksonly`** (that kills mempool relay) and **must be caught up to the tip** (a syncing
node has an unrepresentative mempool).

**Two cheap ways to get a usable node:**

- **(i) Pruned node** — set `prune=550` (or larger). Steady-state disk is small (**~15 GB
  including the ~11 GB unprunable chainstate**), but you still pay a **full ~600 GB IBD
  download** once as the node validates the whole chain top-to-bottom (the blocks are
  discarded as it goes; only the download is unavoidable).
- **(ii) `assumeutxo` snapshot** (Bitcoin Core 26+) — `loadtxoutset` a recent UTXO
  snapshot and the node becomes usable **in minutes**: it jumps to the snapshot height,
  serves mempool/fee RPCs immediately, and **background-validates** the rest of the chain
  behind you. Pair it with `prune=550` for a small, fast-to-stand-up node. (The
  ~11 GB chainstate floor still applies.)

Either way, run Satya against the mainnet RPC (**8332**) with cookie auth:

```bash
BTC_RPC_URL=http://127.0.0.1:8332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
./target/release/satya
```

### Path D — No node (remote provider)

Zero local infrastructure: point `BTC_RPC_URL` at a hosted provider whose API key is in
the URL (no separate auth). **Works today** and is fine for a quick look.

```bash
BTC_RPC_URL=https://go.getblock.io/<YOUR_KEY> ./target/release/satya
```

But it is **degraded** for fee accuracy: the fetch is **latency-bound** (each
`getmempoolentry` is a round-trip over the internet, not a sub-ms local call), you hit
**rate limits**, you **trust the provider's mempool** rather than your own, and there is
**no ZMQ** block-push. Satya stays honest about it — when latency blows the tick budget it
reports `caught_up=false` — but see §4.6 for why your own node is strictly better.

### 4.2 Run it

Once you have an endpoint, run Satya either as a binary or in Docker.

**Binary:**

```bash
cargo build --release            # or: just release   (LTO'd; see [profile.release])

# with inline env (any path above), or copy .env.example -> .env and use `just run`
BTC_RPC_URL=... BTC_RPC_COOKIE_FILE=... ./target/release/satya
```

`just run` loads `.env` (gitignored) automatically; its shipped defaults target the
regtest of Path A. Then check it:

```bash
curl -s http://127.0.0.1:8080/health | jq      # or: just health
```

**Docker:** the multi-stage image builds the binary and ships it on a slim Debian base
with a `curl`-based `HEALTHCHECK` on `/health`.

```bash
just docker                       # == docker compose up --build
```

`docker-compose.yml` reaches a node on the Docker **host** by default
(`http://host.docker.internal:8332`) and mounts `${BITCOIN_DATADIR:-~/.bitcoin}`
read-only so the container can read the `.cookie`. Override `BTC_RPC_URL` /
`BITCOIN_DATADIR` (or swap to `BTC_RPC_USER` / `BTC_RPC_PASS`) via your environment or a
`.env` file. `restart: unless-stopped` pairs with the app's fail-fast design: if the sync
task dies the process exits and the container is restarted.

### 4.3 Health check

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
  "last_sync_ok": 1721557200,
  "age_secs": 2
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
| `HTTP_BIND`             | `127.0.0.1:8080`        | Address the HTTP server binds to.                |
| `POLL_INTERVAL_MS`      | `2000`                  | Mempool poll interval, in milliseconds.          |
| `RPC_TIMEOUT_SECS`      | `30`                    | Timeout for each Bitcoin Core RPC call, seconds. |
| `FETCH_CONCURRENCY`     | `10`                    | Max concurrent `getmempoolentry` calls per tick. Bound by node `rpcthreads`/`rpcworkqueue`. |
| `BTC_ZMQ_BLOCK`         | —                       | Node `zmqpubhashblock` endpoint for immediate recompute on new blocks. Unset = polling only. |
| `TICK_BUDGET_MS`        | `2 × POLL_INTERVAL_MS`  | Max fetch time per tick before bailing and marking stale. |

**Authentication:** provide `BTC_RPC_COOKIE_FILE`, **or** both `BTC_RPC_USER` and
`BTC_RPC_PASS`, **or neither** — omit auth when the endpoint carries its credential in the
URL (hosted providers like GetBlock, e.g. `https://go.getblock.io/<KEY>`). A cookie file
takes precedence; it's the usual choice for a local node (Bitcoin Core writes it to its
data directory, e.g. `~/.bitcoin/.cookie` on mainnet).

### `/health` fields

| Field                    | Type          | Meaning                                                                 |
|--------------------------|---------------|-------------------------------------------------------------------------|
| `caught_up`              | bool          | `true` only after a recent successful sync; `false` before the first sync, while resyncing, or during any node/RPC outage. |
| `mempool_size`           | number        | Transactions currently held in the in-memory mempool.                   |
| `tip_height`             | number        | The node's chain tip height as last seen.                               |
| `mempool_min_fee_sat_vb` | number        | The node's current mempool min fee, in sat/vB (the eviction floor).     |
| `network`                | string        | Network inferred from the node (`bitcoin`, `testnet`, `signet`, `regtest`). |
| `last_sync_ok`           | number \| null | Unix seconds of the last successful sync, or `null` if never synced.    |
| `age_secs`               | number \| null | Seconds since the last successful sync (`last_sync_ok`); `null` if never synced. A freshness signal for consumers. |

### Logging

Logging is structured (via `tracing`) and follows `RUST_LOG` (defaults to `info`). By
design the sync loop is **silent when healthy** — it logs sync-state transitions (e.g.
"mempool in sync" / "mempool out of sync") and lifecycle events, not one line per tick,
so `info` stays readable in production. Control-plane RPC errors (`mempool_info failed`,
`raw_mempool_txids failed`, and the `bulk_resync` RPC failures) surface at `warn` so a
real problem is visible without turning on debug logging.

| I want to see…                        | Set                                          |
|---------------------------------------|----------------------------------------------|
| Per-tx fetch failures + desync detail | `RUST_LOG=info,satya::sync=debug`       |
| HTTP request access log               | on at `info` by default (method/path/status/latency) |
| …but silence the frequent `/health`   | `RUST_LOG=info,tower_http::trace=warn`        |

HTTP requests are logged via [`tower-http`](https://docs.rs/tower-http)'s `TraceLayer`, so
each request emits an INFO line with method, path, status, and latency.

### 4.6 Why your own node (and how it affects the fee algorithm)

You can run Satya against a remote provider (Path D) — but a node you control is strictly
better, and the reason is the fee algorithm itself.

**Trust & privacy.** Fees are computed from *your* node's mempool. You don't have to trust
a third party's view of the network, and you don't leak which fees or transactions you
care about to a provider that sees every request.

**The algorithm is only as good as the mempool it sees.** The (Phase-3) estimate works by
simulating the next few blocks from the live mempool the way a miner would. So its accuracy
is bounded by two properties of that mempool — **completeness** and **freshness**:

- *Completeness.* Missing transactions make the simulation under-count congestion, so it
  estimates fees **too low** — and a too-low fee is exactly what leaves a transaction stuck.
  A local node holds the full mempool up to its `maxmempool`; a rate-limited, latency-bound
  remote fetch can be incomplete (Satya then honestly reports `caught_up=false` rather than
  vouch for a partial view). A small `maxmempool` also evicts the lowest-fee transactions
  and **truncates the bottom of the fee distribution**, which is what skews the economy tier.

- *Freshness.* The estimate reflects the mempool **at fetch time**. Remote latency or an
  un-synced node makes that snapshot stale — and it goes stale **worst exactly when it
  matters most**: during a fee spike or right at a block boundary, when the mempool is
  moving fastest. A local node plus ZMQ block-push (`BTC_ZMQ_BLOCK`) gives the lowest
  possible latency, so the estimate tracks reality instead of trailing it. This is the same
  `caught_up` / `age_secs` freshness contract documented in the [sync loop](#the-sync-loop)
  and [docs/sync-explained.md](docs/sync-explained.md).

**Bottom line.** A local (or pruned / `assumeutxo`) node that is tip-synced, **not**
`blocksonly`, carries a healthy `maxmempool`, and pushes blocks over ZMQ gives Satya the
truest, freshest, most complete input — and therefore the best fee accuracy. Remote
providers work and are fine for a look, but they trade away freshness and trust, which are
precisely the two things the fee estimate is built on.

## 5. Roadmap

| Phase | Scope | Status |
|-------|-------|--------|
| **1** | Mempool builder — RPC, diff-sync, shared state, `/health` | ✅ done |
| **1.1** | Hardening — honest `caught_up`, RPC timeout + cookie-rotation reconnect, resilient fetch, supervised sync task | ✅ done |
| **2** | Fee latency — package-data capture, config knobs, tokio migration, bounded concurrent fetch + time-budget bail, ZMQ block-push, freshness signal | ✅ done |
| **3** | Fees — GBT-style CPFP-aware block assembler; `/fees` endpoint with recommended tiers, gated on `caught_up` | ⏳ planned |

**Beyond:** an optional Esplora/electrs adapter, WebSocket push to clients, connection
keep-alive, and the automated test suite (the codebase is deliberately test-free through
these phases; tests land as their own phase).

## License

See [LICENSE](LICENSE) if present, otherwise this is currently unlicensed / private.
