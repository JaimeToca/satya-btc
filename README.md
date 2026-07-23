# Satya

> **Satya** (Sanskrit: *truth*) — the true, live fee your own node sees.

Satya is a single, self-hosted binary that turns your Bitcoin Core node's live
mempool into honest fee recommendations. It keeps an always-current in-memory
copy of the mempool, synced directly from your node, and (the destination of the
project) simulates the next few blocks the way a miner would to read fee tiers
off that simulation. One static binary. No database, no Redis, no explorer.

## Contents

- [Why Satya exists](#why-satya-exists)
  - [How Satya differs from mempool.space](#how-satya-differs-from-mempoolspace)
- [System design](#system-design)
- [Building the mempool](#building-the-mempool)
- [Talking to the node: the JSON-RPC transport](#talking-to-the-node-the-json-rpc-transport)
- [Estimating fees by simulating the next blocks](#estimating-fees-by-simulating-the-next-blocks-the-next-thing-to-build)
- [Why your own node](#why-your-own-node-and-how-it-shapes-accuracy)
- [Deployment](#deployment)
- [Engineering notes](#engineering-notes)
- [License](#license)

---

## Why Satya exists

Every wallet and service has to answer one deceptively hard question:

> *What fee rate confirms my transaction in roughly N blocks?*

There are two common ways to answer it, and both are unsatisfying:

- **A node's built-in estimator (`estimatesmartfee`)** is convenient but
  **laggy and coarse.** It smooths over short-term mempool dynamics and doesn't
  reflect what a miner would assemble *right now* — so exactly when the mempool
  is moving fastest (a fee spike, a block boundary), it hands you a stale-low
  number, and a too-low fee is what leaves a transaction stuck.
- **A third-party fee API** is accurate but requires **trusting someone else's
  view** of the network, and leaks which fees and transactions you care about to
  a provider that sees every request.

The accurate, trustless way is to **simulate the next blocks from the *live*
mempool the way a miner does** — rank transactions by their real, CPFP-aware
effective fee rate, pack them into projected blocks under the real weight limit,
and read the fee tiers off the boundaries.

### How Satya differs from mempool.space

Block-template fee estimation isn't exotic: it's the same logic Bitcoin Core runs
in `getblocktemplate` to assemble a block, applied to fee estimation instead of
mining. [mempool.space](https://mempool.space) is the best-known service built on
it, so it's the natural point of comparison — and it's a useful cross-check for
the open-source `rust-gbt` version of the algorithm.

The difference is **scope**. mempool.space is a full **block explorer** — a whole
platform — where fee estimation is one small organ inside a much larger organism:

| mempool.space (the platform)                     | Satya                              |
|--------------------------------------------------|------------------------------------|
| Block + address explorer, analytics, mining dashboards, Lightning, RBF/accelerator tooling | Just the fee estimator |
| A backend database, Redis, and multiple node backends | Nothing but your Bitcoin Core node |
| Many services to deploy and operate              | One static binary                  |
| A public site you can also self-host (heavy)     | Purpose-built to self-host (light) |

Satya extracts **only that one organ** — a live mempool plus a block-template
simulation, producing fee tiers — and throws the rest away:

- **Lightweight.** A single static binary. No database, no Redis, no explorer,
  no message bus. It talks to exactly one thing: your node's JSON-RPC endpoint.
- **Self-hosted and trustless.** Your node, your numbers. Nobody else sees which
  fees you query, and you never take a third party's word for the state of the
  network.
- **Purpose-built.** One job — the fee-estimation core — done well, rather than
  a general-purpose explorer you have to operate.

This is the reason the repo exists. It is not a clone; it is the *subtraction* of
everything that isn't the fee number — the same core idea, distilled to a tool you
can run yourself in one process.

---

## System design

Satya is a single async [Tokio](https://tokio.rs) process with two moving parts
around one piece of shared state:

```
  Bitcoin Core ──RPC──►  sync task  ──writes──►  Arc<RwLock<MempoolState>>  ──reads──►  axum HTTP
   (poll ~2s + ZMQ)     (supervised)            (single writer, many readers)          (/health)
```

**Single writer, many readers.** The sync loop is the *only* thing that mutates
state; HTTP handlers only ever read. This is the central simplification of the
whole design:

- Concurrency is trivial to reason about — there is exactly one writer, so there
  are no writer/writer races to think about, only a plain `RwLock`.
- **No lock is ever held across an `.await` or an RPC call.** The sync loop does
  all its slow work (RPC round-trips, fetches) with no lock held, then takes the
  write lock only to apply the finished result. A slow or unreachable node can
  never block a `/health` reader.

**Supervised, fail-fast.** The sync loop runs as a dedicated background task. It
is an infinite loop that only ends via panic; if it ever does, the process
*exits* (so a supervisor — systemd, Docker `restart:` — restarts it) rather than
silently freezing while still cheerfully serving `/health`. A frozen-but-healthy
process is worse than a clean crash.

### Components

| Module          | Responsibility                                                          |
|-----------------|-------------------------------------------------------------------------|
| `config`        | Parse configuration from environment variables / CLI flags.             |
| `rpc`           | Async JSON-RPC client over `reqwest` (auth, timeouts, body caps, calls).|
| `mempool`       | The `MempoolTx` / `MempoolState` model and the diff/apply logic.        |
| `sync/mod`      | The poll loop: cold-load, steady-state diff, restart guard, budget bail.|
| `sync/decision` | The **pure** desync/backlog/cooldown logic — no I/O, unit-tested.       |
| `http`          | The axum router and the `/health` handler.                              |
| `zmq`           | Optional `zmqpubhashblock` listener that wakes the loop on a new block. |
| `main`          | Wire config → shared state → sync task → HTTP server, with supervision. |

The rest of this document walks through the three pieces that make Satya work:
**building the mempool** and **the node transport** (both shipped today), then
**simulating the next blocks to estimate fees** (the next thing to build).

---

## Building the mempool

This is the heart of the running system, and everything downstream stands on it.
The goal: keep an in-memory `{ txid → tx }` cache that faithfully tracks the
node's mempool, cheaply, without ever lying about how fresh it is.

The naive approach — re-download the whole mempool every couple of seconds — is a
non-starter: a busy mempool is 100+ MB, and pulling it every 2 s is wasteful and
slow. The trick is that the mempool *changes* slowly relative to its size, so we
sync the **delta**, not the whole thing.

### One steady-state tick

```text
  wake: every POLL_INTERVAL_MS, or immediately on a ZMQ block
        |
        v
  getmempoolinfo ......... loaded? mempool min-fee
        |
        v
  getrawmempool(false) ... the node's current txid set  (cheap: just IDs)
        |
        v
  decision core (pure) ... node not loaded, or >80% of the cache vanished?
        |                  (mass-drop / restart guard, 60s cooldown)
        |
        +---- yes ---->  bulk resync from scratch   (or wait out the cooldown)
        |
        +---- no  ---->  diff( cache_keys , node_txids )
                              |
                              +-- gone:  remove from cache immediately
                              |          (the id list alone proves they left; no fetch)
                              |
                              +-- new:   fetch getmempoolentry
                                         (bounded concurrency, capped, time-budgeted)
        |
        v
  apply inserts + min-fee + tip; set caught_up = true ONLY if the tick fully reconciled
```

### Cold start

Wait until the node reports its mempool has finished loading
(`getmempoolinfo.loaded` — older nodes that don't report it are treated as
loaded), then do a **single bulk seed**: `getrawmempool true` returns the entire
mempool verbose in one shot. That populates the whole cache, and only then is the
state marked `caught_up`.

### Steady state

Every tick issues the cheap `getrawmempool false` — a bare **txid list** — and
diffs it against the cache's keys (`compute_diff`):

- **Removals are free.** A txid the node no longer lists has left the mempool
  (mined or evicted); the id list alone proves it. We drop it from the cache
  **immediately**, with no fetch, and departed txs never wait on the (possibly
  failing) fetch of new ones.
- **New txs are fetched.** Only newly-seen txids need a `getmempoolentry` detail
  fetch — the small per-tick delta, not the whole mempool. These fetches run with
  **bounded concurrency** (`buffer_unordered(FETCH_CONCURRENCY)`, default 10),
  best-effort (one failed fetch never aborts the batch), and capped at
  **`MAX_NEW_FETCH_PER_TICK` = 2000** so an unbounded or malicious mempool can't
  force one tick to issue hundreds of thousands of RPCs.
- **Time-budget bail.** If a tick's fetching runs past `TICK_BUDGET_MS`, it
  stops. Before stopping it *drains any already-ready results* (`now_or_never`)
  so finished work isn't thrown away, then drops the in-flight futures — and
  because the transport is truly async, dropping a request future cancels it, so
  no orphaned work leaks. Anything not fetched (capped, bailed, or errored)
  simply reappears in the next tick's `new` set. Nothing is ever permanently
  lost; it's just spread across ticks.

### Safety rail — the mass-drop / restart guard

A node restart is the dangerous case: for a moment the node reports an **empty**
mempool, and a naive diff would gleefully delete the entire cache and then
re-download it. Satya guards against this with pure, unit-tested logic in
`sync/decision.rs`:

- `is_mass_drop(cache_len, node_txid_count)` — true only when the cache is large
  enough to bother checking (≥ 100 txs) **and** the node now reports **less than
  1/5** of what we hold (i.e. **> 80 % vanished** in one tick). A small mempool
  legitimately shrinking is ignored; a real drop is not.
- `decide_desync(loaded, cache_len, node_txids, cooling_down)` — the single
  decision point. If the node isn't loaded, or a mass drop is detected, it calls
  for a **bulk resync** from scratch rather than an eviction storm — but subject
  to a **60 s cooldown** (`resync_cooling_down`) so a node that *flaps* can't
  force a full verbose download on every tick. While cooling down it marks the
  state stale and waits it out.

Because this logic is pure — no locks, no I/O, no async — it is the one part of
the system that is directly unit-tested (the loop around it is just I/O).

### The honesty invariant

**`caught_up` is `true` only when a tick fully reconciled the node's set** — no
backlog over the cap, no fetch errors, no budget bail, no RPC failure. Any of
those flips it `false`. This is a first-class design principle, not a detail:
**the system never lies about freshness.** When it isn't sure, it says so, and
`/health` exposes the evidence:

- `caught_up` — the boolean claim.
- `last_sync_ok` — Unix seconds of the last *fully successful* sync, or `null` if
  it has never synced. Lets a consumer distinguish "never synced" from "synced
  N seconds ago but currently degraded".
- `age_secs` — seconds since `last_sync_ok`; a direct freshness signal.

A fee number is only as trustworthy as the mempool it was computed from, so a
consumer must be able to tell a fresh mempool from a stale one. Satya makes that
non-negotiable.

### Latency — ZMQ block-push

Fees swing most **at block boundaries**: the instant a block confirms, a batch of
transactions leaves the mempool and the fee floor for "next block" resets. Waiting
out the poll interval to notice is the worst possible lag at the worst possible
moment.

So Satya optionally subscribes to the node's **`zmqpubhashblock`** publisher
(`BTC_ZMQ_BLOCK`, e.g. `tcp://127.0.0.1:28332`). A new block hash wakes the sync
loop for an **immediate** tick instead of waiting for the next poll. The listener
is an accelerator, never load-bearing: polling remains the baseline, and any ZMQ
error just logs and reconnects with backoff — a dead socket never takes the
process down. Wakes are debounced through a capacity-1 channel, so a burst of
events collapses to a single pending tick.

ZMQ is a raw socket on the node, so this path assumes a **local node** — which is
the intended production deployment anyway. A local node plus ZMQ is the lowest
latency possible: the estimate tracks reality instead of trailing it.

---

## Talking to the node: the JSON-RPC transport

Satya's RPC layer is a **hand-rolled async JSON-RPC client over `reqwest`**. We
deliberately dropped the blocking `bitcoincore-rpc` client so the *entire* system
is one async runtime — which is what makes true request cancellation (the
budget-bail above) possible, since dropping a `reqwest` future actually cancels
the in-flight request.

It's small on purpose, and every design choice is about being **DRY** and safe
against an **untrusted endpoint** (a remote provider is fully supported, so the
transport treats the far side as hostile).

- **One generic path.** Every typed method (`getblockchaininfo`,
  `getmempoolinfo`, `getrawmempool`, `getmempoolentry`) funnels through a single
  `call<T>` that builds the request, applies auth, sends it, and parses the
  `{ result, error }` envelope exactly once. There is one place to get the
  request/parse logic right.

- **Streaming, tiered body caps.** `resp.bytes()` would materialize an entire
  response with no bound, so an untrusted provider could OOM us. Instead `call`
  reads the body as a **bounded stream** (checking `Content-Length` preflight
  *and* enforcing the cap mid-stream, since Content-Length can lie), with three
  tiers matched to expected response sizes:

  | Tier             | Cap      | Used for                                   |
  |------------------|----------|--------------------------------------------|
  | control-plane    | 16 MiB   | `getblockchaininfo`, `getmempoolinfo`, one `getmempoolentry` |
  | txid list        | 64 MiB   | `getrawmempool false` (bare id list)       |
  | verbose          | 512 MiB  | `getrawmempool true` (full verbose dump)   |

- **Error classification.** Bitcoin Core returns *method* errors as HTTP **500
  with a valid JSON-RPC error body** (e.g. `getmempoolentry` on a vanished tx,
  code **`-5`**). So `call` parses the envelope **regardless of HTTP status**,
  and only treats the status itself as the error when the body isn't a parseable
  envelope (a genuine gateway/WAF failure). Code `-5` is mapped to a clean "tx
  vanished" → `Ok(None)` so the fetch loop treats it as "left the mempool", not a
  hard error. 401/403 become a distinct reconnectable `Auth` error (rebuild the
  client, re-read a possibly-rotated cookie, retry).

- **Flexible auth.** Cookie file, `user`/`pass`, or **none** (when the endpoint
  carries its credential in the URL, like a hosted provider's API key in the
  path). Cookie takes precedence and is re-read on reconnect, so a node restart
  that rotates the cookie doesn't wedge the loop on 401 forever.

- **Provider-hardening.** `redirect(none)` — an untrusted provider must not be
  able to redirect us (and our credentials) to another host. Every transport
  error is built with `without_url()`, because `reqwest` does **not** redact a
  path-embedded token (`https://host/<KEY>`) — so the token never reaches the
  logs. Error strings are length-truncated on top of the body cap.

- **Per-request timeout + cancel-on-drop.** The timeout is baked into the client;
  dropping a call cancels it. No custom headers are added.

- **Exact-integer money.** Core reports fees as BTC decimals; they're parsed via
  serde's `as_btc` straight into the `bitcoin` crate's integer-sat `Amount`
  (exact for every real sat value — ≤ 21M BTC fits an f64 mantissa). Fees are
  **never** floating point. The only float in the system is the *fee rate*
  (`mempool_min_fee_sat_vb`), where a rate is the appropriate representation.

The `reqwest` client is internally connection-pooled (keep-alive), so a clone is
cheap and concurrent fetches reuse connections rather than reopening one per
call.

---

## Estimating fees by simulating the next blocks *(the next thing to build)*

This is the destination — the fee number is the whole point of the project — and
the piece that is **not yet implemented**. Everything above exists to feed it a
fresh, complete, honestly-aged mempool; this section specifies the algorithm it
will run. It is described as a design, not as shipped code.

### Are we simulating mining?

Yes — but only the *selection* half of mining, not the *proof-of-work* half.

Real mining is two separate jobs. First a miner **selects** which mempool
transactions to put in the candidate block (the fee-maximizing part). Then it
**grinds** trillions of hashes searching for one below the difficulty target (the
energy-burning, probabilistic part) to actually win the block.

Satya reproduces **only the first job** — the deterministic block *assembly* that
Bitcoin Core exposes as `getblocktemplate` (GBT). We are not hashing, not
competing for a block reward, and not broadcasting anything: we build the same
candidate block a rational miner *would* build from the current mempool, purely to
read the fee floor off it. It is a dry run of block construction, not of mining
economics. (This is exactly why the reference name is "GBT" — get *block
template*.)

### The question

*What fee rate confirms in ~N blocks?* Answer it the way the network actually
answers it: **simulate what a rational miner would select.** A miner building the
next candidate block picks the transactions that maximize the fees it collects,
under a hard size constraint — so if we assemble the same block from the live
mempool, its lowest-fee-rate transaction tells us the price of getting into the
next block.

### The constraint

A block is at most **4,000,000 weight units** (≈ 1,000,000 vB), minus the space
the coinbase takes. Within that budget a miner maximizes total fees.

### Why naive feerate sorting is wrong: dependencies

You cannot just sort every transaction by its own fee rate and fill greedily,
because transactions have **unconfirmed ancestors**:

- A **child can't be mined without its parents.** If a high-fee child depends on
  an unconfirmed parent, the parent has to come first.
- **CPFP** (child-pays-for-parent): a low-fee parent with a high-fee child is
  attractive *as a package* even though the parent alone looks unminable. A miner
  pulls them in together.

So the ranking key is not standalone fee rate but **ancestor / package fee
rate**:

```
                   tx.fee + fees of all its unconfirmed ancestors
  package_feerate = ──────────────────────────────────────────────
                   tx.vsize + vsizes of all its unconfirmed ancestors
```

This is exactly the quantity Bitcoin Core's `CreateNewBlock` and mempool.space's
`rust-gbt` rank by.

### The greedy package algorithm

Repeatedly take the transaction with the best **effective (ancestor) fee rate**,
and add it *together with any of its ancestors not yet included*, if the whole
package fits in the remaining block weight. Then **update** the remaining
transactions' effective fee rates — ancestors that are now included no longer
count toward anyone's package cost, which can raise a formerly-cheap child's
effective rate. Continue until the block is full; then keep going with the
leftover mempool to fill the *next* projected block, and the next, and so on.

```
  weight
  limit ┤ ██████ block 1  (highest package feerates)      ─► "next block" tier
        │ ▒▒▒▒▒▒ block 2                                        boundary feerate
        │ ▓▓▓▓▓▓ block 3  ─► "~30 min" tier
        │ ░░░░░░ ...
        │ ······ block 6  ─► "~1 hour" tier
        └────────────────────────────► descending package feerate

  Each projected block is filled by descending package feerate; the feerate at a
  block's lower boundary is the price of landing in (or before) that block.
```

Each projected block therefore has a **boundary fee rate** at its bottom edge:
the cheapest package that still made it into that block.

### From projected blocks to fee tiers

The tiers are read off the projected-block boundaries, with light smoothing and a
**1 sat/vB floor**:

| Tier            | Source                                                      |
|-----------------|------------------------------------------------------------|
| next block / fastest | boundary fee rate of **block 1**                      |
| ~30 min         | boundary of **~block 3**                                    |
| ~1 hour         | boundary of **~block 6**                                    |
| economy         | near the mempool's min-fee floor                           |
| minimum         | the mempool **min relay fee** (`mempoolminfee`)            |

### Why the sync layer already feeds this

The mempool builder was designed with this algorithm in mind:

- **`MempoolTx` already captures package data.** Each cached tx carries
  `ancestor_fee`/`ancestor_vsize` and `descendant_fee`/`descendant_vsize`,
  pulled from `getmempoolentry` — exactly the ancestor totals the greedy packing
  needs. (One caveat the code already documents: those totals are a *snapshot*
  taken at fetch time and can drift as related txs come and go, so the estimator
  must **recompute** package feerates from a fresh source rather than trust the
  cached snapshot as live.)
- **It should run incrementally.** `rust-gbt` calls this an *audit*: rather than
  rebuild every block template from scratch, re-derive it against each mempool
  delta. Fed by the fresh sync loop and ZMQ block-push, the fee number tracks
  reality with minimal latency — which is the entire reason the sync layer works
  as hard as it does on freshness.

The `/fees` endpoint that exposes these tiers will be gated on `caught_up`, so it
never serves a number computed from a mempool it can't vouch for.

---

## Why your own node (and how it shapes accuracy)

You *can* run Satya against a remote provider, and it works. But a node you
control is strictly better, and the reason is the fee algorithm itself: **a fee
estimate is only ever as good as the mempool it's computed from.** Two properties
of that mempool bound the accuracy:

- **Completeness.** Missing transactions make the simulation under-count
  congestion, so it estimates fees **too low** — and a too-low fee is exactly
  what leaves a transaction stuck. A local node holds the full mempool up to its
  `maxmempool`; a rate-limited, latency-bound remote fetch can be incomplete (and
  Satya then honestly reports `caught_up=false` rather than vouch for a partial
  view). A small `maxmempool` also evicts the lowest-fee transactions and
  **truncates the bottom of the fee distribution**, which is what skews the
  economy tier.

- **Freshness.** The estimate reflects the mempool **at fetch time**, and it goes
  stale **worst exactly when it matters most** — during a fee spike or right at a
  block boundary, when the mempool moves fastest. Remote latency or an un-synced
  node makes that snapshot lag. A local node plus ZMQ block-push gives the lowest
  possible latency.

- **Trust and privacy.** Your numbers are computed locally, from your node's
  mempool. You don't trust a third party's view of the network, and nobody sees
  which fees or transactions you care about.

Remote providers trade away freshness and trust — precisely the two things the
fee estimate is built on. Fine for a look; not how you'd run it for real.

---

## Deployment

Satya is a single static binary that talks to **one** thing: a Bitcoin Core
JSON-RPC endpoint. That endpoint is the only dependency you must supply.

### Dependencies

| Dependency | Required? | How to get it |
|------------|-----------|---------------|
| **Rust** (stable) | to build from source | [rustup](https://rustup.rs): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh`. Builds on latest stable (developed against 1.93). Not needed if you deploy the Docker image. |
| **A Bitcoin RPC endpoint** | **yes** — the one hard dependency | **Run your own node** ([Bitcoin Core](https://bitcoincore.org/en/download/), verified against v29) — regtest/signet/mainnet, see the paths below — **or** point at a **remote provider** (GetBlock, QuickNode, …) with the key in the URL. |
| [**`just`**](https://github.com/casey/just) | optional | `cargo install just`. Task runner for the `just <recipe>` shortcuts below; each recipe is a thin wrapper over the underlying `cargo`/`curl`/`bitcoin-cli` command. |
| [**Docker**](https://docs.docker.com/get-docker/) | optional | Only for the container path (`just docker` / `docker compose up`). |
| [**`jq`**](https://jqlang.github.io/jq/) | optional | Pretty-prints `/health` in `just health`. Any JSON tool (or none) works. |

You need **exactly one** RPC endpoint. Pick the path that matches your goal:

| Path | Endpoint | Disk | Real fees? | Use it for |
|------|----------|------|-----------|------------|
| **A — regtest** | local `bitcoind -regtest` | ~0 | no (you mint blocks) | fastest smoke test of the mechanics |
| **B — signet** | local `bitcoind -signet` | a few GB | no (test-net levels) | realistic relay on a real (test) mempool |
| **C — mainnet node** | your own node | ~15 GB+ | yes | **production** |
| **D — remote provider** | `https://provider/<key>` | 0 | yes but degraded | zero-infra look, works today |

#### Path A — Fastest test (regtest, zero disk)

Regtest is a private chain you mine yourself: no download, no sync, disposable.
It proves the sync loop / `/health` plumbing works, but the fees are meaningless
(you decide when blocks happen) — **mechanics only, not real fees.**

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

#### Path B — Realistic test (signet)

Signet is a stable, low-volume public test network with **real relay** and a real
(if small) mempool. It syncs in minutes and a few GB, exercising Satya against
genuine mempool dynamics — but signet fee levels are **not mainnet fee levels**,
so treat the numbers as a functional check, not a production estimate.

`bitcoin.conf`:

```ini
signet=1
server=1
# blocksonly MUST stay off (default) so the mempool relays
```

Start `bitcoind`, wait for it to reach the tip, and run Satya against the signet
RPC port (**38332**), cookie under `signet/`:

```bash
BTC_RPC_URL=http://127.0.0.1:38332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/signet/.cookie" \
./target/release/satya
```

#### Path C — Production (your own mainnet node, low disk)

The real deployment: fees computed from **your** node's live mainnet mempool. You
need a **full, tip-synced, non-`blocksonly`** node — but not a heavy one:

```ini
server=1              # enable JSON-RPC
prune=550             # MB; smallest allowed. Mempool RPCs are unaffected by pruning.
maxmempool=300        # MB; larger = fuller mempool view = truer fees (see "Why your own node")
# txindex NOT set     # not needed — mempool RPCs never touch a tx index
# blocksonly MUST stay off (default) — it disables mempool relay -> useless fees
rpcthreads=10         # >= FETCH_CONCURRENCY so parallel fetches aren't serialized
# rpcworkqueue=16     # keep FETCH_CONCURRENCY <= this (default 16)
# zmqpubhashblock=tcp://0.0.0.0:28332   # optional: immediate recompute on new blocks
```

**No `txindex`. Pruning is fine.** The mempool lives in memory, independent of
block storage, so every RPC Satya uses works on a pruned node. It **must not** be
`blocksonly` (that kills mempool relay) and **must** be caught up to the tip (a
syncing node has an unrepresentative mempool).

Two cheap ways to stand up a usable node:

- **Pruned node** — set `prune=550` (or larger). Steady-state disk is small
  (**~15 GB**, including the ~11 GB unprunable chainstate), but you still pay a
  one-time **full ~600 GB IBD download** as the node validates the whole chain
  (the blocks are discarded as it goes; only the download is unavoidable).
- **`assumeutxo` snapshot** (Core 26+) — `loadtxoutset` a recent UTXO snapshot and
  the node is usable **in minutes**: it jumps to the snapshot height, serves
  mempool/fee RPCs immediately, and **background-validates** the rest of the chain
  behind you. Pair it with `prune=550` for a small, fast-to-stand-up node.

Either way, run Satya against the mainnet RPC (**8332**):

```bash
BTC_RPC_URL=http://127.0.0.1:8332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
./target/release/satya
```

#### Path D — No node (remote provider)

Zero local infrastructure: point `BTC_RPC_URL` at a hosted provider whose API key
is in the URL (no separate auth). Works today, fine for a quick look.

```bash
BTC_RPC_URL=https://go.getblock.io/<YOUR_KEY> ./target/release/satya
```

But it is **degraded** for fee accuracy: each `getmempoolentry` is an
internet round-trip (not a sub-ms local call), you hit **rate limits**, you
**trust the provider's mempool**, and there is **no ZMQ** block-push. Satya stays
honest — when latency blows the tick budget it reports `caught_up=false` — but see
[Why your own node](#why-your-own-node-and-how-it-shapes-accuracy).

### Run it

**Binary:**

```bash
cargo build --release            # or: just release   (LTO'd; see [profile.release])

BTC_RPC_URL=... BTC_RPC_COOKIE_FILE=... ./target/release/satya
# or: copy .env.example -> .env and use `just run` (loads .env; defaults target Path A)

curl -s http://127.0.0.1:8080/health | jq      # or: just health
```

**Docker:** a multi-stage image builds the binary and ships it on a slim Debian
base with a `curl`-based `HEALTHCHECK` on `/health`.

```bash
just docker                       # == docker compose up --build
```

`docker-compose.yml` reaches a node on the Docker **host** by default
(`http://host.docker.internal:8332`) and mounts `${BITCOIN_DATADIR:-~/.bitcoin}`
read-only so the container can read the `.cookie`. Override `BTC_RPC_URL` /
`BITCOIN_DATADIR` (or swap to `BTC_RPC_USER` / `BTC_RPC_PASS`) via the environment
or a `.env` file. `restart: unless-stopped` pairs with the app's fail-fast
design: if the sync task dies the process exits and the container restarts.

### Configuration

All configuration is via environment variables (equivalent `--kebab-case` CLI
flags also work). Only the RPC connection is required; everything else has a
default.

| Variable                | Default                 | Meaning                                          |
|-------------------------|-------------------------|--------------------------------------------------|
| `BTC_RPC_URL`           | `http://127.0.0.1:8332` | Bitcoin Core JSON-RPC URL (`http://` local or `https://` provider). |
| `BTC_RPC_COOKIE_FILE`   | —                       | Path to the node's `.cookie` file.               |
| `BTC_RPC_USER` / `_PASS`| —                       | RPC username / password (used together).         |
| `RPC_TIMEOUT_SECS`      | `30`                    | Timeout for each Bitcoin Core RPC call, seconds. |
| `HTTP_BIND`             | `127.0.0.1:8080`        | Address the HTTP server binds to.                |
| `POLL_INTERVAL_MS`      | `2000`                  | Mempool poll interval, in milliseconds.          |
| `FETCH_CONCURRENCY`     | `10`                    | Max concurrent `getmempoolentry` calls per tick. Bound by node `rpcthreads`/`rpcworkqueue`. |
| `BTC_ZMQ_BLOCK`         | —                       | Node `zmqpubhashblock` endpoint for immediate recompute on new blocks. Unset = polling only. |
| `TICK_BUDGET_MS`        | `2 × POLL_INTERVAL_MS`  | Max fetch time per tick before bailing and marking stale (floored at `POLL_INTERVAL_MS`). |

**Authentication:** provide `BTC_RPC_COOKIE_FILE`, **or** both `BTC_RPC_USER` and
`BTC_RPC_PASS`, **or neither** — omit auth when the endpoint carries its
credential in the URL (hosted providers like GetBlock,
`https://go.getblock.io/<KEY>`). A cookie file takes precedence and is re-read on
reconnect; it's the usual choice for a local node (Core writes it to its data
directory, e.g. `~/.bitcoin/.cookie` on mainnet).

### `/health` fields

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

| Field                    | Type           | Meaning                                                                 |
|--------------------------|----------------|-------------------------------------------------------------------------|
| `caught_up`              | bool           | `true` only after a recent fully-successful sync; `false` before the first sync, while resyncing, or during any node/RPC outage. |
| `mempool_size`           | number         | Transactions currently held in the in-memory mempool.                   |
| `tip_height`             | number         | The node's chain tip height as last seen.                               |
| `mempool_min_fee_sat_vb` | number         | The node's current mempool min fee, in sat/vB (the eviction floor).     |
| `network`                | string         | Network inferred from the node (`bitcoin`, `testnet`, `signet`, `regtest`) — no network flag to misconfigure. |
| `last_sync_ok`           | number \| null | Unix seconds of the last successful sync, or `null` if never synced.    |
| `age_secs`               | number \| null | Seconds since `last_sync_ok`; `null` if never synced. A freshness signal for consumers. |

### Logging

Logging is structured (via `tracing`) and follows `RUST_LOG` (defaults to
`info`). By design the sync loop is **silent when healthy** — it logs sync-state
*transitions* (e.g. "mempool in sync" / "mempool out of sync") and lifecycle
events, not one line per tick, so `info` stays readable in production.
Control-plane RPC errors surface at `warn` so a real problem is visible without
turning on debug logging.

| I want to see…                        | Set                                          |
|---------------------------------------|----------------------------------------------|
| Per-tx fetch failures + desync detail | `RUST_LOG=info,satya::sync=debug`            |
| HTTP request access log               | on at `info` by default (method/path/status/latency) |
| …but silence the frequent `/health`   | `RUST_LOG=info,tower_http::trace=warn`       |

HTTP requests are logged via [`tower-http`](https://docs.rs/tower-http)'s
`TraceLayer`, one INFO line per request with method, path, status, and latency.

---

## Engineering notes

The design principles the code holds itself to:

- **Money is integer satoshis, never floating point.** Fees flow from Core's BTC
  decimals straight into the `bitcoin` crate's `Amount`; the only float is a fee
  *rate*, where a rate belongs.
- **No lock across an `.await`.** All slow work happens lock-free; the write lock
  is taken only to apply a finished result, so a slow node never blocks readers.
- **Honesty over optimism.** `caught_up` is `true` only when a tick fully
  reconciled the node. When unsure, the system says "not sure" — it never vouches
  for a mempool it can't stand behind.
- **Bound everything; assume the far side is hostile.** Per-tick fetch cap,
  per-tick time budget, tiered response-body caps, no redirects, tokens stripped
  from logs, resync cooldown — an untrusted or misbehaving provider can't OOM,
  flood, or wedge the process.
- **Delete over reuse.** Satya is defined by what it *doesn't* include. The whole
  project is the fee-estimation core with the explorer, database, and services
  subtracted; the transport dropped a blocking RPC crate to become a small async
  `call<T>`.
- **Test the reasoning, not the plumbing.** The first unit tests cover the pure
  decision core (`sync/decision.rs`) — mass-drop, cooldown, backlog, desync
  routing — because that's where the subtle correctness lives; the loop around it
  is I/O.

## License

See [LICENSE](LICENSE) if present; otherwise this is currently unlicensed /
private.
