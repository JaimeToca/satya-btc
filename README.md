<p align="center">
  <img alt="Satya" src="assets/logo/satya-lockup.svg" width="440">
</p>

> **Satya** (Sanskrit: *truth*) — the true, live fee your own node sees.

Satya is a single, self-hosted binary that turns your Bitcoin Core node's live
mempool into honest fee recommendations. It keeps an always-current in-memory
copy of the mempool, synced directly from your node, and simulates the next few
blocks the way a miner would to read fee tiers off that simulation. One static
binary. No database, no Redis, no explorer.

## Contents

- [Why Satya exists](#why-satya-exists)
- [System design](#system-design)
  - [Building the mempool](#building-the-mempool)
  - [Estimating fees by simulating the next blocks](#estimating-fees-by-simulating-the-next-blocks)
  - [What bounds the accuracy of the estimate](#what-bounds-the-accuracy-of-the-estimate)
- [Deployment](#deployment)
  - [Testing without a Bitcoin node](#testing-without-a-bitcoin-node)
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
- **A third-party fee API** is accurate but costs you **trust and privacy.** You
  take a provider's word for the state of the network instead of verifying it
  yourself, and every request tells them which transactions and fee levels you
  care about — a live feed of your intentions, and a single point of failure the
  moment their view is wrong, throttled, or offline.

The accurate, trustless way is to **simulate the next blocks from the *live*
mempool the way a miner does** — rank transactions by their real, CPFP-aware
effective fee rate, pack them into projected blocks under the real weight limit,
and read the fee tiers off the boundaries.

Satya does exactly this and nothing else: it runs the **fee-maximizing half of
block assembly** — the Core-style ancestor-package selection Bitcoin Core's
`CreateNewBlock` / `getblocktemplate` uses to decide *which* transactions a
rational miner would include — and reads the fee tiers off the result, in a single
static binary that talks to nothing but your own node. Full explorers like
[mempool.space](https://mempool.space) also surface a fee estimate, but bundle it
inside a whole platform (analytics, mining dashboards, Lightning, a database,
Redis, multiple node backends). Satya keeps the estimator and drops the rest of
the explorer, so what you run and operate is just the fee engine — self-hosted,
private, and trivial to deploy.

Being small also lets it be built the way a piece of money infrastructure should
be:

- **Written in Rust.** Memory-safe with no garbage-collector pauses, and fees are
  kept as exact integer satoshis end-to-end — never floating point — so there's no
  rounding drift in the numbers you act on.
- **One static binary, zero services.** No database, no Redis, no message broker,
  no runtime to install. It's a single async [Tokio](https://tokio.rs) process you
  drop next to your node; deploy it as a lone executable or a tiny container.
- **Cheap and low-footprint.** Because it holds only the live mempool in memory and
  talks to nothing but your node, it runs comfortably on the same modest,
  low-disk (pruned / `assumeutxo`) box as the node itself.
- **Hardened and honest by design.** The RPC transport is bounded against an
  untrusted endpoint (streaming body caps, timeouts, cancel-on-drop, no redirects,
  no token leaks), and the system refuses to lie about freshness — `/health`
  reports `caught_up=false` rather than serve a stale number as if it were live.

---

## System design

Satya is a single async [Tokio](https://tokio.rs) process with two moving parts
around one piece of shared state:

```
  Bitcoin Core ──RPC──►  sync task  ──writes──►  Arc<RwLock<MempoolState>>  ──reads──►  axum HTTP
   (poll ~2s + ZMQ)     (supervised)            (single writer, many readers)          (/health)
```

<p align="center">
  <img alt="Satya turns your node's live mempool into honest fee tiers by simulating the next blocks a miner would build" src="assets/satya-demo.gif" width="760">
</p>

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

The subsections below cover the three parts of that design in turn: **building the
mempool** (the in-memory view of the node), **estimating fees** (the block
simulation that reads tiers off that mempool), and **what bounds the accuracy** of
the result.

### Building the mempool

This is the heart of the running system, and everything downstream stands on it.
The goal: keep an in-memory `{ txid → tx }` cache that faithfully tracks the
node's mempool, cheaply, without ever lying about how fresh it is.

The naive approach — re-download the whole mempool every couple of seconds — is a
non-starter: a busy mempool is 100+ MB, and pulling it every 2 s is wasteful and
slow. The trick is that the mempool *changes* slowly relative to its size, so we
sync the **delta**, not the whole thing.

#### One steady-state tick

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

#### Cold start

Wait until the node reports its mempool has finished loading
(`getmempoolinfo.loaded` — older nodes that don't report it are treated as
loaded), then do a **single bulk seed**: `getrawmempool true` returns the entire
mempool verbose in one shot. That populates the whole cache, and only then is the
state marked `caught_up`.

#### Steady state

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

#### Safety rail — the mass-drop / restart guard

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

#### The honesty invariant

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

#### Latency — ZMQ block-push

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

### Estimating fees by simulating the next blocks

This is the destination — the fee number is the whole point of the project. The
mempool builder above exists to feed it a fresh, complete, honestly-aged mempool;
this section describes the algorithm that turns that mempool into fee tiers.

#### Are we simulating mining?

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

#### The question

*What fee rate confirms in ~N blocks?* Answer it the way the network actually
answers it: **simulate what a rational miner would select.** A miner building the
next candidate block picks the transactions that maximize the fees it collects,
under a hard size constraint — so if we assemble the same blocks from the live
mempool, the resulting selection tells us the price of getting into the next
block (and the ones after it; see [below](#from-projected-blocks-to-fee-tiers)
for how that price is actually read off the simulation).

#### The constraint

A block is at most **4,000,000 weight units** — weight counts base (non-witness)
bytes ×4 and witness bytes ×1, and vsize is just `weight / 4`, so the cap is
≈1,000,000 vB — minus a small reserve for the coinbase transaction. Within that
budget a miner maximizes total fees. Satya ranks transactions in fee-per-vByte but
packs against the remaining **weight**, exactly as Core does.

This is a deliberate simplification of full block assembly: it models the
fee-maximizing ancestor-package selection, and does **not** model sigop limits,
per-package descendant limits, or RBF conflict replacement. Those rarely move the
tier boundaries, but they're the reason to call this "Core-style selection"
rather than a byte-exact `getblocktemplate`.

#### Why naive feerate sorting is wrong: dependencies

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

This is the same **ancestor score** Bitcoin Core's `CreateNewBlock` ranks by when
it assembles a block.

#### The greedy package algorithm

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
        │ ▒▒▒▒▒▒ block 2
        │ ▓▓▓▓▓▓ block 3  ─► "~30 min" tier
        │ ░░░░░░ ...
        │ ······ block 6  ─► "~1 hour" tier
        └────────────────────────────► descending package feerate

  Each projected block is filled by descending package feerate. The fee tiers are
  not read off any single block's edge, but off a cumulative-weight histogram of
  these effective rates (see below).
```

Packing this way also assigns every transaction a **CPFP-effective fee rate** — a
package member inherits its package's combined rate. Those per-transaction
effective rates, not any single block's bottom edge, are what the fee tiers are
read from (next section).

#### From projected blocks to fee tiers

A tier answers "what fee confirms within ~N blocks?" You might read it off the
Nth projected block's cheapest transaction — but that isn't reliable: greedy
assembly fills the tail of each block with small, low-fee **gap-filler** txs that
happen to fit the leftover weight, so an early block's *minimum* rate can dip
below a later block's. Reading a single block's bottom edge would make "next
block" report one of those gap-filler outliers — a too-low number, exactly the
failure this project exists to avoid.

Instead the tiers are read off a **weight histogram of effective fee rates**: take
every transaction's CPFP-effective rate paired with its weight, sort by rate
(highest first), and walk down accumulating weight. The fee to confirm within N
blocks is the rate at which cumulative weight first reaches **N × 4,000,000 WU**
(N blocks' worth). This is **monotone by construction** — deeper tiers can only be
cheaper — and immune to gap-filler outliers. If the mempool holds less than N
blocks of weight, anything at the relay floor confirms, so that tier is the floor.
The time labels assume the ~10-minute average block interval; they are
expectations, not guarantees (a real block can take much longer):

| Tier            | Source                                                              |
|-----------------|----------------------------------------------------------------------|
| next block / fastest | rate at **1 block** of cumulative weight                      |
| ~30 min         | rate at **3 blocks** of cumulative weight                          |
| ~1 hour         | rate at **6 blocks** of cumulative weight                         |
| economy         | rate at the **projection horizon** (`MAX_BLOCKS` blocks) — the cheapest still expected to confirm in the current backlog |
| minimum         | the mempool **min relay fee** (`mempoolminfee`) — the floor below which the node won't even accept the tx |

#### Why the sync layer feeds this cleanly

The mempool builder was designed with this algorithm in mind:

- **`MempoolTx` captures the package data.** Each cached tx carries
  `ancestor_fee`/`ancestor_vsize` and `descendant_fee`/`descendant_vsize`,
  pulled from `getmempoolentry` — exactly the ancestor totals the greedy packing
  needs. (One caveat: those totals are a *snapshot* taken at fetch time and can
  drift as related txs come and go, so the estimator **recomputes** package
  feerates from the live cache rather than trusting the cached snapshot as final.)
- **It recomputes from a fresh snapshot.** Rather than maintain a second,
  long-lived copy of the mempool, the estimator rebuilds its working set from the
  live cache on each run and re-derives the projection from scratch — so there is
  no separate structure to keep in sync and no stale package data to carry
  forward. The recompute runs off the async path (on a blocking thread) and is
  throttled (`FEE_RECOMPUTE_MIN_INTERVAL_MS`, default 5s), so a fast-churning
  mempool can't spin it. Fed by the fresh sync loop and ZMQ block-push, the fee
  number still tracks reality with minimal latency.

The `/fees` endpoint that exposes these tiers is gated on `caught_up`, so it never
serves a number computed from a mempool it can't vouch for.

### What bounds the accuracy of the estimate

A fee estimate is only ever as good as the mempool it's computed from, and two
properties of that mempool set the ceiling on how accurate Satya can be. They come
from two different places — one is Satya's job, the other is the node operator's.

- **Completeness (a node-config property).** Missing transactions make the
  simulation under-count congestion, so it estimates fees **too low** — and a
  too-low fee is exactly what leaves a transaction stuck. This is bounded by the
  node, not by Satya: a node holds the full mempool only up to its `maxmempool`,
  and a small `maxmempool` evicts the lowest-fee transactions and **truncates the
  bottom of the fee distribution**, which is what skews the economy tier. Note
  `caught_up` does *not* catch this — it only asserts that Satya's cache matches
  *this node's* mempool set, so a truncated node can be fully `caught_up` and still
  give a skewed economy tier. Completeness is a matter of configuring the node
  right (see [Deployment](#deployment)), not something the sync layer can detect.

- **Freshness (Satya's job).** The estimate reflects the mempool **at fetch
  time**, and it goes stale **worst exactly when it matters most** — during a fee
  spike or right at a block boundary, when the mempool moves fastest. This is what
  the sync layer's honesty contract (`caught_up` / `last_sync_ok` / `age_secs`) and
  ZMQ block-push exist to protect: a local node plus ZMQ gives the lowest possible
  latency, and when the view *is* behind, `/health` says so rather than serving a
  stale number as fresh.

---

## Deployment

Satya is a single static binary that talks to **one** thing: a Bitcoin Core
JSON-RPC endpoint. That endpoint is the only dependency you must supply.

To build from source you need **Rust** (stable; developed against 1.93) — skip it
if you deploy the Docker image. Optional helpers: [`just`](https://github.com/casey/just)
for the `just <recipe>` shortcuts, [`jq`](https://jqlang.github.io/jq/) to
pretty-print `/health`, and [Docker](https://docs.docker.com/get-docker/) for the
container path. The one hard dependency is the RPC endpoint below.

### Run against your own node (recommended)

Satya is meant to run against a Bitcoin Core node **you control**, on mainnet. A
minimal, low-disk node is enough — it does **not** need to be a heavy archival
node:

```ini
server=1              # enable JSON-RPC
prune=550             # MB; smallest allowed. Mempool RPCs are unaffected by pruning.
maxmempool=300        # MB; larger = fuller mempool view = truer fees (see below)
# txindex NOT set     # not needed — mempool RPCs never touch a tx index
# blocksonly MUST stay off (default) — it disables mempool relay -> useless fees
rpcthreads=10         # >= FETCH_CONCURRENCY so parallel fetches aren't serialized
# rpcworkqueue=16     # keep FETCH_CONCURRENCY <= this (default 16)
zmqpubhashblock=tcp://127.0.0.1:28332   # recommended: immediate recompute on new blocks
```

The three rules that matter: it **must not** be `blocksonly` (that disables
mempool relay, so there'd be nothing to estimate from), it **must** be caught up
to the tip (a syncing node has an unrepresentative mempool), and a healthy
`maxmempool` keeps the low-fee tail intact (a small one truncates the bottom of
the fee distribution and skews the economy tier). **No `txindex` and pruning are
both fine** — the mempool lives in memory, independent of block storage, so every
RPC Satya uses works on a pruned node.

You don't need to wait out a full initial block download to get there. `prune=550`
plus an **`assumeutxo` snapshot** (Core 26+, via `loadtxoutset`; see the current
[Core assumeutxo documentation](https://github.com/bitcoin/bitcoin/blob/master/doc/assumeutxo.md))
gets a node serving
mempool/fee RPCs far sooner than a from-scratch sync: it jumps to the snapshot
height and background-validates the rest of the chain behind you. How fast "usable"
actually is depends on snapshot download, bandwidth, and peer connectivity — and
note the mempool itself starts empty and **fills from relay** once you have peers,
not from the snapshot, so give it a little time to populate before trusting the
tiers. Steady-state disk is on the order of ~15 GB, dominated by the unprunable
chainstate.

Run Satya against the mainnet RPC (**8332**) with cookie auth:

```bash
BTC_RPC_URL=http://127.0.0.1:8332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/.cookie" \
./target/release/satya
```

### Why not a remote RPC provider

> **Important:** a remote provider is fine for bring-up, but not for a fee
> estimate you'll act on — it's degraded on freshness, completeness, and trust,
> the three properties the estimate depends on. Run against your own node for
> production.

You *can* point `BTC_RPC_URL` at a hosted provider (GetBlock, QuickNode, …) with
the key in the URL — no local node needed — and it works. It's genuinely handy for
**bring-up**: a zero-infrastructure way to kick the tyres or run a quick functional
check. But for a fee estimate you'd actually act on, it is **degraded on exactly
the properties the estimate depends on** — and the reason is structural, not a
knock on any provider's quality.

**Satya's sync pattern is unusually latency-sensitive.** Each steady-state tick
issues `getmempoolinfo` and `getrawmempool false`, then **one `getmempoolentry`
per newly-seen transaction** (up to 2000/tick, `FETCH_CONCURRENCY` in flight). On a
local node every one of those is a sub-millisecond loopback call, so a full tick
finishes in a few ms. Against a remote provider each becomes an **internet
round-trip** (tens to hundreds of ms), so a busy tick — especially the refill burst
right after a block — can blow past `TICK_BUDGET_MS`. Satya stays honest and reports
`caught_up=false`, but your mempool view is now *lagging reality*.

That degradation lands on the two things the estimate is built on:

- **Freshness.** The lag is worst at exactly the wrong moment — a **fee spike or a
  block boundary**, when the mempool churns fastest and a good estimate matters
  most — so the number trails the market instead of tracking it. Two things make it
  worse: **rate limits** (the per-tx `getmempoolentry` fan-out is exactly what
  providers throttle; capped calls get retried next tick, falling further behind),
  and **no ZMQ block-push** over a remote socket (so you lose the immediate-on-block
  recompute and are back to polling). A local node has neither limit.
- **Completeness.** You inherit *their* node's mempool: you don't control its
  `maxmempool` and can't verify it isn't filtered or truncated, so a missing
  low-fee tail silently skews your economy tier. With your own node you *know* it's
  a full, tip-synced, relaying mempool.
- **Trust and privacy.** You'd be trusting the provider's view of the network
  instead of your own, and leaking which fees and transactions you care about to a
  party that sees every request.

A node you control is strictly better on all three counts, which — combined with how
cheap Satya is to self-host next to a pruned / `assumeutxo` node — is why it's the
recommended deployment.

> **Security note.** A provider key embedded in `BTC_RPC_URL`
> (`https://…/<KEY>`) is a credential. Keep it in `.env` (gitignored), not in
> committed files or shared logs — Satya already strips the URL from transport
> errors so the key can't leak that way — and rotate it if it's ever exposed.

### Testing on signet

To exercise Satya against a real (if small) mempool without touching mainnet, run
a local **signet** node — it syncs in minutes and a few GB:

```ini
signet=1
server=1
# blocksonly MUST stay off (default) so the mempool relays
```

Then point Satya at the signet RPC port (**38332**), cookie under the `signet/`
subdir:

```bash
BTC_RPC_URL=http://127.0.0.1:38332 \
BTC_RPC_COOKIE_FILE="$HOME/.bitcoin/signet/.cookie" \
./target/release/satya
```

This exercises the sync/transport/`/health` plumbing against genuine relay, but a
thin signet mempool rarely has enough backlog to fill multiple projected blocks, so
the fee *tiers* it produces are uninteresting by design — signet fee levels aren't
mainnet levels. Treat it as a functional check, not a production estimate.

### Testing without a Bitcoin node

You can exercise the entire indexer — the real `reqwest` client, sync loop, fee
engine, and HTTP API — against an offline fake node, with no Bitcoin Core. The
sim node churns a mempool of realistic CPFP transaction packages (low-fee
parents lifted by high-fee children), mines blocks (confirming whole top
fee-rate ancestor packages and advancing the tip), and can simulate a node
restart. Everything is behind the `simulation`
feature, so the release binary ships none of it. The sim draws standalone fee-rates
from a bounded power-law distribution (a realistic "wall at the relay floor");
`--fee-skew 1` restores the old uniform draw.

Three terminals:

    just simulate           # fake node on :18443, local profile — blocks every 30s + mempool churn
    just sim-run            # the REAL indexer, pointed at :18443, API on :8080
    just watch              # live /health + /fees every 2s

`just simulate` uses the `local` profile (no throttling), so the indexer keeps
up in real time: `/health` reports `caught_up: true` and `/fees` populates
within a few ticks. To reproduce a throttled remote provider instead — where
the indexer falls behind and `/fees` returns `503` while `caught_up: false` —
run `just simulate-throttled` in place of `just simulate`:

    just simulate-throttled  # fake node on :18443, throttled remote profile — reproduces the sync backlog

While the sim node runs, its own log (the terminal running `just simulate` /
`just simulate-throttled`) prints an INFO `sim: mined block ...` line each
time it mines a block (tip height, txs confirmed, resulting mempool size);
with `--reload-every N` it also prints a WARN `sim: node reload ...` line
whenever it simulates a node restart.

Tuning the fake node (restart `sim-serve` to change):

    cargo run --features simulation -- sim-serve \
        --profile remote \       # throttled provider (rate-limited, ~150ms) vs `local`
        --size 20000 \           # initial mempool size
        --arrivals 600 --evictions 600 \   # churn per 2s tick
        --cpfp-fraction 0.3 \    # fraction of arrivals that attach as a CPFP child (0 = no packages)
        --max-chain 3 \          # max linear package/chain length (1 = no chaining)
        --block-secs 30 \        # seconds between blocks (0 = never mine)
        --reload-every 5 \       # simulate a node restart every 5 blocks (0 = off)
        --fee-skew 3             # fee-rate shape: 1 = uniform, higher = more txs near the relay floor

What to look for while `just watch` runs:

- `--profile remote` (`just simulate-throttled`) → `caught_up` flips to
  `false` (reproduces the throttled-provider backlog); `--profile local`
  (`just simulate`) stays caught up.
- `/fees` populates within a few ticks; tiers are monotone (higher confirmation
  target ⇒ lower fee) and dip on each mined block, then recover as churn refills.
  With the default skewed distribution the tiers spread apart (realistic demand)
  rather than clustering near the cap.
- CPFP: a low-fee parent is pulled into an earlier projected block by its
  high-fee child — the estimator's ancestor-package path, exercised end-to-end.
- `--reload-every N` → the mempool collapses and `caught_up` briefly drops, then
  the sync loop resyncs and settles (mass-drop + cooldown path).

For a real node instead, uncomment an auth block in `.env` and point
`BTC_RPC_URL` at your Bitcoin Core RPC (see `just regtest-up`).

### Run it

**Binary:**

```bash
cargo build --release            # or: just release   (LTO'd; see [profile.release])

BTC_RPC_URL=... BTC_RPC_COOKIE_FILE=... ./target/release/satya
# or: copy .env.example -> .env and use `just run` (loads .env)

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

### `/fees` fields

`GET /fees` returns the cached fee estimate, in **sat/vB**. It is gated on
`caught_up`: before the first successful sync, or whenever `/health` reports the
mempool is out of sync, it returns `503` rather than a number it can't vouch for.

| Field           | Meaning                                                       |
|-----------------|-----------------------------------------------------------------|
| `next_block`   | rate to confirm within ~1 block (~next block)                 |
| `within_3_blocks` | rate to confirm within ~3 blocks (~30 min)                    |
| `within_6_blocks`      | rate to confirm within ~6 blocks (~1 hour)                    |
| `horizon`   | rate at the projection horizon, floored at the minimum        |
| `relay_floor`   | mempool min relay fee (`mempoolminfee`)                       |
| `computed_at`         | unix seconds when the estimate was computed                   |

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

**Module-target map** — filter precisely by target instead of a blanket level:

| Target | What it logs |
|--------|--------------|
| `satya::sync` | sync loop: caught_up transitions, desync/bulk resync, per-tick churn (debug), fee recompute (debug) |
| `satya::rpc` | JSON-RPC transport errors |
| `satya::zmq` | ZMQ block-listener connect/reconnect |
| `tower_http::trace` | HTTP access log (method, path, status, latency) |

**Real-time cheat-sheet** — copy-paste any of these to watch the process live:

```bash
RUST_LOG=info cargo run                          # default: quiet when healthy
RUST_LOG=warn cargo run                          # errors/warnings only
RUST_LOG=warn,satya::sync=debug cargo run        # watch sync churn only
RUST_LOG=error,tower_http::trace=info cargo run  # HTTP access logs only
RUST_LOG=info,tower_http::trace=warn cargo run   # app lifecycle, silence /health spam
LOG_FORMAT=json RUST_LOG=info cargo run          # structured JSON for log aggregators
# via just:
just logs-sync   # sync churn    | just logs-errors   # errors    | just logs-http   # access logs
```

**Rotation.** Logs go to stdout; under systemd/Docker use `journalctl -u <svc> -f`
/ `docker compose logs -f satya`, and rely on journald/Docker log rotation — satya
doesn't write files.

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
