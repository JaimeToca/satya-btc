# Fee-Latency: Concurrent Fetch + ZMQ + Freshness — Design

**Date:** 2026-07-21
**Status:** Approved (conversational brainstorm)

## Why this exists

Phase 1 builds a correct in-memory mempool, but its steady-state sync fetches each
new transaction with a **separate, sequential** `getmempoolentry` RPC. Against a
local node that's sub-millisecond; against any latency, and during post-block
refill bursts even locally, a single tick can run long and the mempool view
silently lags — exactly the data a fee estimate will sit on. mempool.space keeps
fees fresh via (a) bounded fetch **concurrency**, and (b) a **ZMQ block push** so a
new block triggers an immediate recompute instead of waiting for the next poll.
This phase closes the *freshness* gap so a future GBT estimator is fed a fresh,
complete, honestly-aged mempool.

Non-goal: the GBT estimator / `/fees` endpoint itself (separate later phase). This
phase is plumbing that feeds it.

## Deployment assumption

**Local (or pruned, tip-synced, non-`blocksonly`) node.** ZMQ is a raw socket on
the node, so hosted providers can't offer it — event-driven latency is a
local-node feature, which is consistent with the project's stated target.

## Design

### 1. Move the sync loop onto tokio (foundational)

Today `sync::run` is a blocking `fn` on a dedicated `std::thread`. Move it to an
async task on the existing tokio runtime. The poll loop becomes an interruptible
wait so a ZMQ event can trigger a tick immediately:

```rust
tokio::select! {
    _ = tokio::time::sleep(poll_interval) => { /* normal tick */ }
    _ = wake_rx.recv()                     => { /* ZMQ block → immediate tick */ }
}
```

The blocking `bitcoincore-rpc` client stays. Individual RPCs run via
`tokio::task::spawn_blocking`. The supervisor in `main.rs` (exit-on-death) is
preserved: if the sync task ends, log and `process::exit(1)`.

### 2. Concurrent fetch (configurable, default 10)

Share the client as `Arc<Client>` (it is `Send + Sync`; `minreq` opens a fresh
connection per call so concurrent calls don't collide). Fan the per-tick new-txid
fetch out with bounded concurrency:

```rust
let results = stream::iter(new_txids)
    .map(|txid| { let c = client.clone();
        tokio::task::spawn_blocking(move || c.get_mempool_entry(&txid)) })
    .buffer_unordered(fetch_concurrency)     // FETCH_CONCURRENCY, default 10
    .collect::<Vec<_>>().await;
```

Best-effort semantics preserved: a single failed fetch doesn't abort the batch;
failures still flip `caught_up=false` (backlog). `MAX_NEW_FETCH_PER_TICK` cap stays.

**Reconnect model change:** a shared `Arc<Client>` can't be rebuilt in place per
call. Since `minreq` reconnects per call, a transient failure just fails that
fetch and the next call reconnects. Cookie rotation is handled at the loop level
(rebuild the Arc between ticks on a persistent auth error), not per-call.

### 3. ZMQ block subscription (`zmqpubhashblock`)

Optional, enabled by config. An async `tmq` (tokio-zmq) task subscribes to the
node's `zmqpubhashblock` endpoint; on a block-hash message it sends on `wake_tx`.
Requirements:
- **Debounce/coalesce:** collapse rapid messages so we don't launch overlapping
  ticks (a `tokio::sync::mpsc` of capacity 1, or a Notify, is enough).
- **Fallback:** ZMQ is an accelerator, not a replacement. Polling remains the
  baseline; if ZMQ isn't configured or the socket dies, the loop still ticks on
  the timer.
- **Opt-in:** `BTC_ZMQ_BLOCK` (e.g. `tcp://127.0.0.1:28332`); unset = polling only.

### 4. Time-based tick bail

If a tick's fetch phase runs longer than `TICK_BUDGET` (default `2 × poll_interval`),
stop fetching, mark `caught_up=false`, and let the remainder reappear in the next
tick's `diff.new`. Bounds tick duration regardless of latency; stops a slow tick
masquerading as fresh.

### 5. Freshness signal

- Flip `caught_up=false` on **time** lag (tick bail, above), not only on count/errors.
- Expose data age in `/health`: `age_secs` = now − `last_sync_ok` (or the raw
  `last_sync_ok` the consumer already gets; add a derived `age_secs` for
  convenience).

### 6. Capture package data (sets up GBT; free here)

Extend `MempoolTx` with ancestor/descendant fee+size (`ancestor_fee`,
`ancestor_vsize`, `descendant_fee`, `descendant_vsize`) — already present in the
`getmempoolentry` / verbose response we parse. Needed for effective (package)
feerate in GBT; capture now while the fetch path is being reworked.

## Configuration (new)

| Var | Default | Meaning / advice |
|-----|---------|------------------|
| `FETCH_CONCURRENCY` | `10` | Concurrent `getmempoolentry` calls. Bounded by node `rpcthreads` (default 4) and `rpcworkqueue` (default 16). Local: set `rpcthreads>=10`; keep `<= rpcworkqueue`. Remote: lower to the provider's rate limit. |
| `BTC_ZMQ_BLOCK` | — | Node `zmqpubhashblock` endpoint, e.g. `tcp://127.0.0.1:28332`. Unset = polling only. |
| `TICK_BUDGET_MS` | `2 × POLL_INTERVAL_MS` | Max fetch time per tick before bailing and marking stale. |

Node-side docs: `zmqpubhashblock=tcp://0.0.0.0:28332`, `rpcthreads`, `rpcworkqueue`.

## Known limitations (documented, not fixed here)

- `minreq` has no keep-alive: each RPC is a fresh TCP(+TLS) connection. Concurrency
  still massively beats sequential; connection pooling would need an async client
  (`reqwest`) — out of scope.
- Full event-driven ingest (`zmqpubsequence` + `zmqpubrawtx`) is a later ceiling;
  this phase does block-push only.
- `MempoolTx`'s package fields (`ancestor_fee`, `ancestor_vsize`,
  `descendant_fee`, `descendant_vsize`) are a **snapshot** taken at fetch time,
  not a live view: they are not refreshed as related txs later enter or leave the
  mempool, so a cached tx's package totals can drift from the node's current
  state. Phase-3 GBT must recompute/refresh package data rather than trust these
  cached values as live.

## Testing

No automated tests this phase (consistent with Phase 1); verify by live run against
a local regtest/mainnet node: concurrent fetch visible in logs, ZMQ block triggers
an immediate tick, tick bail flips `caught_up` under induced slowness.
