# Fee-Latency Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Reduce mempool-view latency toward mempool.space parity via bounded concurrent fetch, ZMQ block-push, a time-based freshness bail, and package-data capture — feeding a future GBT fee estimator.

**Architecture:** Move the sync loop onto the existing tokio runtime; fetch new-tx details with `spawn_blocking` + `buffer_unordered(FETCH_CONCURRENCY)`; subscribe to `zmqpubhashblock` via `tmq` to trigger immediate ticks through an interruptible `tokio::select!` loop; bail on a per-tick time budget; expose data age.

**Tech Stack:** tokio, `futures` (StreamExt), `tmq` (tokio-zmq), existing blocking `bitcoincore-rpc`.

## Global Constraints

- Deployment target: local (or pruned, tip-synced, non-`blocksonly`) node.
- Do NOT push or open PRs (subagents included) — commit locally only.
- No automated tests this phase; verify by live run.
- Keep the blocking `bitcoincore-rpc` client — no async HTTP rewrite.
- Preserve the exit-on-sync-death supervisor behavior in `main.rs`.
- Best-effort fetch semantics preserved: one failed fetch never aborts the batch; failures/backlog/time-lag flip `caught_up=false`.
- ZMQ is opt-in and an accelerator only; polling remains the baseline.

---

### Task 1: Capture ancestor/descendant package data in `MempoolTx`

**Files:** Modify `src/mempool.rs`.

**Interfaces:**
- Produces: `MempoolTx` gains `ancestor_fee: Amount`, `ancestor_vsize: u32`, `descendant_fee: Amount`, `descendant_vsize: u32`. The `From<&GetMempoolEntryResult>` impl populates them from `entry.fees.ancestor`/`entry.fees.descendant`, `entry.ancestor_size`, `entry.descendant_size`.

**Steps:**
- [ ] Add the four fields to `MempoolTx`.
- [ ] Populate them in `impl From<&GetMempoolEntryResult> for MempoolTx` (fields already exist on `GetMempoolEntryResult`: `fees.ancestor`, `fees.descendant`, `ancestor_size`, `descendant_size`; vsize is `u64` → `as u32`).
- [ ] `cargo build` clean (dead-code warnings acceptable, they're consumed by GBT later).
- [ ] Commit.

---

### Task 2: Add config knobs

**Files:** Modify `src/config.rs`.

**Interfaces:**
- Produces on `Config`: `fetch_concurrency: usize` (default 10, clamp ≥1), `zmq_block: Option<String>` (env `BTC_ZMQ_BLOCK`), `tick_budget: Duration` (env `TICK_BUDGET_MS`, default `2 × poll_interval_ms`).

**Steps:**
- [ ] Add CLI args: `#[arg(long, env="FETCH_CONCURRENCY", default_value_t=10)] fetch_concurrency: usize`; `#[arg(long, env="BTC_ZMQ_BLOCK")] zmq_block: Option<String>`; `#[arg(long, env="TICK_BUDGET_MS")] tick_budget_ms: Option<u64>`.
- [ ] In `from_env`: clamp `fetch_concurrency` to `.max(1)`; `tick_budget = Duration::from_millis(tick_budget_ms.unwrap_or(poll_interval_ms * 2))`.
- [ ] `cargo build` clean. Commit.

---

### Task 3: Move sync loop onto tokio (foundational)

**Files:** Modify `src/sync.rs`, `src/main.rs`.

**Interfaces:**
- `sync::run` becomes `pub async fn run(rpc: Rpc, state: SharedState, cfg: SyncConfig, wake_rx: tokio::sync::mpsc::Receiver<()>)`. `SyncConfig` gains `fetch_concurrency: usize` and `tick_budget: Duration` (verbose/heartbeat/poll_interval stay).
- Consumes: `Config` fields from Task 2.
- The steady-state wait uses `tokio::select!` over `tokio::time::sleep(poll_interval)` and `wake_rx.recv()`.
- Blocking RPC calls wrapped in `tokio::task::spawn_blocking` (client shared as `Arc<Client>` — see Task 4; for this task, wrap the existing single-client calls).

**Steps:**
- [ ] Change `Rpc` to expose the inner client as `Arc<Client>` (or add a method returning a clonable handle) so calls can move into `spawn_blocking`. Keep existing typed methods working (they can `.clone()` the Arc internally and run inline via `spawn_blocking` at call sites in sync).
- [ ] Convert `run` to `async fn`; replace `std::thread::sleep` with the `tokio::select!` wait; keep startup + bulk-resync loops (their blocking calls via `spawn_blocking`).
- [ ] In `main.rs`: create `let (wake_tx, wake_rx) = tokio::sync::mpsc::channel(1);` (hold `wake_tx` for Task 5); spawn the loop with `tokio::spawn(sync::run(...))`; keep the supervisor that logs and `process::exit(1)` if the task ends.
- [ ] `cargo build` clean; live-run sanity (syncs, `/health` caught_up true). Commit.

---

### Task 4: Concurrent bounded fetch + time-based bail

**Files:** Modify `src/sync.rs`. Modify `Cargo.toml` (add `futures`).

**Interfaces:**
- Consumes: `cfg.fetch_concurrency`, `cfg.tick_budget`, `Arc<Client>` from Task 3.
- The steady-state new-tx fetch replaced by `stream::iter(diff.new).map(spawn_blocking get_mempool_entry).buffer_unordered(cfg.fetch_concurrency)`, collecting results; a fetch error increments `fetch_errors`; `MAX_NEW_FETCH_PER_TICK` cap retained.
- Time bail: track fetch start `Instant`; if elapsed exceeds `cfg.tick_budget`, stop consuming the stream, set a `budget_exceeded` flag → part of `backlog` → `caught_up=false`.

**Steps:**
- [ ] Add `futures = "0.3"` to `Cargo.toml`.
- [ ] Replace the sequential fetch loop with the bounded-concurrent stream; preserve best-effort (`Promise.allSettled`-style: collect Ok, count Err, don't abort).
- [ ] Add the tick-budget check driving `budget_exceeded` into the existing `backlog` computation.
- [ ] Preserve the verbose/`SYNC_LOG_VERBOSE` and intra-tick heartbeat behavior (progress line can key off items completed).
- [ ] `cargo build` + clippy clean; live-run: confirm concurrent calls in `bitcoincore_rpc=debug` (interleaved), and induced-slowness flips `caught_up=false`. Commit.

---

### Task 5: ZMQ block subscription → immediate tick

**Files:** Create `src/zmq.rs`. Modify `src/main.rs`, `Cargo.toml`.

**Interfaces:**
- Consumes: `cfg.zmq_block: Option<String>`, `wake_tx: tokio::sync::mpsc::Sender<()>` from Task 3.
- `zmq::spawn_block_listener(endpoint: String, wake_tx: Sender<()>)` — a tokio task subscribing to `zmqpubhashblock`; on each block message, `wake_tx.try_send(())` (capacity-1 channel debounces: a full channel means a tick is already pending, drop). On socket error, log and retry with backoff; never panic the process.

**Steps:**
- [ ] Add `tmq = "0.5"` (tokio-zmq) to `Cargo.toml`. (If `tmq` proves incompatible with the tokio version, fall back to a `std::thread` + `zmq` crate that sends on a `tokio::sync::mpsc` via `blocking_send`; note the choice in the report.)
- [ ] Implement `zmq::spawn_block_listener`: subscribe (topic `hashblock`), loop receiving, `try_send(())` on each, reconnect-with-backoff on error.
- [ ] In `main.rs`: if `cfg.zmq_block` is `Some(ep)`, `tokio::spawn(zmq::spawn_block_listener(ep, wake_tx))`. Unset = no listener (polling only).
- [ ] `cargo build` clean; live-run against a node with `zmqpubhashblock` set: a new block triggers an immediate tick (visible: tick fires before the poll interval elapses). Commit.

---

### Task 6: Freshness signal + docs

**Files:** Modify `src/http.rs`, `README.md`, `.env.example`.

**Interfaces:**
- `/health` gains `age_secs: Option<u64>` = `now − last_sync_ok` (None if never synced).

**Steps:**
- [ ] Add `age_secs` to the `Health` struct + handler (derive from `last_sync_ok`).
- [ ] `.env.example`: add `FETCH_CONCURRENCY`, `BTC_ZMQ_BLOCK`, `TICK_BUDGET_MS` with the node-tuning advice (rpcthreads/rpcworkqueue; zmqpubhashblock).
- [ ] `README.md`: new subsection on fetch concurrency + ZMQ setup (node `bitcoin.conf` lines: `zmqpubhashblock=tcp://0.0.0.0:28332`, `rpcthreads=10`), the `age_secs`/freshness fields, and the documented limitations (no keep-alive; block-push only).
- [ ] `cargo build` clean. Commit.

---

## Final whole-branch review

After Task 6: dispatch the final whole-branch reviewer (most capable model) over `merge-base main..HEAD`. Focus: tokio migration correctness (no blocking-in-async, lock-across-await), concurrency safety (shared `Arc<Client>`, best-effort semantics), ZMQ fallback/debounce, freshness honesty. Then finish the branch.
