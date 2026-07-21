# Async reqwest JSON-RPC Client — Design

**Date:** 2026-07-21
**Status:** Approved (brainstorm)
**Base:** builds on the fee-latency branch (`e7f1f82`); removes the `spawn_blocking`/semaphore machinery added there.

## Why this exists

`bitcoincore-rpc` is a **blocking** client with no async version (rust-bitcoin
issue #78). To use it under tokio we wrap every call in `spawn_blocking`, which:
burns blocking-pool threads, opens a fresh TCP(+TLS) connection per call (`minreq`
has no keep-alive), and cannot cancel in-flight work — the last point forced the
Grok-review semaphore that bounds orphaned fetches on a budget bail.

Replacing it with a small **async JSON-RPC client over `reqwest`** removes all three
problems: native async (no blocking pool), connection pooling/keep-alive, and true
cancellation (dropping a future cancels the request). It also lets us delete
`transport.rs`, the `spawn_blocking` helper, and the fetch semaphore.

Behavior, `/health`, and sync semantics are unchanged — this is an internal
transport swap.

## Decisions (from brainstorm)

- **Hand-roll minimal response structs** for exactly the fields we use — no
  `bitcoincore-rpc`/`bitcoincore-rpc-json` dependency. Keep the `bitcoin` crate for
  `Txid`/`Amount`/`Network`.
- **Per-call concurrency** (`buffer_unordered(FETCH_CONCURRENCY)`), not JSON-RPC
  batching. Batching is a possible later enhancement, out of scope here.
- **TLS = rustls** (matches today's minreq-https).
- **`RPC_TIMEOUT_SECS` becomes a real per-request timeout** enforced by reqwest.

## Design

### 1. Dependencies & modules
- **Add:** `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }`.
- **Remove:** `bitcoincore-rpc`, `minreq`, `base64` (and the transitive `jsonrpc`).
- **Keep:** `bitcoin`, `serde`, `serde_json`, `tokio`, `futures`, `zeromq`, `axum`, `clap`, `anyhow`, `tracing`, `thiserror` (new, small).
- **Delete `src/transport.rs`.**
- **Rewrite `src/rpc.rs`** as the async client + hand-rolled types (split types into
  `src/rpc/types.rs` only if `rpc.rs` grows unwieldy).

### 2. The client (`src/rpc.rs`)
- `#[derive(Clone)] struct Rpc { http: reqwest::Client, url: String, auth: Auth, headers: Vec<(String, String)> }`.
  `reqwest::Client` is internally `Arc`'d and connection-pooled, so `Rpc` is cheap to
  clone and share across concurrent fetches — no `Arc<Client>` wrapper.
- **Build:** `reqwest::Client::builder().timeout(cfg.timeout).build()`. Auth resolved
  from `RpcConfig`: cookie file (read now → basic), user/pass (→ basic), else none
  (token-in-URL). Custom headers applied per request (or as default headers).
- **Core helper:**
  ```rust
  async fn call<T: DeserializeOwned>(&self, method: &str, params: serde_json::Value) -> Result<T, RpcError>
  ```
  POSTs `{"jsonrpc":"1.0","id":<n>,"method":method,"params":params}`; maps HTTP
  401/403 → `RpcError::Auth`; other non-success HTTP → `RpcError::Http`; parses the
  JSON-RPC envelope `{result, error}` → `RpcError::Rpc{code,message}` if `error` is
  present, else deserializes `result` into `T`.
- **Typed async methods** (all `&self`):
  - `network() -> Network` (getblockchaininfo → `chain`)
  - `tip_height() -> u64` (getblockchaininfo → `blocks`)
  - `mempool_info() -> MempoolInfo` (getmempoolinfo)
  - `raw_mempool_txids() -> Vec<Txid>` (getrawmempool false)
  - `raw_mempool_verbose() -> Vec<(Txid, MempoolEntry)>` (getrawmempool true)
  - `mempool_entry(&Txid) -> Option<MempoolEntry>` (getmempoolentry; `error.code == -5 → Ok(None)`)
- **Minimal structs** (`#[derive(Deserialize)]`, `#[serde(rename_all=...)]`/`rename`
  to match Core's JSON field names):
  - `MempoolInfo { loaded: Option<bool>, mempoolminfee: Amount }` (BTC/kvB, via
    `as_btc`, preserving the existing `min_fee_sat_vb` = `to_sat() as f64 / 1000.0`)
  - `MempoolEntry { vsize: u64, weight: u64, depends: Vec<Txid>, fees: MempoolEntryFees, ancestorsize: u64, descendantsize: u64 }`
  - `MempoolEntryFees { base: Amount, ancestor: Amount, descendant: Amount }`
  - `BlockchainInfo { chain: Network, blocks: u64 }`
  - **Amounts stay exact sats** via `#[serde(with = "bitcoin::amount::serde::as_btc")]`
    on each `Amount` field — Core reports fees as BTC decimals; this parses to
    integer sats with no float rounding. This is the single most important
    correctness detail.
  - `mempool.rs`'s `From<&MempoolEntry> for MempoolTx` is updated to the new struct
    (same fields it already reads: vsize/weight/base fee/depends/ancestor+descendant fee+size).
- **Errors & reconnect:**
  - `#[derive(thiserror::Error)] enum RpcError { Http(reqwest::Error), Auth, Rpc{code:i32,message:String}, Decode(serde_json::Error) }`.
  - `pub fn reconnect(&mut self)` rebuilds the client (re-reads the cookie file for rotation).
  - `pub fn is_reconnectable(&RpcError) -> bool` = connect/timeout (`reqwest::Error::is_connect()`/`is_timeout()`) **or** `Auth`.

### 3. `sync.rs` cleanup
- **Delete:** the `spawn_blocking` helper (moves to rpc.rs's async methods — gone entirely), the `fetch_semaphore: Arc<Semaphore>`, the `acquire_owned` call, and `mempool_entry_with_permit` (→ plain `mempool_entry`).
- Fetch fan-out: `stream::iter(candidates).map(|txid| { let rpc = rpc.clone(); async move { (txid, rpc.mempool_entry(&txid).await...) } }).buffer_unordered(cfg.fetch_concurrency)`.
- **Budget bail:** dropping the stream now truly cancels in-flight reqwest requests, so no orphaned work — the C2 semaphore is unnecessary. Keep the `now_or_never` drain (#4) and `resolved < to_fetch` (#6). Update/remove the permit/orphan comments.
- Loop-level reconnect stays, using the new `is_reconnectable`. Callers now handle `RpcError` (via `?`/`anyhow::Error` conversion or matching on `RpcError` where `-5`/reconnect classification is needed).

### 4. Config / errors / testing
- `RpcConfig` (url, auth, timeout, headers) unchanged. `RPC_TIMEOUT_SECS` now a real
  per-request timeout enforced + cancellable by reqwest (was only loosely honored).
- Error log levels unchanged: control-plane RPC failures at `warn` with traces;
  per-tx `mempool_entry` and operational detail at `debug`.
- **No automated tests** (phase-consistent). Verify by build + clippy + live run
  against a node/provider: bulk sync, concurrent-fetch overlap, `-5` "vanished tx"
  handling, and (if testable) a 401 → reconnect. ZMQ and local-node checks unchanged.

## Behavior parity
Identical RPC calls, identical `/health` output, identical sync/freshness semantics.
No config or operator-visible change except that `RPC_TIMEOUT_SECS` is now strictly
enforced.

## Known limitations / non-goals
- No JSON-RPC batching (per-call + concurrency; batching is a later option).
- Hand-rolled deserialization owns field-name/amount correctness — mitigated by the
  `as_btc` Amount helper and verifying field names against a live Core response.
- Full event-driven ingest and Esplora adapter remain out of scope (unchanged).
