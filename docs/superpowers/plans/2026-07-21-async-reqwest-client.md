# Async reqwest JSON-RPC Client — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Replace the blocking `bitcoincore-rpc` client with a small async JSON-RPC client over `reqwest`, and delete the `spawn_blocking`/semaphore machinery it required.

**Architecture:** One `reqwest::Client`-backed `Rpc` type; every RPC funnels through a single generic `call<T>` helper (the DRY anchor — no per-method request/parse duplication); hand-rolled minimal response structs; per-call concurrency via `buffer_unordered`.

**Tech Stack:** reqwest (rustls-tls, json), serde, serde_json, thiserror, bitcoin (Txid/Amount/Network), tokio, futures.

## Global Constraints
- Spec: `docs/superpowers/specs/2026-07-21-async-reqwest-client-design.md` — the source of truth.
- Behavior, `/health`, and sync semantics UNCHANGED (internal swap). Only `RPC_TIMEOUT_SECS` becomes strictly enforced.
- **DRY:** all typed methods go through the single `call<T>` helper. Do NOT duplicate request-building or envelope-parsing per method. REUSE `RpcConfig` as-is; UPDATE `mempool.rs`'s `From` impl (don't rewrite the model); keep `sync.rs`'s structure, changing only the fetch/reconnect call sites.
- Fees/amounts stay exact sats via `#[serde(with = "bitcoin::amount::serde::as_btc")]` — no float rounding.
- Do NOT push / open PRs (subagents included) — commit locally.
- No automated tests (phase-consistent); verify by build + clippy + live run.

---

### Task 1: Swap to the async reqwest client (atomic)

This is one coherent change: the crate does not compile until the swap is complete end-to-end, and any intermediate "both clients present" state would duplicate code (forbidden by the DRY constraint). Land it as one compiling, clippy-clean commit.

**Files:**
- Modify: `Cargo.toml` — add `reqwest = { version = "0.12", default-features = false, features = ["rustls-tls", "json"] }`, `thiserror = "1"`; remove `bitcoincore-rpc`, `minreq`, `base64`.
- Rewrite: `src/rpc.rs` — async client, types, `RpcError`, `call<T>`, `is_reconnectable`, `reconnect`.
- Delete: `src/transport.rs` (and its `mod transport;` in `main.rs`).
- Modify: `src/mempool.rs` — update `From<&MempoolEntry> for MempoolTx` to the new struct (same fields read today: vsize, weight, base fee, depends, ancestor/descendant fee+size) and `min_fee_sat_vb(&MempoolInfo)`.
- Modify: `src/sync.rs` — remove the `spawn_blocking` reliance (now in rpc.rs), the `fetch_semaphore`/`acquire_owned`/permit, and `mempool_entry_with_permit` → plain `mempool_entry`; keep `buffer_unordered`, the `now_or_never` drain, `resolved < to_fetch`, and loop-level reconnect (now `RpcError`-classified).
- Modify: `src/main.rs` — `network()` retry loop already exists; adjust to the new error type; remove `mod transport;`.

**Interfaces produced (rpc.rs):**
- `#[derive(Clone)] pub struct Rpc { http: reqwest::Client, url: String, auth: Auth, headers: Vec<(String,String)> }`
- `pub fn connect(&RpcConfig) -> anyhow::Result<Rpc>`
- `async fn call<T: DeserializeOwned>(&self, method: &str, params: serde_json::Value) -> Result<T, RpcError>` — the single request/parse path.
- `pub async fn network(&self) -> Result<Network, RpcError>`, `tip_height() -> Result<u64,_>`, `mempool_info() -> Result<MempoolInfo,_>`, `raw_mempool_txids() -> Result<Vec<Txid>,_>`, `raw_mempool_verbose() -> Result<Vec<(Txid, MempoolEntry)>,_>`, `mempool_entry(&Txid) -> Result<Option<MempoolEntry>,_>` (`error.code == -5 → Ok(None)`).
- `pub fn reconnect(&mut self) -> anyhow::Result<()>`; `pub fn is_reconnectable(&RpcError) -> bool` (connect/timeout or `Auth`).
- Structs: `MempoolInfo { loaded: Option<bool>, #[serde(with=as_btc)] mempoolminfee: Amount }`, `MempoolEntry { vsize:u64, weight:u64, depends:Vec<Txid>, fees: MempoolEntryFees, ancestorsize:u64, descendantsize:u64 }`, `MempoolEntryFees { as_btc base/ancestor/descendant: Amount }`, `BlockchainInfo { chain: Network, blocks: u64 }`. Use `#[serde(rename)]`/`rename_all` to match Core's JSON field names exactly.

**Steps:**
- [ ] Update `Cargo.toml` deps as above.
- [ ] Write `src/rpc.rs`: the structs, `RpcError` (thiserror), the `call<T>` helper (build `{"jsonrpc":"1.0","id":0,"method","params"}`, POST with auth+headers+timeout, map 401/403→`Auth`, other bad HTTP→`Http`, parse `{result,error}`), then the 6 typed methods each a one-liner over `call`. Resolve auth from `RpcConfig` (cookie read → basic; user/pass → basic; else none). Apply custom headers.
- [ ] Delete `src/transport.rs`; remove `mod transport;` from `main.rs`.
- [ ] Update `src/mempool.rs` `From`/`min_fee_sat_vb` to the new structs.
- [ ] Update `src/sync.rs` + `src/main.rs` call sites and error handling; delete the semaphore + permit path.
- [ ] `cargo build` && `cargo clippy --all-targets` clean.
- [ ] Live run against `https://go.getblock.io/6e86595fd191463d832a8fd631ecf483`: confirm bulk sync, concurrent `getmempoolentry` overlap, and `/health` output identical in shape (incl. `age_secs`, `mempool_min_fee_sat_vb`). Verify a `getmempoolentry` for a vanished tx yields `Ok(None)` (no error). Paste ~6 log lines.
- [ ] Commit locally.

---

## Task review + final review
After Task 1: dispatch a reviewer (Opus) over the diff. Focus: serde field names vs Core's actual JSON; `as_btc` amount correctness (exact sats, no rounding); `-5 → Ok(None)`; `is_reconnectable` covers 401 (cookie rotation) + connect/timeout; NO `spawn_blocking`/semaphore/`bitcoincore-rpc` residue; the `call<T>` helper is the sole request path (no duplicated request/parse logic — the DRY gate); `RPC_TIMEOUT_SECS` actually applied. Then a Grok cross-check if the diff warrants it. Fix Critical/Important before done.
