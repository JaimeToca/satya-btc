# The sync loop, explained (for a new dev)

Satya keeps an in-memory copy of the Bitcoin mempool in sync with a node,
checked every ~2 seconds. Transaction IDs are shortened to `a`, `b`, `c`… here
(real ones are 64-char hex).

## The cast
- **The node** — the source of truth (the real mempool). We ask it questions over RPC.
- **Our cache** — a box in memory: `{ txid → tx details }`, plus a `caught_up` flag.

Start: node's mempool = `{a, b, c}`; our cache = empty.

## Startup

**1. Wait until the node is ready**
```
→ getmempoolinfo
← { loaded: true, mempoolminfee: 0.00001 }
```
`loaded: true` → continue. (If `false`, sleep 2s and ask again.)

**2. Bulk load the whole mempool once**
```
→ getrawmempool (verbose=true)
← { "a": {vsize:140, fees:{base:0.00001}, depends:[]},
    "b": {vsize:200, fees:{base:0.00002}, depends:[]},
    "c": {vsize:150, fees:{base:0.00003}, depends:["a"]} }
```
```
cache = {a,b,c}   caught_up = TRUE   last_sync_ok = 12:00:00
```

## Tick 1 — a new tx `d` arrives
```
→ getmempoolinfo         ← { loaded: true, ... }
→ getrawmempool (false)  ← ["a","b","c","d"]      # just IDs (cheap)
```
Diff: gone = none · new = `d`.
```
→ getmempoolentry "d"    ← { vsize:180, fees:{base:0.00004}, depends:[] }
```
```
cache = {a,b,c,d}   caught_up = TRUE   last_sync_ok = 12:00:02
```

## Tick 2 — a block confirms `a` and `c`
```
→ getrawmempool (false)  ← ["b","d"]              # a, c left the mempool
```
Diff: gone = `a,c` · new = none. **Remove `a,c` immediately — no fetch needed**
(the ID list alone proves they left).
```
cache = {b,d}   caught_up = TRUE   last_sync_ok = 12:00:04
```

## Tick 3 — the node has a hiccup (RPC fails)
```
→ getmempoolinfo   ← ⚠️ ERROR (timeout / refused)
```
Don't crash, don't guess — **mark stale** and skip:
```
cache = {b,d}   caught_up = FALSE   last_sync_ok = 12:00:04 (unchanged)
```
`/health` honestly reports `caught_up: false` with the old `last_sync_ok`.
Next tick retries; when it works, `caught_up` flips back to `true`.

## The safety rails (with examples)
- **Cap (flood):** node lists 50,000 new IDs in one tick → fetch only the first
  **2,000**, leave the rest; `caught_up=false`; drain ~2,000/tick over ~25 ticks.
  Never stalls.
- **Resync + cooldown (restart):** node's list drops 25,000 → 3 → do a fresh bulk
  load, but not more than **once per 60s**, so a flapping node can't force constant
  re-downloads.
- **Honesty:** if a `getmempoolentry` fails, that tx stays out of the cache AND
  `caught_up=false`. We claim "caught up" only when everything was fetched.

## One sentence
> Every 2s: ask the node for its list of tx IDs → drop the ones we no longer see →
> fetch details for the new ones (up to 2,000) → and only say "in sync" if we truly
> finished; if the node is unreachable, say "not sure" instead of lying or crashing.

Code: the loop lives in [`src/sync.rs`](../src/sync.rs); the node calls in
[`src/rpc.rs`](../src/rpc.rs); the state model in [`src/mempool.rs`](../src/mempool.rs).
