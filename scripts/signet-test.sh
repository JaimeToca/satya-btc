#!/usr/bin/env bash
#
# Isolated signet node + satya test harness.
#
# Everything lives under ONE datadir (default ~/satya-signet-test); your real
# ~/.bitcoin is never touched. Cleanup is a single `delete` (= stop + rm -rf).
#
# Usage:
#   scripts/signet-test.sh setup    # write conf + start pruned signet bitcoind (-daemon)
#   scripts/signet-test.sh wait     # block until IBD finishes (shows progress)
#   scripts/signet-test.sh up       # setup + wait
#   scripts/signet-test.sh run      # run satya against the node (foreground; Ctrl-C to stop)
#   scripts/signet-test.sh fees     # curl /fees
#   scripts/signet-test.sh health   # curl /health
#   scripts/signet-test.sh info     # node sync status + disk usage
#   scripts/signet-test.sh logs     # tail bitcoind debug.log
#   scripts/signet-test.sh stop     # stop bitcoind
#   scripts/signet-test.sh delete   # stop bitcoind + rm -rf the datadir
#
# Override the datadir with SATYA_SIGNET_DIR=/path scripts/signet-test.sh ...
set -euo pipefail

DATADIR="${SATYA_SIGNET_DIR:-$HOME/satya-signet-test}"
RPC_PORT="${SIGNET_RPC_PORT:-38332}"
HTTP_BIND="${HTTP_BIND:-127.0.0.1:8080}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI=(bitcoin-cli -datadir="$DATADIR" -signet -rpcport="$RPC_PORT")

# Parse one top-level field out of getblockchaininfo via python3 (robust vs grep).
_field() { "${CLI[@]}" getblockchaininfo 2>/dev/null | python3 -c "import sys,json;print(json.load(sys.stdin).get('$1',''))"; }

setup() {
  if "${CLI[@]}" getblockchaininfo >/dev/null 2>&1; then
    echo "bitcoind already running (datadir: $DATADIR)"; return
  fi
  mkdir -p "$DATADIR"
  cat > "$DATADIR/bitcoin.conf" <<EOF
signet=1
server=1
prune=550
rpcport=$RPC_PORT
# blocksonly stays OFF (default) so the mempool relays -> real /fees
# txindex NOT needed for mempool RPCs
EOF
  echo "starting pruned signet bitcoind (datadir: $DATADIR)..."
  bitcoind -datadir="$DATADIR" -daemon
  for _ in $(seq 1 60); do
    if "${CLI[@]}" getblockchaininfo >/dev/null 2>&1; then echo "RPC up on 127.0.0.1:$RPC_PORT"; return; fi
    sleep 1
  done
  echo "ERROR: RPC did not come up in 60s. See $DATADIR/signet/debug.log" >&2; exit 1
}

wait_sync() {
  echo "waiting for signet IBD to finish (node keeps running if you Ctrl-C)..."
  while :; do
    if ! "${CLI[@]}" getblockchaininfo >/dev/null 2>&1; then echo "  RPC not ready, retrying..."; sleep 3; continue; fi
    local ibd blocks headers prog
    ibd=$(_field initialblockdownload); blocks=$(_field blocks); headers=$(_field headers); prog=$(_field verificationprogress)
    printf "\r  blocks %s / %s   progress %.4f   ibd=%s     " "$blocks" "$headers" "${prog:-0}" "$ibd"
    if [ "$ibd" = "False" ]; then echo; echo "caught up (IBD complete)."; return; fi
    sleep 5
  done
}

run_satya() {
  if ! "${CLI[@]}" getblockchaininfo >/dev/null 2>&1; then echo "node not running — run 'setup' first" >&2; exit 1; fi
  echo "running satya -> http://$HTTP_BIND  (Ctrl-C to stop)"
  BTC_RPC_URL="http://127.0.0.1:$RPC_PORT" \
  BTC_RPC_COOKIE_FILE="$DATADIR/signet/.cookie" \
  HTTP_BIND="$HTTP_BIND" \
    cargo run --manifest-path "$REPO/Cargo.toml"
}

info() {
  "${CLI[@]}" getblockchaininfo 2>/dev/null | python3 -c "import sys,json;d=json.load(sys.stdin);print(f\"chain={d['chain']} blocks={d['blocks']}/{d['headers']} ibd={d['initialblockdownload']} progress={d['verificationprogress']:.4f} size_on_disk={d['size_on_disk']/1e9:.2f}GB\")" || echo "node not running"
  echo -n "datadir disk usage: "; du -sh "$DATADIR" 2>/dev/null || echo "(none)"
}

curl_ep() { curl -s "http://$HTTP_BIND/$1" | python3 -m json.tool 2>/dev/null || { echo "no response (is satya running?)"; }; }

stop_node() { "${CLI[@]}" stop 2>/dev/null && echo "bitcoind stopping..." || echo "bitcoind not running"; }

delete_all() {
  stop_node || true
  echo "waiting for bitcoind to exit..."; sleep 3
  rm -rf "$DATADIR" && echo "deleted $DATADIR"
}

case "${1:-help}" in
  setup)  setup ;;
  wait)   wait_sync ;;
  up)     setup; wait_sync ;;
  run)    run_satya ;;
  fees)   curl_ep fees ;;
  health) curl_ep health ;;
  info)   info ;;
  logs)   tail -f "$DATADIR/signet/debug.log" ;;
  stop)   stop_node ;;
  delete) delete_all ;;
  *) sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
esac
