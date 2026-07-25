# Requires the `just` command runner:  cargo install just
# Loads .env automatically (KEY=value); the binary just reads plain env vars.
set dotenv-load := true

# List available recipes
default:
    @just --list

# Debug build
build:
    cargo build

# Optimized release build
release:
    cargo build --release

# Run the indexer (config from .env / environment)
run:
    cargo run

# Type-check without building
check:
    cargo check

# Lint
clippy:
    cargo clippy --all-targets

# Run unit tests (simulation harness included)
test:
    cargo test --features simulation

# Run the offline simulated node (indexer will catch up): local profile, blocks every 30s + CPFP churn, on :18443
simulate:
    cargo run --features simulation -- sim-serve --profile local --block-secs 30

# Like `simulate` but with the throttled remote-provider profile — reproduces the sync backlog (caught_up stays false)
simulate-throttled:
    cargo run --features simulation -- sim-serve --profile remote --block-secs 30

# Format the code
fmt:
    cargo fmt

# curl the /health endpoint (needs `jq`)
health:
    curl -s localhost:8080/health | jq

# Run the REAL indexer against the sim node (unsets any real-node auth from .env)
sim-run:
    env -u BTC_RPC_COOKIE_FILE -u BTC_RPC_USER -u BTC_RPC_PASS \
        BTC_RPC_URL=http://127.0.0.1:18443 cargo run

# curl the /fees endpoint (needs `jq`)
fees:
    curl -s localhost:8080/fees | jq

# Watch /health + /fees live, refreshing every 2s (portable; no `watch` needed)
watch:
    while true; do \
        clear; \
        echo '== /health =='; curl -s localhost:8080/health | jq; \
        echo '== /fees =='; curl -s localhost:8080/fees | jq; \
        sleep 2; \
    done

# Build and run via docker compose
docker:
    docker compose up --build

# Start a throwaway regtest node + wallet + mature coins
regtest-up:
    bitcoind -regtest -daemon
    sleep 1
    -bitcoin-cli -regtest createwallet dev
    bitcoin-cli -regtest generatetoaddress 101 $(bitcoin-cli -regtest getnewaddress)

# Send one tx into the regtest mempool
regtest-tx:
    bitcoin-cli -regtest sendtoaddress $(bitcoin-cli -regtest getnewaddress) 0.1

# Stop the regtest node
regtest-down:
    bitcoin-cli -regtest stop
