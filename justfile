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

# Run unit tests
test:
    cargo test

# Format the code
fmt:
    cargo fmt

# curl the /health endpoint (needs `jq`)
health:
    curl -s localhost:8080/health | jq

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
