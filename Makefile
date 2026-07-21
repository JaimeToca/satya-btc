# Load .env (KEY=value lines) if present, and export the vars to recipe commands.
# The binary itself reads plain env vars — .env is just a dev convenience.
ifneq (,$(wildcard .env))
include .env
export
endif

.DEFAULT_GOAL := help

.PHONY: help build release run check clippy fmt health docker \
        regtest-up regtest-tx regtest-down

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-13s\033[0m %s\n",$$1,$$2}'

build: ## Debug build
	cargo build

release: ## Optimized release build
	cargo build --release

run: ## Run the indexer (config from .env / environment)
	cargo run

check: ## Type-check without building
	cargo check

clippy: ## Lint
	cargo clippy --all-targets

fmt: ## Format the code
	cargo fmt

health: ## curl the /health endpoint (needs `jq`)
	curl -s localhost:8080/health | jq

docker: ## Build and run via docker compose
	docker compose up --build

# --- local regtest node helpers (need bitcoind/bitcoin-cli on PATH) ---
regtest-up: ## Start a throwaway regtest node + wallet + mature coins
	bitcoind -regtest -daemon
	@sleep 1
	-bitcoin-cli -regtest createwallet dev
	bitcoin-cli -regtest generatetoaddress 101 $$(bitcoin-cli -regtest getnewaddress)

regtest-tx: ## Send one tx into the regtest mempool
	bitcoin-cli -regtest sendtoaddress $$(bitcoin-cli -regtest getnewaddress) 0.1

regtest-down: ## Stop the regtest node
	bitcoin-cli -regtest stop
