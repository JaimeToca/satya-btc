# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.1.0] - 2026-07-28

### Added

- In-memory mempool mirror synced from Bitcoin Core (poll + ZMQ block-push), with a `caught_up` honesty contract.
- `GET /fees` — CPFP-aware recommended fee rates via a Bitcoin Core-style ancestor-package block projection (weight-histogram tiers: `next_block` / `within_3_blocks` / `within_6_blocks` / `horizon` / `relay_floor`).
- `GET /health` — sync/freshness status.
- Structured logging (`RUST_LOG` per-module filters, `LOG_FORMAT=json`), fee-recompute visibility.
- Offline simulation harness (behind the `simulation` feature).
- Docker/compose deployment; `just` recipes.
