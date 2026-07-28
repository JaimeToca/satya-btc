# Contributing to Satya

Thank you for your interest in contributing to Satya! We welcome contributions of all kinds — bug reports, feature ideas, documentation improvements, and code.

## Prerequisites

- **Rust stable** (developed against 1.93)
- Optional: `just` and `jq` for convenience recipes

## Building and Running

Build the project:

```bash
just build
# or: cargo build
```

Run the indexer:

```bash
just run
```

## Before Opening a PR

All of the following must pass:

```bash
just fmt       # cargo fmt
just clippy    # cargo clippy --all-targets
just test      # cargo test --features simulation
```

Note: Satya is a binary crate, not a library. When running tests with filters, use:

```bash
cargo test --features simulation <name>
```

## Workflow

1. **Branch off `main`** for your changes
2. **Keep commits focused** — one feature or fix per PR
3. **Keep tests green** — the test suite must pass before merge
4. **Open a PR to `main`** with a clear description of your changes

## Pre-1.0 Development

Satya is in active development (pre-1.0), and public interfaces may change. For larger changes, especially new features or significant refactors, please **open an issue first** to discuss your approach. This helps avoid wasted effort and ensures alignment with the project's direction.

## Questions?

- Check existing issues and discussions
- Open a new issue if you have questions or ideas

Happy contributing!
