//! Offline mempool + network simulation harness. Compiled only under the
//! `simulation` feature (and thus in `cargo test --features simulation`).
//!
//! This crate is a `bin`-only crate (no `lib.rs`), so there's no external
//! consumer to silence rustc's dead-code lint for public API that later sim
//! tasks (the sync-loop harness, node-reload tests, etc.) will call — only
//! this module's own `#[cfg(test)]` block exercises it today. Allow dead code
//! here rather than on individual items; it's removed once the harness that
//! drives `MockNode` end-to-end lands.
#![allow(dead_code, unused_imports)]

pub mod mock_node;

pub use mock_node::{ChurnConfig, FeeDistribution, MockNode};
