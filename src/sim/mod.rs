//! Offline mempool + network simulation harness. Compiled only under the
//! `simulation` feature (and thus in `cargo test --features simulation`).
//!
//! This crate is a `bin`-only crate (no `lib.rs`), so there's no external
//! consumer to silence rustc's dead-code lint for public API that later sim
//! tasks (the sync-loop harness, node-reload tests, etc.) will call — only
//! this module's own `#[cfg(test)]` block exercises it today. Allow dead code
//! here rather than on individual items; it's removed once the harness that
//! drives `MockNode` end-to-end lands. Scoped to `dead_code` only — unused
//! IMPORTS must still fail the gate so later sim tasks can't accumulate stale
//! `use`s unnoticed.
#![allow(dead_code)]

pub mod mock_node;

// Consumed by later sim tasks (`crate::sim::{...}` in network.rs / sync sim
// tests / the HTTP server). Scoped allow on just this re-export so an unused
// import ANYWHERE ELSE in the sim tree still fails the `-D warnings` gate.
#[allow(unused_imports)]
pub use mock_node::{ChurnConfig, FeeDistribution, MockNode};
