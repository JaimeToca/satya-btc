use crate::mempool::SharedState;
use crate::rpc::Rpc;
use std::time::Duration;

/// Blocking loop; call on a dedicated std::thread. Never returns under normal operation.
pub fn run(rpc: Rpc, state: SharedState, poll_interval: Duration) { unimplemented!() } // T5
