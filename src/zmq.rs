//! Optional ZMQ block listener: subscribes to a Bitcoin node's
//! `zmqpubhashblock` publisher and wakes the sync loop for an immediate tick
//! whenever a new block hash is published, so mempool state reflects the
//! confirmed block without waiting out the poll interval.
//!
//! Design notes:
//! - **Accelerator only, never load-bearing.** Polling is the baseline; this
//!   listener just shortens the latency on new blocks. Any error here logs and
//!   retries with backoff — a dead ZMQ socket must never take down the indexer.
//! - **Debounce via a capacity-1 channel + `try_send`.** If a wake is already
//!   pending (channel full), an extra block event is harmlessly dropped rather
//!   than queueing redundant ticks or blocking the listener.
//! - Uses the pure-Rust async `zeromq` crate, so no system `libzmq` is needed.

use std::time::Duration;

use tokio::sync::mpsc::Sender;
use zeromq::{Socket, SocketRecv, SubSocket};

/// Initial reconnect backoff after a connect/recv error.
const BACKOFF_START: Duration = Duration::from_secs(1);
/// Ceiling for the reconnect backoff.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Subscribe to the node's `zmqpubhashblock` publisher at `endpoint` and send
/// `()` on `wake_tx` for each new block-hash message, triggering an immediate
/// sync tick. Runs forever: on any connect/recv error it logs a `warn!`,
/// sleeps with exponential backoff (capped at `BACKOFF_MAX`), and reconnects.
/// Never panics and never returns under normal operation.
pub async fn spawn_block_listener(endpoint: String, wake_tx: Sender<()>) {
    let mut backoff = BACKOFF_START;
    loop {
        // A successful connect resets the backoff so a transient blip doesn't
        // permanently inflate the reconnect delay; the reset is signalled by
        // `run_once` via the `connected` flag.
        let mut connected = false;
        match run_once(&endpoint, &wake_tx, &mut connected).await {
            Ok(()) => {
                // `run_once` only returns Ok if the receive loop ended cleanly
                // (e.g. the publisher closed the connection). Treat as a
                // reconnect condition rather than exiting the listener.
                tracing::warn!(endpoint = %endpoint, "zmq block stream ended; reconnecting");
            }
            Err(e) => {
                tracing::warn!(
                    endpoint = %endpoint,
                    error = %crate::sync::short_err(&e),
                    backoff_secs = backoff.as_secs(),
                    "zmq block listener error; backing off and reconnecting"
                );
            }
        }
        // If we ever connected this session, the error was a mid-stream drop,
        // not a persistent unreachable endpoint — reset backoff so recovery is
        // fast. Otherwise keep growing it toward the cap.
        if connected {
            backoff = BACKOFF_START;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// One connect-subscribe-receive session. Returns `Err` on any socket error
/// (caller backs off and retries) or `Ok(())` if the stream ends cleanly.
/// Sets `*connected = true` once the socket connects and subscribes, so the
/// caller can distinguish a mid-stream drop (reset backoff) from a persistently
/// unreachable endpoint (keep backing off).
async fn run_once(
    endpoint: &str,
    wake_tx: &Sender<()>,
    connected: &mut bool,
) -> Result<(), zeromq::ZmqError> {
    let mut sock = SubSocket::new();
    sock.connect(endpoint).await?;
    // Subscribe to the `hashblock` topic published by `zmqpubhashblock`.
    sock.subscribe("hashblock").await?;
    *connected = true;
    tracing::info!(endpoint = %endpoint, "zmq block listener connected");

    loop {
        // Any recv error propagates up so the caller reconnects with backoff.
        let _msg = sock.recv().await?;
        // Capacity-1 channel + try_send debounces: a full channel means a tick
        // is already pending, so this extra block event is safely dropped.
        // Never block the listener on a full channel.
        let _ = wake_tx.try_send(());
        tracing::debug!(endpoint = %endpoint, "zmq block event -> immediate sync tick");
    }
}
