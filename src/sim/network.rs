use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bitcoin::Txid;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::rpc::{MempoolEntry, MempoolInfo, MempoolRpc, RpcError};

#[derive(Clone)]
pub struct NetworkProfile {
    pub latency: Duration,
    pub req_per_sec: Option<u32>,
    pub body_cap: Option<usize>,
    pub drop_rate: f64,
}

impl NetworkProfile {
    /// Local node: effectively instant, no limits.
    pub fn local_node() -> Self {
        Self {
            latency: Duration::ZERO,
            req_per_sec: None,
            body_cap: None,
            drop_rate: 0.0,
        }
    }
    /// Throttled remote provider (the GetBlock profile that produced the
    /// observed backlog): ~150ms latency, 20 req/sec, generous body cap.
    pub fn getblock_remote() -> Self {
        Self {
            latency: Duration::from_millis(150),
            req_per_sec: Some(20),
            body_cap: Some(512 * 1024 * 1024),
            drop_rate: 0.0,
        }
    }
}

/// Fixed-window per-second limiter. Shared across clones so one budget governs
/// all concurrent calls (mirrors a real provider's account-wide limit).
pub(crate) struct Limiter {
    window_start: Instant,
    count: u32,
}

impl Limiter {
    pub(crate) fn new() -> Self {
        Self {
            window_start: Instant::now(),
            count: 0,
        }
    }
}

/// Fixed-window per-second rate check, shared by `SimulatedRpc::gate` and the
/// HTTP sim server (`sim::server`) so the two callers can't drift. Returns
/// `true` if the call is within budget (and books it against the window),
/// `false` if the window's budget is already exhausted.
pub(crate) fn check_rate_limit(limiter: &mut Limiter, limit: u32, now: Instant) -> bool {
    if now.duration_since(limiter.window_start) >= Duration::from_secs(1) {
        limiter.window_start = now;
        limiter.count = 0;
    }
    if limiter.count >= limit {
        return false;
    }
    limiter.count += 1;
    true
}

struct Shared {
    limiter: Limiter,
    rng: StdRng,
}

#[derive(Clone)]
pub struct SimulatedRpc<N: MempoolRpc> {
    inner: N,
    profile: NetworkProfile,
    shared: Arc<Mutex<Shared>>,
}

impl<N: MempoolRpc> SimulatedRpc<N> {
    pub fn new(inner: N, profile: NetworkProfile) -> Self {
        Self {
            inner,
            profile,
            shared: Arc::new(Mutex::new(Shared {
                limiter: Limiter::new(),
                rng: StdRng::seed_from_u64(0x5A7A),
            })),
        }
    }

    /// Apply network effects that don't depend on the response body. Returns
    /// `Err` if the call should be rejected (rate limit or random drop). Must be
    /// called (and its lock released) BEFORE any `.await` on the inner RPC.
    fn gate(&self) -> Result<(), RpcError> {
        // Scope the lock so it is never held across the latency await below.
        {
            let mut s = self.shared.lock().unwrap();
            if self.profile.drop_rate > 0.0 && s.rng.gen::<f64>() < self.profile.drop_rate {
                return Err(RpcError::HttpStatus {
                    status: 503,
                    body: "simulated transport drop".to_string(),
                });
            }
            if let Some(limit) = self.profile.req_per_sec {
                if !check_rate_limit(&mut s.limiter, limit, Instant::now()) {
                    return Err(RpcError::HttpStatus {
                        status: 429,
                        body: String::new(),
                    });
                }
            }
        }
        Ok(())
    }

    async fn delay(&self) {
        if self.profile.latency > Duration::ZERO {
            tokio::time::sleep(self.profile.latency).await;
        }
    }

    /// Test/sim access to the wrapped node (e.g. to advance churn between ticks).
    pub fn inner_mut(&mut self) -> &mut N {
        &mut self.inner
    }
}

impl<N: MempoolRpc + Clone + Send + Sync + 'static> MempoolRpc for SimulatedRpc<N> {
    async fn mempool_info(&self) -> Result<MempoolInfo, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.mempool_info().await
    }
    async fn raw_mempool_txids(&self) -> Result<Vec<Txid>, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.raw_mempool_txids().await
    }
    async fn raw_mempool_verbose(&self) -> Result<Vec<(Txid, MempoolEntry)>, RpcError> {
        self.gate()?;
        self.delay().await;
        let entries = self.inner.raw_mempool_verbose().await?;
        if let Some(cap) = self.profile.body_cap {
            // Approximate serialized size: ~180 bytes/entry is plenty to trip a
            // deliberately tiny cap in tests and to model a real large body.
            let approx = entries.len().saturating_mul(180);
            if approx > cap {
                return Err(RpcError::BodyTooLarge { limit: cap });
            }
        }
        Ok(entries)
    }
    async fn mempool_entry(&self, txid: &Txid) -> Result<Option<MempoolEntry>, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.mempool_entry(txid).await
    }
    async fn tip_height(&self) -> Result<u64, RpcError> {
        self.gate()?;
        self.delay().await;
        self.inner.tip_height().await
    }
    fn reconnect(&mut self) -> anyhow::Result<()> {
        self.inner.reconnect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::{ChurnConfig, FeeDistribution, MockNode};

    fn node() -> MockNode {
        MockNode::new(
            9,
            100,
            ChurnConfig {
                arrivals_per_tick: 0,
                evictions_per_tick: 0,
                fee: FeeDistribution {
                    min_sat_vb: 1,
                    max_sat_vb: 10,
                },
                cpfp_fraction: 0.0,
                max_chain: 1,
            },
        )
    }

    #[tokio::test]
    async fn rate_limit_surfaces_429_after_budget() {
        let profile = NetworkProfile {
            latency: std::time::Duration::ZERO,
            req_per_sec: Some(2),
            body_cap: None,
            drop_rate: 0.0,
        };
        let rpc = SimulatedRpc::new(node(), profile);
        // 2 allowed in the current second, 3rd rejected.
        assert!(rpc.tip_height().await.is_ok());
        assert!(rpc.tip_height().await.is_ok());
        match rpc.tip_height().await {
            Err(RpcError::HttpStatus { status: 429, .. }) => {}
            other => panic!("expected 429, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn body_cap_rejects_large_verbose() {
        let profile = NetworkProfile {
            latency: std::time::Duration::ZERO,
            req_per_sec: None,
            body_cap: Some(10), // absurdly small: 100-entry verbose exceeds it
            drop_rate: 0.0,
        };
        let rpc = SimulatedRpc::new(node(), profile);
        match rpc.raw_mempool_verbose().await {
            Err(RpcError::BodyTooLarge { .. }) => {}
            other => panic!("expected BodyTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unlimited_profile_passes_through() {
        let rpc = SimulatedRpc::new(node(), NetworkProfile::local_node());
        assert_eq!(rpc.raw_mempool_txids().await.unwrap().len(), 100);
    }
}
