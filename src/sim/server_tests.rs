#[cfg(test)]
mod tests {
    use crate::config::RpcConfig;
    use crate::rpc::{Rpc, RpcError};
    use crate::sim::{server, ChurnConfig, FeeDistribution, MockNode, NetworkProfile};
    use std::time::Duration;

    fn node(size: usize) -> MockNode {
        MockNode::new(
            11,
            size,
            ChurnConfig {
                arrivals_per_tick: 0,
                evictions_per_tick: 0,
                fee: FeeDistribution { min_sat_vb: 1, max_sat_vb: 50 },
            },
        )
    }

    fn client(addr: std::net::SocketAddr) -> Rpc {
        Rpc::connect(&RpcConfig {
            url: format!("http://{addr}"),
            auth: None,
            timeout: Duration::from_secs(5),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn real_client_bulk_loads_over_http() {
        let addr = server::spawn(node(500), NetworkProfile::local_node(), 0).await;
        let rpc = client(addr);
        let entries = rpc.raw_mempool_verbose().await.unwrap();
        assert_eq!(entries.len(), 500);
    }

    #[tokio::test]
    async fn real_client_sees_429_from_throttled_server() {
        let profile = NetworkProfile { req_per_sec: Some(1), ..NetworkProfile::local_node() };
        let addr = server::spawn(node(10), profile, 0).await;
        let rpc = client(addr);
        let _ = rpc.tip_height().await; // consumes the 1/sec budget
        match rpc.tip_height().await {
            Err(RpcError::HttpStatus { status: 429, .. }) | Err(RpcError::Auth) => {}
            other => panic!("expected 429 surfaced by real client, got {other:?}"),
        }
    }
}
