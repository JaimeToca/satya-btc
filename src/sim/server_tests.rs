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
        let addr = server::spawn(node(500), NetworkProfile::local_node(), 0, 0, 0)
            .await
            .unwrap();
        let rpc = client(addr);
        let entries = rpc.raw_mempool_verbose().await.unwrap();
        assert_eq!(entries.len(), 500);
    }

    #[tokio::test]
    async fn real_client_sees_429_from_throttled_server() {
        let profile = NetworkProfile { req_per_sec: Some(1), ..NetworkProfile::local_node() };
        let addr = server::spawn(node(10), profile, 0, 0, 0).await.unwrap();
        let rpc = client(addr);
        let _ = rpc.tip_height().await; // consumes the 1/sec budget
        match rpc.tip_height().await {
            Err(RpcError::HttpStatus { status: 429, .. }) => {}
            other => panic!("expected 429 surfaced by real client, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn real_client_sees_loaded_false_after_reload() {
        use crate::rpc::MempoolRpc;
        let mut n = node(50);
        n.reload(); // node now reports loaded:false until its next advance()
        let addr = server::spawn(n, NetworkProfile::local_node(), 0, 0, 0)
            .await
            .unwrap();
        let rpc = client(addr);
        // Immediately make the request to catch the node in reload state,
        // before the churn timer's first tick completes. The timer waits
        // 2 seconds between ticks after the first one.
        let info = rpc.mempool_info().await.unwrap();
        assert_eq!(info.loaded, Some(false), "reload must surface loaded:false over HTTP");
    }
}
