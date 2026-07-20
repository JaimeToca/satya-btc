use axum::{routing::get, Json, Router};
use serde_json::{json, Value};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(hello))
        .route("/health", get(health));

    let addr = "0.0.0.0:3000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("listening on http://{addr}");
    axum::serve(listener, app).await.unwrap();
}

async fn hello() -> &'static str {
    "Hello, world!"
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
