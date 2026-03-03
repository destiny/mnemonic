use std::{env, net::SocketAddr};

use mnemonic_engine::Engine;
use mnemonic_server::{AppContext, router};

#[tokio::main]
async fn main() {
    let database_path =
        env::var("MNEMONIC_DB_PATH").unwrap_or_else(|_| "mnemonic.sqlite".to_string());
    let bind_addr = env::var("MNEMONIC_SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    let engine = Engine::new(&database_path).expect("failed to initialize engine");
    let app = router(AppContext::new(engine));

    let addr: SocketAddr = bind_addr.parse().expect("invalid MNEMONIC_SERVER_ADDR");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind socket");

    axum::serve(listener, app)
        .await
        .expect("server execution failed");
}
