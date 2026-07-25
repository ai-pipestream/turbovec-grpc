//! Binary entry point for the turbovec gRPC server.
//!
//! Configuration is by environment variable:
//! - `TURBOVEC_GRPC_ADDR` — listen address (default `0.0.0.0:50051`).

use tonic::transport::Server;
use turbovec_grpc::proto::turbo_vec_server::TurboVecServer;
use turbovec_grpc::{proto, IndexStore, TurboVecService};

/// Default listen address when `TURBOVEC_GRPC_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50051";

/// Frame limit for a single request or response message. Vector batches are
/// large, so this is generous; clients should still chunk bulk ingest through
/// the client-streaming `Add` rather than send one enormous frame.
const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("TURBOVEC_GRPC_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;

    let service = TurboVecService::new(IndexStore::new())
        .into_server()
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);

    // Standard gRPC health checking (grpc.health.v1), so orchestrators and
    // load balancers can probe liveness/readiness without calling the API.
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<TurboVecServer<TurboVecService>>()
        .await;

    // Server reflection, so grpcurl and similar tooling work without a local
    // copy of the proto. Register the health descriptors too, or tooling can
    // see the health service's methods but cannot resolve them by name.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    eprintln!("turbovec-grpc listening on {addr}");
    Server::builder()
        .tcp_nodelay(true)
        .add_service(service)
        .add_service(health_service)
        .add_service(reflection)
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;
    eprintln!("turbovec-grpc shut down");
    Ok(())
}

/// Resolve when the process receives Ctrl-C or SIGTERM, so in-flight searches
/// can drain instead of being cut off mid-response.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
