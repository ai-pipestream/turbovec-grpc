//! Binary entry point for the turbovec gRPC server.
//!
//! Configuration is by environment variable:
//! - `TURBOVEC_GRPC_ADDR` — listen address (default `0.0.0.0:50051`).
//! - `TURBOVEC_DATA_DIR` — durable shard-generation root.
//! - `TURBOVEC_ALLOW_EPHEMERAL` — opt into a non-durable demo node.
//! - `TURBOVEC_METRICS_ADDR` — optional OpenMetrics HTTP listener.

use std::sync::Arc;
use tonic::transport::Server;
use turbovec_grpc::proto::turbo_vec_admin_server::TurboVecAdminServer;
use turbovec_grpc::proto::turbo_vec_query_server::TurboVecQueryServer;
use turbovec_grpc::{proto, IndexStore, Metrics, ServiceLimits, TurboVecService};

/// Default listen address when `TURBOVEC_GRPC_ADDR` is not set.
const DEFAULT_ADDR: &str = "0.0.0.0:50051";

/// Frame limit for a single request or response message. Vector batches are
/// large, so this is generous; clients should still chunk bulk ingest through
/// the client-streaming `Add` rather than send one enormous frame.
const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    turbovec_grpc::init_tracing("turbovec-grpc");
    let addr = std::env::var("TURBOVEC_GRPC_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;

    let allow_ephemeral = turbovec_grpc::config::enabled("TURBOVEC_ALLOW_EPHEMERAL")?;
    let store =
        Arc::new(match std::env::var_os("TURBOVEC_DATA_DIR") {
            Some(path) => IndexStore::open(path)?,
            None if allow_ephemeral => IndexStore::new(),
            None => return Err(
                "TURBOVEC_DATA_DIR is required; set TURBOVEC_ALLOW_EPHEMERAL=true only for demos"
                    .into(),
            ),
        });
    let limits = ServiceLimits::from_env()?;
    let max_message_bytes = turbovec_grpc::config::positive_usize(
        "TURBOVEC_MAX_MESSAGE_BYTES",
        DEFAULT_MAX_MESSAGE_BYTES,
    )?;
    let restored = store.handles().len();
    let persistent = store.data_root().is_some();
    let metrics = Metrics::default();
    if let Some(address) = std::env::var_os("TURBOVEC_METRICS_ADDR") {
        metrics
            .clone()
            .start(address.to_string_lossy().parse()?)
            .await?;
    }
    let readiness_service =
        TurboVecService::with_limits_and_metrics(Arc::clone(&store), limits, metrics);
    let query_service = readiness_service
        .clone()
        .into_query_server()
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes);
    let admin_service = readiness_service
        .clone()
        .into_admin_server()
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes);

    // Standard gRPC health checking (grpc.health.v1), so orchestrators and
    // load balancers can probe liveness/readiness without calling the API.
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    if readiness_service.ready() {
        health_reporter
            .set_serving::<TurboVecQueryServer<TurboVecService>>()
            .await;
        health_reporter
            .set_serving::<TurboVecAdminServer<TurboVecService>>()
            .await;
    } else {
        health_reporter
            .set_not_serving::<TurboVecQueryServer<TurboVecService>>()
            .await;
        health_reporter
            .set_not_serving::<TurboVecAdminServer<TurboVecService>>()
            .await;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            if readiness_service.ready() {
                health_reporter
                    .set_serving::<TurboVecQueryServer<TurboVecService>>()
                    .await;
                health_reporter
                    .set_serving::<TurboVecAdminServer<TurboVecService>>()
                    .await;
            } else {
                health_reporter
                    .set_not_serving::<TurboVecQueryServer<TurboVecService>>()
                    .await;
                health_reporter
                    .set_not_serving::<TurboVecAdminServer<TurboVecService>>()
                    .await;
            }
        }
    });

    // Server reflection, so grpcurl and similar tooling work without a local
    // copy of the proto. Register the health descriptors too, or tooling can
    // see the health service's methods but cannot resolve them by name.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    tracing::info!(%addr, restored, persistent, "node listening");
    let serve_result = Server::builder()
        .tcp_nodelay(true)
        .trace_fn(|request| tracing::info_span!("grpc", method = %request.uri().path()))
        .add_service(query_service)
        .add_service(admin_service)
        .add_service(health_service)
        .add_service(reflection)
        .serve_with_shutdown(addr, turbovec_grpc::shutdown_signal())
        .await;
    if persistent {
        let flushed = tokio::task::spawn_blocking(move || store.persist_all()).await??;
        tracing::info!(shards = flushed.len(), "persisted shards on shutdown");
    }
    serve_result?;
    tracing::info!("node shut down");
    Ok(())
}
