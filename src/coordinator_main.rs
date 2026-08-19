//! Binary entry point for the turbovec coordinator.
//!
//! An operator runs N node servers (`turbovec-grpc`) and one of these. The
//! coordinator holds no index of its own: it fans searches out to the nodes,
//! merges their results exactly, and moves rows between them on Split and
//! Join. Clients talk to it and to nothing else.
//!
//! Configuration is by environment variable:
//! - `TURBOVEC_COORD_ADDR` — listen address (default `0.0.0.0:50050`).
//! - `TURBOVEC_COORD_NODES` — the node table, one shard per entry, entries
//!   separated by commas or newlines. A leading `@` reads it from a file
//!   instead. See `coordinator::nodes` for the entry syntax.
//! - `TURBOVEC_COORD_STATE` — atomic topology-generation state file.
//! - `TURBOVEC_ALLOW_EPHEMERAL` — opt into non-durable demo topology.
//! - `TURBOVEC_AUTOSCALE_MAX_ROWS_PER_SHARD` — grow-only autoscaler ceiling;
//!   unset keeps the autoscaler off. `TURBOVEC_AUTOSCALE_INTERVAL_MS` sets
//!   how often it looks (default 30 s).
//! - `TURBOVEC_METRICS_ADDR` — optional OpenMetrics HTTP listener.
//!
//! ```bash
//! TURBOVEC_COORD_NODES='127.0.0.1:50051,127.0.0.1:50052' turbovec-coordinator
//! TURBOVEC_COORD_NODES=@/etc/turbovec/nodes turbovec-coordinator
//! ```

use tonic::transport::Server;
use turbovec_grpc::proto::coordinator_server::CoordinatorServer;
use turbovec_grpc::{proto, CoordinatorLimits, CoordinatorService, Metrics, NodeTable};

/// Default listen address when `TURBOVEC_COORD_ADDR` is not set. One below the
/// node default, so a coordinator and a node can share a host without either
/// one being moved.
const DEFAULT_ADDR: &str = "0.0.0.0:50050";

/// Frame limit for a single request or response message, matching the node
/// binary's: a search over a large batch and a split of a large shard both
/// cross this boundary.
const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    turbovec_grpc::init_tracing("turbovec-coordinator");
    let addr = std::env::var("TURBOVEC_COORD_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;
    let table = node_table()?;
    let limits = CoordinatorLimits::from_env()?;
    let max_message_bytes = turbovec_grpc::config::positive_usize(
        "TURBOVEC_MAX_MESSAGE_BYTES",
        DEFAULT_MAX_MESSAGE_BYTES,
    )?;
    let metrics = Metrics::default();
    if let Some(address) = std::env::var_os("TURBOVEC_METRICS_ADDR") {
        metrics
            .clone()
            .start(address.to_string_lossy().parse()?)
            .await?;
    }
    let coordinator = match std::env::var_os("TURBOVEC_COORD_STATE") {
        Some(path) => CoordinatorService::with_state_file_limits_and_metrics(
            table,
            path,
            limits.clone(),
            metrics,
        )?,
        None if turbovec_grpc::config::enabled("TURBOVEC_ALLOW_EPHEMERAL")? => {
            CoordinatorService::with_limits_and_metrics(table, limits, metrics)
        }
        None => return Err(
            "TURBOVEC_COORD_STATE is required; set TURBOVEC_ALLOW_EPHEMERAL=true only for demos"
                .into(),
        ),
    };
    let (generation, table) = coordinator.topology_snapshot();
    if let Some(policy) = turbovec_grpc::AutoscalePolicy::from_env()? {
        coordinator.spawn_autoscaler(policy);
    }

    tracing::info!(%addr, topology_generation = generation, shards = table.len(), "coordinator listening");
    for shard in &table.shards {
        tracing::info!(
            address = %shard.address,
            index_id = shard.index_id.as_deref().unwrap_or("(sole index)"),
            "configured shard"
        );
    }

    let readiness_coordinator = coordinator.clone();
    let service = coordinator
        .into_server()
        .max_decoding_message_size(max_message_bytes)
        .max_encoding_message_size(max_message_bytes);

    // The same health and reflection services the node binary registers, so a
    // coordinator is probed and explored with the same tooling as a node.
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    if readiness_coordinator.ready().await {
        health_reporter
            .set_serving::<CoordinatorServer<CoordinatorService>>()
            .await;
    } else {
        health_reporter
            .set_not_serving::<CoordinatorServer<CoordinatorService>>()
            .await;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            if readiness_coordinator.ready().await {
                health_reporter
                    .set_serving::<CoordinatorServer<CoordinatorService>>()
                    .await;
            } else {
                health_reporter
                    .set_not_serving::<CoordinatorServer<CoordinatorService>>()
                    .await;
            }
        }
    });
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .tcp_nodelay(true)
        .trace_fn(|request| tracing::info_span!("grpc", method = %request.uri().path()))
        .add_service(service)
        .add_service(health_service)
        .add_service(reflection)
        .serve_with_shutdown(addr, turbovec_grpc::shutdown_signal())
        .await?;
    tracing::info!("coordinator shut down");
    Ok(())
}

/// Read the node table from the environment, from a file when the value
/// starts with `@`.
///
/// A coordinator with no node table has no collection to serve, so this is a
/// startup failure rather than an empty collection discovered on the first
/// search.
fn node_table() -> Result<NodeTable, Box<dyn std::error::Error>> {
    let configured = std::env::var("TURBOVEC_COORD_NODES").map_err(|_| {
        "TURBOVEC_COORD_NODES is not set: the coordinator needs a node table, either inline \
         ('host:port,host:port') or as '@/path/to/file'"
    })?;
    let text = match configured.strip_prefix('@') {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read the node table at {path}: {e}"))?,
        None => configured,
    };
    Ok(NodeTable::parse(&text)?)
}
