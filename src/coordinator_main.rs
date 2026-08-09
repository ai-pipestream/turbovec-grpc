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
//!
//! ```bash
//! TURBOVEC_COORD_NODES='127.0.0.1:50051,127.0.0.1:50052' turbovec-coordinator
//! TURBOVEC_COORD_NODES=@/etc/turbovec/nodes turbovec-coordinator
//! ```

use tonic::transport::Server;
use turbovec_grpc::proto::coordinator_server::CoordinatorServer;
use turbovec_grpc::{proto, CoordinatorService, NodeTable};

/// Default listen address when `TURBOVEC_COORD_ADDR` is not set. One below the
/// node default, so a coordinator and a node can share a host without either
/// one being moved.
const DEFAULT_ADDR: &str = "0.0.0.0:50050";

/// Frame limit for a single request or response message, matching the node
/// binary's: a search over a large batch and a split of a large shard both
/// cross this boundary.
const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("TURBOVEC_COORD_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;
    let table = node_table()?;

    eprintln!(
        "turbovec-coordinator listening on {addr}, {} shard(s) configured",
        table.len()
    );
    for shard in &table.shards {
        eprintln!(
            "  {} {}",
            shard.address,
            shard.index_id.as_deref().unwrap_or("(sole index)")
        );
    }

    let service = CoordinatorService::new(table)
        .into_server()
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);

    // The same health and reflection services the node binary registers, so a
    // coordinator is probed and explored with the same tooling as a node.
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<CoordinatorServer<CoordinatorService>>()
        .await;
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()?;

    Server::builder()
        .tcp_nodelay(true)
        .add_service(service)
        .add_service(health_service)
        .add_service(reflection)
        .serve_with_shutdown(addr, turbovec_grpc::shutdown_signal())
        .await?;
    eprintln!("turbovec-coordinator shut down");
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
