//! A gRPC server for the [turbovec](https://github.com/ai-pipestream/turbovec)
//! vector index.
//!
//! The [`turbovec`] crate is a fast, in-memory, quantized vector index. This
//! crate wraps it in a handle-based gRPC service: a client creates or loads an
//! index, receives an `index_id`, and then adds vectors and runs searches
//! against that id. The Python bindings give turbovec one language; this gives
//! it every language with a gRPC stack, over the same index.
//!
//! Concurrency follows what the underlying crate already guarantees. turbovec's
//! `search` takes `&self` and is safe to run from many threads against one
//! shared index, so searches here run under a read lock and never block one
//! another. The mutating paths (`add`, `remove`) take `&mut self`, so they run
//! under a write lock that blocks only the single index they touch. All of the
//! encode and search work is CPU-bound, so every call does it inside
//! `tokio::task::spawn_blocking` rather than on an async worker.
//!
//! The wire contract lives in `proto/turbovec/v1/turbovec.proto` and is
//! compiled at build time; see [`proto`].

// Every fallible path in this crate ends at a tonic RPC, whose generated trait
// signature is `Result<Response<T>, Status>`. `Status` is 176 bytes, which
// `result_large_err` objects to, and the objection has nowhere to go: the type
// at the boundary is not ours to box, and boxing internally only to unbox at
// the boundary trades a size warning for a layer of indirection on every
// error path.
#![allow(clippy::result_large_err)]

/// Generated protobuf types, client, and server for the `turbovec.v1`
/// package.
///
/// Produced from `proto/turbovec/v1/turbovec.proto` by `tonic-build` in
/// `build.rs`; do not edit by hand.
pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("turbovec.v1");

    /// Encoded `FileDescriptorSet` for the `turbovec.v1` package, emitted by
    /// `build.rs`; used by the binary to serve gRPC reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("turbovec_v1");
}

/// Generated types for the vendored `ai.pipestream.proto.index.hints.v1`
/// indexing-hint options (owned by protomolt; the vendored copy under
/// `proto/ai/pipestream/` must stay byte-identical to the source).
///
/// prost does not generate the `extend google.protobuf.FieldOptions` block
/// itself; [`schema`] reads the extension off client descriptors dynamically
/// and decodes its payload into these typed structs.
pub mod hints {
    #![allow(clippy::all)]
    tonic::include_proto!("ai.pipestream.proto.index.hints.v1");
}

pub mod collapse;
pub mod columns;
pub mod config;
pub mod coordinator;
pub mod documents;
pub mod errors;
pub mod filter;
pub mod observability;
pub mod parents;
pub mod schema;
pub mod service;
pub mod store;

pub use coordinator::{CoordinatorLimits, CoordinatorService, NodeTable, ShardConfig};
pub use documents::DocumentsService;
pub use observability::Metrics;
pub use schema::BoundSchema;
pub use service::{ServiceLimits, TurboVecService};
pub use store::{Index, IndexStore};

/// Install JSON structured logging controlled by `RUST_LOG`.
pub fn init_tracing(service: &'static str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .init();
    tracing::info!(service, "structured logging initialized");
}

/// Resolve when the process receives Ctrl-C or SIGTERM, so in-flight work can
/// drain instead of being cut off mid-response. Shared by both binaries.
pub async fn shutdown_signal() {
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
