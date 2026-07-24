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

/// Generated protobuf types, client, and server for the `turbovec.v1`
/// package.
///
/// Produced from `proto/turbovec/v1/turbovec.proto` by `tonic-build` in
/// `build.rs`; do not edit by hand.
pub mod proto {
    #![allow(clippy::all)]
    tonic::include_proto!("turbovec.v1");
}

pub mod service;
pub mod store;

pub use service::TurboVecService;
pub use store::{Index, IndexStore};
