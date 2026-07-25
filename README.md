# turbovec-grpc

A gRPC server for [turbovec](../README.md).

turbovec is a fast, in-memory, quantized vector index with Python bindings. This
crate puts it behind gRPC, so the same index is reachable from any language with
a gRPC stack, not just Python. The service is handle-based: create or load an
index, get an `index_id`, then add vectors and search against it.

- **Concurrent search, serialized writes.** turbovec's `search` takes `&self`
  and is safe to share across threads, so searches run under a read lock and
  never block one another. `add` and `remove` take a write lock on the single
  index they touch. All of it runs on the blocking pool, off the async workers.
- **Streaming both ways.** `Add` is client-streaming, so a large corpus is
  ingested in chunks with no train step. `SearchStream` is server-streaming:
  the batch is scored under one short read-lock hold, then streamed one
  result per query, so a slow reader never pins index locks or server threads.
- **Filter at search time.** `Search` takes an optional allowlist (external ids
  for an id-mapped index, slot indices for a positional one) and pushes it into
  the kernel, rather than over-fetching and discarding.

## Run

```bash
cargo run -p turbovec-grpc
# turbovec-grpc listening on 0.0.0.0:50051
```

`TURBOVEC_GRPC_ADDR` overrides the listen address.

The server also registers the standard `grpc.health.v1.Health` service and
gRPC server reflection, so orchestrator probes and `grpcurl` work out of the
box. See [docs/deployment.md](docs/deployment.md) for the TLS/auth boundary,
probes, persistence, and sizing, and the [Dockerfile](Dockerfile) for an
example container build.

## The contract

The proto is small, because the payload is just vectors. Every RPC is in
[`proto/turbovec/v1/turbovec.proto`](proto/turbovec/v1/turbovec.proto).

| RPC | Shape | Purpose |
|---|---|---|
| `CreateIndex` | unary | Make an empty index (positional or id-mapped, 2 or 4 bit, optionally lazy). |
| `Add` | client-streaming | Stream vectors in. No train step. |
| `Search` | unary | Top-k for one or more queries, with an optional allowlist. |
| `SearchStream` | server-streaming | Same, streamed one result per query after scoring. |
| `Remove` | unary | Delete by external id (id-mapped indexes). |
| `GetIndexInfo` / `ListIndexes` | unary | Metadata. |
| `Snapshot` / `Load` | unary | Persist to and from a server-local path. |

Vectors travel as flat, row-major `float` arrays. The row width is the index
dimensionality, so a search request carries only the query floats.

## Client examples

Each [`examples/`](examples) directory is self-contained: its own toolchain
setup, a vendored copy of the proto, and a README with startup instructions
and a speed test for that stack.

| Example | Stack | Angle |
|---|---|---|
| [`java-client`](examples/java-client) | Java (Maven) | Speed from the JVM; uint64 and float32 wire fidelity vs JSON. |
| [`ts-client`](examples/ts-client) | Node.js (TypeScript) | The 2^53 id footgun is native to JS; gRPC removes it. |
| [`python-client`](examples/python-client) | Python (grpcio) | The index the embedded bindings use, shared over the network. |
| [`go-client`](examples/go-client) | Go | Sidecar-shaped client for the infra crowd. |
| [`rust-client.rs`](examples/rust-client.rs) | Rust (cargo) | `cargo run -p turbovec-grpc --example rust-client` — the tonic client ships in the crate, no codegen needed. |

## Building

The contract is compiled at build time by `tonic-build`, which needs `protoc` on
`PATH`. The crate inherits turbovec's BLAS requirement: on Linux install
OpenBLAS (`libopenblas-dev`), on macOS the Accelerate framework is used
automatically.

```bash
cargo test -p turbovec-grpc   # end-to-end smoke test against a live server
```
