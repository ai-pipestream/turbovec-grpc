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
  ingested in chunks with no train step. `SearchStream` is server-streaming, so
  a caller can start handling the first query's neighbours before the batch
  finishes.
- **Filter at search time.** `Search` takes an optional allowlist (external ids
  for an id-mapped index, slot indices for a positional one) and pushes it into
  the kernel, rather than over-fetching and discarding.

## Run

```bash
cargo run -p turbovec-grpc
# turbovec-grpc listening on 0.0.0.0:50051
```

`TURBOVEC_GRPC_ADDR` overrides the listen address.

## The contract

The proto is small, because the payload is just vectors. Every RPC is in
[`proto/turbovec/v1/turbovec.proto`](proto/turbovec/v1/turbovec.proto).

| RPC | Shape | Purpose |
|---|---|---|
| `CreateIndex` | unary | Make an empty index (positional or id-mapped, 2 or 4 bit, optionally lazy). |
| `Add` | client-streaming | Stream vectors in. No train step. |
| `Search` | unary | Top-k for one or more queries, with an optional allowlist. |
| `SearchStream` | server-streaming | Same, one result per query as it is scored. |
| `Remove` | unary | Delete by external id (id-mapped indexes). |
| `GetIndexInfo` / `ListIndexes` | unary | Metadata. |
| `Snapshot` / `Load` | unary | Persist to and from a server-local path. |

Vectors travel as flat, row-major `float` arrays. The row width is the index
dimensionality, so a search request carries only the query floats.

## Building

The contract is compiled at build time by `tonic-build`, which needs `protoc` on
`PATH`. The crate inherits turbovec's BLAS requirement: on Linux install
OpenBLAS (`libopenblas-dev`), on macOS the Accelerate framework is used
automatically.

```bash
cargo test -p turbovec-grpc   # end-to-end smoke test against a live server
```
