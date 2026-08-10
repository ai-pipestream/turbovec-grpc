# turbovec-grpc

A gRPC server for [turbovec](https://github.com/RyanCodrai/turbovec).

## Repository map

| Repository | Role | Depends on |
|---|---|---|
| [RyanCodrai/turbovec](https://github.com/RyanCodrai/turbovec) | Upstream vector index library: 2/3/4-bit TurboQuant encoding, SIMD top-k search | — |
| [ai-pipestream/turbovec](https://github.com/ai-pipestream/turbovec), branch `turbovec-pipestream-s13` | Patch fork carrying the two small scan primitives distributed search needs: a seedable score floor (`initial_threshold`) and a live-floor candidate stream (`search_streaming`). Rebased onto upstream main | upstream `main` |
| [ai-pipestream/turbovec-grpc](https://github.com/ai-pipestream/turbovec-grpc) (this repo) | Minimal sharded distributed engine: gRPC nodes plus an exact coordinator. Client examples in Go, Java, Python, TypeScript, and Rust | fork branch `turbovec-pipestream-s13` |
| [ai-pipestream/turbovec-search](https://github.com/ai-pipestream/turbovec-search) | Larger distributed hybrid implementation: sharded vector + BM25 nodes, coordinator, write-ahead log, and offline resharding | fork branch `turbovec-pipestream-s13` |
| [ai-pipestream/grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis) | Text-analysis sidecar: sentence/token spans, term vectors, static embeddings, served over gRPC | — |

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
  `StreamSearch` is the separate bidirectional node protocol used by the
  coordinator: nodes emit candidates while the coordinator feeds a rising
  global score floor back into their scans.
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
box. See [docs/architecture.md](docs/architecture.md) for the thin coordinator
design, [docs/deployment.md](docs/deployment.md) for the TLS/auth boundary,
probes, persistence, and sizing, and the [Dockerfile](Dockerfile) for an example
container build.

## The contract

The proto is small, because the payload is just vectors. Every RPC is in
[`proto/turbovec/v1/turbovec.proto`](proto/turbovec/v1/turbovec.proto).

| RPC | Shape | Purpose |
|---|---|---|
| `CreateIndex` | unary | Make an empty index (positional or id-mapped, 2, 3, or 4 bit, optionally lazy). |
| `Add` | client-streaming | Stream vectors in. No train step. |
| `Search` | unary | Top-k for one or more queries, with an optional allowlist. |
| `SearchStream` | server-streaming | Same, streamed one result per query after scoring. |
| `StreamSearch` | bidirectional streaming | Internal distributed scan: emit candidates above a live inclusive floor and finish with a completion certificate. |
| `Remove` | unary | Delete by external id (id-mapped indexes). |
| `GetIndexInfo` / `ListIndexes` | unary | Metadata. |
| `Snapshot` / `Load` | unary | Persist to and from a server-local path. |
| `SetCalibration` / `GetCalibration` | unary | Commit a TQ+ pair fitted elsewhere, and read one back coordinate by coordinate. |
| `ExportRows` / `ImportRows` | unary | Move a run of rows between servers as the encoded codes the index already holds. |

Vectors travel as flat, row-major `float` arrays. The row width is the index
dimensionality, so a search request carries only the query floats.

## When one machine is not enough

A collection outgrows a node, or several collections already sit on different
machines and you want to query them together. `turbovec-coordinator` serves N
node servers as one collection: a client sends a query batch and a `k` and gets
back the top-k a single index over all the same rows would have returned, with
the same scores to the bit. It never names a shard and never learns there are
any.

The equality is not a tolerance that happens to be small. turbovec's TQ+
calibration is a per-coordinate `(shift, scale)` pair, and under a fixed pair a
row's encoded codes are a pure function of the row: the same vector added to
two indexes calibrated alike encodes to the same bytes and scores the same
against the same query. So a row's score does not depend on which index holds
it, the union of the shards' top-k contains the collection's top-k, and merging
by score is the merge rather than an approximation of it.

Everything else in the layer defends that precondition. The collection is bound
before it is served: every node is probed for its dim, bit width and
calibration pair, and the pair is compared coordinate by coordinate. Nodes that
disagree are refused by name, not merged under a correction, because a merge of
differently calibrated scores is not a worse ranking but a ranking of nothing.
A node that fails mid-query fails the search; `allow_partial` opts into the
alternative explicitly, and the response then says it is partial and names what
dropped out.

For a complete search, the coordinator owns the only top-k heap. Each node
streams every candidate admitted by the inclusive floor in effect for its scan
chunk. Once the global heap holds `k` rows, the coordinator broadcasts its
k-th score back to every unfinished node. That score is a lower bound on the
final global k-th score because it came from an observed subset, so pruning
below it cannot remove a true result. Scores equal to the floor remain
eligible. The coordinator answers only after every node returns
`completed=true`; a broken or incomplete stream is not treated as a short
result.

**Split** redistributes one index's rows across nodes when a collection
outgrows a machine. **Join** combines them back when you consolidate. Both move
the encoded codes the index already holds, so neither re-encodes a row and
neither can drift from its source; a row keeps its own id as it moves, and
searches over the result are bit-identical to searches over the original.

```bash
# three nodes
TURBOVEC_GRPC_ADDR=127.0.0.1:51051 cargo run --release --bin turbovec-grpc
TURBOVEC_GRPC_ADDR=127.0.0.1:51052 cargo run --release --bin turbovec-grpc
TURBOVEC_GRPC_ADDR=127.0.0.1:51053 cargo run --release --bin turbovec-grpc

# one coordinator over them
TURBOVEC_COORD_ADDR=127.0.0.1:51050 \
TURBOVEC_COORD_NODES='127.0.0.1:51051,127.0.0.1:51052,127.0.0.1:51053' \
  cargo run --release --bin turbovec-coordinator
```

`TURBOVEC_COORD_NODES` is one node per entry, entries separated by commas or
newlines, each a node address and optionally the index handle on it; `@/path`
reads the table from a file instead. A node named without a handle resolves to
its only open index, and is refused as ambiguous if it holds none or several.
The table is static: it changes only when Split or Join rebinds it, and it is
not persisted across a coordinator restart.

| RPC | Purpose |
|---|---|
| `Search` | Top-k over the whole collection, merged exactly. |
| `FitCalibration` | Fit one calibration from a sample and commit it to every node. |
| `Split` | Redistribute one index's rows across nodes. |
| `Join` | Combine several same-calibration indexes into one. |
| `ListNodes` | Live per-node state: reachability, rows, calibration, and why a collection is not servable. |

Four calls on the node service exist for this layer rather than for a
single-node client: `SetCalibration` and `GetCalibration` commit and read back
the pair, and `ExportRows` and `ImportRows` move rows between servers as
encoded codes. All four are positional-only, because they need
`TurboQuantIndex`'s raw-parts accessors (`packed_codes`, `scales`, the TQ+
getters, `from_parts`) and `IdMapIndex` does not forward them; an id-mapped
index is refused by name rather than decoded and re-encoded, which would change
its codes and so its scores. A distributed collection carries its external ids
as a row label per shard instead, which is what survives rows moving between
nodes.

This is vector search only: no BM25 or hybrid fusion yet. The distributed
protocol and global heap live here, while turbovec remains the engine library
and exposes only the small generally useful scan primitives. The larger
[turbovec-search](https://github.com/ai-pipestream/turbovec-search) repository
is a source of proven mechanisms, not the required shape of this service.

[`clients/python`](clients/python) is an early thin wrapper over the
coordinator. The Rust gRPC engine is the current priority; the wrapper is not a
release gate until that engine is functionally complete.

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

[`clients/python`](clients/python) is different in kind: a package rather than
a demo, and it talks to the coordinator rather than to a single node.

## Building

The contract is compiled at build time by `tonic-build`, which needs `protoc` on
`PATH`. There is no BLAS requirement: the pinned turbovec revision replaced the
dense rotation with a block-Hadamard transform and dropped the dependency.

```bash
cargo test -p turbovec-grpc   # smoke test, and the distributed proof, against live servers
```

The distributed tests are the argument for the layer rather than a check on it:
they stand up real node servers and a real coordinator, build one monolithic
index, split it, and assert the sharded results are bit-identical to the
monolithic ones through split, join and split again.
