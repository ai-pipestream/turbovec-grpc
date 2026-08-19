# turbovec-grpc

Minimal CPU-only distributed vector search over
[`turbovec`](https://github.com/RyanCodrai/turbovec).

This is a sister project, not code embedded in the upstream engine. It uses the
`ai-pipestream/turbovec` fork branch `turbovec-pipestream-s16`, whose small
patch exposes live-floor streaming scans and chunk-boundary control. All gRPC,
persistence, sharding, topology, and failure behavior lives here.

This dependency reads and writes only TurboVec v7 indexes. A binary built from
this branch cannot restore a pre-v7 shard generation; stage and verify a v7
generation before deploying it over an older durable node.

| Repository | Role | Depends on |
|---|---|---|
| [RyanCodrai/turbovec](https://github.com/RyanCodrai/turbovec) | Upstream vector index library: 4-bit TurboQuant encoding, SIMD top-k search | — |
| [ai-pipestream/turbovec](https://github.com/ai-pipestream/turbovec), branch `turbovec-pipestream-s16` | Patch fork carrying the seedable top-k floor and live-floor streaming collector. Rebased onto upstream `main`; explicit TQ+ calibration is now upstream | upstream `main` |
| [ai-pipestream/turbovec-grpc](https://github.com/ai-pipestream/turbovec-grpc) (this repo) | Network and sharding facade over the fork: durable node service plus an exact distributed coordinator | fork branch `turbovec-pipestream-s16` |
| [ai-pipestream/turbovec-search](https://github.com/ai-pipestream/turbovec-search) | Distributed hybrid search: sharded vector + BM25 nodes, coordinator with floor sharing, write-ahead log, offline resharding | fork branch `turbovec-pipestream-s16` |
| [ai-pipestream/grpc-opennlp-analysis](https://github.com/ai-pipestream/grpc-opennlp-analysis) | Text-analysis sidecar: sentence/token spans, term vectors, static embeddings, served over gRPC | — |

The repository builds two binaries:

- `turbovec-grpc` serves durable handle-addressed shards.
- `turbovec-coordinator` serves one exact collection across static shards.

The coordinator owns the only global top-k heap. Shards stream candidates and
receive a monotonically rising inclusive floor. A result is returned only when
every shard certifies completion. There is no partial-result mode.

## Scope

This project is the network and sharding facade for local turbovec semantics.
It owns vector RPCs, deadlines and cancellation, global heaps and floors,
completion, replicas, topology, persistence, and encoded-row movement. It does
not own document schemas, CEL, BM25, text analysis, model serving, facets,
hybrid ranking, or corpus pipelines. Those are search-product concerns owned by
`turbovec-search` and ProtoMolt.

An earlier revision of this repository carried a transitional `Documents`
service (descriptor-derived schemas, stored scalar columns, CEL filtering,
parent/chunk scopes). It has been removed: the facade accepts plain slot masks
and allowlists compiled by its caller and never interprets a document schema.
Git history preserves the implementation as migration material for
`turbovec-search`. A shard generation persisted with a bound schema is not
restorable by this binary; rebuild it as a plain vector shard.

## Build and test

Rust 1.89 and `protoc` are required.

```bash
cargo build --release --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

The canonical facade contracts are
[`turbovec.proto`](proto/turbovec/v1/turbovec.proto) and
[`coordinator.proto`](proto/turbovec/v1/coordinator.proto). Language examples
generate directly from the canonical node proto, so there are no per-example
copies to drift.

## Run a durable node

```bash
TURBOVEC_DATA_DIR=/var/lib/turbovec \
TURBOVEC_GRPC_ADDR=0.0.0.0:50051 \
cargo run --release --locked --bin turbovec-grpc
```

For a local disposable demo, set `TURBOVEC_ALLOW_EPHEMERAL=true` instead of a
data directory.

Node methods are separated into two gRPC services:

| Service | Methods |
|---|---|
| `TurboVecQuery` | metadata, calibration read, `Search`, `SearchStream`, `StreamSearch` |
| `TurboVecAdmin` | create/delete, retry-safe `Add`, remove, calibration write, `Flush`, streaming row export/import |

`Snapshot` and `Load` server-path RPCs do not exist. `Flush` writes an atomic,
checksummed generation below `TURBOVEC_DATA_DIR`, including stable row labels
and retry-safe ingest metadata. Startup restores the exact stable shard ids.

Retry-safe ingest sets `operation_id`, `expected_len`, and `expected_rows` on
the first `Add` frame. The bounded operation is validated before mutation and
flushed before success. A retry after a lost response or restart is replayed
without duplicating rows.

## Run the coordinator

The node table contains one shard per comma or newline-separated entry:

```text
primary|replica1|replica2  stable-index-id  durable-generation
```

The replica list and generation are optional. A replica is eligible only when
it serves the same index id at exactly the required durable generation.

```bash
TURBOVEC_COORD_ADDR=0.0.0.0:50050 \
TURBOVEC_COORD_NODES=@/etc/turbovec/nodes \
TURBOVEC_COORD_STATE=/var/lib/turbovec/topology.json \
cargo run --release --locked --bin turbovec-coordinator
```

`Split` and `Join` pipe bounded encoded row frames without decoding vectors.
Targets are fully validated and flushed before a new topology generation is
published. The coordinator state survives restart and cannot silently fall
back to the startup table.

A fresh node can announce itself instead of being pre-listed: start it with
`TURBOVEC_COORD_ADDR` (and `TURBOVEC_ADVERTISE_ADDR` when it listens on
`0.0.0.0`) and it registers on startup, re-announcing every
`TURBOVEC_REGISTER_INTERVAL_MS` (default 30 s). The coordinator dials it back,
holds it in the spare pool persisted with the topology, and reports it through
`ListNodes`. Registration never changes the serving topology: rows reach a
spare only when an operator names it as a `Split` or `Join` target.

## Production behavior

- gRPC frames default to 16 MiB; row export frames target 2 MiB.
- `k`, query count, ingest coordinates, scan concurrency, batch-query
  concurrency, and deadlines are bounded and configurable.
- Floor updates are conflated so slow shards receive the newest bound rather
  than an update backlog.
- Cancellation is polled at every engine chunk, even when the floor suppresses
  all candidates.
- Health readiness continuously validates durable node state or complete
  coordinator shard agreement.
- Logs are structured JSON. Set `RUST_LOG` for filtering.
- Set `TURBOVEC_METRICS_ADDR=0.0.0.0:9090` for OpenMetrics at `/metrics`.
- TLS and authentication belong at a gRPC-aware proxy or service mesh. Authorize
  `TurboVecQuery` and `TurboVecAdmin` independently.

See [architecture](docs/architecture.md) for invariants,
[deployment](docs/deployment.md) for configuration, probes, sizing, and the
container boundary, and [scaling](docs/scaling.md) for the metadata protocol,
the wire-compatibility policy, and the autoscaling roadmap.

## Examples

Examples are available for Java, TypeScript, Python, Go, and Rust under
[`examples/`](examples). They use the separated query and admin services. The
Rust gRPC engine and wire contract are the release gate; the packaged Python
wrapper comes afterward.
