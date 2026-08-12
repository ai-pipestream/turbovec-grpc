# turbovec-grpc

Minimal CPU-only distributed vector search over
[`turbovec`](https://github.com/RyanCodrai/turbovec).

This is a sister project, not code embedded in the upstream engine. It uses the
`ai-pipestream/turbovec` fork branch `turbovec-pipestream-s14`, whose small
patch exposes live-floor streaming scans and chunk-boundary control. All gRPC,
persistence, sharding, topology, and failure behavior lives here.

The repository builds two binaries:

- `turbovec-grpc` serves durable handle-addressed shards.
- `turbovec-coordinator` serves one exact collection across static shards.

The coordinator owns the only global top-k heap. Shards stream candidates and
receive a monotonically rising inclusive floor. A result is returned only when
every shard certifies completion. There is no partial-result mode.

## Scope

This project owns vector search only. It does not own a document store, BM25,
text analysis, model serving, or corpus ingestion pipeline. Those concerns stay
in separate services or in the larger `turbovec-search` project.

## Build and test

Rust 1.89 and `protoc` are required.

```bash
cargo build --release --locked
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

The canonical contracts are
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

See [architecture](docs/architecture.md) for invariants and
[deployment](docs/deployment.md) for configuration, probes, sizing, and the
container boundary.

## Examples

Examples are available for Java, TypeScript, Python, Go, and Rust under
[`examples/`](examples). They use the separated query and admin services. The
Rust gRPC engine and wire contract are the release gate; the packaged Python
wrapper comes afterward.
