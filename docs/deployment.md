# Deploying turbovec-grpc

## Required state

Production node processes require `TURBOVEC_DATA_DIR`. Production coordinators
require `TURBOVEC_COORD_STATE`. Startup fails when either is absent. Set
`TURBOVEC_ALLOW_EPHEMERAL=true` only for demos and tests.

Use one durable volume per node. The coordinator state file also belongs on a
durable volume. Do not copy a live data directory as a replication mechanism;
copy an inactive, flushed generation or use a storage snapshot.

## Network boundary

The processes speak plaintext HTTP/2 and do not authenticate callers. Put a
gRPC-aware proxy or service mesh in front for TLS, identity, authorization,
rate limiting, and audit policy.

Authorize these service names separately:

- `turbovec.v1.TurboVecQuery` for search callers
- `turbovec.v1.TurboVecAdmin` for operators and the coordinator control path
- `turbovec.v1.Coordinator` for collection clients

Reflection is enabled. Disable it at the proxy if schema discovery is not
appropriate for the network.

## Health

Both binaries serve `grpc.health.v1.Health`.

- Node readiness is false when a persistent process has a live shard that has
  never been flushed.
- Coordinator readiness is false unless every shard is reachable and agrees
  on dimension, bit width, calibration, and required generation.
- Readiness is refreshed every five seconds. Liveness should probe the empty
  health service name; readiness should name the concrete query or coordinator
  service.

## Limits

| Variable | Default | Meaning |
|---|---:|---|
| `TURBOVEC_MAX_MESSAGE_BYTES` | 16777216 | gRPC encode/decode frame cap |
| `TURBOVEC_MAX_K` | 1000 | largest accepted top-k |
| `TURBOVEC_MAX_QUERIES` | 64 | queries in one request |
| `TURBOVEC_MAX_FRAME_COORDINATES` | 4000000 | coordinates in one ingest operation/frame budget |
| `TURBOVEC_MAX_CONCURRENT_SCANS` | host parallelism | node CPU admission slots |
| `TURBOVEC_MAX_CONCURRENT_QUERIES` | 4 | coordinator batch-query parallelism |
| `TURBOVEC_QUERY_TIMEOUT_MS` | 30000 | deadline per distributed query |

All configured counts must be positive. Invalid values fail startup.

## Observability

`RUST_LOG` controls JSON structured logs. The default level is `info`.

Set `TURBOVEC_METRICS_ADDR`, for example `0.0.0.0:9090`, to expose
OpenMetrics at `/metrics`. Binding failure fails process startup. Metrics cover
active scans, completed node and coordinator searches, errors, candidates,
chunks, ingested rows, and topology generation.

## Container

The image builds both binaries with Rust 1.89 and `--locked`, contains no BLAS
runtime, and runs as uid/gid 65532. The node is the default entrypoint:

```bash
docker build -t turbovec-grpc .
docker run --rm -p 50051:50051 \
  -v "$PWD/data:/var/lib/turbovec" \
  turbovec-grpc
```

Run the coordinator by overriding the entrypoint and mounting its node table
and state path. TLS remains the responsibility of the surrounding platform.

## CPU and memory

The engine is CPU-only. SIMD kernels are selected at runtime, so the image does
not require AVX2 as a baseline and can use AVX2, AVX-512, or NEON when present.
The scan semaphore should normally match the CPU limit assigned to the
container.

Quantized vector payload is approximately `dim * bit_width / 8` bytes per row,
plus scales, labels, index structures, and warm search caches. Resharding holds
the target index plus bounded transfer frames, but the coordinator never holds
the source or target shard in full.
