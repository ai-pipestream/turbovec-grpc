# Thin Coordinator Architecture

## Purpose

`turbovec-grpc` is the minimal distributed vector engine around `turbovec`.
The fork supplies only chunked streaming scan control. This repository owns
gRPC, sharding, exact global top-k, persistence, redistribution, limits, and
failure behavior.

It intentionally does not own documents, text analysis, BM25, embedding
generation, or corpus pipelines. Those remain separate services. Pipestream
Search is where hybrid and document-oriented mechanisms belong.

## Processes and services

```text
search client
    |
    | Coordinator.Search
    v
turbovec-coordinator                 topology.json
  one global top-k heap                   |
    |          |          |               |
    +----------+----------+---------------+
      StreamSearch with a conflated live floor
    |          |          |
    v          v          v
 shard node  shard node  shard node
    |          |          |
 TurboQuantIndex generations below TURBOVEC_DATA_DIR
```

The coordinator may also be linked into its caller as a Rust library. In that
shape the caller invokes the same `CoordinatorService` implementation in
memory, so the first arrow above disappears. Coordinator-to-shard calls remain
gRPC because they cross ownership and usually host boundaries. The standalone
server remains available when the coordinator needs an independent lifecycle
or authorization boundary.

Node RPCs are split by authorization boundary:

- `TurboVecQuery` contains metadata, calibration reads, unary search, and the
  bidirectional distributed scan.
- `TurboVecAdmin` contains create, delete, ingest, calibration writes, flush,
  and encoded row movement.
- `Coordinator` is the collection-level client surface.

The query and admin services share a listener today. Production deployments
authorize them independently at the gRPC proxy or service mesh by full service
name. There is no in-process TLS or authentication.

## Exact search

Every shard in a collection must have the same dimension, bit width, and
coordinate-for-coordinate TQ+ calibration. The coordinator probes and pins
that contract before search.

For each query it opens one `StreamSearch` per shard. Nodes scan in SIMD-sized
chunks and emit candidates at or above the inclusive floor. The coordinator
owns the only top-k heap. Once it has `k` candidates, its k-th score becomes a
safe lower bound and is broadcast to unfinished shards. Only the latest floor
is retained. The engine polls floor and cancellation state at every chunk,
including chunks that emit nothing.

The result is returned only after every shard certifies `completed=true`.
There is no partial-result mode. A missing shard, deadline, protocol error,
shape mismatch, or calibration mismatch fails the request.

Batch queries execute concurrently up to `TURBOVEC_MAX_CONCURRENT_QUERIES`.
Node scans are admitted by `TURBOVEC_MAX_CONCURRENT_SCANS` so CPU work cannot
grow without bound.

Rows imported or redistributed with labels can be queried in stable-label
order and restricted by a stable-label admitted set. Each node resolves that
set against its local label table into a positional mask before the scan. This
keeps product identity stable when Split or Join changes shard and slot. An
explicit presence bit makes an empty set match nothing; omitting the set keeps
the unfiltered behavior. Tie-complete calls retain every candidate at the
final inclusive k-th score while discarding candidates below each rising
floor.

## Durable shard identity

An index handle is a stable shard UUID. `Flush` writes an immutable generation:

```text
TURBOVEC_DATA_DIR/
  <shard-id>/
    CURRENT
    gen-00000000000000000001/
      index.tv
      labels.le64
      manifest.json
```

The manifest records shape, row count, calibration bits, checksums, labels,
and the last retry-safe ingest operation. Files and directories are synced
before `CURRENT` is atomically replaced. Startup restores only `CURRENT` and
fails on identity, checksum, shape, calibration, or label drift. Delete first
renames the shard to a tombstone and syncs the data root, so restart cannot
resurrect it.

Retry-safe ingest uses `operation_id`, `expected_len`, and `expected_rows`.
The complete bounded stream is validated before mutation, committed as one
operation, and flushed before success. Retrying the same operation after a
lost response or process restart returns the committed result without adding
the rows again.

## Topology generations and replicas

`TURBOVEC_COORD_STATE` stores the active topology generation atomically. A
Split or Join builds all target shards, flushes them, reads their durable shard
generations back, then publishes the new topology. Restart loads this file
instead of reverting to startup membership.

Static replicas can be listed with a primary address. Failover is read-only
and eligible only when the topology names a durable generation and the replica
serves the same stable index id at exactly that generation. A merely reachable
but stale replica is ignored. There is no discovery, election, or automatic
replication protocol.

## Redistribution

Split and Join move packed codes, correction scales, labels, and calibration.
They never decode or re-encode vectors, so scores remain bit-identical.

Export is server-streaming in bounded frames. Import is client-streaming and
declares an exact expected row count. A target becomes visible only after all
frames validate and the row count matches. The coordinator pipes frames from
source to target with bounded channels and never holds a whole shard.

## Operations

- Standard gRPC health reflects live shard durability on nodes and complete
  collection agreement on coordinators. It is refreshed every five seconds.
- JSON logs include one gRPC span per request and shared distributed request
  ids across coordinator and shard scans.
- `TURBOVEC_METRICS_ADDR` enables an OpenMetrics `/metrics` listener.
- Messages default to 16 MiB. Export frames target 2 MiB. `k`, query count,
  vector coordinates, scan concurrency, and deadlines are configurable and
  enforced before expensive work.
