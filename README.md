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

The `thin-coordinator` branch currently contains a `Documents` service and its
schema, scalar-column, CEL, and parent/chunk implementation. That code was
completed before this boundary was corrected. Preserve it while migrating the
useful mechanisms, but do not extend it here or treat it as the intended scope
of the facade.

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

Node methods are currently separated into three gRPC services:

| Service | Methods |
|---|---|
| `TurboVecQuery` | metadata, calibration read, `Search`, `SearchStream`, `StreamSearch` |
| `TurboVecAdmin` | create/delete, retry-safe `Add`, remove, calibration write, `Flush`, streaming row export/import |
| `Documents` | Transitional product-layer API awaiting migration to `turbovec-search` or a shared product crate |

`Snapshot` and `Load` server-path RPCs do not exist. `Flush` writes an atomic,
checksummed generation below `TURBOVEC_DATA_DIR`, including stable row labels
and retry-safe ingest metadata. Startup restores the exact stable shard ids.

Retry-safe ingest sets `operation_id`, `expected_len`, and `expected_rows` on
the first `Add` frame. The bounded operation is validated before mutation and
flushed before success. A retry after a lost response or restart is replayed
without duplicating rows.

## Transitional protobuf document implementation

This section records behavior that must not be lost during extraction. It does
not expand the long-term `turbovec-grpc` product boundary.

The `Documents` service indexes the protobuf messages producers already emit,
with no JSON and no hand-maintained field mapping. The contract is
[`schema.proto`](proto/turbovec/v1/schema.proto):

1. `PlanSchema` takes a serialized `google.protobuf.FileDescriptorSet`
   (compiled with `--include_imports`) plus a message type name and returns
   the derived indexing plan: dotted field paths, resolved kinds, and a
   SHA-256 fingerprint over the plan's canonical encoding. Two indexes agree
   on their schema exactly when their fingerprints agree.
2. `BindSchema` creates an id-mapped index shaped by that plan. `Flush`
   persists the bound schema with the shard generation; restart re-derives
   the plan and refuses to serve on a fingerprint mismatch.
3. `AddDocuments` streams serialized messages of the bound type. The node
   decodes each document against the bound descriptor and indexes its
   `(id, vector)` pair along with every planned scalar field's value. A
   broken or invalid stream commits no prefix. The stored field values
   persist with the shard generation (`documents.pb`, checksummed like
   every other section) and restore with it.
4. `SearchDocuments` runs a top-k vector search optionally restricted by a
   [CEL](https://cel.dev) filter over the planned fields, spelled the way
   the proto spells them:

   ```text
   price_cents < 5000 && meta.author == "kagome" && "legal" in tags
     && meta.created_at > timestamp("2020-01-01T00:00:00Z")
   ```

   The expression is evaluated against every document's stored values and
   the admitted labels become an exact allowlist for the vector search, so
   a filtered result is the true top-k of the admitted set — never an
   over-fetch heuristic. Enum fields compare by value name, unset proto3
   fields evaluate as their defaults, and hits report the original
   document id, not just its u64 label. An expression that does not
   parse, references an unplanned field, or does not evaluate to a
   boolean fails with `INVALID_ARGUMENT` naming the problem.

Because a schema-bound index stores field values beside every row, the raw
vector `Add` RPC refuses such indexes; ingest goes through `AddDocuments`,
and `Remove` drops a row's stored fields with the row.

Fields may carry explicit hints as descriptor options using the
`ai.pipestream.proto.index.hints.v1` extension (vendored byte-identically
from [protomolt](https://github.com/ai-pipestream/protomolt), which owns the
vocabulary): a proto annotated for protomolt's indexers works here without
modification. Unhinted fields are inferred from the descriptor. Ambiguity is
an error naming the fix, never a guess: the vector field is either the one
hinted `INDEX_FIELD_TYPE_VECTOR` or the only vector-shaped repeated float
field, and the document id is either the field hinted `BLOCK_ROLE_DOC_ID` or
a singular top-level field named `id`.

Integer ids are used verbatim (zero is refused, because proto3 cannot
distinguish it from unset). String ids reduce to the first 8 bytes of
SHA-256 over their UTF-8 bytes, big-endian — part of the wire contract, so
any client can predict the labels its documents will carry in search
results.

### Chunk scopes (parent / child)

A message may declare one `BLOCK_ROLE_CHUNKS` repeated field. The vector
must live inside that scope; the document id stays on the parent. Ingest
explodes each wire message into N chunk rows (unique labels from the
documented `parent_id‖chunk_id` hash) plus one parent record. Parent
scalars are denormalized onto every chunk row so CEL filters see
`title == "…" && chunks.ordinal == 0` without a join at query time.
`SearchDocuments` hits report `id` (parent), `chunk_id`, and `parent_label`.
`GetParents` resolves parent labels to the live chunk-row labels this node
holds; unknown labels are omitted so a coordinator can fan the same set to
every shard. `collapse_parents` ranks every admitted chunk and returns the
first `k` distinct parents in score order — `k` is then top-k parents, not
top-k chunks. `matched`/`total` stay in chunk rows. `parents.pb` persists
the parent table beside `documents.pb`.

### Sharded document collections

The coordinator serves the same surface over many shards. Its
`SearchDocuments` fans the query batch and the same CEL filter out to every
shard, where each one evaluates the filter over its own stored fields and
returns the exact top-k of the documents it admits; merging those lists by
score is the exact collection top-k, because the union of per-shard admitted
sets is the collection's admitted set. `matched`/`total` sum across shards.

On a chunked collection a second stream overlaps that search: as the first
shard's hits arrive, the coordinator fans `GetParents` for those parent
labels to every shard and unions membership, so a chunk need not share a
shard with its parent. `collapse_parents` asks each shard for its local
top-k parents (every admitted chunk is ranked before collapse); the
coordinator then re-collapses by parent, keeping the max score. That is the
collection's exact top-k parents. The RPC does not return until every shard
search and every parent lookup has finished.

A filter can only mean one thing when every shard agrees what a document is,
so schema agreement is part of collection binding: every shard must carry
the same schema fingerprint, or none may carry any. A collection where some
shards bind a schema and others do not — or bind different fingerprints —
is refused by name (`mixed_schema`), and `SearchDocuments` on a plain vector
collection is refused as `schema_required`. `ListNodes` reports each shard's
fingerprint.

Schema-bound shards need no calibration step to score comparably: an
uncalibrated index encodes each row as a pure function of the row (fixed
rotation and codebook from `(dim, bit_width)`, per-row scales), so the empty
pair is itself a shared calibration and the merge is exact. `Split`/`Join`
refuse id-mapped shards by name — turbovec's `IdMapIndex` does not expose
the encoded rows those calls move — so a document collection is resharded by
re-ingesting, for now.

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

See [architecture](docs/architecture.md) for invariants and
[deployment](docs/deployment.md) for configuration, probes, sizing, and the
container boundary.

## Examples

Examples are available for Java, TypeScript, Python, Go, and Rust under
[`examples/`](examples). They use the separated query and admin services. The
Rust gRPC engine and wire contract are the release gate; the packaged Python
wrapper comes afterward.
