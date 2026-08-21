# VIBE benchmarks

This directory documents how to run the VIBE benchmark harness that lives at
`examples/vibe-bench/`. The harness is a Cargo example, so its extra
dependencies (a pure-Rust HDF5 reader and an HTTPS client) are dev-only and
never land in the server binaries.

The HDF5 files are read with [hidefix](https://github.com/gauteh/hidefix);
its `static` feature builds the bundled HDF5 C library, so no system
libhdf5 is required. Bulk dataset reads go through hidefix's own
pure-Rust reader; the root attributes, which hidefix's index API does not
expose, are read through the same vendored HDF5 build.

## What VIBE is

VIBE (<https://github.com/vector-index-bench/vibe>, arXiv 2505.17810) is an
ANN benchmark derived from ann-benchmarks. It publishes precomputed datasets
as HDF5 files at

```text
https://huggingface.co/datasets/vector-index-bench/vibe/resolve/main/{NAME}.hdf5
```

over plain HTTPS (one redirect to a CDN, no auth). Each file carries:

- root attributes `distance` (`euclidean` | `cosine` | `normalized` | `ip` |
  `hamming`), `dimension`, and `point_type` (`float` | `uint8` | `int8` |
  `binary`);
- datasets `train` (N x dim float32), `test` (M x dim float32), `neighbors`
  (M x 100 integer row indices into `train`, sorted by distance) and
  `distances` (M x 100 float32), all contiguous and uncompressed.

The harness supports `point_type = "float"` files only and rejects the rest
by name. turbovec's score is an inner product (the encoder stores a unit
direction plus the row's norm, and the kernel folds the norm back in), so
the `distance` attribute maps as follows:

| VIBE distance | How the harness runs it |
|---|---|
| `ip`, `normalized` | inner product directly (`normalized` rows are unit-norm, so this is also cosine) |
| `cosine` | train and test rows are L2-normalized at load, then inner product |
| `euclidean`, `hamming` | refused with a clear error: not expressible as an inner product without changing the ranking |

Good datasets to start with: `yi-128-ip` (187,843 x 128, ~100 MB download)
for smoke runs, `yahoo-minilm-384-normalized` (677,305 x 384) for a more
realistic target. Avoid the multi-GB files (`dpr-jina-*`, `msmarco-*`)
unless you mean it.

## Running

```bash
# local mode: build a turbovec index in-process
cargo run --release --example vibe-bench -- --dataset yi-128-ip

# subset for a quick pass: first 50k train rows, 500 queries
cargo run --release --example vibe-bench -- --dataset yi-128-ip \
    --max-train 50000 --max-queries 500
```

Options: `--cache-dir` (default `~/.cache/turbovec-vibe`; the `.hdf5` is
downloaded there once and reused), `--mode local|node|coordinator`,
`--node-addr`, `--coordinator-addr`, `--k` (headline recall depth, default
10), `--max-train`, `--max-queries`, `--calibration-sample` (default
25000), `--provision` (coordinator mode: create the empty shard index on
`--node-addr` when the node holds none), and `--out FILE` for the JSON
report (stdout otherwise).

`--max-train` keeps the ground truth honest: a query whose full published
top-100 names any row past the cut is dropped, and the drop count is
printed. Every run fetches `max(k, 100)` neighbours so recall@1, recall@k
and recall@100 come out of the same timed searches.

Every mode calibrates first (a deterministic, evenly spaced sample of the
train rows), then adds all rows, then searches: a row's codes are a
function of the row and the calibration pair, so this order is the
calibrate-then-fill contract the engine documents. Ingest uses plain adds
on purpose: the retry-safe envelope persists a durable generation per
operation and conflicts with pinned-generation topologies. Keep the
autoscaler off while benchmarking; a split shard refuses further adds.

Recall is approximate by construction: 4-bit quantization trades recall
for speed, and the harness reports recall rather than asserting exact rank
equality.

### node mode

Against one running node:

```bash
TURBOVEC_ALLOW_EPHEMERAL=true ./target/release/turbovec-grpc &
cargo run --release --example vibe-bench -- --dataset yi-128-ip \
    --mode node --node-addr 127.0.0.1:50051
```

The harness creates a positional index, fits the calibration pair locally
(turbovec's fit is deterministic) and commits it with `SetCalibration`,
streams the train rows in with plain `Add` frames, then times one unary
`Search` per query and drops the index at the end.

### coordinator mode

The coordinator has no add path and pins its topology at startup, so this
mode expects a provisioned one-shard collection: one node holding one
empty positional index that the coordinator's node table names. The
harness calibrates through the coordinator (`FitCalibration` fits one pair
and broadcasts it), adds rows directly to the shard's node, and times
`Coordinator.Search` per query, reading each neighbour's label (or slot
when the shard is unlabelled) as the train row id. A populated or
multi-shard collection is refused rather than silently benchmarked stale.

A minimal local fleet, two terminals (ports moved off the 5005x defaults
here only because those were occupied on the writer's machine):

```bash
# terminal 1: ephemeral node + coordinator pointing at its sole index
TURBOVEC_ALLOW_EPHEMERAL=true TURBOVEC_GRPC_ADDR=127.0.0.1:50071 \
    ./target/release/turbovec-grpc
TURBOVEC_ALLOW_EPHEMERAL=true TURBOVEC_COORD_ADDR=127.0.0.1:50070 \
    TURBOVEC_COORD_NODES=127.0.0.1:50071 ./target/release/turbovec-coordinator
```

Then run with `--provision`, which creates the empty shard index on the
node itself (the coordinator resolves a nameless node-table entry to the
node's sole index at bind time, so creating it before the first
coordinator call is enough):

```bash
cargo run --release --example vibe-bench -- --dataset yi-128-ip \
    --mode coordinator --coordinator-addr 127.0.0.1:50070 \
    --node-addr 127.0.0.1:50071 --provision --max-queries 100
```

A durable fleet pins every shard to an index id and a flushed generation,
so `--provision` does not apply there; provision it externally instead.
To point at the compose-demo fleet, run its setup phase so the topology
exists, then stop before the loader ingests (or run the loader with
`DEMO_ROWS` tiny and accept its vectors instead of VIBE's): the collection
must be empty when the harness starts. In practice that means
`docker compose up -d node1 coordinator` against a fresh volume, creating
node1's index at the dataset's dim, then `--coordinator-addr
localhost:50050`. See `examples/compose-demo/README.md` for the topology
details.

## Output

A human-readable table (dataset, mode, rows, dim, k, recall@1/k/100,
mean/p50/p95/p99 single-query latency, QPS, ingest and calibration wall
times) plus the same numbers as JSON on stdout or in `--out FILE`.

## Comparability caveat

VIBE's official leaderboard numbers come from single-core, containerized
runs under the benchmark's own Docker harness. These runs use local cores,
release builds, gRPC where selected, and no container CPU cap, so the
numbers here are not directly comparable to the VIBE leaderboard. Treat
them as turbovec-vs-turbovec (local vs node vs coordinator, or change over
time), not as leaderboard entries.
