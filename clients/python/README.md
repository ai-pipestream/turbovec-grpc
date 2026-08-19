# turbovec-client

A thin Python client for turbovec over gRPC, in two shapes: one index on
one node, or one collection however many machines it is spread over.

```python
from turbovec_client import create_index

with create_index("127.0.0.1:50051", dim=128, bit_width=4) as index:
    index.add(vectors)
    for neighbour in index.search(query, k=10):
        print(neighbour.id, neighbour.score)
```

Code written against the embedded `turbovec` package reads the same here;
the index just lives in a server process. The scores are the scores that
embedded index would have returned, bit for bit, because the same engine
holds the rows. `grpcio` is the only runtime dependency. `grpcio-tools` is
needed once, to generate the stubs.

## Parity with the embedded API

| Embedded (`turbovec` pip package) | This client |
|---|---|
| `TurboQuantIndex(dim, bit_width)` | `create_index(address, dim, bit_width)` |
| `IdMapIndex(dim, bit_width)` | `create_index(address, dim, bit_width, id_mapped=True)` |
| `index.add(vectors)` | `index.add(vectors)` |
| `index.add_with_ids(vectors, ids)` | `index.add_with_ids(vectors, ids)` |
| `index.search(queries, k)` | `index.search(vectors, k)` |
| `index.remove(id)` | `index.remove(id)` |
| `index.write(path, durable=True)` | `index.flush()` — the node owns the path |
| `index.calibrate(sample)` | `collection.calibrate(sample, dim, bit_width)` — fitting lives on the coordinator |
| `len(index)` | `len(index)`, or `index.info()` for the full snapshot |

Where the network changes the meaning, the client says so rather than
impersonating the embedded API:

- **Persistence has no path.** A node owns `TURBOVEC_DATA_DIR`, restores
  its current generation at startup, and `flush()` is the call that
  persists. There is no `load(path)`; `open_index(address)` reattaches to
  what the node restored.
- **There is no `contains`.** The wire has no membership RPC.
- **There is no node-level `calibrate(sample)`.** Fitting a pair from a
  sample is engine work the node surface does not expose — it only commits
  a pair fitted elsewhere. The coordinator's `FitCalibration` fits one pair
  and commits it to every node, which is what `Collection.calibrate` calls.
- **Adds are retry-safe.** Each `add` carries a fresh operation id plus the
  exact row count and prior length, and the node commits the stream as one
  operation under that id. If the response is lost, repeat the call with
  `operation_id=` set to the returned value and the node answers the
  committed result without adding the rows again. A node without
  `TURBOVEC_DATA_DIR` refuses the retry-safe envelope by name; pass
  `operation_id=None` for the plain ingest such a node accepts.
- **Results are dataclasses, not numpy arrays.** `search` returns
  `Neighbour(id, score)` rows. Inputs may be numpy arrays — anything
  sequence-like or with `.tolist()` — but numpy is never required.

## The collection API

A collection is what you search across machines. Connect to a coordinator
and nothing in the API says how many nodes there are or which one a row
came from:

| Call | What it does |
|---|---|
| `connect(address)` | Connect to a coordinator; returns a `Collection`. Also a context manager. |
| `collection.search(vectors, k)` | Top-k for one query or a batch. Returns `Neighbour(id, score)`. |
| `collection.calibrate(sample, dim, bit_width)` | Fit one calibration and commit it to every node. |
| `collection.split(source, targets)` | Spread one index's rows across nodes, and serve the result. |
| `collection.join(target)` | Combine the nodes back into one index on `target`. |
| `collection.health()` | Whether the collection is servable, its row count, and per-node state. |

Failures come back as `CollectionError`, carrying a stable `name` and a
`detail`. The server refuses rather than degrading, so a collection whose
nodes are calibrated differently, or one missing a node mid-query, raises
instead of returning a shorter or subtly wrong result:

```python
try:
    collection.search(query, k=10)
except CollectionError as e:
    if e.name == "mixed_calibration":
        ...
```

If any shard is unavailable, the coordinator fails the search. It never
returns a partial ranking as if it were complete.

## Install

The stubs are generated from the vendored proto in `./proto` and are not
checked in, the same way the crate's own generated code is produced at
build time rather than committed.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
./gen_stubs.sh
.venv/bin/pip install -e .
```

Regenerate whenever the proto changes. Importing `turbovec_client` before
generating them fails with that instruction rather than a bare import error.

## Run the example

`example.py` drives one node in the shape of the embedded API: create an
id-mapped index, fill it, search it, remove a row, flush it. Start a node
first — retry-safe ingest and flush both need durable storage, so give it a
data dir rather than the ephemeral demo mode:

```bash
# from the repo root
TURBOVEC_DATA_DIR=/tmp/turbovec-demo cargo run --release --bin turbovec-grpc
```

Then, from this directory:

```bash
.venv/bin/python example.py
.venv/bin/python example.py 127.0.0.1:50051 100000 768   # address vectors dim
```

`example_collection.py` is the same walkthrough at collection scale: point
it at a running coordinator and it calibrates, fills, splits across the known
nodes, and searches — nothing in it mentions a shard.

```text
turbovec index - connected to 127.0.0.1:50051

[1] indexing 20,000 vectors of dim 128 at 4-bit
    added 20,000 in 0.16s  (operation aa7323ca…)

[2] top-10 search, one query at a time
    200 queries  =  7,844 queries/sec
    latency  p50 0.12 ms   p95 0.15 ms   p99 0.18 ms
    best neighbour: id 1018491, score 16.3624

[3] searching for a stored row
    row 1000000's top neighbour is itself: True

[4] removing that row
    removed 1000000: True
    again (already gone): False
    top neighbour now: id 1017877, score 15.0380

[5] flushing
    durable generation 2 holds 19,999 rows
```

## Run the tests

`tests/test_index.py` starts a real node binary on a scratch port with a
temporary data dir and exercises the whole `Index` surface against it,
including retry replay and flush-restart-restore. It uses the release build
at `target/release/turbovec-grpc` by default; point `TURBOVEC_NODE_BIN`
elsewhere to override. If no binary is found the tests skip and say why.

```bash
.venv/bin/python -m pytest tests/
```

## Notes

- **Row ids.** For a collection built by `split` or `join`, `Neighbour.id` is
  the id the row had in the index it came from, and it does not change when
  the row moves between nodes. For a collection assembled by hand out of
  indexes that carry no ids, it is the row's slot within whichever index
  holds it, which is unique across the collection only if you have arranged
  for it to be.
- **Writing is node-level.** A collection is what you search, and it is
  assembled out of indexes that already exist; `Index` is how you create
  and fill them. The coordinator moves rows between nodes, it does not take
  new ones.
- **This is not the `turbovec` pip package.** Those bindings embed an index
  in your process. This talks to servers over the network, over indexes
  that may be larger than any one machine.
