# turbovec-client

A thin Python client for a turbovec collection: one index, however many
machines it is spread over.

```python
from turbovec_client import connect

with connect("127.0.0.1:50050") as collection:
    for neighbour in collection.search(query, k=10):
        print(neighbour.id, neighbour.score)
```

Nothing in the API says how many nodes there are or which one a row came from,
and the scores are the scores a single index holding every row would have
returned, bit for bit. The two calls that do name nodes, `split` and `join`,
exist to move rows between them.

`grpcio` is the only runtime dependency. `grpcio-tools` is needed once, to
generate the stubs.

## The API

| Call | What it does |
|---|---|
| `connect(address)` | Connect to a coordinator; returns a `Collection`. Also a context manager. |
| `collection.search(vectors, k)` | Top-k for one query or a batch. Returns `Neighbour(id, score)`. |
| `collection.calibrate(sample, dim, bit_width)` | Fit one calibration and commit it to every node. |
| `collection.split(source, targets)` | Spread one index's rows across nodes, and serve the result. |
| `collection.join(target)` | Combine the nodes back into one index on `target`. |
| `collection.health()` | Whether the collection is servable, its row count, and per-node state. |

Failures come back as `CollectionError`, carrying a stable `name` and a
`detail`. The server refuses rather than degrading, so a collection whose nodes
are calibrated differently, or one missing a node mid-query, raises instead of
returning a shorter or subtly wrong result:

```python
try:
    collection.search(query, k=10)
except CollectionError as e:
    if e.name == "mixed_calibration":
        ...
```

`search(..., allow_partial=True)` opts out of that for unreachable nodes only.
It does not make a collection that does not add up servable.

## Install

The stubs are generated from the vendored proto in `./proto` and are not
checked in, the same way the crate's own generated code is produced at build
time rather than committed.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
./gen_stubs.sh
.venv/bin/pip install -e .
```

Regenerate whenever the proto changes. Importing `turbovec_client` before
generating them fails with that instruction rather than a bare import error.

## Run the example

`example.py` calibrates a collection, fills it, splits it across the nodes the
coordinator knows about, searches it, and joins it back. Start three nodes and
a coordinator first:

```bash
# from the repo root, in three shells or with setsid
TURBOVEC_GRPC_ADDR=127.0.0.1:51051 cargo run --release --bin turbovec-grpc
TURBOVEC_GRPC_ADDR=127.0.0.1:51052 cargo run --release --bin turbovec-grpc
TURBOVEC_GRPC_ADDR=127.0.0.1:51053 cargo run --release --bin turbovec-grpc

TURBOVEC_COORD_ADDR=127.0.0.1:51050 \
TURBOVEC_COORD_NODES='127.0.0.1:51051,127.0.0.1:51052,127.0.0.1:51053' \
  cargo run --release --bin turbovec-coordinator
```

Then, from this directory:

```bash
.venv/bin/python example.py 127.0.0.1:51050
.venv/bin/python example.py 127.0.0.1:51050 100000 768   # vectors dim
```

```text
turbovec collection - connected to 127.0.0.1:51050

[1] the collection is 3 node(s)
    http://127.0.0.1:51051  0 rows  ok
    http://127.0.0.1:51052  0 rows  ok
    http://127.0.0.1:51053  0 rows  ok

[2] fitting one calibration from 1,024 sample rows
    committed to every node

[3] indexing 20,000 vectors of dim 128 at 4-bit
    added 20,000 in 0.25s

[4] splitting across 3 nodes
    rows per node: 6,667, 6,667, 6,666
    collection holds 20,000 rows, servable=True

[5] top-10 search, one query at a time
    200 queries  =  4,533 queries/sec
    latency  p50 0.22 ms   p95 0.25 ms   p99 0.27 ms
    best neighbour: id 14781, score 14.6680

[6] a batch of 4 queries
    query 0 -> 10 neighbours, best 12.7060
    query 1 -> 10 neighbours, best 15.6245
    query 2 -> 10 neighbours, best 15.6247
    query 3 -> 10 neighbours, best 17.8826

[7] joining back onto http://127.0.0.1:51051
    combined index holds 20,000 rows
    still searchable: 10 neighbours
```

## Notes

- **Row ids.** For a collection built by `split` or `join`, `Neighbour.id` is
  the id the row had in the index it came from, and it does not change when the
  row moves between nodes. For a collection assembled by hand out of indexes
  that carry no ids, it is the row's slot within whichever index holds it,
  which is unique across the collection only if you have arranged for it to be.
- **Writing.** Adding vectors is a node-level call, not a collection-level one:
  a collection is what you search, and it is assembled out of indexes that
  already exist. `example.py` shows the node stub being used directly for the
  fill, alongside the collection handle for everything else.
- **This is not the `turbovec` pip package.** Those bindings embed an index in
  your process. This talks to a coordinator over the network, over a collection
  that may be larger than any one machine.
- **Not the single-node example.** [`examples/python-client`](../../examples/python-client)
  drives one `turbovec-grpc` server directly and is the place to start if one
  machine is enough.
