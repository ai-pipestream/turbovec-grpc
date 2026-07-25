# python-client

A Python demo for the `turbovec-grpc` server. It shows the index reached from
another language over gRPC: client-stream a corpus in, then time top-k search.
You get ingest throughput and query latency (p50/p95/p99) measured from the
client, with nothing but `grpcio` as a dependency.

Because the wire is protobuf, `uint64` ids and `float` coordinates arrive
byte-for-byte. Python's ints are arbitrary precision, so a uint64 id comes
back exact with nothing to remember — no 2^53 rounding of the kind a
JSON/double client has to watch for.

## Run

Start the server (from the repo root):

```bash
cargo run -p turbovec-grpc
```

It listens on `0.0.0.0:50051`; override with `TURBOVEC_GRPC_ADDR`.

Then run the demo (from this directory). The stubs are generated from
`./proto` by `gen_stubs.sh`; nothing generated is checked in.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
./gen_stubs.sh                     # or: .venv/bin/python -m grpc_tools.protoc ...
.venv/bin/python demo.py
.venv/bin/python demo.py 100000 768 2000        # vectors dim queries
TURBOVEC_GRPC_ADDR=host:port .venv/bin/python demo.py
```

Defaults: 20,000 vectors, dim 128, 500 queries, 4-bit, top-10 — finishes in
well under a minute.

## What you will see

Three blocks: ingest throughput, single-query latency percentiles and QPS,
then a server-streaming search, plus a one-line note on uint64 id fidelity.

```text
turbovec-grpc demo — connected to 127.0.0.1:50051

[1] indexing 20,000 vectors of dim 128 at 4-bit
    added 20,000 in 1.04s  =  19,215 vectors/sec  (10 MB of raw float32 sent)
    server reports 20,000 vectors in the index

[2] top-10 search, one query at a time
    500 queries  =  81 queries/sec served (single client thread)
    latency  p50 12.16 ms   p95 13.73 ms   p99 14.76 ms
    ids come back as exact Python ints, e.g. 10440 — no 2^53 rounding as with a JSON/double wire

[3] server-streaming search, batch of 4 queries
    query 0 -> 10 neighbours, best score 15.7717
    query 1 -> 10 neighbours, best score 14.3152
    query 2 -> 10 neighbours, best score 14.6315
    query 3 -> 10 neighbours, best score 15.2207
```

## Python notes

- **Why a venv.** The demo needs `grpcio` and `grpcio-tools`, and current
  Python distributions ship without pip or with an externally-managed
  environment. A venv in `.venv/` keeps those packages local to the example
  and off the system interpreter. (If pip is unavailable entirely, `uv venv`
  plus `uv pip install -r requirements.txt` works the same way.)
- **Generated stubs are build artifacts.** `gen_stubs.sh` runs
  `grpc_tools.protoc` over the vendored copy of the crate proto (`./proto`)
  and writes `turbovec_pb2.py` / `turbovec_pb2_grpc.py` into `./generated`.
  Both `generated/` and `.venv/` are gitignored; regenerate the stubs whenever
  the proto changes.
- **This is not the `turbovec` pip package.** The `turbovec-python` bindings
  embed the index in your process — no server, no network, the index lives and
  dies with the Python program. This example talks to the `turbovec-grpc`
  server instead: a remote, shared process that many clients (in any language
  with a gRPC stub) can add to and search concurrently, and that keeps serving
  after any one client exits.
- Add frames are kept well under the 4 MB gRPC message limit. For very large
  corpora a production client would chunk by byte size and add backpressure;
  this demo keeps the streaming loop a simple generator.
