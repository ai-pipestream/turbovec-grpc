# go-client

A Go demo for the `turbovec-grpc` server. It exists to show two things a Rust
or Python surface cannot show on their own.

**Speed from another language.** The index is built and queried entirely from
Go over gRPC: client-stream a corpus in, then time top-k search. You get ingest
throughput and query latency (p50/p95/p99) measured from the client.

**A binary contract that cannot corrupt the payload.** Most vector stores are
reached over JSON REST, where every number routes through a 64-bit IEEE-754
double: `uint64` ids round at and above 2^53, and float32 fidelity becomes a
serializer setting. protobuf puts the raw 4-byte IEEE-754 value and the full
8-byte id on the wire, exact in every generated client with nothing to
remember. On the Go side the story is one line: Go's `uint64` is exact end to
end — no `float64` ever enters the picture, so there is no rounding hazard to
demonstrate, only the absence of one.

## Run

Start the server (from the repo root):

```bash
cargo run -p turbovec-grpc
```

It listens on `0.0.0.0:50051`; override with `TURBOVEC_GRPC_ADDR`.

Then run the demo (from this directory). The Go protobuf plugins are needed
once, for stub generation:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Stubs are generated from `./proto` into `./gen`; nothing generated is checked
in.

```bash
./gen_stubs.sh && go run .
go run . 1000000 768 2000          # vectors dim queries
TURBOVEC_GRPC_ADDR=host:port go run .
```

Defaults: 20,000 vectors, dim 128, 500 queries, 4-bit, top-10 — finishes in
well under a minute.

## What you will see

Four blocks: ingest throughput, single-query latency percentiles and QPS, a
server-streaming search, then the uint64 line. Real output from one run:

```
turbovec-grpc demo — connected to 127.0.0.1:50051

[1] indexing 20000 vectors of dim 128 at 4-bit
    added 20000 in 0.91s  =  21909 vectors/sec  (10 MB of raw float32 sent)
    server reports 20000 vectors in the index

[2] top-10 search, one query at a time
    500 queries  =  80 queries/sec served (single client goroutine)
    latency  p50 11.96 ms   p95 13.03 ms   p99 19.00 ms

[3] server-streaming search, batch of 4 queries
    query 0 -> 10 neighbours, best score 14.0916
    query 1 -> 10 neighbours, best score 15.9160
    query 2 -> 10 neighbours, best score 13.2756
    query 3 -> 10 neighbours, best score 15.1033

[4] uint64 ids: Go's uint64 is exact end to end — stored 1861392837450923417, server returned 1861392837450923417
```

The last block stores a snowflake-scale id and reads it back through a top-1
allowlist lookup: the digits survive exactly, because protobuf's `uint64` is a
real type on both sides of the wire.

## Go notes

- Stub generation needs the two protoc plugins, `protoc-gen-go` and
  `protoc-gen-go-grpc` (see Run above). `gen_stubs.sh` adds `$(go env
  GOPATH)/bin` to `PATH` so protoc finds them.
- Generated code under `./gen` is a build artifact and gitignored; rerun
  `gen_stubs.sh` whenever the proto changes.
- The demo uses `insecure.NewCredentials()` because this is a local demo
  against a plaintext listener. In production, TLS belongs in front of the
  server.
- Add frames are kept under the 4 MB gRPC message limit. For very large
  corpora a production client would add flow control; this demo keeps the
  streaming loop simple.
