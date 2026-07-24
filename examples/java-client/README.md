# java-client

A JVM demo for the `turbovec-grpc` server. It exists to show two things a Rust or
Python surface cannot show on their own.

**Speed from another language.** The index is built and queried entirely from
Java over gRPC: client-stream a corpus in, then time top-k search. You get ingest
throughput and query latency (p50/p95/p99) measured from the client.

**A binary contract that cannot corrupt the payload.** Most vector stores are
reached over JSON REST. JSON has no binary number type, so every number is parsed
into a 64-bit IEEE-754 double. That has two consequences the demo makes concrete:

- **uint64 ids above 2^53 are silently rounded.** A double has 53 mantissa bits,
  so id `9007199254740993` comes back as `...992`, and snowflake-scale ids collide
  routinely. This is exactly why proto3's own JSON mapping encodes int64/uint64 as
  strings. turbovec returns your ids as protobuf varints, so they survive exactly,
  in every language, with no configuration.
- **float32 fidelity becomes a serializer setting.** protobuf puts the raw 4-byte
  IEEE-754 value on the wire, so the server receives byte-for-byte what the client
  sent. A JSON producer that trims to 6 significant digits drifts some values and
  not others. Full-precision JSON can round-trip a float, but protobuf needs no
  such care. (turbovec quantizes storage by design; the point here is the wire.)

## Run

Start the server (from the repo root):

```bash
cargo run -p turbovec-grpc
```

It listens on `0.0.0.0:50051`; override with `TURBOVEC_GRPC_ADDR`.

Then run the demo (from this directory). Stubs are generated from `./proto` at
build time; nothing generated is checked in.

```bash
mvn -q compile exec:java
mvn -q compile exec:java -Dexec.args="1000000 768 2000"   # vectors dim queries
TURBOVEC_GRPC_ADDR=host:port mvn -q compile exec:java
```

Defaults: 100,000 vectors, dim 768, 2,000 queries, 4-bit, top-10.

## What you will see

Five blocks: ingest throughput, single-query latency percentiles and QPS, a
server-streaming search, then the id and float fidelity tables. In the id table
the JSON column reads `LOST` at and above 2^53 while the gRPC column stays `ok`.

## Notes

- The Java stubs come from a demo-local copy of the crate proto (`./proto`) with
  `java_package` / `java_multiple_files` added; the crate proto stays Rust-only.
- Add frames are kept under the 4 MB gRPC message limit. For very large corpora a
  production client would add flow control (`ClientCallStreamObserver`); this demo
  keeps the streaming loop simple.
