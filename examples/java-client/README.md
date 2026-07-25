# java-client

Two JVM demos for the `turbovec-grpc` server.

**Speed from another language** (`demo.TurboVecDemo`). The index is built and
queried entirely from Java over gRPC: client-stream a corpus in, then time
top-k search. You get ingest throughput and query latency (p50/p95/p99)
measured from the client. Defaults match the TypeScript, Python, Go, and Rust
examples, so numbers are comparable across languages.

**A binary contract that cannot corrupt the payload** (`demo.WireFidelityDemo`).
Most vector stores are reached over JSON REST. JSON has no binary number type,
so every number is parsed into a 64-bit IEEE-754 double. That has two
consequences the demo makes concrete:

- **uint64 ids have no native JSON type.** Send a large id as a JSON number and it
  rounds the moment a client routes it through a double: JavaScript's `JSON.parse`,
  Gson to `Object`, any `double` field. Distinct ids past 2^53 then collide, and a
  lookup for one record returns another. The fixes are a typed integer field or a
  string id (what proto3's own JSON mapping does), but each is a convention every
  client in the fleet has to honor. turbovec's `uint64` is a real type, exact in
  every generated client with nothing to remember.
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

Then run the demos (from this directory). Stubs are generated from `./proto`
at build time; nothing generated is checked in.

```bash
mvn -q compile exec:java                                # speed test
mvn -q compile exec:java -Dexec.args="100000 768 2000"  # vectors dim queries
mvn -q compile exec:java -Dexec.mainClass=demo.WireFidelityDemo
TURBOVEC_GRPC_ADDR=host:port mvn -q compile exec:java
```

Defaults: 20,000 vectors, dim 128, 500 queries, 4-bit, top-10.

## What you will see

The speed test prints three blocks: ingest throughput, single-query latency
percentiles and QPS, then a server-streaming search.

The fidelity demo prints three more: the id table, an id collision, then the
float table.

The id table shows one large id under three JVM client setups. Both JSON columns
use a real Jackson `ObjectMapper` on the same bytes and differ only in the target
type: read into a `double` it rounds at and above 2^53, read into a `long` it is
exact, and gRPC's `uint64` is exact. So JSON can be correct on the JVM, but only
if every client remembers to keep the id a `long` or a string.

The collision block goes further: it stores two distinct ids (2^53 and 2^53+1),
shows the double path fold them onto one number, then asks the server for the
second one both ways. Through a double the server hands back the first id instead,
so one record silently shadows the other; a typed long or gRPC keeps them apart.

## Notes

- The Java stubs come from a demo-local copy of the crate proto (`./proto`) with
  `java_package` / `java_multiple_files` added; the crate proto stays Rust-only.
- Add frames are kept under the 4 MB gRPC message limit. For very large corpora a
  production client would add flow control (`ClientCallStreamObserver`); this demo
  keeps the streaming loop simple.
