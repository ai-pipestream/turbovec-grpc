# ts-client

A TypeScript/Node demo for the `turbovec-grpc` server. It exists to show two
things a Rust or Python surface cannot show on their own.

**Speed from another language.** The index is built and queried entirely from
Node over gRPC: client-stream a corpus in, then time top-k search. You get
ingest throughput and query latency (p50/p95/p99) measured from the client.

**The uint64 footgun, from the language where it bites hardest.** Every
JavaScript number is a 64-bit IEEE-754 double, so integers stop being exact at
2^53. A vector store reached over JSON REST has no way around this: send a
large id as a JSON number and `JSON.parse` rounds it, and distinct ids past
2^53 collide. protobuf puts a real `uint64` on the wire — but only the loader
option `longs: String` keeps that exactness all the way into your Node code,
by handing ids back as decimal strings instead of doubles. The demo makes both
halves concrete.

## Run

Start the server (from the repo root):

```bash
cargo run -p turbovec-grpc
```

It listens on `0.0.0.0:50051`; override with `TURBOVEC_GRPC_ADDR`.

Then run the demo (from this directory). The proto is loaded dynamically from
`./proto` at startup; there is no codegen step and nothing generated is
checked in.

```bash
npm install && npm start
npm start -- 100000 768 2000                # vectors dim queries
TURBOVEC_GRPC_ADDR=host:port npm start
```

Defaults: 20,000 vectors, dim 128, 500 queries, 4-bit, top-10. Finishes in
well under a minute.

## What you will see

Four blocks: ingest throughput, single-query latency percentiles and QPS, a
server-streaming search, then the uint64 footgun. Real output from a run
against a local server:

```
turbovec-grpc demo — connected to 127.0.0.1:50051

[1] indexing 20,000 vectors of dim 128 at 4-bit
    added 20,000 in 1.00s  =  19,944 vectors/sec  (10 MB of raw float32 sent)
    server reports 20,000 vectors in the index

[2] top-10 search, one query at a time
    500 queries  =  77 queries/sec served (single client)
    latency  p50 12.33 ms   p95 15.46 ms   p99 16.40 ms

[3] server-streaming search, batch of 4 queries
    query 0 -> 10 neighbours, best score 13.1278
    query 1 -> 10 neighbours, best score 15.3767
    query 2 -> 10 neighbours, best score 15.0730
    query 3 -> 10 neighbours, best score 13.8548

[4] the uint64 footgun (id = 2^53 + 1)
    Number("9007199254740993")       -> 9007199254740992  LOST
    JSON.parse("9007199254740993")   -> 9007199254740992  LOST
    gRPC search for that id  -> 9007199254740993  ok, exact string
    longs: String on the proto loader is what keeps uint64 ids exact in Node —
    JS numbers and JSON both stop at 2^53.
```

The footgun block stores one vector under the id 2^53 + 1, then shows the two
ways Node clients usually lose it: `Number()` and `JSON.parse()` both round it
to 2^53 (a different id — one that may belong to a different record). The gRPC
round trip returns the id digit-for-digit exact, as a string.

## Node.js notes

- **Use `@grpc/grpc-js`, never the `grpc` package.** The old `grpc` npm
  package wraps a native C++ addon and was deprecated in 2021; it does not
  build on current Node. `@grpc/grpc-js` is the supported, pure-JS
  implementation. `@grpc/proto-loader` loads `.proto` files at runtime, so
  there is no `protoc` codegen step in this example at all.
- **`longs: String` is not optional for this API.** Node has no integer type
  wider than a double (`BigInt` aside, which proto-loader does not emit by
  default). Without the option, uint64 fields decode as `Long` objects or
  plain numbers that round past 2^53. With it, turbovec's `uint64` ids, index
  lengths, and allowlist entries cross the API as exact decimal strings — and
  you send them back the same way.
- **Keep frames under the 4 MB message limit.** grpc-js enforces a default
  4 MB cap on received messages, and the server caps sent frames the same way.
  The Add streamer sizes frames at ~3 MB of float32 payload per write; for
  very large corpora a production client would also watch `call.write()`'s
  return value for backpressure — this demo keeps the streaming loop simple.
- **ESM and native TypeScript.** The package is `"type": "module"`, and
  `npm start` runs `demo.ts` directly: Node (22.18+, 24 recommended) executes
  TypeScript via built-in type stripping, so there is no `tsc` build step,
  `ts-node`, or bundler in the toolchain — just `node`.
