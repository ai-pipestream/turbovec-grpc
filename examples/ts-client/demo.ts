// TypeScript/Node demo client for the turbovec-grpc server.
//
// Two things, in one run. First, the index reached from Node: build an index
// by client-streaming vectors in, then time top-k queries and report ingest
// throughput and query latency. Second, the uint64 footgun: JavaScript
// numbers are IEEE-754 doubles, so any id at or above 2^53 rounds the moment
// it passes through Number() or JSON.parse — the proto loader's
// `longs: String` option returns ids as decimal strings and keeps them exact.
//
//   npm install && npm start
//   npm start -- 100000 768 2000            # vectors dim queries
//   TURBOVEC_GRPC_ADDR=host:port npm start

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { performance } from "node:perf_hooks";

const ADDR = process.env.TURBOVEC_GRPC_ADDR ?? "127.0.0.1:50051";
const BIT_WIDTH = 4;
const TOP_K = 10;
const WARMUP_QUERIES = 50;

const PROTO_PATH = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "proto/turbovec/v1/turbovec.proto",
);

// longs: String is the load-bearing option. Without it, uint64 fields are
// decoded into Long objects (or plain numbers, which silently round past
// 2^53). With it, every uint64 — ids, counts, allowlist entries — crosses
// the API as an exact decimal string.
const definition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(definition) as any;
const client = new proto.turbovec.v1.TurboVec(ADDR, grpc.credentials.createInsecure());

// grpc-js is callback-based; these thin wrappers keep the demo sequential.
function unary(method: string, request: object): Promise<any> {
  return new Promise((resolve, reject) => {
    client[method](request, (err: Error | null, resp: any) =>
      err ? reject(err) : resolve(resp),
    );
  });
}

// Deterministic PRNG (mulberry32) so runs are reproducible.
function rng(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function randomVector(rand: () => number, dim: number): number[] {
  const out = new Array<number>(dim);
  for (let d = 0; d < dim; d++) out[d] = rand() * 2 - 1;
  return out;
}

function percentileMs(sortedNs: bigint[], percentile: number): number {
  let index = Math.ceil((percentile / 100) * sortedNs.length) - 1;
  index = Math.min(Math.max(index, 0), sortedNs.length - 1);
  return Number(sortedNs[index]) / 1e6;
}

const fmt = (n: number) => n.toLocaleString("en-US");

// Client-streaming Add. Vectors are generated frame by frame and never all
// held in memory at once. Frames stay well under the 4 MB gRPC message limit.
function streamVectors(indexId: string, nVectors: number, dim: number): Promise<number> {
  return new Promise((resolve, reject) => {
    const call = client.add((err: Error | null, resp: any) =>
      err ? reject(err) : resolve(Number(resp.added)),
    );
    const perFrame = Math.max(1, Math.floor(3_000_000 / (dim * Float32Array.BYTES_PER_ELEMENT)));
    const rand = rng(7);
    let sent = 0;
    while (sent < nVectors) {
      const rows = Math.min(perFrame, nVectors - sent);
      const vectors = new Array<number>(rows * dim);
      const ids = new Array<number>(rows);
      for (let r = 0; r < rows; r++) {
        for (let d = 0; d < dim; d++) vectors[r * dim + d] = rand() * 2 - 1;
        ids[r] = sent + r;
      }
      call.write({ index_id: indexId, dim, vectors, ids });
      sent += rows;
    }
    call.end();
  });
}

async function speedDemo(nVectors: number, dim: number, nQueries: number): Promise<void> {
  const created = await unary("createIndex", {
    dim,
    bit_width: BIT_WIDTH,
    kind: "INDEX_KIND_ID_MAP",
    lazy: false,
  });
  const indexId: string = created.index_id;

  console.log(`\n[1] indexing ${fmt(nVectors)} vectors of dim ${dim} at ${BIT_WIDTH}-bit`);
  const ingestStart = performance.now();
  const added = await streamVectors(indexId, nVectors, dim);
  const ingestSecs = (performance.now() - ingestStart) / 1e3;
  const wireMb = (nVectors * dim * Float32Array.BYTES_PER_ELEMENT) / 1e6;
  console.log(
    `    added ${fmt(added)} in ${ingestSecs.toFixed(2)}s  =  ${fmt(Math.round(added / ingestSecs))} vectors/sec  (${fmt(Math.round(wireMb))} MB of raw float32 sent)`,
  );
  const info = await unary("getIndexInfo", { index_id: indexId });
  console.log(`    server reports ${fmt(Number(info.len))} vectors in the index`);

  console.log(`\n[2] top-${TOP_K} search, one query at a time`);
  const rand = rng(1234);
  for (let i = 0; i < WARMUP_QUERIES; i++) {
    await unary("search", { index_id: indexId, queries: randomVector(rand, dim), k: TOP_K });
  }
  // Query vectors are generated outside the timed region, so latency and
  // served QPS reflect the round trip and the server scan, not the client.
  const latenciesNs: bigint[] = [];
  let totalNs = 0n;
  for (let i = 0; i < nQueries; i++) {
    const req = { index_id: indexId, queries: randomVector(rand, dim), k: TOP_K };
    const start = process.hrtime.bigint();
    const resp = await unary("search", req);
    const elapsed = process.hrtime.bigint() - start;
    latenciesNs.push(elapsed);
    totalNs += elapsed;
    if (resp.results[0].ids.length !== TOP_K) throw new Error(`expected ${TOP_K} neighbours`);
  }
  latenciesNs.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  console.log(
    `    ${fmt(nQueries)} queries  =  ${fmt(Math.round(nQueries / (Number(totalNs) / 1e9)))} queries/sec served (single client)`,
  );
  console.log(
    `    latency  p50 ${percentileMs(latenciesNs, 50).toFixed(2)} ms   p95 ${percentileMs(latenciesNs, 95).toFixed(2)} ms   p99 ${percentileMs(latenciesNs, 99).toFixed(2)} ms`,
  );

  // The server-streaming variant: neighbours arrive one query at a time.
  console.log(`\n[3] server-streaming search, batch of 4 queries`);
  const batch: number[] = [];
  for (let q = 0; q < 4; q++) batch.push(...randomVector(rand, dim));
  const stream = client.searchStream({ index_id: indexId, queries: batch, k: TOP_K });
  let q = 0;
  for await (const result of stream as AsyncIterable<any>) {
    console.log(`    query ${q} -> ${result.ids.length} neighbours, best score ${result.scores[0].toFixed(4)}`);
    q++;
  }

  await unary("dropIndex", { index_id: indexId });
}

// Part B: the uint64 footgun. 2^53 + 1 is not representable as a double, so
// Number() and JSON.parse() both round it to 2^53. Sent over gRPC with
// longs: String, the same id comes back digit-for-digit exact.
async function idFidelityDemo(dim: number): Promise<void> {
  const indexId: string = (
    await unary("createIndex", { dim, bit_width: BIT_WIDTH, kind: "INDEX_KIND_ID_MAP", lazy: false })
  ).index_id;

  const bigId = "9007199254740993"; // 2^53 + 1, sent as a string
  const rand = rng(11);
  await new Promise<void>((resolve, reject) => {
    const call = client.add((err: Error | null) => (err ? reject(err) : resolve()));
    call.write({ index_id: indexId, dim, vectors: randomVector(rand, dim), ids: [bigId] });
    call.end();
  });

  console.log(`\n[4] the uint64 footgun (id = 2^53 + 1)`);
  console.log(`    Number("${bigId}")       -> ${Number(bigId)}  LOST`);
  console.log(`    JSON.parse("${bigId}")   -> ${JSON.parse(bigId)}  LOST`);
  const resp = await unary("search", {
    index_id: indexId,
    queries: randomVector(rng(5), dim),
    k: 1,
    allowlist: [bigId], // uint64 on the wire; a string here stays exact
  });
  const got: string = resp.results[0].ids[0];
  console.log(`    gRPC search for that id  -> ${got}  ${got === bigId ? "ok, exact string" : "LOST"}`);
  console.log(
    `    longs: String on the proto loader is what keeps uint64 ids exact in Node —\n    JS numbers and JSON both stop at 2^53.`,
  );

  await unary("dropIndex", { index_id: indexId });
}

async function main(): Promise<void> {
  const nVectors = Number(process.argv[2] ?? 20_000);
  const dim = Number(process.argv[3] ?? 128);
  const nQueries = Number(process.argv[4] ?? 500);

  console.log(`turbovec-grpc demo — connected to ${ADDR}`);
  try {
    await speedDemo(nVectors, dim, nQueries);
    await idFidelityDemo(dim);
  } catch (err: any) {
    console.error(
      `\nrpc failed: ${err?.message ?? err}\nis the server up?  cargo run -p turbovec-grpc`,
    );
    process.exit(1);
  }
  client.close();
}

await main();
