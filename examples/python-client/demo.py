#!/usr/bin/env python3
"""Python demo client for the turbovec-grpc server.

Builds an ID_MAP index by client-streaming vectors in, then times top-k
queries and reports ingest throughput and query latency (p50/p95/p99).

    .venv/bin/python demo.py                     # 20,000 vectors, dim 128, 500 queries
    .venv/bin/python demo.py 100000 768 2000     # vectors dim queries
    TURBOVEC_GRPC_ADDR=host:port .venv/bin/python demo.py
"""

import math
import os
import random
import sys
import time

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated"))
from turbovec.v1 import turbovec_pb2, turbovec_pb2_grpc

DEFAULT_ADDR = "127.0.0.1:50051"
DEFAULT_VECTORS = 20_000
DEFAULT_DIM = 128
DEFAULT_QUERIES = 500

BIT_WIDTH = 4
TOP_K = 10
WARMUP_QUERIES = 50
# Keep Add frames well under the server's 4 MB message limit.
FRAME_BYTES = 3_000_000


def random_vectors(rng, dim, count):
    return [rng.uniform(-1.0, 1.0) for _ in range(dim * count)]


def add_frames(stub, index_id, n_vectors, dim):
    """Yield client-streaming Add frames; vectors are generated per frame."""
    rng = random.Random(7)
    per_frame = max(1, FRAME_BYTES // (dim * 4))
    sent = 0
    while sent < n_vectors:
        rows = min(per_frame, n_vectors - sent)
        yield turbovec_pb2.AddRequest(
            index_id=index_id,
            dim=dim,
            vectors=random_vectors(rng, dim, rows),
            ids=range(sent, sent + rows),
        )
        sent += rows


def percentile_ms(sorted_ns, p):
    i = min(max(math.ceil(p / 100.0 * len(sorted_ns)) - 1, 0), len(sorted_ns) - 1)
    return sorted_ns[i] / 1e6


def main():
    n_vectors = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_VECTORS
    dim = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_DIM
    n_queries = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_QUERIES
    addr = os.environ.get("TURBOVEC_GRPC_ADDR", DEFAULT_ADDR)

    channel = grpc.insecure_channel(addr)
    stub = turbovec_pb2_grpc.TurboVecStub(channel)
    try:
        print(f"turbovec-grpc demo — connected to {addr}")

        created = stub.CreateIndex(
            turbovec_pb2.CreateIndexRequest(
                dim=dim,
                bit_width=BIT_WIDTH,
                kind=turbovec_pb2.INDEX_KIND_ID_MAP,
                lazy=False,
            )
        )
        index_id = created.index_id

        # [1] Ingest: client-stream the corpus in chunked frames.
        print(f"\n[1] indexing {n_vectors:,} vectors of dim {dim} at {BIT_WIDTH}-bit")
        start = time.perf_counter_ns()
        added = stub.Add(add_frames(stub, index_id, n_vectors, dim)).added
        secs = (time.perf_counter_ns() - start) / 1e9
        wire_mb = n_vectors * dim * 4 / 1e6
        print(f"    added {added:,} in {secs:.2f}s  =  {added / secs:,.0f} vectors/sec"
              f"  ({wire_mb:,.0f} MB of raw float32 sent)")
        info = stub.GetIndexInfo(turbovec_pb2.GetIndexInfoRequest(index_id=index_id))
        print(f"    server reports {info.len:,} vectors in the index")

        # [2] Unary top-k search, one query at a time. Query vectors are
        # generated outside the timed region, so latency reflects the round
        # trip and the server scan, not the client.
        print(f"\n[2] top-{TOP_K} search, one query at a time")
        rng = random.Random(1234)
        for _ in range(WARMUP_QUERIES):
            stub.Search(turbovec_pb2.SearchRequest(
                index_id=index_id, queries=random_vectors(rng, dim, 1), k=TOP_K))
        latencies_ns = []
        for _ in range(n_queries):
            req = turbovec_pb2.SearchRequest(
                index_id=index_id, queries=random_vectors(rng, dim, 1), k=TOP_K)
            t0 = time.perf_counter_ns()
            resp = stub.Search(req)
            latencies_ns.append(time.perf_counter_ns() - t0)
            if len(resp.results[0].ids) != TOP_K:
                raise RuntimeError(f"expected {TOP_K} neighbours")
        latencies_ns.sort()
        total_s = sum(latencies_ns) / 1e9
        print(f"    {n_queries:,} queries  =  {n_queries / total_s:,.0f} queries/sec"
              " served (single client thread)")
        print(f"    latency  p50 {percentile_ms(latencies_ns, 50):.2f} ms"
              f"   p95 {percentile_ms(latencies_ns, 95):.2f} ms"
              f"   p99 {percentile_ms(latencies_ns, 99):.2f} ms")
        # Python ints are arbitrary precision, so a uint64 id (even one past
        # 2^53, where JSON/double clients start rounding) comes back exact.
        print(f"    ids come back as exact Python ints, e.g. {resp.results[0].ids[0]}"
              " — no 2^53 rounding as with a JSON/double wire")

        # [3] Server-streaming search: one QueryResult per query, in order.
        print("\n[3] server-streaming search, batch of 4 queries")
        batch = turbovec_pb2.SearchRequest(
            index_id=index_id, queries=random_vectors(rng, dim, 4), k=TOP_K)
        for i, qr in enumerate(stub.SearchStream(batch)):
            print(f"    query {i} -> {len(qr.ids)} neighbours, best score {qr.scores[0]:.4f}")

        stub.DropIndex(turbovec_pb2.DropIndexRequest(index_id=index_id))
    except grpc.RpcError as e:
        print(f"\nrpc failed: {e.code()}"
              "\nis the server up?  cargo run -p turbovec-grpc", file=sys.stderr)
        sys.exit(1)
    finally:
        channel.close()


if __name__ == "__main__":
    main()
