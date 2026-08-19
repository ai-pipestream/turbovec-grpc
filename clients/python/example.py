#!/usr/bin/env python3
"""One index on one node, in the shape of the embedded turbovec API.

Brings up nothing itself: point it at a running node and it will create an
id-mapped index, fill it, search it, remove a row, and flush it — the same
calls the embedded ``turbovec`` package's ``IdMapIndex`` takes, against a
server instead of your own process.

    .venv/bin/python example.py                       # 127.0.0.1:50051
    .venv/bin/python example.py 127.0.0.1:50051
    .venv/bin/python example.py 127.0.0.1:50051 20000 128

Start the node first (from the repo root). Retry-safe ingest and flush both
need durable storage, so give it a data dir rather than the ephemeral demo
mode:

    TURBOVEC_DATA_DIR=/tmp/turbovec-demo cargo run --release --bin turbovec-grpc
"""

import random
import sys
import time

from turbovec_client import CollectionError, create_index

DEFAULT_ADDR = "127.0.0.1:50051"
DEFAULT_VECTORS = 20_000
DEFAULT_DIM = 128
BIT_WIDTH = 4
TOP_K = 10
QUERIES = 200
WARMUP_QUERIES = 50


def rows(rng, dim, count):
    return [[rng.uniform(-1.0, 1.0) for _ in range(dim)] for _ in range(count)]


def percentile_ms(sorted_ns, p):
    i = min(max(round(p / 100.0 * len(sorted_ns)) - 1, 0), len(sorted_ns) - 1)
    return sorted_ns[i] / 1e6


def main():
    address = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ADDR
    n_vectors = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_VECTORS
    dim = int(sys.argv[3]) if len(sys.argv) > 3 else DEFAULT_DIM
    rng = random.Random(7)

    # Id-mapped, so rows carry our own ids and remove works. The same calls
    # against create_index(address, dim, BIT_WIDTH) give a positional index,
    # whose rows are named by slot instead.
    with create_index(address, dim, BIT_WIDTH, id_mapped=True) as index:
        print(f"turbovec index - connected to {address}\n")

        print(f"[1] indexing {n_vectors:,} vectors of dim {dim} at {BIT_WIDTH}-bit")
        vectors = rows(rng, dim, n_vectors)
        ids = [1_000_000 + i for i in range(n_vectors)]
        started = time.perf_counter()
        # add keeps the operation id it used; if the response were lost,
        # repeating the call with operation_id=op would answer the committed
        # result instead of doubling the rows.
        op = index.add_with_ids(vectors, ids)
        elapsed = time.perf_counter() - started
        print(f"    added {len(index):,} in {elapsed:.2f}s  (operation {op})")

        print(f"\n[2] top-{TOP_K} search, one query at a time")
        for query in rows(rng, dim, WARMUP_QUERIES):
            index.search(query, k=TOP_K)
        latencies = []
        for query in rows(rng, dim, QUERIES):
            started = time.perf_counter_ns()
            found = index.search(query, k=TOP_K)
            latencies.append(time.perf_counter_ns() - started)
        latencies.sort()
        total_s = sum(latencies) / 1e9
        print(f"    {QUERIES} queries  =  {QUERIES / total_s:,.0f} queries/sec")
        print(
            f"    latency  p50 {percentile_ms(latencies, 50):.2f} ms"
            f"   p95 {percentile_ms(latencies, 95):.2f} ms"
            f"   p99 {percentile_ms(latencies, 99):.2f} ms"
        )
        print(f"    best neighbour: id {found[0].id}, score {found[0].score:.4f}")

        # A stored row is its own best neighbour.
        print("\n[3] searching for a stored row")
        found = index.search(vectors[0], k=TOP_K)
        print(f"    row {ids[0]}'s top neighbour is itself: {found[0].id == ids[0]}")

        print("\n[4] removing that row")
        print(f"    removed {ids[0]}: {index.remove(ids[0])}")
        print(f"    again (already gone): {index.remove(ids[0])}")
        found = index.search(vectors[0], k=TOP_K)
        print(f"    top neighbour now: id {found[0].id}, score {found[0].score:.4f}")

        # flush() is write(path, durable=True) against a server: the node
        # owns the path, so the client names no file. The flushed generation
        # is what the node restores at startup.
        print("\n[5] flushing")
        print(f"    durable generation {index.flush()} holds {len(index):,} rows")


if __name__ == "__main__":
    try:
        main()
    except CollectionError as error:
        # The server refuses rather than degrading, so a failure here names
        # what is wrong instead of returning less of the answer.
        print(f"\nrefused ({error.name}): {error.detail}", file=sys.stderr)
        sys.exit(1)
