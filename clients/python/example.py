#!/usr/bin/env python3
"""Search one collection that is spread over several machines.

Brings up nothing itself: point it at a running coordinator and it will
calibrate the collection, fill it, split it across the nodes the coordinator
knows about, and search it. Nothing below mentions a shard.

    .venv/bin/python example.py                       # 127.0.0.1:50050
    .venv/bin/python example.py 127.0.0.1:50050
    .venv/bin/python example.py 127.0.0.1:50050 20000 128

See the README for the three commands that start the servers first.
"""

import random
import sys
import time

import grpc

from turbovec_client import CollectionError, connect
from turbovec_client._stubs import turbovec_pb2, turbovec_pb2_grpc

DEFAULT_ADDR = "127.0.0.1:50050"
DEFAULT_VECTORS = 20_000
DEFAULT_DIM = 128
BIT_WIDTH = 4
TOP_K = 10
QUERIES = 200
WARMUP_QUERIES = 50
# turbovec's fit wants a uniform random draw of the rows the collection will
# hold, and enough of them: ~1024 rows matches a fit over a whole corpus.
CALIBRATION_ROWS = 1024


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

    with connect(address) as collection:
        print(f"turbovec collection - connected to {address}\n")

        # Each node needs an empty index for the collection to be made of.
        # Creating one is a node-level call, not a collection-level one: the
        # collection is what you search, and it is assembled out of indexes
        # that already exist.
        health = collection.health()
        nodes = [node.address for node in health.nodes]
        for address in nodes:
            _ensure_index(address, dim)
        health = collection.health()

        print(f"[1] the collection is {len(nodes)} node(s)")
        for node in health.nodes:
            print(f"    {node.address}  {node.rows:,} rows  {node.error or 'ok'}")
        if not health.servable:
            print(f"    not servable: {health.error}", file=sys.stderr)
            sys.exit(1)

        # One calibration for the whole collection. Without it the nodes would
        # each encode into their own coordinate system and the coordinator
        # would refuse to merge them, which is the point.
        print(f"\n[2] fitting one calibration from {CALIBRATION_ROWS:,} sample rows")
        collection.calibrate(rows(rng, dim, CALIBRATION_ROWS), dim, BIT_WIDTH)
        print("    committed to every node")

        # Fill the first node, then spread the rows over all of them. Filling
        # one and splitting is the lifecycle this layer is for: a collection
        # outgrows a machine, and moves onto several without being rebuilt.
        print(f"\n[3] indexing {n_vectors:,} vectors of dim {dim} at {BIT_WIDTH}-bit")
        started = time.perf_counter()
        _fill(nodes[0], health.nodes[0].index_id, rows(rng, dim, n_vectors), dim)
        elapsed = time.perf_counter() - started
        print(f"    added {n_vectors:,} in {elapsed:.2f}s")

        if len(nodes) > 1:
            print(f"\n[4] splitting across {len(nodes)} nodes")
            spread = collection.split(source=nodes[0], targets=nodes)
            print(f"    rows per node: {', '.join(f'{r:,}' for r in spread)}")
        else:
            print("\n[4] one node configured, so there is nothing to split across")

        health = collection.health()
        print(f"    collection holds {health.rows:,} rows, servable={health.servable}")

        print(f"\n[5] top-{TOP_K} search, one query at a time")
        for query in rows(rng, dim, WARMUP_QUERIES):
            collection.search(query, k=TOP_K)
        latencies = []
        for query in rows(rng, dim, QUERIES):
            started = time.perf_counter_ns()
            found = collection.search(query, k=TOP_K)
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

        print("\n[6] a batch of 4 queries")
        for i, result in enumerate(collection.search(rows(rng, dim, 4), k=TOP_K)):
            print(f"    query {i} -> {len(result)} neighbours, best {result[0].score:.4f}")

        if len(nodes) > 1:
            print(f"\n[7] joining back onto {nodes[0]}")
            print(f"    combined index holds {collection.join(nodes[0]):,} rows")
            found = collection.search(rows(rng, dim, 1)[0], k=TOP_K)
            print(f"    still searchable: {len(found)} neighbours")


def _node_stub(channel):
    """A node stub over an open channel."""
    return turbovec_pb2_grpc.TurboVecStub(channel)


def _ensure_index(address, dim):
    """Give a node one empty positional index, if it has none yet.

    The coordinator resolves a node configured without an index handle to its
    only open index, so leaving a node with none, or with several, is what it
    refuses as ambiguous.
    """
    with grpc.insecure_channel(_bare(address)) as channel:
        stub = _node_stub(channel)
        if stub.ListIndexes(turbovec_pb2.ListIndexesRequest()).indexes:
            return
        stub.CreateIndex(
            turbovec_pb2.CreateIndexRequest(
                dim=dim,
                bit_width=BIT_WIDTH,
                kind=turbovec_pb2.INDEX_KIND_POSITIONAL,
            )
        )


def _fill(address, index_id, vectors, dim):
    """Stream vectors into one node's index.

    Filling is a node-level operation, so it uses the node stub rather than the
    collection handle: the collection is what you search, not what you write
    into row by row.
    """
    # Keep each frame well under the 4 MB default gRPC message limit.
    per_frame = max(1, 3_000_000 // (dim * 4))

    def frames():
        for start in range(0, len(vectors), per_frame):
            chunk = vectors[start : start + per_frame]
            flat = [c for row in chunk for c in row]
            yield turbovec_pb2.AddRequest(index_id=index_id, dim=dim, vectors=flat)

    with grpc.insecure_channel(_bare(address)) as channel:
        _node_stub(channel).Add(frames())


def _bare(address):
    """Strip the scheme the coordinator dials with; grpc.insecure_channel
    wants a bare host:port."""
    return address.removeprefix("http://").removeprefix("https://")


if __name__ == "__main__":
    try:
        main()
    except CollectionError as error:
        # The server refuses rather than degrading, so a failure here names
        # what is wrong with the collection instead of returning less of it.
        print(f"\nrefused ({error.name}): {error.detail}", file=sys.stderr)
        sys.exit(1)
