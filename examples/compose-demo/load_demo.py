#!/usr/bin/env python3
"""Fill a one-node collection, then watch the autoscaler spread it over three.

Brings up nothing itself: compose starts one node and the coordinator, this
script calibrates and fills node1's index, and the grow-only autoscaler
splits the shard onto node2 and node3 once they join the spare pool. The
ingest finishes before the other nodes start because it has to: a shard the
autoscaler builds is sealed to further rows, and the split drops the source
index it read from.

    python load_demo.py                          # 1,000,000 rows, localhost
    python load_demo.py 600000                   # a smaller run
    python load_demo.py --node http://node1:50051 \
        --coordinator http://coordinator:50050   # inside compose

See README.md for the compose walkthrough this script is half of.
"""

import argparse
import os
import sys
import time

import grpc
import numpy as np

from turbovec_client import CollectionError, connect, create_index, open_index
from turbovec_client._stubs import (
    coordinator_pb2,
    coordinator_pb2_grpc,
    turbovec_pb2,
    turbovec_pb2_grpc,
)

DIM = 128
BIT_WIDTH = 4
TOP_K = 10
QUERIES = 100
WARMUP_QUERIES = 20
# One Add operation is bounded server-side at 4M coordinates, so the upload
# goes in chunks of rows rather than one stream.
CHUNK_ROWS = 20_000
# turbovec's fit wants a uniform random draw of the rows the collection will
# hold, and enough of them: ~1024 rows matches a fit over a whole corpus.
CALIBRATION_ROWS = 1024
# Clustered gaussian data, so a nearest neighbour means something.
CLUSTERS = 64
CLUSTER_SPREAD = 0.15
SEED = 7
# The stored row the self-match check searches for afterwards.
SELF_MATCH_ROW = 123_456


def fail(message):
    print(f"\n{message}", file=sys.stderr)
    sys.exit(1)


def bare(address):
    """grpc.insecure_channel wants a bare host:port, no scheme."""
    return address.removeprefix("http://").removeprefix("https://")


def wait_for_node(address, timeout=90):
    """The first list of indexes node1 serves, retrying while compose starts it."""
    deadline = time.monotonic() + timeout
    while True:
        channel = grpc.insecure_channel(bare(address))
        try:
            query = turbovec_pb2_grpc.TurboVecQueryStub(channel)
            indexes = query.ListIndexes(turbovec_pb2.ListIndexesRequest(), timeout=5)
            return indexes
        except grpc.RpcError:
            channel.close()
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def wait_for_coordinator(address, timeout=90):
    """A coordinator stub that has answered one ListNodes, retrying likewise."""
    deadline = time.monotonic() + timeout
    while True:
        channel = grpc.insecure_channel(bare(address))
        try:
            stub = coordinator_pb2_grpc.CoordinatorStub(channel)
            stub.ListNodes(coordinator_pb2.ListNodesRequest(), timeout=5)
            return stub
        except grpc.RpcError:
            channel.close()
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def clustered_rows(rng, centers, count):
    """A draw from the seeded clusters: one centre plus a small gaussian step."""
    which = rng.integers(0, len(centers), count)
    rows = centers[which] + rng.normal(0.0, CLUSTER_SPREAD, (count, DIM))
    return rows.astype(np.float32)


def percentile_ms(sorted_ns, p):
    i = min(max(round(p / 100.0 * len(sorted_ns)) - 1, 0), len(sorted_ns) - 1)
    return sorted_ns[i] / 1e6


def signature(listing):
    """What the watch loop considers a change worth printing."""
    return (
        listing.topology_generation,
        tuple(
            (
                shard.shard.address,
                shard.info.len if shard.HasField("info") else 0,
                shard.error,
            )
            for shard in listing.shards
        ),
        tuple(spare.address for spare in listing.spares),
    )


def print_listing(listing):
    state = "servable" if listing.servable else f"not servable: {listing.error}"
    print(
        f"    generation {listing.topology_generation}: "
        f"{len(listing.shards)} shard(s), {listing.rows:,} rows, "
        f"{len(listing.spares)} spare(s) - {state}"
    )
    for shard in listing.shards:
        rows = shard.info.len if shard.HasField("info") else 0
        print(f"      {shard.shard.address}  {rows:,} rows  {shard.error or 'ok'}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "rows",
        nargs="?",
        type=int,
        default=int(os.environ.get("DEMO_ROWS", "1000000")),
        help="vectors to index (default 1,000,000)",
    )
    parser.add_argument(
        "--node",
        default=os.environ.get("DEMO_NODE", "http://localhost:50051"),
        help="node1's address",
    )
    parser.add_argument(
        "--coordinator",
        default=os.environ.get("DEMO_COORDINATOR", "http://localhost:50050"),
        help="the coordinator's address",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=900,
        help="seconds to watch for autosplits before giving up",
    )
    args = parser.parse_args()
    n_rows = args.rows

    rng = np.random.default_rng(SEED)
    centers = rng.normal(0.0, 1.0, (CLUSTERS, DIM)).astype(np.float32)
    # The self-match row is planted away from the clusters: at 4-bit a dense
    # cluster buries the exact row under hundreds of near-identical
    # neighbours, which is quantization, not a retrieval error.
    isolated = np.random.default_rng(SEED + 1).normal(0.0, 1.0, DIM).astype(np.float32)

    print(f"turbovec compose demo - node {args.node}, coordinator {args.coordinator}\n")

    print("[1] waiting for node1 and the coordinator")
    existing = wait_for_node(args.node).indexes
    coordinator = wait_for_coordinator(args.coordinator)
    print("    both answered")

    # The coordinator's node table names node1 by bare address, which resolves
    # to the node's only open index, so node1 must hold exactly one.
    print(f"\n[2] an empty positional index on node1, dim {DIM} at {BIT_WIDTH}-bit")
    if not existing:
        index = create_index(args.node, dim=DIM, bit_width=BIT_WIDTH)
        print(f"    created index {index.index_id}")
    elif len(existing) == 1:
        index = open_index(args.node)
        if len(index) != 0:
            fail(
                f"node1's index already holds {len(index):,} rows; this demo starts "
                "empty. Tear the cluster down with `docker compose down -v` first."
            )
        print(f"    reusing empty index {index.index_id}")
    else:
        fail(
            f"node1 holds {len(existing)} indexes; the coordinator's bare-address "
            "shard entry only resolves when the node holds exactly one."
        )

    with connect(args.coordinator) as collection:
        # One calibration for the whole collection, fitted before any rows:
        # a pair commits at construction, never over encoded rows.
        print(f"\n[3] fitting one calibration from {CALIBRATION_ROWS:,} sample rows")
        collection.calibrate(clustered_rows(rng, centers, CALIBRATION_ROWS), DIM, BIT_WIDTH)
        print("    committed to every node")

        # A collection takes rows only until its first split: Split moves
        # encoded rows with their labels and a labelled index refuses further
        # adds, so the spare pool must stay empty until the ingest is done.
        listing = coordinator.ListNodes(coordinator_pb2.ListNodesRequest())
        if listing.spares:
            spares = ", ".join(spare.address for spare in listing.spares)
            fail(
                f"the spare pool is not empty ({spares}); the autoscaler would "
                "split the shard mid-ingest and the remaining rows would have "
                "nowhere to go. Run the ingest with only node1 up, then start "
                "node2 and node3 (see README.md)."
            )

        print(f"\n[4] indexing {n_rows:,} vectors on node1 alone")
        remember = min(SELF_MATCH_ROW, n_rows - 1)
        remembered = None
        started = time.perf_counter()
        for start in range(0, n_rows, CHUNK_ROWS):
            block = clustered_rows(rng, centers, min(CHUNK_ROWS, n_rows - start))
            if remembered is None and start <= remember < start + len(block):
                block[remember - start] = isolated
                remembered = isolated
            # Plain adds, not the retry-safe envelope: the durable topology
            # pins this shard's durable generation, and a retry-safe add
            # persists a new generation per operation, which the coordinator
            # would read as shard_generation_mismatch. The rows become durable
            # when the autosplit flushes its targets.
            index.add(block, operation_id=None)
            done = start + len(block)
            rate = done / (time.perf_counter() - started)
            print(f"    added {done:,}/{n_rows:,}  ({rate:,.0f} rows/s)")
        elapsed = time.perf_counter() - started
        print(f"    added {n_rows:,} in {elapsed:.1f}s")
        index.close()

        # Watch the autoscaler grow the collection once the spare pool is
        # stocked. It does one split per tick, keeping half the rows on the
        # source node, so three nodes top out at three shards.
        print("\n[5] watching for autosplits")
        print("    if nothing changes, stock the spare pool:")
        print("      docker compose up -d node2 node3")
        deadline = time.monotonic() + args.wait
        last = None
        stable = 0
        seen_split = False
        while time.monotonic() < deadline:
            listing = coordinator.ListNodes(coordinator_pb2.ListNodesRequest())
            current = signature(listing)
            if current != last:
                print_listing(listing)
                last = current
                stable = 0
            else:
                stable += 1
            seen_split = seen_split or len(listing.shards) > 1
            if seen_split and not listing.spares and stable >= 3:
                break
            time.sleep(2)
        else:
            fail(f"no autosplit settled within {args.wait}s")
        print(f"    settled: {len(last[1])} shards, spare pool empty")

        print(f"\n[6] top-{TOP_K} search, one query at a time")
        for query in clustered_rows(rng, centers, WARMUP_QUERIES):
            collection.search(query, k=TOP_K)
        latencies = []
        for query in clustered_rows(rng, centers, QUERIES):
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

        print(f"\n[7] searching for stored row {remember:,}")
        found = collection.search(remembered, k=1)
        self_match = bool(found) and found[0].id == remember
        print(f"    row {remember:,}'s top neighbour is itself: {self_match}")

        print("\n[8] final health")
        health = collection.health()
        for node in health.nodes:
            print(f"    {node.address}  {node.rows:,} rows  {node.error or 'ok'}")
        print(f"    collection holds {health.rows:,} rows, servable={health.servable}")

        checks = [
            (health.servable, f"collection is not servable: {health.error}"),
            (health.rows == n_rows, f"row count {health.rows:,} != ingested {n_rows:,}"),
            (len(health.nodes) == 3, f"{len(health.nodes)} shards, expected 3"),
            (
                len({node.address for node in health.nodes}) == 3,
                "the shards did not spread over three nodes",
            ),
            (all(not node.error for node in health.nodes), "a shard reports an error"),
            (self_match, "the self-match check failed"),
        ]
        failures = [message for ok, message in checks if not ok]
        for message in failures:
            print(f"    FAIL: {message}", file=sys.stderr)
        print(f"\n{'FAIL' if failures else 'PASS'}")
        if failures:
            sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except CollectionError as error:
        # The server refuses rather than degrading, so a failure here names
        # what is wrong with the collection instead of returning less of it.
        fail(f"refused ({error.name}): {error.detail}")
    except grpc.RpcError as error:
        fail(f"unreachable: {error.details() or error}")
