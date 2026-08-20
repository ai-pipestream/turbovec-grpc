#!/usr/bin/env python3
"""Create node1's index and write the coordinator's node-table entry.

A durable coordinator pins every startup shard to an index id and a durable
generation, and both exist only after the index is created and flushed. This
one-shot step does that and writes the entry (`address index-id generation`)
to the shared volume the coordinator's entrypoint reads.

    DEMO_NODE=http://node1:50051 SETUP_ENTRY=node1:50051 python setup_index.py
"""

import os
import sys
import time

import grpc

from turbovec_client import CollectionError, create_index, open_index
from turbovec_client._stubs import turbovec_pb2, turbovec_pb2_grpc

DIM = 128
BIT_WIDTH = 4


def bare(address):
    return address.removeprefix("http://").removeprefix("https://")


def main():
    address = os.environ.get("DEMO_NODE", "http://node1:50051")
    entry = os.environ.get("SETUP_ENTRY", bare(address))
    out = os.environ.get("SETUP_OUT", "/setup/node-entry")

    deadline = time.monotonic() + 90
    while True:
        channel = grpc.insecure_channel(bare(address))
        try:
            query = turbovec_pb2_grpc.TurboVecQueryStub(channel)
            indexes = query.ListIndexes(turbovec_pb2.ListIndexesRequest(), timeout=5).indexes
            break
        except grpc.RpcError:
            channel.close()
            if time.monotonic() > deadline:
                raise
            time.sleep(1)

    if not indexes:
        index = create_index(address, dim=DIM, bit_width=BIT_WIDTH)
        print(f"created index {index.index_id}")
    elif len(indexes) == 1:
        index = open_index(address)
        print(f"reusing index {index.index_id}")
    else:
        print(f"node holds {len(indexes)} indexes; expected at most one", file=sys.stderr)
        sys.exit(1)

    generation = index.info().generation
    if generation == 0:
        generation = index.flush()
    index.close()

    with open(out, "w", encoding="utf-8") as file:
        file.write(f"{entry} {index.index_id} {generation}\n")
    print(f"node-table entry: {entry} {index.index_id} {generation}")


if __name__ == "__main__":
    try:
        main()
    except CollectionError as error:
        print(f"refused ({error.name}): {error.detail}", file=sys.stderr)
        sys.exit(1)
