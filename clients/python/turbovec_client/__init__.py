"""A thin Python client for turbovec over gRPC.

Two shapes, one engine. One index on one node reads like the embedded
``turbovec`` package's ``TurboQuantIndex``:

    from turbovec_client import create_index

    with create_index("127.0.0.1:50051", dim=128, bit_width=4) as index:
        index.add(vectors)
        for neighbour in index.search(query, k=10):
            print(neighbour.id, neighbour.score)
        index.flush()

One collection, however many machines it is spread over, reached through a
coordinator and searched as a single index:

    from turbovec_client import connect

    with connect("127.0.0.1:50050") as collection:
        for neighbour in collection.search(query, k=10):
            print(neighbour.id, neighbour.score)

Either way the scores are the scores a single embedded index holding every
row would have returned, bit for bit. The two collection calls that do name
nodes, :meth:`~turbovec_client.Collection.split` and
:meth:`~turbovec_client.Collection.join`, exist to move rows between them.

The generated protobuf stubs are build artifacts and are not shipped in the
source tree; run ``gen_stubs.sh`` once before importing this package. The
import below fails with instructions if you have not.
"""

from ._collection import Collection, CollectionError, Health, Neighbour, Node, connect
from ._index import ADD_CHUNK_COORDS, Index, IndexInfo, create_index, open_index

__all__ = [
    "ADD_CHUNK_COORDS",
    "Collection",
    "CollectionError",
    "Health",
    "Index",
    "IndexInfo",
    "Neighbour",
    "Node",
    "connect",
    "create_index",
    "open_index",
]

__version__ = "0.1.0"
