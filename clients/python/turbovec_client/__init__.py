"""A thin Python client for a turbovec collection.

One collection, however many machines it is spread over. Connect to a
coordinator and search it as a single index:

    from turbovec_client import connect

    with connect("127.0.0.1:50050") as collection:
        for neighbour in collection.search(query, k=10):
            print(neighbour.id, neighbour.score)

The scores are the scores a single index holding every row would have
returned, bit for bit, and nothing in this API says how many nodes there are
or which one a row came from. The two calls that do name nodes,
:meth:`~turbovec_client.Collection.split` and
:meth:`~turbovec_client.Collection.join`, exist to move rows between them.

The generated protobuf stubs are build artifacts and are not shipped in the
source tree; run ``gen_stubs.sh`` once before importing this package. The
import below fails with instructions if you have not.
"""

from ._collection import Collection, CollectionError, Health, Neighbour, Node, connect

__all__ = [
    "Collection",
    "CollectionError",
    "Health",
    "Neighbour",
    "Node",
    "connect",
]

__version__ = "0.1.0"
