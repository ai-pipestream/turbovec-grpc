"""The Collection handle: a turbovec collection, however many nodes it is on.

Nothing in this module's public surface mentions a shard. A collection is
searched as one index, and the only calls that name a node are the two that
exist to move rows between nodes.
"""

from dataclasses import dataclass

import grpc

from ._stubs import coordinator_pb2, coordinator_pb2_grpc

# Match the bounded server frame limit. Split and Join stream fixed-size row
# blocks, so neither side needs a whole-shard message allowance.
_MAX_MESSAGE_BYTES = 16 * 1024 * 1024

_CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", _MAX_MESSAGE_BYTES),
    ("grpc.max_receive_message_length", _MAX_MESSAGE_BYTES),
]


@dataclass(frozen=True)
class Neighbour:
    """One search result: a row and how well it matched."""

    #: The row's own id. For a collection built by :meth:`Collection.split` or
    #: :meth:`Collection.join` this is the id the row had in the index it came
    #: from, and it does not change when the row moves between nodes. For a
    #: collection assembled by hand out of indexes that carry no ids, it is the
    #: row's slot within whichever index holds it, which is only unique across
    #: the collection if you have arranged for it to be.
    id: int

    #: Similarity score. Identical, to the bit, to the score a single index
    #: over every row in the collection would have given this row.
    score: float


@dataclass(frozen=True)
class Node:
    """One node's state, as reported by :meth:`Collection.nodes`."""

    #: Address the coordinator dials.
    address: str

    #: Index handle on that node.
    index_id: str

    #: Rows the node holds, or 0 if it could not be reached.
    rows: int

    #: Empty when the node is healthy and agrees with the rest of the
    #: collection, otherwise the named reason it does not.
    error: str


@dataclass(frozen=True)
class Health:
    """Whether the collection can be served, and what it is made of."""

    #: True when every node answered and they share one calibration.
    servable: bool

    #: Empty when servable, otherwise the named reason searches are refused.
    error: str

    #: Rows across the whole collection.
    rows: int

    #: The nodes, in the order the coordinator holds them.
    nodes: "list[Node]"


class CollectionError(RuntimeError):
    """A collection that cannot answer, and the name of the reason.

    The server refuses rather than degrading: a collection whose nodes are
    calibrated differently, or one missing a node mid-query, produces this
    instead of a shorter or subtly wrong result. :attr:`name` is the stable
    part to branch on, :attr:`detail` is for whoever has to fix it.
    """

    def __init__(self, message: str):
        super().__init__(message)
        name, separator, detail = message.partition(": ")
        #: Stable name of the failure, e.g. ``mixed_calibration``. Empty if the
        #: server returned something that is not one of the named refusals.
        self.name = name if separator else ""
        #: The specifics: which node, which coordinate, what it held.
        self.detail = detail if separator else message


def _wrap(error: grpc.RpcError) -> Exception:
    """Turn a gRPC error into a CollectionError, keeping the server's wording."""
    return CollectionError(error.details() or str(error))


class Collection:
    """A turbovec collection reached through a coordinator.

    Build one with :func:`turbovec_client.connect`. The handle owns its channel
    and can be closed directly or used as a context manager.
    """

    def __init__(self, channel: grpc.Channel, owns_channel: bool = True):
        self._channel = channel
        self._owns_channel = owns_channel
        self._stub = coordinator_pb2_grpc.CoordinatorStub(channel)

    def __enter__(self) -> "Collection":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    def close(self) -> None:
        """Release the connection. Idempotent."""
        if self._owns_channel:
            self._channel.close()

    def search(self, vectors, k: int):
        """Return the top ``k`` neighbours for each query vector.

        ``vectors`` is either one query as a flat sequence of floats, or a
        sequence of such sequences. The return shape follows: a list of
        :class:`Neighbour` for a single query, a list of those lists for a
        batch.

        The results are the results one index holding every row in the
        collection would have returned, with the same scores to the bit. If any
        node cannot answer, this raises rather than returning a shorter list.
        """
        if k < 1:
            raise ValueError("k must be at least 1")
        queries, batched, _ = _flatten(vectors)
        request = coordinator_pb2.CollectionSearchRequest(queries=queries, k=k)
        try:
            response = self._stub.Search(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        results = [
            [Neighbour(id=_row_id(n), score=n.score) for n in result.neighbours]
            for result in response.results
        ]
        return results if batched else results[0]

    def calibrate(self, sample, dim: int, bit_width: int) -> None:
        """Fit one calibration from ``sample`` and commit it across the whole
        collection, so every node encodes into the same coordinate system.

        ``sample`` is a sequence of rows, or a flat buffer of ``rows * dim``
        floats. It should be a uniform random draw from the rows the collection
        will hold; nothing here can tell a random draw from a biased one, and a
        biased one costs recall.

        Every node's index must still be empty: a calibration is committed at
        construction, not applied to rows already encoded under another one.
        """
        rows, _, _ = _flatten(sample)
        request = coordinator_pb2.FitCalibrationRequest(
            sample=rows, dim=dim, bit_width=bit_width
        )
        try:
            self._stub.FitCalibration(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None

    def split(self, source: str, targets, index_id: str = "", row_counts=None):
        """Redistribute one index's rows across ``targets``, and serve the
        result as this collection.

        ``source`` is the address of the node holding the index to spread out,
        and ``index_id`` names which index on it, defaulting to its only one.
        ``targets`` is a list of node addresses; a node may appear more than
        once. ``row_counts`` overrides the even split and must sum to exactly
        the source's row count.

        The rows move as encoded codes under the source's own calibration, so
        the collection scores exactly as the source did. The source index is
        left alone. Returns the rows now on each target, in target order.
        """
        request = coordinator_pb2.SplitRequest(
            source=coordinator_pb2.ShardRef(address=source, index_id=index_id),
            targets=list(targets),
            row_counts=list(row_counts or []),
        )
        try:
            response = self._stub.Split(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return list(response.rows)

    def join(self, target: str) -> int:
        """Combine the collection's nodes into one index on ``target``, and
        serve that as this collection. The inverse of :meth:`split`.

        Refused, by name, if the nodes are not calibrated alike or do not agree
        on width. The source indexes are left alone. Returns the row count of
        the combined index.
        """
        try:
            response = self._stub.Join(coordinator_pb2.JoinRequest(target=target))
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return response.rows

    def health(self) -> Health:
        """Report whether the collection can be served, and what it holds.

        Probes every node, so an unreachable one is reported here rather than
        raising: this is the call to make when :meth:`search` has started
        refusing and you need to see why.
        """
        try:
            response = self._stub.ListNodes(coordinator_pb2.ListNodesRequest())
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return Health(
            servable=response.servable,
            error=response.error,
            rows=response.rows,
            nodes=[
                Node(
                    address=shard.shard.address,
                    index_id=shard.shard.index_id,
                    rows=shard.info.len if shard.HasField("info") else 0,
                    error=shard.error,
                )
                for shard in response.shards
            ],
        )


def _row_id(neighbour) -> int:
    """The id to report for one result row.

    A collection whose rows carry ids reports those. One assembled out of
    indexes that carry none has only the slot, so that is what comes back.
    """
    return neighbour.label if neighbour.HasField("label") else neighbour.slot


def _flatten(vectors):
    """Accept one row or a sequence of rows; return (flat floats, was_batch,
    row width). Anything sequence-like works, and anything with ``tolist()``
    (a numpy array, say) is converted through it, so numpy is welcome but
    never required."""
    if hasattr(vectors, "tolist"):
        vectors = vectors.tolist()
    rows = list(vectors)
    if not rows:
        raise ValueError("no vectors given")
    if isinstance(rows[0], (int, float)):
        return [float(x) for x in rows], False, len(rows)
    flat = []
    width = None
    for i, row in enumerate(rows):
        if hasattr(row, "tolist"):
            row = row.tolist()
        row = [float(x) for x in row]
        if width is None:
            width = len(row)
        elif len(row) != width:
            raise ValueError(
                f"row {i} has {len(row)} coordinates, the first has {width}; "
                "every row must be the same width"
            )
        flat.extend(row)
    return flat, True, width


def connect(address: str, channel_options=None) -> Collection:
    """Connect to a coordinator and return the collection it serves.

    ``address`` is ``host:port``, with or without an ``http://`` prefix. The
    connection is lazy, so this does not fail on an unreachable coordinator;
    the first call does.
    """
    for prefix in ("http://", "https://"):
        if address.startswith(prefix):
            address = address[len(prefix) :]
            break
    options = list(_CHANNEL_OPTIONS)
    if channel_options:
        options.extend(channel_options)
    return Collection(grpc.insecure_channel(address, options=options))
