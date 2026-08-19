"""The Index handle: one logical index on one node.

This is the parity surface for the embedded ``turbovec`` Python package:
code written against ``TurboQuantIndex(dim, bit_width)`` with ``add``,
``add_with_ids``, ``search``, ``remove`` and ``write(durable=True)`` reads
the same here, with the index living in a server process instead of yours.
The scores are the scores that embedded index would have returned, bit for
bit, because the same engine holds the rows.

Semantic parity, not impersonation. Where the network changes the meaning,
this module says so rather than papering over it:

- ``write(path, durable=True)`` has no path here. Persistence is the node's
  contract: it owns ``TURBOVEC_DATA_DIR``, restores the current generation
  at startup, and :meth:`Index.flush` is the call that persists. There is
  no ``load(path)`` for the same reason; :func:`open_index` reattaches to
  what the node restored.
- There is no ``contains``. The wire has no membership RPC, and a search
  for ``k=1`` is not one.
- There is no node-level ``calibrate(sample)``. Fitting a pair from a
  sample is engine work the node surface does not expose; the node only
  commits a pair fitted elsewhere (``SetCalibration``). Fitting lives on
  the coordinator: :meth:`~turbovec_client.Collection.calibrate` fits one
  pair and commits it to every node, which is also the only way to keep a
  multi-node collection on one calibration.
- Adds are retry-safe by default, which the embedded API has no notion of
  because it cannot lose a response. See :meth:`Index.add`.

Adding rows is a node-level call: a :class:`~turbovec_client.Collection`
is what you search across machines, and it is assembled out of indexes
that already exist. Create and fill them here; search them singly here or
as a collection through :func:`turbovec_client.connect`.
"""

import uuid
from dataclasses import dataclass

import grpc

from ._collection import (
    _CHANNEL_OPTIONS,
    CollectionError,
    Neighbour,
    _flatten,
    _wrap,
)
from ._stubs import turbovec_pb2, turbovec_pb2_grpc

#: Upper bound on the coordinates in one frame of the client-streaming Add
#: upload. The server accepts frames up to 16 MB, but protobuf packs float32
#: coordinates at 4 bytes each plus framing, and keeping a frame near 3 MB
#: leaves headroom for the ids that travel with the same frame on an
#: id-mapped add. Larger uploads are chunked; the server commits the whole
#: stream as one operation regardless of how many frames it arrived in.
ADD_CHUNK_COORDS = 750_000

_KIND = {
    False: turbovec_pb2.INDEX_KIND_POSITIONAL,
    True: turbovec_pb2.INDEX_KIND_ID_MAP,
}


@dataclass(frozen=True)
class IndexInfo:
    """A snapshot of one index's metadata, from :meth:`Index.info`."""

    #: Handle naming the index on its node.
    index_id: str

    #: Vector dimensionality, or 0 while a lazy index's dim is unbound.
    dim: int

    #: Quantization bit width (2, 3, or 4).
    bit_width: int

    #: Vectors currently in the index.
    len: int

    #: ``"calibrated"`` when a TQ+ calibration pair is committed, otherwise
    #: ``"uncalibrated"``.
    calibration_state: str

    #: Durable generation below the node's data dir; 0 while the live index
    #: has never been flushed.
    generation: int


class Index:
    """One logical index on one turbovec node.

    Build one with :func:`create_index` or :func:`open_index`. The handle
    owns its channel and can be closed directly or used as a context
    manager.
    """

    def __init__(self, channel: grpc.Channel, index_id: str, owns_channel: bool = True):
        self._channel = channel
        self._owns_channel = owns_channel
        self._query = turbovec_pb2_grpc.TurboVecQueryStub(channel)
        self._admin = turbovec_pb2_grpc.TurboVecAdminStub(channel)
        #: Handle naming the index on its node, from CreateIndex.
        self.index_id = index_id
        # operation_id -> the expected_len it was first sent with. A retry
        # must reproduce the original envelope, which the node compares
        # against its record of the committed operation.
        self._operations = {}

    def __enter__(self) -> "Index":
        return self

    def __exit__(self, *_exc_info) -> None:
        self.close()

    def __len__(self) -> int:
        return self.info().len

    def close(self) -> None:
        """Release the connection. Idempotent."""
        if self._owns_channel:
            self._channel.close()

    def add(self, vectors, *, operation_id: "str | None" = "") -> str:
        """Append rows to the index and return the operation id used.

        ``vectors`` is one row as a flat sequence of floats, or a sequence
        of such sequences (a numpy array works; numpy is not required). The
        row width must equal the index dim, or binds it on a lazy index's
        first add. On an id-mapped index the wire requires one id per row,
        so use :meth:`add_with_ids` there.

        Retry-safety: the upload carries a fresh ``operation_id`` plus the
        index length observed just before it and the exact row count. The
        node commits the whole stream as one operation and records that id,
        so if the response is lost you can repeat the call with
        ``operation_id`` set to the returned value and the node answers the
        committed result without adding the rows again. The handle
        remembers the envelope of every operation it sent, so a replay
        through the same handle reproduces the original byte for byte; the
        node matches on that envelope, not on content, so the same id with
        a different row count or starting length is refused by name. A node
        running without durable storage (``TURBOVEC_DATA_DIR`` unset)
        refuses the retry-safe envelope by name; pass ``operation_id=None``
        for the plain compatibility ingest such a node accepts.
        """
        flat, rows, width = _shape(vectors)
        if operation_id == "":
            operation_id = uuid.uuid4().hex
        return self._add(flat, rows, width, None, operation_id)

    def add_with_ids(self, vectors, ids, *, operation_id: "str | None" = "") -> str:
        """Append rows with one external id per row. Id-mapped indexes only.

        Same retry-safety contract as :meth:`add`: the returned operation id
        replays the operation without duplicating rows.
        """
        flat, rows, width = _shape(vectors)
        if hasattr(ids, "tolist"):
            ids = ids.tolist()
        ids = [int(i) for i in ids]
        if len(ids) != rows:
            raise ValueError(f"{len(ids)} ids for {rows} rows; give one id per row")
        if operation_id == "":
            operation_id = uuid.uuid4().hex
        return self._add(flat, rows, width, ids, operation_id)

    def _add(self, flat, rows, width, ids, operation_id) -> str:
        expected_len = None
        if operation_id is not None:
            if operation_id not in self._operations:
                self._operations[operation_id] = self.info().len
            expected_len = self._operations[operation_id]
        per_frame = max(1, ADD_CHUNK_COORDS // width)
        first = True

        def frames():
            nonlocal first
            for start in range(0, rows, per_frame):
                stop = min(start + per_frame, rows)
                frame = turbovec_pb2.AddRequest(
                    index_id=self.index_id,
                    dim=width,
                    vectors=flat[start * width : stop * width],
                    ids=ids[start:stop] if ids is not None else [],
                )
                if first:
                    first = False
                    if operation_id is not None:
                        frame.operation_id = operation_id
                        frame.expected_len = expected_len
                        frame.expected_rows = rows
                yield frame

        try:
            self._admin.Add(frames())
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return operation_id

    def search(self, vectors, k: int, *, allowlist=None):
        """Return the top ``k`` neighbours for each query vector.

        ``vectors`` is either one query as a flat sequence of floats, or a
        sequence of such sequences. The return shape follows: a list of
        :class:`Neighbour` for a single query, a list of those lists for a
        batch. ``allowlist`` restricts the search to a candidate set of ids
        (slots for a positional index), like the embedded API's ``mask``
        but naming rows instead of positions.

        Scores are approximate in the way the embedded index's are —
        quantization error, same engine, same numbers. On an id-mapped
        index :attr:`Neighbour.id` is the id the row was added with; on a
        positional one it is the row's slot.
        """
        if k < 1:
            raise ValueError("k must be at least 1")
        queries, batched, _ = _flatten(vectors)
        request = turbovec_pb2.SearchRequest(
            index_id=self.index_id,
            queries=queries,
            k=k,
            allowlist=list(allowlist or []),
        )
        try:
            response = self._query.Search(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        results = [
            [
                Neighbour(id=_node_row_id(result, i), score=result.scores[i])
                for i in range(len(result.scores))
            ]
            for result in response.results
        ]
        return results if batched else results[0]

    def remove(self, id: int) -> bool:
        """Remove one row by its external id. Id-mapped indexes only.

        Returns True if the id was present, False otherwise, like the
        embedded API. On a positional index the node refuses: slots move
        when rows are removed, so positional removal has no stable meaning.
        """
        request = turbovec_pb2.RemoveRequest(index_id=self.index_id, id=int(id))
        try:
            response = self._admin.Remove(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return response.removed

    def flush(self) -> int:
        """Persist the index as a new durable generation and return its
        number.

        This is what ``write(path, durable=True)`` means against a server:
        the node owns the path, so the client names no file. The flushed
        generation is what startup restores, so ``flush`` before a planned
        restart is the durability boundary.
        """
        request = turbovec_pb2.FlushRequest(index_id=self.index_id)
        try:
            response = self._admin.Flush(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return response.generation

    def info(self) -> IndexInfo:
        """Return the index's current metadata."""
        request = turbovec_pb2.GetIndexInfoRequest(index_id=self.index_id)
        try:
            info = self._query.GetIndexInfo(request)
        except grpc.RpcError as error:
            raise _wrap(error) from None
        return IndexInfo(
            index_id=info.index_id,
            dim=info.dim,
            bit_width=info.bit_width,
            len=info.len,
            calibration_state=turbovec_pb2.CalibrationState.Name(info.calibration_state)
            .removeprefix("CALIBRATION_STATE_")
            .lower(),
            generation=info.generation,
        )


def create_index(
    address: str,
    dim: "int | None" = None,
    bit_width: int = 4,
    id_mapped: bool = False,
    channel_options=None,
) -> Index:
    """Create an empty index on the node at ``address`` and return its
    handle.

    The parity call for ``TurboQuantIndex(dim, bit_width)`` (or, with
    ``id_mapped=True``, ``IdMapIndex``). ``dim`` may be omitted, in which
    case the first add binds it, as with the embedded constructor. The
    connection is lazy beyond the create call itself, so an unreachable
    node fails here, not later.
    """
    channel = _channel(address, channel_options)
    admin = turbovec_pb2_grpc.TurboVecAdminStub(channel)
    request = turbovec_pb2.CreateIndexRequest(
        dim=dim or 0,
        bit_width=bit_width,
        kind=_KIND[bool(id_mapped)],
        lazy=dim is None,
    )
    try:
        response = admin.CreateIndex(request)
    except grpc.RpcError as error:
        channel.close()
        raise _wrap(error) from None
    return Index(channel, response.index_id)


def open_index(address: str, index_id: str = "", channel_options=None) -> Index:
    """Attach to an index the node at ``address`` already holds, and return
    its handle.

    This is the stand-in for the embedded ``load``: a node restores its
    flushed generation at startup, so after a restart the index is already
    there and this is how you get it back. With ``index_id`` empty the node
    must hold exactly one index, which is resolved for you; otherwise the
    call refuses rather than guessing.
    """
    channel = _channel(address, channel_options)
    query = turbovec_pb2_grpc.TurboVecQueryStub(channel)
    try:
        if not index_id:
            response = query.ListIndexes(turbovec_pb2.ListIndexesRequest())
            if len(response.indexes) != 1:
                raise CollectionError(
                    f"ambiguous_index: node holds {len(response.indexes)} indexes; "
                    "name one with index_id"
                )
            index_id = response.indexes[0].index_id
        handle = Index(channel, index_id)
        handle.info()  # fail now, not on first use, if the handle is unknown
    except grpc.RpcError as error:
        channel.close()
        raise _wrap(error) from None
    except Exception:
        channel.close()
        raise
    return handle


def _channel(address: str, channel_options=None) -> grpc.Channel:
    """Open an insecure channel to a node, accepting an optional scheme."""
    for prefix in ("http://", "https://"):
        if address.startswith(prefix):
            address = address[len(prefix) :]
            break
    options = list(_CHANNEL_OPTIONS)
    if channel_options:
        options.extend(channel_options)
    return grpc.insecure_channel(address, options=options)


def _shape(vectors):
    """(flat floats, row count, row width) for one row or a batch."""
    flat, batched, width = _flatten(vectors)
    if not batched:
        return flat, 1, width
    return flat, len(flat) // width, width


def _node_row_id(result, i: int) -> int:
    """The id to report for row ``i`` of one node-level QueryResult.

    Mirrors :func:`turbovec_client._collection._row_id` for the node
    surface, whose results carry aligned ``ids`` and ``labels`` arrays
    rather than per-neighbour messages.
    """
    if result.labels:
        return result.labels[i]
    return result.ids[i]
