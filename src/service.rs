//! The `TurboVec` gRPC service implementation.
//!
//! Every call resolves a handle from the [`IndexStore`] and then does the
//! actual index work inside `tokio::task::spawn_blocking`, because turbovec's
//! encode and search are CPU-bound and would otherwise stall the async
//! runtime. Searches take the read lock, mutations take the write lock, and
//! the lock is held only for the duration of the blocking call.
//!
//! turbovec's search paths `panic!` on malformed input: a query buffer whose
//! length is not a multiple of the dim, a non-finite coordinate, an allowlist
//! id that is not present in the index. Those cases are validated here and
//! returned as typed [`Status`] values instead, so a bad request is an
//! `INVALID_ARGUMENT`, not a dropped connection. The add path already returns
//! a typed `AddError`, which is mapped the same way.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use turbovec::{first_invalid_coord, IdMapIndex, TurboQuantIndex};

use crate::proto::turbo_vec_server::{TurboVec, TurboVecServer};
use crate::proto::{
    AddRequest, AddResponse, CreateIndexRequest, CreateIndexResponse, DropIndexRequest,
    DropIndexResponse, GetIndexInfoRequest, IndexInfo, IndexKind, ListIndexesRequest,
    ListIndexesResponse, LoadRequest, LoadResponse, QueryResult, RemoveRequest, RemoveResponse,
    SearchRequest, SearchResponse, SnapshotRequest, SnapshotResponse,
};
use crate::store::{Handle, Index, IndexStore};

/// Channel depth between a blocking search task and a streamed response. Wide
/// enough to keep the search running ahead of a reasonable reader, small
/// enough that a stalled reader applies backpressure instead of letting the
/// results pile up.
const SEARCH_STREAM_CAPACITY: usize = 32;

/// gRPC implementation of `turbovec.v1.TurboVec`.
pub struct TurboVecService {
    store: Arc<IndexStore>,
}

impl TurboVecService {
    /// Create the service around an index registry.
    pub fn new(store: IndexStore) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    /// Wrap into the generated tonic service.
    pub fn into_server(self) -> TurboVecServer<Self> {
        TurboVecServer::new(self)
    }

    /// Resolve a handle or fail the RPC with `NOT_FOUND`.
    fn handle(&self, id: &str) -> Result<Handle, Status> {
        self.store
            .get(id)
            .ok_or_else(|| Status::not_found(format!("unknown index_id: {id}")))
    }
}

/// Build the metadata message for one open index.
fn index_info(id: &str, index: &Index) -> IndexInfo {
    IndexInfo {
        index_id: id.to_string(),
        kind: index.kind() as i32,
        dim: index.dim_opt().unwrap_or(0) as u32,
        bit_width: index.bit_width() as u32,
        len: index.len() as u64,
    }
}

/// A validated, prepared filter for one search, built once and reused across
/// every query in the request.
enum Filter<'a> {
    /// No filter: search the whole index.
    None,
    /// Positional slot mask, one entry per slot, `true` for allowed slots.
    Mask(Vec<bool>),
    /// External ids to restrict an id-mapped search to.
    Ids(&'a [u64]),
}

/// Turn an `allowlist` from a request into a prepared [`Filter`], validating
/// it against the index so the search paths cannot panic.
///
/// For a positional index the allowlist is slot indices; for an id-mapped
/// index it is external ids. An empty allowlist means no filter.
fn prepare_filter<'a>(index: &Index, allowlist: &'a [u64]) -> Result<Filter<'a>, Status> {
    if allowlist.is_empty() {
        return Ok(Filter::None);
    }
    match index {
        Index::Positional(inner) => {
            let len = inner.len();
            let mut mask = vec![false; len];
            for &slot in allowlist {
                let slot = usize::try_from(slot)
                    .map_err(|_| Status::invalid_argument(format!("slot {slot} out of range")))?;
                if slot >= len {
                    return Err(Status::invalid_argument(format!(
                        "slot {slot} out of range (index holds {len} vectors)"
                    )));
                }
                mask[slot] = true;
            }
            Ok(Filter::Mask(mask))
        }
        Index::IdMap(inner) => {
            for &id in allowlist {
                if !inner.contains(id) {
                    return Err(Status::invalid_argument(format!(
                        "allowlist id {id} is not present in the index"
                    )));
                }
            }
            Ok(Filter::Ids(allowlist))
        }
    }
}

/// Search a slice of one or more queries against a prepared filter and return
/// one [`QueryResult`] per query. `queries` must be validated (finite, length
/// a whole multiple of `dim`) before this is called.
fn search_prepared(
    index: &Index,
    queries: &[f32],
    dim: usize,
    k: usize,
    filter: &Filter,
) -> Vec<QueryResult> {
    if index.is_empty() {
        // An empty index scores nothing; return an empty neighbour list per
        // query so the response still lines up one-to-one with the input.
        let nq = queries.len() / dim;
        return (0..nq).map(|_| QueryResult::default()).collect();
    }
    match index {
        Index::Positional(inner) => {
            let results = match filter {
                Filter::Mask(mask) => inner.search_with_mask(queries, k, Some(mask)),
                _ => inner.search(queries, k),
            };
            (0..results.nq)
                .map(|qi| QueryResult {
                    scores: results.scores_for_query(qi).to_vec(),
                    ids: results
                        .indices_for_query(qi)
                        .iter()
                        .map(|&slot| slot as u64)
                        .collect(),
                })
                .collect()
        }
        Index::IdMap(inner) => {
            let (scores, ids) = match filter {
                // Ids were validated against the index when the filter was
                // prepared, and an empty allowlist never builds a
                // `Filter::Ids`, so both `SearchError` variants are
                // unreachable here.
                Filter::Ids(allow) => inner
                    .search_with_allowlist(queries, k, Some(allow))
                    .expect("allowlist prevalidated when the filter was prepared"),
                _ => inner.search(queries, k),
            };
            // The id-mapped search returns flat `nq * k_eff` buffers. Recover
            // the per-query stride from the returned length rather than from
            // `k`, because a filter caps the effective count at the number of
            // allowed candidates.
            let nq = queries.len() / dim;
            let k_eff = scores.len().checked_div(nq).unwrap_or(0);
            (0..nq)
                .map(|qi| {
                    let lo = qi * k_eff;
                    let hi = lo + k_eff;
                    QueryResult {
                        scores: scores[lo..hi].to_vec(),
                        ids: ids[lo..hi].to_vec(),
                    }
                })
                .collect()
        }
    }
}

/// Validate a query buffer against a bound `dim`: non-empty, a whole multiple
/// of `dim`, and every coordinate finite and in range for the SIMD kernel.
fn validate_queries(queries: &[f32], dim: usize) -> Result<(), Status> {
    if queries.is_empty() || queries.len() % dim != 0 {
        return Err(Status::invalid_argument(format!(
            "query buffer length {} is not a positive multiple of dim {dim}",
            queries.len()
        )));
    }
    if let Some((qi, ci, value)) = first_invalid_coord(queries, dim) {
        return Err(Status::invalid_argument(format!(
            "invalid query value at query {qi}, coord {ci}: {value}"
        )));
    }
    Ok(())
}

/// Map a `spawn_blocking` join failure onto an internal status.
fn join_err(err: tokio::task::JoinError) -> Status {
    Status::internal(format!("index task failed: {err}"))
}

#[tonic::async_trait]
impl TurboVec for TurboVecService {
    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        let req = request.into_inner();
        let kind = IndexKind::try_from(req.kind)
            .map_err(|_| Status::invalid_argument("unknown index kind"))?;
        let bit_width = req.bit_width as usize;
        let dim = req.dim as usize;

        let index = match (kind, req.lazy) {
            (IndexKind::Unspecified, _) => {
                return Err(Status::invalid_argument("index kind is required"))
            }
            (IndexKind::Positional, true) => Index::Positional(
                TurboQuantIndex::new_lazy(bit_width)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ),
            (IndexKind::Positional, false) => Index::Positional(
                TurboQuantIndex::new(dim, bit_width)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ),
            (IndexKind::IdMap, true) => Index::IdMap(
                IdMapIndex::new_lazy(bit_width)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ),
            (IndexKind::IdMap, false) => Index::IdMap(
                IdMapIndex::new(dim, bit_width)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
            ),
        };

        let id = self.store.insert(index);
        let handle = self.handle(&id)?;
        let info = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            index_info(&id, &guard)
        };
        Ok(Response::new(CreateIndexResponse {
            index_id: id,
            info: Some(info),
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let dropped = self.store.remove(&request.into_inner().index_id);
        Ok(Response::new(DropIndexResponse { dropped }))
    }

    async fn get_index_info(
        &self,
        request: Request<GetIndexInfoRequest>,
    ) -> Result<Response<IndexInfo>, Status> {
        let id = request.into_inner().index_id;
        let handle = self.handle(&id)?;
        let guard = handle
            .read()
            .map_err(|_| Status::internal("index lock poisoned"))?;
        Ok(Response::new(index_info(&id, &guard)))
    }

    async fn list_indexes(
        &self,
        _request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let mut indexes = Vec::new();
        for (id, handle) in self.store.handles() {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            indexes.push(index_info(&id, &guard));
        }
        Ok(Response::new(ListIndexesResponse { indexes }))
    }

    async fn add(
        &self,
        request: Request<Streaming<AddRequest>>,
    ) -> Result<Response<AddResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty add stream: no frames received"))?;
        let index_id = first.index_id.clone();
        let handle = self.handle(&index_id)?;

        // The first frame carries the index_id; process it, then take the rest
        // of the stream, holding every later frame to the same id.
        let mut added = add_chunk(&handle, first).await?;
        while let Some(chunk) = stream.message().await? {
            if !chunk.index_id.is_empty() && chunk.index_id != index_id {
                return Err(Status::invalid_argument(
                    "every add frame must carry the same index_id",
                ));
            }
            added += add_chunk(&handle, chunk).await?;
        }

        let len = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            guard.len() as u64
        };
        Ok(Response::new(AddResponse { added, len }))
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let removed = tokio::task::spawn_blocking(move || {
            let mut guard = handle
                .write()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            match &mut *guard {
                Index::IdMap(inner) => Ok(inner.remove(req.id)),
                Index::Positional(_) => Err(Status::failed_precondition(
                    "remove requires an ID_MAP index",
                )),
            }
        })
        .await
        .map_err(join_err)??;
        Ok(Response::new(RemoveResponse { removed }))
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let k = req.k as usize;
        if k == 0 {
            return Err(Status::invalid_argument("k must be at least 1"));
        }
        let results = tokio::task::spawn_blocking(move || {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            let Some(dim) = guard.dim_opt() else {
                // A lazy index that has never been added to has no dim and no
                // vectors; there is nothing to search.
                return Ok(Vec::new());
            };
            validate_queries(&req.queries, dim)?;
            let filter = prepare_filter(&guard, &req.allowlist)?;
            Ok::<_, Status>(search_prepared(&guard, &req.queries, dim, k, &filter))
        })
        .await
        .map_err(join_err)??;
        Ok(Response::new(SearchResponse { results }))
    }

    type SearchStreamStream = ReceiverStream<Result<QueryResult, Status>>;

    async fn search_stream(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStreamStream>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let k = req.k as usize;
        if k == 0 {
            return Err(Status::invalid_argument("k must be at least 1"));
        }
        let (tx, rx) = mpsc::channel(SEARCH_STREAM_CAPACITY);
        tokio::task::spawn_blocking(move || {
            let guard = match handle.read() {
                Ok(guard) => guard,
                Err(_) => {
                    let _ = tx.blocking_send(Err(Status::internal("index lock poisoned")));
                    return;
                }
            };
            let Some(dim) = guard.dim_opt() else {
                // Nothing to search; end the stream cleanly.
                return;
            };
            if let Err(status) = validate_queries(&req.queries, dim) {
                let _ = tx.blocking_send(Err(status));
                return;
            }
            let filter = match prepare_filter(&guard, &req.allowlist) {
                Ok(filter) => filter,
                Err(status) => {
                    let _ = tx.blocking_send(Err(status));
                    return;
                }
            };
            // One query at a time so the caller receives each neighbour list
            // as soon as it is scored, rather than after the whole batch.
            for query in req.queries.chunks_exact(dim) {
                let mut result = search_prepared(&guard, query, dim, k, &filter);
                let one = result.pop().unwrap_or_default();
                if tx.blocking_send(Ok(one)).is_err() {
                    // Receiver dropped: the client went away.
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let path = req.path;
        let written = path.clone();
        tokio::task::spawn_blocking(move || {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            let result = match &*guard {
                Index::Positional(inner) => inner.write(&path),
                Index::IdMap(inner) => inner.write(&path),
            };
            result.map_err(|e| Status::internal(format!("snapshot write failed: {e}")))
        })
        .await
        .map_err(join_err)??;
        Ok(Response::new(SnapshotResponse { path: written }))
    }

    async fn load(&self, request: Request<LoadRequest>) -> Result<Response<LoadResponse>, Status> {
        let req = request.into_inner();
        let kind = IndexKind::try_from(req.kind)
            .map_err(|_| Status::invalid_argument("unknown index kind"))?;
        let path = req.path;
        let index = tokio::task::spawn_blocking(move || match kind {
            IndexKind::Positional => TurboQuantIndex::load(&path)
                .map(Index::Positional)
                .map_err(|e| Status::internal(format!("load failed: {e}"))),
            IndexKind::IdMap => IdMapIndex::load(&path)
                .map(Index::IdMap)
                .map_err(|e| Status::internal(format!("load failed: {e}"))),
            IndexKind::Unspecified => Err(Status::invalid_argument("index kind is required")),
        })
        .await
        .map_err(join_err)??;

        let id = self.store.insert(index);
        let handle = self.handle(&id)?;
        let info = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            index_info(&id, &guard)
        };
        Ok(Response::new(LoadResponse {
            index_id: id,
            info: Some(info),
        }))
    }
}

/// Add one streamed chunk of vectors to an index under the write lock, on the
/// blocking pool. Returns the number of vectors added.
async fn add_chunk(handle: &Handle, chunk: AddRequest) -> Result<u64, Status> {
    let handle = Arc::clone(handle);
    tokio::task::spawn_blocking(move || {
        let dim = chunk.dim as usize;
        if dim == 0 || chunk.vectors.len() % dim != 0 {
            return Err(Status::invalid_argument(format!(
                "vector buffer length {} is not a positive multiple of dim {dim}",
                chunk.vectors.len()
            )));
        }
        let n = (chunk.vectors.len() / dim) as u64;
        let mut guard = handle
            .write()
            .map_err(|_| Status::internal("index lock poisoned"))?;
        match &mut *guard {
            Index::Positional(inner) => {
                if !chunk.ids.is_empty() {
                    return Err(Status::invalid_argument(
                        "ids must be empty for a positional index",
                    ));
                }
                inner
                    .add_2d(&chunk.vectors, dim)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
            }
            Index::IdMap(inner) => {
                inner
                    .add_with_ids_2d(&chunk.vectors, dim, &chunk.ids)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
            }
        }
        Ok(n)
    })
    .await
    .map_err(join_err)?
}
