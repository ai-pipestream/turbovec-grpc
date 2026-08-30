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
//!
//! Four calls exist for the distributed layer rather than for a single-node
//! client: `SetCalibration` and `GetCalibration` commit and read back the TQ+
//! pair that makes separately built indexes score comparably, and `ExportRows`
//! and `ImportRows` move rows between servers as encoded codes. `GetCalibration`
//! answers for both index shapes — an uncalibrated id-mapped index truthfully
//! reports the empty pair, which is what lets a coordinator bind a collection
//! of schema-bound shards. The other three stay positional-only, because they
//! need `TurboQuantIndex`'s raw-parts accessors and `IdMapIndex` does not
//! forward them.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Iter;
use tonic::{Request, Response, Status, Streaming};
use turbovec::{first_invalid_coord, IdMapIndex, SearchOptions, StreamControl, TurboQuantIndex};

use crate::errors::{
    self, INDEX_NOT_EMPTY, INVALID_CALIBRATION, LABELLED_INDEX_IMMUTABLE,
    POSITIONAL_INDEX_REQUIRED, ROW_COUNT_MISMATCH,
};
use crate::observability::Metrics;
use crate::proto::turbo_vec_admin_server::{TurboVecAdmin, TurboVecAdminServer};
use crate::proto::turbo_vec_query_server::{TurboVecQuery, TurboVecQueryServer};
use crate::proto::turbo_vec_server::{TurboVec, TurboVecServer};
use crate::proto::{
    AddRequest, AddResponse, Calibration, CreateIndexRequest, CreateIndexResponse,
    DropIndexRequest, DropIndexResponse, ExportRowsRequest, FlushRequest, FlushResponse,
    GetCalibrationRequest, GetIndexInfoRequest, ImportRowsRequest, ImportRowsResponse, IndexInfo,
    IndexKind, LabelBitmap, ListIndexesRequest, ListIndexesResponse, QueryResult, RemoveRequest,
    RemoveResponse, RowBlock, SearchRequest, SearchResponse, SetCalibrationRequest,
    StreamSearchBatch, StreamSearchRequest, StreamSearchResponse, StreamSearchSummary,
};
use crate::store::{Handle, Index, IndexStore, IngestRecord, Labels};

const MAX_EXPORT_FRAME_BYTES: usize = 2 * 1024 * 1024;

/// Resource limits enforced before CPU or heap work begins.
#[derive(Clone, Debug)]
pub struct ServiceLimits {
    pub max_k: usize,
    pub max_queries_per_request: usize,
    pub max_vector_coordinates_per_frame: usize,
    pub max_concurrent_scans: usize,
}

impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            max_k: 1_000,
            max_queries_per_request: 64,
            max_vector_coordinates_per_frame: 4_000_000,
            max_concurrent_scans: std::thread::available_parallelism()
                .map_or(1, std::num::NonZeroUsize::get),
        }
    }
}

impl ServiceLimits {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self {
            max_k: crate::config::positive_usize("TURBOVEC_MAX_K", defaults.max_k)?,
            max_queries_per_request: crate::config::positive_usize(
                "TURBOVEC_MAX_QUERIES",
                defaults.max_queries_per_request,
            )?,
            max_vector_coordinates_per_frame: crate::config::positive_usize(
                "TURBOVEC_MAX_FRAME_COORDINATES",
                defaults.max_vector_coordinates_per_frame,
            )?,
            max_concurrent_scans: crate::config::positive_usize(
                "TURBOVEC_MAX_CONCURRENT_SCANS",
                defaults.max_concurrent_scans,
            )?,
        })
    }
}

/// gRPC implementation of `turbovec.v1.TurboVec`.
#[derive(Clone)]
pub struct TurboVecService {
    store: Arc<IndexStore>,
    limits: ServiceLimits,
    scan_slots: Arc<Semaphore>,
    metrics: Metrics,
    mutations: Arc<Mutex<HashMap<String, Arc<AsyncMutex<()>>>>>,
}

impl TurboVecService {
    /// Create the service around an index registry.
    pub fn new(store: IndexStore) -> Self {
        Self::from_shared(Arc::new(store))
    }

    /// Create the service around a shared registry, allowing the binary to
    /// flush the same store after graceful server shutdown.
    pub fn from_shared(store: Arc<IndexStore>) -> Self {
        Self::with_limits(store, ServiceLimits::default())
    }

    pub fn with_limits(store: Arc<IndexStore>, limits: ServiceLimits) -> Self {
        Self::with_limits_and_metrics(store, limits, Metrics::default())
    }

    pub fn with_limits_and_metrics(
        store: Arc<IndexStore>,
        limits: ServiceLimits,
        metrics: Metrics,
    ) -> Self {
        assert!(limits.max_k > 0, "max_k must be positive");
        assert!(
            limits.max_queries_per_request > 0,
            "max_queries_per_request must be positive"
        );
        assert!(
            limits.max_vector_coordinates_per_frame > 0,
            "max_vector_coordinates_per_frame must be positive"
        );
        assert!(
            limits.max_concurrent_scans > 0,
            "max_concurrent_scans must be positive"
        );
        let scan_slots = Arc::new(Semaphore::new(limits.max_concurrent_scans));
        Self {
            store,
            limits,
            scan_slots,
            metrics,
            mutations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Wrap into the generated tonic service.
    pub fn into_server(self) -> TurboVecServer<Self> {
        TurboVecServer::new(self)
    }

    pub fn into_query_server(self) -> TurboVecQueryServer<Self> {
        TurboVecQueryServer::new(self)
    }

    pub fn into_admin_server(self) -> TurboVecAdminServer<Self> {
        TurboVecAdminServer::new(self)
    }

    pub fn ready(&self) -> bool {
        self.store.handles().iter().all(|(id, handle)| {
            handle.read().is_ok() && self.store.generation(id).is_some_and(|value| value > 0)
        }) || self.store.data_root().is_none()
    }

    /// Resolve a handle or fail the RPC with `NOT_FOUND`.
    fn handle(&self, id: &str) -> Result<Handle, Status> {
        self.store
            .get(id)
            .ok_or_else(|| Status::not_found(format!("unknown index_id: {id}")))
    }

    /// Build the metadata message for one open index, reading its label table
    /// out of the registry.
    fn info(&self, id: &str, index: &Index) -> IndexInfo {
        index_info(
            id,
            index,
            self.store.labels(id).is_some(),
            self.store.generation(id).unwrap_or(0),
        )
    }

    fn validate_k(&self, k: usize) -> Result<(), Status> {
        if k == 0 || k > self.limits.max_k {
            return Err(Status::invalid_argument(format!(
                "k must be between 1 and {}",
                self.limits.max_k
            )));
        }
        Ok(())
    }

    fn validate_vector_frame(&self, coordinates: usize) -> Result<(), Status> {
        if coordinates > self.limits.max_vector_coordinates_per_frame {
            return Err(Status::resource_exhausted(format!(
                "vector frame has {coordinates} coordinates; limit is {}",
                self.limits.max_vector_coordinates_per_frame
            )));
        }
        Ok(())
    }

    fn mutation_lock(&self, index_id: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self.mutations.lock().expect("mutation lock map poisoned");
        Arc::clone(
            locks
                .entry(index_id.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
        )
    }
}

#[tonic::async_trait]
impl TurboVecQuery for TurboVecService {
    type SearchStreamStream = <Self as TurboVec>::SearchStreamStream;
    type StreamSearchStream = <Self as TurboVec>::StreamSearchStream;

    async fn get_index_info(
        &self,
        request: Request<GetIndexInfoRequest>,
    ) -> Result<Response<IndexInfo>, Status> {
        <Self as TurboVec>::get_index_info(self, request).await
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        <Self as TurboVec>::list_indexes(self, request).await
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        <Self as TurboVec>::search(self, request).await
    }

    async fn search_stream(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStreamStream>, Status> {
        <Self as TurboVec>::search_stream(self, request).await
    }

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        <Self as TurboVec>::stream_search(self, request).await
    }

    async fn get_calibration(
        &self,
        request: Request<GetCalibrationRequest>,
    ) -> Result<Response<Calibration>, Status> {
        <Self as TurboVec>::get_calibration(self, request).await
    }
}

#[tonic::async_trait]
impl TurboVecAdmin for TurboVecService {
    type ExportRowsStream = <Self as TurboVec>::ExportRowsStream;

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        <Self as TurboVec>::create_index(self, request).await
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        <Self as TurboVec>::drop_index(self, request).await
    }

    async fn add(
        &self,
        request: Request<Streaming<AddRequest>>,
    ) -> Result<Response<AddResponse>, Status> {
        <Self as TurboVec>::add(self, request).await
    }

    async fn remove(
        &self,
        request: Request<RemoveRequest>,
    ) -> Result<Response<RemoveResponse>, Status> {
        <Self as TurboVec>::remove(self, request).await
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        <Self as TurboVec>::flush(self, request).await
    }

    async fn set_calibration(
        &self,
        request: Request<SetCalibrationRequest>,
    ) -> Result<Response<Calibration>, Status> {
        <Self as TurboVec>::set_calibration(self, request).await
    }

    async fn export_rows(
        &self,
        request: Request<ExportRowsRequest>,
    ) -> Result<Response<Self::ExportRowsStream>, Status> {
        <Self as TurboVec>::export_rows(self, request).await
    }

    async fn import_rows(
        &self,
        request: Request<Streaming<ImportRowsRequest>>,
    ) -> Result<Response<ImportRowsResponse>, Status> {
        <Self as TurboVec>::import_rows(self, request).await
    }
}

/// Build the metadata message for one open index.
pub(crate) fn index_info(id: &str, index: &Index, labelled: bool, generation: u64) -> IndexInfo {
    IndexInfo {
        index_id: id.to_string(),
        kind: index.kind() as i32,
        dim: index.dim_opt().unwrap_or(0) as u32,
        bit_width: index.bit_width() as u32,
        len: index.len() as u64,
        calibration_state: calibration_state(index.calibration_state()) as i32,
        labelled,
        generation,
    }
}

/// Map turbovec's calibration state onto the wire enum.
///
/// `turbovec::CalibrationState` is `#[non_exhaustive]`, so a future upstream
/// state has no wire value here. It is reported as unspecified rather than
/// guessed at, and nothing in the distributed layer decides anything on this
/// field: whether two indexes share a calibration is settled by comparing the
/// pairs themselves, which is a question this enum cannot answer either way.
fn calibration_state(state: turbovec::CalibrationState) -> crate::proto::CalibrationState {
    match state {
        turbovec::CalibrationState::Uncalibrated => crate::proto::CalibrationState::Uncalibrated,
        turbovec::CalibrationState::Calibrated => crate::proto::CalibrationState::Calibrated,
        _ => crate::proto::CalibrationState::Unspecified,
    }
}

/// Borrow the positional index behind a handle, or fail by name.
///
/// The raw-parts accessors this server's distribution calls are built on
/// (`packed_codes`, `scales`, the TQ+ getters, `from_parts`) exist on
/// `TurboQuantIndex` and are not forwarded by `IdMapIndex`, so an id-mapped
/// index cannot be exported, imported into, or have its pair read back. That
/// is a limit of the upstream surface, not a policy, and it is reported as
/// such rather than worked around by decoding and re-encoding rows, which
/// would change the codes and so the scores.
fn positional<'a>(index: &'a Index, what: &str) -> Result<&'a TurboQuantIndex, Status> {
    match index {
        Index::Positional(inner) => Ok(inner),
        Index::IdMap(_) => Err(errors::precondition(
            POSITIONAL_INDEX_REQUIRED,
            format!(
                "{what} needs a POSITIONAL index; turbovec's IdMapIndex does not expose the \
                 packed codes or the calibration pair this call reads"
            ),
        )),
    }
}

/// Read one index's calibration pair into the wire message.
fn calibration_of(index: &TurboQuantIndex) -> Calibration {
    Calibration {
        state: calibration_state(index.calibration_state()) as i32,
        tqplus_shift: index.tqplus_shift().to_vec(),
        tqplus_scale: index.tqplus_scale().to_vec(),
    }
}

/// Read the calibration pair behind a handle, for either index shape.
///
/// The pair is what a distributed caller compares: the coordinator binds a
/// collection by holding every shard to one pair, and schema-bound
/// (id-mapped) shards have to answer that question too. `IdMapIndex` does
/// not forward the TQ+ getters, but an uncalibrated index's pair is the
/// empty one by definition, and no call on this server can commit a pair to
/// an id-mapped index (`SetCalibration` is positional-only), so Uncalibrated
/// is the one state an id-mapped index can truthfully report. The empty pair
/// is not a gap in the exactness story: uncalibrated encoding is a pure
/// per-row function (fixed rotation and codebook from `(dim, bit_width)`,
/// per-row scales), so rows score identically wherever they are stored,
/// exactly as they do under a shared non-empty pair.
///
/// An id-mapped index that reports Calibrated anyway (possible only through
/// foreign snapshot bytes) has a pair this server cannot read, and that is
/// refused by name rather than reported as empty.
fn calibration_of_index(index: &Index) -> Result<Calibration, Status> {
    let (tqplus_shift, tqplus_scale) = match index {
        Index::Positional(inner) => (inner.tqplus_shift().to_vec(), inner.tqplus_scale().to_vec()),
        Index::IdMap(inner) => match inner.calibration_state() {
            turbovec::CalibrationState::Uncalibrated => (Vec::new(), Vec::new()),
            _ => {
                return Err(errors::precondition(
                    INVALID_CALIBRATION,
                    "this id-mapped index holds a committed TQ+ pair, which turbovec's \
                     IdMapIndex does not expose for reading; its pair cannot be verified \
                     against a collection's",
                ))
            }
        },
    };
    Ok(Calibration {
        state: calibration_state(index.calibration_state()) as i32,
        tqplus_shift,
        tqplus_scale,
    })
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

pub(crate) fn validate_label_bitmaps(bitmaps: &[LabelBitmap]) -> Result<(), Status> {
    let mut previous_end = 0u64;
    for (position, bitmap) in bitmaps.iter().enumerate() {
        if bitmap.label_count == 0 {
            return Err(Status::invalid_argument(format!(
                "allowed label bitmap {position} has zero labels"
            )));
        }
        let expected = usize::try_from(bitmap.label_count.div_ceil(8)).map_err(|_| {
            Status::invalid_argument(format!(
                "allowed label bitmap {position} is too large for this process"
            ))
        })?;
        if bitmap.bits.len() != expected {
            return Err(Status::invalid_argument(format!(
                "allowed label bitmap {position} has {} bytes for {} labels; expected {expected}",
                bitmap.bits.len(),
                bitmap.label_count
            )));
        }
        let end = bitmap
            .base_label
            .checked_add(bitmap.label_count)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "allowed label bitmap {position} range overflows u64"
                ))
            })?;
        if position > 0 && bitmap.base_label < previous_end {
            return Err(Status::invalid_argument(format!(
                "allowed label bitmap {position} overlaps or is out of order"
            )));
        }
        if !bitmap.label_count.is_multiple_of(8) {
            let used = (bitmap.label_count % 8) as u8;
            let unused_mask = !((1u8 << used) - 1);
            if bitmap
                .bits
                .last()
                .is_some_and(|byte| byte & unused_mask != 0)
            {
                return Err(Status::invalid_argument(format!(
                    "allowed label bitmap {position} has set bits beyond label_count"
                )));
            }
        }
        previous_end = end;
    }
    Ok(())
}

fn label_in_bitmaps(bitmaps: &[LabelBitmap], label: u64) -> bool {
    let position = bitmaps.partition_point(|bitmap| bitmap.base_label <= label);
    let Some(bitmap) = position.checked_sub(1).and_then(|index| bitmaps.get(index)) else {
        return false;
    };
    let offset = label - bitmap.base_label;
    if offset >= bitmap.label_count {
        return false;
    }
    let Ok(offset) = usize::try_from(offset) else {
        return false;
    };
    bitmap.bits[offset / 8] & (1 << (offset % 8)) != 0
}

/// Search a slice of one or more queries against a prepared filter and return
/// one [`QueryResult`] per query. `queries` must be validated (finite, length
/// a whole multiple of `dim`) before this is called.
///
/// `labels`, when the index carries them, is reported alongside the slots: a
/// distributed caller reads those, because a row's slot changes when it moves
/// between servers and its label does not.
fn search_prepared(
    index: &Index,
    queries: &[f32],
    dim: usize,
    k: usize,
    filter: &Filter,
    labels: Option<&Labels>,
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
                .map(|qi| {
                    let slots: Vec<u64> = results
                        .indices_for_query(qi)
                        .iter()
                        .map(|&slot| slot as u64)
                        .collect();
                    // A label table is registered with exactly one entry per
                    // row and the index it covers never takes another row
                    // (Add refuses a labelled index), so every returned slot
                    // is in range.
                    let labels = labels.map_or_else(Vec::new, |table| {
                        slots
                            .iter()
                            .map(|&slot| {
                                *table
                                    .get(slot as usize)
                                    .expect("label table covers every slot of its index")
                            })
                            .collect()
                    });
                    QueryResult {
                        scores: results.scores_for_query(qi).to_vec(),
                        ids: slots,
                        labels,
                    }
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
            // allowed candidates. The stride is uniform across queries:
            // turbovec computes `effective_k = min(k, n_vectors, n_allowed)`
            // once per call (see `search_with_mask` in the turbovec crate),
            // and the mask is shared by every query in the batch.
            let nq = queries.len() / dim;
            let k_eff = scores.len().checked_div(nq).unwrap_or(0);
            (0..nq)
                .map(|qi| {
                    let lo = qi * k_eff;
                    let hi = lo + k_eff;
                    QueryResult {
                        scores: scores[lo..hi].to_vec(),
                        ids: ids[lo..hi].to_vec(),
                        // An id-mapped index already returns the caller's own
                        // ids, so there is nothing a label table would add;
                        // ImportRows, the only call that registers one, is
                        // positional-only.
                        labels: Vec::new(),
                    }
                })
                .collect()
        }
    }
}

/// Validate a query buffer against a bound `dim`: non-empty, a whole multiple
/// of `dim`, and every coordinate finite and in range for the SIMD kernel.
pub(crate) fn validate_queries(queries: &[f32], dim: usize) -> Result<(), Status> {
    if queries.is_empty() || !queries.len().is_multiple_of(dim) {
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

/// Raise an atomic floating-point floor if `candidate` is strictly greater.
///
/// Every caller rejects NaN first. AtomicU32 is used only as a transport for
/// the bits; ordering is performed as f32 so negative floors retain their
/// numeric order.
fn raise_floor(cell: &AtomicU32, candidate: f32) {
    let mut current_bits = cell.load(Ordering::Acquire);
    loop {
        let current = f32::from_bits(current_bits);
        if candidate <= current {
            return;
        }
        match cell.compare_exchange_weak(
            current_bits,
            candidate.to_bits(),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(actual) => current_bits = actual,
        }
    }
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
            self.info(&id, &guard)
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
        let index_id = request.into_inner().index_id;
        let store = Arc::clone(&self.store);
        let dropped = tokio::task::spawn_blocking(move || store.delete(&index_id))
            .await
            .map_err(join_err)?
            .map_err(|e| Status::internal(e.to_string()))?;
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
        Ok(Response::new(self.info(&id, &guard)))
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
            indexes.push(self.info(&id, &guard));
        }
        Ok(Response::new(ListIndexesResponse { indexes }))
    }

    async fn add(
        &self,
        request: Request<Streaming<AddRequest>>,
    ) -> Result<Response<AddResponse>, Status> {
        let mut stream = request.into_inner();
        let mut combined = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty add stream: no frames received"))?;
        self.validate_vector_frame(combined.vectors.len())?;
        let index_id = combined.index_id.clone();
        let operation_id = combined.operation_id.clone();
        let expected_len = combined.expected_len;
        let expected_rows = combined.expected_rows;
        let handle = self.handle(&index_id)?;
        if self.store.labels(&index_id).is_some() {
            // The label table was built with one entry per row at import and
            // there is no id to give a row added afterwards, so an add would
            // leave the tail of the index unlabelled and every search on it
            // returning ids for some rows and nothing for others.
            return Err(errors::precondition(
                LABELLED_INDEX_IMMUTABLE,
                format!(
                    "index {index_id} carries external row labels from ImportRows; \
                     build a new index from its rows plus the new ones instead"
                ),
            ));
        }

        // Validate and stage the complete bounded operation before mutating the
        // index. A broken stream therefore commits no prefix.
        while let Some(chunk) = stream.message().await? {
            if !chunk.index_id.is_empty() && chunk.index_id != index_id {
                return Err(Status::invalid_argument(
                    "every add frame must carry the same index_id",
                ));
            }
            if chunk.dim != combined.dim
                || (!chunk.operation_id.is_empty() && chunk.operation_id != operation_id)
                || chunk
                    .expected_len
                    .is_some_and(|value| Some(value) != expected_len)
                || (chunk.expected_rows != 0 && chunk.expected_rows != expected_rows)
            {
                return Err(Status::invalid_argument(
                    "every add frame must repeat the same operation metadata",
                ));
            }
            let next_coordinates = combined
                .vectors
                .len()
                .checked_add(chunk.vectors.len())
                .ok_or_else(|| Status::resource_exhausted("add operation is too large"))?;
            self.validate_vector_frame(next_coordinates)?;
            combined.vectors.extend(chunk.vectors);
            combined.ids.extend(chunk.ids);
        }

        let dim = combined.dim as usize;
        if dim == 0 || !combined.vectors.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument(format!(
                "vector buffer length {} is not a positive multiple of dim {dim}",
                combined.vectors.len()
            )));
        }
        let rows = (combined.vectors.len() / dim) as u64;
        if !operation_id.is_empty() {
            if self.store.data_root().is_none() {
                return Err(Status::failed_precondition(
                    "retry-safe ingest requires TURBOVEC_DATA_DIR",
                ));
            }
            if expected_len.is_none() || expected_rows != rows {
                return Err(Status::invalid_argument(format!(
                    "operation {operation_id} must declare expected_len and exactly {rows} expected_rows"
                )));
            }
        } else if expected_len.is_some() || expected_rows != 0 {
            return Err(Status::invalid_argument(
                "expected_len and expected_rows require operation_id",
            ));
        }

        let mutation = self.mutation_lock(&index_id);
        let _mutation_guard = mutation.lock_owned().await;
        let current_len = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            guard.len() as u64
        };
        if !operation_id.is_empty() {
            if let Some(record) = self.store.ingest_record(&index_id) {
                if record.operation_id == operation_id {
                    if record.expected_len != expected_len.expect("checked above")
                        || record.rows != rows
                        || record.len != current_len
                    {
                        return Err(Status::failed_precondition(
                            "operation_id was already used with different ingest metadata",
                        ));
                    }
                    if self.store.generation(&index_id) == Some(record.generation) {
                        return Ok(Response::new(AddResponse {
                            added: rows,
                            len: record.len,
                            generation: record.generation,
                            replayed: true,
                            operation_id,
                        }));
                    }
                    let store = Arc::clone(&self.store);
                    let persisted_id = index_id.clone();
                    let generation =
                        tokio::task::spawn_blocking(move || store.persist(&persisted_id))
                            .await
                            .map_err(join_err)?
                            .map_err(|e| Status::internal(e.to_string()))?;
                    return Ok(Response::new(AddResponse {
                        added: rows,
                        len: record.len,
                        generation,
                        replayed: true,
                        operation_id,
                    }));
                }
            }
            let expected = expected_len.expect("checked above");
            if current_len != expected {
                return Err(Status::aborted(format!(
                    "ingest generation conflict: expected len {expected}, current len is {current_len}"
                )));
            }
        }

        let added = add_chunk(&handle, combined).await?;
        let len = current_len + added;
        let generation = if operation_id.is_empty() {
            0
        } else {
            self.store.set_ingest_record(
                &index_id,
                IngestRecord {
                    operation_id: operation_id.clone(),
                    expected_len: expected_len.expect("checked above"),
                    rows,
                    len,
                    generation: self
                        .store
                        .generation(&index_id)
                        .unwrap_or(0)
                        .checked_add(1)
                        .ok_or_else(|| Status::internal("generation counter overflow"))?,
                },
            );
            let store = Arc::clone(&self.store);
            let persisted_id = index_id.clone();
            tokio::task::spawn_blocking(move || store.persist(&persisted_id))
                .await
                .map_err(join_err)?
                .map_err(|e| Status::internal(e.to_string()))?
        };
        self.metrics.rows_ingested(added);
        Ok(Response::new(AddResponse {
            added,
            len,
            generation,
            replayed: false,
            operation_id,
        }))
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
        let labels = self.store.labels(&req.index_id);
        let k = req.k as usize;
        self.validate_k(k)?;
        self.validate_vector_frame(req.queries.len())?;
        let permit = Arc::clone(&self.scan_slots)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("search admission is shutting down"))?;
        let active_scan = self.metrics.scan_started();
        let max_queries = self.limits.max_queries_per_request;
        let results = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _active_scan = active_scan;
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            let Some(dim) = guard.dim_opt() else {
                // A lazy index that has never been added to has no bound dim,
                // so the query buffer cannot even be chunked into queries.
                return Err(Status::failed_precondition(
                    "index has no vectors; add vectors before searching",
                ));
            };
            validate_queries(&req.queries, dim)?;
            let queries = req.queries.len() / dim;
            if queries > max_queries {
                return Err(Status::resource_exhausted(format!(
                    "request has {queries} queries; limit is {max_queries}"
                )));
            }
            let filter = prepare_filter(&guard, &req.allowlist)?;
            Ok::<_, Status>(search_prepared(
                &guard,
                &req.queries,
                dim,
                k,
                &filter,
                labels.as_ref(),
            ))
        })
        .await
        .map_err(join_err)??;
        self.metrics.node_search_finished(
            results
                .iter()
                .map(|result| result.scores.len() as u64)
                .sum(),
            0,
        );
        Ok(Response::new(SearchResponse { results }))
    }

    type SearchStreamStream = Iter<std::vec::IntoIter<Result<QueryResult, Status>>>;

    /// Same batch search as [`Self::search`], returned as a stream of one
    /// `QueryResult` per query. The whole batch is scored under a single
    /// short read-lock hold and the lock is released before streaming
    /// starts: holding it across the stream would let a stalled client pin
    /// the lock (starving writers on the index) and park a blocking-pool
    /// thread for the life of the stream, and enough such threads exhaust
    /// the pool and stall every `spawn_blocking` call on every index.
    async fn search_stream(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<Self::SearchStreamStream>, Status> {
        let response = <Self as TurboVec>::search(self, request).await?;
        let items: Vec<Result<QueryResult, Status>> =
            response.into_inner().results.into_iter().map(Ok).collect();
        Ok(Response::new(tokio_stream::iter(items)))
    }

    type StreamSearchStream = ReceiverStream<Result<StreamSearchResponse, Status>>;

    /// Run one positional-index scan whose candidate floor can rise while the
    /// scan is in progress. The node owns no top-k heap. It emits every
    /// candidate admitted by turbovec's inclusive live floor and lets the
    /// coordinator decide which candidates survive globally.
    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("stream must start with StartStreamSearch"))?;
        let start = match first.payload {
            Some(crate::proto::stream_search_request::Payload::Start(start)) => start,
            _ => {
                return Err(Status::invalid_argument(
                    "first stream message must be StartStreamSearch",
                ))
            }
        };
        let handle = self.handle(&start.index_id)?;
        let labels = self.store.labels(&start.index_id);
        self.validate_vector_frame(start.vector.len())?;
        if !start.has_allowed_labels
            && (!start.allowed_labels.is_empty() || !start.allowed_label_bitmaps.is_empty())
        {
            return Err(Status::invalid_argument(
                "allowed label filters require has_allowed_labels=true",
            ));
        }
        if !start.allowed_labels.is_empty() && !start.allowed_label_bitmaps.is_empty() {
            return Err(Status::invalid_argument(
                "allowed_labels and allowed_label_bitmaps are mutually exclusive",
            ));
        }
        validate_label_bitmaps(&start.allowed_label_bitmaps)?;
        if start.has_allowed_labels && labels.is_none() {
            return Err(Status::failed_precondition(
                "stable-label filtering requires a labelled positional index",
            ));
        }
        let initial_floor = start.initial_floor.unwrap_or(f32::NEG_INFINITY);
        if initial_floor.is_nan() {
            return Err(Status::invalid_argument("initial_floor must not be NaN"));
        }

        let floor = Arc::new(AtomicU32::new(initial_floor.to_bits()));
        let stop = Arc::new(AtomicBool::new(false));
        let protocol_error: Arc<Mutex<Option<Status>>> = Arc::new(Mutex::new(None));

        // The inbound pump is deliberately independent of the response
        // channel. It may wait for another update after the scan has already
        // completed, and must not keep the outbound stream alive by retaining
        // a sender.
        let pump_floor = Arc::clone(&floor);
        let pump_stop = Arc::clone(&stop);
        let pump_error = Arc::clone(&protocol_error);
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(message)) => match message.payload {
                        Some(crate::proto::stream_search_request::Payload::FloorUpdate(update)) => {
                            if update.floor.is_nan() {
                                *pump_error.lock().expect("stream protocol lock poisoned") =
                                    Some(Status::invalid_argument("floor must not be NaN"));
                                pump_stop.store(true, Ordering::Release);
                                break;
                            }
                            raise_floor(&pump_floor, update.floor);
                        }
                        Some(crate::proto::stream_search_request::Payload::Stop(_)) => {
                            pump_stop.store(true, Ordering::Release);
                            break;
                        }
                        Some(crate::proto::stream_search_request::Payload::Start(_)) | None => {
                            *pump_error.lock().expect("stream protocol lock poisoned") =
                                Some(Status::invalid_argument(
                                    "StartStreamSearch must appear exactly once and first",
                                ));
                            pump_stop.store(true, Ordering::Release);
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(status) => {
                        *pump_error.lock().expect("stream protocol lock poisoned") = Some(status);
                        pump_stop.store(true, Ordering::Release);
                        break;
                    }
                }
            }
        });

        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let request_id = start.request_id.clone();
        let permit = Arc::clone(&self.scan_slots)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("search admission is shutting down"))?;
        let active_scan = self.metrics.scan_started();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let scan_tx = tx.clone();
            let scan_floor = Arc::clone(&floor);
            let scan_stop = Arc::clone(&stop);
            let outcome = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _active_scan = active_scan;
                let guard = handle
                    .read()
                    .map_err(|_| Status::internal("index lock poisoned"))?;
                let inner = positional(&guard, "StreamSearch")?;
                let Some(dim) = inner.dim_opt() else {
                    return Err(Status::failed_precondition(
                        "index has no vectors; add vectors before searching",
                    ));
                };
                validate_queries(&start.vector, dim)?;
                if start.vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "StreamSearch accepts exactly one query of dim {dim}; got {} coordinates",
                        start.vector.len()
                    )));
                }

                let allow_mask = start.has_allowed_labels.then(|| {
                    let labels = labels
                        .as_ref()
                        .expect("label presence validated before scan task");
                    if start.allowed_label_bitmaps.is_empty() {
                        let admitted: HashSet<u64> = start.allowed_labels.iter().copied().collect();
                        labels
                            .iter()
                            .map(|label| admitted.contains(label))
                            .collect::<Vec<_>>()
                    } else {
                        labels
                            .iter()
                            .map(|&label| label_in_bitmaps(&start.allowed_label_bitmaps, label))
                            .collect::<Vec<_>>()
                    }
                });
                let mut options = if initial_floor == f32::NEG_INFINITY {
                    SearchOptions::new()
                } else {
                    SearchOptions::new().with_initial_threshold(initial_floor)
                };
                if let Some(mask) = allow_mask.as_deref() {
                    options = options.with_mask(mask);
                }
                let mut floor_now = initial_floor;
                let mut floor_raises = 0u64;
                let summary = inner
                    .try_search_streaming_controlled(
                        &start.vector,
                        options,
                        |batch| {
                            let slots: Vec<u64> = batch
                                .slots
                                .iter()
                                .map(|&slot| {
                                    u64::try_from(slot).expect(
                                        "streaming search emits only live non-negative slots",
                                    )
                                })
                                .collect();
                            let labels = labels.as_ref().map_or_else(Vec::new, |table| {
                                slots
                                    .iter()
                                    .map(|&slot| {
                                        *table
                                            .get(slot as usize)
                                            .expect("label table covers every slot of its index")
                                    })
                                    .collect()
                            });
                            let response = StreamSearchResponse {
                                payload: Some(
                                    crate::proto::stream_search_response::Payload::Batch(
                                        StreamSearchBatch {
                                            scores: batch.scores.to_vec(),
                                            slots,
                                            labels,
                                        },
                                    ),
                                ),
                            };
                            if scan_tx.blocking_send(Ok(response)).is_err() {
                                StreamControl::Stop
                            } else {
                                StreamControl::Continue
                            }
                        },
                        || {
                            if scan_stop.load(Ordering::Acquire) {
                                return StreamControl::Stop;
                            }
                            let candidate = f32::from_bits(scan_floor.load(Ordering::Acquire));
                            if candidate > floor_now {
                                floor_now = candidate;
                                floor_raises += 1;
                                StreamControl::RaiseFloor(candidate)
                            } else {
                                StreamControl::Continue
                            }
                        },
                    )
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let summary = StreamSearchSummary {
                    completed: summary.completed,
                    emitted: summary.emitted as u64,
                    blocks_scanned: summary.blocks_scanned as u64,
                    floor_raises_applied: floor_raises,
                };
                tracing::info!(
                    request_id = %request_id,
                    index_id = %start.index_id,
                    completed = summary.completed,
                    emitted = summary.emitted,
                    blocks_scanned = summary.blocks_scanned,
                    floor_raises = summary.floor_raises_applied,
                    "shard scan finished"
                );
                Ok::<_, Status>(summary)
            })
            .await
            .map_err(join_err);

            let protocol_error = protocol_error
                .lock()
                .expect("stream protocol lock poisoned")
                .take();
            if let Some(status) = protocol_error {
                let _ = tx.send(Err(status)).await;
                return;
            }
            match outcome {
                Ok(Ok(summary)) => {
                    metrics.node_search_finished(summary.emitted, summary.blocks_scanned);
                    let _ = tx
                        .send(Ok(StreamSearchResponse {
                            payload: Some(crate::proto::stream_search_response::Payload::Summary(
                                summary,
                            )),
                        }))
                        .await;
                }
                Ok(Err(status)) | Err(status) => {
                    metrics.search_failed();
                    let _ = tx.send(Err(status)).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn flush(
        &self,
        request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let index_id = request.into_inner().index_id;
        // Resolve first so an unknown id stays NOT_FOUND rather than being
        // folded into an internal persistence error.
        self.handle(&index_id)?;
        let store = Arc::clone(&self.store);
        let persisted_id = index_id.clone();
        let generation = tokio::task::spawn_blocking(move || store.persist(&persisted_id))
            .await
            .map_err(join_err)?
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        Ok(Response::new(FlushResponse {
            index_id,
            generation,
        }))
    }

    async fn set_calibration(
        &self,
        request: Request<SetCalibrationRequest>,
    ) -> Result<Response<Calibration>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let calibration = tokio::task::spawn_blocking(move || {
            let mut guard = handle
                .write()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            let index = positional(&guard, "SetCalibration")?;
            if !index.is_empty() {
                // Committing a pair here is `from_parts`, which builds an
                // index around the pair. Rows already in the index were
                // encoded under whatever pair was committed when they arrived,
                // and reinterpreting those codes under a different pair would
                // silently change every one of their scores. Re-encoding them
                // instead is turbovec's `calibrate`, a different operation with
                // a different (lossy) contract, which this call does not stand
                // in for.
                return Err(errors::precondition(
                    INDEX_NOT_EMPTY,
                    format!(
                        "SetCalibration needs an empty index; this one holds {} vectors \
                         already encoded under its current pair",
                        index.len()
                    ),
                ));
            }
            let dim = index.dim_opt().ok_or_else(|| {
                errors::precondition(
                    INVALID_CALIBRATION,
                    "SetCalibration needs an index with a bound dim; a lazy index binds its \
                     dim on the first Add, and a pair is per-coordinate",
                )
            })?;
            if req.tqplus_shift.len() != dim || req.tqplus_scale.len() != dim {
                return Err(errors::invalid(
                    INVALID_CALIBRATION,
                    format!(
                        "calibration pair has {} shift and {} scale coordinates, index dim is {dim}",
                        req.tqplus_shift.len(),
                        req.tqplus_scale.len()
                    ),
                ));
            }
            let rebuilt = TurboQuantIndex::from_parts(
                Some(dim),
                index.bit_width(),
                0,
                Vec::new(),
                Vec::new(),
                req.tqplus_shift,
                req.tqplus_scale,
            )
            .map_err(|e| errors::invalid(INVALID_CALIBRATION, e.to_string()))?;
            let calibration = calibration_of(&rebuilt);
            *guard = Index::Positional(rebuilt);
            Ok::<_, Status>(calibration)
        })
        .await
        .map_err(join_err)??;
        Ok(Response::new(calibration))
    }

    async fn get_calibration(
        &self,
        request: Request<GetCalibrationRequest>,
    ) -> Result<Response<Calibration>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let guard = handle
            .read()
            .map_err(|_| Status::internal("index lock poisoned"))?;
        Ok(Response::new(calibration_of_index(&guard)?))
    }

    type ExportRowsStream = ReceiverStream<Result<RowBlock, Status>>;

    async fn export_rows(
        &self,
        request: Request<ExportRowsRequest>,
    ) -> Result<Response<Self::ExportRowsStream>, Status> {
        let req = request.into_inner();
        let handle = self.handle(&req.index_id)?;
        let labels = self.store.labels(&req.index_id);
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tokio::task::spawn_blocking(move || {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"));
            let result = guard.and_then(|guard| {
                let index = positional(&guard, "ExportRows")?;
                let (start, rows) = export_range(index, req.start, req.count)?;
                let dim = index.dim_opt().ok_or_else(|| {
                    errors::precondition(
                        ROW_COUNT_MISMATCH,
                        "ExportRows needs an index with a bound dim; this one has never held a row",
                    )
                })?;
                let bytes_per_row = index.bit_width() * (dim / 8) + 12;
                let calibration_bytes = dim.saturating_mul(8);
                let row_budget = MAX_EXPORT_FRAME_BYTES.saturating_sub(calibration_bytes);
                let rows_per_frame = (row_budget / bytes_per_row.max(1)).max(1);

                if rows == 0 {
                    let block = export_block(index, labels.as_ref(), start as u64, 0)?;
                    tx.blocking_send(Ok(block))
                        .map_err(|_| Status::cancelled("export receiver closed"))?;
                    return Ok(());
                }
                let mut offset = 0usize;
                while offset < rows {
                    let count = rows_per_frame.min(rows - offset);
                    let block = export_block(
                        index,
                        labels.as_ref(),
                        (start + offset) as u64,
                        count as u64,
                    )?;
                    tx.blocking_send(Ok(block))
                        .map_err(|_| Status::cancelled("export receiver closed"))?;
                    offset += count;
                }
                Ok(())
            });
            if let Err(status) = result {
                let _ = tx.blocking_send(Err(status));
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn import_rows(
        &self,
        request: Request<Streaming<ImportRowsRequest>>,
    ) -> Result<Response<ImportRowsResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty import stream"))?;
        let expected_rows = match first.payload {
            Some(crate::proto::import_rows_request::Payload::Start(start)) => {
                usize::try_from(start.expected_rows).map_err(|_| {
                    errors::invalid(ROW_COUNT_MISMATCH, "expected row count is out of range")
                })?
            }
            _ => {
                return Err(Status::invalid_argument(
                    "first import frame must be ImportRowsStart",
                ))
            }
        };
        let mut builder = ImportBuilder::new(expected_rows);
        while let Some(frame) = stream.message().await? {
            match frame.payload {
                Some(crate::proto::import_rows_request::Payload::Block(block)) => {
                    builder.push(block)?;
                }
                Some(crate::proto::import_rows_request::Payload::Start(_)) | None => {
                    return Err(Status::invalid_argument(
                        "ImportRowsStart must appear exactly once and first",
                    ))
                }
            }
        }
        let (index, labels) = tokio::task::spawn_blocking(move || builder.finish())
            .await
            .map_err(join_err)??;

        let id = self.store.insert_labelled(Index::Positional(index), labels);
        let handle = self.handle(&id)?;
        let info = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            self.info(&id, &guard)
        };
        Ok(Response::new(ImportRowsResponse {
            index_id: id,
            info: Some(info),
        }))
    }
}

fn export_range(index: &TurboQuantIndex, start: u64, count: u64) -> Result<(usize, usize), Status> {
    let len = index.len();
    let start = usize::try_from(start).map_err(|_| {
        errors::invalid(ROW_COUNT_MISMATCH, format!("start {start} is out of range"))
    })?;
    let count = usize::try_from(count).map_err(|_| {
        errors::invalid(ROW_COUNT_MISMATCH, format!("count {count} is out of range"))
    })?;
    if start > len {
        return Err(errors::invalid(
            ROW_COUNT_MISMATCH,
            format!("start {start} is past the end of an index holding {len} rows"),
        ));
    }
    let rows = if count == 0 { len - start } else { count };
    let end = start
        .checked_add(rows)
        .ok_or_else(|| errors::invalid(ROW_COUNT_MISMATCH, "export range overflowed"))?;
    if end > len {
        return Err(errors::invalid(
            ROW_COUNT_MISMATCH,
            format!("rows {start}..{} run past an index holding {len} rows", end),
        ));
    }
    Ok((start, rows))
}

/// Copy `count` rows starting at `start` out of `index` into a [`RowBlock`].
///
/// The copy is a byte-range slice of the packed codes: rows are contiguous in
/// the packed layout, `bit_width * dim / 8` bytes each, so nothing is decoded
/// and no code changes. `count` of zero means "to the end".
fn export_block(
    index: &TurboQuantIndex,
    labels: Option<&Labels>,
    start: u64,
    count: u64,
) -> Result<RowBlock, Status> {
    let (start, rows) = export_range(index, start, count)?;

    // An empty index has no bound dim to describe the export with, and a
    // zero-row block from a populated one still carries the geometry, so the
    // only unanswerable case is exporting from an index that has never held a
    // row.
    let dim = index.dim_opt().ok_or_else(|| {
        errors::precondition(
            ROW_COUNT_MISMATCH,
            "ExportRows needs an index with a bound dim; this one has never held a row",
        )
    })?;
    let bytes_per_row = index.bit_width() * (dim / 8);
    let packed = index.packed_codes();
    Ok(RowBlock {
        dim: dim as u32,
        bit_width: index.bit_width() as u32,
        rows: rows as u64,
        packed_codes: packed[start * bytes_per_row..(start + rows) * bytes_per_row].to_vec(),
        scales: index.scales()[start..start + rows].to_vec(),
        tqplus_shift: index.tqplus_shift().to_vec(),
        tqplus_scale: index.tqplus_scale().to_vec(),
        // A source that carries no labels of its own is labelled by slot, so
        // a block always names its own rows and a caller never has to know
        // where the rows came from to keep track of them.
        labels: match labels {
            Some(table) => table[start..start + rows].to_vec(),
            None => (start as u64..(start + rows) as u64).collect(),
        },
    })
}

/// Incrementally assembles one imported index without retaining transport
/// frames. The finished index is inserted into the registry only after every
/// frame validates and the declared row count is met exactly.
struct ImportBuilder {
    expected_rows: usize,
    head: Option<RowBlock>,
    blocks: usize,
    rows: usize,
    packed: Vec<u8>,
    scales: Vec<f32>,
    labels: Vec<u64>,
}

impl ImportBuilder {
    fn new(expected_rows: usize) -> Self {
        Self {
            expected_rows,
            head: None,
            blocks: 0,
            rows: 0,
            packed: Vec::new(),
            scales: Vec::new(),
            labels: Vec::new(),
        }
    }

    fn push(&mut self, block: RowBlock) -> Result<(), Status> {
        let bi = self.blocks;
        if let Some(head) = &self.head {
            if block.dim != head.dim {
                return Err(errors::precondition(
                    crate::errors::DIMENSION_MISMATCH,
                    format!(
                        "block 0 carries dim {} and block {bi} carries dim {}",
                        head.dim, block.dim
                    ),
                ));
            }
            if block.bit_width != head.bit_width {
                return Err(errors::precondition(
                    crate::errors::BIT_WIDTH_MISMATCH,
                    format!(
                        "block 0 is {}-bit and block {bi} is {}-bit",
                        head.bit_width, block.bit_width
                    ),
                ));
            }
            if let Some(detail) = calibration_difference(
                (&head.tqplus_shift, &head.tqplus_scale),
                (&block.tqplus_shift, &block.tqplus_scale),
            ) {
                return Err(errors::precondition(
                    crate::errors::MIXED_CALIBRATION,
                    format!("block 0 and block {bi} were encoded under different pairs: {detail}"),
                ));
            }
        }

        let block_rows = usize::try_from(block.rows).map_err(|_| {
            errors::invalid(
                ROW_COUNT_MISMATCH,
                format!(
                    "block {bi} claims {} rows, which is out of range",
                    block.rows
                ),
            )
        })?;
        if block.scales.len() != block_rows || block.labels.len() != block_rows {
            return Err(errors::invalid(
                ROW_COUNT_MISMATCH,
                format!(
                    "block {bi} claims {block_rows} rows but carries {} scales and {} labels",
                    block.scales.len(),
                    block.labels.len()
                ),
            ));
        }
        let next_rows = self
            .rows
            .checked_add(block_rows)
            .ok_or_else(|| errors::invalid(ROW_COUNT_MISMATCH, "import row count overflowed"))?;
        if next_rows > self.expected_rows {
            return Err(errors::invalid(
                ROW_COUNT_MISMATCH,
                format!(
                    "import declared {} rows but received at least {next_rows}",
                    self.expected_rows
                ),
            ));
        }

        if self.head.is_none() {
            self.head = Some(RowBlock {
                dim: block.dim,
                bit_width: block.bit_width,
                rows: 0,
                packed_codes: Vec::new(),
                scales: Vec::new(),
                tqplus_shift: block.tqplus_shift.clone(),
                tqplus_scale: block.tqplus_scale.clone(),
                labels: Vec::new(),
            });
        }
        self.rows = next_rows;
        self.blocks += 1;
        self.packed.extend(block.packed_codes);
        self.scales.extend(block.scales);
        self.labels.extend(block.labels);
        Ok(())
    }

    fn finish(self) -> Result<(TurboQuantIndex, Vec<u64>), Status> {
        let Some(head) = self.head else {
            return Err(errors::invalid(
                ROW_COUNT_MISMATCH,
                "ImportRows needs at least one row block",
            ));
        };
        if self.rows != self.expected_rows {
            return Err(errors::invalid(
                ROW_COUNT_MISMATCH,
                format!(
                    "import declared {} rows but received {}",
                    self.expected_rows, self.rows
                ),
            ));
        }
        let dim = head.dim as usize;
        let bit_width = head.bit_width as usize;
        let index = TurboQuantIndex::from_parts(
            Some(dim),
            bit_width,
            self.rows,
            self.packed,
            self.scales,
            head.tqplus_shift,
            head.tqplus_scale,
        )
        .map_err(|e| Status::invalid_argument(format!("row blocks do not form an index: {e}")))?;
        Ok((index, self.labels))
    }
}

/// Compare two calibration pairs coordinate by coordinate, returning a
/// description of the first difference, or `None` when they are equal.
///
/// The comparison is exact. Two pairs that differ in the last bit of one
/// coordinate encode some rows to different codes, so "close enough" is not a
/// property that exists here: either the pairs are the same pair, and the two
/// indexes' scores can be merged, or they are not, and no correction applied
/// afterwards recovers what the difference cost.
pub(crate) fn calibration_difference(
    left: (&[f32], &[f32]),
    right: (&[f32], &[f32]),
) -> Option<String> {
    if left.0.len() != right.0.len() {
        return Some(format!(
            "shift has {} coordinates on one side and {} on the other",
            left.0.len(),
            right.0.len()
        ));
    }
    if left.1.len() != right.1.len() {
        return Some(format!(
            "scale has {} coordinates on one side and {} on the other",
            left.1.len(),
            right.1.len()
        ));
    }
    for (i, (a, b)) in left.0.iter().zip(right.0.iter()).enumerate() {
        if a.to_bits() != b.to_bits() {
            return Some(format!(
                "shift coordinate {i} is {a} on one side and {b} on the other"
            ));
        }
    }
    for (i, (a, b)) in left.1.iter().zip(right.1.iter()).enumerate() {
        if a.to_bits() != b.to_bits() {
            return Some(format!(
                "scale coordinate {i} is {a} on one side and {b} on the other"
            ));
        }
    }
    None
}

/// Add one streamed chunk of vectors to an index under the write lock, on the
/// blocking pool. Returns the number of vectors added.
async fn add_chunk(handle: &Handle, chunk: AddRequest) -> Result<u64, Status> {
    let handle = Arc::clone(handle);
    tokio::task::spawn_blocking(move || {
        let dim = chunk.dim as usize;
        if dim == 0 || !chunk.vectors.len().is_multiple_of(dim) {
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
