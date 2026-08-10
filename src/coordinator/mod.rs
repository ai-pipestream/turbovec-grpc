//! The `Coordinator` gRPC service: one logical collection over several nodes.
//!
//! A client of this service sees one index. It sends a query batch and a `k`,
//! and gets back the same top-k a single index holding all the same rows would
//! have returned, with the same scores to the bit. It never names a shard,
//! never learns how many there are, and never has to merge anything itself.
//!
//! The exactness is not a tolerance that happens to be small. turbovec's TQ+
//! calibration is a per-coordinate `(shift, scale)` pair, and under a fixed
//! pair a row's encoded codes are a pure function of the row: the same vector
//! added to two indexes calibrated alike encodes to the same bytes, and scores
//! the same against the same query. So a row's score does not depend on which
//! index holds it, the union of the shards' top-k contains the collection's
//! top-k, and merging by score is the merge, not an approximation of it.
//!
//! Everything else here defends that precondition:
//!
//! - The collection is bound before it is served: every shard is probed for
//!   its dim, bit width and calibration pair, and the pair is compared
//!   coordinate by coordinate. Shards that disagree are not merged under a
//!   correction, they are refused by name. A merge of differently calibrated
//!   scores is not a worse ranking, it is a ranking of nothing.
//! - A shard that fails mid-query fails the search. A top-k missing a shard
//!   is a plausible answer to a question nobody asked, and it is the failure
//!   this layer exists to make impossible. `allow_partial` opts into it
//!   explicitly, and the response then says so and names what dropped out.
//! - Split and Join move encoded codes, never vectors, so neither one
//!   re-encodes a row and neither one can drift from the source.
//!
//! The binding is cached: it is established on the first call that needs it
//! and reused until a Split or a Join rebinds the collection. That trades a
//! per-search round trip to every node against the possibility that someone
//! recalibrates a shard underneath a running coordinator, which no longer
//! matches what was pinned. `ListNodes` re-probes on every call, so the
//! operator-facing view is never the cached one.

pub mod nodes;

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use turbovec::TurboQuantIndex;

use crate::errors::{
    self, AMBIGUOUS_INDEX, BIT_WIDTH_MISMATCH, DIMENSION_MISMATCH, EMPTY_COLLECTION,
    MIXED_CALIBRATION, NODE_UNREACHABLE, ROW_COUNT_MISMATCH,
};
use crate::proto::coordinator_server::{Coordinator, CoordinatorServer};
use crate::proto::turbo_vec_client::TurboVecClient;
use crate::proto::{
    Calibration, CollectionQueryResult, CollectionSearchRequest, CollectionSearchResponse,
    ExportRowsRequest, FitCalibrationRequest, FitCalibrationResponse, GetCalibrationRequest,
    GetIndexInfoRequest, ImportRowsRequest, IndexInfo, JoinRequest, JoinResponse,
    ListIndexesRequest, ListNodesRequest, ListNodesResponse, Neighbour, QueryResult, RowBlock,
    SearchRequest, SetCalibrationRequest, ShardFailure, ShardRef, ShardStatus, SplitRequest,
    SplitResponse, StartStreamSearch, StreamSearchRequest, StreamSearchResponse,
};
use crate::service::calibration_difference;

pub use nodes::{NodeTable, ShardConfig};

/// Frame limit for a message between the coordinator and a node. Row blocks
/// carry a whole shard's codes, which is the largest thing on this wire by a
/// wide margin, so it matches the node binary's own limit.
const MAX_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// gRPC implementation of `turbovec.v1.Coordinator`.
pub struct CoordinatorService {
    /// The configured shards, replaced wholesale by Split and Join.
    table: RwLock<Vec<ShardConfig>>,

    /// The bound collection, or `None` when it has yet to be established or
    /// has been invalidated by a rebind.
    pinned: Mutex<Option<Arc<Pinned>>>,

    /// Lazily dialled channels, one per node address. A `Channel` is a handle
    /// to a connection pool that reconnects on its own, so caching one per
    /// address costs nothing and saves a dial per call.
    channels: Mutex<HashMap<String, Channel>>,
}

/// One shard as the coordinator found it on the node.
#[derive(Clone)]
struct Probe {
    /// Node address.
    address: String,

    /// Resolved index handle on that node.
    index_id: String,

    /// Metadata the node reported.
    info: IndexInfo,

    /// Calibration pair the node reported.
    calibration: Calibration,
}

/// A collection that has been probed and found servable.
struct Pinned {
    /// The shards, in table order.
    shards: Vec<Probe>,

    /// Vector dimensionality, shared by every shard.
    dim: usize,

    /// Rows across the whole collection.
    rows: u64,

    /// The calibration pair every shard holds.
    calibration: Calibration,
}

/// One candidate in the coordinator's bounded global heap.
///
/// [`BinaryHeap`] exposes its greatest item. This ordering deliberately makes
/// the worst candidate greatest: lower scores lose, then later shards and
/// larger slots lose ties. The heap root is therefore the current global
/// k-th candidate and its score is the safe floor broadcast to every shard.
#[derive(Clone)]
struct HeapCandidate {
    score: f32,
    shard_rank: usize,
    address: String,
    index_id: String,
    slot: u64,
    label: Option<u64>,
}

impl PartialEq for HeapCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapCandidate {}

impl PartialOrd for HeapCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.shard_rank.cmp(&other.shard_rank))
            .then_with(|| self.slot.cmp(&other.slot))
    }
}

/// Event forwarded by one shard response task to the query's collector.
enum StreamEvent {
    Message {
        shard_rank: usize,
        response: StreamSearchResponse,
    },
    Error {
        shard_rank: usize,
        status: Status,
    },
    Closed {
        shard_rank: usize,
    },
}

impl CoordinatorService {
    /// Create the service over a configured node table.
    pub fn new(table: NodeTable) -> Self {
        Self {
            table: RwLock::new(table.shards),
            pinned: Mutex::new(None),
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap into the generated tonic service.
    pub fn into_server(self) -> CoordinatorServer<Self> {
        CoordinatorServer::new(self)
    }

    /// The configured shards, copied out from under the lock.
    fn table(&self) -> Vec<ShardConfig> {
        self.table
            .read()
            .expect("coordinator node table lock poisoned")
            .clone()
    }

    /// Replace the shard table and drop the binding built from the old one.
    ///
    /// Split and Join both end here. Neither reshapes a collection in place:
    /// they build the new shards, and only once every one of them exists does
    /// the collection start pointing at them.
    fn rebind(&self, shards: Vec<ShardConfig>) {
        *self
            .table
            .write()
            .expect("coordinator node table lock poisoned") = shards;
        *self
            .pinned
            .lock()
            .expect("coordinator binding lock poisoned") = None;
    }

    /// A client for one node address, dialled lazily and cached.
    fn client(&self, address: &str) -> Result<TurboVecClient<Channel>, Status> {
        let mut cache = self
            .channels
            .lock()
            .expect("coordinator channel cache lock poisoned");
        let channel = match cache.get(address) {
            Some(channel) => channel.clone(),
            None => {
                let endpoint = Endpoint::from_shared(address.to_string())
                    .map_err(|e| {
                        errors::invalid(
                            NODE_UNREACHABLE,
                            format!("node address {address} is not a valid endpoint: {e}"),
                        )
                    })?
                    .tcp_nodelay(true);
                // Lazy: the connection is made on the first call, so a
                // coordinator starts with nodes still coming up and reports
                // them through ListNodes rather than refusing to boot.
                let channel = endpoint.connect_lazy();
                cache.insert(address.to_string(), channel.clone());
                channel
            }
        };
        Ok(TurboVecClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES))
    }

    /// Search one query with one global heap while every shard streams
    /// candidates above the highest floor observed so far.
    async fn stream_query(
        &self,
        pinned: &Pinned,
        vector: Vec<f32>,
        k: usize,
    ) -> Result<CollectionQueryResult, Status> {
        let event_capacity = (pinned.shards.len() * 4).max(16);
        let (event_tx, mut event_rx) = mpsc::channel(event_capacity);
        let mut outbound = Vec::with_capacity(pinned.shards.len());

        for (shard_rank, shard) in pinned.shards.iter().enumerate() {
            let mut client = self.client(&shard.address)?;
            let (request_tx, request_rx) = mpsc::channel(8);
            request_tx
                .send(StreamSearchRequest {
                    payload: Some(crate::proto::stream_search_request::Payload::Start(
                        StartStreamSearch {
                            index_id: shard.index_id.clone(),
                            vector: vector.clone(),
                            initial_floor: None,
                            request_id: uuid::Uuid::new_v4().to_string(),
                        },
                    )),
                })
                .await
                .map_err(|_| Status::internal("cannot start shard request stream"))?;
            let mut responses = client
                .stream_search(ReceiverStream::new(request_rx))
                .await
                .map_err(|status| node_error(&shard.address, &status))?
                .into_inner();
            let shard_events = event_tx.clone();
            tokio::spawn(async move {
                loop {
                    match responses.message().await {
                        Ok(Some(response)) => {
                            if shard_events
                                .send(StreamEvent::Message {
                                    shard_rank,
                                    response,
                                })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Ok(None) => {
                            let _ = shard_events.send(StreamEvent::Closed { shard_rank }).await;
                            break;
                        }
                        Err(status) => {
                            let _ = shard_events
                                .send(StreamEvent::Error { shard_rank, status })
                                .await;
                            break;
                        }
                    }
                }
            });
            outbound.push(Some(request_tx));
        }
        drop(event_tx);

        let mut heap = BinaryHeap::with_capacity(k + 1);
        let mut published_floor = f32::NEG_INFINITY;
        let mut completed = vec![false; pinned.shards.len()];
        let mut remaining = pinned.shards.len();

        while remaining > 0 {
            let event = event_rx.recv().await.ok_or_else(|| {
                Status::internal("all shard response streams closed before completing")
            })?;
            match event {
                StreamEvent::Message {
                    shard_rank,
                    response,
                } => match response.payload {
                    Some(crate::proto::stream_search_response::Payload::Batch(batch)) => {
                        if completed[shard_rank] {
                            return Err(Status::internal(format!(
                                "shard {} emitted candidates after its completion summary",
                                pinned.shards[shard_rank].address
                            )));
                        }
                        if batch.scores.len() != batch.slots.len()
                            || (!batch.labels.is_empty()
                                && batch.labels.len() != batch.scores.len())
                        {
                            return Err(Status::internal(format!(
                                "shard {} returned misaligned streaming candidates",
                                pinned.shards[shard_rank].address
                            )));
                        }
                        let labelled = !batch.labels.is_empty();
                        let shard = &pinned.shards[shard_rank];
                        for (rank, (score, slot)) in
                            batch.scores.into_iter().zip(batch.slots).enumerate()
                        {
                            if score.is_nan() {
                                return Err(Status::internal(format!(
                                    "shard {} returned a NaN score",
                                    shard.address
                                )));
                            }
                            let candidate = HeapCandidate {
                                score,
                                shard_rank,
                                address: shard.address.clone(),
                                index_id: shard.index_id.clone(),
                                slot,
                                label: labelled.then(|| batch.labels[rank]),
                            };
                            if heap.len() < k {
                                heap.push(candidate);
                            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                                heap.pop();
                                heap.push(candidate);
                            }
                        }

                        if heap.len() == k {
                            let floor = heap.peek().expect("a full top-k heap has a root").score;
                            if floor > published_floor {
                                published_floor = floor;
                                for (rank, sender) in outbound.iter().enumerate() {
                                    if completed[rank] {
                                        continue;
                                    }
                                    sender
                                        .as_ref()
                                        .expect("incomplete shard retains its request stream")
                                        .send(StreamSearchRequest {
                                            payload: Some(
                                                crate::proto::stream_search_request::Payload::FloorUpdate(
                                                    crate::proto::FloorUpdate { floor },
                                                ),
                                            ),
                                        })
                                        .await
                                        .map_err(|_| {
                                            errors::unavailable(
                                                NODE_UNREACHABLE,
                                                format!(
                                                    "{} closed its floor-update stream",
                                                    pinned.shards[rank].address
                                                ),
                                            )
                                        })?;
                                }
                            }
                        }
                    }
                    Some(crate::proto::stream_search_response::Payload::Summary(summary)) => {
                        if completed[shard_rank] {
                            return Err(Status::internal(format!(
                                "shard {} sent more than one completion summary",
                                pinned.shards[shard_rank].address
                            )));
                        }
                        if !summary.completed {
                            return Err(Status::aborted(format!(
                                "shard {} did not complete its streaming scan",
                                pinned.shards[shard_rank].address
                            )));
                        }
                        completed[shard_rank] = true;
                        remaining -= 1;
                        outbound[shard_rank] = None;
                    }
                    None => {
                        return Err(Status::internal(format!(
                            "shard {} returned an empty streaming response",
                            pinned.shards[shard_rank].address
                        )))
                    }
                },
                StreamEvent::Error { shard_rank, status } => {
                    return Err(node_error(&pinned.shards[shard_rank].address, &status));
                }
                StreamEvent::Closed { shard_rank } => {
                    if !completed[shard_rank] {
                        return Err(errors::unavailable(
                            NODE_UNREACHABLE,
                            format!(
                                "{} closed its streaming scan without a completion summary",
                                pinned.shards[shard_rank].address
                            ),
                        ));
                    }
                }
            }
        }

        let mut candidates = heap.into_vec();
        candidates.sort();
        Ok(CollectionQueryResult {
            neighbours: candidates
                .into_iter()
                .map(|candidate| Neighbour {
                    score: candidate.score,
                    label: candidate.label,
                    address: candidate.address,
                    index_id: candidate.index_id,
                    slot: candidate.slot,
                })
                .collect(),
        })
    }

    /// Probe every configured shard concurrently, keeping table order.
    ///
    /// Each shard's outcome is returned on its own, so one unreachable node
    /// does not hide what the others reported. The callers decide what a
    /// failure means: `ListNodes` shows it, everything else refuses.
    async fn probe_all(&self, table: &[ShardConfig]) -> Vec<Result<Probe, Status>> {
        let mut tasks = Vec::with_capacity(table.len());
        for shard in table {
            let client = self.client(&shard.address);
            let address = shard.address.clone();
            let index_id = shard.index_id.clone();
            tasks.push(tokio::spawn(async move {
                probe(client?, address, index_id).await
            }));
        }
        let mut probes = Vec::with_capacity(tasks.len());
        for task in tasks {
            probes.push(match task.await {
                Ok(result) => result,
                Err(e) => Err(Status::internal(format!("probe task failed: {e}"))),
            });
        }
        probes
    }

    /// The bound collection, establishing it first if it is not bound yet.
    async fn collection(&self) -> Result<Arc<Pinned>, Status> {
        if let Some(pinned) = self
            .pinned
            .lock()
            .expect("coordinator binding lock poisoned")
            .clone()
        {
            return Ok(pinned);
        }

        let table = self.table();
        if table.is_empty() {
            return Err(errors::precondition(
                EMPTY_COLLECTION,
                "no shards are configured, so there is no collection to serve",
            ));
        }
        let probes = self.probe_all(&table).await;
        let mut shards = Vec::with_capacity(probes.len());
        for probe in probes {
            shards.push(probe?);
        }
        let pinned = Arc::new(bind(shards)?);
        // A concurrent caller may have bound the collection first. Both
        // bindings probed the same nodes and agree, so either will do.
        *self
            .pinned
            .lock()
            .expect("coordinator binding lock poisoned") = Some(Arc::clone(&pinned));
        Ok(pinned)
    }

    /// Resolve one shard's index handle without holding it to the
    /// collection's calibration.
    ///
    /// Used by the calls that establish or change what the calibration is, and
    /// by Split and Join, which name their own sources and targets rather than
    /// working through the bound collection.
    async fn resolve(&self, shard: &ShardConfig) -> Result<Probe, Status> {
        probe(
            self.client(&shard.address)?,
            shard.address.clone(),
            shard.index_id.clone(),
        )
        .await
    }

    /// Read a run of rows out of one shard as an encoded block.
    async fn export(&self, source: &Probe, start: u64, count: u64) -> Result<RowBlock, Status> {
        if count == 0 {
            // Zero means "to the end of the index" on the wire, which is the
            // useful default for exporting a whole shard but is not what a
            // target that was allotted no rows should receive. An empty run is
            // therefore built here rather than asked for. It still carries the
            // source's geometry and pair, so importing it yields a
            // well-formed empty shard of the collection rather than a shard
            // that agrees with nothing.
            return Ok(RowBlock {
                dim: source.info.dim,
                bit_width: source.info.bit_width,
                rows: 0,
                packed_codes: Vec::new(),
                scales: Vec::new(),
                tqplus_shift: source.calibration.tqplus_shift.clone(),
                tqplus_scale: source.calibration.tqplus_scale.clone(),
                labels: Vec::new(),
            });
        }
        let mut client = self.client(&source.address)?;
        Ok(client
            .export_rows(ExportRowsRequest {
                index_id: source.index_id.clone(),
                start,
                count,
            })
            .await
            .map_err(|e| node_error(&source.address, &e))?
            .into_inner())
    }
}

/// Read one node's view of one shard: which index, its metadata, its pair.
async fn probe(
    mut client: TurboVecClient<Channel>,
    address: String,
    index_id: Option<String>,
) -> Result<Probe, Status> {
    let index_id = match index_id {
        Some(id) => id,
        None => {
            // No handle configured: the node must hold exactly one index for
            // there to be an unambiguous answer. Picking one of several would
            // make the collection depend on the order a HashMap iterated in.
            let listed = client
                .list_indexes(ListIndexesRequest {})
                .await
                .map_err(|e| node_error(&address, &e))?
                .into_inner()
                .indexes;
            if listed.len() != 1 {
                return Err(errors::precondition(
                    AMBIGUOUS_INDEX,
                    format!(
                        "shard {address} was configured without an index handle and the node \
                         holds {} indexes; name the handle in the node table",
                        listed.len()
                    ),
                ));
            }
            listed[0].index_id.clone()
        }
    };

    let info = client
        .get_index_info(GetIndexInfoRequest {
            index_id: index_id.clone(),
        })
        .await
        .map_err(|e| node_error(&address, &e))?
        .into_inner();
    let calibration = client
        .get_calibration(GetCalibrationRequest {
            index_id: index_id.clone(),
        })
        .await
        .map_err(|e| node_error(&address, &e))?
        .into_inner();
    Ok(Probe {
        address,
        index_id,
        info,
        calibration,
    })
}

/// Hold a set of probed shards to one dim, one bit width and one calibration
/// pair, or name the first shard that breaks it.
///
/// The first shard sets what the rest are held to. That is arbitrary and it
/// does not matter: the check is for agreement, and a disagreement is reported
/// as the pair of shards it is between, not as one of them being wrong.
fn bind(shards: Vec<Probe>) -> Result<Pinned, Status> {
    let head = shards.first().ok_or_else(|| {
        errors::precondition(
            EMPTY_COLLECTION,
            "no shards are configured, so there is no collection to serve",
        )
    })?;
    let dim = head.info.dim;
    let bit_width = head.info.bit_width;
    let mut rows = 0u64;
    for shard in &shards {
        if shard.info.dim != dim {
            return Err(errors::precondition(
                DIMENSION_MISMATCH,
                format!(
                    "shard {} holds dim {} and shard {} holds dim {}",
                    head.address, dim, shard.address, shard.info.dim
                ),
            ));
        }
        if shard.info.bit_width != bit_width {
            return Err(errors::precondition(
                BIT_WIDTH_MISMATCH,
                format!(
                    "shard {} is {}-bit and shard {} is {}-bit",
                    head.address, bit_width, shard.address, shard.info.bit_width
                ),
            ));
        }
        if let Some(detail) = calibration_difference(
            (
                &head.calibration.tqplus_shift,
                &head.calibration.tqplus_scale,
            ),
            (
                &shard.calibration.tqplus_shift,
                &shard.calibration.tqplus_scale,
            ),
        ) {
            return Err(errors::precondition(
                MIXED_CALIBRATION,
                format!(
                    "shard {} and shard {} are calibrated differently, so their scores are not \
                     on one scale and merging them would rank nothing: {detail}",
                    head.address, shard.address
                ),
            ));
        }
        rows += shard.info.len;
    }
    let calibration = head.calibration.clone();
    Ok(Pinned {
        shards,
        dim: dim as usize,
        rows,
        calibration,
    })
}

/// Carry a node's failure back to the caller, saying which shard it came from.
///
/// A node that refused something by name has already said what is wrong, so
/// its status is passed through with the shard appended: rewrapping it as "the
/// node did not answer" would turn a diagnosis into a symptom. Anything else,
/// including a node that genuinely did not answer, becomes `node_unreachable`
/// carrying the node's own code and wording.
fn node_error(address: &str, status: &Status) -> Status {
    if errors::is_named(status.message()) {
        return Status::new(
            status.code(),
            format!("{} (shard {address})", status.message()),
        );
    }
    errors::unavailable(
        NODE_UNREACHABLE,
        format!(
            "node {address} did not answer: {} ({:?})",
            status.message(),
            status.code()
        ),
    )
}

/// The wire form of one shard reference.
fn shard_ref(address: &str, index_id: &str) -> ShardRef {
    ShardRef {
        address: address.to_string(),
        index_id: index_id.to_string(),
    }
}

/// Split `rows` across `targets`, or validate the counts the caller gave.
///
/// An even split puts the remainder on the earlier targets. An explicit set of
/// counts must have one entry per target and sum to exactly the source's row
/// count: a split is a redistribution, and counts that do not add up would
/// drop rows or duplicate them without ever looking wrong.
fn plan_counts(rows: u64, targets: usize, requested: &[u64]) -> Result<Vec<u64>, Status> {
    if targets == 0 {
        return Err(errors::invalid(
            ROW_COUNT_MISMATCH,
            "a split needs at least one target node",
        ));
    }
    if requested.is_empty() {
        let targets = targets as u64;
        let base = rows / targets;
        let remainder = rows % targets;
        return Ok((0..targets)
            .map(|i| base + u64::from(i < remainder))
            .collect());
    }
    if requested.len() != targets {
        return Err(errors::invalid(
            ROW_COUNT_MISMATCH,
            format!(
                "{} row counts were given for {targets} targets",
                requested.len()
            ),
        ));
    }
    let total: u64 = requested.iter().sum();
    if total != rows {
        return Err(errors::invalid(
            ROW_COUNT_MISMATCH,
            format!("row counts sum to {total} but the source holds {rows} rows"),
        ));
    }
    Ok(requested.to_vec())
}

#[tonic::async_trait]
impl Coordinator for CoordinatorService {
    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        let table = self.table();
        let probes = self.probe_all(&table).await;

        let mut statuses = Vec::with_capacity(probes.len());
        let mut healthy = Vec::new();
        for (shard, probe) in table.iter().zip(probes) {
            match probe {
                Ok(probe) => {
                    statuses.push(ShardStatus {
                        shard: Some(shard_ref(&probe.address, &probe.index_id)),
                        info: Some(probe.info.clone()),
                        calibration: Some(probe.calibration.clone()),
                        error: String::new(),
                    });
                    healthy.push(probe);
                }
                Err(e) => statuses.push(ShardStatus {
                    shard: Some(ShardRef {
                        address: shard.address.clone(),
                        index_id: shard.index_id.clone().unwrap_or_default(),
                    }),
                    info: None,
                    calibration: None,
                    error: e.message().to_string(),
                }),
            }
        }

        // Only a collection whose every shard answered can be held to one
        // calibration, so an unreachable node is reported as itself rather
        // than as a disagreement it was never part of.
        let (servable, error, rows) = if healthy.len() != table.len() {
            (
                false,
                format!(
                    "{}: {} of {} shards did not answer",
                    NODE_UNREACHABLE,
                    table.len() - healthy.len(),
                    table.len()
                ),
                0,
            )
        } else {
            match bind(healthy) {
                Ok(pinned) => (true, String::new(), pinned.rows),
                Err(e) => (false, e.message().to_string(), 0),
            }
        };
        Ok(Response::new(ListNodesResponse {
            shards: statuses,
            servable,
            error,
            rows,
        }))
    }

    async fn search(
        &self,
        request: Request<CollectionSearchRequest>,
    ) -> Result<Response<CollectionSearchResponse>, Status> {
        let req = request.into_inner();
        let k = req.k as usize;
        if k == 0 {
            return Err(Status::invalid_argument("k must be at least 1"));
        }
        let pinned = self.collection().await?;
        if req.queries.is_empty() || req.queries.len() % pinned.dim != 0 {
            return Err(Status::invalid_argument(format!(
                "query buffer length {} is not a positive multiple of the collection dim {}",
                req.queries.len(),
                pinned.dim
            )));
        }
        let nq = req.queries.len() / pinned.dim;

        // Complete searches use the collaborative streaming collector: the
        // coordinator owns the only top-k heap and sends its rising k-th score
        // back to every shard. The legacy unary path below remains solely for
        // allow_partial until that response contract can identify a shard
        // failure for a particular query in a batch without ambiguity.
        if !req.allow_partial {
            let mut results = Vec::with_capacity(nq);
            for vector in req.queries.chunks_exact(pinned.dim) {
                results.push(self.stream_query(&pinned, vector.to_vec(), k).await?);
            }
            return Ok(Response::new(CollectionSearchResponse {
                results,
                partial: false,
                failures: Vec::new(),
            }));
        }

        // Every shard is asked for its own top-k. A shard can contribute at
        // most k rows to a global top-k, so k per shard is exactly enough:
        // asking for more would return rows that cannot place, asking for
        // fewer could miss one that can.
        let mut tasks = Vec::with_capacity(pinned.shards.len());
        for shard in &pinned.shards {
            let client = self.client(&shard.address);
            let index_id = shard.index_id.clone();
            let queries = req.queries.clone();
            let k = req.k;
            tasks.push(tokio::spawn(async move {
                let mut client = client?;
                client
                    .search(SearchRequest {
                        index_id,
                        queries,
                        k,
                        allowlist: Vec::new(),
                    })
                    .await
                    .map(|r| r.into_inner())
            }));
        }

        let mut merged: Vec<Vec<Neighbour>> = vec![Vec::new(); nq];
        let mut failures = Vec::new();
        for (shard, task) in pinned.shards.iter().zip(tasks) {
            let outcome = match task.await {
                Ok(result) => result,
                Err(e) => Err(Status::internal(format!("search task failed: {e}"))),
            };
            let response = match outcome {
                Ok(response) => response,
                Err(e) => {
                    let failure = node_error(&shard.address, &e);
                    if !req.allow_partial {
                        return Err(failure);
                    }
                    failures.push(ShardFailure {
                        shard: Some(shard_ref(&shard.address, &shard.index_id)),
                        error: failure.message().to_string(),
                    });
                    continue;
                }
            };
            if response.results.len() != nq {
                // The node answers one result per query or it is not answering
                // this request. Merging a short response would silently drop
                // whichever queries it ran out on.
                return Err(Status::internal(format!(
                    "shard {} returned {} results for {nq} queries",
                    shard.address,
                    response.results.len()
                )));
            }
            for (qi, result) in response.results.into_iter().enumerate() {
                let QueryResult {
                    scores,
                    ids,
                    labels,
                } = result;
                // A shard built by ImportRows carries an external id per row
                // and reports it; one built by plain adds does not, and its
                // rows are identified by node, index and slot instead.
                let labelled = labels.len() == ids.len();
                for (rank, (score, slot)) in scores.into_iter().zip(ids).enumerate() {
                    merged[qi].push(Neighbour {
                        score,
                        label: labelled.then(|| labels[rank]),
                        address: shard.address.clone(),
                        index_id: shard.index_id.clone(),
                        slot,
                    });
                }
            }
        }

        if failures.len() == pinned.shards.len() {
            return Err(errors::unavailable(
                NODE_UNREACHABLE,
                format!(
                    "every one of the {} shards failed, so there is no result to be partial \
                     about",
                    pinned.shards.len()
                ),
            ));
        }

        let results = merged
            .into_iter()
            .map(|mut neighbours| {
                // A total order over the scores, so the merge is deterministic
                // without any comparison being able to fail. The sort is
                // stable, so rows that tie keep shard order: a tie has no
                // winner, and this at least makes it the same non-winner
                // every time.
                neighbours.sort_by(|a, b| b.score.total_cmp(&a.score));
                neighbours.truncate(k);
                CollectionQueryResult { neighbours }
            })
            .collect();
        Ok(Response::new(CollectionSearchResponse {
            results,
            partial: !failures.is_empty(),
            failures,
        }))
    }

    async fn fit_calibration(
        &self,
        request: Request<FitCalibrationRequest>,
    ) -> Result<Response<FitCalibrationResponse>, Status> {
        let req = request.into_inner();
        let dim = req.dim as usize;
        let bit_width = req.bit_width as usize;

        // The fit happens once, here, and the fitted pair is what travels. The
        // sample could be broadcast instead and each node fit its own, which
        // would agree (turbovec's fit is deterministic across platforms and
        // thread counts by construction), but then agreement would be
        // something to trust rather than something to arrange.
        let fitted = tokio::task::spawn_blocking(move || {
            let mut index = TurboQuantIndex::new(dim, bit_width)
                .map_err(|e| Status::invalid_argument(format!("cannot fit at this shape: {e}")))?;
            index
                .calibrate_2d(&req.sample, dim)
                .map_err(|e| Status::invalid_argument(format!("calibration fit failed: {e}")))?;
            Ok::<_, Status>((index.tqplus_shift().to_vec(), index.tqplus_scale().to_vec()))
        })
        .await
        .map_err(|e| Status::internal(format!("calibration fit task failed: {e}")))??;

        let table = self.table();
        if table.is_empty() {
            return Err(errors::precondition(
                EMPTY_COLLECTION,
                "no shards are configured, so there is nothing to calibrate",
            ));
        }

        let mut shards = Vec::with_capacity(table.len());
        for shard in &table {
            let probe = self.resolve(shard).await?;
            let mut client = self.client(&probe.address)?;
            let committed = client
                .set_calibration(SetCalibrationRequest {
                    index_id: probe.index_id.clone(),
                    tqplus_shift: fitted.0.clone(),
                    tqplus_scale: fitted.1.clone(),
                })
                .await
                .map_err(|e| node_error(&probe.address, &e))?
                .into_inner();
            // Read back what the node actually holds rather than trusting the
            // broadcast: this call exists to establish that the shards share a
            // pair, and an unverified broadcast establishes nothing.
            if let Some(detail) = calibration_difference(
                (&fitted.0, &fitted.1),
                (&committed.tqplus_shift, &committed.tqplus_scale),
            ) {
                return Err(errors::precondition(
                    MIXED_CALIBRATION,
                    format!(
                        "shard {} did not commit the pair it was sent: {detail}",
                        probe.address
                    ),
                ));
            }
            shards.push(shard_ref(&probe.address, &probe.index_id));
        }

        // Every shard now holds a handle that may be new, and the old binding
        // described the pair they held before.
        self.rebind(
            shards
                .iter()
                .map(|s| ShardConfig::with_index(&s.address, &s.index_id))
                .collect(),
        );
        let pinned = self.collection().await?;
        Ok(Response::new(FitCalibrationResponse {
            calibration: Some(pinned.calibration.clone()),
            shards,
        }))
    }

    async fn split(
        &self,
        request: Request<SplitRequest>,
    ) -> Result<Response<SplitResponse>, Status> {
        let req = request.into_inner();
        let source = req.source.ok_or_else(|| {
            Status::invalid_argument("split needs a source shard to redistribute")
        })?;
        let source = self.resolve(&to_config(&source)).await?;
        let counts = plan_counts(source.info.len, req.targets.len(), &req.row_counts)?;

        // One target at a time: a block is a full copy of its rows in the
        // coordinator's memory, and doing them in sequence bounds that to the
        // largest single shard rather than to the whole source.
        let mut shards = Vec::with_capacity(req.targets.len());
        let mut start = 0u64;
        for (target, &count) in req.targets.iter().zip(counts.iter()) {
            let block = self.export(&source, start, count).await?;
            let target = ShardConfig::new(target.clone());
            let mut client = self.client(&target.address)?;
            let imported = client
                .import_rows(ImportRowsRequest {
                    blocks: vec![block],
                })
                .await
                .map_err(|e| node_error(&target.address, &e))?
                .into_inner();
            shards.push(shard_ref(&target.address, &imported.index_id));
            start += count;
        }

        self.rebind(
            shards
                .iter()
                .map(|s| ShardConfig::with_index(&s.address, &s.index_id))
                .collect(),
        );
        Ok(Response::new(SplitResponse {
            shards,
            rows: counts,
            calibration: Some(source.calibration),
        }))
    }

    async fn join(&self, request: Request<JoinRequest>) -> Result<Response<JoinResponse>, Status> {
        let req = request.into_inner();
        let sources: Vec<ShardConfig> = if req.sources.is_empty() {
            self.table()
        } else {
            req.sources.iter().map(to_config).collect()
        };
        if sources.is_empty() {
            return Err(errors::precondition(
                EMPTY_COLLECTION,
                "no sources were named and no shards are configured, so there is nothing to join",
            ));
        }
        if req.target.is_empty() {
            return Err(Status::invalid_argument(
                "join needs a target node address to build the combined index on",
            ));
        }

        // Probe and agree before moving a single row. The node checks the same
        // things when it concatenates the blocks, but it can only name them as
        // block numbers; here they are still shards with addresses, which is
        // what an operator has to act on.
        let mut probes = Vec::with_capacity(sources.len());
        for source in &sources {
            probes.push(self.resolve(source).await?);
        }
        let pinned = bind(probes.clone())?;

        let mut blocks = Vec::with_capacity(probes.len());
        for probe in &probes {
            blocks.push(self.export(probe, 0, probe.info.len).await?);
        }
        let target = ShardConfig::new(req.target);
        let mut client = self.client(&target.address)?;
        let imported = client
            .import_rows(ImportRowsRequest { blocks })
            .await
            .map_err(|e| node_error(&target.address, &e))?
            .into_inner();
        let shard = shard_ref(&target.address, &imported.index_id);

        self.rebind(vec![ShardConfig::with_index(
            &shard.address,
            &shard.index_id,
        )]);
        Ok(Response::new(JoinResponse {
            rows: pinned.rows,
            calibration: Some(pinned.calibration),
            shard: Some(shard),
        }))
    }
}

/// Turn a wire shard reference into a configuration entry.
fn to_config(shard: &ShardRef) -> ShardConfig {
    match shard.index_id.is_empty() {
        true => ShardConfig::new(shard.address.clone()),
        false => ShardConfig::with_index(shard.address.clone(), shard.index_id.clone()),
    }
}
