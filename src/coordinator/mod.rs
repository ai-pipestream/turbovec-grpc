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
//!   this layer exists to make impossible.
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
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use turbovec::TurboQuantIndex;

use crate::errors::{
    self, AMBIGUOUS_INDEX, BIT_WIDTH_MISMATCH, DIMENSION_MISMATCH, EMPTY_COLLECTION,
    MIXED_CALIBRATION, NODE_UNREACHABLE, ROW_COUNT_MISMATCH,
};
use crate::observability::Metrics;
use crate::proto::coordinator_server::{Coordinator, CoordinatorServer};
use crate::proto::turbo_vec_admin_client::TurboVecAdminClient;
use crate::proto::turbo_vec_query_client::TurboVecQueryClient;
use crate::proto::{
    Calibration, CollectionQueryResult, CollectionSearchRequest, CollectionSearchResponse,
    ExportRowsRequest, FitCalibrationRequest, FitCalibrationResponse, FlushRequest,
    GetCalibrationRequest, GetIndexInfoRequest, ImportRowsRequest, ImportRowsResponse,
    ImportRowsStart, IndexInfo, JoinRequest, JoinResponse, ListIndexesRequest, ListNodesRequest,
    ListNodesResponse, Neighbour, RegisterNodeRequest, RegisterNodeResponse, RowBlock,
    SetCalibrationRequest, ShardRef, ShardStatus, SpareNodeStatus, SplitRequest, SplitResponse,
    StartStreamSearch, StreamSearchRequest, StreamSearchResponse,
};
use crate::service::calibration_difference;

pub use nodes::{NodeTable, ShardConfig};

/// Frame limit for a message between the coordinator and a node. Row blocks
/// carry a whole shard's codes, which is the largest thing on this wire by a
/// wide margin, so it matches the node binary's own limit.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Coordinator-side request limits.
#[derive(Clone, Debug)]
pub struct CoordinatorLimits {
    pub max_k: usize,
    pub max_queries_per_request: usize,
    pub max_concurrent_queries: usize,
    pub query_timeout: Duration,
}

impl Default for CoordinatorLimits {
    fn default() -> Self {
        Self {
            max_k: 1_000,
            max_queries_per_request: 64,
            max_concurrent_queries: 4,
            query_timeout: Duration::from_secs(30),
        }
    }
}

impl CoordinatorLimits {
    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        let timeout_ms = crate::config::positive_usize(
            "TURBOVEC_QUERY_TIMEOUT_MS",
            defaults.query_timeout.as_millis() as usize,
        )?;
        Ok(Self {
            max_k: crate::config::positive_usize("TURBOVEC_MAX_K", defaults.max_k)?,
            max_queries_per_request: crate::config::positive_usize(
                "TURBOVEC_MAX_QUERIES",
                defaults.max_queries_per_request,
            )?,
            max_concurrent_queries: crate::config::positive_usize(
                "TURBOVEC_MAX_CONCURRENT_QUERIES",
                defaults.max_concurrent_queries,
            )?,
            query_timeout: Duration::from_millis(timeout_ms as u64),
        })
    }
}

/// gRPC implementation of `turbovec.v1.Coordinator`.
#[derive(Clone)]
pub struct CoordinatorService {
    /// The active topology generation and its shards, replaced atomically by
    /// Split and Join.
    topology: Arc<RwLock<Topology>>,

    /// Optional durable topology state file. When configured, a new
    /// generation is fsynced here before it becomes active in memory.
    topology_path: Option<PathBuf>,

    /// Registered nodes not serving any shard, in registration order.
    /// Fed by `RegisterNode`, drained by a rebind that makes one of them a
    /// member, persisted in the same state file as the topology. Never read
    /// by Search: a spare holds no shard, so it takes no query traffic.
    spares: Arc<Mutex<Vec<String>>>,

    /// The bound collection, or `None` when it has yet to be established or
    /// has been invalidated by a rebind.
    pinned: Arc<Mutex<Option<Arc<Pinned>>>>,

    /// Lazily dialled channels, one per node address. A `Channel` is a handle
    /// to a connection pool that reconnects on its own, so caching one per
    /// address costs nothing and saves a dial per call.
    channels: Arc<Mutex<HashMap<String, Channel>>>,

    limits: CoordinatorLimits,
    metrics: Metrics,
}

struct Topology {
    generation: u64,
    shards: Vec<ShardConfig>,
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

struct ExportPlan {
    source: Probe,
    start: u64,
    count: u64,
}

/// A collection that has been probed and found servable.
struct Pinned {
    /// Topology generation this binding was built from.
    generation: u64,

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

struct ShardControl {
    sender: watch::Sender<StreamSearchRequest>,
    completed: bool,
}

impl Drop for ShardControl {
    fn drop(&mut self) {
        if !self.completed {
            self.sender.send_replace(StreamSearchRequest {
                payload: Some(crate::proto::stream_search_request::Payload::Stop(
                    crate::proto::StopStreamSearch {},
                )),
            });
        }
    }
}

impl CoordinatorService {
    /// Create the service over a configured node table.
    pub fn new(table: NodeTable) -> Self {
        Self::with_limits(table, CoordinatorLimits::default())
    }

    pub fn with_limits(table: NodeTable, limits: CoordinatorLimits) -> Self {
        Self::with_limits_and_metrics(table, limits, Metrics::default())
    }

    pub fn with_limits_and_metrics(
        table: NodeTable,
        limits: CoordinatorLimits,
        metrics: Metrics,
    ) -> Self {
        Self::from_topology(1, table, Vec::new(), None, limits, metrics)
    }

    /// Create a coordinator whose topology survives restart. An existing
    /// state file wins over startup nodes; an absent one is seeded at
    /// generation 1 before the service starts.
    pub fn with_state_file(table: NodeTable, path: impl Into<PathBuf>) -> Result<Self, String> {
        Self::with_state_file_and_limits(table, path, CoordinatorLimits::default())
    }

    pub fn with_state_file_and_limits(
        table: NodeTable,
        path: impl Into<PathBuf>,
        limits: CoordinatorLimits,
    ) -> Result<Self, String> {
        Self::with_state_file_limits_and_metrics(table, path, limits, Metrics::default())
    }

    pub fn with_state_file_limits_and_metrics(
        table: NodeTable,
        path: impl Into<PathBuf>,
        limits: CoordinatorLimits,
        metrics: Metrics,
    ) -> Result<Self, String> {
        let path = path.into();
        let (generation, table, spares) = nodes::load_or_initialize(&path, &table)?;
        if table
            .shards
            .iter()
            .any(|shard| shard.required_generation.is_none())
        {
            return Err(format!(
                "durable topology {} requires an index id and generation for every shard",
                path.display()
            ));
        }
        Ok(Self::from_topology(
            generation,
            table,
            spares,
            Some(path),
            limits,
            metrics,
        ))
    }

    fn from_topology(
        generation: u64,
        table: NodeTable,
        spares: Vec<String>,
        topology_path: Option<PathBuf>,
        limits: CoordinatorLimits,
        metrics: Metrics,
    ) -> Self {
        assert!(limits.max_k > 0, "max_k must be positive");
        assert!(
            limits.max_queries_per_request > 0,
            "max_queries_per_request must be positive"
        );
        assert!(
            limits.max_concurrent_queries > 0,
            "max_concurrent_queries must be positive"
        );
        assert!(
            !limits.query_timeout.is_zero(),
            "query_timeout must be positive"
        );
        metrics.set_topology_generation(generation);
        Self {
            topology: Arc::new(RwLock::new(Topology {
                generation,
                shards: table.shards,
            })),
            topology_path,
            spares: Arc::new(Mutex::new(spares)),
            pinned: Arc::new(Mutex::new(None)),
            channels: Arc::new(Mutex::new(HashMap::new())),
            limits,
            metrics,
        }
    }

    /// Wrap into the generated tonic service.
    pub fn into_server(self) -> CoordinatorServer<Self> {
        CoordinatorServer::new(self)
    }

    /// Active topology generation and shard table, for startup diagnostics.
    pub fn topology_snapshot(&self) -> (u64, NodeTable) {
        let (generation, shards) = self.topology();
        (generation, NodeTable::new(shards))
    }

    pub async fn ready(&self) -> bool {
        let (generation, table) = self.topology();
        let probes = self.probe_all(&table).await;
        if probes.iter().any(Result::is_err) {
            return false;
        }
        bind(generation, probes.into_iter().map(Result::unwrap).collect()).is_ok()
    }

    /// The configured shards, copied out from under the lock.
    fn topology(&self) -> (u64, Vec<ShardConfig>) {
        let topology = self
            .topology
            .read()
            .expect("coordinator topology lock poisoned");
        (topology.generation, topology.shards.clone())
    }

    fn table(&self) -> Vec<ShardConfig> {
        self.topology().1
    }

    /// Replace the shard table and drop the binding built from the old one.
    ///
    /// Split and Join both end here. Neither reshapes a collection in place:
    /// they build the new shards, and only once every one of them exists does
    /// the collection start pointing at them.
    fn rebind(&self, shards: Vec<ShardConfig>) -> Result<u64, Status> {
        let mut topology = self
            .topology
            .write()
            .expect("coordinator topology lock poisoned");
        let generation = topology
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::internal("topology generation counter overflow"))?;
        // A spare that the new topology places is a spare no longer. Compute
        // the surviving pool under the topology lock so the persisted file
        // is one consistent picture, and only commit it in memory after the
        // file is durable.
        let spares: Vec<String> = self
            .spares
            .lock()
            .expect("coordinator spare pool lock poisoned")
            .iter()
            .filter(|spare| !shards.iter().any(|shard| serves_address(shard, spare)))
            .cloned()
            .collect();
        if let Some(path) = self.topology_path.as_deref() {
            nodes::persist_topology(path, generation, &shards, &spares).map_err(|e| {
                Status::internal(format!("persist topology generation {generation}: {e}"))
            })?;
        }
        topology.generation = generation;
        topology.shards = shards;
        *self
            .spares
            .lock()
            .expect("coordinator spare pool lock poisoned") = spares;
        drop(topology);
        self.metrics.set_topology_generation(generation);
        *self
            .pinned
            .lock()
            .expect("coordinator binding lock poisoned") = None;
        Ok(generation)
    }

    /// A client for one node address, dialled lazily and cached.
    fn channel(&self, address: &str) -> Result<Channel, Status> {
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
                    .tcp_nodelay(true)
                    .connect_timeout(Duration::from_secs(2))
                    .timeout(Duration::from_secs(5));
                // Lazy: the connection is made on the first call, so a
                // coordinator starts with nodes still coming up and reports
                // them through ListNodes rather than refusing to boot.
                let channel = endpoint.connect_lazy();
                cache.insert(address.to_string(), channel.clone());
                channel
            }
        };
        Ok(channel)
    }

    fn query_client(&self, address: &str) -> Result<TurboVecQueryClient<Channel>, Status> {
        Ok(TurboVecQueryClient::new(self.channel(address)?)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES))
    }

    fn admin_client(&self, address: &str) -> Result<TurboVecAdminClient<Channel>, Status> {
        Ok(TurboVecAdminClient::new(self.channel(address)?)
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
        let request_id = uuid::Uuid::new_v4().to_string();

        for (shard_rank, shard) in pinned.shards.iter().enumerate() {
            let mut client = self.query_client(&shard.address)?;
            let (request_tx, request_rx) = watch::channel(StreamSearchRequest {
                payload: Some(crate::proto::stream_search_request::Payload::Start(
                    StartStreamSearch {
                        index_id: shard.index_id.clone(),
                        vector: vector.clone(),
                        initial_floor: None,
                        request_id: request_id.clone(),
                    },
                )),
            });
            let mut shard_request = Request::new(WatchStream::new(request_rx));
            shard_request.set_timeout(self.limits.query_timeout);
            let mut responses = client
                .stream_search(shard_request)
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
            outbound.push(Some(ShardControl {
                sender: request_tx,
                completed: false,
            }));
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
                                        .sender
                                        .send_replace(StreamSearchRequest {
                                            payload: Some(
                                                crate::proto::stream_search_request::Payload::FloorUpdate(
                                                    crate::proto::FloorUpdate { floor },
                                                ),
                                            ),
                                        });
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
                        outbound[shard_rank]
                            .as_mut()
                            .expect("incomplete shard retains its request stream")
                            .completed = true;
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
        tracing::info!(
            request_id = %request_id,
            topology_generation = pinned.generation,
            shards = pinned.shards.len(),
            k,
            returned = candidates.len(),
            final_floor = published_floor,
            "distributed scan finished"
        );
        Ok(CollectionQueryResult {
            neighbours: candidates
                .into_iter()
                .map(|candidate| {
                    let shard = &pinned.shards[candidate.shard_rank];
                    Neighbour {
                        score: candidate.score,
                        label: candidate.label,
                        address: shard.address.clone(),
                        index_id: shard.index_id.clone(),
                        slot: candidate.slot,
                    }
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
            let service = self.clone();
            let shard = shard.clone();
            tasks.push(tokio::spawn(async move {
                service.probe_for_search(&shard).await
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

    async fn probe_for_search(&self, shard: &ShardConfig) -> Result<Probe, Status> {
        let primary = probe(
            self.query_client(&shard.address)?,
            shard.address.clone(),
            shard.index_id.clone(),
        )
        .await
        .and_then(|probe| {
            validate_required_generation(shard, &probe)?;
            Ok(probe)
        });
        match primary {
            Ok(probe) => Ok(probe),
            Err(primary_error) => {
                let Some(required) = shard.required_generation else {
                    return Err(primary_error);
                };
                for address in &shard.replicas {
                    let replica = probe(
                        self.query_client(address)?,
                        address.clone(),
                        shard.index_id.clone(),
                    )
                    .await;
                    if let Ok(replica) = replica {
                        if replica.info.generation == required {
                            tracing::warn!(
                                primary = %shard.address,
                                replica = %address,
                                generation = required,
                                "serving shard from replica"
                            );
                            return Ok(replica);
                        }
                    }
                }
                Err(primary_error)
            }
        }
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

        let (generation, table) = self.topology();
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
        let pinned = Arc::new(bind(generation, shards)?);
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
            self.query_client(&shard.address)?,
            shard.address.clone(),
            shard.index_id.clone(),
        )
        .await
    }

    /// Pipe bounded encoded blocks from one or more sources into one target.
    /// The target activates nothing unless the complete expected row count
    /// arrives and every block agrees on shape and calibration.
    async fn import_ranges(
        &self,
        target: &str,
        expected_rows: u64,
        plans: Vec<ExportPlan>,
    ) -> Result<ImportRowsResponse, Status> {
        let mut exports = Vec::with_capacity(plans.len());
        for plan in plans {
            exports.push((self.admin_client(&plan.source.address)?, plan));
        }
        let (tx, rx) = mpsc::channel(4);
        tx.send(ImportRowsRequest {
            payload: Some(crate::proto::import_rows_request::Payload::Start(
                ImportRowsStart { expected_rows },
            )),
        })
        .await
        .map_err(|_| Status::cancelled("import stream closed before it started"))?;

        let producer = tokio::spawn(async move {
            for (mut client, plan) in exports {
                if plan.count == 0 {
                    tx.send(ImportRowsRequest {
                        payload: Some(crate::proto::import_rows_request::Payload::Block(
                            empty_block(&plan.source),
                        )),
                    })
                    .await
                    .map_err(|_| Status::cancelled("target closed the import stream"))?;
                    continue;
                }
                let mut stream = client
                    .export_rows(ExportRowsRequest {
                        index_id: plan.source.index_id.clone(),
                        start: plan.start,
                        count: plan.count,
                    })
                    .await
                    .map_err(|e| node_error(&plan.source.address, &e))?
                    .into_inner();
                while let Some(block) = stream
                    .message()
                    .await
                    .map_err(|e| node_error(&plan.source.address, &e))?
                {
                    tx.send(ImportRowsRequest {
                        payload: Some(crate::proto::import_rows_request::Payload::Block(block)),
                    })
                    .await
                    .map_err(|_| Status::cancelled("target closed the import stream"))?;
                }
            }
            Ok::<(), Status>(())
        });

        let mut target_client = self.admin_client(target)?;
        let imported = target_client.import_rows(ReceiverStream::new(rx)).await;
        let produced = producer
            .await
            .map_err(|e| Status::internal(format!("row transfer task failed: {e}")))?;
        let imported = imported.map_err(|status| node_error(target, &status))?;
        produced?;
        Ok(imported.into_inner())
    }

    /// A durable topology may point only at durable shard generations. Flush
    /// every future member before publishing the topology file so a restart
    /// can never restore handles that disappeared with a node process.
    async fn flush_before_durable_rebind(&self, shards: &[ShardRef]) -> Result<(), Status> {
        if self.topology_path.is_none() {
            return Ok(());
        }
        for shard in shards {
            let mut client = self.admin_client(&shard.address)?;
            client
                .flush(FlushRequest {
                    index_id: shard.index_id.clone(),
                })
                .await
                .map_err(|e| node_error(&shard.address, &e))?;
        }
        Ok(())
    }

    async fn configs_for_topology(&self, shards: &[ShardRef]) -> Result<Vec<ShardConfig>, Status> {
        let previous = self.table();
        let mut configs = Vec::with_capacity(shards.len());
        for shard in shards {
            let mut client = self.query_client(&shard.address)?;
            let info = client
                .get_index_info(GetIndexInfoRequest {
                    index_id: shard.index_id.clone(),
                })
                .await
                .map_err(|error| node_error(&shard.address, &error))?
                .into_inner();
            let mut config = ShardConfig::with_index_generation(
                &shard.address,
                &shard.index_id,
                (info.generation > 0).then_some(info.generation),
            );
            if let Some(old) = previous.iter().find(|old| old.address == config.address) {
                config.replicas = old.replicas.clone();
            }
            configs.push(config);
        }
        Ok(configs)
    }
}

fn empty_block(source: &Probe) -> RowBlock {
    RowBlock {
        dim: source.info.dim,
        bit_width: source.info.bit_width,
        rows: 0,
        packed_codes: Vec::new(),
        scales: Vec::new(),
        tqplus_shift: source.calibration.tqplus_shift.clone(),
        tqplus_scale: source.calibration.tqplus_scale.clone(),
        labels: Vec::new(),
    }
}

/// Read one node's view of one shard: which index, its metadata, its pair.
async fn probe(
    mut client: TurboVecQueryClient<Channel>,
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
fn bind(generation: u64, shards: Vec<Probe>) -> Result<Pinned, Status> {
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
        generation,
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

fn validate_required_generation(config: &ShardConfig, probe: &Probe) -> Result<(), Status> {
    if let Some(required) = config.required_generation {
        if probe.info.generation != required {
            return Err(Status::failed_precondition(format!(
                "shard_generation_mismatch: {} serves generation {}, topology requires {required}",
                probe.address, probe.info.generation
            )));
        }
    }
    Ok(())
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
        let (generation, table) = self.topology();
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
            match bind(generation, healthy) {
                Ok(pinned) => (true, String::new(), pinned.rows),
                Err(e) => (false, e.message().to_string(), 0),
            }
        };
        // The spare pool is probed with the same liveness the shards get: a
        // spare that died since it registered shows up here as its error,
        // not as a target that fails when named.
        let pool = self
            .spares
            .lock()
            .expect("coordinator spare pool lock poisoned")
            .clone();
        let mut spare_tasks = Vec::with_capacity(pool.len());
        for address in pool {
            let service = self.clone();
            spare_tasks.push(tokio::spawn(async move {
                let probed = async {
                    let mut client = service.query_client(&address)?;
                    client
                        .list_indexes(ListIndexesRequest {})
                        .await
                        .map_err(|e| node_error(&address, &e))
                }
                .await;
                match probed {
                    Ok(listed) => SpareNodeStatus {
                        address,
                        indexes: listed.into_inner().indexes.len() as u64,
                        error: String::new(),
                    },
                    Err(e) => SpareNodeStatus {
                        address,
                        indexes: 0,
                        error: e.message().to_string(),
                    },
                }
            }));
        }
        let mut spares = Vec::with_capacity(spare_tasks.len());
        for task in spare_tasks {
            spares.push(
                task.await
                    .map_err(|e| Status::internal(format!("spare probe task failed: {e}")))?,
            );
        }

        Ok(Response::new(ListNodesResponse {
            shards: statuses,
            servable,
            error,
            rows,
            topology_generation: generation,
            spares,
        }))
    }

    async fn search(
        &self,
        request: Request<CollectionSearchRequest>,
    ) -> Result<Response<CollectionSearchResponse>, Status> {
        let req = request.into_inner();
        let k = req.k as usize;
        if k == 0 || k > self.limits.max_k {
            return Err(Status::invalid_argument(format!(
                "k must be between 1 and {}",
                self.limits.max_k
            )));
        }
        let pinned = self.collection().await?;
        if req.queries.is_empty() || !req.queries.len().is_multiple_of(pinned.dim) {
            return Err(Status::invalid_argument(format!(
                "query buffer length {} is not a positive multiple of the collection dim {}",
                req.queries.len(),
                pinned.dim
            )));
        }
        let nq = req.queries.len() / pinned.dim;
        if nq > self.limits.max_queries_per_request {
            return Err(Status::resource_exhausted(format!(
                "request has {nq} queries; limit is {}",
                self.limits.max_queries_per_request
            )));
        }

        // There is one distributed search algorithm: the coordinator owns the
        // global heap, nodes stream above its live floor, and every node must
        // certify completion. A degraded partial ranking is never returned.
        let vectors: Vec<Vec<f32>> = req
            .queries
            .chunks_exact(pinned.dim)
            .map(<[f32]>::to_vec)
            .collect();
        let mut results: Vec<Option<CollectionQueryResult>> = (0..nq).map(|_| None).collect();
        let mut tasks = tokio::task::JoinSet::new();
        let mut next = 0usize;
        while next < nq || !tasks.is_empty() {
            while next < nq && tasks.len() < self.limits.max_concurrent_queries {
                let service = self.clone();
                let pinned = Arc::clone(&pinned);
                let vector = vectors[next].clone();
                let query_index = next;
                let timeout = self.limits.query_timeout;
                tasks.spawn(async move {
                    let result =
                        tokio::time::timeout(timeout, service.stream_query(&pinned, vector, k))
                            .await
                            .map_err(|_| {
                                Status::deadline_exceeded("distributed search deadline exceeded")
                            })?;
                    Ok::<_, Status>((query_index, result?))
                });
                next += 1;
            }
            let result = tasks
                .join_next()
                .await
                .expect("query task set is non-empty")
                .map_err(|error| Status::internal(format!("query task failed: {error}")))?;
            match result {
                Ok((query_index, result)) => results[query_index] = Some(result),
                Err(status) => {
                    self.metrics.search_failed();
                    return Err(status);
                }
            }
        }
        let results = results
            .into_iter()
            .map(|result| result.expect("every query task filled its result slot"))
            .collect();
        self.metrics.coordinator_search_finished();
        Ok(Response::new(CollectionSearchResponse {
            results,
            topology_generation: pinned.generation,
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
            let mut client = self.admin_client(&probe.address)?;
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
        self.flush_before_durable_rebind(&shards).await?;
        let configs = self.configs_for_topology(&shards).await?;
        self.rebind(configs)?;
        let pinned = self.collection().await?;
        Ok(Response::new(FitCalibrationResponse {
            calibration: Some(pinned.calibration.clone()),
            shards,
            topology_generation: pinned.generation,
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

        // One target at a time. Every transfer is a bounded block stream and
        // the target activates only after receiving its exact row count.
        let mut shards = Vec::with_capacity(req.targets.len());
        let mut start = 0u64;
        for (target, &count) in req.targets.iter().zip(counts.iter()) {
            let imported = self
                .import_ranges(
                    target,
                    count,
                    vec![ExportPlan {
                        source: source.clone(),
                        start,
                        count,
                    }],
                )
                .await?;
            shards.push(shard_ref(target, &imported.index_id));
            start += count;
        }

        self.flush_before_durable_rebind(&shards).await?;
        let configs = self.configs_for_topology(&shards).await?;
        let topology_generation = self.rebind(configs)?;
        Ok(Response::new(SplitResponse {
            shards,
            rows: counts,
            calibration: Some(source.calibration),
            topology_generation,
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
        let pinned = bind(self.topology().0, probes.clone())?;

        let target = ShardConfig::new(req.target);
        let plans = probes
            .into_iter()
            .map(|source| ExportPlan {
                count: source.info.len,
                source,
                start: 0,
            })
            .collect();
        let imported = self
            .import_ranges(&target.address, pinned.rows, plans)
            .await?;
        let shard = shard_ref(&target.address, &imported.index_id);

        self.flush_before_durable_rebind(std::slice::from_ref(&shard))
            .await?;
        let configs = self
            .configs_for_topology(std::slice::from_ref(&shard))
            .await?;
        let topology_generation = self.rebind(configs)?;
        Ok(Response::new(JoinResponse {
            rows: pinned.rows,
            calibration: Some(pinned.calibration),
            shard: Some(shard),
            topology_generation,
        }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        if req.address.trim().is_empty() {
            return Err(Status::invalid_argument(
                "a node registers the address the coordinator should dial it at; \
                 a node listening on 0.0.0.0 must say which of its names to use",
            ));
        }
        let address = nodes::with_scheme(req.address.trim().to_string());

        // Dial the node back before accepting it. An address the coordinator
        // cannot reach is refused now, while the operator is looking at the
        // node that sent it, rather than kept as a spare that fails the
        // Split it is eventually named in.
        let mut client = self.query_client(&address)?;
        client
            .list_indexes(ListIndexesRequest {})
            .await
            .map_err(|e| node_error(&address, &e))?;

        // Held across the persist so a concurrent Split cannot advance the
        // generation between this snapshot and the file write; the lock
        // order (topology, then spares) matches `rebind`.
        let topology = self
            .topology
            .read()
            .expect("coordinator topology lock poisoned");
        let generation = topology.generation;
        if topology
            .shards
            .iter()
            .any(|shard| serves_address(shard, &address))
        {
            return Ok(Response::new(RegisterNodeResponse {
                member: true,
                topology_generation: generation,
            }));
        }

        // Idempotent insert, persisted before it is acknowledged: a spare a
        // node was told it is must still be there after a coordinator
        // restart, or the "re-announce periodically" contract quietly turns
        // into "hope the coordinator did not restart".
        let mut spares = self
            .spares
            .lock()
            .expect("coordinator spare pool lock poisoned");
        if !spares.contains(&address) {
            let mut next = spares.clone();
            next.push(address.clone());
            if let Some(path) = self.topology_path.as_deref() {
                nodes::persist_topology(path, generation, &topology.shards, &next)
                    .map_err(|e| Status::internal(format!("persist spare pool: {e}")))?;
            }
            *spares = next;
            tracing::info!(%address, "node registered as spare");
        }
        Ok(Response::new(RegisterNodeResponse {
            member: false,
            topology_generation: generation,
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

/// Whether `address` serves this shard, as its primary or as a replica.
fn serves_address(shard: &ShardConfig, address: &str) -> bool {
    shard.address == address || shard.replicas.iter().any(|replica| replica == address)
}
