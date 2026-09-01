//! Integration tests for the grow-only autoscaler.
//!
//! The autoscaler turns "the operator names Split targets" into "the
//! operator keeps the spare pool stocked", so these tests stand up a real
//! node, a real spare, and a real coordinator, and check placement rather
//! than plumbing: an over-ceiling shard is split onto its own node and the
//! spare, the spare leaves the pool, the quiesced source goes away — and
//! the collection answers the same queries with the same rows at the same
//! scores to the bit, because the split moved encoded rows and re-encoded
//! nothing.
//!
//! The exactness gate is the same one `distributed.rs` applies to an
//! operator-driven Split: the autoscaler is not a second mutation path, so
//! it is held to the same account.

use std::time::Duration;

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddRequest, CollectionSearchRequest, CreateIndexRequest, IndexKind, ListIndexesRequest,
    ListNodesRequest, RegisterNodeRequest, SetCalibrationRequest,
};
use turbovec_grpc::{
    AutoscalePolicy, CoordinatorService, IndexStore, NodeTable, ShardConfig, TurboVecService,
};

/// Vector width used throughout. A multiple of 8, as turbovec requires, and
/// small enough that a few hundred rows cost nothing to encode.
const DIM: usize = 64;

/// Quantization bit width used throughout.
const BIT_WIDTH: u32 = 4;

/// Rows in the serving shard: comfortably over the ceiling, and an even
/// number so the autosplit leaves both halves under it.
const ROWS: usize = 300;

/// The autoscaler ceiling: ROWS crosses it, each half of the even split
/// does not.
const MAX_ROWS_PER_SHARD: u64 = 200;

/// The autoscaler tick in these tests: fast enough that a bounded wait is
/// short, slow enough that several ticks fit in the disabled/no-spare
/// observation windows.
const INTERVAL: Duration = Duration::from_millis(100);

/// Neighbours asked for. Comfortably smaller than any one shard.
const K: u32 = 12;

/// A tiny linear congruential generator, so the corpora are deterministic
/// and the test needs no RNG dependency. The constants are Numerical
/// Recipes'.
struct Lcg(u32);

impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        // Top 24 bits, mapped to [-1, 1). Taking the top bits rather than the
        // bottom ones matters: an LCG's low bits have short periods.
        ((self.0 >> 8) as f32 / 8_388_608.0) - 1.0
    }

    fn rows(&mut self, rows: usize) -> Vec<f32> {
        (0..rows * DIM).map(|_| self.next_f32()).collect()
    }
}

/// Start a node server on a loopback ephemeral port; returns its address.
async fn start_node() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = TurboVecService::new(IndexStore::new());
    let compatibility = service.clone().into_server();
    let query = service.clone().into_query_server();
    let admin = service.into_admin_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(compatibility)
            .add_service(query)
            .add_service(admin)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

/// Serve a coordinator and hand back both a client and the service itself,
/// so a test can decide whether the autoscaler runs on it.
async fn start_coordinator(table: NodeTable) -> (CoordinatorClient<Channel>, CoordinatorService) {
    let service = CoordinatorService::new(table);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = service.clone().into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (
        CoordinatorClient::new(connect(&format!("http://{addr}")).await),
        service,
    )
}

/// Dial an address, retrying briefly while the server binds.
async fn connect(address: &str) -> Channel {
    Endpoint::from_shared(address.to_string())
        .unwrap()
        .connect()
        .await
        .expect("server accepted the connection")
}

/// A client for one node.
async fn node_client(address: &str) -> TurboVecClient<Channel> {
    TurboVecClient::new(connect(address).await)
}

/// Fit a calibration pair the way the coordinator does.
fn fit_pair(seed: u32) -> (Vec<f32>, Vec<f32>) {
    let sample = Lcg(seed).rows(turbovec::MIN_CALIBRATION_ROWS.max(512));
    let mut index = turbovec::TurboQuantIndex::new(DIM, BIT_WIDTH as usize).unwrap();
    index.calibrate_2d(&sample, DIM).unwrap();
    (index.tqplus_shift().to_vec(), index.tqplus_scale().to_vec())
}

/// Build the serving shard: one calibrated positional index holding the
/// whole corpus, over the autoscaler ceiling.
async fn build_serving_shard(address: &str) -> String {
    let mut client = node_client(address).await;
    let index_id = client
        .create_index(CreateIndexRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    let pair = fit_pair(7);
    client
        .set_calibration(SetCalibrationRequest {
            index_id: index_id.clone(),
            tqplus_shift: pair.0,
            tqplus_scale: pair.1,
        })
        .await
        .unwrap();
    let added = client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: index_id.clone(),
            dim: DIM as u32,
            vectors: Lcg(99).rows(ROWS),
            ids: Vec::new(),
            ..Default::default()
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.len, ROWS as u64);
    index_id
}

/// One query result reduced to what must match: the score bits and the row.
type Ranking = Vec<(u32, u64)>;

/// Search the collection through the coordinator and take its ranking per
/// query, identifying rows by the labels the shards carry.
///
/// A shard built by the autosplit carries an external id per row, which is
/// the row's slot in the index the collection was built as; the unsplit
/// shard carries none, and its slots are the same numbers. So the ranking
/// is directly comparable across the split.
async fn distributed_ranking(
    coordinator: &mut CoordinatorClient<Channel>,
    queries: &[f32],
) -> Vec<Ranking> {
    let response = coordinator
        .search(CollectionSearchRequest {
            queries: queries.to_vec(),
            k: K,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    response
        .results
        .into_iter()
        .map(|r| {
            r.neighbours
                .into_iter()
                .map(|n| (n.score.to_bits(), n.label.unwrap_or(n.slot)))
                .collect()
        })
        .collect()
}

/// Assert two rankings are the same ranking.
///
/// Scores must match bit for bit: the codes on both sides are the same
/// bytes, so the kernel computes the same float, and anything less than
/// equality would mean a row was re-encoded somewhere. Row order must match
/// too, except where two rows carry the same score, which the merge is free
/// to order either way because there is nothing to order them by.
fn assert_same_ranking(before: &[Ranking], after: &[Ranking], label: &str) {
    assert_eq!(before.len(), after.len(), "{label}: query counts differ");
    for (qi, (b, a)) in before.iter().zip(after.iter()).enumerate() {
        assert_eq!(b.len(), a.len(), "{label}: query {qi} lengths differ");
        let before_scores: Vec<u32> = b.iter().map(|e| e.0).collect();
        let after_scores: Vec<u32> = a.iter().map(|e| e.0).collect();
        assert_eq!(
            before_scores, after_scores,
            "{label}: query {qi} scores are not bit-identical"
        );
        for (rank, (x, y)) in b.iter().zip(a.iter()).enumerate() {
            if x.1 != y.1 {
                assert_eq!(
                    x.0, y.0,
                    "{label}: query {qi} rank {rank} holds a different row ({} against {}) at \
                     different scores, so this is a real disagreement and not a tie",
                    x.1, y.1
                );
            }
        }
        let mut before_rows: Vec<u64> = b.iter().map(|e| e.1).collect();
        let mut after_rows: Vec<u64> = a.iter().map(|e| e.1).collect();
        before_rows.sort_unstable();
        after_rows.sort_unstable();
        assert_eq!(
            before_rows, after_rows,
            "{label}: query {qi} returned a different set of rows"
        );
    }
}

/// The index ids a node currently holds.
async fn index_ids(address: &str) -> Vec<String> {
    node_client(address)
        .await
        .list_indexes(ListIndexesRequest {})
        .await
        .unwrap()
        .into_inner()
        .indexes
        .into_iter()
        .map(|index| index.index_id)
        .collect()
}

/// Disabled by default: an over-ceiling shard with a stocked spare pool is
/// left exactly where it is, generation after generation.
#[tokio::test]
async fn an_over_ceiling_shard_is_left_alone_when_the_autoscaler_is_off() {
    // The ceiling is the enable knob: unset, there is no policy at all.
    if std::env::var_os("TURBOVEC_AUTOSCALE_MAX_ROWS_PER_SHARD").is_none() {
        assert!(AutoscalePolicy::from_env().unwrap().is_none());
    }

    let serving = start_node().await;
    let index_id = build_serving_shard(&serving).await;
    let table = NodeTable::new(vec![ShardConfig::with_index(&serving, &index_id)]);
    let (mut coordinator, _service) = start_coordinator(table).await;

    let spare = start_node().await;
    coordinator
        .register_node(RegisterNodeRequest {
            address: spare.clone(),
        })
        .await
        .unwrap();

    // Several autoscaler intervals pass with nothing running them.
    tokio::time::sleep(INTERVAL * 5).await;

    let listed = coordinator
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.topology_generation, 1);
    assert_eq!(listed.shards.len(), 1);
    assert!(listed.servable, "listing said: {}", listed.error);
    assert_eq!(listed.rows, ROWS as u64);
    assert_eq!(listed.spares.len(), 1, "the spare pool is untouched");
    assert_eq!(
        index_ids(&spare).await.len(),
        0,
        "the spare received nothing"
    );
}

/// The main path: the coordinator splits the over-ceiling shard onto its own
/// node and the spare, publishes the new generation, drops the quiesced
/// source, and the collection answers exactly as it did before.
#[tokio::test]
async fn an_over_ceiling_shard_is_split_onto_the_spare_with_identical_results() {
    let serving = start_node().await;
    let index_id = build_serving_shard(&serving).await;
    let table = NodeTable::new(vec![ShardConfig::with_index(&serving, &index_id)]);
    let (mut coordinator, service) = start_coordinator(table).await;

    let spare = start_node().await;
    coordinator
        .register_node(RegisterNodeRequest {
            address: spare.clone(),
        })
        .await
        .unwrap();

    // Four queries, none of them drawn from the corpus, so the rankings are
    // not a set of exact hits that any implementation would get right.
    let queries = Lcg(4242).rows(4);
    let before = distributed_ranking(&mut coordinator, &queries).await;
    for query in &before {
        assert_eq!(query.len(), K as usize);
    }

    service.spawn_autoscaler(AutoscalePolicy::new(MAX_ROWS_PER_SHARD, INTERVAL));

    // A 300-row split at this tick is sub-second; the bound is for a loaded
    // CI host, not for the split.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let listed = loop {
        let listed = coordinator
            .list_nodes(ListNodesRequest {})
            .await
            .unwrap()
            .into_inner();
        if listed.topology_generation == 2
            && listed.shards.len() == 2
            && listed.servable
            && index_ids(&serving).await.len() == 1
        {
            break listed;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the autoscaler did not finish in time: generation {}, {} shards, servable: {}, \
             source indexes: {:?}",
            listed.topology_generation,
            listed.shards.len(),
            listed.servable,
            index_ids(&serving).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The split is even, both halves fit under the ceiling, and the spare
    // became a member.
    assert_eq!(listed.rows, ROWS as u64);
    let rows: Vec<u64> = listed
        .shards
        .iter()
        .map(|shard| shard.info.as_ref().unwrap().len)
        .collect();
    assert_eq!(rows, vec![ROWS as u64 / 2, ROWS as u64 / 2]);
    assert!(
        listed.spares.is_empty(),
        "a placed spare must leave the pool"
    );
    let addresses: Vec<&str> = listed
        .shards
        .iter()
        .map(|shard| shard.shard.as_ref().unwrap().address.as_str())
        .collect();
    assert_eq!(addresses, vec![serving.as_str(), spare.as_str()]);

    // The quiesced source is gone from its node, which now holds only the
    // half it kept; the spare holds exactly the other half.
    let serving_indexes = index_ids(&serving).await;
    assert!(
        !serving_indexes.contains(&index_id),
        "the quiesced source must be dropped"
    );
    assert_eq!(index_ids(&spare).await.len(), 1);

    // The exactness gate: same rows, bit-identical scores, before and after.
    let after = distributed_ranking(&mut coordinator, &queries).await;
    assert_same_ranking(&before, &after, "autosplit");

    // The halves are both under the ceiling, so the next ticks change
    // nothing.
    tokio::time::sleep(INTERVAL * 5).await;
    let settled = coordinator
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(settled.topology_generation, 2);
    assert_eq!(settled.shards.len(), 2);
}

/// No spare, nowhere to grow: the shard stays over the ceiling, the
/// collection stays servable, and the topology does not move.
#[tokio::test]
async fn an_over_ceiling_shard_without_a_spare_is_left_serving() {
    let serving = start_node().await;
    let index_id = build_serving_shard(&serving).await;
    let table = NodeTable::new(vec![ShardConfig::with_index(&serving, &index_id)]);
    let (mut coordinator, service) = start_coordinator(table).await;

    service.spawn_autoscaler(AutoscalePolicy::new(MAX_ROWS_PER_SHARD, INTERVAL));
    tokio::time::sleep(INTERVAL * 5).await;

    let listed = coordinator
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.topology_generation, 1);
    assert_eq!(listed.shards.len(), 1);
    assert!(listed.servable, "listing said: {}", listed.error);
    assert_eq!(listed.rows, ROWS as u64);

    // And it still answers.
    let queries = Lcg(4242).rows(2);
    let ranking = distributed_ranking(&mut coordinator, &queries).await;
    for query in &ranking {
        assert_eq!(query.len(), K as usize);
    }
}
