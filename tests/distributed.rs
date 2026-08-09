//! Integration tests for the thin distributed layer.
//!
//! The claim the layer makes is that a client cannot tell a sharded collection
//! from a single index, so these tests do not check that distributed search is
//! close to monolithic search. They check that it is the same: the same scores
//! to the bit, the same rows, in the same order except where two rows tie and
//! the order between them means nothing.
//!
//! Everything runs against real servers over real sockets. Each node is a
//! `TurboVecService` on its own ephemeral loopback port and the coordinator is
//! a `CoordinatorService` on another, so the tests exercise the wire, the
//! fan-out, and the merge rather than the functions underneath them.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddRequest, Calibration, CollectionSearchRequest, CreateIndexRequest, ExportRowsRequest,
    ImportRowsRequest, IndexKind, JoinRequest, RowBlock, SearchRequest, SetCalibrationRequest,
    ShardRef, SplitRequest,
};
use turbovec_grpc::{CoordinatorService, IndexStore, NodeTable, ShardConfig, TurboVecService};

/// Vector width used throughout. A multiple of 8, as turbovec requires, and
/// small enough that a few hundred rows cost nothing to encode.
const DIM: usize = 64;

/// Quantization bit width used throughout.
const BIT_WIDTH: u32 = 4;

/// Rows in the corpus the tests build and then redistribute.
const ROWS: usize = 600;

/// Neighbours asked for. Comfortably smaller than any one shard, so every
/// shard is a real contributor to the merge rather than being exhausted.
const K: u32 = 12;

/// A tiny linear congruential generator, so the corpora are deterministic and
/// the test needs no RNG dependency. The constants are Numerical Recipes'.
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
    let service = TurboVecService::new(IndexStore::new()).into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

/// Start a coordinator over `table`; returns a connected client.
async fn start_coordinator(table: NodeTable) -> CoordinatorClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = CoordinatorService::new(table).into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    CoordinatorClient::new(connect(&format!("http://{addr}")).await)
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

/// Fit a calibration pair the way the coordinator does, so a test can commit
/// the same pair to several indexes without going through `FitCalibration`.
fn fit_pair(seed: u32) -> (Vec<f32>, Vec<f32>) {
    let sample = Lcg(seed).rows(turbovec::MIN_CALIBRATION_ROWS.max(512));
    let mut index = turbovec::TurboQuantIndex::new(DIM, BIT_WIDTH as usize).unwrap();
    index.calibrate_2d(&sample, DIM).unwrap();
    (index.tqplus_shift().to_vec(), index.tqplus_scale().to_vec())
}

/// Create an empty positional index on `client`, at `dim`.
async fn create_index(client: &mut TurboVecClient<Channel>, dim: usize) -> String {
    client
        .create_index(CreateIndexRequest {
            dim: dim as u32,
            bit_width: BIT_WIDTH,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id
}

/// Build the monolithic index: one positional index, calibrated, holding the
/// whole corpus. This is what every distributed result is measured against.
async fn build_monolith(address: &str, pair: &(Vec<f32>, Vec<f32>)) -> (String, Vec<f32>) {
    let mut client = node_client(address).await;
    let index_id = create_index(&mut client, DIM).await;
    client
        .set_calibration(SetCalibrationRequest {
            index_id: index_id.clone(),
            tqplus_shift: pair.0.clone(),
            tqplus_scale: pair.1.clone(),
        })
        .await
        .unwrap();

    let corpus = Lcg(99).rows(ROWS);
    let added = client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: index_id.clone(),
            dim: DIM as u32,
            vectors: corpus.clone(),
            ids: Vec::new(),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.len, ROWS as u64);
    (index_id, corpus)
}

/// One query result reduced to what must match: the score bits and the row.
type Ranking = Vec<(u32, u64)>;

/// Search the monolithic index directly and take its ranking per query.
async fn monolithic_ranking(address: &str, index_id: &str, queries: &[f32]) -> Vec<Ranking> {
    let mut client = node_client(address).await;
    let response = client
        .search(SearchRequest {
            index_id: index_id.to_string(),
            queries: queries.to_vec(),
            k: K,
            allowlist: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    response
        .results
        .into_iter()
        .map(|r| {
            r.scores
                .iter()
                .zip(r.ids.iter())
                .map(|(s, id)| (s.to_bits(), *id))
                .collect()
        })
        .collect()
}

/// Search the collection through the coordinator and take its ranking per
/// query, identifying rows by the labels the shards carry.
///
/// A shard built by Split or Join carries an external id per row, which is the
/// row's slot in the index the collection was originally built as, so it is
/// directly comparable to a monolithic result. A shard built by plain adds
/// carries none, and its rows are identified by slot instead, which for an
/// unsplit single-shard collection is the same number.
async fn distributed_ranking(
    coordinator: &mut CoordinatorClient<Channel>,
    queries: &[f32],
) -> Vec<Ranking> {
    let response = coordinator
        .search(CollectionSearchRequest {
            queries: queries.to_vec(),
            k: K,
            allow_partial: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        !response.partial,
        "a complete search must never report itself partial"
    );
    assert!(response.failures.is_empty());
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
/// Scores must match bit for bit: the codes on both sides are the same bytes,
/// so the kernel computes the same float, and anything less than equality
/// would mean a row was re-encoded somewhere. Row order must match too, except
/// where two rows carry the same score, which the merge is free to order
/// either way because there is nothing to order them by.
fn assert_same_ranking(monolithic: &[Ranking], distributed: &[Ranking], label: &str) {
    assert_eq!(
        monolithic.len(),
        distributed.len(),
        "{label}: query counts differ"
    );
    for (qi, (mono, dist)) in monolithic.iter().zip(distributed.iter()).enumerate() {
        assert_eq!(
            mono.len(),
            dist.len(),
            "{label}: query {qi} returned {} rows against {}",
            dist.len(),
            mono.len()
        );
        let mono_scores: Vec<u32> = mono.iter().map(|e| e.0).collect();
        let dist_scores: Vec<u32> = dist.iter().map(|e| e.0).collect();
        assert_eq!(
            mono_scores, dist_scores,
            "{label}: query {qi} scores are not bit-identical"
        );
        for (rank, (m, d)) in mono.iter().zip(dist.iter()).enumerate() {
            if m.1 != d.1 {
                assert_eq!(
                    m.0, d.0,
                    "{label}: query {qi} rank {rank} holds a different row ({} against {}) at \
                     different scores, so this is a real disagreement and not a tie",
                    m.1, d.1
                );
            }
        }
        let mut mono_rows: Vec<u64> = mono.iter().map(|e| e.1).collect();
        let mut dist_rows: Vec<u64> = dist.iter().map(|e| e.1).collect();
        mono_rows.sort_unstable();
        dist_rows.sort_unstable();
        assert_eq!(
            mono_rows, dist_rows,
            "{label}: query {qi} returned a different set of rows"
        );
    }
}

/// The whole lifecycle in one pass: calibrate a collection, fill it, split it
/// across three nodes, search it, and join it back.
///
/// Each stage is checked against the same monolithic index, so a stage that
/// silently changed a row would show up as a changed score at the next
/// comparison rather than being carried forward.
#[tokio::test]
async fn split_search_join_all_equal_the_monolithic_index() {
    let nodes = [start_node().await, start_node().await, start_node().await];
    let pair = fit_pair(7);
    let (monolith_id, _corpus) = build_monolith(&nodes[0], &pair).await;

    // Four queries, none of them drawn from the corpus, so the rankings are
    // not a set of exact hits that any implementation would get right.
    let queries = Lcg(4242).rows(4);
    let expected = monolithic_ranking(&nodes[0], &monolith_id, &queries).await;
    for query in &expected {
        assert_eq!(query.len(), K as usize);
    }

    // The collection starts as the single monolithic shard.
    let mut coordinator = start_coordinator(NodeTable::new(vec![ShardConfig::with_index(
        &nodes[0],
        &monolith_id,
    )]))
    .await;

    // One shard is still a collection, and searching it must already agree.
    assert_same_ranking(
        &expected,
        &distributed_ranking(&mut coordinator, &queries).await,
        "unsplit",
    );

    // Split across all three nodes, deliberately unevenly, so no shard
    // boundary lines up with a round number of rows.
    let split = coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[0].clone(),
                index_id: monolith_id.clone(),
            }),
            targets: nodes.to_vec(),
            row_counts: vec![137, 251, (ROWS - 137 - 251) as u64],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(split.shards.len(), 3);
    assert_eq!(split.rows, vec![137, 251, 212]);
    assert_eq!(
        split.calibration.as_ref().map(|c| c.tqplus_shift.clone()),
        Some(pair.0.clone()),
        "the split must move rows under the source's own pair"
    );

    assert_same_ranking(
        &expected,
        &distributed_ranking(&mut coordinator, &queries).await,
        "split across three nodes",
    );

    // Every row now lives on a shard that is not the one it was added to, and
    // every result still names it by the id it had in the monolithic index.
    // That is what makes the comparison above a comparison of rows rather than
    // of coincidentally equal slot numbers.
    let after_split = coordinator
        .search(CollectionSearchRequest {
            queries: queries.clone(),
            k: K,
            allow_partial: false,
        })
        .await
        .unwrap()
        .into_inner();
    for result in &after_split.results {
        for neighbour in &result.neighbours {
            assert!(
                neighbour.label.is_some(),
                "a shard built by Split carries a label per row"
            );
            assert!(
                nodes.contains(&neighbour.address),
                "a neighbour must name the node it was found on"
            );
        }
    }

    // The collection is now three shards and reports itself so.
    let listed = coordinator
        .list_nodes(turbovec_grpc::proto::ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(listed.servable, "listing said: {}", listed.error);
    assert_eq!(listed.shards.len(), 3);
    assert_eq!(listed.rows, ROWS as u64);

    // Join it all back onto one node, and it is the monolithic index again.
    let joined = coordinator
        .join(JoinRequest {
            sources: Vec::new(),
            target: nodes[2].clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(joined.rows, ROWS as u64);
    assert_eq!(
        joined.calibration.map(|c| c.tqplus_scale),
        Some(pair.1.clone()),
        "the join must keep the sources' pair"
    );

    assert_same_ranking(
        &expected,
        &distributed_ranking(&mut coordinator, &queries).await,
        "joined back to one node",
    );

    // Splitting the joined index again lands on the same answers, so the
    // round trip is not a one-way approximation that happened to survive once.
    let rejoined = joined.shard.unwrap();
    coordinator
        .split(SplitRequest {
            source: Some(rejoined),
            targets: vec![nodes[0].clone(), nodes[1].clone()],
            row_counts: Vec::new(),
        })
        .await
        .unwrap();
    assert_same_ranking(
        &expected,
        &distributed_ranking(&mut coordinator, &queries).await,
        "split again after the join",
    );
}

/// The coordinator fits one pair and every shard ends up holding it.
#[tokio::test]
async fn fit_calibration_broadcasts_one_pair() {
    let nodes = [start_node().await, start_node().await];
    let mut shards = Vec::new();
    for address in &nodes {
        let mut client = node_client(address).await;
        let index_id = create_index(&mut client, DIM).await;
        shards.push(ShardConfig::with_index(address, index_id));
    }
    let mut coordinator = start_coordinator(NodeTable::new(shards.clone())).await;

    let fitted = coordinator
        .fit_calibration(turbovec_grpc::proto::FitCalibrationRequest {
            sample: Lcg(11).rows(turbovec::MIN_CALIBRATION_ROWS.max(512)),
            dim: DIM as u32,
            bit_width: BIT_WIDTH,
        })
        .await
        .unwrap()
        .into_inner();
    let calibration = fitted.calibration.expect("a fitted pair is returned");
    assert_eq!(calibration.tqplus_shift.len(), DIM);
    assert_eq!(calibration.tqplus_scale.len(), DIM);

    // Read each node back independently of the coordinator's own account.
    for shard in &shards {
        let mut client = node_client(&shard.address).await;
        let held = client
            .get_calibration(turbovec_grpc::proto::GetCalibrationRequest {
                index_id: shard.index_id.clone().unwrap(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(held.tqplus_shift, calibration.tqplus_shift);
        assert_eq!(held.tqplus_scale, calibration.tqplus_scale);
    }
}

/// Set up two nodes holding one empty index each, at the given dims and pairs.
async fn two_shard_collection(
    dims: [usize; 2],
    pairs: [Option<(Vec<f32>, Vec<f32>)>; 2],
) -> (Vec<ShardConfig>, CoordinatorClient<Channel>) {
    let mut shards = Vec::new();
    for (i, dim) in dims.into_iter().enumerate() {
        let address = start_node().await;
        let mut client = node_client(&address).await;
        let index_id = create_index(&mut client, dim).await;
        if let Some(pair) = &pairs[i] {
            client
                .set_calibration(SetCalibrationRequest {
                    index_id: index_id.clone(),
                    tqplus_shift: pair.0.clone(),
                    tqplus_scale: pair.1.clone(),
                })
                .await
                .unwrap();
        }
        shards.push(ShardConfig::with_index(address, index_id));
    }
    let coordinator = start_coordinator(NodeTable::new(shards.clone())).await;
    (shards, coordinator)
}

/// Turn a configured shard into the wire reference for it.
fn shard_ref(shard: &ShardConfig) -> ShardRef {
    ShardRef {
        address: shard.address.clone(),
        index_id: shard.index_id.clone().unwrap_or_default(),
    }
}

/// Two shards calibrated differently are refused, by name, for both search and
/// join. Their scores are on two scales, and a merge of them would rank
/// nothing while looking exactly like a ranking.
#[tokio::test]
async fn mixed_calibration_is_refused_by_name() {
    let (shards, mut coordinator) =
        two_shard_collection([DIM, DIM], [Some(fit_pair(1)), Some(fit_pair(2))]).await;

    let search = coordinator
        .search(CollectionSearchRequest {
            queries: Lcg(5).rows(1),
            k: K,
            allow_partial: false,
        })
        .await
        .unwrap_err();
    assert!(
        search.message().starts_with("mixed_calibration:"),
        "search said: {}",
        search.message()
    );

    let join = coordinator
        .join(JoinRequest {
            sources: shards.iter().map(shard_ref).collect(),
            target: shards[0].address.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        join.message().starts_with("mixed_calibration:"),
        "join said: {}",
        join.message()
    );

    // `allow_partial` is about unreachable nodes, not about serving a
    // collection that does not add up: it must not open a back door here.
    let partial = coordinator
        .search(CollectionSearchRequest {
            queries: Lcg(5).rows(1),
            k: K,
            allow_partial: true,
        })
        .await
        .unwrap_err();
    assert!(partial.message().starts_with("mixed_calibration:"));
}

/// Two shards of different width are refused, by name, for both search and
/// join.
#[tokio::test]
async fn dimension_mismatch_is_refused_by_name() {
    let (shards, mut coordinator) = two_shard_collection([DIM, DIM * 2], [None, None]).await;

    let search = coordinator
        .search(CollectionSearchRequest {
            queries: Lcg(6).rows(1),
            k: K,
            allow_partial: false,
        })
        .await
        .unwrap_err();
    assert!(
        search.message().starts_with("dimension_mismatch:"),
        "search said: {}",
        search.message()
    );

    let join = coordinator
        .join(JoinRequest {
            sources: shards.iter().map(shard_ref).collect(),
            target: shards[0].address.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        join.message().starts_with("dimension_mismatch:"),
        "join said: {}",
        join.message()
    );
}

/// Split's own refusals.
///
/// A split has exactly one source, so it cannot meet a calibration or
/// dimension disagreement: there is nothing for the source to disagree with,
/// and the shards it produces are copies of its own rows under its own pair.
/// What it can meet is a source it cannot read rows out of, and a row plan
/// that does not add up, and it refuses both by name rather than splitting
/// something else or dropping the difference.
#[tokio::test]
async fn split_refuses_an_unreadable_source_and_a_plan_that_does_not_add_up() {
    let nodes = [start_node().await, start_node().await];
    let pair = fit_pair(7);
    let (monolith_id, _corpus) = build_monolith(&nodes[0], &pair).await;
    let mut coordinator = start_coordinator(NodeTable::new(vec![ShardConfig::with_index(
        &nodes[0],
        &monolith_id,
    )]))
    .await;

    // Counts that do not sum to the source's rows would silently drop rows or
    // ask for ones that are not there.
    let bad_counts = coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[0].clone(),
                index_id: monolith_id.clone(),
            }),
            targets: vec![nodes[0].clone(), nodes[1].clone()],
            row_counts: vec![100, 100],
        })
        .await
        .unwrap_err();
    assert!(
        bad_counts.message().starts_with("row_count_mismatch:"),
        "split said: {}",
        bad_counts.message()
    );

    // No targets at all is the same class of failure.
    let no_targets = coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[0].clone(),
                index_id: monolith_id.clone(),
            }),
            targets: Vec::new(),
            row_counts: Vec::new(),
        })
        .await
        .unwrap_err();
    assert!(no_targets.message().starts_with("row_count_mismatch:"));

    // An id-mapped source cannot be split: turbovec's IdMapIndex does not
    // expose the packed codes, and the only way around that would be to
    // decode and re-encode the rows, which would change their scores.
    let mut client = node_client(&nodes[1]).await;
    let id_mapped = client
        .create_index(CreateIndexRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH,
            kind: IndexKind::IdMap as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    let wrong_kind = coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[1].clone(),
                index_id: id_mapped,
            }),
            targets: vec![nodes[0].clone()],
            row_counts: Vec::new(),
        })
        .await
        .unwrap_err();
    assert!(
        wrong_kind
            .message()
            .starts_with("positional_index_required:"),
        "split said: {}",
        wrong_kind.message()
    );

    // The collection was never rebound by any of the three, so it still
    // serves what it served before.
    let listed = coordinator
        .list_nodes(turbovec_grpc::proto::ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(listed.servable);
    assert_eq!(listed.rows, ROWS as u64);
}

/// A node that has gone away fails the search rather than shortening it, and
/// the opt-in that allows a short result says so in the response.
#[tokio::test]
async fn an_unreachable_shard_fails_the_search_unless_partial_is_asked_for() {
    let nodes = [start_node().await, start_node().await];
    let pair = fit_pair(7);
    let (monolith_id, _corpus) = build_monolith(&nodes[0], &pair).await;
    let mut coordinator = start_coordinator(NodeTable::new(vec![ShardConfig::with_index(
        &nodes[0],
        &monolith_id,
    )]))
    .await;

    // Split across the live node and a port nothing is listening on. The
    // split itself reaches the dead address only on import, so build the
    // collection by hand instead.
    let dead = "http://127.0.0.1:1";
    coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[0].clone(),
                index_id: monolith_id.clone(),
            }),
            targets: vec![nodes[0].clone(), nodes[1].clone()],
            row_counts: Vec::new(),
        })
        .await
        .unwrap();
    let queries = Lcg(31).rows(2);
    let whole = distributed_ranking(&mut coordinator, &queries).await;

    // Now point the collection at one live shard and one that is not there.
    let mut with_dead = start_coordinator(NodeTable::new(vec![
        ShardConfig::new(dead),
        ShardConfig::with_index(&nodes[0], &monolith_id),
    ]))
    .await;
    let refused = with_dead
        .search(CollectionSearchRequest {
            queries: queries.clone(),
            k: K,
            allow_partial: false,
        })
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("node_unreachable:"),
        "search said: {}",
        refused.message()
    );

    // Even asked for explicitly, a partial search is still refused here: the
    // collection could not be bound at all, so there is no calibration to
    // merge the survivors under.
    let still_refused = with_dead
        .search(CollectionSearchRequest {
            queries: queries.clone(),
            k: K,
            allow_partial: true,
        })
        .await
        .unwrap_err();
    assert!(still_refused.message().starts_with("node_unreachable:"));

    // The healthy collection is unaffected by any of that.
    assert_eq!(whole.len(), 2);
}

/// The node-level guards Join leans on, checked directly: two blocks that
/// disagree are refused rather than concatenated into an index whose scores
/// would mean two different things.
#[tokio::test]
async fn import_rows_refuses_blocks_that_disagree() {
    let address = start_node().await;
    let mut client = node_client(&address).await;

    let pair = fit_pair(7);
    let index_id = create_index(&mut client, DIM).await;
    client
        .set_calibration(SetCalibrationRequest {
            index_id: index_id.clone(),
            tqplus_shift: pair.0.clone(),
            tqplus_scale: pair.1.clone(),
        })
        .await
        .unwrap();
    client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: index_id.clone(),
            dim: DIM as u32,
            vectors: Lcg(3).rows(64),
            ids: Vec::new(),
        }]))
        .await
        .unwrap();

    let block = client
        .export_rows(ExportRowsRequest {
            index_id: index_id.clone(),
            start: 0,
            count: 0,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(block.rows, 64);
    assert_eq!(block.labels, (0..64u64).collect::<Vec<_>>());
    assert_eq!(block.tqplus_shift, pair.0);

    // A second block under a different pair.
    let other = fit_pair(2);
    let mismatched = RowBlock {
        tqplus_shift: other.0,
        tqplus_scale: other.1,
        ..block.clone()
    };
    let refused = client
        .import_rows(ImportRowsRequest {
            blocks: vec![block.clone(), mismatched],
        })
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("mixed_calibration:"),
        "import said: {}",
        refused.message()
    );

    // A second block of a different width.
    let wider = RowBlock {
        dim: (DIM * 2) as u32,
        ..block.clone()
    };
    let refused = client
        .import_rows(ImportRowsRequest {
            blocks: vec![block.clone(), wider],
        })
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("dimension_mismatch:"),
        "import said: {}",
        refused.message()
    );

    // An imported index carries labels, and refuses to take further rows,
    // because there would be no id to give them.
    let imported = client
        .import_rows(ImportRowsRequest {
            blocks: vec![block],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(imported.info.unwrap().labelled);
    let refused = client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: imported.index_id.clone(),
            dim: DIM as u32,
            vectors: Lcg(4).rows(1),
            ids: Vec::new(),
        }]))
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("labelled_index_immutable:"),
        "add said: {}",
        refused.message()
    );
}

/// Committing a pair to an index that already holds rows is refused: those
/// rows were encoded under the pair they arrived with, and reinterpreting
/// their codes under another one would change every score they produce
/// without changing a byte of them.
#[tokio::test]
async fn set_calibration_refuses_a_populated_index() {
    let address = start_node().await;
    let pair = fit_pair(7);
    let (index_id, _corpus) = build_monolith(&address, &pair).await;

    let mut client = node_client(&address).await;
    let refused = client
        .set_calibration(SetCalibrationRequest {
            index_id,
            tqplus_shift: pair.0.clone(),
            tqplus_scale: pair.1.clone(),
        })
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("index_not_empty:"),
        "set_calibration said: {}",
        refused.message()
    );
}

/// A pair of the wrong length is refused before anything is committed.
#[tokio::test]
async fn set_calibration_refuses_a_pair_of_the_wrong_width() {
    let address = start_node().await;
    let mut client = node_client(&address).await;
    let index_id = create_index(&mut client, DIM).await;

    let refused = client
        .set_calibration(SetCalibrationRequest {
            index_id: index_id.clone(),
            tqplus_shift: vec![0.5; DIM - 8],
            tqplus_scale: vec![1.5; DIM - 8],
        })
        .await
        .unwrap_err();
    assert!(
        refused.message().starts_with("invalid_calibration:"),
        "set_calibration said: {}",
        refused.message()
    );

    // Nothing was committed, so the index is still the uncalibrated one.
    let held: Calibration = client
        .get_calibration(turbovec_grpc::proto::GetCalibrationRequest { index_id })
        .await
        .unwrap()
        .into_inner();
    assert!(held.tqplus_shift.is_empty());
}
