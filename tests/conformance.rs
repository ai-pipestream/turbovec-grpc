//! Contract tests between the in-process `turbovec` API and its gRPC facade.
//!
//! The facade is allowed to add network failure semantics, but a successful
//! create/add/search sequence must keep the local engine's score bits and
//! winning rows. The same corpus is checked through a direct index, one gRPC
//! node, a one-shard coordinator, and a coordinator after an encoded-row split.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use turbovec::TurboQuantIndex;
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddRequest, CollectionSearchRequest, CreateIndexRequest, IndexKind, SearchRequest,
    SetCalibrationRequest, ShardRef, SplitRequest,
};
use turbovec_grpc::{CoordinatorService, IndexStore, NodeTable, ShardConfig, TurboVecService};

const DIM: usize = 64;
const BIT_WIDTH: usize = 4;
const ROWS: usize = 257;
const K: usize = 17;

type Ranking = Vec<(u32, u64)>;

struct Lcg(u32);

impl Lcg {
    fn next_f32(&mut self) -> f32 {
        self.0 = self.0.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        ((self.0 >> 8) as f32 / 8_388_608.0) - 1.0
    }

    fn vectors(&mut self, rows: usize) -> Vec<f32> {
        (0..rows * DIM).map(|_| self.next_f32()).collect()
    }
}

async fn connect(address: &str) -> Channel {
    Endpoint::from_shared(address.to_string())
        .unwrap()
        .connect()
        .await
        .expect("server accepted the connection")
}

async fn start_node() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
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
    format!("http://{address}")
}

async fn node_client(address: &str) -> TurboVecClient<Channel> {
    TurboVecClient::new(connect(address).await)
}

async fn start_coordinator(table: NodeTable) -> CoordinatorClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let service = CoordinatorService::new(table).into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    CoordinatorClient::new(connect(&format!("http://{address}")).await)
}

fn fit_pair() -> (Vec<f32>, Vec<f32>) {
    let sample = Lcg(7).vectors(turbovec::MIN_CALIBRATION_ROWS.max(512));
    let mut fitted = TurboQuantIndex::new(DIM, BIT_WIDTH).unwrap();
    fitted.calibrate_2d(&sample, DIM).unwrap();
    (
        fitted.tqplus_shift().to_vec(),
        fitted.tqplus_scale().to_vec(),
    )
}

fn direct_rankings(
    corpus: &[f32],
    queries: &[f32],
    pair: &(Vec<f32>, Vec<f32>),
    allowlist: &[u64],
) -> Vec<Ranking> {
    let mut index = TurboQuantIndex::from_parts(
        Some(DIM),
        BIT_WIDTH,
        0,
        Vec::new(),
        Vec::new(),
        pair.0.clone(),
        pair.1.clone(),
    )
    .unwrap();
    index.add(corpus);
    let mask = (!allowlist.is_empty()).then(|| {
        let mut mask = vec![false; ROWS];
        for &slot in allowlist {
            mask[slot as usize] = true;
        }
        mask
    });
    let results = match &mask {
        Some(mask) => index.search_with_mask(queries, K, Some(mask)),
        None => index.search(queries, K),
    };
    (0..results.nq)
        .map(|query| {
            results
                .scores_for_query(query)
                .iter()
                .zip(results.indices_for_query(query))
                .map(|(score, slot)| (score.to_bits(), *slot as u64))
                .collect()
        })
        .collect()
}

async fn node_rankings(
    client: &mut TurboVecClient<Channel>,
    index_id: &str,
    queries: &[f32],
    allowlist: &[u64],
) -> Vec<Ranking> {
    client
        .search(SearchRequest {
            index_id: index_id.to_string(),
            queries: queries.to_vec(),
            k: K as u32,
            allowlist: allowlist.to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .results
        .into_iter()
        .map(|result| {
            result
                .scores
                .iter()
                .zip(result.ids)
                .map(|(score, slot)| (score.to_bits(), slot))
                .collect()
        })
        .collect()
}

async fn collection_rankings(
    client: &mut CoordinatorClient<Channel>,
    queries: &[f32],
) -> Vec<Ranking> {
    client
        .search(CollectionSearchRequest {
            queries: queries.to_vec(),
            k: K as u32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .results
        .into_iter()
        .map(|result| {
            result
                .neighbours
                .into_iter()
                .map(|hit| (hit.score.to_bits(), hit.label.unwrap_or(hit.slot)))
                .collect()
        })
        .collect()
}

fn assert_equivalent(expected: &[Ranking], actual: &[Ranking], surface: &str) {
    assert_eq!(expected.len(), actual.len(), "{surface}: query count");
    for (query, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(
            expected.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            actual.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            "{surface}: query {query} score bits"
        );
        let mut expected_rows = expected.iter().map(|entry| entry.1).collect::<Vec<_>>();
        let mut actual_rows = actual.iter().map(|entry| entry.1).collect::<Vec<_>>();
        expected_rows.sort_unstable();
        actual_rows.sort_unstable();
        assert_eq!(
            expected_rows, actual_rows,
            "{surface}: query {query} winning rows"
        );
    }
}

#[tokio::test]
async fn local_node_and_sharded_coordinator_share_one_search_contract() {
    let corpus = Lcg(99).vectors(ROWS);
    let queries = Lcg(4_242).vectors(5);
    let pair = fit_pair();
    let expected = direct_rankings(&corpus, &queries, &pair, &[]);
    let allowlist = vec![1, 3, 8, 13, 21, 34, 55, 89, 144, 233];
    let expected_filtered = direct_rankings(&corpus, &queries, &pair, &allowlist);

    let nodes = [start_node().await, start_node().await];
    let mut node = node_client(&nodes[0]).await;
    let index_id = node
        .create_index(CreateIndexRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    node.set_calibration(SetCalibrationRequest {
        index_id: index_id.clone(),
        tqplus_shift: pair.0,
        tqplus_scale: pair.1,
    })
    .await
    .unwrap();
    node.add(tokio_stream::iter([AddRequest {
        index_id: index_id.clone(),
        dim: DIM as u32,
        vectors: corpus,
        ids: Vec::new(),
        ..Default::default()
    }]))
    .await
    .unwrap();

    assert_equivalent(
        &expected,
        &node_rankings(&mut node, &index_id, &queries, &[]).await,
        "one gRPC node",
    );
    assert_equivalent(
        &expected_filtered,
        &node_rankings(&mut node, &index_id, &queries, &allowlist).await,
        "one gRPC node with an allowlist",
    );

    let table = NodeTable::new(vec![ShardConfig::with_index(&nodes[0], &index_id)]);
    let mut coordinator = start_coordinator(table).await;
    assert_equivalent(
        &expected,
        &collection_rankings(&mut coordinator, &queries).await,
        "one-shard coordinator",
    );

    coordinator
        .split(SplitRequest {
            source: Some(ShardRef {
                address: nodes[0].clone(),
                index_id,
            }),
            targets: nodes.to_vec(),
            row_counts: vec![103, (ROWS - 103) as u64],
        })
        .await
        .unwrap();
    assert_equivalent(
        &expected,
        &collection_rankings(&mut coordinator, &queries).await,
        "two-shard coordinator",
    );
}
