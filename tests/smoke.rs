//! End-to-end smoke test for the gRPC surface.
//!
//! Stands up a real tonic server on an ephemeral port and drives it through
//! the generated client. Exercises:
//!   - create an id-mapped index, then client-streaming `add`.
//!   - unary `search`, and filtered `search` against an allowlist.
//!   - server-streaming `search_stream` yielding one result per query.
//!   - `get_index_info`, `remove` by id, and `drop_index`.

use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddRequest, CreateIndexRequest, DropIndexRequest, GetIndexInfoRequest, IndexKind, SearchRequest,
};
use turbovec_grpc::{IndexStore, TurboVecService};

/// Start a server on a loopback ephemeral port and return a connected client.
async fn start() -> TurboVecClient<tonic::transport::Channel> {
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
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    TurboVecClient::new(channel)
}

/// Deterministic 8-dim vector, so the test needs no RNG dependency.
fn vector(seed: usize) -> Vec<f32> {
    (0..8).map(|i| ((i + seed) as f32) * 0.13 - 1.0).collect()
}

#[tokio::test]
async fn create_add_search_filter_stream_idmap() {
    let mut client = start().await;

    let created = client
        .create_index(CreateIndexRequest {
            dim: 8,
            bit_width: 4,
            kind: IndexKind::IdMap as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner();
    let id = created.index_id;
    assert_eq!(created.info.unwrap().dim, 8);

    // Client-streaming add of 64 vectors with external ids 0..64.
    let n = 64usize;
    let mut vectors = Vec::with_capacity(n * 8);
    let mut ids = Vec::with_capacity(n);
    for s in 0..n {
        vectors.extend(vector(s));
        ids.push(s as u64);
    }
    let add = client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: id.clone(),
            dim: 8,
            vectors,
            ids: ids.clone(),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(add.added, n as u64);
    assert_eq!(add.len, n as u64);

    // Unary search: one query, k=5, results drawn from the id set.
    let query = vector(5);
    let res = client
        .search(SearchRequest {
            index_id: id.clone(),
            queries: query.clone(),
            k: 5,
            allowlist: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(res.results.len(), 1);
    assert_eq!(res.results[0].ids.len(), 5);
    for got in &res.results[0].ids {
        assert!(ids.contains(got), "returned id {got} not in the index");
    }

    // Filtered search: restrict to ids {20, 30, 40}; results must be a subset.
    let allowed = vec![20u64, 30, 40];
    let filtered = client
        .search(SearchRequest {
            index_id: id.clone(),
            queries: query.clone(),
            k: 5,
            allowlist: allowed.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(filtered.results[0].ids.len(), allowed.len());
    for got in &filtered.results[0].ids {
        assert!(
            allowed.contains(got),
            "filtered result {got} outside allowlist"
        );
    }

    // Streaming search: two queries yield two streamed QueryResults, in order.
    let two = [vector(5), vector(50)].concat();
    let mut stream = client
        .search_stream(SearchRequest {
            index_id: id.clone(),
            queries: two,
            k: 3,
            allowlist: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    let mut streamed = 0;
    while let Some(qr) = stream.message().await.unwrap() {
        assert_eq!(qr.ids.len(), 3);
        streamed += 1;
    }
    assert_eq!(streamed, 2);

    // Info reflects the adds.
    let info = client
        .get_index_info(GetIndexInfoRequest {
            index_id: id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.len, n as u64);

    // Remove one id, then it is gone.
    let removed = client
        .remove(turbovec_grpc::proto::RemoveRequest {
            index_id: id.clone(),
            id: 20,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(removed.removed);

    // Drop the handle.
    let dropped = client
        .drop_index(DropIndexRequest { index_id: id })
        .await
        .unwrap()
        .into_inner();
    assert!(dropped.dropped);
}

/// Searching a lazy index whose dim was never bound by an Add fails with
/// FAILED_PRECONDITION on both the unary and the streaming path: without a
/// dim the query buffer cannot be chunked, so there is no result shape.
#[tokio::test]
async fn search_on_unbound_lazy_index_is_failed_precondition() {
    let mut client = start().await;

    let created = client
        .create_index(CreateIndexRequest {
            dim: 0,
            bit_width: 4,
            kind: IndexKind::IdMap as i32,
            lazy: true,
        })
        .await
        .unwrap()
        .into_inner();
    let id = created.index_id;

    let request = SearchRequest {
        index_id: id.clone(),
        queries: vector(0),
        k: 5,
        allowlist: vec![],
    };
    let err = client.search(request.clone()).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let err = client.search_stream(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    client
        .drop_index(DropIndexRequest { index_id: id })
        .await
        .unwrap();
}
