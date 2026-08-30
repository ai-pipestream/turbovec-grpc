//! End-to-end smoke test for the gRPC surface.
//!
//! Stands up a real tonic server on an ephemeral port and drives it through
//! the generated client. Exercises:
//!   - create an id-mapped index, then client-streaming `add`.
//!   - unary `search`, and filtered `search` against an allowlist.
//!   - server-streaming `search_stream` yielding one result per query.
//!   - `get_index_info`, `remove` by id, and `drop_index`.

use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Endpoint, Server};
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    stream_search_request, stream_search_response, AddRequest, CreateIndexRequest,
    DropIndexRequest, FloorUpdate, FlushRequest, GetIndexInfoRequest, IndexKind, SearchRequest,
    StartStreamSearch, StreamSearchRequest,
};
use turbovec_grpc::{IndexStore, TurboVecService};

/// Start a server on a loopback ephemeral port and return a connected client.
async fn start() -> TurboVecClient<tonic::transport::Channel> {
    start_with_store(IndexStore::new()).await
}

/// Start a server around an explicitly configured registry.
async fn start_with_store(store: IndexStore) -> TurboVecClient<tonic::transport::Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = TurboVecService::new(store);
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
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    TurboVecClient::new(channel)
}

#[tokio::test]
async fn flush_rpc_activates_restart_safe_generations() {
    let root = std::env::temp_dir().join(format!("turbovec-flush-{}", uuid::Uuid::new_v4()));
    let mut client = start_with_store(IndexStore::open(&root).unwrap()).await;
    let id = client
        .create_index(CreateIndexRequest {
            dim: 8,
            bit_width: 4,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: id.clone(),
            dim: 8,
            vectors: vector(1),
            ids: Vec::new(),
            ..Default::default()
        }]))
        .await
        .unwrap();
    let first = client
        .flush(FlushRequest {
            index_id: id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.generation, 1);
    assert_eq!(
        client
            .get_index_info(GetIndexInfoRequest {
                index_id: id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .generation,
        1
    );

    client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: id.clone(),
            dim: 8,
            vectors: vector(2),
            ids: Vec::new(),
            ..Default::default()
        }]))
        .await
        .unwrap();
    assert_eq!(
        client
            .flush(FlushRequest {
                index_id: id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .generation,
        2
    );

    let restored = IndexStore::open(&root).unwrap();
    assert_eq!(restored.generation(&id), Some(2));
    assert_eq!(restored.get(&id).unwrap().read().unwrap().len(), 2);
    assert!(
        client
            .drop_index(DropIndexRequest {
                index_id: id.clone(),
            })
            .await
            .unwrap()
            .into_inner()
            .dropped
    );
    assert!(IndexStore::open(&root).unwrap().get(&id).is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn retry_safe_ingest_replays_after_restart() {
    let root = std::env::temp_dir().join(format!("turbovec-ingest-{}", uuid::Uuid::new_v4()));
    let mut client = start_with_store(IndexStore::open(&root).unwrap()).await;
    let id = client
        .create_index(CreateIndexRequest {
            dim: 8,
            bit_width: 4,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    let operation = AddRequest {
        index_id: id.clone(),
        dim: 8,
        vectors: vector(7),
        ids: Vec::new(),
        operation_id: "ingest-0001".to_string(),
        expected_len: Some(0),
        expected_rows: 1,
    };
    let first = client
        .add(tokio_stream::iter(vec![operation.clone()]))
        .await
        .unwrap()
        .into_inner();
    assert!(!first.replayed);
    assert_eq!(first.len, 1);
    assert_eq!(first.generation, 1);

    let mut restarted = start_with_store(IndexStore::open(&root).unwrap()).await;
    let replay = restarted
        .add(tokio_stream::iter(vec![operation]))
        .await
        .unwrap()
        .into_inner();
    assert!(replay.replayed);
    assert_eq!(replay.len, 1);
    assert_eq!(
        restarted
            .get_index_info(GetIndexInfoRequest { index_id: id })
            .await
            .unwrap()
            .into_inner()
            .len,
        1
    );
    std::fs::remove_dir_all(root).unwrap();
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
            ..Default::default()
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

    // The collaborative distributed scan is positional-only. An id-mapped
    // handle is refused explicitly rather than silently losing its id map.
    let mut distributed_stream = client
        .stream_search(tokio_stream::iter(vec![StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                index_id: id.clone(),
                vector: query,
                initial_floor: None,
                request_id: "id-map-refusal".to_string(),
                ..Default::default()
            })),
        }]))
        .await
        .unwrap()
        .into_inner();
    let error = distributed_stream.message().await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("positional_index_required"));

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

/// The collaborative node stream is an exact candidate source under both a
/// seeded floor and a floor raised after the scan has started. The node does
/// not choose k: retaining the best k emitted candidates must reproduce its
/// ordinary unary top-k bit for bit.
#[tokio::test]
async fn live_floor_stream_is_exact_and_reports_completion() {
    const ROWS: usize = 100_000;
    const K: usize = 10;

    let mut client = start().await;
    let created = client
        .create_index(CreateIndexRequest {
            dim: 8,
            bit_width: 4,
            kind: IndexKind::Positional as i32,
            lazy: false,
        })
        .await
        .unwrap()
        .into_inner();
    let index_id = created.index_id;

    // Deterministic pseudo-random rows avoid the large score-tie groups a
    // simple arithmetic sequence produces after quantization.
    let mut state = 0x5eed_u32;
    let vectors: Vec<f32> = (0..ROWS * 8)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            ((state >> 8) as f32 / 8_388_608.0) - 1.0
        })
        .collect();
    client
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: index_id.clone(),
            dim: 8,
            vectors: vectors.clone(),
            ids: Vec::new(),
            ..Default::default()
        }]))
        .await
        .unwrap();
    let query = vectors[17 * 8..18 * 8].to_vec();
    let expected = client
        .search(SearchRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: K as u32,
            allowlist: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner()
        .results
        .remove(0);
    let safe_floor = *expected.scores.last().unwrap();

    // A starting floor should suppress almost all candidate traffic while
    // preserving the exact top-k, and the terminal summary certifies that the
    // whole index was scanned.
    let seeded = StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            index_id: index_id.clone(),
            vector: query.clone(),
            initial_floor: Some(safe_floor),
            request_id: "seeded-floor".to_string(),
            ..Default::default()
        })),
    };
    let mut seeded_stream = client
        .stream_search(tokio_stream::iter(vec![seeded]))
        .await
        .unwrap()
        .into_inner();
    let mut seeded_candidates = Vec::new();
    let mut seeded_summary = None;
    while let Some(response) = seeded_stream.message().await.unwrap() {
        match response.payload.unwrap() {
            stream_search_response::Payload::Batch(batch) => {
                assert_eq!(batch.scores.len(), batch.slots.len());
                seeded_candidates.extend(batch.scores.into_iter().zip(batch.slots));
            }
            stream_search_response::Payload::Summary(summary) => {
                assert!(seeded_summary.replace(summary).is_none());
            }
        }
    }
    let summary = seeded_summary.expect("stream ended with a completion summary");
    assert!(summary.completed);
    assert_eq!(summary.emitted as usize, seeded_candidates.len());
    assert!(
        summary.blocks_scanned > 1,
        "test corpus must span scan chunks"
    );
    assert!(
        seeded_candidates.len() < ROWS / 100,
        "a top-k floor should suppress nearly all candidate emissions"
    );
    seeded_candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    seeded_candidates.truncate(K);
    let expected_pairs: Vec<(u32, u64)> = expected
        .scores
        .iter()
        .zip(&expected.ids)
        .map(|(&score, &slot)| (score.to_bits(), slot))
        .collect();
    let seeded_pairs: Vec<(u32, u64)> = seeded_candidates
        .iter()
        .map(|&(score, slot)| (score.to_bits(), slot))
        .collect();
    assert_eq!(seeded_pairs, expected_pairs);

    // Raise the floor after the first unseeded batch. A sufficiently large
    // corpus keeps the stream in flight, and the node must apply the update at
    // a later chunk boundary without changing the final top-k.
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(4);
    request_tx
        .send(StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                index_id: index_id.clone(),
                vector: query.clone(),
                initial_floor: None,
                request_id: "raised-floor".to_string(),
                ..Default::default()
            })),
        })
        .await
        .unwrap();
    let mut live_stream = client
        .stream_search(ReceiverStream::new(request_rx))
        .await
        .unwrap()
        .into_inner();
    let first = live_stream.message().await.unwrap().unwrap();
    let first_batch = match first.payload.unwrap() {
        stream_search_response::Payload::Batch(batch) => batch,
        stream_search_response::Payload::Summary(_) => panic!("large scan ended before a batch"),
    };
    let mut live_candidates: Vec<(f32, u64)> = first_batch
        .scores
        .into_iter()
        .zip(first_batch.slots)
        .collect();
    request_tx
        .send(StreamSearchRequest {
            payload: Some(stream_search_request::Payload::FloorUpdate(FloorUpdate {
                floor: safe_floor,
            })),
        })
        .await
        .unwrap();
    drop(request_tx);
    let mut live_summary = None;
    while let Some(response) = live_stream.message().await.unwrap() {
        match response.payload.unwrap() {
            stream_search_response::Payload::Batch(batch) => {
                live_candidates.extend(batch.scores.into_iter().zip(batch.slots));
            }
            stream_search_response::Payload::Summary(summary) => {
                assert!(live_summary.replace(summary).is_none());
            }
        }
    }
    let summary = live_summary.expect("live stream ended with a completion summary");
    assert!(summary.completed);
    assert!(
        summary.floor_raises_applied > 0,
        "the mid-scan floor update must bind at a later chunk"
    );
    live_candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    live_candidates.truncate(K);
    let live_pairs: Vec<(u32, u64)> = live_candidates
        .iter()
        .map(|&(score, slot)| (score.to_bits(), slot))
        .collect();
    assert_eq!(live_pairs, expected_pairs);

    // Cancellation is polled at chunk boundaries even when an infinite floor
    // suppresses every candidate batch.
    let cancel_frames = vec![
        StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
                index_id: index_id.clone(),
                vector: query.clone(),
                initial_floor: Some(f32::INFINITY),
                request_id: "cancelled-empty-stream".to_string(),
                ..Default::default()
            })),
        },
        StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Stop(
                turbovec_grpc::proto::StopStreamSearch {},
            )),
        },
    ];
    let mut cancelled = client
        .stream_search(tokio_stream::iter(cancel_frames))
        .await
        .unwrap()
        .into_inner();
    let response = cancelled.message().await.unwrap().unwrap();
    let summary = match response.payload.unwrap() {
        stream_search_response::Payload::Batch(_) => {
            panic!("an infinite floor must suppress every candidate batch")
        }
        stream_search_response::Payload::Summary(summary) => summary,
    };
    assert!(!summary.completed);
}
