//! Integration tests for schema-bound collections behind the coordinator.
//!
//! The claim under test is the same one the plain distributed tests make,
//! extended to documents: a client of the coordinator's `SearchDocuments`
//! cannot tell a sharded document collection from a single schema-bound
//! index holding all the same documents — the same hits, the same ids,
//! the same scores to the bit, under the same CEL filter. And a collection
//! whose shards do not agree on one schema fingerprint is refused by name,
//! not searched.
//!
//! Everything runs against real servers over real sockets: each node
//! serves the query, admin and Documents services on its own ephemeral
//! loopback port, and the coordinator runs on another.

use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage as _, Value};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::documents_client::DocumentsClient;
use turbovec_grpc::proto::turbo_vec_admin_client::TurboVecAdminClient;
use turbovec_grpc::proto::{
    AddDocumentsRequest, BindSchemaRequest, CollectionSearchDocumentsRequest, CreateIndexRequest,
    IndexKind, ListNodesRequest, SchemaSource, SearchDocumentsRequest,
};
use turbovec_grpc::{
    CoordinatorService, DocumentsService, IndexStore, NodeTable, ServiceLimits, ShardConfig,
    TurboVecService,
};

/// The same annotated product type the single-node document tests use: an
/// explicit VECTOR hint with declared dims, a nested message, a
/// Timestamp, an enum, and a repeated scalar.
const ANNOTATED: &str = r#"
syntax = "proto3";
package test.v1;

import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";
import "google/protobuf/timestamp.proto";

message Product {
  string id = 1;
  string title = 2;
  string sku_code = 3;
  int64 price_cents = 4;
  bool in_stock = 5;
  Meta meta = 6;
  repeated float embedding = 7 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_VECTOR
    vector_dims: 8
  }];
  string scratch = 8 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_SKIP
  }];
  repeated string tags = 9;
  Status status = 10;
}

enum Status {
  STATUS_UNSPECIFIED = 0;
  STATUS_ACTIVE = 1;
  STATUS_DISCONTINUED = 2;
}

message Meta {
  string author = 1;
  google.protobuf.Timestamp created_at = 2;
}
"#;

/// A different type, so two shards can bind two genuinely different
/// fingerprints.
const OTHER: &str = r#"
syntax = "proto3";
package test.v1;

message Note {
  string id = 1;
  string body = 2;
  repeated float embedding = 3;
}
"#;

/// Compile one `.proto` source into a serialized FileDescriptorSet with
/// imports included, resolving the vendored hints proto from this
/// repository's own `proto/` directory.
fn compile(source: &str) -> Vec<u8> {
    let dir = std::env::temp_dir().join(format!("turbovec-dist-docs-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("test.proto");
    std::fs::write(&file, source).unwrap();
    let mut compiler =
        protox::Compiler::new([dir.as_path(), std::path::Path::new("proto")]).unwrap();
    compiler.include_imports(true);
    compiler.open_file(&file).unwrap();
    let bytes = compiler.encode_file_descriptor_set();
    std::fs::remove_dir_all(&dir).unwrap();
    bytes
}

/// Distinct, well-separated directions (one dominant coordinate each), so
/// nearest-neighbour identity is unambiguous even after quantization.
fn embedding(doc: usize) -> Vec<Value> {
    (0..8)
        .map(|i| Value::F32(if i == doc * 2 { 1.0 } else { 0.05 }))
        .collect()
}

/// Encode one fully populated test.v1.Product, nested Timestamp included.
fn product(descriptor_set: &[u8], doc: usize, price_cents: i64, author: &str) -> Vec<u8> {
    use prost::Message as _;
    let pool = DescriptorPool::decode(descriptor_set).unwrap();
    let descriptor = pool.get_message_by_name("test.v1.Product").unwrap();
    let mut m = DynamicMessage::new(descriptor);
    let pool = m.descriptor().parent_pool().clone();
    m.set_field_by_name("id", Value::String(format!("doc-{doc}")));
    m.set_field_by_name("title", Value::String(format!("product {doc}")));
    m.set_field_by_name("price_cents", Value::I64(price_cents));
    m.set_field_by_name("in_stock", Value::Bool(doc.is_multiple_of(2)));
    m.set_field_by_name("embedding", Value::List(embedding(doc)));
    m.set_field_by_name("tags", Value::List(vec![Value::String("tag".into())]));
    m.set_field_by_name("status", Value::EnumNumber(1));
    let mut timestamp = DynamicMessage::new(
        pool.get_message_by_name("google.protobuf.Timestamp")
            .unwrap(),
    );
    timestamp.set_field_by_name("seconds", Value::I64(1_600_000_000 + doc as i64));
    let mut meta = DynamicMessage::new(pool.get_message_by_name("test.v1.Meta").unwrap());
    meta.set_field_by_name("author", Value::String(author.to_string()));
    meta.set_field_by_name("created_at", Value::Message(timestamp));
    m.set_field_by_name("meta", Value::Message(meta));
    m.encode_to_vec()
}

/// Start one node serving the query, admin and Documents services on an
/// ephemeral loopback port; returns its address.
async fn start_node() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = std::sync::Arc::new(IndexStore::new());
    let service = TurboVecService::from_shared(std::sync::Arc::clone(&store));
    let documents = DocumentsService::new(store, ServiceLimits::default());
    let query = service.clone().into_query_server();
    let admin = service.into_admin_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(query)
            .add_service(admin)
            .add_service(documents.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    format!("http://{addr}")
}

async fn connect(address: &str) -> Channel {
    Endpoint::from_shared(address.to_string())
        .unwrap()
        .connect()
        .await
        .expect("server accepted the connection")
}

async fn start_coordinator(shards: Vec<ShardConfig>) -> CoordinatorClient<Channel> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = CoordinatorService::new(NodeTable::new(shards)).into_server();
    tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    CoordinatorClient::new(connect(&format!("http://{addr}")).await)
}

/// Bind the schema on one node and return the new index handle.
async fn bind_schema(address: &str, descriptor_set: &[u8], message_type: &str) -> String {
    let mut client = DocumentsClient::new(connect(address).await);
    client
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: descriptor_set.to_vec(),
                message_type: message_type.to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id
}

/// Stream the given documents into one schema-bound index.
async fn add_documents(address: &str, index_id: &str, documents: Vec<Vec<u8>>) {
    let mut client = DocumentsClient::new(connect(address).await);
    let request = AddDocumentsRequest {
        index_id: index_id.to_string(),
        documents,
    };
    client
        .add_documents(tokio_stream::iter(vec![request]))
        .await
        .unwrap();
}

/// The corpus: four products with unambiguous nearest-neighbour identity,
/// prices that straddle any threshold a filter picks, and two authors.
fn corpus(descriptor_set: &[u8]) -> Vec<Vec<u8>> {
    vec![
        product(descriptor_set, 0, 1_000, "kagome"),
        product(descriptor_set, 1, 2_000, "ryoko"),
        product(descriptor_set, 2, 3_000, "kagome"),
        product(descriptor_set, 3, 4_000, "ryoko"),
    ]
}

/// A query pointing straight at one document's dominant direction.
fn query(doc: usize) -> Vec<f32> {
    (0..8)
        .map(|i| if i == doc * 2 { 1.0 } else { 0.05 })
        .collect()
}

fn hits_of(
    response: &turbovec_grpc::proto::SearchDocumentsResponse,
    qi: usize,
) -> Vec<(String, f32)> {
    response.results[qi]
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.score))
        .collect()
}

fn collection_hits_of(
    response: &turbovec_grpc::proto::CollectionSearchDocumentsResponse,
    qi: usize,
) -> Vec<(String, f32)> {
    response.results[qi]
        .hits
        .iter()
        .map(|hit| (hit.id.clone(), hit.score))
        .collect()
}

#[tokio::test]
async fn a_sharded_document_collection_equals_the_monolithic_one_under_a_filter() {
    let descriptor_set = compile(ANNOTATED);
    let corpus = corpus(&descriptor_set);

    // Two shards holding two documents each, and one monolith holding all
    // four. Same schema, same bit width; encoding is a pure per-row
    // function, so no calibration step is needed for the shards to score
    // comparably.
    let shard_addresses = [start_node().await, start_node().await];
    let shard_a = bind_schema(&shard_addresses[0], &descriptor_set, "test.v1.Product").await;
    let shard_b = bind_schema(&shard_addresses[1], &descriptor_set, "test.v1.Product").await;
    add_documents(&shard_addresses[0], &shard_a, corpus[..2].to_vec()).await;
    add_documents(&shard_addresses[1], &shard_b, corpus[2..].to_vec()).await;

    let monolith_address = start_node().await;
    let monolith = bind_schema(&monolith_address, &descriptor_set, "test.v1.Product").await;
    add_documents(&monolith_address, &monolith, corpus.clone()).await;
    let mut monolith_client = DocumentsClient::new(connect(&monolith_address).await);

    let mut coordinator = start_coordinator(vec![
        ShardConfig::with_index(shard_addresses[0].clone(), shard_a.clone()),
        ShardConfig::with_index(shard_addresses[1].clone(), shard_b.clone()),
    ])
    .await;

    // The shard table shows the agreement it enforces: servable, every
    // shard carrying the same schema fingerprint.
    let listed = coordinator
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(listed.servable, "{}", listed.error);
    assert_eq!(listed.rows, 4);
    let fingerprints: Vec<&str> = listed
        .shards
        .iter()
        .map(|s| s.schema_fingerprint.as_str())
        .collect();
    assert_eq!(fingerprints.len(), 2);
    assert!(!fingerprints[0].is_empty());
    assert_eq!(fingerprints[0], fingerprints[1]);

    // Unfiltered: the collection's top-k is the monolith's, id for id and
    // score for score, with both shards contributing.
    let queries: Vec<f32> = query(0).into_iter().chain(query(3)).collect();
    let distributed = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: queries.clone(),
            k: 4,
            filter: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let monolithic = monolith_client
        .search_documents(SearchDocumentsRequest {
            index_id: monolith.clone(),
            queries: queries.clone(),
            k: 4,
            filter: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(distributed.matched, 4);
    assert_eq!(distributed.total, 4);
    for qi in 0..2 {
        assert_eq!(
            collection_hits_of(&distributed, qi),
            hits_of(&monolithic, qi)
        );
    }
    assert_eq!(collection_hits_of(&distributed, 0)[0].0, "doc-0");
    assert_eq!(collection_hits_of(&distributed, 1)[0].0, "doc-3");

    // Filtered: the same CEL expression means the same thing on every
    // shard, and the filtered collection top-k is the filtered monolithic
    // top-k. The filter excludes each query's nearest neighbour (doc-0
    // costs 1000, doc-3 is ryoko's), so exactness shows as the true next
    // best surfacing, not as a truncated list.
    let filter = r#"price_cents > 1500 && meta.author == "kagome""#;
    let distributed = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: queries.clone(),
            k: 4,
            filter: filter.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let monolithic = monolith_client
        .search_documents(SearchDocumentsRequest {
            index_id: monolith.clone(),
            queries: queries.clone(),
            k: 4,
            filter: filter.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    // Only doc-2 (3000, kagome) passes: matched counts the collection.
    assert_eq!(distributed.matched, 1);
    assert_eq!(distributed.total, 4);
    assert_eq!(monolithic.matched, 1);
    for qi in 0..2 {
        let hits = collection_hits_of(&distributed, qi);
        assert_eq!(hits, hits_of(&monolithic, qi));
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].0, "doc-2");
    }

    // A filter spanning both shards, still bit-equal to the monolith.
    let filter = "price_cents >= 2000";
    let distributed = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: queries.clone(),
            k: 4,
            filter: filter.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let monolithic = monolith_client
        .search_documents(SearchDocumentsRequest {
            index_id: monolith.clone(),
            queries: queries.clone(),
            k: 4,
            filter: filter.to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(distributed.matched, 3);
    for qi in 0..2 {
        assert_eq!(
            collection_hits_of(&distributed, qi),
            hits_of(&monolithic, qi)
        );
    }

    // A broken filter fails the collection search by the same wording the
    // node uses, with the shard named.
    let error = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: queries.clone(),
            k: 4,
            filter: "no_such_field > 1".to_string(),
        })
        .await
        .expect_err("an unplanned field should be refused");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error.message().contains("no_such_field"),
        "unexpected message: {}",
        error.message()
    );
}

#[tokio::test]
async fn shards_that_disagree_about_the_schema_are_refused_by_name() {
    let descriptor_set = compile(ANNOTATED);
    let other_set = compile(OTHER);

    // Two shards bound to two different message types: two fingerprints.
    let addresses = [start_node().await, start_node().await];
    let shard_a = bind_schema(&addresses[0], &descriptor_set, "test.v1.Product").await;
    let shard_b = bind_schema(&addresses[1], &other_set, "test.v1.Note").await;
    // Note has no dims hint, so its index is lazy: one document binds its
    // dim to 8 so the collection agrees on shape and disagrees only on
    // schema, which is the refusal under test.
    let note = {
        use prost::Message as _;
        let pool = DescriptorPool::decode(other_set.as_slice()).unwrap();
        let descriptor = pool.get_message_by_name("test.v1.Note").unwrap();
        let mut m = DynamicMessage::new(descriptor);
        m.set_field_by_name("id", Value::String("note-0".into()));
        m.set_field_by_name("embedding", Value::List(embedding(0)));
        m.encode_to_vec()
    };
    add_documents(&addresses[1], &shard_b, vec![note]).await;

    let mut coordinator = start_coordinator(vec![
        ShardConfig::with_index(addresses[0].clone(), shard_a.clone()),
        ShardConfig::with_index(addresses[1].clone(), shard_b.clone()),
    ])
    .await;
    let error = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: query(0),
            k: 1,
            filter: String::new(),
        })
        .await
        .expect_err("disagreeing schemas should refuse to bind");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().starts_with("mixed_schema: "),
        "unexpected message: {}",
        error.message()
    );

    // ListNodes stays usable while the collection is refused: it shows
    // both fingerprints and names the disagreement.
    let listed = coordinator
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(!listed.servable);
    assert!(
        listed.error.starts_with("mixed_schema: "),
        "unexpected error: {}",
        listed.error
    );
    assert_ne!(
        listed.shards[0].schema_fingerprint,
        listed.shards[1].schema_fingerprint
    );
}

#[tokio::test]
async fn a_schema_bound_shard_next_to_a_plain_one_is_refused_by_name() {
    let descriptor_set = compile(ANNOTATED);
    let addresses = [start_node().await, start_node().await];
    let shard_a = bind_schema(&addresses[0], &descriptor_set, "test.v1.Product").await;

    // The second shard is a plain positional index of the same shape:
    // same dim, same bit width, agreeing (empty) calibration. Only the
    // schema disagrees.
    let mut admin = TurboVecAdminClient::new(connect(&addresses[1]).await);
    let plain = admin
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

    let mut coordinator = start_coordinator(vec![
        ShardConfig::with_index(addresses[0].clone(), shard_a.clone()),
        ShardConfig::with_index(addresses[1].clone(), plain.clone()),
    ])
    .await;
    let error = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: query(0),
            k: 1,
            filter: String::new(),
        })
        .await
        .expect_err("a half-bound collection should refuse to bind");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().starts_with("mixed_schema: "),
        "unexpected message: {}",
        error.message()
    );
}

#[tokio::test]
async fn search_documents_requires_a_schema_bound_collection() {
    // Two plain positional shards agree on everything a vector collection
    // needs, so the collection binds — and SearchDocuments still refuses,
    // because there is no schema to spell a filter against.
    let addresses = [start_node().await, start_node().await];
    let mut shards = Vec::new();
    for address in &addresses {
        let mut admin = TurboVecAdminClient::new(connect(address).await);
        let index_id = admin
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
        shards.push(ShardConfig::with_index(address.clone(), index_id));
    }
    let mut coordinator = start_coordinator(shards).await;
    let error = coordinator
        .search_documents(CollectionSearchDocumentsRequest {
            queries: query(0),
            k: 1,
            filter: String::new(),
        })
        .await
        .expect_err("a plain collection has no schema to filter against");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().starts_with("schema_required: "),
        "unexpected message: {}",
        error.message()
    );
}
