//! Tests for the protobuf-first schema layer and the Documents service.
//!
//! Test schemas are compiled from `.proto` source at test time with protox
//! (a pure-Rust protoc), against the same vendored hints file the server
//! reads, so what is exercised here is exactly what a client toolchain
//! produces: a serialized `FileDescriptorSet` with imports included.

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use turbovec_grpc::proto::documents_client::DocumentsClient;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddDocumentsRequest, BindSchemaRequest, FieldKind, FieldRole, FlushRequest, GetSchemaRequest,
    PlanSchemaRequest, SchemaSource, SearchRequest,
};
use turbovec_grpc::schema::{hash_string_id, BoundSchema};
use turbovec_grpc::{DocumentsService, IndexStore, ServiceLimits, TurboVecService};

/// A product type exercising hints and inference together: an explicit
/// VECTOR hint with declared dims, an explicit SKIP, a nested message that
/// expands into dotted paths, a Timestamp leaf, and inferred keyword/text
/// splits.
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
}

message Meta {
  string author = 1;
  google.protobuf.Timestamp created_at = 2;
}
"#;

/// The same shape with no hints anywhere: the id comes from the "id"
/// fallback and the vector from the single vector-shaped repeated float.
const UNANNOTATED: &str = r#"
syntax = "proto3";
package test.v1;

message Note {
  string id = 1;
  string body = 2;
  repeated float embedding = 3;
}
"#;

/// Compile one `.proto` source into a serialized FileDescriptorSet with
/// every import included, resolving the vendored hints proto from this
/// repository's own `proto/` directory.
///
/// Uses `protox::Compiler::encode_file_descriptor_set`, not the convenience
/// `protox::compile`: the latter returns a prost-types struct, and prost
/// drops extension fields (the custom options) when re-encoding it. The
/// encoded bytes are what a real protoc `--descriptor_set_out` carries.
fn compile(source: &str) -> Vec<u8> {
    let dir = std::env::temp_dir().join(format!("turbovec-schema-test-{}", uuid::Uuid::new_v4()));
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

fn derive(source: &str, message_type: &str) -> BoundSchema {
    BoundSchema::derive(&compile(source), message_type).unwrap()
}

fn derive_err(source: &str, message_type: &str) -> String {
    BoundSchema::derive(&compile(source), message_type)
        .expect_err("derivation should fail")
        .to_string()
}

/// Encode one test.v1 document dynamically, the way any client would.
fn document(
    descriptor_set: &[u8],
    message_type: &str,
    set: impl Fn(&mut DynamicMessage),
) -> Vec<u8> {
    let pool = DescriptorPool::decode(descriptor_set).unwrap();
    let descriptor = pool.get_message_by_name(message_type).unwrap();
    let mut message = DynamicMessage::new(descriptor);
    set(&mut message);
    message.encode_to_vec()
}

/// Distinct, well-separated directions (one dominant coordinate each), so
/// nearest-neighbour identity is unambiguous even after quantization.
fn embedding(doc: usize) -> Vec<Value> {
    (0..8)
        .map(|i| Value::F32(if i == doc * 2 { 1.0 } else { 0.05 }))
        .collect()
}

#[test]
fn derives_a_deterministic_plan_with_hints_and_inference() {
    let bound = derive(ANNOTATED, "test.v1.Product");
    let schema = &bound.schema;
    assert_eq!(schema.message_type, "test.v1.Product");
    assert_eq!(schema.vector_path, "embedding");
    assert_eq!(schema.doc_id_path, "id");
    assert_eq!(schema.dim, 8);

    let summary: Vec<(&str, &str, FieldKind, FieldRole)> = schema
        .fields
        .iter()
        .map(|f| {
            (
                f.path.as_str(),
                f.name.as_str(),
                FieldKind::try_from(f.kind).unwrap(),
                FieldRole::try_from(f.role).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            ("id", "id", FieldKind::Keyword, FieldRole::DocId),
            ("title", "title", FieldKind::Text, FieldRole::None),
            ("sku_code", "sku_code", FieldKind::Keyword, FieldRole::None),
            (
                "price_cents",
                "price_cents",
                FieldKind::Int64,
                FieldRole::None
            ),
            ("in_stock", "in_stock", FieldKind::Boolean, FieldRole::None),
            (
                "meta.author",
                "meta_author",
                FieldKind::Text,
                FieldRole::None
            ),
            (
                "meta.created_at",
                "meta_created_at",
                FieldKind::Date,
                FieldRole::None
            ),
            ("embedding", "embedding", FieldKind::Vector, FieldRole::None),
            // scratch is hinted SKIP and does not appear.
        ]
    );

    // The fingerprint is a function of the plan: stable across derivations,
    // 64 hex chars, and different for a different schema.
    assert_eq!(schema.fingerprint.len(), 64);
    assert!(schema.fingerprint.chars().all(|c| c.is_ascii_hexdigit()));
    let again = derive(ANNOTATED, "test.v1.Product");
    assert_eq!(schema.fingerprint, again.schema.fingerprint);
    let other = derive(UNANNOTATED, "test.v1.Note");
    assert_ne!(schema.fingerprint, other.schema.fingerprint);
}

#[test]
fn an_unannotated_proto_indexes_when_nothing_is_ambiguous() {
    let bound = derive(UNANNOTATED, "test.v1.Note");
    let schema = &bound.schema;
    assert_eq!(schema.vector_path, "embedding");
    assert_eq!(schema.doc_id_path, "id");
    assert_eq!(
        schema.dim, 0,
        "no declared dims: bound by the first document"
    );
    let embedding = schema
        .fields
        .iter()
        .find(|f| f.path == "embedding")
        .unwrap();
    assert_eq!(embedding.kind, FieldKind::Vector as i32);
    let id = schema.fields.iter().find(|f| f.path == "id").unwrap();
    assert_eq!(id.role, FieldRole::DocId as i32);
}

#[test]
fn ambiguity_and_missing_identities_fail_by_name() {
    let two_vectors = r#"
        syntax = "proto3";
        package test.v1;
        message Doc {
          string id = 1;
          repeated float title_embedding = 2;
          repeated float body_embedding = 3;
        }
    "#;
    let error = derive_err(two_vectors, "test.v1.Doc");
    assert!(
        error.contains("title_embedding") && error.contains("body_embedding"),
        "ambiguity must name the candidates: {error}"
    );
    assert!(
        error.contains("INDEX_FIELD_TYPE_VECTOR"),
        "and the fix: {error}"
    );

    let no_vector = r#"
        syntax = "proto3";
        package test.v1;
        message Doc {
          string id = 1;
          string body = 2;
        }
    "#;
    let error = derive_err(no_vector, "test.v1.Doc");
    assert!(error.contains("no vector field"), "{error}");

    let no_id = r#"
        syntax = "proto3";
        package test.v1;
        message Doc {
          string slug = 1;
          repeated float embedding = 2;
        }
    "#;
    let error = derive_err(no_id, "test.v1.Doc");
    assert!(error.contains("no document id field"), "{error}");
    assert!(error.contains("BLOCK_ROLE_DOC_ID"), "and the fix: {error}");

    let repeated_doc_id = r#"
        syntax = "proto3";
        package test.v1;
        import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";
        message Doc {
          repeated string id = 1 [(ai.pipestream.proto.index.hints.v1.index) = {
            block_role: BLOCK_ROLE_DOC_ID
          }];
          repeated float embedding = 2;
        }
    "#;
    let error = derive_err(repeated_doc_id, "test.v1.Doc");
    assert!(error.contains("singular"), "{error}");

    let unknown_type = derive_err(UNANNOTATED, "test.v1.Missing");
    assert!(
        unknown_type.contains("test.v1.Note"),
        "an unknown type names what is present: {unknown_type}"
    );
}

#[test]
fn string_ids_reduce_to_the_documented_hash() {
    // Known answers computed independently: first 8 bytes of SHA-256 over
    // the UTF-8 bytes, big-endian. This is the wire contract clients rely
    // on to predict their documents' labels.
    assert_eq!(hash_string_id("doc-1"), 13_478_797_910_862_173_401);
    assert_eq!(
        hash_string_id("courtlistener/opinion/12345#0"),
        3_258_657_104_408_055_717
    );
}

#[test]
fn extraction_reads_ids_and_vectors_and_fails_loud() {
    let descriptor_set = compile(ANNOTATED);
    let bound = BoundSchema::derive(&descriptor_set, "test.v1.Product").unwrap();

    let good = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("id", Value::String("doc-1".into()));
        m.set_field_by_name("embedding", Value::List(embedding(0)));
    });
    let (label, vector) = bound.extract(&good).unwrap();
    assert_eq!(label, hash_string_id("doc-1"));
    assert_eq!(vector.len(), 8);

    let missing_id = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("embedding", Value::List(embedding(0)));
    });
    let error = bound.extract(&missing_id).unwrap_err().to_string();
    assert!(error.contains("id") && error.contains("empty"), "{error}");

    let wrong_dim = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("id", Value::String("doc-2".into()));
        m.set_field_by_name("embedding", Value::List(vec![Value::F32(1.0); 5]));
    });
    let error = bound.extract(&wrong_dim).unwrap_err().to_string();
    assert!(error.contains("5") && error.contains("8"), "{error}");

    let no_vector = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("id", Value::String("doc-3".into()));
    });
    let error = bound.extract(&no_vector).unwrap_err().to_string();
    assert!(error.contains("empty vector"), "{error}");
}

/// Start a node with the vector services and the Documents service on one
/// shared store, returning clients for both plus the store's data root.
async fn start(
    root: &std::path::Path,
) -> (
    DocumentsClient<tonic::transport::Channel>,
    TurboVecClient<tonic::transport::Channel>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = std::sync::Arc::new(IndexStore::open(root).unwrap());
    let service = TurboVecService::from_shared(std::sync::Arc::clone(&store));
    let documents = DocumentsService::new(store, ServiceLimits::default());
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_server())
            .add_service(documents.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    (
        DocumentsClient::new(channel.clone()),
        TurboVecClient::new(channel),
    )
}

#[tokio::test]
async fn documents_round_trip_over_grpc_and_survive_restart() {
    let root = std::env::temp_dir().join(format!("turbovec-docs-{}", uuid::Uuid::new_v4()));
    let (mut documents, mut vectors) = start(&root).await;
    let descriptor_set = compile(ANNOTATED);
    let source = SchemaSource {
        descriptor_set: descriptor_set.clone(),
        message_type: "test.v1.Product".to_string(),
    };

    // The dry run and the bind derive the same schema.
    let planned = documents
        .plan_schema(PlanSchemaRequest {
            source: Some(source.clone()),
        })
        .await
        .unwrap()
        .into_inner()
        .schema
        .unwrap();
    let bind = documents
        .bind_schema(BindSchemaRequest {
            source: Some(source),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner();
    let index_id = bind.index_id;
    let schema = bind.schema.unwrap();
    assert_eq!(planned.fingerprint, schema.fingerprint);
    assert_eq!(bind.info.unwrap().dim, 8);

    // Documents travel as the protobuf messages they already are, split
    // across frames like any real ingest.
    let docs: Vec<Vec<u8>> = (0..4)
        .map(|i| {
            document(&descriptor_set, "test.v1.Product", move |m| {
                m.set_field_by_name("id", Value::String(format!("doc-{i}")));
                m.set_field_by_name("title", Value::String(format!("product {i}")));
                m.set_field_by_name("embedding", Value::List(embedding(i)));
            })
        })
        .collect();
    let response = documents
        .add_documents(tokio_stream::iter(vec![
            AddDocumentsRequest {
                index_id: index_id.clone(),
                documents: docs[..2].to_vec(),
            },
            AddDocumentsRequest {
                index_id: index_id.clone(),
                documents: docs[2..].to_vec(),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.added, 4);
    assert_eq!(response.len, 4);

    // Searching the same index through the ordinary vector RPC returns the
    // ids the hash contract predicts.
    let results = vectors
        .search(SearchRequest {
            index_id: index_id.clone(),
            queries: embedding(2)
                .iter()
                .map(|v| match v {
                    Value::F32(f) => *f,
                    _ => unreachable!(),
                })
                .collect(),
            k: 1,
            allowlist: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(results.results[0].ids, vec![hash_string_id("doc-2")]);

    // GetSchema returns the bound schema, and a restart restores it after
    // a flush: same fingerprint, same extraction behavior.
    let fetched = documents
        .get_schema(GetSchemaRequest {
            index_id: index_id.clone(),
        })
        .await
        .unwrap()
        .into_inner()
        .schema
        .unwrap();
    assert_eq!(fetched.fingerprint, schema.fingerprint);
    vectors
        .flush(FlushRequest {
            index_id: index_id.clone(),
        })
        .await
        .unwrap();

    let restored = IndexStore::open(&root).unwrap();
    let bound = restored.schema(&index_id).expect("schema survives restart");
    assert_eq!(bound.schema.fingerprint, schema.fingerprint);
    assert_eq!(restored.get(&index_id).unwrap().read().unwrap().len(), 4);
    drop(restored);

    // A corrupted persisted descriptor set fails the whole restore, loudly,
    // rather than serving an index whose schema cannot be trusted.
    let schema_file = root
        .join(&index_id)
        .join("gen-00000000000000000001")
        .join("schema.fds");
    let mut bytes = std::fs::read(&schema_file).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&schema_file, bytes).unwrap();
    let error = IndexStore::open(&root)
        .err()
        .expect("corrupt schema must fail restore");
    assert!(error.to_string().contains("schema"), "{error}");

    std::fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn bad_documents_fail_by_position_and_commit_nothing() {
    let root = std::env::temp_dir().join(format!("turbovec-docs-{}", uuid::Uuid::new_v4()));
    let (mut documents, mut vectors) = start(&root).await;
    let descriptor_set = compile(ANNOTATED);
    let index_id = documents
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: descriptor_set.clone(),
                message_type: "test.v1.Product".to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;

    let good = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("id", Value::String("doc-0".into()));
        m.set_field_by_name("embedding", Value::List(embedding(0)));
    });
    let missing_id = document(&descriptor_set, "test.v1.Product", |m| {
        m.set_field_by_name("embedding", Value::List(embedding(1)));
    });
    let status = documents
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            index_id: index_id.clone(),
            documents: vec![good, missing_id],
        }]))
        .await
        .expect_err("a document without an id must fail the stream");
    assert!(
        status.message().contains("document 1"),
        "failures name the document's position: {}",
        status.message()
    );

    // Nothing before the bad document was applied.
    let info = vectors
        .get_index_info(turbovec_grpc::proto::GetIndexInfoRequest {
            index_id: index_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.len, 0, "a broken stream commits no prefix");

    std::fs::remove_dir_all(&root).unwrap();
}
