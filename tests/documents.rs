//! Tests for the protobuf-first schema layer and the Documents service.
//!
//! Test schemas are compiled from `.proto` source at test time with protox
//! (a pure-Rust protoc), against the same vendored hints file the server
//! reads, so what is exercised here is exactly what a client toolchain
//! produces: a serialized `FileDescriptorSet` with imports included.

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, ReflectMessage as _, Value};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use turbovec_grpc::filter::CompiledFilter;
use turbovec_grpc::proto::documents_client::DocumentsClient;
use turbovec_grpc::proto::turbo_vec_client::TurboVecClient;
use turbovec_grpc::proto::{
    AddDocumentsRequest, AddRequest, BindSchemaRequest, FieldKind, FieldRole, FlushRequest,
    GetParentsRequest, GetSchemaRequest, PlanSchemaRequest, RemoveRequest, SchemaSource,
    SearchDocumentsRequest, SearchRequest,
};
use turbovec_grpc::schema::{hash_chunk_label, hash_string_id, BoundSchema};
use turbovec_grpc::{DocumentsService, IndexStore, ServiceLimits, TurboVecService};

/// A product type exercising hints and inference together: an explicit
/// VECTOR hint with declared dims, an explicit SKIP, a nested message that
/// expands into dotted paths, a Timestamp leaf, an enum, a repeated
/// scalar, and inferred keyword/text splits.
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

/// Encode one fully populated test.v1.Product, nested Timestamp included.
fn product(
    descriptor_set: &[u8],
    doc: usize,
    price_cents: i64,
    author: &str,
    created_seconds: i64,
    tags: &[&str],
    status: i32,
) -> Vec<u8> {
    document(descriptor_set, "test.v1.Product", move |m| {
        let pool = m.descriptor().parent_pool().clone();
        m.set_field_by_name("id", Value::String(format!("doc-{doc}")));
        m.set_field_by_name("title", Value::String(format!("product {doc}")));
        m.set_field_by_name("price_cents", Value::I64(price_cents));
        m.set_field_by_name("in_stock", Value::Bool(doc.is_multiple_of(2)));
        m.set_field_by_name("embedding", Value::List(embedding(doc)));
        m.set_field_by_name(
            "tags",
            Value::List(tags.iter().map(|t| Value::String(t.to_string())).collect()),
        );
        m.set_field_by_name("status", Value::EnumNumber(status));
        let mut timestamp = DynamicMessage::new(
            pool.get_message_by_name("google.protobuf.Timestamp")
                .unwrap(),
        );
        timestamp.set_field_by_name("seconds", Value::I64(created_seconds));
        let mut meta = DynamicMessage::new(pool.get_message_by_name("test.v1.Meta").unwrap());
        meta.set_field_by_name("author", Value::String(author.to_string()));
        meta.set_field_by_name("created_at", Value::Message(timestamp));
        m.set_field_by_name("meta", Value::Message(meta));
    })
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
            ("tags", "tags", FieldKind::Text, FieldRole::None),
            ("status", "status", FieldKind::Keyword, FieldRole::None),
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
    let extracted = bound.extract(&good).unwrap();
    assert!(extracted.parent.is_none());
    assert_eq!(extracted.rows.len(), 1);
    let row = &extracted.rows[0];
    assert_eq!(row.label, hash_string_id("doc-1"));
    assert_eq!(row.vector.len(), 8);
    // Every stored field is present, defaults included: 9 planned fields
    // minus the vector, which is not a stored field.
    assert_eq!(row.fields.len(), bound.schema.fields.len() - 1);

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

#[test]
fn cel_filters_read_the_documents_own_proto_fields() {
    let descriptor_set = compile(ANNOTATED);
    let bound = BoundSchema::derive(&descriptor_set, "test.v1.Product").unwrap();
    let full = bound
        .extract(&product(
            &descriptor_set,
            1,
            4200,
            "kagome",
            1_700_000_000,
            &["legal", "opinion"],
            1, // STATUS_ACTIVE
        ))
        .unwrap()
        .rows
        .into_iter()
        .next()
        .unwrap();
    let sparse = bound
        .extract(&document(&descriptor_set, "test.v1.Product", |m| {
            m.set_field_by_name("id", Value::String("doc-9".into()));
            m.set_field_by_name("embedding", Value::List(embedding(0)));
        }))
        .unwrap()
        .rows
        .into_iter()
        .next()
        .unwrap();

    let admits = |expression: &str, fields: &_| {
        CompiledFilter::compile(expression, &bound.schema, bound.stored_fields())
            .unwrap()
            .matches(fields)
            .unwrap()
    };

    // Scalars, nested paths, timestamps, repeated membership, enum names.
    assert!(admits("price_cents < 5000", &full.fields));
    assert!(!admits("price_cents < 4200", &full.fields));
    assert!(admits(r#"meta.author == "kagome""#, &full.fields));
    assert!(admits(
        r#"meta.created_at > timestamp("2020-01-01T00:00:00Z")"#,
        &full.fields
    ));
    assert!(admits(r#""legal" in tags"#, &full.fields));
    assert!(admits(r#"status == "STATUS_ACTIVE""#, &full.fields));
    assert!(admits(
        r#"price_cents < 5000 && meta.author == "kagome" && !("draft" in tags)"#,
        &full.fields
    ));

    // Unset proto3 fields evaluate as their defaults, epoch included.
    assert!(admits(r#"title == """#, &sparse.fields));
    assert!(admits("in_stock == false", &sparse.fields));
    assert!(admits("size(tags) == 0", &sparse.fields));
    assert!(admits(r#"status == "STATUS_UNSPECIFIED""#, &sparse.fields));
    assert!(admits(
        r#"meta.created_at == timestamp("1970-01-01T00:00:00Z")"#,
        &sparse.fields
    ));

    // Failures are loud and name the problem.
    let unknown = CompiledFilter::compile("nonexistent > 1", &bound.schema, bound.stored_fields())
        .expect_err("unknown fields fail at compile time")
        .to_string();
    assert!(
        unknown.contains("nonexistent") && unknown.contains("price_cents"),
        "names the field and what is available: {unknown}"
    );
    let unparsed = CompiledFilter::compile("price_cents <", &bound.schema, bound.stored_fields())
        .expect_err("syntax errors fail at compile time")
        .to_string();
    assert!(unparsed.contains("parse"), "{unparsed}");
    let non_bool = CompiledFilter::compile("price_cents + 1", &bound.schema, bound.stored_fields())
        .unwrap()
        .matches(&full.fields)
        .expect_err("a non-boolean filter fails at evaluation")
        .to_string();
    assert!(non_bool.contains("boolean"), "{non_bool}");
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
async fn filtered_search_is_the_exact_top_k_of_the_admitted_set() {
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

    // doc-i costs 1000 * (i + 1) cents; authors alternate.
    let docs: Vec<Vec<u8>> = (0..4)
        .map(|i| {
            product(
                &descriptor_set,
                i,
                1000 * (i as i64 + 1),
                if i % 2 == 0 { "kagome" } else { "rin" },
                1_600_000_000 + i as i64,
                &["legal"],
                1,
            )
        })
        .collect();
    documents
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            index_id: index_id.clone(),
            documents: docs,
        }]))
        .await
        .unwrap();
    let query: Vec<f32> = embedding(2)
        .iter()
        .map(|v| match v {
            Value::F32(f) => *f,
            _ => unreachable!(),
        })
        .collect();

    // Unfiltered: doc-2 is the nearest neighbour and hits carry the
    // original string id, not just the label.
    let unfiltered = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 1,
            filter: String::new(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(unfiltered.matched, 4);
    assert_eq!(unfiltered.total, 4);
    let top = &unfiltered.results[0].hits[0];
    assert_eq!(top.label, hash_string_id("doc-2"));
    assert_eq!(top.id, "doc-2");

    // A filter that excludes the nearest neighbour returns the exact
    // top-k of the admitted set, not a re-ranked approximation.
    let filtered = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: "price_cents <= 2000".to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(filtered.matched, 2);
    assert_eq!(filtered.total, 4);
    let mut ids: Vec<String> = filtered.results[0]
        .hits
        .iter()
        .map(|hit| hit.id.clone())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["doc-0", "doc-1"]);

    // Nested paths and identity work through the same expression surface.
    let by_author = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: r#"meta.author == "rin" && id != "doc-1""#.to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(by_author.matched, 1);
    assert_eq!(by_author.results[0].hits[0].id, "doc-3");

    // A bad expression is an INVALID_ARGUMENT naming the problem, never
    // an empty result.
    let status = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 1,
            filter: "no_such_field == 1".to_string(),
            collapse_parents: false,
        })
        .await
        .expect_err("unknown fields fail the request");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("no_such_field"));

    // Remove drops the row's stored fields with the row.
    vectors
        .remove(RemoveRequest {
            index_id: index_id.clone(),
            id: hash_string_id("doc-3"),
        })
        .await
        .unwrap();
    let gone = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: r#"meta.author == "rin" && id != "doc-1""#.to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(gone.matched, 0);
    assert_eq!(gone.total, 3);
    assert!(gone.results[0].hits.is_empty());

    // A raw vector Add would create rows with no stored fields, so a
    // schema-bound index refuses it.
    let status = vectors
        .add(tokio_stream::iter(vec![AddRequest {
            index_id: index_id.clone(),
            dim: 8,
            vectors: vec![0.5; 8],
            ids: vec![77],
            operation_id: String::new(),
            expected_len: None,
            expected_rows: 0,
        }]))
        .await
        .expect_err("schema-bound indexes only ingest through AddDocuments");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("AddDocuments"));

    // The stored fields persist with the generation and restore exactly;
    // a corrupted documents file fails the whole restore.
    vectors
        .flush(FlushRequest {
            index_id: index_id.clone(),
        })
        .await
        .unwrap();
    let restored = IndexStore::open(&root).unwrap();
    let columns = restored
        .columns(&index_id)
        .expect("columns survive restart");
    assert_eq!(columns.read().unwrap().len(), 3);
    drop(restored);

    let documents_file = root
        .join(&index_id)
        .join("gen-00000000000000000001")
        .join("documents.pb");
    let mut bytes = std::fs::read(&documents_file).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    std::fs::write(&documents_file, bytes).unwrap();
    let error = IndexStore::open(&root)
        .err()
        .expect("corrupt stored documents must fail restore");
    assert!(
        error.to_string().contains("documents.pb"),
        "names the file: {error}"
    );

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

/// Parent with a CHUNKS scope: each chunk is its own indexed row.
const CHUNKED: &str = r#"
syntax = "proto3";
package test.v1;

import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";

message Opinion {
  string id = 1 [(ai.pipestream.proto.index.hints.v1.index) = {
    block_role: BLOCK_ROLE_DOC_ID
  }];
  string title = 2;
  repeated Chunk chunks = 3 [(ai.pipestream.proto.index.hints.v1.index) = {
    block_role: BLOCK_ROLE_CHUNKS
  }];
}

message Chunk {
  string chunk_id = 1 [(ai.pipestream.proto.index.hints.v1.index) = {
    block_role: BLOCK_ROLE_CHUNK_ID
  }];
  string body = 2;
  int64 ordinal = 3;
  repeated float embedding = 4 [(ai.pipestream.proto.index.hints.v1.index) = {
    type: INDEX_FIELD_TYPE_VECTOR
    vector_dims: 8
  }];
}
"#;

fn chunked_opinion(descriptor_set: &[u8], doc: usize, n_chunks: usize) -> Vec<u8> {
    chunked_opinion_chunks(descriptor_set, doc, &(0..n_chunks).collect::<Vec<_>>())
}

fn chunked_opinion_chunks(descriptor_set: &[u8], doc: usize, chunk_ids: &[usize]) -> Vec<u8> {
    let chunk_ids = chunk_ids.to_vec();
    document(descriptor_set, "test.v1.Opinion", move |m| {
        let pool = m.descriptor().parent_pool().clone();
        m.set_field_by_name("id", Value::String(format!("op-{doc}")));
        m.set_field_by_name("title", Value::String(format!("opinion {doc}")));
        let chunk_desc = pool.get_message_by_name("test.v1.Chunk").unwrap();
        let chunks: Vec<Value> = chunk_ids
            .iter()
            .map(|&c| {
                let mut chunk = DynamicMessage::new(chunk_desc.clone());
                chunk.set_field_by_name("chunk_id", Value::String(format!("c{c}")));
                chunk.set_field_by_name("body", Value::String(format!("body-{doc}-{c}")));
                chunk.set_field_by_name("ordinal", Value::I64(c as i64));
                chunk.set_field_by_name(
                    "embedding",
                    Value::List(
                        (0..8)
                            .map(|i| Value::F32(if i == (doc * 2 + c) % 8 { 1.0 } else { 0.05 }))
                            .collect(),
                    ),
                );
                Value::Message(chunk)
            })
            .collect();
        m.set_field_by_name("chunks", Value::List(chunks));
    })
}

#[test]
fn chunked_schema_indexes_each_chunk_as_its_own_row() {
    let descriptor_set = compile(CHUNKED);
    let bound = BoundSchema::derive(&descriptor_set, "test.v1.Opinion").unwrap();
    assert!(bound.is_chunked());
    assert_eq!(bound.schema.vector_path, "chunks.embedding");
    assert_eq!(bound.schema.doc_id_path, "id");

    let ingest = bound
        .extract(&chunked_opinion(&descriptor_set, 0, 3))
        .unwrap();
    let parent = ingest.parent.expect("chunked extract yields a parent");
    assert_eq!(parent.parent_label, hash_string_id("op-0"));
    assert_eq!(ingest.rows.len(), 3);
    for (i, row) in ingest.rows.iter().enumerate() {
        assert_eq!(row.parent_id, "op-0");
        assert_eq!(row.chunk_id, format!("c{i}"));
        assert_eq!(row.label, hash_chunk_label("op-0", &format!("c{i}")));
        assert_eq!(row.parent_label, parent.parent_label);
        assert!(row.fields.contains_key(&bound.doc_id_ordinal()));
    }

    let empty = bound
        .extract(&document(&descriptor_set, "test.v1.Opinion", |m| {
            m.set_field_by_name("id", Value::String("op-empty".into()));
        }))
        .unwrap_err()
        .to_string();
    assert!(empty.contains("no chunks"), "{empty}");

    let flat_vector = r#"
        syntax = "proto3";
        package test.v1;
        import "ai/pipestream/proto/index/hints/v1/indexing_hints.proto";
        message Doc {
          string id = 1;
          repeated float embedding = 2 [(ai.pipestream.proto.index.hints.v1.index) = {
            type: INDEX_FIELD_TYPE_VECTOR
            vector_dims: 8
          }];
          repeated Chunk chunks = 3 [(ai.pipestream.proto.index.hints.v1.index) = {
            block_role: BLOCK_ROLE_CHUNKS
          }];
        }
        message Chunk {
          string body = 1;
        }
    "#;
    let error = derive_err(flat_vector, "test.v1.Doc");
    assert!(
        error.contains("inside the CHUNKS scope") || error.contains("CHUNKS"),
        "{error}"
    );
}

#[tokio::test]
async fn chunked_documents_search_filter_and_survive_restart() {
    let root = std::env::temp_dir().join(format!("turbovec-chunks-{}", uuid::Uuid::new_v4()));
    let (mut documents, mut vectors) = start(&root).await;
    let descriptor_set = compile(CHUNKED);
    let index_id = documents
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: descriptor_set.clone(),
                message_type: "test.v1.Opinion".to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;

    let added = documents
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            index_id: index_id.clone(),
            documents: vec![
                chunked_opinion(&descriptor_set, 0, 2),
                chunked_opinion(&descriptor_set, 1, 2),
            ],
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added, 4);
    assert_eq!(added.len, 4);

    let mut query = vec![0.05f32; 8];
    query[1] = 1.0;
    let response = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: String::new(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.total, 4);
    let top = &response.results[0].hits[0];
    assert_eq!(top.id, "op-0");
    assert_eq!(top.chunk_id, "c1");
    assert_eq!(top.label, hash_chunk_label("op-0", "c1"));
    assert_eq!(top.parent_label, hash_string_id("op-0"));
    assert_eq!(top.parent_chunks, 2);
    assert_eq!(top.collapsed, 0);

    let filtered = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: r#"title == "opinion 1" && chunks.ordinal == 0"#.to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(filtered.matched, 1);
    assert_eq!(filtered.results[0].hits[0].id, "op-1");
    assert_eq!(filtered.results[0].hits[0].chunk_id, "c0");

    let chunk_label = hash_chunk_label("op-0", "c0");
    assert!(
        vectors
            .remove(RemoveRequest {
                index_id: index_id.clone(),
                id: chunk_label,
            })
            .await
            .unwrap()
            .into_inner()
            .removed
    );
    let after = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 4,
            filter: r#"id == "op-0""#.to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(after.matched, 1);
    assert_eq!(after.results[0].hits[0].chunk_id, "c1");

    vectors
        .flush(FlushRequest {
            index_id: index_id.clone(),
        })
        .await
        .unwrap();
    drop(documents);
    drop(vectors);

    let (mut documents, _) = start(&root).await;
    let restored = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query,
            k: 4,
            filter: r#"id == "op-0""#.to_string(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(restored.matched, 1);
    assert_eq!(restored.results[0].hits[0].chunk_id, "c1");

    std::fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn collapse_parents_keeps_weaker_parents_when_one_parent_has_many_chunks() {
    let root = std::env::temp_dir().join(format!("turbovec-collapse-{}", uuid::Uuid::new_v4()));
    let (mut documents, _) = start(&root).await;
    let descriptor_set = compile(CHUNKED);
    let index_id = documents
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: descriptor_set.clone(),
                message_type: "test.v1.Opinion".to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;

    // op-0 has three chunks peaking at dims 0, 1, 2. op-2 has one chunk
    // peaking at dim 4. A query aligned with 0..2 makes every op-0 chunk
    // beat op-2, so an uncollapsed top-2 is two siblings of op-0.
    documents
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            index_id: index_id.clone(),
            documents: vec![
                chunked_opinion(&descriptor_set, 0, 3),
                chunked_opinion(&descriptor_set, 2, 1),
            ],
        }]))
        .await
        .unwrap();

    let query: Vec<f32> = (0..8).map(|i| if i < 3 { 1.0 } else { 0.05 }).collect();
    let uncollapsed = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 2,
            filter: String::new(),
            collapse_parents: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(uncollapsed.matched, 4);
    assert_eq!(uncollapsed.total, 4);
    assert_eq!(uncollapsed.results[0].hits.len(), 2);
    assert!(
        uncollapsed.results[0]
            .hits
            .iter()
            .all(|hit| hit.id == "op-0"),
        "uncollapsed top-2 is crowded by one parent: {:?}",
        uncollapsed.results[0]
            .hits
            .iter()
            .map(|h| (&h.id, &h.chunk_id))
            .collect::<Vec<_>>()
    );

    let collapsed = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query.clone(),
            k: 2,
            filter: String::new(),
            collapse_parents: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(collapsed.matched, 4);
    assert_eq!(collapsed.total, 4);
    let ids: Vec<&str> = collapsed.results[0]
        .hits
        .iter()
        .map(|hit| hit.id.as_str())
        .collect();
    assert_eq!(ids, vec!["op-0", "op-2"]);
    assert_eq!(collapsed.results[0].hits[0].collapsed, 2);
    assert_eq!(collapsed.results[0].hits[0].parent_chunks, 3);
    assert_eq!(collapsed.results[0].hits[1].collapsed, 0);
    assert_eq!(collapsed.results[0].hits[1].parent_chunks, 1);
    assert_eq!(
        collapsed.results[0].hits[0].parent_label,
        hash_string_id("op-0")
    );

    let filtered = documents
        .search_documents(SearchDocumentsRequest {
            index_id: index_id.clone(),
            queries: query,
            k: 1,
            filter: r#"title == "opinion 0""#.to_string(),
            collapse_parents: true,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(filtered.matched, 3);
    assert_eq!(filtered.results[0].hits.len(), 1);
    assert_eq!(filtered.results[0].hits[0].id, "op-0");
    assert_eq!(filtered.results[0].hits[0].collapsed, 2);

    std::fs::remove_dir_all(&root).unwrap();
}

#[tokio::test]
async fn get_parents_resolves_membership_and_omits_unknown() {
    let root = std::env::temp_dir().join(format!("turbovec-parents-rpc-{}", uuid::Uuid::new_v4()));
    let (mut documents, _) = start(&root).await;
    let descriptor_set = compile(CHUNKED);
    let index_id = documents
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: descriptor_set.clone(),
                message_type: "test.v1.Opinion".to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    documents
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            index_id: index_id.clone(),
            documents: vec![
                chunked_opinion(&descriptor_set, 0, 2),
                chunked_opinion(&descriptor_set, 1, 1),
            ],
        }]))
        .await
        .unwrap();

    let parent_0 = hash_string_id("op-0");
    let parent_1 = hash_string_id("op-1");
    let resolved = documents
        .get_parents(GetParentsRequest {
            index_id: index_id.clone(),
            parent_labels: vec![parent_0, 0xdead_beef, parent_1],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resolved.parents.len(), 2);
    assert_eq!(resolved.parents[0].parent_label, parent_0);
    assert_eq!(resolved.parents[0].id, "op-0");
    let mut expected = vec![
        hash_chunk_label("op-0", "c0"),
        hash_chunk_label("op-0", "c1"),
    ];
    expected.sort_unstable();
    assert_eq!(resolved.parents[0].chunk_labels, expected);
    assert_eq!(resolved.parents[1].id, "op-1");
    assert_eq!(
        resolved.parents[1].chunk_labels,
        vec![hash_chunk_label("op-1", "c0")]
    );

    let flat_id = documents
        .bind_schema(BindSchemaRequest {
            source: Some(SchemaSource {
                descriptor_set: compile(ANNOTATED),
                message_type: "test.v1.Product".to_string(),
            }),
            bit_width: 4,
        })
        .await
        .unwrap()
        .into_inner()
        .index_id;
    let empty = documents
        .get_parents(GetParentsRequest {
            index_id: flat_id,
            parent_labels: vec![parent_0],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.parents.is_empty());

    std::fs::remove_dir_all(&root).unwrap();
}
