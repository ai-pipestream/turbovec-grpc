//! The `turbovec.v1.Documents` gRPC service: protobuf-first ingestion
//! and filtered search.
//!
//! A client registers the schema its producers already maintain — a
//! serialized `FileDescriptorSet` and a message type name — and then
//! streams documents as the serialized protobuf messages they already are.
//! The node derives the indexing plan (see [`crate::schema`]), decodes
//! each document against the bound descriptor, and indexes the extracted
//! `(id, vector)` pair alongside the planned scalar field values. No JSON,
//! no intermediate document model, no field mapping to maintain by hand.
//!
//! `SearchDocuments` closes the loop: a boolean CEL expression over those
//! stored fields becomes an exact allowlist for the vector search, so a
//! filtered top-k is the true top-k of the admitted set. Optional parent
//! collapse ranks every admitted chunk and keeps the first `k` distinct
//! parents. `GetParents` is the PK lookup the coordinator overlaps with
//! that search so membership can be unioned across shards.
//!
//! Derivation, extraction, and filtering are CPU-bound, so all of it runs
//! inside `tokio::task::spawn_blocking`, following the rest of this crate.
//! `AddDocuments` stages the complete stream before mutating the index, so
//! a broken or invalid stream commits no prefix, the same contract the
//! vector `Add` keeps.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::collapse;
use crate::columns::StoredRow;
use crate::filter::CompiledFilter;
use crate::proto::documents_server::{Documents, DocumentsServer};
use crate::proto::{
    stored_value, AddDocumentsRequest, AddDocumentsResponse, BindSchemaRequest, BindSchemaResponse,
    DocumentHit, DocumentQueryResult, GetParentsRequest, GetParentsResponse, GetSchemaRequest,
    GetSchemaResponse, PlanSchemaRequest, PlanSchemaResponse, ResolvedParent, SchemaSource,
    SearchDocumentsRequest, SearchDocumentsResponse, StoredValue,
};
use crate::schema::BoundSchema;
use crate::service::ServiceLimits;
use crate::store::{Index, IndexStore};
use turbovec::IdMapIndex;

/// gRPC implementation of `turbovec.v1.Documents`.
#[derive(Clone)]
pub struct DocumentsService {
    store: Arc<IndexStore>,
    limits: ServiceLimits,
}

impl DocumentsService {
    /// Create the service around a shared registry, normally the same one
    /// the vector services run on, so schema-bound indexes are searchable
    /// through the ordinary Search RPCs.
    pub fn new(store: Arc<IndexStore>, limits: ServiceLimits) -> Self {
        Self { store, limits }
    }

    /// Wrap into the generated tonic service.
    pub fn into_server(self) -> DocumentsServer<Self> {
        DocumentsServer::new(self)
    }

    fn validate_frame(&self, coordinates: usize) -> Result<(), Status> {
        if coordinates > self.limits.max_vector_coordinates_per_frame {
            return Err(Status::resource_exhausted(format!(
                "extracted vectors hold {coordinates} coordinates; limit is {}",
                self.limits.max_vector_coordinates_per_frame
            )));
        }
        Ok(())
    }

    fn validate_k(&self, k: usize) -> Result<(), Status> {
        if k == 0 || k > self.limits.max_k {
            return Err(Status::invalid_argument(format!(
                "k must be between 1 and {}",
                self.limits.max_k
            )));
        }
        Ok(())
    }
}

/// Render a string/integer stored id field back into the string the
/// client indexed.
fn id_of(fields: &HashMap<u32, StoredValue>, ordinal: u32) -> String {
    match fields.get(&ordinal).and_then(|v| v.value.as_ref()) {
        Some(stored_value::Value::StringValue(text)) => text.clone(),
        Some(stored_value::Value::IntValue(v)) => v.to_string(),
        Some(stored_value::Value::UintValue(v)) => v.to_string(),
        _ => String::new(),
    }
}

/// Derive a schema on the blocking pool, mapping failures to
/// `INVALID_ARGUMENT`: every derivation failure is about the request, and
/// the message already names the field path and the fix.
async fn derive(source: Option<SchemaSource>) -> Result<BoundSchema, Status> {
    let source = source.ok_or_else(|| Status::invalid_argument("source is required"))?;
    tokio::task::spawn_blocking(move || {
        BoundSchema::derive(&source.descriptor_set, &source.message_type)
    })
    .await
    .map_err(join_err)?
    .map_err(|e| Status::invalid_argument(e.to_string()))
}

#[tonic::async_trait]
impl Documents for DocumentsService {
    async fn plan_schema(
        &self,
        request: Request<PlanSchemaRequest>,
    ) -> Result<Response<PlanSchemaResponse>, Status> {
        let bound = derive(request.into_inner().source).await?;
        Ok(Response::new(PlanSchemaResponse {
            schema: Some(bound.schema),
        }))
    }

    async fn bind_schema(
        &self,
        request: Request<BindSchemaRequest>,
    ) -> Result<Response<BindSchemaResponse>, Status> {
        let req = request.into_inner();
        let bound = derive(req.source).await?;
        let bit_width = req.bit_width as usize;
        let index = if bound.schema.dim > 0 {
            IdMapIndex::new(bound.schema.dim as usize, bit_width).map_err(|e| {
                Status::invalid_argument(format!(
                    "cannot build an index over {} dims (from the vector field's \
                     vector_dims hint): {e}",
                    bound.schema.dim
                ))
            })?
        } else {
            IdMapIndex::new_lazy(bit_width).map_err(|e| Status::invalid_argument(e.to_string()))?
        };
        let schema = bound.schema.clone();
        let bound = Arc::new(bound);
        let id = self.store.insert(Index::IdMap(index));
        self.store.bind_schema(&id, bound);
        let handle = self
            .store
            .get(&id)
            .expect("an index inserted moments ago exists");
        let info = {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            crate::service::index_info(&id, &guard, false, 0)
        };
        Ok(Response::new(BindSchemaResponse {
            index_id: id,
            schema: Some(schema),
            info: Some(info),
        }))
    }

    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let id = request.into_inner().index_id;
        if self.store.get(&id).is_none() {
            return Err(Status::not_found(format!("unknown index_id: {id}")));
        }
        let bound = self.store.schema(&id).ok_or_else(|| {
            Status::not_found(format!(
                "index {id} has no bound schema; it was created by CreateIndex or \
                 ImportRows, not BindSchema"
            ))
        })?;
        Ok(Response::new(GetSchemaResponse {
            schema: Some(bound.schema.clone()),
        }))
    }

    async fn add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty add stream: no frames received"))?;
        let index_id = first.index_id.clone();
        let handle = self
            .store
            .get(&index_id)
            .ok_or_else(|| Status::not_found(format!("unknown index_id: {index_id}")))?;
        let bound = self.store.schema(&index_id).ok_or_else(|| {
            Status::failed_precondition(format!(
                "index {index_id} has no bound schema; create it with BindSchema \
                 before streaming documents"
            ))
        })?;

        let columns = self.store.columns(&index_id).ok_or_else(|| {
            Status::internal(format!(
                "index {index_id} has a bound schema but no document columns"
            ))
        })?;
        let parents = self.store.parents(&index_id);
        if bound.is_chunked() && parents.is_none() {
            return Err(Status::internal(format!(
                "index {index_id} is chunked but has no parent store"
            )));
        }

        // Stage the complete operation before mutating the index: decode and
        // extract every frame, then add once. A document that fails is named
        // by its position across the whole stream, and nothing is applied.
        // `position` counts parent wire messages; `added` counts indexed rows.
        let mut ids: Vec<u64> = Vec::new();
        let mut vectors: Vec<f32> = Vec::new();
        let mut rows: Vec<StoredRow> = Vec::new();
        let mut parent_upserts: Vec<(u64, HashMap<u32, StoredValue>, Vec<u64>)> = Vec::new();
        let mut dim: usize = 0;
        let mut position: u64 = 0;
        let mut frame = Some(first);
        while let Some(current) = frame.take() {
            if !current.index_id.is_empty() && current.index_id != index_id {
                return Err(Status::invalid_argument(
                    "every frame must carry the same index_id",
                ));
            }
            let schema = Arc::clone(&bound);
            let extracted = tokio::task::spawn_blocking(move || {
                let mut out = Vec::with_capacity(current.documents.len());
                for (offset, document) in current.documents.iter().enumerate() {
                    let extracted = schema.extract(document).map_err(|e| {
                        Status::invalid_argument(format!(
                            "document {}: {e}",
                            position + offset as u64
                        ))
                    })?;
                    out.push(extracted);
                }
                Ok::<_, Status>(out)
            })
            .await
            .map_err(join_err)??;
            for ingest in extracted {
                let mut chunk_labels = Vec::with_capacity(ingest.rows.len());
                for row in ingest.rows {
                    if dim == 0 {
                        dim = row.vector.len();
                    } else if row.vector.len() != dim {
                        return Err(Status::invalid_argument(format!(
                            "document {position} has a {}-coordinate vector; earlier \
                             documents in this stream have {dim}",
                            row.vector.len()
                        )));
                    }
                    ids.push(row.label);
                    chunk_labels.push(row.label);
                    vectors.extend_from_slice(&row.vector);
                    rows.push(StoredRow {
                        fields: row.fields,
                        parent_id: row.parent_id,
                        chunk_id: row.chunk_id,
                        parent_label: row.parent_label,
                    });
                }
                if let Some(parent) = ingest.parent {
                    parent_upserts.push((parent.parent_label, parent.fields, chunk_labels));
                }
                position += 1;
            }
            self.validate_frame(vectors.len())?;
            frame = stream.message().await?;
        }
        if ids.is_empty() {
            return Err(Status::invalid_argument("the stream carried no documents"));
        }

        let added = ids.len() as u64;
        let len = tokio::task::spawn_blocking(move || {
            let mut guard = handle
                .write()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            match &mut *guard {
                Index::IdMap(index) => index
                    .add_with_ids_2d(&vectors, dim, &ids)
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
                Index::Positional(_) => {
                    return Err(Status::failed_precondition(
                        "schema-bound indexes are id-mapped; this index is positional",
                    ))
                }
            }
            // Columns and parents update under the index write lock so every
            // reader with the index read lock sees them move together.
            let mut columns = columns
                .write()
                .map_err(|_| Status::internal("columns lock poisoned"))?;
            for (label, row) in ids.iter().zip(rows) {
                columns.insert(*label, row);
            }
            if let Some(parents) = parents {
                let mut parents = parents
                    .write()
                    .map_err(|_| Status::internal("parents lock poisoned"))?;
                for (parent_label, parent_fields, chunk_labels) in parent_upserts {
                    parents.upsert(parent_label, parent_fields, chunk_labels);
                }
            }
            Ok::<_, Status>(guard.len() as u64)
        })
        .await
        .map_err(join_err)??;

        Ok(Response::new(AddDocumentsResponse { added, len }))
    }

    async fn search_documents(
        &self,
        request: Request<SearchDocumentsRequest>,
    ) -> Result<Response<SearchDocumentsResponse>, Status> {
        let req = request.into_inner();
        let index_id = req.index_id.clone();
        let handle = self
            .store
            .get(&index_id)
            .ok_or_else(|| Status::not_found(format!("unknown index_id: {index_id}")))?;
        let bound = self.store.schema(&index_id).ok_or_else(|| {
            Status::failed_precondition(format!(
                "index {index_id} has no bound schema; SearchDocuments needs a \
                 schema-bound index — plain vector indexes use Search"
            ))
        })?;
        let columns = self.store.columns(&index_id).ok_or_else(|| {
            Status::internal(format!(
                "index {index_id} has a bound schema but no document columns"
            ))
        })?;
        let parents = self.store.parents(&index_id);
        let k = req.k as usize;
        self.validate_k(k)?;
        self.validate_frame(req.queries.len())?;
        let max_queries = self.limits.max_queries_per_request;

        let response = tokio::task::spawn_blocking(move || {
            let guard = handle
                .read()
                .map_err(|_| Status::internal("index lock poisoned"))?;
            let index = match &*guard {
                Index::IdMap(index) => index,
                Index::Positional(_) => {
                    return Err(Status::failed_precondition(
                        "schema-bound indexes are id-mapped; this index is positional",
                    ))
                }
            };
            let Some(dim) = guard.dim_opt() else {
                return Err(Status::failed_precondition(
                    "index has no documents; add documents before searching",
                ));
            };
            crate::service::validate_queries(&req.queries, dim)?;
            let nq = req.queries.len() / dim;
            if nq > max_queries {
                return Err(Status::resource_exhausted(format!(
                    "request has {nq} queries; limit is {max_queries}"
                )));
            }

            let columns = columns
                .read()
                .map_err(|_| Status::internal("columns lock poisoned"))?;
            let parents = match &parents {
                Some(parents) => Some(
                    parents
                        .read()
                        .map_err(|_| Status::internal("parents lock poisoned"))?,
                ),
                None => None,
            };
            let total = columns.len() as u64;

            // The filter compiles once and evaluates over every document's
            // stored values; the admitted labels become an exact allowlist
            // for the vector search. An evaluation failure is a request
            // problem (the expression met a value it cannot handle) and
            // fails the search rather than shrinking the result.
            let allow: Option<Vec<u64>> = if req.filter.is_empty() {
                None
            } else {
                let compiled =
                    CompiledFilter::compile(&req.filter, &bound.schema, bound.stored_fields())
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                let mut allow = Vec::new();
                for (label, row) in columns.iter() {
                    let admitted = compiled
                        .matches(&row.fields)
                        .map_err(|e| Status::invalid_argument(format!("document {label}: {e}")))?;
                    if admitted && index.contains(label) {
                        allow.push(label);
                    }
                }
                Some(allow)
            };
            let matched = allow.as_ref().map_or(total, |a| a.len() as u64);

            // Collapse is top-k parents. Ranking only the request k chunks
            // and collapsing afterwards is not exact: one parent with many
            // high-scoring chunks can fill that local top-k and hide every
            // other parent. Rank every admitted chunk, then take the first
            // k distinct parents. Request k is still what validate_k saw;
            // the internal width is not a client-facing limit.
            let search_k = if req.collapse_parents {
                allow.as_ref().map_or(columns.len(), Vec::len)
            } else {
                k
            };

            let results =
                if guard.is_empty() || allow.as_ref().is_some_and(Vec::is_empty) || search_k == 0 {
                    vec![DocumentQueryResult::default(); nq]
                } else {
                    let (scores, labels) = match &allow {
                        Some(allow) => index
                            .search_with_allowlist(&req.queries, search_k, Some(allow.as_slice()))
                            .map_err(|e| Status::internal(format!("allowlist search: {e}")))?,
                        None => index.search(&req.queries, search_k),
                    };
                    let k_eff = scores.len().checked_div(nq).unwrap_or(0);
                    (0..nq)
                        .map(|qi| {
                            let lo = qi * k_eff;
                            let hi = lo + k_eff;
                            let mut hits: Vec<DocumentHit> = scores[lo..hi]
                                .iter()
                                .zip(&labels[lo..hi])
                                .map(|(&score, &label)| {
                                    let row = columns.get(label);
                                    let parent_label =
                                        row.map(|row| row.parent_label).unwrap_or(label);
                                    let parent_chunks = parents
                                        .as_ref()
                                        .and_then(|store| store.get(parent_label))
                                        .map(|parent| parent.chunk_labels.len() as u32)
                                        .unwrap_or(0);
                                    DocumentHit {
                                        score,
                                        label,
                                        id: row
                                            .map(|row| {
                                                if row.parent_id.is_empty() {
                                                    id_of(&row.fields, bound.doc_id_ordinal())
                                                } else {
                                                    row.parent_id.clone()
                                                }
                                            })
                                            .unwrap_or_default(),
                                        chunk_id: row
                                            .map(|row| row.chunk_id.clone())
                                            .unwrap_or_default(),
                                        parent_label,
                                        collapsed: 0,
                                        parent_chunks,
                                    }
                                })
                                .collect();
                            if req.collapse_parents {
                                hits = collapse::collapse_parents(hits, k);
                            }
                            DocumentQueryResult { hits }
                        })
                        .collect()
                };
            Ok::<_, Status>(SearchDocumentsResponse {
                results,
                matched,
                total,
            })
        })
        .await
        .map_err(join_err)??;

        Ok(Response::new(response))
    }

    async fn get_parents(
        &self,
        request: Request<GetParentsRequest>,
    ) -> Result<Response<GetParentsResponse>, Status> {
        let req = request.into_inner();
        let index_id = req.index_id.clone();
        if self.store.get(&index_id).is_none() {
            return Err(Status::not_found(format!("unknown index_id: {index_id}")));
        }
        let Some(bound) = self.store.schema(&index_id) else {
            return Ok(Response::new(GetParentsResponse::default()));
        };
        if !bound.is_chunked() {
            return Ok(Response::new(GetParentsResponse::default()));
        }
        let parents = self.store.parents(&index_id).ok_or_else(|| {
            Status::internal(format!(
                "index {index_id} is chunked but has no parent store"
            ))
        })?;

        let response = tokio::task::spawn_blocking(move || {
            let parents = parents
                .read()
                .map_err(|_| Status::internal("parents lock poisoned"))?;
            let mut resolved = Vec::new();
            for parent_label in req.parent_labels {
                let Some(record) = parents.get(parent_label) else {
                    continue;
                };
                resolved.push(ResolvedParent {
                    parent_label,
                    id: id_of(&record.fields, bound.doc_id_ordinal()),
                    chunk_labels: record.chunk_labels.iter().copied().collect(),
                });
            }
            Ok::<_, Status>(GetParentsResponse { parents: resolved })
        })
        .await
        .map_err(join_err)??;

        Ok(Response::new(response))
    }
}

fn join_err(err: tokio::task::JoinError) -> Status {
    Status::internal(format!("document task failed: {err}"))
}
