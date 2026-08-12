//! The `turbovec.v1.Documents` gRPC service: protobuf-first ingestion.
//!
//! A client registers the schema its producers already maintain — a
//! serialized `FileDescriptorSet` and a message type name — and then
//! streams documents as the serialized protobuf messages they already are.
//! The node derives the indexing plan (see [`crate::schema`]), decodes
//! each document against the bound descriptor, and indexes the extracted
//! `(id, vector)` pair. No JSON, no intermediate document model, no field
//! mapping to maintain by hand.
//!
//! Derivation and extraction are CPU-bound, so both run inside
//! `tokio::task::spawn_blocking`, following the rest of this crate.
//! `AddDocuments` stages the complete stream before mutating the index, so
//! a broken or invalid stream commits no prefix, the same contract the
//! vector `Add` keeps.

use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};

use crate::proto::documents_server::{Documents, DocumentsServer};
use crate::proto::{
    AddDocumentsRequest, AddDocumentsResponse, BindSchemaRequest, BindSchemaResponse,
    GetSchemaRequest, GetSchemaResponse, PlanSchemaRequest, PlanSchemaResponse, SchemaSource,
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

        // Stage the complete operation before mutating the index: decode and
        // extract every frame, then add once. A document that fails is named
        // by its position across the whole stream, and nothing is applied.
        let mut ids: Vec<u64> = Vec::new();
        let mut vectors: Vec<f32> = Vec::new();
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
                    let pair = schema.extract(document).map_err(|e| {
                        Status::invalid_argument(format!(
                            "document {}: {e}",
                            position + offset as u64
                        ))
                    })?;
                    out.push(pair);
                }
                Ok::<_, Status>(out)
            })
            .await
            .map_err(join_err)??;
            for (label, vector) in extracted {
                if dim == 0 {
                    dim = vector.len();
                } else if vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "document {position} has a {}-coordinate vector; earlier \
                         documents in this stream have {dim}",
                        vector.len()
                    )));
                }
                ids.push(label);
                vectors.extend_from_slice(&vector);
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
            Ok::<_, Status>(guard.len() as u64)
        })
        .await
        .map_err(join_err)??;

        Ok(Response::new(AddDocumentsResponse { added, len }))
    }
}

fn join_err(err: tokio::task::JoinError) -> Status {
    Status::internal(format!("document task failed: {err}"))
}
