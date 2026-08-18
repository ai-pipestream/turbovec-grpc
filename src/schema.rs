//! Protobuf-first index schema derivation.
//!
//! The input is what a producer already has: a serialized
//! `google.protobuf.FileDescriptorSet` and a fully qualified message type
//! name. The output is a deterministic indexing plan — which fields exist,
//! at what dotted paths, with what resolved kinds — plus a fingerprint that
//! identifies the plan the way an analysis fingerprint identifies an
//! analyzer. Documents are then decoded and read against that plan directly
//! as protobuf; nothing on this path is transcoded to JSON or any other
//! intermediate document model.
//!
//! Fields may carry explicit hints as descriptor options, using the
//! `ai.pipestream.proto.index.hints.v1` extension vendored from protomolt
//! (this module is the Rust equivalent of protomolt's
//! `IndexingPlanFactory` / `ProtoOptionsIndexingHintSource` /
//! `InferringIndexingHintSource` chain). Where a field carries no hint, its
//! kind is inferred from the descriptor with the same rules protomolt uses,
//! with one deliberate deviation: an unannotated singular message field is
//! expanded into dotted paths rather than kept as a single OBJECT entry,
//! because turbovec is a flat engine with no native object type. An
//! explicit OBJECT or NESTED hint still keeps the single entry.
//!
//! Everything ambiguous is an error, not a guess. A message with two
//! plausible vector fields, or no resolvable document id, fails derivation
//! with the hint the caller should add.

use prost::Message as _;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value,
};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;

use crate::hints;
use crate::proto::{
    stored_value, FieldKind, FieldRole, IndexSchema, PlannedField, StoredValue, StoredValueList,
};

/// Full name of the field-option extension carrying indexing hints, owned
/// by protomolt and vendored under `proto/ai/pipestream/`.
const HINT_EXTENSION_NAME: &str = "ai.pipestream.proto.index.hints.v1.index";

/// The extension's registered field number on `google.protobuf.FieldOptions`.
const HINT_EXTENSION_NUMBER: u32 = 59_100_471;

/// Nested messages deeper than this stop expanding and are recorded as a
/// single OBJECT entry, which also bounds recursive message types.
const MAX_DEPTH: usize = 8;

/// Version tag mixed into the canonical fingerprint bytes. Bump on any
/// change to derivation semantics or to the canonical encoding: a changed
/// fingerprint is how drift is caught at restore time.
const FINGERPRINT_VERSION: &str = "turbovec-schema/v2";

/// A derivation failure, with the dotted field path where it applies.
#[derive(Debug)]
pub struct SchemaError(String);

impl SchemaError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn at(path: &str, message: impl AsRef<str>) -> Self {
        Self(format!("{}: {}", path, message.as_ref()))
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaError {}

/// How the document id field's value becomes a `u64` label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DocIdSource {
    /// An unsigned or non-negative signed integer field, used verbatim.
    Integer,

    /// A string field, reduced to the first 8 bytes of SHA-256 over its
    /// UTF-8 bytes, big-endian. Deterministic and computable by any
    /// client; see [`hash_string_id`].
    HashedString,
}

/// One planned field whose value ingest keeps for filtering: its ordinal in
/// the plan and the navigation steps to its leaf.
pub struct StoredField {
    /// Index of this field in `IndexSchema.fields`. Covered by the schema
    /// fingerprint, so persisted values keyed by ordinal only ever pair
    /// with the plan they were written under.
    pub ordinal: u32,

    /// Field steps from the extraction root to the leaf. For parent-level
    /// fields the root is the bound message; for chunk-local fields it is
    /// the chunk message.
    steps: Vec<FieldDescriptor>,
}

/// A schema bound to an index: the derived plan plus everything needed to
/// decode and extract documents at ingest time.
pub struct BoundSchema {
    /// The derived plan, as returned on the wire.
    pub schema: IndexSchema,

    /// The registered descriptor set, byte for byte, so persistence and
    /// re-derivation always work from the caller's exact input.
    pub descriptor_set: Vec<u8>,

    /// Descriptor of the bound message type.
    message: MessageDescriptor,

    /// Field steps to the vector leaf. From the parent root when flat;
    /// from the chunk message when chunked.
    vector: Vec<FieldDescriptor>,

    /// Field steps from the parent root to the document id leaf.
    doc_id: Vec<FieldDescriptor>,

    /// How the id leaf's value becomes a parent label.
    doc_id_source: DocIdSource,

    /// Planned fields whose values are stored on every indexed row, in
    /// plan order. For a chunked schema this is parent scalars plus
    /// chunk scalars (the row denormalizes both).
    stored: Vec<StoredField>,

    /// Parent-level stored fields only. Empty when the schema is flat.
    /// Used to build the parent table beside the chunk rows.
    parent_stored: Vec<StoredField>,

    /// Chunk-local stored fields only. Empty when the schema is flat.
    chunk_stored: Vec<StoredField>,

    /// The repeated CHUNKS field on the parent, when the schema is chunked.
    chunks_field: Option<FieldDescriptor>,

    /// Field steps from the chunk message to the CHUNK_ID leaf, when set.
    chunk_id: Option<Vec<FieldDescriptor>>,

    /// Ordinal of the document id field in `IndexSchema.fields`.
    doc_id_ordinal: u32,

    /// Ordinal of the CHUNK_ID field in `IndexSchema.fields`, when set.
    chunk_id_ordinal: Option<u32>,
}

/// Everything extracted from one parent wire message ready for ingest.
#[derive(Debug)]
pub struct ExtractedIngest {
    /// Parent table entry when the schema is chunked; `None` when flat.
    pub parent: Option<ExtractedParent>,

    /// Indexed rows: one for a flat document, one per chunk when chunked.
    pub rows: Vec<ExtractedDocument>,
}

/// Parent-level fields for the parent table.
#[derive(Debug)]
pub struct ExtractedParent {
    /// Parent document id's u64 reduction.
    pub parent_label: u64,

    /// Parent-level stored field values.
    pub fields: HashMap<u32, StoredValue>,
}

/// One indexed row extracted from a decoded document.
#[derive(Debug)]
pub struct ExtractedDocument {
    /// The u64 label the row is indexed under.
    pub label: u64,

    /// Parent document id's u64 reduction. Equal to `label` when flat.
    pub parent_label: u64,

    /// The parent document id's original value, as the client indexed it.
    pub parent_id: String,

    /// Chunk id string; empty when the schema is flat.
    pub chunk_id: String,

    /// The row's vector, in document order.
    pub vector: Vec<f32>,

    /// Values of the stored fields, keyed by ordinal into
    /// `IndexSchema.fields`. Every stored field is present, defaults
    /// included, so filter evaluation never has to guess at absence.
    /// Chunk rows denormalize parent scalars onto the row.
    pub fields: HashMap<u32, StoredValue>,
}

impl fmt::Debug for BoundSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundSchema")
            .field("message_type", &self.schema.message_type)
            .field("fingerprint", &self.schema.fingerprint)
            .field("fields", &self.schema.fields.len())
            .finish()
    }
}

impl BoundSchema {
    /// Derive the plan for `message_type` inside `descriptor_set` and
    /// resolve the extraction paths ingest needs. Every ambiguity and
    /// every unsupported hint is an error here, before any index exists.
    pub fn derive(descriptor_set: &[u8], message_type: &str) -> Result<Self, SchemaError> {
        if message_type.is_empty() {
            return Err(SchemaError::new("message_type is required"));
        }
        if descriptor_set.is_empty() {
            return Err(SchemaError::new(
                "descriptor_set is required: a serialized google.protobuf.FileDescriptorSet \
                 with every import included",
            ));
        }
        let pool = DescriptorPool::decode(descriptor_set).map_err(|e| {
            SchemaError::new(format!(
                "descriptor_set does not decode as a complete FileDescriptorSet \
                 (compile with --include_imports): {e}"
            ))
        })?;
        let message = pool.get_message_by_name(message_type).ok_or_else(|| {
            SchemaError::new(format!(
                "message type {message_type:?} is not in the descriptor set; \
                 types present include e.g. {}",
                sample_types(&pool)
            ))
        })?;

        let mut fields = Vec::new();
        let mut visiting = Vec::new();
        walk(&message, "", "", 0, &mut fields, &mut visiting)?;
        if fields.is_empty() {
            return Err(SchemaError::new(format!(
                "message type {message_type} has no indexable fields"
            )));
        }

        let chunks_path = resolve_chunks(&fields)?;
        let vector_path = resolve_vector(&mut fields, chunks_path.as_deref())?;
        let doc_id_path = resolve_doc_id(&mut fields, chunks_path.as_deref())?;
        let chunk_id_path = resolve_chunk_id(&fields, chunks_path.as_deref())?;
        let dim = fields
            .iter()
            .find(|f| f.path == vector_path)
            .map_or(0, |f| f.vector_dims);

        let mut schema = IndexSchema {
            message_type: message_type.to_string(),
            fields,
            fingerprint: String::new(),
            vector_path: vector_path.clone(),
            doc_id_path: doc_id_path.clone(),
            dim,
        };
        schema.fingerprint = fingerprint(&schema);

        let doc_id = navigate(&message, &doc_id_path)?;
        let doc_id_source = match doc_id.last().expect("navigated path is non-empty").kind() {
            Kind::String => DocIdSource::HashedString,
            Kind::Int32
            | Kind::Int64
            | Kind::Uint32
            | Kind::Uint64
            | Kind::Sint32
            | Kind::Sint64
            | Kind::Fixed32
            | Kind::Fixed64
            | Kind::Sfixed32
            | Kind::Sfixed64 => DocIdSource::Integer,
            other => {
                return Err(SchemaError::at(
                    &doc_id_path,
                    format!("document id must be an integer or string field, not {other:?}"),
                ))
            }
        };

        let (chunks_field, vector, chunk_id, parent_stored, chunk_stored, stored) =
            if let Some(chunks_path) = chunks_path.as_deref() {
                let chunks_field = message.get_field_by_name(chunks_path).ok_or_else(|| {
                    SchemaError::at(chunks_path, "CHUNKS field missing from descriptor")
                })?;
                let chunk_message = match chunks_field.kind() {
                    Kind::Message(child) => child,
                    _ => {
                        return Err(SchemaError::at(
                            chunks_path,
                            "BLOCK_ROLE_CHUNKS requires a repeated message field",
                        ))
                    }
                };
                let vector_rel = strip_prefix(&vector_path, chunks_path)?;
                let vector = navigate(&chunk_message, &vector_rel)?;
                let chunk_id = match chunk_id_path.as_deref() {
                    Some(path) => {
                        let rel = strip_prefix(path, chunks_path)?;
                        Some(navigate(&chunk_message, &rel)?)
                    }
                    None => None,
                };
                let (parent_stored, chunk_stored, stored) =
                    stored_fields_chunked(&message, &chunk_message, &schema, chunks_path)?;
                (
                    Some(chunks_field),
                    vector,
                    chunk_id,
                    parent_stored,
                    chunk_stored,
                    stored,
                )
            } else {
                let vector = navigate(&message, &vector_path)?;
                let (stored, _) = stored_fields_flat(&message, &schema)?;
                (None, vector, None, Vec::new(), Vec::new(), stored)
            };

        let doc_id_ordinal = schema
            .fields
            .iter()
            .position(|f| f.role == FieldRole::DocId as i32)
            .expect("resolve_doc_id planned a doc id") as u32;
        let chunk_id_ordinal = schema
            .fields
            .iter()
            .position(|f| f.role == FieldRole::ChunkId as i32)
            .map(|i| i as u32);

        Ok(Self {
            schema,
            descriptor_set: descriptor_set.to_vec(),
            message,
            vector,
            doc_id,
            doc_id_source,
            stored,
            parent_stored,
            chunk_stored,
            chunks_field,
            chunk_id,
            doc_id_ordinal,
            chunk_id_ordinal,
        })
    }

    /// The bound message type's full name.
    pub fn message_type(&self) -> &str {
        &self.schema.message_type
    }

    /// True when the schema has a CHUNKS scope and indexes chunk rows.
    pub fn is_chunked(&self) -> bool {
        self.chunks_field.is_some()
    }

    /// The planned fields whose values ingest keeps on every row.
    pub fn stored_fields(&self) -> &[StoredField] {
        &self.stored
    }

    /// Ordinal of the document id field in `IndexSchema.fields`.
    pub fn doc_id_ordinal(&self) -> u32 {
        self.doc_id_ordinal
    }

    /// Ordinal of the CHUNK_ID field, when the schema declares one.
    pub fn chunk_id_ordinal(&self) -> Option<u32> {
        self.chunk_id_ordinal
    }

    /// Decode one serialized document of the bound type and extract the
    /// indexed rows (and parent record, when chunked).
    pub fn extract(&self, document: &[u8]) -> Result<ExtractedIngest, SchemaError> {
        let message = DynamicMessage::decode(self.message.clone(), document).map_err(|e| {
            SchemaError::new(format!(
                "document does not decode as {}: {e}",
                self.schema.message_type
            ))
        })?;

        let id_value = read_leaf(&message, &self.doc_id, &self.schema.doc_id_path)?;
        let (parent_label, parent_id) = match self.doc_id_source {
            DocIdSource::Integer => {
                let label = integer_id(&id_value, &self.schema.doc_id_path)?;
                (label, label.to_string())
            }
            DocIdSource::HashedString => {
                let text = id_value.as_str().ok_or_else(|| {
                    SchemaError::at(&self.schema.doc_id_path, "document id is not a string")
                })?;
                if text.is_empty() {
                    return Err(SchemaError::at(
                        &self.schema.doc_id_path,
                        "document id is empty; every document must carry a set id",
                    ));
                }
                (hash_string_id(text), text.to_string())
            }
        };

        if let Some(chunks_field) = &self.chunks_field {
            self.extract_chunked(&message, chunks_field, parent_label, parent_id)
        } else {
            self.extract_flat(&message, parent_label, parent_id)
        }
    }

    fn extract_flat(
        &self,
        message: &DynamicMessage,
        parent_label: u64,
        parent_id: String,
    ) -> Result<ExtractedIngest, SchemaError> {
        let vector = read_vector(
            message,
            &self.vector,
            &self.schema.vector_path,
            self.schema.dim,
        )?;
        let mut fields = HashMap::with_capacity(self.stored.len());
        for stored in &self.stored {
            let path = &self.schema.fields[stored.ordinal as usize].path;
            let value = read_leaf(message, &stored.steps, path)?;
            let leaf = stored.steps.last().expect("navigated path is non-empty");
            fields.insert(stored.ordinal, stored_value(&value, leaf, path)?);
        }
        Ok(ExtractedIngest {
            parent: None,
            rows: vec![ExtractedDocument {
                label: parent_label,
                parent_label,
                parent_id,
                chunk_id: String::new(),
                vector,
                fields,
            }],
        })
    }

    fn extract_chunked(
        &self,
        message: &DynamicMessage,
        chunks_field: &FieldDescriptor,
        parent_label: u64,
        parent_id: String,
    ) -> Result<ExtractedIngest, SchemaError> {
        let chunks_path = chunks_field.name();
        let list = match message.get_field(chunks_field).into_owned() {
            Value::List(list) => list,
            _ => {
                return Err(SchemaError::at(
                    chunks_path,
                    "CHUNKS field did not extract as a repeated message list",
                ))
            }
        };
        if list.is_empty() {
            return Err(SchemaError::at(
                chunks_path,
                "document has no chunks; every document must carry at least one",
            ));
        }

        let mut parent_fields = HashMap::with_capacity(self.parent_stored.len());
        for stored in &self.parent_stored {
            let path = &self.schema.fields[stored.ordinal as usize].path;
            let value = read_leaf(message, &stored.steps, path)?;
            let leaf = stored.steps.last().expect("navigated path is non-empty");
            parent_fields.insert(stored.ordinal, stored_value(&value, leaf, path)?);
        }

        let mut rows = Vec::with_capacity(list.len());
        for (ordinal, entry) in list.iter().enumerate() {
            let chunk = match entry {
                Value::Message(chunk) => chunk,
                other => {
                    return Err(SchemaError::at(
                        chunks_path,
                        format!("chunk {ordinal} is not a message: {other:?}"),
                    ))
                }
            };
            let vector = read_vector(
                chunk,
                &self.vector,
                &self.schema.vector_path,
                self.schema.dim,
            )?;
            let chunk_id = match &self.chunk_id {
                Some(steps) => {
                    let path = self
                        .schema
                        .fields
                        .iter()
                        .find(|f| f.role == FieldRole::ChunkId as i32)
                        .map(|f| f.path.as_str())
                        .unwrap_or("chunk_id");
                    let value = read_leaf(chunk, steps, path)?;
                    chunk_id_string(&value, path)?
                }
                None => ordinal.to_string(),
            };
            if chunk_id.is_empty() {
                return Err(SchemaError::at(
                    &format!("{chunks_path}[{ordinal}]"),
                    "chunk id is empty; every chunk must carry a set id",
                ));
            }
            let label = hash_chunk_label(&parent_id, &chunk_id);

            let mut fields = HashMap::with_capacity(self.stored.len());
            for (ordinal, value) in &parent_fields {
                fields.insert(*ordinal, value.clone());
            }
            for stored in &self.chunk_stored {
                let path = &self.schema.fields[stored.ordinal as usize].path;
                let value = read_leaf(chunk, &stored.steps, path)?;
                let leaf = stored.steps.last().expect("navigated path is non-empty");
                fields.insert(stored.ordinal, stored_value(&value, leaf, path)?);
            }

            rows.push(ExtractedDocument {
                label,
                parent_label,
                parent_id: parent_id.clone(),
                chunk_id,
                vector,
                fields,
            });
        }

        Ok(ExtractedIngest {
            parent: Some(ExtractedParent {
                parent_label,
                fields: parent_fields,
            }),
            rows,
        })
    }
}

fn read_vector(
    message: &DynamicMessage,
    steps: &[FieldDescriptor],
    path: &str,
    declared_dim: u32,
) -> Result<Vec<f32>, SchemaError> {
    let vector_value = read_leaf(message, steps, path)?;
    let list = vector_value
        .as_list()
        .ok_or_else(|| SchemaError::at(path, "vector field is not repeated"))?;
    if list.is_empty() {
        return Err(SchemaError::at(
            path,
            "document has an empty vector; every document must carry one",
        ));
    }
    let mut vector = Vec::with_capacity(list.len());
    for value in list {
        let coord = match value {
            Value::F32(v) => *v,
            Value::F64(v) => *v as f32,
            other => {
                return Err(SchemaError::at(
                    path,
                    format!("vector element is not a float: {other:?}"),
                ))
            }
        };
        vector.push(coord);
    }
    if declared_dim != 0 && vector.len() != declared_dim as usize {
        return Err(SchemaError::at(
            path,
            format!(
                "vector has {} coordinates, schema declares {declared_dim}",
                vector.len()
            ),
        ));
    }
    Ok(vector)
}

fn chunk_id_string(value: &Value, path: &str) -> Result<String, SchemaError> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::I32(v) => Ok(v.to_string()),
        Value::I64(v) => Ok(v.to_string()),
        Value::U32(v) => Ok(v.to_string()),
        Value::U64(v) => Ok(v.to_string()),
        other => Err(SchemaError::at(
            path,
            format!("chunk id must be a string or integer, not {other:?}"),
        )),
    }
}

/// Select stored fields for a flat (non-chunked) schema.
fn stored_fields_flat(
    root: &MessageDescriptor,
    schema: &IndexSchema,
) -> Result<(Vec<StoredField>, u32), SchemaError> {
    let mut stored = Vec::new();
    let mut doc_id_ordinal = None;
    for (ordinal, field) in schema.fields.iter().enumerate() {
        let kind = FieldKind::try_from(field.kind).expect("derived plans hold known kinds");
        if matches!(
            kind,
            FieldKind::Vector | FieldKind::Object | FieldKind::Nested | FieldKind::Unspecified
        ) {
            continue;
        }
        if field.role == FieldRole::Chunks as i32 {
            continue;
        }
        let ordinal = u32::try_from(ordinal).expect("a plan holds far fewer than 2^32 fields");
        if field.role == FieldRole::DocId as i32 {
            doc_id_ordinal = Some(ordinal);
        }
        stored.push(StoredField {
            ordinal,
            steps: navigate(root, &field.path)?,
        });
    }
    let doc_id_ordinal =
        doc_id_ordinal.expect("resolve_doc_id planned a storable doc id field before this ran");
    Ok((stored, doc_id_ordinal))
}

/// Select parent-level and chunk-local stored fields for a chunked schema.
type StoredFieldTriple = (Vec<StoredField>, Vec<StoredField>, Vec<StoredField>);

fn stored_fields_chunked(
    root: &MessageDescriptor,
    chunk: &MessageDescriptor,
    schema: &IndexSchema,
    chunks_path: &str,
) -> Result<StoredFieldTriple, SchemaError> {
    let prefix = format!("{chunks_path}.");
    let mut parent_stored = Vec::new();
    let mut chunk_stored = Vec::new();
    for (ordinal, field) in schema.fields.iter().enumerate() {
        let kind = FieldKind::try_from(field.kind).expect("derived plans hold known kinds");
        if matches!(
            kind,
            FieldKind::Vector | FieldKind::Object | FieldKind::Nested | FieldKind::Unspecified
        ) {
            continue;
        }
        if field.role == FieldRole::Chunks as i32 {
            continue;
        }
        let ordinal = u32::try_from(ordinal).expect("a plan holds far fewer than 2^32 fields");
        if let Some(rel) = field.path.strip_prefix(&prefix) {
            chunk_stored.push(StoredField {
                ordinal,
                steps: navigate(chunk, rel)?,
            });
        } else if field.path != chunks_path {
            parent_stored.push(StoredField {
                ordinal,
                steps: navigate(root, &field.path)?,
            });
        }
    }
    let stored: Vec<StoredField> = parent_stored
        .iter()
        .chain(chunk_stored.iter())
        .map(|f| StoredField {
            ordinal: f.ordinal,
            steps: f.steps.clone(),
        })
        .collect();
    Ok((parent_stored, chunk_stored, stored))
}

/// Convert one extracted leaf value into its stored form. The descriptor
/// guarantees the value's shape, so a mismatch here is a bug, not bad
/// input — except a Timestamp, whose seconds/nanos any client can set.
fn stored_value(
    value: &Value,
    leaf: &FieldDescriptor,
    path: &str,
) -> Result<StoredValue, SchemaError> {
    if leaf.is_list() {
        let list = value
            .as_list()
            .ok_or_else(|| SchemaError::at(path, "a repeated field did not extract as a list"))?;
        let mut values = Vec::with_capacity(list.len());
        for element in list {
            values.push(stored_scalar(element, leaf, path)?);
        }
        return Ok(StoredValue {
            value: Some(stored_value::Value::ListValue(StoredValueList { values })),
        });
    }
    stored_scalar(value, leaf, path)
}

fn stored_scalar(
    value: &Value,
    leaf: &FieldDescriptor,
    path: &str,
) -> Result<StoredValue, SchemaError> {
    use stored_value::Value as V;
    let stored = match value {
        Value::String(text) => V::StringValue(text.clone()),
        Value::Bool(v) => V::BoolValue(*v),
        Value::I32(v) => V::IntValue(i64::from(*v)),
        Value::I64(v) => V::IntValue(*v),
        Value::U32(v) => V::UintValue(u64::from(*v)),
        Value::U64(v) => V::UintValue(*v),
        Value::F32(v) => V::DoubleValue(f64::from(*v)),
        Value::F64(v) => V::DoubleValue(*v),
        Value::Bytes(bytes) => V::BytesValue(bytes.to_vec()),
        Value::EnumNumber(number) => V::StringValue(enum_value_name(leaf, *number)),
        Value::Message(message) => V::TimestampValue(timestamp_of(message, path)?),
        other => {
            return Err(SchemaError::at(
                path,
                format!("stored field extracted an unsupported value: {other:?}"),
            ))
        }
    };
    Ok(StoredValue {
        value: Some(stored),
    })
}

/// An enum field stores its value's declared name, so filters read
/// `status == "STATUS_ACTIVE"` rather than magic numbers. proto3 enums are
/// open, so an unknown number (from a newer producer) stores as its
/// decimal rendering rather than failing ingest.
fn enum_value_name(leaf: &FieldDescriptor, number: i32) -> String {
    match leaf.kind() {
        Kind::Enum(descriptor) => descriptor
            .get_value(number)
            .map(|v| v.name().to_string())
            .unwrap_or_else(|| number.to_string()),
        _ => number.to_string(),
    }
}

/// Read a google.protobuf.Timestamp leaf into the stored form, verbatim.
/// The plan only assigns FIELD_KIND_DATE to Timestamp fields, so the
/// message shape is guaranteed; the range is validated here, at ingest,
/// so filter evaluation later never meets a timestamp chrono cannot hold.
fn timestamp_of(
    message: &DynamicMessage,
    path: &str,
) -> Result<::prost_types::Timestamp, SchemaError> {
    let seconds = message
        .get_field_by_name("seconds")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| SchemaError::at(path, "Timestamp has no seconds field"))?;
    let nanos = message
        .get_field_by_name("nanos")
        .and_then(|v| v.as_i32())
        .ok_or_else(|| SchemaError::at(path, "Timestamp has no nanos field"))?;
    let nanos_u32 = u32::try_from(nanos)
        .map_err(|_| SchemaError::at(path, format!("timestamp nanos {nanos} is negative")))?;
    if chrono::DateTime::from_timestamp(seconds, nanos_u32).is_none() {
        return Err(SchemaError::at(
            path,
            format!("timestamp seconds={seconds} nanos={nanos} is out of range"),
        ));
    }
    Ok(::prost_types::Timestamp { seconds, nanos })
}

/// Reduce a string document id to a `u64` label: the first 8 bytes of
/// SHA-256 over the UTF-8 bytes, big-endian. Part of the wire contract, so
/// any client can compute the label its documents will carry.
pub fn hash_string_id(id: &str) -> u64 {
    let digest = Sha256::digest(id.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 yields 32 bytes"))
}

/// Reduce a chunk row's identity to a `u64` label: the first 8 bytes of
/// SHA-256 over `turbovec-chunk-label/v1\0{parent_id}\0{chunk_id}`,
/// big-endian. Distinct from [`hash_string_id`] so a parent id never
/// collides with one of its chunk rows. Part of the wire contract.
pub fn hash_chunk_label(parent_id: &str, chunk_id: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"turbovec-chunk-label/v1\0");
    hasher.update(parent_id.as_bytes());
    hasher.update([0]);
    hasher.update(chunk_id.as_bytes());
    let digest = hasher.finalize();
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 yields 32 bytes"))
}

/// A hint after merging: the resolved kind, whether it was explicit, and
/// the attributes turbovec records.
struct ResolvedHint {
    kind: FieldKind,
    explicit_kind: bool,
    name_override: String,
    role: FieldRole,
    vector_dims: u32,
    analyzer: String,
    search_analyzer: String,
    skip: bool,
}

/// Walk one message's fields, appending planned fields in declaration
/// order. `visiting` holds the message names on the current branch, so a
/// recursive type stops expanding instead of looping.
fn walk(
    message: &MessageDescriptor,
    path_prefix: &str,
    name_prefix: &str,
    depth: usize,
    out: &mut Vec<PlannedField>,
    visiting: &mut Vec<String>,
) -> Result<(), SchemaError> {
    visiting.push(message.full_name().to_string());
    for field in message.fields() {
        let path = join_path(path_prefix, field.name());
        let hint = resolve_hint(&field, &path)?;
        if hint.skip {
            continue;
        }
        let qualified = if name_prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{name_prefix}_{}", field.name())
        };
        let name = if hint.name_override.is_empty() {
            qualified
        } else {
            hint.name_override.clone()
        };
        validate_hint(&field, &hint, &path)?;

        if let Kind::Message(child) = field.kind() {
            let blocked = depth >= MAX_DEPTH || visiting.iter().any(|n| n == child.full_name());
            if hint.role == FieldRole::Chunks {
                // The chunk scope keeps its container entry and expands its
                // children as unprefixed fields: within a block the children
                // are their own documents, not properties of the parent.
                out.push(planned(&path, &name, &field, &hint));
                if !blocked {
                    walk(&child, &path, "", depth + 1, out, visiting)?;
                }
                continue;
            }
            let expandable = !field.is_list()
                && !field.is_map()
                && !well_known_leaf(&child)
                && !matches!(hint.kind, FieldKind::Object | FieldKind::Nested if hint.explicit_kind);
            if expandable && !blocked {
                walk(&child, &path, &name, depth + 1, out, visiting)?;
                continue;
            }
        }
        out.push(planned(&path, &name, &field, &hint));
    }
    visiting.pop();
    Ok(())
}

fn planned(path: &str, name: &str, field: &FieldDescriptor, hint: &ResolvedHint) -> PlannedField {
    PlannedField {
        path: path.to_string(),
        name: name.to_string(),
        kind: hint.kind as i32,
        repeated: field.is_list() || field.is_map(),
        role: hint.role as i32,
        vector_dims: hint.vector_dims,
        analyzer: hint.analyzer.clone(),
        search_analyzer: hint.search_analyzer.clone(),
    }
}

/// Resolve one field's hint: the explicit `(index)` option when present,
/// inference otherwise. An explicit hint with an unset type still infers
/// the kind while its other attributes win, matching protomolt.
fn resolve_hint(field: &FieldDescriptor, path: &str) -> Result<ResolvedHint, SchemaError> {
    let Some(hint) = explicit_hint(field, path)? else {
        return Ok(inferred_hint(field));
    };
    let (kind, explicit_kind, skip) = match hints::IndexFieldType::try_from(hint.r#type) {
        Ok(hints::IndexFieldType::Unspecified) => {
            let inferred = inferred_hint(field);
            (inferred.kind, false, false)
        }
        Ok(hints::IndexFieldType::Skip) => (FieldKind::Unspecified, true, true),
        Ok(explicit) => (convert_kind(explicit, path)?, true, false),
        Err(_) => {
            return Err(SchemaError::at(
                path,
                format!("hint declares unknown index type {}", hint.r#type),
            ))
        }
    };
    let role = match hints::BlockRole::try_from(hint.block_role) {
        Ok(hints::BlockRole::Unspecified) => FieldRole::None,
        Ok(hints::BlockRole::Chunks) => FieldRole::Chunks,
        Ok(hints::BlockRole::DocId) => FieldRole::DocId,
        Ok(hints::BlockRole::ChunkId) => FieldRole::ChunkId,
        Err(_) => {
            return Err(SchemaError::at(
                path,
                format!("hint declares unknown block role {}", hint.block_role),
            ))
        }
    };
    if hint.chunk_recipe.is_some() {
        return Err(SchemaError::at(
            path,
            "chunk_recipe hints are not supported by this engine yet; \
             chunk and embed before ingest",
        ));
    }
    Ok(ResolvedHint {
        kind,
        explicit_kind,
        name_override: hint.name,
        role,
        vector_dims: u32::try_from(hint.vector_dims.max(0)).expect("clamped to non-negative"),
        analyzer: hint.analyzer.unwrap_or_default(),
        search_analyzer: hint.search_analyzer.unwrap_or_default(),
        skip,
    })
}

/// Read the `(ai.pipestream.proto.index.hints.v1.index)` extension off one
/// field's options, when the caller's descriptor set declares it and the
/// field sets it. The dynamic payload is transcoded into the vendored
/// generated type, so unknown future attributes are tolerated and known
/// ones are read with real field types.
fn explicit_hint(
    field: &FieldDescriptor,
    path: &str,
) -> Result<Option<hints::FieldIndexHint>, SchemaError> {
    let options = field.options();
    for extension in options.extensions() {
        let (descriptor, value) = extension;
        if descriptor.full_name() != HINT_EXTENSION_NAME {
            continue;
        }
        if descriptor.number() != HINT_EXTENSION_NUMBER {
            return Err(SchemaError::at(
                path,
                format!(
                    "extension {HINT_EXTENSION_NAME} is declared with number {}, \
                     expected {HINT_EXTENSION_NUMBER}; the descriptor set carries a \
                     modified copy of indexing_hints.proto",
                    descriptor.number()
                ),
            ));
        }
        let message = value.as_message().ok_or_else(|| {
            SchemaError::at(path, "the (index) hint option is not a message value")
        })?;
        let bytes = message.encode_to_vec();
        let hint = hints::FieldIndexHint::decode(bytes.as_slice())
            .map_err(|e| SchemaError::at(path, format!("hint does not decode: {e}")))?;
        return Ok(Some(hint));
    }
    Ok(None)
}

/// Infer a hint from the descriptor alone, with protomolt's rules: strings
/// whose names look like identifiers become KEYWORD, Timestamp becomes
/// DATE, Struct/Value stay OBJECT, repeated messages stay NESTED.
fn inferred_hint(field: &FieldDescriptor) -> ResolvedHint {
    let kind = match field.kind() {
        Kind::String => {
            if looks_like_keyword(field.name()) {
                FieldKind::Keyword
            } else {
                FieldKind::Text
            }
        }
        Kind::Bool => FieldKind::Boolean,
        Kind::Int32 | Kind::Uint32 | Kind::Sint32 | Kind::Fixed32 | Kind::Sfixed32 => {
            FieldKind::Int32
        }
        Kind::Int64 | Kind::Uint64 | Kind::Sint64 | Kind::Fixed64 | Kind::Sfixed64 => {
            FieldKind::Int64
        }
        Kind::Float => FieldKind::Float,
        Kind::Double => FieldKind::Double,
        Kind::Bytes => FieldKind::Binary,
        Kind::Enum(_) => FieldKind::Keyword,
        Kind::Message(message) => match message.full_name() {
            "google.protobuf.Timestamp" => FieldKind::Date,
            "google.protobuf.Struct" | "google.protobuf.Value" => FieldKind::Object,
            _ if field.is_list() || field.is_map() => FieldKind::Nested,
            _ => FieldKind::Object,
        },
    };
    ResolvedHint {
        kind,
        explicit_kind: false,
        name_override: String::new(),
        role: FieldRole::None,
        vector_dims: 0,
        analyzer: String::new(),
        search_analyzer: String::new(),
        skip: false,
    }
}

/// protomolt's keyword-name heuristic, verbatim: identifier-shaped names
/// index as exact values rather than analyzed text.
fn looks_like_keyword(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "id"
        || name.ends_with("_id")
        || (name.ends_with("id") && name.len() <= 4)
        || name.ends_with("_key")
        || name.ends_with("_code")
        || name == "uri"
        || name.ends_with("_uri")
        || name == "status"
        || name == "type"
        || name.ends_with("_type")
}

fn convert_kind(hint: hints::IndexFieldType, path: &str) -> Result<FieldKind, SchemaError> {
    use hints::IndexFieldType as H;
    Ok(match hint {
        H::Text => FieldKind::Text,
        H::Keyword => FieldKind::Keyword,
        H::Int32 => FieldKind::Int32,
        H::Int64 => FieldKind::Int64,
        H::Float => FieldKind::Float,
        H::Double => FieldKind::Double,
        H::Boolean => FieldKind::Boolean,
        H::Date => FieldKind::Date,
        H::Binary => FieldKind::Binary,
        H::Vector => FieldKind::Vector,
        H::Object => FieldKind::Object,
        H::Nested => FieldKind::Nested,
        H::IntRange | H::LongRange | H::FloatRange | H::DoubleRange | H::DateRange => {
            return Err(SchemaError::at(
                path,
                "range hints are not supported by this engine yet",
            ))
        }
        H::Unspecified | H::Skip => unreachable!("handled by resolve_hint"),
    })
}

/// Hints that cannot possibly apply to the field they sit on fail here,
/// with the path, before any plan is returned.
fn validate_hint(
    field: &FieldDescriptor,
    hint: &ResolvedHint,
    path: &str,
) -> Result<(), SchemaError> {
    if hint.kind == FieldKind::Vector {
        let element_ok = matches!(field.kind(), Kind::Float | Kind::Double);
        if !field.is_list() || !element_ok {
            return Err(SchemaError::at(
                path,
                "a VECTOR hint requires a repeated float or repeated double field",
            ));
        }
    }
    if hint.role == FieldRole::Chunks {
        let is_message = matches!(field.kind(), Kind::Message(_));
        if !field.is_list() || field.is_map() || !is_message {
            return Err(SchemaError::at(
                path,
                "BLOCK_ROLE_CHUNKS requires a repeated message field",
            ));
        }
    }
    if hint.role == FieldRole::DocId || hint.role == FieldRole::ChunkId {
        let role_name = if hint.role == FieldRole::DocId {
            "BLOCK_ROLE_DOC_ID"
        } else {
            "BLOCK_ROLE_CHUNK_ID"
        };
        if field.is_list() || field.is_map() {
            return Err(SchemaError::at(
                path,
                format!("{role_name} requires a singular field"),
            ));
        }
        let id_ok = matches!(
            field.kind(),
            Kind::String
                | Kind::Int32
                | Kind::Int64
                | Kind::Uint32
                | Kind::Uint64
                | Kind::Sint32
                | Kind::Sint64
                | Kind::Fixed32
                | Kind::Fixed64
                | Kind::Sfixed32
                | Kind::Sfixed64
        );
        if !id_ok {
            return Err(SchemaError::at(
                path,
                format!("{role_name} requires an integer or string field"),
            ));
        }
    }
    Ok(())
}

/// At most one CHUNKS scope per schema.
fn resolve_chunks(fields: &[PlannedField]) -> Result<Option<String>, SchemaError> {
    let chunks: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.role == FieldRole::Chunks as i32)
        .collect();
    match chunks.len() {
        0 => Ok(None),
        1 => Ok(Some(chunks[0].path.clone())),
        _ => Err(SchemaError::new(format!(
            "the schema hints {} CHUNKS fields ({}); a document has at most one chunk scope",
            chunks.len(),
            paths(&chunks)
        ))),
    }
}

/// Pick the plan's vector field. An explicit VECTOR hint wins; without
/// one, exactly one repeated float/double field with a vector-shaped name
/// is accepted and its planned kind is rewritten to VECTOR. Anything else
/// is an error naming the fix.
///
/// When the schema has a CHUNKS scope the vector must live inside it
/// (each chunk is a searchable row). When there is no CHUNKS scope the
/// vector must not pass through one.
fn resolve_vector(
    fields: &mut [PlannedField],
    chunks_path: Option<&str>,
) -> Result<String, SchemaError> {
    let explicit: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.kind == FieldKind::Vector as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let candidates: Vec<usize> = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    f.repeated
                        && (f.kind == FieldKind::Float as i32 || f.kind == FieldKind::Double as i32)
                        && vector_shaped_name(f.path.rsplit('.').next().unwrap_or(&f.path))
                })
                .map(|(i, _)| i)
                .collect();
            match candidates.len() {
                1 => {
                    let index = candidates[0];
                    fields[index].kind = FieldKind::Vector as i32;
                    fields[index].path.clone()
                }
                0 => {
                    return Err(SchemaError::new(
                        "no vector field: hint exactly one repeated float field with \
                         (ai.pipestream.proto.index.hints.v1.index).type = INDEX_FIELD_TYPE_VECTOR, \
                         or name it vector/embedding",
                    ))
                }
                _ => {
                    let named: Vec<&PlannedField> =
                        candidates.iter().map(|&i| &fields[i]).collect();
                    return Err(SchemaError::new(format!(
                        "several fields look like the vector ({}); hint the intended one with \
                         (ai.pipestream.proto.index.hints.v1.index).type = INDEX_FIELD_TYPE_VECTOR",
                        paths(&named)
                    )));
                }
            }
        }
        _ => {
            return Err(SchemaError::new(format!(
                "the schema hints {} VECTOR fields ({}); an index is built over exactly one",
                explicit.len(),
                paths(&explicit)
            )))
        }
    };
    validate_vector_scope(&path, chunks_path)?;
    Ok(path)
}

/// Pick the plan's document id field. An explicit DOC_ID role wins;
/// without one, a singular top-level field named "id" is accepted and its
/// planned role is rewritten. The document id always lives on the parent,
/// never inside the CHUNKS scope.
fn resolve_doc_id(
    fields: &mut [PlannedField],
    chunks_path: Option<&str>,
) -> Result<String, SchemaError> {
    let explicit: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.role == FieldRole::DocId as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let fallback = fields.iter().position(|f| {
                f.path == "id"
                    && !f.repeated
                    && (f.kind == FieldKind::Keyword as i32
                        || f.kind == FieldKind::Text as i32
                        || f.kind == FieldKind::Int32 as i32
                        || f.kind == FieldKind::Int64 as i32)
            });
            match fallback {
                Some(index) => {
                    fields[index].role = FieldRole::DocId as i32;
                    fields[index].path.clone()
                }
                None => {
                    return Err(SchemaError::new(
                        "no document id field: hint exactly one integer or string field with \
                         (ai.pipestream.proto.index.hints.v1.index).block_role = BLOCK_ROLE_DOC_ID, \
                         or declare a singular top-level field named \"id\"",
                    ))
                }
            }
        }
        _ => {
            return Err(SchemaError::new(format!(
                "the schema hints {} DOC_ID fields ({}); a document has exactly one identity",
                explicit.len(),
                paths(&explicit)
            )))
        }
    };
    if let Some(chunks) = chunks_path {
        if path == chunks || path.starts_with(&format!("{chunks}.")) {
            return Err(SchemaError::at(
                &path,
                "the document id field cannot live inside the CHUNKS scope",
            ));
        }
    }
    let target = fields
        .iter()
        .find(|f| f.path == path)
        .expect("path came from this plan");
    if target.repeated {
        return Err(SchemaError::at(
            &path,
            "the document id field must be singular",
        ));
    }
    Ok(path)
}

/// Optional CHUNK_ID inside the CHUNKS scope.
fn resolve_chunk_id(
    fields: &[PlannedField],
    chunks_path: Option<&str>,
) -> Result<Option<String>, SchemaError> {
    let ids: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.role == FieldRole::ChunkId as i32)
        .collect();
    match (chunks_path, ids.len()) {
        (_, 0) => Ok(None),
        (None, _) => Err(SchemaError::new(format!(
            "CHUNK_ID fields ({}) require a CHUNKS scope",
            paths(&ids)
        ))),
        (Some(chunks), 1) => {
            let path = &ids[0].path;
            if !path.starts_with(&format!("{chunks}.")) {
                return Err(SchemaError::at(
                    path,
                    "CHUNK_ID must live inside the CHUNKS scope",
                ));
            }
            if ids[0].repeated {
                return Err(SchemaError::at(path, "CHUNK_ID must be singular"));
            }
            Ok(Some(path.clone()))
        }
        (Some(_), n) => Err(SchemaError::new(format!(
            "the schema hints {n} CHUNK_ID fields ({}); a chunk has at most one identity",
            paths(&ids)
        ))),
    }
}

fn validate_vector_scope(path: &str, chunks_path: Option<&str>) -> Result<(), SchemaError> {
    if let Some(chunks) = chunks_path {
        if !path.starts_with(&format!("{chunks}.")) {
            return Err(SchemaError::at(
                path,
                "the vector field must live inside the CHUNKS scope when the schema \
                 declares one; each chunk is a searchable row",
            ));
        }
    }
    Ok(())
}

fn strip_prefix(path: &str, chunks_path: &str) -> Result<String, SchemaError> {
    let prefix = format!("{chunks_path}.");
    path.strip_prefix(&prefix)
        .map(|s| s.to_string())
        .ok_or_else(|| {
            SchemaError::at(
                path,
                format!("expected a path under the CHUNKS scope {chunks_path}"),
            )
        })
}

fn vector_shaped_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "vector"
        || name == "embedding"
        || name.ends_with("_vector")
        || name.ends_with("_embedding")
}

fn paths(fields: &[&PlannedField]) -> String {
    fields
        .iter()
        .map(|f| f.path.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}.{segment}")
    }
}

/// Well-known message types that plan as leaves rather than expanding.
fn well_known_leaf(message: &MessageDescriptor) -> bool {
    matches!(
        message.full_name(),
        "google.protobuf.Timestamp" | "google.protobuf.Struct" | "google.protobuf.Value"
    )
}

/// Resolve a dotted path into field steps, requiring every intermediate
/// step to be a singular message field.
fn navigate(root: &MessageDescriptor, path: &str) -> Result<Vec<FieldDescriptor>, SchemaError> {
    let mut steps = Vec::new();
    let mut current = root.clone();
    let segments: Vec<&str> = path.split('.').collect();
    for (position, segment) in segments.iter().enumerate() {
        let field = current.get_field_by_name(segment).ok_or_else(|| {
            SchemaError::at(
                path,
                format!("{} has no field {segment:?}", current.full_name()),
            )
        })?;
        if position + 1 < segments.len() {
            if field.is_list() || field.is_map() {
                return Err(SchemaError::at(
                    path,
                    format!("segment {segment:?} is repeated; the path must be singular"),
                ));
            }
            match field.kind() {
                Kind::Message(child) => current = child,
                _ => {
                    return Err(SchemaError::at(
                        path,
                        format!("segment {segment:?} is not a message field"),
                    ))
                }
            }
        }
        steps.push(field);
    }
    Ok(steps)
}

/// Read the value at a navigated path out of one decoded document.
fn read_leaf(
    message: &DynamicMessage,
    steps: &[FieldDescriptor],
    path: &str,
) -> Result<Value, SchemaError> {
    let (leaf, intermediate) = steps.split_last().expect("navigated path is non-empty");
    let mut current = message.clone();
    for step in intermediate {
        match current.get_field(step).into_owned() {
            Value::Message(next) => current = next,
            _ => {
                return Err(SchemaError::at(
                    path,
                    format!("segment {:?} is not set to a message", step.name()),
                ))
            }
        }
    }
    Ok(current.get_field(leaf).into_owned())
}

fn integer_id(value: &Value, path: &str) -> Result<u64, SchemaError> {
    let id = match *value {
        Value::U32(v) => u64::from(v),
        Value::U64(v) => v,
        Value::I32(v) => u64::try_from(v)
            .map_err(|_| SchemaError::at(path, format!("document id {v} is negative")))?,
        Value::I64(v) => u64::try_from(v)
            .map_err(|_| SchemaError::at(path, format!("document id {v} is negative")))?,
        ref other => {
            return Err(SchemaError::at(
                path,
                format!("document id is not an integer: {other:?}"),
            ))
        }
    };
    if id == 0 {
        return Err(SchemaError::at(
            path,
            "document id is 0, which proto3 cannot distinguish from unset; \
             every document must carry a set, non-zero id",
        ));
    }
    Ok(id)
}

fn sample_types(pool: &DescriptorPool) -> String {
    let mut names: Vec<String> = pool
        .all_messages()
        .filter(|m| !m.full_name().starts_with("google.protobuf."))
        .map(|m| m.full_name().to_string())
        .take(5)
        .collect();
    names.sort_unstable();
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}

/// Canonical fingerprint over the derived plan: a fixed-layout byte
/// encoding (never protobuf serialization, whose byte layout is not
/// canonical) hashed with SHA-256 and rendered as lowercase hex.
fn fingerprint(schema: &IndexSchema) -> String {
    let mut hasher = Sha256::new();
    write_str(&mut hasher, FINGERPRINT_VERSION);
    write_str(&mut hasher, &schema.message_type);
    write_str(&mut hasher, &schema.vector_path);
    write_str(&mut hasher, &schema.doc_id_path);
    hasher.update(schema.dim.to_le_bytes());
    hasher.update((schema.fields.len() as u32).to_le_bytes());
    for field in &schema.fields {
        write_str(&mut hasher, &field.path);
        write_str(&mut hasher, &field.name);
        hasher.update(field.kind.to_le_bytes());
        hasher.update([u8::from(field.repeated)]);
        hasher.update(field.role.to_le_bytes());
        hasher.update(field.vector_dims.to_le_bytes());
        write_str(&mut hasher, &field.analyzer);
        write_str(&mut hasher, &field.search_analyzer);
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn write_str(hasher: &mut Sha256, text: &str) {
    hasher.update((text.len() as u32).to_le_bytes());
    hasher.update(text.as_bytes());
}
