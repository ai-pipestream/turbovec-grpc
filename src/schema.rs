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
use std::fmt;

use crate::hints;
use crate::proto::{FieldKind, FieldRole, IndexSchema, PlannedField};

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
const FINGERPRINT_VERSION: &str = "turbovec-schema/v1";

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

    /// Field steps from the root to the vector leaf.
    vector: Vec<FieldDescriptor>,

    /// Field steps from the root to the document id leaf.
    doc_id: Vec<FieldDescriptor>,

    /// How the id leaf's value becomes a label.
    doc_id_source: DocIdSource,
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

        let vector_path = resolve_vector(&mut fields)?;
        let doc_id_path = resolve_doc_id(&mut fields)?;
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

        let vector = navigate(&message, &vector_path)?;
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

        Ok(Self {
            schema,
            descriptor_set: descriptor_set.to_vec(),
            message,
            vector,
            doc_id,
            doc_id_source,
        })
    }

    /// The bound message type's full name.
    pub fn message_type(&self) -> &str {
        &self.schema.message_type
    }

    /// Decode one serialized document of the bound type and extract its
    /// `(label, vector)` pair per the plan. Anything missing or malformed
    /// is an error naming the path; the caller adds the document's
    /// position in the stream.
    pub fn extract(&self, document: &[u8]) -> Result<(u64, Vec<f32>), SchemaError> {
        let message = DynamicMessage::decode(self.message.clone(), document).map_err(|e| {
            SchemaError::new(format!(
                "document does not decode as {}: {e}",
                self.schema.message_type
            ))
        })?;

        let id_value = read_leaf(&message, &self.doc_id, &self.schema.doc_id_path)?;
        let label = match self.doc_id_source {
            DocIdSource::Integer => integer_id(&id_value, &self.schema.doc_id_path)?,
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
                hash_string_id(text)
            }
        };

        let vector_value = read_leaf(&message, &self.vector, &self.schema.vector_path)?;
        let list = vector_value.as_list().ok_or_else(|| {
            SchemaError::at(&self.schema.vector_path, "vector field is not repeated")
        })?;
        if list.is_empty() {
            return Err(SchemaError::at(
                &self.schema.vector_path,
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
                        &self.schema.vector_path,
                        format!("vector element is not a float: {other:?}"),
                    ))
                }
            };
            vector.push(coord);
        }
        if self.schema.dim != 0 && vector.len() != self.schema.dim as usize {
            return Err(SchemaError::at(
                &self.schema.vector_path,
                format!(
                    "vector has {} coordinates, schema declares {}",
                    vector.len(),
                    self.schema.dim
                ),
            ));
        }
        Ok((label, vector))
    }
}

/// Reduce a string document id to a `u64` label: the first 8 bytes of
/// SHA-256 over the UTF-8 bytes, big-endian. Part of the wire contract, so
/// any client can compute the label its documents will carry.
pub fn hash_string_id(id: &str) -> u64 {
    let digest = Sha256::digest(id.as_bytes());
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
    if hint.role == FieldRole::DocId {
        if field.is_list() || field.is_map() {
            return Err(SchemaError::at(
                path,
                "BLOCK_ROLE_DOC_ID requires a singular field",
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
                "BLOCK_ROLE_DOC_ID requires an integer or string field",
            ));
        }
    }
    Ok(())
}

/// Pick the plan's vector field. An explicit VECTOR hint wins; without
/// one, exactly one repeated float/double field with a vector-shaped name
/// is accepted and its planned kind is rewritten to VECTOR. Anything else
/// is an error naming the fix.
fn resolve_vector(fields: &mut [PlannedField]) -> Result<String, SchemaError> {
    let explicit: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.kind == FieldKind::Vector as i32)
        .collect();
    match explicit.len() {
        1 => return validated_scope(explicit[0].path.clone(), fields, "vector"),
        0 => {}
        _ => {
            return Err(SchemaError::new(format!(
                "the schema hints {} VECTOR fields ({}); an index is built over exactly one",
                explicit.len(),
                paths(&explicit)
            )))
        }
    }
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
            validated_scope(fields[index].path.clone(), fields, "vector")
        }
        0 => Err(SchemaError::new(
            "no vector field: hint exactly one repeated float field with \
             (ai.pipestream.proto.index.hints.v1.index).type = INDEX_FIELD_TYPE_VECTOR, \
             or name it vector/embedding",
        )),
        _ => {
            let named: Vec<&PlannedField> = candidates.iter().map(|&i| &fields[i]).collect();
            Err(SchemaError::new(format!(
                "several fields look like the vector ({}); hint the intended one with \
                 (ai.pipestream.proto.index.hints.v1.index).type = INDEX_FIELD_TYPE_VECTOR",
                paths(&named)
            )))
        }
    }
}

/// Pick the plan's document id field. An explicit DOC_ID role wins;
/// without one, a singular top-level field named "id" is accepted and its
/// planned role is rewritten. Anything else is an error naming the fix.
fn resolve_doc_id(fields: &mut [PlannedField]) -> Result<String, SchemaError> {
    let explicit: Vec<&PlannedField> = fields
        .iter()
        .filter(|f| f.role == FieldRole::DocId as i32)
        .collect();
    match explicit.len() {
        1 => return validated_scope(explicit[0].path.clone(), fields, "document id"),
        0 => {}
        _ => {
            return Err(SchemaError::new(format!(
                "the schema hints {} DOC_ID fields ({}); a document has exactly one identity",
                explicit.len(),
                paths(&explicit)
            )))
        }
    }
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
            Ok(fields[index].path.clone())
        }
        None => Err(SchemaError::new(
            "no document id field: hint exactly one integer or string field with \
             (ai.pipestream.proto.index.hints.v1.index).block_role = BLOCK_ROLE_DOC_ID, \
             or declare a singular top-level field named \"id\"",
        )),
    }
}

/// The vector and the document id identify the whole document, so their
/// paths must not pass through a repeated or CHUNKS scope, where one
/// document would have several values.
fn validated_scope(
    path: String,
    fields: &[PlannedField],
    what: &str,
) -> Result<String, SchemaError> {
    let target = fields
        .iter()
        .find(|f| f.path == path)
        .expect("path came from this plan");
    let inside_chunks = fields.iter().any(|f| {
        f.role == FieldRole::Chunks as i32
            && f.path != path
            && path.starts_with(&format!("{}.", f.path))
    });
    if inside_chunks {
        return Err(SchemaError::at(
            &path,
            format!("the {what} field cannot live inside the CHUNKS scope"),
        ));
    }
    if what != "vector" && target.repeated {
        return Err(SchemaError::at(
            &path,
            format!("the {what} field must be singular"),
        ));
    }
    Ok(path)
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
