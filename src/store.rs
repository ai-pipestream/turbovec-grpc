//! Thread-safe index registry and restart-safe shard generations.
//!
//! The in-memory registry keeps search locks per index. When a data root is
//! configured, [`IndexStore::persist`] writes an immutable generation
//! directory and atomically replaces the shard's `CURRENT` pointer. Startup
//! restores only the referenced generation and validates its manifest,
//! checksums, shape, calibration bits, and labels before registering it.

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crc32fast::Hasher;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use turbovec::{CalibrationState, IdMapIndex, TurboQuantIndex};

use crate::columns::DocumentColumns;
use crate::proto::{IndexKind, StoredDocumentSet};
use crate::schema::BoundSchema;

const MANIFEST_VERSION: u32 = 1;
const CURRENT_FILE: &str = "CURRENT";
const MANIFEST_FILE: &str = "manifest.json";
const INDEX_FILE: &str = "index.tv";
const LABELS_FILE: &str = "labels.le64";
const SCHEMA_FILE: &str = "schema.fds";
const DOCUMENTS_FILE: &str = "documents.pb";

/// One open index, of either storage model.
pub enum Index {
    /// A positional index ([`TurboQuantIndex`]). Search returns slot indices.
    Positional(TurboQuantIndex),

    /// An id-mapped index ([`IdMapIndex`]). Search returns external ids.
    IdMap(IdMapIndex),
}

impl Index {
    /// Storage model of this index.
    pub fn kind(&self) -> IndexKind {
        match self {
            Self::Positional(_) => IndexKind::Positional,
            Self::IdMap(_) => IndexKind::IdMap,
        }
    }

    /// Number of vectors currently held.
    pub fn len(&self) -> usize {
        match self {
            Self::Positional(index) => index.len(),
            Self::IdMap(index) => index.len(),
        }
    }

    /// True when the index holds no vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bound dimensionality, or `None` for a never-initialized lazy index.
    pub fn dim_opt(&self) -> Option<usize> {
        match self {
            Self::Positional(index) => index.dim_opt(),
            Self::IdMap(index) => index.dim_opt(),
        }
    }

    /// Quantization bit width (2, 3, or 4).
    pub fn bit_width(&self) -> usize {
        match self {
            Self::Positional(index) => index.bit_width(),
            Self::IdMap(index) => index.bit_width(),
        }
    }

    /// Whether a TQ+ calibration pair is committed.
    pub fn calibration_state(&self) -> CalibrationState {
        match self {
            Self::Positional(index) => index.calibration_state(),
            Self::IdMap(index) => index.calibration_state(),
        }
    }
}

/// A handle cloned out of the registry for one request.
pub type Handle = Arc<RwLock<Index>>;

/// External row ids, `labels[slot]` being the stable id for that row.
pub type Labels = Arc<Vec<u64>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestRecord {
    pub operation_id: String,
    pub expected_len: u64,
    pub rows: u64,
    pub len: u64,
    #[serde(default)]
    pub generation: u64,
}

/// A persistence failure with enough path context to act on.
#[derive(Debug)]
pub struct PersistenceError(String);

impl PersistenceError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PersistenceError {}

/// Manifest entry for a bound schema. The descriptor set itself lives in
/// `schema.fds` beside the index, verbatim as the client registered it,
/// and the stored document field values live in `documents.pb`; the
/// manifest carries what restore needs to validate both.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SchemaManifest {
    message_type: String,
    fingerprint: String,
    descriptor_bytes: u64,
    descriptor_crc32: u32,
    documents_bytes: u64,
    documents_crc32: u32,
    documents_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShardManifest {
    version: u32,
    shard_id: String,
    generation: u64,
    kind: String,
    dim: Option<usize>,
    bit_width: usize,
    rows: usize,
    calibration_state: String,
    tqplus_shift_bits: Vec<u32>,
    tqplus_scale_bits: Vec<u32>,
    index_bytes: u64,
    index_crc32: u32,
    labelled: bool,
    labels_count: u64,
    labels_crc32: u32,
    #[serde(default)]
    last_ingest: Option<IngestRecord>,
    #[serde(default)]
    schema: Option<SchemaManifest>,
}

/// Thread-safe registry keyed by stable shard id.
pub struct IndexStore {
    inner: RwLock<HashMap<String, Handle>>,
    labels: RwLock<HashMap<String, Labels>>,
    generations: RwLock<HashMap<String, u64>>,
    ingests: RwLock<HashMap<String, IngestRecord>>,
    schemas: RwLock<HashMap<String, Arc<BoundSchema>>>,
    columns: RwLock<HashMap<String, Arc<RwLock<DocumentColumns>>>>,
    data_root: Option<PathBuf>,
}

impl Default for IndexStore {
    fn default() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            labels: RwLock::new(HashMap::new()),
            generations: RwLock::new(HashMap::new()),
            ingests: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            columns: RwLock::new(HashMap::new()),
            data_root: None,
        }
    }
}

impl IndexStore {
    /// Create an ephemeral empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Open a persistent registry and restore every shard referenced by a
    /// valid `CURRENT` pointer. Any corrupt shard fails the whole open.
    pub fn open(data_root: impl Into<PathBuf>) -> Result<Self, PersistenceError> {
        let data_root = data_root.into();
        fs::create_dir_all(&data_root).map_err(|e| {
            PersistenceError::new(format!("create data root {}: {e}", data_root.display()))
        })?;
        let store = Self {
            data_root: Some(data_root),
            ..Self::default()
        };
        store.restore_all()?;
        Ok(store)
    }

    /// Configured persistence root, absent for an ephemeral store.
    pub fn data_root(&self) -> Option<&Path> {
        self.data_root.as_deref()
    }

    /// Register an index under a fresh stable UUID.
    pub fn insert(&self, index: Index) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.insert_with_id(id.clone(), index, None, 0)
            .expect("a fresh UUID cannot collide");
        id
    }

    /// Register an index and its stable row labels under a fresh UUID.
    pub fn insert_labelled(&self, index: Index, labels: Vec<u64>) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.insert_with_id(id.clone(), index, Some(labels), 0)
            .expect("a fresh UUID cannot collide");
        id
    }

    fn insert_with_id(
        &self,
        id: String,
        index: Index,
        labels: Option<Vec<u64>>,
        generation: u64,
    ) -> Result<(), PersistenceError> {
        validate_shard_id(&id)?;
        if labels.as_ref().is_some_and(|v| v.len() != index.len()) {
            return Err(PersistenceError::new(format!(
                "shard {id} has {} rows but {} labels",
                index.len(),
                labels.as_ref().map_or(0, Vec::len)
            )));
        }
        let mut inner = self.inner.write().expect("index registry lock poisoned");
        if inner.contains_key(&id) {
            return Err(PersistenceError::new(format!(
                "duplicate shard id {id} in data root"
            )));
        }
        inner.insert(id.clone(), Arc::new(RwLock::new(index)));
        drop(inner);
        if let Some(labels) = labels {
            self.labels
                .write()
                .expect("index registry lock poisoned")
                .insert(id.clone(), Arc::new(labels));
        }
        self.generations
            .write()
            .expect("index registry lock poisoned")
            .insert(id, generation);
        Ok(())
    }

    /// External row ids for an index, or `None` for an unlabelled index.
    pub fn labels(&self, id: &str) -> Option<Labels> {
        self.labels
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    /// Current durable generation, or zero for a never-flushed index.
    pub fn generation(&self, id: &str) -> Option<u64> {
        self.generations
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .copied()
    }

    pub fn ingest_record(&self, id: &str) -> Option<IngestRecord> {
        self.ingests
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    pub fn set_ingest_record(&self, id: &str, record: IngestRecord) {
        self.ingests
            .write()
            .expect("index registry lock poisoned")
            .insert(id.to_string(), record);
    }

    /// Bind a derived schema to an open index, with an empty column set
    /// for its documents. The next persist writes both with the
    /// generation, and restore re-derives and validates them.
    pub fn bind_schema(&self, id: &str, schema: Arc<BoundSchema>) {
        let columns = DocumentColumns::new(schema.schema.fingerprint.clone());
        self.bind_schema_with_columns(id, schema, columns);
    }

    fn bind_schema_with_columns(
        &self,
        id: &str,
        schema: Arc<BoundSchema>,
        columns: DocumentColumns,
    ) {
        self.schemas
            .write()
            .expect("index registry lock poisoned")
            .insert(id.to_string(), schema);
        self.columns
            .write()
            .expect("index registry lock poisoned")
            .insert(id.to_string(), Arc::new(RwLock::new(columns)));
    }

    /// The schema bound to an index, or `None` for a plain vector index.
    pub fn schema(&self, id: &str) -> Option<Arc<BoundSchema>> {
        self.schemas
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    /// The stored document field values of a schema-bound index, or
    /// `None` for a plain vector index.
    pub fn columns(&self, id: &str) -> Option<Arc<RwLock<DocumentColumns>>> {
        self.columns
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    /// Look up an open index by stable shard id.
    pub fn get(&self, id: &str) -> Option<Handle> {
        self.inner
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    /// Remove an index and any durable generations without allowing restart
    /// to resurrect it.
    pub fn delete(&self, id: &str) -> Result<bool, PersistenceError> {
        validate_shard_id(id)?;
        let live = self.get(id).is_some();
        let mut durable = false;
        if let Some(root) = self.data_root.as_deref() {
            let shard_dir = root.join(id);
            if shard_dir.exists() {
                durable = true;
                let tombstone = root.join(format!(".deleted-{id}-{}", uuid::Uuid::new_v4()));
                fs::rename(&shard_dir, &tombstone)
                    .map_err(|e| path_error("tombstone shard", &shard_dir, e))?;
                sync_dir(root)?;
                fs::remove_dir_all(&tombstone)
                    .map_err(|e| path_error("remove tombstoned shard", &tombstone, e))?;
                sync_dir(root)?;
            }
        }
        self.labels
            .write()
            .expect("index registry lock poisoned")
            .remove(id);
        self.generations
            .write()
            .expect("index registry lock poisoned")
            .remove(id);
        self.ingests
            .write()
            .expect("index registry lock poisoned")
            .remove(id);
        self.schemas
            .write()
            .expect("index registry lock poisoned")
            .remove(id);
        self.columns
            .write()
            .expect("index registry lock poisoned")
            .remove(id);
        let removed = self
            .inner
            .write()
            .expect("index registry lock poisoned")
            .remove(id)
            .is_some();
        Ok(live || durable || removed)
    }

    /// Snapshot the handles currently open, for listing or flushing.
    pub fn handles(&self) -> Vec<(String, Handle)> {
        self.inner
            .read()
            .expect("index registry lock poisoned")
            .iter()
            .map(|(id, handle)| (id.clone(), Arc::clone(handle)))
            .collect()
    }

    /// Atomically persist one shard and return its new generation.
    pub fn persist(&self, id: &str) -> Result<u64, PersistenceError> {
        let root = self.data_root.as_ref().ok_or_else(|| {
            PersistenceError::new("persistence is disabled: TURBOVEC_DATA_DIR is not configured")
        })?;
        let handle = self
            .get(id)
            .ok_or_else(|| PersistenceError::new(format!("unknown shard id {id}")))?;
        let labels = self.labels(id);
        let current = self.generation(id).unwrap_or(0);
        let generation = current.checked_add(1).ok_or_else(|| {
            PersistenceError::new(format!("generation counter overflow for shard {id}"))
        })?;
        let shard_dir = root.join(id);
        fs::create_dir_all(&shard_dir)
            .map_err(|e| path_error("create shard directory", &shard_dir, e))?;
        let final_dir = shard_dir.join(generation_name(generation));
        if final_dir.exists() {
            return Err(PersistenceError::new(format!(
                "refusing to overwrite existing generation {}",
                final_dir.display()
            )));
        }
        let temp_dir = shard_dir.join(format!(
            ".{}.tmp-{}",
            generation_name(generation),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&temp_dir)
            .map_err(|e| path_error("create temporary generation", &temp_dir, e))?;

        let schema = self.schema(id);
        let result = (|| {
            let index_path = temp_dir.join(INDEX_FILE);
            let last_ingest = self.ingest_record(id);
            let manifest = {
                let guard = handle
                    .read()
                    .map_err(|_| PersistenceError::new("index lock poisoned"))?;
                match &*guard {
                    Index::Positional(index) => index.write(&index_path),
                    Index::IdMap(index) => index.write(&index_path),
                }
                .map_err(|e| path_error("write index", &index_path, e))?;
                sync_file(&index_path)?;
                let (index_bytes, index_crc32) = file_size_crc(&index_path)?;
                let (labels_count, labels_crc32) = match labels.as_deref() {
                    Some(values) => {
                        let path = temp_dir.join(LABELS_FILE);
                        let crc = write_labels(&path, values)?;
                        (values.len() as u64, crc)
                    }
                    None => (0, 0),
                };
                let schema_manifest = match schema.as_deref() {
                    Some(bound) => {
                        let path = temp_dir.join(SCHEMA_FILE);
                        write_synced(&path, &bound.descriptor_set)?;
                        let (descriptor_bytes, descriptor_crc32) = file_size_crc(&path)?;
                        // The columns snapshot is coherent with the index
                        // snapshot: this thread holds the index read lock,
                        // and every columns mutation happens under the
                        // index write lock.
                        let columns = self.columns(id).ok_or_else(|| {
                            PersistenceError::new(format!(
                                "shard {id} has a bound schema but no document columns"
                            ))
                        })?;
                        let set = columns
                            .read()
                            .map_err(|_| PersistenceError::new("columns lock poisoned"))?
                            .to_set();
                        let documents_count = set.documents.len() as u64;
                        let documents_path = temp_dir.join(DOCUMENTS_FILE);
                        write_synced(&documents_path, &set.encode_to_vec())?;
                        let (documents_bytes, documents_crc32) = file_size_crc(&documents_path)?;
                        Some(SchemaManifest {
                            message_type: bound.schema.message_type.clone(),
                            fingerprint: bound.schema.fingerprint.clone(),
                            descriptor_bytes,
                            descriptor_crc32,
                            documents_bytes,
                            documents_crc32,
                            documents_count,
                        })
                    }
                    None => None,
                };
                manifest_for(
                    id,
                    generation,
                    &guard,
                    PersistedFiles {
                        index_bytes,
                        index_crc32,
                        labelled: labels.is_some(),
                        labels_count,
                        labels_crc32,
                        schema: schema_manifest,
                    },
                    last_ingest,
                )
            };

            let manifest_path = temp_dir.join(MANIFEST_FILE);
            let bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| {
                PersistenceError::new(format!("encode manifest for shard {id}: {e}"))
            })?;
            write_synced(&manifest_path, &bytes)?;
            sync_dir(&temp_dir)?;
            fs::rename(&temp_dir, &final_dir)
                .map_err(|e| path_error("activate generation directory", &final_dir, e))?;
            sync_dir(&shard_dir)?;

            let current_temp = shard_dir.join(format!(".CURRENT.tmp-{}", uuid::Uuid::new_v4()));
            write_synced(&current_temp, format!("{generation}\n").as_bytes())?;
            fs::rename(&current_temp, shard_dir.join(CURRENT_FILE)).map_err(|e| {
                path_error("activate CURRENT pointer", &shard_dir.join(CURRENT_FILE), e)
            })?;
            sync_dir(&shard_dir)?;
            Ok::<(), PersistenceError>(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&temp_dir);
        }
        result?;
        self.generations
            .write()
            .expect("index registry lock poisoned")
            .insert(id.to_string(), generation);
        cleanup_old_generations(&shard_dir, generation);
        Ok(generation)
    }

    /// Persist every open shard in stable id order.
    pub fn persist_all(&self) -> Result<Vec<(String, u64)>, PersistenceError> {
        let mut ids: Vec<String> = self.handles().into_iter().map(|(id, _)| id).collect();
        ids.sort();
        ids.into_iter()
            .map(|id| self.persist(&id).map(|generation| (id, generation)))
            .collect()
    }

    fn restore_all(&self) -> Result<(), PersistenceError> {
        let root = self
            .data_root
            .as_ref()
            .expect("restore_all is only called for persistent stores");
        let mut dirs: Vec<PathBuf> = fs::read_dir(root)
            .map_err(|e| path_error("read data root", root, e))?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                entry
                    .file_type()
                    .ok()
                    .filter(|t| t.is_dir())
                    .map(|_| entry.path())
            })
            .collect();
        dirs.sort();
        for shard_dir in dirs {
            let Some(id) = shard_dir.file_name().and_then(|s| s.to_str()) else {
                return Err(PersistenceError::new(format!(
                    "non-UTF-8 shard directory under {}",
                    root.display()
                )));
            };
            if id.starts_with(".deleted-") {
                fs::remove_dir_all(&shard_dir)
                    .map_err(|e| path_error("clean tombstoned shard", &shard_dir, e))?;
                continue;
            }
            validate_shard_id(id)?;
            let current_path = shard_dir.join(CURRENT_FILE);
            if !current_path.exists() {
                continue;
            }
            let current_text = fs::read_to_string(&current_path)
                .map_err(|e| path_error("read CURRENT pointer", &current_path, e))?;
            let generation: u64 = current_text.trim().parse().map_err(|e| {
                PersistenceError::new(format!(
                    "parse generation in {}: {e}",
                    current_path.display()
                ))
            })?;
            let generation_dir = shard_dir.join(generation_name(generation));
            let manifest_path = generation_dir.join(MANIFEST_FILE);
            let manifest: ShardManifest = serde_json::from_slice(
                &fs::read(&manifest_path)
                    .map_err(|e| path_error("read shard manifest", &manifest_path, e))?,
            )
            .map_err(|e| {
                PersistenceError::new(format!("decode {}: {e}", manifest_path.display()))
            })?;
            validate_manifest_header(&manifest, id, generation)?;
            let index_path = generation_dir.join(INDEX_FILE);
            verify_file(&index_path, manifest.index_bytes, manifest.index_crc32)?;
            let index = match manifest.kind.as_str() {
                "positional" => TurboQuantIndex::load(&index_path)
                    .map(Index::Positional)
                    .map_err(|e| path_error("load positional index", &index_path, e))?,
                "id_map" => IdMapIndex::load(&index_path)
                    .map(Index::IdMap)
                    .map_err(|e| path_error("load id-mapped index", &index_path, e))?,
                other => {
                    return Err(PersistenceError::new(format!(
                        "manifest for shard {id} has unknown kind {other:?}"
                    )))
                }
            };
            validate_loaded_index(&manifest, &index)?;
            let labels = if manifest.labelled {
                let path = generation_dir.join(LABELS_FILE);
                Some(read_labels(
                    &path,
                    manifest.labels_count,
                    manifest.labels_crc32,
                )?)
            } else {
                if manifest.labels_count != 0 || manifest.labels_crc32 != 0 {
                    return Err(PersistenceError::new(format!(
                        "unlabelled shard {id} has non-empty label metadata"
                    )));
                }
                None
            };
            let schema = match &manifest.schema {
                Some(record) => {
                    let bound = restore_schema(&generation_dir, record)?;
                    let columns = restore_columns(&generation_dir, record, manifest.rows)?;
                    Some((bound, columns))
                }
                None => None,
            };
            self.insert_with_id(id.to_string(), index, labels, generation)?;
            if let Some(record) = manifest.last_ingest {
                self.set_ingest_record(id, record);
            }
            if let Some((bound, columns)) = schema {
                self.bind_schema_with_columns(id, bound, columns);
            }
        }
        Ok(())
    }
}

/// Rebuild a bound schema from its persisted descriptor set. The plan is
/// re-derived by the code that is loading it, and a fingerprint that no
/// longer matches the manifest fails the restore: a derivation change is
/// an index compatibility event, and serving through it would quietly
/// change what the index means.
fn restore_schema(
    generation_dir: &Path,
    record: &SchemaManifest,
) -> Result<Arc<BoundSchema>, PersistenceError> {
    let path = generation_dir.join(SCHEMA_FILE);
    verify_file(&path, record.descriptor_bytes, record.descriptor_crc32)?;
    let bytes = fs::read(&path).map_err(|e| path_error("read schema descriptor set", &path, e))?;
    let bound = BoundSchema::derive(&bytes, &record.message_type).map_err(|e| {
        PersistenceError::new(format!(
            "re-derive persisted schema {}: {e}",
            record.message_type
        ))
    })?;
    if bound.schema.fingerprint != record.fingerprint {
        return Err(PersistenceError::new(format!(
            "schema fingerprint drift for {}: persisted {}, re-derived {}; \
             the derivation rules changed since this shard was written, so this \
             build refuses to serve it",
            record.message_type, record.fingerprint, bound.schema.fingerprint
        )));
    }
    Ok(Arc::new(bound))
}

/// Restore a schema-bound shard's stored document field values, verified
/// against the manifest and cross-checked against the row count: a
/// schema-bound index without columns for exactly its rows cannot answer
/// filters truthfully, so it does not serve.
fn restore_columns(
    generation_dir: &Path,
    record: &SchemaManifest,
    rows: usize,
) -> Result<DocumentColumns, PersistenceError> {
    let path = generation_dir.join(DOCUMENTS_FILE);
    verify_file(&path, record.documents_bytes, record.documents_crc32)?;
    let bytes = fs::read(&path).map_err(|e| path_error("read stored documents", &path, e))?;
    let set = StoredDocumentSet::decode(bytes.as_slice())
        .map_err(|e| PersistenceError::new(format!("decode {}: {e}", path.display())))?;
    if set.documents.len() as u64 != record.documents_count {
        return Err(PersistenceError::new(format!(
            "{} holds {} documents, manifest expects {}",
            path.display(),
            set.documents.len(),
            record.documents_count
        )));
    }
    if set.documents.len() != rows {
        return Err(PersistenceError::new(format!(
            "{} holds {} documents but the index holds {rows} rows",
            path.display(),
            set.documents.len()
        )));
    }
    DocumentColumns::from_set(set, &record.fingerprint)
        .map_err(|e| PersistenceError::new(format!("restore {}: {e}", path.display())))
}

fn validate_shard_id(id: &str) -> Result<(), PersistenceError> {
    if id.is_empty()
        || id == "."
        || id == ".."
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err(PersistenceError::new(format!(
            "invalid shard id {id:?}: use ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

fn generation_name(generation: u64) -> String {
    format!("gen-{generation:020}")
}

fn calibration_name(state: CalibrationState) -> &'static str {
    match state {
        CalibrationState::Uncalibrated => "uncalibrated",
        CalibrationState::Calibrated => "calibrated",
        _ => "unknown",
    }
}

struct PersistedFiles {
    index_bytes: u64,
    index_crc32: u32,
    labelled: bool,
    labels_count: u64,
    labels_crc32: u32,
    schema: Option<SchemaManifest>,
}

fn manifest_for(
    id: &str,
    generation: u64,
    index: &Index,
    files: PersistedFiles,
    last_ingest: Option<IngestRecord>,
) -> ShardManifest {
    let (kind, shift, scale) = match index {
        Index::Positional(index) => (
            "positional",
            index.tqplus_shift().iter().map(|v| v.to_bits()).collect(),
            index.tqplus_scale().iter().map(|v| v.to_bits()).collect(),
        ),
        Index::IdMap(_) => ("id_map", Vec::new(), Vec::new()),
    };
    ShardManifest {
        version: MANIFEST_VERSION,
        shard_id: id.to_string(),
        generation,
        kind: kind.to_string(),
        dim: index.dim_opt(),
        bit_width: index.bit_width(),
        rows: index.len(),
        calibration_state: calibration_name(index.calibration_state()).to_string(),
        tqplus_shift_bits: shift,
        tqplus_scale_bits: scale,
        index_bytes: files.index_bytes,
        index_crc32: files.index_crc32,
        labelled: files.labelled,
        labels_count: files.labels_count,
        labels_crc32: files.labels_crc32,
        last_ingest,
        schema: files.schema,
    }
}

fn validate_manifest_header(
    manifest: &ShardManifest,
    id: &str,
    generation: u64,
) -> Result<(), PersistenceError> {
    if manifest.version != MANIFEST_VERSION {
        return Err(PersistenceError::new(format!(
            "shard {id} manifest version {} is unsupported (expected {MANIFEST_VERSION})",
            manifest.version
        )));
    }
    if manifest.shard_id != id || manifest.generation != generation {
        return Err(PersistenceError::new(format!(
            "manifest identity mismatch for {id}: contains shard {:?} generation {}",
            manifest.shard_id, manifest.generation
        )));
    }
    Ok(())
}

fn validate_loaded_index(manifest: &ShardManifest, index: &Index) -> Result<(), PersistenceError> {
    let actual_kind = match index {
        Index::Positional(_) => "positional",
        Index::IdMap(_) => "id_map",
    };
    if manifest.kind != actual_kind
        || manifest.dim != index.dim_opt()
        || manifest.bit_width != index.bit_width()
        || manifest.rows != index.len()
        || manifest.calibration_state != calibration_name(index.calibration_state())
    {
        return Err(PersistenceError::new(format!(
            "loaded shard {} does not match its manifest shape or calibration state",
            manifest.shard_id
        )));
    }
    if let Index::Positional(index) = index {
        let shift: Vec<u32> = index.tqplus_shift().iter().map(|v| v.to_bits()).collect();
        let scale: Vec<u32> = index.tqplus_scale().iter().map(|v| v.to_bits()).collect();
        if shift != manifest.tqplus_shift_bits || scale != manifest.tqplus_scale_bits {
            return Err(PersistenceError::new(format!(
                "loaded shard {} calibration bits differ from its manifest",
                manifest.shard_id
            )));
        }
    } else if !manifest.tqplus_shift_bits.is_empty() || !manifest.tqplus_scale_bits.is_empty() {
        return Err(PersistenceError::new(format!(
            "id-mapped shard {} unexpectedly carries positional calibration bits",
            manifest.shard_id
        )));
    }
    Ok(())
}

fn write_labels(path: &Path, labels: &[u64]) -> Result<u32, PersistenceError> {
    let file = File::create(path).map_err(|e| path_error("create labels", path, e))?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Hasher::new();
    for label in labels {
        let bytes = label.to_le_bytes();
        writer
            .write_all(&bytes)
            .map_err(|e| path_error("write labels", path, e))?;
        hasher.update(&bytes);
    }
    writer
        .flush()
        .map_err(|e| path_error("flush labels", path, e))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|e| path_error("sync labels", path, e))?;
    Ok(hasher.finalize())
}

fn read_labels(path: &Path, count: u64, expected_crc: u32) -> Result<Vec<u64>, PersistenceError> {
    let expected_bytes = count.checked_mul(8).ok_or_else(|| {
        PersistenceError::new(format!("label byte count overflow in {}", path.display()))
    })?;
    let metadata = fs::metadata(path).map_err(|e| path_error("stat labels", path, e))?;
    if metadata.len() != expected_bytes {
        return Err(PersistenceError::new(format!(
            "labels {} has {} bytes, manifest expects {expected_bytes}",
            path.display(),
            metadata.len()
        )));
    }
    let capacity = usize::try_from(count).map_err(|_| {
        PersistenceError::new(format!("label count {count} does not fit this process"))
    })?;
    let mut labels = Vec::with_capacity(capacity);
    let mut reader =
        BufReader::new(File::open(path).map_err(|e| path_error("open labels", path, e))?);
    let mut hasher = Hasher::new();
    let mut bytes = [0u8; 8];
    for _ in 0..count {
        reader
            .read_exact(&mut bytes)
            .map_err(|e| path_error("read labels", path, e))?;
        hasher.update(&bytes);
        labels.push(u64::from_le_bytes(bytes));
    }
    let actual_crc = hasher.finalize();
    if actual_crc != expected_crc {
        return Err(PersistenceError::new(format!(
            "labels {} checksum {:08x} differs from manifest {:08x}",
            path.display(),
            actual_crc,
            expected_crc
        )));
    }
    Ok(labels)
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), PersistenceError> {
    let mut file = File::create(path).map_err(|e| path_error("create file", path, e))?;
    file.write_all(bytes)
        .map_err(|e| path_error("write file", path, e))?;
    file.sync_all()
        .map_err(|e| path_error("sync file", path, e))
}

fn sync_file(path: &Path) -> Result<(), PersistenceError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| path_error("sync file", path, e))
}

fn sync_dir(path: &Path) -> Result<(), PersistenceError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|e| path_error("sync directory", path, e))
}

fn file_size_crc(path: &Path) -> Result<(u64, u32), PersistenceError> {
    let file = File::open(path).map_err(|e| path_error("open file for checksum", path, e))?;
    let size = file
        .metadata()
        .map_err(|e| path_error("stat file", path, e))?
        .len();
    let mut reader = BufReader::new(file);
    let mut hasher = Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buffer)
            .map_err(|e| path_error("read file for checksum", path, e))?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok((size, hasher.finalize()))
}

fn verify_file(path: &Path, size: u64, crc: u32) -> Result<(), PersistenceError> {
    let (actual_size, actual_crc) = file_size_crc(path)?;
    if actual_size != size || actual_crc != crc {
        return Err(PersistenceError::new(format!(
            "index {} differs from manifest: bytes {actual_size}/{size}, crc {actual_crc:08x}/{crc:08x}",
            path.display()
        )));
    }
    Ok(())
}

fn cleanup_old_generations(shard_dir: &Path, current: u64) {
    let keep_from = current.saturating_sub(1);
    let Ok(entries) = fs::read_dir(shard_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(number) = name.strip_prefix("gen-") else {
            continue;
        };
        let Ok(generation) = number.parse::<u64>() else {
            continue;
        };
        if generation < keep_from {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
}

fn path_error(action: &str, path: &Path, error: impl fmt::Display) -> PersistenceError {
    PersistenceError::new(format!("{action} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labelled_shard_round_trips_with_stable_id_and_generation() {
        let root = std::env::temp_dir().join(format!("turbovec-store-{}", uuid::Uuid::new_v4()));
        let store = IndexStore::open(&root).unwrap();
        let mut index = TurboQuantIndex::new(8, 4).unwrap();
        index.add(&[0.1; 16]);
        let id = store.insert_labelled(Index::Positional(index), vec![41, 99]);
        assert_eq!(store.persist(&id).unwrap(), 1);
        drop(store);

        let restored = IndexStore::open(&root).unwrap();
        assert_eq!(restored.generation(&id), Some(1));
        assert_eq!(restored.labels(&id).as_deref(), Some(&vec![41, 99]));
        assert_eq!(restored.get(&id).unwrap().read().unwrap().len(), 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn checksum_corruption_fails_restore() {
        let root = std::env::temp_dir().join(format!("turbovec-store-{}", uuid::Uuid::new_v4()));
        let store = IndexStore::open(&root).unwrap();
        let mut index = TurboQuantIndex::new(8, 4).unwrap();
        index.add(&[0.1; 8]);
        let id = store.insert(Index::Positional(index));
        store.persist(&id).unwrap();
        drop(store);

        let index_path = root.join(&id).join(generation_name(1)).join(INDEX_FILE);
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(index_path)
            .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(IndexStore::open(&root).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
