//! Per-index document field values, kept beside the vectors.
//!
//! A schema-bound index stores every planned scalar field of every
//! indexed row it holds, keyed by the row's u64 label, so CEL filters
//! evaluate against what each row actually carried at ingest. The
//! in-memory representation is the wire/storage types themselves
//! ([`StoredValue`], see `stored_documents.proto`): persistence is one
//! deterministic encode, restore is one decode, and there is no parallel
//! value model to drift.

use std::collections::{BTreeMap, HashMap};

use crate::proto::{StoredDocument, StoredDocumentSet, StoredValue};

/// One indexed row's stored fields and identity strings.
#[derive(Clone, Debug)]
pub struct StoredRow {
    pub fields: HashMap<u32, StoredValue>,
    pub parent_id: String,
    pub chunk_id: String,
    pub parent_label: u64,
}

/// The stored field values of every indexed row in one index.
///
/// Labels are kept in a `BTreeMap` so iteration — and therefore the
/// persisted byte stream — is deterministic for a given state.
pub struct DocumentColumns {
    fingerprint: String,
    documents: BTreeMap<u64, StoredRow>,
}

impl DocumentColumns {
    /// Create an empty column set for a schema with this fingerprint.
    pub fn new(fingerprint: String) -> Self {
        Self {
            fingerprint,
            documents: BTreeMap::new(),
        }
    }

    /// Fingerprint of the schema the field ordinals refer to.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Number of documents held.
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// True when no documents are held.
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Insert (or replace) one row.
    pub fn insert(&mut self, label: u64, row: StoredRow) {
        self.documents.insert(label, row);
    }

    /// Remove one row's values. Returns whether it was present.
    pub fn remove(&mut self, label: u64) -> bool {
        self.documents.remove(&label).is_some()
    }

    /// One row, by label.
    pub fn get(&self, label: u64) -> Option<&StoredRow> {
        self.documents.get(&label)
    }

    /// Iterate every row in ascending label order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &StoredRow)> {
        self.documents.iter().map(|(label, row)| (*label, row))
    }

    /// Encode the whole set for persistence, in ascending label order.
    pub fn to_set(&self) -> StoredDocumentSet {
        StoredDocumentSet {
            fingerprint: self.fingerprint.clone(),
            documents: self
                .documents
                .iter()
                .map(|(label, row)| StoredDocument {
                    label: *label,
                    fields: row.fields.clone(),
                    parent_id: row.parent_id.clone(),
                    chunk_id: row.chunk_id.clone(),
                    parent_label: row.parent_label,
                })
                .collect(),
        }
    }

    /// Rebuild from a persisted set, refusing a fingerprint mismatch.
    pub fn from_set(set: StoredDocumentSet, expected_fingerprint: &str) -> Result<Self, String> {
        if set.fingerprint != expected_fingerprint {
            return Err(format!(
                "stored documents were written under schema fingerprint {}, \
                 the generation's schema is {expected_fingerprint}",
                set.fingerprint
            ));
        }
        let mut documents = BTreeMap::new();
        for document in set.documents {
            let parent_label = if document.parent_label == 0 && document.chunk_id.is_empty() {
                // Legacy flat rows written before parent_label existed:
                // the row label is the parent label.
                document.label
            } else if document.parent_label == 0 {
                document.label
            } else {
                document.parent_label
            };
            let parent_id = if document.parent_id.is_empty() {
                String::new()
            } else {
                document.parent_id
            };
            if documents
                .insert(
                    document.label,
                    StoredRow {
                        fields: document.fields,
                        parent_id,
                        chunk_id: document.chunk_id,
                        parent_label,
                    },
                )
                .is_some()
            {
                return Err(format!(
                    "stored documents carry label {} twice",
                    document.label
                ));
            }
        }
        Ok(Self {
            fingerprint: set.fingerprint,
            documents,
        })
    }
}
