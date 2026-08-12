//! Per-index document field values, kept beside the vectors.
//!
//! A schema-bound index stores every planned scalar field of every
//! document it holds, keyed by the document's u64 label, so CEL filters
//! evaluate against what each document actually carried at ingest. The
//! in-memory representation is the wire/storage types themselves
//! ([`StoredValue`], see `stored_documents.proto`): persistence is one
//! deterministic encode, restore is one decode, and there is no parallel
//! value model to drift.

use std::collections::{BTreeMap, HashMap};

use crate::proto::{StoredDocument, StoredDocumentSet, StoredValue};

/// The stored field values of every document in one index.
///
/// Labels are kept in a `BTreeMap` so iteration — and therefore the
/// persisted byte stream — is deterministic for a given state.
pub struct DocumentColumns {
    fingerprint: String,
    documents: BTreeMap<u64, HashMap<u32, StoredValue>>,
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

    /// Insert (or replace) one document's values.
    pub fn insert(&mut self, label: u64, fields: HashMap<u32, StoredValue>) {
        self.documents.insert(label, fields);
    }

    /// Remove one document's values. Returns whether it was present.
    pub fn remove(&mut self, label: u64) -> bool {
        self.documents.remove(&label).is_some()
    }

    /// One document's values, by label.
    pub fn get(&self, label: u64) -> Option<&HashMap<u32, StoredValue>> {
        self.documents.get(&label)
    }

    /// Iterate every document in ascending label order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &HashMap<u32, StoredValue>)> {
        self.documents
            .iter()
            .map(|(label, fields)| (*label, fields))
    }

    /// Encode the whole set for persistence, in ascending label order.
    pub fn to_set(&self) -> StoredDocumentSet {
        StoredDocumentSet {
            fingerprint: self.fingerprint.clone(),
            documents: self
                .documents
                .iter()
                .map(|(label, fields)| StoredDocument {
                    label: *label,
                    fields: fields.clone(),
                })
                .collect(),
        }
    }

    /// Rebuild from a persisted set, refusing one written under a
    /// different schema fingerprint: ordinals are only meaningful against
    /// the plan they were derived from.
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
            if documents.insert(document.label, document.fields).is_some() {
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
