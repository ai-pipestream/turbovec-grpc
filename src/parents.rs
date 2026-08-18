//! Per-index parent documents for schemas with a CHUNKS scope.
//!
//! Chunk rows live in the vector index and in [`crate::columns::DocumentColumns`].
//! Parents live here: keyed by the parent document id's u64 reduction, holding
//! parent-level field values and the set of chunk labels that belong to them.
//! Remove drops a chunk from its parent and drops the parent when the last
//! chunk goes. Persistence is one deterministic encode (`parents.pb`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::proto::{StoredParent, StoredParentSet, StoredValue};

/// One parent's stored fields and its live chunk labels.
#[derive(Clone, Debug)]
pub struct ParentRecord {
    pub fields: HashMap<u32, StoredValue>,
    pub chunk_labels: BTreeSet<u64>,
}

/// The parent table of one schema-bound index.
pub struct ParentStore {
    fingerprint: String,
    parents: BTreeMap<u64, ParentRecord>,
}

impl ParentStore {
    /// Create an empty parent table for a schema with this fingerprint.
    pub fn new(fingerprint: String) -> Self {
        Self {
            fingerprint,
            parents: BTreeMap::new(),
        }
    }

    /// Fingerprint of the schema the field ordinals refer to.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Number of parents held.
    pub fn len(&self) -> usize {
        self.parents.len()
    }

    /// True when no parents are held.
    pub fn is_empty(&self) -> bool {
        self.parents.is_empty()
    }

    /// Insert or update one parent and union its chunk labels. Re-ingesting
    /// more chunks for the same parent keeps the ones already present.
    pub fn upsert(
        &mut self,
        parent_label: u64,
        fields: HashMap<u32, StoredValue>,
        chunk_labels: impl IntoIterator<Item = u64>,
    ) {
        use std::collections::btree_map::Entry;
        match self.parents.entry(parent_label) {
            Entry::Occupied(mut entry) => {
                let parent = entry.get_mut();
                parent.fields = fields;
                parent.chunk_labels.extend(chunk_labels);
            }
            Entry::Vacant(entry) => {
                let mut labels = BTreeSet::new();
                labels.extend(chunk_labels);
                entry.insert(ParentRecord {
                    fields,
                    chunk_labels: labels,
                });
            }
        }
    }

    /// Attach one more chunk label to an existing parent. Returns false
    /// when the parent is unknown.
    pub fn add_chunk(&mut self, parent_label: u64, chunk_label: u64) -> bool {
        match self.parents.get_mut(&parent_label) {
            Some(parent) => {
                parent.chunk_labels.insert(chunk_label);
                true
            }
            None => false,
        }
    }

    /// Drop one chunk label from its parent. When the parent has no chunks
    /// left it is removed entirely. Returns whether the chunk was known.
    pub fn remove_chunk(&mut self, parent_label: u64, chunk_label: u64) -> bool {
        let Some(parent) = self.parents.get_mut(&parent_label) else {
            return false;
        };
        if !parent.chunk_labels.remove(&chunk_label) {
            return false;
        }
        if parent.chunk_labels.is_empty() {
            self.parents.remove(&parent_label);
        }
        true
    }

    /// One parent's record, by parent label.
    pub fn get(&self, parent_label: u64) -> Option<&ParentRecord> {
        self.parents.get(&parent_label)
    }

    /// Iterate every parent in ascending label order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, &ParentRecord)> {
        self.parents.iter().map(|(label, record)| (*label, record))
    }

    /// Encode the whole set for persistence.
    pub fn to_set(&self) -> StoredParentSet {
        StoredParentSet {
            fingerprint: self.fingerprint.clone(),
            parents: self
                .parents
                .iter()
                .map(|(parent_label, record)| StoredParent {
                    parent_label: *parent_label,
                    fields: record.fields.clone(),
                    chunk_labels: record.chunk_labels.iter().copied().collect(),
                })
                .collect(),
        }
    }

    /// Rebuild from a persisted set, refusing a fingerprint mismatch.
    pub fn from_set(set: StoredParentSet, expected_fingerprint: &str) -> Result<Self, String> {
        if set.fingerprint != expected_fingerprint {
            return Err(format!(
                "stored parents were written under schema fingerprint {}, \
                 the generation's schema is {expected_fingerprint}",
                set.fingerprint
            ));
        }
        let mut parents = BTreeMap::new();
        for parent in set.parents {
            let mut chunk_labels = BTreeSet::new();
            for label in parent.chunk_labels {
                if !chunk_labels.insert(label) {
                    return Err(format!(
                        "stored parent {} carries chunk label {label} twice",
                        parent.parent_label
                    ));
                }
            }
            if parents
                .insert(
                    parent.parent_label,
                    ParentRecord {
                        fields: parent.fields,
                        chunk_labels,
                    },
                )
                .is_some()
            {
                return Err(format!(
                    "stored parents carry parent label {} twice",
                    parent.parent_label
                ));
            }
        }
        Ok(Self {
            fingerprint: set.fingerprint,
            parents,
        })
    }
}
