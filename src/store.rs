//! In-memory registry of open indexes.
//!
//! Each index is created or loaded once and then addressed by a `String`
//! handle. The registry hands out an `Arc<RwLock<Index>>` per handle: the read
//! lock covers searches, which turbovec already makes safe to run concurrently
//! against one shared index, and the write lock covers the mutating paths (add,
//! remove), which take `&mut` on the underlying index. A write therefore blocks
//! reads only on the one index it touches, never across the registry.
//!
//! The registry's own lock is held only long enough to clone or remove an
//! `Arc`, never while indexing or searching, so it never throttles work.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use turbovec::{IdMapIndex, TurboQuantIndex};

use crate::proto::IndexKind;

/// One open index, of either storage model.
///
/// The two variants mirror the two index types turbovec exposes. They differ
/// only in what a search returns and whether removal by id is supported;
/// everything else the service does is the same for both.
pub enum Index {
    /// A positional index ([`TurboQuantIndex`]). Search returns slot indices.
    Positional(TurboQuantIndex),

    /// An id-mapped index ([`IdMapIndex`]). Search returns external ids, and
    /// removal by id is supported.
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

    /// Bound dimensionality, or `None` for a lazy index that has not yet taken
    /// its first add.
    pub fn dim_opt(&self) -> Option<usize> {
        match self {
            Self::Positional(index) => index.dim_opt(),
            Self::IdMap(index) => index.dim_opt(),
        }
    }

    /// Quantization bit width (2 or 4).
    pub fn bit_width(&self) -> usize {
        match self {
            Self::Positional(index) => index.bit_width(),
            Self::IdMap(index) => index.bit_width(),
        }
    }
}

/// A handle to one open index, cloned out of the registry for the duration of
/// a single request.
pub type Handle = Arc<RwLock<Index>>;

/// Thread-safe registry of open indexes, keyed by handle.
#[derive(Default)]
pub struct IndexStore {
    inner: RwLock<HashMap<String, Handle>>,
}

impl IndexStore {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `index` under a fresh handle and return the handle.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock was poisoned by a panic on another thread.
    pub fn insert(&self, index: Index) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.inner
            .write()
            .expect("index registry lock poisoned")
            .insert(id.clone(), Arc::new(RwLock::new(index)));
        id
    }

    /// Look up an open index by handle.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock was poisoned by a panic on another thread.
    pub fn get(&self, id: &str) -> Option<Handle> {
        self.inner
            .read()
            .expect("index registry lock poisoned")
            .get(id)
            .cloned()
    }

    /// Remove an index from the registry. Returns true if it existed.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock was poisoned by a panic on another thread.
    pub fn remove(&self, id: &str) -> bool {
        self.inner
            .write()
            .expect("index registry lock poisoned")
            .remove(id)
            .is_some()
    }

    /// Snapshot the handles currently open, for listing metadata.
    ///
    /// # Panics
    ///
    /// Panics if the registry lock was poisoned by a panic on another thread.
    pub fn handles(&self) -> Vec<(String, Handle)> {
        self.inner
            .read()
            .expect("index registry lock poisoned")
            .iter()
            .map(|(id, handle)| (id.clone(), Arc::clone(handle)))
            .collect()
    }
}
