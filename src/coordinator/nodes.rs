//! The coordinator's node registry: which shards a collection is made of.
//!
//! The serving table is operator-controlled. It comes from configuration at
//! startup, and the only thing that changes it afterwards is a Split or a
//! Join, which rebinds it to the shards they just built. There is no gossip
//! and no automatic placement: an operator says which nodes hold the
//! collection, and the coordinator holds them to it.
//!
//! What a node can do on its own is announce itself. `RegisterNode` adds an
//! address to the spare pool persisted alongside the table, where it waits
//! until an operator names it as a Split or Join target. Registration never
//! changes the serving topology.
//!
//! When `TURBOVEC_COORD_STATE` is configured, the active table and its
//! generation are persisted atomically. A restart loads that state instead of
//! reverting a collection reshaped by Split or Join.
//!
//! Configuration is one entry per shard, entries separated by commas or
//! newlines, so the same syntax works in an environment variable and in a
//! file. Each entry is a primary and optional `|`-separated replicas, followed
//! by an optional index handle and durable generation. A `#` starts a comment.
//!
//! ```text
//! # two shards, one with an explicit handle
//! http://127.0.0.1:50051  9a5c1e40-0f4a-4d2b-9f9e-1a2b3c4d5e6f
//! 127.0.0.1:50052         # the node's only open index
//! ```

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

const TOPOLOGY_VERSION: u32 = 1;

/// One configured shard: a node address, and which index on it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardConfig {
    /// Address the coordinator dials, always carrying a scheme.
    pub address: String,

    /// Index handle on that node, or `None` for "the only index it has open",
    /// which the coordinator resolves when it binds the collection.
    pub index_id: Option<String>,

    /// Read-only failover addresses holding the same stable index id.
    #[serde(default)]
    pub replicas: Vec<String>,

    /// Durable generation every replica must serve before it is eligible.
    #[serde(default)]
    pub required_generation: Option<u64>,
}

impl ShardConfig {
    /// A shard at `address`, with the handle left to be resolved.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: with_scheme(address.into()),
            index_id: None,
            replicas: Vec::new(),
            required_generation: None,
        }
    }

    /// A shard naming both the node and the index handle on it.
    pub fn with_index(address: impl Into<String>, index_id: impl Into<String>) -> Self {
        Self {
            address: with_scheme(address.into()),
            index_id: Some(index_id.into()),
            replicas: Vec::new(),
            required_generation: None,
        }
    }

    pub fn with_index_generation(
        address: impl Into<String>,
        index_id: impl Into<String>,
        generation: Option<u64>,
    ) -> Self {
        Self {
            required_generation: generation.filter(|value| *value > 0),
            ..Self::with_index(address, index_id)
        }
    }
}

/// The configured shards of one collection, in shard order.
///
/// Order is meaningful only as a tie-break: two rows with equal scores are
/// returned in shard order, so the same query gives the same answer twice.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeTable {
    /// Shards, in configuration order.
    pub shards: Vec<ShardConfig>,
}

#[derive(Serialize, Deserialize)]
struct PersistedTopology {
    version: u32,
    generation: u64,
    shards: Vec<ShardConfig>,

    /// Registered nodes not serving any shard, in registration order.
    /// Absent in files written before the spare pool existed, which is the
    /// same as empty.
    #[serde(default)]
    spares: Vec<String>,
}

impl NodeTable {
    /// A table holding exactly these shards.
    pub fn new(shards: Vec<ShardConfig>) -> Self {
        Self { shards }
    }

    /// Parse a node table out of its configuration text.
    ///
    /// Hand-rolled rather than pattern-matched: the grammar is two fields and
    /// a comment character, and a parser for it is shorter than the expression
    /// that would recognise it. Returns the first entry that does not fit,
    /// naming its position, rather than skipping it: a shard silently dropped
    /// from a collection is a search that silently returns less than it should.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut shards = Vec::new();
        for (position, entry) in split_entries(text).into_iter().enumerate() {
            let entry = strip_comment(entry).trim();
            if entry.is_empty() {
                continue;
            }
            let mut fields = entry.split_ascii_whitespace();
            // `entry` is non-empty after trimming, so it has a first field.
            let addresses = fields.next().expect("non-empty entry has a first field");
            let index_id = fields.next();
            let generation = fields
                .next()
                .map(|value| {
                    value.parse::<u64>().map_err(|e| {
                        format!("shard at position {position}: invalid generation {value:?}: {e}")
                    })
                })
                .transpose()?;
            if let Some(extra) = fields.next() {
                return Err(format!(
                    "shard at position {position}: expected addresses, an optional index handle, \
                     and an optional generation; found an extra field {extra:?}"
                ));
            }
            let mut addresses = addresses.split('|');
            let primary = addresses.next().expect("address field is non-empty");
            let replicas: Vec<String> = addresses
                .map(|value| with_scheme(value.to_string()))
                .collect();
            let mut shard = match index_id {
                Some(id) => ShardConfig::with_index_generation(primary, id, generation),
                None if generation.is_none() => ShardConfig::new(primary),
                None => {
                    return Err(format!(
                        "shard at position {position}: generation requires an index handle"
                    ))
                }
            };
            shard.replicas = replicas;
            shards.push(shard);
        }
        if shards.is_empty() {
            return Err("no shards configured: the node table is empty".to_string());
        }
        Ok(Self { shards })
    }

    /// Number of configured shards.
    pub fn len(&self) -> usize {
        self.shards.len()
    }

    /// Whether the table holds no shards.
    pub fn is_empty(&self) -> bool {
        self.shards.is_empty()
    }
}

/// Load an existing topology state file, or atomically seed it from startup
/// configuration at generation 1. Returns the generation, the shard table,
/// and the persisted spare pool.
pub fn load_or_initialize(
    path: &Path,
    initial: &NodeTable,
) -> Result<(u64, NodeTable, Vec<String>), String> {
    if path.exists() {
        let bytes =
            fs::read(path).map_err(|e| format!("read topology state {}: {e}", path.display()))?;
        let persisted: PersistedTopology = serde_json::from_slice(&bytes)
            .map_err(|e| format!("decode topology state {}: {e}", path.display()))?;
        if persisted.version != TOPOLOGY_VERSION {
            return Err(format!(
                "topology state {} has version {}, expected {TOPOLOGY_VERSION}",
                path.display(),
                persisted.version
            ));
        }
        if persisted.generation == 0 || persisted.shards.is_empty() {
            return Err(format!(
                "topology state {} has generation {} and {} shards",
                path.display(),
                persisted.generation,
                persisted.shards.len()
            ));
        }
        return Ok((
            persisted.generation,
            NodeTable::new(persisted.shards),
            persisted.spares,
        ));
    }
    if initial.is_empty() {
        return Err("cannot initialize an empty topology".to_string());
    }
    persist_topology(path, 1, &initial.shards, &[])?;
    Ok((1, initial.clone(), Vec::new()))
}

/// Atomically replace a topology state file with one new generation.
pub fn persist_topology(
    path: &Path,
    generation: u64,
    shards: &[ShardConfig],
    spares: &[String],
) -> Result<(), String> {
    if generation == 0 || shards.is_empty() {
        return Err(format!(
            "refusing topology generation {generation} with {} shards",
            shards.len()
        ));
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|e| format!("create topology directory {}: {e}", parent.display()))?;
    let bytes = serde_json::to_vec_pretty(&PersistedTopology {
        version: TOPOLOGY_VERSION,
        generation,
        shards: shards.to_vec(),
        spares: spares.to_vec(),
    })
    .map_err(|e| format!("encode topology generation {generation}: {e}"))?;
    let temp = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("topology"),
        uuid::Uuid::new_v4()
    ));
    let result = (|| {
        let mut file = File::create(&temp)
            .map_err(|e| format!("create topology temp {}: {e}", temp.display()))?;
        file.write_all(&bytes)
            .map_err(|e| format!("write topology temp {}: {e}", temp.display()))?;
        file.sync_all()
            .map_err(|e| format!("sync topology temp {}: {e}", temp.display()))?;
        fs::rename(&temp, path)
            .map_err(|e| format!("activate topology state {}: {e}", path.display()))?;
        File::open(parent)
            .and_then(|dir| dir.sync_all())
            .map_err(|e| format!("sync topology directory {}: {e}", parent.display()))
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

/// Split configuration text into entries on commas and newlines.
fn split_entries(text: &str) -> Vec<&str> {
    let mut entries = Vec::new();
    let mut start = 0usize;
    for (at, ch) in text.char_indices() {
        if ch == ',' || ch == '\n' {
            entries.push(&text[start..at]);
            start = at + ch.len_utf8();
        }
    }
    entries.push(&text[start..]);
    entries
}

/// Drop everything from the first `#` onward.
fn strip_comment(entry: &str) -> &str {
    match entry.find('#') {
        Some(at) => &entry[..at],
        None => entry,
    }
}

/// Give a bare `host:port` the `http://` scheme the transport needs, and leave
/// an address that already names a scheme alone.
pub fn with_scheme(address: String) -> String {
    if address.contains("://") {
        address
    } else {
        format!("http://{address}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_addresses_handles_and_comments() {
        let table = NodeTable::parse(
            "# the collection\n\
             http://127.0.0.1:50051 abc-123\n\
             127.0.0.1:50052   # sole index\n\
             \n\
             127.0.0.1:50053",
        )
        .unwrap();
        assert_eq!(
            table.shards,
            vec![
                ShardConfig::with_index("http://127.0.0.1:50051", "abc-123"),
                ShardConfig::new("127.0.0.1:50052"),
                ShardConfig::new("127.0.0.1:50053"),
            ]
        );
    }

    #[test]
    fn parses_a_comma_separated_line() {
        let table = NodeTable::parse("127.0.0.1:1 a,127.0.0.1:2 b").unwrap();
        assert_eq!(table.len(), 2);
        assert_eq!(table.shards[1].index_id.as_deref(), Some("b"));
    }

    #[test]
    fn rejects_an_extra_field_and_an_empty_table() {
        assert!(NodeTable::parse("127.0.0.1:1 a b").is_err());
        assert!(NodeTable::parse("   \n # nothing here\n").is_err());
    }

    #[test]
    fn persisted_topology_survives_restart_and_advances_atomically() {
        let root = std::env::temp_dir().join(format!("turbovec-topology-{}", uuid::Uuid::new_v4()));
        let path = root.join("topology.json");
        let initial = NodeTable::new(vec![ShardConfig::with_index("node-a:1", "shard-a")]);
        let (generation, loaded, spares) = load_or_initialize(&path, &initial).unwrap();
        assert_eq!(generation, 1);
        assert_eq!(loaded, initial);
        assert!(spares.is_empty());

        let next = vec![
            ShardConfig::with_index("node-b:2", "shard-b"),
            ShardConfig::with_index("node-c:3", "shard-c"),
        ];
        let pool = vec!["http://node-d:4".to_string()];
        persist_topology(&path, 2, &next, &pool).unwrap();
        let (generation, loaded, spares) = load_or_initialize(&path, &initial).unwrap();
        assert_eq!(generation, 2);
        assert_eq!(loaded.shards, next);
        assert_eq!(spares, pool);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn state_files_without_a_spare_pool_still_load() {
        let root = std::env::temp_dir().join(format!("turbovec-topology-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("topology.json");
        fs::write(
            &path,
            r#"{"version":1,"generation":3,"shards":[{"address":"http://node-a:1","index_id":"shard-a","replicas":[],"required_generation":7}]}"#,
        )
        .unwrap();
        let initial = NodeTable::new(vec![ShardConfig::new("unused:1")]);
        let (generation, loaded, spares) = load_or_initialize(&path, &initial).unwrap();
        assert_eq!(generation, 3);
        assert_eq!(loaded.shards[0].required_generation, Some(7));
        assert!(spares.is_empty());
        fs::remove_dir_all(root).unwrap();
    }
}
