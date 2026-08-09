//! The coordinator's node registry: which shards a collection is made of.
//!
//! Registration is static in this version. The table comes from configuration
//! at startup, and the only thing that changes it afterwards is a Split or a
//! Join, which rebinds it to the shards they just built. There is no
//! discovery, no gossip and no membership protocol: an operator says which
//! nodes hold the collection, and the coordinator holds them to it.
//!
//! The table is not persisted. A coordinator restart reads the configured
//! table again, so a collection that was reshaped by Split or Join needs its
//! configuration updated to match, or the restart serves the pre-split shape.
//! `ListNodes` reports the live table, which is how you check.
//!
//! Configuration is one entry per shard, entries separated by commas or
//! newlines, so the same syntax works in an environment variable and in a
//! file. Each entry is a node address, optionally followed by whitespace and
//! the index handle on that node. A `#` starts a comment that runs to the end
//! of the entry.
//!
//! ```text
//! # two shards, one with an explicit handle
//! http://127.0.0.1:50051  9a5c1e40-0f4a-4d2b-9f9e-1a2b3c4d5e6f
//! 127.0.0.1:50052         # the node's only open index
//! ```

/// One configured shard: a node address, and which index on it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardConfig {
    /// Address the coordinator dials, always carrying a scheme.
    pub address: String,

    /// Index handle on that node, or `None` for "the only index it has open",
    /// which the coordinator resolves when it binds the collection.
    pub index_id: Option<String>,
}

impl ShardConfig {
    /// A shard at `address`, with the handle left to be resolved.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: with_scheme(address.into()),
            index_id: None,
        }
    }

    /// A shard naming both the node and the index handle on it.
    pub fn with_index(address: impl Into<String>, index_id: impl Into<String>) -> Self {
        Self {
            address: with_scheme(address.into()),
            index_id: Some(index_id.into()),
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
            let address = fields.next().expect("non-empty entry has a first field");
            let index_id = fields.next();
            if let Some(extra) = fields.next() {
                return Err(format!(
                    "shard at position {position}: expected an address and an optional index \
                     handle, found a third field {extra:?}"
                ));
            }
            shards.push(match index_id {
                Some(id) => ShardConfig::with_index(address, id),
                None => ShardConfig::new(address),
            });
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
fn with_scheme(address: String) -> String {
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
}
