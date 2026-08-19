//! Names for the states this server refuses to serve through.
//!
//! A distributed search has failure modes a single-node one does not, and the
//! damaging ones are quiet: a shard calibrated differently from its siblings
//! still returns scores, they are simply on another scale, and a merge of them
//! is a plausible-looking ranking that means nothing. A node that drops out
//! mid-query still leaves a top-k, just not the top-k that was asked for.
//!
//! Neither is repaired here, and neither is smoothed over. Every one of them
//! ends an RPC with a `Status` whose message begins with one of the names
//! below, followed by the specifics: which shard, which coordinate, what it
//! held and what the collection expected. The name is stable and is what a
//! caller matches on; the detail is for whoever has to fix it.

use tonic::Status;

/// Shards do not share one calibration pair, so their scores are not on one
/// scale and cannot be merged.
pub const MIXED_CALIBRATION: &str = "mixed_calibration";

/// Indexes disagree on vector dimensionality.
pub const DIMENSION_MISMATCH: &str = "dimension_mismatch";

/// Indexes disagree on quantization bit width, which changes the encoding
/// and so the scores.
pub const BIT_WIDTH_MISMATCH: &str = "bit_width_mismatch";

/// A node could not be reached, or failed the call.
pub const NODE_UNREACHABLE: &str = "node_unreachable";

/// The collection has no shards configured.
pub const EMPTY_COLLECTION: &str = "empty_collection";

/// A shard was configured without an index handle and the node does not hold
/// exactly one index, so there is nothing to resolve it to.
pub const AMBIGUOUS_INDEX: &str = "ambiguous_index";

/// The call needs a positional index and was given an id-mapped one.
pub const POSITIONAL_INDEX_REQUIRED: &str = "positional_index_required";

/// The call needs an empty index and was given a populated one.
pub const INDEX_NOT_EMPTY: &str = "index_not_empty";

/// An index carrying row labels cannot take further rows.
pub const LABELLED_INDEX_IMMUTABLE: &str = "labelled_index_immutable";

/// A row range, or a per-target row count, does not fit the rows available.
pub const ROW_COUNT_MISMATCH: &str = "row_count_mismatch";

/// A calibration pair is the wrong length, or holds a value the encoder
/// cannot use.
pub const INVALID_CALIBRATION: &str = "invalid_calibration";

/// Every name above, so a caller can recognise one it did not raise itself.
const NAMES: &[&str] = &[
    MIXED_CALIBRATION,
    DIMENSION_MISMATCH,
    BIT_WIDTH_MISMATCH,
    NODE_UNREACHABLE,
    EMPTY_COLLECTION,
    AMBIGUOUS_INDEX,
    POSITIONAL_INDEX_REQUIRED,
    INDEX_NOT_EMPTY,
    LABELLED_INDEX_IMMUTABLE,
    ROW_COUNT_MISMATCH,
    INVALID_CALIBRATION,
];

/// Whether a status message is one of these named refusals.
///
/// The coordinator asks this of the failures its nodes hand back. A node that
/// refused something by name has already said the useful thing, and rewrapping
/// it as "the node did not answer" would replace a diagnosis with a symptom.
/// Matched by prefix against a fixed list rather than by pattern: the set of
/// names is closed and sits a few lines above.
pub fn is_named(message: &str) -> bool {
    NAMES.iter().any(|name| {
        message
            .strip_prefix(*name)
            .is_some_and(|rest| rest.starts_with(": "))
    })
}

/// Build a `FAILED_PRECONDITION` whose message is `name: detail`.
///
/// Used for the states that are wrong about the deployment rather than about
/// the request: the caller sent something well-formed, and the collection is
/// not in a shape that can answer it.
pub fn precondition(name: &str, detail: impl AsRef<str>) -> Status {
    Status::failed_precondition(format!("{name}: {}", detail.as_ref()))
}

/// Build an `INVALID_ARGUMENT` whose message is `name: detail`.
pub fn invalid(name: &str, detail: impl AsRef<str>) -> Status {
    Status::invalid_argument(format!("{name}: {}", detail.as_ref()))
}

/// Build an `UNAVAILABLE` whose message is `name: detail`.
///
/// Used only for a node that did not answer. It is separated from the
/// precondition failures because it is the one that may succeed on a retry.
pub fn unavailable(name: &str, detail: impl AsRef<str>) -> Status {
    Status::unavailable(format!("{name}: {}", detail.as_ref()))
}
