//! CEL filters over the planned fields of a schema-bound index.
//!
//! A filter is a boolean CEL expression whose variables are the
//! document's own proto fields, spelled the way the proto spells them:
//! `price_cents < 5000 && meta.author == "kagome"`. Nested messages bind
//! as nested maps, so a dotted proto path reads naturally in CEL.
//!
//! Filtering is exact, and it stays exact by construction: the filter is
//! evaluated against the stored field values of every document, the
//! admitted labels become an allowlist, and the vector search runs
//! restricted to that allowlist. Nothing is over-fetched, re-ranked, or
//! approximated.
//!
//! Failures are requests problems and they fail loudly: an expression
//! that does not parse, references a field the schema does not plan, or
//! does not evaluate to a boolean is an error naming the problem, never
//! an empty result.

use std::collections::HashMap;
use std::sync::Arc;

use cel::{Context, Program, Value as CelValue};

use crate::proto::{stored_value, IndexSchema, StoredValue};
use crate::schema::StoredField;

/// A filter failure, already worded for the caller.
#[derive(Debug)]
pub struct FilterError(String);

impl FilterError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FilterError {}

/// One node of the binding tree: either a stored leaf or a nested scope.
enum Node {
    Leaf { ordinal: u32 },
    Branch(Vec<(String, Node)>),
}

/// A compiled filter: the parsed program, a base context holding the CEL
/// standard library, and the binding tree that turns one document's
/// stored values into CEL variables.
pub struct CompiledFilter {
    program: Program,
    base: Context<'static>,
    roots: Vec<(String, Node)>,
}

impl std::fmt::Debug for CompiledFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledFilter")
            .field("roots", &self.roots.len())
            .finish()
    }
}

impl CompiledFilter {
    /// Parse and validate one expression against a schema's stored
    /// fields. Every top-level identifier the expression references must
    /// be a planned filterable field (or the root of a nested scope that
    /// holds one); an unknown identifier fails here, once, naming what
    /// is available.
    pub fn compile(
        expression: &str,
        schema: &IndexSchema,
        stored: &[StoredField],
    ) -> Result<Self, FilterError> {
        let program = Program::compile(expression)
            .map_err(|e| FilterError::new(format!("filter does not parse: {e}")))?;
        let roots = binding_tree(schema, stored);
        let known: Vec<&str> = roots.iter().map(|(name, _)| name.as_str()).collect();
        for variable in program.references().variables() {
            if !known.contains(&variable) {
                return Err(FilterError::new(format!(
                    "filter references {variable:?}, which the schema does not plan as a \
                     filterable field; available: {}",
                    filterable_paths(schema, stored).join(", ")
                )));
            }
        }
        Ok(Self {
            program,
            base: Context::default(),
            roots,
        })
    }

    /// Evaluate the filter against one document's stored values.
    pub fn matches(&self, fields: &HashMap<u32, StoredValue>) -> Result<bool, FilterError> {
        let mut scope = self.base.new_inner_scope();
        for (name, node) in &self.roots {
            scope.add_variable_from_value(name.clone(), bind(node, fields)?);
        }
        match self.program.execute(&scope) {
            Ok(CelValue::Bool(admitted)) => Ok(admitted),
            Ok(other) => Err(FilterError::new(format!(
                "filter evaluated to {:?}, expected a boolean",
                other.type_of()
            ))),
            Err(e) => Err(FilterError::new(format!("filter failed to evaluate: {e}"))),
        }
    }
}

/// The dotted paths a filter may reference, for error messages.
fn filterable_paths(schema: &IndexSchema, stored: &[StoredField]) -> Vec<String> {
    stored
        .iter()
        .map(|f| schema.fields[f.ordinal as usize].path.clone())
        .collect()
}

/// Group the stored fields' dotted paths into a tree of nested scopes.
/// Intermediate segments are always message scopes: the plan never emits
/// both a leaf and children at one path.
fn binding_tree(schema: &IndexSchema, stored: &[StoredField]) -> Vec<(String, Node)> {
    let mut roots: Vec<(String, Node)> = Vec::new();
    for field in stored {
        let path = &schema.fields[field.ordinal as usize].path;
        let segments: Vec<&str> = path.split('.').collect();
        insert(
            &mut roots,
            &segments,
            Node::Leaf {
                ordinal: field.ordinal,
            },
        );
    }
    roots
}

fn insert(level: &mut Vec<(String, Node)>, segments: &[&str], leaf: Node) {
    let (head, rest) = segments.split_first().expect("paths are never empty");
    if rest.is_empty() {
        level.push((head.to_string(), leaf));
        return;
    }
    let branch = match level.iter_mut().find(|(name, _)| name == head) {
        Some((_, Node::Branch(children))) => children,
        Some(_) => unreachable!("a plan never has both a leaf and children at one path"),
        None => {
            level.push((head.to_string(), Node::Branch(Vec::new())));
            match &mut level.last_mut().expect("just pushed").1 {
                Node::Branch(children) => children,
                Node::Leaf { .. } => unreachable!("just pushed a branch"),
            }
        }
    };
    insert(branch, rest, leaf);
}

/// Materialize one node of the binding tree as a CEL value for one
/// document.
fn bind(node: &Node, fields: &HashMap<u32, StoredValue>) -> Result<CelValue, FilterError> {
    match node {
        Node::Leaf { ordinal } => {
            let stored = fields.get(ordinal).ok_or_else(|| {
                FilterError::new(format!(
                    "document is missing stored field ordinal {ordinal}; \
                     its columns do not match its schema"
                ))
            })?;
            cel_value(stored)
        }
        Node::Branch(children) => {
            let mut map: HashMap<String, CelValue> = HashMap::with_capacity(children.len());
            for (name, child) in children {
                map.insert(name.clone(), bind(child, fields)?);
            }
            Ok(map.into())
        }
    }
}

/// Convert one stored value into its CEL value. Extraction stores every
/// stored field explicitly (defaults included), so an empty oneof is a
/// corruption, not an unset field.
fn cel_value(stored: &StoredValue) -> Result<CelValue, FilterError> {
    use stored_value::Value as V;
    let value = stored
        .value
        .as_ref()
        .ok_or_else(|| FilterError::new("stored value holds no payload"))?;
    Ok(match value {
        V::StringValue(text) => CelValue::String(Arc::new(text.clone())),
        V::IntValue(v) => CelValue::Int(*v),
        V::UintValue(v) => CelValue::UInt(*v),
        V::DoubleValue(v) => CelValue::Float(*v),
        V::BoolValue(v) => CelValue::Bool(*v),
        V::BytesValue(bytes) => CelValue::Bytes(Arc::new(bytes.clone())),
        V::TimestampValue(timestamp) => {
            let nanos = u32::try_from(timestamp.nanos).map_err(|_| {
                FilterError::new(format!(
                    "stored timestamp nanos {} is negative",
                    timestamp.nanos
                ))
            })?;
            let instant =
                chrono::DateTime::from_timestamp(timestamp.seconds, nanos).ok_or_else(|| {
                    FilterError::new(format!(
                        "stored timestamp seconds={} is out of range",
                        timestamp.seconds
                    ))
                })?;
            CelValue::Timestamp(instant.fixed_offset())
        }
        V::ListValue(list) => {
            let mut values = Vec::with_capacity(list.values.len());
            for element in &list.values {
                values.push(cel_value(element)?);
            }
            CelValue::List(Arc::new(values))
        }
    })
}
