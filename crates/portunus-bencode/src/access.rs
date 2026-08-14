//! Typed, non-panicking access to borrowed bencode trees.
//!
//! Direct accessors validate one node. [`Value::at_path`] validates a sequence
//! of raw-byte dictionary keys and list indices while retaining the exact segment
//! where traversal stopped:
//!
//! ```text
//! root ──Key(b"info")──> dictionary ──Key(b"files")──> list ──Index(1)──> leaf
//!   segment 0                         segment 1                segment 2
//! ```
//!
//! Path keys borrow caller memory and selected byte strings continue borrowing
//! parser input. Traversal does not allocate, decode text, coerce types, or apply
//! an application schema. Typed deserialization can build on this layer without
//! introducing torrent-specific concepts into the syntax tree.

use crate::Value;
use std::{collections::BTreeMap, fmt};
use thiserror::Error;

/// The stable semantic kind of a bencode value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueKind {
    Bytes,
    Integer,
    List,
    Dictionary,
}

impl fmt::Display for ValueKind {
    // Inputs: one stable value kind and a mutable formatting destination.
    // Outputs: a lowercase human-readable kind name or a formatting error.
    // Logic: keep error messages independent from Rust enum debug formatting.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Bytes => "bytes",
            Self::Integer => "integer",
            Self::List => "list",
            Self::Dictionary => "dictionary",
        };
        formatter.write_str(name)
    }
}

/// A direct typed-access mismatch at one value node.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("expected {expected}, found {actual}")]
pub struct TypeError {
    pub expected: ValueKind,
    pub actual: ValueKind,
}

/// One protocol-neutral step through a nested value tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSegment<'path> {
    Key(&'path [u8]),
    Index(usize),
}

/// A precise failure while traversing a nested path.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum PathError<'path> {
    #[error("expected {expected}, found {actual} at path segment {segment}")]
    TypeMismatch {
        segment: usize,
        expected: ValueKind,
        actual: ValueKind,
    },
    #[error("missing dictionary key {key:?} at path segment {segment}")]
    MissingKey { segment: usize, key: &'path [u8] },
    #[error("list index {index} is outside length {len} at path segment {segment}")]
    IndexOutOfBounds {
        segment: usize,
        index: usize,
        len: usize,
    },
}

impl<'input> Value<'input> {
    /// Returns this node's stable semantic kind.
    ///
    /// **Inputs:** A shared value borrow.
    ///
    /// **Outputs:** One copyable [`ValueKind`]; no state changes.
    ///
    /// **Logic:** Map variants explicitly so error contracts do not depend on
    /// derived debug names.
    #[must_use]
    pub const fn kind(&self) -> ValueKind {
        match self {
            Self::Bytes(_) => ValueKind::Bytes,
            Self::Integer(_) => ValueKind::Integer,
            Self::List(_) => ValueKind::List,
            Self::Dictionary(_) => ValueKind::Dictionary,
        }
    }

    /// Borrows this node's byte-string payload.
    ///
    /// **Inputs:** A shared value whose byte payload may borrow parser input.
    ///
    /// **Outputs:** The original borrowed bytes or an exact kind mismatch.
    ///
    /// **Logic:** Return the stored input slice without allocation or text decoding.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError`] when this node is not [`ValueKind::Bytes`].
    pub const fn as_bytes(&self) -> Result<&'input [u8], TypeError> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            _ => Err(self.type_error(ValueKind::Bytes)),
        }
    }

    /// Copies this node's signed integer value.
    ///
    /// **Inputs:** A shared value borrow.
    ///
    /// **Outputs:** The stored `i64` or an exact kind mismatch.
    ///
    /// **Logic:** Integers are copyable, so no output lifetime or allocation is needed.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError`] when this node is not [`ValueKind::Integer`].
    pub const fn as_integer(&self) -> Result<i64, TypeError> {
        match self {
            Self::Integer(integer) => Ok(*integer),
            _ => Err(self.type_error(ValueKind::Integer)),
        }
    }

    /// Borrows this node's ordered list entries.
    ///
    /// **Inputs:** A shared value borrow.
    ///
    /// **Outputs:** A slice tied to the receiver borrow or an exact kind mismatch.
    ///
    /// **Logic:** Hide vector mutation while preserving order and constant-time length.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError`] when this node is not [`ValueKind::List`].
    pub const fn as_list(&self) -> Result<&[Self], TypeError> {
        match self {
            Self::List(values) => Ok(values.as_slice()),
            _ => Err(self.type_error(ValueKind::List)),
        }
    }

    /// Borrows this node's byte-keyed dictionary.
    ///
    /// **Inputs:** A shared value borrow.
    ///
    /// **Outputs:** The ordered dictionary tied to the receiver or a kind mismatch.
    ///
    /// **Logic:** Preserve raw borrowed keys and expose lookup without text coercion.
    ///
    /// # Errors
    ///
    /// Returns [`TypeError`] when this node is not [`ValueKind::Dictionary`].
    pub const fn as_dictionary(&self) -> Result<&BTreeMap<&'input [u8], Self>, TypeError> {
        match self {
            Self::Dictionary(values) => Ok(values),
            _ => Err(self.type_error(ValueKind::Dictionary)),
        }
    }

    /// Traverses raw dictionary keys and list indices from this node.
    ///
    /// **Inputs:** A shared root and ordered path whose keys borrow caller memory.
    ///
    /// **Outputs:** The selected node tied to the root borrow, or a precise error
    /// tied to the path when a segment cannot be applied.
    ///
    /// **Logic:** Validate each segment before lookup/indexing, update the current
    /// node only after success, and retain the first failing segment number.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] for incompatible containers, missing keys, or list
    /// indices greater than or equal to the observed list length.
    pub fn at_path<'value, 'path>(
        &'value self,
        path: &'path [PathSegment<'path>],
    ) -> Result<&'value Self, PathError<'path>> {
        let mut current = self;
        for (segment_index, segment) in path.iter().enumerate() {
            current = match segment {
                PathSegment::Key(key) => match current {
                    Self::Dictionary(values) => values.get(*key).ok_or(PathError::MissingKey {
                        segment: segment_index,
                        key,
                    })?,
                    _ => {
                        return Err(PathError::TypeMismatch {
                            segment: segment_index,
                            expected: ValueKind::Dictionary,
                            actual: current.kind(),
                        });
                    }
                },
                PathSegment::Index(index) => match current {
                    Self::List(values) => {
                        values.get(*index).ok_or(PathError::IndexOutOfBounds {
                            segment: segment_index,
                            index: *index,
                            len: values.len(),
                        })?
                    }
                    _ => {
                        return Err(PathError::TypeMismatch {
                            segment: segment_index,
                            expected: ValueKind::List,
                            actual: current.kind(),
                        });
                    }
                },
            };
        }
        Ok(current)
    }

    // Inputs: one actual value node and the expected semantic kind.
    // Outputs: a stable direct-access mismatch containing both kinds.
    // Logic: centralize error construction so every accessor reports consistently.
    const fn type_error(&self, expected: ValueKind) -> TypeError {
        TypeError {
            expected,
            actual: self.kind(),
        }
    }
}
