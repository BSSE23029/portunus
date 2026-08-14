//! Exact wire spans for borrowed bencode syntax trees.
//!
//! Span-aware parsing retains each complete encoded token alongside its decoded
//! representation. Ranges use absolute byte offsets into the original input and
//! are half-open: `start` is included and `end` is excluded.
//!
//! ```text
//! d1:ali7eee
//! 0          11  root dictionary: 0..11
//!     4    10     nested list:     4..10
//!      5  8       integer:         5..8
//! ```
//!
//! The tree borrows encoded bytes and scalar payloads but allocates list and map
//! containers. This opt-in parser does not change ordinary [`crate::parse`]
//! allocation behavior, hash data itself, interpret schemas, or retain invalid
//! partial trees.

use crate::{Error, LimitKind, Limits, PathError, PathSegment, ValueKind};
use std::{collections::BTreeMap, ops::Range};

mod parser;

/// One decoded bencode kind whose children retain their own exact spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpannedKind<'input> {
    Bytes(&'input [u8]),
    Integer(i64),
    List(Vec<SpannedValue<'input>>),
    Dictionary(BTreeMap<&'input [u8], SpannedValue<'input>>),
}

/// A decoded value tied to its exact original wire token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpannedValue<'input> {
    offset: usize,
    encoded: &'input [u8],
    kind: SpannedKind<'input>,
}

impl<'input> SpannedValue<'input> {
    /// Returns the node's absolute half-open byte range in the parser input.
    ///
    /// **Inputs:** A shared node borrow.
    ///
    /// **Outputs:** An owned `start..end` range; both offsets are measured in bytes.
    ///
    /// **Logic:** Add the retained token length to its absolute starting offset.
    #[must_use]
    pub const fn span(&self) -> Range<usize> {
        self.offset..self.offset + self.encoded.len()
    }

    /// Borrows the node's complete original encoding.
    ///
    /// **Inputs:** A shared node borrow.
    ///
    /// **Outputs:** The exact token slice, including prefixes and terminators.
    ///
    /// **Logic:** Return the parser-retained input subslice without copying.
    #[must_use]
    pub const fn encoded(&self) -> &'input [u8] {
        self.encoded
    }

    /// Borrows the decoded kind and any recursively spanned children.
    ///
    /// **Inputs:** A shared node borrow.
    ///
    /// **Outputs:** A kind reference tied to the receiver borrow.
    ///
    /// **Logic:** Expose semantic inspection without permitting tree mutation.
    #[must_use]
    pub const fn kind(&self) -> &SpannedKind<'input> {
        &self.kind
    }

    /// Traverses raw dictionary keys and list indices from this spanned node.
    ///
    /// **Inputs:** A shared root and ordered, borrowed protocol-neutral path.
    ///
    /// **Outputs:** The selected spanned node or the first precise traversal error.
    ///
    /// **Logic:** Apply each segment to decoded containers while retaining the
    /// selected node's original encoded slice and absolute range.
    ///
    /// # Errors
    ///
    /// Returns [`PathError`] for a wrong container, missing key, or invalid index.
    pub fn at_path<'value, 'path>(
        &'value self,
        path: &'path [PathSegment<'path>],
    ) -> Result<&'value Self, PathError<'path>> {
        let mut current = self;
        for (position, segment) in path.iter().enumerate() {
            current = match segment {
                PathSegment::Key(key) => match &current.kind {
                    SpannedKind::Dictionary(values) => {
                        values.get(*key).ok_or(PathError::MissingKey {
                            segment: position,
                            key,
                        })?
                    }
                    _ => return Err(current.path_type_error(position, ValueKind::Dictionary)),
                },
                PathSegment::Index(index) => match &current.kind {
                    SpannedKind::List(values) => {
                        values.get(*index).ok_or(PathError::IndexOutOfBounds {
                            segment: position,
                            index: *index,
                            len: values.len(),
                        })?
                    }
                    _ => return Err(current.path_type_error(position, ValueKind::List)),
                },
            };
        }
        Ok(current)
    }

    // Inputs: failing path position and required container kind.
    // Outputs: a traversal mismatch containing stable expected and actual kinds.
    // Logic: centralize kind mapping so path errors match the ordinary value API.
    const fn path_type_error<'path>(
        &self,
        segment: usize,
        expected: ValueKind,
    ) -> PathError<'path> {
        PathError::TypeMismatch {
            segment,
            expected,
            actual: match self.kind {
                SpannedKind::Bytes(_) => ValueKind::Bytes,
                SpannedKind::Integer(_) => ValueKind::Integer,
                SpannedKind::List(_) => ValueKind::List,
                SpannedKind::Dictionary(_) => ValueKind::Dictionary,
            },
        }
    }
}

/// Parses one complete input while retaining every exact encoded token slice.
///
/// **Inputs:** A complete borrowed byte slice governed by bounded default limits.
///
/// **Outputs:** A recursively spanned borrowed tree or a stable parser error.
///
/// **Logic:** Delegate to explicit-limit parsing so validation remains identical.
///
/// # Errors
///
/// Returns [`enum@Error`] for malformed, truncated, trailing, or over-budget input.
pub fn parse_spanned(input: &[u8]) -> Result<SpannedValue<'_>, Error> {
    parse_spanned_with_limits(input, Limits::default())
}

/// Parses one complete input with exact spans and explicit resource ceilings.
///
/// **Inputs:** A borrowed encoding and independent inclusive parser budgets.
///
/// **Outputs:** A spanned tree borrowing `input`, or the first stable parser error.
///
/// **Logic:** Reject total size first, recursively parse one root, then reject
/// trailing bytes so every returned root span covers the complete input.
///
/// # Errors
///
/// Returns [`enum@Error`] for syntax, truncation, trailing data, or exceeded limits.
pub fn parse_spanned_with_limits(input: &[u8], limits: Limits) -> Result<SpannedValue<'_>, Error> {
    if input.len() > limits.input_len {
        return Err(Error::LimitExceeded {
            kind: LimitKind::InputLength,
            offset: limits.input_len,
            limit: limits.input_len,
        });
    }
    let (value, consumed) = parser::parse_at(input, 0, 0, &limits)?;
    if consumed != input.len() {
        return Err(Error::TrailingData(consumed));
    }
    Ok(value)
}
