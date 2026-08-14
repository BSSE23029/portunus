//! Borrow-preserving Serde deserialization from bencode values and input slices.
//!
//! The adapter maps byte strings to borrowed bytes or validated UTF-8 text,
//! integers to checked Rust numeric targets, lists to sequences, and dictionaries
//! to maps/structs. Collection adapters attach context while errors unwind:
//!
//! ```text
//! type mismatch
//!      └── prepend Key(b"length")
//!              └── prepend Key(b"info")  => $.info.length
//! ```
//!
//! Successful traversal does not allocate path state or copy borrowed scalar
//! payloads. Target-owned vectors/maps allocate according to their Serde models.
//! This module does not coerce booleans, interpret application schemas, retain
//! encoded spans, or weaken parser resource limits.

use crate::{
    deserialize_collections::{DictionaryAccess, ListAccess},
    parse, Value,
};
use serde::{
    de::{self, Visitor},
    Deserialize,
};
use std::fmt;

/// One owned segment in a failed deserialization path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeserializePath {
    Key(Vec<u8>),
    Index(usize),
}

/// A Serde conversion failure with root-to-leaf structural context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeserializeError {
    path: Vec<DeserializePath>,
    message: String,
}

impl DeserializeError {
    /// Borrows the ordered root-to-leaf failure path.
    ///
    /// **Inputs:** A shared error borrow.
    ///
    /// **Outputs:** An immutable path slice; empty means the root itself failed.
    ///
    /// **Logic:** Expose stable structured context separately from display text.
    #[must_use]
    pub fn path(&self) -> &[DeserializePath] {
        &self.path
    }

    /// Borrows the underlying conversion or syntax explanation.
    ///
    /// **Inputs:** A shared error borrow.
    ///
    /// **Outputs:** A stable message slice without rendered path decoration.
    ///
    /// **Logic:** Allow control planes to structure paths while retaining detail.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    // Inputs: one leaf explanation with no structural context yet.
    // Outputs: a root-local error owning its message.
    // Logic: allocate error state only after a conversion has failed.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            path: Vec::new(),
            message: message.into(),
        }
    }

    // Inputs: an existing child error and its immediate parent path segment.
    // Outputs: the same error with the parent inserted before child context.
    // Logic: build root-to-leaf order while recursive Serde calls unwind.
    pub(crate) fn prepend(mut self, segment: DeserializePath) -> Self {
        self.path.insert(0, segment);
        self
    }
}

impl fmt::Display for DeserializeError {
    // Inputs: structured path/message state and a formatting destination.
    // Outputs: human-readable path plus explanation, or a formatting error.
    // Logic: render binary keys losslessly with debug notation and indices plainly.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("$")?;
        for segment in &self.path {
            match segment {
                DeserializePath::Key(key) => write!(formatter, "[{key:?}]")?,
                DeserializePath::Index(index) => write!(formatter, "[{index}]")?,
            }
        }
        write!(formatter, ": {}", self.message)
    }
}

impl std::error::Error for DeserializeError {}

impl de::Error for DeserializeError {
    // Inputs: any displayable custom error supplied by Serde or a target type.
    // Outputs: an owned root-local deserialization error.
    // Logic: retain third-party target explanations without guessing structure.
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self::new(message.to_string())
    }
}

/// Parses and deserializes one complete bencode input.
///
/// **Inputs:** A complete borrowed byte slice and target implementing Serde
/// deserialization for that input lifetime.
///
/// **Outputs:** A typed target that may borrow input bytes, or a syntax/conversion
/// error with structural path context.
///
/// **Logic:** Apply the bounded default parser, then deserialize from its borrowed
/// tree; returned borrows point to `input`, not the temporary tree containers.
///
/// # Errors
///
/// Returns [`DeserializeError`] for parser rejection, schema mismatch, invalid
/// UTF-8 text, numeric range failure, or target-defined Serde errors.
pub fn from_slice<'de, T>(input: &'de [u8]) -> Result<T, DeserializeError>
where
    T: Deserialize<'de>,
{
    let value = parse(input).map_err(|error| DeserializeError::new(error.to_string()))?;
    from_value(&value)
}

/// Deserializes a typed target from an existing borrowed syntax tree.
///
/// **Inputs:** A shared tree whose scalar bytes borrow for lifetime `'de`.
///
/// **Outputs:** A typed target that may retain those scalar borrows, or a precise
/// conversion error; the tree is not mutated.
///
/// **Logic:** Present the root through the Serde data-model adapter without
/// re-encoding or reparsing it.
///
/// # Errors
///
/// Returns [`DeserializeError`] for schema, text, range, or target errors.
pub fn from_value<'de, T>(value: &Value<'de>) -> Result<T, DeserializeError>
where
    T: Deserialize<'de>,
{
    T::deserialize(NodeDeserializer { value })
}

#[derive(Clone, Copy)]
pub struct NodeDeserializer<'value, 'de> {
    pub value: &'value Value<'de>,
}

impl NodeDeserializer<'_, '_> {
    // Inputs: the actual node and a target type description.
    // Outputs: a root-local mismatch error.
    // Logic: keep mismatch wording consistent across Serde entry points.
    fn expected(self, expected: &str) -> DeserializeError {
        DeserializeError::new(format!("expected {expected}, found {}", self.value.kind()))
    }
}

impl<'de> de::Deserializer<'de> for NodeDeserializer<'_, 'de> {
    type Error = DeserializeError;

    // Inputs: one borrowed node and a target-provided visitor.
    // Outputs: visitor output or a conversion error.
    // Logic: dispatch the four bencode kinds into their closest Serde data models.
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        match self.value {
            Value::Bytes(bytes) => visitor.visit_borrowed_bytes(bytes),
            Value::Integer(integer) => visitor.visit_i64(*integer),
            Value::List(values) => visitor.visit_seq(ListAccess::new(values)),
            Value::Dictionary(values) => visitor.visit_map(DictionaryAccess::new(values)),
        }
    }

    // Inputs: a byte-string node and byte-oriented visitor.
    // Outputs: borrowed bytes or a type mismatch.
    // Logic: preserve the parser input slice without copying.
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.value
            .as_bytes()
            .map_err(|_| self.expected("bytes"))
            .and_then(|bytes| visitor.visit_borrowed_bytes(bytes))
    }

    // Inputs: a byte-string node and owned-byte visitor.
    // Outputs: visitor-selected byte representation or a type mismatch.
    // Logic: offer borrowed bytes; visitors allocate only when their target owns data.
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    // Inputs: a byte-string node requested as UTF-8 text.
    // Outputs: borrowed text or a validation/type error.
    // Logic: validate UTF-8 only at this textual target boundary.
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let bytes = self
            .value
            .as_bytes()
            .map_err(|_| self.expected("UTF-8 bytes"))?;
        let text = std::str::from_utf8(bytes)
            .map_err(|error| DeserializeError::new(format!("invalid UTF-8: {error}")))?;
        visitor.visit_borrowed_str(text)
    }

    // Inputs: a byte-string node requested as owned text.
    // Outputs: visitor-selected string representation or a validation/type error.
    // Logic: reuse borrowed validation; target visitor decides whether to allocate.
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    // Inputs: an integer node and signed-integer visitor.
    // Outputs: the stored i64 or a type/range error from the target visitor.
    // Logic: preserve the full bencode integer domain before target narrowing.
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.value
            .as_integer()
            .map_err(|_| self.expected("integer"))
            .and_then(|integer| visitor.visit_i64(integer))
    }

    // Inputs: a nonnegative integer node and unsigned-integer visitor.
    // Outputs: converted u64 or a type/range error.
    // Logic: reject negatives explicitly rather than wrapping their bit pattern.
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let integer = self
            .value
            .as_integer()
            .map_err(|_| self.expected("nonnegative integer"))?;
        let unsigned = u64::try_from(integer)
            .map_err(|_| DeserializeError::new("expected nonnegative integer"))?;
        visitor.visit_u64(unsigned)
    }

    // Inputs: any present bencode value and an option visitor.
    // Outputs: `Some` target output or its nested error.
    // Logic: bencode has no null token, so every existing node is present.
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_some(self)
    }

    // Inputs: a transparently wrapped target and visitor.
    // Outputs: nested target output or its error.
    // Logic: newtypes have no bencode wire representation of their own.
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        visitor.visit_newtype_struct(self)
    }

    // Inputs: any node and a visitor discarding its content.
    // Outputs: visitor unit output.
    // Logic: parsing already validated the subtree, so no traversal is required.
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i128 u8 u16 u32 u128 f32 f64 char unit unit_struct
        seq tuple tuple_struct map struct enum identifier
    }
}
