//! A small, zero-copy bencode decoder.
//!
//! Bencode has four tokens: byte strings (`4:spam`), integers (`i42e`), lists
//! (`l...e`), and dictionaries (`d...e`). The parser stores byte strings as
//! slices of the input, which avoids allocating and copying their contents.
//!
//! ```text
//! d4:name4:datae
//! │ └─key └─value
//! └─dictionary                  => { b"name": b"data" }
//! ```
//!
//! # Example
//!
//! ```
//! use portunus_bencode::{parse, Value};
//!
//! let input = b"li7e4:spame";
//! let value = parse(input)?;
//! assert_eq!(value, Value::List(vec![Value::Integer(7), Value::Bytes(b"spam")]));
//! # Ok::<(), portunus_bencode::Error>(())
//! ```
use std::collections::BTreeMap;
use thiserror::Error;

mod access;
mod deserialize;
mod encode;
mod limits;

pub use access::{PathError, PathSegment, TypeError, ValueKind};
pub use deserialize::{from_slice, from_value, DeserializeError, DeserializePath};
pub use encode::encode;
pub use limits::{LimitKind, Limits};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
    List(Vec<Self>),
    Dictionary(BTreeMap<&'a [u8], Self>),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("invalid bencode token at byte {0}")]
    InvalidToken(usize),
    #[error("invalid integer at byte {0}")]
    InvalidInteger(usize),
    #[error("invalid byte string length at byte {0}")]
    InvalidLength(usize),
    #[error("dictionary key must be a byte string at byte {0}")]
    NonByteKey(usize),
    #[error("trailing data at byte {0}")]
    TrailingData(usize),
    #[error("{kind:?} limit {limit} exceeded at byte {offset}")]
    LimitExceeded {
        kind: LimitKind,
        offset: usize,
        limit: usize,
    },
}

/// Parses exactly one complete bencoded value.
///
/// **Inputs:** `input` is the complete encoded byte slice. It remains borrowed
/// for as long as the returned [`Value`] exists.
///
/// **Outputs:** A zero-copy [`Value`] on success, or an [`enum@Error`] describing
/// malformed input, excessive nesting, or trailing bytes.
///
/// **Logic:** Delegate recursive token recognition to `parse_at`, then require
/// its cursor to equal the input length so accidental trailing data is rejected.
///
/// # Errors
///
/// Returns a typed syntax, boundary, trailing-data, or nesting-limit error.
pub fn parse(input: &[u8]) -> Result<Value<'_>, Error> {
    parse_with_limits(input, Limits::default())
}

/// Parses exactly one complete bencoded value under explicit resource limits.
///
/// **Inputs:** `input` is the complete borrowed encoding and `limits` supplies
/// inclusive ceilings for all parser-controlled resources.
///
/// **Outputs:** A borrowed [`Value`] on success, or a precise syntax, boundary,
/// trailing-data, or resource-limit error.
///
/// **Logic:** Reject oversized input before traversal, recursively decode with
/// the same policy, then require the cursor to consume the complete input.
///
/// # Errors
///
/// Returns a typed error at the byte where malformed input or excess work is
/// first observable.
pub fn parse_with_limits(input: &[u8], limits: Limits) -> Result<Value<'_>, Error> {
    if input.len() > limits.input_len {
        return Err(Error::LimitExceeded {
            kind: LimitKind::InputLength,
            offset: limits.input_len,
            limit: limits.input_len,
        });
    }
    let (value, consumed) = parse_at(input, 0, 0, &limits)?;
    if consumed != input.len() {
        return Err(Error::TrailingData(consumed));
    }
    Ok(value)
}

// Inputs:
// - `input`: the complete backing byte slice.
// - `pos`: the byte offset of the next token.
// - `depth`: number of containers enclosing the next token.
// - `limits`: shared immutable resource policy.
// Outputs:
// - A borrowed value and the offset immediately after it, or a parse error.
// Logic:
// - Inspect the leading token and recursively parse list/dictionary children.
//   The explicit depth counter bounds stack use for hostile nested input.
fn parse_at<'a>(
    input: &'a [u8],
    pos: usize,
    depth: usize,
    limits: &Limits,
) -> Result<(Value<'a>, usize), Error> {
    match input.get(pos).copied().ok_or(Error::UnexpectedEof)? {
        b'i' => parse_integer(input, pos),
        b'l' => {
            enforce_depth(depth, pos, limits)?;
            let mut values = Vec::new();
            let mut cursor = pos + 1;
            while input.get(cursor) != Some(&b'e') {
                enforce_collection_len(values.len(), cursor, limits)?;
                let (value, next) = parse_at(input, cursor, depth + 1, limits)?;
                values.push(value);
                cursor = next;
            }
            Ok((Value::List(values), cursor + 1))
        }
        b'd' => {
            enforce_depth(depth, pos, limits)?;
            let mut values = BTreeMap::new();
            let mut cursor = pos + 1;
            while input.get(cursor) != Some(&b'e') {
                enforce_collection_len(values.len(), cursor, limits)?;
                let (key, next) = parse_bytes(input, cursor, limits)?;
                let Value::Bytes(key) = key else {
                    return Err(Error::NonByteKey(cursor));
                };
                let (value, next) = parse_at(input, next, depth + 1, limits)?;
                values.insert(key, value);
                cursor = next;
            }
            Ok((Value::Dictionary(values), cursor + 1))
        }
        b'0'..=b'9' => parse_bytes(input, pos, limits),
        _ => Err(Error::InvalidToken(pos)),
    }
}

// Inputs:
// - `input`: the complete backing byte slice.
// - `pos`: offset of the integer's leading `i` byte.
// Outputs:
// - `Value::Integer` and the position after `e`, or an integer-format error.
// Logic:
// - Find the terminator, enforce canonical spellings such as rejecting `03` and
//   `-0`, decode UTF-8 digits, and use Rust's checked `i64` parser.
fn parse_integer(input: &[u8], pos: usize) -> Result<(Value<'_>, usize), Error> {
    let end = input[pos + 1..]
        .iter()
        .position(|b| *b == b'e')
        .map(|n| pos + 1 + n)
        .ok_or(Error::UnexpectedEof)?;
    let raw = &input[pos + 1..end];
    if raw.is_empty()
        || raw == b"-0"
        || (raw[0] == b'0' && raw.len() > 1)
        || (raw.starts_with(b"-0"))
    {
        return Err(Error::InvalidInteger(pos));
    }
    let text = std::str::from_utf8(raw).map_err(|_| Error::InvalidInteger(pos))?;
    let number = text.parse().map_err(|_| Error::InvalidInteger(pos))?;
    Ok((Value::Integer(number), end + 1))
}

// Inputs:
// - `input`: the complete backing byte slice.
// - `pos`: offset of the first decimal length digit.
// Outputs:
// - A borrowed `Value::Bytes` and its ending offset, or a length/input error.
// Logic:
// - Parse `<length>:`, check the end offset without integer overflow, then borrow
//   that exact range rather than copying it into a new allocation.
fn parse_bytes<'a>(
    input: &'a [u8],
    pos: usize,
    limits: &Limits,
) -> Result<(Value<'a>, usize), Error> {
    let colon = input[pos..]
        .iter()
        .position(|b| *b == b':')
        .map(|n| pos + n)
        .ok_or(Error::UnexpectedEof)?;
    let raw_len = &input[pos..colon];
    if raw_len.is_empty() || (raw_len[0] == b'0' && raw_len.len() > 1) {
        return Err(Error::InvalidLength(pos));
    }
    let len: usize = std::str::from_utf8(raw_len)
        .map_err(|_| Error::InvalidLength(pos))?
        .parse()
        .map_err(|_| Error::InvalidLength(pos))?;
    if len > limits.byte_string_len {
        return Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: pos,
            limit: limits.byte_string_len,
        });
    }
    let start = colon + 1;
    let end = start.checked_add(len).ok_or(Error::InvalidLength(pos))?;
    let bytes = input.get(start..end).ok_or(Error::UnexpectedEof)?;
    Ok((Value::Bytes(bytes), end))
}

// Inputs: current enclosing-container count, token offset, and resource policy.
// Outputs: unit when another container is allowed, otherwise a precise limit error.
// Logic: treat the configured maximum as inclusive and identify the rejected token.
const fn enforce_depth(depth: usize, offset: usize, limits: &Limits) -> Result<(), Error> {
    if depth >= limits.depth {
        return Err(Error::LimitExceeded {
            kind: LimitKind::Depth,
            offset,
            limit: limits.depth,
        });
    }
    Ok(())
}

// Inputs: accepted item count, next item offset, and resource policy.
// Outputs: unit when another item is allowed, otherwise a precise limit error.
// Logic: check before parsing or allocating the first item beyond the ceiling.
const fn enforce_collection_len(count: usize, offset: usize, limits: &Limits) -> Result<(), Error> {
    if count >= limits.collection_len {
        return Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset,
            limit: limits.collection_len,
        });
    }
    Ok(())
}
