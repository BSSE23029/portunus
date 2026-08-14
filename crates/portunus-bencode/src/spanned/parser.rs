//! Recursive grammar mechanics for exact-span bencode parsing.
//!
//! Every successful node retains the complete `start..end` token slice after its
//! children have been validated. Limits are checked before protected recursion,
//! collection growth, or byte exposure. This module builds span-aware syntax
//! only; it does not expose public entry points, interpret schemas, hash bytes,
//! perform I/O, or alter the ordinary parser's allocation profile.

use super::{SpannedKind, SpannedValue};
use crate::{Error, LimitKind, Limits};
use std::collections::BTreeMap;

// Inputs: complete input, next token offset, enclosing depth, and resource policy.
// Outputs: a recursively spanned node and its exclusive ending cursor, or error.
// Logic: dispatch one token while retaining absolute offsets through recursion.
pub(super) fn parse_at<'input>(
    input: &'input [u8],
    pos: usize,
    depth: usize,
    limits: &Limits,
) -> Result<(SpannedValue<'input>, usize), Error> {
    match input.get(pos).copied().ok_or(Error::UnexpectedEof)? {
        b'i' => parse_integer(input, pos),
        b'0'..=b'9' => parse_bytes(input, pos, limits),
        b'l' => parse_list(input, pos, depth, limits),
        b'd' => parse_dictionary(input, pos, depth, limits),
        _ => Err(Error::InvalidToken(pos)),
    }
}

// Inputs: complete input, list-token offset, enclosing depth, and limits.
// Outputs: a spanned ordered list and exclusive cursor, or bounded parse error.
// Logic: validate depth/items before recursion and include both container markers.
fn parse_list<'input>(
    input: &'input [u8],
    pos: usize,
    depth: usize,
    limits: &Limits,
) -> Result<(SpannedValue<'input>, usize), Error> {
    enforce_depth(depth, pos, limits)?;
    let mut values = Vec::new();
    let mut cursor = pos + 1;
    while input.get(cursor) != Some(&b'e') {
        enforce_collection_len(values.len(), cursor, limits)?;
        let (value, next) = parse_at(input, cursor, depth + 1, limits)?;
        values.push(value);
        cursor = next;
    }
    let end = cursor + 1;
    Ok((node(input, pos, end, SpannedKind::List(values)), end))
}

// Inputs: complete input, dictionary offset, enclosing depth, and limits.
// Outputs: a raw-byte-keyed spanned map and exclusive cursor, or parse error.
// Logic: parse bounded byte keys then values, retaining canonical map semantics.
fn parse_dictionary<'input>(
    input: &'input [u8],
    pos: usize,
    depth: usize,
    limits: &Limits,
) -> Result<(SpannedValue<'input>, usize), Error> {
    enforce_depth(depth, pos, limits)?;
    let mut values = BTreeMap::new();
    let mut cursor = pos + 1;
    while input.get(cursor) != Some(&b'e') {
        enforce_collection_len(values.len(), cursor, limits)?;
        let (key, next) = parse_bytes(input, cursor, limits)?;
        let SpannedKind::Bytes(key) = key.kind else {
            return Err(Error::NonByteKey(cursor));
        };
        let (value, next) = parse_at(input, next, depth + 1, limits)?;
        values.insert(key, value);
        cursor = next;
    }
    let end = cursor + 1;
    Ok((node(input, pos, end, SpannedKind::Dictionary(values)), end))
}

// Inputs: complete input and offset of an integer's leading marker.
// Outputs: a spanned signed integer and exclusive cursor, or canonicality error.
// Logic: find the terminator, reject ambiguous spellings, then parse checked i64.
fn parse_integer(input: &[u8], pos: usize) -> Result<(SpannedValue<'_>, usize), Error> {
    let end = input[pos + 1..]
        .iter()
        .position(|byte| *byte == b'e')
        .map(|distance| pos + 1 + distance)
        .ok_or(Error::UnexpectedEof)?;
    let raw = &input[pos + 1..end];
    if raw.is_empty() || raw == b"-0" || (raw[0] == b'0' && raw.len() > 1) || raw.starts_with(b"-0")
    {
        return Err(Error::InvalidInteger(pos));
    }
    let text = std::str::from_utf8(raw).map_err(|_| Error::InvalidInteger(pos))?;
    let integer = text.parse().map_err(|_| Error::InvalidInteger(pos))?;
    let cursor = end + 1;
    Ok((
        node(input, pos, cursor, SpannedKind::Integer(integer)),
        cursor,
    ))
}

// Inputs: complete input, first length-digit offset, and byte-string limit.
// Outputs: a spanned borrowed byte string and exclusive cursor, or parse error.
// Logic: validate canonical length, checked end arithmetic, budget, and availability.
fn parse_bytes<'input>(
    input: &'input [u8],
    pos: usize,
    limits: &Limits,
) -> Result<(SpannedValue<'input>, usize), Error> {
    let colon = input[pos..]
        .iter()
        .position(|byte| *byte == b':')
        .map(|distance| pos + distance)
        .ok_or(Error::UnexpectedEof)?;
    let raw_length = &input[pos..colon];
    if raw_length.is_empty() || (raw_length[0] == b'0' && raw_length.len() > 1) {
        return Err(Error::InvalidLength(pos));
    }
    let length: usize = std::str::from_utf8(raw_length)
        .map_err(|_| Error::InvalidLength(pos))?
        .parse()
        .map_err(|_| Error::InvalidLength(pos))?;
    if length > limits.byte_string_len {
        return Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: pos,
            limit: limits.byte_string_len,
        });
    }
    let payload_start = colon + 1;
    let end = payload_start
        .checked_add(length)
        .ok_or(Error::InvalidLength(pos))?;
    let payload = input.get(payload_start..end).ok_or(Error::UnexpectedEof)?;
    Ok((node(input, pos, end, SpannedKind::Bytes(payload)), end))
}

// Inputs: complete input, absolute start/end offsets, and decoded node kind.
// Outputs: a node borrowing the exact validated token slice.
// Logic: construct only with parser-produced in-bounds offsets.
fn node<'input>(
    input: &'input [u8],
    offset: usize,
    end: usize,
    kind: SpannedKind<'input>,
) -> SpannedValue<'input> {
    SpannedValue {
        offset,
        encoded: &input[offset..end],
        kind,
    }
}

// Inputs: current enclosing count, token offset, and inclusive depth ceiling.
// Outputs: unit if entering is permitted, otherwise a precise limit error.
// Logic: reject before recursion when the current count reaches the ceiling.
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

// Inputs: accepted entries, next entry offset, and inclusive collection ceiling.
// Outputs: unit if another entry is permitted, otherwise a precise limit error.
// Logic: reject before parsing or allocating the first one-over-limit entry.
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
