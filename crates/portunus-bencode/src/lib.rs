//! A small, zero-copy bencode decoder.
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value<'a> {
    Bytes(&'a [u8]),
    Integer(i64),
    List(Vec<Value<'a>>),
    Dictionary(BTreeMap<&'a [u8], Value<'a>>),
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
    #[error("nesting limit exceeded")]
    NestingLimit,
}

pub fn parse(input: &[u8]) -> Result<Value<'_>, Error> {
    let (value, consumed) = parse_at(input, 0, 0)?;
    if consumed != input.len() {
        return Err(Error::TrailingData(consumed));
    }
    Ok(value)
}

fn parse_at(input: &[u8], pos: usize, depth: usize) -> Result<(Value<'_>, usize), Error> {
    if depth > 128 {
        return Err(Error::NestingLimit);
    }
    match input.get(pos).copied().ok_or(Error::UnexpectedEof)? {
        b'i' => parse_integer(input, pos),
        b'l' => {
            let mut values = Vec::new();
            let mut cursor = pos + 1;
            while input.get(cursor) != Some(&b'e') {
                let (value, next) = parse_at(input, cursor, depth + 1)?;
                values.push(value);
                cursor = next;
            }
            Ok((Value::List(values), cursor + 1))
        }
        b'd' => {
            let mut values = BTreeMap::new();
            let mut cursor = pos + 1;
            while input.get(cursor) != Some(&b'e') {
                let (key, next) = parse_bytes(input, cursor)?;
                let Value::Bytes(key) = key else {
                    return Err(Error::NonByteKey(cursor));
                };
                let (value, next) = parse_at(input, next, depth + 1)?;
                values.insert(key, value);
                cursor = next;
            }
            Ok((Value::Dictionary(values), cursor + 1))
        }
        b'0'..=b'9' => parse_bytes(input, pos),
        _ => Err(Error::InvalidToken(pos)),
    }
}

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

fn parse_bytes(input: &[u8], pos: usize) -> Result<(Value<'_>, usize), Error> {
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
    let start = colon + 1;
    let end = start.checked_add(len).ok_or(Error::InvalidLength(pos))?;
    let bytes = input.get(start..end).ok_or(Error::UnexpectedEof)?;
    Ok((Value::Bytes(bytes), end))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_nested_zero_copy_value() {
        let source = b"d4:listli42e4:spam e";
        assert!(matches!(parse(source), Err(Error::InvalidToken(_))));
        let source = b"d4:listli42e4:spamee";
        let Value::Dictionary(dict) = parse(source).unwrap() else {
            panic!()
        };
        assert_eq!(
            dict[b"list".as_slice()],
            Value::List(vec![Value::Integer(42), Value::Bytes(b"spam")])
        );
    }
    #[test]
    fn rejects_noncanonical_integer() {
        assert_eq!(parse(b"i03e"), Err(Error::InvalidInteger(0)));
    }
}
