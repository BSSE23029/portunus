//! Canonical serialization of borrowed bencode values.
//!
//! Canonical output uses minimal decimal spellings and bytewise-sorted unique
//! dictionary keys. [`Value::Dictionary`](crate::Value::Dictionary) stores keys
//! in a `BTreeMap<&[u8], _>`, so ordering and uniqueness are properties of the
//! public value model rather than last-minute encoder policy.
//!
//! ```text
//! Value::Dictionary({ b"a": 1 })
//!          │             │   │
//!          └──── d 1:a ──┘  i1e e
//!                key length   │ │
//!                         integer dictionary end
//! ```
//!
//! Encoding allocates one output vector. Byte-string payloads are copied once
//! into that vector; integer and length digits use a fixed stack buffer instead
//! of temporary strings. This module performs no I/O, hashing, compression, or
//! application-specific validation. Callers remain responsible for bounding
//! constructed value trees and the resulting output size.

use crate::Value;

/// Encodes one value using the canonical bencode representation.
///
/// **Inputs:** A shared value tree whose borrowed byte strings remain readable
/// for the duration of the call.
///
/// **Outputs:** A newly allocated byte vector containing exactly one complete
/// canonical value; no external state changes.
///
/// **Logic:** Traverse the value in semantic order, emit minimal decimal forms,
/// preserve list order, and rely on the dictionary's raw-byte ordering.
#[must_use]
pub fn encode(value: &Value<'_>) -> Vec<u8> {
    let mut output = Vec::new();
    append_value(value, &mut output);
    output
}

// Inputs: one borrowed value node and the caller-owned output vector.
// Outputs: the node's complete canonical bytes appended to `output`.
// Logic: emit scalar framing directly and recursively delimit ordered containers.
fn append_value(value: &Value<'_>, output: &mut Vec<u8>) {
    match value {
        Value::Bytes(bytes) => append_bytes(bytes, output),
        Value::Integer(integer) => {
            output.push(b'i');
            append_integer(*integer, output);
            output.push(b'e');
        }
        Value::List(values) => {
            output.push(b'l');
            for value in values {
                append_value(value, output);
            }
            output.push(b'e');
        }
        Value::Dictionary(values) => {
            output.push(b'd');
            for (key, value) in values {
                append_bytes(key, output);
                append_value(value, output);
            }
            output.push(b'e');
        }
    }
}

// Inputs: arbitrary binary payload and the caller-owned output vector.
// Outputs: minimal decimal length, colon, and payload appended once.
// Logic: encode byte count rather than characters so non-UTF-8 data is canonical.
fn append_bytes(bytes: &[u8], output: &mut Vec<u8>) {
    append_unsigned(bytes.len() as u128, output);
    output.push(b':');
    output.extend_from_slice(bytes);
}

// Inputs: any signed 64-bit integer and the caller-owned output vector.
// Outputs: its minimal base-ten ASCII spelling, without bencode delimiters.
// Logic: emit a sign only for negatives and use unsigned magnitude for `i64::MIN`.
fn append_integer(integer: i64, output: &mut Vec<u8>) {
    if integer < 0 {
        output.push(b'-');
    }
    append_unsigned(u128::from(integer.unsigned_abs()), output);
}

// Inputs: an unsigned integer and the caller-owned output vector.
// Outputs: its minimal base-ten ASCII digits appended without allocation.
// Logic: fill a fixed maximum-width buffer backwards, then append the used suffix.
fn append_unsigned(mut integer: u128, output: &mut Vec<u8>) {
    const MAX_DECIMAL_DIGITS: usize = 39;
    let mut digits = [0_u8; MAX_DECIMAL_DIGITS];
    let mut cursor = digits.len();
    loop {
        cursor -= 1;
        digits[cursor] = b'0' + (integer % 10) as u8;
        integer /= 10;
        if integer == 0 {
            break;
        }
    }
    output.extend_from_slice(&digits[cursor..]);
}
