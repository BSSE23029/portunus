use portunus_bencode::{from_slice, DeserializePath};
use serde::Deserialize;
use std::collections::BTreeMap;

#[path = "deserialize/collections.rs"]
mod collections;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Metadata<'a> {
    #[serde(borrow)]
    announce: &'a [u8],
    #[serde(borrow)]
    info: Info<'a>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Info<'a> {
    length: i64,
    name: &'a str,
    #[serde(rename = "piece length")]
    piece_length: u64,
    #[serde(borrow)]
    pieces: &'a [u8],
}

// Inputs: deterministic nested torrent metadata and a borrowing Serde target.
// Outputs: typed fields whose byte/string references point into the original input.
// Logic: prove generic struct decoding and input borrowing over a realistic corpus.
#[test]
fn deserializes_borrowed_reference_metadata() {
    let fixture = include_bytes!("fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);
    let decoded: Metadata<'_> = from_slice(metadata).unwrap();

    assert_eq!(decoded.announce, b"http://tracker.test/announce");
    assert_eq!(decoded.info.length, 5);
    assert_eq!(decoded.info.name, "test.bin");
    assert_eq!(decoded.info.piece_length, 16_384);
    assert_eq!(decoded.info.pieces, b"12345678901234567890");
    assert!(metadata
        .as_ptr_range()
        .contains(&decoded.info.pieces.as_ptr()));
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct WrongLength {
    info: WrongInfo,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct WrongInfo {
    length: i64,
}

// Inputs: a nested field whose encoded value has the wrong semantic type.
// Outputs: an error path ordered from root key to failing leaf key.
// Logic: ensure collection adapters add context only while failures unwind.
#[test]
fn reports_nested_dictionary_error_paths() {
    let error = from_slice::<WrongLength>(b"d4:infod6:length3:badee").unwrap_err();

    assert_eq!(
        error.path(),
        &[
            DeserializePath::Key(b"info".to_vec()),
            DeserializePath::Key(b"length".to_vec()),
        ]
    );
    assert!(error.to_string().contains("integer"));
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Unsigned {
    count: u64,
}

// Inputs: a negative integer requested as an unsigned field.
// Outputs: a field-qualified conversion error rather than numeric wrapping.
// Logic: preserve Rust target ranges at the binary/schema boundary.
#[test]
fn rejects_negative_unsigned_values() {
    let error = from_slice::<Unsigned>(b"d5:counti-1ee").unwrap_err();
    assert_eq!(error.path(), &[DeserializePath::Key(b"count".to_vec())]);
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Text<'a> {
    value: &'a str,
}

// Inputs: a non-UTF-8 byte string requested first as text and then as raw bytes.
// Outputs: text validation failure while binary deserialization remains lossless.
// Logic: keep bencode byte-native and validate UTF-8 only for textual target types.
#[test]
fn validates_utf8_only_for_text_targets() {
    let error = from_slice::<Text<'_>>(b"d5:value1:\xffe").unwrap_err();
    assert_eq!(error.path(), &[DeserializePath::Key(b"value".to_vec())]);

    let raw: BTreeMap<&[u8], &[u8]> = from_slice(b"d5:value1:\xffe").unwrap();
    assert_eq!(raw[b"value".as_slice()], b"\xff");
}
