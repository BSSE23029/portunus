use portunus_bencode::{
    parse_spanned, Error, Limits, PathError, PathSegment, SpannedKind, ValueKind,
};

#[path = "spanned/parser.rs"]
mod parser;

// Inputs: deterministic metadata and a path to its nested `info` dictionary.
// Outputs: the original encoded dictionary bytes and their exact half-open range.
// Logic: prove hashing callers can consume the wire slice without re-encoding it.
#[test]
fn exposes_exact_nested_encoding_from_reference_metadata() {
    let fixture = include_bytes!("fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);
    let document = parse_spanned(metadata).unwrap();
    let info = document.at_path(&[PathSegment::Key(b"info")]).unwrap();

    assert_eq!(info.span(), 48..130);
    assert_eq!(info.encoded(), &metadata[48..130]);
    assert_eq!(info.encoded().as_ptr(), metadata[48..].as_ptr());
    assert!(matches!(info.kind(), SpannedKind::Dictionary(_)));
}

// Inputs: every bencode value form nested below list and dictionary containers.
// Outputs: exact token ranges including scalar prefixes and container terminators.
// Logic: pin inclusive starts and exclusive ends for root, container, and leaf nodes.
#[test]
fn reports_half_open_spans_for_every_value_form() {
    let document = parse_spanned(b"d1:ali7e1:xee").unwrap();
    let list = document.at_path(&[PathSegment::Key(b"a")]).unwrap();
    let integer = list.at_path(&[PathSegment::Index(0)]).unwrap();
    let bytes = list.at_path(&[PathSegment::Index(1)]).unwrap();

    assert_eq!(document.span(), 0..13);
    assert_eq!(list.span(), 4..12);
    assert_eq!(integer.span(), 5..8);
    assert_eq!(bytes.span(), 8..11);
    assert_eq!(bytes.encoded(), b"1:x");
}

// Inputs: malformed, trailing, and over-budget encodings.
// Outputs: the same stable parser errors exposed by ordinary parsing.
// Logic: exact-span parsing must preserve hostile-input validation and limits.
#[test]
fn preserves_parser_errors_and_resource_limits() {
    assert_eq!(parse_spanned(b"i03e"), Err(Error::InvalidInteger(0)));
    assert_eq!(parse_spanned(b"1:xy"), Err(Error::TrailingData(3)));
    assert!(matches!(
        portunus_bencode::parse_spanned_with_limits(b"li1ee", Limits::new(5, 0, 1, 1)),
        Err(Error::LimitExceeded { .. })
    ));
}

// Inputs: missing, one-over-index, and wrong-container traversal segments.
// Outputs: stable errors identifying the first unusable segment and observed kind.
// Logic: keep spanned traversal behavior aligned with ordinary value traversal.
#[test]
fn reports_precise_path_failures() {
    let document = parse_spanned(b"d1:ali1eee").unwrap();

    assert_eq!(
        document.at_path(&[PathSegment::Key(b"missing")]),
        Err(PathError::MissingKey {
            segment: 0,
            key: b"missing",
        })
    );
    assert_eq!(
        document.at_path(&[PathSegment::Key(b"a"), PathSegment::Index(1)]),
        Err(PathError::IndexOutOfBounds {
            segment: 1,
            index: 1,
            len: 1,
        })
    );
    assert_eq!(
        document.at_path(&[PathSegment::Index(0)]),
        Err(PathError::TypeMismatch {
            segment: 0,
            expected: ValueKind::List,
            actual: ValueKind::Dictionary,
        })
    );
}
