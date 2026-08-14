use portunus_bencode::{parse, PathError, PathSegment, TypeError, Value, ValueKind};

// Inputs: each syntax-tree variant and its matching typed accessor.
// Outputs: borrowed containers/bytes and copied integer values without mutation.
// Logic: prove callers can inspect public values without pattern matching or panics.
#[test]
fn accesses_each_value_kind() {
    let bytes = Value::Bytes(b"payload");
    let integer = Value::Integer(42);
    let list = Value::List(vec![Value::Integer(1)]);
    let dictionary = parse(b"d1:ai2ee").unwrap();

    assert_eq!(bytes.as_bytes(), Ok(b"payload".as_slice()));
    assert_eq!(integer.as_integer(), Ok(42));
    assert_eq!(list.as_list().unwrap(), &[Value::Integer(1)]);
    assert_eq!(dictionary.as_dictionary().unwrap().len(), 1);
    assert_eq!(bytes.kind(), ValueKind::Bytes);
}

// Inputs: an integer requested through the byte-string accessor.
// Outputs: a stable error naming both expected and actual value kinds.
// Logic: make schema mismatches explicit instead of encoding absence as `None`.
#[test]
fn reports_typed_scalar_mismatches() {
    assert_eq!(
        Value::Integer(7).as_bytes(),
        Err(TypeError {
            expected: ValueKind::Bytes,
            actual: ValueKind::Integer,
        })
    );
}

// Inputs: a nested dictionary/list path over borrowed parsed metadata.
// Outputs: a reference to the selected leaf that still aliases the parse tree.
// Logic: alternate raw-byte keys and indices without allocating path components.
#[test]
fn traverses_borrowed_key_and_index_paths() {
    let value = parse(b"d4:infod5:filesl4:zero3:oneeee").unwrap();
    let path = [
        PathSegment::Key(b"info"),
        PathSegment::Key(b"files"),
        PathSegment::Index(1),
    ];

    assert_eq!(
        value.at_path(&path).unwrap().as_bytes(),
        Ok(b"one".as_slice())
    );
}

// Inputs: a path containing a dictionary key that is not present.
// Outputs: the failing segment and the original borrowed key in a typed error.
// Logic: preserve enough context for callers to report schema failures precisely.
#[test]
fn reports_missing_dictionary_keys() {
    let value = parse(b"d1:ai1ee").unwrap();
    let path = [PathSegment::Key(b"missing")];

    assert_eq!(
        value.at_path(&path),
        Err(PathError::MissingKey {
            segment: 0,
            key: b"missing",
        })
    );
}

// Inputs: a list path whose requested index equals the list length.
// Outputs: a typed one-over-boundary error with index and observed length.
// Logic: pin the exclusive list-index boundary and avoid indexing panics.
#[test]
fn reports_list_index_boundaries() {
    let value = parse(b"li1ee").unwrap();
    let path = [PathSegment::Index(1)];

    assert_eq!(
        value.at_path(&path),
        Err(PathError::IndexOutOfBounds {
            segment: 0,
            index: 1,
            len: 1,
        })
    );
}

// Inputs: key and index segments applied to incompatible container kinds.
// Outputs: errors identifying the exact failing segment and expected container.
// Logic: distinguish structural type errors from missing keys and bad indices.
#[test]
fn reports_path_container_mismatches() {
    let value = parse(b"li1ee").unwrap();
    let key_path = [PathSegment::Key(b"key")];
    assert_eq!(
        value.at_path(&key_path),
        Err(PathError::TypeMismatch {
            segment: 0,
            expected: ValueKind::Dictionary,
            actual: ValueKind::List,
        })
    );

    let value = Value::Integer(1);
    let index_path = [PathSegment::Index(0)];
    assert_eq!(
        value.at_path(&index_path),
        Err(PathError::TypeMismatch {
            segment: 0,
            expected: ValueKind::List,
            actual: ValueKind::Integer,
        })
    );
}
