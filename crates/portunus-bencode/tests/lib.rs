use portunus_bencode::{parse, parse_with_limits, Error, LimitKind, Limits, Value};

// Inputs: a valid nested dictionary with integer and borrowed bytes.
// Outputs: its exact value tree or a test failure.
// Logic: exercise recursive parsing exclusively through the public API.
#[test]
fn parses_nested_zero_copy_value() {
    let Value::Dictionary(dict) = parse(b"d4:listli42e4:spamee").unwrap() else {
        panic!()
    };
    assert_eq!(
        dict[b"list".as_slice()],
        Value::List(vec![Value::Integer(42), Value::Bytes(b"spam")])
    );
}

// Inputs: canonical scalar and empty-container encodings.
// Outputs: their corresponding public value variants.
// Logic: cover each grammar leaf and both empty recursive containers.
#[test]
fn parses_value_forms() {
    assert_eq!(parse(b"0:"), Ok(Value::Bytes(b"")));
    assert_eq!(parse(b"i-42e"), Ok(Value::Integer(-42)));
    assert_eq!(parse(b"le"), Ok(Value::List(vec![])));
    assert!(matches!(parse(b"de"), Ok(Value::Dictionary(map)) if map.is_empty()));
}

// Inputs: noncanonical, truncated, unknown, and trailing encodings.
// Outputs: stable typed errors with useful offsets.
// Logic: ensure hostile input cannot be silently accepted or partially parsed.
#[test]
fn rejects_malformed_input() {
    assert_eq!(parse(b"i03e"), Err(Error::InvalidInteger(0)));
    assert_eq!(parse(b"i-0e"), Err(Error::InvalidInteger(0)));
    assert_eq!(parse(b"03:abc"), Err(Error::InvalidLength(0)));
    assert_eq!(parse(b"4:abc"), Err(Error::UnexpectedEof));
    assert_eq!(parse(b"x"), Err(Error::InvalidToken(0)));
    assert_eq!(parse(b"i1ejunk"), Err(Error::TrailingData(3)));
}

// Inputs: valid encodings that exceed each independently configured resource limit.
// Outputs: a typed limit error containing the resource, configured bound, and byte offset.
// Logic: prove callers can impose workload-specific budgets before accepting hostile input.
#[test]
fn enforces_configurable_resource_limits() {
    let cases = [
        (
            b"4:spam".as_slice(),
            Limits::default().with_max_input_len(5),
            Error::LimitExceeded {
                kind: LimitKind::InputLength,
                offset: 5,
                limit: 5,
            },
        ),
        (
            b"lli1eee".as_slice(),
            Limits::default().with_max_depth(1),
            Error::LimitExceeded {
                kind: LimitKind::Depth,
                offset: 1,
                limit: 1,
            },
        ),
        (
            b"li1ei2ee".as_slice(),
            Limits::default().with_max_collection_len(1),
            Error::LimitExceeded {
                kind: LimitKind::CollectionLength,
                offset: 4,
                limit: 1,
            },
        ),
        (
            b"4:spam".as_slice(),
            Limits::default().with_max_byte_string_len(3),
            Error::LimitExceeded {
                kind: LimitKind::ByteStringLength,
                offset: 0,
                limit: 3,
            },
        ),
    ];

    for (input, limits, expected) in cases {
        assert_eq!(parse_with_limits(input, limits), Err(expected));
    }
}

// Inputs: containers whose element count is exactly the configured collection limit.
// Outputs: successfully parsed lists and dictionaries.
// Logic: ensure limits are inclusive and dictionary entries count as one item each.
#[test]
fn accepts_values_at_configured_boundaries() {
    let limits = Limits::new(9, 1, 2, 1);

    assert_eq!(
        parse_with_limits(b"l1:a1:be", limits),
        Ok(Value::List(vec![Value::Bytes(b"a"), Value::Bytes(b"b")]))
    );
    assert!(matches!(
        parse_with_limits(b"d1:ai1ee", limits),
        Ok(Value::Dictionary(values)) if values.len() == 1
    ));
}
