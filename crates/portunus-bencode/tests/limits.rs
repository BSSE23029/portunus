use portunus_bencode::{parse_with_limits, Error, LimitKind, Limits, Value};

// Inputs: encodings at and one byte beyond explicit total-input ceilings.
// Outputs: successful exact-boundary parsing and precise first-rejected-byte errors.
// Logic: cover zero, exact, and one-over boundaries without relying on parser defaults.
#[test]
fn bounds_total_input_length() {
    assert_eq!(
        parse_with_limits(b"i", Limits::default().with_max_input_len(0)),
        Err(Error::LimitExceeded {
            kind: LimitKind::InputLength,
            offset: 0,
            limit: 0,
        })
    );
    assert_eq!(
        parse_with_limits(b"1:a", Limits::default().with_max_input_len(3)),
        Ok(Value::Bytes(b"a"))
    );
    assert_eq!(
        parse_with_limits(b"1:ab", Limits::default().with_max_input_len(3)),
        Err(Error::LimitExceeded {
            kind: LimitKind::InputLength,
            offset: 3,
            limit: 3,
        })
    );
}

// Inputs: scalars and containers at zero, exact, and excessive nesting depths.
// Outputs: accepted scalars/containers or a depth error at the rejected container token.
// Logic: define depth as enclosing containers and verify the inclusive boundary.
#[test]
fn bounds_container_depth() {
    let zero = Limits::default().with_max_depth(0);
    assert_eq!(parse_with_limits(b"i1e", zero), Ok(Value::Integer(1)));
    assert_eq!(
        parse_with_limits(b"le", zero),
        Err(Error::LimitExceeded {
            kind: LimitKind::Depth,
            offset: 0,
            limit: 0,
        })
    );

    let one = Limits::default().with_max_depth(1);
    assert_eq!(
        parse_with_limits(b"li1ee", one),
        Ok(Value::List(vec![Value::Integer(1)]))
    );
    assert_eq!(
        parse_with_limits(b"lli1eee", one),
        Err(Error::LimitExceeded {
            kind: LimitKind::Depth,
            offset: 1,
            limit: 1,
        })
    );
}

// Inputs: empty, exact-boundary, and one-over-boundary lists.
// Outputs: bounded list values or an error at the first disallowed item.
// Logic: ensure item allocation stops before parsing work beyond the budget.
#[test]
fn bounds_list_length() {
    let zero = Limits::default().with_max_collection_len(0);
    assert_eq!(parse_with_limits(b"le", zero), Ok(Value::List(vec![])));
    assert_eq!(
        parse_with_limits(b"li1ee", zero),
        Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset: 1,
            limit: 0,
        })
    );

    let two = Limits::default().with_max_collection_len(2);
    assert!(matches!(parse_with_limits(b"li1ei2ee", two), Ok(Value::List(v)) if v.len() == 2));
    assert_eq!(
        parse_with_limits(b"li1ei2ei3ee", two),
        Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset: 7,
            limit: 2,
        })
    );
}

// Inputs: empty, exact-boundary, and one-over-boundary dictionaries.
// Outputs: bounded maps or an error at the first disallowed key.
// Logic: count key-value pairs as entries rather than counting keys and values separately.
#[test]
fn bounds_dictionary_length() {
    let zero = Limits::default().with_max_collection_len(0);
    assert!(matches!(parse_with_limits(b"de", zero), Ok(Value::Dictionary(v)) if v.is_empty()));
    assert_eq!(
        parse_with_limits(b"d1:ai1ee", zero),
        Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset: 1,
            limit: 0,
        })
    );

    let one = Limits::default().with_max_collection_len(1);
    assert!(
        matches!(parse_with_limits(b"d1:ai1ee", one), Ok(Value::Dictionary(v)) if v.len() == 1)
    );
    assert_eq!(
        parse_with_limits(b"d1:ai1e1:bi2ee", one),
        Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset: 7,
            limit: 1,
        })
    );
}

// Inputs: empty, exact-boundary, and excessive byte strings as values and keys.
// Outputs: borrowed strings or errors anchored at their length prefix.
// Logic: apply one byte-string policy consistently without copying payload bytes.
#[test]
fn bounds_byte_string_length_in_values_and_keys() {
    let zero = Limits::default().with_max_byte_string_len(0);
    assert_eq!(parse_with_limits(b"0:", zero), Ok(Value::Bytes(b"")));
    assert_eq!(
        parse_with_limits(b"1:a", zero),
        Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: 0,
            limit: 0,
        })
    );

    let four = Limits::default().with_max_byte_string_len(4);
    assert_eq!(
        parse_with_limits(b"4:spam", four),
        Ok(Value::Bytes(b"spam"))
    );
    assert_eq!(
        parse_with_limits(b"5:spams", four),
        Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: 0,
            limit: 4,
        })
    );
    assert_eq!(
        parse_with_limits(b"d5:spamsi1ee", four),
        Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: 1,
            limit: 4,
        })
    );
}

// Inputs: a policy created with all four ceilings and an input violating only string size.
// Outputs: the string-specific error rather than a different resource classification.
// Logic: verify `Limits::new` preserves the independent positional arguments.
#[test]
fn constructor_keeps_budgets_independent() {
    assert_eq!(
        parse_with_limits(b"2:ab", Limits::new(4, 1, 1, 1)),
        Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: 0,
            limit: 1,
        })
    );
}
