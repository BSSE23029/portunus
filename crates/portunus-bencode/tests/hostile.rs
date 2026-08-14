use portunus_bencode::{parse, Error, LimitKind, Limits, Value};

// Inputs: oversized decimal declarations, deep containers, and truncated tokens.
// Outputs: deterministic typed rejection without allocation based on declarations.
// Logic: retain a repository-owned hostile corpus for recurring parser regression.
#[test]
fn rejects_hostile_declarations_nesting_and_truncation() {
    let cases = [
        (
            &b"999999999999999999999999999999999999:"[..],
            Error::InvalidLength(0),
        ),
        (
            &b"i999999999999999999999999999999999999e"[..],
            Error::InvalidInteger(0),
        ),
        (&b"4:abc"[..], Error::UnexpectedEof),
        (&b"d1:ai1e"[..], Error::UnexpectedEof),
    ];
    for (input, expected) in cases {
        assert_eq!(parse(input), Err(expected));
    }

    let limits = Limits::new(16, 2, 4, 4);
    assert_eq!(
        portunus_bencode::parse_with_limits(b"llli1eeee", limits),
        Err(Error::LimitExceeded {
            kind: LimitKind::Depth,
            offset: 2,
            limit: 2,
        })
    );
}

// Inputs: a dictionary containing the same raw key twice with distinct values.
// Outputs: the currently explicit last-value-wins compatibility behavior.
// Logic: pin duplicate handling until a future strict decoder mode is introduced.
#[test]
fn isolates_duplicate_dictionary_keys_deterministically() {
    let Value::Dictionary(values) = parse(b"d1:ai1e1:ai2ee").unwrap() else {
        panic!("fixture root must be a dictionary");
    };
    assert_eq!(values.len(), 1);
    assert_eq!(values[b"a".as_slice()], Value::Integer(2));
}

// Inputs: a string declaration one byte above its configured payload ceiling.
// Outputs: rejection at the declaration offset before payload availability matters.
// Logic: prove declared size cannot induce proportional buffering or allocation.
#[test]
fn rejects_large_declared_payload_before_reading_it() {
    let limits = Limits::new(32, 1, 1, 8);
    assert_eq!(
        portunus_bencode::parse_with_limits(b"9:", limits),
        Err(Error::LimitExceeded {
            kind: LimitKind::ByteStringLength,
            offset: 0,
            limit: 8,
        })
    );
}
