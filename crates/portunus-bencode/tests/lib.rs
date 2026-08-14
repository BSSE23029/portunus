use portunus_bencode::{parse, Error, Value};

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
