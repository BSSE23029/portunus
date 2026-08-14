use portunus_bencode::{encode, parse, Value};
use std::collections::BTreeMap;

// Inputs: every public scalar and container variant, including empty values.
// Outputs: the exact minimal canonical byte representation for each value.
// Logic: pin token delimiters, decimal spellings, list order, and empty boundaries.
#[test]
fn encodes_every_value_form_canonically() {
    let cases = [
        (Value::Bytes(b""), b"0:".as_slice()),
        (Value::Bytes(b"spam"), b"4:spam".as_slice()),
        (Value::Integer(0), b"i0e".as_slice()),
        (Value::Integer(-42), b"i-42e".as_slice()),
        (Value::List(vec![]), b"le".as_slice()),
        (
            Value::List(vec![Value::Integer(7), Value::Bytes(b"spam")]),
            b"li7e4:spame".as_slice(),
        ),
        (Value::Dictionary(BTreeMap::new()), b"de".as_slice()),
    ];

    for (value, expected) in cases {
        assert_eq!(encode(&value), expected);
    }
}

// Inputs: minimum/maximum integers and a byte string whose length has two digits.
// Outputs: minimal decimal spellings without signs on positive values or leading zeroes.
// Logic: exercise numeric formatting boundaries used for both integers and lengths.
#[test]
fn emits_minimal_decimal_boundaries() {
    assert_eq!(encode(&Value::Integer(i64::MIN)), b"i-9223372036854775808e");
    assert_eq!(encode(&Value::Integer(i64::MAX)), b"i9223372036854775807e");
    assert_eq!(encode(&Value::Bytes(b"0123456789")), b"10:0123456789");
}

// Inputs: a dictionary inserted in reverse order with non-UTF-8 byte keys.
// Outputs: entries ordered by unsigned bytewise key comparison.
// Logic: prove canonical ordering depends on raw protocol bytes, not insertion or text order.
#[test]
fn orders_dictionary_keys_by_raw_bytes() {
    let mut dictionary = BTreeMap::new();
    dictionary.insert(b"z".as_slice(), Value::Integer(3));
    dictionary.insert(b"\xff".as_slice(), Value::Integer(4));
    dictionary.insert(b"aa".as_slice(), Value::Integer(2));
    dictionary.insert(b"a".as_slice(), Value::Integer(1));

    assert_eq!(
        encode(&Value::Dictionary(dictionary)),
        b"d1:ai1e2:aai2e1:zi3e1:\xffi4ee"
    );
}

// Inputs: the deterministic BitTorrent metadata compatibility fixture.
// Outputs: canonical bytes identical to the fixture and an equivalent second parse tree.
// Logic: verify the generic codec round trip against realistic nested binary metadata.
#[test]
fn round_trips_reference_metadata_exactly() {
    let fixture = include_bytes!("fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);
    let parsed = parse(metadata).unwrap();
    let encoded = encode(&parsed);

    assert_eq!(encoded, metadata);
    assert_eq!(parse(&encoded), Ok(parsed));
}
