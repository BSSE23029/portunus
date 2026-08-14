use portunus_bencode::from_slice;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct Collections<'a> {
    values: Vec<i64>,
    #[serde(borrow)]
    labels: BTreeMap<&'a [u8], i64>,
}

// Inputs: a list and dictionary with raw non-UTF-8 keys.
// Outputs: standard typed containers preserving list and raw-byte key semantics.
// Logic: exercise sequence/map adapters without imposing UTF-8 on generic map keys.
#[test]
fn deserializes_sequences_and_binary_keyed_maps() {
    let decoded: Collections<'_> =
        from_slice(b"d6:labelsd1:ai1e1:\xffi2ee6:valuesli3ei4eee").unwrap();

    assert_eq!(decoded.values, vec![3, 4]);
    assert_eq!(decoded.labels[b"a".as_slice()], 1);
    assert_eq!(decoded.labels[b"\xff".as_slice()], 2);
}
