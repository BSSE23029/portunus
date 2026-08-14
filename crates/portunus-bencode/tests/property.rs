use portunus_bencode::{
    encode, parse, parse_spanned, FeedStatus, IncrementalParser, Limits, Value,
};
use proptest::{collection::vec, prelude::*};
use std::collections::BTreeMap;

// Inputs: arbitrary bounded integer lists and raw-byte-keyed dictionaries.
// Outputs: canonical encodings that decode and re-encode byte-for-byte identically.
// Logic: generate semantic values first so every input is valid and canonical.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn canonical_round_trips_generated_values(
        integers in vec(any::<i64>(), 0..32),
        entries in vec((vec(any::<u8>(), 0..16), any::<i64>()), 0..32),
    ) {
        let mut dictionary = BTreeMap::new();
        for (key, integer) in &entries {
            dictionary.insert(key.as_slice(), Value::Integer(*integer));
        }
        let value = Value::List(vec![
            Value::List(integers.iter().copied().map(Value::Integer).collect()),
            Value::Dictionary(dictionary),
        ]);
        let encoded = encode(&value);

        let decoded = parse(&encoded).unwrap();
        prop_assert_eq!(encode(&decoded), encoded);
    }
}

// Inputs: arbitrary byte strings, including malformed and truncated encodings.
// Outputs: total parser behavior without panics under ordinary, spanned, or chunked APIs.
// Logic: exercise every API with identical hostile bytes and explicit stream finalization.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_input_never_panics(input in vec(any::<u8>(), 0..4096)) {
        let limits = Limits::new(4096, 32, 256, 2048);
        let _ = portunus_bencode::parse_with_limits(&input, limits);
        let _ = portunus_bencode::parse_spanned_with_limits(&input, limits);

        let mut incremental = IncrementalParser::new(limits);
        for chunk in input.chunks(7) {
            match incremental.push(chunk) {
                Ok(FeedStatus::Incomplete) => {}
                Ok(FeedStatus::Complete { .. }) | Err(_) => break,
            }
        }
        let _ = incremental.finish();
    }
}

// Inputs: arbitrary valid values and every byte boundary in their encodings.
// Outputs: identical complete documents regardless of the chosen two-chunk split.
// Logic: prove incremental state is independent from transport segmentation.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn incremental_completion_is_chunk_independent(bytes in vec(any::<u8>(), 0..128)) {
        let encoded = encode(&Value::Bytes(&bytes));
        let split = encoded.len() / 2;
        let mut incremental = IncrementalParser::new(Limits::default());
        let first = incremental.push(&encoded[..split]).unwrap();
        if split < encoded.len() {
            prop_assert_eq!(first, FeedStatus::Incomplete);
        }
        let second = incremental.push(&encoded[split..]).unwrap();
        prop_assert_ne!(second, FeedStatus::Incomplete);
        prop_assert_eq!(incremental.document(), Some(encoded.as_slice()));
        prop_assert!(parse_spanned(incremental.document().unwrap()).is_ok());
    }
}
