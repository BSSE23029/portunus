use portunus_bencode::{Error, FeedStatus, IncrementalParser, Limits};

// Inputs: canonical extrema and noncanonical scalar spellings split byte by byte.
// Outputs: accepted signed range boundaries and stable format/range rejections.
// Logic: exercise lexical state retained across the smallest possible chunks.
#[test]
fn retains_numeric_canonicality_state_across_chunks() {
    let mut valid = IncrementalParser::new(Limits::default());
    for byte in b"i-9223372036854775808e" {
        valid.push(std::slice::from_ref(byte)).unwrap();
    }
    assert!(matches!(
        valid.push(b""),
        Ok(FeedStatus::Complete { consumed: 0 })
    ));

    let mut leading_zero = IncrementalParser::new(Limits::default());
    leading_zero.push(b"i0").unwrap();
    assert_eq!(leading_zero.push(b"1"), Err(Error::InvalidInteger(0)));

    let mut overflow = IncrementalParser::new(Limits::default());
    assert_eq!(
        overflow.push(b"i9223372036854775808"),
        Err(Error::InvalidInteger(0))
    );
}
