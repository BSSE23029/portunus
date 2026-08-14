use portunus_bencode::{Error, FeedStatus, IncrementalParser, LimitKind, Limits};

// Inputs: malformed tokens and collection/depth excess split across feed calls.
// Outputs: absolute offsets and stable error variants independent of chunking.
// Logic: exercise scanner state transitions through the public incremental API.
#[test]
fn preserves_errors_and_limits_across_chunks() {
    let mut malformed = IncrementalParser::new(Limits::default());
    assert_eq!(malformed.push(b"d1:a").unwrap(), FeedStatus::Incomplete);
    assert_eq!(malformed.push(b"x"), Err(Error::InvalidToken(4)));

    let mut collection = IncrementalParser::new(Limits::new(16, 2, 1, 4));
    assert_eq!(collection.push(b"li1e").unwrap(), FeedStatus::Incomplete);
    assert_eq!(
        collection.push(b"i2e"),
        Err(Error::LimitExceeded {
            kind: LimitKind::CollectionLength,
            offset: 4,
            limit: 1,
        })
    );

    let mut depth = IncrementalParser::new(Limits::new(16, 1, 2, 4));
    assert_eq!(depth.push(b"l").unwrap(), FeedStatus::Incomplete);
    assert_eq!(
        depth.push(b"l"),
        Err(Error::LimitExceeded {
            kind: LimitKind::Depth,
            offset: 1,
            limit: 1,
        })
    );
}
