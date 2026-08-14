use portunus_bencode::{FeedStatus, IncrementalParser, Limits};

#[path = "incremental/scanner.rs"]
mod scanner;
#[path = "incremental/state.rs"]
mod state;

// Inputs: one dictionary split across token boundaries with following stream bytes.
// Outputs: incomplete statuses, then the exact consumed prefix and retained document.
// Logic: prove chunks are scanned once and bytes after the first value stay caller-owned.
#[test]
fn completes_one_document_without_consuming_the_next() {
    let mut parser = IncrementalParser::new(Limits::default());

    assert_eq!(parser.push(b"d1:a").unwrap(), FeedStatus::Incomplete);
    assert_eq!(
        parser.push(b"i1eeNEXT").unwrap(),
        FeedStatus::Complete { consumed: 4 }
    );
    assert_eq!(parser.document(), Some(b"d1:ai1ee".as_slice()));
    assert_eq!(parser.buffered_len(), 8);
}

// Inputs: a valid scalar exactly at the input budget and one required byte beyond it.
// Outputs: exact-limit completion and a stable one-over input-limit rejection.
// Logic: bound retained buffering before copying the first excessive byte.
#[test]
fn bounds_retained_input_at_the_exact_boundary() {
    let limits = Limits::new(3, 1, 1, 1);
    let mut exact = IncrementalParser::new(limits);
    assert_eq!(
        exact.push(b"i1e").unwrap(),
        FeedStatus::Complete { consumed: 3 }
    );

    let mut one_over = IncrementalParser::new(Limits::new(2, 1, 1, 1));
    assert_eq!(one_over.push(b"i1").unwrap(), FeedStatus::Incomplete);
    let error = one_over.push(b"e").unwrap_err();
    assert_eq!(error.to_string(), "InputLength limit 2 exceeded at byte 2");
    assert_eq!(one_over.buffered_len(), 2);
}

// Inputs: an unfinished integer and an explicitly signaled end of stream.
// Outputs: incomplete during feeding and `UnexpectedEof` only when input is final.
// Logic: distinguish temporary chunk boundaries from terminal truncation.
#[test]
fn distinguishes_incomplete_chunks_from_final_truncation() {
    let mut parser = IncrementalParser::new(Limits::default());
    assert_eq!(parser.push(b"i42").unwrap(), FeedStatus::Incomplete);
    assert_eq!(parser.finish(), Err(portunus_bencode::Error::UnexpectedEof));
}

// Inputs: one completed document followed by parser reuse for a second document.
// Outputs: cleared state and independent absolute offsets for the next document.
// Logic: make lifecycle and buffer ownership explicit without reallocating policy.
#[test]
fn resets_after_consuming_a_document() {
    let mut parser = IncrementalParser::new(Limits::default());
    parser.push(b"1:a").unwrap();
    parser.reset();

    assert_eq!(parser.document(), None);
    assert_eq!(parser.buffered_len(), 0);
    assert_eq!(
        parser.push(b"i2e").unwrap(),
        FeedStatus::Complete { consumed: 3 }
    );
    assert_eq!(parser.document(), Some(b"i2e".as_slice()));
}

// Inputs: reference metadata delivered one byte per chunk.
// Outputs: incomplete until the final byte, then the unchanged complete document.
// Logic: stress every possible lexical boundary without timing or public network I/O.
#[test]
fn parses_reference_metadata_one_byte_at_a_time() {
    let fixture = include_bytes!("fixtures/torrent_metadata.bencode");
    let metadata = fixture.strip_suffix(b"\n").unwrap_or(fixture);
    let mut parser = IncrementalParser::new(Limits::default());

    for (index, byte) in metadata.iter().enumerate() {
        let status = parser.push(std::slice::from_ref(byte)).unwrap();
        if index + 1 == metadata.len() {
            assert_eq!(status, FeedStatus::Complete { consumed: 1 });
        } else {
            assert_eq!(status, FeedStatus::Incomplete);
        }
    }
    assert_eq!(parser.document(), Some(metadata));
}
