use portunus_bencode::{parse_spanned, Error};

// Inputs: truncated scalar and container tokens at recursive parser boundaries.
// Outputs: stable unexpected-end errors without panic or partial public trees.
// Logic: exercise private grammar mechanics through the public spanned entry point.
#[test]
fn rejects_truncated_tokens_at_recursive_boundaries() {
    assert_eq!(parse_spanned(b"i12"), Err(Error::UnexpectedEof));
    assert_eq!(parse_spanned(b"3:ab"), Err(Error::UnexpectedEof));
    assert_eq!(parse_spanned(b"li1e"), Err(Error::UnexpectedEof));
    assert_eq!(parse_spanned(b"d1:a"), Err(Error::UnexpectedEof));
}
