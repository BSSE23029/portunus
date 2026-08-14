#![no_main]

use libfuzzer_sys::fuzz_target;
use portunus_bencode::{encode, parse, parse_spanned, FeedStatus, IncrementalParser, Limits};

fuzz_target!(|input: &[u8]| {
    let limits = Limits::new(64 * 1024, 64, 4_096, 32 * 1024);
    if let Ok(value) = portunus_bencode::parse_with_limits(input, limits) {
        let canonical = encode(&value);
        assert_eq!(parse(&canonical), Ok(value));
    }
    let _ = portunus_bencode::parse_spanned_with_limits(input, limits);
    let _ = parse_spanned(input);

    let mut incremental = IncrementalParser::new(limits);
    for chunk in input.chunks(13) {
        match incremental.push(chunk) {
            Ok(FeedStatus::Incomplete) => {}
            Ok(FeedStatus::Complete { .. }) | Err(_) => break,
        }
    }
    let _ = incremental.finish();
});
