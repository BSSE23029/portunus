use portunus_storage::layout::{FileSpec, Layout, LayoutError, RangeSegment};

// Inputs: logical range crossing two nonempty files around an empty file.
// Outputs: ordered physical segments with exact file and request offsets.
// Logic: prove multi-file mapping neither emits nor miscounts zero-length entries.
#[test]
fn maps_ranges_across_multiple_files() {
    let layout = Layout::new(
        vec![
            FileSpec::new("first", 3),
            FileSpec::new("empty", 0),
            FileSpec::new("second", 4),
        ],
        3,
    )
    .unwrap();
    assert_eq!(layout.total_length(), 7);
    assert_eq!(
        layout.map(2, 4).unwrap(),
        vec![
            RangeSegment {
                file_index: 0,
                file_offset: 2,
                request_offset: 0,
                length: 1,
            },
            RangeSegment {
                file_index: 2,
                file_offset: 0,
                request_offset: 1,
                length: 3,
            },
        ]
    );
}

// Inputs: zero file budget, empty manifest, exact count, and one-over count.
// Outputs: stable independent construction failures and exact-bound success.
// Logic: reject hostile collection sizes before retaining manifest entries.
#[test]
fn enforces_manifest_boundaries() {
    assert_eq!(
        Layout::new(vec![FileSpec::new("one", 1)], 0).unwrap_err(),
        LayoutError::ZeroFileLimit
    );
    assert_eq!(
        Layout::new(Vec::new(), 1).unwrap_err(),
        LayoutError::EmptyManifest
    );
    assert_eq!(
        Layout::new(vec![FileSpec::new("", 1)], 1).unwrap_err(),
        LayoutError::EmptyFileKey { file_index: 0 }
    );
    assert_eq!(
        Layout::new(vec![FileSpec::new("same", 1), FileSpec::new("same", 1)], 2).unwrap_err(),
        LayoutError::DuplicateFileKey {
            first_index: 0,
            duplicate_index: 1,
        }
    );
    assert!(Layout::new(vec![FileSpec::new("one", 1)], 1).is_ok());
    assert_eq!(
        Layout::new(vec![FileSpec::new("one", 1), FileSpec::new("two", 1)], 1).unwrap_err(),
        LayoutError::TooManyFiles {
            actual: 2,
            limit: 1,
        }
    );
}

// Inputs: exact terminal empty range, first byte beyond end, and overflowing range.
// Outputs: empty mapping or precise out-of-range details without arithmetic wrapping.
// Logic: define all logical ranges as half-open byte intervals.
#[test]
fn validates_half_open_range_boundaries() {
    let layout = Layout::new(vec![FileSpec::new("file", 4)], 1).unwrap();
    assert!(layout.map(4, 0).unwrap().is_empty());
    assert_eq!(
        layout.map(4, 1).unwrap_err(),
        LayoutError::RangeOutOfBounds {
            offset: 4,
            length: 1,
            total_length: 4,
        }
    );
    assert!(matches!(
        layout.map(u64::MAX, 2),
        Err(LayoutError::RangeOutOfBounds { .. })
    ));
}
