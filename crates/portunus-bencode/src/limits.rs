//! Resource policy for hostile bencode inputs.
//!
//! [`Limits`] separates four independent inclusive budgets: bytes accepted from
//! the caller, nested containers entered by the parser, entries retained in any
//! one list or dictionary, and bytes exposed by any one string. Keeping those
//! dimensions separate lets a control-plane message permit a deep but tiny tree
//! while a metadata workload permits large byte strings without permitting
//! unbounded collections.
//!
//! The parser checks each budget before performing the work it protects:
//!
//! ```text
//! input length ──> enter container ──> accept next entry ──> borrow string
//!      │                  │                    │                   │
//! total bytes        stack depth       vector/map growth     exposed bytes
//! ```
//!
//! This module defines policy only. It does not parse bytes, allocate collection
//! storage, perform I/O, or choose application-specific limits. Defaults provide
//! a bounded convenience policy; production callers should select budgets from
//! their own admission-control and memory accounting rules.

/// Resource ceilings applied while decoding untrusted input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub(crate) input_len: usize,
    pub(crate) depth: usize,
    pub(crate) collection_len: usize,
    pub(crate) byte_string_len: usize,
}

impl Limits {
    /// Creates a complete parser resource policy.
    ///
    /// **Inputs:** Inclusive ceilings for total input bytes, nested containers,
    /// items per collection, and bytes per string.
    ///
    /// **Outputs:** An immutable, copyable limits value with the supplied bounds.
    ///
    /// **Logic:** Store every independent budget so parsing never relies on an
    /// implicit unbounded resource.
    #[must_use]
    pub const fn new(
        max_input_len: usize,
        max_depth: usize,
        max_collection_len: usize,
        max_byte_string_len: usize,
    ) -> Self {
        Self {
            input_len: max_input_len,
            depth: max_depth,
            collection_len: max_collection_len,
            byte_string_len: max_byte_string_len,
        }
    }

    /// Replaces the total input-byte ceiling.
    ///
    /// **Inputs:** This policy and the new inclusive byte ceiling.
    ///
    /// **Outputs:** The updated policy; no external state changes.
    ///
    /// **Logic:** Use a consuming builder so policies remain easy to compose.
    #[must_use]
    pub const fn with_max_input_len(mut self, limit: usize) -> Self {
        self.input_len = limit;
        self
    }

    /// Replaces the nested-container ceiling.
    ///
    /// **Inputs:** This policy and the maximum number of enclosing containers.
    ///
    /// **Outputs:** The updated policy; no external state changes.
    ///
    /// **Logic:** A zero ceiling permits scalar values but rejects containers.
    #[must_use]
    pub const fn with_max_depth(mut self, limit: usize) -> Self {
        self.depth = limit;
        self
    }

    /// Replaces the per-collection item ceiling.
    ///
    /// **Inputs:** This policy and the maximum list items or dictionary entries.
    ///
    /// **Outputs:** The updated policy; no external state changes.
    ///
    /// **Logic:** Lists count values and dictionaries count key-value entries.
    #[must_use]
    pub const fn with_max_collection_len(mut self, limit: usize) -> Self {
        self.collection_len = limit;
        self
    }

    /// Replaces the byte-string length ceiling.
    ///
    /// **Inputs:** This policy and the inclusive maximum decoded string length.
    ///
    /// **Outputs:** The updated policy; no external state changes.
    ///
    /// **Logic:** The limit applies equally to values and dictionary keys.
    #[must_use]
    pub const fn with_max_byte_string_len(mut self, limit: usize) -> Self {
        self.byte_string_len = limit;
        self
    }
}

impl Default for Limits {
    /// Returns conservative defaults suitable for ordinary backend messages.
    ///
    /// **Inputs:** No parameters or environmental state.
    ///
    /// **Outputs:** A bounded policy allowing 16 MiB inputs, 128 nested
    /// containers, one million collection entries, and 8 MiB strings.
    ///
    /// **Logic:** Keep the convenience parser safe by default while allowing
    /// applications to select tighter or larger explicit budgets.
    fn default() -> Self {
        Self::new(16 * 1024 * 1024, 128, 1_000_000, 8 * 1024 * 1024)
    }
}

/// The resource whose configured parser ceiling was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitKind {
    InputLength,
    Depth,
    CollectionLength,
    ByteStringLength,
}
