//! Bounded incremental recognition of one self-delimiting bencode document.
//!
//! [`IncrementalParser`] owns only bytes belonging to the current document. Each
//! byte is scanned once as chunks arrive; when the root closes, [`FeedStatus`]
//! reports how much of the latest chunk was consumed, leaving subsequent stream
//! bytes with the caller.
//!
//! ```text
//! push("d1:a")       -> Incomplete       retained: d1:a
//! push("i1eeNEXT")   -> Complete(4)      retained: d1:ai1ee
//!                                  caller keeps: NEXT
//! ```
//!
//! Input, depth, collection, and string limits are checked before protected
//! buffering or work. This component recognizes boundaries and validates syntax;
//! it does not perform I/O, decode a value tree, manage multiple documents, or
//! retain bytes after the first complete root.

use crate::{Error, LimitKind, Limits};

mod scanner;
mod state;

use scanner::Scanner;

/// Result of feeding one chunk into an incremental parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedStatus {
    Incomplete,
    Complete { consumed: usize },
}

/// A bounded, reusable recognizer and buffer for one bencode document.
#[derive(Debug, Clone)]
pub struct IncrementalParser {
    limits: Limits,
    buffer: Vec<u8>,
    scanner: Scanner,
}

impl IncrementalParser {
    /// Creates an empty incremental parser with explicit resource ceilings.
    ///
    /// **Inputs:** Independent inclusive limits for the next document.
    ///
    /// **Outputs:** An empty parser that has not consumed or allocated payload bytes.
    ///
    /// **Logic:** Store policy and initialize the scanner at the root value boundary.
    #[must_use]
    pub const fn new(limits: Limits) -> Self {
        Self {
            limits,
            buffer: Vec::new(),
            scanner: Scanner::new(limits),
        }
    }

    /// Feeds bytes until the chunk ends or the first root document completes.
    ///
    /// **Inputs:** A mutable parser and a borrowed chunk; the caller retains chunk
    /// ownership and must handle any suffix after `consumed`.
    ///
    /// **Outputs:** Incomplete, or complete with the exact prefix consumed from
    /// this call. Only consumed bytes are retained. Syntax and limit failures
    /// include absolute offsets from the current document start.
    ///
    /// **Logic:** Check the input ceiling before copying each byte, append one byte,
    /// and advance the persistent scanner exactly once. A completed parser consumes
    /// zero bytes until [`Self::reset`] is called.
    ///
    /// # Errors
    ///
    /// Returns [`enum@Error`] on malformed input or a resource-limit violation.
    pub fn push(&mut self, chunk: &[u8]) -> Result<FeedStatus, Error> {
        if self.scanner.is_complete() {
            return Ok(FeedStatus::Complete { consumed: 0 });
        }
        for (index, byte) in chunk.iter().copied().enumerate() {
            let offset = self.buffer.len();
            if offset >= self.limits.input_len {
                return Err(Error::LimitExceeded {
                    kind: LimitKind::InputLength,
                    offset,
                    limit: self.limits.input_len,
                });
            }
            self.buffer.push(byte);
            if self.scanner.feed(byte, offset)? {
                return Ok(FeedStatus::Complete {
                    consumed: index + 1,
                });
            }
        }
        Ok(FeedStatus::Incomplete)
    }

    /// Signals that no more bytes will arrive for the current document.
    ///
    /// **Inputs:** A shared parser at any feed boundary.
    ///
    /// **Outputs:** Unit for a complete root, or `UnexpectedEof` for partial input.
    ///
    /// **Logic:** Chunk exhaustion is temporary; only this explicit finalization
    /// converts the scanner's incomplete state into terminal truncation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnexpectedEof`] unless a root document has completed.
    pub const fn finish(&self) -> Result<(), Error> {
        if self.scanner.is_complete() {
            Ok(())
        } else {
            Err(Error::UnexpectedEof)
        }
    }

    /// Borrows the complete retained document when available.
    ///
    /// **Inputs:** A shared parser borrow.
    ///
    /// **Outputs:** The exact completed encoding, or `None` while incomplete.
    ///
    /// **Logic:** Hide partial bytes from decoding callers while avoiding a copy.
    #[must_use]
    pub fn document(&self) -> Option<&[u8]> {
        self.scanner.is_complete().then_some(self.buffer.as_slice())
    }

    /// Returns the number of document bytes currently retained.
    ///
    /// **Inputs:** A shared parser borrow.
    ///
    /// **Outputs:** Buffered bytes in the inclusive input-budget domain.
    ///
    /// **Logic:** Expose memory accounting without exposing incomplete payload data.
    #[must_use]
    pub const fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Clears document and scanner state while preserving allocated capacity.
    ///
    /// **Inputs:** Exclusive parser access after success or failure.
    ///
    /// **Outputs:** No value; retained length becomes zero and offsets restart at zero.
    ///
    /// **Logic:** Clear bytes and replace scanner state using the unchanged policy.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.scanner = Scanner::new(self.limits);
    }
}
