//! Compact lexical and container state retained between incremental feed calls.
//!
//! State records only counters and canonicality facts needed to resume at the
//! next byte. It contains no borrowed input, decoded payload, application schema,
//! I/O handle, or unbounded collection. The scanner owns all transitions and
//! enforces limits before growing its container stack.

#[derive(Debug, Clone, Copy)]
pub(super) enum Mode {
    Value,
    Integer(IntegerState),
    Length(LengthState),
    Payload { remaining: usize },
    Complete,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct IntegerState {
    pub(super) start: usize,
    pub(super) negative: bool,
    pub(super) digits: usize,
    pub(super) leading_zero: bool,
    pub(super) magnitude: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct LengthState {
    pub(super) start: usize,
    pub(super) leading_zero: bool,
    pub(super) length: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Container {
    List { entries: usize },
    Dictionary { entries: usize, expects_key: bool },
}
