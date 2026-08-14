//! Allocation-bounded state machine for chunk-independent bencode recognition.
//!
//! The scanner retains lexical state plus one small frame per open container.
//! The owning buffer supplies absolute offsets, making errors chunk-independent.
//! Limits precede protected work. This module does not own bytes, build trees,
//! perform I/O, or consume a second document.

use super::state::{Container, IntegerState, LengthState, Mode};
use crate::{Error, LimitKind, Limits};

#[derive(Debug, Clone)]
pub(super) struct Scanner {
    limits: Limits,
    mode: Mode,
    containers: Vec<Container>,
}

impl Scanner {
    // Inputs: independent inclusive parser resource ceilings.
    // Outputs: a scanner positioned before its root token.
    // Logic: allocate no stack frames until a container is actually entered.
    pub(super) const fn new(limits: Limits) -> Self {
        Self {
            limits,
            mode: Mode::Value,
            containers: Vec::new(),
        }
    }

    // Inputs: one byte and its absolute offset in the current document.
    // Outputs: true exactly when this byte completes the root, or a stable error.
    // Logic: advance lexical state once and delegate completed tokens to parents.
    pub(super) fn feed(&mut self, byte: u8, offset: usize) -> Result<bool, Error> {
        match self.mode {
            Mode::Value => self.start_value(byte, offset)?,
            Mode::Integer(state) => self.scan_integer(byte, state)?,
            Mode::Length(state) => self.scan_length(byte, state)?,
            Mode::Payload { remaining } => self.scan_payload(remaining),
            Mode::Complete => return Ok(true),
        }
        Ok(self.is_complete())
    }

    // Inputs: shared scanner state.
    // Outputs: whether one complete root has been recognized.
    // Logic: completion is a terminal lexical mode until reset.
    pub(super) const fn is_complete(&self) -> bool {
        matches!(self.mode, Mode::Complete)
    }

    // Inputs: first byte of a value and its absolute offset.
    // Outputs: updated lexical/container state or a precise token/limit error.
    // Logic: validate the parent slot before beginning work protected by its budget.
    fn start_value(&mut self, byte: u8, offset: usize) -> Result<(), Error> {
        if byte == b'e' {
            return self.close_container(offset);
        }
        self.enforce_parent_slot(offset)?;
        if self.dictionary_expects_key() && !byte.is_ascii_digit() {
            return Err(Error::NonByteKey(offset));
        }
        self.mode = match byte {
            b'i' => Mode::Integer(IntegerState {
                start: offset,
                negative: false,
                digits: 0,
                leading_zero: false,
                magnitude: 0,
            }),
            b'l' => {
                self.enter_container(offset, Container::List { entries: 0 })?;
                Mode::Value
            }
            b'd' => {
                self.enter_container(
                    offset,
                    Container::Dictionary {
                        entries: 0,
                        expects_key: true,
                    },
                )?;
                Mode::Value
            }
            digit if digit.is_ascii_digit() => Mode::Length(LengthState {
                start: offset,
                leading_zero: digit == b'0',
                length: usize::from(digit - b'0'),
            }),
            _ => return Err(Error::InvalidToken(offset)),
        };
        Ok(())
    }

    // Inputs: next integer byte and accumulated canonical/range state.
    // Outputs: advanced integer state, completed scalar, or integer-format error.
    // Logic: validate sign/leading zero and accumulate within the signed i64 domain.
    fn scan_integer(&mut self, byte: u8, mut state: IntegerState) -> Result<(), Error> {
        if byte == b'e' {
            if state.digits == 0 || (state.negative && state.magnitude == 0) {
                return Err(Error::InvalidInteger(state.start));
            }
            self.complete_token();
            return Ok(());
        }
        if byte == b'-' && state.digits == 0 && !state.negative {
            state.negative = true;
            self.mode = Mode::Integer(state);
            return Ok(());
        }
        if !byte.is_ascii_digit() || state.leading_zero {
            return Err(Error::InvalidInteger(state.start));
        }
        state.leading_zero = state.digits == 0 && byte == b'0';
        state.digits += 1;
        state.magnitude = state
            .magnitude
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(byte - b'0')))
            .ok_or(Error::InvalidInteger(state.start))?;
        let maximum = if state.negative {
            i64::MAX as u64 + 1
        } else {
            i64::MAX as u64
        };
        if state.magnitude > maximum {
            return Err(Error::InvalidInteger(state.start));
        }
        self.mode = Mode::Integer(state);
        Ok(())
    }

    // Inputs: next length byte and accumulated decimal length state.
    // Outputs: advanced length, payload state, immediate empty value, or error.
    // Logic: enforce canonical decimal and string budget before accepting payload.
    fn scan_length(&mut self, byte: u8, mut state: LengthState) -> Result<(), Error> {
        if byte == b':' {
            if state.length > self.limits.byte_string_len {
                return Err(Error::LimitExceeded {
                    kind: LimitKind::ByteStringLength,
                    offset: state.start,
                    limit: self.limits.byte_string_len,
                });
            }
            if state.length == 0 {
                self.complete_token();
                return Ok(());
            }
            self.mode = Mode::Payload {
                remaining: state.length,
            };
            return Ok(());
        }
        if !byte.is_ascii_digit() || state.leading_zero {
            return Err(Error::InvalidLength(state.start));
        }
        state.length = state
            .length
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(byte - b'0')))
            .ok_or(Error::InvalidLength(state.start))?;
        self.mode = Mode::Length(state);
        Ok(())
    }

    // Inputs: payload bytes remaining before consuming the current byte.
    // Outputs: decremented payload state or a completed byte-string token.
    // Logic: payload contents are opaque; only their declared byte count matters.
    fn scan_payload(&mut self, remaining: usize) {
        if remaining == 1 {
            self.complete_token();
        } else {
            self.mode = Mode::Payload {
                remaining: remaining - 1,
            };
        }
    }

    // Inputs: absolute offset of a container terminator in value-start state.
    // Outputs: parent/root completion or an error for an illegal terminator.
    // Logic: dictionaries close only between entries; a closed container is one value.
    fn close_container(&mut self, offset: usize) -> Result<(), Error> {
        let Some(container) = self.containers.last() else {
            return Err(Error::InvalidToken(offset));
        };
        if matches!(
            container,
            Container::Dictionary {
                expects_key: false,
                ..
            }
        ) {
            return Err(Error::InvalidToken(offset));
        }
        self.containers.pop();
        self.complete_token();
        Ok(())
    }

    // Inputs: absolute offset and a new empty container frame.
    // Outputs: unit after bounded stack growth, or a depth-limit error.
    // Logic: enclosing-frame count equals parser depth before container entry.
    fn enter_container(&mut self, offset: usize, container: Container) -> Result<(), Error> {
        if self.containers.len() >= self.limits.depth {
            return Err(Error::LimitExceeded {
                kind: LimitKind::Depth,
                offset,
                limit: self.limits.depth,
            });
        }
        self.containers.push(container);
        Ok(())
    }

    // Inputs: absolute offset of a candidate child token.
    // Outputs: unit or a collection-limit error before excess work.
    // Logic: count list values and dictionary entries at their token starts.
    fn enforce_parent_slot(&self, offset: usize) -> Result<(), Error> {
        let entries = match self.containers.last() {
            Some(
                Container::List { entries }
                | Container::Dictionary {
                    entries,
                    expects_key: true,
                },
            ) => Some(*entries),
            _ => None,
        };
        if entries.is_some_and(|count| count >= self.limits.collection_len) {
            return Err(Error::LimitExceeded {
                kind: LimitKind::CollectionLength,
                offset,
                limit: self.limits.collection_len,
            });
        }
        Ok(())
    }

    // Inputs: shared scanner at a value-start boundary.
    // Outputs: whether the innermost dictionary requires a byte-string key.
    // Logic: only dictionary key positions restrict the next token kind.
    fn dictionary_expects_key(&self) -> bool {
        matches!(
            self.containers.last(),
            Some(Container::Dictionary {
                expects_key: true,
                ..
            })
        )
    }

    // Inputs: state immediately after one scalar or container closes.
    // Outputs: updated parent slot state or terminal root completion.
    // Logic: count list values, alternate dictionary slots, or close the root.
    fn complete_token(&mut self) {
        match self.containers.last_mut() {
            Some(Container::List { entries }) => {
                *entries += 1;
                self.mode = Mode::Value;
            }
            Some(Container::Dictionary {
                entries,
                expects_key,
            }) => {
                if *expects_key {
                    *expects_key = false;
                } else {
                    *entries += 1;
                    *expects_key = true;
                }
                self.mode = Mode::Value;
            }
            None => self.mode = Mode::Complete,
        }
    }
}
