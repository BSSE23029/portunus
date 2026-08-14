//! `BitTorrent` peer-wire handshake and framed message codec.
//!
//! TCP is an ordered byte stream: a single read may contain half a message or
//! several messages. [`PeerCodec`] preserves incomplete bytes in `BytesMut` and
//! emits a message only when its full length-prefixed frame is available.
//!
//! ```text
//! ┌──── 4-byte length ────┬─ 1-byte ID ─┬──── payload ────┐
//! │ 00 00 00 0d           │ 06          │ index/begin/len │
//! └───────────────────────┴─────────────┴─────────────────┘
//! ```
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

mod correlation;
mod reconnect;
mod runtime;
mod session;
mod timing;

pub use correlation::{CorrelationError, CorrelationId, CorrelationInsertError, CorrelationTable};
pub use reconnect::{ReconnectConfigError, ReconnectPolicy};
pub use runtime::{start_session, FrameCodec, Session, SessionError, SessionReport};
pub use session::{
    LifecycleEvent, SessionConfig, SessionConfigError, SessionMachine, SessionState,
    TransitionError,
};
pub use timing::{ConnectionTimer, TimingAction, TimingConfig, TimingConfigError};

impl FrameCodec for PeerCodec {
    type Inbound = Message;
    type Outbound = Message;

    // Inputs: mutable codec and persistent caller-owned receive buffer.
    // Outputs: one decoded peer message, incomplete state, or I/O-compatible error.
    // Logic: delegate to the BitTorrent adapter's Tokio decoder implementation.
    fn decode_frame(&mut self, source: &mut BytesMut) -> io::Result<Option<Self::Inbound>> {
        Decoder::decode(self, source)
    }

    // Inputs: one owned peer message and mutable caller-owned transmit buffer.
    // Outputs: appended wire frame or I/O-compatible encoding error.
    // Logic: delegate to the BitTorrent adapter's Tokio encoder implementation.
    fn encode_frame(&mut self, item: Self::Outbound, destination: &mut BytesMut) -> io::Result<()> {
        Encoder::encode(self, item, destination)
    }
}

pub const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    /// Serializes a structured handshake into its fixed wire representation.
    ///
    /// **Inputs:** The handshake's reserved feature bits, content info hash, and
    /// peer identity through `self`.
    ///
    /// **Outputs:** Exactly 68 bytes suitable for writing to a TCP connection.
    ///
    /// **Logic:** Place the protocol length/name and each fixed-size field at the
    /// offsets defined by the peer-wire protocol.
    #[must_use]
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0; HANDSHAKE_LEN];
        out[0] = 19;
        out[1..20].copy_from_slice(PROTOCOL);
        out[20..28].copy_from_slice(&self.reserved);
        out[28..48].copy_from_slice(&self.info_hash);
        out[48..68].copy_from_slice(&self.peer_id);
        out
    }
    /// Parses and validates a fixed-size peer handshake.
    ///
    /// **Inputs:** `input`, expected to contain exactly one 68-byte handshake.
    ///
    /// **Outputs:** A structured [`Handshake`], or `InvalidData` when the size,
    /// protocol-name length, or protocol name is incorrect.
    ///
    /// **Logic:** Authenticate the framing constants first, then copy the three
    /// fixed-width identity/feature fields into strongly sized arrays.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` when size, protocol length, or name is invalid.
    pub fn decode(input: &[u8]) -> io::Result<Self> {
        if input.len() != HANDSHAKE_LEN || input[0] != 19 || &input[1..20] != PROTOCOL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BitTorrent handshake",
            ));
        }
        Ok(Self {
            reserved: input[20..28]
                .try_into()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?,
            info_hash: input[28..48]
                .try_into()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?,
            peer_id: input[48..68]
                .try_into()
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Bytes),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Bytes,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}

#[derive(Debug, Default)]
pub struct PeerCodec {
    max_frame: usize,
}
impl PeerCodec {
    /// Creates a codec with a defensive frame-size budget.
    ///
    /// **Inputs:** `max_frame`, the largest accepted length prefix in bytes.
    ///
    /// **Outputs:** A state-free codec configured with that upper bound.
    ///
    /// **Logic:** Store the limit for every subsequent decode. This converts a
    /// remote peer's declared frame length into a locally controlled allocation.
    #[must_use]
    pub const fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }
}

impl Decoder for PeerCodec {
    type Item = Message;
    type Error = io::Error;
    // Inputs:
    // - `self`: supplies the configured maximum frame size.
    // - `src`: a persistent receive buffer that may hold partial/multiple frames.
    // Outputs:
    // - `Ok(None)` for incomplete data, `Ok(Some(Message))` for one complete
    //   frame, or `io::Error` for an oversized/malformed frame.
    // Logic:
    // - Peek at the four-byte length without consuming it, enforce the budget,
    //   wait for the entire frame, then consume exactly one frame and decode its
    //   message ID/payload. Remaining bytes stay buffered for the next call.
    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Message>> {
        if src.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(src[..4].try_into().unwrap()) as usize;
        if len > self.max_frame {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer frame exceeds configured limit",
            ));
        }
        if src.len() < 4 + len {
            src.reserve(4 + len - src.len());
            return Ok(None);
        }
        src.advance(4);
        if len == 0 {
            return Ok(Some(Message::KeepAlive));
        }
        let mut frame = src.split_to(len);
        let id = frame.get_u8();
        let msg = match id {
            0 if frame.is_empty() => Message::Choke,
            1 if frame.is_empty() => Message::Unchoke,
            2 if frame.is_empty() => Message::Interested,
            3 if frame.is_empty() => Message::NotInterested,
            4 if frame.len() == 4 => Message::Have(frame.get_u32()),
            5 => Message::Bitfield(frame.freeze()),
            6 if frame.len() == 12 => Message::Request {
                index: frame.get_u32(),
                begin: frame.get_u32(),
                length: frame.get_u32(),
            },
            7 if frame.len() >= 8 => Message::Piece {
                index: frame.get_u32(),
                begin: frame.get_u32(),
                block: frame.freeze(),
            },
            8 if frame.len() == 12 => Message::Cancel {
                index: frame.get_u32(),
                begin: frame.get_u32(),
                length: frame.get_u32(),
            },
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid peer message",
                ))
            }
        };
        Ok(Some(msg))
    }
}

impl Encoder<Message> for PeerCodec {
    type Error = io::Error;
    // Inputs:
    // - `item`: one structured peer message.
    // - `dst`: caller-owned buffer to append the encoded frame to.
    // Outputs:
    // - `Ok(())` after appending bytes, or an I/O-compatible encoding error.
    // Logic:
    // - Calculate payload size, write a big-endian length and message ID, then
    //   serialize variant-specific integers/bytes without an intermediate frame.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> io::Result<()> {
        if item == Message::KeepAlive {
            dst.put_u32(0);
            return Ok(());
        }
        let payload = match &item {
            Message::Bitfield(b) => b.len(),
            Message::Piece { block, .. } => 8 + block.len(),
            Message::Have(_) => 4,
            Message::Request { .. } | Message::Cancel { .. } => 12,
            _ => 0,
        };
        let frame_len = u32::try_from(1 + payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame is too large"))?;
        dst.put_u32(frame_len);
        match item {
            Message::Choke => dst.put_u8(0),
            Message::Unchoke => dst.put_u8(1),
            Message::Interested => dst.put_u8(2),
            Message::NotInterested => dst.put_u8(3),
            Message::Have(i) => {
                dst.put_u8(4);
                dst.put_u32(i);
            }
            Message::Bitfield(b) => {
                dst.put_u8(5);
                dst.extend_from_slice(&b);
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                dst.put_u8(6);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_u32(length);
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                dst.put_u8(7);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.extend_from_slice(&block);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                dst.put_u8(8);
                dst.put_u32(index);
                dst.put_u32(begin);
                dst.put_u32(length);
            }
            Message::KeepAlive => unreachable!(),
        }
        Ok(())
    }
}
