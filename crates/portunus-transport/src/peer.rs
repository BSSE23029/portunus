//! `BitTorrent` peer-wire protocol adapter for the generic session runtime.
//!
//! TCP may split or combine frames, so decoding consumes only complete messages
//! and leaves incomplete bytes caller-owned. Declared frame bodies are bounded by
//! an inclusive byte limit, but the codec never reserves receive capacity: the
//! generic runtime exclusively owns buffer allocation and accounting policy.
//!
//! ```text
//! ┌──── 4-byte length ────┬─ 1-byte ID ─┬──── payload ────┐
//! │ 00 00 00 0d           │ 06          │ index/begin/len │
//! └───────────────────────┴─────────────┴─────────────────┘
//! ```
//!
//! This module translates peer-wire values at the protocol boundary. It does not
//! own sockets, connection lifecycle, scheduling, torrent discovery, or storage.

use crate::FrameCodec;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

pub const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 68;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
}

impl Handshake {
    /// Inputs: reserved feature bits, content hash, and peer identity in `self`.
    /// Outputs: exactly 68 bytes suitable for a peer TCP connection.
    /// Logic: place the protocol marker and fixed-width fields at wire offsets.
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

    /// Inputs: `input`, expected to contain exactly one 68-byte handshake.
    /// Outputs: a handshake or `InvalidData` for invalid size/protocol marker.
    /// Logic: validate framing first, then copy the three fixed-width fields.
    /// # Errors
    /// Returns `InvalidData` when the size, protocol length, or name is invalid.
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
    /// Inputs: `max_frame`, the inclusive largest accepted length prefix in bytes.
    /// Outputs: a state-free codec configured with that defensive upper bound.
    /// Logic: retain the limit without allocating; the runtime owns buffer growth.
    #[must_use]
    pub const fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }
}

impl FrameCodec for PeerCodec {
    type Inbound = Message;
    type Outbound = Message;

    // Inputs: mutable codec and persistent caller-owned receive buffer.
    // Outputs: one decoded message, incomplete state, or I/O-compatible error.
    // Logic: delegate wire parsing to this adapter's Tokio decoder.
    fn decode_frame(&mut self, source: &mut BytesMut) -> io::Result<Option<Self::Inbound>> {
        Decoder::decode(self, source)
    }

    // Inputs: owned peer message and caller-owned transmit buffer.
    // Outputs: appended wire frame or I/O-compatible encoding error.
    // Logic: delegate wire serialization to this adapter's Tokio encoder.
    fn encode_frame(&mut self, item: Self::Outbound, destination: &mut BytesMut) -> io::Result<()> {
        Encoder::encode(self, item, destination)
    }
}

impl Decoder for PeerCodec {
    type Item = Message;
    type Error = io::Error;

    // Inputs: configured codec and caller-owned partial/multi-frame buffer.
    // Outputs: incomplete state, one message, or a stable invalid-data error.
    // Logic: enforce the prefix budget, consume exactly one complete frame, and
    // leave both incomplete and subsequent bytes untouched for runtime policy.
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
            return Ok(None);
        }
        src.advance(4);
        if len == 0 {
            return Ok(Some(Message::KeepAlive));
        }
        let mut frame = src.split_to(len);
        let id = frame.get_u8();
        let message = match id {
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
        Ok(Some(message))
    }
}

impl Encoder<Message> for PeerCodec {
    type Error = io::Error;

    // Inputs: one structured peer message and caller-owned output buffer.
    // Outputs: appended frame or `InvalidInput` if its length exceeds `u32`.
    // Logic: write length, ID, and variant payload without an intermediate frame.
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> io::Result<()> {
        if item == Message::KeepAlive {
            dst.put_u32(0);
            return Ok(());
        }
        let payload = match &item {
            Message::Bitfield(bytes) => bytes.len(),
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
            Message::Have(index) => {
                dst.put_u8(4);
                dst.put_u32(index);
            }
            Message::Bitfield(bytes) => {
                dst.put_u8(5);
                dst.extend_from_slice(&bytes);
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
