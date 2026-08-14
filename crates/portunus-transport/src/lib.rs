//! BitTorrent peer-wire handshake and framed message codec.
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
    pub fn encode(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0; HANDSHAKE_LEN];
        out[0] = 19;
        out[1..20].copy_from_slice(PROTOCOL);
        out[20..28].copy_from_slice(&self.reserved);
        out[28..48].copy_from_slice(&self.info_hash);
        out[48..68].copy_from_slice(&self.peer_id);
        out
    }
    pub fn decode(input: &[u8]) -> io::Result<Self> {
        if input.len() != HANDSHAKE_LEN || input[0] != 19 || &input[1..20] != PROTOCOL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid BitTorrent handshake",
            ));
        }
        Ok(Self {
            reserved: input[20..28].try_into().unwrap(),
            info_hash: input[28..48].try_into().unwrap(),
            peer_id: input[48..68].try_into().unwrap(),
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
    pub fn new(max_frame: usize) -> Self {
        Self { max_frame }
    }
}

impl Decoder for PeerCodec {
    type Item = Message;
    type Error = io::Error;
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
        dst.put_u32((1 + payload) as u32);
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn codec_roundtrip_piece() {
        let msg = Message::Piece {
            index: 2,
            begin: 16,
            block: Bytes::from_static(b"data"),
        };
        let mut buf = BytesMut::new();
        let mut c = PeerCodec::new(1024);
        c.encode(msg.clone(), &mut buf).unwrap();
        assert_eq!(c.decode(&mut buf).unwrap(), Some(msg));
    }
    #[test]
    fn handshake_roundtrip() {
        let h = Handshake {
            reserved: [0; 8],
            info_hash: [1; 20],
            peer_id: [2; 20],
        };
        assert_eq!(Handshake::decode(&h.encode()).unwrap(), h);
    }
}
