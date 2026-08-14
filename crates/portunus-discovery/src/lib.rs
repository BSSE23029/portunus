//! Tracker discovery protocol primitives (BEP 15).
use bytes::{BufMut, BytesMut};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use thiserror::Error;

pub const UDP_PROTOCOL_ID: u64 = 0x0417_2710_1980;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("tracker response is truncated")]
    Truncated,
    #[error("unexpected tracker action {0}")]
    Action(u32),
    #[error("transaction id mismatch")]
    TransactionMismatch,
    #[error("compact peer list has invalid length")]
    InvalidPeerList,
}

pub fn connect_request(transaction_id: u32) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    out[8..12].copy_from_slice(&0u32.to_be_bytes());
    out[12..].copy_from_slice(&transaction_id.to_be_bytes());
    out
}

pub fn parse_connect_response(bytes: &[u8], expected_transaction: u32) -> Result<u64, Error> {
    if bytes.len() < 16 {
        return Err(Error::Truncated);
    }
    let action = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    if action != 0 {
        return Err(Error::Action(action));
    }
    let tx = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if tx != expected_transaction {
        return Err(Error::TransactionMismatch);
    }
    Ok(u64::from_be_bytes(bytes[8..16].try_into().unwrap()))
}

pub fn parse_compact_ipv4(bytes: &[u8]) -> Result<Vec<SocketAddr>, Error> {
    if !bytes.len().is_multiple_of(6) {
        return Err(Error::InvalidPeerList);
    }
    Ok(bytes
        .chunks_exact(6)
        .map(|p| {
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(p[0], p[1], p[2], p[3])),
                u16::from_be_bytes([p[4], p[5]]),
            )
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
pub fn announce_request(
    connection_id: u64,
    transaction_id: u32,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    downloaded: u64,
    left: u64,
    uploaded: u64,
    port: u16,
) -> BytesMut {
    let mut out = BytesMut::with_capacity(98);
    out.put_u64(connection_id);
    out.put_u32(1);
    out.put_u32(transaction_id);
    out.extend_from_slice(&info_hash);
    out.extend_from_slice(&peer_id);
    out.put_u64(downloaded);
    out.put_u64(left);
    out.put_u64(uploaded);
    out.put_u32(0);
    out.put_u32(0);
    out.put_u32(rand::random());
    out.put_i32(-1);
    out.put_u16(port);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_peers() {
        assert_eq!(
            parse_compact_ipv4(&[127, 0, 0, 1, 0x1a, 0xe1]).unwrap()[0],
            "127.0.0.1:6881".parse().unwrap()
        );
    }
    #[test]
    fn connect_packet() {
        assert_eq!(&connect_request(7)[12..], &7u32.to_be_bytes());
    }
}
