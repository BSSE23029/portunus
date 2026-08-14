//! Tracker discovery protocol primitives (BEP 15).
//!
//! Discovery is the systems problem of turning a logical content identifier
//! into reachable network endpoints. This crate currently implements the binary
//! packet layer; socket retries, deadlines, and provider abstractions come next.
//!
//! ```text
//! client ──connect(transaction N)──> tracker
//! client <──connection ID, N──────── tracker
//! client ──announce(connection ID)─> tracker
//! client <──compact peer endpoints── tracker
//! ```
use bytes::{BufMut, BytesMut};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use thiserror::Error;

mod api;
mod r#static;

pub use api::{DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot, Endpoint};
pub use r#static::StaticProvider;

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

/// Builds the fixed-size UDP tracker connection request.
///
/// **Inputs:** `transaction_id`, chosen by the client to correlate an unreliable
/// UDP response with this request.
///
/// **Outputs:** Exactly 16 network-order bytes containing protocol ID, connect
/// action, and transaction ID.
///
/// **Logic:** Write each fixed-width integer into its protocol-defined offset in
/// big-endian order. No heap allocation is necessary for a fixed packet.
#[must_use]
pub fn connect_request(transaction_id: u32) -> [u8; 16] {
    let mut out = [0; 16];
    out[..8].copy_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    out[8..12].copy_from_slice(&0u32.to_be_bytes());
    out[12..].copy_from_slice(&transaction_id.to_be_bytes());
    out
}

/// Validates and extracts a UDP tracker connection ID.
///
/// **Inputs:** Raw response `bytes` and the transaction ID originally sent.
///
/// **Outputs:** The tracker's temporary connection ID, or a typed error for a
/// truncated packet, wrong action, or mismatched transaction.
///
/// **Logic:** Validate the minimum size and correlation fields before trusting
/// and decoding the final eight-byte connection identifier.
///
/// # Errors
///
/// Returns an error for truncation, an unexpected action, or failed correlation.
pub fn parse_connect_response(bytes: &[u8], expected_transaction: u32) -> Result<u64, Error> {
    if bytes.len() < 16 {
        return Err(Error::Truncated);
    }
    let action = u32::from_be_bytes(bytes[0..4].try_into().map_err(|_| Error::Truncated)?);
    if action != 0 {
        return Err(Error::Action(action));
    }
    let tx = u32::from_be_bytes(bytes[4..8].try_into().map_err(|_| Error::Truncated)?);
    if tx != expected_transaction {
        return Err(Error::TransactionMismatch);
    }
    Ok(u64::from_be_bytes(
        bytes[8..16].try_into().map_err(|_| Error::Truncated)?,
    ))
}

/// Decodes compact IPv4 peer endpoints.
///
/// **Inputs:** A byte slice containing zero or more six-byte records: four bytes
/// of IPv4 address followed by a two-byte network-order port.
///
/// **Outputs:** Owned socket addresses, or [`Error::InvalidPeerList`] when a
/// partial record remains.
///
/// **Logic:** Validate record alignment, split into exact six-byte chunks, and
/// translate each chunk into Rust's standard `SocketAddr` representation.
///
/// # Errors
///
/// Returns [`Error::InvalidPeerList`] for a trailing partial record.
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
/// Encodes a UDP tracker announce request.
///
/// **Inputs:** Tracker connection/transaction IDs, 20-byte content and client
/// identities, transfer counters, remaining bytes, and the listening port.
///
/// **Outputs:** A 98-byte mutable network buffer ready for UDP transmission.
///
/// **Logic:** Append BEP 15 fields in wire order and big-endian encoding. The
/// request uses the neutral event/IP values, a random client key, and `-1` to
/// request the tracker's default number of endpoints.
#[must_use]
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
