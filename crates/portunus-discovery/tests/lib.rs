use portunus_discovery::{
    announce_request, connect_request, parse_compact_ipv4, parse_connect_response, Error,
    UDP_PROTOCOL_ID,
};

// Inputs: transaction ID 7.
// Outputs: the exact protocol/action/transaction wire fields.
// Logic: verify fixed offsets independently of a network socket.
#[test]
fn builds_connect_packet() {
    let packet = connect_request(7);
    assert_eq!(&packet[..8], &UDP_PROTOCOL_ID.to_be_bytes());
    assert_eq!(&packet[8..12], &0_u32.to_be_bytes());
    assert_eq!(&packet[12..], &7_u32.to_be_bytes());
}

// Inputs: valid response plus wrong transaction, action, and short variants.
// Outputs: connection ID or each correlation/format error.
// Logic: prove response data is trusted only after envelope validation.
#[test]
fn validates_connect_response() {
    let mut packet = [0_u8; 16];
    packet[4..8].copy_from_slice(&7_u32.to_be_bytes());
    packet[8..].copy_from_slice(&11_u64.to_be_bytes());
    assert_eq!(parse_connect_response(&packet, 7), Ok(11));
    assert_eq!(
        parse_connect_response(&packet, 8),
        Err(Error::TransactionMismatch)
    );
    packet[..4].copy_from_slice(&1_u32.to_be_bytes());
    assert_eq!(parse_connect_response(&packet, 7), Err(Error::Action(1)));
    assert_eq!(
        parse_connect_response(&packet[..8], 7),
        Err(Error::Truncated)
    );
}

// Inputs: two compact endpoints and one malformed partial endpoint.
// Outputs: decoded addresses or invalid-list error.
// Logic: exercise exact six-byte record alignment and network byte order.
#[test]
fn parses_compact_peers() {
    let raw = [127, 0, 0, 1, 0x1a, 0xe1, 10, 0, 0, 1, 0, 80];
    let peers = parse_compact_ipv4(&raw).unwrap();
    assert_eq!(peers[0], "127.0.0.1:6881".parse().unwrap());
    assert_eq!(peers[1], "10.0.0.1:80".parse().unwrap());
    assert_eq!(parse_compact_ipv4(&raw[..11]), Err(Error::InvalidPeerList));
}

// Inputs: recognizable values for every announce field.
// Outputs: a 98-byte request with critical fields at correct offsets.
// Logic: validate the encoder's layout while ignoring its intentionally random key.
#[test]
fn builds_announce_packet() {
    let packet = announce_request(1, 2, [3; 20], [4; 20], 5, 6, 7, 6881);
    assert_eq!(packet.len(), 98);
    assert_eq!(&packet[..8], &1_u64.to_be_bytes());
    assert_eq!(&packet[12..16], &2_u32.to_be_bytes());
    assert_eq!(&packet[16..36], &[3; 20]);
    assert_eq!(&packet[96..], &6881_u16.to_be_bytes());
}
