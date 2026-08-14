use bytes::{Bytes, BytesMut};
use portunus_transport::{Handshake, Message, PeerCodec, HANDSHAKE_LEN};
use tokio_util::codec::{Decoder, Encoder};

// Inputs: a handshake with recognizable field bytes.
// Outputs: exact 68-byte encoding and equal decoded structure.
// Logic: verify fixed offsets and validation through a public round trip.
#[test]
fn handshake_round_trip() {
    let handshake = Handshake {
        reserved: [0; 8],
        info_hash: [1; 20],
        peer_id: [2; 20],
    };
    let encoded = handshake.encode();
    assert_eq!(encoded.len(), HANDSHAKE_LEN);
    assert_eq!(Handshake::decode(&encoded).unwrap(), handshake);
}

// Inputs: incorrect size, protocol length, and protocol name handshakes.
// Outputs: invalid-data errors for every malformed case.
// Logic: ensure identity fields are never read before framing is authenticated.
#[test]
fn handshake_rejects_malformed_input() {
    assert!(Handshake::decode(&[0; 67]).is_err());
    let mut encoded = Handshake {
        reserved: [0; 8],
        info_hash: [1; 20],
        peer_id: [2; 20],
    }
    .encode();
    encoded[0] = 18;
    assert!(Handshake::decode(&encoded).is_err());
    encoded[0] = 19;
    encoded[1] = b'X';
    assert!(Handshake::decode(&encoded).is_err());
}

// Inputs: representative fixed, variable, and keepalive messages.
// Outputs: equality after encoding and decoding every message.
// Logic: use one helper loop to cover framing and all payload shapes.
#[test]
fn codec_round_trips_messages() {
    let messages = [
        Message::KeepAlive,
        Message::Have(9),
        Message::Bitfield(Bytes::from_static(&[0x80])),
        Message::Request {
            index: 2,
            begin: 16,
            length: 32,
        },
        Message::Piece {
            index: 2,
            begin: 16,
            block: Bytes::from_static(b"data"),
        },
    ];
    let mut codec = PeerCodec::new(1024);
    for message in messages {
        let mut buffer = BytesMut::new();
        codec.encode(message.clone(), &mut buffer).unwrap();
        assert_eq!(codec.decode(&mut buffer).unwrap(), Some(message));
    }
}

// Inputs: partial header, partial body, and an oversized declared frame.
// Outputs: wait states for partial data and an error for budget violation.
// Logic: prove stream fragmentation is normal while allocation abuse is rejected.
#[test]
fn codec_handles_stream_boundaries() {
    let mut codec = PeerCodec::new(4);
    assert_eq!(
        codec.decode(&mut BytesMut::from(&[0, 0][..])).unwrap(),
        None
    );
    assert_eq!(
        codec
            .decode(&mut BytesMut::from(&[0, 0, 0, 1][..]))
            .unwrap(),
        None
    );
    assert!(codec
        .decode(&mut BytesMut::from(&[0, 0, 0, 5][..]))
        .is_err());
}
