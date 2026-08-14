//! Integration coverage for stable structured control-plane error details.

use portunus_daemon::errors::engine_status;
use portunus_engine::torrent::Error;
use portunus_proto::ErrorDetail;
use prost::Message;
use tonic::Code;

// Inputs: validation, lookup, and overload-adjacent engine failures.
// Outputs: stable gRPC codes plus decodable versioned reason/resource details.
// Logic: preserve machine-readable failure policy independently from display text.
#[test]
fn maps_engine_failures_to_structured_statuses() {
    let cases = [
        (
            Error::EmptySource,
            Code::InvalidArgument,
            "INVALID_SOURCE",
            false,
            "transfer.source",
        ),
        (
            Error::UnknownTransfer("transfer-9".into()),
            Code::NotFound,
            "TRANSFER_NOT_FOUND",
            false,
            "transfer",
        ),
        (
            Error::Closed,
            Code::Unavailable,
            "ENGINE_UNAVAILABLE",
            true,
            "engine.command_queue",
        ),
    ];
    for (error, code, reason, retryable, resource) in cases {
        let status = engine_status(error);
        assert_eq!(status.code(), code);
        let detail = ErrorDetail::decode(status.details()).unwrap();
        assert_eq!(detail.reason, reason);
        assert_eq!(detail.retryable, retryable);
        assert_eq!(detail.resource, resource);
    }
}
