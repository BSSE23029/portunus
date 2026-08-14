//! Stable structured translation from domain failures to gRPC status.
//!
//! Each mapped status carries a versioned protobuf detail with a stable uppercase
//! reason, retryability decision, and bounded resource identifier. Human messages
//! remain diagnostic only and clients never need to parse them.
//!
//! This module does not log, retry, expose source values, choose authentication
//! policy, or translate failures owned by unrelated protocol/storage crates.

use bytes::Bytes;
use portunus_engine::torrent::Error;
use portunus_proto::ErrorDetail;
use prost::Message;
use tonic::{Code, Status};

/// Inputs: owned compatibility-engine failure from a control-plane operation.
///
/// Outputs: gRPC status with stable code and encoded [`ErrorDetail`].
/// Logic: classify each closed error enum variant without embedding caller data in
/// structured fields; the source display remains only the bounded human message.
#[must_use]
pub fn engine_status(error: Error) -> Status {
    let (code, reason, retryable, resource, message) = match error {
        Error::EmptySource => (
            Code::InvalidArgument,
            "INVALID_SOURCE",
            false,
            "transfer.source",
            "transfer source cannot be empty".into(),
        ),
        Error::UnknownTransfer(id) => (
            Code::NotFound,
            "TRANSFER_NOT_FOUND",
            false,
            "transfer",
            format!("unknown transfer {id}"),
        ),
        Error::Closed => (
            Code::Unavailable,
            "ENGINE_UNAVAILABLE",
            true,
            "engine.command_queue",
            "engine command queue is closed".into(),
        ),
    };
    let detail = ErrorDetail {
        reason: reason.into(),
        retryable,
        resource: resource.into(),
    };
    Status::with_details(code, message, Bytes::from(detail.encode_to_vec()))
}
