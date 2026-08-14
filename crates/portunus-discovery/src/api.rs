//! Transport-independent contracts for bounded endpoint discovery.
//!
//! Providers resolve an opaque byte namespace into a snapshot of network
//! endpoints. Every request carries a hard deadline, endpoint admission limit,
//! and cooperative cancellation token shared across DNS, static, UDP, LAN, or
//! registry adapters.
//!
//! ```text
//! namespace + controls ──> DiscoveryProvider ──> snapshot(endpoints, TTL, source)
//!                    cancellation/deadline ─────┘
//! ```
//!
//! This module defines contracts and common validation only. It does not select a
//! transport, retry policy, cache, global logger, namespace encoding, or endpoint
//! health semantics.

use async_trait::async_trait;
use std::{net::SocketAddr, time::Instant};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// One transport-neutral reachable network endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint {
    address: SocketAddr,
}

impl Endpoint {
    /// Creates an endpoint from a concrete socket address.
    ///
    /// **Inputs:** One IPv4 or IPv6 address and port.
    ///
    /// **Outputs:** A copyable, orderable endpoint with no health assumptions.
    ///
    /// **Logic:** Keep provider results protocol-neutral and deduplicatable.
    #[must_use]
    pub const fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    /// Returns the endpoint's address and port.
    ///
    /// **Inputs:** A shared endpoint borrow.
    ///
    /// **Outputs:** The copyable standard socket address.
    ///
    /// **Logic:** Expose addressing without provider-specific metadata.
    #[must_use]
    pub const fn address(&self) -> SocketAddr {
        self.address
    }
}

/// Uniform admission, deadline, and cancellation controls for one lookup.
#[derive(Debug, Clone)]
pub struct DiscoverOptions {
    deadline: Instant,
    max_endpoints: usize,
    cancellation: CancellationToken,
}

impl DiscoverOptions {
    /// Creates a complete discovery control policy.
    ///
    /// **Inputs:** Absolute monotonic deadline, positive result ceiling, and token.
    ///
    /// **Outputs:** Immutable request options; validation occurs before provider work.
    ///
    /// **Logic:** Require every adapter to receive identical bounded controls.
    #[must_use]
    pub const fn new(
        deadline: Instant,
        max_endpoints: usize,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            deadline,
            max_endpoints,
            cancellation,
        }
    }

    /// Validates common discovery admission controls.
    ///
    /// **Inputs:** A shared options borrow; no clock or external state is read.
    ///
    /// **Outputs:** Unit, or a stable error for a zero result budget.
    ///
    /// **Logic:** Deadline and cancellation are dynamic and remain provider checks.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::InvalidEndpointLimit`] for a zero ceiling.
    pub const fn validate(&self) -> Result<(), DiscoveryError> {
        if self.max_endpoints == 0 {
            Err(DiscoveryError::InvalidEndpointLimit)
        } else {
            Ok(())
        }
    }

    /// Returns the absolute monotonic deadline.
    ///
    /// **Inputs:** A shared options borrow.
    ///
    /// **Outputs:** The copyable deadline instant.
    ///
    /// **Logic:** Providers compare this value and bound each awaited operation.
    #[must_use]
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns the maximum number of admitted unique endpoints.
    ///
    /// **Inputs:** A shared options borrow.
    ///
    /// **Outputs:** A positive ceiling after successful validation.
    ///
    /// **Logic:** Adapters truncate only after deterministic deduplication.
    #[must_use]
    pub const fn max_endpoints(&self) -> usize {
        self.max_endpoints
    }

    /// Borrows the cooperative cancellation token.
    ///
    /// **Inputs:** A shared options borrow.
    ///
    /// **Outputs:** The request-scoped token without cloning its shared state.
    ///
    /// **Logic:** Providers select on cancellation around every blocking operation.
    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// One immutable discovery result with provenance and refresh boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverySnapshot {
    namespace: Vec<u8>,
    endpoints: Vec<Endpoint>,
    valid_until: Instant,
    source: String,
}

impl DiscoverySnapshot {
    /// Creates a provider-attributed immutable result snapshot.
    ///
    /// **Inputs:** Owned namespace, admitted endpoints, expiry instant, and source.
    ///
    /// **Outputs:** A snapshot preserving provider order exactly.
    ///
    /// **Logic:** Construction remains available to adapters; policy helpers decide
    /// deduplication, ordering, and truncation before calling it.
    #[must_use]
    pub fn new(
        namespace: Vec<u8>,
        endpoints: Vec<Endpoint>,
        valid_until: Instant,
        source: impl Into<String>,
    ) -> Self {
        Self {
            namespace,
            endpoints,
            valid_until,
            source: source.into(),
        }
    }

    /// Borrows the opaque namespace resolved by this snapshot.
    ///
    /// **Inputs:** A shared snapshot borrow.
    ///
    /// **Outputs:** Original namespace bytes tied to the snapshot lifetime.
    ///
    /// **Logic:** Preserve byte-native naming without UTF-8 coercion or copying.
    #[must_use]
    pub fn namespace(&self) -> &[u8] {
        &self.namespace
    }

    /// Borrows admitted endpoints in deterministic provider order.
    ///
    /// **Inputs:** A shared snapshot borrow.
    ///
    /// **Outputs:** An immutable slice of unique admitted endpoints.
    ///
    /// **Logic:** Hide vector mutation while permitting allocation-free iteration.
    #[must_use]
    pub fn endpoints(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// Returns the monotonic instant at which consumers should refresh.
    ///
    /// **Inputs:** A shared snapshot borrow.
    ///
    /// **Outputs:** Copyable absolute monotonic refresh boundary.
    ///
    /// **Logic:** Consumers compare against their clock without provider coupling.
    #[must_use]
    pub const fn valid_until(&self) -> Instant {
        self.valid_until
    }

    /// Borrows the stable provider source label for telemetry.
    ///
    /// **Inputs:** A shared snapshot borrow.
    ///
    /// **Outputs:** Provider label tied to the snapshot lifetime.
    ///
    /// **Logic:** Expose bounded provenance without returning implementation state.
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }
}

/// Stable failure categories shared by discovery providers.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum DiscoveryError {
    #[error("maximum endpoints must be greater than zero")]
    InvalidEndpointLimit,
    #[error("retry policy requires attempts and nonzero bounded timeouts")]
    InvalidRetryPolicy,
    #[error("discovery request was cancelled")]
    Cancelled,
    #[error("discovery deadline elapsed")]
    DeadlineExceeded,
    #[error("provider rejected the request: {0}")]
    Provider(String),
}

/// Pluggable asynchronous endpoint discovery over any transport.
#[async_trait]
pub trait DiscoveryProvider: Send + Sync {
    /// Resolves one opaque namespace under explicit controls.
    ///
    /// **Inputs:** Borrowed namespace bytes and owned bounded request options.
    ///
    /// **Outputs:** One immutable snapshot, or a stable common/provider error.
    ///
    /// **Logic:** Implementations validate options before I/O, observe cancellation
    /// and deadline during waits, and cap unique endpoints before returning.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError>;
}
