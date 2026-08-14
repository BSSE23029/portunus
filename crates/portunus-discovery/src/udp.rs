//! UDP tracker adapter for the transport-independent discovery contract.
//!
//! The adapter performs BEP 15 connect then announce exchanges over one connected
//! datagram socket. Every attempt uses a fresh transaction identifier; unrelated,
//! truncated, or malformed datagrams are isolated until the attempt timeout.
//! Exponential attempt windows are capped and every send/receive races the request
//! deadline and cooperative cancellation token.
//!
//! This module is a compatibility adapter. It does not expose torrent concepts in
//! the provider trait, contact public trackers in tests, cache connection IDs,
//! install logging policy, or retry beyond its explicit attempt budget.

use crate::{
    announce_request, connect_request, parse_announce_response, parse_connect_response,
    DiscoverOptions, DiscoveryError, DiscoveryProvider, DiscoverySnapshot, Endpoint,
};
use async_trait::async_trait;
use std::{
    collections::BTreeSet,
    future::Future,
    net::SocketAddr,
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, time::sleep_until};

/// Bounded exponential timeout policy for unreliable discovery requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    attempts: usize,
    initial_timeout: Duration,
    max_timeout: Duration,
}

impl RetryPolicy {
    /// Creates a validated retry policy.
    ///
    /// **Inputs:** Positive attempt count and nonzero initial/maximum timeouts.
    ///
    /// **Outputs:** A bounded policy, or a stable validation error.
    ///
    /// **Logic:** Reject zero work and timeout inversion before any network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`DiscoveryError::InvalidRetryPolicy`] for invalid bounds.
    pub fn new(
        attempts: usize,
        initial_timeout: Duration,
        max_timeout: Duration,
    ) -> Result<Self, DiscoveryError> {
        if attempts == 0 || initial_timeout.is_zero() || max_timeout < initial_timeout {
            return Err(DiscoveryError::InvalidRetryPolicy);
        }
        Ok(Self {
            attempts,
            initial_timeout,
            max_timeout,
        })
    }

    // Inputs: zero-based attempt number.
    // Outputs: exponentially increased duration capped at the configured maximum.
    // Logic: use checked multiplication so hostile counts cannot overflow duration.
    fn timeout(self, attempt: usize) -> Duration {
        let shift = u32::try_from(attempt).unwrap_or(u32::MAX).min(31);
        self.initial_timeout
            .checked_mul(1_u32 << shift)
            .unwrap_or(self.max_timeout)
            .min(self.max_timeout)
    }
}

/// UDP tracker configuration implementing generic endpoint discovery.
#[derive(Debug, Clone)]
pub struct UdpTrackerProvider {
    tracker: SocketAddr,
    peer_id: [u8; 20],
    port: u16,
    retry: RetryPolicy,
}

impl UdpTrackerProvider {
    /// Creates a tracker adapter with immutable client and retry configuration.
    ///
    /// **Inputs:** Tracker address, compatibility peer ID/listen port, and retry policy.
    ///
    /// **Outputs:** A reusable provider; no socket or background task is created yet.
    ///
    /// **Logic:** Defer per-request sockets and transaction IDs to `discover`.
    #[must_use]
    pub const fn new(
        tracker: SocketAddr,
        peer_id: [u8; 20],
        port: u16,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            tracker,
            peer_id,
            port,
            retry,
        }
    }

    // Inputs: connected socket, request controls, packet/parser factory, and stage.
    // Outputs: first correlated parsed response, or cancellation/deadline/retry error.
    // Logic: retransmit with fresh IDs and isolate malformed datagrams per attempt.
    async fn exchange<T, Build, Parse>(
        &self,
        socket: &UdpSocket,
        options: &DiscoverOptions,
        stage: &'static str,
        build: Build,
        parse: Parse,
    ) -> Result<T, DiscoveryError>
    where
        Build: Fn() -> (Vec<u8>, u32) + Send + Sync,
        Parse: Fn(&[u8], u32) -> Result<T, crate::Error> + Send + Sync,
    {
        let mut response = vec![0_u8; 65_507];
        for attempt in 0..self.retry.attempts {
            let (request, transaction) = build();
            race_controls(options, socket.send(&request))
                .await?
                .map_err(|error| provider_io(&error))?;
            let attempt_end = Instant::now()
                .checked_add(self.retry.timeout(attempt))
                .unwrap_or_else(|| options.deadline())
                .min(options.deadline());
            loop {
                let received = tokio::select! {
                    () = options.cancellation().cancelled() => return Err(DiscoveryError::Cancelled),
                    () = sleep_until(attempt_end.into()) => break,
                    result = socket.recv(&mut response) => result,
                };
                let length = received.map_err(|error| provider_io(&error))?;
                match parse(&response[..length], transaction) {
                    Ok(value) => return Ok(value),
                    Err(error) => tracing::warn!(
                        provider = "udp-tracker",
                        stage,
                        attempt = attempt + 1,
                        error = %error,
                        "isolated malformed discovery response"
                    ),
                }
            }
            tracing::debug!(
                provider = "udp-tracker",
                stage,
                attempt = attempt + 1,
                timeout_millis = self.retry.timeout(attempt).as_millis(),
                "discovery request retrying"
            );
        }
        if Instant::now() >= options.deadline() {
            Err(DiscoveryError::DeadlineExceeded)
        } else {
            Err(DiscoveryError::Provider(format!(
                "UDP {stage} retry budget exhausted"
            )))
        }
    }
}

#[async_trait]
impl DiscoveryProvider for UdpTrackerProvider {
    /// Resolves a 20-byte compatibility namespace through a UDP tracker.
    ///
    /// **Inputs:** Exactly 20 namespace bytes plus common bounded request controls.
    ///
    /// **Outputs:** Unique endpoint snapshot with tracker TTL, or stable control/
    /// provider failure. Tracker-specific concepts remain inside this adapter.
    ///
    /// **Logic:** Validate controls, connect a per-request socket, exchange connect
    /// and announce packets, then sort/deduplicate/truncate endpoints.
    async fn discover(
        &self,
        namespace: &[u8],
        options: DiscoverOptions,
    ) -> Result<DiscoverySnapshot, DiscoveryError> {
        options.validate()?;
        let info_hash: [u8; 20] = namespace.try_into().map_err(|_| {
            DiscoveryError::Provider("UDP tracker namespace must be 20 bytes".to_owned())
        })?;
        if options.cancellation().is_cancelled() {
            return Err(DiscoveryError::Cancelled);
        }
        if Instant::now() >= options.deadline() {
            return Err(DiscoveryError::DeadlineExceeded);
        }
        let bind_address = if self.tracker.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let socket = race_controls(&options, UdpSocket::bind(bind_address))
            .await?
            .map_err(|error| provider_io(&error))?;
        race_controls(&options, socket.connect(self.tracker))
            .await?
            .map_err(|error| provider_io(&error))?;

        let connection = self
            .exchange(
                &socket,
                &options,
                "connect",
                || {
                    let transaction = rand::random();
                    (connect_request(transaction).to_vec(), transaction)
                },
                parse_connect_response,
            )
            .await?;
        let (interval, addresses) = self
            .exchange(
                &socket,
                &options,
                "announce",
                || {
                    let transaction = rand::random();
                    (
                        announce_request(
                            connection,
                            transaction,
                            info_hash,
                            self.peer_id,
                            0,
                            0,
                            0,
                            self.port,
                        )
                        .to_vec(),
                        transaction,
                    )
                },
                parse_announce_response,
            )
            .await?;
        let mut endpoints: Vec<_> = addresses
            .into_iter()
            .map(Endpoint::new)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        endpoints.truncate(options.max_endpoints());
        let valid_until = Instant::now()
            .checked_add(Duration::from_secs(u64::from(interval)))
            .ok_or_else(|| DiscoveryError::Provider("tracker TTL overflow".to_owned()))?;
        tracing::info!(
            provider = "udp-tracker",
            admitted = endpoints.len(),
            ttl_seconds = interval,
            "discovery snapshot resolved"
        );
        Ok(DiscoverySnapshot::new(
            namespace.to_vec(),
            endpoints,
            valid_until,
            "udp-tracker",
        ))
    }
}

// Inputs: common controls and one socket future.
// Outputs: future output, or cancellation/deadline before it completes.
// Logic: centralize control races for socket creation, connect, and send operations.
async fn race_controls<T, E>(
    options: &DiscoverOptions,
    future: impl Future<Output = Result<T, E>>,
) -> Result<Result<T, E>, DiscoveryError> {
    tokio::select! {
        () = options.cancellation().cancelled() => Err(DiscoveryError::Cancelled),
        () = sleep_until(options.deadline().into()) => Err(DiscoveryError::DeadlineExceeded),
        result = future => Ok(result),
    }
}

// Inputs: one standard socket I/O error.
// Outputs: provider-scoped stable discovery error without payload data.
// Logic: retain diagnostic text while hiding transport error types from the trait.
fn provider_io(error: &std::io::Error) -> DiscoveryError {
    DiscoveryError::Provider(format!("UDP I/O failed: {error}"))
}
