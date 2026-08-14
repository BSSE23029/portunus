//! Deterministic offline composition harness for the reference protocol workload.
//!
//! The daemon owns this torrent-specific proof because reusable crates expose only
//! protocol-neutral parsing, discovery, transport, storage, and orchestration APIs.
//! Synthetic endpoints and an in-memory duplex stream avoid public-network access.

use bytes::Bytes;
use portunus_bencode::{parse, PathSegment};
use portunus_discovery::{DiscoverOptions, DiscoveryProvider, Endpoint, StaticProvider};
use portunus_engine::{
    budget::{BudgetConfig, ResourceRequest},
    orchestrator::{JobCompletion, JobSpec, Orchestrator, OrchestratorConfig},
    policy::{ExponentialRetry, PriorityScheduler},
};
use portunus_storage::{integrity::sha1_digest, torrent::PieceStore};
use portunus_transport::{
    peer::{Message, PeerCodec},
    start_session, SessionConfig,
};
use std::{
    net::{Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const NAMESPACE: &[u8] = b"reference-workload";
const METADATA: &[u8] = b"d4:infod6:lengthi15e4:name13:reference.binee";

/// Validated inputs for one deterministic reference transfer.
#[derive(Debug, Clone)]
pub struct ReferenceWorkloadConfig {
    destination: PathBuf,
    candidate_endpoints: usize,
    max_endpoints: usize,
    payload: Vec<u8>,
    fault: Option<SimulationStage>,
}

impl ReferenceWorkloadConfig {
    /// Inputs: destination, synthetic population, admission ceiling, and one piece.
    /// Outputs: bounded immutable simulation configuration.
    /// Logic: reject empty or unrepresentable populations before allocating fixtures.
    /// # Errors
    /// Returns [`SimulationError::InvalidConfig`] for inconsistent bounds.
    pub fn new(
        destination: PathBuf,
        candidates: usize,
        admitted: usize,
        payload: Vec<u8>,
    ) -> Result<Self, SimulationError> {
        if candidates == 0
            || candidates > usize::from(u16::MAX)
            || admitted == 0
            || admitted > candidates
            || payload.is_empty()
            || payload.len() > u32::MAX as usize
        {
            return Err(SimulationError::InvalidConfig);
        }
        Ok(Self {
            destination,
            candidate_endpoints: candidates,
            max_endpoints: admitted,
            payload,
            fault: None,
        })
    }

    /// Inputs: one explicit boundary at which execution should fail.
    /// Outputs: updated immutable-style configuration.
    /// Logic: retain a single bounded fault instead of an ambient mutable script.
    #[must_use]
    pub const fn with_fault(mut self, stage: SimulationStage) -> Self {
        self.fault = Some(stage);
        self
    }
}

/// Stable measurements produced by the offline composition harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceWorkloadReport {
    pub candidate_endpoints: usize,
    pub admitted_endpoints: usize,
    pub parsed_name: Vec<u8>,
    pub transferred_bytes: usize,
    pub outbound_frames: u64,
    pub inbound_frames: u64,
    pub engine_completed: bool,
}

/// Stable component boundaries available for deterministic failure injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulationStage {
    AfterDiscovery,
    AfterTransport,
}

/// Stable harness failures without leaking component implementation types.
#[derive(Debug, Error)]
pub enum SimulationError {
    #[error("reference workload configuration is invalid")]
    InvalidConfig,
    #[error("reference workload injected a failure at {0:?}")]
    Injected(SimulationStage),
    #[error("reference workload failed during {0}")]
    Stage(&'static str),
}

/// Runs one bounded, offline transfer through all reusable data-plane components.
///
/// Inputs: validated deterministic fixture and resource ceilings.
/// Outputs: measured discovery, transport, storage, and engine completion report.
/// Logic: parse borrowed metadata, discover synthetic peers, transfer one framed
/// piece through memory, verify/commit it, then execute one engine-owned completion.
/// # Errors
/// Returns a stable stage label when any component rejects the reference workload.
pub async fn run_reference_workload(
    config: ReferenceWorkloadConfig,
) -> Result<ReferenceWorkloadReport, SimulationError> {
    let parsed_name = parse_name()?;
    let admitted_endpoints =
        discover_endpoints(config.candidate_endpoints, config.max_endpoints).await?;
    inject(config.fault, SimulationStage::AfterDiscovery)?;
    let (transferred_bytes, outbound_frames, inbound_frames) =
        transfer_piece(&config.destination, &config.payload).await?;
    inject(config.fault, SimulationStage::AfterTransport)?;
    let engine_completed = complete_engine_job().await?;
    Ok(ReferenceWorkloadReport {
        candidate_endpoints: config.candidate_endpoints,
        admitted_endpoints,
        parsed_name,
        transferred_bytes,
        outbound_frames,
        inbound_frames,
        engine_completed,
    })
}

/// Inputs: optional configured fault and the boundary currently reached.
/// Outputs: unit or the exact injected boundary.
/// Logic: compare stable enum values without clocks, randomness, or global state.
fn inject(
    configured: Option<SimulationStage>,
    reached: SimulationStage,
) -> Result<(), SimulationError> {
    if configured == Some(reached) {
        Err(SimulationError::Injected(reached))
    } else {
        Ok(())
    }
}

/// Inputs: fixed borrowed reference metadata.
/// Outputs: owned name selected through typed byte-key traversal.
/// Logic: exercise bounded parsing without introducing a torrent schema crate.
fn parse_name() -> Result<Vec<u8>, SimulationError> {
    let parsed = parse(METADATA).map_err(|_| SimulationError::Stage("metadata"))?;
    let name = parsed
        .at_path(&[PathSegment::Key(b"info"), PathSegment::Key(b"name")])
        .and_then(|value| {
            value
                .as_bytes()
                .map_err(|_| portunus_bencode::PathError::TypeMismatch {
                    segment: 1,
                    expected: portunus_bencode::ValueKind::Bytes,
                    actual: value.kind(),
                })
        })
        .map_err(|_| SimulationError::Stage("metadata-name"))?;
    Ok(name.to_vec())
}

/// Inputs: validated synthetic population and result ceiling.
/// Outputs: deterministic admitted endpoint count.
/// Logic: materialize loopback peers, then use the real provider's dedup and cap.
async fn discover_endpoints(candidates: usize, admitted: usize) -> Result<usize, SimulationError> {
    let endpoints = (0..candidates)
        .map(|index| {
            u16::try_from(index)
                .map(|port| Endpoint::new(SocketAddr::new(Ipv6Addr::LOCALHOST.into(), port)))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| SimulationError::InvalidConfig)?;
    let provider =
        StaticProvider::new(Duration::from_secs(30)).with_namespace(NAMESPACE.to_vec(), endpoints);
    let snapshot = provider
        .discover(
            NAMESPACE,
            DiscoverOptions::new(
                Instant::now() + Duration::from_secs(1),
                admitted,
                CancellationToken::new(),
            ),
        )
        .await
        .map_err(|_| SimulationError::Stage("discovery"))?;
    Ok(snapshot.endpoints().len())
}

/// Inputs: destination path and one nonempty bounded piece.
/// Outputs: transferred byte count plus sender/receiver frame measurements.
/// Logic: exchange the piece over bounded in-memory sessions and commit after SHA-1.
async fn transfer_piece(
    destination: &PathBuf,
    payload: &[u8],
) -> Result<(usize, u64, u64), SimulationError> {
    let session_config =
        SessionConfig::new(1, 1, 1).map_err(|_| SimulationError::Stage("session-config"))?;
    let (left, right) = tokio::io::duplex(payload.len() + 32);
    let mut receiver = start_session(left, PeerCodec::new(payload.len() + 9), session_config);
    let sender = start_session(right, PeerCodec::new(payload.len() + 9), session_config);
    sender
        .try_send(Message::Piece {
            index: 0,
            begin: 0,
            block: Bytes::copy_from_slice(payload),
        })
        .map_err(|_| SimulationError::Stage("send"))?;
    let message = receiver
        .recv()
        .await
        .ok_or(SimulationError::Stage("receive"))?;
    let Message::Piece { block, .. } = message else {
        return Err(SimulationError::Stage("message"));
    };
    let store = PieceStore::create(
        destination,
        block.len() as u64,
        block.len() as u64,
        vec![sha1_digest(&block)],
    )
    .await
    .map_err(|_| SimulationError::Stage("storage-create"))?;
    store
        .write_verified_piece(0, &block)
        .await
        .map_err(|_| SimulationError::Stage("storage-write"))?;
    sender.cancel();
    receiver.cancel();
    let sender_report = sender
        .join()
        .await
        .map_err(|_| SimulationError::Stage("sender-join"))?;
    let receiver_report = receiver
        .join()
        .await
        .map_err(|_| SimulationError::Stage("receiver-join"))?;
    Ok((
        block.len(),
        sender_report.outbound_frames(),
        receiver_report.inbound_frames(),
    ))
}

/// Inputs: no ambient clock or external work.
/// Outputs: whether one bounded engine job reached completed state.
/// Logic: compose deterministic scheduling, retry, task ownership, and resource admission.
async fn complete_engine_job() -> Result<bool, SimulationError> {
    let mut engine = Orchestrator::new(
        OrchestratorConfig::new(1, 1).map_err(|_| SimulationError::Stage("engine-config"))?,
        BudgetConfig::new(1, 1, 1, 1).map_err(|_| SimulationError::Stage("engine-budget"))?,
        Box::new(PriorityScheduler),
        Box::new(
            ExponentialRetry::new(1, Duration::from_millis(1), Duration::from_millis(1))
                .map_err(|_| SimulationError::Stage("engine-retry"))?,
        ),
    );
    engine
        .try_submit(JobSpec::new(
            1,
            1,
            ResourceRequest::new(1, 1, 1),
            Arc::new(|_, _| Box::pin(async { Ok(()) })),
        ))
        .map_err(|_| SimulationError::Stage("engine-submit"))?;
    let dispatch = engine
        .try_dispatch_next(Duration::ZERO)
        .map_err(|_| SimulationError::Stage("engine-dispatch"))?
        .ok_or(SimulationError::Stage("engine-empty"))?;
    let completed = engine
        .join(dispatch.task_id, Duration::ZERO)
        .await
        .map_err(|_| SimulationError::Stage("engine-join"))?
        == JobCompletion::Completed;
    Ok(completed)
}
