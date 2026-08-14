//! Bounded-command orchestrator and scheduling primitives.
//!
//! The engine uses an **actor**: one task owns mutable transfer state and changes
//! it only after receiving commands. Callers never lock that transfer map.
//!
//! ```text
//! caller A ─┐                    ┌─ oneshot reply ─> caller A
//! caller B ─┼─ bounded MPSC ─> actor ──owns──> HashMap<Transfer>
//! gRPC    ──┘                    └─ watch metrics ─> subscribers
//! ```
//!
//! A bounded queue is deliberate backpressure: when the actor cannot keep up,
//! producers wait rather than turning overload into unbounded memory growth.
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch, RwLock};

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub download_limit: u64,
    pub upload_limit: u64,
    pub max_peers: u32,
    pub command_buffer: u32,
}
impl Default for Config {
    // Inputs:
    // - No parameters; this is the standard default-construction contract.
    // Outputs:
    // - A conservative runtime configuration with unlimited byte rates, at most
    //   200 peers, and space for 128 pending commands.
    // Logic:
    // - Centralize operational defaults so every composition root starts from the
    //   same explicit resource policy.
    fn default() -> Self {
        Self {
            download_limit: 0,
            upload_limit: 0,
            max_peers: 200,
            command_buffer: 128,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub download_speed: f32,
    pub upload_speed: f32,
    pub connected_peers: u32,
    pub progress: f32,
    pub active_transfers: u32,
}
#[derive(Debug, Clone)]
pub struct Transfer {
    pub id: String,
    pub source: String,
    pub destination: PathBuf,
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    #[error("engine command queue is closed")]
    Closed,
    #[error("transfer source cannot be empty")]
    EmptySource,
    #[error("unknown transfer {0}")]
    UnknownTransfer(String),
}
enum Command {
    Add {
        source: String,
        destination: PathBuf,
        reply: oneshot::Sender<Result<String, Error>>,
    },
    Stop {
        id: String,
        reply: oneshot::Sender<Result<(), Error>>,
    },
}
#[derive(Clone)]
pub struct Engine {
    tx: mpsc::Sender<Command>,
    config: Arc<RwLock<Config>>,
    metrics: watch::Receiver<Metrics>,
}
impl Engine {
    /// Starts the engine actor and returns its cloneable control handle.
    ///
    /// **Inputs:** Initial [`Config`], including the command-queue capacity.
    ///
    /// **Outputs:** An [`Engine`] handle containing a bounded command sender,
    /// shared configuration, and a subscription to the latest metrics snapshot.
    ///
    /// **Logic:** Create MPSC and watch channels, spawn the sole transfer-state
    /// owner, then expose only message-based mutation to callers.
    #[must_use]
    pub fn start(config: Config) -> Self {
        let (tx, rx) = mpsc::channel(config.command_buffer as usize);
        let (metrics_tx, metrics) = watch::channel(Metrics::default());
        let state = config;
        tokio::spawn(run(rx, metrics_tx));
        Self {
            tx,
            config: Arc::new(RwLock::new(state)),
            metrics,
        }
    }
    /// Submits a transfer and waits for the actor-assigned identifier.
    ///
    /// **Inputs:** A logical `source` string and destination filesystem path.
    ///
    /// **Outputs:** A transfer ID, a validation error from the actor, or `Closed`
    /// when either side of command/reply communication has stopped.
    ///
    /// **Logic:** Create a one-use reply channel, send an `Add` command through
    /// the bounded queue (waiting under backpressure), then await its exact reply.
    ///
    /// # Errors
    ///
    /// Returns validation errors or [`Error::Closed`] when the actor stops.
    pub async fn add_transfer(
        &self,
        source: String,
        destination: PathBuf,
    ) -> Result<String, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Command::Add {
                source,
                destination,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::Closed)?;
        reply_rx.await.map_err(|_| Error::Closed)?
    }
    /// Requests removal of one active transfer.
    ///
    /// **Inputs:** The engine-assigned transfer `id`.
    ///
    /// **Outputs:** Success, `UnknownTransfer`, or `Closed` if the actor is gone.
    ///
    /// **Logic:** Package the ID with a one-shot response sender, serialize the
    /// mutation through the command queue, and asynchronously await the outcome.
    ///
    /// # Errors
    ///
    /// Returns unknown-transfer or closed-engine errors.
    pub async fn stop_transfer(&self, id: String) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Stop { id, reply: tx })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
    /// Creates an independent subscription to current and future metrics.
    ///
    /// **Inputs:** The engine handle through `self`.
    ///
    /// **Outputs:** A cloned watch receiver initialized with the latest snapshot.
    ///
    /// **Logic:** Clone only the receiver cursor; all subscribers observe the same
    /// latest-value channel without copying engine ownership.
    #[must_use]
    pub fn subscribe_metrics(&self) -> watch::Receiver<Metrics> {
        self.metrics.clone()
    }
    /// Reads a consistent snapshot of the current runtime configuration.
    ///
    /// **Inputs:** The engine handle through `self`.
    ///
    /// **Outputs:** An owned [`Config`] snapshot that no longer holds the lock.
    ///
    /// **Logic:** Acquire a shared read lock, clone the small configuration, and
    /// release the guard at the end of the expression.
    pub async fn config(&self) -> Config {
        *self.config.read().await
    }
    /// Applies an in-process mutation to runtime configuration.
    ///
    /// **Inputs:** A one-use closure receiving mutable access to [`Config`].
    ///
    /// **Outputs:** Unit after the closure has completed; this API cannot itself
    /// report validation errors yet.
    ///
    /// **Logic:** Take the exclusive configuration lock and run the caller's
    /// mutation while protected from concurrent readers/writers.
    pub async fn update_config(&self, f: impl FnOnce(&mut Config)) {
        let mut config = self.config.write().await;
        f(&mut config);
    }
}
// Inputs:
// - `rx`: sole receiver for all engine commands.
// - `metrics_tx`: latest-value publisher for observable engine state.
// Outputs:
// - No return value; the task ends naturally when every command sender is gone.
// Logic:
// - Own the transfer map and monotonically increasing ID sequence, process one
//   mutation at a time, answer via each command's one-shot channel, then publish
//   a fresh active-transfer count. This avoids locks around transfer state.
async fn run(mut rx: mpsc::Receiver<Command>, metrics_tx: watch::Sender<Metrics>) {
    let mut transfers = HashMap::<String, Transfer>::new();
    let mut sequence = 0u64;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            Command::Add {
                source,
                destination,
                reply,
            } => {
                if source.trim().is_empty() {
                    let _ = reply.send(Err(Error::EmptySource));
                    continue;
                }
                sequence += 1;
                let id = format!("transfer-{sequence}");
                transfers.insert(
                    id.clone(),
                    Transfer {
                        id: id.clone(),
                        source,
                        destination,
                    },
                );
                let _ = reply.send(Ok(id));
            }
            Command::Stop { id, reply } => {
                let result = transfers
                    .remove(&id)
                    .map(|_| ())
                    .ok_or(Error::UnknownTransfer(id));
                let _ = reply.send(result);
            }
        }
        let mut m = metrics_tx.borrow().clone();
        m.active_transfers = u32::try_from(transfers.len()).unwrap_or(u32::MAX);
        let _ = metrics_tx.send(m);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Block {
    pub piece: u32,
    pub offset: u32,
    pub length: u32,
}
/// Selects the least replicated eligible piece.
///
/// **Inputs:** Per-piece peer `availability`, plus sets of completed and currently
/// inflight piece indices.
///
/// **Outputs:** The selected piece index, or `None` when nothing is available and
/// eligible. Equal rarity is resolved by lowest index for deterministic behavior.
///
/// **Logic:** Enumerate availability counts, remove unavailable/completed/inflight
/// candidates, compare by count then index, and return the winning index.
#[must_use]
pub fn rarest_first<S1, S2>(
    availability: &[u32],
    complete: &HashSet<u32, S1>,
    inflight: &HashSet<u32, S2>,
) -> Option<u32>
where
    S1: std::hash::BuildHasher,
    S2: std::hash::BuildHasher,
{
    availability
        .iter()
        .enumerate()
        .filter(|(index, count)| {
            let Ok(index) = u32::try_from(*index) else {
                return false;
            };
            **count > 0 && !complete.contains(&index) && !inflight.contains(&index)
        })
        .min_by(|(ia, a), (ib, b)| match a.cmp(b) {
            Ordering::Equal => ia.cmp(ib),
            o => o,
        })
        .and_then(|(index, _)| u32::try_from(index).ok())
}
