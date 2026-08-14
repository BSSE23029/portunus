//! Bounded-command swarm orchestrator and rarest-first scheduler.
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch, RwLock};

#[derive(Debug, Clone)]
pub struct Config {
    pub download_limit: u64,
    pub upload_limit: u64,
    pub max_peers: u32,
    pub command_buffer: u32,
}
impl Default for Config {
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
#[derive(Debug, Error)]
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
    pub fn start(config: Config) -> Self {
        let (tx, rx) = mpsc::channel(config.command_buffer as usize);
        let (metrics_tx, metrics) = watch::channel(Metrics::default());
        let state = config.clone();
        tokio::spawn(run(rx, metrics_tx));
        Self {
            tx,
            config: Arc::new(RwLock::new(state)),
            metrics,
        }
    }
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
    pub async fn stop_transfer(&self, id: String) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Stop { id, reply: tx })
            .await
            .map_err(|_| Error::Closed)?;
        rx.await.map_err(|_| Error::Closed)?
    }
    pub fn subscribe_metrics(&self) -> watch::Receiver<Metrics> {
        self.metrics.clone()
    }
    pub async fn config(&self) -> Config {
        self.config.read().await.clone()
    }
    pub async fn update_config(&self, f: impl FnOnce(&mut Config)) {
        let mut config = self.config.write().await;
        f(&mut config);
    }
}
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
        m.active_transfers = transfers.len() as u32;
        let _ = metrics_tx.send(m);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Block {
    pub piece: u32,
    pub offset: u32,
    pub length: u32,
}
pub fn rarest_first(
    availability: &[u32],
    complete: &HashSet<u32>,
    inflight: &HashSet<u32>,
) -> Option<u32> {
    availability
        .iter()
        .enumerate()
        .filter(|(i, n)| {
            **n > 0 && !complete.contains(&(*i as u32)) && !inflight.contains(&(*i as u32))
        })
        .min_by(|(ia, a), (ib, b)| match a.cmp(b) {
            Ordering::Equal => ia.cmp(ib),
            o => o,
        })
        .map(|(i, _)| i as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chooses_rarest_available() {
        assert_eq!(
            rarest_first(&[4, 1, 2], &HashSet::new(), &HashSet::new()),
            Some(1)
        );
    }
    #[tokio::test]
    async fn bounded_actor_accepts_transfer() {
        let e = Engine::start(Config::default());
        assert_eq!(
            e.add_transfer("magnet:?xt=x".into(), ".".into())
                .await
                .unwrap(),
            "transfer-1"
        );
    }
}
