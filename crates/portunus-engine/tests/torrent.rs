//! Integration coverage for the isolated `BitTorrent` compatibility adapter.

use portunus_engine::torrent::{rarest_first, Config, Engine, Error};
use std::collections::HashSet;

// Inputs: availability with completed and inflight exclusions.
// Outputs: least replicated eligible piece or no selection.
// Logic: cover rarity, deterministic ties, exclusions, and unavailable pieces.
#[test]
fn schedules_rarest_eligible_piece() {
    assert_eq!(
        rarest_first(&[4, 1, 2], &HashSet::new(), &HashSet::new()),
        Some(1)
    );
    assert_eq!(
        rarest_first(&[1, 1], &HashSet::new(), &HashSet::new()),
        Some(0)
    );
    assert_eq!(
        rarest_first(&[0, 0], &HashSet::new(), &HashSet::new()),
        None
    );
    assert_eq!(
        rarest_first(&[1], &HashSet::from([0]), &HashSet::new()),
        None
    );
    assert_eq!(
        rarest_first(&[1], &HashSet::new(), &HashSet::from([0])),
        None
    );
}

// Inputs: two valid additions followed by one stop.
// Outputs: sequential IDs and updated active-transfer snapshots.
// Logic: cross command, reply, state mutation, and metrics channel boundaries.
#[tokio::test]
async fn actor_manages_transfer_lifecycle() {
    let engine = Engine::start(Config::default());
    let mut metrics = engine.subscribe_metrics();
    assert_eq!(
        engine.add_transfer("one".into(), ".".into()).await.unwrap(),
        "transfer-1"
    );
    metrics.changed().await.unwrap();
    assert_eq!(metrics.borrow().active_transfers, 1);
    let second = engine.add_transfer("two".into(), ".".into()).await.unwrap();
    engine.stop_transfer(second).await.unwrap();
    assert_eq!(
        engine.stop_transfer("missing".into()).await,
        Err(Error::UnknownTransfer("missing".into()))
    );
}

// Inputs: empty source and a partial runtime configuration update.
// Outputs: validation failure plus preserved and changed config fields.
// Logic: cover actor validation and patch-like configuration semantics.
#[tokio::test]
async fn validates_commands_and_updates_config() {
    let engine = Engine::start(Config::default());
    assert_eq!(
        engine.add_transfer("  ".into(), ".".into()).await,
        Err(Error::EmptySource)
    );
    engine.update_config(|config| config.max_peers = 12).await;
    let config = engine.config().await;
    assert_eq!(config.max_peers, 12);
    assert_eq!(config.command_buffer, 128);
}
