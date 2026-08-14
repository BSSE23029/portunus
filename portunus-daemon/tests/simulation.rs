//! Offline full-stack reference-workload simulation coverage.

use portunus_daemon::simulation::{
    run_reference_workload, ReferenceWorkloadConfig, SimulationError, SimulationStage,
};

// Inputs: 8,192 deterministic candidate endpoints and a 4,096-endpoint admission budget.
// Outputs: one verified offline transfer with bounded discovery and engine/session reports.
// Logic: prove every reusable data-plane crate composes without sockets or public services.
#[tokio::test]
async fn transfers_reference_piece_across_thousands_of_simulated_endpoints() {
    let path = std::env::temp_dir().join(format!("portunus-full-stack-{}", std::process::id()));
    let config =
        ReferenceWorkloadConfig::new(path.clone(), 8_192, 4_096, b"offline payload".to_vec())
            .unwrap();

    let report = run_reference_workload(config).await.unwrap();

    assert_eq!(report.candidate_endpoints, 8_192);
    assert_eq!(report.admitted_endpoints, 4_096);
    assert_eq!(report.parsed_name, b"reference.bin");
    assert_eq!(report.transferred_bytes, 15);
    assert_eq!(report.outbound_frames, 1);
    assert_eq!(report.inbound_frames, 1);
    assert!(report.engine_completed);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"offline payload");
    tokio::fs::remove_file(path).await.unwrap();
}

// Inputs: a valid offline workload with a deterministic post-discovery fault.
// Outputs: the exact injected stage and no destination-file side effect.
// Logic: prove failure scenarios are reproducible and isolated from later stages.
#[tokio::test]
async fn injects_a_deterministic_full_stack_failure() {
    let path =
        std::env::temp_dir().join(format!("portunus-full-stack-fault-{}", std::process::id()));
    let config = ReferenceWorkloadConfig::new(path.clone(), 2_048, 64, b"payload".to_vec())
        .unwrap()
        .with_fault(SimulationStage::AfterDiscovery);

    assert!(matches!(
        run_reference_workload(config).await,
        Err(SimulationError::Injected(SimulationStage::AfterDiscovery))
    ));
    assert!(!path.exists());
}
