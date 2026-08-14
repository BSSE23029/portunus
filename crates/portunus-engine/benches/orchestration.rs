//! Bounded orchestration dispatch-to-completion latency benchmark.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use portunus_engine::{
    budget::{BudgetConfig, ResourceRequest},
    orchestrator::{JobSpec, Orchestrator, OrchestratorConfig},
    policy::{ExponentialRetry, PriorityScheduler},
};
use std::{sync::Arc, time::Duration};

// Inputs: Criterion context and one immediately successful bounded job per sample.
// Outputs: latency samples spanning admission, scheduling, spawn, join, and publication.
// Logic: construct isolated owners so retained terminal state cannot bias later samples.
fn benchmark_orchestration(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    criterion.bench_function("engine/orchestrator/dispatch_complete", |bencher| {
        bencher.iter(|| {
            runtime.block_on(async {
                let mut engine = benchmark_engine();
                engine.try_submit(successful_job()).unwrap();
                let dispatch = engine.try_dispatch_next(Duration::ZERO).unwrap().unwrap();
                black_box(engine.join(dispatch.task_id, Duration::ZERO).await.unwrap());
            });
        });
    });
}

// Inputs: no ambient state beyond the entered Tokio runtime.
// Outputs: isolated single-job orchestrator with one unit in every resource dimension.
// Logic: rebuild ownership and policy state for every benchmark sample.
fn benchmark_engine() -> Orchestrator {
    Orchestrator::new(
        OrchestratorConfig::new(1, 8).unwrap(),
        BudgetConfig::new(1, 1, 1, 1).unwrap(),
        Box::new(PriorityScheduler),
        Box::new(
            ExponentialRetry::new(1, Duration::from_millis(1), Duration::from_millis(1)).unwrap(),
        ),
    )
}

// Inputs: no external state.
// Outputs: one immediately successful job requesting the exact available resources.
// Logic: isolate orchestration overhead from application work and I/O.
fn successful_job() -> JobSpec {
    JobSpec::new(
        1,
        1,
        ResourceRequest::new(1, 1, 1),
        Arc::new(|_, _| Box::pin(async { Ok(()) })),
    )
}

criterion_group!(benches, benchmark_orchestration);
criterion_main!(benches);
