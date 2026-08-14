use portunus_engine::{
    budget::{BudgetConfig, BudgetError, BudgetPool, ResourceRequest},
    runtime::{TaskError, TaskGroup, TaskTermination},
};

// Inputs: one-slot group, cooperative task, and second immediate spawn.
// Outputs: saturated rejection, cancellation outcome, and released budget.
// Logic: cross structured ownership, task cancellation, joining, and RAII admission.
#[tokio::test]
async fn owns_cancels_and_joins_bounded_tasks() {
    let pool = BudgetPool::new(BudgetConfig::new(1, 4, 4, 4).unwrap());
    let mut group = TaskGroup::new(pool.clone());
    let task = group
        .try_spawn(ResourceRequest::new(4, 4, 4), |cancellation| async move {
            cancellation.cancelled().await;
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        group.try_spawn(ResourceRequest::new(0, 0, 0), |_| async { Ok(()) }),
        Err(TaskError::Budget(BudgetError::Saturated))
    ));
    group.cancel(task).unwrap();
    let outcome = group.join(task).await.unwrap();
    assert_eq!(outcome.id, task);
    assert_eq!(outcome.termination, TaskTermination::Cancelled);
    assert_eq!(pool.snapshot().active_tasks, 0);
}

// Inputs: one successful task, one application failure, and unknown ID.
// Outputs: exact terminal classifications and stable lookup error.
// Logic: keep application failure codes distinct from runtime cancellation/panic.
#[tokio::test]
async fn reports_task_terminal_outcomes() {
    let pool = BudgetPool::new(BudgetConfig::new(2, 2, 2, 2).unwrap());
    let mut group = TaskGroup::new(pool);
    let success = group
        .try_spawn(ResourceRequest::new(1, 1, 1), |_| async { Ok(()) })
        .unwrap();
    let failure = group
        .try_spawn(ResourceRequest::new(1, 1, 1), |_| async { Err(17) })
        .unwrap();
    assert_eq!(
        group.join(success).await.unwrap().termination,
        TaskTermination::Completed
    );
    assert_eq!(
        group.join(failure).await.unwrap().termination,
        TaskTermination::Failed { code: 17 }
    );
    assert_eq!(
        group.join(failure).await.unwrap_err(),
        TaskError::UnknownTask(failure)
    );
}

// Inputs: two cooperative tasks and group-wide shutdown.
// Outputs: deterministically ID-ordered cancelled outcomes and empty ownership.
// Logic: prove draining shutdown joins every child instead of detaching work.
#[tokio::test]
async fn shuts_down_all_owned_tasks() {
    let pool = BudgetPool::new(BudgetConfig::new(2, 2, 2, 2).unwrap());
    let mut group = TaskGroup::new(pool);
    for _ in 0..2 {
        group
            .try_spawn(ResourceRequest::new(1, 1, 1), |cancellation| async move {
                cancellation.cancelled().await;
                Ok(())
            })
            .unwrap();
    }
    let outcomes = group.shutdown().await;
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.windows(2).all(|pair| pair[0].id < pair[1].id));
    assert!(outcomes
        .iter()
        .all(|outcome| outcome.termination == TaskTermination::Cancelled));
}
