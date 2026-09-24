package com.github.lisama569.durablefunctionruntimejava.engine.model;

/** Lifecycle status of a single step within a workflow instance. */
public enum StepStatus {
    PENDING,
    RUNNING,
    WAITING,
    RETRY_WAIT,
    COMPLETED,
    FAILED,
    SKIPPED
}
