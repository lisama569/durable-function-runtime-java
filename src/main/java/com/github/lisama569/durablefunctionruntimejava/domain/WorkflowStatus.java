package com.github.lisama569.durablefunctionruntimejava.domain;

/** 工作流实例的终态与中间态。 */
public enum WorkflowStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    TERMINATED
}
