package com.github.lisama569.durablefunctionruntimejava.domain;

public enum StepStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    /** 未被条件分支选中的活动：跳过执行，不阻塞流程，不计为失败。 */
    SKIPPED
}
