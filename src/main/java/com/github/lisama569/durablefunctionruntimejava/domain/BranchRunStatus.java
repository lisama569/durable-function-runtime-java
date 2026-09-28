package com.github.lisama569.durablefunctionruntimejava.domain;

/** 并行分支的整体运行状态。 */
public enum BranchRunStatus {
    RUNNING,
    COMPLETED,
    /** 因其他分支终态失败/超时或实例终止而被取消。 */
    CANCELLED
}
