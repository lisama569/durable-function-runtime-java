package com.github.lisama569.durablefunctionruntimejava.domain;

public enum StepStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED,
    /** 分支选择后未被选中的分支步骤：直接跳过，不执行，不影响整体结果。 */
    SKIPPED,
    /** 因兄弟分支终态失败/超时或实例终止而被停下、不再执行的步骤。 */
    CANCELLED
}
