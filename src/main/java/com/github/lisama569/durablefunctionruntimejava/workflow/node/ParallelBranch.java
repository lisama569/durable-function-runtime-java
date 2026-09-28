package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepHandler;

/**
 * 并行分支：一个活动 + 其在并行组内的展示标签（同一 ParallelNode 内唯一）。
 * 每路分支沿用活动自身的 {@link RetryPolicy}（重试次数、退避、超时）。
 */
public record ParallelBranch(String label, String activityName, StepHandler handler, RetryPolicy retryPolicy) {

    public ParallelBranch {
        if (label == null || label.isBlank()) {
            throw new IllegalArgumentException("parallel branch label must not be blank");
        }
        if (activityName == null || activityName.isBlank()) {
            throw new IllegalArgumentException("parallel branch activity name must not be blank");
        }
        if (handler == null) {
            throw new IllegalArgumentException("parallel branch handler must not be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy must not be null");
        }
    }
}
