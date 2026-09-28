package com.github.lisama569.durablefunctionruntimejava.workflow.node;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;
import com.github.lisama569.durablefunctionruntimejava.workflow.StepHandler;

/** 活动节点：执行业务 handler，语义与串行模型中的单个步骤完全一致（含重试/超时）。 */
public record ActivityNode(String name, StepHandler handler, RetryPolicy retryPolicy) implements WorkflowNode {

    public ActivityNode {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("activity name must not be blank");
        }
        if (handler == null) {
            throw new IllegalArgumentException("activity handler must not be null");
        }
        if (retryPolicy == null) {
            throw new IllegalArgumentException("retryPolicy must not be null");
        }
    }
}
