package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;

/**
 * 业务任务节点：在 worker 线程执行 {@link StepHandler}，沿用既有的重试、超时与失败语义。
 */
public record TaskNode(String name, StepHandler handler, RetryPolicy retryPolicy) implements FlowNode {

    public TaskNode {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("task name must not be blank");
        if (handler == null) throw new IllegalArgumentException("task handler must not be null");
        if (retryPolicy == null) throw new IllegalArgumentException("retryPolicy must not be null");
    }
}
