package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 并行汇合逻辑：在 worker 线程中执行（不阻塞引擎事件循环），整个并行节点生命周期内逻辑上最多生效一次。
 *
 * <p>引擎保证传入 {@code outputs} 时按<b>分支定义顺序</b>排列，
 * 因此结果与各路实际完成的先后顺序无关。</p>
 *
 * <p>失败/超时语义与普通步骤一致：抛 {@code RetryableException} 按退避重试；
 * 超过 {@code joinRetryPolicy.stepTimeoutMillis} 判超时（执行线程收到中断信号）；
 * 抛 {@code NonRetryableException} 或重试耗尽则实例进入确定终态。
 * 重试/崩溃可能导致本方法被再次调用，对外副作用必须以 {@link JoinContext#idempotencyKey()} 幂等。</p>
 */
@FunctionalInterface
public interface JoinHandler {
    String join(JoinContext context, List<String> outputsInBranchOrder) throws Exception;
}
