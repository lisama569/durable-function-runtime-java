package com.github.lisama569.durablefunctionruntimejava.workflow;

import java.util.List;

/**
 * 并行汇合逻辑：整个并行节点生命周期内成功恰好一次；在 worker 线程（而非事件循环）中执行，
 * 因此单个实例的汇合再慢或阻塞都不会拖住同进程其他实例的推进。
 *
 * <p>引擎保证传入 {@code outputs} 时按<b>分支定义顺序</b>排列，
 * 因此结果与各路实际完成的先后顺序无关。</p>
 *
 * <p>失败与超时语义与普通步骤一致（策略由 {@code ParallelNode.joinRetryPolicy()} 定义）：</p>
 * <ul>
 *   <li>抛 {@link com.github.lisama569.durablefunctionruntimejava.engine.RetryableException}
 *       或执行超过 {@code stepTimeoutMillis}（均按可重试处理）：按指数退避重试；重试只重跑汇合本身，
 *       已完成的分支不会回滚或重跑；</li>
 *   <li>抛 {@link com.github.lisama569.durablefunctionruntimejava.engine.NonRetryableException}
 *       或其他未分类异常：实例立即 {@code FAILED}；</li>
 *   <li>重试/超时次数耗尽：实例进入确定终态（{@code FAILED} / {@code TIMED_OUT}），
 *       其余在跑任务被取消，不留下后台任务继续改状态。</li>
 * </ul>
 *
 * <p>超时或终态取消时执行线程会收到中断信号，汇合逻辑应及时响应中断。
 * 由于 at-least-once 重放（重试、超时中断、崩溃恢复），汇合的对外副作用必须以
 * {@link JoinContext#idempotencyKey()} 幂等。</p>
 */
@FunctionalInterface
public interface JoinHandler {
    String join(JoinContext context, List<String> outputsInBranchOrder) throws Exception;
}
