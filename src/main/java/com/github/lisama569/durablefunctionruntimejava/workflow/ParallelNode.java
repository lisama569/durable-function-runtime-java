package com.github.lisama569.durablefunctionruntimejava.workflow;

import com.github.lisama569.durablefunctionruntimejava.domain.RetryPolicy;

import java.util.List;

/**
 * 并行扇出节点：同时拉起所有分支的首个任务，各分支内部顺序推进；
 * 全部分支完成后汇合恰好一次，汇合输入按分支定义顺序排列，与完成先后无关。
 *
 * <p>各路任务沿用各自的重试/超时/失败语义；<b>汇合（join）也在 worker 线程执行，
 * 沿用与普通步骤一致的 {@link #joinRetryPolicy}（退避重试、超时看门狗、失败终态）</b>。
 * 任一路或汇合彻底失败/超时，整个实例立即落入确定终态，并取消其余仍在运行的任务。</p>
 */
public record ParallelNode(String name, List<FlowBranch> branches, JoinHandler join,
                           RetryPolicy joinRetryPolicy) implements FlowNode {

    public ParallelNode {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("parallel name must not be blank");
        if (branches == null || branches.size() < 2) {
            throw new IllegalArgumentException("parallel must define at least two branches");
        }
        if (join == null) throw new IllegalArgumentException("join handler must not be null");
        if (joinRetryPolicy == null) throw new IllegalArgumentException("joinRetryPolicy must not be null");
        branches = List.copyOf(branches);
    }

    /** 未显式指定汇合策略时使用与普通步骤相同的默认重试/超时语义。 */
    public ParallelNode(String name, List<FlowBranch> branches, JoinHandler join) {
        this(name, branches, join, RetryPolicy.defaults());
    }
}
